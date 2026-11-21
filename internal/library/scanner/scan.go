package scanner

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/storage"
)

// Options configures a Scanner instance.
type Options struct {
	Outbox *events.Outbox
	Now    func() time.Time
}

// ScanOptions configures a single library scan pass.
type ScanOptions struct {
	// Workers caps concurrent file inspections per root walk (ADR-0003 §3, M1B-D2).
	// Zero selects DefaultInspectionWorkers; negative values return ErrInvalidWorkerCount.
	Workers int

	// Formats selects the ContainerReader registry used to inspect archives.
	// When nil, WalkRoot uses a default CBZ registry.
	Formats *container.FormatRegistry

	// OpenDir optionally overrides opening a directory for incremental pre-check reads.
	OpenDir func(rawDir []byte) (DirStream, error)

	// ReadDir optionally overrides os.ReadDir during root traversal and pre-checks.
	ReadDir func(rawDir []byte) ([]os.DirEntry, error)

	// Stat optionally overrides os.Stat during root traversal.
	Stat func(rawPath []byte) (os.FileInfo, error)

	// Open optionally overrides read-only file opening for archive inspection.
	Open func(ctx context.Context, rawPath []byte) (ReadAtCloser, error)
}

// ChangedFile pairs an existing media asset with fresh disk inspection data when
// a known raw path changed size or modification time (or was previously missing),
// supplying M1-T10.6 and M1-T22.2 reconciliation.
type ChangedFile struct {
	Existing  domain.MediaAsset
	Inspected InspectedFile
}

// RootScanResult summarizes the walk and indexing pass for one library root.
type RootScanResult struct {
	LibraryRootID  string
	RootPath       domain.PathIdentity
	Complete       bool
	Err            error
	IndexedCount   int
	UnchangedCount int
	Changed        []ChangedFile
	Skipped        []SkippedEntry
	SkippedCount   int
}

// ScanResult summarizes a multi-root library scan pass.
type ScanResult struct {
	LibraryID      string
	PreCheck       PreCheckResult
	Offline        bool
	ApplyRemovals  bool
	Complete       bool
	Roots          []RootScanResult
	IndexedCount   int
	UnchangedCount int
	ChangedCount   int
	SkippedCount   int
}

// Scanner walks every root of a library, skips unchanged known files without
// opening them, collects inspection data for changed existing paths, and indexes
// newly discovered CBZ files sequentially in one transaction per file.
type Scanner struct {
	tx      storage.Transactor
	indexer *Indexer
	outbox  *events.Outbox
	now     func() time.Time
}

// NewScanner constructs a Scanner backed by tx.
func NewScanner(tx storage.Transactor, opts Options) (*Scanner, error) {
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	outbox := opts.Outbox
	if outbox == nil {
		outbox = events.NewOutbox(events.WithClock(nowFn))
	}
	idx, err := NewIndexer(tx, IndexerOptions{
		Outbox: outbox,
		Now:    nowFn,
	})
	if err != nil {
		return nil, err
	}
	return &Scanner{
		tx:      tx,
		indexer: idx,
		outbox:  outbox,
		now:     nowFn,
	}, nil
}

// ScanLibrary pre-checks every root of libraryID and stops if any failed
// (M1B-D10). When all roots are online, it walks every root, skips files
// matching recorded size and mtime without opening them, records inspection
// data for changed existing paths, and indexes new CBZ files through Indexer
// from a single sequential writer.
func (s *Scanner) ScanLibrary(ctx context.Context, libraryID string, opts ScanOptions) (ScanResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.tx == nil || s.indexer == nil || s.outbox == nil || s.now == nil {
		return ScanResult{}, errors.New("scanner: scanner is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return ScanResult{}, err
	}
	if strings.TrimSpace(libraryID) == "" {
		return ScanResult{}, domain.ErrEmptyLibraryID
	}
	if opts.Workers < 0 {
		return ScanResult{}, ErrInvalidWorkerCount
	}

	preCheck, err := s.PreCheckLibrary(ctx, libraryID, PreCheckOptions{
		Stat:    opts.Stat,
		OpenDir: opts.OpenDir,
		ReadDir: opts.ReadDir,
	})
	if err != nil {
		return ScanResult{}, err
	}

	res := ScanResult{
		LibraryID:     preCheck.LibraryID,
		PreCheck:      preCheck,
		Offline:       !preCheck.ShouldScan,
		ApplyRemovals: preCheck.ApplyRemovals,
	}
	if !preCheck.ShouldScan {
		res.Complete = false
		res.ApplyRemovals = false
		return res, nil
	}

	_, currentRoots, err := s.loadLibraryAndRoots(ctx, libraryID)
	if err != nil {
		return ScanResult{}, err
	}
	if !rootSnapshotPathsMatch(preCheck.Roots, currentRoots) {
		return ScanResult{}, ErrStalePreCheck
	}

	res.Complete = true
	res.Roots = make([]RootScanResult, 0, len(preCheck.Roots))
	var rootErrs []error
	for _, checkedRoot := range preCheck.Roots {
		if err := ctx.Err(); err != nil {
			res.Complete = false
			res.ApplyRemovals = false
			return res, err
		}
		root := domain.LibraryRoot{
			ID:        checkedRoot.LibraryRootID,
			LibraryID: preCheck.LibraryID,
			Path:      checkedRoot.RootPath,
			State:     checkedRoot.State,
		}
		rootRes, loadErr := s.scanOneRoot(ctx, preCheck.LibraryID, root, opts)
		if loadErr != nil {
			res.Complete = false
			res.ApplyRemovals = false
			return res, loadErr
		}
		res.recordRoot(rootRes)
		if rootRes.Err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				res.Complete = false
				res.ApplyRemovals = false
				return res, ctxErr
			}
			rootErrs = append(rootErrs, rootRes.Err)
		}
	}
	res.ApplyRemovals = res.Complete && preCheck.ApplyRemovals
	if len(rootErrs) > 0 {
		return res, errors.Join(rootErrs...)
	}
	return res, nil
}

func (r *ScanResult) recordRoot(rootRes RootScanResult) {
	r.Roots = append(r.Roots, rootRes)
	r.IndexedCount += rootRes.IndexedCount
	r.UnchangedCount += rootRes.UnchangedCount
	r.ChangedCount += len(rootRes.Changed)
	r.SkippedCount += rootRes.SkippedCount
	if !rootRes.Complete {
		r.Complete = false
	}
}

func rootSnapshotPathsMatch(checked []RootCheckResult, current []domain.LibraryRoot) bool {
	if len(checked) != len(current) {
		return false
	}
	byID := make(map[string]domain.LibraryRoot, len(current))
	for _, root := range current {
		byID[root.ID] = root
	}
	for _, item := range checked {
		cur, ok := byID[item.LibraryRootID]
		if !ok || !bytes.Equal(item.RootPath.Bytes, cur.Path.Bytes) {
			return false
		}
	}
	return true
}

func (s *Scanner) loadLibraryAndRoots(ctx context.Context, libraryID string) (*domain.Library, []domain.LibraryRoot, error) {
	var (
		lib   *domain.Library
		roots []domain.LibraryRoot
	)
	err := s.tx.WithTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()
		if repo == nil {
			return errors.New("scanner: library repository is required")
		}
		var err error
		lib, err = repo.GetLibraryByID(ctx, libraryID)
		if err != nil {
			return err
		}
		roots, err = repo.ListLibraryRoots(ctx, lib.ID)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return lib, roots, nil
}

func (s *Scanner) loadRootAssets(ctx context.Context, rootID string) ([]domain.MediaAsset, error) {
	var assets []domain.MediaAsset
	err := s.tx.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		if cw == nil {
			return errors.New("scanner: catalogue write repository is required")
		}
		var err error
		assets, err = cw.ListAssetsByRoot(ctx, rootID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return assets, nil
}

func (s *Scanner) scanOneRoot(
	ctx context.Context,
	libraryID string,
	root domain.LibraryRoot,
	opts ScanOptions,
) (RootScanResult, error) {
	assets, err := s.loadRootAssets(ctx, root.ID)
	if err != nil {
		return RootScanResult{}, err
	}

	knownByPath, knownForWalk := buildKnownMaps(assets)
	state := &rootScanState{
		ctx:         ctx,
		libraryID:   libraryID,
		rootID:      root.ID,
		indexer:     s.indexer,
		knownByPath: knownByPath,
		result: RootScanResult{
			LibraryRootID: root.ID,
			RootPath:      root.Path,
		},
	}

	walkOpts := WalkOptions{
		Workers: opts.Workers,
		Known:   knownForWalk,
		Formats: opts.Formats,
		OnEntry: state.handleEntry,
		ReadDir: opts.ReadDir,
		Stat:    opts.Stat,
		Open:    opts.Open,
	}
	walkRes, walkErr := WalkRoot(ctx, root.Path.Bytes, walkOpts)
	state.result.Err = walkErr
	state.result.Complete = walkErr == nil && walkRes.Complete && ctx.Err() == nil
	return state.result, nil
}

func buildKnownMaps(assets []domain.MediaAsset) (map[string]domain.MediaAsset, map[string]KnownFile) {
	knownByPath := make(map[string]domain.MediaAsset, len(assets))
	knownForWalk := make(map[string]KnownFile, len(assets))
	for _, asset := range assets {
		key := string(asset.RelPathBytes)
		knownByPath[key] = asset
		if asset.MissingSince == nil {
			knownForWalk[key] = KnownFile{
				Size:  asset.FileSize,
				Mtime: asset.FileMtime,
			}
		}
	}
	return knownByPath, knownForWalk
}

type rootScanState struct {
	ctx         context.Context
	libraryID   string
	rootID      string
	indexer     *Indexer
	knownByPath map[string]domain.MediaAsset
	result      RootScanResult
}

func (st *rootScanState) handleEntry(entry WalkEntry) error {
	if err := st.ctx.Err(); err != nil {
		return err
	}
	switch entry.Kind {
	case EntryUnchanged:
		if entry.Unchanged != nil {
			st.result.UnchangedCount++
		}
		return nil
	case EntrySkipped:
		if entry.Skipped != nil {
			st.result.Skipped = append(st.result.Skipped, *entry.Skipped)
			st.result.SkippedCount++
		}
		return nil
	case EntryInspected:
		if entry.Inspected == nil {
			return nil
		}
		return st.handleInspected(*entry.Inspected)
	default:
		return nil
	}
}

func (st *rootScanState) handleInspected(inspected InspectedFile) error {
	if existing, ok := st.knownByPath[string(inspected.RelPath.Bytes)]; ok {
		st.result.Changed = append(st.result.Changed, ChangedFile{
			Existing:  existing,
			Inspected: inspected,
		})
		return nil
	}
	_, err := st.indexer.IndexFile(st.ctx, IndexFileInput{
		LibraryID:     st.libraryID,
		LibraryRootID: st.rootID,
		Inspected:     inspected,
	})
	if err != nil {
		if ctxErr := st.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		st.result.Skipped = append(st.result.Skipped, SkippedEntry{
			RelPath: inspected.RelPath,
			Reason:  SkipReasonIndexFailed,
			Err:     err,
		})
		st.result.SkippedCount++
		return nil
	}
	st.result.IndexedCount++
	return nil
}
