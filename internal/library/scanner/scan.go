package scanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/storage"
)

const (
	// MissingGraceDaysSettingKey is the PostgreSQL system_settings key configuring
	// how many days a missing asset remains in soft-delete grace before retirement
	// sets retired_at (ADR-0019 §6).
	MissingGraceDaysSettingKey = "scanner.missing_grace_days"

	// DefaultMissingGraceDays is the default missing-file grace period in days (ADR-0019 §6).
	DefaultMissingGraceDays = 7

	// DefaultMissingGracePeriod is the default missing-file grace duration (7 days).
	DefaultMissingGracePeriod = DefaultMissingGraceDays * 24 * time.Hour
)

var (
	// ErrInvalidMissingGracePeriod is returned when the configured missing grace period is
	// negative or cannot be parsed as a non-negative integer number of days.
	ErrInvalidMissingGracePeriod = errors.New("scanner: invalid missing grace period")

	// ErrLibraryNotFound is returned when the requested library does not exist.
	// It wraps storage.ErrNotFound while distinguishing library absence from stale
	// volume or asset lookups during reconciliation.
	ErrLibraryNotFound = errors.New("scanner: library not found")

	// ErrIncompleteWalk is returned by the SCAN job handler when an ONLINE library's
	// walk finishes incomplete (for example, due to an unreadable subtree) so the
	// job retries with backoff instead of succeeding without indexing.
	ErrIncompleteWalk = errors.New("scanner: incomplete walk of online library")
)

func wrapLibraryNotFound(err error) error {
	if errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w: %w", ErrLibraryNotFound, err)
	}
	return err
}

// Options configures a Scanner instance.
type Options struct {
	Outbox *events.Outbox
	Now    func() time.Time
	// MissingGracePeriod optionally overrides the MissingGraceDaysSettingKey
	// system_settings value when non-nil (ADR-0019 §6).
	MissingGracePeriod *time.Duration
	// Open optionally overrides default read-only file opening for archive
	// inspection when ScanOptions.Open is nil.
	Open func(ctx context.Context, rawPath []byte) (ReadAtCloser, error)
	// ReadDir optionally overrides directory reads during pre-check and root
	// walks when ScanOptions.ReadDir is nil.
	ReadDir func(rawDir []byte) ([]os.DirEntry, error)
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

	// OnProgress, when non-nil, is called sequentially as scanned files are observed
	// across the library's roots with the cumulative scanned file count and the
	// root-relative display path of the current file.
	OnProgress func(scannedCount int64, currentPath string)

	// CheckCancel, when non-nil, is called at scan and file boundaries during
	// walking and reconciliation. Returning a non-nil error aborts the scan
	// immediately before further files or reconciliation writes run.
	CheckCancel func(context.Context) error
}

type cancelProbeError struct {
	err error
}

func (e cancelProbeError) Error() string { return e.err.Error() }
func (e cancelProbeError) Unwrap() error { return e.err }

func checkScanCancel(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	if err := fn(ctx); err != nil {
		return cancelProbeError{err: err}
	}
	return nil
}

func isScanAbortErr(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	var probe cancelProbeError
	return errors.As(err, &probe)
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
	MovedCount     int
	ModifiedCount  int
	RestoredCount  int
	MissingCount   int
	RetiredCount   int
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
	MovedCount     int
	ModifiedCount  int
	RestoredCount  int
	MissingCount   int
	RetiredCount   int
	UnchangedCount int
	ChangedCount   int
	SkippedCount   int
}

// Scanner walks every root of a library, skips unchanged known files without
// opening them, collects inspection data for changed existing paths, and indexes
// newly discovered CBZ files sequentially in one transaction per file.
type Scanner struct {
	tx                 storage.Transactor
	indexer            *Indexer
	outbox             *events.Outbox
	now                func() time.Time
	missingGracePeriod *time.Duration
	open               func(ctx context.Context, rawPath []byte) (ReadAtCloser, error)
	readDir            func(rawDir []byte) ([]os.DirEntry, error)
}

// NewScanner constructs a Scanner backed by tx.
func NewScanner(tx storage.Transactor, opts Options) (*Scanner, error) {
	if opts.MissingGracePeriod != nil && *opts.MissingGracePeriod < 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidMissingGracePeriod, *opts.MissingGracePeriod)
	}
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
	var graceOverride *time.Duration
	if opts.MissingGracePeriod != nil {
		d := *opts.MissingGracePeriod
		graceOverride = &d
	}
	return &Scanner{
		tx:                 tx,
		indexer:            idx,
		outbox:             outbox,
		now:                nowFn,
		missingGracePeriod: graceOverride,
		open:               opts.Open,
		readDir:            opts.ReadDir,
	}, nil
}

func (s *Scanner) validateScanRequest(ctx context.Context, libraryID string, opts ScanOptions) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.tx == nil || s.indexer == nil || s.outbox == nil || s.now == nil {
		return ctx, errors.New("scanner: scanner is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	if strings.TrimSpace(libraryID) == "" {
		return ctx, domain.ErrEmptyLibraryID
	}
	if opts.Workers < 0 {
		return ctx, ErrInvalidWorkerCount
	}
	return ctx, checkScanCancel(ctx, opts.CheckCancel)
}

func (s *Scanner) walkOnlineRoots(
	ctx context.Context,
	preCheck PreCheckResult,
	assetsByRoot map[string][]domain.MediaAsset,
	obs *libraryWalkObservations,
	opts ScanOptions,
	res *ScanResult,
) ([]error, error) {
	var scannedCount int64
	res.Complete = true
	res.Roots = make([]RootScanResult, 0, len(preCheck.Roots))
	var rootErrs []error

	for _, checkedRoot := range preCheck.Roots {
		if err := ctx.Err(); err != nil {
			res.Complete = false
			res.ApplyRemovals = false
			return nil, err
		}
		if err := checkScanCancel(ctx, opts.CheckCancel); err != nil {
			res.Complete = false
			res.ApplyRemovals = false
			return nil, err
		}
		root := domain.LibraryRoot{
			ID:        checkedRoot.LibraryRootID,
			LibraryID: preCheck.LibraryID,
			Path:      checkedRoot.RootPath,
			State:     checkedRoot.State,
		}
		rootRes := s.scanOneRoot(ctx, root, assetsByRoot[root.ID], obs, &scannedCount, opts)
		res.recordRoot(rootRes)
		if rootRes.Err == nil {
			continue
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			res.Complete = false
			res.ApplyRemovals = false
			return nil, ctxErr
		}
		if isScanAbortErr(ctx, rootRes.Err) {
			res.Complete = false
			res.ApplyRemovals = false
			return nil, rootRes.Err
		}
		rootErrs = append(rootErrs, rootRes.Err)
	}
	return rootErrs, nil
}

// ScanLibrary pre-checks every root of libraryID and stops if any failed
// (M1B-D10). When all roots are online, it walks every root, skips files
// matching recorded size and mtime without opening them, records inspection
// data for changed existing paths, and indexes new CBZ files through Indexer
// from a single sequential writer.
func (s *Scanner) ScanLibrary(ctx context.Context, libraryID string, opts ScanOptions) (ScanResult, error) {
	ctx, err := s.validateScanRequest(ctx, libraryID, opts)
	if err != nil {
		return ScanResult{}, err
	}

	readDirFn := opts.ReadDir
	if readDirFn == nil {
		readDirFn = s.readDir
	}
	preCheckOpts := PreCheckOptions{
		Stat:    opts.Stat,
		OpenDir: opts.OpenDir,
		ReadDir: readDirFn,
	}
	preCheck, err := s.PreCheckLibrary(ctx, libraryID, preCheckOpts)
	if err != nil {
		return ScanResult{}, err
	}

	res := ScanResult{
		LibraryID: preCheck.LibraryID,
		PreCheck:  preCheck,
		Offline:   !preCheck.ShouldScan,
	}
	if err := checkScanCancel(ctx, opts.CheckCancel); err != nil {
		return res, err
	}
	if !preCheck.ShouldScan {
		res.Complete = false
		return res, nil
	}

	currentRoots, preWalkAssets, gracePeriod, err := s.loadLibraryScanSnapshot(ctx, libraryID)
	if err != nil {
		return ScanResult{}, err
	}
	if !rootSnapshotMatches(preCheck.Roots, currentRoots) || !allRootsOnline(currentRoots) {
		return ScanResult{}, ErrStalePreCheck
	}

	assetsByRoot := indexPreWalkAssetsByRoot(preWalkAssets, len(preCheck.Roots))
	obs := newLibraryWalkObservations()
	rootErrs, abortErr := s.walkOnlineRoots(ctx, preCheck, assetsByRoot, obs, opts, &res)
	if abortErr != nil {
		return res, abortErr
	}
	if res.Complete && len(rootErrs) == 0 && ctx.Err() == nil {
		if err := s.reconcileLibraryPass(ctx, preCheck.LibraryID, preCheck.Roots, preWalkAssets, obs, gracePeriod, preCheckOpts, opts.CheckCancel, &res); err != nil {
			res.Complete = false
			res.ApplyRemovals = false
			return res, err
		}
	}
	if err := checkScanCancel(ctx, opts.CheckCancel); err != nil {
		res.Complete = false
		res.ApplyRemovals = false
		return res, err
	}
	res.ApplyRemovals = res.Complete && len(rootErrs) == 0 && ctx.Err() == nil && preCheck.ApplyRemovals
	if len(rootErrs) > 0 {
		return res, errors.Join(rootErrs...)
	}
	return res, nil
}

func (r *ScanResult) recordRoot(rootRes RootScanResult) {
	r.Roots = append(r.Roots, rootRes)
	r.IndexedCount += rootRes.IndexedCount
	r.MovedCount += rootRes.MovedCount
	r.ModifiedCount += rootRes.ModifiedCount
	r.RestoredCount += rootRes.RestoredCount
	r.MissingCount += rootRes.MissingCount
	r.RetiredCount += rootRes.RetiredCount
	r.UnchangedCount += rootRes.UnchangedCount
	r.ChangedCount += len(rootRes.Changed)
	r.SkippedCount += rootRes.SkippedCount
	if !rootRes.Complete {
		r.Complete = false
	}
}

func (r *ScanResult) findRootIndex(rootID string) int {
	for i := range r.Roots {
		if r.Roots[i].LibraryRootID == rootID {
			return i
		}
	}
	return -1
}

func (r *ScanResult) recordReconcileMoved(rootID string) {
	r.MovedCount++
	if i := r.findRootIndex(rootID); i >= 0 {
		r.Roots[i].MovedCount++
	}
}

func (r *ScanResult) recordReconcileModified(rootID string) {
	r.ModifiedCount++
	if i := r.findRootIndex(rootID); i >= 0 {
		r.Roots[i].ModifiedCount++
	}
}

func (r *ScanResult) recordReconcileRestored(rootID string) {
	r.RestoredCount++
	if i := r.findRootIndex(rootID); i >= 0 {
		r.Roots[i].RestoredCount++
	}
}

func (r *ScanResult) recordReconcileMissing(rootID string) {
	r.MissingCount++
	if i := r.findRootIndex(rootID); i >= 0 {
		r.Roots[i].MissingCount++
	}
}

func (r *ScanResult) recordReconcileRetired(rootID string) {
	r.RetiredCount++
	if i := r.findRootIndex(rootID); i >= 0 {
		r.Roots[i].RetiredCount++
	}
}

func (r *ScanResult) recordReconcileIndexed(rootID string) {
	r.IndexedCount++
	if i := r.findRootIndex(rootID); i >= 0 {
		r.Roots[i].IndexedCount++
	}
}

func (r *ScanResult) recordReconcileSkipped(rootID string, skip SkippedEntry) {
	r.SkippedCount++
	if i := r.findRootIndex(rootID); i >= 0 {
		r.Roots[i].Skipped = append(r.Roots[i].Skipped, skip)
		r.Roots[i].SkippedCount++
	}
}

func rootSnapshotMatches(checked []RootCheckResult, current []domain.LibraryRoot) bool {
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
		if cur.LastCheckedAt == nil || !cur.LastCheckedAt.UTC().Equal(item.CheckedAt.UTC()) {
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
			return wrapLibraryNotFound(err)
		}
		roots, err = repo.ListLibraryRoots(ctx, lib.ID)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return lib, roots, nil
}

func (s *Scanner) loadLibraryScanSnapshot(ctx context.Context, libraryID string) ([]domain.LibraryRoot, []domain.MediaAsset, time.Duration, error) {
	var (
		roots       []domain.LibraryRoot
		assets      []domain.MediaAsset
		gracePeriod time.Duration
	)
	err := s.tx.WithTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()
		if repo == nil {
			return errors.New("scanner: library repository is required")
		}
		cw := tx.CatalogueWrites()
		if cw == nil {
			return errors.New("scanner: catalogue write repository is required")
		}
		lib, err := repo.GetLibraryByID(ctx, libraryID)
		if err != nil {
			return wrapLibraryNotFound(err)
		}
		roots, err = repo.ListLibraryRoots(ctx, lib.ID)
		if err != nil {
			return err
		}
		assets, err = cw.ListAssetsByLibrary(ctx, lib.ID)
		if err != nil {
			return err
		}
		gracePeriod, err = s.resolveMissingGracePeriod(ctx, tx)
		return err
	})
	if err != nil {
		return nil, nil, 0, err
	}
	return roots, assets, gracePeriod, nil
}

func (s *Scanner) resolveMissingGracePeriod(ctx context.Context, tx storage.Tx) (time.Duration, error) {
	if s.missingGracePeriod != nil {
		return *s.missingGracePeriod, nil
	}
	settings := tx.Settings()
	if settings == nil {
		return 0, errors.New("scanner: settings repository is required")
	}
	setting, err := settings.GetSetting(ctx, MissingGraceDaysSettingKey)
	if errors.Is(err, storage.ErrNotFound) {
		return DefaultMissingGracePeriod, nil
	}
	if err != nil {
		return 0, err
	}
	if setting == nil {
		return DefaultMissingGracePeriod, nil
	}
	raw := strings.TrimSpace(setting.ValueString())
	if raw == "" {
		return DefaultMissingGracePeriod, nil
	}
	days, err := strconv.Atoi(raw)
	if err != nil || days < 0 || days > math.MaxInt64/int(24*time.Hour) {
		return 0, fmt.Errorf("%w: %s=%q", ErrInvalidMissingGracePeriod, MissingGraceDaysSettingKey, raw)
	}
	return time.Duration(days) * 24 * time.Hour, nil
}

func indexPreWalkAssetsByRoot(assets []domain.MediaAsset, rootCap int) map[string][]domain.MediaAsset {
	byRoot := make(map[string][]domain.MediaAsset, rootCap)
	for _, asset := range assets {
		byRoot[asset.LibraryRootID] = append(byRoot[asset.LibraryRootID], asset)
	}
	return byRoot
}

func (s *Scanner) scanOneRoot(
	ctx context.Context,
	root domain.LibraryRoot,
	assets []domain.MediaAsset,
	obs *libraryWalkObservations,
	scannedCount *int64,
	opts ScanOptions,
) RootScanResult {
	if scannedCount == nil {
		var local int64
		scannedCount = &local
	}
	knownByPath, knownForWalk := buildKnownMaps(assets)
	state := &rootScanState{
		ctx:          ctx,
		rootID:       root.ID,
		knownByPath:  knownByPath,
		obs:          obs,
		scannedCount: scannedCount,
		onProgress:   opts.OnProgress,
		result: RootScanResult{
			LibraryRootID: root.ID,
			RootPath:      root.Path,
		},
	}

	openFn := opts.Open
	if openFn == nil {
		openFn = s.open
	}
	readDirFn := opts.ReadDir
	if readDirFn == nil {
		readDirFn = s.readDir
	}
	var walkCheckCancel func(context.Context) error
	if opts.CheckCancel != nil {
		walkCheckCancel = func(checkCtx context.Context) error {
			return checkScanCancel(checkCtx, opts.CheckCancel)
		}
	}
	walkOpts := WalkOptions{
		Workers:     opts.Workers,
		Known:       knownForWalk,
		Formats:     opts.Formats,
		OnEntry:     state.handleEntry,
		CheckCancel: walkCheckCancel,
		ReadDir:     readDirFn,
		Stat:        opts.Stat,
		Open:        openFn,
	}
	walkRes, walkErr := WalkRoot(ctx, root.Path.Bytes, walkOpts)
	state.result.Err = walkErr
	state.result.Complete = walkErr == nil && walkRes.Complete && ctx.Err() == nil
	return state.result
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
	ctx          context.Context
	rootID       string
	knownByPath  map[string]domain.MediaAsset
	obs          *libraryWalkObservations
	scannedCount *int64
	onProgress   func(scannedCount int64, currentPath string)
	result       RootScanResult
}

func (st *rootScanState) reportFileProgress(displayPath string) {
	if st.scannedCount == nil {
		return
	}
	*st.scannedCount++
	if st.onProgress != nil && displayPath != "" {
		st.onProgress(*st.scannedCount, displayPath)
	}
}

func (st *rootScanState) handleEntry(entry WalkEntry) error {
	if err := st.ctx.Err(); err != nil {
		return err
	}
	switch entry.Kind {
	case EntryUnchanged:
		if entry.Unchanged != nil {
			st.result.UnchangedCount++
			loc := assetLocation{
				rootID:       st.rootID,
				relPathBytes: string(entry.Unchanged.RelPath.Bytes),
			}
			st.obs.seenUnchanged[loc] = struct{}{}
			st.reportFileProgress(entry.Unchanged.RelPath.Display)
		}
		return nil
	case EntrySkipped:
		if entry.Skipped != nil {
			st.result.Skipped = append(st.result.Skipped, *entry.Skipped)
			st.result.SkippedCount++
			if !entry.Skipped.IsDir {
				loc := assetLocation{
					rootID:       st.rootID,
					relPathBytes: string(entry.Skipped.RelPath.Bytes),
				}
				st.obs.seenSkipped[loc] = struct{}{}
			}
		}
		return nil
	case EntryInspected:
		if entry.Inspected == nil {
			return nil
		}
		if err := st.handleInspected(*entry.Inspected); err != nil {
			return err
		}
		st.reportFileProgress(entry.Inspected.RelPath.Display)
		return nil
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
	// Defer all new asset identity assignment until every configured root has
	// completed its walk and move/modify reconciliation runs.
	st.obs.newFiles = append(st.obs.newFiles, observedFile{
		rootID:    st.rootID,
		inspected: inspected,
	})
	return nil
}
