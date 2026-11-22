package scanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/container/cbz"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/metadata/comicinfo"
)

// DefaultInspectionWorkers is the conservative default cap on concurrent file
// inspections within one root scan when Workers is zero (ADR-0003 §3, M1B-D2).
const DefaultInspectionWorkers = 2

const maxComicInfoBytes = 16 * 1024 * 1024

var (
	// ErrInvalidWorkerCount is returned when WalkOptions.Workers is negative.
	ErrInvalidWorkerCount = errors.New("scanner: inspection worker count cannot be negative")
	// ErrRootNotDirectory is returned when the configured root path is not a directory.
	ErrRootNotDirectory = errors.New("scanner: root path is not a directory")
)

// KnownFile records the previously indexed size and modification time for a
// root-relative path, used to skip unchanged files without opening them (ADR-0003 §1).
type KnownFile struct {
	Size  int64
	Mtime *time.Time
}

// SkipReason identifies why an encountered entry was skipped during a root walk.
type SkipReason string

const (
	// SkipReasonHidden indicates a dot-prefixed entry or macOS "__MACOSX" directory.
	SkipReasonHidden SkipReason = "hidden"
	// SkipReasonTemporary indicates a temporary or in-progress download/write file.
	SkipReasonTemporary SkipReason = "temporary"
	// SkipReasonNonCBZ indicates a regular file whose extension is not .cbz.
	SkipReasonNonCBZ SkipReason = "non_cbz"
	// SkipReasonNonRegular indicates a special filesystem object (FIFO, socket, device).
	SkipReasonNonRegular SkipReason = "non_regular"
	// SkipReasonUnreadable indicates an entry that could not be stat'd, read, or opened.
	SkipReasonUnreadable SkipReason = "unreadable"
	// SkipReasonCorruptArchive indicates an archive that failed sniffing, opening, or CRC/structure checks.
	SkipReasonCorruptArchive SkipReason = "corrupt_archive"
	// SkipReasonBrokenSymlink indicates a symlink whose target does not exist or cannot be resolved.
	SkipReasonBrokenSymlink SkipReason = "broken_symlink"
	// SkipReasonSymlinkCycle indicates a directory symlink pointing to an ancestor in the active walk path.
	SkipReasonSymlinkCycle SkipReason = "symlink_cycle"
	// SkipReasonIndexFailed indicates a file whose inspection succeeded but whose catalogue indexing failed.
	SkipReasonIndexFailed SkipReason = "index_failed"
)

// EntryKind classifies one result emitted by WalkRoot.
type EntryKind string

const (
	// EntryInspected identifies a new or changed CBZ archive that was opened and inspected.
	EntryInspected EntryKind = "inspected"
	// EntryUnchanged identifies a CBZ file skipped without opening because size and mtime matched Known.
	EntryUnchanged EntryKind = "unchanged"
	// EntrySkipped identifies a skipped file, directory, symlink, or corrupt/unreadable archive.
	EntrySkipped EntryKind = "skipped"
)

// SkippedEntry describes an entry skipped during traversal without aborting the walk.
type SkippedEntry struct {
	RelPath domain.PathIdentity
	IsDir   bool
	Reason  SkipReason
	Err     error
}

// UnchangedFile describes a CBZ file skipped without opening because its size
// and modification time matched the caller's known state.
type UnchangedFile struct {
	RelPath domain.PathIdentity
	Size    int64
	Mtime   time.Time
}

// InspectedFile holds the identity, fingerprint, container layout, and parsed
// ComicInfo metadata for a new or changed CBZ file.
type InspectedFile struct {
	RelPath              domain.PathIdentity
	Size                 int64
	Mtime                time.Time
	Format               domain.MediaFormat
	DigestKind           domain.DigestKind
	Digest               string
	PageCount            int
	CoverPageIndex       int
	SuppressedPages      []int
	ImageCodecs          []string
	ArchiveInvalidUTF8   bool
	ComicInfo            *comicinfo.Metadata
	ComicInfoDiagnostics []comicinfo.Diagnostic
	ComicInfoErr         error
}

// WalkEntry carries a single inspected, unchanged, or skipped outcome from WalkRoot.
type WalkEntry struct {
	Kind      EntryKind
	Inspected *InspectedFile
	Unchanged *UnchangedFile
	Skipped   *SkippedEntry
}

// WalkResult summarizes a WalkRoot invocation. Complete is true only if every
// reachable directory in the root was read and all scheduled inspections
// completed without lost subtree coverage, cancellation, or callback errors.
type WalkResult struct {
	Complete       bool
	Files          []InspectedFile
	Unchanged      []UnchangedFile
	Skipped        []SkippedEntry
	InspectedCount int
	UnchangedCount int
	SkippedCount   int
}

// ReadAtCloser is the read-only random-access file handle used to inspect an archive.
type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

// WalkOptions configures WalkRoot.
type WalkOptions struct {
	// Workers caps concurrent file inspections within this scan (ADR-0003 §3, M1B-D2).
	// Zero selects DefaultInspectionWorkers; negative values return ErrInvalidWorkerCount.
	Workers int

	// Known maps raw root-relative path bytes (string(relBytes)) to previously
	// recorded size and mtime values for skipping unchanged CBZ files without opening them.
	Known map[string]KnownFile

	// Formats selects the ContainerReader registry used to inspect archives.
	// When nil, a default registry with cbz.Format() is created.
	Formats *container.FormatRegistry

	// OnEntry, when non-nil, is called sequentially from a single goroutine as each
	// result is produced. Returning a non-nil error cancels the walk, marks the
	// result incomplete, and returns that error. When nil, WalkRoot collects all
	// results into WalkResult.
	OnEntry func(WalkEntry) error

	// ReadDir optionally overrides os.ReadDir for reading one directory at a time.
	ReadDir func(rawDir []byte) ([]os.DirEntry, error)

	// Stat optionally overrides os.Stat for following symlinks and inspecting file metadata.
	Stat func(rawPath []byte) (os.FileInfo, error)

	// Open optionally overrides read-only file opening for archive inspection.
	Open func(ctx context.Context, rawPath []byte) (ReadAtCloser, error)

	// CheckCancel, when non-nil, is called before admitting each new archive
	// inspection task in a worker. Returning a non-nil error aborts the walk
	// and propagates that error. It may be called concurrently when Workers > 1.
	CheckCancel func(context.Context) error
}

type inspectTask struct {
	relPath domain.PathIdentity
	absPath []byte
	size    int64
	mtime   time.Time
}

type statReadAtCloser interface {
	Stat() (os.FileInfo, error)
}

type walkRuntime struct {
	opts              WalkOptions
	root              []byte
	workers           int
	formats           *container.FormatRegistry
	readDir           func([]byte) ([]os.DirEntry, error)
	stat              func([]byte) (os.FileInfo, error)
	open              func(context.Context, []byte) (ReadAtCloser, error)
	subtreeIncomplete atomic.Bool
}

func (rt *walkRuntime) markSubtreeIncomplete() {
	rt.subtreeIncomplete.Store(true)
}

// WalkRoot sequentially walks rootBytes (reading one directory at a time in
// deterministic raw-byte order) and inspects new or changed CBZ files on at most
// WalkOptions.Workers concurrent workers (ADR-0003 §§1, 3, 5; M1B-D2, M1B-D6).
//
// Scanner access is strictly read-only. Symlinks to readable files and
// directories (including targets outside rootBytes) are followed while preserving
// the raw link-relative path under rootBytes as asset identity. Directory cycles
// are detected against the active traversal ancestry so separate paths to a
// shared target can still be walked independently.
func WalkRoot(ctx context.Context, rootBytes []byte, opts WalkOptions) (WalkResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := domain.ValidateRootPathBytes(rootBytes); err != nil {
		return WalkResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return WalkResult{}, err
	}

	rt, err := newWalkRuntime(rootBytes, opts)
	if err != nil {
		return WalkResult{}, err
	}

	rootInfo, err := rt.stat(rt.root)
	if err != nil {
		return WalkResult{}, fmt.Errorf("scanner: stat root: %w", err)
	}
	if !rootInfo.IsDir() {
		return WalkResult{}, ErrRootNotDirectory
	}

	return rt.execute(ctx, rootInfo)
}

func newWalkRuntime(rootBytes []byte, opts WalkOptions) (*walkRuntime, error) {
	if opts.Workers < 0 {
		return nil, ErrInvalidWorkerCount
	}
	workers := opts.Workers
	if workers == 0 {
		workers = DefaultInspectionWorkers
	}

	formats := opts.Formats
	if formats == nil {
		formats = container.NewFormatRegistry()
		if err := formats.Register(cbz.Format()); err != nil {
			return nil, fmt.Errorf("scanner: register cbz format: %w", err)
		}
	}

	readDirFn := opts.ReadDir
	if readDirFn == nil {
		readDirFn = func(rawDir []byte) ([]os.DirEntry, error) {
			return os.ReadDir(string(rawDir))
		}
	}
	statFn := opts.Stat
	if statFn == nil {
		statFn = func(rawPath []byte) (os.FileInfo, error) {
			return os.Stat(string(rawPath))
		}
	}
	openFn := opts.Open
	if openFn == nil {
		openFn = func(ctx context.Context, rawPath []byte) (ReadAtCloser, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return openRawReadOnlyNonBlock(rawPath)
		}
	}

	return &walkRuntime{
		opts:    opts,
		root:    trimTrailingSlashes(rootBytes),
		workers: workers,
		formats: formats,
		readDir: readDirFn,
		stat:    statFn,
		open:    openFn,
	}, nil
}

func openRawReadOnlyNonBlock(rawPath []byte) (*os.File, error) {
	if len(rawPath) == 0 || bytes.IndexByte(rawPath, 0) >= 0 {
		return nil, paths.ErrInvalidRawPath
	}
	return os.OpenFile(string(rawPath), os.O_RDONLY|syscall.O_NONBLOCK, 0)
}

func trimTrailingSlashes(raw []byte) []byte {
	out := slices.Clone(raw)
	for len(out) > 1 && out[len(out)-1] == '/' {
		out = out[:len(out)-1]
	}
	return out
}

func (rt *walkRuntime) execute(ctx context.Context, rootInfo os.FileInfo) (WalkResult, error) {
	walkCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	tasksCh := make(chan inspectTask)
	resultsCh := make(chan WalkEntry)

	var workersWG sync.WaitGroup
	workersWG.Add(rt.workers)
	for range rt.workers {
		go func() {
			defer workersWG.Done()
			rt.runWorker(walkCtx, cancel, tasksCh, resultsCh)
		}()
	}

	go func() {
		defer close(resultsCh)
		defer workersWG.Wait()
		defer close(tasksCh)
		if err := rt.walkDir(walkCtx, nil, rt.root, []os.FileInfo{rootInfo}, true, tasksCh, resultsCh); err != nil {
			cancel(err)
		}
	}()

	res := rt.collectResults(walkCtx, cancel, resultsCh)
	if cause := context.Cause(walkCtx); cause != nil {
		res.Complete = false
		return res, cause
	}

	res.Complete = !rt.subtreeIncomplete.Load()
	return res, nil
}

func (rt *walkRuntime) collectResults(
	walkCtx context.Context,
	cancel context.CancelCauseFunc,
	resultsCh <-chan WalkEntry,
) WalkResult {
	var res WalkResult
	var aborted bool
	collectSlices := rt.opts.OnEntry == nil

	for entry := range resultsCh {
		if !aborted && walkCtx.Err() != nil {
			aborted = true
		}
		if !aborted && rt.opts.CheckCancel != nil {
			if err := rt.opts.CheckCancel(walkCtx); err != nil {
				cancel(err)
				aborted = true
				continue
			}
		}

		switch entry.Kind {
		case EntryInspected:
			res.InspectedCount++
			if collectSlices && entry.Inspected != nil {
				res.Files = append(res.Files, *entry.Inspected)
			}
		case EntryUnchanged:
			res.UnchangedCount++
			if collectSlices && entry.Unchanged != nil {
				res.Unchanged = append(res.Unchanged, *entry.Unchanged)
			}
		case EntrySkipped:
			res.SkippedCount++
			if collectSlices && entry.Skipped != nil {
				res.Skipped = append(res.Skipped, *entry.Skipped)
			}
		}

		if rt.opts.OnEntry != nil && !aborted {
			if err := rt.opts.OnEntry(entry); err != nil {
				cancel(err)
				aborted = true
			}
		}
	}
	return res
}

func (rt *walkRuntime) walkDir(
	ctx context.Context,
	relDir []byte,
	absDir []byte,
	ancestry []os.FileInfo,
	isRoot bool,
	tasksCh chan<- inspectTask,
	resultsCh chan<- WalkEntry,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := rt.readDir(absDir)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if isRoot {
			return fmt.Errorf("scanner: read root directory: %w", err)
		}
		rt.markSubtreeIncomplete()
		relID, idErr := domain.NewRelativePathIdentity(relDir)
		if idErr != nil {
			return idErr
		}
		return emitSkipped(ctx, resultsCh, SkippedEntry{
			RelPath: relID,
			IsDir:   true,
			Reason:  SkipReasonUnreadable,
			Err:     err,
		})
	}

	// Ensure deterministic raw-byte order even when a custom ReadDir hook returns unsorted entries.
	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		return bytes.Compare([]byte(a.Name()), []byte(b.Name()))
	})

	for _, de := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := rt.visitEntry(ctx, relDir, absDir, ancestry, de, tasksCh, resultsCh); err != nil {
			return err
		}
	}
	return nil
}

func (rt *walkRuntime) visitEntry(
	ctx context.Context,
	relDir []byte,
	absDir []byte,
	ancestry []os.FileInfo,
	de os.DirEntry,
	tasksCh chan<- inspectTask,
	resultsCh chan<- WalkEntry,
) error {
	nameBytes := []byte(de.Name())
	relID, err := domain.NewRelativePathIdentity(joinRelativeBytes(relDir, nameBytes))
	if err != nil {
		return err
	}
	absBytes := joinAbsoluteBytes(absDir, nameBytes)

	entryType := de.Type()
	isDirHint := entryType.IsDir()

	if isTemporaryName(nameBytes) {
		return emitSkipped(ctx, resultsCh, SkippedEntry{
			RelPath: relID,
			IsDir:   isDirHint,
			Reason:  SkipReasonTemporary,
		})
	}
	if isHiddenName(nameBytes) {
		return emitSkipped(ctx, resultsCh, SkippedEntry{
			RelPath: relID,
			IsDir:   isDirHint,
			Reason:  SkipReasonHidden,
		})
	}

	needsFollow := entryType&os.ModeSymlink != 0 || entryType == 0
	if !needsFollow && !isDirHint {
		if !entryType.IsRegular() {
			return emitSkipped(ctx, resultsCh, SkippedEntry{
				RelPath: relID,
				Reason:  SkipReasonNonRegular,
			})
		}
		if !isCBZName(nameBytes) {
			return emitSkipped(ctx, resultsCh, SkippedEntry{
				RelPath: relID,
				Reason:  SkipReasonNonCBZ,
			})
		}
	}

	info, resolveErr := rt.resolveEntryInfo(absBytes, de, needsFollow)
	if resolveErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		reason := SkipReasonUnreadable
		if needsFollow {
			reason = classifyStatError(resolveErr)
		}
		if (isDirHint || needsFollow) && reason == SkipReasonUnreadable {
			rt.markSubtreeIncomplete()
		}
		return emitSkipped(ctx, resultsCh, SkippedEntry{
			RelPath: relID,
			IsDir:   isDirHint,
			Reason:  reason,
			Err:     resolveErr,
		})
	}

	return rt.dispatchResolved(ctx, relID, absBytes, ancestry, nameBytes, info, tasksCh, resultsCh)
}

func (rt *walkRuntime) resolveEntryInfo(absBytes []byte, de os.DirEntry, follow bool) (os.FileInfo, error) {
	if follow || rt.opts.Stat != nil {
		return rt.stat(absBytes)
	}
	return de.Info()
}

func (rt *walkRuntime) dispatchResolved(
	ctx context.Context,
	relID domain.PathIdentity,
	absBytes []byte,
	ancestry []os.FileInfo,
	nameBytes []byte,
	info os.FileInfo,
	tasksCh chan<- inspectTask,
	resultsCh chan<- WalkEntry,
) error {
	if info.IsDir() {
		if hasSameFile(ancestry, info) {
			return emitSkipped(ctx, resultsCh, SkippedEntry{
				RelPath: relID,
				IsDir:   true,
				Reason:  SkipReasonSymlinkCycle,
			})
		}
		nextAncestry := append(slices.Clone(ancestry), info)
		return rt.walkDir(ctx, relID.Bytes, absBytes, nextAncestry, false, tasksCh, resultsCh)
	}
	if !info.Mode().IsRegular() {
		return emitSkipped(ctx, resultsCh, SkippedEntry{
			RelPath: relID,
			Reason:  SkipReasonNonRegular,
		})
	}
	if !isCBZName(nameBytes) {
		return emitSkipped(ctx, resultsCh, SkippedEntry{
			RelPath: relID,
			Reason:  SkipReasonNonCBZ,
		})
	}
	return rt.handleCBZFile(ctx, relID, absBytes, info, tasksCh, resultsCh)
}

func (rt *walkRuntime) handleCBZFile(
	ctx context.Context,
	relID domain.PathIdentity,
	absBytes []byte,
	info os.FileInfo,
	tasksCh chan<- inspectTask,
	resultsCh chan<- WalkEntry,
) error {
	size := info.Size()
	mtime := normalizeMtime(info.ModTime())
	if known, ok := rt.opts.Known[string(relID.Bytes)]; ok && isUnchanged(size, mtime, known) {
		return emitEntry(ctx, resultsCh, WalkEntry{
			Kind: EntryUnchanged,
			Unchanged: &UnchangedFile{
				RelPath: relID,
				Size:    size,
				Mtime:   mtime,
			},
		})
	}

	task := inspectTask{
		relPath: relID,
		absPath: absBytes,
		size:    size,
		mtime:   mtime,
	}
	select {
	case tasksCh <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rt *walkRuntime) runWorker(
	walkCtx context.Context,
	cancel context.CancelCauseFunc,
	tasksCh <-chan inspectTask,
	resultsCh chan<- WalkEntry,
) {
	for {
		select {
		case <-walkCtx.Done():
			return
		case task, ok := <-tasksCh:
			if !ok {
				return
			}
			if walkCtx.Err() != nil {
				return
			}
			if rt.opts.CheckCancel != nil {
				if err := rt.opts.CheckCancel(walkCtx); err != nil {
					cancel(err)
					return
				}
			}
			entry, cancelled := rt.inspectOne(walkCtx, task)
			if cancelled {
				return
			}
			if err := emitEntry(walkCtx, resultsCh, entry); err != nil {
				return
			}
		}
	}
}

func (rt *walkRuntime) inspectOne(ctx context.Context, task inspectTask) (WalkEntry, bool) {
	f, err := rt.open(ctx, task.absPath)
	if err != nil {
		if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return WalkEntry{}, true
		}
		return skippedEntry(task.relPath, SkipReasonUnreadable, err), false
	}
	defer func() {
		_ = f.Close()
	}()

	size, mtime := task.size, task.mtime
	if statable, ok := f.(statReadAtCloser); ok {
		openedInfo, statErr := statable.Stat()
		if statErr != nil {
			return skippedEntry(task.relPath, SkipReasonUnreadable, statErr), false
		}
		if !openedInfo.Mode().IsRegular() {
			if openedInfo.IsDir() {
				rt.markSubtreeIncomplete()
			}
			return WalkEntry{
				Kind: EntrySkipped,
				Skipped: &SkippedEntry{
					RelPath: task.relPath,
					IsDir:   openedInfo.IsDir(),
					Reason:  SkipReasonNonRegular,
				},
			}, false
		}
		size = openedInfo.Size()
		mtime = normalizeMtime(openedInfo.ModTime())
	}

	digest, err := PartialFingerprint(f, size)
	if err != nil {
		if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return WalkEntry{}, true
		}
		return skippedEntry(task.relPath, SkipReasonUnreadable, err), false
	}

	cr, err := rt.formats.Open(f, size)
	if err != nil {
		if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return WalkEntry{}, true
		}
		return skippedEntry(task.relPath, classifyOpenArchiveError(err), err), false
	}
	defer func() {
		_ = cr.Close()
	}()

	pages := cr.Pages()
	diag := cr.Diagnostics()

	var metaPtr *comicinfo.Metadata
	var ciDiags []comicinfo.Diagnostic
	var ciErr error

	if ciEntry, hasCI := cr.ComicInfo(); hasCI {
		meta, diags, readErr, parseErr := readComicInfoEntry(cr, ciEntry.Index)
		if readErr != nil {
			return skippedEntry(task.relPath, classifyOpenArchiveError(readErr), readErr), false
		}
		if parseErr != nil {
			ciErr = parseErr
		} else {
			metaPtr = &meta
			ciDiags = diags
		}
	}

	coverIdx, suppressed := selectPagesAndCover(pages, metaPtr)

	return WalkEntry{
		Kind: EntryInspected,
		Inspected: &InspectedFile{
			RelPath:              task.relPath,
			Size:                 size,
			Mtime:                mtime,
			Format:               domain.MediaFormatCBZ,
			DigestKind:           domain.DigestKindXXH364Partial,
			Digest:               digest,
			PageCount:            len(pages),
			CoverPageIndex:       coverIdx,
			SuppressedPages:      suppressed,
			ImageCodecs:          slices.Clone(diag.ImageCodecs),
			ArchiveInvalidUTF8:   diag.InvalidUTF8Path,
			ComicInfo:            metaPtr,
			ComicInfoDiagnostics: ciDiags,
			ComicInfoErr:         ciErr,
		},
	}, false
}

func readComicInfoEntry(cr container.ContainerReader, index int) (comicinfo.Metadata, []comicinfo.Diagnostic, error, error) {
	rc, err := cr.Open(index)
	if err != nil {
		return comicinfo.Metadata{}, nil, err, nil
	}
	defer func() {
		_ = rc.Close()
	}()

	limited := io.LimitReader(rc, maxComicInfoBytes+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return comicinfo.Metadata{}, nil, err, nil
	}
	if len(raw) > maxComicInfoBytes {
		return comicinfo.Metadata{}, nil, nil, fmt.Errorf("comicinfo: XML exceeds %d bytes", maxComicInfoBytes)
	}
	meta, diags, parseErr := comicinfo.Parse(bytes.NewReader(raw), comicinfo.Lenient)
	return meta, diags, nil, parseErr
}

func selectPagesAndCover(pages []container.Entry, meta *comicinfo.Metadata) (int, []int) {
	if len(pages) == 0 {
		return 0, nil
	}

	suppressedSet, suppressed := collectSuppressedPages(len(pages), meta)
	if frontIdx := findFrontCoverIndex(len(pages), suppressedSet, meta); frontIdx >= 0 {
		return frontIdx, suppressed
	}
	if meta != nil && meta.CoverImage != "" {
		if idx := matchCoverImagePage(pages, suppressedSet, meta.CoverImage); idx >= 0 {
			return idx, suppressed
		}
	}
	return firstDisplayablePage(len(pages), suppressedSet), suppressed
}

func collectSuppressedPages(pageCount int, meta *comicinfo.Metadata) (map[int]bool, []int) {
	if meta == nil {
		return nil, nil
	}
	suppressedSet := make(map[int]bool)
	for _, p := range meta.Pages {
		idx := int(p.Image)
		if idx >= 0 && idx < pageCount && isSuppressedPage(p.Type) {
			suppressedSet[idx] = true
		}
	}
	if len(suppressedSet) == 0 {
		return nil, nil
	}
	suppressed := make([]int, 0, len(suppressedSet))
	for idx := range suppressedSet {
		suppressed = append(suppressed, idx)
	}
	slices.Sort(suppressed)
	return suppressedSet, suppressed
}

func findFrontCoverIndex(pageCount int, suppressedSet map[int]bool, meta *comicinfo.Metadata) int {
	if meta == nil {
		return -1
	}
	best := -1
	for _, p := range meta.Pages {
		idx := int(p.Image)
		if idx < 0 || idx >= pageCount || suppressedSet[idx] {
			continue
		}
		if slices.Contains(p.Type, "FrontCover") && (best < 0 || idx < best) {
			best = idx
		}
	}
	return best
}

func firstDisplayablePage(pageCount int, suppressedSet map[int]bool) int {
	for i := range pageCount {
		if !suppressedSet[i] {
			return i
		}
	}
	return 0
}

func isSuppressedPage(types []string) bool {
	return slices.Contains(types, "Deleted") || slices.Contains(types, "Advertisement")
}

func matchCoverImagePage(pages []container.Entry, suppressedSet map[int]bool, target string) int {
	normTarget := strings.TrimPrefix(strings.ReplaceAll(strings.TrimSpace(target), "\\", "/"), "./")
	if normTarget == "" {
		return -1
	}
	for i, p := range pages {
		if suppressedSet[i] {
			continue
		}
		normEntry := strings.TrimPrefix(strings.ReplaceAll(p.Name, "\\", "/"), "./")
		if strings.EqualFold(normEntry, normTarget) {
			return i
		}
		if slash := strings.LastIndexByte(normEntry, '/'); slash >= 0 {
			if strings.EqualFold(normEntry[slash+1:], normTarget) {
				return i
			}
		}
	}
	return -1
}

func isUnchanged(size int64, mtime time.Time, known KnownFile) bool {
	if known.Mtime == nil {
		return false
	}
	if size != known.Size {
		return false
	}
	return mtime.Equal(normalizeMtime(*known.Mtime))
}

func normalizeMtime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}

func hasSameFile(ancestry []os.FileInfo, target os.FileInfo) bool {
	for _, a := range ancestry {
		if os.SameFile(a, target) {
			return true
		}
	}
	return false
}

func joinRelativeBytes(relDir, name []byte) []byte {
	if len(relDir) == 0 {
		return slices.Clone(name)
	}
	out := make([]byte, 0, len(relDir)+1+len(name))
	out = append(out, relDir...)
	out = append(out, '/')
	out = append(out, name...)
	return out
}

func joinAbsoluteBytes(absDir, name []byte) []byte {
	if len(absDir) == 1 && absDir[0] == '/' {
		out := make([]byte, 0, 1+len(name))
		out = append(out, '/')
		out = append(out, name...)
		return out
	}
	out := make([]byte, 0, len(absDir)+1+len(name))
	out = append(out, absDir...)
	out = append(out, '/')
	out = append(out, name...)
	return out
}

func isHiddenName(name []byte) bool {
	if len(name) == 0 {
		return false
	}
	return name[0] == '.' || bytes.Equal(name, []byte("__MACOSX"))
}

var tempSuffixes = [][]byte{
	[]byte(".tmp"),
	[]byte(".part"),
	[]byte(".crdownload"),
	[]byte(".partial"),
	[]byte(".download"),
	[]byte(".swp"),
	[]byte(".swx"),
}

func isTemporaryName(name []byte) bool {
	if len(name) == 0 {
		return false
	}
	if bytes.HasPrefix(name, []byte(".tobiten-tmp-")) || bytes.HasPrefix(name, []byte("~$")) {
		return true
	}
	if name[len(name)-1] == '~' {
		return true
	}
	for _, suffix := range tempSuffixes {
		if len(name) >= len(suffix) && asciiEqualFoldBytes(name[len(name)-len(suffix):], suffix) {
			return true
		}
	}
	return false
}

func isCBZName(name []byte) bool {
	if len(name) <= 4 {
		return false
	}
	return asciiEqualFoldBytes(name[len(name)-4:], []byte(".cbz"))
}

func asciiEqualFoldBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func classifyStatError(err error) SkipReason {
	if errors.Is(err, syscall.ELOOP) {
		return SkipReasonSymlinkCycle
	}
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR) {
		return SkipReasonBrokenSymlink
	}
	return SkipReasonUnreadable
}

func classifyOpenArchiveError(err error) SkipReason {
	if errors.Is(err, container.ErrUnknownFormat) ||
		errors.Is(err, container.ErrCorrupt) ||
		errors.Is(err, container.ErrUnsupported) ||
		errors.Is(err, container.ErrEncrypted) {
		return SkipReasonCorruptArchive
	}
	return SkipReasonUnreadable
}

func skippedEntry(rel domain.PathIdentity, reason SkipReason, err error) WalkEntry {
	return WalkEntry{
		Kind: EntrySkipped,
		Skipped: &SkippedEntry{
			RelPath: rel,
			Reason:  reason,
			Err:     err,
		},
	}
}

func emitSkipped(ctx context.Context, ch chan<- WalkEntry, item SkippedEntry) error {
	return emitEntry(ctx, ch, WalkEntry{
		Kind:    EntrySkipped,
		Skipped: &item,
	})
}

func emitEntry(ctx context.Context, ch chan<- WalkEntry, entry WalkEntry) error {
	select {
	case ch <- entry:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
