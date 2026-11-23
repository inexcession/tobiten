package scanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
)

// RootMarkerFilename is the optional marker file allowing a legitimately empty
// library root directory to pass the availability pre-check (ADR-0019 §5).
const RootMarkerFilename = ".tobiten-root"

// ErrRootEmpty is returned when a library root directory exists and is readable
// but contains no files and no .tobiten-root marker (ADR-0019 §5).
var ErrRootEmpty = errors.New("scanner: root directory contains no files and no .tobiten-root marker")

// ErrStalePreCheck is returned when a library's root set, root path, or newer
// availability check supersedes an in-flight pre-check observation before commit.
var ErrStalePreCheck = errors.New("scanner: library root snapshot changed during availability pre-check")

// DirStream incrementally reads directory entries so root availability pre-checks
// can stop at the first qualifying file without reading large directories in full.
type DirStream interface {
	ReadDir(n int) ([]os.DirEntry, error)
	Close() error
}

// PreCheckOptions configures a library root availability pre-check.
type PreCheckOptions struct {
	// Stat optionally overrides os.Stat when inspecting roots and entries.
	Stat func(rawPath []byte) (os.FileInfo, error)

	// OpenDir optionally overrides opening a directory for incremental reads.
	OpenDir func(rawDir []byte) (DirStream, error)

	// ReadDir optionally overrides directory reads when OpenDir is nil.
	ReadDir func(rawDir []byte) ([]os.DirEntry, error)
}

// RootCheckResult reports the availability pre-check outcome for one library root.
type RootCheckResult struct {
	LibraryRootID string
	RootPath      domain.PathIdentity
	PreviousState domain.LibraryRootState
	State         domain.LibraryRootState
	CheckedAt     time.Time
	LastError     *string
	Err           error
}

// PreCheckResult summarizes a library-wide root availability pre-check
// (ADR-0019 §5, ADR-0018 S3, M1-T22.4, M1B-D10).
type PreCheckResult struct {
	LibraryID     string
	PreviousState domain.LibraryRootState
	State         domain.LibraryRootState
	ApplyRemovals bool
	ShouldScan    bool
	CheckedAt     time.Time
	Roots         []RootCheckResult
	FailedRootIDs []string
	Event         *domain.DomainEvent
}

type rootProbeHooks struct {
	stat       func(rawPath []byte) (os.FileInfo, error)
	openDir    func(rawDir []byte) (DirStream, error)
	customStat bool
}

type checkedRootProbe struct {
	root domain.LibraryRoot
	err  error
}

type sliceDirStream struct {
	entries []os.DirEntry
	pos     int
}

func (s *sliceDirStream) ReadDir(n int) ([]os.DirEntry, error) {
	if s.pos >= len(s.entries) {
		return nil, io.EOF
	}
	if n <= 0 || n > len(s.entries)-s.pos {
		n = len(s.entries) - s.pos
	}
	chunk := s.entries[s.pos : s.pos+n]
	s.pos += n
	if s.pos >= len(s.entries) {
		return chunk, io.EOF
	}
	return chunk, nil
}

func (s *sliceDirStream) Close() error {
	return nil
}

// PreCheckLibrary evaluates every configured root of libraryID without writing
// under any root, updates each root's state/last_checked_at/last_error, appends
// library.offline or library.online on state transitions in the same
// transaction, and reports whether the scan may proceed (M1B-D10).
func (s *Scanner) PreCheckLibrary(
	ctx context.Context,
	libraryID string,
	opts PreCheckOptions,
) (PreCheckResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.tx == nil || s.outbox == nil || s.now == nil {
		return PreCheckResult{}, errors.New("scanner: scanner is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return PreCheckResult{}, err
	}
	if strings.TrimSpace(libraryID) == "" {
		return PreCheckResult{}, domain.ErrEmptyLibraryID
	}

	lib, roots, err := s.loadLibraryAndRoots(ctx, libraryID)
	if err != nil {
		return PreCheckResult{}, err
	}

	checkedAt := s.now().UTC().Truncate(time.Microsecond)
	probe := newRootProbeHooks(opts)
	checkedRoots, err := probeAllRoots(ctx, roots, probe)
	if err != nil {
		return PreCheckResult{}, err
	}

	var result PreCheckResult
	err = s.tx.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		var txErr error
		result, txErr = s.commitPreCheckTx(ctx, tx, lib.ID, checkedRoots, checkedAt)
		return txErr
	})
	if err != nil {
		return PreCheckResult{}, err
	}
	return result, nil
}

func (s *Scanner) commitPreCheckTx(
	ctx context.Context,
	tx storage.Tx,
	libraryID string,
	checkedRoots []checkedRootProbe,
	checkedAt time.Time,
) (PreCheckResult, error) {
	repo := tx.Libraries()
	if repo == nil {
		return PreCheckResult{}, errors.New("scanner: library repository is required")
	}
	if err := repo.LockLibrary(ctx, libraryID); err != nil {
		return PreCheckResult{}, wrapLibraryNotFound(err)
	}
	currentRoots, err := repo.ListLibraryRoots(ctx, libraryID)
	if err != nil {
		return PreCheckResult{}, err
	}

	matched, err := matchCheckedSnapshot(checkedRoots, currentRoots, checkedAt)
	if err != nil {
		return PreCheckResult{}, err
	}

	effectiveCheckedAt := checkedAt
	for _, item := range matched {
		if item.root.LastCheckedAt == nil {
			continue
		}
		if prev := item.root.LastCheckedAt.UTC(); !effectiveCheckedAt.After(prev) {
			effectiveCheckedAt = prev.Add(time.Microsecond)
		}
	}

	wasOnline := allRootsOnline(currentRoots)
	rootResults := make([]RootCheckResult, 0, len(matched))
	failedRootIDs := make([]string, 0)

	for _, item := range matched {
		root := item.root
		nextState := domain.LibraryRootStateOnline
		var lastErr *string
		if item.err != nil {
			nextState = domain.LibraryRootStateOffline
			msg := formatSafeDiagnosticError(item.err)
			lastErr = &msg
			failedRootIDs = append(failedRootIDs, root.ID)
		}

		if err := repo.UpdateLibraryRootStatus(ctx, root, nextState, effectiveCheckedAt, lastErr); errors.Is(err, storage.ErrNotFound) {
			return PreCheckResult{}, ErrStalePreCheck
		} else if err != nil {
			return PreCheckResult{}, err
		}

		rootResults = append(rootResults, RootCheckResult{
			LibraryRootID: root.ID,
			RootPath:      root.Path,
			PreviousState: root.State,
			State:         nextState,
			CheckedAt:     effectiveCheckedAt,
			LastError:     lastErr,
			Err:           item.err,
		})
	}

	slices.Sort(failedRootIDs)
	prevLibState := libraryStateFromBool(wasOnline)
	nextLibState := libraryStateFromBool(len(failedRootIDs) == 0)

	transitionEv, err := s.appendAvailabilityTransitionEvent(
		ctx,
		tx,
		libraryID,
		prevLibState,
		nextLibState,
		failedRootIDs,
	)
	if err != nil {
		return PreCheckResult{}, err
	}

	isOnline := nextLibState == domain.LibraryRootStateOnline
	return PreCheckResult{
		LibraryID:     libraryID,
		PreviousState: prevLibState,
		State:         nextLibState,
		ApplyRemovals: isOnline,
		ShouldScan:    isOnline,
		CheckedAt:     effectiveCheckedAt,
		Roots:         rootResults,
		FailedRootIDs: failedRootIDs,
		Event:         transitionEv,
	}, nil
}

func matchCheckedSnapshot(
	checkedRoots []checkedRootProbe,
	currentRoots []domain.LibraryRoot,
	observedAt time.Time,
) ([]checkedRootProbe, error) {
	if len(checkedRoots) != len(currentRoots) {
		return nil, ErrStalePreCheck
	}
	checkedByID := make(map[string]checkedRootProbe, len(checkedRoots))
	for _, item := range checkedRoots {
		checkedByID[item.root.ID] = item
	}

	matched := make([]checkedRootProbe, 0, len(currentRoots))
	for _, cur := range currentRoots {
		item, ok := checkedByID[cur.ID]
		if !ok || !bytes.Equal(item.root.Path.Bytes, cur.Path.Bytes) {
			return nil, ErrStalePreCheck
		}
		if rootStatusChanged(item.root, cur) {
			if cur.LastCheckedAt == nil || !observedAt.After(cur.LastCheckedAt.UTC()) {
				return nil, ErrStalePreCheck
			}
		}
		matched = append(matched, checkedRootProbe{
			root: cur,
			err:  item.err,
		})
	}
	return matched, nil
}

func rootStatusChanged(initial, current domain.LibraryRoot) bool {
	if initial.State != current.State {
		return true
	}
	if !equalOptionalTime(initial.LastCheckedAt, current.LastCheckedAt) {
		return true
	}
	return !equalOptionalString(initial.LastError, current.LastError)
}

func equalOptionalTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UTC().Equal(b.UTC())
}

func equalOptionalString(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func formatSafeDiagnosticError(err error) string {
	if err == nil {
		return ""
	}
	msg, _ := domain.FormatPathDisplay([]byte(err.Error()))
	if strings.IndexByte(msg, 0) >= 0 {
		msg = strings.ReplaceAll(msg, "\x00", `\x00`)
	}
	return msg
}

func (s *Scanner) appendAvailabilityTransitionEvent(
	ctx context.Context,
	tx storage.Tx,
	libraryID string,
	prevState domain.LibraryRootState,
	nextState domain.LibraryRootState,
	failedRootIDs []string,
) (*domain.DomainEvent, error) {
	if prevState == nextState {
		return nil, nil
	}
	appender := tx.Events()
	if appender == nil {
		return nil, errors.New("scanner: event repository is required")
	}

	if prevState == domain.LibraryRootStateOnline && nextState == domain.LibraryRootStateOffline {
		return s.outbox.AppendPayload(
			ctx,
			appender,
			domain.EventLibraryOffline,
			domain.EventVersion1,
			domain.AggregateTypeLibrary,
			libraryID,
			domain.LibraryEventPayloadV1{
				LibraryID:     libraryID,
				FailedRootIDs: slices.Clone(failedRootIDs),
			},
		)
	}

	return s.outbox.AppendPayload(
		ctx,
		appender,
		domain.EventLibraryOnline,
		domain.EventVersion1,
		domain.AggregateTypeLibrary,
		libraryID,
		domain.LibraryEventPayloadV1{
			LibraryID: libraryID,
		},
	)
}

func newRootProbeHooks(opts PreCheckOptions) rootProbeHooks {
	statFn := opts.Stat
	customStat := statFn != nil
	if statFn == nil {
		statFn = func(rawPath []byte) (os.FileInfo, error) {
			return os.Stat(string(rawPath))
		}
	}

	openDirFn := opts.OpenDir
	if openDirFn == nil && opts.ReadDir != nil {
		readDirFn := opts.ReadDir
		openDirFn = func(rawDir []byte) (DirStream, error) {
			entries, err := readDirFn(rawDir)
			if err != nil {
				return nil, err
			}
			return &sliceDirStream{entries: entries}, nil
		}
	}
	if openDirFn == nil {
		openDirFn = openRawDirStream
	}

	return rootProbeHooks{
		stat:       statFn,
		openDir:    openDirFn,
		customStat: customStat,
	}
}

func openRawDirStream(rawDir []byte) (DirStream, error) {
	f, err := openRawReadOnlyNonBlock(rawDir)
	if err != nil {
		return nil, err
	}
	// Clear O_NONBLOCK on the directory handle only insofar as normal ReadDir needs;
	// verify it is a directory before returning so special files are rejected safely.
	info, statErr := f.Stat()
	if statErr != nil {
		_ = f.Close()
		return nil, statErr
	}
	if !info.IsDir() {
		_ = f.Close()
		return nil, ErrRootNotDirectory
	}
	return f, nil
}

func probeAllRoots(ctx context.Context, roots []domain.LibraryRoot, probe rootProbeHooks) ([]checkedRootProbe, error) {
	outcomes := make([]checkedRootProbe, 0, len(roots))
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		err := probeRootAvailability(ctx, root.Path.Bytes, probe)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		outcomes = append(outcomes, checkedRootProbe{
			root: root,
			err:  err,
		})
	}
	return outcomes, nil
}

func probeRootAvailability(ctx context.Context, rootBytes []byte, probe rootProbeHooks) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := domain.ValidateRootPathBytes(rootBytes); err != nil {
		return err
	}
	root := trimTrailingSlashes(rootBytes)
	rootInfo, err := probe.stat(root)
	if err != nil {
		return fmt.Errorf("stat root: %w", err)
	}
	if !rootInfo.IsDir() {
		return ErrRootNotDirectory
	}

	found, err := checkDirForFile(ctx, root, []os.FileInfo{rootInfo}, true, probe)
	if err != nil {
		return err
	}
	if !found {
		return ErrRootEmpty
	}
	return nil
}

func checkDirForFile(
	ctx context.Context,
	absDir []byte,
	ancestry []os.FileInfo,
	isRoot bool,
	probe rootProbeHooks,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ds, err := probe.openDir(absDir)
	if err != nil {
		return handleOpenDirFailure(ctx, isRoot, err)
	}
	defer func() {
		_ = ds.Close()
	}()

	if err := verifyOpenStreamIsDir(ds, isRoot); err != nil {
		if isRoot {
			return false, err
		}
		return false, nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		entries, readErr := ds.ReadDir(1)
		for _, de := range entries {
			found, err := checkDirEntryForFile(ctx, absDir, ancestry, isRoot, de, probe)
			if err != nil {
				return false, err
			}
			if found {
				return true, nil
			}
		}
		if readErr != nil {
			return handleReadDirStreamError(ctx, isRoot, readErr)
		}
		if len(entries) == 0 {
			return false, nil
		}
	}
}

func handleOpenDirFailure(ctx context.Context, isRoot bool, err error) (bool, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	if isRoot {
		return false, fmt.Errorf("read root directory: %w", err)
	}
	return false, nil
}

func handleReadDirStreamError(ctx context.Context, isRoot bool, readErr error) (bool, error) {
	if errors.Is(readErr, io.EOF) {
		return false, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, ctxErr
	}
	if isRoot {
		return false, fmt.Errorf("read root directory: %w", readErr)
	}
	return false, nil
}

func verifyOpenStreamIsDir(ds DirStream, isRoot bool) error {
	statable, ok := ds.(statReadAtCloser)
	if !ok {
		return nil
	}
	info, err := statable.Stat()
	if err != nil {
		if isRoot {
			return fmt.Errorf("stat open root directory: %w", err)
		}
		return err
	}
	if !info.IsDir() {
		return ErrRootNotDirectory
	}
	return nil
}

func checkDirEntryForFile(
	ctx context.Context,
	absDir []byte,
	ancestry []os.FileInfo,
	isRoot bool,
	de os.DirEntry,
	probe rootProbeHooks,
) (bool, error) {
	nameBytes := []byte(de.Name())
	if len(nameBytes) == 0 || bytes.Equal(nameBytes, []byte(".")) || bytes.Equal(nameBytes, []byte("..")) {
		return false, nil
	}
	absChild := joinAbsoluteBytes(absDir, nameBytes)
	entryType := de.Type()

	if isRoot && bytes.Equal(nameBytes, []byte(RootMarkerFilename)) {
		return isMarkerFile(absChild, entryType, probe), nil
	}
	if isHiddenName(nameBytes) || isTemporaryName(nameBytes) {
		return false, nil
	}

	needsFollow := entryType&os.ModeSymlink != 0 || entryType == 0
	if !needsFollow && !entryType.IsDir() {
		if !entryType.IsRegular() {
			return false, nil
		}
		if !probe.customStat {
			return true, nil
		}
	}

	info, ok := probe.resolveEntryInfo(absChild, de, needsFollow)
	if !ok {
		return false, ctx.Err()
	}
	if info.Mode().IsRegular() {
		return true, nil
	}
	if info.IsDir() {
		if hasSameFile(ancestry, info) {
			return false, nil
		}
		nextAncestry := append(slices.Clone(ancestry), info)
		return checkDirForFile(ctx, absChild, nextAncestry, false, probe)
	}
	return false, nil
}

func isMarkerFile(absPath []byte, entryType os.FileMode, probe rootProbeHooks) bool {
	return isRootMarkerFile(absPath, entryType, probe.customStat, probe.stat)
}

func isRootMarkerFile(
	absPath []byte,
	entryType os.FileMode,
	customStat bool,
	statFn func([]byte) (os.FileInfo, error),
) bool {
	needsFollow := entryType&os.ModeSymlink != 0 || entryType == 0
	if !needsFollow {
		if !entryType.IsRegular() {
			return false
		}
		if !customStat {
			return true
		}
	}
	info, err := statFn(absPath)
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}

func (p rootProbeHooks) resolveEntryInfo(absPath []byte, de os.DirEntry, follow bool) (os.FileInfo, bool) {
	var (
		info os.FileInfo
		err  error
	)
	if follow || p.customStat {
		info, err = p.stat(absPath)
	} else {
		info, err = de.Info()
	}
	if err != nil {
		return nil, false
	}
	return info, true
}

func allRootsOnline(roots []domain.LibraryRoot) bool {
	for _, r := range roots {
		if r.State != domain.LibraryRootStateOnline {
			return false
		}
	}
	return true
}

func libraryStateFromBool(online bool) domain.LibraryRootState {
	if online {
		return domain.LibraryRootStateOnline
	}
	return domain.LibraryRootStateOffline
}
