package scanner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/logging"
	"github.com/inexcession/tobiten/internal/mount"
)

const (
	// DefaultPollInterval is the default polling interval for network mounts and
	// polling-watched library roots (ADR-0003 §4).
	DefaultPollInterval = time.Minute

	// DefaultDebounceWindow is the default quiet window used to coalesce bursts
	// of filesystem changes into a single automatic SCAN job per library.
	DefaultDebounceWindow = 2 * time.Second
)

var (
	// ErrWatcherAlreadyRunning is returned when Start is called on an active Watcher.
	ErrWatcherAlreadyRunning = errors.New("scanner: mount watcher is already running")
	// ErrInvalidPollInterval is returned when WatcherOptions.PollInterval is negative.
	ErrInvalidPollInterval = errors.New("scanner: poll interval must be non-negative")
	// ErrInvalidDebounceWindow is returned when WatcherOptions.DebounceWindow is negative.
	ErrInvalidDebounceWindow = errors.New("scanner: debounce window must be non-negative")
	// ErrWatcherNoTrigger is returned when WatcherOptions.Trigger is nil.
	ErrWatcherNoTrigger = errors.New("scanner: job trigger is required")
)

// WatchMode identifies how a library root is monitored for filesystem changes.
type WatchMode string

const (
	// WatchModePoll monitors a root by sequential read-only size and mtime polling.
	WatchModePoll WatchMode = "poll"
	// WatchModeOS monitors a local root via OS filesystem notifications.
	WatchModeOS WatchMode = "os"
)

// MountDetector inspects a directory path and reports filesystem mount properties.
type MountDetector interface {
	Detect(rawPath []byte) (mount.FilesystemInfo, error)
}

// MountDetectorFunc adapts a function to MountDetector.
type MountDetectorFunc func(rawPath []byte) (mount.FilesystemInfo, error)

// Detect calls f(rawPath).
func (f MountDetectorFunc) Detect(rawPath []byte) (mount.FilesystemInfo, error) {
	return f(rawPath)
}

// Timer represents a one-shot timer used by Watcher.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Clock abstracts time reading and timer creation for deterministic watcher tests.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct {
	t *time.Timer
}

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// OSWatcher is the optional OS-notification backend seam (implemented in M1-T11.2).
// Network-mounted roots never register with OSWatcher.
type OSWatcher interface {
	AddRoot(root domain.LibraryRoot) error
	RemoveRoot(rootID string) error
	Close() error
}

// RootLister loads the currently configured library roots for watcher synchronization.
type RootLister interface {
	ListAllLibraryRoots(ctx context.Context) ([]domain.LibraryRoot, error)
}

// RootWatchStatus reports the resolved watch mode and mount metadata for one root.
type RootWatchStatus struct {
	RootID    string
	LibraryID string
	Mode      WatchMode
	FSType    string
	IsNetwork bool
}

// WatcherOptions configures a mount-aware library Watcher.
type WatcherOptions struct {
	Roots          RootLister
	StaticRoots    []domain.LibraryRoot
	Trigger        JobTrigger
	MountDetector  MountDetector
	OSWatcher      OSWatcher
	Clock          Clock
	PollInterval   time.Duration
	DebounceWindow time.Duration
	Logger         *slog.Logger
	ReadDir        func(rawDir []byte) ([]os.DirEntry, error)
	Stat           func(rawPath []byte) (os.FileInfo, error)
	OnPoll         func()
	OnTrigger      func(libraryID, jobID string, err error)
}

type pollEntry struct {
	isDir      bool
	unreadable bool
	size       int64
	mtime      time.Time
}

type rootSnapshot struct {
	entries map[string]pollEntry
}

type watchedRoot struct {
	root               domain.LibraryRoot
	mode               WatchMode
	fsType             string
	isNetwork          bool
	mountDetected      bool
	osWatched          bool
	hasBaseline        bool
	unreadable         bool
	unreadableSubpaths map[string]struct{}
	snapshot           rootSnapshot
}

// Watcher monitors configured library roots using mount-type detection, sequential
// read-only size/mtime polling, and per-library debounced automatic SCAN triggers.
type Watcher struct {
	roots          RootLister
	staticRoots    []domain.LibraryRoot
	trigger        JobTrigger
	mountDetector  MountDetector
	osWatcher      OSWatcher
	clock          Clock
	pollInterval   time.Duration
	debounceWindow time.Duration
	logger         *slog.Logger
	readDir        func(rawDir []byte) ([]os.DirEntry, error)
	stat           func(rawPath []byte) (os.FileInfo, error)
	customStat     bool
	onPoll         func()
	onTrigger      func(libraryID, jobID string, err error)

	syncCh chan struct{}

	mu         sync.Mutex
	running    bool
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	watched    map[string]*watchedRoot
	syncErrLog bool
}

// NewWatcher constructs a Watcher from opts.
func NewWatcher(opts WatcherOptions) (*Watcher, error) {
	if opts.Trigger == nil {
		return nil, ErrWatcherNoTrigger
	}
	if opts.PollInterval < 0 {
		return nil, ErrInvalidPollInterval
	}
	if opts.DebounceWindow < 0 {
		return nil, ErrInvalidDebounceWindow
	}

	pollInterval := opts.PollInterval
	if pollInterval == 0 {
		pollInterval = DefaultPollInterval
	}
	debounceWindow := opts.DebounceWindow
	if debounceWindow == 0 {
		debounceWindow = DefaultDebounceWindow
	}

	detector := opts.MountDetector
	if detector == nil {
		detector = MountDetectorFunc(mount.DetectFilesystemBytes)
	}
	clk := opts.Clock
	if clk == nil {
		clk = realClock{}
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	readDirFn := opts.ReadDir
	if readDirFn == nil {
		readDirFn = func(rawDir []byte) ([]os.DirEntry, error) {
			return os.ReadDir(string(rawDir))
		}
	}
	customStat := opts.Stat != nil
	statFn := opts.Stat
	if statFn == nil {
		statFn = func(rawPath []byte) (os.FileInfo, error) {
			return os.Stat(string(rawPath))
		}
	}

	return &Watcher{
		roots:          opts.Roots,
		staticRoots:    slices.Clone(opts.StaticRoots),
		trigger:        opts.Trigger,
		mountDetector:  detector,
		osWatcher:      opts.OSWatcher,
		clock:          clk,
		pollInterval:   pollInterval,
		debounceWindow: debounceWindow,
		logger:         logger,
		readDir:        readDirFn,
		stat:           statFn,
		customStat:     customStat,
		onPoll:         opts.OnPoll,
		onTrigger:      opts.OnTrigger,
		syncCh:         make(chan struct{}, 1),
		watched:        make(map[string]*watchedRoot),
	}, nil
}

// Start synchronizes configured roots, captures initial read-only baselines without
// enqueueing scans, and starts the background polling and debounce loop.
func (w *Watcher) Start(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return ErrWatcherAlreadyRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.running = true
	w.cancel = cancel
	w.mu.Unlock()

	if err := w.syncRoots(runCtx, nil); err != nil {
		cancel()
		w.mu.Lock()
		w.running = false
		w.cancel = nil
		w.mu.Unlock()
		return err
	}

	w.captureInitialBaselines(runCtx)

	w.wg.Add(1)
	go w.runLoop(runCtx)
	return nil
}

// Stop cancels the background watcher loop, waits for all goroutines to exit, and
// closes any attached OS watcher. It is safe to call repeatedly or on a nil Watcher.
func (w *Watcher) Stop() {
	if w == nil {
		return
	}

	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	cancel := w.cancel
	w.cancel = nil
	w.running = false
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	w.wg.Wait()

	if w.osWatcher != nil {
		_ = w.osWatcher.Close()
	}
}

// NotifySync wakes the watcher loop to reload configured library roots immediately.
func (w *Watcher) NotifySync() {
	if w == nil {
		return
	}
	select {
	case w.syncCh <- struct{}{}:
	default:
	}
}

// RootStatus returns the watch status for rootID, if currently tracked.
func (w *Watcher) RootStatus(rootID string) (RootWatchStatus, bool) {
	if w == nil {
		return RootWatchStatus{}, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	wr, ok := w.watched[rootID]
	if !ok {
		return RootWatchStatus{}, false
	}
	return toRootWatchStatus(wr), true
}

// RootStatuses returns a deterministic snapshot of all currently tracked roots.
func (w *Watcher) RootStatuses() []RootWatchStatus {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]RootWatchStatus, 0, len(w.watched))
	for _, wr := range w.watched {
		out = append(out, toRootWatchStatus(wr))
	}
	slices.SortFunc(out, func(a, b RootWatchStatus) int {
		return strings.Compare(a.RootID, b.RootID)
	})
	return out
}

func toRootWatchStatus(wr *watchedRoot) RootWatchStatus {
	return RootWatchStatus{
		RootID:    wr.root.ID,
		LibraryID: wr.root.LibraryID,
		Mode:      wr.mode,
		FSType:    wr.fsType,
		IsNetwork: wr.isNetwork,
	}
}

func (w *Watcher) runLoop(ctx context.Context) {
	defer w.wg.Done()

	nextPollAt := w.clock.Now().Add(w.pollInterval)
	pending := make(map[string]time.Time)

	for {
		if ctx.Err() != nil {
			return
		}

		now := w.clock.Now()
		w.fireDueScans(ctx, now, pending)

		if !now.Before(nextPollAt) {
			w.pollStep(ctx, pending)
			nextPollAt = w.clock.Now().Add(w.pollInterval)
			w.fireDueScans(ctx, w.clock.Now(), pending)
		}

		wait := nextWakeDelay(w.clock.Now(), nextPollAt, pending)
		timer := w.clock.NewTimer(wait)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-w.syncCh:
			timer.Stop()
			_ = w.syncRoots(ctx, pending)
		case <-timer.C():
		}
	}
}

func nextWakeDelay(now, nextPollAt time.Time, pending map[string]time.Time) time.Duration {
	nextWake := nextPollAt
	for _, fireAt := range pending {
		if fireAt.Before(nextWake) {
			nextWake = fireAt
		}
	}
	wait := nextWake.Sub(now)
	if wait < 0 {
		return 0
	}
	return wait
}

func (w *Watcher) fireDueScans(ctx context.Context, now time.Time, pending map[string]time.Time) {
	if len(pending) == 0 {
		return
	}
	var due []string
	for libID, fireAt := range pending {
		if !now.Before(fireAt) {
			due = append(due, libID)
		}
	}
	slices.Sort(due)
	for _, libID := range due {
		delete(pending, libID)
		w.fireScan(ctx, libID)
	}
}

func (w *Watcher) fireScan(ctx context.Context, libraryID string) {
	if ctx.Err() != nil {
		return
	}
	jobID, err := TriggerScan(ctx, w.trigger, libraryID, TriggerScanOptions{
		CreatedBy: nil,
	})
	if err != nil && ctx.Err() == nil {
		if errors.Is(err, app.ErrConflict) {
			w.logger.Debug("automatic library scan skipped due to active root operation",
				slog.String("library_id", libraryID),
				logging.Err(err),
			)
		} else {
			w.logger.Error("failed to enqueue automatic library scan",
				slog.String("library_id", libraryID),
				logging.Err(err),
			)
		}
	}
	if w.onTrigger != nil {
		w.onTrigger(libraryID, jobID, err)
	}
}

func (w *Watcher) pollStep(ctx context.Context, pending map[string]time.Time) {
	if err := w.syncRoots(ctx, pending); err != nil && ctx.Err() == nil {
		if !w.syncErrLog {
			w.syncErrLog = true
			w.logger.Error("failed to refresh library roots during watch poll", logging.Err(err))
		}
	} else {
		w.syncErrLog = false
	}

	roots := w.snapshotWatchedRoots()
	for _, wr := range roots {
		if ctx.Err() != nil {
			return
		}
		if wr.mode != WatchModePoll {
			continue
		}
		changed := w.pollWatchedRoot(ctx, wr)
		if changed {
			pending[wr.root.LibraryID] = w.clock.Now().Add(w.debounceWindow)
		}
	}
	if w.onPoll != nil {
		w.onPoll()
	}
}

func (w *Watcher) captureInitialBaselines(ctx context.Context) {
	for _, wr := range w.snapshotWatchedRoots() {
		if ctx.Err() != nil {
			return
		}
		if wr.mode != WatchModePoll {
			continue
		}
		_ = w.pollWatchedRoot(ctx, wr)
	}
}

func (w *Watcher) snapshotWatchedRoots() []*watchedRoot {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*watchedRoot, 0, len(w.watched))
	for _, wr := range w.watched {
		out = append(out, wr)
	}
	slices.SortFunc(out, func(a, b *watchedRoot) int {
		return strings.Compare(a.root.ID, b.root.ID)
	})
	return out
}

func (w *Watcher) syncRoots(ctx context.Context, pending map[string]time.Time) error {
	roots, err := w.loadConfiguredRoots(ctx)
	if err != nil {
		return err
	}

	desired := make(map[string]domain.LibraryRoot, len(roots))
	activeLibs := make(map[string]struct{}, len(roots))
	for _, r := range roots {
		desired[r.ID] = r
		activeLibs[r.LibraryID] = struct{}{}
	}

	w.mu.Lock()
	var toRemove []string
	for id, existing := range w.watched {
		fresh, ok := desired[id]
		if !ok || !bytes.Equal(existing.root.Path.Bytes, fresh.Path.Bytes) {
			toRemove = append(toRemove, id)
		}
	}
	for _, id := range toRemove {
		if wr := w.watched[id]; wr != nil && wr.osWatched && w.osWatcher != nil {
			_ = w.osWatcher.RemoveRoot(id)
		}
		delete(w.watched, id)
	}

	var added []*watchedRoot
	for _, r := range roots {
		if _, exists := w.watched[r.ID]; exists {
			continue
		}
		wr := w.initWatchedRoot(r)
		w.watched[r.ID] = wr
		added = append(added, wr)
	}
	w.mu.Unlock()

	for _, wr := range added {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if wr.mode == WatchModePoll && !wr.hasBaseline {
			_ = w.pollWatchedRoot(ctx, wr)
		}
	}

	for libID := range pending {
		if _, ok := activeLibs[libID]; !ok {
			delete(pending, libID)
		}
	}
	return nil
}

func (w *Watcher) loadConfiguredRoots(ctx context.Context) ([]domain.LibraryRoot, error) {
	if w.roots != nil {
		return w.roots.ListAllLibraryRoots(ctx)
	}
	return slices.Clone(w.staticRoots), nil
}

func (w *Watcher) initWatchedRoot(r domain.LibraryRoot) *watchedRoot {
	wr := &watchedRoot{
		root:               r,
		mode:               WatchModePoll,
		unreadableSubpaths: make(map[string]struct{}),
	}
	w.detectRootWatchMode(wr)
	return wr
}

func (w *Watcher) detectRootWatchMode(wr *watchedRoot) {
	info, err := w.mountDetector.Detect(wr.root.Path.Bytes)
	if err != nil {
		wr.mode = WatchModePoll
		wr.mountDetected = false
		return
	}
	wr.fsType = info.Type
	wr.isNetwork = info.IsNetwork
	wr.mountDetected = true

	if info.IsNetwork || w.osWatcher == nil {
		wr.mode = WatchModePoll
		return
	}
	if err := w.osWatcher.AddRoot(wr.root); err != nil {
		w.logger.Warn("failed to register OS watch; falling back to polling",
			slog.String("library_id", wr.root.LibraryID),
			slog.String("library_root_id", wr.root.ID),
			slog.String("path", wr.root.Path.Display),
			logging.Err(err),
		)
		wr.mode = WatchModePoll
		return
	}
	wr.mode = WatchModeOS
	wr.osWatched = true
}

func (w *Watcher) pollWatchedRoot(ctx context.Context, wr *watchedRoot) bool {
	nextUnreadable := make(map[string]struct{})
	snap, err := w.captureRootSnapshot(ctx, wr, nextUnreadable)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		if !wr.unreadable {
			wr.unreadable = true
			w.logger.Error("library root unreadable during watch poll",
				slog.String("library_id", wr.root.LibraryID),
				slog.String("library_root_id", wr.root.ID),
				slog.String("path", wr.root.Path.Display),
				logging.Err(err),
			)
		}
		return false
	}

	wr.unreadableSubpaths = nextUnreadable

	if wr.unreadable {
		wr.unreadable = false
		if !wr.mountDetected {
			w.detectRootWatchMode(wr)
		}
		wr.snapshot = snap
		wr.hasBaseline = true
		return true
	}

	if !wr.hasBaseline {
		wr.snapshot = snap
		wr.hasBaseline = true
		return false
	}

	changed := !snapshotsEqual(wr.snapshot, snap)
	wr.snapshot = snap
	return changed
}

func (w *Watcher) captureRootSnapshot(
	ctx context.Context,
	wr *watchedRoot,
	nextUnreadable map[string]struct{},
) (rootSnapshot, error) {
	rootBytes := trimTrailingSlashes(wr.root.Path.Bytes)
	rootInfo, err := w.stat(rootBytes)
	if err != nil {
		return rootSnapshot{}, err
	}
	if !rootInfo.IsDir() {
		return rootSnapshot{}, ErrRootNotDirectory
	}

	snap := rootSnapshot{entries: make(map[string]pollEntry)}
	if err := w.walkSnapshotDir(ctx, wr, nil, rootBytes, []os.FileInfo{rootInfo}, true, snap.entries, nextUnreadable); err != nil {
		return rootSnapshot{}, err
	}
	return snap, nil
}

func (w *Watcher) walkSnapshotDir(
	ctx context.Context,
	wr *watchedRoot,
	relDir []byte,
	absDir []byte,
	ancestry []os.FileInfo,
	isRoot bool,
	out map[string]pollEntry,
	nextUnreadable map[string]struct{},
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := w.readDir(absDir)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isRoot {
			return err
		}
		relKey := string(relDir)
		out[relKey] = pollEntry{isDir: true, unreadable: true}
		nextUnreadable[relKey] = struct{}{}
		if _, seen := wr.unreadableSubpaths[relKey]; !seen {
			w.logger.Error("library subdirectory unreadable during watch poll",
				slog.String("library_id", wr.root.LibraryID),
				slog.String("library_root_id", wr.root.ID),
				slog.String("rel_path", relKey),
				logging.Err(err),
			)
		}
		return nil
	}

	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		return bytes.Compare([]byte(a.Name()), []byte(b.Name()))
	})

	for _, de := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.visitSnapshotEntry(ctx, wr, relDir, absDir, ancestry, de, out, nextUnreadable); err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) visitSnapshotEntry(
	ctx context.Context,
	wr *watchedRoot,
	relDir []byte,
	absDir []byte,
	ancestry []os.FileInfo,
	de os.DirEntry,
	out map[string]pollEntry,
	nextUnreadable map[string]struct{},
) error {
	nameBytes := []byte(de.Name())
	if isTemporaryName(nameBytes) || isHiddenName(nameBytes) {
		return nil
	}

	relBytes := joinRelativeBytes(relDir, nameBytes)
	absBytes := joinAbsoluteBytes(absDir, nameBytes)
	entryType := de.Type()
	needsFollow := entryType&os.ModeSymlink != 0 || entryType == 0

	var (
		info os.FileInfo
		err  error
	)
	if needsFollow || w.customStat {
		info, err = w.stat(absBytes)
	} else {
		info, err = de.Info()
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		relKey := string(relBytes)
		out[relKey] = pollEntry{isDir: entryType.IsDir(), unreadable: true}
		nextUnreadable[relKey] = struct{}{}
		if _, seen := wr.unreadableSubpaths[relKey]; !seen {
			w.logger.Error("library entry unreadable during watch poll",
				slog.String("library_id", wr.root.LibraryID),
				slog.String("library_root_id", wr.root.ID),
				slog.String("rel_path", relKey),
				logging.Err(err),
			)
		}
		return nil
	}

	relKey := string(relBytes)
	if info.IsDir() {
		if hasSameFile(ancestry, info) {
			return nil
		}
		out[relKey] = pollEntry{isDir: true}
		nextAncestry := append(slices.Clone(ancestry), info)
		return w.walkSnapshotDir(ctx, wr, relBytes, absBytes, nextAncestry, false, out, nextUnreadable)
	}

	if info.Mode().IsRegular() {
		out[relKey] = pollEntry{
			size:  info.Size(),
			mtime: info.ModTime().UTC(),
		}
	}
	return nil
}

func snapshotsEqual(a, b rootSnapshot) bool {
	if len(a.entries) != len(b.entries) {
		return false
	}
	for k, va := range a.entries {
		vb, ok := b.entries[k]
		if !ok {
			return false
		}
		if va.isDir != vb.isDir || va.unreadable != vb.unreadable || va.size != vb.size || !va.mtime.Equal(vb.mtime) {
			return false
		}
	}
	return true
}
