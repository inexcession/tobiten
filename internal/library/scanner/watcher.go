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
	// PollIntervalSettingKey is the PostgreSQL system_settings key configuring
	// the polling interval for mount watching (ADR-0003 §4). Supported values are
	// positive Go duration strings (for example "30s" or "1m") or positive whole
	// seconds within time.Duration range; blank or unset defaults to DefaultPollInterval (1m).
	PollIntervalSettingKey = "scanner.poll_interval"

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
	// ErrInvalidPollInterval is returned when WatcherOptions.PollInterval is negative or invalid.
	ErrInvalidPollInterval = errors.New("scanner: poll interval must be positive")
	// ErrInvalidDebounceWindow is returned when WatcherOptions.DebounceWindow is negative.
	ErrInvalidDebounceWindow = errors.New("scanner: debounce window must be non-negative")
	// ErrInvalidRetryInterval is returned when WatcherOptions.RetryInterval is negative.
	ErrInvalidRetryInterval = errors.New("scanner: retry interval must be non-negative")
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

// JobReader retrieves persisted job state when checking whether a deduplicated
// active scan can still cover a newly observed filesystem change.
type JobReader interface {
	GetJobByID(ctx context.Context, id string) (*domain.Job, error)
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
	Jobs           JobReader
	MountDetector  MountDetector
	OSWatcher      OSWatcher
	Clock          Clock
	PollInterval   time.Duration
	DebounceWindow time.Duration
	RetryInterval  time.Duration
	Logger         *slog.Logger
	ReadDir        func(rawDir []byte) ([]os.DirEntry, error)
	Stat           func(rawPath []byte) (os.FileInfo, error)
	OnPoll         func()
	OnTrigger      func(libraryID, jobID string, err error)
}

type pollEntry struct {
	isDir      bool
	isMarker   bool
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

type pendingScan struct {
	fireAt    time.Time
	errLogged bool
}

// Watcher monitors configured library roots using mount-type detection, sequential
// read-only size/mtime polling, and per-library debounced automatic SCAN triggers.
type Watcher struct {
	roots          RootLister
	staticRoots    []domain.LibraryRoot
	trigger        JobTrigger
	jobs           JobReader
	mountDetector  MountDetector
	osWatcher      OSWatcher
	clock          Clock
	pollInterval   time.Duration
	debounceWindow time.Duration
	retryInterval  time.Duration
	logger         *slog.Logger
	readDir        func(rawDir []byte) ([]os.DirEntry, error)
	stat           func(rawPath []byte) (os.FileInfo, error)
	customStat     bool
	onPoll         func()
	onTrigger      func(libraryID, jobID string, err error)

	syncCh chan struct{}

	lifecycleMu sync.Mutex
	running     bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	mu         sync.Mutex
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
	if opts.RetryInterval < 0 {
		return nil, ErrInvalidRetryInterval
	}

	pollInterval := opts.PollInterval
	if pollInterval == 0 {
		pollInterval = DefaultPollInterval
	}
	debounceWindow := opts.DebounceWindow
	if debounceWindow == 0 {
		debounceWindow = DefaultDebounceWindow
	}
	retryInterval := opts.RetryInterval
	if retryInterval == 0 {
		retryInterval = debounceWindow
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
		jobs:           opts.Jobs,
		mountDetector:  detector,
		osWatcher:      opts.OSWatcher,
		clock:          clk,
		pollInterval:   pollInterval,
		debounceWindow: debounceWindow,
		retryInterval:  retryInterval,
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

// PollInterval returns the effective polling interval configured on w.
func (w *Watcher) PollInterval() time.Duration {
	if w == nil {
		return 0
	}
	return w.pollInterval
}

// RetryInterval returns the effective bounded retry interval used when a scan
// cannot yet be admitted.
func (w *Watcher) RetryInterval() time.Duration {
	if w == nil {
		return 0
	}
	return w.retryInterval
}

// Start launches the background mount watcher loop without blocking the caller on
// filesystem or mount I/O. Initial root baselining runs asynchronously inside the loop.
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

	w.lifecycleMu.Lock()
	if w.running {
		w.lifecycleMu.Unlock()
		return ErrWatcherAlreadyRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.running = true
	w.cancel = cancel
	w.wg.Add(1)
	w.lifecycleMu.Unlock()

	go w.runLoop(runCtx)
	return nil
}

// Stop cancels the background watcher loop, waits for all goroutines to exit, and
// closes any attached OS watcher. It is safe to call repeatedly or on a nil Watcher.
func (w *Watcher) Stop() {
	if w == nil {
		return
	}

	w.lifecycleMu.Lock()
	if !w.running {
		w.lifecycleMu.Unlock()
		return
	}
	cancel := w.cancel
	w.cancel = nil
	w.running = false
	if cancel != nil {
		cancel()
	}
	w.lifecycleMu.Unlock()

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

	pending := make(map[string]*pendingScan)
	w.syncAndBaseline(ctx, pending)
	if ctx.Err() != nil {
		return
	}
	nextPollAt := w.clock.Now().Add(w.pollInterval)

	for {
		if ctx.Err() != nil {
			return
		}

		now := w.clock.Now()
		w.fireDueScans(ctx, now, pending)

		if !now.Before(nextPollAt) {
			w.pollStep(ctx, pending)
			if ctx.Err() != nil {
				return
			}
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
			w.syncAndBaseline(ctx, pending)
		case <-timer.C():
		}
	}
}

func nextWakeDelay(now, nextPollAt time.Time, pending map[string]*pendingScan) time.Duration {
	nextWake := nextPollAt
	for _, entry := range pending {
		if entry.fireAt.Before(nextWake) {
			nextWake = entry.fireAt
		}
	}
	wait := nextWake.Sub(now)
	if wait < 0 {
		return 0
	}
	return wait
}

func markLibraryDirty(pending map[string]*pendingScan, libraryID string, fireAt time.Time) {
	if existing, ok := pending[libraryID]; ok {
		existing.fireAt = fireAt
		return
	}
	pending[libraryID] = &pendingScan{fireAt: fireAt}
}

func (w *Watcher) fireDueScans(ctx context.Context, now time.Time, pending map[string]*pendingScan) {
	if len(pending) == 0 {
		return
	}
	var due []string
	for libID, entry := range pending {
		if !now.Before(entry.fireAt) {
			due = append(due, libID)
		}
	}
	slices.Sort(due)
	for _, libID := range due {
		if ctx.Err() != nil {
			return
		}
		entry := pending[libID]
		if entry == nil {
			continue
		}
		admitted, jobID, err := w.admitScan(ctx, libID)
		if ctx.Err() != nil {
			return
		}
		if admitted {
			delete(pending, libID)
		} else {
			entry.fireAt = w.clock.Now().Add(w.retryInterval)
			w.logDeferredScan(libID, jobID, err, entry)
		}
		if w.onTrigger != nil {
			w.onTrigger(libID, jobID, err)
		}
	}
}

func (w *Watcher) logDeferredScan(libraryID, jobID string, err error, entry *pendingScan) {
	if err == nil {
		entry.errLogged = false
		w.logger.Debug("deferring automatic library scan until active scan completes",
			slog.String("library_id", libraryID),
			slog.String("active_job_id", jobID),
		)
		return
	}
	if errors.Is(err, app.ErrConflict) {
		entry.errLogged = false
		w.logger.Debug("automatic library scan deferred due to active root operation",
			slog.String("library_id", libraryID),
			logging.Err(err),
		)
		return
	}
	if !entry.errLogged {
		entry.errLogged = true
		w.logger.Error("failed to enqueue automatic library scan; retaining pending change for retry",
			slog.String("library_id", libraryID),
			logging.Err(err),
		)
	}
}

func (w *Watcher) admitScan(ctx context.Context, libraryID string) (bool, string, error) {
	req, err := buildScanTriggerRequest(libraryID, TriggerScanOptions{
		CreatedBy: nil,
	})
	if err != nil {
		return false, "", err
	}

	if dt, ok := w.trigger.(DetailedJobTrigger); ok {
		res, err := dt.TriggerOutcome(ctx, req)
		if err != nil {
			return false, "", err
		}
		if res.Created || res.State == domain.JobStateQueued {
			return true, res.JobID, nil
		}
		if res.State == "" && w.jobs != nil {
			return w.checkExistingJobQueued(ctx, res.JobID)
		}
		return false, res.JobID, nil
	}

	jobID, err := w.trigger.Trigger(ctx, req)
	if err != nil {
		return false, "", err
	}
	if w.jobs != nil {
		return w.checkExistingJobQueued(ctx, jobID)
	}
	return true, jobID, nil
}

func (w *Watcher) checkExistingJobQueued(ctx context.Context, jobID string) (bool, string, error) {
	job, err := w.jobs.GetJobByID(ctx, jobID)
	if err != nil {
		return false, jobID, err
	}
	return job.State == domain.JobStateQueued, jobID, nil
}

func (w *Watcher) pollStep(ctx context.Context, pending map[string]*pendingScan) {
	baselined := w.syncAndBaseline(ctx, pending)
	if ctx.Err() != nil {
		return
	}

	roots := w.snapshotWatchedRoots()
	for _, wr := range roots {
		if ctx.Err() != nil {
			return
		}
		if baselined[wr.root.ID] || wr.mode != WatchModePoll {
			continue
		}
		if w.pollWatchedRoot(ctx, wr) {
			markLibraryDirty(pending, wr.root.LibraryID, w.clock.Now().Add(w.debounceWindow))
		}
	}
	if ctx.Err() != nil {
		return
	}
	if w.onPoll != nil {
		w.onPoll()
	}
}

func (w *Watcher) syncAndBaseline(ctx context.Context, pending map[string]*pendingScan) map[string]bool {
	added, err := w.syncRoots(ctx, pending)
	if err != nil {
		if ctx.Err() == nil && !w.syncErrLog {
			w.syncErrLog = true
			w.logger.Error("failed to refresh library roots during watch poll", logging.Err(err))
		}
		return nil
	}
	w.syncErrLog = false

	if len(added) == 0 {
		return nil
	}
	baselined := make(map[string]bool, len(added))
	for _, wr := range added {
		if ctx.Err() != nil {
			return baselined
		}
		if wr.mode == WatchModePoll && !wr.hasBaseline {
			_ = w.pollWatchedRoot(ctx, wr)
			baselined[wr.root.ID] = true
		}
	}
	return baselined
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

type removedRootWatch struct {
	id        string
	osWatched bool
}

type resolvedWatchMode struct {
	mode          WatchMode
	fsType        string
	isNetwork     bool
	mountDetected bool
	osWatched     bool
}

func (w *Watcher) syncRoots(ctx context.Context, pending map[string]*pendingScan) ([]*watchedRoot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	roots, err := w.loadConfiguredRoots(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	desired := make(map[string]domain.LibraryRoot, len(roots))
	activeLibs := make(map[string]struct{}, len(roots))
	for _, r := range roots {
		desired[r.ID] = r
		activeLibs[r.LibraryID] = struct{}{}
	}

	removed, toAdd := w.planRootSync(roots, desired)
	for _, rem := range removed {
		if rem.osWatched && w.osWatcher != nil {
			_ = w.osWatcher.RemoveRoot(rem.id)
		}
	}

	var added []*watchedRoot
	for _, r := range toAdd {
		if err := ctx.Err(); err != nil {
			return added, err
		}
		wr, ok := w.initWatchedRoot(ctx, r)
		if !ok {
			return added, ctx.Err()
		}
		if w.commitAddedWatchedRoot(ctx, wr) {
			added = append(added, wr)
		} else {
			return added, ctx.Err()
		}
	}

	for libID := range pending {
		if _, ok := activeLibs[libID]; !ok {
			delete(pending, libID)
		}
	}
	return added, nil
}

func (w *Watcher) planRootSync(
	roots []domain.LibraryRoot,
	desired map[string]domain.LibraryRoot,
) ([]removedRootWatch, []domain.LibraryRoot) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var removed []removedRootWatch
	for id, existing := range w.watched {
		fresh, ok := desired[id]
		if !ok || !bytes.Equal(existing.root.Path.Bytes, fresh.Path.Bytes) {
			removed = append(removed, removedRootWatch{id: id, osWatched: existing.osWatched})
			delete(w.watched, id)
		}
	}

	var toAdd []domain.LibraryRoot
	for _, r := range roots {
		if _, exists := w.watched[r.ID]; !exists {
			toAdd = append(toAdd, r)
		}
	}
	return removed, toAdd
}

func (w *Watcher) commitAddedWatchedRoot(ctx context.Context, wr *watchedRoot) bool {
	if ctx.Err() != nil {
		if wr.osWatched && w.osWatcher != nil {
			_ = w.osWatcher.RemoveRoot(wr.root.ID)
		}
		return false
	}
	w.mu.Lock()
	if ctx.Err() != nil {
		w.mu.Unlock()
		if wr.osWatched && w.osWatcher != nil {
			_ = w.osWatcher.RemoveRoot(wr.root.ID)
		}
		return false
	}
	w.watched[wr.root.ID] = wr
	w.mu.Unlock()
	return true
}

func (w *Watcher) loadConfiguredRoots(ctx context.Context) ([]domain.LibraryRoot, error) {
	if w.roots != nil {
		return w.roots.ListAllLibraryRoots(ctx)
	}
	return slices.Clone(w.staticRoots), nil
}

func (w *Watcher) initWatchedRoot(ctx context.Context, r domain.LibraryRoot) (*watchedRoot, bool) {
	resolved, ok := w.resolveRootWatchMode(ctx, r)
	if !ok {
		return nil, false
	}
	return &watchedRoot{
		root:               r,
		mode:               resolved.mode,
		fsType:             resolved.fsType,
		isNetwork:          resolved.isNetwork,
		mountDetected:      resolved.mountDetected,
		osWatched:          resolved.osWatched,
		unreadableSubpaths: make(map[string]struct{}),
	}, true
}

func (w *Watcher) resolveRootWatchMode(ctx context.Context, r domain.LibraryRoot) (resolvedWatchMode, bool) {
	if ctx.Err() != nil {
		return resolvedWatchMode{}, false
	}
	info, err := w.mountDetector.Detect(r.Path.Bytes)
	if ctx.Err() != nil {
		return resolvedWatchMode{}, false
	}
	if err != nil {
		return resolvedWatchMode{
			mode:          WatchModePoll,
			mountDetected: false,
		}, true
	}

	resolved := resolvedWatchMode{
		mode:          WatchModePoll,
		fsType:        info.Type,
		isNetwork:     info.IsNetwork,
		mountDetected: true,
	}
	if info.IsNetwork || w.osWatcher == nil {
		return resolved, true
	}
	if ctx.Err() != nil {
		return resolvedWatchMode{}, false
	}
	if err := w.osWatcher.AddRoot(r); err != nil {
		w.logger.Warn("failed to register OS watch; falling back to polling",
			slog.String("library_id", r.LibraryID),
			slog.String("library_root_id", r.ID),
			slog.String("path", r.Path.Display),
			logging.Err(err),
		)
		return resolved, true
	}
	resolved.mode = WatchModeOS
	resolved.osWatched = true
	return resolved, true
}

func (w *Watcher) applyWatchMode(wr *watchedRoot, resolved resolvedWatchMode) {
	w.mu.Lock()
	defer w.mu.Unlock()
	wr.mode = resolved.mode
	wr.fsType = resolved.fsType
	wr.isNetwork = resolved.isNetwork
	wr.mountDetected = resolved.mountDetected
	wr.osWatched = resolved.osWatched
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

	if ctx.Err() != nil {
		return false
	}

	wr.unreadableSubpaths = nextUnreadable

	if wr.unreadable {
		if !wr.mountDetected {
			resolved, ok := w.resolveRootWatchMode(ctx, wr.root)
			if !ok {
				return false
			}
			w.applyWatchMode(wr, resolved)
		}
		wr.unreadable = false
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
	if err := ctx.Err(); err != nil {
		return rootSnapshot{}, err
	}
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
		if err := w.visitSnapshotEntry(ctx, wr, relDir, absDir, ancestry, isRoot, de, out, nextUnreadable); err != nil {
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
	isRoot bool,
	de os.DirEntry,
	out map[string]pollEntry,
	nextUnreadable map[string]struct{},
) error {
	nameBytes := []byte(de.Name())
	absBytes := joinAbsoluteBytes(absDir, nameBytes)
	entryType := de.Type()

	if isRoot && bytes.Equal(nameBytes, []byte(RootMarkerFilename)) {
		if isRootMarkerFile(absBytes, entryType, w.customStat, w.stat) {
			out[RootMarkerFilename] = pollEntry{isMarker: true}
		}
		return nil
	}
	if isTemporaryName(nameBytes) || isHiddenName(nameBytes) {
		return nil
	}

	relBytes := joinRelativeBytes(relDir, nameBytes)
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
		if va.isDir != vb.isDir || va.isMarker != vb.isMarker || va.unreadable != vb.unreadable || va.size != vb.size || !va.mtime.Equal(vb.mtime) {
			return false
		}
	}
	return true
}
