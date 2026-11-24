package scanner

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"

	"github.com/fsnotify/fsnotify"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/logging"
)

type fsnotifyHandle interface {
	Add(name string) error
	Remove(name string) error
	Close() error
	WatchList() []string
	Events() <-chan fsnotify.Event
	Errors() <-chan error
}

type realFSNotifyHandle struct {
	w *fsnotify.Watcher
}

func newRealFSNotifyHandle() (fsnotifyHandle, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &realFSNotifyHandle{w: w}, nil
}

func (r *realFSNotifyHandle) Add(name string) error         { return r.w.Add(name) }
func (r *realFSNotifyHandle) Remove(name string) error      { return r.w.Remove(name) }
func (r *realFSNotifyHandle) Close() error                  { return r.w.Close() }
func (r *realFSNotifyHandle) WatchList() []string           { return r.w.WatchList() }
func (r *realFSNotifyHandle) Events() <-chan fsnotify.Event { return r.w.Events }
func (r *realFSNotifyHandle) Errors() <-chan error          { return r.w.Errors }

type contextOSWatcher interface {
	AddRootContext(ctx context.Context, root domain.LibraryRoot) error
}

type watchPathInspector interface {
	WatchedPaths() []string
	RootWatchedPaths(rootID string) []string
}

type symlinkAwareOSWatcher interface {
	RootHasFileSymlinks(rootID string) bool
}

type osRootEntry struct {
	root      domain.LibraryRoot
	rootPath  string
	removed   bool
	dirs      map[string]struct{}
	watches   map[string]struct{}
	fileLinks map[string]struct{}
}

type fsnotifyOSWatcherOptions struct {
	newHandle  func() (fsnotifyHandle, error)
	readDir    func(rawDir []byte) ([]os.DirEntry, error)
	stat       func(rawPath []byte) (os.FileInfo, error)
	customStat bool
	logger     *slog.Logger
	onChange   func(rootID string)
	onRepair   func(rootID string)
	onOverflow func()
}

type fsnotifyOSWatcher struct {
	newHandle  func() (fsnotifyHandle, error)
	readDir    func(rawDir []byte) ([]os.DirEntry, error)
	stat       func(rawPath []byte) (os.FileInfo, error)
	customStat bool
	logger     *slog.Logger
	onChange   func(rootID string)
	onRepair   func(rootID string)
	onOverflow func()

	mu     sync.Mutex
	closed bool
	handle fsnotifyHandle
	roots  map[string]*osRootEntry
	wg     sync.WaitGroup
}

func newFSNotifyOSWatcher(opts fsnotifyOSWatcherOptions) *fsnotifyOSWatcher {
	newHandle := opts.newHandle
	if newHandle == nil {
		newHandle = newRealFSNotifyHandle
	}
	readDirFn := opts.readDir
	if readDirFn == nil {
		readDirFn = func(rawDir []byte) ([]os.DirEntry, error) {
			return os.ReadDir(string(rawDir))
		}
	}
	statFn := opts.stat
	if statFn == nil {
		statFn = func(rawPath []byte) (os.FileInfo, error) {
			return os.Stat(string(rawPath))
		}
	}
	logger := opts.logger
	if logger == nil {
		logger = slog.Default()
	}
	return &fsnotifyOSWatcher{
		newHandle:  newHandle,
		readDir:    readDirFn,
		stat:       statFn,
		customStat: opts.customStat,
		logger:     logger,
		onChange:   opts.onChange,
		onRepair:   opts.onRepair,
		onOverflow: opts.onOverflow,
		roots:      make(map[string]*osRootEntry),
	}
}

func (f *fsnotifyOSWatcher) AddRoot(root domain.LibraryRoot) error {
	return f.AddRootContext(context.Background(), root)
}

func (f *fsnotifyOSWatcher) AddRootContext(ctx context.Context, root domain.LibraryRoot) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	rootBytes := trimTrailingSlashes(root.Path.Bytes)
	if len(rootBytes) == 0 {
		return ErrRootNotDirectory
	}
	rootInfo, err := f.stat(rootBytes)
	if err != nil {
		return err
	}
	if !rootInfo.IsDir() {
		return ErrRootNotDirectory
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	h, err := f.ensureHandle()
	if err != nil {
		return err
	}

	candidate, prevWatches, err := f.beginRootRegistration(ctx, h, root, string(rootBytes))
	if err != nil {
		return err
	}

	if err := f.watchInitialDir(ctx, h, root.ID, candidate, rootBytes, []os.FileInfo{rootInfo}); err != nil {
		f.abortRootRegistration(h, root.ID, candidate, prevWatches)
		return err
	}

	return f.finishRootRegistration(ctx, h, root.ID, candidate, prevWatches)
}

func (f *fsnotifyOSWatcher) ensureHandle() (fsnotifyHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, fsnotify.ErrClosed
	}
	if f.handle != nil {
		return f.handle, nil
	}
	h, err := f.newHandle()
	if err != nil {
		return nil, err
	}
	f.handle = h
	f.wg.Add(1)
	go f.readLoop(h)
	return h, nil
}

func (f *fsnotifyOSWatcher) beginRootRegistration(
	ctx context.Context,
	h fsnotifyHandle,
	root domain.LibraryRoot,
	rootPath string,
) (*osRootEntry, map[string]struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed || f.handle != h {
		return nil, nil, fsnotify.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	var prevWatches map[string]struct{}
	if prev, exists := f.roots[root.ID]; exists {
		prev.removed = true
		if len(prev.watches) > 0 {
			prevWatches = make(map[string]struct{}, len(prev.watches))
			for p := range prev.watches {
				prevWatches[p] = struct{}{}
			}
		}
	}

	candidate := &osRootEntry{
		root:      root,
		rootPath:  rootPath,
		dirs:      make(map[string]struct{}),
		watches:   make(map[string]struct{}),
		fileLinks: make(map[string]struct{}),
	}
	// Publish the candidate before adding kernel watches so events delivered while
	// walking subdirectories are matched to root.ID rather than dropped.
	f.roots[root.ID] = candidate
	return candidate, prevWatches, nil
}

func (f *fsnotifyOSWatcher) stageInitialWatch(
	rootID string,
	candidate *osRootEntry,
	path string,
	isDir bool,
	isFileLink bool,
	addKernelWatch bool,
) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed || candidate.removed || f.roots[rootID] != candidate {
		return false
	}
	if isDir {
		candidate.dirs[path] = struct{}{}
	}
	if isFileLink {
		candidate.fileLinks[path] = struct{}{}
	}
	if addKernelWatch {
		// Record the path before calling h.Add so a partial registration failure
		// (e.g. Darwin kqueue failing in watchDirectoryFiles after opening the
		// directory descriptor) is always cleaned up on rollback.
		candidate.watches[path] = struct{}{}
	}
	return true
}

func (f *fsnotifyOSWatcher) completeInitialWatch(
	h fsnotifyHandle,
	rootID string,
	candidate *osRootEntry,
	path string,
	addErr error,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	active := !f.closed && !candidate.removed && f.roots[rootID] == candidate
	if active && addErr == nil {
		return nil
	}
	delete(candidate.dirs, path)
	delete(candidate.fileLinks, path)
	delete(candidate.watches, path)
	f.removeUnwatchedPathLocked(h, candidate, path)
	if addErr != nil {
		return addErr
	}
	return fsnotify.ErrClosed
}

func (f *fsnotifyOSWatcher) abortRootRegistration(
	h fsnotifyHandle,
	rootID string,
	candidate *osRootEntry,
	prevWatches map[string]struct{},
) {
	if h == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	candidate.removed = true
	all := make(map[string]struct{}, len(candidate.watches)+len(prevWatches))
	for p := range candidate.watches {
		all[p] = struct{}{}
	}
	for p := range prevWatches {
		all[p] = struct{}{}
	}
	if f.roots[rootID] == candidate {
		delete(f.roots, rootID)
	}
	toRemove := make([]string, 0, len(all))
	for p := range all {
		toRemove = append(toRemove, p)
	}
	f.removeUnwatchedPathsLocked(h, candidate, toRemove)
}

func (f *fsnotifyOSWatcher) finishRootRegistration(
	ctx context.Context,
	h fsnotifyHandle,
	rootID string,
	candidate *osRootEntry,
	prevWatches map[string]struct{},
) error {
	if err := ctx.Err(); err != nil {
		f.abortRootRegistration(h, rootID, candidate, prevWatches)
		return err
	}

	f.mu.Lock()
	if f.closed || f.handle != h || candidate.removed || f.roots[rootID] != candidate {
		f.mu.Unlock()
		f.abortRootRegistration(h, rootID, candidate, prevWatches)
		return fsnotify.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		f.mu.Unlock()
		f.abortRootRegistration(h, rootID, candidate, prevWatches)
		return err
	}
	var stale []string
	for p := range prevWatches {
		if _, keep := candidate.watches[p]; !keep {
			stale = append(stale, p)
		}
	}
	f.removeUnwatchedPathsLocked(h, candidate, stale)
	f.mu.Unlock()
	return nil
}

func (f *fsnotifyOSWatcher) isPathWatchedLocked(exclude *osRootEntry, path string) bool {
	for _, entry := range f.roots {
		if entry == exclude || entry.removed {
			continue
		}
		if _, ok := entry.watches[path]; ok {
			return true
		}
	}
	return false
}

func (f *fsnotifyOSWatcher) removeUnwatchedPathLocked(h fsnotifyHandle, exclude *osRootEntry, path string) {
	if f.closed || h == nil || f.isPathWatchedLocked(exclude, path) {
		return
	}
	_ = h.Remove(path)
}

func (f *fsnotifyOSWatcher) removeUnwatchedPathsLocked(h fsnotifyHandle, exclude *osRootEntry, paths []string) {
	if f.closed || h == nil || len(paths) == 0 {
		return
	}
	slices.Sort(paths)
	for _, p := range paths {
		if !f.isPathWatchedLocked(exclude, p) {
			_ = h.Remove(p)
		}
	}
}

func (f *fsnotifyOSWatcher) watchInitialDir(
	ctx context.Context,
	h fsnotifyHandle,
	rootID string,
	candidate *osRootEntry,
	absDir []byte,
	ancestry []os.FileInfo,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dirStr := string(absDir)
	if !f.stageInitialWatch(rootID, candidate, dirStr, true, false, true) {
		return fsnotify.ErrClosed
	}
	if err := f.completeInitialWatch(h, rootID, candidate, dirStr, h.Add(dirStr)); err != nil {
		return err
	}

	entries, err := f.readDir(absDir)
	if err != nil {
		return err
	}
	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		return bytes.Compare([]byte(a.Name()), []byte(b.Name()))
	})

	for _, de := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := f.visitInitialEntry(ctx, h, rootID, candidate, absDir, ancestry, de); err != nil {
			return err
		}
	}
	return nil
}

func (f *fsnotifyOSWatcher) visitInitialEntry(
	ctx context.Context,
	h fsnotifyHandle,
	rootID string,
	candidate *osRootEntry,
	absDir []byte,
	ancestry []os.FileInfo,
	de os.DirEntry,
) error {
	nameBytes := []byte(de.Name())
	if isTemporaryName(nameBytes) || isHiddenName(nameBytes) {
		return nil
	}

	entryType := de.Type()
	needsFollow := entryType&os.ModeSymlink != 0 || entryType == 0
	if !needsFollow && !entryType.IsDir() {
		return nil
	}

	absChild := joinAbsoluteBytes(absDir, nameBytes)
	info, err := f.resolveDirEntryInfo(absChild, de, needsFollow)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if needsFollow && classifyStatError(err) != SkipReasonUnreadable {
			// Record dangling/broken symlinks so the root also participates in scheduled
			// polling snapshots to observe when the external target later appears.
			if isSymlinkEntry(absChild, entryType) {
				if !f.stageInitialWatch(rootID, candidate, string(absChild), false, true, false) {
					return fsnotify.ErrClosed
				}
			}
			return nil
		}
		return err
	}
	if info.IsDir() {
		if hasSameFile(ancestry, info) {
			return nil
		}
		nextAncestry := append(slices.Clone(ancestry), info)
		return f.watchInitialDir(ctx, h, rootID, candidate, absChild, nextAncestry)
	}
	if info.Mode().IsRegular() && isSymlinkEntry(absChild, entryType) {
		return f.watchInitialFileSymlink(h, rootID, candidate, string(absChild))
	}
	return nil
}

func (f *fsnotifyOSWatcher) watchInitialFileSymlink(
	h fsnotifyHandle,
	rootID string,
	candidate *osRootEntry,
	linkPath string,
) error {
	// Darwin's kqueue backend internally opens and watches child entries (following
	// symlinks) when the parent directory is watched, whereas Linux inotify on the
	// parent directory does not receive events when a symlink's external target file
	// is edited directly. On non-Darwin platforms we register the symlink path with
	// fsnotify so inotify watches the target inode and emits events under linkPath.
	addKernel := runtime.GOOS != "darwin"
	if !f.stageInitialWatch(rootID, candidate, linkPath, false, true, addKernel) {
		return fsnotify.ErrClosed
	}
	if !addKernel {
		return nil
	}
	return f.completeInitialWatch(h, rootID, candidate, linkPath, h.Add(linkPath))
}

func (f *fsnotifyOSWatcher) resolveDirEntryInfo(absBytes []byte, de os.DirEntry, needsFollow bool) (os.FileInfo, error) {
	if needsFollow || f.customStat {
		return f.stat(absBytes)
	}
	return de.Info()
}

func (f *fsnotifyOSWatcher) RemoveRoot(rootID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, ok := f.roots[rootID]
	if !ok {
		return nil
	}
	entry.removed = true
	delete(f.roots, rootID)
	if len(entry.watches) == 0 {
		return nil
	}
	toRemove := make([]string, 0, len(entry.watches))
	for p := range entry.watches {
		toRemove = append(toRemove, p)
	}
	f.removeUnwatchedPathsLocked(f.handle, entry, toRemove)
	return nil
}

func (f *fsnotifyOSWatcher) Close() error {
	f.mu.Lock()
	f.closed = true
	h := f.handle
	f.handle = nil
	for _, entry := range f.roots {
		entry.removed = true
	}
	clear(f.roots)
	f.mu.Unlock()

	var err error
	if h != nil {
		err = h.Close()
	}
	f.wg.Wait()
	return err
}

// WatchedPaths returns the paths explicitly registered via fsnotify.Watcher.Add
// (WatchList). Note that on Darwin (kqueue), fsnotify additionally opens an
// internal O_EVTONLY file descriptor for each file entry inside a watched
// directory even though WatchList reports only the parent directory path.
func (f *fsnotifyOSWatcher) WatchedPaths() []string {
	f.mu.Lock()
	h := f.handle
	closed := f.closed
	f.mu.Unlock()

	if closed || h == nil {
		return nil
	}
	list := slices.Clone(h.WatchList())
	slices.Sort(list)
	return list
}

func (f *fsnotifyOSWatcher) RootWatchedPaths(rootID string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil
	}
	entry, ok := f.roots[rootID]
	if !ok || entry.removed || len(entry.dirs) == 0 {
		return nil
	}
	out := make([]string, 0, len(entry.dirs))
	for dir := range entry.dirs {
		out = append(out, dir)
	}
	slices.Sort(out)
	return out
}

func (f *fsnotifyOSWatcher) RootHasFileSymlinks(rootID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return false
	}
	entry, ok := f.roots[rootID]
	return ok && !entry.removed && len(entry.fileLinks) > 0
}

func (f *fsnotifyOSWatcher) readLoop(h fsnotifyHandle) {
	defer f.wg.Done()

	eventsCh := h.Events()
	errorsCh := h.Errors()
	for eventsCh != nil || errorsCh != nil {
		select {
		case ev, ok := <-eventsCh:
			if !ok {
				eventsCh = nil
				continue
			}
			f.handleEvent(h, ev)
		case err, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			f.handleError(err)
		}
	}
}

func (f *fsnotifyOSWatcher) handleError(err error) {
	if err == nil || errors.Is(err, fsnotify.ErrClosed) || errors.Is(err, os.ErrClosed) {
		return
	}
	if errors.Is(err, fsnotify.ErrEventOverflow) {
		f.logger.Warn("fsnotify event queue overflow; triggering full library scan", logging.Err(err))
	} else {
		f.logger.Warn("fsnotify watch error; triggering full library scan", logging.Err(err))
	}
	if f.onOverflow != nil {
		f.onOverflow()
	}
}

type matchedOSRoot struct {
	rootID   string
	rootPath string
}

func (f *fsnotifyOSWatcher) matchingRoots(evPath string) []matchedOSRoot {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed || len(f.roots) == 0 {
		return nil
	}
	var out []matchedOSRoot
	for id, entry := range f.roots {
		if entry.removed {
			continue
		}
		if evPath == entry.rootPath || isSubpathOf(evPath, entry.rootPath) {
			out = append(out, matchedOSRoot{
				rootID:   id,
				rootPath: entry.rootPath,
			})
		}
	}
	return out
}

func (f *fsnotifyOSWatcher) handleEvent(h fsnotifyHandle, ev fsnotify.Event) {
	if ev.Name == "" || ev.Op == 0 {
		return
	}
	evPath := trimTrailingSlashesString(ev.Name)
	matches := f.matchingRoots(evPath)
	for _, m := range matches {
		f.handleRootEvent(h, m, evPath, ev.Op)
	}
}

func (f *fsnotifyOSWatcher) handleRootEvent(h fsnotifyHandle, m matchedOSRoot, evPath string, op fsnotify.Op) {
	if evPath == m.rootPath {
		if op.Has(fsnotify.Remove) || op.Has(fsnotify.Rename) {
			f.clearRootWatches(h, m.rootID)
			f.emitRepair(m.rootID)
		}
		return
	}

	relPath := relativeToRootPath(evPath, m.rootPath)
	switch classifyWatchRelPath(relPath) {
	case watchRelIgnored:
		return
	case watchRelRootMarker:
		if op.Has(fsnotify.Remove) || op.Has(fsnotify.Rename) || isRootMarkerFile([]byte(evPath), 0, true, f.stat) {
			f.emitChange(m.rootID)
		}
		return
	case watchRelNormal:
		if f.isWatchedDir(m.rootID, evPath) {
			f.handleWatchedDirEvent(h, m.rootID, evPath, op)
			return
		}
		f.handleChildPathEvent(h, m.rootID, evPath, op)
	}
}

func (f *fsnotifyOSWatcher) handleWatchedDirEvent(h fsnotifyHandle, rootID, evPath string, op fsnotify.Op) {
	if op.Has(fsnotify.Remove) || op.Has(fsnotify.Rename) {
		f.removeWatchedPrefix(h, rootID, evPath)
		if info, err := f.stat([]byte(evPath)); err == nil && info.IsDir() {
			if _, watchErr := f.watchRuntimeDirTree(h, rootID, []byte(evPath), []os.FileInfo{info}); watchErr != nil {
				f.emitRepair(rootID)
				return
			}
		}
		f.emitChange(rootID)
		return
	}
	if op.Has(fsnotify.Create) {
		if info, err := f.stat([]byte(evPath)); err == nil && info.IsDir() {
			foundFiles, watchErr := f.watchRuntimeDirTree(h, rootID, []byte(evPath), []os.FileInfo{info})
			if watchErr != nil {
				f.emitRepair(rootID)
				return
			}
			if foundFiles {
				f.emitChange(rootID)
			}
		}
	}
}

func (f *fsnotifyOSWatcher) handleChildPathEvent(h fsnotifyHandle, rootID, evPath string, op fsnotify.Op) {
	wasFileLink := f.isWatchedFileSymlink(rootID, evPath)
	info, err := f.stat([]byte(evPath))
	if err == nil {
		if info.IsDir() {
			if wasFileLink {
				f.removeWatchedFileSymlink(h, rootID, evPath)
			}
			foundFiles, watchErr := f.watchRuntimeDirTree(h, rootID, []byte(evPath), []os.FileInfo{info})
			if watchErr != nil {
				f.emitRepair(rootID)
				return
			}
			if foundFiles || op.Has(fsnotify.Rename) {
				f.emitChange(rootID)
			}
			return
		}
		if info.Mode().IsRegular() {
			if wasFileLink || isSymlinkPath(evPath) {
				if watchErr := f.watchRuntimeFileSymlink(h, rootID, evPath); watchErr != nil {
					f.emitRepair(rootID)
					return
				}
			}
			if op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename|fsnotify.Chmod) != 0 {
				f.emitChange(rootID)
			}
		}
		return
	}

	f.removeWatchedPrefix(h, rootID, evPath)
	if isSymlinkPath(evPath) {
		f.recordDanglingRuntimeSymlink(h, rootID, evPath)
	}
	if op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename|fsnotify.Chmod) != 0 {
		f.emitChange(rootID)
	}
}

func (f *fsnotifyOSWatcher) watchRuntimeDirTree(
	h fsnotifyHandle,
	rootID string,
	absDir []byte,
	ancestry []os.FileInfo,
) (bool, error) {
	dirStr := string(absDir)
	entry, ok := f.stageRuntimeWatch(rootID, dirStr, true, false)
	if !ok {
		return false, nil
	}
	active, addErr := f.completeRuntimeWatch(h, entry, dirStr, h.Add(dirStr))
	if addErr != nil {
		f.logger.Warn("failed to add OS watch for new subdirectory; triggering full library scan",
			slog.String("library_root_id", rootID),
			slog.String("path", dirStr),
			logging.Err(addErr),
		)
		return false, addErr
	}
	if !active {
		return false, nil
	}

	entries, err := f.readDir(absDir)
	if err != nil {
		if isTransientDisappearance(err) {
			f.cleanupFailedRuntimeWatch(h, entry, dirStr)
			return false, nil
		}
		f.logger.Warn("failed to read newly watched subdirectory; triggering full library scan",
			slog.String("library_root_id", rootID),
			slog.String("path", dirStr),
			logging.Err(err),
		)
		return false, err
	}

	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		return bytes.Compare([]byte(a.Name()), []byte(b.Name()))
	})

	foundFiles := false
	var firstErr error
	for _, de := range entries {
		childFiles, childErr := f.visitRuntimeDirEntry(h, rootID, absDir, ancestry, de)
		if childFiles {
			foundFiles = true
		}
		if childErr != nil && firstErr == nil {
			firstErr = childErr
		}
	}
	return foundFiles, firstErr
}

func (f *fsnotifyOSWatcher) visitRuntimeDirEntry(
	h fsnotifyHandle,
	rootID string,
	absDir []byte,
	ancestry []os.FileInfo,
	de os.DirEntry,
) (bool, error) {
	nameBytes := []byte(de.Name())
	if isTemporaryName(nameBytes) || isHiddenName(nameBytes) {
		return false, nil
	}

	entryType := de.Type()
	needsFollow := entryType&os.ModeSymlink != 0 || entryType == 0
	if !needsFollow && !f.customStat {
		if entryType.IsRegular() {
			return true, nil
		}
		if !entryType.IsDir() {
			return false, nil
		}
	}

	absChild := joinAbsoluteBytes(absDir, nameBytes)
	info, err := f.resolveDirEntryInfo(absChild, de, needsFollow)
	if err != nil {
		if isTransientDisappearance(err) || (needsFollow && classifyStatError(err) != SkipReasonUnreadable) {
			if needsFollow && isSymlinkEntry(absChild, entryType) {
				_, _ = f.stageRuntimeWatch(rootID, string(absChild), false, true)
			}
			return false, nil
		}
		return false, err
	}
	if info.IsDir() {
		if hasSameFile(ancestry, info) {
			return false, nil
		}
		nextAncestry := append(slices.Clone(ancestry), info)
		return f.watchRuntimeDirTree(h, rootID, absChild, nextAncestry)
	}
	if info.Mode().IsRegular() {
		if isSymlinkEntry(absChild, entryType) {
			if err := f.watchRuntimeFileSymlink(h, rootID, string(absChild)); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	return false, nil
}

func (f *fsnotifyOSWatcher) watchRuntimeFileSymlink(h fsnotifyHandle, rootID, linkPath string) error {
	addKernel := runtime.GOOS != "darwin"
	entry, ok := f.stageRuntimeWatch(rootID, linkPath, false, true)
	if !ok || !addKernel {
		return nil
	}
	_, addErr := f.completeRuntimeWatch(h, entry, linkPath, h.Add(linkPath))
	if addErr != nil {
		f.logger.Warn("failed to add OS watch for followed file symlink; triggering full library scan",
			slog.String("library_root_id", rootID),
			slog.String("path", linkPath),
			logging.Err(addErr),
		)
		return addErr
	}
	return nil
}

func (f *fsnotifyOSWatcher) recordDanglingRuntimeSymlink(h fsnotifyHandle, rootID, linkPath string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return
	}
	entry, ok := f.roots[rootID]
	if !ok || entry.removed {
		return
	}
	entry.fileLinks[linkPath] = struct{}{}
	if _, hadWatch := entry.watches[linkPath]; hadWatch {
		delete(entry.watches, linkPath)
		f.removeUnwatchedPathLocked(h, entry, linkPath)
	}
}

func (f *fsnotifyOSWatcher) stageRuntimeWatch(
	rootID string,
	path string,
	isDir bool,
	isFileLink bool,
) (*osRootEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, false
	}
	entry, ok := f.roots[rootID]
	if !ok || entry.removed {
		return nil, false
	}
	if isDir {
		entry.dirs[path] = struct{}{}
	}
	if isFileLink {
		entry.fileLinks[path] = struct{}{}
	}
	return entry, true
}

func (f *fsnotifyOSWatcher) completeRuntimeWatch(
	h fsnotifyHandle,
	entry *osRootEntry,
	path string,
	addErr error,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	rootID := ""
	if entry != nil {
		rootID = entry.root.ID
	}
	active := !f.closed && entry != nil && !entry.removed && f.roots[rootID] == entry
	if active && addErr == nil {
		entry.watches[path] = struct{}{}
		return true, nil
	}
	if entry != nil {
		delete(entry.dirs, path)
		delete(entry.fileLinks, path)
		delete(entry.watches, path)
	}
	f.removeUnwatchedPathLocked(h, entry, path)
	if !active || isTransientDisappearance(addErr) || errors.Is(addErr, fsnotify.ErrClosed) {
		return false, nil
	}
	return false, addErr
}

func (f *fsnotifyOSWatcher) cleanupFailedRuntimeWatch(h fsnotifyHandle, entry *osRootEntry, path string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if entry != nil {
		delete(entry.dirs, path)
		delete(entry.fileLinks, path)
		delete(entry.watches, path)
	}
	f.removeUnwatchedPathLocked(h, entry, path)
}

func (f *fsnotifyOSWatcher) isWatchedDir(rootID, dir string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.roots[rootID]
	if !ok || entry.removed {
		return false
	}
	_, watched := entry.dirs[dir]
	return watched
}

func (f *fsnotifyOSWatcher) isWatchedFileSymlink(rootID, path string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.roots[rootID]
	if !ok || entry.removed {
		return false
	}
	_, watched := entry.fileLinks[path]
	return watched
}

func (f *fsnotifyOSWatcher) removeWatchedFileSymlink(h fsnotifyHandle, rootID, path string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return
	}
	entry, ok := f.roots[rootID]
	if !ok || entry.removed {
		return
	}
	delete(entry.fileLinks, path)
	if _, hadWatch := entry.watches[path]; hadWatch {
		delete(entry.watches, path)
		f.removeUnwatchedPathLocked(h, entry, path)
	}
}

func (f *fsnotifyOSWatcher) clearRootWatches(h fsnotifyHandle, rootID string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, ok := f.roots[rootID]
	if !ok || entry.removed {
		return
	}
	toRemove := make([]string, 0, len(entry.watches))
	for p := range entry.watches {
		toRemove = append(toRemove, p)
	}
	clear(entry.dirs)
	clear(entry.watches)
	clear(entry.fileLinks)
	f.removeUnwatchedPathsLocked(h, entry, toRemove)
}

func (f *fsnotifyOSWatcher) removeWatchedPrefix(h fsnotifyHandle, rootID, prefix string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, ok := f.roots[rootID]
	if !ok || entry.removed {
		return
	}
	for dir := range entry.dirs {
		if dir == prefix || isSubpathOf(dir, prefix) {
			delete(entry.dirs, dir)
		}
	}
	for link := range entry.fileLinks {
		if link == prefix || isSubpathOf(link, prefix) {
			delete(entry.fileLinks, link)
		}
	}
	var toRemove []string
	for p := range entry.watches {
		if p == prefix || isSubpathOf(p, prefix) {
			delete(entry.watches, p)
			toRemove = append(toRemove, p)
		}
	}
	f.removeUnwatchedPathsLocked(h, entry, toRemove)
}

func (f *fsnotifyOSWatcher) emitChange(rootID string) {
	if f.onChange != nil {
		f.onChange(rootID)
	}
}

func (f *fsnotifyOSWatcher) emitRepair(rootID string) {
	if f.onRepair != nil {
		f.onRepair(rootID)
		return
	}
	if f.onChange != nil {
		f.onChange(rootID)
	}
}

func isSymlinkEntry(absPath []byte, entryType fs.FileMode) bool {
	if entryType&os.ModeSymlink != 0 {
		return true
	}
	if entryType != 0 {
		return false
	}
	return isSymlinkPath(string(absPath))
}

func isSymlinkPath(path string) bool {
	fi, err := os.Lstat(path)
	return err == nil && fi.Mode()&os.ModeSymlink != 0
}

type watchRelClass int

const (
	watchRelIgnored watchRelClass = iota
	watchRelRootMarker
	watchRelNormal
)

func classifyWatchRelPath(rel string) watchRelClass {
	if rel == "" {
		return watchRelIgnored
	}
	if rel == RootMarkerFilename {
		return watchRelRootMarker
	}
	start := 0
	for i := 0; i <= len(rel); i++ {
		if i < len(rel) && rel[i] != '/' {
			continue
		}
		seg := rel[start:i]
		start = i + 1
		if seg == "" {
			return watchRelIgnored
		}
		segBytes := []byte(seg)
		if isHiddenName(segBytes) || isTemporaryName(segBytes) {
			return watchRelIgnored
		}
	}
	return watchRelNormal
}

func trimTrailingSlashesString(s string) string {
	for len(s) > 1 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func isSubpathOf(path, parent string) bool {
	if parent == "/" {
		return len(path) > 1 && path[0] == '/'
	}
	return len(path) > len(parent)+1 && path[len(parent)] == '/' && strings.HasPrefix(path, parent)
}

func relativeToRootPath(path, rootPath string) string {
	if rootPath == "/" {
		return strings.TrimPrefix(path, "/")
	}
	return path[len(rootPath)+1:]
}

func isTransientDisappearance(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR)
}
