package scanner

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
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

type osRootEntry struct {
	root     domain.LibraryRoot
	rootPath string
	dirs     map[string]struct{}
}

type fsnotifyOSWatcherOptions struct {
	newHandle  func() (fsnotifyHandle, error)
	readDir    func(rawDir []byte) ([]os.DirEntry, error)
	stat       func(rawPath []byte) (os.FileInfo, error)
	customStat bool
	logger     *slog.Logger
	onChange   func(rootID string)
	onOverflow func()
}

type fsnotifyOSWatcher struct {
	newHandle  func() (fsnotifyHandle, error)
	readDir    func(rawDir []byte) ([]os.DirEntry, error)
	stat       func(rawPath []byte) (os.FileInfo, error)
	customStat bool
	logger     *slog.Logger
	onChange   func(rootID string)
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

	dirs := make(map[string]struct{})
	if err := f.watchInitialDir(ctx, h, rootBytes, []os.FileInfo{rootInfo}, dirs); err != nil {
		f.rollbackAddedDirs(h, dirs)
		return err
	}

	return f.commitRootWatches(ctx, h, root, string(rootBytes), dirs)
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

func (f *fsnotifyOSWatcher) commitRootWatches(
	ctx context.Context,
	h fsnotifyHandle,
	root domain.LibraryRoot,
	rootPath string,
	dirs map[string]struct{},
) error {
	if err := ctx.Err(); err != nil {
		f.rollbackAddedDirs(h, dirs)
		return err
	}

	var stale []string
	f.mu.Lock()
	if f.closed || f.handle != h {
		f.mu.Unlock()
		f.rollbackAddedDirs(h, dirs)
		return fsnotify.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		f.mu.Unlock()
		f.rollbackAddedDirs(h, dirs)
		return err
	}
	if prev, exists := f.roots[root.ID]; exists {
		for dir := range prev.dirs {
			if _, keep := dirs[dir]; !keep && !f.isDirWatchedByOtherRootLocked(root.ID, dir) {
				stale = append(stale, dir)
			}
		}
	}
	f.roots[root.ID] = &osRootEntry{
		root:     root,
		rootPath: rootPath,
		dirs:     dirs,
	}
	f.mu.Unlock()

	for _, dir := range stale {
		_ = h.Remove(dir)
	}
	return nil
}

func (f *fsnotifyOSWatcher) rollbackAddedDirs(h fsnotifyHandle, dirs map[string]struct{}) {
	if h == nil || len(dirs) == 0 {
		return
	}
	var toRemove []string
	f.mu.Lock()
	for dir := range dirs {
		if !f.isDirWatchedByOtherRootLocked("", dir) {
			toRemove = append(toRemove, dir)
		}
	}
	f.mu.Unlock()

	for _, dir := range toRemove {
		_ = h.Remove(dir)
	}
}

func (f *fsnotifyOSWatcher) isDirWatchedByOtherRootLocked(excludeRootID, dir string) bool {
	for id, entry := range f.roots {
		if id == excludeRootID {
			continue
		}
		if _, ok := entry.dirs[dir]; ok {
			return true
		}
	}
	return false
}

func (f *fsnotifyOSWatcher) watchInitialDir(
	ctx context.Context,
	h fsnotifyHandle,
	absDir []byte,
	ancestry []os.FileInfo,
	dirs map[string]struct{},
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dirStr := string(absDir)
	if err := h.Add(dirStr); err != nil {
		return err
	}
	dirs[dirStr] = struct{}{}

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
		if err := f.visitInitialEntry(ctx, h, absDir, ancestry, de, dirs); err != nil {
			return err
		}
	}
	return nil
}

func (f *fsnotifyOSWatcher) visitInitialEntry(
	ctx context.Context,
	h fsnotifyHandle,
	absDir []byte,
	ancestry []os.FileInfo,
	de os.DirEntry,
	dirs map[string]struct{},
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
			return nil
		}
		return err
	}
	if !info.IsDir() || hasSameFile(ancestry, info) {
		return nil
	}

	nextAncestry := append(slices.Clone(ancestry), info)
	return f.watchInitialDir(ctx, h, absChild, nextAncestry, dirs)
}

func (f *fsnotifyOSWatcher) resolveDirEntryInfo(absBytes []byte, de os.DirEntry, needsFollow bool) (os.FileInfo, error) {
	if needsFollow || f.customStat {
		return f.stat(absBytes)
	}
	return de.Info()
}

func (f *fsnotifyOSWatcher) RemoveRoot(rootID string) error {
	var (
		h        fsnotifyHandle
		toRemove []string
	)
	f.mu.Lock()
	entry, ok := f.roots[rootID]
	if ok {
		delete(f.roots, rootID)
		h = f.handle
		for dir := range entry.dirs {
			if !f.isDirWatchedByOtherRootLocked(rootID, dir) {
				toRemove = append(toRemove, dir)
			}
		}
	}
	f.mu.Unlock()

	if h != nil {
		slices.Sort(toRemove)
		for _, dir := range toRemove {
			_ = h.Remove(dir)
		}
	}
	return nil
}

func (f *fsnotifyOSWatcher) Close() error {
	f.mu.Lock()
	f.closed = true
	h := f.handle
	f.handle = nil
	clear(f.roots)
	f.mu.Unlock()

	var err error
	if h != nil {
		err = h.Close()
	}
	f.wg.Wait()
	return err
}

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
	if !ok || len(entry.dirs) == 0 {
		return nil
	}
	out := make([]string, 0, len(entry.dirs))
	for dir := range entry.dirs {
		out = append(out, dir)
	}
	slices.Sort(out)
	return out
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
			f.emitChange(m.rootID)
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
			_, _ = f.watchRuntimeDirTree(h, rootID, []byte(evPath), []os.FileInfo{info})
		}
		f.emitChange(rootID)
		return
	}
	if op.Has(fsnotify.Create) {
		if info, err := f.stat([]byte(evPath)); err == nil && info.IsDir() {
			if foundFiles, watchErr := f.watchRuntimeDirTree(h, rootID, []byte(evPath), []os.FileInfo{info}); foundFiles || watchErr != nil {
				f.emitChange(rootID)
			}
		}
	}
}

func (f *fsnotifyOSWatcher) handleChildPathEvent(h fsnotifyHandle, rootID, evPath string, op fsnotify.Op) {
	info, err := f.stat([]byte(evPath))
	if err == nil {
		if info.IsDir() {
			foundFiles, watchErr := f.watchRuntimeDirTree(h, rootID, []byte(evPath), []os.FileInfo{info})
			if foundFiles || watchErr != nil || op.Has(fsnotify.Rename) {
				f.emitChange(rootID)
			}
			return
		}
		if info.Mode().IsRegular() && op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename|fsnotify.Chmod) != 0 {
			f.emitChange(rootID)
		}
		return
	}

	f.removeWatchedPrefix(h, rootID, evPath)
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
	if !f.isRootActive(rootID) {
		return false, nil
	}
	dirStr := string(absDir)
	if err := h.Add(dirStr); err != nil {
		if isTransientDisappearance(err) || errors.Is(err, fsnotify.ErrClosed) {
			return false, nil
		}
		f.logger.Warn("failed to add OS watch for new subdirectory; triggering full library scan",
			slog.String("library_root_id", rootID),
			slog.String("path", dirStr),
			logging.Err(err),
		)
		return false, err
	}
	if !f.recordWatchedDir(rootID, dirStr) {
		_ = h.Remove(dirStr)
		return false, nil
	}

	entries, err := f.readDir(absDir)
	if err != nil {
		if isTransientDisappearance(err) {
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
	return info.Mode().IsRegular(), nil
}

func (f *fsnotifyOSWatcher) isRootActive(rootID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.roots[rootID]
	return !f.closed && ok
}

func (f *fsnotifyOSWatcher) isWatchedDir(rootID, dir string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.roots[rootID]
	if !ok {
		return false
	}
	_, watched := entry.dirs[dir]
	return watched
}

func (f *fsnotifyOSWatcher) recordWatchedDir(rootID, dir string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return false
	}
	entry, ok := f.roots[rootID]
	if !ok {
		return false
	}
	entry.dirs[dir] = struct{}{}
	return true
}

func (f *fsnotifyOSWatcher) removeWatchedPrefix(h fsnotifyHandle, rootID, prefix string) {
	var toRemove []string
	f.mu.Lock()
	entry, ok := f.roots[rootID]
	if ok {
		for dir := range entry.dirs {
			if dir == prefix || isSubpathOf(dir, prefix) {
				delete(entry.dirs, dir)
				if !f.isDirWatchedByOtherRootLocked(rootID, dir) {
					toRemove = append(toRemove, dir)
				}
			}
		}
	}
	f.mu.Unlock()

	for _, dir := range toRemove {
		_ = h.Remove(dir)
	}
}

func (f *fsnotifyOSWatcher) emitChange(rootID string) {
	if f.onChange != nil {
		f.onChange(rootID)
	}
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
