package scanner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/mount"
)

type fakeWatcherClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeWatcherTimer
	parked  chan struct{}
	parkSeq uint64
}

type fakeWatcherTimer struct {
	clock    *fakeWatcherClock
	deadline time.Time
	ch       chan time.Time
	active   bool
}

func newFakeWatcherClock(start time.Time) *fakeWatcherClock {
	return &fakeWatcherClock{
		now:    start,
		parked: make(chan struct{}, 16),
	}
}

func (c *fakeWatcherClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeWatcherClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &fakeWatcherTimer{
		clock:    c,
		deadline: c.now.Add(d),
		ch:       make(chan time.Time, 1),
	}
	if d <= 0 {
		t.ch <- c.now
		return t
	}
	t.active = true
	c.timers = append(c.timers, t)
	c.parkSeq++
	select {
	case c.parked <- struct{}{}:
	default:
	}
	return t
}

func (t *fakeWatcherTimer) C() <-chan time.Time {
	return t.ch
}

func (t *fakeWatcherTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := t.active
	t.active = false
	return was
}

func (t *fakeWatcherTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := t.active
	t.deadline = t.clock.now.Add(d)
	if d <= 0 {
		t.active = false
		select {
		case t.ch <- t.clock.now:
		default:
		}
		return was
	}
	if !t.active {
		t.active = true
		t.clock.timers = append(t.clock.timers, t)
	}
	return was
}

func (c *fakeWatcherClock) waitForParked(t testing.TB, minSeq uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c.mu.Lock()
		seq := c.parkSeq
		activeCount := 0
		for _, tm := range c.timers {
			if tm.active {
				activeCount++
			}
		}
		c.mu.Unlock()
		if seq >= minSeq && activeCount > 0 {
			return seq
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for watcher loop to park (seq=%d, want>=%d, active=%d)", seq, minSeq, activeCount)
		}
		select {
		case <-c.parked:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (c *fakeWatcherClock) advanceAndWait(t testing.TB, step time.Duration) {
	t.Helper()
	c.mu.Lock()
	seqBefore := c.parkSeq
	c.mu.Unlock()

	if seqBefore == 0 {
		seqBefore = c.waitForParked(t, 1)
	}

	c.mu.Lock()
	c.now = c.now.Add(step)
	var kept []*fakeWatcherTimer
	var fired []*fakeWatcherTimer
	for _, tm := range c.timers {
		if !tm.active {
			continue
		}
		if !tm.deadline.After(c.now) {
			tm.active = false
			fired = append(fired, tm)
		} else {
			kept = append(kept, tm)
		}
	}
	c.timers = kept
	now := c.now
	c.mu.Unlock()

	for _, tm := range fired {
		select {
		case tm.ch <- now:
		default:
		}
	}
	if len(fired) > 0 {
		c.waitForParked(t, seqBefore+1)
	}
}

type recordedTrigger struct {
	mu   sync.Mutex
	reqs []jobs.TriggerRequest
	err  error
}

func (r *recordedTrigger) Trigger(_ context.Context, req jobs.TriggerRequest) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return "", r.err
	}
	r.reqs = append(r.reqs, req)
	return "job-auto-" + string(rune('0'+len(r.reqs))), nil
}

func (r *recordedTrigger) snapshot() []jobs.TriggerRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reqs)
}

type spyOSWatcher struct {
	mu       sync.Mutex
	added    []domain.LibraryRoot
	removed  []string
	closeCnt int
}

func (s *spyOSWatcher) AddRoot(root domain.LibraryRoot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.added = append(s.added, root)
	return nil
}

func (s *spyOSWatcher) RemoveRoot(rootID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removed = append(s.removed, rootID)
	return nil
}

func (s *spyOSWatcher) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCnt++
	return nil
}

func (s *spyOSWatcher) addedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.added)
}

func (s *spyOSWatcher) closed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCnt
}

type capturedLogRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

type memoryLogHandler struct {
	mu      sync.Mutex
	records []capturedLogRecord
}

func (h *memoryLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *memoryLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any)
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, capturedLogRecord{
		Level:   r.Level,
		Message: r.Message,
		Attrs:   attrs,
	})
	h.mu.Unlock()
	return nil
}

func (h *memoryLogHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *memoryLogHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *memoryLogHandler) errorCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, rec := range h.records {
		if rec.Level == slog.LevelError {
			n++
		}
	}
	return n
}

func makeWatcherTestRoot(t *testing.T, libID, rootID, dir string) domain.LibraryRoot {
	t.Helper()
	pid, err := domain.NewRootPathIdentity([]byte(dir))
	if err != nil {
		t.Fatalf("NewRootPathIdentity(%q): %v", dir, err)
	}
	return domain.LibraryRoot{
		ID:        rootID,
		LibraryID: libID,
		Path:      pid,
		State:     domain.LibraryRootStateOnline,
	}
}

func writeWatcherFile(t *testing.T, path string, data []byte, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatalf("Chtimes(%s): %v", path, err)
		}
	}
}

func TestWatcher_NetworkMountPollsWithNoOSWatch(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	r1 := makeWatcherTestRoot(t, "lib-1", "root-1", dir1)
	r2 := makeWatcherTestRoot(t, "lib-2", "root-2", dir2)

	osSpy := &spyOSWatcher{}
	trig := &recordedTrigger{}
	clk := newFakeWatcherClock(time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC))

	w, err := NewWatcher(WatcherOptions{
		StaticRoots: []domain.LibraryRoot{r1, r2},
		Trigger:     trig,
		OSWatcher:   osSpy,
		Clock:       clk,
		MountDetector: MountDetectorFunc(func(rawPath []byte) (mount.FilesystemInfo, error) {
			if bytes.Equal(rawPath, []byte(dir1)) {
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}
			return mount.FilesystemInfo{Type: "smbfs", IsNetwork: true}, nil
		}),
		PollInterval:   20 * time.Millisecond,
		DebounceWindow: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()
	clk.waitForParked(t, 1)

	if osSpy.addedCount() != 0 {
		t.Fatalf("OSWatcher.AddRoot called %d times for network mounts, want 0", osSpy.addedCount())
	}
	statuses := w.RootStatuses()
	if len(statuses) != 2 {
		t.Fatalf("RootStatuses len = %d, want 2", len(statuses))
	}
	for _, st := range statuses {
		if st.Mode != WatchModePoll || !st.IsNetwork {
			t.Fatalf("status = %+v, want Mode=poll IsNetwork=true", st)
		}
	}
}

func TestWatcher_UnchangedTreeEnqueuesNoJobAcrossTicks(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	writeWatcherFile(t, filepath.Join(dir, "SeriesA/Vol01.cbz"), []byte("cbz-1"), t0)
	writeWatcherFile(t, filepath.Join(dir, "SeriesB/Vol02.cbz"), []byte("cbz-2"), t0)
	writeWatcherFile(t, filepath.Join(dir, ".hidden/ignored.cbz"), []byte("hidden"), t0)

	root := makeWatcherTestRoot(t, "lib-unchanged", "root-unchanged", dir)
	trig := &recordedTrigger{}
	clk := newFakeWatcherClock(t0)

	w, err := NewWatcher(WatcherOptions{
		StaticRoots: []domain.LibraryRoot{root},
		Trigger:     trig,
		Clock:       clk,
		MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
			return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
		}),
		PollInterval:   20 * time.Millisecond,
		DebounceWindow: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	for range 6 {
		clk.advanceAndWait(t, 20*time.Millisecond)
	}
	if reqs := trig.snapshot(); len(reqs) != 0 {
		t.Fatalf("unchanged tree triggered %d SCAN jobs, want 0", len(reqs))
	}
}

func TestWatcher_DetectsCreateModifyRenameDeleteAndDebounces(t *testing.T) {
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	t.Run("each change kind emits one automatic SCAN", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(t *testing.T, dir string)
		}{
			{
				name: "create",
				mutate: func(t *testing.T, dir string) {
					writeWatcherFile(t, filepath.Join(dir, "sub/new.cbz"), []byte("new"), t0.Add(time.Second))
				},
			},
			{
				name: "modify_mtime_same_size",
				mutate: func(t *testing.T, dir string) {
					writeWatcherFile(t, filepath.Join(dir, "existing.cbz"), []byte("12345"), t0.Add(2*time.Second))
				},
			},
			{
				name: "modify_size",
				mutate: func(t *testing.T, dir string) {
					writeWatcherFile(t, filepath.Join(dir, "existing.cbz"), []byte("123456789"), t0)
				},
			},
			{
				name: "rename",
				mutate: func(t *testing.T, dir string) {
					if err := os.Rename(filepath.Join(dir, "existing.cbz"), filepath.Join(dir, "renamed.cbz")); err != nil {
						t.Fatalf("Rename: %v", err)
					}
				},
			},
			{
				name: "delete",
				mutate: func(t *testing.T, dir string) {
					if err := os.Remove(filepath.Join(dir, "existing.cbz")); err != nil {
						t.Fatalf("Remove: %v", err)
					}
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				writeWatcherFile(t, filepath.Join(dir, "existing.cbz"), []byte("12345"), t0)
				root := makeWatcherTestRoot(t, "lib-1", "root-1", dir)

				trig := &recordedTrigger{}
				clk := newFakeWatcherClock(t0)
				w, err := NewWatcher(WatcherOptions{
					StaticRoots: []domain.LibraryRoot{root},
					Trigger:     trig,
					Clock:       clk,
					MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
						return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
					}),
					PollInterval:   10 * time.Millisecond,
					DebounceWindow: 30 * time.Millisecond,
				})
				if err != nil {
					t.Fatalf("NewWatcher: %v", err)
				}
				if err := w.Start(t.Context()); err != nil {
					t.Fatalf("Start: %v", err)
				}
				defer w.Stop()
				clk.waitForParked(t, 1)

				tc.mutate(t, dir)

				// Poll detects change and arms 30ms debounce.
				clk.advanceAndWait(t, 10*time.Millisecond)
				if len(trig.snapshot()) != 0 {
					t.Fatalf("expected debounce delay before trigger")
				}
				// Advance through debounce window in 10ms poll steps.
				clk.advanceAndWait(t, 10*time.Millisecond)
				clk.advanceAndWait(t, 10*time.Millisecond)
				clk.advanceAndWait(t, 10*time.Millisecond)

				reqs := trig.snapshot()
				if len(reqs) != 1 {
					t.Fatalf("got %d triggered jobs, want 1", len(reqs))
				}
				req := reqs[0]
				if req.Kind != domain.JobKindScan {
					t.Fatalf("Kind = %s, want SCAN", req.Kind)
				}
				if req.CreatedBy != nil {
					t.Fatalf("CreatedBy = %v, want nil (automatic job)", *req.CreatedBy)
				}
				if req.DedupeKey == nil || *req.DedupeKey != "SCAN:lib-1" {
					t.Fatalf("DedupeKey = %v, want SCAN:lib-1", req.DedupeKey)
				}
				var payload ScanJobPayload
				if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.LibraryID != "lib-1" {
					t.Fatalf("Payload = %s (%v), want library_id=lib-1", req.Payload, err)
				}
			})
		}
	})

	t.Run("burst across multiple files and roots coalesces into one SCAN per debounce window", func(t *testing.T) {
		dirA := t.TempDir()
		dirB := t.TempDir()
		writeWatcherFile(t, filepath.Join(dirA, "a1.cbz"), []byte("a1"), t0)
		writeWatcherFile(t, filepath.Join(dirB, "b1.cbz"), []byte("b1"), t0)

		rootA := makeWatcherTestRoot(t, "lib-multi", "root-a", dirA)
		rootB := makeWatcherTestRoot(t, "lib-multi", "root-b", dirB)
		trig := &recordedTrigger{}
		clk := newFakeWatcherClock(t0)

		w, err := NewWatcher(WatcherOptions{
			StaticRoots: []domain.LibraryRoot{rootA, rootB},
			Trigger:     trig,
			Clock:       clk,
			MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				return mount.FilesystemInfo{Type: "cifs", IsNetwork: true}, nil
			}),
			PollInterval:   10 * time.Millisecond,
			DebounceWindow: 50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		if err := w.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer w.Stop()
		clk.waitForParked(t, 1)

		// Burst part 1 across both roots.
		writeWatcherFile(t, filepath.Join(dirA, "a2.cbz"), []byte("a2"), t0)
		writeWatcherFile(t, filepath.Join(dirA, "a3.cbz"), []byte("a3"), t0)
		writeWatcherFile(t, filepath.Join(dirB, "b2.cbz"), []byte("b2"), t0)
		clk.advanceAndWait(t, 10*time.Millisecond) // t=10ms, debounce -> 60ms

		// Burst part 2 before debounce expires.
		if err := os.Rename(filepath.Join(dirA, "a1.cbz"), filepath.Join(dirA, "a1-renamed.cbz")); err != nil {
			t.Fatalf("Rename: %v", err)
		}
		if err := os.Remove(filepath.Join(dirB, "b1.cbz")); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		clk.advanceAndWait(t, 10*time.Millisecond) // t=20ms, debounce -> 70ms

		for range 4 {
			clk.advanceAndWait(t, 10*time.Millisecond) // t=30..60ms
		}
		if len(trig.snapshot()) != 0 {
			t.Fatalf("triggered before extended debounce elapsed: %d", len(trig.snapshot()))
		}

		clk.advanceAndWait(t, 10*time.Millisecond) // t=70ms -> fires once
		if got := len(trig.snapshot()); got != 1 {
			t.Fatalf("got %d triggers after burst, want 1", got)
		}

		// Subsequent unchanged ticks emit no further jobs.
		for range 4 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if got := len(trig.snapshot()); got != 1 {
			t.Fatalf("got %d triggers after quiet period, want 1", got)
		}

		// A change in a new debounce window enqueues a second automatic SCAN.
		writeWatcherFile(t, filepath.Join(dirB, "b3.cbz"), []byte("b3"), t0.Add(time.Second))
		for range 6 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if got := len(trig.snapshot()); got != 2 {
			t.Fatalf("got %d triggers after second window, want 2", got)
		}
	})
}

func TestWatcher_RealClockShortPollIntervalAndBurst(t *testing.T) {
	dir := t.TempDir()
	writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), time.Time{})
	root := makeWatcherTestRoot(t, "lib-real", "root-real", dir)

	trig := &recordedTrigger{}
	w, err := NewWatcher(WatcherOptions{
		StaticRoots: []domain.LibraryRoot{root},
		Trigger:     trig,
		MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
			return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
		}),
		PollInterval:   15 * time.Millisecond,
		DebounceWindow: 45 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	// Unchanged across several ticks.
	time.Sleep(60 * time.Millisecond)
	if got := len(trig.snapshot()); got != 0 {
		t.Fatalf("unchanged root triggered %d times, want 0", got)
	}

	writeWatcherFile(t, filepath.Join(dir, "vol02.cbz"), []byte("v2"), time.Time{})
	writeWatcherFile(t, filepath.Join(dir, "vol03.cbz"), []byte("v3"), time.Time{})

	deadline := time.Now().Add(2 * time.Second)
	for len(trig.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(80 * time.Millisecond)

	reqs := trig.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("got %d triggers, want 1", len(reqs))
	}
	if reqs[0].CreatedBy != nil || reqs[0].DedupeKey == nil || *reqs[0].DedupeKey != "SCAN:lib-real" {
		t.Fatalf("unexpected trigger request: %+v", reqs[0])
	}
}

func TestWatcher_UnreadableRootDeduplicatesLogsAndTriggersOnReturn(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), t0)
	root := makeWatcherTestRoot(t, "lib-unreadable", "root-unreadable", dir)

	var mu sync.Mutex
	unreadable := false
	pollCount := 0

	logHandler := &memoryLogHandler{}
	logger := slog.New(logHandler)
	trig := &recordedTrigger{}
	clk := newFakeWatcherClock(t0)

	w, err := NewWatcher(WatcherOptions{
		StaticRoots: []domain.LibraryRoot{root},
		Trigger:     trig,
		Clock:       clk,
		Logger:      logger,
		MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
			return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
		}),
		PollInterval:   10 * time.Millisecond,
		DebounceWindow: 20 * time.Millisecond,
		Stat: func(rawPath []byte) (os.FileInfo, error) {
			mu.Lock()
			fail := unreadable
			mu.Unlock()
			if fail && bytes.Equal(rawPath, []byte(dir)) {
				return nil, fs.ErrPermission
			}
			return os.Stat(string(rawPath))
		},
		OnPoll: func() {
			mu.Lock()
			pollCount++
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()
	clk.waitForParked(t, 1)

	// Make root unreadable across 5 poll ticks.
	mu.Lock()
	unreadable = true
	mu.Unlock()

	for range 5 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}

	mu.Lock()
	gotPolls := pollCount
	mu.Unlock()
	if gotPolls != 5 {
		t.Fatalf("pollCount = %d, want 5 while unreadable", gotPolls)
	}
	if errs := logHandler.errorCount(); errs != 1 {
		t.Fatalf("error log count = %d, want 1 (deduplicated)", errs)
	}
	if got := len(trig.snapshot()); got != 0 {
		t.Fatalf("got %d triggers while unreadable, want 0", got)
	}

	// Restore readability -> triggers again on return.
	mu.Lock()
	unreadable = false
	mu.Unlock()

	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 1 {
		t.Fatalf("got %d triggers after root returned, want 1", got)
	}

	// Unchanged after return produces no extra triggers or errors.
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 1 {
		t.Fatalf("got %d triggers on steady state after return, want 1", got)
	}
	if errs := logHandler.errorCount(); errs != 1 {
		t.Fatalf("error log count = %d, want 1", errs)
	}

	// Second outage logs once more and recovers cleanly.
	mu.Lock()
	unreadable = true
	mu.Unlock()
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if errs := logHandler.errorCount(); errs != 2 {
		t.Fatalf("error log count on second outage = %d, want 2", errs)
	}
	mu.Lock()
	unreadable = false
	mu.Unlock()
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 2 {
		t.Fatalf("got %d triggers after second recovery, want 2", got)
	}
}

func TestWatcher_WritesNothingUnderAnyRoot(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "Series")
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	writeWatcherFile(t, filepath.Join(sub, "vol01.cbz"), []byte("cbz"), t0)

	// Make directories read-only so any write attempt under the root would fail,
	// and restore write bits in cleanup so t.TempDir() can remove them.
	if err := os.Chmod(sub, 0o555); err != nil {
		t.Fatalf("Chmod sub: %v", err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("Chmod dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o755)
		_ = os.Chmod(sub, 0o755)
	})

	beforeEntries := collectTreeEntries(t, dir)

	root := makeWatcherTestRoot(t, "lib-ro", "root-ro", dir)
	trig := &recordedTrigger{}
	clk := newFakeWatcherClock(t0)

	w, err := NewWatcher(WatcherOptions{
		StaticRoots: []domain.LibraryRoot{root},
		Trigger:     trig,
		Clock:       clk,
		MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
			return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
		}),
		PollInterval:   10 * time.Millisecond,
		DebounceWindow: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	for range 5 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}

	afterEntries := collectTreeEntries(t, dir)
	if !slices.Equal(beforeEntries, afterEntries) {
		t.Fatalf("root tree mutated by watcher:\nbefore=%v\nafter=%v", beforeEntries, afterEntries)
	}
}

func collectTreeEntries(t *testing.T, root string) []string {
	t.Helper()
	var entries []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		entries = append(entries, rel+":"+info.Mode().String())
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%s): %v", root, err)
	}
	slices.Sort(entries)
	return entries
}

func TestWatcher_StopJoinsGoroutinesAndConflictIsNonFatal(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), t0)

	// Add a symlink cycle to verify polling handles cycles safely.
	if err := os.Symlink(dir, filepath.Join(dir, "cycle")); err != nil {
		t.Fatalf("Symlink cycle: %v", err)
	}

	root := makeWatcherTestRoot(t, "lib-stop", "root-stop", dir)
	osSpy := &spyOSWatcher{}
	trig := &recordedTrigger{err: app.ErrConflict}
	clk := newFakeWatcherClock(t0)
	logHandler := &memoryLogHandler{}

	w, err := NewWatcher(WatcherOptions{
		StaticRoots: []domain.LibraryRoot{root},
		Trigger:     trig,
		OSWatcher:   osSpy,
		Clock:       clk,
		Logger:      slog.New(logHandler),
		MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
			return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
		}),
		PollInterval:   10 * time.Millisecond,
		DebounceWindow: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := w.Start(t.Context()); !errors.Is(err, ErrWatcherAlreadyRunning) {
		t.Fatalf("second Start err = %v, want ErrWatcherAlreadyRunning", err)
	}
	clk.waitForParked(t, 1)

	writeWatcherFile(t, filepath.Join(dir, "vol02.cbz"), []byte("v2"), t0)
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	// ErrConflict from active RELOCATE_ROOT must not log ERROR or stop the watcher.
	if errs := logHandler.errorCount(); errs != 0 {
		t.Fatalf("ErrConflict logged %d ERRORs, want 0", errs)
	}

	w.Stop()
	w.Stop() // idempotent
	if osSpy.closed() != 1 {
		t.Fatalf("OSWatcher.Close called %d times, want 1", osSpy.closed())
	}
}

func TestWatcher_InvalidOptions(t *testing.T) {
	if _, err := NewWatcher(WatcherOptions{}); !errors.Is(err, ErrWatcherNoTrigger) {
		t.Fatalf("missing trigger err = %v, want ErrWatcherNoTrigger", err)
	}
	if _, err := NewWatcher(WatcherOptions{
		Trigger:      &recordedTrigger{},
		PollInterval: -time.Second,
	}); !errors.Is(err, ErrInvalidPollInterval) {
		t.Fatalf("negative poll interval err = %v, want ErrInvalidPollInterval", err)
	}
	if _, err := NewWatcher(WatcherOptions{
		Trigger:        &recordedTrigger{},
		DebounceWindow: -time.Second,
	}); !errors.Is(err, ErrInvalidDebounceWindow) {
		t.Fatalf("negative debounce err = %v, want ErrInvalidDebounceWindow", err)
	}

	// Verify dynamic RootLister sync drops removed roots and updates relocated roots.
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	writeWatcherFile(t, filepath.Join(dir1, "a.cbz"), []byte("a"), t0)
	writeWatcherFile(t, filepath.Join(dir2, "b.cbz"), []byte("b"), t0)

	lister := &stubRootLister{
		roots: []domain.LibraryRoot{makeWatcherTestRoot(t, "lib-dyn", "root-dyn", dir1)},
	}
	trig := &recordedTrigger{}
	clk := newFakeWatcherClock(t0)

	w, err := NewWatcher(WatcherOptions{
		Roots:   lister,
		Trigger: trig,
		Clock:   clk,
		MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
			return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
		}),
		PollInterval:   10 * time.Millisecond,
		DebounceWindow: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()
	clk.waitForParked(t, 1)

	// Relocate root path in RootLister -> re-baselines new path without spurious scan.
	lister.setRoots([]domain.LibraryRoot{makeWatcherTestRoot(t, "lib-dyn", "root-dyn", dir2)})
	clk.advanceAndWait(t, 10*time.Millisecond)
	clk.advanceAndWait(t, 10*time.Millisecond)
	clk.advanceAndWait(t, 10*time.Millisecond)
	if len(trig.snapshot()) != 0 {
		t.Fatalf("relocated path sync triggered %d scans, want 0", len(trig.snapshot()))
	}

	// Now create a file at the relocated path -> triggers 1 scan.
	writeWatcherFile(t, filepath.Join(dir2, "c.cbz"), []byte("c"), t0)
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 1 || !strings.Contains(string(trig.snapshot()[0].Payload), "lib-dyn") {
		t.Fatalf("triggers after relocate = %+v, want 1 for lib-dyn", trig.snapshot())
	}
}

type stubRootLister struct {
	mu    sync.Mutex
	roots []domain.LibraryRoot
}

func (s *stubRootLister) ListAllLibraryRoots(_ context.Context) ([]domain.LibraryRoot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.roots), nil
}

func (s *stubRootLister) setRoots(roots []domain.LibraryRoot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roots = slices.Clone(roots)
}

type scriptTriggerStep struct {
	outcome jobs.TriggerOutcome
	err     error
}

type scriptedDetailedTrigger struct {
	mu    sync.Mutex
	steps []scriptTriggerStep
	calls []jobs.TriggerRequest
}

func (s *scriptedDetailedTrigger) Trigger(ctx context.Context, req jobs.TriggerRequest) (string, error) {
	out, err := s.TriggerOutcome(ctx, req)
	return out.JobID, err
}

func (s *scriptedDetailedTrigger) TriggerOutcome(_ context.Context, req jobs.TriggerRequest) (jobs.TriggerOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, req)
	if len(s.steps) == 0 {
		return jobs.TriggerOutcome{
			JobID:   "job-default",
			Created: true,
			State:   domain.JobStateQueued,
		}, nil
	}
	step := s.steps[0]
	if len(s.steps) > 1 {
		s.steps = s.steps[1:]
	} else {
		s.steps = nil
	}
	return step.outcome, step.err
}

func (s *scriptedDetailedTrigger) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func TestWatcher_RetainsPendingUntilScanCoversObservation(t *testing.T) {
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	t.Run("retries after relocation conflict and transient error with unchanged files", func(t *testing.T) {
		dir := t.TempDir()
		writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), t0)
		root := makeWatcherTestRoot(t, "lib-retry", "root-retry", dir)

		trig := &scriptedDetailedTrigger{
			steps: []scriptTriggerStep{
				{err: app.ErrConflict},
				{err: app.ErrConflict},
				{err: errors.New("transient db error")},
				{err: errors.New("transient db error")},
				{outcome: jobs.TriggerOutcome{JobID: "job-admitted", Created: true, State: domain.JobStateQueued}},
			},
		}
		logHandler := &memoryLogHandler{}
		clk := newFakeWatcherClock(t0)

		w, err := NewWatcher(WatcherOptions{
			StaticRoots: []domain.LibraryRoot{root},
			Trigger:     trig,
			Clock:       clk,
			Logger:      slog.New(logHandler),
			MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}),
			PollInterval:   10 * time.Millisecond,
			DebounceWindow: 20 * time.Millisecond,
			RetryInterval:  30 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		if err := w.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer w.Stop()
		clk.waitForParked(t, 1)

		// Single file change; files stay untouched after this point.
		writeWatcherFile(t, filepath.Join(dir, "vol02.cbz"), []byte("v2"), t0)
		clk.advanceAndWait(t, 10*time.Millisecond) // t=10ms: poll detects change, debounce -> 30ms
		clk.advanceAndWait(t, 10*time.Millisecond) // t=20ms: poll unchanged
		clk.advanceAndWait(t, 10*time.Millisecond) // t=30ms: attempt 1 -> ErrConflict, retry -> 60ms

		if trig.callCount() != 1 {
			t.Fatalf("callCount after debounce = %d, want 1", trig.callCount())
		}

		// Retry 2 at t=60ms -> ErrConflict, retry -> 90ms.
		for range 3 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if trig.callCount() != 2 {
			t.Fatalf("callCount at t=60ms = %d, want 2", trig.callCount())
		}

		// Retry 3 at t=90ms -> transient error (logged once), retry -> 120ms.
		for range 3 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if trig.callCount() != 3 {
			t.Fatalf("callCount at t=90ms = %d, want 3", trig.callCount())
		}

		// Retry 4 at t=120ms -> second transient error (deduplicated log), retry -> 150ms.
		for range 3 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if trig.callCount() != 4 {
			t.Fatalf("callCount at t=120ms = %d, want 4", trig.callCount())
		}
		if errs := logHandler.errorCount(); errs != 1 {
			t.Fatalf("error log count = %d, want 1 (deduplicated)", errs)
		}

		// Retry 5 at t=150ms -> admitted!
		for range 3 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if trig.callCount() != 5 {
			t.Fatalf("callCount at t=150ms = %d, want 5", trig.callCount())
		}

		// No more retries once admitted.
		for range 6 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if trig.callCount() != 5 {
			t.Fatalf("callCount after admission = %d, want 5", trig.callCount())
		}
	})

	t.Run("retains pending when deduplicated to RUNNING scan and clears on QUEUED", func(t *testing.T) {
		dir := t.TempDir()
		writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), t0)
		root := makeWatcherTestRoot(t, "lib-dedupe", "root-dedupe", dir)

		trig := &scriptedDetailedTrigger{
			steps: []scriptTriggerStep{
				// First attempt deduplicates to an older RUNNING scan.
				{outcome: jobs.TriggerOutcome{JobID: "scan-old-running", Created: false, State: domain.JobStateRunning}},
				// Retry finds existing QUEUED scan (not yet started) -> covers observation!
				{outcome: jobs.TriggerOutcome{JobID: "scan-queued", Created: false, State: domain.JobStateQueued}},
			},
		}
		clk := newFakeWatcherClock(t0)

		w, err := NewWatcher(WatcherOptions{
			StaticRoots: []domain.LibraryRoot{root},
			Trigger:     trig,
			Clock:       clk,
			MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}),
			PollInterval:   10 * time.Millisecond,
			DebounceWindow: 20 * time.Millisecond,
			RetryInterval:  20 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		if err := w.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer w.Stop()
		clk.waitForParked(t, 1)

		writeWatcherFile(t, filepath.Join(dir, "vol02.cbz"), []byte("v2"), t0)
		for range 3 {
			clk.advanceAndWait(t, 10*time.Millisecond) // t=30ms: attempt 1 -> RUNNING
		}
		if trig.callCount() != 1 {
			t.Fatalf("callCount after RUNNING deduplication = %d, want 1", trig.callCount())
		}

		// Retry at t=50ms -> QUEUED -> admitted and cleared.
		for range 2 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if trig.callCount() != 2 {
			t.Fatalf("callCount after retry = %d, want 2", trig.callCount())
		}

		// Further unchanged ticks do not re-trigger.
		for range 4 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if trig.callCount() != 2 {
			t.Fatalf("callCount after QUEUED admission = %d, want 2", trig.callCount())
		}
	})

	t.Run("cancels pending retry when library is removed", func(t *testing.T) {
		dir := t.TempDir()
		writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), t0)
		root := makeWatcherTestRoot(t, "lib-removed", "root-removed", dir)

		lister := &stubRootLister{roots: []domain.LibraryRoot{root}}
		trig := &scriptedDetailedTrigger{
			steps: []scriptTriggerStep{
				{err: app.ErrConflict},
				{outcome: jobs.TriggerOutcome{JobID: "should-not-run", Created: true, State: domain.JobStateQueued}},
			},
		}
		clk := newFakeWatcherClock(t0)

		w, err := NewWatcher(WatcherOptions{
			Roots:   lister,
			Trigger: trig,
			Clock:   clk,
			MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}),
			PollInterval:   10 * time.Millisecond,
			DebounceWindow: 20 * time.Millisecond,
			RetryInterval:  30 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		if err := w.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer w.Stop()
		clk.waitForParked(t, 1)

		writeWatcherFile(t, filepath.Join(dir, "vol02.cbz"), []byte("v2"), t0)
		for range 3 {
			clk.advanceAndWait(t, 10*time.Millisecond) // t=30ms: attempt 1 -> ErrConflict, retry -> 60ms
		}
		if trig.callCount() != 1 {
			t.Fatalf("callCount = %d, want 1", trig.callCount())
		}

		// Remove the library before retry fires.
		lister.setRoots(nil)
		for range 6 {
			clk.advanceAndWait(t, 10*time.Millisecond)
		}
		if trig.callCount() != 1 {
			t.Fatalf("callCount after library removal = %d, want 1 (pending retry cancelled)", trig.callCount())
		}
	})
}

func TestWatcher_RootMarkerAddRemoveAndValidity(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatalf("MkdirAll sub: %v", err)
	}
	root := makeWatcherTestRoot(t, "lib-marker", "root-marker", dir)

	trig := &recordedTrigger{}
	clk := newFakeWatcherClock(t0)

	w, err := NewWatcher(WatcherOptions{
		StaticRoots: []domain.LibraryRoot{root},
		Trigger:     trig,
		Clock:       clk,
		MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
			return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
		}),
		PollInterval:   10 * time.Millisecond,
		DebounceWindow: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()
	clk.waitForParked(t, 1)

	// Empty root without marker emits no scans.
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 0 {
		t.Fatalf("empty root triggered %d scans, want 0", got)
	}

	// Subdirectory marker is ignored (root-level only).
	writeWatcherFile(t, filepath.Join(dir, "sub", RootMarkerFilename), nil, t0)
	for range 4 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 0 {
		t.Fatalf("subdirectory marker triggered %d scans, want 0", got)
	}

	// Add root-level regular file .tobiten-root -> triggers 1 SCAN.
	markerPath := filepath.Join(dir, RootMarkerFilename)
	writeWatcherFile(t, markerPath, nil, t0)
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 1 {
		t.Fatalf("adding root marker triggered %d scans, want 1", got)
	}

	// Steady state with marker emits no further scans.
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 1 {
		t.Fatalf("steady root with marker triggered %d scans, want 1", got)
	}

	// Remove root-level marker -> triggers 1 SCAN.
	if err := os.Remove(markerPath); err != nil {
		t.Fatalf("Remove marker: %v", err)
	}
	for range 3 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 2 {
		t.Fatalf("removing root marker triggered %d scans, want 2", got)
	}

	// Directory named .tobiten-root is not a valid marker -> triggers 0 scans.
	if err := os.Mkdir(markerPath, 0o755); err != nil {
		t.Fatalf("Mkdir marker: %v", err)
	}
	for range 4 {
		clk.advanceAndWait(t, 10*time.Millisecond)
	}
	if got := len(trig.snapshot()); got != 2 {
		t.Fatalf("directory marker triggered %d scans, want 2 total", got)
	}
}

func TestWatcher_StartDoesNotBlockOnMountIO(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), t0)
	root := makeWatcherTestRoot(t, "lib-nonblock", "root-nonblock", dir)

	readEntered := make(chan struct{})
	releaseRead := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseRead) })
	}
	t.Cleanup(release)
	var readOnce sync.Once
	var mu sync.Mutex
	rootStatCalls := 0

	trig := &recordedTrigger{}
	clk := newFakeWatcherClock(t0)

	w, err := NewWatcher(WatcherOptions{
		StaticRoots: []domain.LibraryRoot{root},
		Trigger:     trig,
		Clock:       clk,
		MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
			return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
		}),
		PollInterval:   10 * time.Millisecond,
		DebounceWindow: 20 * time.Millisecond,
		Stat: func(rawPath []byte) (os.FileInfo, error) {
			if bytes.Equal(rawPath, []byte(dir)) {
				mu.Lock()
				rootStatCalls++
				mu.Unlock()
				readOnce.Do(func() {
					close(readEntered)
					select {
					case <-releaseRead:
					case <-t.Context().Done():
					}
				})
			}
			return os.Stat(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}

	// Start must return immediately even while the initial root filesystem read is blocked.
	startDone := make(chan error, 1)
	go func() {
		startDone <- w.Start(t.Context())
	}()

	select {
	case err := <-startDone:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(time.Second):
		release()
		t.Fatal("Start blocked on mount I/O")
	}
	defer w.Stop()

	// Wait until the background goroutine reaches the initial filesystem read, then release it.
	select {
	case <-readEntered:
	case <-time.After(time.Second):
		release()
		t.Fatal("background baseline did not start")
	}
	release()

	// Wait for initial baseline pass to park on the first timer.
	clk.waitForParked(t, 1)

	mu.Lock()
	callsAfterStart := rootStatCalls
	mu.Unlock()
	if callsAfterStart != 1 {
		t.Fatalf("initial baseline walked root %d times, want 1", callsAfterStart)
	}
	if got := len(trig.snapshot()); got != 0 {
		t.Fatalf("initial baseline triggered %d scans, want 0", got)
	}
}

func TestWatcher_StopCancelsDuringMountProbeAndSkipsSecondRoot(t *testing.T) {
	t.Run("Stop cancels watcher context during blocked first probe and skips second root", func(t *testing.T) {
		dir1 := t.TempDir()
		dir2 := t.TempDir()
		r1 := makeWatcherTestRoot(t, "lib-1", "root-1", dir1)
		r2 := makeWatcherTestRoot(t, "lib-2", "root-2", dir2)

		probeEntered := make(chan struct{})
		releaseProbe := make(chan struct{})
		var releaseOnce sync.Once
		release := func() {
			releaseOnce.Do(func() { close(releaseProbe) })
		}
		t.Cleanup(release)

		var probeOnce sync.Once
		var mu sync.Mutex
		probeCalls := 0

		trig := &recordedTrigger{}
		clk := newFakeWatcherClock(time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC))

		w, err := NewWatcher(WatcherOptions{
			StaticRoots: []domain.LibraryRoot{r1, r2},
			Trigger:     trig,
			Clock:       clk,
			MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				mu.Lock()
				probeCalls++
				mu.Unlock()
				probeOnce.Do(func() {
					close(probeEntered)
					select {
					case <-releaseProbe:
					case <-t.Context().Done():
					}
				})
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}),
			PollInterval:   10 * time.Millisecond,
			DebounceWindow: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}
		if err := w.Start(t.Context()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		select {
		case <-probeEntered:
		case <-time.After(time.Second):
			t.Fatal("watcher did not enter first mount probe")
		}

		// Stop cancels the watcher context even while the first mount probe is blocked.
		stopDone := make(chan struct{})
		go func() {
			w.Stop()
			close(stopDone)
		}()

		// Wait until Stop has acquired lifecycleMu, canceled runCtx, and marked w.running=false
		// while the first mount probe is still blocked.
		cancelDeadline := time.Now().Add(time.Second)
		for {
			w.lifecycleMu.Lock()
			stillRunning := w.running
			w.lifecycleMu.Unlock()
			if !stillRunning {
				break
			}
			if time.Now().After(cancelDeadline) {
				release()
				t.Fatal("Stop blocked on mount probe before canceling watcher context")
			}
			time.Sleep(time.Millisecond)
		}

		select {
		case <-stopDone:
			t.Fatal("Stop returned before blocked first mount probe finished")
		default:
		}

		release()

		select {
		case <-stopDone:
		case <-time.After(time.Second):
			t.Fatal("Stop did not finish after releasing first mount probe")
		}

		mu.Lock()
		calls := probeCalls
		mu.Unlock()
		if calls != 1 {
			t.Fatalf("mount probe calls = %d, want 1 (second root must not be probed after shutdown)", calls)
		}
	})

	t.Run("parent context cancellation during first probe skips second root", func(t *testing.T) {
		dir1 := t.TempDir()
		dir2 := t.TempDir()
		r1 := makeWatcherTestRoot(t, "lib-1", "root-1", dir1)
		r2 := makeWatcherTestRoot(t, "lib-2", "root-2", dir2)

		probeEntered := make(chan struct{})
		releaseProbe := make(chan struct{})
		var releaseOnce sync.Once
		release := func() {
			releaseOnce.Do(func() { close(releaseProbe) })
		}
		t.Cleanup(release)

		var probeOnce sync.Once
		var mu sync.Mutex
		probeCalls := 0

		trig := &recordedTrigger{}
		clk := newFakeWatcherClock(time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC))

		w, err := NewWatcher(WatcherOptions{
			StaticRoots: []domain.LibraryRoot{r1, r2},
			Trigger:     trig,
			Clock:       clk,
			MountDetector: MountDetectorFunc(func([]byte) (mount.FilesystemInfo, error) {
				mu.Lock()
				probeCalls++
				mu.Unlock()
				probeOnce.Do(func() {
					close(probeEntered)
					select {
					case <-releaseProbe:
					case <-t.Context().Done():
					}
				})
				return mount.FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
			}),
			PollInterval:   10 * time.Millisecond,
			DebounceWindow: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewWatcher: %v", err)
		}

		parentCtx, cancelParent := context.WithCancel(t.Context())
		defer cancelParent()
		if err := w.Start(parentCtx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		select {
		case <-probeEntered:
		case <-time.After(time.Second):
			t.Fatal("watcher did not enter first mount probe")
		}

		// Cancel parent context while the first probe is blocked, then release it.
		cancelParent()
		release()
		w.Stop()

		mu.Lock()
		calls := probeCalls
		mu.Unlock()
		if calls != 1 {
			t.Fatalf("mount probe calls = %d, want 1 (second root must not be probed after parent context cancellation)", calls)
		}
	})
}

func (h *memoryLogHandler) snapshotRecords() []capturedLogRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.records)
}

func (h *memoryLogHandler) hasWarnMessage(substr string) bool {
	for _, rec := range h.snapshotRecords() {
		if rec.Level == slog.LevelWarn && strings.Contains(rec.Message, substr) {
			return true
		}
	}
	return false
}

func waitForOSWatchedRoot(t *testing.T, w *Watcher, rootID string) RootWatchStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if st, ok := w.RootStatus(rootID); ok && st.Mode == WatchModeOS {
			return st
		}
		if time.Now().After(deadline) {
			st, ok := w.RootStatus(rootID)
			t.Fatalf("timed out waiting for root %q mode=os (got ok=%v status=%+v)", rootID, ok, st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForRootRemoved(t *testing.T, w *Watcher, rootID string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := w.RootStatus(rootID); !ok && len(w.RootWatchedPaths(rootID)) == 0 {
			return
		}
		if time.Now().After(deadline) {
			st, ok := w.RootStatus(rootID)
			t.Fatalf("timed out waiting for root %q removal (ok=%v status=%+v watched=%v)", rootID, ok, st, w.RootWatchedPaths(rootID))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForTriggerCount(t *testing.T, trig *recordedTrigger, want int) []jobs.TriggerRequest {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		reqs := trig.snapshot()
		if len(reqs) >= want {
			return reqs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d triggered SCAN jobs, got %d", want, len(reqs))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForWatchedPathsContain(t *testing.T, w *Watcher, wantPaths ...string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		watched := w.WatchedPaths()
		allFound := true
		for _, p := range wantPaths {
			if !slices.Contains(watched, p) {
				allFound = false
				break
			}
		}
		if allFound {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for WatchedPaths to contain %v; got %v", wantPaths, watched)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertNoWatcherGoroutinesRunning(t *testing.T, baselineGoroutines int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		stacks := string(buf[:n])
		hasLeak := strings.Contains(stacks, "(*Watcher).runLoop") ||
			strings.Contains(stacks, "(*fsnotifyOSWatcher).readLoop") ||
			strings.Contains(stacks, "fsnotify.(*inotify).readEvents") ||
			strings.Contains(stacks, "fsnotify.(*kqueue).readEvents")
		cur := runtime.NumGoroutine()
		if !hasLeak && cur <= baselineGoroutines {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("watcher goroutines remained after Stop (goroutines=%d baseline=%d hasLeak=%v):\n%s",
				cur, baselineGoroutines, hasLeak, stacks)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertAutomaticScanRequest(t *testing.T, req jobs.TriggerRequest, wantLibID string) {
	t.Helper()
	if req.Kind != domain.JobKindScan {
		t.Fatalf("Kind = %s, want SCAN", req.Kind)
	}
	if req.CreatedBy != nil {
		t.Fatalf("CreatedBy = %v, want nil (automatic job)", *req.CreatedBy)
	}
	wantDedupe := "SCAN:" + wantLibID
	if req.DedupeKey == nil || *req.DedupeKey != wantDedupe {
		t.Fatalf("DedupeKey = %v, want %s", req.DedupeKey, wantDedupe)
	}
	var payload ScanJobPayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil || payload.LibraryID != wantLibID {
		t.Fatalf("Payload = %s (%v), want library_id=%s", req.Payload, err, wantLibID)
	}
}

func TestWatcher_LocalFilesystemUsesFSNotifyForExistingAndNewSubdirectories(t *testing.T) {
	dir := t.TempDir()
	existingSub := filepath.Join(dir, "existing")
	writeWatcherFile(t, filepath.Join(existingSub, "vol01.cbz"), []byte("v1"), time.Time{})
	// Hidden and temporary directories must never be watched.
	writeWatcherFile(t, filepath.Join(dir, ".hidden", "ignored.cbz"), []byte("hidden"), time.Time{})
	writeWatcherFile(t, filepath.Join(dir, ".tobiten-tmp-1", "temp.cbz"), []byte("temp"), time.Time{})

	root := makeWatcherTestRoot(t, "lib-local", "root-local", dir)
	trig := &recordedTrigger{}
	var pollTicks atomic.Int32

	w, err := NewWatcher(WatcherOptions{
		StaticRoots:    []domain.LibraryRoot{root},
		Trigger:        trig,
		PollInterval:   time.Hour,
		DebounceWindow: 35 * time.Millisecond,
		OnPoll: func() {
			pollTicks.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	st := waitForOSWatchedRoot(t, w, root.ID)
	if st.IsNetwork {
		t.Fatalf("RootWatchStatus.IsNetwork = true for local root: %+v", st)
	}

	// Verify only directories (and neither regular files nor hidden/temp dirs) are watched.
	wantInitialWatches := []string{dir, existingSub}
	slices.Sort(wantInitialWatches)
	if got := w.WatchedPaths(); !slices.Equal(got, wantInitialWatches) {
		t.Fatalf("initial WatchedPaths = %v, want %v (directories only)", got, wantInitialWatches)
	}

	// Quiet baseline produces zero triggers.
	time.Sleep(60 * time.Millisecond)
	if got := len(trig.snapshot()); got != 0 {
		t.Fatalf("quiet baseline triggered %d scans, want 0", got)
	}

	// 1. Existing subdirectory: create, rename, delete without waiting for a poll tick.
	createdExisting := filepath.Join(existingSub, "vol02.cbz")
	renamedExisting := filepath.Join(existingSub, "vol02-renamed.cbz")

	writeWatcherFile(t, createdExisting, []byte("v2"), time.Time{})
	reqs := waitForTriggerCount(t, trig, 1)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 1 {
		t.Fatalf("create in existing subdir triggered %d scans, want 1", got)
	}
	assertAutomaticScanRequest(t, reqs[0], "lib-local")

	if err := os.Rename(createdExisting, renamedExisting); err != nil {
		t.Fatalf("Rename in existing subdir: %v", err)
	}
	reqs = waitForTriggerCount(t, trig, 2)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 2 {
		t.Fatalf("rename in existing subdir triggered %d total scans, want 2", got)
	}
	assertAutomaticScanRequest(t, reqs[1], "lib-local")

	if err := os.Remove(renamedExisting); err != nil {
		t.Fatalf("Remove in existing subdir: %v", err)
	}
	reqs = waitForTriggerCount(t, trig, 3)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 3 {
		t.Fatalf("delete in existing subdir triggered %d total scans, want 3", got)
	}
	assertAutomaticScanRequest(t, reqs[2], "lib-local")

	// 2. Newly created subdirectory: create directory + file, verify new directories are watched,
	// then rename and delete the file inside the newly created subdirectory.
	newSub := filepath.Join(dir, "newseries")
	nestedSub := filepath.Join(newSub, "arc1")
	createdNew := filepath.Join(nestedSub, "vol03.cbz")
	renamedNew := filepath.Join(nestedSub, "vol03-renamed.cbz")

	writeWatcherFile(t, createdNew, []byte("v3"), time.Time{})
	reqs = waitForTriggerCount(t, trig, 4)
	waitForWatchedPathsContain(t, w, newSub, nestedSub)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 4 {
		t.Fatalf("create in newly created subdir triggered %d total scans, want 4", got)
	}
	assertAutomaticScanRequest(t, reqs[3], "lib-local")
	if slices.Contains(w.WatchedPaths(), createdNew) {
		t.Fatalf("WatchedPaths contains regular file %q; must watch directories only", createdNew)
	}

	if err := os.Rename(createdNew, renamedNew); err != nil {
		t.Fatalf("Rename in newly created subdir: %v", err)
	}
	reqs = waitForTriggerCount(t, trig, 5)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 5 {
		t.Fatalf("rename in newly created subdir triggered %d total scans, want 5", got)
	}
	assertAutomaticScanRequest(t, reqs[4], "lib-local")

	if err := os.Remove(renamedNew); err != nil {
		t.Fatalf("Remove in newly created subdir: %v", err)
	}
	reqs = waitForTriggerCount(t, trig, 6)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 6 {
		t.Fatalf("delete in newly created subdir triggered %d total scans, want 6", got)
	}
	assertAutomaticScanRequest(t, reqs[5], "lib-local")

	// 3. Also verify creating an empty subdirectory first adds a watch on it, and a later file
	// creation inside that subdirectory triggers one debounced automatic SCAN.
	emptyThenPopulated := filepath.Join(dir, "later-sub")
	if err := os.Mkdir(emptyThenPopulated, 0o755); err != nil {
		t.Fatalf("Mkdir(%s): %v", emptyThenPopulated, err)
	}
	waitForWatchedPathsContain(t, w, emptyThenPopulated)
	writeWatcherFile(t, filepath.Join(emptyThenPopulated, "vol04.cbz"), []byte("v4"), time.Time{})
	waitForTriggerCount(t, trig, 7)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 7 {
		t.Fatalf("create in pre-created new subdir triggered %d total scans, want 7", got)
	}

	// 4. Hidden and temporary files inside watched directories are ignored, while a burst
	// of multiple file changes coalesces into a single debounced automatic SCAN.
	writeWatcherFile(t, filepath.Join(existingSub, ".DS_Store"), []byte("meta"), time.Time{})
	writeWatcherFile(t, filepath.Join(nestedSub, "downloading.cbz.part"), []byte("part"), time.Time{})
	time.Sleep(70 * time.Millisecond)
	if got := len(trig.snapshot()); got != 7 {
		t.Fatalf("hidden/temp files triggered scan: got %d, want 7", got)
	}

	writeWatcherFile(t, filepath.Join(existingSub, "burst1.cbz"), []byte("b1"), time.Time{})
	writeWatcherFile(t, filepath.Join(nestedSub, "burst2.cbz"), []byte("b2"), time.Time{})
	writeWatcherFile(t, filepath.Join(emptyThenPopulated, "burst3.cbz"), []byte("b3"), time.Time{})
	waitForTriggerCount(t, trig, 8)
	time.Sleep(70 * time.Millisecond)
	if got := len(trig.snapshot()); got != 8 {
		t.Fatalf("burst across subdirectories triggered %d total scans, want 8", got)
	}

	// 5. Followed file symlink pointing outside the root (ADR-0003 §5, M1B-D6): creating the
	// symlink triggers a scan, and editing the external target file directly through its
	// external path triggers a follow-up scan on both Linux and Darwin without a poll tick.
	externalDir := t.TempDir()
	externalTarget := filepath.Join(externalDir, "external-target.cbz")
	writeWatcherFile(t, externalTarget, []byte("ext-v1"), time.Time{})
	linkInRoot := filepath.Join(existingSub, "linked-external.cbz")
	if err := os.Symlink(externalTarget, linkInRoot); err != nil {
		t.Fatalf("Symlink(%s -> %s): %v", linkInRoot, externalTarget, err)
	}
	waitForTriggerCount(t, trig, 9)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 9 {
		t.Fatalf("creating file symlink triggered %d total scans, want 9", got)
	}

	// Edit the symlink's target through its external path outside the watched root.
	writeWatcherFile(t, externalTarget, []byte("ext-v2-modified-directly"), time.Time{})
	reqs = waitForTriggerCount(t, trig, 10)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 10 {
		t.Fatalf("editing external target of followed file symlink triggered %d total scans, want 10", got)
	}
	assertAutomaticScanRequest(t, reqs[9], "lib-local")

	if gotPolls := pollTicks.Load(); gotPolls != 0 {
		t.Fatalf("pollTicks = %d, want 0 (all triggers must come from fsnotify without waiting for a poll tick)", gotPolls)
	}
}

type faultInjectingFSNotifyHandle struct {
	inner       fsnotifyHandle
	eventsCh    chan fsnotify.Event
	errorsCh    chan error
	stopCh      chan struct{}
	closeOnce   sync.Once
	wg          sync.WaitGroup
	addHook     func(inner fsnotifyHandle, name string) error
	postAddHook func(name string, err error)

	dropMu    sync.Mutex
	dropEvent func(fsnotify.Event) bool
}

func newFaultInjectingFSNotifyHandle(addHook func(inner fsnotifyHandle, name string) error) (*faultInjectingFSNotifyHandle, error) {
	inner, err := newRealFSNotifyHandle()
	if err != nil {
		return nil, err
	}
	h := &faultInjectingFSNotifyHandle{
		inner:    inner,
		eventsCh: make(chan fsnotify.Event, 16),
		errorsCh: make(chan error, 16),
		stopCh:   make(chan struct{}),
		addHook:  addHook,
	}
	h.wg.Add(1)
	go h.forward()
	return h, nil
}

func (h *faultInjectingFSNotifyHandle) setDropEvent(fn func(fsnotify.Event) bool) {
	h.dropMu.Lock()
	h.dropEvent = fn
	h.dropMu.Unlock()
}

func (h *faultInjectingFSNotifyHandle) shouldDrop(ev fsnotify.Event) bool {
	h.dropMu.Lock()
	fn := h.dropEvent
	h.dropMu.Unlock()
	return fn != nil && fn(ev)
}

func (h *faultInjectingFSNotifyHandle) forward() {
	defer h.wg.Done()
	defer close(h.eventsCh)
	defer close(h.errorsCh)

	inEvents := h.inner.Events()
	inErrors := h.inner.Errors()
	for inEvents != nil || inErrors != nil {
		select {
		case <-h.stopCh:
			return
		case ev, ok := <-inEvents:
			if !ok {
				inEvents = nil
				continue
			}
			if h.shouldDrop(ev) {
				continue
			}
			select {
			case h.eventsCh <- ev:
			case <-h.stopCh:
				return
			}
		case err, ok := <-inErrors:
			if !ok {
				inErrors = nil
				continue
			}
			select {
			case h.errorsCh <- err:
			case <-h.stopCh:
				return
			}
		}
	}
}

func (h *faultInjectingFSNotifyHandle) Add(name string) error {
	if h.addHook != nil {
		if err := h.addHook(h.inner, name); err != nil {
			return err
		}
	}
	err := h.inner.Add(name)
	if h.postAddHook != nil {
		h.postAddHook(name, err)
	}
	return err
}

func (h *faultInjectingFSNotifyHandle) Remove(name string) error      { return h.inner.Remove(name) }
func (h *faultInjectingFSNotifyHandle) WatchList() []string           { return h.inner.WatchList() }
func (h *faultInjectingFSNotifyHandle) Events() <-chan fsnotify.Event { return h.eventsCh }
func (h *faultInjectingFSNotifyHandle) Errors() <-chan error          { return h.errorsCh }

func (h *faultInjectingFSNotifyHandle) Close() error {
	var err error
	h.closeOnce.Do(func() {
		close(h.stopCh)
		err = h.inner.Close()
		h.wg.Wait()
	})
	return err
}

func (h *faultInjectingFSNotifyHandle) injectError(err error) {
	select {
	case h.errorsCh <- err:
	case <-h.stopCh:
	}
}

func TestWatcher_FSNotifyWatchErrorAndOverflowTriggerOneFullScan(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "watched-root")
	writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), time.Time{})

	// Also seed a followed file symlink present before startup and a dangling symlink whose
	// external target does not exist yet at startup so both direct external target edits and
	// scheduled polling for temporarily missing symlink targets are exercised.
	extDir := t.TempDir()
	extTarget := filepath.Join(extDir, "seeded-external.cbz")
	writeWatcherFile(t, extTarget, []byte("seeded-ext-1"), time.Time{})
	if err := os.Symlink(extTarget, filepath.Join(dir, "seeded-link.cbz")); err != nil {
		t.Fatalf("Symlink seeded: %v", err)
	}
	danglingTarget := filepath.Join(extDir, "dangling-external.cbz")
	if err := os.Symlink(danglingTarget, filepath.Join(dir, "dangling-link.cbz")); err != nil {
		t.Fatalf("Symlink dangling: %v", err)
	}

	lostOverflowSubDir := filepath.Join(dir, "lost-overflow-sub")
	failSubDir := filepath.Join(dir, "unwatchable-new-sub")

	var handleMu sync.Mutex
	var activeHandle *faultInjectingFSNotifyHandle

	root := makeWatcherTestRoot(t, "lib-overflow", "root-overflow", dir)
	trig := &recordedTrigger{}
	logHandler := &memoryLogHandler{}

	w, err := NewWatcher(WatcherOptions{
		StaticRoots:    []domain.LibraryRoot{root},
		Trigger:        trig,
		Logger:         slog.New(logHandler),
		PollInterval:   25 * time.Millisecond,
		DebounceWindow: 35 * time.Millisecond,
		newFSNotify: func() (fsnotifyHandle, error) {
			h, err := newFaultInjectingFSNotifyHandle(func(inner fsnotifyHandle, name string) error {
				if name == failSubDir {
					// Register in inner first before returning ENOSPC to verify partial runtime Add cleanup.
					_ = inner.Add(name)
					return syscall.ENOSPC
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			handleMu.Lock()
			activeHandle = h
			handleMu.Unlock()
			return h, nil
		},
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	waitForOSWatchedRoot(t, w, root.ID)

	handleMu.Lock()
	h := activeHandle
	handleMu.Unlock()
	if h == nil {
		t.Fatal("expected active fsnotify handle")
	}

	// 0. Editing the external target of the pre-existing file symlink triggers a scan,
	// and later creating the external target of the startup-dangling symlink triggers a
	// scan on the scheduled poll tick without touching any entry inside the watched root.
	writeWatcherFile(t, extTarget, []byte("seeded-ext-2-edited"), time.Time{})
	reqs := waitForTriggerCount(t, trig, 1)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 1 {
		t.Fatalf("editing seeded symlink target triggered %d scans, want 1", got)
	}
	assertAutomaticScanRequest(t, reqs[0], "lib-overflow")

	writeWatcherFile(t, danglingTarget, []byte("dangling-target-appeared"), time.Time{})
	reqs = waitForTriggerCount(t, trig, 2)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 2 {
		t.Fatalf("appearing target for startup-dangling symlink triggered %d total scans, want 2", got)
	}
	assertAutomaticScanRequest(t, reqs[1], "lib-overflow")

	// 1. Event-queue overflow drops a directory-create event: overflow triggers one debounced
	// full scan AND repairs directory watches so a later independent edit in the new directory triggers.
	h.setDropEvent(func(ev fsnotify.Event) bool {
		return ev.Name == lostOverflowSubDir || strings.HasPrefix(ev.Name, lostOverflowSubDir+"/")
	})
	if err := os.Mkdir(lostOverflowSubDir, 0o755); err != nil {
		t.Fatalf("Mkdir(%s): %v", lostOverflowSubDir, err)
	}
	time.Sleep(25 * time.Millisecond)
	h.setDropEvent(nil)

	h.injectError(fsnotify.ErrEventOverflow)
	h.injectError(fsnotify.ErrEventOverflow)
	reqs = waitForTriggerCount(t, trig, 3)
	waitForWatchedPathsContain(t, w, lostOverflowSubDir)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 3 {
		t.Fatalf("event-queue overflow burst triggered %d total scans, want 3", got)
	}
	assertAutomaticScanRequest(t, reqs[2], "lib-overflow")
	if !logHandler.hasWarnMessage("fsnotify event queue overflow") {
		t.Fatalf("expected overflow warning log, got records=%+v", logHandler.snapshotRecords())
	}

	// Later independent file edit inside lostOverflowSubDir must trigger a second scan.
	writeWatcherFile(t, filepath.Join(lostOverflowSubDir, "after-overflow.cbz"), []byte("ao"), time.Time{})
	reqs = waitForTriggerCount(t, trig, 4)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 4 {
		t.Fatalf("edit in overflow-recovered subdir triggered %d total scans, want 4", got)
	}
	assertAutomaticScanRequest(t, reqs[3], "lib-overflow")

	// 2. Watch error on Errors channel triggers one full scan.
	h.injectError(errors.New("kernel watch descriptor failure"))
	reqs = waitForTriggerCount(t, trig, 5)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 5 {
		t.Fatalf("watch error triggered %d total scans, want 5", got)
	}
	assertAutomaticScanRequest(t, reqs[4], "lib-overflow")
	if !logHandler.hasWarnMessage("fsnotify watch error") {
		t.Fatalf("expected watch error warning log, got records=%+v", logHandler.snapshotRecords())
	}

	// 3. Root replacement (rename away + recreate at configured root path): queues one debounced
	// scan for the replacement incident and repairs watches so a later independent edit triggers.
	movedRoot := filepath.Join(t.TempDir(), "moved-root")
	replacedSub := filepath.Join(dir, "replaced-sub")
	if err := os.Rename(dir, movedRoot); err != nil {
		t.Fatalf("Rename root: %v", err)
	}
	if err := os.MkdirAll(replacedSub, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", replacedSub, err)
	}
	reqs = waitForTriggerCount(t, trig, 6)
	waitForWatchedPathsContain(t, w, dir, replacedSub)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 6 {
		t.Fatalf("root replacement triggered %d total scans, want 6", got)
	}
	assertAutomaticScanRequest(t, reqs[5], "lib-overflow")

	// Later independent file edit inside the replaced root must trigger a scan.
	writeWatcherFile(t, filepath.Join(replacedSub, "after-replace.cbz"), []byte("ar"), time.Time{})
	reqs = waitForTriggerCount(t, trig, 7)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 7 {
		t.Fatalf("edit after root replacement triggered %d total scans, want 7", got)
	}
	assertAutomaticScanRequest(t, reqs[6], "lib-overflow")

	// 4. Runtime registration failure on a new subdirectory: triggers one debounced scan for
	// the incident, releases OS watches, and falls back to polling with a diagnostic so a
	// later independent file edit in the unwatchable subdirectory triggers a scan.
	if err := os.Mkdir(failSubDir, 0o755); err != nil {
		t.Fatalf("Mkdir(%s): %v", failSubDir, err)
	}
	reqs = waitForTriggerCount(t, trig, 8)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 8 {
		t.Fatalf("subdirectory watch error triggered %d total scans, want 8", got)
	}
	assertAutomaticScanRequest(t, reqs[7], "lib-overflow")
	if !logHandler.hasWarnMessage("failed to add OS watch for new subdirectory") {
		t.Fatalf("expected subdirectory watch failure warning log, got records=%+v", logHandler.snapshotRecords())
	}
	if !logHandler.hasWarnMessage("failed to register OS watch; falling back to polling") {
		t.Fatalf("expected polling fallback diagnostic log after runtime watch failure, got records=%+v", logHandler.snapshotRecords())
	}
	stAfterFallback, ok := w.RootStatus(root.ID)
	if !ok || stAfterFallback.Mode != WatchModePoll {
		t.Fatalf("root status after runtime watch failure = %+v (ok=%v), want Mode=poll", stAfterFallback, ok)
	}
	if got := w.WatchedPaths(); len(got) != 0 {
		t.Fatalf("WatchedPaths after runtime fallback = %v, want empty", got)
	}

	// Later independent file edit inside failSubDir must trigger a scan via polling fallback.
	writeWatcherFile(t, filepath.Join(failSubDir, "after-fallback.cbz"), []byte("af"), time.Time{})
	reqs = waitForTriggerCount(t, trig, 9)
	time.Sleep(60 * time.Millisecond)
	if got := len(trig.snapshot()); got != 9 {
		t.Fatalf("edit after runtime fallback to polling triggered %d total scans, want 9", got)
	}
	assertAutomaticScanRequest(t, reqs[8], "lib-overflow")
}

func TestWatcher_UnwatchableLocalRootFallsBackToPollingWithDiagnostic(t *testing.T) {
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	fallbackDir := t.TempDir()
	fallbackSub := filepath.Join(fallbackDir, "sub")
	writeWatcherFile(t, filepath.Join(fallbackSub, "vol01.cbz"), []byte("v1"), t0)

	healthyDir := t.TempDir()
	healthySub := filepath.Join(healthyDir, "sub")
	writeWatcherFile(t, filepath.Join(healthySub, "vol01.cbz"), []byte("v1"), t0)

	rootFallback := makeWatcherTestRoot(t, "lib-fallback", "root-fallback", fallbackDir)
	rootHealthy := makeWatcherTestRoot(t, "lib-healthy", "root-healthy", healthyDir)

	fallbackHoldEntered := make(chan struct{})
	releaseFallbackHold := make(chan struct{})
	var releaseFallbackOnce sync.Once
	releaseFallback := func() {
		releaseFallbackOnce.Do(func() { close(releaseFallbackHold) })
	}
	t.Cleanup(releaseFallback)
	var fallbackHoldOnce sync.Once

	healthyHoldEntered := make(chan struct{})
	releaseHealthyHold := make(chan struct{})
	var releaseHealthyOnce sync.Once
	releaseHealthy := func() {
		releaseHealthyOnce.Do(func() { close(releaseHealthyHold) })
	}
	t.Cleanup(releaseHealthy)
	var healthyHoldOnce sync.Once

	trig := &recordedTrigger{}
	logHandler := &memoryLogHandler{}
	clk := newFakeWatcherClock(t0)

	w, err := NewWatcher(WatcherOptions{
		StaticRoots:    []domain.LibraryRoot{rootFallback, rootHealthy},
		Trigger:        trig,
		Clock:          clk,
		Logger:         slog.New(logHandler),
		PollInterval:   15 * time.Millisecond,
		DebounceWindow: 30 * time.Millisecond,
		ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
			if bytes.Equal(rawDir, []byte(fallbackDir)) {
				fallbackHoldOnce.Do(func() {
					close(fallbackHoldEntered)
					select {
					case <-releaseFallbackHold:
					case <-t.Context().Done():
					}
				})
			}
			if bytes.Equal(rawDir, []byte(healthySub)) {
				healthyHoldOnce.Do(func() {
					close(healthyHoldEntered)
					select {
					case <-releaseHealthyHold:
					case <-t.Context().Done():
					}
				})
			}
			return os.ReadDir(string(rawDir))
		},
		newFSNotify: func() (fsnotifyHandle, error) {
			return newFaultInjectingFSNotifyHandle(func(inner fsnotifyHandle, name string) error {
				// Simulate Darwin kqueue partial registration where the directory and initial
				// descriptors are registered in inner before watchDirectoryFiles fails with EMFILE.
				if name == fallbackSub {
					_ = inner.Add(name)
					return syscall.EMFILE
				}
				return nil
			})
		},
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	// 1. While fallbackDir is mid-registration (after kernel watch on fallbackDir is installed,
	// before fallbackSub fails with EMFILE and rolls back), create a file in fallbackDir.
	// Because registration rolls back to polling, this OS event must be discarded and baseline
	// polling must remain quiet.
	select {
	case <-fallbackHoldEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fallbackDir ReadDir during registration")
	}
	writeWatcherFile(t, filepath.Join(fallbackDir, "during-rollback.cbz"), []byte("rb"), t0)
	time.Sleep(25 * time.Millisecond)
	releaseFallback()

	// 2. While healthyDir is mid-registration (after kernel watch on healthyDir is installed,
	// before healthySub finishes and rootHealthy is published), create a file in healthyDir.
	// This event must be retained and trigger a debounced automatic SCAN once rootHealthy is ready.
	select {
	case <-healthyHoldEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for healthySub ReadDir during registration")
	}
	writeWatcherFile(t, filepath.Join(healthyDir, "during-init.cbz"), []byte("init"), t0)
	time.Sleep(25 * time.Millisecond)
	releaseHealthy()

	clk.waitForParked(t, 1)

	stFallback, ok := w.RootStatus(rootFallback.ID)
	if !ok || stFallback.Mode != WatchModePoll || stFallback.IsNetwork {
		t.Fatalf("fallback root status = %+v (ok=%v), want Mode=poll IsNetwork=false", stFallback, ok)
	}
	stHealthy, ok := w.RootStatus(rootHealthy.ID)
	if !ok || stHealthy.Mode != WatchModeOS || stHealthy.IsNetwork {
		t.Fatalf("healthy root status = %+v (ok=%v), want Mode=os IsNetwork=false", stHealthy, ok)
	}

	// Partial watches on both fallbackDir and fallbackSub (registered before EMFILE) must
	// have been rolled back, while healthyDir and healthySub remain watched.
	wantWatched := []string{healthyDir, healthySub}
	slices.Sort(wantWatched)
	if got := w.WatchedPaths(); !slices.Equal(got, wantWatched) {
		t.Fatalf("WatchedPaths after partial EMFILE rollback = %v, want %v", got, wantWatched)
	}
	if !logHandler.hasWarnMessage("failed to register OS watch; falling back to polling") {
		t.Fatalf("expected fallback diagnostic warning log, got %+v", logHandler.snapshotRecords())
	}

	// Advance through debounce window: the retained event on healthyDir fires 1 scan for
	// lib-healthy, while the rolled-back event on fallbackDir was discarded.
	for range 3 {
		clk.advanceAndWait(t, 15*time.Millisecond)
	}
	reqs := trig.snapshot()
	if len(reqs) != 1 {
		t.Fatalf("retained registration event triggered %d scans, want 1 (reqs=%+v)", len(reqs), reqs)
	}
	assertAutomaticScanRequest(t, reqs[0], "lib-healthy")

	// Mutating fallbackDir is detected on the next poll tick and triggers one automatic SCAN for lib-fallback.
	writeWatcherFile(t, filepath.Join(fallbackSub, "vol02.cbz"), []byte("v2"), t0.Add(time.Second))
	for range 4 {
		clk.advanceAndWait(t, 15*time.Millisecond)
	}
	reqs = trig.snapshot()
	if len(reqs) != 2 {
		t.Fatalf("fallback polling triggered %d total scans, want 2", len(reqs))
	}
	assertAutomaticScanRequest(t, reqs[1], "lib-fallback")
}

func TestWatcher_RemovingLibraryOrRootReleasesWatchesAndShutdownStopsGoroutines(t *testing.T) {
	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()

	dir1A := t.TempDir()
	sub1A := filepath.Join(dir1A, "sub1a")
	writeWatcherFile(t, filepath.Join(sub1A, "vol01.cbz"), []byte("v1a"), time.Time{})

	dir1B := t.TempDir()
	sub1B := filepath.Join(dir1B, "sub1b")
	vanishingSub1B := filepath.Join(dir1B, "vanishing-sub1b")
	rebuildSub1B := filepath.Join(dir1B, "rebuild-inflight-sub1b")
	inflightSub1B := filepath.Join(dir1B, "inflight-sub1b")
	writeWatcherFile(t, filepath.Join(sub1B, "vol01.cbz"), []byte("v1b"), time.Time{})
	if err := os.MkdirAll(vanishingSub1B, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", vanishingSub1B, err)
	}

	dir2 := t.TempDir()
	sub2 := filepath.Join(dir2, "sub2")
	writeWatcherFile(t, filepath.Join(sub2, "vol01.cbz"), []byte("v2"), time.Time{})

	root1A := makeWatcherTestRoot(t, "lib-1", "root-1a", dir1A)
	root1B := makeWatcherTestRoot(t, "lib-1", "root-1b", dir1B)
	root2 := makeWatcherTestRoot(t, "lib-2", "root-2", dir2)

	var armRebuildDetect atomic.Bool
	rebuildDetectEntered := make(chan struct{})
	var rebuildDetectOnce sync.Once

	rebuildAddEntered := make(chan struct{})
	releaseRebuildAdd := make(chan struct{})
	rebuildAddInstalled := make(chan struct{})
	var releaseRebuildOnce sync.Once
	releaseRebuild := func() {
		releaseRebuildOnce.Do(func() { close(releaseRebuildAdd) })
	}
	t.Cleanup(releaseRebuild)
	var rebuildHoldTriggered atomic.Bool

	var armVanishingRebuild atomic.Bool
	var vanishingOmitDirOnce atomic.Bool
	var vanishingPauseSubOnce atomic.Bool
	vanishingWalkPaused := make(chan struct{})
	vanishingRuntimeInstalled := make(chan struct{})
	releaseVanishingCompletion := make(chan struct{})
	var releaseVanishingOnce sync.Once
	releaseVanishing := func() {
		releaseVanishingOnce.Do(func() { close(releaseVanishingCompletion) })
	}
	t.Cleanup(releaseVanishing)
	var vanishingPostTriggered atomic.Bool

	inflightAddEntered := make(chan struct{})
	releaseInflightAdd := make(chan struct{})
	inflightAddInstalled := make(chan struct{})
	var releaseInflightOnce sync.Once
	releaseInflight := func() {
		releaseInflightOnce.Do(func() { close(releaseInflightAdd) })
	}
	var inflightHoldTriggered atomic.Bool

	var handleMu sync.Mutex
	var activeHandle *faultInjectingFSNotifyHandle

	lister := &stubRootLister{
		roots: []domain.LibraryRoot{root1A, root1B, root2},
	}
	trig := &recordedTrigger{}

	w, err := NewWatcher(WatcherOptions{
		Roots:          lister,
		Trigger:        trig,
		PollInterval:   time.Hour,
		DebounceWindow: 30 * time.Millisecond,
		ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
			entries, err := os.ReadDir(string(rawDir))
			if err != nil {
				return nil, err
			}
			if armVanishingRebuild.Load() && string(rawDir) == dir1B && vanishingOmitDirOnce.CompareAndSwap(false, true) {
				filtered := entries[:0]
				vanishName := filepath.Base(vanishingSub1B)
				for _, de := range entries {
					if de.Name() != vanishName {
						filtered = append(filtered, de)
					}
				}
				return filtered, nil
			}
			if armVanishingRebuild.Load() && string(rawDir) == sub1B && vanishingPauseSubOnce.CompareAndSwap(false, true) {
				close(vanishingWalkPaused)
				select {
				case <-vanishingRuntimeInstalled:
				case <-t.Context().Done():
				}
			}
			return entries, nil
		},
		MountDetector: MountDetectorFunc(func(rawPath []byte) (mount.FilesystemInfo, error) {
			if armRebuildDetect.Load() && string(rawPath) == dir1B {
				rebuildDetectOnce.Do(func() {
					close(rebuildDetectEntered)
					select {
					case <-rebuildAddEntered:
					case <-t.Context().Done():
					}
				})
			}
			return mount.DetectFilesystemBytes(rawPath)
		}),
		newFSNotify: func() (fsnotifyHandle, error) {
			h, err := newFaultInjectingFSNotifyHandle(func(inner fsnotifyHandle, name string) error {
				if name == rebuildSub1B && rebuildHoldTriggered.CompareAndSwap(false, true) {
					close(rebuildAddEntered)
					select {
					case <-releaseRebuildAdd:
					case <-t.Context().Done():
					}
					_ = inner.Add(name)
					close(rebuildAddInstalled)
				}
				if name == inflightSub1B && inflightHoldTriggered.CompareAndSwap(false, true) {
					close(inflightAddEntered)
					select {
					case <-releaseInflightAdd:
					case <-t.Context().Done():
					}
					_ = inner.Add(name)
					close(inflightAddInstalled)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			h.postAddHook = func(name string, addErr error) {
				if addErr == nil && name == vanishingSub1B && armVanishingRebuild.Load() && vanishingPostTriggered.CompareAndSwap(false, true) {
					close(vanishingRuntimeInstalled)
					select {
					case <-releaseVanishingCompletion:
					case <-t.Context().Done():
					}
				}
			}
			handleMu.Lock()
			activeHandle = h
			handleMu.Unlock()
			return h, nil
		},
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()
	defer releaseInflight()
	defer releaseVanishing()
	defer releaseRebuild()

	waitForOSWatchedRoot(t, w, root1A.ID)
	waitForOSWatchedRoot(t, w, root1B.ID)
	waitForOSWatchedRoot(t, w, root2.ID)

	handleMu.Lock()
	h := activeHandle
	handleMu.Unlock()
	if h == nil {
		t.Fatal("expected active fsnotify handle")
	}

	wantAll := []string{dir1A, sub1A, dir1B, sub1B, vanishingSub1B, dir2, sub2}
	slices.Sort(wantAll)
	if got := w.WatchedPaths(); !slices.Equal(got, wantAll) {
		t.Fatalf("WatchedPaths = %v, want %v", got, wantAll)
	}

	// 1. Hold a runtime subdirectory Add on root1B while overflow recovery rebuilds the same
	// root1B registration. Releasing the stale Add must preserve the rebuilt native watch.
	armRebuildDetect.Store(true)
	h.injectError(fsnotify.ErrEventOverflow)
	select {
	case <-rebuildDetectEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for overflow recovery mount detection on root-1b")
	}
	if err := os.Mkdir(rebuildSub1B, 0o755); err != nil {
		t.Fatalf("Mkdir(%s): %v", rebuildSub1B, err)
	}
	waitForTriggerCount(t, trig, 2)
	releaseRebuild()
	select {
	case <-rebuildAddInstalled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for held runtime Add on rebuildSub1B to complete")
	}
	// Trigger an event on sub2 so readLoop finishes completeRuntimeWatch for rebuildSub1B
	// and returns to its event loop before we check WatchedPaths and rebuildSub1B events.
	writeWatcherFile(t, filepath.Join(sub2, "sync-after-rebuild.cbz"), []byte("sync"), time.Time{})
	waitForTriggerCount(t, trig, 3)

	if got := w.WatchedPaths(); !slices.Contains(got, rebuildSub1B) {
		t.Fatalf("WatchedPaths after stale-Add cleanup = %v, want to preserve %s", got, rebuildSub1B)
	}
	writeWatcherFile(t, filepath.Join(rebuildSub1B, "after-rebuild.cbz"), []byte("ar"), time.Time{})
	waitForTriggerCount(t, trig, 4)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 4 {
		t.Fatalf("after-rebuild write triggered %d total scans, want 4", got)
	}

	// 2. Previously watched vanishingSub1B misses the rebuild's ReadDir snapshot, then returns
	// while the rebuild is still in flight. Pause readLoop AFTER native Add installs the watch
	// and let finishRootRegistration complete before completeRuntimeWatch records watches.
	armVanishingRebuild.Store(true)
	h.injectError(fsnotify.ErrEventOverflow)
	select {
	case <-vanishingWalkPaused:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rebuild walk to pause after ReadDir(dir1B)")
	}
	h.setDropEvent(func(ev fsnotify.Event) bool {
		return ev.Name == vanishingSub1B && !ev.Op.Has(fsnotify.Create)
	})
	if err := os.Remove(vanishingSub1B); err != nil {
		t.Fatalf("Remove(%s): %v", vanishingSub1B, err)
	}
	time.Sleep(15 * time.Millisecond)
	h.setDropEvent(nil)
	if err := os.Mkdir(vanishingSub1B, 0o755); err != nil {
		t.Fatalf("Mkdir(%s): %v", vanishingSub1B, err)
	}
	// Wait for the rebuild's overflow scans (after finishRootRegistration completed).
	waitForTriggerCount(t, trig, 6)
	releaseVanishing()
	writeWatcherFile(t, filepath.Join(sub2, "sync-vanishing.cbz"), []byte("sync"), time.Time{})
	waitForTriggerCount(t, trig, 7)

	if got := w.WatchedPaths(); !slices.Contains(got, vanishingSub1B) {
		t.Fatalf("WatchedPaths after rebuild stale-cleanup race = %v, want to preserve %s", got, vanishingSub1B)
	}
	writeWatcherFile(t, filepath.Join(vanishingSub1B, "after-vanish.cbz"), []byte("av"), time.Time{})
	waitForTriggerCount(t, trig, 8)
	time.Sleep(55 * time.Millisecond)
	if got := len(trig.snapshot()); got != 8 {
		t.Fatalf("after-vanish write triggered %d total scans, want 8", got)
	}

	// 3. Trigger a runtime subdirectory Add on root1B, hold it in flight while removing
	// root1B via NotifySync, then release the held Add and verify no native watch leaks.
	if err := os.Mkdir(inflightSub1B, 0o755); err != nil {
		t.Fatalf("Mkdir(%s): %v", inflightSub1B, err)
	}
	select {
	case <-inflightAddEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for in-flight runtime Add on root-1b")
	}

	lister.setRoots([]domain.LibraryRoot{root1A, root2})
	w.NotifySync()
	waitForRootRemoved(t, w, root1B.ID)
	releaseInflight()

	select {
	case <-inflightAddInstalled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for held runtime Add to install before rollback check")
	}

	wantAfterRootRemove := []string{dir1A, sub1A, dir2, sub2}
	slices.Sort(wantAfterRootRemove)
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := w.WatchedPaths()
		if slices.Equal(got, wantAfterRootRemove) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("WatchedPaths after removing root-1b during in-flight Add = %v, want %v", got, wantAfterRootRemove)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Changes in removed root1B (including sub1B, vanishingSub1B, rebuildSub1B, and inflightSub1B) must not trigger any scan.
	writeWatcherFile(t, filepath.Join(sub1B, "ignored.cbz"), []byte("ignored"), time.Time{})
	writeWatcherFile(t, filepath.Join(vanishingSub1B, "ignored-vanish.cbz"), []byte("ignored"), time.Time{})
	writeWatcherFile(t, filepath.Join(rebuildSub1B, "ignored-rebuild.cbz"), []byte("ignored"), time.Time{})
	writeWatcherFile(t, filepath.Join(inflightSub1B, "ignored-inflight.cbz"), []byte("ignored"), time.Time{})
	time.Sleep(60 * time.Millisecond)
	if got := len(trig.snapshot()); got != 8 {
		t.Fatalf("removed root triggered %d scans total, want 8", got)
	}

	// Remaining root1A in lib-1 still triggers normally.
	writeWatcherFile(t, filepath.Join(sub1A, "active.cbz"), []byte("active"), time.Time{})
	reqs := waitForTriggerCount(t, trig, 9)
	assertAutomaticScanRequest(t, reqs[8], "lib-1")

	// 4. Remove library lib-2 (root2) -> releases watches for dir2 and sub2.
	lister.setRoots([]domain.LibraryRoot{root1A})
	w.NotifySync()
	waitForRootRemoved(t, w, root2.ID)

	wantAfterLibRemove := []string{dir1A, sub1A}
	slices.Sort(wantAfterLibRemove)
	if got := w.WatchedPaths(); !slices.Equal(got, wantAfterLibRemove) {
		t.Fatalf("WatchedPaths after removing lib-2 = %v, want %v", got, wantAfterLibRemove)
	}

	writeWatcherFile(t, filepath.Join(sub2, "ignored2.cbz"), []byte("ignored2"), time.Time{})
	time.Sleep(60 * time.Millisecond)
	if got := len(trig.snapshot()); got != 9 {
		t.Fatalf("removed library triggered scan: got %d total, want 9", got)
	}

	// 4. Stop closes the OS watcher, releases all remaining watches, and leaves no goroutine running.
	w.Stop()
	if got := w.WatchedPaths(); len(got) != 0 {
		t.Fatalf("WatchedPaths after Stop = %v, want empty", got)
	}
	assertNoWatcherGoroutinesRunning(t, baselineGoroutines+2)
}

func TestWatcher_RuntimeDanglingSymlinkPollsWhenTargetAppears(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "runtime-dangling-root")
	writeWatcherFile(t, filepath.Join(dir, "vol01.cbz"), []byte("v1"), time.Time{})
	root := makeWatcherTestRoot(t, "lib-dangling", "root-dangling", dir)

	trig := &recordedTrigger{}
	w, err := NewWatcher(WatcherOptions{
		StaticRoots:    []domain.LibraryRoot{root},
		Trigger:        trig,
		PollInterval:   25 * time.Millisecond,
		DebounceWindow: 35 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWatcher: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	waitForOSWatchedRoot(t, w, root.ID)
	if w.rootHasFileSymlinks(root.ID) {
		t.Fatal("expected zero symlinks at startup")
	}

	// Create a dangling symlink at runtime pointing outside the root where the target does not yet exist.
	extDir := t.TempDir()
	extTarget := filepath.Join(extDir, "runtime-target.cbz")
	linkPath := filepath.Join(dir, "runtime-dangling.cbz")
	if err := os.Symlink(extTarget, linkPath); err != nil {
		t.Fatalf("Symlink(%s -> %s): %v", linkPath, extTarget, err)
	}

	reqs := waitForTriggerCount(t, trig, 1)
	assertAutomaticScanRequest(t, reqs[0], "lib-dangling")
	time.Sleep(80 * time.Millisecond)
	if got := len(trig.snapshot()); got != 1 {
		t.Fatalf("dangling link without target triggered %d scans, want 1", got)
	}

	// Creating the external target outside the root must be detected on the next scheduled poll tick.
	writeWatcherFile(t, extTarget, []byte("runtime-target-appeared"), time.Time{})
	reqs = waitForTriggerCount(t, trig, 2)
	time.Sleep(60 * time.Millisecond)
	if got := len(trig.snapshot()); got != 2 {
		t.Fatalf("appearing target for runtime-created dangling symlink triggered %d total scans, want 2", got)
	}
	assertAutomaticScanRequest(t, reqs[1], "lib-dangling")
}
