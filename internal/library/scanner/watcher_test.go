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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

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
