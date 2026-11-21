package scanner_test

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/testdb"
)

type countingDirStream struct {
	inner     *os.File
	calls     *atomic.Int32
	entries   *atomic.Int32
	requested *[]int
	mu        *sync.Mutex
}

func (s *countingDirStream) ReadDir(n int) ([]os.DirEntry, error) {
	s.calls.Add(1)
	s.mu.Lock()
	*s.requested = append(*s.requested, n)
	s.mu.Unlock()
	list, err := s.inner.ReadDir(n)
	s.entries.Add(int32(len(list)))
	return list, err
}

func (s *countingDirStream) Close() error {
	return s.inner.Close()
}

func loadOutboxEventsDetailed(ctx context.Context, t *testing.T, db *testdb.TestDB) []domain.DomainEvent {
	t.Helper()
	rows, err := db.Query(ctx, `
		SELECT id, kind, version, aggregate_type, aggregate_id, payload, created_at
		FROM domain_events
		ORDER BY id ASC
	`)
	if err != nil {
		t.Fatalf("query domain_events: %v", err)
	}
	defer rows.Close()

	var out []domain.DomainEvent
	for rows.Next() {
		var (
			ev      domain.DomainEvent
			kind    string
			payload []byte
		)
		if err := rows.Scan(&ev.ID, &kind, &ev.Version, &ev.AggregateType, &ev.AggregateID, &payload, &ev.CreatedAt); err != nil {
			t.Fatalf("scan domain_event: %v", err)
		}
		ev.Kind = domain.EventKind(kind)
		ev.Payload = json.RawMessage(payload)
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate domain_events: %v", err)
	}
	return out
}

func loadRootsForLibrary(ctx context.Context, t *testing.T, tx storage.Transactor, libraryID string) []domain.LibraryRoot {
	t.Helper()
	var roots []domain.LibraryRoot
	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		var err error
		roots, err = st.Libraries().ListLibraryRoots(ctx, libraryID)
		return err
	}); err != nil {
		t.Fatalf("ListLibraryRoots(%s): %v", libraryID, err)
	}
	return roots
}

func TestPreCheckLibrary_FailureModesTransitionsAndRecovery(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	missingRoot := filepath.Join(base, "01-missing")
	emptyRoot := filepath.Join(base, "02-empty")
	unreadableRoot := filepath.Join(base, "03-unreadable")
	healthyRoot := filepath.Join(base, "04-healthy")

	if err := os.MkdirAll(emptyRoot, 0o755); err != nil {
		t.Fatalf("mkdir emptyRoot: %v", err)
	}
	if err := os.MkdirAll(unreadableRoot, 0o755); err != nil {
		t.Fatalf("mkdir unreadableRoot: %v", err)
	}
	// Hidden files other than .tobiten-root and empty subdirectories must not mark an empty root online.
	writeDiskFile(t, filepath.Join(emptyRoot, ".DS_Store"), []byte("finder"))
	if err := os.MkdirAll(filepath.Join(emptyRoot, "EmptySeries"), 0o755); err != nil {
		t.Fatalf("mkdir empty subdir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(emptyRoot, scanner.RootMarkerFilename), 0o755); err != nil {
		// Replace directory named .tobiten-root before testing marker file; a directory named .tobiten-root must NOT pass.
		t.Fatalf("mkdir fake marker dir: %v", err)
	}

	bookBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("page-1"), method: zip.Store})
	writeDiskFile(t, filepath.Join(healthyRoot, "Nested/Vol01.cbz"), bookBytes)

	var (
		lib   domain.Library
		roots []domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, roots = seedLibraryAndRoots(
			ctx, t, tx, "PreCheck Lifecycle Lib", nil,
			[]byte(missingRoot), []byte(emptyRoot), []byte(unreadableRoot), []byte(healthyRoot),
		)
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	clockNow := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	setClock := func(ts time.Time) {
		mu.Lock()
		defer mu.Unlock()
		clockNow = ts
	}
	getClock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clockNow
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{Now: getClock})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}

	snapEmpty := snapshotTree(t, emptyRoot)
	snapHealthy := snapshotTree(t, healthyRoot)

	var visited []string
	var visitMu sync.Mutex
	preOpts := scanner.PreCheckOptions{
		OpenDir: func(rawDir []byte) (scanner.DirStream, error) {
			p := string(rawDir)
			visitMu.Lock()
			visited = append(visited, p)
			visitMu.Unlock()
			if p == unreadableRoot {
				return nil, os.ErrPermission
			}
			return os.Open(p)
		},
		Stat: func(rawPath []byte) (os.FileInfo, error) {
			p := string(rawPath)
			if p == missingRoot || p == emptyRoot || p == unreadableRoot || p == healthyRoot {
				visitMu.Lock()
				visited = append(visited, "stat:"+p)
				visitMu.Unlock()
			}
			return os.Stat(p)
		},
	}

	// 1. First failing check: evaluates EVERY root, marks failing roots OFFLINE with
	//    last_checked_at and last_error, leaves healthy root ONLINE, reports OFFLINE +
	//    ApplyRemovals=false, writes nothing on disk, and appends exactly one library.offline event.
	res1, err := sc.PreCheckLibrary(ctx, lib.ID, preOpts)
	if err != nil {
		t.Fatalf("PreCheckLibrary(1): %v", err)
	}
	assertTreeUnchanged(t, "emptyRoot", emptyRoot, snapEmpty)
	assertTreeUnchanged(t, "healthyRoot", healthyRoot, snapHealthy)

	for _, rootPath := range []string{missingRoot, emptyRoot, unreadableRoot, healthyRoot} {
		if !slices.Contains(visited, "stat:"+rootPath) {
			t.Fatalf("expected root %s to be evaluated before verdict; visited=%v", rootPath, visited)
		}
	}
	if res1.State != domain.LibraryRootStateOffline || res1.PreviousState != domain.LibraryRootStateOnline {
		t.Fatalf("unexpected state transition on check 1: %+v", res1)
	}
	if res1.ApplyRemovals || res1.ShouldScan {
		t.Fatalf("expected ApplyRemovals=false and ShouldScan=false when OFFLINE, got %+v", res1)
	}
	if len(res1.Roots) != 4 {
		t.Fatalf("expected 4 root check results, got %d", len(res1.Roots))
	}

	expectedFailed := []string{roots[0].ID, roots[1].ID, roots[2].ID}
	slices.Sort(expectedFailed)
	if !slices.Equal(res1.FailedRootIDs, expectedFailed) {
		t.Fatalf("FailedRootIDs = %v, want %v", res1.FailedRootIDs, expectedFailed)
	}
	if res1.Event == nil || res1.Event.Kind != domain.EventLibraryOffline {
		t.Fatalf("expected library.offline event on transition, got %+v", res1.Event)
	}

	dbRoots1 := loadRootsForLibrary(ctx, t, transactor, lib.ID)
	for i, r := range dbRoots1 {
		if r.LastCheckedAt == nil || !r.LastCheckedAt.Equal(clockNow) {
			t.Fatalf("root[%d] LastCheckedAt = %v, want %v", i, r.LastCheckedAt, clockNow)
		}
		if i == 3 {
			if r.State != domain.LibraryRootStateOnline || r.LastError != nil {
				t.Fatalf("healthy root expected ONLINE with nil LastError, got state=%s err=%v", r.State, r.LastError)
			}
			continue
		}
		if r.State != domain.LibraryRootStateOffline || r.LastError == nil || *r.LastError == "" {
			t.Fatalf("failing root[%d] expected OFFLINE with non-empty LastError, got state=%s err=%v", i, r.State, r.LastError)
		}
	}

	events1 := loadOutboxEventsDetailed(ctx, t, db)
	if len(events1) != 1 ||
		events1[0].Kind != domain.EventLibraryOffline ||
		events1[0].AggregateType != domain.AggregateTypeLibrary ||
		events1[0].AggregateID != lib.ID {
		t.Fatalf("expected 1 library.offline outbox event, got %+v", events1)
	}
	payload1, err := domain.DecodeEventPayload[domain.LibraryEventPayloadV1](events1[0])
	if err != nil {
		t.Fatalf("DecodeEventPayload(library.offline): %v", err)
	}
	if payload1.LibraryID != lib.ID || !slices.Equal(payload1.FailedRootIDs, expectedFailed) {
		t.Fatalf("unexpected library.offline payload: %+v (want failed=%v)", payload1, expectedFailed)
	}

	// 2. Repeated failing check: updates last_checked_at, emits NO duplicate library.offline event.
	t2 := clockNow.Add(5 * time.Minute)
	setClock(t2)
	res2, err := sc.PreCheckLibrary(ctx, lib.ID, preOpts)
	if err != nil {
		t.Fatalf("PreCheckLibrary(2): %v", err)
	}
	if res2.State != domain.LibraryRootStateOffline || res2.PreviousState != domain.LibraryRootStateOffline {
		t.Fatalf("expected OFFLINE -> OFFLINE on repeated check, got %+v", res2)
	}
	if res2.ApplyRemovals || res2.ShouldScan || res2.Event != nil {
		t.Fatalf("expected ApplyRemovals=false, ShouldScan=false, Event=nil on repeated failing check, got %+v", res2)
	}
	dbRoots2 := loadRootsForLibrary(ctx, t, transactor, lib.ID)
	for i, r := range dbRoots2 {
		if r.LastCheckedAt == nil || !r.LastCheckedAt.Equal(t2) {
			t.Fatalf("root[%d] LastCheckedAt = %v, want %v", i, r.LastCheckedAt, t2)
		}
	}
	if got := loadOutboxEventsDetailed(ctx, t, db); len(got) != 1 {
		t.Fatalf("expected still 1 outbox event after repeated failing check, got %d", len(got))
	}

	// 3. Partial recovery (2 of 3 failing roots recover; 1 remains failing): still OFFLINE, no event.
	if err := os.RemoveAll(filepath.Join(emptyRoot, scanner.RootMarkerFilename)); err != nil {
		t.Fatalf("remove fake marker dir: %v", err)
	}
	writeDiskFile(t, filepath.Join(emptyRoot, scanner.RootMarkerFilename), nil)
	writeDiskFile(t, filepath.Join(missingRoot, "returned.cbz"), bookBytes)

	t3 := t2.Add(5 * time.Minute)
	setClock(t3)
	res3, err := sc.PreCheckLibrary(ctx, lib.ID, preOpts)
	if err != nil {
		t.Fatalf("PreCheckLibrary(3): %v", err)
	}
	if res3.State != domain.LibraryRootStateOffline || res3.Event != nil || !slices.Equal(res3.FailedRootIDs, []string{roots[2].ID}) {
		t.Fatalf("expected partial recovery to stay OFFLINE without event, got %+v", res3)
	}
	if got := loadOutboxEventsDetailed(ctx, t, db); len(got) != 1 {
		t.Fatalf("expected still 1 outbox event on partial recovery, got %d", len(got))
	}

	// 4. Full recovery: files + marker return, unreadable root becomes readable with a file.
	writeDiskFile(t, filepath.Join(unreadableRoot, "back.cbz"), bookBytes)
	preOpts.OpenDir = nil
	preOpts.Stat = nil

	t4 := t3.Add(5 * time.Minute)
	setClock(t4)
	res4, err := sc.PreCheckLibrary(ctx, lib.ID, preOpts)
	if err != nil {
		t.Fatalf("PreCheckLibrary(4): %v", err)
	}
	if res4.PreviousState != domain.LibraryRootStateOffline || res4.State != domain.LibraryRootStateOnline {
		t.Fatalf("expected OFFLINE -> ONLINE on recovery, got %+v", res4)
	}
	if !res4.ApplyRemovals || !res4.ShouldScan || len(res4.FailedRootIDs) != 0 {
		t.Fatalf("expected ApplyRemovals=true, ShouldScan=true, no failed roots, got %+v", res4)
	}
	if res4.Event == nil || res4.Event.Kind != domain.EventLibraryOnline {
		t.Fatalf("expected library.online event on recovery, got %+v", res4.Event)
	}

	dbRoots4 := loadRootsForLibrary(ctx, t, transactor, lib.ID)
	for i, r := range dbRoots4 {
		if r.State != domain.LibraryRootStateOnline || r.LastError != nil || r.LastCheckedAt == nil || !r.LastCheckedAt.Equal(t4) {
			t.Fatalf("root[%d] expected ONLINE with cleared LastError and t4=%v, got %+v", i, t4, r)
		}
	}

	events4 := loadOutboxEventsDetailed(ctx, t, db)
	if len(events4) != 2 || events4[1].Kind != domain.EventLibraryOnline ||
		events4[1].AggregateType != domain.AggregateTypeLibrary || events4[1].AggregateID != lib.ID {
		t.Fatalf("expected library.online as 2nd outbox event, got %+v", events4)
	}
	payload4, err := domain.DecodeEventPayload[domain.LibraryEventPayloadV1](events4[1])
	if err != nil {
		t.Fatalf("DecodeEventPayload(library.online): %v", err)
	}
	if payload4.LibraryID != lib.ID || len(payload4.FailedRootIDs) != 0 {
		t.Fatalf("unexpected library.online payload: %+v", payload4)
	}

	// 5. Repeated passing check: stays ONLINE, emits NO duplicate library.online event.
	t5 := t4.Add(5 * time.Minute)
	setClock(t5)
	res5, err := sc.PreCheckLibrary(ctx, lib.ID, preOpts)
	if err != nil {
		t.Fatalf("PreCheckLibrary(5): %v", err)
	}
	if res5.State != domain.LibraryRootStateOnline || !res5.ApplyRemovals || res5.Event != nil {
		t.Fatalf("expected unchanged ONLINE check with nil Event, got %+v", res5)
	}
	if got := loadOutboxEventsDetailed(ctx, t, db); len(got) != 2 {
		t.Fatalf("expected still 2 outbox events after repeated ONLINE check, got %d", len(got))
	}
}

func TestPreCheckLibrary_OSPermissionsFileRootAndStopsAtFirstFile(t *testing.T) {
	t.Parallel()

	t.Run("regular file as root fails pre-check", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		fileRoot := filepath.Join(t.TempDir(), "not-a-dir.cbz")
		writeDiskFile(t, fileRoot, []byte("not-a-dir"))

		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "File Root Lib", nil, []byte(fileRoot))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		res, err := sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{})
		if err != nil {
			t.Fatalf("PreCheckLibrary: %v", err)
		}
		if res.State != domain.LibraryRootStateOffline || len(res.Roots) != 1 || !errors.Is(res.Roots[0].Err, scanner.ErrRootNotDirectory) {
			t.Fatalf("expected OFFLINE with ErrRootNotDirectory, got %+v", res)
		}
	})

	t.Run("chmod 0000 unreadable root fails pre-check on OS", func(t *testing.T) {
		t.Parallel()
		if os.Geteuid() == 0 {
			t.Skip("running as root bypasses chmod 0000 read permissions")
		}
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		dir := filepath.Join(t.TempDir(), "unreadable")
		writeDiskFile(t, filepath.Join(dir, "book.cbz"), []byte("data"))
		if err := os.Chmod(dir, 0o000); err != nil {
			t.Fatalf("chmod 0000: %v", err)
		}
		t.Cleanup(func() {
			_ = os.Chmod(dir, 0o755)
		})

		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "Chmod Unreadable Lib", nil, []byte(dir))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		res, err := sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{})
		if err != nil {
			t.Fatalf("PreCheckLibrary: %v", err)
		}
		if res.State != domain.LibraryRootStateOffline || res.Roots[0].LastError == nil {
			t.Fatalf("expected OFFLINE for unreadable root, got %+v", res)
		}
	})

	t.Run("stops directory read at first file on large root", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		largeRoot := filepath.Join(t.TempDir(), "large")
		if err := os.MkdirAll(largeRoot, 0o755); err != nil {
			t.Fatalf("mkdir largeRoot: %v", err)
		}
		for i := range 250 {
			name := filepath.Join(largeRoot, fmt.Sprintf("file_%03d.cbz", i))
			writeDiskFile(t, name, []byte("x"))
		}

		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "Large Root Lib", nil, []byte(largeRoot))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}

		var (
			readCalls   atomic.Int32
			readEntries atomic.Int32
			requested   []int
			mu          sync.Mutex
		)
		res, err := sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{
			OpenDir: func(rawDir []byte) (scanner.DirStream, error) {
				f, err := os.Open(string(rawDir))
				if err != nil {
					return nil, err
				}
				return &countingDirStream{
					inner:     f,
					calls:     &readCalls,
					entries:   &readEntries,
					requested: &requested,
					mu:        &mu,
				}, nil
			},
		})
		if err != nil {
			t.Fatalf("PreCheckLibrary(large): %v", err)
		}
		if res.State != domain.LibraryRootStateOnline {
			t.Fatalf("expected ONLINE, got %+v", res)
		}
		if readCalls.Load() != 1 || readEntries.Load() != 1 || !slices.Equal(requested, []int{1}) {
			t.Fatalf("expected directory read to stop after 1 ReadDir(1) entry, got calls=%d entries=%d requested=%v",
				readCalls.Load(), readEntries.Load(), requested)
		}
	})
}

func TestPreCheckLibrary_AtomicRollbackConcurrencyAndScanStop(t *testing.T) {
	t.Parallel()

	t.Run("outbox failure rolls back root status update atomically", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		missingRoot := filepath.Join(t.TempDir(), "missing")
		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Rollback PreCheck Lib", nil, []byte(missingRoot))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		injectedErr := errors.New("injected outbox failure")
		faultTx := &faultInjectingTransactor{
			inner:     transactor,
			failStage: "event_added",
			err:       injectedErr,
		}
		sc, err := scanner.NewScanner(faultTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		if _, err := sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{}); !errors.Is(err, injectedErr) {
			t.Fatalf("expected injectedErr, got %v", err)
		}

		gotRoots := loadRootsForLibrary(ctx, t, transactor, lib.ID)
		if gotRoots[0].ID != roots[0].ID ||
			gotRoots[0].State != domain.LibraryRootStateOnline ||
			gotRoots[0].LastCheckedAt != nil ||
			gotRoots[0].LastError != nil {
			t.Fatalf("expected root status rolled back to initial ONLINE state, got %+v", gotRoots[0])
		}
		if evs := loadOutboxEventsDetailed(ctx, t, db); len(evs) != 0 {
			for _, ev := range evs {
				if ev.AggregateID == lib.ID {
					t.Fatalf("expected zero outbox events for rolled back library, got %+v", ev)
				}
			}
		}
	})

	t.Run("concurrent pre-checks emit single library.offline event", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		missingRoot := filepath.Join(t.TempDir(), "missing")
		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "Concurrent PreCheck Lib", nil, []byte(missingRoot))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make([]scanner.PreCheckResult, 2)
		errs := make([]error, 2)
		for i := range 2 {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-start
				results[idx], errs[idx] = sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{})
			}(i)
		}
		close(start)
		wg.Wait()

		succeeded := 0
		for i, err := range errs {
			if errors.Is(err, scanner.ErrStalePreCheck) {
				continue
			}
			if err != nil {
				t.Fatalf("concurrent PreCheckLibrary[%d]: %v", i, err)
			}
			if results[i].State != domain.LibraryRootStateOffline {
				t.Fatalf("concurrent PreCheckLibrary[%d] state = %s, want OFFLINE", i, results[i].State)
			}
			succeeded++
		}
		if succeeded == 0 {
			t.Fatal("expected at least one concurrent pre-check to commit")
		}

		var offlineEventCount int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM domain_events
			WHERE aggregate_type = 'library' AND aggregate_id = $1 AND kind = 'library.offline'
		`, lib.ID).Scan(&offlineEventCount); err != nil {
			t.Fatalf("count library.offline events: %v", err)
		}
		if offlineEventCount != 1 {
			t.Fatalf("expected exactly 1 library.offline event across concurrent checks, got %d", offlineEventCount)
		}
	})

	t.Run("ScanLibrary checks every root, stops when offline, and resumes indexing upon recovery (M1B-D10)", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		base := t.TempDir()
		rootA := filepath.Join(base, "a-online")
		rootB := filepath.Join(base, "b-offline")
		rootC := filepath.Join(base, "c-online")

		cbzA := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("a"), method: zip.Store})
		cbzC := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("c"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootA, "a.cbz"), cbzA)
		if err := os.MkdirAll(rootB, 0o755); err != nil {
			t.Fatalf("mkdir rootB: %v", err)
		}
		writeDiskFile(t, filepath.Join(rootC, "c.cbz"), cbzC)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(
				ctx, t, tx, "D10 Stop Scan Lib", nil,
				[]byte(rootA), []byte(rootB), []byte(rootC),
			)
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}

		var archiveOpens atomic.Int32
		res1, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				archiveOpens.Add(1)
				return os.Open(string(rawPath))
			},
		})
		if err != nil {
			t.Fatalf("ScanLibrary(offline): %v", err)
		}
		if !res1.Offline || res1.Complete || res1.ApplyRemovals {
			t.Fatalf("expected Offline=true Complete=false ApplyRemovals=false, got %+v", res1)
		}
		if archiveOpens.Load() != 0 || res1.IndexedCount != 0 || len(res1.Roots) != 0 {
			t.Fatalf("expected M1B-D10 scan stop with 0 opens/indexed/walked roots, got opens=%d res=%+v",
				archiveOpens.Load(), res1)
		}
		if len(res1.PreCheck.Roots) != 3 ||
			res1.PreCheck.Roots[0].State != domain.LibraryRootStateOnline ||
			res1.PreCheck.Roots[1].State != domain.LibraryRootStateOffline ||
			res1.PreCheck.Roots[2].State != domain.LibraryRootStateOnline {
			t.Fatalf("expected all 3 roots evaluated before stopping, got %+v", res1.PreCheck.Roots)
		}
		for _, r := range roots {
			if len(listAssetsForRoot(ctx, t, transactor, r.ID)) != 0 {
				t.Fatalf("expected 0 assets for root %s while library OFFLINE", r.ID)
			}
		}

		// Place .tobiten-root marker on rootB; next scan returns ONLINE and indexes rootA and rootC.
		writeDiskFile(t, filepath.Join(rootB, scanner.RootMarkerFilename), nil)
		res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("ScanLibrary(recovered): %v", err)
		}
		if res2.Offline || !res2.Complete || !res2.ApplyRemovals || res2.IndexedCount != 2 {
			t.Fatalf("expected ONLINE completed scan indexing 2 files, got %+v", res2)
		}
		if res2.PreCheck.Event == nil || res2.PreCheck.Event.Kind != domain.EventLibraryOnline {
			t.Fatalf("expected library.online transition event on recovery scan, got %+v", res2.PreCheck.Event)
		}
		if len(listAssetsForRoot(ctx, t, transactor, roots[0].ID)) != 1 ||
			len(listAssetsForRoot(ctx, t, transactor, roots[1].ID)) != 0 ||
			len(listAssetsForRoot(ctx, t, transactor, roots[2].ID)) != 1 {
			t.Fatal("expected rootA and rootC indexed and marker-only rootB with 0 assets")
		}
	})

	t.Run("cancellation during pre-check leaves database untouched and validation guards", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		missingRoot := filepath.Join(t.TempDir(), "missing")
		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "Cancel PreCheck Lib", nil, []byte(missingRoot))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}

		var nilScanner *scanner.Scanner
		if _, err := nilScanner.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{}); err == nil {
			t.Fatal("expected error for nil scanner")
		}
		if _, err := sc.PreCheckLibrary(ctx, " ", scanner.PreCheckOptions{}); !errors.Is(err, domain.ErrEmptyLibraryID) {
			t.Fatalf("expected ErrEmptyLibraryID, got %v", err)
		}
		if _, err := sc.PreCheckLibrary(ctx, "missing-lib", scanner.PreCheckOptions{}); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected storage.ErrNotFound, got %v", err)
		}

		probeCtx, probeCancel := context.WithCancel(ctx)
		_, err = sc.PreCheckLibrary(probeCtx, lib.ID, scanner.PreCheckOptions{
			Stat: func(_ []byte) (os.FileInfo, error) {
				probeCancel()
				return nil, context.Canceled
			},
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		gotRoots := loadRootsForLibrary(ctx, t, transactor, lib.ID)
		if gotRoots[0].State != domain.LibraryRootStateOnline || gotRoots[0].LastCheckedAt != nil {
			t.Fatalf("expected cancelled pre-check to persist nothing, got %+v", gotRoots[0])
		}
	})

	t.Run("synthetic DirStream EOF and non-regular entries handled safely", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		synthRoot := []byte("/synthetic/precheck-root")
		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "Synthetic PreCheck Lib", nil, synthRoot)
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}

		now := time.Unix(1700000000, 0).UTC()
		res, err := sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{
			Stat: func(rawPath []byte) (os.FileInfo, error) {
				switch string(rawPath) {
				case "/synthetic/precheck-root":
					return syntheticFileInfo{name: "precheck-root", mode: fs.ModeDir | 0o755, mtime: now, id: 1}, nil
				default:
					return syntheticFileInfo{name: filepath.Base(string(rawPath)), mode: fs.ModeNamedPipe | 0o600, mtime: now, id: 2}, nil
				}
			},
			OpenDir: func(_ []byte) (scanner.DirStream, error) {
				return &sliceTestDirStream{
					entries: []os.DirEntry{
						syntheticDirEntry{name: "fifo", mode: fs.ModeNamedPipe, mtime: now},
					},
				}, nil
			},
		})
		if err != nil {
			t.Fatalf("PreCheckLibrary(synthetic): %v", err)
		}
		if res.State != domain.LibraryRootStateOffline || !errors.Is(res.Roots[0].Err, scanner.ErrRootEmpty) {
			t.Fatalf("expected OFFLINE ErrRootEmpty when only FIFO exists, got %+v", res)
		}
	})

	t.Run("root path replacement during pre-check rejects stale probe without overwriting new path", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		base := t.TempDir()
		oldMissing := filepath.Join(base, "old-missing")
		newHealthy := filepath.Join(base, "new-healthy")
		bookBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("ok"), method: zip.Store})
		writeDiskFile(t, filepath.Join(newHealthy, "ok.cbz"), bookBytes)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Root Replace Race Lib", nil, []byte(oldMissing))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}

		probedOld := make(chan struct{})
		releaseOld := make(chan struct{})
		var (
			checkRes scanner.PreCheckResult
			checkErr error
		)
		done := make(chan struct{})
		go func() {
			defer close(done)
			checkRes, checkErr = sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{
				Stat: func(rawPath []byte) (os.FileInfo, error) {
					info, statErr := os.Stat(string(rawPath))
					if string(rawPath) == oldMissing {
						close(probedOld)
						select {
						case <-releaseOld:
						case <-ctx.Done():
						}
					}
					return info, statErr
				},
			})
		}()

		select {
		case <-probedOld:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for old root probe: %v", ctx.Err())
		}

		newPathID, err := domain.NewRootPathIdentity([]byte(newHealthy))
		if err != nil {
			t.Fatalf("NewRootPathIdentity: %v", err)
		}
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			updated := roots[0]
			updated.Path = newPathID
			return tx.Libraries().UpdateLibraryRoot(ctx, &updated)
		}); err != nil {
			t.Fatalf("UpdateLibraryRoot: %v", err)
		}
		close(releaseOld)
		<-done

		if !errors.Is(checkErr, scanner.ErrStalePreCheck) {
			t.Fatalf("expected ErrStalePreCheck when root path changed mid-check, got (%+v, %v)", checkRes, checkErr)
		}
		gotRoots := loadRootsForLibrary(ctx, t, transactor, lib.ID)
		if string(gotRoots[0].Path.Bytes) != newHealthy ||
			gotRoots[0].State != domain.LibraryRootStateOnline ||
			gotRoots[0].LastCheckedAt != nil ||
			gotRoots[0].LastError != nil {
			t.Fatalf("expected replaced root to stay ONLINE and untouched by stale probe, got %+v", gotRoots[0])
		}
		if evs := loadOutboxEventsDetailed(ctx, t, db); len(evs) != 0 {
			t.Fatalf("expected 0 outbox events after stale pre-check rejection, got %+v", evs)
		}

		freshRes, err := sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{})
		if err != nil || freshRes.State != domain.LibraryRootStateOnline {
			t.Fatalf("fresh PreCheckLibrary on replaced root = (%+v, %v)", freshRes, err)
		}
	})

	t.Run("overlapping opposite observations prevent older check from masquerading as recovery", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		bookBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("ok"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootDir, "book.cbz"), bookBytes)

		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "Overlapping Opposite Lib", nil, []byte(rootDir))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		t1 := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
		t2 := t1.Add(time.Minute)
		t3 := t2.Add(time.Minute)
		t4 := t3.Add(time.Minute)

		// Subcase A: older ONLINE observation (t1) pauses, newer OFFLINE observation (t2) commits;
		// older observation must be rejected without restoring ONLINE or emitting library.online.
		sc1, err := scanner.NewScanner(transactor, scanner.Options{Now: func() time.Time { return t1 }})
		if err != nil {
			t.Fatalf("NewScanner(sc1): %v", err)
		}
		sc2, err := scanner.NewScanner(transactor, scanner.Options{Now: func() time.Time { return t2 }})
		if err != nil {
			t.Fatalf("NewScanner(sc2): %v", err)
		}

		probed1 := make(chan struct{})
		release1 := make(chan struct{})
		var err1 error
		done1 := make(chan struct{})
		go func() {
			defer close(done1)
			_, err1 = sc1.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{
				OpenDir: func(rawDir []byte) (scanner.DirStream, error) {
					f, openErr := os.Open(string(rawDir))
					return &hookAfterCloseDirStream{
						inner: f,
						onClose: func() {
							close(probed1)
							select {
							case <-release1:
							case <-ctx.Done():
							}
						},
					}, openErr
				},
			})
		}()

		select {
		case <-probed1:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for check 1 probe: %v", ctx.Err())
		}

		if err := os.Remove(filepath.Join(rootDir, "book.cbz")); err != nil {
			t.Fatalf("remove book.cbz: %v", err)
		}
		res2, err2 := sc2.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{})
		if err2 != nil || res2.State != domain.LibraryRootStateOffline {
			t.Fatalf("newer check 2 = (%+v, %v), want OFFLINE", res2, err2)
		}

		close(release1)
		<-done1
		if !errors.Is(err1, scanner.ErrStalePreCheck) {
			t.Fatalf("older ONLINE check err = %v, want ErrStalePreCheck", err1)
		}
		rootsAfterA := loadRootsForLibrary(ctx, t, transactor, lib.ID)
		if rootsAfterA[0].State != domain.LibraryRootStateOffline ||
			rootsAfterA[0].LastCheckedAt == nil || !rootsAfterA[0].LastCheckedAt.Equal(t2) {
			t.Fatalf("expected DB to retain newer OFFLINE verdict at t2, got %+v", rootsAfterA[0])
		}
		evsA := loadOutboxEventsDetailed(ctx, t, db)
		if len(evsA) != 1 || evsA[0].Kind != domain.EventLibraryOffline {
			t.Fatalf("expected only library.offline event, got %+v", evsA)
		}

		// Subcase B: older OFFLINE observation (t3) pauses, newer ONLINE recovery (t4) commits;
		// older observation must be rejected without reverting to OFFLINE.
		sc3, err := scanner.NewScanner(transactor, scanner.Options{Now: func() time.Time { return t3 }})
		if err != nil {
			t.Fatalf("NewScanner(sc3): %v", err)
		}
		sc4, err := scanner.NewScanner(transactor, scanner.Options{Now: func() time.Time { return t4 }})
		if err != nil {
			t.Fatalf("NewScanner(sc4): %v", err)
		}

		probed3 := make(chan struct{})
		release3 := make(chan struct{})
		var err3 error
		done3 := make(chan struct{})
		go func() {
			defer close(done3)
			_, err3 = sc3.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{
				OpenDir: func(rawDir []byte) (scanner.DirStream, error) {
					f, openErr := os.Open(string(rawDir))
					return &hookAfterCloseDirStream{
						inner: f,
						onClose: func() {
							close(probed3)
							select {
							case <-release3:
							case <-ctx.Done():
							}
						},
					}, openErr
				},
			})
		}()

		select {
		case <-probed3:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for check 3 probe: %v", ctx.Err())
		}

		writeDiskFile(t, filepath.Join(rootDir, "book.cbz"), bookBytes)
		res4, err4 := sc4.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{})
		if err4 != nil || res4.State != domain.LibraryRootStateOnline {
			t.Fatalf("newer recovery check 4 = (%+v, %v), want ONLINE", res4, err4)
		}

		close(release3)
		<-done3
		if !errors.Is(err3, scanner.ErrStalePreCheck) {
			t.Fatalf("older OFFLINE check err = %v, want ErrStalePreCheck", err3)
		}
		rootsAfterB := loadRootsForLibrary(ctx, t, transactor, lib.ID)
		if rootsAfterB[0].State != domain.LibraryRootStateOnline ||
			rootsAfterB[0].LastCheckedAt == nil || !rootsAfterB[0].LastCheckedAt.Equal(t4) {
			t.Fatalf("expected DB to retain newer ONLINE verdict at t4, got %+v", rootsAfterB[0])
		}
		evsB := loadOutboxEventsDetailed(ctx, t, db)
		if len(evsB) != 2 || evsB[1].Kind != domain.EventLibraryOnline {
			t.Fatalf("expected exactly [library.offline, library.online], got %+v", evsB)
		}
	})

	t.Run("ScanLibrary rejects root snapshot changed after pre-check without walking unchecked root", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		base := t.TempDir()
		rootA := filepath.Join(base, "root-a")
		rootB := filepath.Join(base, "root-b")
		bookA := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("a"), method: zip.Store})
		bookB := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("b"), method: zip.Store})
		writeDiskFile(t, filepath.Join(rootA, "a.cbz"), bookA)
		writeDiskFile(t, filepath.Join(rootB, "b.cbz"), bookB)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Post-Check Root Drift Lib", nil, []byte(rootA))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		var rootBAdded atomic.Bool
		driftTx := &postPreCheckRootAddTransactor{
			inner: transactor,
			onPostPreCheck: func() error {
				if !rootBAdded.CompareAndSwap(false, true) {
					return nil
				}
				return transactor.WithTx(ctx, func(tx storage.Tx) error {
					rp, err := domain.NewRootPathIdentity([]byte(rootB))
					if err != nil {
						return err
					}
					return tx.Libraries().CreateLibraryRoot(ctx, &domain.LibraryRoot{
						LibraryID: lib.ID,
						Path:      rp,
						State:     domain.LibraryRootStateOnline,
					})
				})
			},
		}

		sc, err := scanner.NewScanner(driftTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		var openCalls atomic.Int32
		_, err = sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				openCalls.Add(1)
				return os.Open(string(rawPath))
			},
		})
		if !errors.Is(err, scanner.ErrStalePreCheck) {
			t.Fatalf("ScanLibrary after root snapshot drift err = %v, want ErrStalePreCheck", err)
		}
		if openCalls.Load() != 0 {
			t.Fatalf("expected 0 archive opens when snapshot drifted after pre-check, got %d", openCalls.Load())
		}
		if len(listAssetsForRoot(ctx, t, transactor, roots[0].ID)) != 0 {
			t.Fatal("expected 0 indexed assets after ErrStalePreCheck")
		}
	})

	t.Run("missing raw-byte root path with invalid UTF-8 commits valid UTF-8 diagnostic and offline event", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		rawMissing, err := paths.JoinRootRelative([]byte(t.TempDir()), []byte{'b', 'a', 'd', 0xff, 'r', 'o', 'o', 't'})
		if err != nil {
			t.Fatalf("JoinRootRelative: %v", err)
		}

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Invalid UTF8 Raw Root Lib", nil, rawMissing)
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		res, err := sc.PreCheckLibrary(ctx, lib.ID, scanner.PreCheckOptions{})
		if err != nil {
			t.Fatalf("PreCheckLibrary(raw invalid UTF-8): %v", err)
		}
		if res.State != domain.LibraryRootStateOffline || len(res.Roots) != 1 {
			t.Fatalf("expected OFFLINE for missing invalid-UTF-8 root, got %+v", res)
		}
		if !errors.Is(res.Roots[0].Err, os.ErrNotExist) {
			t.Fatalf("expected typed os.ErrNotExist preserved in RootCheckResult.Err, got %v", res.Roots[0].Err)
		}
		if res.Roots[0].LastError == nil || !utf8.ValidString(*res.Roots[0].LastError) || !strings.Contains(*res.Roots[0].LastError, `\xff`) {
			t.Fatalf("expected valid UTF-8 escaped diagnostic containing \\xff, got %v", res.Roots[0].LastError)
		}

		gotRoots := loadRootsForLibrary(ctx, t, transactor, lib.ID)
		if gotRoots[0].ID != roots[0].ID ||
			gotRoots[0].State != domain.LibraryRootStateOffline ||
			gotRoots[0].LastError == nil ||
			*gotRoots[0].LastError != *res.Roots[0].LastError {
			t.Fatalf("unexpected persisted root state: %+v", gotRoots[0])
		}
		evs := loadOutboxEventsDetailed(ctx, t, db)
		if len(evs) != 1 || evs[0].Kind != domain.EventLibraryOffline || evs[0].AggregateID != lib.ID {
			t.Fatalf("expected 1 library.offline event, got %+v", evs)
		}
	})
}

type hookAfterCloseDirStream struct {
	inner   *os.File
	onClose func()
	once    sync.Once
}

func (s *hookAfterCloseDirStream) ReadDir(n int) ([]os.DirEntry, error) {
	return s.inner.ReadDir(n)
}

func (s *hookAfterCloseDirStream) Close() error {
	err := s.inner.Close()
	if s.onClose != nil {
		s.once.Do(s.onClose)
	}
	return err
}

type postPreCheckRootAddTransactor struct {
	inner          storage.Transactor
	withTxCalls    atomic.Int32
	onPostPreCheck func() error
}

func (p *postPreCheckRootAddTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	// Call 1: PreCheckLibrary initial load; Call 2: ScanLibrary snapshot recheck right after pre-check commit.
	if p.withTxCalls.Add(1) == 2 && p.onPostPreCheck != nil {
		if err := p.onPostPreCheck(); err != nil {
			return err
		}
	}
	return p.inner.WithTx(ctx, fn)
}

func (p *postPreCheckRootAddTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return p.inner.WithReadCommittedTx(ctx, fn)
}

type sliceTestDirStream struct {
	entries []os.DirEntry
	pos     int
}

func (s *sliceTestDirStream) ReadDir(n int) ([]os.DirEntry, error) {
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

func (s *sliceTestDirStream) Close() error { return nil }
