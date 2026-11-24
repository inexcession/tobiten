package image

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

type simulatedCrashPanic struct {
	stage string
}

func TestCoverCache_WriteProtocolOrder(t *testing.T) {
	coversDir := t.TempDir()
	cache, err := NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	var trace []string
	var createdTemp string
	var renamedFrom, renamedTo string
	var syncedDir string

	origOps := cache.ops
	cache.ops.createTemp = func(dir, pattern string) (*os.File, error) {
		trace = append(trace, "createTemp")
		f, err := origOps.createTemp(dir, pattern)
		if err == nil {
			createdTemp = f.Name()
		}
		return f, err
	}
	cache.ops.writeAll = func(f *os.File, data []byte) error {
		trace = append(trace, "writeAll")
		return origOps.writeAll(f, data)
	}
	cache.ops.syncFile = func(f *os.File) error {
		trace = append(trace, "syncFile")
		return origOps.syncFile(f)
	}
	cache.ops.closeFile = func(f *os.File) error {
		trace = append(trace, "closeFile")
		return origOps.closeFile(f)
	}
	cache.ops.rename = func(oldPath, newPath string) error {
		trace = append(trace, "rename")
		renamedFrom = oldPath
		renamedTo = newPath
		return origOps.rename(oldPath, newPath)
	}
	cache.ops.syncDir = func(dir string) error {
		// Record only the final shard directory sync after rename (ignore parent
		// directory syncs during initial shard directory creation).
		if slices.Contains(trace, "rename") {
			trace = append(trace, "syncDir")
			syncedDir = dir
		}
		return origOps.syncDir(dir)
	}

	key := ArchiveCoverKey("abcdef0123456789", 0, DefaultCoverOptions())
	payload := []byte("complete-jpeg-cover-bytes")
	entry, err := cache.Put(context.Background(), key, payload)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	wantTrace := []string{"createTemp", "writeAll", "syncFile", "closeFile", "rename", "syncDir"}
	if !slices.Equal(trace, wantTrace) {
		t.Fatalf("write protocol order = %v, want %v", trace, wantTrace)
	}
	if !strings.HasPrefix(filepath.Base(createdTemp), coverTempPrefix) {
		t.Errorf("temp file %q does not start with %q", createdTemp, coverTempPrefix)
	}
	if filepath.Dir(createdTemp) != filepath.Dir(entry.Path) {
		t.Errorf("temp dir %q != final dir %q", filepath.Dir(createdTemp), filepath.Dir(entry.Path))
	}
	if renamedFrom != createdTemp || renamedTo != entry.Path {
		t.Errorf("rename = (%q -> %q), want (%q -> %q)", renamedFrom, renamedTo, createdTemp, entry.Path)
	}
	if syncedDir != filepath.Dir(entry.Path) {
		t.Errorf("syncedDir = %q, want %q", syncedDir, filepath.Dir(entry.Path))
	}
}

func TestCoverCache_SimulatedCrashLeavesNoPartialEntryAndCleansTemp(t *testing.T) {
	ctx := context.Background()
	initialPayload := []byte("prior-complete-cover")
	newPayload := []byte("new-complete-cover-payload")

	stages := []struct {
		name           string
		failAt         string
		preExisting    bool
		expectNewEntry bool
		expectTempLeft bool
	}{
		{
			name:           "crash_mid_write_fresh_key",
			failAt:         "writeAll",
			preExisting:    false,
			expectNewEntry: false,
			expectTempLeft: true,
		},
		{
			name:           "crash_before_rename_fresh_key",
			failAt:         "syncFile",
			preExisting:    false,
			expectNewEntry: false,
			expectTempLeft: true,
		},
		{
			name:           "crash_during_rename_preserves_existing",
			failAt:         "rename",
			preExisting:    true,
			expectNewEntry: false,
			expectTempLeft: true,
		},
		{
			name:           "crash_after_rename_before_dir_sync_leaves_complete_entry",
			failAt:         "syncDir",
			preExisting:    false,
			expectNewEntry: true,
			expectTempLeft: false,
		},
	}

	for _, tc := range stages {
		t.Run(tc.name, func(t *testing.T) {
			coversDir := t.TempDir()
			cache, err := NewCoverCache(coversDir)
			if err != nil {
				t.Fatalf("NewCoverCache: %v", err)
			}

			key := ArchiveCoverKey("1122334455667788", 0, DefaultCoverOptions())
			if tc.preExisting {
				if _, err := cache.Put(ctx, key, initialPayload); err != nil {
					t.Fatalf("seed Put: %v", err)
				}
			}

			origOps := cache.ops
			var openCrashFile *os.File
			crashWithPartial := func(stage string) {
				panic(simulatedCrashPanic{stage: stage})
			}

			switch tc.failAt {
			case "writeAll":
				cache.ops.writeAll = func(f *os.File, data []byte) error {
					openCrashFile = f
					// Write only partial bytes before crashing without cleanup.
					_, _ = f.Write(data[:len(data)/2])
					crashWithPartial("writeAll")
					return nil
				}
			case "syncFile":
				cache.ops.syncFile = func(f *os.File) error {
					openCrashFile = f
					crashWithPartial("syncFile")
					return nil
				}
			case "rename":
				cache.ops.rename = func(oldPath, newPath string) error {
					crashWithPartial("rename")
					return nil
				}
			case "syncDir":
				cache.ops.syncDir = func(dir string) error {
					if dir == filepath.Dir(cache.entryPath(mustNormalize(t, key))) {
						crashWithPartial("syncDir")
					}
					return origOps.syncDir(dir)
				}
			}

			func() {
				defer func() {
					r := recover()
					if r == nil {
						t.Fatalf("expected simulated crash panic at %s", tc.failAt)
					}
					if _, ok := r.(simulatedCrashPanic); !ok {
						panic(r)
					}
				}()
				_, _ = cache.Put(ctx, key, newPayload)
			}()

			if openCrashFile != nil {
				_ = openCrashFile.Close()
			}

			// Simulate process restart with a fresh CoverCache handle.
			restarted, err := NewCoverCache(coversDir)
			if err != nil {
				t.Fatalf("NewCoverCache after crash: %v", err)
			}

			got, err := restarted.Get(ctx, key)
			switch {
			case tc.expectNewEntry:
				if err != nil {
					t.Fatalf("Get after post-rename crash: %v", err)
				}
				if !bytes.Equal(got.Data, newPayload) {
					t.Fatalf("Get payload = %q, want %q", got.Data, newPayload)
				}
			case tc.preExisting:
				if err != nil {
					t.Fatalf("Get existing entry after pre-rename crash: %v", err)
				}
				if !bytes.Equal(got.Data, initialPayload) {
					t.Fatalf("Get payload = %q, want intact %q", got.Data, initialPayload)
				}
			default:
				if !errors.Is(err, ErrCacheMiss) {
					t.Fatalf("Get after pre-rename crash = (%v, %v), want ErrCacheMiss", got, err)
				}
			}

			// Leftover temp files must be ignored by Get/Stat and removed by CleanupTemp.
			removed, err := restarted.CleanupTemp(ctx)
			if err != nil {
				t.Fatalf("CleanupTemp: %v", err)
			}
			if tc.expectTempLeft && removed != 1 {
				t.Errorf("CleanupTemp removed = %d, want 1", removed)
			}
			if !tc.expectTempLeft && removed != 0 {
				t.Errorf("CleanupTemp removed = %d, want 0", removed)
			}

			removedAgain, err := restarted.CleanupTemp(ctx)
			if err != nil {
				t.Fatalf("second CleanupTemp: %v", err)
			}
			if removedAgain != 0 {
				t.Errorf("second CleanupTemp removed = %d, want 0", removedAgain)
			}
		})
	}

	for _, tc := range []struct {
		name     string
		crashDir string
	}{
		{name: "crash_after_covers_root_mkdir_before_parent_sync", crashDir: "nested"},
		{name: "crash_after_nested_ancestor_mkdir_before_parent_sync", crashDir: "base"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			nested := filepath.Join(base, "nested")
			coversDir := filepath.Join(nested, "covers")

			cache, err := NewCoverCache(coversDir)
			if err != nil {
				t.Fatalf("NewCoverCache: %v", err)
			}

			targetParent := base
			if tc.crashDir == "nested" {
				targetParent = nested
			}
			origSyncDir := cache.ops.syncDir
			cache.ops.syncDir = func(dir string) error {
				if dir == targetParent {
					// Simulate abrupt process crash after Mkdir(child) and before syncDir(parent).
					panic(simulatedCrashPanic{stage: "root_mkdir_parent_sync"})
				}
				return origSyncDir(dir)
			}

			key := ArchiveCoverKey("1122334455667788", 0, DefaultCoverOptions())
			func() {
				defer func() {
					r := recover()
					if r == nil {
						t.Fatalf("expected simulated crash panic at %s", targetParent)
					}
					if _, ok := r.(simulatedCrashPanic); !ok {
						panic(r)
					}
				}()
				_, _ = cache.Put(ctx, key, []byte("interrupted-root-cover"))
			}()

			// Reopen with a fresh CoverCache handle; interrupted parent sync must complete before Put succeeds.
			reopened, err := NewCoverCache(coversDir)
			if err != nil {
				t.Fatalf("reopen NewCoverCache: %v", err)
			}
			var synced []string
			reopenOrigSync := reopened.ops.syncDir
			reopened.ops.syncDir = func(dir string) error {
				synced = append(synced, dir)
				return reopenOrigSync(dir)
			}

			if _, err := reopened.Put(ctx, key, []byte("interrupted-root-cover")); err != nil {
				t.Fatalf("Put after reopened interrupted root creation: %v", err)
			}
			if !slices.Contains(synced, targetParent) {
				t.Fatalf("reopened Put did not sync interrupted parent %q; synced=%v", targetParent, synced)
			}
			got, err := reopened.Get(ctx, key)
			if err != nil || string(got.Data) != "interrupted-root-cover" {
				t.Fatalf("Get after reopened Put = (%v, %v)", got, err)
			}
		})
	}
}

func TestCoverCache_PutErrorRemovesOwnTempFile(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("injected io failure")

	for _, stage := range []string{"writeAll", "syncFile", "closeFile", "rename"} {
		t.Run(stage, func(t *testing.T) {
			coversDir := t.TempDir()
			cache, err := NewCoverCache(coversDir)
			if err != nil {
				t.Fatalf("NewCoverCache: %v", err)
			}

			switch stage {
			case "writeAll":
				cache.ops.writeAll = func(*os.File, []byte) error { return boom }
			case "syncFile":
				cache.ops.syncFile = func(*os.File) error { return boom }
			case "closeFile":
				origClose := cache.ops.closeFile
				cache.ops.closeFile = func(f *os.File) error {
					_ = origClose(f)
					return boom
				}
			case "rename":
				cache.ops.rename = func(string, string) error { return boom }
			}

			key := ArchiveCoverKey("aabbccddeeff0011", 0, DefaultCoverOptions())
			_, err = cache.Put(ctx, key, []byte("cover-bytes"))
			if !errors.Is(err, boom) {
				t.Fatalf("Put error = %v, want %v", err, boom)
			}

			if _, err := cache.Get(ctx, key); !errors.Is(err, ErrCacheMiss) {
				t.Fatalf("Get after failed Put = %v, want ErrCacheMiss", err)
			}
			removed, err := cache.CleanupTemp(ctx)
			if err != nil {
				t.Fatalf("CleanupTemp: %v", err)
			}
			if removed != 0 {
				t.Errorf("CleanupTemp removed = %d, want 0 (failed Put should remove its own temp)", removed)
			}
		})
	}
}

func TestCoverCache_FreshNestedRootSyncsAllCreatedDirectories(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	coversDir := filepath.Join(base, "nested", "storage", "covers")

	cache, err := NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	var synced []string
	origSyncDir := cache.ops.syncDir
	cache.ops.syncDir = func(dir string) error {
		synced = append(synced, dir)
		return origSyncDir(dir)
	}

	key := ArchiveCoverKey("abcdef0123456789", 0, DefaultCoverOptions())
	if _, err := cache.Put(ctx, key, []byte("nested-root-cover")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	wantSynced := []string{
		filepath.Dir(base),
		base,
		filepath.Join(base, "nested"),
		filepath.Join(base, "nested", "storage"),
		coversDir,
		filepath.Join(coversDir, "ab"),
		filepath.Join(coversDir, "ab", "cd"),
	}
	if !slices.Equal(synced, wantSynced) {
		t.Fatalf("synced dirs = %v, want %v", synced, wantSynced)
	}
}

func TestCoverCache_DirectorySyncFailuresFinishOnRetry(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("injected syncDir failure")

	t.Run("put_retries_root_ancestor_and_shard_parent_syncs", func(t *testing.T) {
		base := t.TempDir()
		nestedParent := filepath.Join(base, "nested")
		coversDir := filepath.Join(nestedParent, "covers")
		shard1Dir := filepath.Join(coversDir, "ab")

		for _, reopen := range []bool{false, true} {
			for _, failDir := range []string{nestedParent, coversDir, shard1Dir} {
				name := filepath.Base(failDir)
				if reopen {
					name += "_reopen"
				}
				t.Run(name, func(t *testing.T) {
					if err := os.RemoveAll(nestedParent); err != nil {
						t.Fatalf("RemoveAll: %v", err)
					}
					cache, err := NewCoverCache(coversDir)
					if err != nil {
						t.Fatalf("NewCoverCache: %v", err)
					}

					failOnce := true
					origSyncDir := cache.ops.syncDir
					cache.ops.syncDir = func(dir string) error {
						if failOnce && dir == failDir {
							failOnce = false
							return boom
						}
						return origSyncDir(dir)
					}

					key := ArchiveCoverKey("abcdef0123456789", 0, DefaultCoverOptions())
					if _, err := cache.Put(ctx, key, []byte("retry-cover")); !errors.Is(err, boom) {
						t.Fatalf("first Put err = %v, want %v", err, boom)
					}

					retryCache := cache
					if reopen {
						retryCache, err = NewCoverCache(coversDir)
						if err != nil {
							t.Fatalf("reopen NewCoverCache: %v", err)
						}
					}
					var synced []string
					retryOrigSync := retryCache.ops.syncDir
					retryCache.ops.syncDir = func(dir string) error {
						synced = append(synced, dir)
						return retryOrigSync(dir)
					}

					if _, err := retryCache.Put(ctx, key, []byte("retry-cover")); err != nil {
						t.Fatalf("retry Put err = %v", err)
					}
					if !slices.Contains(synced, failDir) {
						t.Errorf("retry Put skipped missing syncDir(%q); synced=%v", failDir, synced)
					}
					if !slices.Contains(synced, filepath.Join(shard1Dir, "cd")) {
						t.Errorf("retry Put skipped leaf syncDir; synced=%v", synced)
					}
				})
			}
		}
	})

	t.Run("rekey_retries_pending_target_source_and_root_syncs_across_reopen", func(t *testing.T) {
		oldDigest := "1234567890abcdef"
		newDigest := "fedcba0987654321"

		cases := []struct {
			name         string
			wantRetryMov int
		}{
			{name: "toDir", wantRetryMov: 0},
			{name: "fromDir", wantRetryMov: 0},
			{name: "rootDir_before_rename", wantRetryMov: 1},
		}

		for _, reopen := range []bool{false, true} {
			for _, tc := range cases {
				name := tc.name
				if reopen {
					name += "_reopen"
				}
				t.Run(name, func(t *testing.T) {
					coversDir := t.TempDir()
					cache, err := NewCoverCache(coversDir)
					if err != nil {
						t.Fatalf("NewCoverCache: %v", err)
					}

					oldKey := ArchiveCoverKey(oldDigest, 0, DefaultCoverOptions())
					if _, err := cache.Put(ctx, oldKey, []byte("rekey-retry-bytes")); err != nil {
						t.Fatalf("Put: %v", err)
					}

					var failTarget string
					switch tc.name {
					case "toDir":
						failTarget = filepath.Join(coversDir, "fe", "dc")
					case "fromDir":
						failTarget = filepath.Join(coversDir, "12", "34")
					case "rootDir_before_rename":
						failTarget = coversDir
					}

					failOnce := true
					origSyncDir := cache.ops.syncDir
					cache.ops.syncDir = func(dir string) error {
						if failOnce && dir == failTarget {
							failOnce = false
							return boom
						}
						return origSyncDir(dir)
					}

					if _, err := cache.Rekey(ctx, ArchiveSource(oldDigest), ArchiveSource(newDigest)); !errors.Is(err, boom) {
						t.Fatalf("first Rekey err = %v, want %v", err, boom)
					}

					retryCache := cache
					if reopen {
						retryCache, err = NewCoverCache(coversDir)
						if err != nil {
							t.Fatalf("reopen NewCoverCache: %v", err)
						}
					}
					var synced []string
					retryOrigSync := retryCache.ops.syncDir
					retryCache.ops.syncDir = func(dir string) error {
						synced = append(synced, dir)
						return retryOrigSync(dir)
					}

					moved, err := retryCache.Rekey(ctx, ArchiveSource(oldDigest), ArchiveSource(newDigest))
					if err != nil || moved != tc.wantRetryMov {
						t.Fatalf("retry Rekey = (%d, %v), want (%d, nil)", moved, err, tc.wantRetryMov)
					}
					if !slices.Contains(synced, failTarget) {
						t.Errorf("retry Rekey skipped syncDir(%q); synced=%v", failTarget, synced)
					}
					if _, err := retryCache.Get(ctx, ArchiveCoverKey(newDigest, 0, DefaultCoverOptions())); err != nil {
						t.Errorf("Get(newDigest) after retry Rekey: %v", err)
					}
				})
			}
		}
	})

	t.Run("invalidate_retries_pending_leaf_sync_across_reopen", func(t *testing.T) {
		digest := "aabbccddeeff0011"
		for _, reopen := range []bool{false, true} {
			name := "same_handle"
			if reopen {
				name = "reopen"
			}
			t.Run(name, func(t *testing.T) {
				coversDir := t.TempDir()
				cache, err := NewCoverCache(coversDir)
				if err != nil {
					t.Fatalf("NewCoverCache: %v", err)
				}

				key := ArchiveCoverKey(digest, 0, DefaultCoverOptions())
				if _, err := cache.Put(ctx, key, []byte("invalidate-retry-bytes")); err != nil {
					t.Fatalf("Put: %v", err)
				}

				leafDir := filepath.Join(coversDir, "aa", "bb")
				failOnce := true
				origSyncDir := cache.ops.syncDir
				cache.ops.syncDir = func(dir string) error {
					if failOnce && dir == leafDir {
						failOnce = false
						return boom
					}
					return origSyncDir(dir)
				}

				if _, err := cache.Invalidate(ctx, ArchiveSource(digest)); !errors.Is(err, boom) {
					t.Fatalf("first Invalidate err = %v, want %v", err, boom)
				}

				retryCache := cache
				if reopen {
					retryCache, err = NewCoverCache(coversDir)
					if err != nil {
						t.Fatalf("reopen NewCoverCache: %v", err)
					}
				}
				var synced []string
				retryOrigSync := retryCache.ops.syncDir
				retryCache.ops.syncDir = func(dir string) error {
					synced = append(synced, dir)
					return retryOrigSync(dir)
				}

				if removed, err := retryCache.Invalidate(ctx, ArchiveSource(digest)); err != nil || removed != 0 {
					t.Fatalf("retry Invalidate = (%d, %v), want (0, nil)", removed, err)
				}
				if !slices.Contains(synced, leafDir) {
					t.Errorf("retry Invalidate skipped syncDir(%q); synced=%v", leafDir, synced)
				}
			})
		}
	})

	t.Run("cleanup_temp_retries_pending_leaf_sync_across_reopen", func(t *testing.T) {
		for _, reopen := range []bool{false, true} {
			name := "same_handle"
			if reopen {
				name = "reopen"
			}
			t.Run(name, func(t *testing.T) {
				coversDir := t.TempDir()
				cache, err := NewCoverCache(coversDir)
				if err != nil {
					t.Fatalf("NewCoverCache: %v", err)
				}

				leafDir := filepath.Join(coversDir, "aa", "bb")
				if err := os.MkdirAll(leafDir, 0o755); err != nil {
					t.Fatalf("MkdirAll: %v", err)
				}
				if err := os.WriteFile(filepath.Join(leafDir, coverTempPrefix+"orphan"), []byte("tmp"), 0o600); err != nil {
					t.Fatalf("WriteFile: %v", err)
				}

				failOnce := true
				origSyncDir := cache.ops.syncDir
				cache.ops.syncDir = func(dir string) error {
					if failOnce && dir == leafDir {
						failOnce = false
						return boom
					}
					return origSyncDir(dir)
				}

				if _, err := cache.CleanupTemp(ctx); !errors.Is(err, boom) {
					t.Fatalf("first CleanupTemp err = %v, want %v", err, boom)
				}

				retryCache := cache
				if reopen {
					retryCache, err = NewCoverCache(coversDir)
					if err != nil {
						t.Fatalf("reopen NewCoverCache: %v", err)
					}
				}
				var synced []string
				retryOrigSync := retryCache.ops.syncDir
				retryCache.ops.syncDir = func(dir string) error {
					synced = append(synced, dir)
					return retryOrigSync(dir)
				}

				if removed, err := retryCache.CleanupTemp(ctx); err != nil || removed != 0 {
					t.Fatalf("retry CleanupTemp = (%d, %v), want (0, nil)", removed, err)
				}
				if !slices.Contains(synced, leafDir) {
					t.Errorf("retry CleanupTemp skipped syncDir(%q); synced=%v", leafDir, synced)
				}
			})
		}
	})

	t.Run("concurrent_overlapping_shard_put_retry_syncs_root", func(t *testing.T) {
		coversDir := t.TempDir()
		cache, err := NewCoverCache(coversDir)
		if err != nil {
			t.Fatalf("NewCoverCache: %v", err)
		}

		firstSyncEntered := make(chan struct{})
		releaseFirstSync := make(chan struct{})
		failSecondRootSync := make(chan struct{})

		origSyncDir := cache.ops.syncDir
		var rootCalls int
		var retrySynced []string
		var mu sync.Mutex
		recordingRetry := false

		cache.ops.syncDir = func(dir string) error {
			if dir == coversDir {
				mu.Lock()
				callIdx := rootCalls
				rootCalls++
				isRetry := recordingRetry
				if isRetry {
					retrySynced = append(retrySynced, dir)
				}
				mu.Unlock()

				if !isRetry {
					switch callIdx {
					case 0:
						close(firstSyncEntered)
						<-releaseFirstSync
					case 1:
						close(failSecondRootSync)
						return boom
					}
				}
			}
			return origSyncDir(dir)
		}

		errCh1 := make(chan error, 1)
		go func() {
			_, err := cache.Put(ctx, ArchiveCoverKey("aa11223344556677", 0, DefaultCoverOptions()), []byte("shard-aa"))
			errCh1 <- err
		}()

		<-firstSyncEntered

		errCh2 := make(chan error, 1)
		go func() {
			_, err := cache.Put(ctx, ArchiveCoverKey("bb11223344556677", 0, DefaultCoverOptions()), []byte("shard-bb"))
			errCh2 <- err
		}()

		<-failSecondRootSync
		close(releaseFirstSync)

		if err := <-errCh1; err != nil {
			t.Fatalf("first concurrent Put failed: %v", err)
		}
		if err := <-errCh2; !errors.Is(err, boom) {
			t.Fatalf("second concurrent Put err = %v, want %v", err, boom)
		}

		mu.Lock()
		recordingRetry = true
		mu.Unlock()

		if _, err := cache.Put(ctx, ArchiveCoverKey("bb11223344556677", 0, DefaultCoverOptions()), []byte("shard-bb")); err != nil {
			t.Fatalf("retry Put(shard-bb) failed: %v", err)
		}
		mu.Lock()
		gotRetrySynced := slices.Clone(retrySynced)
		mu.Unlock()
		if !slices.Contains(gotRetrySynced, coversDir) {
			t.Fatalf("retry Put(shard-bb) skipped syncDir(%q) after overlapping shard sync; retrySynced=%v", coversDir, gotRetrySynced)
		}
	})

	t.Run("rekey_same_shard_collision_only_syncs_and_retries", func(t *testing.T) {
		coversDir := t.TempDir()
		cache, err := NewCoverCache(coversDir)
		if err != nil {
			t.Fatalf("NewCoverCache: %v", err)
		}

		const oldDigest = "abcd111122223333"
		const newDigest = "abcd444455556666"
		opts := DefaultCoverOptions()
		oldKey := ArchiveCoverKey(oldDigest, 0, opts)
		newKey := ArchiveCoverKey(newDigest, 0, opts)

		if _, err := cache.Put(ctx, oldKey, []byte("old-same-shard")); err != nil {
			t.Fatalf("Put(old): %v", err)
		}
		if _, err := cache.Put(ctx, newKey, []byte("dest-same-shard")); err != nil {
			t.Fatalf("Put(new): %v", err)
		}

		leafDir := filepath.Join(coversDir, "ab", "cd")
		// Fail the post-unlink syncDir(leafDir) (2nd syncDir(leafDir): 1st is pre-remove dest sync).
		var leafCalls int
		origSyncDir := cache.ops.syncDir
		cache.ops.syncDir = func(dir string) error {
			if dir == leafDir {
				leafCalls++
				if leafCalls == 2 {
					return boom
				}
			}
			return origSyncDir(dir)
		}

		if _, err := cache.Rekey(ctx, ArchiveSource(oldDigest), ArchiveSource(newDigest)); !errors.Is(err, boom) {
			t.Fatalf("first same-shard collision Rekey err = %v, want %v", err, boom)
		}
		if leafCalls != 2 {
			t.Fatalf("same-shard collision Rekey leaf sync calls = %d, want 2 (pre-remove dest + post-remove source)", leafCalls)
		}

		// Retry on reopened handle must sync leafDir and succeed.
		reopened, err := NewCoverCache(coversDir)
		if err != nil {
			t.Fatalf("reopen NewCoverCache: %v", err)
		}
		var retrySynced []string
		reopenOrigSync := reopened.ops.syncDir
		reopened.ops.syncDir = func(dir string) error {
			retrySynced = append(retrySynced, dir)
			return reopenOrigSync(dir)
		}

		if moved, err := reopened.Rekey(ctx, ArchiveSource(oldDigest), ArchiveSource(newDigest)); err != nil || moved != 0 {
			t.Fatalf("retry same-shard collision Rekey = (%d, %v), want (0, nil)", moved, err)
		}
		if !slices.Contains(retrySynced, leafDir) {
			t.Fatalf("retry Rekey skipped syncDir(%q); synced=%v", leafDir, retrySynced)
		}
		got, err := reopened.Get(ctx, newKey)
		if err != nil || string(got.Data) != "dest-same-shard" {
			t.Fatalf("Get(newKey) = (%v, %v), want dest-same-shard", got, err)
		}
		if _, err := reopened.Get(ctx, oldKey); !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("oldKey still present: %v", err)
		}
	})

	t.Run("rekey_collision_retains_source_until_unsynced_destination_is_durable", func(t *testing.T) {
		coversDir := t.TempDir()
		cache, err := NewCoverCache(coversDir)
		if err != nil {
			t.Fatalf("NewCoverCache: %v", err)
		}

		const oldDigest = "1122000011112222"
		const newDigest = "9988000011112222"
		opts := DefaultCoverOptions()
		oldKey := ArchiveCoverKey(oldDigest, 0, opts)
		newKey := ArchiveCoverKey(newDigest, 0, opts)

		if _, err := cache.Put(ctx, oldKey, []byte("durable-source")); err != nil {
			t.Fatalf("Put(old): %v", err)
		}

		destLeafDir := filepath.Join(coversDir, "99", "88")
		sourceLeafDir := filepath.Join(coversDir, "11", "22")
		failDestSync := true
		origSyncDir := cache.ops.syncDir
		var synced []string
		cache.ops.syncDir = func(dir string) error {
			if failDestSync && dir == destLeafDir {
				return boom
			}
			synced = append(synced, dir)
			return origSyncDir(dir)
		}

		// Destination Put leaves file visible in 99/88 while final syncDir(99/88) fails.
		if _, err := cache.Put(ctx, newKey, []byte("unsynced-dest")); !errors.Is(err, boom) {
			t.Fatalf("Put(new) err = %v, want %v", err, boom)
		}

		// With 99/88 sync still failing, cross-shard Rekey must fail and MUST retain oldKey.
		if _, err := cache.Rekey(ctx, ArchiveSource(oldDigest), ArchiveSource(newDigest)); !errors.Is(err, boom) {
			t.Fatalf("Rekey with failing dest sync err = %v, want %v", err, boom)
		}
		gotOld, err := cache.Get(ctx, oldKey)
		if err != nil || string(gotOld.Data) != "durable-source" {
			t.Fatalf("source entry lost when destination durability check failed: (%v, %v)", gotOld, err)
		}

		// Once 99/88 sync recovers, Rekey confirms dest durability, removes oldKey, and syncs 11/22.
		failDestSync = false
		synced = nil
		if moved, err := cache.Rekey(ctx, ArchiveSource(oldDigest), ArchiveSource(newDigest)); err != nil || moved != 0 {
			t.Fatalf("recovered Rekey = (%d, %v), want (0, nil)", moved, err)
		}
		if !slices.Contains(synced, destLeafDir) || !slices.Contains(synced, sourceLeafDir) {
			t.Fatalf("recovered Rekey synced=%v, want both %q and %q", synced, destLeafDir, sourceLeafDir)
		}
		if _, err := cache.Get(ctx, oldKey); !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("superseded oldKey still present after recovered Rekey: %v", err)
		}
		gotNew, err := cache.Get(ctx, newKey)
		if err != nil || string(gotNew.Data) != "unsynced-dest" {
			t.Fatalf("Get(newKey) = (%v, %v), want unsynced-dest", gotNew, err)
		}
	})

	t.Run("cache_created_directory_propagates_permission_error", func(t *testing.T) {
		base := t.TempDir()
		writable := filepath.Join(base, "writable")
		if err := os.MkdirAll(writable, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		coversDir := filepath.Join(writable, "covers")

		cache, err := NewCoverCache(coversDir)
		if err != nil {
			t.Fatalf("NewCoverCache: %v", err)
		}
		origSyncDir := cache.ops.syncDir
		cache.ops.syncDir = func(dir string) error {
			if dir == writable {
				return os.ErrPermission
			}
			return origSyncDir(dir)
		}

		key := ArchiveCoverKey("abcdef0123456789", 0, DefaultCoverOptions())
		if _, err := cache.Put(ctx, key, []byte("perm-fail")); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("Put err = %v, want os.ErrPermission", err)
		}
		if _, err := os.Stat(coversDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("coversDir survived rollback after parent permission failure: %v", err)
		}
	})
}

func mustNormalize(t *testing.T, k CoverKey) CoverKey {
	t.Helper()
	norm, err := k.Normalize()
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	return norm
}
