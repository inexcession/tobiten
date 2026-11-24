package image

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

		for _, failDir := range []string{nestedParent, coversDir, shard1Dir} {
			t.Run(filepath.Base(failDir), func(t *testing.T) {
				if err := os.RemoveAll(nestedParent); err != nil {
					t.Fatalf("RemoveAll: %v", err)
				}
				cache, err := NewCoverCache(coversDir)
				if err != nil {
					t.Fatalf("NewCoverCache: %v", err)
				}

				var synced []string
				failOnce := true
				origSyncDir := cache.ops.syncDir
				cache.ops.syncDir = func(dir string) error {
					if failOnce && dir == failDir {
						failOnce = false
						return boom
					}
					synced = append(synced, dir)
					return origSyncDir(dir)
				}

				key := ArchiveCoverKey("abcdef0123456789", 0, DefaultCoverOptions())
				if _, err := cache.Put(ctx, key, []byte("retry-cover")); !errors.Is(err, boom) {
					t.Fatalf("first Put err = %v, want %v", err, boom)
				}

				synced = nil
				if _, err := cache.Put(ctx, key, []byte("retry-cover")); err != nil {
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
	})

	t.Run("rekey_retries_pending_target_source_and_prune_syncs", func(t *testing.T) {
		oldDigest := "1234567890abcdef"
		newDigest := "fedcba0987654321"

		cases := []struct {
			name         string
			seedDestDir  bool
			wantRetryMov int
		}{
			{name: "toDir", wantRetryMov: 0},
			{name: "fromDir", wantRetryMov: 0},
			{name: "fromShard1", wantRetryMov: 0},
			{name: "rootDir_before_rename", wantRetryMov: 1},
			{name: "rootDir_after_prune", seedDestDir: true, wantRetryMov: 0},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				coversDir := t.TempDir()
				cache, err := NewCoverCache(coversDir)
				if err != nil {
					t.Fatalf("NewCoverCache: %v", err)
				}

				oldKey := ArchiveCoverKey(oldDigest, 0, DefaultCoverOptions())
				if _, err := cache.Put(ctx, oldKey, []byte("rekey-retry-bytes")); err != nil {
					t.Fatalf("Put: %v", err)
				}
				if tc.seedDestDir {
					if err := os.MkdirAll(filepath.Join(coversDir, "fe", "dc"), 0o755); err != nil {
						t.Fatalf("MkdirAll: %v", err)
					}
				}

				var failTarget string
				switch tc.name {
				case "toDir":
					failTarget = filepath.Join(coversDir, "fe", "dc")
				case "fromDir":
					failTarget = filepath.Join(coversDir, "12", "34")
				case "fromShard1":
					failTarget = filepath.Join(coversDir, "12")
				case "rootDir_before_rename", "rootDir_after_prune":
					failTarget = coversDir
				}

				var synced []string
				failOnce := true
				origSyncDir := cache.ops.syncDir
				cache.ops.syncDir = func(dir string) error {
					if failOnce && dir == failTarget {
						failOnce = false
						return boom
					}
					synced = append(synced, dir)
					return origSyncDir(dir)
				}

				if _, err := cache.Rekey(ctx, ArchiveSource(oldDigest), ArchiveSource(newDigest)); !errors.Is(err, boom) {
					t.Fatalf("first Rekey err = %v, want %v", err, boom)
				}

				synced = nil
				moved, err := cache.Rekey(ctx, ArchiveSource(oldDigest), ArchiveSource(newDigest))
				if err != nil || moved != tc.wantRetryMov {
					t.Fatalf("retry Rekey = (%d, %v), want (%d, nil)", moved, err, tc.wantRetryMov)
				}
				if !slices.Contains(synced, failTarget) {
					t.Errorf("retry Rekey skipped syncDir(%q); synced=%v", failTarget, synced)
				}
				if _, err := cache.Get(ctx, ArchiveCoverKey(newDigest, 0, DefaultCoverOptions())); err != nil {
					t.Errorf("Get(newDigest) after retry Rekey: %v", err)
				}
			})
		}
	})

	t.Run("invalidate_retries_pending_leaf_shard_and_root_syncs", func(t *testing.T) {
		digest := "aabbccddeeff0011"
		for _, targetKind := range []string{"leafDir", "shard1Dir", "rootDir"} {
			t.Run(targetKind, func(t *testing.T) {
				coversDir := t.TempDir()
				cache, err := NewCoverCache(coversDir)
				if err != nil {
					t.Fatalf("NewCoverCache: %v", err)
				}

				key := ArchiveCoverKey(digest, 0, DefaultCoverOptions())
				if _, err := cache.Put(ctx, key, []byte("invalidate-retry-bytes")); err != nil {
					t.Fatalf("Put: %v", err)
				}

				var failTarget string
				switch targetKind {
				case "leafDir":
					failTarget = filepath.Join(coversDir, "aa", "bb")
				case "shard1Dir":
					failTarget = filepath.Join(coversDir, "aa")
				case "rootDir":
					failTarget = coversDir
				}

				var synced []string
				failOnce := true
				origSyncDir := cache.ops.syncDir
				cache.ops.syncDir = func(dir string) error {
					if failOnce && dir == failTarget {
						failOnce = false
						return boom
					}
					synced = append(synced, dir)
					return origSyncDir(dir)
				}

				if _, err := cache.Invalidate(ctx, ArchiveSource(digest)); !errors.Is(err, boom) {
					t.Fatalf("first Invalidate err = %v, want %v", err, boom)
				}

				synced = nil
				if removed, err := cache.Invalidate(ctx, ArchiveSource(digest)); err != nil || removed != 0 {
					t.Fatalf("retry Invalidate = (%d, %v), want (0, nil)", removed, err)
				}
				if !slices.Contains(synced, failTarget) {
					t.Errorf("retry Invalidate skipped syncDir(%q); synced=%v", failTarget, synced)
				}
				if _, err := os.Stat(filepath.Join(coversDir, "aa")); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("empty shard1 dir survived retry Invalidate: %v", err)
				}
			})
		}
	})

	t.Run("cleanup_temp_retries_pending_leaf_and_shard_syncs", func(t *testing.T) {
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

		var synced []string
		failOnce := true
		origSyncDir := cache.ops.syncDir
		cache.ops.syncDir = func(dir string) error {
			if failOnce && dir == leafDir {
				failOnce = false
				return boom
			}
			synced = append(synced, dir)
			return origSyncDir(dir)
		}

		if _, err := cache.CleanupTemp(ctx); !errors.Is(err, boom) {
			t.Fatalf("first CleanupTemp err = %v, want %v", err, boom)
		}

		synced = nil
		if removed, err := cache.CleanupTemp(ctx); err != nil || removed != 0 {
			t.Fatalf("retry CleanupTemp = (%d, %v), want (0, nil)", removed, err)
		}
		if !slices.Contains(synced, leafDir) {
			t.Errorf("retry CleanupTemp skipped syncDir(%q); synced=%v", leafDir, synced)
		}
		if _, err := os.Stat(filepath.Join(coversDir, "aa")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("empty shard1 dir survived retry CleanupTemp: %v", err)
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
