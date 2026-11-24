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
	if !strings.HasPrefix(filepath.Base(createdTemp), tempFilePrefix) {
		t.Errorf("temp file %q does not start with %q", createdTemp, tempFilePrefix)
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

func mustNormalize(t *testing.T, k CoverKey) CoverKey {
	t.Helper()
	norm, err := k.Normalize()
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	return norm
}
