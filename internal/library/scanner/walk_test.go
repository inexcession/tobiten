package scanner_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/scanner"
)

func writeDiskFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
}

func inspectedMap(res scanner.WalkResult) map[string]scanner.InspectedFile {
	out := make(map[string]scanner.InspectedFile, len(res.Files))
	for _, f := range res.Files {
		out[string(f.RelPath.Bytes)] = f
	}
	return out
}

func unchangedMap(res scanner.WalkResult) map[string]scanner.UnchangedFile {
	out := make(map[string]scanner.UnchangedFile, len(res.Unchanged))
	for _, f := range res.Unchanged {
		out[string(f.RelPath.Bytes)] = f
	}
	return out
}

func skippedMap(res scanner.WalkResult) map[string]scanner.SkippedEntry {
	out := make(map[string]scanner.SkippedEntry, len(res.Skipped))
	for _, s := range res.Skipped {
		out[string(s.RelPath.Bytes)] = s
	}
	return out
}

type fileSnapshot struct {
	isDir   bool
	symlink string
	mtime   time.Time
	bytes   []byte
}

func snapshotTree(t *testing.T, root string) map[string]fileSnapshot {
	t.Helper()
	out := make(map[string]fileSnapshot)
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		snap := fileSnapshot{
			isDir: info.IsDir(),
			mtime: info.ModTime(),
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			snap.symlink = target
		} else if info.Mode().IsRegular() {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			snap.bytes = b
		}
		out[rel] = snap
		return nil
	})
	if err != nil {
		t.Fatalf("snapshotTree(%q): %v", root, err)
	}
	return out
}

func assertTreeUnchanged(t *testing.T, label, root string, before map[string]fileSnapshot) {
	t.Helper()
	after := snapshotTree(t, root)
	if len(after) != len(before) {
		t.Fatalf("%s entry count changed: before=%d after=%d", label, len(before), len(after))
	}
	for rel, b := range before {
		a, ok := after[rel]
		if !ok {
			t.Fatalf("%s missing entry %q after walk", label, rel)
		}
		if a.isDir != b.isDir || a.symlink != b.symlink || !a.mtime.Equal(b.mtime) || !bytes.Equal(a.bytes, b.bytes) {
			t.Fatalf("%s entry %q mutated during walk: before(mtime=%v,len=%d,link=%q) after(mtime=%v,len=%d,link=%q)",
				label, rel, b.mtime, len(b.bytes), b.symlink, a.mtime, len(a.bytes), a.symlink)
		}
	}
}

func TestWalkRoot_SequentialDeterministicDirWalkAndBoundedWorkers(t *testing.T) {
	cases := []struct {
		name        string
		workers     int
		wantWorkers int
	}{
		{name: "configured 1 worker", workers: 1, wantWorkers: 1},
		{name: "configured 2 workers", workers: 2, wantWorkers: 2},
		{name: "zero selects conservative default", workers: 0, wantWorkers: scanner.DefaultInspectionWorkers},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("page1"), method: zip.Store})

			// Create directories in reverse order to verify deterministic sorted order.
			relFiles := []string{
				"zeta/book6.cbz",
				"alpha/sub/book2.cbz",
				"alpha/book1.cbz",
				"beta/book3.cbz",
				"beta/book4.cbz",
				"top.cbz",
			}
			for _, rel := range relFiles {
				writeDiskFile(t, filepath.Join(root, rel), cbzBytes)
			}

			var activeReadDir atomic.Int32
			var maxActiveReadDir atomic.Int32
			var readOrderMu sync.Mutex
			var readOrder []string

			var activeOpen atomic.Int32
			var maxActiveOpen atomic.Int32
			gateReached := make(chan struct{})
			var gateOnce sync.Once
			releaseGate := make(chan struct{})

			opts := scanner.WalkOptions{
				Workers: tc.workers,
				ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
					cur := activeReadDir.Add(1)
					for {
						prev := maxActiveReadDir.Load()
						if cur <= prev || maxActiveReadDir.CompareAndSwap(prev, cur) {
							break
						}
					}
					defer activeReadDir.Add(-1)

					rel, err := filepath.Rel(root, string(rawDir))
					if err != nil {
						rel = string(rawDir)
					}
					readOrderMu.Lock()
					readOrder = append(readOrder, filepath.ToSlash(rel))
					readOrderMu.Unlock()

					return os.ReadDir(string(rawDir))
				},
				Open: func(ctx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
					cur := activeOpen.Add(1)
					for {
						prev := maxActiveOpen.Load()
						if cur <= prev || maxActiveOpen.CompareAndSwap(prev, cur) {
							break
						}
					}
					if int(cur) == tc.wantWorkers {
						gateOnce.Do(func() { close(gateReached) })
					}
					defer activeOpen.Add(-1)

					select {
					case <-releaseGate:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return paths.OpenRaw(rawPath)
				},
			}

			doneCh := make(chan struct{})
			var res scanner.WalkResult
			var walkErr error
			go func() {
				defer close(doneCh)
				res, walkErr = scanner.WalkRoot(context.Background(), []byte(root), opts)
			}()

			select {
			case <-gateReached:
			case <-time.After(5 * time.Second):
				close(releaseGate)
				t.Fatalf("timed out waiting for %d concurrent inspection workers", tc.wantWorkers)
			}

			// Give any stray extra workers a chance to enter Open before releasing the gate.
			time.Sleep(25 * time.Millisecond)
			if got := int(maxActiveOpen.Load()); got != tc.wantWorkers {
				close(releaseGate)
				t.Fatalf("max concurrent opens while blocked = %d, want %d", got, tc.wantWorkers)
			}
			close(releaseGate)

			<-doneCh
			if walkErr != nil {
				t.Fatalf("WalkRoot: %v", walkErr)
			}
			if !res.Complete {
				t.Fatal("WalkRoot reported Complete=false")
			}
			if res.InspectedCount != len(relFiles) || len(res.Files) != len(relFiles) {
				t.Fatalf("inspected = (%d, %d), want %d", res.InspectedCount, len(res.Files), len(relFiles))
			}
			if got := maxActiveReadDir.Load(); got != 1 {
				t.Fatalf("max concurrent ReadDir = %d, want 1", got)
			}
			if got := int(maxActiveOpen.Load()); got > tc.wantWorkers {
				t.Fatalf("max concurrent opens = %d, want <= %d", got, tc.wantWorkers)
			}

			wantOrder := []string{".", "alpha", "alpha/sub", "beta", "zeta"}
			if !slices.Equal(readOrder, wantOrder) {
				t.Fatalf("directory read order = %v, want %v", readOrder, wantOrder)
			}
		})
	}
}

func TestWalkRoot_SizeAndMtimeSkipWithoutOpening(t *testing.T) {
	root := t.TempDir()
	unchangedData := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("unchanged"), method: zip.Store})
	changedSizeData := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("initial"), method: zip.Store})
	changedSizeUpdated := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("longer-payload"), method: zip.Store})
	changedMtimeData := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("same-size"), method: zip.Store})
	nilMtimeData := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("nil-mtime"), method: zip.Store})
	newData := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("brand-new"), method: zip.Store})

	writeDiskFile(t, filepath.Join(root, "unchanged.cbz"), unchangedData)
	writeDiskFile(t, filepath.Join(root, "size_changed.cbz"), changedSizeData)
	writeDiskFile(t, filepath.Join(root, "mtime_changed.cbz"), changedMtimeData)
	writeDiskFile(t, filepath.Join(root, "nil_mtime.cbz"), nilMtimeData)

	first, err := scanner.WalkRoot(context.Background(), []byte(root), scanner.WalkOptions{})
	if err != nil || !first.Complete {
		t.Fatalf("initial WalkRoot = (%+v, %v)", first, err)
	}
	firstByPath := inspectedMap(first)

	// Mutate size_changed.cbz and mtime_changed.cbz, and add new.cbz.
	writeDiskFile(t, filepath.Join(root, "size_changed.cbz"), changedSizeUpdated)
	// Preserve original mtime on size_changed.cbz to prove size change alone triggers re-inspection.
	origSizeMtime := firstByPath["size_changed.cbz"].Mtime
	if err := os.Chtimes(filepath.Join(root, "size_changed.cbz"), origSizeMtime, origSizeMtime); err != nil {
		t.Fatalf("Chtimes(size_changed): %v", err)
	}

	bumpedMtime := firstByPath["mtime_changed.cbz"].Mtime.Add(2 * time.Second)
	if err := os.Chtimes(filepath.Join(root, "mtime_changed.cbz"), bumpedMtime, bumpedMtime); err != nil {
		t.Fatalf("Chtimes(mtime_changed): %v", err)
	}

	// Ensure nanosecond mtime on disk still matches microsecond-truncated PostgreSQL TIMESTAMPTZ known state.
	nanoDiskTime := time.Unix(1700000100, 123456789).UTC()
	if err := os.Chtimes(filepath.Join(root, "unchanged.cbz"), nanoDiskTime, nanoDiskTime); err != nil {
		t.Fatalf("Chtimes(unchanged): %v", err)
	}

	writeDiskFile(t, filepath.Join(root, "new.cbz"), newData)

	unchangedMtime := nanoDiskTime.Truncate(time.Microsecond)
	changedMtimeKnown := firstByPath["mtime_changed.cbz"].Mtime
	known := map[string]scanner.KnownFile{
		"unchanged.cbz":     {Size: firstByPath["unchanged.cbz"].Size, Mtime: &unchangedMtime},
		"size_changed.cbz":  {Size: firstByPath["size_changed.cbz"].Size, Mtime: &origSizeMtime},
		"mtime_changed.cbz": {Size: firstByPath["mtime_changed.cbz"].Size, Mtime: &changedMtimeKnown},
		"nil_mtime.cbz":     {Size: firstByPath["nil_mtime.cbz"].Size, Mtime: nil},
	}

	var openedMu sync.Mutex
	opened := make(map[string]int)
	second, err := scanner.WalkRoot(context.Background(), []byte(root), scanner.WalkOptions{
		Known: known,
		Open: func(ctx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			rel, err := filepath.Rel(root, string(rawPath))
			if err != nil {
				rel = string(rawPath)
			}
			openedMu.Lock()
			opened[filepath.ToSlash(rel)]++
			openedMu.Unlock()
			return paths.OpenRaw(rawPath)
		},
	})
	if err != nil || !second.Complete {
		t.Fatalf("second WalkRoot = (%+v, %v)", second, err)
	}

	if opened["unchanged.cbz"] != 0 {
		t.Fatalf("unchanged.cbz was opened %d times, want 0", opened["unchanged.cbz"])
	}
	for _, wantOpened := range []string{"size_changed.cbz", "mtime_changed.cbz", "nil_mtime.cbz", "new.cbz"} {
		if opened[wantOpened] != 1 {
			t.Errorf("%s opened %d times, want 1", wantOpened, opened[wantOpened])
		}
	}

	unchanged := unchangedMap(second)
	if len(unchanged) != 1 || unchanged["unchanged.cbz"].Size != int64(len(unchangedData)) {
		t.Fatalf("unexpected unchanged entries: %+v", second.Unchanged)
	}
	inspected := inspectedMap(second)
	if len(inspected) != 4 {
		t.Fatalf("inspected count = %d, want 4", len(inspected))
	}
}

func TestWalkRoot_CBZAndComicInfoInspectionFields(t *testing.T) {
	root := t.TempDir()

	comicXML := `<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Series>Dorohedoro</Series>
  <Title>Chaos</Title>
  <Number>01</Number>
  <Volume>1</Volume>
  <Writer>Q Hayashida</Writer>
  <Characters>Caiman, Nikaido</Characters>
  <Pages>
    <Page Image="0" Type="FrontCover Deleted"/>
    <Page Image="1" Type="Advertisement"/>
    <Page Image="2" Type="FrontCover Story"/>
    <Page Image="99" Type="Deleted"/>
  </Pages>
</ComicInfo>`

	richCBZ := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(comicXML), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("ad-cover"), method: zip.Store},
		zipEntry{name: "002.jxl", data: []byte("ad-jxl"), method: zip.Store},
		zipEntry{name: "003.avif", data: []byte("real-cover"), method: zip.Store},
		zipEntry{name: "caf\xe9/004.png", data: []byte("page-4"), method: zip.Store},
		zipEntry{name: "__MACOSX/._001.jpg", data: []byte("junk"), method: zip.Store},
	)
	coverImageCBZ := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>CoverExt</Series><CoverImage>002.png</CoverImage></ComicInfo>`), method: zip.Store},
		zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
		zipEntry{name: "002.png", data: []byte("p2"), method: zip.Store},
	)
	noComicInfoCBZ := buildDeterministicZip(t, "",
		zipEntry{name: "10.jpg", data: []byte("p10"), method: zip.Store},
		zipEntry{name: "2.jpg", data: []byte("p2"), method: zip.Store},
	)
	badComicInfoCBZ := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Broken`), method: zip.Store},
		zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store},
	)
	emptyCBZ := buildDeterministicZip(t, "")

	writeDiskFile(t, filepath.Join(root, "Dorohedoro/Vol 01.cbz"), richCBZ)
	writeDiskFile(t, filepath.Join(root, "CoverExt.cbz"), coverImageCBZ)
	writeDiskFile(t, filepath.Join(root, "Plain.cbz"), noComicInfoCBZ)
	writeDiskFile(t, filepath.Join(root, "BadMeta.cbz"), badComicInfoCBZ)
	writeDiskFile(t, filepath.Join(root, "Empty.cbz"), emptyCBZ)

	res, err := scanner.WalkRoot(context.Background(), []byte(root), scanner.WalkOptions{})
	if err != nil || !res.Complete {
		t.Fatalf("WalkRoot = (%+v, %v)", res, err)
	}
	if res.SkippedCount != 0 {
		t.Fatalf("unexpected skipped entries: %+v", res.Skipped)
	}

	byPath := inspectedMap(res)
	rich := byPath["Dorohedoro/Vol 01.cbz"]
	wantDigest, err := scanner.PartialFingerprint(bytes.NewReader(richCBZ), int64(len(richCBZ)))
	if err != nil {
		t.Fatalf("PartialFingerprint: %v", err)
	}
	if string(rich.RelPath.Bytes) != "Dorohedoro/Vol 01.cbz" ||
		rich.RelPath.Display != "Dorohedoro/Vol 01.cbz" ||
		rich.RelPath.Match != "Dorohedoro/Vol 01.cbz" ||
		rich.RelPath.InvalidUTF8 {
		t.Fatalf("unexpected RelPath: %+v", rich.RelPath)
	}
	if rich.Size != int64(len(richCBZ)) || rich.Mtime.IsZero() {
		t.Fatalf("unexpected Size/Mtime: %d / %v", rich.Size, rich.Mtime)
	}
	if rich.Format != domain.MediaFormatCBZ || rich.DigestKind != domain.DigestKindXXH364Partial || rich.Digest != wantDigest {
		t.Fatalf("unexpected format/digest: format=%s kind=%s got=%s want=%s", rich.Format, rich.DigestKind, rich.Digest, wantDigest)
	}
	if rich.PageCount != 4 || rich.CoverPageIndex != 2 {
		t.Fatalf("PageCount=%d CoverPageIndex=%d, want 4 and 2 (skipping suppressed FrontCover)", rich.PageCount, rich.CoverPageIndex)
	}
	if !slices.Equal(rich.SuppressedPages, []int{0, 1}) {
		t.Fatalf("SuppressedPages = %v, want [0 1]", rich.SuppressedPages)
	}
	if !slices.Equal(rich.ImageCodecs, []string{"jxl", "avif"}) {
		t.Fatalf("ImageCodecs = %v, want [jxl avif]", rich.ImageCodecs)
	}
	if !rich.ArchiveInvalidUTF8 {
		t.Fatal("ArchiveInvalidUTF8 = false, want true for non-UTF-8 entry inside archive")
	}
	if rich.ComicInfo == nil || rich.ComicInfo.Series != "Dorohedoro" || rich.ComicInfo.Number != "01" ||
		rich.ComicInfo.Volume == nil || *rich.ComicInfo.Volume != 1 || !slices.Equal(rich.ComicInfo.Characters, []string{"Caiman", "Nikaido"}) {
		t.Fatalf("unexpected ComicInfo: %+v", rich.ComicInfo)
	}
	if rich.ComicInfoErr != nil {
		t.Fatalf("unexpected ComicInfoErr: %v", rich.ComicInfoErr)
	}

	ext := byPath["CoverExt.cbz"]
	if ext.CoverPageIndex != 1 {
		t.Fatalf("CoverExt CoverPageIndex = %d, want 1 via CoverImage", ext.CoverPageIndex)
	}

	plain := byPath["Plain.cbz"]
	if plain.PageCount != 2 || plain.CoverPageIndex != 0 || len(plain.SuppressedPages) != 0 || plain.ComicInfo != nil {
		t.Fatalf("unexpected Plain.cbz result: %+v", plain)
	}

	badMeta := byPath["BadMeta.cbz"]
	if badMeta.PageCount != 1 || badMeta.ComicInfo != nil || badMeta.ComicInfoErr == nil {
		t.Fatalf("unexpected BadMeta.cbz result: %+v", badMeta)
	}

	empty := byPath["Empty.cbz"]
	if empty.PageCount != 0 || empty.CoverPageIndex != 0 || len(empty.SuppressedPages) != 0 {
		t.Fatalf("unexpected Empty.cbz result: %+v", empty)
	}
}

func TestWalkRoot_SkipsHiddenTempNonCBZCorruptAndUnreadableEntries(t *testing.T) {
	root := t.TempDir()
	goodCBZ := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("good"), method: zip.Store})

	writeDiskFile(t, filepath.Join(root, "valid.cbz"), goodCBZ)
	writeDiskFile(t, filepath.Join(root, "sub/also_valid.CBZ"), goodCBZ)

	// Hidden files & directories
	writeDiskFile(t, filepath.Join(root, ".DS_Store"), []byte("ds"))
	writeDiskFile(t, filepath.Join(root, ".tobiten-root"), nil)
	writeDiskFile(t, filepath.Join(root, "._valid.cbz"), []byte("appledouble"))
	writeDiskFile(t, filepath.Join(root, ".hidden_dir/inside.cbz"), goodCBZ)
	writeDiskFile(t, filepath.Join(root, "__MACOSX/debris.cbz"), goodCBZ)

	// Temporary files
	writeDiskFile(t, filepath.Join(root, ".tobiten-tmp-job1-book.cbz"), goodCBZ)
	writeDiskFile(t, filepath.Join(root, "book.cbz.tmp"), goodCBZ)
	writeDiskFile(t, filepath.Join(root, "book.part"), goodCBZ)
	writeDiskFile(t, filepath.Join(root, "book.crdownload"), goodCBZ)
	writeDiskFile(t, filepath.Join(root, "book.cbz~"), goodCBZ)
	writeDiskFile(t, filepath.Join(root, "~$book.cbz"), goodCBZ)

	// Non-CBZ files
	writeDiskFile(t, filepath.Join(root, "notes.txt"), []byte("hello"))
	writeDiskFile(t, filepath.Join(root, "cover.jpg"), []byte("jpg"))
	writeDiskFile(t, filepath.Join(root, "archive.zip"), goodCBZ)
	writeDiskFile(t, filepath.Join(root, "comic.cbr"), []byte("Rar!"))

	// Corrupt / empty CBZ archives
	writeDiskFile(t, filepath.Join(root, "corrupt.cbz"), []byte("not-a-zip"))
	writeDiskFile(t, filepath.Join(root, "empty_zero.cbz"), nil)

	// Unreadable CBZ simulated deterministically via hooks
	// (avoids root-UID chmod 0000 bypasses in container CI).
	writeDiskFile(t, filepath.Join(root, "unreadable.cbz"), goodCBZ)

	res, err := scanner.WalkRoot(context.Background(), []byte(root), scanner.WalkOptions{
		Open: func(ctx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			if filepath.Base(string(rawPath)) == "unreadable.cbz" {
				return nil, os.ErrPermission
			}
			return paths.OpenRaw(rawPath)
		},
	})
	if err != nil {
		t.Fatalf("WalkRoot error: %v", err)
	}
	if !res.Complete {
		t.Fatal("WalkRoot reported Complete=false")
	}

	inspected := inspectedMap(res)
	if len(inspected) != 2 {
		t.Fatalf("inspected = %v, want valid.cbz and sub/also_valid.CBZ", inspected)
	}
	if _, ok := inspected["valid.cbz"]; !ok {
		t.Error("missing valid.cbz")
	}
	if _, ok := inspected["sub/also_valid.CBZ"]; !ok {
		t.Error("missing sub/also_valid.CBZ")
	}

	skipped := skippedMap(res)
	wantReasons := map[string]scanner.SkipReason{
		".DS_Store":                  scanner.SkipReasonHidden,
		".tobiten-root":              scanner.SkipReasonHidden,
		"._valid.cbz":                scanner.SkipReasonHidden,
		".hidden_dir":                scanner.SkipReasonHidden,
		"__MACOSX":                   scanner.SkipReasonHidden,
		".tobiten-tmp-job1-book.cbz": scanner.SkipReasonTemporary,
		"book.cbz.tmp":               scanner.SkipReasonTemporary,
		"book.part":                  scanner.SkipReasonTemporary,
		"book.crdownload":            scanner.SkipReasonTemporary,
		"book.cbz~":                  scanner.SkipReasonTemporary,
		"~$book.cbz":                 scanner.SkipReasonTemporary,
		"notes.txt":                  scanner.SkipReasonNonCBZ,
		"cover.jpg":                  scanner.SkipReasonNonCBZ,
		"archive.zip":                scanner.SkipReasonNonCBZ,
		"comic.cbr":                  scanner.SkipReasonNonCBZ,
		"corrupt.cbz":                scanner.SkipReasonCorruptArchive,
		"empty_zero.cbz":             scanner.SkipReasonCorruptArchive,
		"unreadable.cbz":             scanner.SkipReasonUnreadable,
	}
	for path, wantReason := range wantReasons {
		got, ok := skipped[path]
		if !ok {
			t.Errorf("missing skipped entry %q", path)
			continue
		}
		if got.Reason != wantReason {
			t.Errorf("skipped[%q].Reason = %q, want %q", path, got.Reason, wantReason)
		}
	}
	if _, descended := skipped[".hidden_dir/inside.cbz"]; descended {
		t.Error("walk descended into hidden directory .hidden_dir")
	}
	if _, descended := inspected[".hidden_dir/inside.cbz"]; descended {
		t.Error("walk inspected file inside hidden directory .hidden_dir")
	}
}

func TestWalkRoot_CancellationStopsWalkAndReportsIncomplete(t *testing.T) {
	root := t.TempDir()
	cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("page"), method: zip.Store})
	writeDiskFile(t, filepath.Join(root, "a/01.cbz"), cbzBytes)
	writeDiskFile(t, filepath.Join(root, "b/02.cbz"), cbzBytes)
	writeDiskFile(t, filepath.Join(root, "c/03.cbz"), cbzBytes)

	t.Run("pre-cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		res, err := scanner.WalkRoot(ctx, []byte(root), scanner.WalkOptions{})
		if !errors.Is(err, context.Canceled) || res.Complete {
			t.Fatalf("WalkRoot(cancelled) = (Complete=%v, %v), want (false, context.Canceled)", res.Complete, err)
		}
	})

	t.Run("cancelled while worker blocked in Open", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		entered := make(chan struct{})
		var enteredOnce sync.Once
		var dirReadMu sync.Mutex
		var dirsRead []string

		var res scanner.WalkResult
		var err error
		done := make(chan struct{})
		go func() {
			defer close(done)
			res, err = scanner.WalkRoot(ctx, []byte(root), scanner.WalkOptions{
				Workers: 1,
				ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
					rel, relErr := filepath.Rel(root, string(rawDir))
					if relErr == nil {
						dirReadMu.Lock()
						dirsRead = append(dirsRead, filepath.ToSlash(rel))
						dirReadMu.Unlock()
					}
					return os.ReadDir(string(rawDir))
				},
				Open: func(openCtx context.Context, _ []byte) (scanner.ReadAtCloser, error) {
					enteredOnce.Do(func() { close(entered) })
					<-openCtx.Done()
					return nil, openCtx.Err()
				},
			})
		}()

		<-entered
		cancel()
		<-done

		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WalkRoot error = %v, want context.Canceled", err)
		}
		if res.Complete {
			t.Fatal("WalkRoot reported Complete=true after cancellation")
		}
		if slices.Contains(dirsRead, "c") {
			t.Fatalf("walk continued into directory c after cancellation: %v", dirsRead)
		}
	})

	t.Run("OnEntry callback error stops walk incomplete", func(t *testing.T) {
		sentinel := errors.New("stop walk")
		var count atomic.Int32
		res, err := scanner.WalkRoot(context.Background(), []byte(root), scanner.WalkOptions{
			Workers: 1,
			OnEntry: func(_ scanner.WalkEntry) error {
				if count.Add(1) == 1 {
					return sentinel
				}
				return nil
			},
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("WalkRoot error = %v, want %v", err, sentinel)
		}
		if res.Complete {
			t.Fatal("WalkRoot reported Complete=true after callback error")
		}
	})
}

func TestWalkRoot_NFCNFDAndInvalidUTF8Names(t *testing.T) {
	cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("p1"), method: zip.Store})

	t.Run("NFC and NFD separate roots preserve raw bytes and match keys", func(t *testing.T) {
		base := t.TempDir()
		nfcRoot := filepath.Join(base, "nfc")
		nfdRoot := filepath.Join(base, "nfd")

		nfcRel := "Series/\u304c.cbz"       // が (NFC)
		nfdRel := "Series/\u304b\u3099.cbz" // か + U+3099 (NFD)
		writeDiskFile(t, filepath.Join(nfcRoot, nfcRel), cbzBytes)
		writeDiskFile(t, filepath.Join(nfdRoot, nfdRel), cbzBytes)

		resNFC, err := scanner.WalkRoot(context.Background(), []byte(nfcRoot), scanner.WalkOptions{})
		if err != nil || !resNFC.Complete || len(resNFC.Files) != 1 {
			t.Fatalf("NFC WalkRoot = (%+v, %v)", resNFC, err)
		}
		resNFD, err := scanner.WalkRoot(context.Background(), []byte(nfdRoot), scanner.WalkOptions{})
		if err != nil || !resNFD.Complete || len(resNFD.Files) != 1 {
			t.Fatalf("NFD WalkRoot = (%+v, %v)", resNFD, err)
		}

		fNFC := resNFC.Files[0]
		fNFD := resNFD.Files[0]
		if !bytes.Equal(fNFC.RelPath.Bytes, []byte(nfcRel)) {
			t.Errorf("NFC bytes = %x, want %x", fNFC.RelPath.Bytes, []byte(nfcRel))
		}
		if !bytes.Equal(fNFD.RelPath.Bytes, []byte(nfdRel)) {
			t.Errorf("NFD bytes = %x, want %x", fNFD.RelPath.Bytes, []byte(nfdRel))
		}
		if fNFC.RelPath.Match != fNFD.RelPath.Match {
			t.Errorf("Match keys differ: %q vs %q", fNFC.RelPath.Match, fNFD.RelPath.Match)
		}
		if fNFC.RelPath.InvalidUTF8 || fNFD.RelPath.InvalidUTF8 {
			t.Errorf("unexpected InvalidUTF8 flag on NFC/NFD paths")
		}
	})

	t.Run("invalid UTF-8 on disk when filesystem permits", func(t *testing.T) {
		root := []byte(t.TempDir())
		dirRaw, err := paths.JoinRootRelative(root, []byte{0xff, 0xfe})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(string(dirRaw), 0o755); err != nil {
			if runtime.GOOS != "linux" && (errors.Is(err, syscall.EILSEQ) || errors.Is(err, syscall.EINVAL)) {
				t.Skipf("filesystem rejected invalid UTF-8 directory (%v)", err)
			}
			t.Fatalf("Mkdir invalid UTF-8: %v", err)
		}
		rel := []byte{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', 0x80, '.', 'c', 'b', 'z'}
		fileRaw, err := paths.JoinRootRelative(root, rel)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(string(fileRaw), cbzBytes, 0o600); err != nil {
			if runtime.GOOS != "linux" && (errors.Is(err, syscall.EILSEQ) || errors.Is(err, syscall.EINVAL)) {
				t.Skipf("filesystem rejected invalid UTF-8 file (%v)", err)
			}
			t.Fatalf("WriteFile invalid UTF-8: %v", err)
		}

		res, err := scanner.WalkRoot(context.Background(), root, scanner.WalkOptions{})
		if err != nil || !res.Complete || len(res.Files) != 1 {
			t.Fatalf("WalkRoot invalid UTF-8 = (%+v, %v)", res, err)
		}
		got := res.Files[0]
		if !bytes.Equal(got.RelPath.Bytes, rel) || !got.RelPath.InvalidUTF8 || got.RelPath.Display != `\xff\xfe/book\x80.cbz` {
			t.Fatalf("unexpected invalid UTF-8 RelPath: %+v", got.RelPath)
		}
	})

	t.Run("synthetic invalid UTF-8 directory and file walk on all platforms", func(t *testing.T) {
		fakeRoot := []byte("/synthetic/root")
		badDirName := string([]byte{0xff, 0xfe})
		badFileName := string([]byte{'b', 'o', 'o', 'k', 0x80, '.', 'c', 'b', 'z'})
		now := time.Unix(1700000000, 123456789).UTC()

		res, err := scanner.WalkRoot(context.Background(), fakeRoot, scanner.WalkOptions{
			ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
				switch string(rawDir) {
				case "/synthetic/root":
					return []os.DirEntry{syntheticDirEntry{name: badDirName, mode: fs.ModeDir, size: 0, mtime: now}}, nil
				case string(append([]byte("/synthetic/root/"), []byte(badDirName)...)):
					return []os.DirEntry{syntheticDirEntry{name: badFileName, mode: 0, size: int64(len(cbzBytes)), mtime: now}}, nil
				default:
					return nil, os.ErrNotExist
				}
			},
			Stat: func(rawPath []byte) (os.FileInfo, error) {
				switch string(rawPath) {
				case "/synthetic/root":
					return syntheticFileInfo{name: "root", mode: fs.ModeDir | 0o755, mtime: now, id: 1}, nil
				case string(append([]byte("/synthetic/root/"), []byte(badDirName)...)):
					return syntheticFileInfo{name: badDirName, mode: fs.ModeDir | 0o755, mtime: now, id: 2}, nil
				default:
					return syntheticFileInfo{name: badFileName, mode: 0o644, size: int64(len(cbzBytes)), mtime: now, id: 3}, nil
				}
			},
			Open: func(_ context.Context, _ []byte) (scanner.ReadAtCloser, error) {
				return &bytesReadAtCloser{Reader: bytes.NewReader(cbzBytes)}, nil
			},
		})
		if err != nil || !res.Complete || len(res.Files) != 1 {
			t.Fatalf("synthetic WalkRoot = (%+v, %v)", res, err)
		}
		got := res.Files[0]
		wantRel := []byte{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', 0x80, '.', 'c', 'b', 'z'}
		if !bytes.Equal(got.RelPath.Bytes, wantRel) || !got.RelPath.InvalidUTF8 || got.RelPath.Display != `\xff\xfe/book\x80.cbz` {
			t.Fatalf("unexpected synthetic invalid UTF-8 RelPath: %+v", got.RelPath)
		}
	})
}

func TestWalkRoot_SymlinksCyclesAndReadOnlyPreservation(t *testing.T) {
	root := t.TempDir()
	ext := t.TempDir()

	cbzA := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("aaa"), method: zip.Store})
	cbzB := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("bbb"), method: zip.Store})
	cbzC := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("ccc"), method: zip.Store})

	// Local files inside root
	writeDiskFile(t, filepath.Join(root, "local/orig.cbz"), cbzA)
	writeDiskFile(t, filepath.Join(root, "zzz_unrelated.cbz"), cbzB)

	// External targets outside root
	writeDiskFile(t, filepath.Join(ext, "ext_file.cbz"), cbzC)
	writeDiskFile(t, filepath.Join(ext, "shared/item.cbz"), cbzA)

	// Symlinks to external file, internal file, and two separate paths to one shared external directory
	if err := os.MkdirAll(filepath.Join(root, "links"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustSymlink(t, filepath.Join(ext, "ext_file.cbz"), filepath.Join(root, "links/ext_link.cbz"))
	mustSymlink(t, "../local/orig.cbz", filepath.Join(root, "links/rel_link.cbz"))
	mustSymlink(t, filepath.Join(ext, "shared"), filepath.Join(root, "branchA/shared_dir"))
	mustSymlink(t, filepath.Join(ext, "shared"), filepath.Join(root, "branchB/shared_dir"))

	// Directory cycles: inside root, inside external shared dir pointing back to root, self-loop, and ELOOP
	mustSymlink(t, root, filepath.Join(ext, "shared/back_to_root"))
	mustSymlink(t, ".", filepath.Join(root, "local/self_cycle"))
	mustSymlink(t, filepath.Join(root, "local"), filepath.Join(root, "local/sub/up_cycle"))
	mustSymlink(t, filepath.Join(root, "eloop_b"), filepath.Join(root, "eloop_a"))
	mustSymlink(t, filepath.Join(root, "eloop_a"), filepath.Join(root, "eloop_b"))

	// Broken symlink, hidden symlink, and symlink to non-CBZ file
	mustSymlink(t, filepath.Join(ext, "does_not_exist.cbz"), filepath.Join(root, "links/broken.cbz"))
	writeDiskFile(t, filepath.Join(ext, "secret_dir/hidden_target.cbz"), cbzA)
	mustSymlink(t, filepath.Join(ext, "secret_dir"), filepath.Join(root, ".hidden_link"))
	writeDiskFile(t, filepath.Join(ext, "notes.txt"), []byte("text"))
	mustSymlink(t, filepath.Join(ext, "notes.txt"), filepath.Join(root, "links/note_link.txt"))

	// Snapshot both root and external targets before walking to prove zero mutations / mtime changes.
	rootBefore := snapshotTree(t, root)
	extBefore := snapshotTree(t, ext)

	res, err := scanner.WalkRoot(context.Background(), []byte(root+"/"), scanner.WalkOptions{})
	if err != nil {
		t.Fatalf("WalkRoot: %v", err)
	}
	if !res.Complete {
		t.Fatal("WalkRoot reported Complete=false")
	}

	assertTreeUnchanged(t, "root", root, rootBefore)
	assertTreeUnchanged(t, "external", ext, extBefore)

	inspected := inspectedMap(res)
	wantInspected := []string{
		"branchA/shared_dir/item.cbz",
		"branchB/shared_dir/item.cbz",
		"links/ext_link.cbz",
		"links/rel_link.cbz",
		"local/orig.cbz",
		"zzz_unrelated.cbz",
	}
	var gotPaths []string
	for p := range inspected {
		gotPaths = append(gotPaths, p)
	}
	sort.Strings(gotPaths)
	if !slices.Equal(gotPaths, wantInspected) {
		t.Fatalf("inspected paths = %v, want %v", gotPaths, wantInspected)
	}

	skipped := skippedMap(res)
	wantSkipped := map[string]scanner.SkipReason{
		".hidden_link":                    scanner.SkipReasonHidden,
		"links/note_link.txt":             scanner.SkipReasonNonCBZ,
		"links/broken.cbz":                scanner.SkipReasonBrokenSymlink,
		"branchA/shared_dir/back_to_root": scanner.SkipReasonSymlinkCycle,
		"branchB/shared_dir/back_to_root": scanner.SkipReasonSymlinkCycle,
		"local/self_cycle":                scanner.SkipReasonSymlinkCycle,
		"local/sub/up_cycle":              scanner.SkipReasonSymlinkCycle,
		"eloop_a":                         scanner.SkipReasonSymlinkCycle,
		"eloop_b":                         scanner.SkipReasonSymlinkCycle,
	}
	for path, wantReason := range wantSkipped {
		got, ok := skipped[path]
		if !ok {
			t.Errorf("missing expected skipped cycle/broken link %q (skipped=%v)", path, skipped)
			continue
		}
		if got.Reason != wantReason {
			t.Errorf("skipped[%q].Reason = %q, want %q", path, got.Reason, wantReason)
		}
	}
}

func TestWalkRoot_ValidationAndOnEntryStreaming(t *testing.T) {
	root := t.TempDir()
	cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("page"), method: zip.Store})
	writeDiskFile(t, filepath.Join(root, "a.cbz"), cbzBytes)
	writeDiskFile(t, filepath.Join(root, "note.txt"), []byte("txt"))

	if _, err := scanner.WalkRoot(context.Background(), []byte("relative/path"), scanner.WalkOptions{}); !errors.Is(err, domain.ErrInvalidRootPath) {
		t.Errorf("relative root error = %v, want ErrInvalidRootPath", err)
	}
	if _, err := scanner.WalkRoot(context.Background(), []byte(root), scanner.WalkOptions{Workers: -1}); !errors.Is(err, scanner.ErrInvalidWorkerCount) {
		t.Errorf("negative workers error = %v, want ErrInvalidWorkerCount", err)
	}
	notDir := filepath.Join(root, "note.txt")
	if _, err := scanner.WalkRoot(context.Background(), []byte(notDir), scanner.WalkOptions{}); !errors.Is(err, scanner.ErrRootNotDirectory) {
		t.Errorf("file-as-root error = %v, want ErrRootNotDirectory", err)
	}
	if _, err := scanner.WalkRoot(context.Background(), []byte(filepath.Join(root, "missing")), scanner.WalkOptions{}); err == nil {
		t.Error("missing root succeeded, want error")
	}

	var streamed []scanner.WalkEntry
	res, err := scanner.WalkRoot(context.Background(), []byte(root), scanner.WalkOptions{
		OnEntry: func(e scanner.WalkEntry) error {
			streamed = append(streamed, e)
			return nil
		},
	})
	if err != nil || !res.Complete {
		t.Fatalf("streaming WalkRoot = (%+v, %v)", res, err)
	}
	if res.InspectedCount != 1 || res.SkippedCount != 1 || len(streamed) != 2 {
		t.Fatalf("unexpected streaming counts: res=%+v len(streamed)=%d", res, len(streamed))
	}
	if len(res.Files) != 0 || len(res.Skipped) != 0 {
		t.Errorf("expected result slices empty when OnEntry is set, got files=%d skipped=%d", len(res.Files), len(res.Skipped))
	}
}

func TestWalkRoot_UnreadableSubtreeMarksIncompleteAndContinuesSiblings(t *testing.T) {
	goodCBZ := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("good"), method: zip.Store})

	cases := []struct {
		name     string
		setup    func(t *testing.T, root string) scanner.WalkOptions
		wantSkip string
		wantDir  bool
	}{
		{
			name: "descendant ReadDir permission error",
			setup: func(t *testing.T, root string) scanner.WalkOptions {
				writeDiskFile(t, filepath.Join(root, "alpha.cbz"), goodCBZ)
				writeDiskFile(t, filepath.Join(root, "locked_dir/hidden_inside.cbz"), goodCBZ)
				writeDiskFile(t, filepath.Join(root, "zeta/sibling.cbz"), goodCBZ)
				return scanner.WalkOptions{
					ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
						if filepath.Base(string(rawDir)) == "locked_dir" {
							return nil, os.ErrPermission
						}
						return os.ReadDir(string(rawDir))
					},
				}
			},
			wantSkip: "locked_dir",
			wantDir:  true,
		},
		{
			name: "descendant directory stat error",
			setup: func(t *testing.T, root string) scanner.WalkOptions {
				writeDiskFile(t, filepath.Join(root, "alpha.cbz"), goodCBZ)
				writeDiskFile(t, filepath.Join(root, "unstat_dir/inside.cbz"), goodCBZ)
				writeDiskFile(t, filepath.Join(root, "zeta/sibling.cbz"), goodCBZ)
				return scanner.WalkOptions{
					Stat: func(rawPath []byte) (os.FileInfo, error) {
						if filepath.Base(string(rawPath)) == "unstat_dir" {
							return nil, syscall.EIO
						}
						return os.Stat(string(rawPath))
					},
				}
			},
			wantSkip: "unstat_dir",
			wantDir:  true,
		},
		{
			name: "symlink target unreadable via traversal error",
			setup: func(t *testing.T, root string) scanner.WalkOptions {
				writeDiskFile(t, filepath.Join(root, "alpha.cbz"), goodCBZ)
				writeDiskFile(t, filepath.Join(root, "zeta/sibling.cbz"), goodCBZ)
				mustSymlink(t, filepath.Join(root, "zeta"), filepath.Join(root, "blocked_link"))
				return scanner.WalkOptions{
					Stat: func(rawPath []byte) (os.FileInfo, error) {
						if filepath.Base(string(rawPath)) == "blocked_link" {
							return nil, os.ErrPermission
						}
						return os.Stat(string(rawPath))
					},
				}
			},
			wantSkip: "blocked_link",
		},
		{
			name: "unknown entry type stat traversal error",
			setup: func(t *testing.T, root string) scanner.WalkOptions {
				writeDiskFile(t, filepath.Join(root, "alpha.cbz"), goodCBZ)
				writeDiskFile(t, filepath.Join(root, "zeta/sibling.cbz"), goodCBZ)
				return scanner.WalkOptions{
					ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
						entries, err := os.ReadDir(string(rawDir))
						if err != nil {
							return nil, err
						}
						if string(rawDir) == root {
							entries = append(entries, syntheticDirEntry{name: "unknown_node", mode: 0})
						}
						return entries, nil
					},
					Stat: func(rawPath []byte) (os.FileInfo, error) {
						if filepath.Base(string(rawPath)) == "unknown_node" {
							return nil, syscall.EIO
						}
						return os.Stat(string(rawPath))
					},
				}
			},
			wantSkip: "unknown_node",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			baseOpts := tc.setup(t, root)

			// 1. Collected results mode
			res, err := scanner.WalkRoot(context.Background(), []byte(root), baseOpts)
			if err != nil {
				t.Fatalf("WalkRoot error = %v, want nil", err)
			}
			if res.Complete {
				t.Fatal("WalkRoot reported Complete=true despite unreadable subtree")
			}
			inspected := inspectedMap(res)
			if _, ok := inspected["alpha.cbz"]; !ok {
				t.Errorf("missing sibling alpha.cbz in inspected: %v", inspected)
			}
			if _, ok := inspected["zeta/sibling.cbz"]; !ok {
				t.Errorf("missing sibling zeta/sibling.cbz in inspected: %v", inspected)
			}
			skipped := skippedMap(res)
			gotSkip, ok := skipped[tc.wantSkip]
			if !ok {
				t.Fatalf("missing skipped entry %q in %v", tc.wantSkip, skipped)
			}
			if gotSkip.Reason != scanner.SkipReasonUnreadable || gotSkip.IsDir != tc.wantDir || gotSkip.Err == nil {
				t.Fatalf("unexpected skip diagnostic: %+v", gotSkip)
			}

			// 2. Callback mode
			var streamedInspected []string
			var streamedSkipped []scanner.SkippedEntry
			cbOpts := baseOpts
			cbOpts.OnEntry = func(e scanner.WalkEntry) error {
				switch e.Kind {
				case scanner.EntryInspected:
					streamedInspected = append(streamedInspected, string(e.Inspected.RelPath.Bytes))
				case scanner.EntrySkipped:
					streamedSkipped = append(streamedSkipped, *e.Skipped)
				}
				return nil
			}
			cbRes, err := scanner.WalkRoot(context.Background(), []byte(root), cbOpts)
			if err != nil {
				t.Fatalf("callback WalkRoot error = %v, want nil", err)
			}
			if cbRes.Complete {
				t.Fatal("callback WalkRoot reported Complete=true despite unreadable subtree")
			}
			sort.Strings(streamedInspected)
			if !slices.Equal(streamedInspected, []string{"alpha.cbz", "zeta/sibling.cbz"}) {
				t.Fatalf("callback inspected = %v, want [alpha.cbz zeta/sibling.cbz]", streamedInspected)
			}
			if len(streamedSkipped) != 1 || string(streamedSkipped[0].RelPath.Bytes) != tc.wantSkip ||
				streamedSkipped[0].Reason != scanner.SkipReasonUnreadable {
				t.Fatalf("callback skipped = %+v, want %q unreadable", streamedSkipped, tc.wantSkip)
			}
		})
	}
}

func TestWalkRoot_FIFOSwapAndFileReplacementBetweenStatAndOpen(t *testing.T) {
	t.Run("link swapped to FIFO after stat never blocks production Open", func(t *testing.T) {
		root := t.TempDir()
		ext := t.TempDir()

		targetCBZ := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("target"), method: zip.Store})
		siblingCBZ := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("sibling"), method: zip.Store})
		targetPath := filepath.Join(ext, "target.cbz")
		fifoPath := filepath.Join(ext, "pipe.fifo")
		linkPath := filepath.Join(root, "link.cbz")
		directFifoPath := filepath.Join(root, "direct.cbz")

		writeDiskFile(t, targetPath, targetCBZ)
		writeDiskFile(t, filepath.Join(root, "sibling.cbz"), siblingCBZ)
		mustSymlink(t, targetPath, linkPath)
		if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
			t.Fatalf("Mkfifo(ext): %v", err)
		}
		if err := syscall.Mkfifo(directFifoPath, 0o600); err != nil {
			t.Fatalf("Mkfifo(direct): %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var swapped atomic.Bool
		res, err := scanner.WalkRoot(ctx, []byte(root), scanner.WalkOptions{
			Workers: 1,
			Stat: func(rawPath []byte) (os.FileInfo, error) {
				info, statErr := os.Stat(string(rawPath))
				if statErr == nil && string(rawPath) == linkPath && swapped.CompareAndSwap(false, true) {
					if rmErr := os.Remove(linkPath); rmErr != nil {
						return nil, rmErr
					}
					if symErr := os.Symlink(fifoPath, linkPath); symErr != nil {
						return nil, symErr
					}
				}
				return info, statErr
			},
		})
		if err != nil {
			t.Fatalf("WalkRoot error = %v", err)
		}
		if !res.Complete {
			t.Fatal("WalkRoot reported Complete=false")
		}
		inspected := inspectedMap(res)
		if _, ok := inspected["sibling.cbz"]; !ok {
			t.Fatalf("missing sibling.cbz in inspected: %v", inspected)
		}
		skipped := skippedMap(res)
		for _, p := range []string{"link.cbz", "direct.cbz"} {
			got, ok := skipped[p]
			if !ok {
				t.Fatalf("missing skipped FIFO %q in %v", p, skipped)
			}
			if got.Reason != scanner.SkipReasonNonRegular {
				t.Fatalf("skipped[%q].Reason = %q, want %q", p, got.Reason, scanner.SkipReasonNonRegular)
			}
		}
	})

	t.Run("regular file replaced between stat and open uses opened handle metadata", func(t *testing.T) {
		root := t.TempDir()
		oldCBZ := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("old-small"), method: zip.Store})
		newCBZ := buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Replaced</Series></ComicInfo>`), method: zip.Store},
			zipEntry{name: "001.jpg", data: []byte("new-much-larger-page-one"), method: zip.Store},
			zipEntry{name: "002.jpg", data: []byte("new-much-larger-page-two"), method: zip.Store},
		)

		swappedPath := filepath.Join(root, "swapped.cbz")
		unchangedPath := filepath.Join(root, "unchanged.cbz")
		writeDiskFile(t, swappedPath, oldCBZ)
		writeDiskFile(t, unchangedPath, oldCBZ)

		oldTime := time.Unix(1700000100, 0).UTC()
		newTime := time.Unix(1700000500, 0).UTC()
		if err := os.Chtimes(swappedPath, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(unchangedPath, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}

		wantNewDigest, err := scanner.PartialFingerprint(bytes.NewReader(newCBZ), int64(len(newCBZ)))
		if err != nil {
			t.Fatal(err)
		}

		var replaced atomic.Bool
		res, err := scanner.WalkRoot(context.Background(), []byte(root), scanner.WalkOptions{
			Workers: 1,
			Known: map[string]scanner.KnownFile{
				"unchanged.cbz": {Size: int64(len(oldCBZ)), Mtime: &oldTime},
			},
			Stat: func(rawPath []byte) (os.FileInfo, error) {
				info, statErr := os.Stat(string(rawPath))
				if statErr == nil && string(rawPath) == swappedPath && replaced.CompareAndSwap(false, true) {
					if writeErr := os.WriteFile(swappedPath, newCBZ, 0o600); writeErr != nil {
						return nil, writeErr
					}
					if chErr := os.Chtimes(swappedPath, newTime, newTime); chErr != nil {
						return nil, chErr
					}
				}
				return info, statErr
			},
		})
		if err != nil {
			t.Fatalf("WalkRoot error = %v", err)
		}
		if !res.Complete {
			t.Fatal("WalkRoot reported Complete=false")
		}
		unchanged := unchangedMap(res)
		if _, ok := unchanged["unchanged.cbz"]; !ok {
			t.Fatalf("missing unchanged.cbz in %v", unchanged)
		}
		inspected := inspectedMap(res)
		got, ok := inspected["swapped.cbz"]
		if !ok {
			t.Fatalf("missing swapped.cbz in inspected: %v (skipped=%v)", inspected, res.Skipped)
		}
		if got.Size != int64(len(newCBZ)) {
			t.Errorf("swapped.cbz Size = %d, want %d from opened handle", got.Size, len(newCBZ))
		}
		if !got.Mtime.Equal(newTime) {
			t.Errorf("swapped.cbz Mtime = %v, want %v from opened handle", got.Mtime, newTime)
		}
		if got.Digest != wantNewDigest {
			t.Errorf("swapped.cbz Digest = %s, want %s", got.Digest, wantNewDigest)
		}
		if got.PageCount != 2 || got.ComicInfo == nil || got.ComicInfo.Series != "Replaced" {
			t.Errorf("swapped.cbz pages/ComicInfo mismatch: pages=%d meta=%+v", got.PageCount, got.ComicInfo)
		}
	})
}

func mustSymlink(t *testing.T, target, linkPath string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(linkPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", linkPath, err)
	}
	if err := os.Symlink(target, linkPath); err != nil {
		t.Fatalf("Symlink(%q -> %q): %v", linkPath, target, err)
	}
}

type bytesReadAtCloser struct {
	*bytes.Reader
}

func (b *bytesReadAtCloser) Close() error { return nil }

type syntheticDirEntry struct {
	name  string
	mode  fs.FileMode
	size  int64
	mtime time.Time
}

func (d syntheticDirEntry) Name() string      { return d.name }
func (d syntheticDirEntry) IsDir() bool       { return d.mode.IsDir() }
func (d syntheticDirEntry) Type() fs.FileMode { return d.mode.Type() }
func (d syntheticDirEntry) Info() (fs.FileInfo, error) {
	return syntheticFileInfo{name: d.name, mode: d.mode, size: d.size, mtime: d.mtime}, nil
}

type syntheticFileInfo struct {
	name  string
	mode  fs.FileMode
	size  int64
	mtime time.Time
	id    uint64
}

func (f syntheticFileInfo) Name() string       { return f.name }
func (f syntheticFileInfo) Size() int64        { return f.size }
func (f syntheticFileInfo) Mode() fs.FileMode  { return f.mode }
func (f syntheticFileInfo) ModTime() time.Time { return f.mtime }
func (f syntheticFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f syntheticFileInfo) Sys() any           { return f.id }

var _ io.ReaderAt = (*bytesReadAtCloser)(nil)

func TestWalkRoot_CancelBeforeNextArchiveAdmission_OneWorker(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	first := filepath.Join(root, "01_first.cbz")
	second := filepath.Join(root, "02_second.cbz")
	cbzBytes := buildDeterministicZip(t, "",
		zipEntry{name: "001.jpg", data: []byte("page1"), method: zip.Store},
	)
	if err := os.WriteFile(first, cbzBytes, 0o600); err != nil {
		t.Fatalf("WriteFile(first): %v", err)
	}
	if err := os.WriteFile(second, cbzBytes, 0o600); err != nil {
		t.Fatalf("WriteFile(second): %v", err)
	}

	sentinelCancel := errors.New("cancel requested before next archive")

	// Run multiple iterations to eliminate scheduling luck between worker admission and OnEntry.
	for iter := range 10 {
		var cancelPending atomic.Bool
		var secondOpens atomic.Int32
		enteredFirst := make(chan struct{}, 1)
		releaseFirst := make(chan struct{})

		type walkOutcome struct {
			res scanner.WalkResult
			err error
		}
		doneCh := make(chan walkOutcome, 1)
		go func() {
			res, err := scanner.WalkRoot(t.Context(), []byte(root), scanner.WalkOptions{
				Workers: 1,
				CheckCancel: func(_ context.Context) error {
					if cancelPending.Load() {
						return sentinelCancel
					}
					return nil
				},
				Open: func(ctx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
					p := string(rawPath)
					if p == second {
						secondOpens.Add(1)
					}
					if p == first {
						select {
						case enteredFirst <- struct{}{}:
						default:
						}
						select {
						case <-releaseFirst:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					return os.Open(p)
				},
				OnEntry: func(_ scanner.WalkEntry) error {
					if cancelPending.Load() {
						return sentinelCancel
					}
					return nil
				},
			})
			doneCh <- walkOutcome{res: res, err: err}
		}()

		select {
		case <-enteredFirst:
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: timed out waiting for Open(first)", iter)
		}
		cancelPending.Store(true)
		close(releaseFirst)

		out := <-doneCh
		if !errors.Is(out.err, sentinelCancel) || out.res.Complete {
			t.Fatalf("iter %d: WalkRoot = (%+v, %v), want Complete=false err=%v", iter, out.res, out.err, sentinelCancel)
		}
		if got := secondOpens.Load(); got != 0 {
			t.Fatalf("iter %d: 02_second.cbz opened %d times after cancel, want 0", iter, got)
		}
	}
}

func TestWalkRoot_TwoWorkersAbortStopsOutstandingAdmissionCheck(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cbzBytes := buildDeterministicZip(t, "",
		zipEntry{name: "001.jpg", data: []byte("page1"), method: zip.Store},
	)
	for _, name := range []string{"01_first.cbz", "02_second.cbz"} {
		if err := os.WriteFile(filepath.Join(root, name), cbzBytes, 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}

	parentCtx, parentCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer parentCancel()

	sentinelCancel := errors.New("walk cancel requested")
	var callNum atomic.Int32
	var entered atomic.Int32
	bothEntered := make(chan struct{})
	var openCalls atomic.Int32

	type outcome struct {
		res scanner.WalkResult
		err error
	}
	doneCh := make(chan outcome, 1)
	go func() {
		res, err := scanner.WalkRoot(parentCtx, []byte(root), scanner.WalkOptions{
			Workers: 2,
			CheckCancel: func(checkCtx context.Context) error {
				id := callNum.Add(1)
				if entered.Add(1) == 2 {
					close(bothEntered)
				}
				select {
				case <-bothEntered:
				case <-checkCtx.Done():
					return checkCtx.Err()
				}
				if id == 1 {
					return sentinelCancel
				}
				// Sibling worker stays in CheckCancel until walkCtx is cancelled; parentCtx remains live.
				<-checkCtx.Done()
				return checkCtx.Err()
			},
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				openCalls.Add(1)
				return os.Open(string(rawPath))
			},
		})
		doneCh <- outcome{res: res, err: err}
	}()

	select {
	case out := <-doneCh:
		if parentCtx.Err() != nil {
			t.Fatalf("parentCtx unexpectedly cancelled: %v", parentCtx.Err())
		}
		if !errors.Is(out.err, sentinelCancel) {
			t.Fatalf("WalkRoot err = %v, want errors.Is(_, %v)", out.err, sentinelCancel)
		}
		if out.res.Complete {
			t.Fatalf("WalkRoot Complete = true, want false")
		}
		if got := openCalls.Load(); got != 0 {
			t.Fatalf("Open called %d times, want 0", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WalkRoot hung waiting for outstanding sibling CheckCancel while parent context remained live")
	}
}
