package scanner_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/testdb"
)

type scanLogRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

type scanLogHandler struct {
	mu      sync.Mutex
	records []scanLogRecord
}

func (h *scanLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *scanLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any)
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.Any()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, scanLogRecord{
		Level:   r.Level,
		Message: r.Message,
		Attrs:   attrs,
	})
	h.mu.Unlock()
	return nil
}

func (h *scanLogHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *scanLogHandler) WithGroup(_ string) slog.Handler      { return h }

type singleWriterWitnessTransactor struct {
	inner     storage.Transactor
	active    atomic.Int32
	maxActive atomic.Int32
}

func (w *singleWriterWitnessTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	cur := w.active.Add(1)
	defer w.active.Add(-1)
	for {
		prev := w.maxActive.Load()
		if cur <= prev || w.maxActive.CompareAndSwap(prev, cur) {
			break
		}
	}
	return w.inner.WithTx(ctx, fn)
}

func (w *singleWriterWitnessTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	cur := w.active.Add(1)
	defer w.active.Add(-1)
	for {
		prev := w.maxActive.Load()
		if cur <= prev || w.maxActive.CompareAndSwap(prev, cur) {
			break
		}
	}
	return w.inner.WithReadCommittedTx(ctx, fn)
}

type cancelOnThirdAssetTransactor struct {
	inner      storage.Transactor
	count      atomic.Int32
	cancelAt   int32
	cancelFunc context.CancelFunc
}

func (c *cancelOnThirdAssetTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return c.inner.WithTx(ctx, func(tx storage.Tx) error {
		return fn(&cancelOnThirdAssetTx{Tx: tx, parent: c})
	})
}

func (c *cancelOnThirdAssetTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return c.inner.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		return fn(&cancelOnThirdAssetTx{Tx: tx, parent: c})
	})
}

type cancelOnThirdAssetTx struct {
	storage.Tx
	parent *cancelOnThirdAssetTransactor
}

func (c *cancelOnThirdAssetTx) CatalogueWrites() storage.CatalogueWriteRepository {
	return &cancelOnThirdAssetRepo{CatalogueWriteRepository: c.Tx.CatalogueWrites(), parent: c.parent}
}

type cancelOnThirdAssetRepo struct {
	storage.CatalogueWriteRepository
	parent *cancelOnThirdAssetTransactor
}

func (r *cancelOnThirdAssetRepo) CreateAsset(ctx context.Context, asset *domain.MediaAsset) error {
	if r.parent.count.Add(1) == r.parent.cancelAt {
		r.parent.cancelFunc()
		return context.Canceled
	}
	return r.CatalogueWriteRepository.CreateAsset(ctx, asset)
}

func tableCounts(ctx context.Context, t *testing.T, db *testdb.TestDB, tables ...string) map[string]int {
	t.Helper()
	out := make(map[string]int, len(tables))
	for _, table := range tables {
		var n int
		if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		out[table] = n
	}
	return out
}

func listAssetsForRoot(ctx context.Context, t *testing.T, tx storage.Transactor, rootID string) []domain.MediaAsset {
	t.Helper()
	var assets []domain.MediaAsset
	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		var err error
		assets, err = st.CatalogueWrites().ListAssetsByRoot(ctx, rootID)
		return err
	}); err != nil {
		t.Fatalf("ListAssetsByRoot(%s): %v", rootID, err)
	}
	return assets
}

func listOutboxEventsForLibrary(ctx context.Context, t *testing.T, db *testdb.TestDB) []string {
	t.Helper()
	rows, err := db.Query(ctx, `SELECT kind FROM domain_events ORDER BY created_at ASC, id ASC`)
	if err != nil {
		t.Fatalf("query domain_events: %v", err)
	}
	defer rows.Close()
	var kinds []string
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			t.Fatalf("scan domain_event kind: %v", err)
		}
		kinds = append(kinds, kind)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate domain_events: %v", err)
	}
	return kinds
}

func TestScanner_MultiRootIndexingUnchangedRescanAndChangedFile(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	base := t.TempDir()
	rootA := filepath.Join(base, "root-a")
	rootB := filepath.Join(base, "root-b")

	cbzSeries1 := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Series>Shared Chronicle</Series>
  <Number>1</Number>
  <Title>Arrival</Title>
  <Writer>Author One</Writer>
  <Characters>Hero One</Characters>
</ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-1"), method: zip.Store},
	)
	cbzSeries2 := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Series>Shared Chronicle</Series>
  <Number>2</Number>
  <Title>Departure</Title>
  <Writer>Author One</Writer>
</ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-2"), method: zip.Store},
	)
	cbzStandalone := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("solo"), method: zip.Store})
	cbzFilename := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("fname"), method: zip.Store})

	writeDiskFile(t, filepath.Join(rootA, "Shared/Vol01.cbz"), cbzSeries1)
	writeDiskFile(t, filepath.Join(rootA, "Standalone.cbz"), cbzStandalone)
	writeDiskFile(t, filepath.Join(rootA, ".hidden.cbz"), cbzStandalone)
	writeDiskFile(t, filepath.Join(rootA, "notes.txt"), []byte("ignore"))
	writeDiskFile(t, filepath.Join(rootB, "More/Vol02.cbz"), cbzSeries2)
	writeDiskFile(t, filepath.Join(rootB, "FileSeries v03.cbz"), cbzFilename)

	var (
		lib   domain.Library
		roots []domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Multi-Root Library", domain.DefaultResolverChain(), []byte(rootA), []byte(rootB))
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	snapA := snapshotTree(t, rootA)
	snapB := snapshotTree(t, rootB)

	witness := &singleWriterWitnessTransactor{inner: transactor}
	sc, err := scanner.NewScanner(witness, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}

	res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{Workers: 4})
	if err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}
	if !res.Complete {
		t.Fatalf("ScanLibrary reported Complete=false: %+v", res)
	}
	if res.IndexedCount != 4 || res.UnchangedCount != 0 || res.ChangedCount != 0 || res.SkippedCount != 2 {
		t.Fatalf("unexpected first scan counts: %+v", res)
	}
	if len(res.Roots) != 2 || !res.Roots[0].Complete || !res.Roots[1].Complete {
		t.Fatalf("unexpected root results: %+v", res.Roots)
	}
	if witness.maxActive.Load() != 1 {
		t.Fatalf("expected single sequential writer (maxActive=1), got %d", witness.maxActive.Load())
	}

	assertTreeUnchanged(t, "rootA", rootA, snapA)
	assertTreeUnchanged(t, "rootB", rootB, snapB)

	assetsA := listAssetsForRoot(ctx, t, transactor, roots[0].ID)
	assetsB := listAssetsForRoot(ctx, t, transactor, roots[1].ID)
	if len(assetsA) != 2 || len(assetsB) != 2 {
		t.Fatalf("expected 2 assets per root, got %d and %d", len(assetsA), len(assetsB))
	}

	var seriesCount, volumeCount, personCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM series WHERE library_id = $1`, lib.ID).Scan(&seriesCount); err != nil {
		t.Fatalf("count series: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM volumes WHERE library_id = $1`, lib.ID).Scan(&volumeCount); err != nil {
		t.Fatalf("count volumes: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM people`).Scan(&personCount); err != nil {
		t.Fatalf("count people: %v", err)
	}
	if seriesCount != 2 || volumeCount != 4 || personCount != 1 {
		t.Fatalf("unexpected catalogue counts series=%d volumes=%d people=%d (want 2, 4, 1)", seriesCount, volumeCount, personCount)
	}

	trackedTables := []string{
		"series", "volumes", "series_memberships", "media_assets",
		"asset_digests", "people", "credits", "entities", "volume_entities", "domain_events",
	}
	beforeRescanCounts := tableCounts(ctx, t, db, trackedTables...)
	var maxUpdatedBefore time.Time
	if err := db.QueryRow(ctx, `SELECT MAX(updated_at) FROM media_assets`).Scan(&maxUpdatedBefore); err != nil {
		t.Fatalf("max updated_at: %v", err)
	}

	// Second scan on unchanged tree: must open zero archives and write zero rows.
	var openCalls atomic.Int32
	res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		Workers: 2,
		Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			openCalls.Add(1)
			return os.Open(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("rescan: %v", err)
	}
	if !res2.Complete || res2.UnchangedCount != 4 || res2.IndexedCount != 0 || res2.ChangedCount != 0 || res2.SkippedCount != 2 {
		t.Fatalf("unexpected rescan result: %+v", res2)
	}
	if openCalls.Load() != 0 {
		t.Fatalf("expected 0 archive opens on unchanged rescan, got %d", openCalls.Load())
	}
	afterRescanCounts := tableCounts(ctx, t, db, trackedTables...)
	for _, table := range trackedTables {
		if afterRescanCounts[table] != beforeRescanCounts[table] {
			t.Fatalf("table %s row count changed on unchanged rescan: %d -> %d", table, beforeRescanCounts[table], afterRescanCounts[table])
		}
	}
	var maxUpdatedAfter time.Time
	if err := db.QueryRow(ctx, `SELECT MAX(updated_at) FROM media_assets`).Scan(&maxUpdatedAfter); err != nil {
		t.Fatalf("max updated_at after: %v", err)
	}
	if !maxUpdatedAfter.Equal(maxUpdatedBefore) {
		t.Fatalf("media_assets.updated_at changed on unchanged rescan: %v -> %v", maxUpdatedBefore, maxUpdatedAfter)
	}

	// Modify one existing file on disk: size/mtime change triggers EXTERNAL_MODIFY reconciliation.
	modifiedBytes := buildDeterministicZip(t, "",
		zipEntry{name: "001.jpg", data: []byte("new-page-1-longer"), method: zip.Store},
		zipEntry{name: "002.jpg", data: []byte("new-page-2"), method: zip.Store},
	)
	standalonePath := filepath.Join(rootA, "Standalone.cbz")
	if err := os.WriteFile(standalonePath, modifiedBytes, 0o600); err != nil {
		t.Fatalf("overwrite Standalone.cbz: %v", err)
	}
	newTime := time.Now().Add(2 * time.Second).UTC()
	if err := os.Chtimes(standalonePath, newTime, newTime); err != nil {
		t.Fatalf("chtimes Standalone.cbz: %v", err)
	}

	openCalls.Store(0)
	res3, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			openCalls.Add(1)
			return os.Open(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("scan after file modify: %v", err)
	}
	if !res3.Complete || openCalls.Load() != 1 {
		t.Fatalf("expected Complete=true and 1 open for modified file, got Complete=%v opens=%d", res3.Complete, openCalls.Load())
	}
	if res3.UnchangedCount != 3 || res3.IndexedCount != 0 || res3.ChangedCount != 1 || res3.ModifiedCount != 1 || res3.MovedCount != 0 || res3.SkippedCount != 2 {
		t.Fatalf("unexpected counts after modify: %+v", res3)
	}
	if len(res3.Roots[0].Changed) != 1 || res3.Roots[0].ModifiedCount != 1 {
		t.Fatalf("expected 1 changed & modified file in rootA, got %+v", res3.Roots[0])
	}
	ch := res3.Roots[0].Changed[0]
	if string(ch.Existing.RelPathBytes) != "Standalone.cbz" || ch.Inspected.PageCount != 2 || ch.Inspected.Size != int64(len(modifiedBytes)) {
		t.Fatalf("unexpected ChangedFile payload: %+v", ch)
	}

	afterModifyCounts := tableCounts(ctx, t, db, trackedTables...)
	for _, table := range trackedTables {
		want := beforeRescanCounts[table]
		if table == "asset_digests" || table == "domain_events" {
			want++
		}
		if afterModifyCounts[table] != want {
			t.Fatalf("after modify table %s count = %d, want %d", table, afterModifyCounts[table], want)
		}
	}

	var gotAssetPages, gotVolPages int
	if err := db.QueryRow(ctx, `
		SELECT a.page_count, v.page_count
		FROM media_assets a
		JOIN volumes v ON v.id = a.volume_id
		WHERE a.id = $1`, ch.Existing.ID,
	).Scan(&gotAssetPages, &gotVolPages); err != nil {
		t.Fatalf("query modified page counts: %v", err)
	}
	if gotAssetPages != 2 || gotVolPages != 2 {
		t.Fatalf("modified page counts = (asset=%d, volume=%d), want (2, 2)", gotAssetPages, gotVolPages)
	}

	var digestTotal, digestCurrent, extModCurrent int
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*),
		       COUNT(*) FILTER (WHERE is_current),
		       COUNT(*) FILTER (WHERE is_current AND reason = 'EXTERNAL_MODIFY')
		FROM asset_digests
		WHERE asset_id = $1`, ch.Existing.ID,
	).Scan(&digestTotal, &digestCurrent, &extModCurrent); err != nil {
		t.Fatalf("query asset_digests: %v", err)
	}
	if digestTotal != 2 || digestCurrent != 1 || extModCurrent != 1 {
		t.Fatalf("unexpected digest rotation counts total=%d current=%d extModCurrent=%d", digestTotal, digestCurrent, extModCurrent)
	}

	var evKind string
	if err := db.QueryRow(ctx, `
		SELECT kind
		FROM domain_events
		WHERE aggregate_id = $1
		ORDER BY created_at DESC, id DESC
		LIMIT 1`, ch.Existing.ID,
	).Scan(&evKind); err != nil {
		t.Fatalf("query latest event for modified asset: %v", err)
	}
	if evKind != string(domain.EventAssetModified) {
		t.Fatalf("latest event kind = %q, want %q", evKind, domain.EventAssetModified)
	}

	// Fourth scan on unchanged tree after modification: opens zero archives and writes zero rows.
	openCalls.Store(0)
	res4, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			openCalls.Add(1)
			return os.Open(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("4th scan: %v", err)
	}
	if !res4.Complete || res4.UnchangedCount != 4 || res4.ModifiedCount != 0 || res4.MovedCount != 0 || openCalls.Load() != 0 {
		t.Fatalf("unexpected 4th scan result: res=%+v opens=%d", res4, openCalls.Load())
	}
}

func TestScanner_NFCAndNFDInSeparateRoots(t *testing.T) {
	t.Parallel()
	_, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	base := t.TempDir()
	nfcRoot := filepath.Join(base, "nfc")
	nfdRoot := filepath.Join(base, "nfd")

	nfcRel := "Series/\u304c.cbz"       // が (NFC)
	nfdRel := "Series/\u304b\u3099.cbz" // か + U+3099 (NFD)
	cbzA := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("nfc"), method: zip.Store})
	cbzB := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("nfd"), method: zip.Store})
	writeDiskFile(t, filepath.Join(nfcRoot, nfcRel), cbzA)
	writeDiskFile(t, filepath.Join(nfdRoot, nfdRel), cbzB)

	var (
		lib   domain.Library
		roots []domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, roots = seedLibraryAndRoots(
			ctx, t, tx, "NFC-NFD Lib",
			[]domain.ResolverName{domain.ResolverFolder, domain.ResolverMetadata, domain.ResolverFilename},
			[]byte(nfcRoot), []byte(nfdRoot),
		)
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	snapNFC := snapshotTree(t, nfcRoot)
	snapNFD := snapshotTree(t, nfdRoot)

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}
	if !res.Complete || res.IndexedCount != 2 || res.SkippedCount != 0 {
		t.Fatalf("unexpected scan result: %+v", res)
	}

	assertTreeUnchanged(t, "nfcRoot", nfcRoot, snapNFC)
	assertTreeUnchanged(t, "nfdRoot", nfdRoot, snapNFD)

	assetsByRoot := make(map[string]domain.MediaAsset, 2)
	for _, r := range roots {
		list := listAssetsForRoot(ctx, t, transactor, r.ID)
		if len(list) != 1 {
			t.Fatalf("root %s expected 1 asset, got %d", r.ID, len(list))
		}
		assetsByRoot[r.ID] = list[0]
	}

	aNFC := assetsByRoot[roots[0].ID]
	aNFD := assetsByRoot[roots[1].ID]
	if aNFC.ID == aNFD.ID {
		t.Fatalf("expected distinct asset IDs, both got %q", aNFC.ID)
	}
	if !bytes.Equal(aNFC.RelPathBytes, []byte(nfcRel)) {
		t.Fatalf("NFC RelPathBytes = %x, want %x", aNFC.RelPathBytes, []byte(nfcRel))
	}
	if !bytes.Equal(aNFD.RelPathBytes, []byte(nfdRel)) {
		t.Fatalf("NFD RelPathBytes = %x, want %x", aNFD.RelPathBytes, []byte(nfdRel))
	}
	if aNFC.RelPathMatch != aNFD.RelPathMatch {
		t.Fatalf("expected equal RelPathMatch keys, got %q vs %q", aNFC.RelPathMatch, aNFD.RelPathMatch)
	}
	if aNFC.Diagnostics.InvalidUTF8Path || aNFD.Diagnostics.InvalidUTF8Path {
		t.Fatalf("unexpected InvalidUTF8Path diagnostic on valid NFC/NFD paths")
	}
}

func TestScanner_InvalidUTF8NameIndexedAndFlagged(t *testing.T) {
	t.Parallel()
	cbzBytes := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("raw"), method: zip.Store})
	rel := []byte{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', 0x80, '.', 'c', 'b', 'z'}

	assertInvalidAsset := func(t *testing.T, ctx context.Context, db *testdb.TestDB, tx storage.Transactor, rootID string) {
		t.Helper()
		assets := listAssetsForRoot(ctx, t, tx, rootID)
		if len(assets) != 1 {
			t.Fatalf("expected 1 asset, got %d", len(assets))
		}
		got := assets[0]
		if !bytes.Equal(got.RelPathBytes, rel) {
			t.Fatalf("RelPathBytes = %x, want %x", got.RelPathBytes, rel)
		}
		if got.RelPathDisplay != `\xff\xfe/book\x80.cbz` {
			t.Fatalf("RelPathDisplay = %q, want %q", got.RelPathDisplay, `\xff\xfe/book\x80.cbz`)
		}
		if !got.Diagnostics.InvalidUTF8Path {
			t.Fatal("expected Diagnostics.InvalidUTF8Path=true")
		}
		events := listOutboxEventsForLibrary(ctx, t, db)
		if !slices.Contains(events, string(domain.EventAssetAdded)) ||
			!slices.Contains(events, string(domain.EventAssetInvalidUTF8Path)) {
			t.Fatalf("expected asset.added and asset.invalid_utf8_path events, got %v", events)
		}
	}

	t.Run("on disk when filesystem permits", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

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

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Invalid UTF8 Disk", nil, root)
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		snap := snapshotTree(t, string(root))
		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("ScanLibrary: %v", err)
		}
		if !res.Complete || res.IndexedCount != 1 || res.SkippedCount != 0 {
			t.Fatalf("unexpected scan result: %+v", res)
		}
		assertTreeUnchanged(t, "invalid-utf8-root", string(root), snap)
		assertInvalidAsset(t, ctx, db, transactor, roots[0].ID)
	})

	t.Run("synthetic invalid UTF-8 across all platforms", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		fakeRoot := []byte("/synthetic/invalid-utf8-root")
		badDirName := string([]byte{0xff, 0xfe})
		badFileName := string([]byte{'b', 'o', 'o', 'k', 0x80, '.', 'c', 'b', 'z'})
		now := time.Unix(1700000000, 0).UTC()

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Invalid UTF8 Synth", nil, fakeRoot)
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
			ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
				switch string(rawDir) {
				case "/synthetic/invalid-utf8-root":
					return []os.DirEntry{syntheticDirEntry{name: badDirName, mode: fs.ModeDir, mtime: now}}, nil
				case string(append([]byte("/synthetic/invalid-utf8-root/"), []byte(badDirName)...)):
					return []os.DirEntry{syntheticDirEntry{name: badFileName, mode: 0, size: int64(len(cbzBytes)), mtime: now}}, nil
				default:
					return nil, os.ErrNotExist
				}
			},
			Stat: func(rawPath []byte) (os.FileInfo, error) {
				switch string(rawPath) {
				case "/synthetic/invalid-utf8-root":
					return syntheticFileInfo{name: "invalid-utf8-root", mode: fs.ModeDir | 0o755, mtime: now, id: 1}, nil
				case string(append([]byte("/synthetic/invalid-utf8-root/"), []byte(badDirName)...)):
					return syntheticFileInfo{name: badDirName, mode: fs.ModeDir | 0o755, mtime: now, id: 2}, nil
				default:
					return syntheticFileInfo{name: badFileName, mode: 0o644, size: int64(len(cbzBytes)), mtime: now, id: 3}, nil
				}
			},
			Open: func(_ context.Context, _ []byte) (scanner.ReadAtCloser, error) {
				return &bytesReadAtCloser{Reader: bytes.NewReader(cbzBytes)}, nil
			},
		})
		if err != nil {
			t.Fatalf("ScanLibrary synthetic: %v", err)
		}
		if !res.Complete || res.IndexedCount != 1 {
			t.Fatalf("unexpected synthetic result: %+v", res)
		}
		assertInvalidAsset(t, ctx, db, transactor, roots[0].ID)
	})
}

func TestScanner_MetadataFirstVsFolderFirstAndStandalones(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	metaRoot := filepath.Join(t.TempDir(), "meta")
	folderRoot := filepath.Join(t.TempDir(), "folder")

	seriesBook := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Series>Metadata Title</Series>
  <Number>1</Number>
</ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("series-book"), method: zip.Store},
	)
	standaloneBook := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("standalone"), method: zip.Store})

	for _, root := range []string{metaRoot, folderRoot} {
		writeDiskFile(t, filepath.Join(root, "Folder Title/issue.cbz"), seriesBook)
		writeDiskFile(t, filepath.Join(root, "OneShot.cbz"), standaloneBook)
	}

	var metaLib, folderLib domain.Library
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		metaLib, _ = seedLibraryAndRoots(
			ctx, t, tx, "Meta-First",
			[]domain.ResolverName{domain.ResolverMetadata, domain.ResolverFolder, domain.ResolverFilename},
			[]byte(metaRoot),
		)
		folderLib, _ = seedLibraryAndRoots(
			ctx, t, tx, "Folder-First",
			[]domain.ResolverName{domain.ResolverFolder, domain.ResolverMetadata, domain.ResolverFilename},
			[]byte(folderRoot),
		)
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	snapMeta := snapshotTree(t, metaRoot)
	snapFolder := snapshotTree(t, folderRoot)

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	resMeta, err := sc.ScanLibrary(ctx, metaLib.ID, scanner.ScanOptions{})
	if err != nil || !resMeta.Complete || resMeta.IndexedCount != 2 {
		t.Fatalf("ScanLibrary(metaLib) = (%+v, %v)", resMeta, err)
	}
	resFolder, err := sc.ScanLibrary(ctx, folderLib.ID, scanner.ScanOptions{})
	if err != nil || !resFolder.Complete || resFolder.IndexedCount != 2 {
		t.Fatalf("ScanLibrary(folderLib) = (%+v, %v)", resFolder, err)
	}

	assertTreeUnchanged(t, "metaRoot", metaRoot, snapMeta)
	assertTreeUnchanged(t, "folderRoot", folderRoot, snapFolder)

	loadSeriesTitle := func(libID string) (string, string) {
		var title, resolverKey string
		if err := db.QueryRow(ctx, `SELECT title, resolver_key FROM series WHERE library_id = $1`, libID).Scan(&title, &resolverKey); err != nil {
			t.Fatalf("query series for %s: %v", libID, err)
		}
		return title, resolverKey
	}
	metaTitle, metaKey := loadSeriesTitle(metaLib.ID)
	folderTitle, folderKey := loadSeriesTitle(folderLib.ID)
	if metaTitle != "Metadata Title" || folderTitle != "Folder Title" || metaKey == folderKey {
		t.Fatalf("expected distinct series: meta=(%q, %q) folder=(%q, %q)", metaTitle, metaKey, folderTitle, folderKey)
	}

	for _, libID := range []string{metaLib.ID, folderLib.ID} {
		var standaloneCount int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM volumes v
			WHERE v.library_id = $1
			  AND NOT EXISTS (SELECT 1 FROM series_memberships sm WHERE sm.volume_id = v.id)
		`, libID).Scan(&standaloneCount); err != nil {
			t.Fatalf("count standalone volumes for %s: %v", libID, err)
		}
		if standaloneCount != 1 {
			t.Fatalf("library %s expected 1 standalone volume, got %d", libID, standaloneCount)
		}
	}
}

func TestScanner_FailuresSkippedAndWalkCompleteness(t *testing.T) {
	t.Parallel()

	goodA := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("a"), method: zip.Store})
	goodB := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("b"), method: zip.Store})
	goodC := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("c"), method: zip.Store})

	t.Run("corrupt archive skipped while remaining files index and walk completes", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		base := t.TempDir()
		root1 := filepath.Join(base, "r1")
		root2 := filepath.Join(base, "r2")
		writeDiskFile(t, filepath.Join(root1, "corrupt.cbz"), []byte("not-a-zip"))
		writeDiskFile(t, filepath.Join(root1, "good1.cbz"), goodA)
		writeDiskFile(t, filepath.Join(root2, "good2.cbz"), goodB)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Corrupt Skip Lib", nil, []byte(root1), []byte(root2))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("ScanLibrary: %v", err)
		}
		if !res.Complete || res.IndexedCount != 2 || res.SkippedCount != 1 {
			t.Fatalf("unexpected scan result: %+v", res)
		}
		if len(res.Roots[0].Skipped) != 1 || res.Roots[0].Skipped[0].Reason != scanner.SkipReasonCorruptArchive {
			t.Fatalf("expected corrupt_archive skip in root 0, got %+v", res.Roots[0].Skipped)
		}
		if len(listAssetsForRoot(ctx, t, transactor, roots[0].ID)) != 1 ||
			len(listAssetsForRoot(ctx, t, transactor, roots[1].ID)) != 1 {
			t.Fatal("expected good1.cbz and good2.cbz to be persisted")
		}
	})

	t.Run("indexing failure skips failed file rolls back its tx and commits others", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		base := t.TempDir()
		root1 := filepath.Join(base, "r1")
		root2 := filepath.Join(base, "r2")
		writeDiskFile(t, filepath.Join(root1, "alpha.cbz"), goodA)
		writeDiskFile(t, filepath.Join(root1, "fail.cbz"), goodB)
		writeDiskFile(t, filepath.Join(root2, "omega.cbz"), goodC)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Index Fail Lib", nil, []byte(root1), []byte(root2))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		injectedErr := errors.New("injected asset failure")
		faultTx := &faultInjectingTransactor{
			inner:         transactor,
			failStage:     "asset_create",
			failAssetPath: "fail.cbz",
			err:           injectedErr,
		}
		scFault, err := scanner.NewScanner(faultTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(faultTx): %v", err)
		}

		res, err := scFault.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("ScanLibrary: %v", err)
		}
		if !res.Complete || res.IndexedCount != 2 || res.SkippedCount != 1 {
			t.Fatalf("unexpected result with indexing failure: %+v", res)
		}
		if len(res.Roots[0].Skipped) != 1 ||
			res.Roots[0].Skipped[0].Reason != scanner.SkipReasonIndexFailed ||
			!errors.Is(res.Roots[0].Skipped[0].Err, injectedErr) {
			t.Fatalf("expected index_failed skip with injected error, got %+v", res.Roots[0].Skipped)
		}

		// Verify fail.cbz left zero orphan volumes/events and alpha/omega committed.
		var volCount, eventCount int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM volumes WHERE library_id = $1`, lib.ID).Scan(&volCount); err != nil {
			t.Fatalf("count volumes: %v", err)
		}
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events`).Scan(&eventCount); err != nil {
			t.Fatalf("count events: %v", err)
		}
		if volCount != 2 || eventCount != 2 {
			t.Fatalf("expected 2 volumes and 2 events after rollback of fail.cbz, got %d and %d", volCount, eventCount)
		}

		// Rerun without fault: committed alpha.cbz and omega.cbz are skipped without opening; fail.cbz indexes.
		scClean, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner(clean): %v", err)
		}
		var openCalls atomic.Int32
		res2, err := scClean.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				openCalls.Add(1)
				return os.Open(string(rawPath))
			},
		})
		if err != nil {
			t.Fatalf("rescan after indexing failure: %v", err)
		}
		if !res2.Complete || res2.IndexedCount != 1 || res2.UnchangedCount != 2 || openCalls.Load() != 1 {
			t.Fatalf("unexpected recovery rescan: res=%+v opens=%d", res2, openCalls.Load())
		}
		if len(listAssetsForRoot(ctx, t, transactor, roots[0].ID)) != 2 {
			t.Fatal("expected both root1 assets after recovery rescan")
		}
	})

	t.Run("unreadable subtree in one root indexes siblings and reports incomplete prefix", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		base := t.TempDir()
		root1 := filepath.Join(base, "r1")
		root2 := filepath.Join(base, "r2")
		writeDiskFile(t, filepath.Join(root1, "alpha.cbz"), goodA)
		writeDiskFile(t, filepath.Join(root1, "locked_dir/hidden.cbz"), goodB)
		writeDiskFile(t, filepath.Join(root2, "beta.cbz"), goodC)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Incomplete Subtree Lib", nil, []byte(root1), []byte(root2))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		logHandler := &scanLogHandler{}
		sc, err := scanner.NewScanner(transactor, scanner.Options{
			Logger: slog.New(logHandler),
		})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		var blockSubdir atomic.Bool
		blockSubdir.Store(true)
		res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
			ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
				if blockSubdir.Load() && filepath.Base(string(rawDir)) == "locked_dir" {
					return nil, syscall.EACCES
				}
				return os.ReadDir(string(rawDir))
			},
		})
		if err != nil {
			t.Fatalf("ScanLibrary: %v", err)
		}
		if res.Complete || !res.ApplyRemovals {
			t.Fatalf("expected Complete=false and ApplyRemovals=true when root1 had an unreadable subtree, got %+v", res)
		}
		if res.Roots[0].Complete || !res.Roots[1].Complete {
			t.Fatalf("expected root0 Complete=false and root1 Complete=true, got %v / %v", res.Roots[0].Complete, res.Roots[1].Complete)
		}
		if res.IndexedCount != 2 || res.SkippedCount != 1 {
			t.Fatalf("expected 2 indexed and 1 skipped dir when subtree is unreadable, got %+v", res)
		}
		if len(res.Roots[0].IncompletePrefixes) != 1 ||
			res.Roots[0].IncompletePrefixes[0].RelPath.Display != "locked_dir" ||
			res.Roots[0].IncompletePrefixes[0].Reason != scanner.SkipReasonUnreadable {
			t.Fatalf("Roots[0].IncompletePrefixes = %+v, want [locked_dir unreadable]", res.Roots[0].IncompletePrefixes)
		}
		if len(res.IncompletePrefixes) != 1 ||
			res.IncompletePrefixes[0].LibraryRootID != roots[0].ID ||
			res.IncompletePrefixes[0].RelPath.Display != "locked_dir" {
			t.Fatalf("res.IncompletePrefixes = %+v, want [(%s, locked_dir)]", res.IncompletePrefixes, roots[0].ID)
		}
		if len(listAssetsForRoot(ctx, t, transactor, roots[0].ID)) != 1 ||
			len(listAssetsForRoot(ctx, t, transactor, roots[1].ID)) != 1 {
			t.Fatal("expected alpha.cbz and beta.cbz committed despite unreadable locked_dir")
		}

		logHandler.mu.Lock()
		records := slices.Clone(logHandler.records)
		logHandler.mu.Unlock()
		if len(records) != 1 || records[0].Level != slog.LevelWarn ||
			records[0].Attrs["prefix"] != "locked_dir" ||
			records[0].Attrs["library_root_id"] != roots[0].ID {
			t.Fatalf("expected 1 Warn diagnostic for locked_dir, got %+v", records)
		}

		// Once locked_dir becomes readable, the remaining hidden.cbz indexes cleanly.
		blockSubdir.Store(false)
		res2, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("recovery ScanLibrary: %v", err)
		}
		if !res2.Complete || !res2.ApplyRemovals || res2.IndexedCount != 1 || res2.UnchangedCount != 2 || len(res2.IncompletePrefixes) != 0 {
			t.Fatalf("unexpected recovery result: %+v", res2)
		}
		if len(listAssetsForRoot(ctx, t, transactor, roots[0].ID)) != 2 {
			t.Fatal("expected both root1 assets after recovery")
		}
	})

	t.Run("missing root fails pre-check and stops scan before walking surviving roots", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		base := t.TempDir()
		missingRoot := filepath.Join(base, "a-missing")
		healthyRoot := filepath.Join(base, "b-healthy")
		writeDiskFile(t, filepath.Join(healthyRoot, "ok.cbz"), goodA)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Missing Root Lib", nil, []byte(missingRoot), []byte(healthyRoot))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{})
		if err != nil {
			t.Fatalf("ScanLibrary(offline): %v", err)
		}
		if !res.Offline || res.Complete || res.ApplyRemovals {
			t.Fatalf("expected Offline=true Complete=false ApplyRemovals=false, got %+v", res)
		}
		if res.IndexedCount != 0 || len(res.Roots) != 0 {
			t.Fatalf("expected scan to stop after pre-check with 0 walked roots, got %+v", res)
		}
		if len(res.PreCheck.Roots) != 2 ||
			res.PreCheck.Roots[0].State != domain.LibraryRootStateOffline ||
			res.PreCheck.Roots[1].State != domain.LibraryRootStateOnline {
			t.Fatalf("expected both roots evaluated before stopping, got %+v", res.PreCheck.Roots)
		}
		if len(listAssetsForRoot(ctx, t, transactor, roots[1].ID)) != 0 {
			t.Fatal("expected surviving root not to be indexed when library is OFFLINE")
		}
	})

	t.Run("walk error after passing pre-check marks incomplete and still walks surviving roots", func(t *testing.T) {
		t.Parallel()
		_, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		base := t.TempDir()
		flakyRoot := filepath.Join(base, "a-flaky")
		healthyRoot := filepath.Join(base, "b-healthy")
		writeDiskFile(t, filepath.Join(flakyRoot, "precheck.cbz"), goodA)
		writeDiskFile(t, filepath.Join(healthyRoot, "ok.cbz"), goodB)

		var (
			lib   domain.Library
			roots []domain.LibraryRoot
		)
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Walk Error Root Lib", nil, []byte(flakyRoot), []byte(healthyRoot))
			return nil
		}); err != nil {
			t.Fatalf("seed failed: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
			OpenDir: func(rawDir []byte) (scanner.DirStream, error) {
				return os.Open(string(rawDir))
			},
			ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
				if string(rawDir) == flakyRoot {
					return nil, os.ErrPermission
				}
				return os.ReadDir(string(rawDir))
			},
		})
		if err == nil {
			t.Fatal("expected error when one root walk fails")
		}
		if res.Complete || res.ApplyRemovals {
			t.Fatalf("expected Complete=false ApplyRemovals=false when a root walk failed, got %+v", res)
		}
		if len(res.Roots) != 2 {
			t.Fatalf("expected 2 root results, got %d", len(res.Roots))
		}
		if res.Roots[0].Complete || res.Roots[0].Err == nil {
			t.Fatalf("expected root walk error and Complete=false, got %+v", res.Roots[0])
		}
		if !res.Roots[1].Complete || res.Roots[1].IndexedCount != 0 || res.IndexedCount != 0 {
			t.Fatalf("expected healthy root walk to complete while deferring indexing, got root=%+v res=%+v", res.Roots[1], res)
		}
		if len(listAssetsForRoot(ctx, t, transactor, roots[1].ID)) != 0 {
			t.Fatal("expected healthy root assets not to commit until all roots walk completely")
		}
	})
}

func TestScanner_InterruptedScanCommitsCompletedFilesAndRerunSkipsThem(t *testing.T) {
	t.Parallel()
	_, transactor := setupIndexerTestDB(t)

	root := filepath.Join(t.TempDir(), "root")
	cbz1 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("1"), method: zip.Store})
	cbz2 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("2"), method: zip.Store})
	cbz3 := buildDeterministicZip(t, "", zipEntry{name: "001.jpg", data: []byte("3"), method: zip.Store})
	writeDiskFile(t, filepath.Join(root, "01.cbz"), cbz1)
	writeDiskFile(t, filepath.Join(root, "02.cbz"), cbz2)
	writeDiskFile(t, filepath.Join(root, "03.cbz"), cbz3)

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer setupCancel()

	var (
		lib   domain.Library
		roots []domain.LibraryRoot
	)
	if err := transactor.WithTx(setupCtx, func(tx storage.Tx) error {
		lib, roots = seedLibraryAndRoots(setupCtx, t, tx, "Interrupted Lib", nil, []byte(root))
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	scanCtx, scanCancel := context.WithCancel(setupCtx)
	defer scanCancel()

	// Cancel right as the 3rd file attempts to persist, after files 1 and 2 committed.
	cancelTx := &cancelOnThirdAssetTransactor{
		inner:      transactor,
		cancelAt:   3,
		cancelFunc: scanCancel,
	}
	scInterrupt, err := scanner.NewScanner(cancelTx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner(cancelTx): %v", err)
	}
	res1, err := scInterrupt.ScanLibrary(scanCtx, lib.ID, scanner.ScanOptions{Workers: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ScanLibrary interrupted err = %v, want context.Canceled", err)
	}
	if res1.Complete {
		t.Fatal("expected res1.Complete=false when cancelled mid-scan")
	}

	committed := listAssetsForRoot(setupCtx, t, transactor, roots[0].ID)
	if len(committed) != 2 {
		t.Fatalf("expected 2 committed assets before cancellation, got %d", len(committed))
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	var rerunOpens atomic.Int32
	res2, err := sc.ScanLibrary(setupCtx, lib.ID, scanner.ScanOptions{
		Workers: 1,
		Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			rerunOpens.Add(1)
			return os.Open(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("rerun ScanLibrary: %v", err)
	}
	if !res2.Complete || res2.UnchangedCount != 2 || res2.IndexedCount != 1 || rerunOpens.Load() != 1 {
		t.Fatalf("unexpected rerun result: res=%+v opens=%d", res2, rerunOpens.Load())
	}
	if len(listAssetsForRoot(setupCtx, t, transactor, roots[0].ID)) != 3 {
		t.Fatal("expected all 3 assets after rerun")
	}
}

func TestScanner_ValidationErrors(t *testing.T) {
	t.Parallel()
	_, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := scanner.NewScanner(nil, scanner.Options{}); err == nil {
		t.Fatal("expected error for nil transactor")
	}
	var nilScanner *scanner.Scanner
	if _, err := nilScanner.ScanLibrary(ctx, "lib-1", scanner.ScanOptions{}); err == nil {
		t.Fatal("expected error for nil scanner")
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(ctx, " ", scanner.ScanOptions{}); !errors.Is(err, domain.ErrEmptyLibraryID) {
		t.Fatalf("empty libraryID err = %v, want ErrEmptyLibraryID", err)
	}
	if _, err := sc.ScanLibrary(ctx, "lib-1", scanner.ScanOptions{Workers: -1}); !errors.Is(err, scanner.ErrInvalidWorkerCount) {
		t.Fatalf("negative workers err = %v, want ErrInvalidWorkerCount", err)
	}
	if _, err := sc.ScanLibrary(ctx, "missing-library", scanner.ScanOptions{}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("unknown library err = %v, want storage.ErrNotFound", err)
	}

	canceledCtx, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if _, err := sc.ScanLibrary(canceledCtx, "lib-1", scanner.ScanOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx err = %v, want context.Canceled", err)
	}
}

func TestScanner_UnreadableSubtreeProtectsAssetsFromMissingAndMovesWhileIndexingOthers(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	keep1Bytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Locked</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("locked-keep-1"), method: zip.Store},
	)
	keep2Bytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Locked</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("locked-keep-2"), method: zip.Store},
	)
	siblingDeleteBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>SiblingDelete</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("sibling-delete"), method: zip.Store},
	)
	healthyStayBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Healthy</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("healthy-stay"), method: zip.Store},
	)
	brandNewBytes := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Healthy</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("healthy-new"), method: zip.Store},
	)

	writeDiskFile(t, filepath.Join(rootDir, "locked_dir/keep1.cbz"), keep1Bytes)
	writeDiskFile(t, filepath.Join(rootDir, "locked_dir/nested/keep2.cbz"), keep2Bytes)
	deleteMePath := filepath.Join(rootDir, "locked_dir_sibling/delete_me.cbz")
	writeDiskFile(t, deleteMePath, siblingDeleteBytes)
	writeDiskFile(t, filepath.Join(rootDir, "healthy/stay.cbz"), healthyStayBytes)

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Scoped Subtree Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seedLibraryAndRoots: %v", err)
	}

	logHandler := &scanLogHandler{}
	sc, err := scanner.NewScanner(transactor, scanner.Options{
		Logger: slog.New(logHandler),
	})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 4 {
		t.Fatalf("baseline scan = (%+v, %v)", res, err)
	}

	baselineByPath := make(map[string]domain.MediaAsset)
	for _, a := range listAssetsForRoot(ctx, t, transactor, root.ID) {
		baselineByPath[a.RelPathDisplay] = a
	}

	// Delete locked_dir_sibling/delete_me.cbz (must not be shielded by locked_dir prefix),
	// add healthy/brand_new.cbz, and add a file with identical bytes to locked_dir/keep1.cbz.
	if err := os.Remove(deleteMePath); err != nil {
		t.Fatalf("Remove(deleteMePath): %v", err)
	}
	writeDiskFile(t, filepath.Join(rootDir, "healthy/brand_new.cbz"), brandNewBytes)
	writeDiskFile(t, filepath.Join(rootDir, "copies/copy_of_keep1.cbz"), keep1Bytes)

	res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{
		ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
			if string(rawDir) == filepath.Join(rootDir, "locked_dir") {
				return nil, syscall.EACCES
			}
			return os.ReadDir(string(rawDir))
		},
	})
	if err != nil {
		t.Fatalf("ScanLibrary with EACCES subdir: %v", err)
	}
	if res.Complete || !res.ApplyRemovals {
		t.Fatalf("expected Complete=false ApplyRemovals=true, got %+v", res)
	}
	if res.IndexedCount != 2 || res.MovedCount != 0 || res.MissingCount != 1 {
		t.Fatalf("counts = (indexed=%d, moved=%d, missing=%d), want (2, 0, 1): %+v",
			res.IndexedCount, res.MovedCount, res.MissingCount, res)
	}
	if len(res.IncompletePrefixes) != 1 || res.IncompletePrefixes[0].RelPath.Display != "locked_dir" {
		t.Fatalf("IncompletePrefixes = %+v, want [locked_dir]", res.IncompletePrefixes)
	}

	afterByPath := make(map[string]domain.MediaAsset)
	for _, a := range listAssetsForRoot(ctx, t, transactor, root.ID) {
		afterByPath[a.RelPathDisplay] = a
	}
	if len(afterByPath) != 6 {
		t.Fatalf("assets count = %d, want 6 (4 baseline + 2 new)", len(afterByPath))
	}
	for _, protectedRel := range []string{"locked_dir/keep1.cbz", "locked_dir/nested/keep2.cbz"} {
		got, ok := afterByPath[protectedRel]
		if !ok || got.ID != baselineByPath[protectedRel].ID || got.MissingSince != nil {
			t.Fatalf("protected asset %q = (%+v, %v), want active with original ID %s",
				protectedRel, got, ok, baselineByPath[protectedRel].ID)
		}
	}
	if gotDeleted := afterByPath["locked_dir_sibling/delete_me.cbz"]; gotDeleted.MissingSince == nil {
		t.Fatalf("expected locked_dir_sibling/delete_me.cbz marked missing, got %+v", gotDeleted)
	}
	if gotCopy, ok := afterByPath["copies/copy_of_keep1.cbz"]; !ok || gotCopy.ID == baselineByPath["locked_dir/keep1.cbz"].ID {
		t.Fatalf("expected copies/copy_of_keep1.cbz indexed as distinct new asset, got (%+v, %v)", gotCopy, ok)
	}
	if _, ok := afterByPath["healthy/brand_new.cbz"]; !ok {
		t.Fatal("expected healthy/brand_new.cbz indexed")
	}

	events := listOutboxEventsForLibrary(ctx, t, db)
	if slices.Contains(events, string(domain.EventAssetMoved)) {
		t.Fatalf("unexpected asset.moved event: %v", events)
	}

	logHandler.mu.Lock()
	records := slices.Clone(logHandler.records)
	logHandler.mu.Unlock()
	if len(records) != 1 || records[0].Level != slog.LevelWarn || records[0].Attrs["prefix"] != "locked_dir" {
		t.Fatalf("expected 1 Warn diagnostic for locked_dir, got %+v", records)
	}
}
