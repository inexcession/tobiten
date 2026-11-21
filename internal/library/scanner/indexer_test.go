package scanner_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/metadata/comicinfo"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

var (
	pkgDBOnce    sync.Once
	pkgDBCleanup func()
	pkgDBErr     error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if pkgDBCleanup != nil {
		pkgDBCleanup()
	}
	testdb.Stop()
	os.Exit(code)
}

func setupIndexerTestDB(t *testing.T) (*testdb.TestDB, *postgres.Transactor) {
	t.Helper()
	// Start the shared PostgreSQL instance lazily so non-DB scanner tests
	// (e.g. WalkRoot, PartialFingerprint) never invoke initdb.
	pkgDBOnce.Do(func() {
		var pkgDB *testdb.TestDB
		pkgDB, pkgDBCleanup, pkgDBErr = testdb.NewPackageDB()
		if pkgDB != nil {
			pkgDB.Close()
		}
	})
	if pkgDBErr != nil {
		t.Fatalf("setupIndexerTestDB: initializing testdb server: %v", pkgDBErr)
	}

	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupIndexerTestDB: applying migrations: %v", err)
	}
	return db, postgres.NewTransactor(db.Pool)
}

func seedLibraryAndRoots(
	ctx context.Context,
	t *testing.T,
	tx storage.Tx,
	name string,
	chain []domain.ResolverName,
	rootBytes ...[]byte,
) (domain.Library, []domain.LibraryRoot) {
	t.Helper()
	lib := domain.Library{
		Name:          name,
		Mode:          domain.LibraryModeVirtual,
		ResolverChain: chain,
	}
	if err := tx.Libraries().CreateLibrary(ctx, &lib); err != nil {
		t.Fatalf("CreateLibrary(%q): %v", name, err)
	}
	roots := make([]domain.LibraryRoot, len(rootBytes))
	for i, rb := range rootBytes {
		rp, err := domain.NewRootPathIdentity(rb)
		if err != nil {
			t.Fatalf("NewRootPathIdentity(%q): %v", rb, err)
		}
		r := domain.LibraryRoot{
			LibraryID: lib.ID,
			Path:      rp,
			State:     domain.LibraryRootStateOnline,
		}
		if err := tx.Libraries().CreateLibraryRoot(ctx, &r); err != nil {
			t.Fatalf("CreateLibraryRoot(%q): %v", rb, err)
		}
		roots[i] = r
	}
	return lib, roots
}

func mustRelIdentity(t *testing.T, raw []byte) domain.PathIdentity {
	t.Helper()
	p, err := domain.NewRelativePathIdentity(raw)
	if err != nil {
		t.Fatalf("NewRelativePathIdentity(%x): %v", raw, err)
	}
	return p
}

func int32Ptr(v int32) *int32 { return &v }

func TestIndexer_SingleCallCommitsCatalogueAndOutboxEvent(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Manga", nil, []byte("/srv/manga"))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	fixedNow := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	mtime := fixedNow.Add(-1 * time.Hour)
	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{
		Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	meta := &comicinfo.Metadata{
		Series:      "Dorohedoro",
		Title:       "Hole Dwellers",
		Number:      "1.5",
		Volume:      int32Ptr(2018),
		Count:       int32Ptr(23),
		Summary:     "Caiman searches for his face.",
		Publisher:   "Shogakukan",
		Imprint:     "IKKI",
		LanguageISO: "ja",
		AgeRating:   "Mature 17+",
		Year:        int32Ptr(2018),
		Month:       int32Ptr(3),
		Day:         int32Ptr(15),
		GTIN:        "9784091882813",
		Web:         "https://example.invalid/dorohedoro/1.5",
		Writer:      []string{"Q Hayashida"},
		Penciller:   []string{"Q Hayashida"},
		Inker:       []string{"Assistant A"},
		Colorist:    []string{"Colorist B"},
		Letterer:    []string{"Letterer C"},
		CoverArtist: []string{"Q Hayashida"},
		Editor:      []string{"Editor D"},
		Translator:  []string{"Translator E"},
		Characters:  []string{"Caiman", "Nikaido"},
		Teams:       []string{"En Family"},
		Locations:   []string{"Hole"},
		Genre:       []string{"Action", "Dark Fantasy"},
		Tags:        []string{"Seinen", "Magic"},
	}

	res, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: root.ID,
		Inspected: scanner.InspectedFile{
			RelPath:         mustRelIdentity(t, []byte("Dorohedoro/Dorohedoro v01.cbz")),
			Size:            4096,
			Mtime:           mtime,
			Format:          domain.MediaFormatCBZ,
			DigestKind:      domain.DigestKindXXH364Partial,
			Digest:          "0123456789abcdef",
			PageCount:       192,
			CoverPageIndex:  1,
			SuppressedPages: []int{0, 191},
			ImageCodecs:     []string{"jpeg", "png"},
			ComicInfo:       meta,
		},
	})
	if err != nil {
		t.Fatalf("IndexFile: %v", err)
	}

	if res.Series == nil || !res.SeriesCreated || !res.VolumeCreated {
		t.Fatalf("unexpected creation flags: series=%+v seriesCreated=%v volCreated=%v", res.Series, res.SeriesCreated, res.VolumeCreated)
	}
	if res.InvalidUTF8PathEvent != nil {
		t.Fatalf("unexpected invalid UTF-8 event for clean path: %+v", res.InvalidUTF8PathEvent)
	}

	// Verify persisted rows in fresh transactions/queries.
	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	gotSeries, err := cw.GetSeriesByID(ctx, res.Series.ID)
	if err != nil {
		t.Fatalf("GetSeriesByID: %v", err)
	}
	if gotSeries.Title != "Dorohedoro" ||
		gotSeries.ResolverKey == nil || *gotSeries.ResolverKey != "title:run=2018:dorohedoro" ||
		gotSeries.Publisher == nil || *gotSeries.Publisher != "Shogakukan" ||
		gotSeries.Imprint == nil || *gotSeries.Imprint != "IKKI" ||
		gotSeries.Language == nil || *gotSeries.Language != "ja" ||
		gotSeries.AgeRating == nil || *gotSeries.AgeRating != "Mature 17+" ||
		gotSeries.TotalVolumes == nil || *gotSeries.TotalVolumes != 23 ||
		gotSeries.ReleaseYear == nil || *gotSeries.ReleaseYear != 2018 ||
		!slices.Equal(gotSeries.Genres, []string{"Action", "Dark Fantasy"}) ||
		!slices.Equal(gotSeries.Tags, []string{"Seinen", "Magic"}) {
		t.Fatalf("unexpected series persisted: %+v", gotSeries)
	}

	gotVol, err := cw.GetVolumeByID(ctx, res.Volume.ID)
	if err != nil {
		t.Fatalf("GetVolumeByID: %v", err)
	}
	wantDate := time.Date(2018, 3, 15, 0, 0, 0, 0, time.UTC)
	if gotVol.Kind != domain.VolumeKindVolume ||
		gotVol.Number == nil || *gotVol.Number != 1.5 ||
		gotVol.Title == nil || *gotVol.Title != "Hole Dwellers" ||
		gotVol.Summary == nil || *gotVol.Summary != "Caiman searches for his face." ||
		gotVol.ReleaseDate == nil || !gotVol.ReleaseDate.Equal(wantDate) ||
		gotVol.PageCount != 192 ||
		gotVol.ExternalIDs["gtin"] != "9784091882813" ||
		gotVol.ExternalIDs["web"] != "https://example.invalid/dorohedoro/1.5" {
		t.Fatalf("unexpected volume persisted: %+v", gotVol)
	}

	memberships, err := postgres.NewSeriesMembershipRepository(db.Pool).ListMembershipsByVolume(ctx, gotVol.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume: %v", err)
	}
	if len(memberships) != 1 || memberships[0].SeriesID != gotSeries.ID || !memberships[0].IsPrimary ||
		memberships[0].Source != domain.MembershipSourceResolver || memberships[0].Position == nil || *memberships[0].Position != 1.5 {
		t.Fatalf("unexpected memberships: %+v", memberships)
	}

	gotAsset, err := cw.GetAssetByID(ctx, res.Asset.ID)
	if err != nil {
		t.Fatalf("GetAssetByID: %v", err)
	}
	if gotAsset.VolumeID != gotVol.ID || gotAsset.LibraryRootID != root.ID ||
		gotAsset.RelPathDisplay != "Dorohedoro/Dorohedoro v01.cbz" ||
		gotAsset.FileSize != 4096 || gotAsset.PageCount != 192 || gotAsset.CoverPageIndex != 1 ||
		!slices.Equal(gotAsset.SuppressedPages, []int{0, 191}) ||
		!slices.Equal(gotAsset.Diagnostics.ImageCodecs, []string{"jpeg", "png"}) ||
		gotAsset.Diagnostics.InvalidUTF8Path ||
		len(gotAsset.Digests) != 1 ||
		gotAsset.Digests[0].Kind != domain.DigestKindXXH364Partial ||
		gotAsset.Digests[0].Value != "0123456789abcdef" ||
		!gotAsset.Digests[0].IsCurrent ||
		gotAsset.Digests[0].Reason != domain.DigestReasonScan {
		t.Fatalf("unexpected asset persisted: %+v", gotAsset)
	}

	credits, err := postgres.NewCreditRepository(db.Pool).ListCreditsByVolume(ctx, gotVol.ID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume: %v", err)
	}
	if len(credits) != 8 {
		t.Fatalf("len(credits) = %d, want 8", len(credits))
	}
	links, err := postgres.NewEntityRepository(db.Pool).ListVolumeEntities(ctx, gotVol.ID)
	if err != nil {
		t.Fatalf("ListVolumeEntities: %v", err)
	}
	if len(links) != 4 {
		t.Fatalf("len(volume_entities) = %d, want 4 (2 chars + 1 team + 1 location)", len(links))
	}

	// Verify exactly one asset.added outbox event was persisted.
	evRepo := postgres.NewEventRepository(db.Pool)
	published, err := evRepo.PublishUnsequencedEvents(ctx, fixedNow)
	if err != nil {
		t.Fatalf("PublishUnsequencedEvents: %v", err)
	}
	if len(published) != 1 {
		t.Fatalf("len(published) = %d, want 1", len(published))
	}
	ev := published[0]
	if ev.Kind != domain.EventAssetAdded || ev.Version != domain.EventVersion1 ||
		ev.AggregateType != domain.AggregateTypeAsset || ev.AggregateID != gotAsset.ID {
		t.Fatalf("unexpected outbox event: %+v", ev)
	}
	payload, err := domain.DecodeEventPayload[domain.AssetEventPayloadV1](ev)
	if err != nil {
		t.Fatalf("DecodeEventPayload: %v", err)
	}
	if payload.AssetID != gotAsset.ID || payload.VolumeID != gotVol.ID ||
		payload.LibraryID != lib.ID || payload.LibraryRootID != root.ID ||
		payload.Format != domain.MediaFormatCBZ || payload.PathDisplay != "Dorohedoro/Dorohedoro v01.cbz" {
		t.Fatalf("unexpected AssetEventPayloadV1: %+v", payload)
	}
}

func TestIndexer_StandaloneAndSeriesReuseAndDistinctRuns(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib1  domain.Library
		lib2  domain.Library
		roots []domain.LibraryRoot
		root2 domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib1, roots = seedLibraryAndRoots(ctx, t, tx, "Lib 1", nil, []byte("/srv/lib1-a"), []byte("/srv/lib1-b"))
		var r2 []domain.LibraryRoot
		lib2, r2 = seedLibraryAndRoots(ctx, t, tx, "Lib 2", nil, []byte("/srv/lib2"))
		root2 = r2[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	// 1. Standalone root-level file with no metadata and no filename marker.
	standaloneRes, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib1.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("One-Shot Special.cbz")),
			Size:       500,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "1111111111111111",
			PageCount:  40,
		},
	})
	if err != nil {
		t.Fatalf("IndexFile standalone: %v", err)
	}
	if standaloneRes.Series != nil || standaloneRes.SeriesCreated || len(standaloneRes.Memberships) != 0 {
		t.Fatalf("expected standalone with no series or memberships, got %+v", standaloneRes)
	}
	var seriesCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM series WHERE library_id = $1`, lib1.ID).Scan(&seriesCount); err != nil {
		t.Fatalf("count series: %v", err)
	}
	if seriesCount != 0 {
		t.Fatalf("seriesCount = %d, want 0 after standalone", seriesCount)
	}

	// 2. Two files resolving to the same series (case/whitespace variation) reuse the series row.
	res1, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib1.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("RootA/Series v01.cbz")),
			Size:       1000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "2222222222222222",
			PageCount:  100,
			ComicInfo:  &comicinfo.Metadata{Series: "Example Series", Volume: int32Ptr(2018), Number: "1"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile series 1: %v", err)
	}
	res2, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib1.ID,
		LibraryRootID: roots[1].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("RootB/Series v02.cbz")),
			Size:       1000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "3333333333333333",
			PageCount:  105,
			ComicInfo:  &comicinfo.Metadata{Series: "  EXAMPLE   SERIES  ", Volume: int32Ptr(2018), Number: "2"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile series 2: %v", err)
	}
	if !res1.SeriesCreated || res2.SeriesCreated || res1.Series.ID != res2.Series.ID {
		t.Fatalf("expected second file to reuse series %s, got s1=%+v (created=%v) s2=%+v (created=%v)",
			res1.Series.ID, res1.Series, res1.SeriesCreated, res2.Series, res2.SeriesCreated)
	}
	if res1.Volume.ID == res2.Volume.ID {
		t.Fatal("expected distinct volume IDs for numbers 1 and 2")
	}

	// 3. Different run (Volume=2024 vs Volume=2018 vs nil) creates distinct series; same key in lib2 is distinct.
	resDiffRun, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib1.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("RootA/Series 2024 v01.cbz")),
			Size:       1000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "4444444444444444",
			ComicInfo:  &comicinfo.Metadata{Series: "Example Series", Volume: int32Ptr(2024), Number: "1"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile diff run: %v", err)
	}
	if !resDiffRun.SeriesCreated || resDiffRun.Series.ID == res1.Series.ID {
		t.Fatalf("expected new series for Volume=2024 run, got %+v", resDiffRun.Series)
	}

	resLib2, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib2.ID,
		LibraryRootID: root2.ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Series v01.cbz")),
			Size:       1000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "5555555555555555",
			ComicInfo:  &comicinfo.Metadata{Series: "Example Series", Volume: int32Ptr(2018), Number: "1"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile lib2: %v", err)
	}
	if !resLib2.SeriesCreated || resLib2.Series.ID == res1.Series.ID {
		t.Fatalf("expected separate series in lib2, got %+v", resLib2.Series)
	}
}

func TestIndexer_VariantGroupingAndNonGroupingRules(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib   domain.Library
		roots []domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Variants", nil, []byte("/srv/v-a"), []byte("/srv/v-b"))
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	// 1. Two files with same primary series, kind (VOLUME), and known number (1) attach to ONE volume.
	v1a, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("One Piece v01 [Scan].cbz")),
			Size:       1000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "aaaaaaaaaaaaaaaa",
			PageCount:  190,
			ComicInfo:  &comicinfo.Metadata{Series: "One Piece", Number: "1"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile v1a: %v", err)
	}
	v1b, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: roots[1].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("One Piece v01 [Digital].cbz")),
			Size:       2000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "bbbbbbbbbbbbbbbb",
			PageCount:  192,
			ComicInfo:  &comicinfo.Metadata{Series: "One Piece", Number: "1"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile v1b: %v", err)
	}
	if !v1a.VolumeCreated || v1b.VolumeCreated || v1a.Volume.ID != v1b.Volume.ID || v1a.Asset.ID == v1b.Asset.ID {
		t.Fatalf("expected v1b to attach as second asset on %s, got v1a=(vol:%s created:%v asset:%s) v1b=(vol:%s created:%v asset:%s)",
			v1a.Volume.ID, v1a.Volume.ID, v1a.VolumeCreated, v1a.Asset.ID, v1b.Volume.ID, v1b.VolumeCreated, v1b.Asset.ID)
	}

	// Same series and number=1, but kind=CHAPTER (filename fallback) -> must NOT attach to VOLUME 1.
	ch1, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("One Piece/One Piece c001.cbz")),
			Size:       500,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "cccccccccccccccc",
			ComicInfo:  &comicinfo.Metadata{Series: "One Piece"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile ch1: %v", err)
	}
	if !ch1.VolumeCreated || ch1.Volume.ID == v1a.Volume.ID || ch1.Volume.Kind != domain.VolumeKindChapter {
		t.Fatalf("expected CHAPTER 1 to be its own volume, got %+v", ch1.Volume)
	}

	// 2. Standalone files with identical number are NOT grouped without independent evidence.
	sa1, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Standalone A.cbz")),
			Size:       300,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "dddddddddddddddd",
			ComicInfo:  &comicinfo.Metadata{Number: "1", Title: "Book A"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile sa1: %v", err)
	}
	sa2, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Standalone B.cbz")),
			Size:       300,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "eeeeeeeeeeeeeeee",
			ComicInfo:  &comicinfo.Metadata{Number: "1", Title: "Book B"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile sa2: %v", err)
	}
	if sa1.Volume.ID == sa2.Volume.ID {
		t.Fatal("standalone files must not group without independent identity evidence")
	}

	// 3. Unnumbered files in the same series are NOT grouped without independent evidence.
	un1, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Unnumbered/A.cbz")),
			Size:       300,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "ffffffffffffffff",
			ComicInfo:  &comicinfo.Metadata{Series: "Unnumbered Anthology"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile un1: %v", err)
	}
	un2, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Unnumbered/B.cbz")),
			Size:       300,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "1212121212121212",
			ComicInfo:  &comicinfo.Metadata{Series: "Unnumbered Anthology"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile un2: %v", err)
	}
	if un1.Volume.ID == un2.Volume.ID {
		t.Fatal("unnumbered files in same series must not group without independent identity evidence")
	}

	// 4. Ambiguous match (>1 existing volumes with same primary series, kind, number) does NOT group.
	num5 := 5.0
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		sm := tx.SeriesMemberships()
		for range 2 {
			v := domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume, Number: &num5}
			if err := cw.CreateVolume(ctx, &v); err != nil {
				return err
			}
			if err := sm.SetVolumeMemberships(ctx, v.ID, []domain.SeriesMembership{
				{VolumeID: v.ID, SeriesID: v1a.Series.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &num5, Source: domain.MembershipSourceManual},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed ambiguous volumes: %v", err)
	}
	ambigRes, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: roots[0].ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("One Piece v05 [Third].cbz")),
			Size:       1000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "3434343434343434",
			ComicInfo:  &comicinfo.Metadata{Series: "One Piece", Number: "5"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile ambiguous: %v", err)
	}
	if !ambigRes.VolumeCreated {
		t.Fatal("expected ambiguous match (>1 volumes) to create a new volume instead of guessing")
	}

	// 5. Independent identity evidence (TargetVolumeID) groups standalone/unnumbered/ambiguous inputs.
	saGrouped, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:      lib.ID,
		LibraryRootID:  roots[1].ID,
		TargetVolumeID: sa1.Volume.ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Standalone A [ALT].cbz")),
			Size:       310,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "5656565656565656",
			ComicInfo:  &comicinfo.Metadata{Number: "1", Title: "Book A Alt"},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile TargetVolumeID: %v", err)
	}
	if saGrouped.VolumeCreated || saGrouped.Volume.ID != sa1.Volume.ID {
		t.Fatalf("expected TargetVolumeID grouping onto %s, got %+v", sa1.Volume.ID, saGrouped)
	}

	var assetCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM media_assets WHERE volume_id = $1`, v1a.Volume.ID).Scan(&assetCount); err != nil {
		t.Fatalf("count v1a assets: %v", err)
	}
	if assetCount != 2 {
		t.Fatalf("v1a assetCount = %d, want 2", assetCount)
	}
}

func TestIndexer_MetadataFirstVsFolderFirstChains(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		metaLib, folderLib   domain.Library
		metaRoot, folderRoot domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var r1, r2 []domain.LibraryRoot
		metaLib, r1 = seedLibraryAndRoots(ctx, t, tx, "MetaFirst",
			[]domain.ResolverName{domain.ResolverMetadata, domain.ResolverFolder, domain.ResolverFilename},
			[]byte("/srv/meta-first"),
		)
		folderLib, r2 = seedLibraryAndRoots(ctx, t, tx, "FolderFirst",
			[]domain.ResolverName{domain.ResolverFolder, domain.ResolverMetadata, domain.ResolverFilename},
			[]byte("/srv/folder-first"),
		)
		metaRoot = r1[0]
		folderRoot = r2[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	inputs := []scanner.InspectedFile{
		{
			RelPath:    mustRelIdentity(t, []byte("Shelf A/Issue 01.cbz")),
			Size:       100,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "aa00000000000001",
			ComicInfo:  &comicinfo.Metadata{Series: "Meta Series X", Number: "1"},
		},
		{
			RelPath:    mustRelIdentity(t, []byte("Shelf B/Issue 02.cbz")),
			Size:       100,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "aa00000000000002",
			ComicInfo:  &comicinfo.Metadata{Series: "Meta Series X", Number: "2"},
		},
		{
			RelPath:    mustRelIdentity(t, []byte("Standalone.cbz")),
			Size:       100,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "aa00000000000003",
		},
	}

	var metaSeriesIDs, folderSeriesIDs []string
	for _, f := range inputs {
		rMeta, err := idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     metaLib.ID,
			LibraryRootID: metaRoot.ID,
			Inspected:     f,
		})
		if err != nil {
			t.Fatalf("meta IndexFile(%s): %v", f.RelPath.Display, err)
		}
		if rMeta.Series != nil {
			metaSeriesIDs = append(metaSeriesIDs, rMeta.Series.ID)
		}

		rFolder, err := idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     folderLib.ID,
			LibraryRootID: folderRoot.ID,
			Inspected:     f,
		})
		if err != nil {
			t.Fatalf("folder IndexFile(%s): %v", f.RelPath.Display, err)
		}
		if rFolder.Series != nil {
			folderSeriesIDs = append(folderSeriesIDs, rFolder.Series.ID)
		}
	}

	// Metadata-first groups Shelf A + Shelf B into 1 series ("Meta Series X"), plus 1 standalone.
	if len(metaSeriesIDs) != 2 || metaSeriesIDs[0] != metaSeriesIDs[1] {
		t.Fatalf("expected metadata-first to group both files into one series, got %v", metaSeriesIDs)
	}
	// Folder-first separates Shelf A and Shelf B into 2 folder series, plus 1 standalone.
	if len(folderSeriesIDs) != 2 || folderSeriesIDs[0] == folderSeriesIDs[1] {
		t.Fatalf("expected folder-first to separate Shelf A and Shelf B into two series, got %v", folderSeriesIDs)
	}

	var metaTitles, folderTitles []string
	rows, err := db.Query(ctx, `SELECT title FROM series WHERE library_id = $1 ORDER BY title ASC`, metaLib.ID)
	if err != nil {
		t.Fatalf("query meta series: %v", err)
	}
	for rows.Next() {
		var title string
		if err := rows.Scan(&title); err != nil {
			t.Fatal(err)
		}
		metaTitles = append(metaTitles, title)
	}
	rows.Close()

	rows, err = db.Query(ctx, `SELECT title FROM series WHERE library_id = $1 ORDER BY title ASC`, folderLib.ID)
	if err != nil {
		t.Fatalf("query folder series: %v", err)
	}
	for rows.Next() {
		var title string
		if err := rows.Scan(&title); err != nil {
			t.Fatal(err)
		}
		folderTitles = append(folderTitles, title)
	}
	rows.Close()

	if !slices.Equal(metaTitles, []string{"Meta Series X"}) {
		t.Fatalf("metaTitles = %v, want [Meta Series X]", metaTitles)
	}
	if !slices.Equal(folderTitles, []string{"Shelf A", "Shelf B"}) {
		t.Fatalf("folderTitles = %v, want [Shelf A, Shelf B]", folderTitles)
	}
}

func TestIndexer_PreservesManualAndLockedMembershipsAndLockedFields(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib       domain.Library
		root      domain.LibraryRoot
		secSeries domain.Series
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Locked Lib", nil, []byte("/srv/locked"))
		root = roots[0]
		secSeries = domain.Series{
			LibraryID: lib.ID,
			Title:     "Curated Universe",
			TitleSort: "curated universe",
			Status:    domain.SeriesStatusOngoing,
		}
		return tx.CatalogueWrites().CreateSeries(ctx, &secSeries)
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	// Index first file to establish series, volume, membership, credits, and entities.
	first, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: root.ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Locked/Book v01.cbz")),
			Size:       1000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "7777777777777771",
			PageCount:  100,
			ComicInfo: &comicinfo.Metadata{
				Series:     "Locked Saga",
				Number:     "1",
				Title:      "Curated Vol Title",
				Summary:    "Curated Summary",
				Publisher:  "Curated Pub",
				Imprint:    "Old Imprint",
				AgeRating:  "Teen",
				GTIN:       "1111111111111",
				Writer:     []string{"Locked Writer"},
				Penciller:  []string{"Unlocked Penciller"},
				Characters: []string{"Locked Hero"},
				Locations:  []string{"Unlocked City"},
			},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile first: %v", err)
	}

	// Lock selected series/volume fields, convert primary membership to MANUAL with locked position,
	// add a MANUAL secondary membership, and lock one credit and one entity link.
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		s, err := cw.GetSeriesByID(ctx, first.Series.ID)
		if err != nil {
			return err
		}
		s.Title = "Admin Custom Series Title"
		s.LockedFields = domain.FieldLocks{"title": true, "publisher": true, "genres": true}
		s.Genres = []string{"Curated Genre"}
		if err := cw.UpdateSeries(ctx, s); err != nil {
			return err
		}

		v, err := cw.GetVolumeByID(ctx, first.Volume.ID)
		if err != nil {
			return err
		}
		v.LockedFields = domain.FieldLocks{"title": true, "summary": true, "external_ids": true}
		if err := cw.UpdateVolume(ctx, v); err != nil {
			return err
		}

		customPos := 99.0
		secPos := 4.0
		if err := tx.SeriesMemberships().SetVolumeMemberships(ctx, v.ID, []domain.SeriesMembership{
			{
				VolumeID:     v.ID,
				SeriesID:     s.ID,
				IsPrimary:    true,
				Kind:         domain.MembershipKindSeries,
				Position:     &customPos,
				Source:       domain.MembershipSourceManual,
				LockedFields: domain.FieldLocks{"position": true},
			},
			{
				VolumeID:  v.ID,
				SeriesID:  secSeries.ID,
				IsPrimary: false,
				Kind:      domain.MembershipKindUniverse,
				Position:  &secPos,
				Source:    domain.MembershipSourceManual,
			},
		}); err != nil {
			return err
		}

		credits, err := tx.Credits().ListCreditsByVolume(ctx, v.ID)
		if err != nil {
			return err
		}
		for _, c := range credits {
			if c.Role == domain.CreditRoleWriter {
				c.LockedFields = domain.FieldLocks{"credit": true}
				if err := tx.Credits().UpdateCredit(ctx, &c); err != nil {
					return err
				}
			}
		}

		links, err := tx.Entities().ListVolumeEntities(ctx, v.ID)
		if err != nil {
			return err
		}
		heroEnt, err := tx.Entities().GetEntityByImportKey(ctx, domain.EntityKindCharacter, domain.NormalizeImportName("Locked Hero"))
		if err != nil {
			return err
		}
		for i := range links {
			if links[i].EntityID == heroEnt.ID {
				links[i].LockedFields = domain.FieldLocks{"entity": true}
			}
		}
		return tx.Entities().SetVolumeEntities(ctx, v.ID, links)
	}); err != nil {
		t.Fatalf("apply locks failed: %v", err)
	}

	// Index a second asset matching (series, kind, number=1) with conflicting metadata.
	second, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: root.ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Locked/Book v01 [Rev].cbz")),
			Size:       1200,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "7777777777777772",
			PageCount:  104,
			ComicInfo: &comicinfo.Metadata{
				Series:     "Locked Saga",
				Number:     "1",
				Title:      "Overwritten Vol Title",
				Summary:    "Overwritten Summary",
				Publisher:  "Overwritten Pub",
				Imprint:    "New Imprint",
				AgeRating:  "Everyone",
				Genre:      []string{"Comedy"},
				Tags:       []string{"UpdatedTag"},
				Year:       int32Ptr(2025),
				Month:      int32Ptr(6),
				Day:        int32Ptr(20),
				GTIN:       "9999999999999",
				Writer:     []string{"New Writer"},
				Penciller:  []string{"New Penciller"},
				Characters: []string{"New Hero"},
				Locations:  []string{"New City"},
			},
		},
	})
	if err != nil {
		t.Fatalf("IndexFile second: %v", err)
	}
	if second.Series.ID != first.Series.ID || second.Volume.ID != first.Volume.ID {
		t.Fatalf("expected attachment to existing series/volume, got series=%s vol=%s", second.Series.ID, second.Volume.ID)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	gotSeries, err := cw.GetSeriesByID(ctx, first.Series.ID)
	if err != nil {
		t.Fatalf("GetSeriesByID: %v", err)
	}
	if gotSeries.Title != "Admin Custom Series Title" ||
		gotSeries.Publisher == nil || *gotSeries.Publisher != "Curated Pub" ||
		!slices.Equal(gotSeries.Genres, []string{"Curated Genre"}) {
		t.Fatalf("locked series fields were modified: %+v", gotSeries)
	}
	if gotSeries.Imprint == nil || *gotSeries.Imprint != "New Imprint" ||
		gotSeries.AgeRating == nil || *gotSeries.AgeRating != "Everyone" ||
		!slices.Equal(gotSeries.Tags, []string{"UpdatedTag"}) {
		t.Fatalf("unlocked series fields were not updated: %+v", gotSeries)
	}

	gotVol, err := cw.GetVolumeByID(ctx, first.Volume.ID)
	if err != nil {
		t.Fatalf("GetVolumeByID: %v", err)
	}
	wantDate := time.Date(2025, 6, 20, 0, 0, 0, 0, time.UTC)
	if gotVol.Title == nil || *gotVol.Title != "Curated Vol Title" ||
		gotVol.Summary == nil || *gotVol.Summary != "Curated Summary" ||
		gotVol.ExternalIDs["gtin"] != "1111111111111" {
		t.Fatalf("locked volume fields were modified: %+v", gotVol)
	}
	if gotVol.PageCount != 104 || gotVol.ReleaseDate == nil || !gotVol.ReleaseDate.Equal(wantDate) {
		t.Fatalf("unlocked volume fields were not updated: %+v", gotVol)
	}

	memberships, err := postgres.NewSeriesMembershipRepository(db.Pool).ListMembershipsByVolume(ctx, gotVol.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume: %v", err)
	}
	if len(memberships) != 2 {
		t.Fatalf("expected 2 preserved MANUAL memberships, got %+v", memberships)
	}
	if memberships[0].SeriesID != first.Series.ID || !memberships[0].IsPrimary ||
		memberships[0].Source != domain.MembershipSourceManual ||
		memberships[0].Position == nil || *memberships[0].Position != 99.0 ||
		!maps.Equal(memberships[0].LockedFields, domain.FieldLocks{"position": true}) {
		t.Fatalf("primary MANUAL/locked membership altered: %+v", memberships[0])
	}
	if memberships[1].SeriesID != secSeries.ID || memberships[1].IsPrimary ||
		memberships[1].Source != domain.MembershipSourceManual {
		t.Fatalf("secondary MANUAL membership altered: %+v", memberships[1])
	}

	credits, err := postgres.NewCreditRepository(db.Pool).ListCreditsByVolume(ctx, gotVol.ID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume: %v", err)
	}
	peopleRepo := postgres.NewPersonRepository(db.Pool)
	var creditNames []string
	for _, c := range credits {
		p, pErr := peopleRepo.GetPersonByID(ctx, c.PersonID)
		if pErr != nil {
			t.Fatalf("GetPersonByID: %v", pErr)
		}
		creditNames = append(creditNames, fmt.Sprintf("%s:%s", c.Role, p.Name))
	}
	slices.Sort(creditNames)
	wantCredits := []string{"PENCILLER:New Penciller", "WRITER:Locked Writer", "WRITER:New Writer"}
	if !slices.Equal(creditNames, wantCredits) {
		t.Fatalf("credits = %v, want %v", creditNames, wantCredits)
	}

	links, err := postgres.NewEntityRepository(db.Pool).ListVolumeEntities(ctx, gotVol.ID)
	if err != nil {
		t.Fatalf("ListVolumeEntities: %v", err)
	}
	entRepo := postgres.NewEntityRepository(db.Pool)
	var entityNames []string
	for _, l := range links {
		e, eErr := entRepo.GetEntityByID(ctx, l.EntityID)
		if eErr != nil {
			t.Fatalf("GetEntityByID: %v", eErr)
		}
		entityNames = append(entityNames, fmt.Sprintf("%s:%s", e.Kind, e.Name))
	}
	slices.Sort(entityNames)
	wantEntities := []string{"CHARACTER:Locked Hero", "CHARACTER:New Hero", "LOCATION:New City"}
	if !slices.Equal(entityNames, wantEntities) {
		t.Fatalf("entities = %v, want %v", entityNames, wantEntities)
	}

	// Verify wildcard "*" lock protects every field on series and volume.
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		s, err := cw.GetSeriesByID(ctx, first.Series.ID)
		if err != nil {
			return err
		}
		s.LockedFields = domain.FieldLocks{"*": true}
		if err := cw.UpdateSeries(ctx, s); err != nil {
			return err
		}
		v, err := cw.GetVolumeByID(ctx, first.Volume.ID)
		if err != nil {
			return err
		}
		v.LockedFields = domain.FieldLocks{"*": true}
		return cw.UpdateVolume(ctx, v)
	}); err != nil {
		t.Fatalf("apply wildcard lock: %v", err)
	}

	if _, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: root.ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Locked/Book v01 [Rev2].cbz")),
			Size:       1300,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "7777777777777773",
			PageCount:  250,
			ComicInfo: &comicinfo.Metadata{
				Series:    "Locked Saga",
				Number:    "1",
				Imprint:   "Wildcard Should Not Overwrite",
				AgeRating: "Adults Only 18+",
				Tags:      []string{"BlockedTag"},
				Year:      int32Ptr(2026),
				Month:     int32Ptr(12),
				Day:       int32Ptr(31),
			},
		},
	}); err != nil {
		t.Fatalf("IndexFile third (wildcard lock): %v", err)
	}

	gotSeriesWild, err := cw.GetSeriesByID(ctx, first.Series.ID)
	if err != nil {
		t.Fatalf("GetSeriesByID wildcard: %v", err)
	}
	if gotSeriesWild.Imprint == nil || *gotSeriesWild.Imprint != "New Imprint" ||
		gotSeriesWild.AgeRating == nil || *gotSeriesWild.AgeRating != "Everyone" ||
		!slices.Equal(gotSeriesWild.Tags, []string{"UpdatedTag"}) {
		t.Fatalf("wildcard lock on series failed to preserve fields: %+v", gotSeriesWild)
	}
	gotVolWild, err := cw.GetVolumeByID(ctx, first.Volume.ID)
	if err != nil {
		t.Fatalf("GetVolumeByID wildcard: %v", err)
	}
	if gotVolWild.PageCount != 104 || gotVolWild.ReleaseDate == nil || !gotVolWild.ReleaseDate.Equal(wantDate) {
		t.Fatalf("wildcard lock on volume failed to preserve fields: %+v", gotVolWild)
	}
}

func TestIndexer_InvalidUTF8PathStoresDiagnosticAndEmitsAdminEvent(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "RawBytes Lib",
			[]domain.ResolverName{domain.ResolverFolder, domain.ResolverFilename},
			[]byte("/srv/raw"),
		)
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	fixedNow := time.Date(2026, 11, 21, 13, 0, 0, 0, time.UTC)
	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{
		Now: func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	rawPath := []byte{'S', 'e', 'r', 'i', 'e', 's', 0xff, '/', 'B', 'o', 'o', 'k', 0xfe, ' ', 'v', '0', '1', '.', 'c', 'b', 'z'}
	relPath := mustRelIdentity(t, rawPath)
	if !relPath.InvalidUTF8 {
		t.Fatalf("expected InvalidUTF8=true for %x", rawPath)
	}

	res, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: root.ID,
		Inspected: scanner.InspectedFile{
			RelPath:    relPath,
			Size:       2048,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "8888888888888888",
			PageCount:  24,
		},
	})
	if err != nil {
		t.Fatalf("IndexFile invalid UTF-8: %v", err)
	}
	if !res.Asset.Diagnostics.InvalidUTF8Path || res.InvalidUTF8PathEvent == nil {
		t.Fatalf("expected invalid_utf8_path diagnostic and event, got diag=%+v ev=%+v",
			res.Asset.Diagnostics, res.InvalidUTF8PathEvent)
	}

	gotAsset, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, res.Asset.ID)
	if err != nil {
		t.Fatalf("GetAssetByID: %v", err)
	}
	if !slices.Equal(gotAsset.RelPathBytes, rawPath) {
		t.Fatalf("RelPathBytes = %x, want %x", gotAsset.RelPathBytes, rawPath)
	}
	if gotAsset.RelPathDisplay != relPath.Display || !gotAsset.Diagnostics.InvalidUTF8Path {
		t.Fatalf("unexpected persisted invalid UTF-8 asset: %+v", gotAsset)
	}

	published, err := postgres.NewEventRepository(db.Pool).PublishUnsequencedEvents(ctx, fixedNow)
	if err != nil {
		t.Fatalf("PublishUnsequencedEvents: %v", err)
	}
	if len(published) != 2 {
		t.Fatalf("len(published) = %d, want 2 (asset.added + asset.invalid_utf8_path)", len(published))
	}
	if published[0].Kind != domain.EventAssetAdded || published[1].Kind != domain.EventAssetInvalidUTF8Path {
		t.Fatalf("event kinds = [%s, %s], want [asset.added, asset.invalid_utf8_path]", published[0].Kind, published[1].Kind)
	}

	diagEv := published[1]
	if diagEv.Version != domain.EventVersion1 || diagEv.AggregateType != domain.AggregateTypeAsset || diagEv.AggregateID != gotAsset.ID {
		t.Fatalf("unexpected invalid_utf8_path event envelope: %+v", diagEv)
	}
	diagPayload, err := domain.DecodeEventPayload[domain.AssetEventPayloadV1](diagEv)
	if err != nil {
		t.Fatalf("DecodeEventPayload: %v", err)
	}
	if diagPayload.AssetID != gotAsset.ID || diagPayload.VolumeID != gotAsset.VolumeID ||
		diagPayload.LibraryID != lib.ID || diagPayload.LibraryRootID != root.ID ||
		diagPayload.PathDisplay != relPath.Display {
		t.Fatalf("unexpected invalid_utf8_path payload: %+v", diagPayload)
	}
}

func TestIndexer_FailureInAnyStepRollsBackRowsAndEvents(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Rollback Lib", nil, []byte("/srv/rb"))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	assertZeroCatalogueRows := func(t *testing.T) {
		t.Helper()
		for _, table := range []string{
			"series",
			"volumes",
			"series_memberships",
			"media_assets",
			"asset_digests",
			"people",
			"credits",
			"entities",
			"volume_entities",
			"domain_events",
		} {
			var count int
			if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
				t.Fatalf("count %s: %v", table, err)
			}
			if count != 0 {
				t.Fatalf("expected 0 rows in %s after rollback, got %d", table, count)
			}
		}
	}

	sampleInput := func(path string) scanner.IndexFileInput {
		return scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte(path)),
				Size:       1000,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "9999999999999999",
				PageCount:  30,
				ComicInfo: &comicinfo.Metadata{
					Series:     "Rollback Series",
					Number:     "1",
					Writer:     []string{"Rollback Writer"},
					Characters: []string{"Rollback Hero"},
				},
			},
		}
	}

	injectedErr := errors.New("injected step failure")
	for _, stage := range []string{
		"membership_sync",
		"asset_create",
		"credit_sync",
		"event_added",
		"event_invalid_utf8",
	} {
		t.Run("fail_"+stage, func(t *testing.T) {
			failingTx := &faultInjectingTransactor{
				inner:     transactor,
				failStage: stage,
				err:       injectedErr,
			}
			idx, err := scanner.NewIndexer(failingTx, scanner.IndexerOptions{})
			if err != nil {
				t.Fatalf("NewIndexer: %v", err)
			}
			in := sampleInput("Rollback/Book v01.cbz")
			if stage == "event_invalid_utf8" {
				in.Inspected.RelPath = mustRelIdentity(t, []byte{'R', 'a', 'w', 0xff, '/', 'v', '0', '1', '.', 'c', 'b', 'z'})
			}
			if _, err := idx.IndexFile(ctx, in); !errors.Is(err, injectedErr) {
				t.Fatalf("IndexFile(%s) err = %v, want %v", stage, err, injectedErr)
			}
			assertZeroCatalogueRows(t)
		})
	}

	// Real PostgreSQL duplicate-path failure at CreateAsset rolls back series, volume, and membership too.
	t.Run("duplicate_path_rolls_back_all_prior_steps", func(t *testing.T) {
		idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
		if err != nil {
			t.Fatalf("NewIndexer: %v", err)
		}
		if _, err := idx.IndexFile(ctx, sampleInput("Existing/First v01.cbz")); err != nil {
			t.Fatalf("seed first file: %v", err)
		}

		dupInput := scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte("Existing/First v01.cbz")),
				Size:       2000,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "abababababababab",
				ComicInfo: &comicinfo.Metadata{
					Series:     "Should Roll Back Series",
					Number:     "99",
					Writer:     []string{"Should Roll Back Writer"},
					Characters: []string{"Should Roll Back Hero"},
				},
			},
		}
		if _, err := idx.IndexFile(ctx, dupInput); !errors.Is(err, storage.ErrAlreadyExists) {
			t.Fatalf("duplicate path IndexFile err = %v, want storage.ErrAlreadyExists", err)
		}

		for _, check := range []struct {
			table string
			want  int
		}{
			{"series", 1},
			{"volumes", 1},
			{"series_memberships", 1},
			{"media_assets", 1},
			{"asset_digests", 1},
			{"people", 1},
			{"credits", 1},
			{"entities", 1},
			{"volume_entities", 1},
			{"domain_events", 1},
		} {
			var got int
			if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM "+check.table).Scan(&got); err != nil {
				t.Fatalf("count %s: %v", check.table, err)
			}
			if got != check.want {
				t.Fatalf("after duplicate-path rollback: %s count = %d, want %d", check.table, got, check.want)
			}
		}
	})
}

type faultInjectingTransactor struct {
	inner     storage.Transactor
	failStage string
	err       error
}

func (f *faultInjectingTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return f.inner.WithTx(ctx, func(tx storage.Tx) error {
		return fn(&faultInjectingTx{Tx: tx, failStage: f.failStage, err: f.err})
	})
}

func (f *faultInjectingTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return f.inner.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		return fn(&faultInjectingTx{Tx: tx, failStage: f.failStage, err: f.err})
	})
}

type faultInjectingTx struct {
	storage.Tx
	failStage string
	err       error
}

func (f *faultInjectingTx) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return f.Tx.WithTx(ctx, func(inner storage.Tx) error {
		return fn(&faultInjectingTx{Tx: inner, failStage: f.failStage, err: f.err})
	})
}

func (f *faultInjectingTx) CatalogueWrites() storage.CatalogueWriteRepository {
	return &faultCatalogueWrites{CatalogueWriteRepository: f.Tx.CatalogueWrites(), failStage: f.failStage, err: f.err}
}

func (f *faultInjectingTx) SeriesMemberships() storage.SeriesMembershipRepository {
	return &faultMemberships{SeriesMembershipRepository: f.Tx.SeriesMemberships(), failStage: f.failStage, err: f.err}
}

func (f *faultInjectingTx) Credits() storage.CreditRepository {
	return &faultCredits{CreditRepository: f.Tx.Credits(), failStage: f.failStage, err: f.err}
}

func (f *faultInjectingTx) Events() storage.EventRepository {
	return &faultEvents{EventRepository: f.Tx.Events(), failStage: f.failStage, err: f.err}
}

type faultCatalogueWrites struct {
	storage.CatalogueWriteRepository
	failStage string
	err       error
}

func (f *faultCatalogueWrites) CreateAsset(ctx context.Context, asset *domain.MediaAsset) error {
	if f.failStage == "asset_create" {
		return f.err
	}
	return f.CatalogueWriteRepository.CreateAsset(ctx, asset)
}

type faultMemberships struct {
	storage.SeriesMembershipRepository
	failStage string
	err       error
}

func (f *faultMemberships) SyncResolverMemberships(ctx context.Context, volumeID string, desired []domain.SeriesMembership) error {
	if f.failStage == "membership_sync" {
		return f.err
	}
	return f.SeriesMembershipRepository.SyncResolverMemberships(ctx, volumeID, desired)
}

type faultCredits struct {
	storage.CreditRepository
	failStage string
	err       error
}

func (f *faultCredits) SyncVolumeCredits(ctx context.Context, volumeID string, desired []domain.Credit) error {
	if f.failStage == "credit_sync" {
		return f.err
	}
	return f.CreditRepository.SyncVolumeCredits(ctx, volumeID, desired)
}

type faultEvents struct {
	storage.EventRepository
	failStage string
	err       error
	calls     int
}

func (f *faultEvents) AppendEvent(ctx context.Context, event *domain.DomainEvent) error {
	f.calls++
	if f.failStage == "event_added" && f.calls == 1 {
		return f.err
	}
	if f.failStage == "event_invalid_utf8" && f.calls == 2 {
		return f.err
	}
	return f.EventRepository.AppendEvent(ctx, event)
}

func TestIndexer_InputValidation(t *testing.T) {
	t.Parallel()
	if _, err := scanner.NewIndexer(nil, scanner.IndexerOptions{}); err == nil {
		t.Fatal("expected NewIndexer(nil) error")
	}
	idx, err := scanner.NewIndexer(&faultInjectingTransactor{}, scanner.IndexerOptions{})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	ctx := context.Background()
	validRel := mustRelIdentity(t, []byte("Series/v01.cbz"))

	cases := []struct {
		name string
		in   scanner.IndexFileInput
	}{
		{
			name: "missing library ID",
			in: scanner.IndexFileInput{
				LibraryRootID: "root-1",
				Inspected: scanner.InspectedFile{
					RelPath: validRel, Format: domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial, Digest: "deadbeef",
				},
			},
		},
		{
			name: "missing root ID",
			in: scanner.IndexFileInput{
				LibraryID: "lib-1",
				Inspected: scanner.InspectedFile{
					RelPath: validRel, Format: domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial, Digest: "deadbeef",
				},
			},
		},
		{
			name: "absolute path rejected",
			in: scanner.IndexFileInput{
				LibraryID: "lib-1", LibraryRootID: "root-1",
				Inspected: scanner.InspectedFile{
					RelPath: domain.PathIdentity{Bytes: []byte("/abs.cbz")}, Format: domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial, Digest: "deadbeef",
				},
			},
		},
		{
			name: "invalid format rejected",
			in: scanner.IndexFileInput{
				LibraryID: "lib-1", LibraryRootID: "root-1",
				Inspected: scanner.InspectedFile{
					RelPath: validRel, Format: domain.MediaFormat("UNKNOWN"),
					DigestKind: domain.DigestKindXXH364Partial, Digest: "deadbeef",
				},
			},
		},
		{
			name: "empty digest rejected",
			in: scanner.IndexFileInput{
				LibraryID: "lib-1", LibraryRootID: "root-1",
				Inspected: scanner.InspectedFile{
					RelPath: validRel, Format: domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial, Digest: "",
				},
			},
		},
		{
			name: "negative size rejected",
			in: scanner.IndexFileInput{
				LibraryID: "lib-1", LibraryRootID: "root-1",
				Inspected: scanner.InspectedFile{
					RelPath: validRel, Format: domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial, Digest: "deadbeef", Size: -1,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := idx.IndexFile(ctx, tc.in); err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			}
		})
	}
}

func waitForBlockedVolumeRowLock(ctx context.Context, t *testing.T, db *testdb.TestDB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var blocked int
		err := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM pg_locks WHERE relation = 'volumes'::regclass AND locktype = 'tuple' AND NOT granted`,
		).Scan(&blocked)
		if err == nil && blocked > 0 {
			return
		}
		// Fallback check for transactionid lock when waiting on a row lock without tuple lock.
		var waiting int
		err2 := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`,
		).Scan(&waiting)
		if err2 == nil && waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for concurrent indexer to block on volume lock (err1=%v err2=%v)", err, err2)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestIndexer_ConcurrentVolumeMoveAwayDoesNotAttachToStaleMatch(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib  domain.Library
		root domain.LibraryRoot
		s1   domain.Series
		s2   domain.Series
		v1   domain.Volume
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Race Move Lib", nil, []byte("/srv/race-move"))
		root = roots[0]
		cw := tx.CatalogueWrites()

		k1 := "title:run=none:source series"
		s1 = domain.Series{LibraryID: lib.ID, ResolverKey: &k1, Title: "Source Series", TitleSort: "source series", Status: domain.SeriesStatusOngoing}
		if err := cw.CreateSeries(ctx, &s1); err != nil {
			return err
		}
		k2 := "title:run=none:destination series"
		s2 = domain.Series{LibraryID: lib.ID, ResolverKey: &k2, Title: "Destination Series", TitleSort: "destination series", Status: domain.SeriesStatusOngoing}
		if err := cw.CreateSeries(ctx, &s2); err != nil {
			return err
		}

		num := 1.0
		v1 = domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume, Number: &num}
		if err := cw.CreateVolume(ctx, &v1); err != nil {
			return err
		}
		return tx.SeriesMemberships().SetVolumeMemberships(ctx, v1.ID, []domain.SeriesMembership{
			{VolumeID: v1.ID, SeriesID: s1.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &num, Source: domain.MembershipSourceResolver},
		})
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	curatorLocked := make(chan struct{})
	releaseCurator := make(chan struct{})
	curatorErrCh := make(chan error, 1)

	go func() {
		curatorErrCh <- transactor.WithTx(ctx, func(tx storage.Tx) error {
			if _, err := tx.CatalogueWrites().GetVolumeByIDForUpdate(ctx, v1.ID); err != nil {
				return err
			}
			close(curatorLocked)
			<-releaseCurator
			pos := 1.0
			return tx.SeriesMemberships().SetVolumeMemberships(ctx, v1.ID, []domain.SeriesMembership{
				{VolumeID: v1.ID, SeriesID: s2.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &pos, Source: domain.MembershipSourceManual},
			})
		})
	}()

	<-curatorLocked

	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	type indexOutcome struct {
		res *scanner.IndexFileResult
		err error
	}
	idxDone := make(chan indexOutcome, 1)
	go func() {
		res, err := idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte("Source Series v01.cbz")),
				Size:       1024,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "1010101010101010",
				PageCount:  50,
				ComicInfo:  &comicinfo.Metadata{Series: "Source Series", Number: "1"},
			},
		})
		idxDone <- indexOutcome{res: res, err: err}
	}()

	waitForBlockedVolumeRowLock(ctx, t, db)
	close(releaseCurator)

	if err := <-curatorErrCh; err != nil {
		t.Fatalf("curator transaction failed: %v", err)
	}
	out := <-idxDone
	if out.err != nil {
		t.Fatalf("IndexFile failed: %v", out.err)
	}
	if !out.res.VolumeCreated || out.res.Volume.ID == v1.ID {
		t.Fatalf("indexer attached to stale volume %s after curator moved it to %s: got %+v", v1.ID, s2.ID, out.res)
	}
	if out.res.Asset.VolumeID != out.res.Volume.ID {
		t.Fatalf("asset volume %s != result volume %s", out.res.Asset.VolumeID, out.res.Volume.ID)
	}

	// Verify v1 remains untouched in s2 with zero assets.
	var v1Assets int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM media_assets WHERE volume_id = $1`, v1.ID).Scan(&v1Assets); err != nil {
		t.Fatalf("count v1 assets: %v", err)
	}
	if v1Assets != 0 {
		t.Fatalf("expected 0 assets on moved volume %s, got %d", v1.ID, v1Assets)
	}
	v1Memberships, err := postgres.NewSeriesMembershipRepository(db.Pool).ListMembershipsByVolume(ctx, v1.ID)
	if err != nil {
		t.Fatalf("ListMembershipsByVolume(v1): %v", err)
	}
	if len(v1Memberships) != 1 || v1Memberships[0].SeriesID != s2.ID || !v1Memberships[0].IsPrimary {
		t.Fatalf("unexpected v1 memberships after race: %+v", v1Memberships)
	}
}

func TestIndexer_ConcurrentTargetAndAutoMatchObserveCuratorFieldLocks(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib    domain.Library
		root   domain.LibraryRoot
		s1     domain.Series
		autoV  domain.Volume
		standV domain.Volume
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Race Lock Lib", nil, []byte("/srv/race-lock"))
		root = roots[0]
		cw := tx.CatalogueWrites()

		k1 := "title:run=none:concurrent lock series"
		s1 = domain.Series{LibraryID: lib.ID, ResolverKey: &k1, Title: "Concurrent Lock Series", TitleSort: "concurrent lock series", Status: domain.SeriesStatusOngoing}
		if err := cw.CreateSeries(ctx, &s1); err != nil {
			return err
		}

		num := 2.0
		autoV = domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume, Number: &num, Title: strPtrPtr("Initial Auto")}
		if err := cw.CreateVolume(ctx, &autoV); err != nil {
			return err
		}
		if err := tx.SeriesMemberships().SetVolumeMemberships(ctx, autoV.ID, []domain.SeriesMembership{
			{VolumeID: autoV.ID, SeriesID: s1.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &num, Source: domain.MembershipSourceResolver},
		}); err != nil {
			return err
		}

		standV = domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume, Title: strPtrPtr("Initial Explicit")}
		return cw.CreateVolume(ctx, &standV)
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	runConcurrentLockCase := func(t *testing.T, volID, curatorTitle, relPath, digest, targetVolID string, meta *comicinfo.Metadata) {
		t.Helper()
		curatorLocked := make(chan struct{})
		releaseCurator := make(chan struct{})
		curatorErrCh := make(chan error, 1)

		go func() {
			curatorErrCh <- transactor.WithTx(ctx, func(tx storage.Tx) error {
				cw := tx.CatalogueWrites()
				v, err := cw.GetVolumeByIDForUpdate(ctx, volID)
				if err != nil {
					return err
				}
				close(curatorLocked)
				<-releaseCurator
				v.Title = &curatorTitle
				v.LockedFields = domain.FieldLocks{"title": true}
				return cw.UpdateVolume(ctx, v)
			})
		}()

		<-curatorLocked

		idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
		if err != nil {
			t.Fatalf("NewIndexer: %v", err)
		}
		type outcome struct {
			res *scanner.IndexFileResult
			err error
		}
		idxDone := make(chan outcome, 1)
		go func() {
			res, err := idx.IndexFile(ctx, scanner.IndexFileInput{
				LibraryID:      lib.ID,
				LibraryRootID:  root.ID,
				TargetVolumeID: targetVolID,
				Inspected: scanner.InspectedFile{
					RelPath:    mustRelIdentity(t, []byte(relPath)),
					Size:       1000,
					Format:     domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial,
					Digest:     digest,
					PageCount:  42,
					ComicInfo:  meta,
				},
			})
			idxDone <- outcome{res: res, err: err}
		}()

		waitForBlockedVolumeRowLock(ctx, t, db)
		close(releaseCurator)

		if err := <-curatorErrCh; err != nil {
			t.Fatalf("curator transaction failed: %v", err)
		}
		out := <-idxDone
		if out.err != nil {
			t.Fatalf("IndexFile failed: %v", out.err)
		}
		if out.res.VolumeCreated || out.res.Volume.ID != volID {
			t.Fatalf("expected attachment to %s, got created=%v id=%s", volID, out.res.VolumeCreated, out.res.Volume.ID)
		}

		gotVol, err := postgres.NewCatalogueWriteRepository(db.Pool).GetVolumeByID(ctx, volID)
		if err != nil {
			t.Fatalf("GetVolumeByID: %v", err)
		}
		if gotVol.Title == nil || *gotVol.Title != curatorTitle || !gotVol.LockedFields.IsLocked("title") {
			t.Fatalf("curator locked title lost: got title=%v locks=%+v, want %q", gotVol.Title, gotVol.LockedFields, curatorTitle)
		}
		if gotVol.Summary == nil || *gotVol.Summary != "Updated Unlocked Summary" {
			t.Fatalf("expected unlocked summary update, got %v", gotVol.Summary)
		}
	}

	t.Run("explicit_target", func(t *testing.T) {
		runConcurrentLockCase(
			t,
			standV.ID,
			"Curator Locked Explicit Title",
			"Standalone Explicit.cbz",
			"2020202020202020",
			standV.ID,
			&comicinfo.Metadata{Title: "Indexer Explicit Overwrite", Summary: "Updated Unlocked Summary"},
		)
	})

	t.Run("automatic_match", func(t *testing.T) {
		runConcurrentLockCase(
			t,
			autoV.ID,
			"Curator Locked Auto Title",
			"Concurrent Lock Series v02.cbz",
			"3030303030303030",
			"",
			&comicinfo.Metadata{Series: "Concurrent Lock Series", Number: "2", Title: "Indexer Auto Overwrite", Summary: "Updated Unlocked Summary"},
		)
	})
}

func strPtrPtr(s string) *string { return &s }

func makeIncompressibleTitle(seed uint64, length int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var b strings.Builder
	b.Grow(length)
	state := seed
	for b.Len() < length {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d", seed, state)))
		for _, by := range sum {
			if b.Len() >= length {
				break
			}
			b.WriteByte(alphabet[int(by)%len(alphabet)])
		}
		state++
	}
	return b.String()
}

func TestIndexer_LongPoorlyCompressibleSeriesKeyAndHashCollisionIsolation(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Long Key Lib", nil, []byte("/srv/long"))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	t.Run("long_poorly_compressible_title_at_short_path_and_reuse", func(t *testing.T) {
		longTitleA := makeIncompressibleTitle(0xdeadbeef, 5000)
		longTitleB := longTitleA[:len(longTitleA)-1] + "!"

		r1, err := idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte("a.cbz")),
				Size:       100,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "5050505050505051",
				ComicInfo:  &comicinfo.Metadata{Series: longTitleA, Number: "1"},
			},
		})
		if err != nil {
			t.Fatalf("IndexFile longTitleA #1: %v", err)
		}
		if r1.Series == nil || !r1.SeriesCreated {
			t.Fatalf("expected created series for longTitleA, got %+v", r1.Series)
		}

		// Same long key at another short path must reuse r1.Series.
		r2, err := idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte("b.cbz")),
				Size:       100,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "5050505050505052",
				ComicInfo:  &comicinfo.Metadata{Series: strings.ToUpper(longTitleA), Number: "2"},
			},
		})
		if err != nil {
			t.Fatalf("IndexFile longTitleA #2: %v", err)
		}
		if r2.SeriesCreated || r2.Series.ID != r1.Series.ID {
			t.Fatalf("expected reuse of series %s, got created=%v id=%s", r1.Series.ID, r2.SeriesCreated, r2.Series.ID)
		}

		// Slightly different 5000-byte title must create a distinct series.
		r3, err := idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte("c.cbz")),
				Size:       100,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "5050505050505053",
				ComicInfo:  &comicinfo.Metadata{Series: longTitleB, Number: "1"},
			},
		})
		if err != nil {
			t.Fatalf("IndexFile longTitleB: %v", err)
		}
		if !r3.SeriesCreated || r3.Series.ID == r1.Series.ID {
			t.Fatalf("expected distinct series for longTitleB, got created=%v id=%s (A=%s)", r3.SeriesCreated, r3.Series.ID, r1.Series.ID)
		}
	})

	t.Run("bounded_index_token_collision_never_attaches_wrong_exact_key", func(t *testing.T) {
		alphaRes, err := idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte("alpha.cbz")),
				Size:       100,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "6060606060606061",
				ComicInfo:  &comicinfo.Metadata{Series: "Collision Alpha", Number: "1"},
			},
		})
		if err != nil {
			t.Fatalf("IndexFile alpha: %v", err)
		}

		// Force Series Alpha's resolver_key_hash to equal Beta's SHA-256 token,
		// simulating an exact collision on the bounded unique digest index.
		betaKey := "title:run=none:collision beta"
		betaSum := sha256.Sum256([]byte(betaKey))
		if _, err := db.Exec(ctx,
			`UPDATE series SET resolver_key_hash = decode($2, 'hex') WHERE id = $1`,
			alphaRes.Series.ID, hex.EncodeToString(betaSum[:]),
		); err != nil {
			t.Fatalf("inject hash collision: %v", err)
		}

		// Both IndexFile and GetSeriesByResolverKey must surface an explicit
		// ErrResolverKeyHashCollision rather than silently attaching Beta to Alpha.
		_, err = idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte("beta-1.cbz")),
				Size:       100,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "6060606060606062",
				ComicInfo:  &comicinfo.Metadata{Series: "Collision Beta", Number: "1"},
			},
		})
		if !errors.Is(err, storage.ErrResolverKeyHashCollision) {
			t.Fatalf("IndexFile beta under hash collision = %v, want storage.ErrResolverKeyHashCollision", err)
		}

		_, err = postgres.NewCatalogueWriteRepository(db.Pool).GetSeriesByResolverKey(ctx, lib.ID, betaKey)
		if !errors.Is(err, storage.ErrResolverKeyHashCollision) {
			t.Fatalf("GetSeriesByResolverKey(beta) = %v, want storage.ErrResolverKeyHashCollision", err)
		}

		// Verify Beta's asset never attached to Alpha and Alpha still owns only its original volume.
		var alphaVolumes int
		if err := db.QueryRow(ctx,
			`SELECT count(*) FROM series_memberships WHERE series_id = $1`,
			alphaRes.Series.ID,
		).Scan(&alphaVolumes); err != nil {
			t.Fatalf("count alpha memberships: %v", err)
		}
		if alphaVolumes != 1 {
			t.Fatalf("alpha membership count = %d, want 1 (Beta must never attach to Alpha)", alphaVolumes)
		}
	})
}

func TestIndexer_ConcurrentNewNumberInExistingSeriesAttachesToOneVolume(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		lib  domain.Library
		root domain.LibraryRoot
	)
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Variant Race Lib", nil, []byte("/srv/variant-race"))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	idx, err := scanner.NewIndexer(transactor, scanner.IndexerOptions{})
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}

	// Seed the existing series with Volume 1 so the series already exists before
	// concurrent workers index a previously unseen number (#2).
	seedRes, err := idx.IndexFile(ctx, scanner.IndexFileInput{
		LibraryID:     lib.ID,
		LibraryRootID: root.ID,
		Inspected: scanner.InspectedFile{
			RelPath:    mustRelIdentity(t, []byte("Variant Race v01.cbz")),
			Size:       1000,
			Format:     domain.MediaFormatCBZ,
			DigestKind: domain.DigestKindXXH364Partial,
			Digest:     "7070707070707071",
			PageCount:  100,
			ComicInfo:  &comicinfo.Metadata{Series: "Variant Race", Number: "1"},
		},
	})
	if err != nil {
		t.Fatalf("seed volume 1: %v", err)
	}

	// Deterministic serialization witness: hold AcquireSeriesVariantLock(series, VOLUME, 99),
	// start a concurrent IndexFile for #99, verify it blocks on the advisory lock, create #99
	// before releasing, and confirm the waiting indexer attaches to the newly committed volume
	// rather than creating a second volume.
	holdEntered := make(chan struct{})
	releaseHold := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- transactor.WithTx(ctx, func(tx storage.Tx) error {
			cw := tx.CatalogueWrites()
			if err := cw.AcquireSeriesVariantLock(ctx, seedRes.Series.ID, domain.VolumeKindVolume, 99); err != nil {
				return err
			}
			close(holdEntered)
			<-releaseHold

			num := 99.0
			vol := domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume, Number: &num}
			if err := cw.CreateVolume(ctx, &vol); err != nil {
				return err
			}
			return tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
				{VolumeID: vol.ID, SeriesID: seedRes.Series.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &num, Source: domain.MembershipSourceResolver},
			})
		})
	}()
	<-holdEntered

	waiterResCh := make(chan *scanner.IndexFileResult, 1)
	waiterErrCh := make(chan error, 1)
	go func() {
		r, err := idx.IndexFile(ctx, scanner.IndexFileInput{
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Inspected: scanner.InspectedFile{
				RelPath:    mustRelIdentity(t, []byte("Variant Race v99-waiter.cbz")),
				Size:       1500,
				Format:     domain.MediaFormatCBZ,
				DigestKind: domain.DigestKindXXH364Partial,
				Digest:     "7070707070707099",
				PageCount:  120,
				ComicInfo:  &comicinfo.Metadata{Series: "Variant Race", Number: "99"},
			},
		})
		waiterResCh <- r
		waiterErrCh <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		if err := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`,
		).Scan(&waiting); err == nil && waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			close(releaseHold)
			t.Fatal("timed out waiting for IndexFile to block on AcquireSeriesVariantLock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(releaseHold)
	if err := <-holderDone; err != nil {
		t.Fatalf("holder tx: %v", err)
	}
	waiterRes := <-waiterResCh
	if err := <-waiterErrCh; err != nil {
		t.Fatalf("waiter IndexFile: %v", err)
	}
	if waiterRes.VolumeCreated {
		t.Fatalf("expected waiter to attach to holder's committed #99 volume, got VolumeCreated=true (%s)", waiterRes.Volume.ID)
	}

	// Concurrent race: two goroutines simultaneously index a previously unseen number (#2)
	// in the seeded series; both must serialize and attach to a single volume #2.
	const workers = 2
	start := make(chan struct{})
	type workerOutcome struct {
		res *scanner.IndexFileResult
		err error
	}
	outcomes := make([]workerOutcome, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(idxNum int) {
			defer wg.Done()
			<-start
			r, err := idx.IndexFile(ctx, scanner.IndexFileInput{
				LibraryID:     lib.ID,
				LibraryRootID: root.ID,
				Inspected: scanner.InspectedFile{
					RelPath:    mustRelIdentity(t, []byte(fmt.Sprintf("Variant Race v02-%d.cbz", idxNum))),
					Size:       int64(2000 + idxNum),
					Format:     domain.MediaFormatCBZ,
					DigestKind: domain.DigestKindXXH364Partial,
					Digest:     fmt.Sprintf("707070707070702%d", idxNum),
					PageCount:  150,
					ComicInfo:  &comicinfo.Metadata{Series: "Variant Race", Number: "2"},
				},
			})
			outcomes[idxNum] = workerOutcome{res: r, err: err}
		}(i)
	}
	close(start)
	wg.Wait()

	var vol2ID string
	createdVol2 := 0
	for i, o := range outcomes {
		if o.err != nil {
			t.Fatalf("concurrent worker %d failed: %v", i, o.err)
		}
		if vol2ID == "" {
			vol2ID = o.res.Volume.ID
		} else if o.res.Volume.ID != vol2ID {
			t.Fatalf("concurrent workers created separate #2 volumes: %s vs %s", vol2ID, o.res.Volume.ID)
		}
		if o.res.VolumeCreated {
			createdVol2++
		}
	}
	if createdVol2 != 1 {
		t.Fatalf("createdVol2 = %d, want 1", createdVol2)
	}

	var vol2Count int
	if err := db.QueryRow(ctx,
		`SELECT count(*) FROM volumes WHERE library_id = $1 AND kind = 'VOLUME' AND number = 2`,
		lib.ID,
	).Scan(&vol2Count); err != nil {
		t.Fatalf("count volume #2: %v", err)
	}
	if vol2Count != 1 {
		t.Fatalf("volume #2 row count = %d, want 1", vol2Count)
	}

	var assetCount int
	if err := db.QueryRow(ctx,
		`SELECT count(*) FROM media_assets WHERE volume_id = $1`,
		vol2ID,
	).Scan(&assetCount); err != nil {
		t.Fatalf("count volume #2 assets: %v", err)
	}
	if assetCount != workers {
		t.Fatalf("volume #2 asset count = %d, want %d", assetCount, workers)
	}
}
