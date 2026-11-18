package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupCatalogueWriteTestDB(t *testing.T) (*testdb.TestDB, *postgres.Transactor) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupCatalogueWriteTestDB: applying migrations: %v", err)
	}
	return db, postgres.NewTransactor(db.Pool)
}

func seedTestLibraryAndRoot(
	ctx context.Context,
	t *testing.T,
	tx storage.Tx,
	libName string,
	rootBytes []byte,
) (domain.Library, domain.LibraryRoot) {
	t.Helper()
	lib := domain.Library{
		Name: libName,
		Mode: domain.LibraryModeVirtual,
	}
	if err := tx.Libraries().CreateLibrary(ctx, &lib); err != nil {
		t.Fatalf("CreateLibrary(%q): %v", libName, err)
	}
	rootPath, err := domain.NewRootPathIdentity(rootBytes)
	if err != nil {
		t.Fatalf("NewRootPathIdentity(%q): %v", rootBytes, err)
	}
	root := domain.LibraryRoot{
		LibraryID: lib.ID,
		Path:      rootPath,
		State:     domain.LibraryRootStateOnline,
	}
	if err := tx.Libraries().CreateLibraryRoot(ctx, &root); err != nil {
		t.Fatalf("CreateLibraryRoot(%q): %v", rootBytes, err)
	}
	return lib, root
}

func mustRelPath(t *testing.T, raw []byte) domain.PathIdentity {
	t.Helper()
	p, err := domain.NewRelativePathIdentity(raw)
	if err != nil {
		t.Fatalf("NewRelativePathIdentity(%x): %v", raw, err)
	}
	return p
}

func TestCatalogueWriteRepository_CreateSeriesVolumeAssetAndOutboxViaTx(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	mtime := now.Add(-2 * time.Hour)
	relDate := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	totalVols := 12
	releaseYear := 2020
	volNum := 1.0
	lang := "ja"
	romaji := "Dorohedoro"
	publisher := "Shogakukan"
	volTitle := "Volume 1"
	policy := domain.CoverPolicyFirst

	var (
		lib    domain.Library
		root   domain.LibraryRoot
		series domain.Series
		vol    domain.Volume
		asset  domain.MediaAsset
		ev     domain.DomainEvent
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, root = seedTestLibraryAndRoot(ctx, t, tx, "Manga", []byte("/srv/manga"))
		cw := tx.CatalogueWrites()

		series = domain.Series{
			LibraryID:    lib.ID,
			Title:        "ドロヘドロ",
			TitleSort:    "Dorohedoro",
			Language:     &lang,
			RomajiTitle:  &romaji,
			Publisher:    &publisher,
			Status:       domain.SeriesStatusEnded,
			Genres:       []string{"Action", "Fantasy"},
			Tags:         []string{"Seinen"},
			CoverPolicy:  &policy,
			LockedFields: domain.FieldLocks{"title": true},
			ExternalIDs:  map[string]any{"anilist": float64(31133)},
			TotalVolumes: &totalVols,
			ReleaseYear:  &releaseYear,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := cw.CreateSeries(ctx, &series); err != nil {
			return err
		}

		vol = domain.Volume{
			LibraryID:    lib.ID,
			Kind:         domain.VolumeKindVolume,
			Number:       &volNum,
			Title:        &volTitle,
			ReleaseDate:  &relDate,
			PageCount:    210,
			ExternalIDs:  map[string]any{"isbn13": "9784091882813"},
			LockedFields: domain.FieldLocks{"number": true},
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := cw.CreateVolume(ctx, &vol); err != nil {
			return err
		}

		relRaw := []byte("Dorohedoro/v01.cbz")
		asset = domain.MediaAsset{
			VolumeID:        vol.ID,
			LibraryRootID:   root.ID,
			RelPathBytes:    relRaw,
			FileSize:        424242,
			FileMtime:       &mtime,
			Format:          domain.MediaFormatCBZ,
			PageCount:       210,
			CoverPageIndex:  1,
			SuppressedPages: []int{209},
			Diagnostics: domain.AssetDiagnostics{
				ImageCodecs: []string{"jxl"},
			},
			Digests: []domain.AssetDigest{
				{
					Kind:       domain.DigestKindXXH364Partial,
					Value:      "0123456789abcdef",
					IsCurrent:  true,
					Reason:     domain.DigestReasonScan,
					RecordedAt: now,
				},
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := cw.CreateAsset(ctx, &asset); err != nil {
			return err
		}

		payload, err := json.Marshal(domain.AssetEventPayloadV1{
			AssetID:       asset.ID,
			VolumeID:      vol.ID,
			LibraryID:     lib.ID,
			LibraryRootID: root.ID,
			Format:        asset.Format,
			PathDisplay:   asset.RelPathDisplay,
		})
		if err != nil {
			return err
		}
		ev, err = domain.NewDomainEvent(
			domain.EventAssetAdded,
			domain.EventVersion1,
			domain.AggregateTypeAsset,
			asset.ID,
			payload,
			now,
		)
		if err != nil {
			return err
		}
		return tx.Events().AppendEvent(ctx, &ev)
	})
	if err != nil {
		t.Fatalf("WithTx failed: %v", err)
	}

	readRepo := postgres.NewCatalogueWriteRepository(db.Pool)

	gotSeries, err := readRepo.GetSeriesByID(ctx, series.ID)
	if err != nil {
		t.Fatalf("GetSeriesByID: %v", err)
	}
	if gotSeries.Title != "ドロヘドロ" || gotSeries.TitleSort != "Dorohedoro" || gotSeries.Status != domain.SeriesStatusEnded {
		t.Errorf("unexpected series core fields: %+v", gotSeries)
	}
	if !gotSeries.LockedFields.IsLocked("title") || gotSeries.TotalVolumes == nil || *gotSeries.TotalVolumes != 12 {
		t.Errorf("unexpected series metadata: %+v", gotSeries)
	}

	gotVol, err := readRepo.GetVolumeByID(ctx, vol.ID)
	if err != nil {
		t.Fatalf("GetVolumeByID: %v", err)
	}
	if gotVol.Number == nil || *gotVol.Number != 1.0 || gotVol.PageCount != 210 {
		t.Errorf("unexpected volume fields: %+v", gotVol)
	}
	if gotVol.ReleaseDate == nil || !gotVol.ReleaseDate.Equal(relDate) {
		t.Errorf("ReleaseDate = %v, want %v", gotVol.ReleaseDate, relDate)
	}
	if !gotVol.IsStandalone() {
		t.Errorf("expected standalone volume, got memberships %+v", gotVol.Memberships)
	}

	gotAsset, err := readRepo.GetAssetByID(ctx, asset.ID)
	if err != nil {
		t.Fatalf("GetAssetByID: %v", err)
	}
	if !bytes.Equal(gotAsset.RelPathBytes, []byte("Dorohedoro/v01.cbz")) {
		t.Errorf("RelPathBytes = %q, want Dorohedoro/v01.cbz", gotAsset.RelPathBytes)
	}
	if gotAsset.FileMtime == nil || !gotAsset.FileMtime.Equal(mtime) {
		t.Errorf("FileMtime = %v, want %v", gotAsset.FileMtime, mtime)
	}
	currDigest, ok := gotAsset.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok || currDigest.Value != "0123456789abcdef" || currDigest.Reason != domain.DigestReasonScan {
		t.Errorf("unexpected current digest: (%+v, %v)", currDigest, ok)
	}

	gotEv, err := postgres.NewEventRepository(db.Pool).GetEventByID(ctx, ev.ID)
	if err != nil {
		t.Fatalf("GetEventByID: %v", err)
	}
	if gotEv.Kind != domain.EventAssetAdded || gotEv.AggregateID != asset.ID {
		t.Errorf("unexpected outbox event: %+v", gotEv)
	}

	// Verify UpdateSeries and UpdateVolume round-trip cleanly.
	updatedTitle := "Volume 1 (Revised)"
	gotVol.Title = &updatedTitle
	gotVol.PageCount = 212
	if err := readRepo.UpdateVolume(ctx, gotVol); err != nil {
		t.Fatalf("UpdateVolume: %v", err)
	}
	gotSeries.Status = domain.SeriesStatusHiatus
	if err := readRepo.UpdateSeries(ctx, gotSeries); err != nil {
		t.Fatalf("UpdateSeries: %v", err)
	}
	reloadedSeries, err := readRepo.GetSeriesByID(ctx, gotSeries.ID)
	if err != nil || reloadedSeries.Status != domain.SeriesStatusHiatus {
		t.Fatalf("reloaded series = (%+v, %v), want HIATUS", reloadedSeries, err)
	}
}

func TestCatalogueWriteRepository_ChapterRangesWholeOrNotAtAll(t *testing.T) {
	t.Parallel()
	_, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var libID string
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, _ := seedTestLibraryAndRoot(ctx, t, tx, "Manga", []byte("/srv/manga"))
		libID = lib.ID
		return nil
	}); err != nil {
		t.Fatalf("seed library: %v", err)
	}

	fPtr := func(v float64) *float64 { return &v }
	srcPtr := func(s domain.ChapterRangeSource) *domain.ChapterRangeSource { return &s }

	var volID string
	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		v := domain.Volume{
			LibraryID:          libID,
			Kind:               domain.VolumeKindVolume,
			ChapterStart:       fPtr(291),
			ChapterEnd:         fPtr(300),
			ChapterRangeSource: srcPtr(domain.ChapterRangeSourceArchive),
		}
		if err := cw.CreateVolume(ctx, &v); err != nil {
			return err
		}
		volID = v.ID

		// Reject partial and inverted ranges before persistence.
		invalid := []domain.Volume{
			{LibraryID: libID, Kind: domain.VolumeKindVolume, ChapterStart: fPtr(1)},
			{LibraryID: libID, Kind: domain.VolumeKindVolume, ChapterEnd: fPtr(5)},
			{LibraryID: libID, Kind: domain.VolumeKindVolume, ChapterRangeSource: srcPtr(domain.ChapterRangeSourceManual)},
			{LibraryID: libID, Kind: domain.VolumeKindVolume, ChapterStart: fPtr(1), ChapterEnd: fPtr(5)},
			{LibraryID: libID, Kind: domain.VolumeKindVolume, ChapterStart: fPtr(10), ChapterEnd: fPtr(5), ChapterRangeSource: srcPtr(domain.ChapterRangeSourceScraper)},
			{LibraryID: libID, Kind: domain.VolumeKindVolume, ChapterStart: fPtr(1), ChapterEnd: fPtr(2), ChapterRangeSource: srcPtr(domain.ChapterRangeSource("UNKNOWN"))},
			{LibraryID: libID, Kind: domain.VolumeKindVolume, ChapterStart: fPtr(math.NaN()), ChapterEnd: fPtr(2), ChapterRangeSource: srcPtr(domain.ChapterRangeSourceManual)},
		}
		for i := range invalid {
			cand := invalid[i]
			if err := cw.CreateVolume(ctx, &cand); err == nil {
				t.Fatalf("expected invalid chapter range case %d to fail CreateVolume", i)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx create volume: %v", err)
	}

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		v, err := cw.GetVolumeByID(ctx, volID)
		if err != nil {
			return err
		}
		cr, ok := v.ChapterRange()
		if !ok || cr.Start != 291 || cr.End != 300 || cr.Source != domain.ChapterRangeSourceArchive {
			t.Fatalf("loaded ChapterRange = (%+v, %v), want 291..300 ARCHIVE", cr, ok)
		}

		// Partial update must fail and keep existing complete range intact.
		partial := *v
		partial.ChapterEnd = nil
		if err := cw.UpdateVolume(ctx, &partial); err == nil {
			t.Fatal("expected partial chapter range update to fail")
		}

		// Clearing all three fields succeeds.
		v.ChapterStart = nil
		v.ChapterEnd = nil
		v.ChapterRangeSource = nil
		if err := cw.UpdateVolume(ctx, v); err != nil {
			return err
		}
		reloaded, err := cw.GetVolumeByID(ctx, volID)
		if err != nil {
			return err
		}
		if reloaded.HasKnownChapterRange() {
			t.Fatalf("expected cleared chapter range, got %+v", reloaded)
		}

		// Single-chapter equality range with MANUAL source persists cleanly.
		reloaded.ChapterStart = fPtr(10.5)
		reloaded.ChapterEnd = fPtr(10.5)
		reloaded.ChapterRangeSource = srcPtr(domain.ChapterRangeSourceManual)
		return cw.UpdateVolume(ctx, reloaded)
	})
	if err != nil {
		t.Fatalf("WithTx update chapter range: %v", err)
	}
}

func TestCatalogueWriteRepository_RecordDigestRotationAndOneCurrentPerKind(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	t2 := t1.Add(time.Minute)
	t3 := t2.Add(time.Minute)

	var assetID string
	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, root := seedTestLibraryAndRoot(ctx, t, tx, "Comics", []byte("/srv/comics"))
		cw := tx.CatalogueWrites()

		vol := domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume}
		if err := cw.CreateVolume(ctx, &vol); err != nil {
			return err
		}
		asset := domain.MediaAsset{
			VolumeID:      vol.ID,
			LibraryRootID: root.ID,
			RelPathBytes:  []byte("Issue 01.cbz"),
			FileSize:      1024,
			Format:        domain.MediaFormatCBZ,
			Digests: []domain.AssetDigest{
				{
					Kind:       domain.DigestKindXXH364Partial,
					Value:      "aaaa1111bbbb2222",
					IsCurrent:  true,
					Reason:     domain.DigestReasonScan,
					RecordedAt: t0,
				},
				{
					Kind:       domain.DigestKindXXH3128Full,
					Value:      "full0000111122223333444455556666",
					IsCurrent:  true,
					Reason:     domain.DigestReasonDedupe,
					RecordedAt: t0,
				},
			},
		}
		if err := cw.CreateAsset(ctx, &asset); err != nil {
			return err
		}
		assetID = asset.ID

		// Rotate partial digest -> WRITE_BACK, then EXTERNAL_MODIFY.
		if err := cw.RecordAssetDigest(ctx, &domain.AssetDigest{
			AssetID:    assetID,
			Kind:       domain.DigestKindXXH364Partial,
			Value:      "cccc3333dddd4444",
			Reason:     domain.DigestReasonWriteBack,
			RecordedAt: t1,
		}); err != nil {
			return err
		}
		if err := cw.RecordAssetDigest(ctx, &domain.AssetDigest{
			AssetID:    assetID,
			Kind:       domain.DigestKindXXH364Partial,
			Value:      "eeee5555ffff6666",
			Reason:     domain.DigestReasonExternalModify,
			RecordedAt: t2,
		}); err != nil {
			return err
		}

		// Unknown asset must return ErrNotFound.
		err := cw.RecordAssetDigest(ctx, &domain.AssetDigest{
			AssetID: domain.MustNewID(),
			Kind:    domain.DigestKindXXH364Partial,
			Value:   "0000000000000000",
			Reason:  domain.DigestReasonScan,
		})
		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("RecordAssetDigest on unknown asset = %v, want ErrNotFound", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx digest rotation: %v", err)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	got, err := cw.GetAssetByID(ctx, assetID)
	if err != nil {
		t.Fatalf("GetAssetByID: %v", err)
	}

	partialCurr, ok := got.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok || partialCurr.Value != "eeee5555ffff6666" || partialCurr.Reason != domain.DigestReasonExternalModify {
		t.Fatalf("unexpected current partial digest: (%+v, %v)", partialCurr, ok)
	}
	fullCurr, ok := got.CurrentDigest(domain.DigestKindXXH3128Full)
	if !ok || fullCurr.Value != "full0000111122223333444455556666" || fullCurr.Reason != domain.DigestReasonDedupe {
		t.Fatalf("unexpected current full digest: (%+v, %v)", fullCurr, ok)
	}

	var partialCount, partialCurrentCount, totalCurrentCount int
	if err := db.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE kind = 'XXH3_64_PARTIAL'),
			COUNT(*) FILTER (WHERE kind = 'XXH3_64_PARTIAL' AND is_current),
			COUNT(*) FILTER (WHERE is_current)
		FROM asset_digests
		WHERE asset_id = $1`, assetID,
	).Scan(&partialCount, &partialCurrentCount, &totalCurrentCount); err != nil {
		t.Fatalf("querying asset_digests counts: %v", err)
	}
	if partialCount != 3 || partialCurrentCount != 1 || totalCurrentCount != 2 {
		t.Fatalf("digest counts = (partial=%d, partialCurrent=%d, totalCurrent=%d), want (3, 1, 2)",
			partialCount, partialCurrentCount, totalCurrentCount)
	}

	// Verify demoted historical rows retained their original reasons.
	for _, tc := range []struct {
		value  string
		reason string
	}{
		{"aaaa1111bbbb2222", "SCAN"},
		{"cccc3333dddd4444", "WRITE_BACK"},
	} {
		var isCurrent bool
		var reason string
		if err := db.QueryRow(ctx, `
			SELECT is_current, reason
			FROM asset_digests
			WHERE asset_id = $1 AND kind = 'XXH3_64_PARTIAL' AND value = $2`,
			assetID, tc.value,
		).Scan(&isCurrent, &reason); err != nil {
			t.Fatalf("querying historical digest %s: %v", tc.value, err)
		}
		if isCurrent || reason != tc.reason {
			t.Errorf("digest %s = (is_current=%v, reason=%q), want (false, %q)", tc.value, isCurrent, reason, tc.reason)
		}
	}

	// Re-recording a previously seen digest promotes it back to current without PK collision.
	if err := cw.RecordAssetDigest(ctx, &domain.AssetDigest{
		AssetID:    assetID,
		Kind:       domain.DigestKindXXH364Partial,
		Value:      "aaaa1111bbbb2222",
		Reason:     domain.DigestReasonRepack,
		RecordedAt: t3,
	}); err != nil {
		t.Fatalf("re-recording historical digest: %v", err)
	}
	reloaded, err := cw.GetAssetByID(ctx, assetID)
	if err != nil {
		t.Fatalf("GetAssetByID after promote: %v", err)
	}
	currAfter, ok := reloaded.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok || currAfter.Value != "aaaa1111bbbb2222" || currAfter.Reason != domain.DigestReasonRepack {
		t.Fatalf("expected promoted digest with REPACK, got (%+v, %v)", currAfter, ok)
	}
}

func TestCatalogueWriteRepository_FindAssetsByDigestLibraryScoped(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		libAID  string
		libBID  string
		assetA1 string
		assetA2 string
		assetB1 string
		t0      = time.Date(2026, 11, 18, 8, 0, 0, 0, time.UTC)
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		libA, rootA := seedTestLibraryAndRoot(ctx, t, tx, "Library A", []byte("/srv/a"))
		libB, rootB := seedTestLibraryAndRoot(ctx, t, tx, "Library B", []byte("/srv/b"))
		libAID, libBID = libA.ID, libB.ID
		cw := tx.CatalogueWrites()

		vA1 := domain.Volume{LibraryID: libA.ID, Kind: domain.VolumeKindVolume}
		vA2 := domain.Volume{LibraryID: libA.ID, Kind: domain.VolumeKindVolume}
		vB1 := domain.Volume{LibraryID: libB.ID, Kind: domain.VolumeKindVolume}
		for _, v := range []*domain.Volume{&vA1, &vA2, &vB1} {
			if err := cw.CreateVolume(ctx, v); err != nil {
				return err
			}
		}

		a1 := domain.MediaAsset{
			VolumeID:      vA1.ID,
			LibraryRootID: rootA.ID,
			RelPathBytes:  []byte("book1.cbz"),
			FileSize:      100,
			Format:        domain.MediaFormatCBZ,
			Digests: []domain.AssetDigest{{
				Kind:       domain.DigestKindXXH364Partial,
				Value:      "shared-old-digest",
				IsCurrent:  true,
				Reason:     domain.DigestReasonScan,
				RecordedAt: t0,
			}},
			CreatedAt: t0,
		}
		if err := cw.CreateAsset(ctx, &a1); err != nil {
			return err
		}
		assetA1 = a1.ID

		// Rotate a1 so "shared-old-digest" becomes historical on a1.
		if err := cw.RecordAssetDigest(ctx, &domain.AssetDigest{
			AssetID:    a1.ID,
			Kind:       domain.DigestKindXXH364Partial,
			Value:      "liba-new-digest",
			Reason:     domain.DigestReasonWriteBack,
			RecordedAt: t0.Add(time.Minute),
		}); err != nil {
			return err
		}

		// Second asset in libA currently holding "shared-old-digest".
		a2 := domain.MediaAsset{
			VolumeID:      vA2.ID,
			LibraryRootID: rootA.ID,
			RelPathBytes:  []byte("book1-copy.cbz"),
			FileSize:      100,
			Format:        domain.MediaFormatCBZ,
			Digests: []domain.AssetDigest{{
				Kind:       domain.DigestKindXXH364Partial,
				Value:      "shared-old-digest",
				IsCurrent:  true,
				Reason:     domain.DigestReasonScan,
				RecordedAt: t0.Add(2 * time.Minute),
			}},
			CreatedAt: t0.Add(2 * time.Minute),
		}
		if err := cw.CreateAsset(ctx, &a2); err != nil {
			return err
		}
		assetA2 = a2.ID

		// Asset in libB also holding "shared-old-digest".
		b1 := domain.MediaAsset{
			VolumeID:      vB1.ID,
			LibraryRootID: rootB.ID,
			RelPathBytes:  []byte("other.cbz"),
			FileSize:      100,
			Format:        domain.MediaFormatCBZ,
			Digests: []domain.AssetDigest{{
				Kind:       domain.DigestKindXXH364Partial,
				Value:      "shared-old-digest",
				IsCurrent:  true,
				Reason:     domain.DigestReasonScan,
				RecordedAt: t0,
			}},
			CreatedAt: t0,
		}
		if err := cw.CreateAsset(ctx, &b1); err != nil {
			return err
		}
		assetB1 = b1.ID
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx setup: %v", err)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)

	// Historical + current match within libA must return [assetA1, assetA2] and never assetB1.
	inA, err := cw.FindAssetsByDigest(ctx, libAID, domain.DigestKindXXH364Partial, "shared-old-digest")
	if err != nil {
		t.Fatalf("FindAssetsByDigest(libA, shared-old-digest): %v", err)
	}
	if len(inA) != 2 || inA[0].ID != assetA1 || inA[1].ID != assetA2 {
		t.Fatalf("libA old digest matches = %+v, want [%s, %s]", inA, assetA1, assetA2)
	}

	// Current-only digest in libA returns [assetA1] and nothing in libB.
	inANew, err := cw.FindAssetsByDigest(ctx, libAID, domain.DigestKindXXH364Partial, "liba-new-digest")
	if err != nil {
		t.Fatalf("FindAssetsByDigest(libA, liba-new-digest): %v", err)
	}
	if len(inANew) != 1 || inANew[0].ID != assetA1 {
		t.Fatalf("libA new digest matches = %+v, want [%s]", inANew, assetA1)
	}

	inBNew, err := cw.FindAssetsByDigest(ctx, libBID, domain.DigestKindXXH364Partial, "liba-new-digest")
	if err != nil {
		t.Fatalf("FindAssetsByDigest(libB, liba-new-digest): %v", err)
	}
	if len(inBNew) != 0 {
		t.Fatalf("expected 0 matches in libB for libA digest, got %+v", inBNew)
	}

	inBOld, err := cw.FindAssetsByDigest(ctx, libBID, domain.DigestKindXXH364Partial, "shared-old-digest")
	if err != nil {
		t.Fatalf("FindAssetsByDigest(libB, shared-old-digest): %v", err)
	}
	if len(inBOld) != 1 || inBOld[0].ID != assetB1 {
		t.Fatalf("libB old digest matches = %+v, want [%s]", inBOld, assetB1)
	}
}

func TestCatalogueWriteRepository_UpdateAssetLocationKeepsIDAndReadingProgress(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		userID  string
		root1ID string
		root2ID string
		volID   string
		assetID string
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, root1 := seedTestLibraryAndRoot(ctx, t, tx, "Library", []byte("/srv/root1"))
		root1ID = root1.ID

		root2 := domain.LibraryRoot{
			LibraryID: lib.ID,
			Path:      mustRootPathBytes(t, []byte("/srv/root2")),
			State:     domain.LibraryRootStateOnline,
		}
		if err := tx.Libraries().CreateLibraryRoot(ctx, &root2); err != nil {
			return err
		}
		root2ID = root2.ID

		user := domain.User{ID: domain.MustNewID(), Username: "reader-move"}
		if err := tx.Users().CreateUser(ctx, &user); err != nil {
			return err
		}
		userID = user.ID

		cw := tx.CatalogueWrites()
		vol := domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume}
		if err := cw.CreateVolume(ctx, &vol); err != nil {
			return err
		}
		volID = vol.ID

		asset := domain.MediaAsset{
			VolumeID:      vol.ID,
			LibraryRootID: root1ID,
			RelPathBytes:  []byte("OldFolder/v01.cbz"),
			FileSize:      512,
			Format:        domain.MediaFormatCBZ,
			Diagnostics: domain.AssetDiagnostics{
				ImageCodecs: []string{"jxl"},
			},
			Digests: []domain.AssetDigest{{
				Kind:      domain.DigestKindXXH364Partial,
				Value:     "1122334455667788",
				IsCurrent: true,
				Reason:    domain.DigestReasonScan,
			}},
		}
		if err := cw.CreateAsset(ctx, &asset); err != nil {
			return err
		}
		assetID = asset.ID
		return nil
	})
	if err != nil {
		t.Fatalf("WithTx setup: %v", err)
	}

	locatorJSON, err := json.Marshal(domain.Locator{
		Type:        domain.LocatorTypePage,
		Page:        intPtrForWriteTest(14),
		AssetID:     assetID,
		PageCount:   intPtrForWriteTest(180),
		AssetDigest: &domain.LocatorDigest{Kind: domain.DigestKindXXH364Partial, Value: "1122334455667788"},
	})
	if err != nil {
		t.Fatalf("marshal locator: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
		VALUES ($1, $2, $3, 0.25, 'KOMGA', FALSE)`,
		userID, volID, locatorJSON,
	); err != nil {
		t.Fatalf("seed reading_progress: %v", err)
	}

	// Move across roots to a raw non-UTF-8 path inside storage.Tx.
	movedRaw := []byte{0xff, 0xfe, '/', 'v', '0', '1', '.', 'c', 'b', 'z'}
	movedPath := mustRelPath(t, movedRaw)
	moveTime := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.CatalogueWrites().UpdateAssetLocation(ctx, assetID, root2ID, movedPath, moveTime)
	}); err != nil {
		t.Fatalf("UpdateAssetLocation to root2: %v", err)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	moved, err := cw.GetAssetByID(ctx, assetID)
	if err != nil {
		t.Fatalf("GetAssetByID after move: %v", err)
	}
	if moved.ID != assetID || moved.VolumeID != volID || moved.LibraryRootID != root2ID {
		t.Fatalf("unexpected moved asset identity/root: %+v", moved)
	}
	if !bytes.Equal(moved.RelPathBytes, movedRaw) || moved.RelPathDisplay != `\xff\xfe/v01.cbz` {
		t.Fatalf("unexpected moved path fields: bytes=%x display=%q", moved.RelPathBytes, moved.RelPathDisplay)
	}
	if !moved.Diagnostics.InvalidUTF8Path || len(moved.Diagnostics.ImageCodecs) != 1 || moved.Diagnostics.ImageCodecs[0] != "jxl" {
		t.Fatalf("unexpected diagnostics after move to invalid UTF-8 path: %+v", moved.Diagnostics)
	}

	// Verify seeded reading_progress and locator.asset_id remain intact.
	var (
		gotLocatorBytes []byte
		gotProg         float32
	)
	if err := db.QueryRow(ctx, `
		SELECT locator, total_progression
		FROM reading_progress
		WHERE user_id = $1 AND volume_id = $2`,
		userID, volID,
	).Scan(&gotLocatorBytes, &gotProg); err != nil {
		t.Fatalf("querying reading_progress: %v", err)
	}
	var gotLocator domain.Locator
	if err := json.Unmarshal(gotLocatorBytes, &gotLocator); err != nil {
		t.Fatalf("unmarshal locator: %v", err)
	}
	if gotLocator.AssetID != assetID || gotLocator.Page == nil || *gotLocator.Page != 14 {
		t.Fatalf("reading_progress locator altered after move: %+v", gotLocator)
	}

	// Moving back to a valid UTF-8 path clears invalid_utf8_path while preserving image_codecs.
	validPath := mustRelPath(t, []byte("NewFolder/v01.cbz"))
	if err := cw.UpdateAssetLocation(ctx, assetID, root2ID, validPath, moveTime.Add(time.Minute)); err != nil {
		t.Fatalf("UpdateAssetLocation to valid UTF-8 path: %v", err)
	}
	clean, err := cw.GetAssetByID(ctx, assetID)
	if err != nil {
		t.Fatalf("GetAssetByID after valid move: %v", err)
	}
	if clean.Diagnostics.InvalidUTF8Path || len(clean.Diagnostics.ImageCodecs) != 1 || clean.Diagnostics.ImageCodecs[0] != "jxl" {
		t.Fatalf("expected invalid_utf8_path cleared and image_codecs preserved, got %+v", clean.Diagnostics)
	}
}

func TestCatalogueWriteRepository_DuplicateRootPathMapsToErrAlreadyExists(t *testing.T) {
	t.Parallel()
	_, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var rootID, volID, asset2ID string
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, root := seedTestLibraryAndRoot(ctx, t, tx, "Library", []byte("/srv/lib"))
		rootID = root.ID
		cw := tx.CatalogueWrites()

		vol := domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume}
		if err := cw.CreateVolume(ctx, &vol); err != nil {
			return err
		}
		volID = vol.ID

		a1 := domain.MediaAsset{
			VolumeID:      volID,
			LibraryRootID: rootID,
			RelPathBytes:  []byte("Series/v01.cbz"),
			FileSize:      100,
			Format:        domain.MediaFormatCBZ,
		}
		if err := cw.CreateAsset(ctx, &a1); err != nil {
			return err
		}
		a2 := domain.MediaAsset{
			VolumeID:      volID,
			LibraryRootID: rootID,
			RelPathBytes:  []byte("Series/v02.cbz"),
			FileSize:      100,
			Format:        domain.MediaFormatCBZ,
		}
		if err := cw.CreateAsset(ctx, &a2); err != nil {
			return err
		}
		asset2ID = a2.ID
		return nil
	}); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		dup := domain.MediaAsset{
			VolumeID:      volID,
			LibraryRootID: rootID,
			RelPathBytes:  []byte("Series/v01.cbz"),
			FileSize:      200,
			Format:        domain.MediaFormatCBZ,
		}
		return tx.CatalogueWrites().CreateAsset(ctx, &dup)
	})
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("duplicate CreateAsset err = %v, want storage.ErrAlreadyExists", err)
	}

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.CatalogueWrites().UpdateAssetLocation(ctx, asset2ID, rootID, mustRelPath(t, []byte("Series/v01.cbz")), time.Now().UTC())
	})
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("duplicate UpdateAssetLocation err = %v, want storage.ErrAlreadyExists", err)
	}
}

func TestCatalogueWriteRepository_CrossLibraryAssetRootRejectedAtCommit(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		libA, libB   domain.Library
		rootA, rootB domain.LibraryRoot
		volA         domain.Volume
		assetA       domain.MediaAsset
	)

	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		libA, rootA = seedTestLibraryAndRoot(ctx, t, tx, "Lib A", []byte("/srv/lib-a"))
		libB, rootB = seedTestLibraryAndRoot(ctx, t, tx, "Lib B", []byte("/srv/lib-b"))
		cw := tx.CatalogueWrites()

		volA = domain.Volume{LibraryID: libA.ID, Kind: domain.VolumeKindVolume}
		if err := cw.CreateVolume(ctx, &volA); err != nil {
			return err
		}
		assetA = domain.MediaAsset{
			VolumeID:      volA.ID,
			LibraryRootID: rootA.ID,
			RelPathBytes:  []byte("valid.cbz"),
			FileSize:      100,
			Format:        domain.MediaFormatCBZ,
		}
		return cw.CreateAsset(ctx, &assetA)
	}); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// 1. Insert linking libA volume with libB root must fail at commit and leave no asset or outbox row.
	badAssetID := domain.MustNewID()
	commitErr := transactor.WithTx(ctx, func(tx storage.Tx) error {
		badAsset := domain.MediaAsset{
			ID:            badAssetID,
			VolumeID:      volA.ID,
			LibraryRootID: rootB.ID,
			RelPathBytes:  []byte("cross.cbz"),
			FileSize:      100,
			Format:        domain.MediaFormatCBZ,
		}
		if err := tx.CatalogueWrites().CreateAsset(ctx, &badAsset); err != nil {
			return err
		}
		ev, err := domain.NewDomainEvent(
			domain.EventAssetAdded,
			domain.EventVersion1,
			domain.AggregateTypeAsset,
			badAssetID,
			domain.AssetEventPayloadV1{
				AssetID:       badAssetID,
				VolumeID:      volA.ID,
				LibraryID:     libB.ID,
				LibraryRootID: rootB.ID,
			},
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}
		return tx.Events().AppendEvent(ctx, &ev)
	})
	if commitErr == nil {
		t.Fatal("expected cross-library CreateAsset transaction to fail at commit")
	}

	var assetCount, eventCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM media_assets WHERE id = $1`, badAssetID).Scan(&assetCount); err != nil {
		t.Fatalf("count bad asset: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1`, badAssetID).Scan(&eventCount); err != nil {
		t.Fatalf("count bad events: %v", err)
	}
	if assetCount != 0 || eventCount != 0 {
		t.Fatalf("expected rollback on trigger failure, got assets=%d events=%d", assetCount, eventCount)
	}

	// 2. Updating an existing asset's root to a different library must also fail at commit.
	moveErr := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.CatalogueWrites().UpdateAssetLocation(ctx, assetA.ID, rootB.ID, mustRelPath(t, []byte("moved.cbz")), time.Now().UTC())
	})
	if moveErr == nil {
		t.Fatal("expected cross-library UpdateAssetLocation transaction to fail at commit")
	}
	reloaded, err := postgres.NewCatalogueWriteRepository(db.Pool).GetAssetByID(ctx, assetA.ID)
	if err != nil {
		t.Fatalf("GetAssetByID after rejected move: %v", err)
	}
	if reloaded.LibraryRootID != rootA.ID {
		t.Fatalf("expected asset root to remain %s after rollback, got %s", rootA.ID, reloaded.LibraryRootID)
	}
}

func TestCatalogueWriteRepository_ListAssetsByRoot(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t0 := time.Date(2026, 11, 18, 7, 0, 0, 0, time.UTC)
	missingAt := t0.Add(time.Hour)
	retiredAt := t0.Add(48 * time.Hour)
	mtimeAvail := t0.Add(-10 * time.Minute)
	mtimeMiss := t0.Add(-20 * time.Minute)

	var (
		root1ID    string
		availID    string
		missingID  string
		retiredID  string
		rawUTF8Bad = []byte{'S', 'e', 'r', 'i', 'e', 's', '/', 0xff, '.', 'c', 'b', 'z'}
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, root1 := seedTestLibraryAndRoot(ctx, t, tx, "Library", []byte("/srv/root1"))
		root1ID = root1.ID
		root2 := domain.LibraryRoot{
			LibraryID: lib.ID,
			Path:      mustRootPathBytes(t, []byte("/srv/root2")),
			State:     domain.LibraryRootStateOnline,
		}
		if err := tx.Libraries().CreateLibraryRoot(ctx, &root2); err != nil {
			return err
		}

		cw := tx.CatalogueWrites()
		vol := domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume}
		if err := cw.CreateVolume(ctx, &vol); err != nil {
			return err
		}

		// 1. Available asset with rotated digest (only current digest should appear in ListAssetsByRoot).
		avail := domain.MediaAsset{
			VolumeID:      vol.ID,
			LibraryRootID: root1ID,
			RelPathBytes:  rawUTF8Bad,
			FileSize:      1111,
			FileMtime:     &mtimeAvail,
			Format:        domain.MediaFormatCBZ,
			Digests: []domain.AssetDigest{
				{
					Kind:       domain.DigestKindXXH364Partial,
					Value:      "old-partial-hex",
					IsCurrent:  false,
					Reason:     domain.DigestReasonScan,
					RecordedAt: t0,
				},
				{
					Kind:       domain.DigestKindXXH364Partial,
					Value:      "curr-partial-hex",
					IsCurrent:  true,
					Reason:     domain.DigestReasonWriteBack,
					RecordedAt: t0.Add(time.Minute),
				},
				{
					Kind:       domain.DigestKindXXH3128Full,
					Value:      "curr-full-hex",
					IsCurrent:  true,
					Reason:     domain.DigestReasonDedupe,
					RecordedAt: t0.Add(time.Minute),
				},
			},
		}
		if err := cw.CreateAsset(ctx, &avail); err != nil {
			return err
		}
		availID = avail.ID

		// 2. Missing asset (within grace).
		missing := domain.MediaAsset{
			VolumeID:      vol.ID,
			LibraryRootID: root1ID,
			RelPathBytes:  []byte("Series/missing.cbz"),
			FileSize:      2222,
			FileMtime:     &mtimeMiss,
			Format:        domain.MediaFormatCBZ,
			MissingSince:  &missingAt,
			Digests: []domain.AssetDigest{{
				Kind:       domain.DigestKindXXH364Partial,
				Value:      "missing-hex",
				IsCurrent:  true,
				Reason:     domain.DigestReasonScan,
				RecordedAt: t0,
			}},
		}
		if err := cw.CreateAsset(ctx, &missing); err != nil {
			return err
		}
		missingID = missing.ID

		// 3. Retired asset (after grace).
		retired := domain.MediaAsset{
			VolumeID:      vol.ID,
			LibraryRootID: root1ID,
			RelPathBytes:  []byte("Series/retired.cbz"),
			FileSize:      3333,
			Format:        domain.MediaFormatCBZ,
			MissingSince:  &missingAt,
			RetiredAt:     &retiredAt,
			Digests: []domain.AssetDigest{{
				Kind:       domain.DigestKindXXH364Partial,
				Value:      "retired-hex",
				IsCurrent:  true,
				Reason:     domain.DigestReasonScan,
				RecordedAt: t0,
			}},
		}
		if err := cw.CreateAsset(ctx, &retired); err != nil {
			return err
		}
		retiredID = retired.ID

		// 4. Asset on root2 (must be excluded when listing root1).
		other := domain.MediaAsset{
			VolumeID:      vol.ID,
			LibraryRootID: root2.ID,
			RelPathBytes:  []byte("other.cbz"),
			FileSize:      4444,
			Format:        domain.MediaFormatCBZ,
		}
		return cw.CreateAsset(ctx, &other)
	})
	if err != nil {
		t.Fatalf("WithTx setup: %v", err)
	}

	list, err := postgres.NewCatalogueWriteRepository(db.Pool).ListAssetsByRoot(ctx, root1ID)
	if err != nil {
		t.Fatalf("ListAssetsByRoot: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("len(ListAssetsByRoot) = %d, want 3", len(list))
	}

	byID := make(map[string]domain.MediaAsset, len(list))
	for _, a := range list {
		byID[a.ID] = a
	}

	gotAvail := byID[availID]
	if !bytes.Equal(gotAvail.RelPathBytes, rawUTF8Bad) || gotAvail.FileSize != 1111 ||
		gotAvail.FileMtime == nil || !gotAvail.FileMtime.Equal(mtimeAvail) || !gotAvail.IsAvailable() {
		t.Fatalf("unexpected available asset in root list: %+v", gotAvail)
	}
	if len(gotAvail.Digests) != 2 {
		t.Fatalf("expected only 2 current digests for available asset, got %+v", gotAvail.Digests)
	}
	if d, ok := gotAvail.CurrentDigest(domain.DigestKindXXH364Partial); !ok || d.Value != "curr-partial-hex" {
		t.Fatalf("unexpected current partial digest in root list: (%+v, %v)", d, ok)
	}

	gotMiss := byID[missingID]
	if !gotMiss.IsMissing() || gotMiss.IsRetired() || gotMiss.MissingSince == nil || !gotMiss.MissingSince.Equal(missingAt) {
		t.Fatalf("unexpected missing asset state: %+v", gotMiss)
	}
	if gotMiss.FileSize != 2222 || gotMiss.FileMtime == nil || !gotMiss.FileMtime.Equal(mtimeMiss) {
		t.Fatalf("unexpected missing asset size/mtime: %+v", gotMiss)
	}

	gotRet := byID[retiredID]
	if !gotRet.IsMissing() || !gotRet.IsRetired() || gotRet.RetiredAt == nil || !gotRet.RetiredAt.Equal(retiredAt) {
		t.Fatalf("unexpected retired asset state: %+v", gotRet)
	}
}

func TestCatalogueWriteRepository_RollbackLeavesNeitherRowsNorEvents(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var libID, rootID string
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, root := seedTestLibraryAndRoot(ctx, t, tx, "Rollback Lib", []byte("/srv/rb"))
		libID, rootID = lib.ID, root.ID
		return nil
	}); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	sentinel := errors.New("abort catalogue write")
	seriesID := domain.MustNewID()
	volID := domain.MustNewID()
	assetID := domain.MustNewID()

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		cw := tx.CatalogueWrites()
		if err := cw.CreateSeries(ctx, &domain.Series{
			ID:        seriesID,
			LibraryID: libID,
			Title:     "Should Roll Back",
		}); err != nil {
			return err
		}
		if err := cw.CreateVolume(ctx, &domain.Volume{
			ID:        volID,
			LibraryID: libID,
			Kind:      domain.VolumeKindVolume,
		}); err != nil {
			return err
		}
		if err := cw.CreateAsset(ctx, &domain.MediaAsset{
			ID:            assetID,
			VolumeID:      volID,
			LibraryRootID: rootID,
			RelPathBytes:  []byte("rollback.cbz"),
			FileSize:      999,
			Format:        domain.MediaFormatCBZ,
			Digests: []domain.AssetDigest{{
				Kind:      domain.DigestKindXXH364Partial,
				Value:     "deadbeef00001111",
				IsCurrent: true,
				Reason:    domain.DigestReasonScan,
			}},
		}); err != nil {
			return err
		}

		ev, err := domain.NewDomainEvent(
			domain.EventAssetAdded,
			domain.EventVersion1,
			domain.AggregateTypeAsset,
			assetID,
			domain.AssetEventPayloadV1{
				AssetID:       assetID,
				VolumeID:      volID,
				LibraryID:     libID,
				LibraryRootID: rootID,
			},
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}
		if err := tx.Events().AppendEvent(ctx, &ev); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}

	for _, check := range []struct {
		query string
		arg   string
		label string
	}{
		{"SELECT COUNT(*) FROM series WHERE id = $1", seriesID, "series"},
		{"SELECT COUNT(*) FROM volumes WHERE id = $1", volID, "volumes"},
		{"SELECT COUNT(*) FROM media_assets WHERE id = $1", assetID, "media_assets"},
		{"SELECT COUNT(*) FROM asset_digests WHERE asset_id = $1", assetID, "asset_digests"},
		{"SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1", assetID, "domain_events"},
	} {
		var count int
		if err := db.QueryRow(ctx, check.query, check.arg).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", check.label, err)
		}
		if count != 0 {
			t.Fatalf("expected 0 %s rows after rollback, got %d", check.label, count)
		}
	}
}

func mustRootPathBytes(t *testing.T, raw []byte) domain.PathIdentity {
	t.Helper()
	p, err := domain.NewRootPathIdentity(raw)
	if err != nil {
		t.Fatalf("NewRootPathIdentity(%q): %v", raw, err)
	}
	return p
}

func intPtrForWriteTest(v int) *int { return &v }

func TestCatalogueWriteRepository_UpdateAssetAndConcurrentDigestRotation(t *testing.T) {
	t.Parallel()
	db, transactor := setupCatalogueWriteTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var assetID string
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib, root := seedTestLibraryAndRoot(ctx, t, tx, "Concurrent Lib", []byte("/srv/conc"))
		cw := tx.CatalogueWrites()
		vol := domain.Volume{LibraryID: lib.ID, Kind: domain.VolumeKindVolume}
		if err := cw.CreateVolume(ctx, &vol); err != nil {
			return err
		}
		asset := domain.MediaAsset{
			VolumeID:      vol.ID,
			LibraryRootID: root.ID,
			RelPathBytes:  []byte("conc.cbz"),
			FileSize:      500,
			Format:        domain.MediaFormatCBZ,
			Digests: []domain.AssetDigest{{
				Kind:      domain.DigestKindXXH364Partial,
				Value:     "initial-hex",
				IsCurrent: true,
				Reason:    domain.DigestReasonScan,
			}},
		}
		if err := cw.CreateAsset(ctx, &asset); err != nil {
			return err
		}
		assetID = asset.ID
		return nil
	}); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	cw := postgres.NewCatalogueWriteRepository(db.Pool)
	got, err := cw.GetAssetByID(ctx, assetID)
	if err != nil {
		t.Fatalf("GetAssetByID: %v", err)
	}

	// Invalid retired without missing_since must be rejected before SQL.
	bad := *got
	now := time.Now().UTC().Truncate(time.Microsecond)
	bad.RetiredAt = &now
	if err := cw.UpdateAsset(ctx, &bad); !errors.Is(err, domain.ErrRetiredWithoutMissing) {
		t.Fatalf("UpdateAsset retired without missing = %v, want ErrRetiredWithoutMissing", err)
	}

	// Valid missing + retired state and metadata update round-trip cleanly.
	missingAt := now.Add(-48 * time.Hour)
	got.FileSize = 750
	got.PageCount = 32
	got.CoverPageIndex = 2
	got.SuppressedPages = []int{0, 31}
	got.MissingSince = &missingAt
	got.RetiredAt = &now
	if err := cw.UpdateAsset(ctx, got); err != nil {
		t.Fatalf("UpdateAsset: %v", err)
	}
	reloaded, err := cw.GetAssetByID(ctx, assetID)
	if err != nil {
		t.Fatalf("GetAssetByID after UpdateAsset: %v", err)
	}
	if reloaded.FileSize != 750 || reloaded.PageCount != 32 || !reloaded.IsMissing() || !reloaded.IsRetired() {
		t.Fatalf("unexpected asset after UpdateAsset: %+v", reloaded)
	}

	// Concurrent digest rotations on the same asset serialize via FOR UPDATE and leave one current row.
	const workers = 6
	errs := make(chan error, workers)
	start := make(chan struct{})
	for i := range workers {
		go func(idx int) {
			<-start
			errs <- cw.RecordAssetDigest(ctx, &domain.AssetDigest{
				AssetID: assetID,
				Kind:    domain.DigestKindXXH364Partial,
				Value:   "worker-hex-" + string(rune('a'+idx)),
				Reason:  domain.DigestReasonExternalModify,
			})
		}(i)
	}
	close(start)
	for range workers {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent RecordAssetDigest failed: %v", err)
		}
	}

	var currentCount, totalCount int
	if err := db.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE is_current), COUNT(*)
		FROM asset_digests
		WHERE asset_id = $1 AND kind = 'XXH3_64_PARTIAL'`,
		assetID,
	).Scan(&currentCount, &totalCount); err != nil {
		t.Fatalf("querying concurrent digest counts: %v", err)
	}
	if currentCount != 1 || totalCount != workers+1 {
		t.Fatalf("concurrent digest counts = (current=%d, total=%d), want (1, %d)", currentCount, totalCount, workers+1)
	}
}
