package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

type folderBrowseFixture struct {
	ctx       context.Context
	tdb       *testdb.TestDB
	repo      *postgres.CatalogueRepository
	admin     *domain.Principal
	reader    *domain.Principal
	libraryID string
	rootIDs   []string
	volumeID  string
}

func setupFolderBrowseFixture(t *testing.T, rootPaths ...string) *folderBrowseFixture {
	t.Helper()
	ctx := context.Background()
	tdb := testdb.New(t)
	if err := migrations.RunFS(ctx, tdb.Pool, migrations.CoreFS); err != nil {
		t.Fatalf("RunFS(CoreFS): %v", err)
	}

	userRepo := postgres.NewUserRepository(tdb.Pool)
	adminRole, ok := domain.BuiltInRoleByName(domain.RoleAdminName)
	if !ok {
		t.Fatal("Admin role missing")
	}
	readerRole, ok := domain.BuiltInRoleByName(domain.RoleReaderName)
	if !ok {
		t.Fatal("Reader role missing")
	}
	adminUser := createSeededTestUser(ctx, t, userRepo, "fb-admin")
	readerUser := createSeededTestUser(ctx, t, userRepo, "fb-reader")
	if err := userRepo.AssignUserRole(ctx, adminUser.ID, adminRole.ID); err != nil {
		t.Fatalf("AssignUserRole admin: %v", err)
	}
	if err := userRepo.AssignUserRole(ctx, readerUser.ID, readerRole.ID); err != nil {
		t.Fatalf("AssignUserRole reader: %v", err)
	}

	libID := domain.MustNewID()
	if _, err := tdb.Exec(ctx, `INSERT INTO libraries (id, name) VALUES ($1, $2)`, libID, "Folder Browse Lib"); err != nil {
		t.Fatalf("insert library: %v", err)
	}

	rootIDs := make([]string, 0, len(rootPaths))
	for _, rp := range rootPaths {
		rid := domain.MustNewID()
		pid, err := domain.NewRootPathIdentity([]byte(rp))
		if err != nil {
			t.Fatalf("NewRootPathIdentity(%q): %v", rp, err)
		}
		if _, err := tdb.Exec(ctx, `
			INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match)
			VALUES ($1, $2, $3, $4, $5)`,
			rid, libID, pid.Bytes, pid.Display, pid.Match,
		); err != nil {
			t.Fatalf("insert library_root %q: %v", rp, err)
		}
		rootIDs = append(rootIDs, rid)
	}

	volID := domain.MustNewID()
	if _, err := tdb.Exec(ctx, `INSERT INTO volumes (id, library_id, title) VALUES ($1, $2, 'Shared Vol')`, volID, libID); err != nil {
		t.Fatalf("insert volume: %v", err)
	}

	return &folderBrowseFixture{
		ctx:       ctx,
		tdb:       tdb,
		repo:      postgres.NewCatalogueRepository(tdb.Pool),
		admin:     domain.NewPrincipal(*adminUser, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-admin", nil),
		reader:    domain.NewPrincipal(*readerUser, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-reader", nil),
		libraryID: libID,
		rootIDs:   rootIDs,
		volumeID:  volID,
	}
}

func (f *folderBrowseFixture) insertAsset(t *testing.T, rootID, volumeID string, relBytes []byte, format domain.MediaFormat, size int64, pages int, missingAt, retiredAt *time.Time) string {
	t.Helper()
	pid, err := domain.NewRelativePathIdentity(relBytes)
	if err != nil {
		t.Fatalf("NewRelativePathIdentity(%x): %v", relBytes, err)
	}
	id := domain.MustNewID()
	if _, err := f.tdb.Exec(f.ctx, `
		INSERT INTO media_assets (
			id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match,
			file_size, format, page_count, missing_since, retired_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		id, volumeID, rootID, pid.Bytes, pid.Display, pid.Match,
		size, string(format), pages, missingAt, retiredAt,
	); err != nil {
		t.Fatalf("insert asset (%x): %v", relBytes, err)
	}
	return id
}

func TestCatalogueRepository_BrowseFolderMultiRootHierarchyAndPrefixBoundaries(t *testing.T) {
	fx := setupFolderBrowseFixture(t, "/mnt/root1", "/mnt/root2", "/mnt/root3")
	scope := storage.CatalogueReadScope{Principal: fx.reader}

	// Root 1
	aRoot1Intro := fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("Manga/intro.cbz"), domain.MediaFormatCBZ, 1001, 12, nil, nil)
	aRoot1OP1 := fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("Manga/One Piece/v01.cbz"), domain.MediaFormatCBZ, 2001, 24, nil, nil)
	_ = fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("Manga/Naruto/v01.cbz"), domain.MediaFormatCBZ, 2002, 22, nil, nil)
	_ = fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("Manga-Extra/extra.cbz"), domain.MediaFormatCBZ, 300, 5, nil, nil)
	_ = fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("Manga0/zero.cbz"), domain.MediaFormatCBZ, 301, 6, nil, nil)
	_ = fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("Manga2/two.cbz"), domain.MediaFormatCBZ, 302, 7, nil, nil)
	aRootA := fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("root-a.cbz"), domain.MediaFormatCBZ, 401, 10, nil, nil)
	aCollisionFile := fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("collision.cbz"), domain.MediaFormatCBZ, 501, 11, nil, nil)

	// Root 2 (shares "Manga" and "Manga/One Piece", shares "Manga/intro.cbz" path, plus folder "collision.cbz")
	aRoot2Intro := fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("Manga/intro.cbz"), domain.MediaFormatEPUB, 1002, 13, nil, nil)
	aRoot2OP2 := fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("Manga/One Piece/v02.cbz"), domain.MediaFormatCBZ, 2003, 25, nil, nil)
	_ = fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("Manga/Bleach/v01.cbz"), domain.MediaFormatCBZ, 2004, 21, nil, nil)
	aRootB := fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("root-b.pdf"), domain.MediaFormatPDF, 402, 14, nil, nil)
	aCollisionNested := fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("collision.cbz/inside.cbz"), domain.MediaFormatCBZ, 502, 15, nil, nil)

	// Root 3 (deep nesting)
	aDeep := fx.insertAsset(t, fx.rootIDs[2], fx.volumeID, []byte("Solo/deep/nested/file.cbz"), domain.MediaFormatCBZ, 601, 16, nil, nil)

	// Second library (must remain completely isolated from fx.libraryID)
	otherLibID := domain.MustNewID()
	otherRootID := domain.MustNewID()
	otherVolID := domain.MustNewID()
	if _, err := fx.tdb.Exec(fx.ctx, `INSERT INTO libraries (id, name) VALUES ($1, 'Other Lib')`, otherLibID); err != nil {
		t.Fatalf("insert other library: %v", err)
	}
	if _, err := fx.tdb.Exec(fx.ctx, `INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match) VALUES ($1, $2, $3, '/mnt/other', '/mnt/other')`, otherRootID, otherLibID, []byte("/mnt/other")); err != nil {
		t.Fatalf("insert other root: %v", err)
	}
	if _, err := fx.tdb.Exec(fx.ctx, `INSERT INTO volumes (id, library_id, title) VALUES ($1, $2, 'Other Vol')`, otherVolID, otherLibID); err != nil {
		t.Fatalf("insert other vol: %v", err)
	}
	_ = fx.insertAsset(t, otherRootID, otherVolID, []byte("Manga/OtherLib/x.cbz"), domain.MediaFormatCBZ, 999, 9, nil, nil)
	_ = fx.insertAsset(t, otherRootID, otherVolID, []byte("other-root.cbz"), domain.MediaFormatCBZ, 999, 9, nil, nil)

	// 1. Browse root (both nil and empty []byte{})
	for _, rootBytes := range [][]byte{nil, {}} {
		res, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
			LibraryID:       fx.libraryID,
			FolderPathBytes: rootBytes,
		})
		if err != nil {
			t.Fatalf("BrowseFolder(root=%v): %v", rootBytes, err)
		}
		if res.LibraryID != fx.libraryID || res.FolderPath.Display != "" || len(res.FolderPath.Bytes) != 0 {
			t.Fatalf("unexpected root result header: %+v", res)
		}
		if res.TotalCount != 9 || len(res.Entries) != 9 {
			t.Fatalf("root TotalCount=%d len=%d, want 9 (entries=%+v)", res.TotalCount, len(res.Entries), res.Entries)
		}

		// Folders precede files in byte order: Manga, Manga-Extra, Manga0, Manga2, Solo, collision.cbz
		wantFolders := []string{"Manga", "Manga-Extra", "Manga0", "Manga2", "Solo", "collision.cbz"}
		for i, want := range wantFolders {
			e := res.Entries[i]
			if e.Kind != storage.FolderEntryKindFolder || string(e.Name.Bytes) != want || string(e.RelPath.Bytes) != want || e.AssetID != "" {
				t.Fatalf("root entry[%d] = %+v, want folder %q", i, e, want)
			}
		}

		// Immediate root files follow: collision.cbz, root-a.cbz, root-b.pdf
		wantFiles := []struct {
			name    string
			assetID string
			rootID  string
			format  domain.MediaFormat
		}{
			{"collision.cbz", aCollisionFile, fx.rootIDs[0], domain.MediaFormatCBZ},
			{"root-a.cbz", aRootA, fx.rootIDs[0], domain.MediaFormatCBZ},
			{"root-b.pdf", aRootB, fx.rootIDs[1], domain.MediaFormatPDF},
		}
		for i, wf := range wantFiles {
			e := res.Entries[len(wantFolders)+i]
			if e.Kind != storage.FolderEntryKindFile || string(e.Name.Bytes) != wf.name || e.AssetID != wf.assetID || e.LibraryRootID != wf.rootID || e.Format != wf.format {
				t.Fatalf("root file[%d] = %+v, want (%q, %s, %s, %s)", i, e, wf.name, wf.assetID, wf.rootID, wf.format)
			}
		}
	}

	// 2. Browse "Manga" across Root 1 and Root 2; ensure Manga-Extra/Manga0/Manga2/OtherLib do not leak
	mangaRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: []byte("Manga"),
	})
	if err != nil {
		t.Fatalf("BrowseFolder(Manga): %v", err)
	}
	if mangaRes.TotalCount != 5 || len(mangaRes.Entries) != 5 {
		t.Fatalf("Manga TotalCount=%d len=%d, want 5", mangaRes.TotalCount, len(mangaRes.Entries))
	}
	if mangaRes.FolderPath.Display != "Manga" || !bytes.Equal(mangaRes.FolderPath.Bytes, []byte("Manga")) {
		t.Fatalf("Manga FolderPath = %+v", mangaRes.FolderPath)
	}
	for i, want := range []struct {
		name    string
		relPath string
	}{
		{"Bleach", "Manga/Bleach"},
		{"Naruto", "Manga/Naruto"},
		{"One Piece", "Manga/One Piece"},
	} {
		e := mangaRes.Entries[i]
		if e.Kind != storage.FolderEntryKindFolder || e.Name.Display != want.name || e.RelPath.Display != want.relPath {
			t.Fatalf("Manga folder[%d] = %+v, want %+v", i, e, want)
		}
	}
	introIDs := []string{mangaRes.Entries[3].AssetID, mangaRes.Entries[4].AssetID}
	wantIntroIDs := []string{aRoot1Intro, aRoot2Intro}
	slices.Sort(introIDs)
	slices.Sort(wantIntroIDs)
	if !slices.Equal(introIDs, wantIntroIDs) {
		t.Fatalf("Manga intro file AssetIDs = %v, want %v", introIDs, wantIntroIDs)
	}
	for _, e := range mangaRes.Entries[3:] {
		if e.Kind != storage.FolderEntryKindFile || e.Name.Display != "intro.cbz" || e.RelPath.Display != "Manga/intro.cbz" {
			t.Fatalf("Manga intro file unexpected entry: %+v", e)
		}
		wantFormat := domain.MediaFormatCBZ
		if e.AssetID == aRoot2Intro {
			wantFormat = domain.MediaFormatEPUB
		}
		if e.Format != wantFormat {
			t.Fatalf("Manga intro %s Format = %q, want %q", e.AssetID, e.Format, wantFormat)
		}
	}

	// 3. Browse "Manga/One Piece" -> files from Root 1 and Root 2 with full asset metadata
	opRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: []byte("Manga/One Piece"),
	})
	if err != nil {
		t.Fatalf("BrowseFolder(Manga/One Piece): %v", err)
	}
	if opRes.TotalCount != 2 || len(opRes.Entries) != 2 {
		t.Fatalf("Manga/One Piece = (%d, %d), want 2", opRes.TotalCount, len(opRes.Entries))
	}
	if opRes.Entries[0].AssetID != aRoot1OP1 || opRes.Entries[0].Name.Display != "v01.cbz" ||
		opRes.Entries[0].RelPath.Display != "Manga/One Piece/v01.cbz" ||
		opRes.Entries[0].FileSize != 2001 || opRes.Entries[0].PageCount != 24 || opRes.Entries[0].Format != domain.MediaFormatCBZ ||
		opRes.Entries[0].VolumeID != fx.volumeID || opRes.Entries[0].LibraryRootID != fx.rootIDs[0] {
		t.Fatalf("unexpected v01 entry: %+v", opRes.Entries[0])
	}
	if opRes.Entries[1].AssetID != aRoot2OP2 || opRes.Entries[1].Name.Display != "v02.cbz" || opRes.Entries[1].LibraryRootID != fx.rootIDs[1] {
		t.Fatalf("unexpected v02 entry: %+v", opRes.Entries[1])
	}

	// 4. Browse "collision.cbz" folder -> only inside.cbz (does not confuse with root file collision.cbz)
	colRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: []byte("collision.cbz"),
	})
	if err != nil || colRes.TotalCount != 1 || len(colRes.Entries) != 1 || colRes.Entries[0].AssetID != aCollisionNested {
		t.Fatalf("BrowseFolder(collision.cbz) = (%+v, %v), want [%s]", colRes, err, aCollisionNested)
	}

	// 5. Deep drill-down "Solo" -> "Solo/deep" -> "Solo/deep/nested" -> "file.cbz"
	soloRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{LibraryID: fx.libraryID, FolderPathBytes: []byte("Solo")})
	if err != nil || len(soloRes.Entries) != 1 || soloRes.Entries[0].Kind != storage.FolderEntryKindFolder || soloRes.Entries[0].RelPath.Display != "Solo/deep" {
		t.Fatalf("BrowseFolder(Solo) = (%+v, %v)", soloRes, err)
	}
	deepRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{LibraryID: fx.libraryID, FolderPathBytes: soloRes.Entries[0].RelPath.Bytes})
	if err != nil || len(deepRes.Entries) != 1 || deepRes.Entries[0].Kind != storage.FolderEntryKindFolder || deepRes.Entries[0].RelPath.Display != "Solo/deep/nested" {
		t.Fatalf("BrowseFolder(Solo/deep) = (%+v, %v)", deepRes, err)
	}
	nestedRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{LibraryID: fx.libraryID, FolderPathBytes: deepRes.Entries[0].RelPath.Bytes})
	if err != nil || len(nestedRes.Entries) != 1 || nestedRes.Entries[0].Kind != storage.FolderEntryKindFile || nestedRes.Entries[0].AssetID != aDeep {
		t.Fatalf("BrowseFolder(Solo/deep/nested) = (%+v, %v)", nestedRes, err)
	}

	// 6. Nonexistent folder returns empty non-nil slice and TotalCount=0
	missingFolderRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{LibraryID: fx.libraryID, FolderPathBytes: []byte("DoesNotExist")})
	if err != nil || missingFolderRes.TotalCount != 0 || missingFolderRes.Entries == nil || len(missingFolderRes.Entries) != 0 {
		t.Fatalf("BrowseFolder(DoesNotExist) = (%+v, %v), want empty non-nil", missingFolderRes, err)
	}
}

func TestCatalogueRepository_BrowseFolderRawBytesInvalidUTF8AndNFCvsNFD(t *testing.T) {
	fx := setupFolderBrowseFixture(t, "/mnt/raw1", "/mnt/raw2")
	scope := storage.CatalogueReadScope{Principal: fx.reader}

	rawInvalidFolder := []byte{0xff, 0xfe}
	litEscapedFolder := []byte(`\xff\xfe`) // Identical Display "\xff\xfe", distinct raw bytes!

	aRawDirect := fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, append(slices.Clone(rawInvalidFolder), []byte("/raw-book.cbz")...), domain.MediaFormatCBZ, 100, 10, nil, nil)
	aRawSubFile := fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, append(slices.Clone(rawInvalidFolder), []byte("/sub/nested.cbz")...), domain.MediaFormatCBZ, 101, 11, nil, nil)
	aLitDirect := fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, append(slices.Clone(litEscapedFolder), []byte("/lit-book.cbz")...), domain.MediaFormatCBZ, 102, 12, nil, nil)

	// Nested invalid UTF-8 folder + invalid UTF-8 file name
	nestedInvalidFolder := []byte{'s', 'u', 'b', '/', 0xc3}
	aNestedInvalidFile := fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte{'s', 'u', 'b', '/', 0xc3, '/', 0x80, '.', 'c', 'b', 'z'}, domain.MediaFormatCBZ, 103, 13, nil, nil)

	// NFC vs NFD folder names in separate roots (same RelPath.Match, distinct RelPath.Bytes)
	nfcFolder := []byte("Unicode/\u304c")
	nfdFolder := []byte("Unicode/\u304b\u3099")
	aNFC := fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, append(slices.Clone(nfcFolder), []byte("/nfc.cbz")...), domain.MediaFormatCBZ, 104, 14, nil, nil)
	aNFD := fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, append(slices.Clone(nfdFolder), []byte("/nfd.cbz")...), domain.MediaFormatCBZ, 105, 15, nil, nil)

	// 1. Root contains 4 distinct folders: "Unicode", `\xff\xfe`, "sub", and raw 0xff 0xfe
	rootRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{LibraryID: fx.libraryID})
	if err != nil {
		t.Fatalf("BrowseFolder(root): %v", err)
	}
	if rootRes.TotalCount != 4 || len(rootRes.Entries) != 4 {
		t.Fatalf("root TotalCount=%d len=%d, want 4 (%+v)", rootRes.TotalCount, len(rootRes.Entries), rootRes.Entries)
	}

	var sawRaw, sawLit bool
	for _, e := range rootRes.Entries {
		if bytes.Equal(e.Name.Bytes, rawInvalidFolder) {
			sawRaw = true
			if e.Name.Display != `\xff\xfe` || !e.Name.InvalidUTF8 || !e.RelPath.InvalidUTF8 {
				t.Fatalf("raw invalid-UTF8 folder unexpected identity: %+v", e)
			}
		}
		if bytes.Equal(e.Name.Bytes, litEscapedFolder) {
			sawLit = true
			if e.Name.Display != `\xff\xfe` || e.Name.InvalidUTF8 || e.RelPath.InvalidUTF8 {
				t.Fatalf("literal escaped folder unexpected identity: %+v", e)
			}
		}
	}
	if !sawRaw || !sawLit {
		t.Fatalf("expected both raw (sawRaw=%v) and literal (sawLit=%v) folders at root; got %+v", sawRaw, sawLit, rootRes.Entries)
	}

	// 2. Browsing raw invalid-UTF-8 prefix returns ONLY rawInvalidFolder's children (across both roots), never literal `\xff\xfe`
	rawRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: rawInvalidFolder,
	})
	if err != nil {
		t.Fatalf("BrowseFolder(rawInvalidFolder): %v", err)
	}
	if rawRes.TotalCount != 2 || len(rawRes.Entries) != 2 {
		t.Fatalf("rawInvalidFolder entries = %+v, want 2 (sub + raw-book.cbz)", rawRes.Entries)
	}
	if !rawRes.FolderPath.InvalidUTF8 || rawRes.FolderPath.Display != `\xff\xfe` {
		t.Fatalf("rawRes.FolderPath = %+v, want InvalidUTF8=true Display=\\xff\\xfe", rawRes.FolderPath)
	}
	subEntry := rawRes.Entries[0]
	if subEntry.Kind != storage.FolderEntryKindFolder || subEntry.Name.Display != "sub" || subEntry.Name.InvalidUTF8 ||
		subEntry.RelPath.Display != `\xff\xfe/sub` || !subEntry.RelPath.InvalidUTF8 {
		t.Fatalf("sub entry unexpected identity: %+v", subEntry)
	}
	rawBookEntry := rawRes.Entries[1]
	if rawBookEntry.Kind != storage.FolderEntryKindFile || rawBookEntry.AssetID != aRawDirect ||
		rawBookEntry.Name.Display != "raw-book.cbz" || rawBookEntry.Name.InvalidUTF8 ||
		rawBookEntry.RelPath.Display != `\xff\xfe/raw-book.cbz` || !rawBookEntry.RelPath.InvalidUTF8 {
		t.Fatalf("raw-book entry unexpected identity: %+v", rawBookEntry)
	}

	// Drill into rawInvalidFolder + "/sub"
	rawSubRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: subEntry.RelPath.Bytes,
	})
	if err != nil || rawSubRes.TotalCount != 1 || len(rawSubRes.Entries) != 1 || rawSubRes.Entries[0].AssetID != aRawSubFile {
		t.Fatalf("BrowseFolder(\\xff\\xfe/sub) = (%+v, %v), want [%s]", rawSubRes, err, aRawSubFile)
	}

	// 3. Browsing literal `\xff\xfe` returns ONLY lit-book.cbz
	litRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: litEscapedFolder,
	})
	if err != nil || litRes.TotalCount != 1 || len(litRes.Entries) != 1 || litRes.Entries[0].AssetID != aLitDirect {
		t.Fatalf("BrowseFolder(literal \\xff\\xfe) = (%+v, %v), want [%s]", litRes, err, aLitDirect)
	}

	// 4. Nested invalid-UTF-8 subfolder and file name
	nestedInvRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: nestedInvalidFolder,
	})
	if err != nil || nestedInvRes.TotalCount != 1 || len(nestedInvRes.Entries) != 1 {
		t.Fatalf("BrowseFolder(sub/\\xc3) = (%+v, %v)", nestedInvRes, err)
	}
	invFile := nestedInvRes.Entries[0]
	if invFile.AssetID != aNestedInvalidFile ||
		invFile.Name.Display != `\x80.cbz` || !invFile.Name.InvalidUTF8 ||
		invFile.RelPath.Display != `sub/\xc3/\x80.cbz` || !invFile.RelPath.InvalidUTF8 {
		t.Fatalf("invalid UTF-8 file identity = %+v", invFile)
	}

	// 5. NFC and NFD folders remain distinct by rel_path_bytes
	uniRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: []byte("Unicode"),
	})
	if err != nil || uniRes.TotalCount != 2 || len(uniRes.Entries) != 2 {
		t.Fatalf("BrowseFolder(Unicode) = (%+v, %v), want 2 distinct NFC/NFD folders", uniRes, err)
	}
	if uniRes.Entries[0].RelPath.Match != uniRes.Entries[1].RelPath.Match ||
		bytes.Equal(uniRes.Entries[0].RelPath.Bytes, uniRes.Entries[1].RelPath.Bytes) {
		t.Fatalf("expected equal Match keys and distinct Bytes for NFC vs NFD: %+v", uniRes.Entries)
	}
	if uniRes.Entries[0].Name.InvalidUTF8 || uniRes.Entries[1].Name.InvalidUTF8 ||
		uniRes.Entries[0].RelPath.InvalidUTF8 || uniRes.Entries[1].RelPath.InvalidUTF8 {
		t.Fatalf("expected InvalidUTF8=false for valid NFC/NFD paths: %+v", uniRes.Entries)
	}
	nfcRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{LibraryID: fx.libraryID, FolderPathBytes: nfcFolder})
	if err != nil || len(nfcRes.Entries) != 1 || nfcRes.Entries[0].AssetID != aNFC {
		t.Fatalf("BrowseFolder(NFC) = (%+v, %v), want [%s]", nfcRes, err, aNFC)
	}
	nfdRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{LibraryID: fx.libraryID, FolderPathBytes: nfdFolder})
	if err != nil || len(nfdRes.Entries) != 1 || nfdRes.Entries[0].AssetID != aNFD {
		t.Fatalf("BrowseFolder(NFD) = (%+v, %v), want [%s]", nfdRes, err, aNFD)
	}

	// 6. Invalid LibraryID and invalid FolderPathBytes fail closed
	if _, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{LibraryID: "   "}); !errors.Is(err, domain.ErrEmptyLibraryID) {
		t.Fatalf("empty LibraryID error = %v, want ErrEmptyLibraryID", err)
	}
	for _, bad := range [][]byte{
		[]byte("/abs"),
		[]byte("../escape"),
		[]byte("sub/.."),
		[]byte("."),
		[]byte(".."),
		[]byte("sub/"),
		[]byte("sub//child"),
		[]byte("sub/./child"),
		[]byte("nul\x00byte"),
	} {
		if _, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
			LibraryID:       fx.libraryID,
			FolderPathBytes: bad,
		}); !errors.Is(err, domain.ErrInvalidAssetPath) {
			t.Fatalf("bad FolderPathBytes %q error = %v, want ErrInvalidAssetPath", bad, err)
		}
	}
}

func TestCatalogueRepository_BrowseFolderPaginationAndHardMaximum(t *testing.T) {
	fx := setupFolderBrowseFixture(t, "/mnt/page1", "/mnt/page2")
	scope := storage.CatalogueReadScope{Principal: fx.reader}

	const folderCount = 105
	const fileCount = 105
	const totalEntries = folderCount + fileCount // 210 > MaxFolderBrowseLimit (200)

	// Batch insert 105 child folders + 105 child files under "Shelf" across two roots.
	for i := range folderCount {
		rel := fmt.Sprintf("Shelf/dir-%03d/v01.cbz", i)
		fx.insertAsset(t, fx.rootIDs[i%2], fx.volumeID, []byte(rel), domain.MediaFormatCBZ, 100, 10, nil, nil)
	}
	for i := range fileCount {
		rel := fmt.Sprintf("Shelf/file-%03d.cbz", i)
		fx.insertAsset(t, fx.rootIDs[i%2], fx.volumeID, []byte(rel), domain.MediaFormatCBZ, 100, 10, nil, nil)
	}

	// 1. Default limit when Limit == 0
	defRes, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: []byte("Shelf"),
	})
	if err != nil {
		t.Fatalf("BrowseFolder default limit: %v", err)
	}
	if defRes.TotalCount != totalEntries || defRes.Limit != storage.DefaultFolderBrowseLimit || len(defRes.Entries) != storage.DefaultFolderBrowseLimit {
		t.Fatalf("default limit page = (total=%d, limit=%d, len=%d), want (%d, %d, %d)",
			defRes.TotalCount, defRes.Limit, len(defRes.Entries), totalEntries, storage.DefaultFolderBrowseLimit, storage.DefaultFolderBrowseLimit)
	}
	for i, e := range defRes.Entries {
		wantName := fmt.Sprintf("dir-%03d", i)
		if e.Kind != storage.FolderEntryKindFolder || e.Name.Display != wantName {
			t.Fatalf("default page entry[%d] = (%s, %q), want (FOLDER, %q)", i, e.Kind, e.Name.Display, wantName)
		}
	}

	// 2. Requesting above MaxFolderBrowseLimit is capped at MaxFolderBrowseLimit, and paging recovers all entries once
	page1, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: []byte("Shelf"),
		Limit:           storage.MaxFolderBrowseLimit + 500,
		Offset:          0,
	})
	if err != nil {
		t.Fatalf("BrowseFolder over-max page1: %v", err)
	}
	if page1.Limit != storage.MaxFolderBrowseLimit || len(page1.Entries) != storage.MaxFolderBrowseLimit || page1.TotalCount != totalEntries {
		t.Fatalf("page1 = (total=%d, limit=%d, len=%d), want (%d, %d, %d)",
			page1.TotalCount, page1.Limit, len(page1.Entries), totalEntries, storage.MaxFolderBrowseLimit, storage.MaxFolderBrowseLimit)
	}

	page2, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: []byte("Shelf"),
		Limit:           storage.MaxFolderBrowseLimit + 500,
		Offset:          storage.MaxFolderBrowseLimit,
	})
	if err != nil {
		t.Fatalf("BrowseFolder over-max page2: %v", err)
	}
	wantRemainder := totalEntries - storage.MaxFolderBrowseLimit
	if page2.Limit != storage.MaxFolderBrowseLimit || len(page2.Entries) != wantRemainder || page2.TotalCount != totalEntries {
		t.Fatalf("page2 = (total=%d, limit=%d, len=%d), want (%d, %d, %d)",
			page2.TotalCount, page2.Limit, len(page2.Entries), totalEntries, storage.MaxFolderBrowseLimit, wantRemainder)
	}

	combined := append(slices.Clone(page1.Entries), page2.Entries...)
	if len(combined) != totalEntries {
		t.Fatalf("combined len = %d, want %d", len(combined), totalEntries)
	}
	for i := range folderCount {
		if combined[i].Kind != storage.FolderEntryKindFolder || combined[i].Name.Display != fmt.Sprintf("dir-%03d", i) {
			t.Fatalf("combined[%d] = %+v, want dir-%03d", i, combined[i], i)
		}
	}
	for i := range fileCount {
		idx := folderCount + i
		if combined[idx].Kind != storage.FolderEntryKindFile || combined[idx].Name.Display != fmt.Sprintf("file-%03d.cbz", i) {
			t.Fatalf("combined[%d] = %+v, want file-%03d.cbz", idx, combined[idx], i)
		}
	}

	// 3. Offset at/past TotalCount returns empty slice while preserving exact TotalCount
	pastEnd, err := fx.repo.BrowseFolder(fx.ctx, scope, storage.FolderBrowseFilter{
		LibraryID:       fx.libraryID,
		FolderPathBytes: []byte("Shelf"),
		Limit:           25,
		Offset:          totalEntries,
	})
	if err != nil {
		t.Fatalf("BrowseFolder pastEnd: %v", err)
	}
	if pastEnd.TotalCount != totalEntries || len(pastEnd.Entries) != 0 || pastEnd.Offset != totalEntries || pastEnd.Limit != 25 {
		t.Fatalf("pastEnd = %+v, want TotalCount=%d len=0 Limit=25 Offset=%d", pastEnd, totalEntries, totalEntries)
	}

	// 4. Invalid negative or overflowing pagination fails closed
	for _, bad := range []storage.FolderBrowseFilter{
		{LibraryID: fx.libraryID, Limit: -1},
		{LibraryID: fx.libraryID, Offset: -1},
		{LibraryID: fx.libraryID, Offset: math.MaxInt32 + 1},
	} {
		if _, err := fx.repo.BrowseFolder(fx.ctx, scope, bad); err == nil {
			t.Fatalf("expected error for invalid pagination %+v", bad)
		}
	}
}

func TestCatalogueRepository_BrowseFolderExcludesMissingAndRetiredAssets(t *testing.T) {
	fx := setupFolderBrowseFixture(t, "/mnt/avail1", "/mnt/avail2")

	missingAt := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	retiredAt := missingAt.Add(8 * 24 * time.Hour)

	_ = fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("MissingOnly/a.cbz"), domain.MediaFormatCBZ, 100, 10, &missingAt, nil)
	_ = fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("RetiredOnly/b.cbz"), domain.MediaFormatCBZ, 100, 10, &missingAt, &retiredAt)
	_ = fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("MixedAvail/gone.cbz"), domain.MediaFormatCBZ, 100, 10, &missingAt, nil)
	_ = fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("MixedAvail/retired.cbz"), domain.MediaFormatCBZ, 100, 10, &missingAt, &retiredAt)
	aMixLive := fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("MixedAvail/live.cbz"), domain.MediaFormatCBZ, 100, 10, nil, nil)
	_ = fx.insertAsset(t, fx.rootIDs[0], fx.volumeID, []byte("root-missing.cbz"), domain.MediaFormatCBZ, 100, 10, &missingAt, nil)
	_ = fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("root-retired.cbz"), domain.MediaFormatCBZ, 100, 10, &missingAt, &retiredAt)
	aRootLive := fx.insertAsset(t, fx.rootIDs[1], fx.volumeID, []byte("root-live.cbz"), domain.MediaFormatCBZ, 100, 10, nil, nil)

	for _, sc := range []storage.CatalogueReadScope{
		{Principal: fx.admin, IncludeTrashed: true},
		{Principal: fx.reader},
	} {
		rootRes, err := fx.repo.BrowseFolder(fx.ctx, sc, storage.FolderBrowseFilter{LibraryID: fx.libraryID})
		if err != nil {
			t.Fatalf("BrowseFolder(root): %v", err)
		}
		if rootRes.TotalCount != 2 || len(rootRes.Entries) != 2 {
			t.Fatalf("root = (%d, %+v), want 2 entries (MixedAvail folder + root-live.cbz)", rootRes.TotalCount, rootRes.Entries)
		}
		if rootRes.Entries[0].Kind != storage.FolderEntryKindFolder || rootRes.Entries[0].Name.Display != "MixedAvail" {
			t.Fatalf("root[0] = %+v, want folder MixedAvail", rootRes.Entries[0])
		}
		if rootRes.Entries[1].Kind != storage.FolderEntryKindFile || rootRes.Entries[1].AssetID != aRootLive {
			t.Fatalf("root[1] = %+v, want file %s", rootRes.Entries[1], aRootLive)
		}

		for _, hiddenFolder := range [][]byte{[]byte("MissingOnly"), []byte("RetiredOnly")} {
			res, err := fx.repo.BrowseFolder(fx.ctx, sc, storage.FolderBrowseFilter{
				LibraryID:       fx.libraryID,
				FolderPathBytes: hiddenFolder,
			})
			if err != nil || res.TotalCount != 0 || len(res.Entries) != 0 {
				t.Fatalf("BrowseFolder(%s) = (%+v, %v), want empty", hiddenFolder, res, err)
			}
		}

		mixRes, err := fx.repo.BrowseFolder(fx.ctx, sc, storage.FolderBrowseFilter{
			LibraryID:       fx.libraryID,
			FolderPathBytes: []byte("MixedAvail"),
		})
		if err != nil || mixRes.TotalCount != 1 || len(mixRes.Entries) != 1 || mixRes.Entries[0].AssetID != aMixLive {
			t.Fatalf("BrowseFolder(MixedAvail) = (%+v, %v), want [%s]", mixRes, err, aMixLive)
		}
	}
}
