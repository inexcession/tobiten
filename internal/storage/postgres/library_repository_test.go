package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5/pgconn"
)

func setupLibraryTestDB(t *testing.T) (*testdb.TestDB, *postgres.Transactor) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupLibraryTestDB: applying migrations: %v", err)
	}

	return db, postgres.NewTransactor(db.Pool)
}

func mustRootPath(t *testing.T, raw []byte) domain.PathIdentity {
	t.Helper()
	p, err := domain.NewRootPathIdentity(raw)
	if err != nil {
		t.Fatalf("NewRootPathIdentity(%x) failed: %v", raw, err)
	}
	return p
}

func assertPathIdentityEqual(t *testing.T, label string, got, want domain.PathIdentity) {
	t.Helper()
	if !bytes.Equal(got.Bytes, want.Bytes) {
		t.Errorf("%s.Bytes = %x, want %x", label, got.Bytes, want.Bytes)
	}
	if got.Display != want.Display {
		t.Errorf("%s.Display = %q, want %q", label, got.Display, want.Display)
	}
	if got.Match != want.Match {
		t.Errorf("%s.Match = %q, want %q", label, got.Match, want.Match)
	}
	if got.InvalidUTF8 != want.InvalidUTF8 {
		t.Errorf("%s.InvalidUTF8 = %v, want %v", label, got.InvalidUTF8, want.InvalidUTF8)
	}
}

func TestLibraryRepository_CRUDAndMultiRootViaTx(t *testing.T) {
	t.Parallel()
	_, transactor := setupLibraryTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		lib1ID  string
		lib2ID  string
		root1ID string
		root2ID string
		root3ID string
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()

		// Create virtual library using creation defaults for resolver_chain, precedence, cover policy.
		lib1 := &domain.Library{
			Name: "Main Manga",
			Mode: domain.LibraryModeVirtual,
		}
		if err := repo.CreateLibrary(ctx, lib1); err != nil {
			return err
		}
		lib1ID = lib1.ID
		if lib1.ID == "" || lib1.CreatedAt.IsZero() {
			t.Fatalf("expected generated ID and CreatedAt, got %+v", lib1)
		}
		if !slices.Equal(lib1.ResolverChain, domain.DefaultResolverChain()) {
			t.Fatalf("expected default resolver chain, got %v", lib1.ResolverChain)
		}

		// Create managed library with explicit settings and invalid-UTF-8 intake path.
		rawIntake := []byte{'/', 'm', 'n', 't', '/', 'i', 'n', 't', 'a', 'k', 'e', '-', 0xff, 0xfe}
		intake := mustRootPath(t, rawIntake)
		lib2 := &domain.Library{
			Name:                  "Western Comics",
			Mode:                  domain.LibraryModeManaged,
			IntakePath:            &intake,
			AutoMetadataWriteback: true,
			ResolverChain:         []domain.ResolverName{domain.ResolverFolder, domain.ResolverMetadata, domain.ResolverFilename},
			MetadataPrecedence:    domain.MetadataPrecedenceOnlineOverEmbedded,
			DefaultCoverPolicy:    domain.CoverPolicyLatest,
			CreatedAt:             lib1.CreatedAt.Add(time.Second),
		}
		if err := repo.CreateLibrary(ctx, lib2); err != nil {
			return err
		}
		lib2ID = lib2.ID

		// Create multiple roots on lib1 (including NFD and invalid-UTF-8 paths) and one on lib2.
		root1 := &domain.LibraryRoot{
			LibraryID: lib1.ID,
			Path:      mustRootPath(t, []byte("/mnt/Cafe\u0301")),
		}
		if err := repo.CreateLibraryRoot(ctx, root1); err != nil {
			return err
		}
		root1ID = root1.ID
		if root1.State != domain.LibraryRootStateOnline {
			t.Fatalf("expected default root state ONLINE, got %q", root1.State)
		}

		root2 := &domain.LibraryRoot{
			LibraryID: lib1.ID,
			Path:      mustRootPath(t, []byte{'/', 'm', 'n', 't', '/', 0xe3, 0x81, '/', 'a', 'r', 'c', 'h', 'i', 'v', 'e'}),
			State:     domain.LibraryRootStateOnline,
		}
		if err := repo.CreateLibraryRoot(ctx, root2); err != nil {
			return err
		}
		root2ID = root2.ID

		root3 := &domain.LibraryRoot{
			LibraryID: lib2.ID,
			Path:      mustRootPath(t, []byte("/mnt/western")),
		}
		if err := repo.CreateLibraryRoot(ctx, root3); err != nil {
			return err
		}
		root3ID = root3.ID

		return nil
	})
	if err != nil {
		t.Fatalf("setup transaction failed: %v", err)
	}

	// Read, update, and delete in subsequent transactions obtained from storage.Tx.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()

		got1, err := repo.GetLibraryByID(ctx, lib1ID)
		if err != nil {
			t.Fatalf("GetLibraryByID(lib1): %v", err)
		}
		if got1.Name != "Main Manga" || got1.Mode != domain.LibraryModeVirtual || got1.IntakePath != nil {
			t.Fatalf("unexpected lib1: %+v", got1)
		}

		got2, err := repo.GetLibraryByName(ctx, "Western Comics")
		if err != nil {
			t.Fatalf("GetLibraryByName(lib2): %v", err)
		}
		wantIntake := mustRootPath(t, []byte{'/', 'm', 'n', 't', '/', 'i', 'n', 't', 'a', 'k', 'e', '-', 0xff, 0xfe})
		if got2.IntakePath == nil {
			t.Fatal("expected non-nil IntakePath on managed library")
		}
		assertPathIdentityEqual(t, "lib2.IntakePath", *got2.IntakePath, wantIntake)
		if !got2.AutoMetadataWriteback ||
			!slices.Equal(got2.ResolverChain, []domain.ResolverName{domain.ResolverFolder, domain.ResolverMetadata, domain.ResolverFilename}) ||
			got2.MetadataPrecedence != domain.MetadataPrecedenceOnlineOverEmbedded ||
			got2.DefaultCoverPolicy != domain.CoverPolicyLatest {
			t.Fatalf("unexpected lib2 fields: %+v", got2)
		}

		libs, err := repo.ListLibraries(ctx)
		if err != nil {
			t.Fatalf("ListLibraries: %v", err)
		}
		if len(libs) != 2 || libs[0].ID != lib1ID || libs[1].ID != lib2ID {
			t.Fatalf("unexpected ListLibraries result: %+v", libs)
		}

		rootsLib1, err := repo.ListLibraryRoots(ctx, lib1ID)
		if err != nil {
			t.Fatalf("ListLibraryRoots(lib1): %v", err)
		}
		if len(rootsLib1) != 2 {
			t.Fatalf("expected 2 roots for lib1, got %d", len(rootsLib1))
		}

		allRoots, err := repo.ListAllLibraryRoots(ctx)
		if err != nil {
			t.Fatalf("ListAllLibraryRoots: %v", err)
		}
		if len(allRoots) != 3 {
			t.Fatalf("expected 3 total roots, got %d", len(allRoots))
		}

		gotRoot1, err := repo.GetLibraryRootByID(ctx, root1ID)
		if err != nil {
			t.Fatalf("GetLibraryRootByID(root1): %v", err)
		}
		assertPathIdentityEqual(t, "root1.Path", gotRoot1.Path, mustRootPath(t, []byte("/mnt/Cafe\u0301")))

		gotRoot2, err := repo.GetLibraryRootByID(ctx, root2ID)
		if err != nil {
			t.Fatalf("GetLibraryRootByID(root2): %v", err)
		}
		assertPathIdentityEqual(t, "root2.Path", gotRoot2.Path, mustRootPath(t, []byte{'/', 'm', 'n', 't', '/', 0xe3, 0x81, '/', 'a', 'r', 'c', 'h', 'i', 'v', 'e'}))

		// Update library: switch lib1 to MANAGED with custom resolver subset, update root1 path.
		newIntake := mustRootPath(t, []byte("/mnt/manga-intake"))
		got1.Name = "Main Manga Updated"
		got1.Mode = domain.LibraryModeManaged
		got1.IntakePath = &newIntake
		got1.ResolverChain = []domain.ResolverName{domain.ResolverFolder, domain.ResolverFilename}
		got1.MetadataPrecedence = domain.MetadataPrecedenceOnlineOverEmbedded
		got1.DefaultCoverPolicy = domain.CoverPolicyManual
		if err := repo.UpdateLibrary(ctx, got1); err != nil {
			t.Fatalf("UpdateLibrary: %v", err)
		}

		reloaded1, err := repo.GetLibraryByID(ctx, lib1ID)
		if err != nil {
			t.Fatalf("GetLibraryByID after update: %v", err)
		}
		if reloaded1.Name != "Main Manga Updated" || reloaded1.Mode != domain.LibraryModeManaged || reloaded1.IntakePath == nil {
			t.Fatalf("unexpected reloaded lib1: %+v", reloaded1)
		}
		assertPathIdentityEqual(t, "reloaded1.IntakePath", *reloaded1.IntakePath, newIntake)
		if !slices.Equal(reloaded1.ResolverChain, []domain.ResolverName{domain.ResolverFolder, domain.ResolverFilename}) {
			t.Fatalf("unexpected reloaded resolver chain: %v", reloaded1.ResolverChain)
		}

		// Update root2 path and delete root1.
		newRoot2Path := mustRootPath(t, []byte("/mnt/manga-relocated"))
		gotRoot2.Path = newRoot2Path
		if err := repo.UpdateLibraryRoot(ctx, gotRoot2); err != nil {
			t.Fatalf("UpdateLibraryRoot: %v", err)
		}
		reloadedRoot2, err := repo.GetLibraryRootByID(ctx, root2ID)
		if err != nil {
			t.Fatalf("GetLibraryRootByID after update: %v", err)
		}
		assertPathIdentityEqual(t, "reloadedRoot2.Path", reloadedRoot2.Path, newRoot2Path)

		if err := repo.DeleteLibraryRoot(ctx, root1ID); err != nil {
			t.Fatalf("DeleteLibraryRoot(root1): %v", err)
		}
		if _, err := repo.GetLibraryRootByID(ctx, root1ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected ErrNotFound after deleting root1, got %v", err)
		}

		// Delete lib2 and verify its root cascades.
		if err := repo.DeleteLibrary(ctx, lib2ID); err != nil {
			t.Fatalf("DeleteLibrary(lib2): %v", err)
		}
		if _, err := repo.GetLibraryByID(ctx, lib2ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected ErrNotFound after deleting lib2, got %v", err)
		}
		if _, err := repo.GetLibraryRootByID(ctx, root3ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for cascaded root3, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify/update/delete transaction failed: %v", err)
	}
}

func TestLibraryRepository_IntakeModeRulesAndResolverValidation(t *testing.T) {
	t.Parallel()
	_, transactor := setupLibraryTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	intake := mustRootPath(t, []byte("/mnt/intake"))

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()

		// MANAGED without intake path is rejected.
		if err := repo.CreateLibrary(ctx, &domain.Library{
			Name: "Bad Managed",
			Mode: domain.LibraryModeManaged,
		}); !errors.Is(err, domain.ErrManagedLibraryRequiresIntake) {
			t.Fatalf("expected ErrManagedLibraryRequiresIntake, got %v", err)
		}

		// VIRTUAL with intake path is rejected.
		if err := repo.CreateLibrary(ctx, &domain.Library{
			Name:       "Bad Virtual",
			Mode:       domain.LibraryModeVirtual,
			IntakePath: &intake,
		}); !errors.Is(err, domain.ErrVirtualLibraryWithIntake) {
			t.Fatalf("expected ErrVirtualLibraryWithIntake, got %v", err)
		}

		// Unknown resolver name is rejected.
		if err := repo.CreateLibrary(ctx, &domain.Library{
			Name:          "Bad Resolver",
			Mode:          domain.LibraryModeVirtual,
			ResolverChain: []domain.ResolverName{domain.ResolverMetadata, domain.ResolverName("unknown")},
		}); !errors.Is(err, domain.ErrInvalidResolverName) {
			t.Fatalf("expected ErrInvalidResolverName, got %v", err)
		}

		// Duplicate resolver name is rejected.
		if err := repo.CreateLibrary(ctx, &domain.Library{
			Name:          "Dup Resolver",
			Mode:          domain.LibraryModeVirtual,
			ResolverChain: []domain.ResolverName{domain.ResolverFolder, domain.ResolverFolder},
		}); !errors.Is(err, domain.ErrDuplicateResolverName) {
			t.Fatalf("expected ErrDuplicateResolverName, got %v", err)
		}

		// Explicit empty resolver chain is rejected.
		if err := repo.CreateLibrary(ctx, &domain.Library{
			Name:          "Empty Resolver",
			Mode:          domain.LibraryModeVirtual,
			ResolverChain: []domain.ResolverName{},
		}); !errors.Is(err, domain.ErrEmptyResolverChain) {
			t.Fatalf("expected ErrEmptyResolverChain, got %v", err)
		}

		// Valid library created, then invalid UpdateLibrary is rejected and does not mutate persisted row.
		lib := &domain.Library{
			Name: "Valid Managed",
			Mode: domain.LibraryModeManaged,
			IntakePath: func() *domain.PathIdentity {
				p := mustRootPath(t, []byte("/mnt/valid-intake"))
				return &p
			}(),
		}
		if err := repo.CreateLibrary(ctx, lib); err != nil {
			t.Fatalf("CreateLibrary(valid managed): %v", err)
		}

		badSwitch := *lib
		badSwitch.Mode = domain.LibraryModeVirtual
		if err := repo.UpdateLibrary(ctx, &badSwitch); !errors.Is(err, domain.ErrVirtualLibraryWithIntake) {
			t.Fatalf("UpdateLibrary(virtual with intake) = %v, want ErrVirtualLibraryWithIntake", err)
		}

		// Switching MANAGED -> VIRTUAL without intake succeeds and clears intake columns.
		goodSwitch := *lib
		goodSwitch.Mode = domain.LibraryModeVirtual
		goodSwitch.IntakePath = nil
		if err := repo.UpdateLibrary(ctx, &goodSwitch); err != nil {
			t.Fatalf("UpdateLibrary(managed -> virtual): %v", err)
		}
		reloaded, err := repo.GetLibraryByID(ctx, lib.ID)
		if err != nil {
			t.Fatalf("GetLibraryByID: %v", err)
		}
		if reloaded.Mode != domain.LibraryModeVirtual || reloaded.IntakePath != nil {
			t.Fatalf("expected virtual mode with nil intake after switch, got %+v", reloaded)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("transaction failed: %v", err)
	}
}

func TestLibraryRepository_PersistedModeSurvivesConfigChange(t *testing.T) {
	t.Parallel()
	_, transactor := setupLibraryTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfgVirtual, err := config.Load(nil, config.WithEnviron([]string{"TOBITEN_LIBRARY_MODE=virtual"}), config.WithoutEnvFile())
	if err != nil {
		t.Fatalf("config.Load(virtual): %v", err)
	}
	defaultMode1, err := domain.ParseLibraryMode(cfgVirtual.LibraryMode)
	if err != nil {
		t.Fatalf("ParseLibraryMode(%q): %v", cfgVirtual.LibraryMode, err)
	}

	var lib1ID, lib2ID string
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib := &domain.Library{
			Name: "Created Under Virtual Default",
			Mode: defaultMode1,
		}
		if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
			return err
		}
		lib1ID = lib.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create lib1: %v", err)
	}

	// Change TOBITEN_LIBRARY_MODE to managed and create a second library.
	cfgManaged, err := config.Load(nil, config.WithEnviron([]string{"TOBITEN_LIBRARY_MODE=managed"}), config.WithoutEnvFile())
	if err != nil {
		t.Fatalf("config.Load(managed): %v", err)
	}
	defaultMode2, err := domain.ParseLibraryMode(cfgManaged.LibraryMode)
	if err != nil {
		t.Fatalf("ParseLibraryMode(%q): %v", cfgManaged.LibraryMode, err)
	}
	intake := mustRootPath(t, []byte("/mnt/managed-intake"))

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib := &domain.Library{
			Name:       "Created Under Managed Default",
			Mode:       defaultMode2,
			IntakePath: &intake,
		}
		if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
			return err
		}
		lib2ID = lib.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create lib2: %v", err)
	}

	// Switch config back to virtual and verify both persisted libraries keep their stored modes.
	cfgVirtualAgain, err := config.Load(nil, config.WithEnviron([]string{"TOBITEN_LIBRARY_MODE=virtual"}), config.WithoutEnvFile())
	if err != nil {
		t.Fatalf("config.Load(virtual again): %v", err)
	}
	if cfgVirtualAgain.LibraryMode != "virtual" {
		t.Fatalf("expected reloaded config mode 'virtual', got %q", cfgVirtualAgain.LibraryMode)
	}

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		got1, err := tx.Libraries().GetLibraryByID(ctx, lib1ID)
		if err != nil {
			return err
		}
		if got1.Mode != domain.LibraryModeVirtual || got1.IntakePath != nil {
			t.Fatalf("lib1 mode mutated after config change: %+v", got1)
		}

		got2, err := tx.Libraries().GetLibraryByID(ctx, lib2ID)
		if err != nil {
			return err
		}
		if got2.Mode != domain.LibraryModeManaged || got2.IntakePath == nil {
			t.Fatalf("lib2 mode mutated after config change: %+v", got2)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify persisted modes: %v", err)
	}
}

func TestLibraryRepository_RootStatusDuplicatePathAndFK(t *testing.T) {
	t.Parallel()
	db, transactor := setupLibraryTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var lib1ID, lib2ID, root1ID, root2ID string
	pathA := mustRootPath(t, []byte("/mnt/shared-root"))
	pathB := mustRootPath(t, []byte("/mnt/second-root"))

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()
		l1 := &domain.Library{Name: "Lib A", Mode: domain.LibraryModeVirtual}
		if err := repo.CreateLibrary(ctx, l1); err != nil {
			return err
		}
		lib1ID = l1.ID

		l2 := &domain.Library{Name: "Lib B", Mode: domain.LibraryModeVirtual}
		if err := repo.CreateLibrary(ctx, l2); err != nil {
			return err
		}
		lib2ID = l2.ID

		r1 := &domain.LibraryRoot{LibraryID: lib1ID, Path: pathA}
		if err := repo.CreateLibraryRoot(ctx, r1); err != nil {
			return err
		}
		root1ID = r1.ID

		r2 := &domain.LibraryRoot{LibraryID: lib2ID, Path: pathB}
		if err := repo.CreateLibraryRoot(ctx, r2); err != nil {
			return err
		}
		root2ID = r2.ID
		return nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Duplicate path_bytes maps to storage.ErrAlreadyExists on create and update.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.Libraries().CreateLibraryRoot(ctx, &domain.LibraryRoot{LibraryID: lib2ID, Path: pathA})
	})
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists for duplicate path_bytes on create, got %v", err)
	}

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()
		r2, err := repo.GetLibraryRootByID(ctx, root2ID)
		if err != nil {
			return err
		}
		r2.Path = pathA
		return repo.UpdateLibraryRoot(ctx, r2)
	})
	if !errors.Is(err, storage.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists for duplicate path_bytes on update, got %v", err)
	}

	// Root status updates: OFFLINE with error -> ONLINE clearing error.
	t1 := time.Date(2026, 11, 17, 10, 30, 0, 123456789, time.UTC)
	t2 := t1.Add(5 * time.Minute)
	offlineMsg := "root directory has no files and no .tobiten-root marker"

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		repo := tx.Libraries()

		if err := repo.UpdateLibraryRootStatus(ctx, root1ID, domain.LibraryRootStateOffline, t1, &offlineMsg); err != nil {
			t.Fatalf("UpdateLibraryRootStatus(OFFLINE): %v", err)
		}
		got, err := repo.GetLibraryRootByID(ctx, root1ID)
		if err != nil {
			return err
		}
		if got.State != domain.LibraryRootStateOffline {
			t.Fatalf("expected state OFFLINE, got %q", got.State)
		}
		if got.LastCheckedAt == nil || !got.LastCheckedAt.Equal(t1.Truncate(time.Microsecond)) {
			t.Fatalf("LastCheckedAt = %v, want %v", got.LastCheckedAt, t1.Truncate(time.Microsecond))
		}
		if got.LastError == nil || *got.LastError != offlineMsg {
			t.Fatalf("LastError = %v, want %q", got.LastError, offlineMsg)
		}

		if err := repo.UpdateLibraryRootStatus(ctx, root1ID, domain.LibraryRootStateOnline, t2, nil); err != nil {
			t.Fatalf("UpdateLibraryRootStatus(ONLINE): %v", err)
		}
		recovered, err := repo.GetLibraryRootByID(ctx, root1ID)
		if err != nil {
			return err
		}
		if recovered.State != domain.LibraryRootStateOnline ||
			recovered.LastCheckedAt == nil || !recovered.LastCheckedAt.Equal(t2.Truncate(time.Microsecond)) ||
			recovered.LastError != nil {
			t.Fatalf("expected recovered ONLINE root with cleared LastError, got %+v", recovered)
		}

		if err := repo.UpdateLibraryRootStatus(ctx, domain.MustNewID(), domain.LibraryRootStateOffline, t1, &offlineMsg); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected ErrNotFound for unknown root status update, got %v", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("status update check failed: %v", err)
	}

	// Unknown library_id on root create maps to ErrNotFound.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.Libraries().CreateLibraryRoot(ctx, &domain.LibraryRoot{
			LibraryID: domain.MustNewID(),
			Path:      mustRootPath(t, []byte("/mnt/missing-lib")),
		})
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for root with missing library, got %v", err)
	}

	// Deleting a root that still owns media_assets maps to storage.ErrLibraryRootInUse.
	volID := domain.MustNewID()
	assetID := domain.MustNewID()
	if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ($1, $2)`, volID, lib1ID); err != nil {
		t.Fatalf("insert volume: %v", err)
	}
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
		VALUES ($1, $2, $3, $4, 'book.cbz', 'book.cbz', 100, 'CBZ')`,
		assetID, volID, root1ID, []byte("book.cbz"),
	); err != nil {
		t.Fatalf("insert media_asset: %v", err)
	}

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.Libraries().DeleteLibraryRoot(ctx, root1ID)
	})
	if !errors.Is(err, storage.ErrLibraryRootInUse) {
		t.Fatalf("expected ErrLibraryRootInUse when deleting root with assets, got %v", err)
	}

	// Transaction rollback discards writes cleanly.
	rollbackSentinel := errors.New("abort tx")
	var rolledBackLibID string
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		lib := &domain.Library{Name: "Rolled Back Lib", Mode: domain.LibraryModeVirtual}
		if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
			return err
		}
		rolledBackLibID = lib.ID
		return rollbackSentinel
	})
	if !errors.Is(err, rollbackSentinel) {
		t.Fatalf("expected rollbackSentinel, got %v", err)
	}
	if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryByID(ctx, rolledBackLibID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after transaction rollback, got %v", err)
	}
}

func waitForLockWaiter(ctx context.Context, t *testing.T, db *testdb.TestDB, pidQuery string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := db.QueryRow(ctx, pidQuery).Scan(&waiting); err != nil {
			t.Fatalf("polling lock waiter: %v", err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for PostgreSQL lock contention (%s)", pidQuery)
}

func TestLibraryRepository_DeleteLibraryCatalogueGuardAndConcurrency(t *testing.T) {
	t.Parallel()
	db, transactor := setupLibraryTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Create a user for reading_progress rows.
	userID := domain.MustNewID()
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		return tx.Users().CreateUser(ctx, &domain.User{ID: userID, Username: "reader-delete-guard"})
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	t.Run("missing library returns ErrNotFound", func(t *testing.T) {
		err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.Libraries().DeleteLibrary(ctx, domain.MustNewID())
		})
		if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("DeleteLibrary(missing) = %v, want ErrNotFound", err)
		}
	})

	t.Run("library with empty catalogue and empty roots deletes cleanly", func(t *testing.T) {
		var libID, root1ID, root2ID string
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib := &domain.Library{Name: "Empty With Roots", Mode: domain.LibraryModeVirtual}
			if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
				return err
			}
			libID = lib.ID
			r1 := &domain.LibraryRoot{LibraryID: libID, Path: mustRootPath(t, []byte("/mnt/empty-1"))}
			r2 := &domain.LibraryRoot{LibraryID: libID, Path: mustRootPath(t, []byte("/mnt/empty-2"))}
			if err := tx.Libraries().CreateLibraryRoot(ctx, r1); err != nil {
				return err
			}
			if err := tx.Libraries().CreateLibraryRoot(ctx, r2); err != nil {
				return err
			}
			root1ID, root2ID = r1.ID, r2.ID
			return nil
		}); err != nil {
			t.Fatalf("setup empty library with roots: %v", err)
		}

		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.Libraries().DeleteLibrary(ctx, libID)
		}); err != nil {
			t.Fatalf("DeleteLibrary(empty with roots): %v", err)
		}

		if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryByID(ctx, libID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected deleted library ErrNotFound, got %v", err)
		}
		for _, rid := range []string{root1ID, root2ID} {
			if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryRootByID(ctx, rid); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("expected cascaded empty root %s ErrNotFound, got %v", rid, err)
			}
		}
	})

	t.Run("refuses library with zero-asset standalone volume and reading progress", func(t *testing.T) {
		var libID string
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib := &domain.Library{Name: "Standalone Volume Lib", Mode: domain.LibraryModeVirtual}
			if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
				return err
			}
			libID = lib.ID
			return tx.Libraries().CreateLibraryRoot(ctx, &domain.LibraryRoot{
				LibraryID: libID,
				Path:      mustRootPath(t, []byte("/mnt/standalone-vol")),
			})
		}); err != nil {
			t.Fatalf("setup standalone library: %v", err)
		}

		volID := domain.MustNewID()
		if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ($1, $2)`, volID, libID); err != nil {
			t.Fatalf("insert standalone volume: %v", err)
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
			VALUES ($1, $2, '{"type":"page","page":12}'::jsonb, 0.5, 'REST', FALSE)`,
			userID, volID,
		); err != nil {
			t.Fatalf("insert reading_progress: %v", err)
		}

		err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.Libraries().DeleteLibrary(ctx, libID)
		})
		if !errors.Is(err, storage.ErrLibraryInUse) {
			t.Fatalf("DeleteLibrary(standalone zero-asset volume) = %v, want ErrLibraryInUse", err)
		}
		if errors.Is(err, storage.ErrLibraryRootInUse) {
			t.Fatalf("ErrLibraryInUse must be distinct from ErrLibraryRootInUse")
		}

		if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryByID(ctx, libID); err != nil {
			t.Fatalf("library should remain after rejected delete: %v", err)
		}
		var volCount, progCount int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM volumes WHERE id = $1`, volID).Scan(&volCount); err != nil {
			t.Fatalf("count volume: %v", err)
		}
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM reading_progress WHERE user_id = $1 AND volume_id = $2`, userID, volID).Scan(&progCount); err != nil {
			t.Fatalf("count reading_progress: %v", err)
		}
		if volCount != 1 || progCount != 1 {
			t.Fatalf("expected volume and reading_progress preserved, got vol=%d prog=%d", volCount, progCount)
		}
	})

	t.Run("refuses library with series without volumes", func(t *testing.T) {
		var libID string
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib := &domain.Library{Name: "Empty Series Lib", Mode: domain.LibraryModeVirtual}
			if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
				return err
			}
			libID = lib.ID
			return nil
		}); err != nil {
			t.Fatalf("setup series-only library: %v", err)
		}

		seriesID := domain.MustNewID()
		if _, err := db.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ($1, $2, 'Dorohedoro', 'Dorohedoro')`, seriesID, libID); err != nil {
			t.Fatalf("insert series: %v", err)
		}

		err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			return tx.Libraries().DeleteLibrary(ctx, libID)
		})
		if !errors.Is(err, storage.ErrLibraryInUse) {
			t.Fatalf("DeleteLibrary(series without volumes) = %v, want ErrLibraryInUse", err)
		}

		if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryByID(ctx, libID); err != nil {
			t.Fatalf("library should remain after rejected delete: %v", err)
		}
		var seriesCount int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM series WHERE id = $1`, seriesID).Scan(&seriesCount); err != nil {
			t.Fatalf("count series: %v", err)
		}
		if seriesCount != 1 {
			t.Fatalf("expected series preserved, got count=%d", seriesCount)
		}
	})

	t.Run("concurrent volume and progress insert blocks DeleteLibrary and survives", func(t *testing.T) {
		var libID string
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib := &domain.Library{Name: "Concurrent Insert Wins Lib", Mode: domain.LibraryModeVirtual}
			if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
				return err
			}
			libID = lib.ID
			return nil
		}); err != nil {
			t.Fatalf("setup concurrent insert library: %v", err)
		}

		insertTx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin insertTx: %v", err)
		}
		defer func() { _ = insertTx.Rollback(ctx) }()

		volID := domain.MustNewID()
		if _, err := insertTx.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ($1, $2)`, volID, libID); err != nil {
			t.Fatalf("insertTx insert volume: %v", err)
		}
		if _, err := insertTx.Exec(ctx, `
			INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
			VALUES ($1, $2, '{"type":"page","page":3}'::jsonb, 0.25, 'REST', FALSE)`,
			userID, volID,
		); err != nil {
			t.Fatalf("insertTx insert reading_progress: %v", err)
		}

		deleteDone := make(chan error, 1)
		go func() {
			deleteDone <- transactor.WithTx(ctx, func(tx storage.Tx) error {
				return tx.Libraries().DeleteLibrary(ctx, libID)
			})
		}()

		waitForLockWaiter(ctx, t, db, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%FOR UPDATE%'
			)`)

		if err := insertTx.Commit(ctx); err != nil {
			t.Fatalf("commit insertTx: %v", err)
		}

		delErr := <-deleteDone
		if !errors.Is(delErr, storage.ErrLibraryInUse) {
			t.Fatalf("concurrent DeleteLibrary = %v, want ErrLibraryInUse", delErr)
		}

		if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryByID(ctx, libID); err != nil {
			t.Fatalf("library should remain after concurrent insert commit: %v", err)
		}
		var volCount, progCount int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM volumes WHERE id = $1`, volID).Scan(&volCount); err != nil {
			t.Fatalf("count volume: %v", err)
		}
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM reading_progress WHERE user_id = $1 AND volume_id = $2`, userID, volID).Scan(&progCount); err != nil {
			t.Fatalf("count reading_progress: %v", err)
		}
		if volCount != 1 || progCount != 1 {
			t.Fatalf("expected volume and reading_progress preserved after race, got vol=%d prog=%d", volCount, progCount)
		}
	})

	t.Run("concurrent series insert blocks DeleteLibrary and survives", func(t *testing.T) {
		var libID string
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib := &domain.Library{Name: "Concurrent Series Insert Lib", Mode: domain.LibraryModeVirtual}
			if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
				return err
			}
			libID = lib.ID
			return nil
		}); err != nil {
			t.Fatalf("setup concurrent series insert library: %v", err)
		}

		insertTx, err := db.Begin(ctx)
		if err != nil {
			t.Fatalf("begin insertTx: %v", err)
		}
		defer func() { _ = insertTx.Rollback(ctx) }()

		seriesID := domain.MustNewID()
		if _, err := insertTx.Exec(ctx, `INSERT INTO series (id, library_id, title, title_sort) VALUES ($1, $2, 'Akira', 'Akira')`, seriesID, libID); err != nil {
			t.Fatalf("insertTx insert series: %v", err)
		}

		deleteDone := make(chan error, 1)
		go func() {
			deleteDone <- transactor.WithTx(ctx, func(tx storage.Tx) error {
				return tx.Libraries().DeleteLibrary(ctx, libID)
			})
		}()

		waitForLockWaiter(ctx, t, db, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%FOR UPDATE%'
			)`)

		if err := insertTx.Commit(ctx); err != nil {
			t.Fatalf("commit insertTx: %v", err)
		}

		delErr := <-deleteDone
		if !errors.Is(delErr, storage.ErrLibraryInUse) {
			t.Fatalf("concurrent DeleteLibrary = %v, want ErrLibraryInUse", delErr)
		}
		var seriesCount int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM series WHERE id = $1`, seriesID).Scan(&seriesCount); err != nil {
			t.Fatalf("count series: %v", err)
		}
		if seriesCount != 1 {
			t.Fatalf("expected series preserved after race, got count=%d", seriesCount)
		}
	})

	t.Run("DeleteLibrary lock blocks concurrent insert until commit then insert fails FK", func(t *testing.T) {
		var libID string
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib := &domain.Library{Name: "Concurrent Delete Wins Lib", Mode: domain.LibraryModeVirtual}
			if err := tx.Libraries().CreateLibrary(ctx, lib); err != nil {
				return err
			}
			libID = lib.ID
			return nil
		}); err != nil {
			t.Fatalf("setup concurrent delete-wins library: %v", err)
		}

		deleteLocked := make(chan struct{})
		releaseDelete := make(chan struct{})
		deleteDone := make(chan error, 1)
		go func() {
			deleteDone <- transactor.WithTx(ctx, func(tx storage.Tx) error {
				if err := tx.Libraries().DeleteLibrary(ctx, libID); err != nil {
					return err
				}
				close(deleteLocked)
				<-releaseDelete
				return nil
			})
		}()

		<-deleteLocked

		insertDone := make(chan error, 1)
		go func() {
			_, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ($1, $2)`, domain.MustNewID(), libID)
			insertDone <- err
		}()

		waitForLockWaiter(ctx, t, db, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%INSERT INTO volumes%'
			)`)

		close(releaseDelete)

		if err := <-deleteDone; err != nil {
			t.Fatalf("DeleteLibrary commit failed: %v", err)
		}
		insErr := <-insertDone
		var pgErr *pgconn.PgError
		if !errors.As(insErr, &pgErr) || pgErr.Code != "23503" {
			t.Fatalf("expected FK violation 23503 for concurrent insert after committed delete, got %v", insErr)
		}
	})
}
