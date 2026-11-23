package roots_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/roots"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func setupConfigServiceTestDB(t *testing.T, opts roots.ConfigServiceOptions) (*testdb.TestDB, *postgres.Transactor, *roots.ConfigService, *domain.Principal) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}

	transactor := postgres.NewTransactor(db.Pool)
	svc, err := roots.NewConfigService(transactor, opts)
	if err != nil {
		t.Fatalf("NewConfigService: %v", err)
	}

	var adminRole domain.Role
	for _, r := range domain.BuiltInRoles() {
		if r.ID == domain.RoleAdminID {
			adminRole = r
			break
		}
	}
	admin := domain.NewPrincipal(
		domain.User{ID: "user-admin", Username: "admin"},
		[]domain.Role{adminRole},
		domain.CredentialKindPassword,
		"cred-admin",
		nil,
	)

	return db, transactor, svc, admin
}

type fsSnapshot struct {
	relPath string
	mode    fs.FileMode
	size    int64
	modTime time.Time
	content []byte
	target  string
}

func captureSnapshot(t *testing.T, root string) []fsSnapshot {
	t.Helper()
	var entries []fsSnapshot
	err := filepath.WalkDir(root, func(p string, _ fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		fi, err := os.Lstat(p)
		if err != nil {
			return err
		}
		s := fsSnapshot{
			relPath: rel,
			mode:    fi.Mode(),
			size:    fi.Size(),
			modTime: fi.ModTime(),
		}
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			s.target = target
		case !fi.IsDir():
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			s.content = data
		}
		entries = append(entries, s)
		return nil
	})
	if err != nil {
		t.Fatalf("captureSnapshot(%q): %v", root, err)
	}
	return entries
}

func assertSnapshotIdentical(t *testing.T, root string, before []fsSnapshot) {
	t.Helper()
	after := captureSnapshot(t, root)
	if len(after) != len(before) {
		t.Fatalf("fixture tree entry count changed: before=%d after=%d", len(before), len(after))
	}
	for i := range before {
		b, a := before[i], after[i]
		if b.relPath != a.relPath || b.mode != a.mode || b.size != a.size || !b.modTime.Equal(a.modTime) ||
			b.target != a.target || !bytes.Equal(b.content, a.content) {
			t.Fatalf("fixture tree changed at %q: before=%+v after=%+v", b.relPath, b, a)
		}
	}
}

func makeFixtureTree(t *testing.T) (canonBase, mntDir, dirA, dirASub, dirAB, intakeDir, aliasA, aliasMnt string) {
	t.Helper()
	base := t.TempDir()
	var err error
	canonBase, err = paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath(base): %v", err)
	}

	mntDir = filepath.Join(canonBase, "mnt")
	dirA = filepath.Join(mntDir, "a")
	dirASub = filepath.Join(dirA, "sub")
	dirAB = filepath.Join(mntDir, "ab")
	intakeDir = filepath.Join(mntDir, "intake")
	for _, d := range []string{dirASub, dirAB, intakeDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}
	sampleFile := filepath.Join(dirASub, "vol01.cbz")
	if err := os.WriteFile(sampleFile, []byte("PK\x03\x04comic-bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q): %v", sampleFile, err)
	}
	past := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	for _, p := range []string{sampleFile, dirASub, dirA, dirAB, intakeDir, mntDir} {
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatalf("Chtimes(%q): %v", p, err)
		}
	}

	aliasA = filepath.Join(canonBase, "alias-a")
	if err := os.Symlink(dirA, aliasA); err != nil {
		t.Fatalf("Symlink(aliasA): %v", err)
	}
	aliasMnt = filepath.Join(canonBase, "alias-mnt")
	if err := os.Symlink(mntDir, aliasMnt); err != nil {
		t.Fatalf("Symlink(aliasMnt): %v", err)
	}
	return canonBase, mntDir, dirA, dirASub, dirAB, intakeDir, aliasA, aliasMnt
}

func TestConfigService_CreateLibrary_OverlapModeAndRollback(t *testing.T) {
	db, _, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{
		DefaultMode: domain.LibraryModeVirtual,
	})
	canonBase, mntDir, dirA, dirASub, dirAB, intakeDir, aliasA, aliasMnt := makeFixtureTree(t)
	before := captureSnapshot(t, canonBase)
	t.Chdir(canonBase)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Create a managed library with an intake path and one root using default-less explicit mode.
	created, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:       "Lib One",
		Mode:       domain.LibraryModeManaged,
		IntakePath: []byte(intakeDir),
		Roots:      [][]byte{[]byte(dirA)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Lib One): %v", err)
	}
	if created.Library.Mode != domain.LibraryModeManaged || len(created.Roots) != 1 {
		t.Fatalf("unexpected CreateLibrary result: %+v", created)
	}

	// 2. Reject creating another library whose root or intake overlaps Lib One's root or intake
	// (equal, inside, containing, symlink alias, relative, dot-dot).
	rejectReqs := []struct {
		name string
		req  app.CreateLibraryRequest
	}{
		{
			name: "root equal to existing root",
			req:  app.CreateLibraryRequest{Name: "Conflict 1", Roots: [][]byte{[]byte(dirA)}},
		},
		{
			name: "root inside existing root via symlink",
			req:  app.CreateLibraryRequest{Name: "Conflict 2", Roots: [][]byte{[]byte(filepath.Join(aliasA, "sub"))}},
		},
		{
			name: "root containing existing root via alias",
			req:  app.CreateLibraryRequest{Name: "Conflict 3", Roots: [][]byte{[]byte(aliasMnt)}},
		},
		{
			name: "root relative dotdot into existing root",
			req:  app.CreateLibraryRequest{Name: "Conflict 4", Roots: [][]byte{[]byte("./mnt/ab/../a")}},
		},
		{
			name: "root overlaps existing intake",
			req:  app.CreateLibraryRequest{Name: "Conflict 5", Roots: [][]byte{[]byte(filepath.Join(intakeDir, "sub"))}},
		},
		{
			name: "managed intake overlaps existing root via symlink",
			req: app.CreateLibraryRequest{
				Name:       "Conflict 6",
				Mode:       domain.LibraryModeManaged,
				IntakePath: []byte(aliasA),
				Roots:      [][]byte{[]byte(dirAB)},
			},
		},
		{
			name: "managed intake overlaps existing intake via relative dotdot",
			req: app.CreateLibraryRequest{
				Name:       "Conflict 7",
				Mode:       domain.LibraryModeManaged,
				IntakePath: []byte("mnt/a/../intake"),
				Roots:      [][]byte{[]byte(dirAB)},
			},
		},
		{
			name: "intra-request intake overlaps own root",
			req: app.CreateLibraryRequest{
				Name:       "Conflict 8",
				Mode:       domain.LibraryModeManaged,
				IntakePath: []byte(dirAB),
				Roots:      [][]byte{[]byte(filepath.Join(dirAB, "nested"))},
			},
		},
		{
			name: "intra-request second root overlaps first root and rolls back",
			req: app.CreateLibraryRequest{
				Name:  "Conflict 9",
				Roots: [][]byte{[]byte(dirAB), []byte(filepath.Join(aliasMnt, "ab", "child"))},
			},
		},
	}

	for _, tc := range rejectReqs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CreateLibrary(ctx, admin, tc.req)
			if !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("CreateLibrary(%s) err = %v, want ErrConflict + ErrPathOverlap", tc.name, err)
			}
			// Verify nothing leaked into PostgreSQL on rejection.
			if _, lookupErr := postgres.NewLibraryRepository(db.Pool).GetLibraryByName(ctx, tc.req.Name); !errors.Is(lookupErr, storage.ErrNotFound) {
				t.Fatalf("rejected library %q should not be persisted, got err=%v", tc.req.Name, lookupErr)
			}
		})
	}

	// 3. Name-prefix sibling (/mnt/a vs /mnt/ab) succeeds and uses DefaultMode when Mode is empty.
	sibling, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:  "Lib Sibling",
		Roots: [][]byte{[]byte("./mnt/ab")},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Lib Sibling): %v", err)
	}
	if sibling.Library.Mode != domain.LibraryModeVirtual {
		t.Fatalf("expected DefaultMode VIRTUAL, got %q", sibling.Library.Mode)
	}
	if string(sibling.Roots[0].Path.Bytes) != dirAB {
		t.Fatalf("expected canonical root %q, got %q", dirAB, sibling.Roots[0].Path.Bytes)
	}

	// Verify dirASub is still untouched by failed/succeeded operations.
	_ = mntDir
	_ = dirASub
	assertSnapshotIdentical(t, canonBase, before)
}

func TestConfigService_AddRootAndSetManagedIntakePath(t *testing.T) {
	_, _, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})
	canonBase, mntDir, dirA, dirASub, dirAB, intakeDir, aliasA, aliasMnt := makeFixtureTree(t)
	otherIntake := filepath.Join(canonBase, "other-intake")
	newIntake := filepath.Join(canonBase, "new-intake")
	siblingRoot := filepath.Join(canonBase, "sibling-root")
	for _, d := range []string{otherIntake, newIntake, siblingRoot} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}

	before := captureSnapshot(t, canonBase)
	t.Chdir(canonBase)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create Lib 1 (MANAGED, root=/mnt/a, intake=/mnt/intake) and Lib 2 (VIRTUAL, root=/mnt/ab).
	lib1, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:       "Managed Lib",
		Mode:       domain.LibraryModeManaged,
		IntakePath: []byte(intakeDir),
		Roots:      [][]byte{[]byte(dirA)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Managed Lib): %v", err)
	}
	lib2, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:  "Virtual Lib",
		Mode:  domain.LibraryModeVirtual,
		Roots: [][]byte{[]byte(dirAB)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Virtual Lib): %v", err)
	}
	lib3, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:       "Managed Lib 2",
		Mode:       domain.LibraryModeManaged,
		IntakePath: []byte(otherIntake),
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Managed Lib 2): %v", err)
	}

	// AddRoot rejections: same library root, same library intake, another library root, another library intake,
	// via direct, symlink, relative, or dot-dot spellings.
	addRootRejects := [][]byte{
		[]byte(dirA),
		[]byte(dirASub),
		[]byte(mntDir),
		[]byte(aliasA),
		[]byte(filepath.Join(aliasMnt, "a", "sub")),
		[]byte("./mnt/intake"),
		[]byte("mnt/a/../intake/sub"),
		[]byte(dirAB),
		[]byte(filepath.Join(otherIntake, "nested")),
	}
	for _, rawPath := range addRootRejects {
		if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
			LibraryID: lib1.Library.ID,
			Path:      rawPath,
		}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
			t.Fatalf("AddRoot(%q) err = %v, want ErrConflict + ErrPathOverlap", rawPath, err)
		}
	}

	// AddRoot on missing library returns ErrNotFound.
	if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: domain.MustNewID(),
		Path:      []byte(siblingRoot),
	}); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("AddRoot(missing library) err = %v, want ErrNotFound", err)
	}

	// AddRoot accepted for non-overlapping sibling.
	addedRoot, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: lib1.Library.ID,
		Path:      []byte("./sibling-root"),
	})
	if err != nil {
		t.Fatalf("AddRoot(sibling-root): %v", err)
	}
	if string(addedRoot.Path.Bytes) != siblingRoot {
		t.Fatalf("addedRoot.Path.Bytes = %q, want %q", addedRoot.Path.Bytes, siblingRoot)
	}

	// SetManagedIntakePath on VIRTUAL library is rejected.
	if _, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
		LibraryID:  lib2.Library.ID,
		IntakePath: []byte(newIntake),
	}); !errors.Is(err, app.ErrInvalidInput) || !errors.Is(err, domain.ErrVirtualLibraryWithIntake) {
		t.Fatalf("SetManagedIntakePath(virtual) err = %v, want ErrInvalidInput + ErrVirtualLibraryWithIntake", err)
	}

	// SetManagedIntakePath rejections: own roots, other library roots, other library intake.
	intakeRejects := [][]byte{
		[]byte(dirA),
		[]byte(dirASub),
		[]byte(mntDir),
		[]byte(aliasA),
		[]byte("./sibling-root"),
		[]byte("mnt/a/../ab"),
		[]byte(otherIntake),
		[]byte(filepath.Join(otherIntake, "drop")),
		[]byte(canonBase),
	}
	for _, rawIntake := range intakeRejects {
		if _, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
			LibraryID:  lib1.Library.ID,
			IntakePath: rawIntake,
		}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
			t.Fatalf("SetManagedIntakePath(%q) err = %v, want ErrConflict + ErrPathOverlap", rawIntake, err)
		}
	}

	// SetManagedIntakePath accepted when updating to a non-overlapping path (and allows replacing own old intake).
	updated, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
		LibraryID:  lib1.Library.ID,
		IntakePath: []byte("./new-intake"),
	})
	if err != nil {
		t.Fatalf("SetManagedIntakePath(new-intake): %v", err)
	}
	if updated.IntakePath == nil || string(updated.IntakePath.Bytes) != newIntake {
		t.Fatalf("updated.IntakePath = %+v, want %q", updated.IntakePath, newIntake)
	}

	// And replacing own intake with a subdirectory of its previous intake works once old intake is replaced.
	subOfCurrent := filepath.Join(newIntake, "sub")
	updated2, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
		LibraryID:  lib1.Library.ID,
		IntakePath: []byte(subOfCurrent),
	})
	if err != nil {
		t.Fatalf("SetManagedIntakePath(subOfCurrent): %v", err)
	}
	if updated2.IntakePath == nil || string(updated2.IntakePath.Bytes) != subOfCurrent {
		t.Fatalf("updated2.IntakePath = %+v, want %q", updated2.IntakePath, subOfCurrent)
	}

	// Also verify lib3 cannot take Lib 1's new intake.
	if _, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
		LibraryID:  lib3.Library.ID,
		IntakePath: []byte(newIntake),
	}); !errors.Is(err, roots.ErrPathOverlap) {
		t.Fatalf("expected lib3 intake overlapping lib1 intake to fail, got %v", err)
	}

	assertSnapshotIdentical(t, canonBase, before)
}

func TestConfigService_ConcurrentConflictingRequests(t *testing.T) {
	t.Parallel()
	db, transactor, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("deterministic advisory lock blocks conflicting request until first commits then rejects", func(t *testing.T) {
		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath(base): %v", err)
		}
		realDir := filepath.Join(canonBase, "shared")
		if err := os.MkdirAll(filepath.Join(realDir, "sub"), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		aliasDir := filepath.Join(canonBase, "alias-shared")
		if err := os.Symlink(realDir, aliasDir); err != nil {
			t.Fatalf("Symlink: %v", err)
		}

		subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
		defer subCancel()

		// Simulate Request 1 holding the configuration advisory lock and creating realDir before commit.
		tx1Locked := make(chan struct{})
		releaseTx1 := make(chan struct{})
		var releaseOnce sync.Once
		closeRelease := func() {
			releaseOnce.Do(func() { close(releaseTx1) })
		}

		var tx1Err error
		tx1Done := make(chan struct{})
		go func() {
			defer close(tx1Done)
			tx1Err = transactor.WithTx(subCtx, func(tx storage.Tx) error {
				repo := tx.Libraries()
				if err := repo.LockConfiguration(subCtx); err != nil {
					return err
				}
				close(tx1Locked)
				select {
				case <-releaseTx1:
					if err := subCtx.Err(); err != nil {
						return err
					}
				case <-subCtx.Done():
					return subCtx.Err()
				}
				pathID, err := domain.NewRootPathIdentity([]byte(realDir))
				if err != nil {
					return err
				}
				lib := &domain.Library{Name: "Lock Winner", Mode: domain.LibraryModeVirtual}
				if err := repo.CreateLibrary(subCtx, lib); err != nil {
					return err
				}
				return repo.CreateLibraryRoot(subCtx, &domain.LibraryRoot{
					LibraryID: lib.ID,
					Path:      pathID,
				})
			})
		}()

		var req2Err error
		req2Done := make(chan struct{})
		req2Started := false
		defer func() {
			subCancel()
			closeRelease()
			<-tx1Done
			if req2Started {
				<-req2Done
			}
		}()

		select {
		case <-tx1Locked:
		case <-tx1Done:
			t.Fatalf("tx1 exited before acquiring lock: %v", tx1Err)
		case <-subCtx.Done():
			t.Fatalf("timed out waiting for tx1 lock: %v", subCtx.Err())
		}

		req2Started = true
		go func() {
			defer close(req2Done)
			_, req2Err = svc.CreateLibrary(subCtx, admin, app.CreateLibraryRequest{
				Name:  "Lock Loser",
				Mode:  domain.LibraryModeVirtual,
				Roots: [][]byte{[]byte(filepath.Join(aliasDir, "sub"))},
			})
		}()

		// Wait until Request 2 is blocked on PostgreSQL's advisory lock.
		waitDeadline := time.Now().Add(5 * time.Second)
		for {
			select {
			case <-req2Done:
				t.Fatalf("Request 2 exited before blocking on advisory lock: %v", req2Err)
			default:
			}
			var waiting bool
			if err := db.QueryRow(subCtx, `
				SELECT EXISTS (
					SELECT 1
					FROM pg_stat_activity
					WHERE datname = current_database()
					  AND pid <> pg_backend_pid()
					  AND state = 'active'
					  AND wait_event_type = 'Lock'
					  AND wait_event = 'advisory'
				)`).Scan(&waiting); err != nil {
				t.Fatalf("polling advisory lock waiter: %v", err)
			}
			if waiting {
				break
			}
			if time.Now().After(waitDeadline) {
				t.Fatal("timed out waiting for Request 2 to block on advisory lock")
			}
			time.Sleep(10 * time.Millisecond)
		}

		closeRelease()

		<-tx1Done
		if tx1Err != nil {
			t.Fatalf("tx1 commit failed: %v", tx1Err)
		}
		<-req2Done
		if !errors.Is(req2Err, app.ErrConflict) || !errors.Is(req2Err, roots.ErrPathOverlap) {
			t.Fatalf("Request 2 err = %v, want ErrConflict + ErrPathOverlap", req2Err)
		}

		if _, err := postgres.NewLibraryRepository(db.Pool).GetLibraryByName(subCtx, "Lock Loser"); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected Lock Loser not to be committed, got %v", err)
		}
	})

	assertOneWinner := func(t *testing.T, label string, errA, errB error) {
		t.Helper()
		wins := 0
		var loserErr error
		for _, err := range []error{errA, errB} {
			if err == nil {
				wins++
			} else {
				loserErr = err
			}
		}
		if wins != 1 {
			t.Fatalf("%s: expected exactly 1 commit and 1 conflict, got errA=%v errB=%v", label, errA, errB)
		}
		if !errors.Is(loserErr, app.ErrConflict) || !errors.Is(loserErr, roots.ErrPathOverlap) {
			t.Fatalf("%s: expected loser ErrConflict + ErrPathOverlap, got %v", label, loserErr)
		}
	}

	runBarrierRace := func(fnA, fnB func() error) (error, error) {
		start := make(chan struct{})
		var ready sync.WaitGroup
		ready.Add(2)

		var errA, errB error
		var done sync.WaitGroup
		done.Add(2)

		go func() {
			defer done.Done()
			ready.Done()
			<-start
			errA = fnA()
		}()
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			errB = fnB()
		}()

		ready.Wait()
		close(start)
		done.Wait()
		return errA, errB
	}

	t.Run("concurrent CreateLibrary with nested symlink overlap", func(t *testing.T) {
		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath(base): %v", err)
		}
		realDir := filepath.Join(canonBase, "race-root")
		if err := os.MkdirAll(filepath.Join(realDir, "nested"), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		aliasDir := filepath.Join(canonBase, "race-alias")
		if err := os.Symlink(realDir, aliasDir); err != nil {
			t.Fatalf("Symlink: %v", err)
		}

		errA, errB := runBarrierRace(
			func() error {
				_, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
					Name:  "Race Create A",
					Roots: [][]byte{[]byte(realDir)},
				})
				return err
			},
			func() error {
				_, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
					Name:  "Race Create B",
					Roots: [][]byte{[]byte(filepath.Join(aliasDir, "nested"))},
				})
				return err
			},
		)
		assertOneWinner(t, "CreateLibrary race", errA, errB)
	})

	t.Run("concurrent AddRoot parent vs child on same library", func(t *testing.T) {
		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath(base): %v", err)
		}
		parentDir := filepath.Join(canonBase, "add-parent")
		childDir := filepath.Join(parentDir, "child")
		if err := os.MkdirAll(childDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}

		lib, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{Name: "Race AddRoot Lib"})
		if err != nil {
			t.Fatalf("CreateLibrary: %v", err)
		}

		errA, errB := runBarrierRace(
			func() error {
				_, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
					LibraryID: lib.Library.ID,
					Path:      []byte(parentDir),
				})
				return err
			},
			func() error {
				_, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
					LibraryID: lib.Library.ID,
					Path:      []byte(childDir),
				})
				return err
			},
		)
		assertOneWinner(t, "AddRoot race", errA, errB)

		storedRoots, err := postgres.NewLibraryRepository(db.Pool).ListLibraryRoots(ctx, lib.Library.ID)
		if err != nil {
			t.Fatalf("ListLibraryRoots: %v", err)
		}
		if len(storedRoots) != 1 {
			t.Fatalf("expected 1 committed root after AddRoot race, got %d", len(storedRoots))
		}
	})

	t.Run("concurrent AddRoot vs SetManagedIntakePath on overlapping path", func(t *testing.T) {
		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath(base): %v", err)
		}
		initIntake := filepath.Join(canonBase, "init-intake")
		contested := filepath.Join(canonBase, "contested")
		if err := os.MkdirAll(initIntake, 0o755); err != nil {
			t.Fatalf("MkdirAll(initIntake): %v", err)
		}
		if err := os.MkdirAll(filepath.Join(contested, "sub"), 0o755); err != nil {
			t.Fatalf("MkdirAll(contested): %v", err)
		}

		managedLib, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
			Name:       "Race Intake vs Root",
			Mode:       domain.LibraryModeManaged,
			IntakePath: []byte(initIntake),
		})
		if err != nil {
			t.Fatalf("CreateLibrary: %v", err)
		}

		errA, errB := runBarrierRace(
			func() error {
				_, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
					LibraryID: managedLib.Library.ID,
					Path:      []byte(contested),
				})
				return err
			},
			func() error {
				_, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
					LibraryID:  managedLib.Library.ID,
					IntakePath: []byte(filepath.Join(contested, "sub")),
				})
				return err
			},
		)
		assertOneWinner(t, "AddRoot vs SetManagedIntakePath race", errA, errB)
	})

	t.Run("concurrent SetManagedIntakePath vs SetManagedIntakePath on overlapping target", func(t *testing.T) {
		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath(base): %v", err)
		}
		in1 := filepath.Join(canonBase, "in-1")
		in2 := filepath.Join(canonBase, "in-2")
		target := filepath.Join(canonBase, "shared-target")
		for _, d := range []string{in1, in2, target} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatalf("MkdirAll(%q): %v", d, err)
			}
		}

		lib1, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
			Name:       "Race Intake 1",
			Mode:       domain.LibraryModeManaged,
			IntakePath: []byte(in1),
		})
		if err != nil {
			t.Fatalf("CreateLibrary(1): %v", err)
		}
		lib2, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
			Name:       "Race Intake 2",
			Mode:       domain.LibraryModeManaged,
			IntakePath: []byte(in2),
		})
		if err != nil {
			t.Fatalf("CreateLibrary(2): %v", err)
		}

		errA, errB := runBarrierRace(
			func() error {
				_, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
					LibraryID:  lib1.Library.ID,
					IntakePath: []byte(target),
				})
				return err
			},
			func() error {
				_, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
					LibraryID:  lib2.Library.ID,
					IntakePath: []byte(target),
				})
				return err
			},
		)
		assertOneWinner(t, "SetManagedIntakePath race", errA, errB)
	})

	t.Run("concurrent non-conflicting sibling prefix requests both commit", func(t *testing.T) {
		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath(base): %v", err)
		}
		pathA := filepath.Join(canonBase, "a")
		pathAB := filepath.Join(canonBase, "ab")
		for _, d := range []string{pathA, pathAB} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatalf("MkdirAll(%q): %v", d, err)
			}
		}

		errA, errB := runBarrierRace(
			func() error {
				_, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
					Name:  fmt.Sprintf("Sibling A %d", time.Now().UnixNano()),
					Roots: [][]byte{[]byte(pathA)},
				})
				return err
			},
			func() error {
				_, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
					Name:  fmt.Sprintf("Sibling AB %d", time.Now().UnixNano()),
					Roots: [][]byte{[]byte(pathAB)},
				})
				return err
			},
		)
		if errA != nil || errB != nil {
			t.Fatalf("expected both sibling prefix creates to commit, got errA=%v errB=%v", errA, errB)
		}
	})
}

func pollAdvisoryLockWaiter(ctx context.Context, db *testdb.TestDB, exited <-chan struct{}) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-exited:
			return errors.New("waiting operation exited before hitting advisory lock")
		default:
		}
		var waiting bool
		if err := db.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND state = 'active'
				  AND wait_event_type = 'Lock'
				  AND wait_event = 'advisory'
			)`).Scan(&waiting); err != nil {
			return fmt.Errorf("polling advisory lock waiter: %w", err)
		}
		if waiting {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for PostgreSQL advisory lock contention")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConfigService_IsolationContract(t *testing.T) {
	t.Parallel()
	db, transactor, _, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("pool-backed service reads fresh snapshot after lock even under repeatable-read session default", func(t *testing.T) {
		poolCfg, err := pgxpool.ParseConfig(db.URL)
		if err != nil {
			t.Fatalf("ParseConfig: %v", err)
		}
		poolCfg.MaxConns = 4
		poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, execErr := conn.Exec(ctx, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
			return execErr
		}
		rrPool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			t.Fatalf("pgxpool.NewWithConfig: %v", err)
		}
		defer rrPool.Close()

		rrSvc, err := roots.NewConfigService(postgres.NewTransactor(rrPool), roots.ConfigServiceOptions{})
		if err != nil {
			t.Fatalf("NewConfigService(rrPool): %v", err)
		}

		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath: %v", err)
		}
		parentDir := filepath.Join(canonBase, "rr-parent")
		childDir := filepath.Join(parentDir, "child")
		if err := os.MkdirAll(childDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}

		subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
		defer subCancel()

		tx1Locked := make(chan struct{})
		releaseTx1 := make(chan struct{})
		var releaseOnce sync.Once
		closeRelease := func() { releaseOnce.Do(func() { close(releaseTx1) }) }

		var tx1Err error
		tx1Done := make(chan struct{})
		go func() {
			defer close(tx1Done)
			tx1Err = transactor.WithReadCommittedTx(subCtx, func(tx storage.Tx) error {
				repo := tx.Libraries()
				if err := repo.LockConfiguration(subCtx); err != nil {
					return err
				}
				close(tx1Locked)
				select {
				case <-releaseTx1:
					if err := subCtx.Err(); err != nil {
						return err
					}
				case <-subCtx.Done():
					return subCtx.Err()
				}
				pathID, err := domain.NewRootPathIdentity([]byte(parentDir))
				if err != nil {
					return err
				}
				lib := &domain.Library{Name: "RR Pool Winner", Mode: domain.LibraryModeVirtual}
				if err := repo.CreateLibrary(subCtx, lib); err != nil {
					return err
				}
				return repo.CreateLibraryRoot(subCtx, &domain.LibraryRoot{
					LibraryID: lib.ID,
					Path:      pathID,
				})
			})
		}()

		var req2Err error
		req2Done := make(chan struct{})
		req2Started := false
		defer func() {
			subCancel()
			closeRelease()
			<-tx1Done
			if req2Started {
				<-req2Done
			}
		}()

		select {
		case <-tx1Locked:
		case <-tx1Done:
			t.Fatalf("tx1 exited before lock: %v", tx1Err)
		case <-subCtx.Done():
			t.Fatalf("timed out waiting for tx1 lock: %v", subCtx.Err())
		}

		req2Started = true
		go func() {
			defer close(req2Done)
			_, req2Err = rrSvc.CreateLibrary(subCtx, admin, app.CreateLibraryRequest{
				Name:  "RR Pool Loser",
				Roots: [][]byte{[]byte(childDir)},
			})
		}()

		if err := pollAdvisoryLockWaiter(subCtx, db, req2Done); err != nil {
			subCancel()
			closeRelease()
			<-tx1Done
			<-req2Done
			t.Fatalf("waiting for rrSvc lock: %v (tx1Err=%v req2Err=%v)", err, tx1Err, req2Err)
		}
		closeRelease()

		<-tx1Done
		if tx1Err != nil {
			t.Fatalf("tx1 commit failed: %v", tx1Err)
		}
		<-req2Done
		if !errors.Is(req2Err, app.ErrConflict) || !errors.Is(req2Err, roots.ErrPathOverlap) {
			t.Fatalf("rrSvc CreateLibrary err = %v, want ErrConflict + ErrPathOverlap", req2Err)
		}

		repo := postgres.NewLibraryRepository(db.Pool)
		libs, err := repo.ListLibraries(subCtx)
		if err != nil {
			t.Fatalf("ListLibraries: %v", err)
		}
		if len(libs) != 1 || libs[0].Name != "RR Pool Winner" {
			t.Fatalf("persisted libraries = %+v, want only RR Pool Winner", libs)
		}
		storedRoots, err := repo.ListAllLibraryRoots(subCtx)
		if err != nil {
			t.Fatalf("ListAllLibraryRoots: %v", err)
		}
		if len(storedRoots) != 1 || string(storedRoots[0].Path.Bytes) != parentDir {
			t.Fatalf("persisted roots = %+v, want only %q", storedRoots, parentDir)
		}
	})

	t.Run("caller-owned repeatable-read and serializable transactions fail closed", func(t *testing.T) {
		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath: %v", err)
		}
		dirOK := filepath.Join(canonBase, "ok-root")
		dirIn := filepath.Join(canonBase, "ok-intake")
		for _, d := range []string{dirOK, dirIn} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatalf("MkdirAll(%q): %v", d, err)
			}
		}

		// Seed a managed library so AddRoot and SetManagedIntakePath can also be probed.
		baseSvc, err := roots.NewConfigService(transactor, roots.ConfigServiceOptions{})
		if err != nil {
			t.Fatalf("NewConfigService: %v", err)
		}
		seeded, err := baseSvc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
			Name:       "Caller Iso Seed",
			Mode:       domain.LibraryModeManaged,
			IntakePath: []byte(dirIn),
		})
		if err != nil {
			t.Fatalf("CreateLibrary(seed): %v", err)
		}

		for _, iso := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
			callerTx, err := db.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: iso})
			if err != nil {
				t.Fatalf("BeginTx(%s): %v", iso, err)
			}
			txSvc, err := roots.NewConfigService(postgres.NewTransactor(callerTx), roots.ConfigServiceOptions{})
			if err != nil {
				_ = callerTx.Rollback(ctx)
				t.Fatalf("NewConfigService(callerTx): %v", err)
			}

			if _, err := txSvc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
				Name:  "Should Fail " + string(iso),
				Roots: [][]byte{[]byte(dirOK)},
			}); !errors.Is(err, storage.ErrTransactionIsolation) {
				_ = callerTx.Rollback(ctx)
				t.Fatalf("CreateLibrary(%s) = %v, want ErrTransactionIsolation", iso, err)
			}
			if _, err := txSvc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
				LibraryID: seeded.Library.ID,
				Path:      []byte(dirOK),
			}); !errors.Is(err, storage.ErrTransactionIsolation) {
				_ = callerTx.Rollback(ctx)
				t.Fatalf("AddRoot(%s) = %v, want ErrTransactionIsolation", iso, err)
			}
			if _, err := txSvc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
				LibraryID:  seeded.Library.ID,
				IntakePath: []byte(filepath.Join(canonBase, "new-in")),
			}); !errors.Is(err, storage.ErrTransactionIsolation) {
				_ = callerTx.Rollback(ctx)
				t.Fatalf("SetManagedIntakePath(%s) = %v, want ErrTransactionIsolation", iso, err)
			}
			_ = callerTx.Rollback(ctx)
		}

		storedRoots, err := postgres.NewLibraryRepository(db.Pool).ListAllLibraryRoots(ctx)
		if err != nil {
			t.Fatalf("ListAllLibraryRoots: %v", err)
		}
		for _, r := range storedRoots {
			if string(r.Path.Bytes) == dirOK {
				t.Fatalf("unexpected root persisted from failing isolation tx: %+v", r)
			}
		}
	})

	t.Run("caller-owned read-committed transaction sees fresh snapshot after advisory lock even after prior read", func(t *testing.T) {
		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath: %v", err)
		}
		parentDir := filepath.Join(canonBase, "rc-parent")
		childDir := filepath.Join(parentDir, "nested")
		if err := os.MkdirAll(childDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}

		subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
		defer subCancel()

		// Caller begins a READ COMMITTED transaction and executes a read BEFORE tx1 commits.
		callerTx, err := db.Pool.BeginTx(subCtx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Fatalf("BeginTx(ReadCommitted): %v", err)
		}
		defer func() { _ = callerTx.Rollback(context.Background()) }()
		var initialCount int
		if err := callerTx.QueryRow(subCtx, "SELECT COUNT(*) FROM library_roots").Scan(&initialCount); err != nil {
			t.Fatalf("initial query on callerTx: %v", err)
		}
		callerSvc, err := roots.NewConfigService(postgres.NewTransactor(callerTx), roots.ConfigServiceOptions{})
		if err != nil {
			t.Fatalf("NewConfigService(callerTx): %v", err)
		}

		tx1Locked := make(chan struct{})
		releaseTx1 := make(chan struct{})
		var releaseOnce sync.Once
		closeRelease := func() { releaseOnce.Do(func() { close(releaseTx1) }) }

		var tx1Err error
		tx1Done := make(chan struct{})
		go func() {
			defer close(tx1Done)
			tx1Err = transactor.WithReadCommittedTx(subCtx, func(tx storage.Tx) error {
				repo := tx.Libraries()
				if err := repo.LockConfiguration(subCtx); err != nil {
					return err
				}
				close(tx1Locked)
				select {
				case <-releaseTx1:
					if err := subCtx.Err(); err != nil {
						return err
					}
				case <-subCtx.Done():
					return subCtx.Err()
				}
				pathID, err := domain.NewRootPathIdentity([]byte(parentDir))
				if err != nil {
					return err
				}
				lib := &domain.Library{Name: "RC Caller Winner", Mode: domain.LibraryModeVirtual}
				if err := repo.CreateLibrary(subCtx, lib); err != nil {
					return err
				}
				return repo.CreateLibraryRoot(subCtx, &domain.LibraryRoot{
					LibraryID: lib.ID,
					Path:      pathID,
				})
			})
		}()

		var callerErr error
		callerDone := make(chan struct{})
		callerStarted := false
		defer func() {
			subCancel()
			closeRelease()
			<-tx1Done
			if callerStarted {
				<-callerDone
			}
		}()

		select {
		case <-tx1Locked:
		case <-tx1Done:
			t.Fatalf("tx1 exited before lock: %v", tx1Err)
		case <-subCtx.Done():
			t.Fatalf("timed out waiting for tx1 lock: %v", subCtx.Err())
		}

		callerStarted = true
		go func() {
			defer close(callerDone)
			_, callerErr = callerSvc.CreateLibrary(subCtx, admin, app.CreateLibraryRequest{
				Name:  "RC Caller Loser",
				Roots: [][]byte{[]byte(childDir)},
			})
		}()

		if err := pollAdvisoryLockWaiter(subCtx, db, callerDone); err != nil {
			subCancel()
			closeRelease()
			<-tx1Done
			<-callerDone
			t.Fatalf("waiting for callerSvc lock: %v (tx1Err=%v callerErr=%v)", err, tx1Err, callerErr)
		}
		closeRelease()

		<-tx1Done
		if tx1Err != nil {
			t.Fatalf("tx1 commit failed: %v", tx1Err)
		}
		<-callerDone
		if !errors.Is(callerErr, app.ErrConflict) || !errors.Is(callerErr, roots.ErrPathOverlap) {
			t.Fatalf("callerSvc CreateLibrary err = %v, want ErrConflict + ErrPathOverlap", callerErr)
		}
		_ = callerTx.Rollback(subCtx)

		repo := postgres.NewLibraryRepository(db.Pool)
		if _, err := repo.GetLibraryByName(subCtx, "RC Caller Loser"); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected RC Caller Loser absent, got err=%v", err)
		}
		winner, err := repo.GetLibraryByName(subCtx, "RC Caller Winner")
		if err != nil {
			t.Fatalf("GetLibraryByName(RC Caller Winner): %v", err)
		}
		winnerRoots, err := repo.ListLibraryRoots(subCtx, winner.ID)
		if err != nil {
			t.Fatalf("ListLibraryRoots(winner): %v", err)
		}
		if len(winnerRoots) != 1 || string(winnerRoots[0].Path.Bytes) != parentDir {
			t.Fatalf("winnerRoots = %+v, want only %q", winnerRoots, parentDir)
		}
	})
}

func TestConfigService_CaseAndNormalizationAliases(t *testing.T) {
	t.Parallel()
	db, _, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath(base): %v", err)
	}

	dirUpper := filepath.Join(canonBase, "Manga")
	dirUpperSub := filepath.Join(dirUpper, "existing-sub")
	dirNFD := filepath.Join(canonBase, "Cafe\u0301")
	dirNFDSub := filepath.Join(dirNFD, "existing-sub")
	childTarget := filepath.Join(canonBase, "AncestorRoot", "Nested")
	for _, d := range []string{dirUpperSub, dirNFDSub, childTarget} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}

	dirLower := filepath.Join(canonBase, "manga")
	dirNFC := filepath.Join(canonBase, "Caf\u00e9")

	upperInfo, err := os.Lstat(dirUpper)
	if err != nil {
		t.Fatalf("Lstat(%q): %v", dirUpper, err)
	}
	lowerInfo, lowerErr := os.Lstat(dirLower)
	caseAliased := lowerErr == nil && os.SameFile(upperInfo, lowerInfo)

	nfdInfo, err := os.Lstat(dirNFD)
	if err != nil {
		t.Fatalf("Lstat(%q): %v", dirNFD, err)
	}
	nfcInfo, nfcErr := os.Lstat(dirNFC)
	normAliased := nfcErr == nil && os.SameFile(nfdInfo, nfcInfo)

	if !caseAliased {
		if err := os.MkdirAll(filepath.Join(dirLower, "existing-sub"), 0o755); err != nil {
			t.Fatalf("MkdirAll(distinct lowercase): %v", err)
		}
	}
	if !normAliased {
		if err := os.MkdirAll(filepath.Join(dirNFC, "existing-sub"), 0o755); err != nil {
			t.Fatalf("MkdirAll(distinct NFC): %v", err)
		}
	}

	before := captureSnapshot(t, canonBase)

	libCase, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:       "Alias Case Lib",
		Mode:       domain.LibraryModeManaged,
		IntakePath: []byte(dirNFD),
		Roots:      [][]byte{[]byte(dirUpper)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Alias Case Lib): %v", err)
	}
	if string(libCase.Roots[0].Path.Bytes) != dirUpper {
		t.Fatalf("stored root bytes = %q, want %q", libCase.Roots[0].Path.Bytes, dirUpper)
	}
	if libCase.Library.IntakePath == nil || string(libCase.Library.IntakePath.Bytes) != dirNFD {
		t.Fatalf("stored intake bytes = %+v, want %q", libCase.Library.IntakePath, dirNFD)
	}

	if caseAliased {
		for _, aliasPath := range [][]byte{
			[]byte(dirLower),
			[]byte(filepath.Join(dirLower, "existing-sub")),
			[]byte(filepath.Join(dirLower, "future-sub")),
		} {
			if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
				LibraryID: libCase.Library.ID,
				Path:      aliasPath,
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("AddRoot(case alias %q) = %v, want ErrConflict + ErrPathOverlap", aliasPath, err)
			}
			if _, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
				LibraryID:  libCase.Library.ID,
				IntakePath: aliasPath,
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("SetManagedIntakePath(case alias %q) = %v, want ErrConflict + ErrPathOverlap", aliasPath, err)
			}
		}
	} else {
		distinctLower, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
			Name:  "Distinct Lower Lib",
			Roots: [][]byte{[]byte(dirLower)},
		})
		if err != nil {
			t.Fatalf("CreateLibrary(distinct lower on sensitive fs): %v", err)
		}
		if string(distinctLower.Roots[0].Path.Bytes) != dirLower {
			t.Fatalf("distinct lower bytes = %q, want %q", distinctLower.Roots[0].Path.Bytes, dirLower)
		}
	}

	if normAliased {
		for _, aliasPath := range [][]byte{
			[]byte(dirNFC),
			[]byte(filepath.Join(dirNFC, "existing-sub")),
			[]byte(filepath.Join(dirNFC, "future-sub")),
		} {
			if _, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
				Name:  fmt.Sprintf("Norm Conflict %s", aliasPath),
				Roots: [][]byte{aliasPath},
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("CreateLibrary(norm alias %q) = %v, want ErrConflict + ErrPathOverlap", aliasPath, err)
			}
		}
	} else {
		distinctNFC, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
			Name:  "Distinct NFC Lib",
			Roots: [][]byte{[]byte(dirNFC)},
		})
		if err != nil {
			t.Fatalf("CreateLibrary(distinct NFC on sensitive fs): %v", err)
		}
		if string(distinctNFC.Roots[0].Path.Bytes) != dirNFC {
			t.Fatalf("distinct NFC bytes = %q, want %q", distinctNFC.Roots[0].Path.Bytes, dirNFC)
		}
	}

	// Verify ancestor case/norm alias is also rejected when child directory is the stored root.
	childLib, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:  "Child Root Lib",
		Roots: [][]byte{[]byte(childTarget)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Child Root Lib): %v", err)
	}
	if caseAliased {
		if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
			LibraryID: childLib.Library.ID,
			Path:      []byte(filepath.Join(canonBase, "ancestorroot")),
		}); !errors.Is(err, roots.ErrPathOverlap) {
			t.Fatalf("expected ancestor case alias to fail with ErrPathOverlap, got %v", err)
		}
	}

	storedLibs, err := postgres.NewLibraryRepository(db.Pool).ListLibraries(ctx)
	if err != nil {
		t.Fatalf("ListLibraries: %v", err)
	}
	wantLibs := 2
	if !caseAliased {
		wantLibs++
	}
	if !normAliased {
		wantLibs++
	}
	if len(storedLibs) != wantLibs {
		t.Fatalf("storedLibs count = %d, want %d (%+v)", len(storedLibs), wantLibs, storedLibs)
	}

	assertSnapshotIdentical(t, canonBase, before)
}

func TestConfigService_MissingDirectoryAliases(t *testing.T) {
	t.Parallel()
	db, _, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath(base): %v", err)
	}
	before := captureSnapshot(t, canonBase)

	missingUpper := filepath.Join(canonBase, "Future")
	missingLower := filepath.Join(canonBase, "future")
	missingLowerChild := filepath.Join(missingLower, "child")
	missingNFD := filepath.Join(canonBase, "MissingCafe\u0301")
	missingNFC := filepath.Join(canonBase, "MissingCaf\u00e9")
	missingNFCChild := filepath.Join(missingNFC, "child")

	// Probe whether the mounted filesystem folds case or NFC/NFD by checking an already-existing
	// probe pair in an isolated sibling directory, or testing via CheckPathOverlap-free probe?
	// Wait: without write probes in canonBase, create an isolated probe dir outside canonBase to
	// assert whether the host OS mount aliases case/NFC-NFD, while keeping canonBase untouched.
	probeBase := t.TempDir()
	probeUpper := filepath.Join(probeBase, "Probe")
	probeNFD := filepath.Join(probeBase, "ProbeCafe\u0301")
	for _, d := range []string{probeUpper, probeNFD} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}
	uInfo, err := os.Lstat(probeUpper)
	if err != nil {
		t.Fatalf("Lstat(probeUpper): %v", err)
	}
	lInfo, lErr := os.Lstat(filepath.Join(probeBase, "probe"))
	caseAliased := lErr == nil && os.SameFile(uInfo, lInfo)

	nfdInfo, err := os.Lstat(probeNFD)
	if err != nil {
		t.Fatalf("Lstat(probeNFD): %v", err)
	}
	nfcInfo, nfcErr := os.Lstat(filepath.Join(probeBase, "ProbeCaf\u00e9"))
	normAliased := nfcErr == nil && os.SameFile(nfdInfo, nfcInfo)

	created, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:       "Missing Alias Lib",
		Mode:       domain.LibraryModeManaged,
		IntakePath: []byte(missingNFD),
		Roots:      [][]byte{[]byte(missingUpper)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Missing Alias Lib): %v", err)
	}
	if string(created.Roots[0].Path.Bytes) != missingUpper {
		t.Fatalf("stored root bytes = %q, want %q", created.Roots[0].Path.Bytes, missingUpper)
	}
	if created.Library.IntakePath == nil || string(created.Library.IntakePath.Bytes) != missingNFD {
		t.Fatalf("stored intake bytes = %+v, want %q", created.Library.IntakePath, missingNFD)
	}

	otherManagedDir := filepath.Join(canonBase, "OtherManagedIntake")
	created2, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:       "Missing Alias Lib 2",
		Mode:       domain.LibraryModeManaged,
		IntakePath: []byte(otherManagedDir),
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Missing Alias Lib 2): %v", err)
	}

	if caseAliased {
		for _, aliasPath := range [][]byte{[]byte(missingLower), []byte(missingLowerChild)} {
			if _, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
				Name:  fmt.Sprintf("Case Conflict %s", aliasPath),
				Roots: [][]byte{aliasPath},
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("CreateLibrary(missing case alias %q) = %v, want ErrConflict + ErrPathOverlap", aliasPath, err)
			}
			if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
				LibraryID: created.Library.ID,
				Path:      aliasPath,
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("AddRoot(missing case alias %q) = %v, want ErrConflict + ErrPathOverlap", aliasPath, err)
			}
			if _, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
				LibraryID:  created.Library.ID,
				IntakePath: aliasPath,
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("SetManagedIntakePath(missing case alias %q vs own root) = %v, want ErrConflict + ErrPathOverlap", aliasPath, err)
			}
		}
	} else {
		distinct, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
			Name:  "Distinct Missing Lower Lib",
			Roots: [][]byte{[]byte(missingLowerChild)},
		})
		if err != nil {
			t.Fatalf("CreateLibrary(distinct missing lower on sensitive fs): %v", err)
		}
		if string(distinct.Roots[0].Path.Bytes) != missingLowerChild {
			t.Fatalf("stored distinct lower bytes = %q, want %q", distinct.Roots[0].Path.Bytes, missingLowerChild)
		}
	}

	if normAliased {
		for _, aliasPath := range [][]byte{[]byte(missingNFC), []byte(missingNFCChild)} {
			if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
				LibraryID: created.Library.ID,
				Path:      aliasPath,
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("AddRoot(missing norm alias %q) = %v, want ErrConflict + ErrPathOverlap", aliasPath, err)
			}
			if _, err := svc.SetManagedIntakePath(ctx, admin, app.SetManagedIntakePathRequest{
				LibraryID:  created2.Library.ID,
				IntakePath: aliasPath,
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("SetManagedIntakePath(missing norm alias %q vs other library intake) = %v, want ErrConflict + ErrPathOverlap", aliasPath, err)
			}
		}
	} else {
		distinctNFC, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
			Name:  "Distinct Missing NFC Lib",
			Roots: [][]byte{[]byte(missingNFCChild)},
		})
		if err != nil {
			t.Fatalf("CreateLibrary(distinct missing NFC on sensitive fs): %v", err)
		}
		if string(distinctNFC.Roots[0].Path.Bytes) != missingNFCChild {
			t.Fatalf("stored distinct NFC bytes = %q, want %q", distinctNFC.Roots[0].Path.Bytes, missingNFCChild)
		}
	}

	// Disjoint missing sibling succeeds on all filesystems.
	disjointSibling := filepath.Join(canonBase, "Future2")
	if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: created.Library.ID,
		Path:      []byte(disjointSibling),
	}); err != nil {
		t.Fatalf("AddRoot(disjoint missing sibling): %v", err)
	}

	// Distinct malformed UTF-8 missing paths differing in ASCII case succeed on all filesystems,
	// while identical malformed byte prefixes are rejected.
	malUpper := append([]byte(canonBase+"/Mal-"), 0xff)
	malLowerChild := append(append([]byte(canonBase+"/mal-"), 0xff), []byte("/child")...)
	malUpperChild := append(append([]byte{}, malUpper...), []byte("/child")...)
	if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: created.Library.ID,
		Path:      malUpper,
	}); err != nil {
		t.Fatalf("AddRoot(malUpper): %v", err)
	}
	if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: created.Library.ID,
		Path:      malLowerChild,
	}); err != nil {
		t.Fatalf("AddRoot(distinct malLowerChild): %v", err)
	}
	if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: created.Library.ID,
		Path:      malUpperChild,
	}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
		t.Fatalf("AddRoot(identical malUpperChild) = %v, want ErrConflict + ErrPathOverlap", err)
	}

	// Verify persisted libraries count matches expected sensitive/aliasing outcome.
	storedLibs, err := postgres.NewLibraryRepository(db.Pool).ListLibraries(ctx)
	if err != nil {
		t.Fatalf("ListLibraries: %v", err)
	}
	wantLibs := 2
	if !caseAliased {
		wantLibs++
	}
	if !normAliased {
		wantLibs++
	}
	if len(storedLibs) != wantLibs {
		t.Fatalf("storedLibs = %d, want %d (%+v)", len(storedLibs), wantLibs, storedLibs)
	}

	assertSnapshotIdentical(t, canonBase, before)
}

func TestConfigService_RemoveRoot_GuardsAssetsAndDeletesEmpty(t *testing.T) {
	db, _, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	dirKeep := filepath.Join(canonBase, "keep")
	dirEmpty := filepath.Join(canonBase, "empty")
	dirSibling := filepath.Join(canonBase, "sibling")
	for _, d := range []string{dirKeep, dirEmpty, dirSibling} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}

	created, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:  "Remove Guard Lib",
		Roots: [][]byte{[]byte(dirKeep)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary: %v", err)
	}
	keepRoot := created.Roots[0]
	emptyRoot, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: created.Library.ID,
		Path:      []byte(dirEmpty),
	})
	if err != nil {
		t.Fatalf("AddRoot(empty): %v", err)
	}

	otherDir := filepath.Join(canonBase, "other")
	if err := os.MkdirAll(otherDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", otherDir, err)
	}
	otherLib, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:  "Other Lib",
		Roots: [][]byte{[]byte(otherDir)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(other): %v", err)
	}

	// Cross-library or unknown root returns ErrNotFound.
	if err := svc.RemoveRoot(ctx, admin, app.RemoveLibraryRootRequest{
		LibraryID: otherLib.Library.ID,
		RootID:    emptyRoot.ID,
	}); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("RemoveRoot(cross-library) = %v, want ErrNotFound", err)
	}
	if err := svc.RemoveRoot(ctx, admin, app.RemoveLibraryRootRequest{
		LibraryID: created.Library.ID,
		RootID:    domain.MustNewID(),
	}); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("RemoveRoot(missing) = %v, want ErrNotFound", err)
	}

	// Empty root succeeds when a sibling remains.
	if err := svc.RemoveRoot(ctx, admin, app.RemoveLibraryRootRequest{
		LibraryID: created.Library.ID,
		RootID:    emptyRoot.ID,
	}); err != nil {
		t.Fatalf("RemoveRoot(empty) = %v, want nil", err)
	}
	libRepo := postgres.NewLibraryRepository(db.Pool)
	if _, err := libRepo.GetLibraryRootByID(ctx, emptyRoot.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected emptyRoot deleted, got %v", err)
	}

	// Removing the sole empty root of a library fails with ErrConflict and preserves the root (ADR-0019 §3).
	if err := svc.RemoveRoot(ctx, admin, app.RemoveLibraryRootRequest{
		LibraryID: created.Library.ID,
		RootID:    keepRoot.ID,
	}); !errors.Is(err, app.ErrConflict) {
		t.Fatalf("RemoveRoot(sole empty root) = %v, want ErrConflict", err)
	}
	if _, err := libRepo.GetLibraryRootByID(ctx, keepRoot.ID); err != nil {
		t.Fatalf("sole root must be preserved after 409: %v", err)
	}

	// Add a sibling root before seeding assets so asset-conflict cases exercise the non-cascading FK guard.
	if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: created.Library.ID,
		Path:      []byte(dirSibling),
	}); err != nil {
		t.Fatalf("AddRoot(sibling): %v", err)
	}

	// Seed an available, missing, and retired asset on keepRoot; each state blocks removal with ErrConflict
	// and preserves all root and catalogue rows.
	volID := domain.MustNewID()
	if _, err := db.Exec(ctx, `INSERT INTO volumes (id, library_id) VALUES ($1, $2)`, volID, created.Library.ID); err != nil {
		t.Fatalf("insert volume: %v", err)
	}
	assetID := domain.MustNewID()
	if _, err := db.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format)
		VALUES ($1, $2, $3, $4, 'Series/v01.cbz', 'series/v01.cbz', 42, 'CBZ')
	`, assetID, volID, keepRoot.ID, []byte("Series/v01.cbz")); err != nil {
		t.Fatalf("insert media_asset: %v", err)
	}

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			name: "available asset",
			sql:  `UPDATE media_assets SET missing_since = NULL, retired_at = NULL WHERE id = $1`,
		},
		{
			name: "missing asset",
			sql:  `UPDATE media_assets SET missing_since = NOW(), retired_at = NULL WHERE id = $1`,
		},
		{
			name: "retired asset",
			sql:  `UPDATE media_assets SET missing_since = NOW() - INTERVAL '10 days', retired_at = NOW() WHERE id = $1`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := db.Exec(ctx, tc.sql, assetID); err != nil {
				t.Fatalf("update asset state (%s): %v", tc.name, err)
			}
			if err := svc.RemoveRoot(ctx, admin, app.RemoveLibraryRootRequest{
				LibraryID: created.Library.ID,
				RootID:    keepRoot.ID,
			}); !errors.Is(err, app.ErrConflict) {
				t.Fatalf("RemoveRoot(%s) = %v, want ErrConflict", tc.name, err)
			}
			if _, err := libRepo.GetLibraryRootByID(ctx, keepRoot.ID); err != nil {
				t.Fatalf("keepRoot must be preserved after 409 (%s): %v", tc.name, err)
			}
			var count int
			if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM media_assets WHERE id = $1`, assetID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("asset must be preserved after 409 (%s): count=%d err=%v", tc.name, count, err)
			}
		})
	}
}
