package roots_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/roots"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestMain(m *testing.M) { os.Exit(testdb.Run(m)) }

func buildTestCBZBytes(t *testing.T, series, title, number string, pageBody string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	ci := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?><ComicInfo><Series>%s</Series><Title>%s</Title><Number>%s</Number></ComicInfo>`,
		series, title, number)
	wCI, err := zw.Create("ComicInfo.xml")
	if err != nil {
		t.Fatalf("zip create ComicInfo.xml: %v", err)
	}
	if _, err := wCI.Write([]byte(ci)); err != nil {
		t.Fatalf("zip write ComicInfo.xml: %v", err)
	}
	wPage, err := zw.Create("001.jpg")
	if err != nil {
		t.Fatalf("zip create 001.jpg: %v", err)
	}
	if _, err := wPage.Write([]byte(pageBody)); err != nil {
		t.Fatalf("zip write 001.jpg: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func writeRelocateTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func newRootsRunner(t *testing.T, jobRepo storage.JobRepository, rel *roots.Relocator) *jobs.Runner {
	t.Helper()
	mod, err := roots.NewModule(rel)
	if err != nil {
		t.Fatalf("roots.NewModule: %v", err)
	}
	reg := module.NewRegistry()
	if err := reg.Register(mod); err != nil {
		t.Fatalf("reg.Register: %v", err)
	}
	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{})
	if err := runner.RegisterModuleKinds(reg); err != nil {
		t.Fatalf("RegisterModuleKinds: %v", err)
	}
	if err := runner.RegisterModuleHandlers(reg); err != nil {
		t.Fatalf("RegisterModuleHandlers: %v", err)
	}
	if _, err := runner.Recover(t.Context()); err != nil {
		t.Fatalf("runner.Recover: %v", err)
	}
	return runner
}

type seededRelocateFixture struct {
	lib       domain.Library
	root      domain.LibraryRoot
	avail     domain.MediaAsset
	missing   domain.MediaAsset
	retired   domain.MediaAsset
	userID    string
	availData []byte
	missData  []byte
	retData   []byte
}

func seedRelocateFixture(
	ctx context.Context,
	t *testing.T,
	db *testdb.TestDB,
	transactor *postgres.Transactor,
	svc *roots.ConfigService,
	admin *domain.Principal,
	mode domain.LibraryMode,
	oldRoot string,
	intakeDir string,
) seededRelocateFixture {
	t.Helper()
	if admin != nil && admin.User.ID != "" {
		if _, err := db.Exec(ctx, `
			INSERT INTO users (id, username, created_at, updated_at)
			VALUES ($1, $2, NOW(), NOW())
			ON CONFLICT (id) DO NOTHING
		`, admin.User.ID, admin.User.Username); err != nil {
			t.Fatalf("ensure admin user: %v", err)
		}
	}

	availData := buildTestCBZBytes(t, "Reloc", "One", "1", "page-avail-001")
	missData := buildTestCBZBytes(t, "Reloc", "Two", "2", "page-miss-002")
	retData := buildTestCBZBytes(t, "Reloc", "Three", "3", "page-ret-003")

	availPath := filepath.Join(oldRoot, "Reloc/v01.cbz")
	missPath := filepath.Join(oldRoot, "Reloc/v02-missing.cbz")
	retPath := filepath.Join(oldRoot, "Reloc/v03-retired.cbz")
	writeRelocateTestFile(t, availPath, availData)
	writeRelocateTestFile(t, missPath, missData)
	writeRelocateTestFile(t, retPath, retData)

	req := app.CreateLibraryRequest{
		Name:  "Reloc Lib " + domain.MustNewID(),
		Mode:  mode,
		Roots: [][]byte{[]byte(oldRoot)},
	}
	if mode == domain.LibraryModeManaged {
		req.IntakePath = []byte(intakeDir)
	}
	created, err := svc.CreateLibrary(ctx, admin, req)
	if err != nil {
		t.Fatalf("CreateLibrary: %v", err)
	}
	lib := created.Library
	root := created.Roots[0]

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 3 {
		t.Fatalf("initial ScanLibrary = (%+v, %v), want Complete IndexedCount=3", res, err)
	}

	// Remove missing/retired files from oldRoot so they reflect prior missing/retired state,
	// and seed historical digest + missing/retired timestamps + reading_progress.
	if err := os.Remove(missPath); err != nil {
		t.Fatalf("remove missPath: %v", err)
	}
	if err := os.Remove(retPath); err != nil {
		t.Fatalf("remove retPath: %v", err)
	}

	assets, err := postgres.NewCatalogueWriteRepository(db.Pool).ListAssetsByRoot(ctx, root.ID)
	if err != nil {
		t.Fatalf("ListAssetsByRoot: %v", err)
	}
	if len(assets) != 3 {
		t.Fatalf("expected 3 assets, got %d", len(assets))
	}
	byRel := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byRel[string(a.RelPathBytes)] = a
	}
	avail := byRel["Reloc/v01.cbz"]
	miss := byRel["Reloc/v02-missing.cbz"]
	ret := byRel["Reloc/v03-retired.cbz"]

	missAt := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	retAt := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	userID := domain.MustNewID()
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		if err := tx.Users().CreateUser(ctx, &domain.User{ID: userID, Username: "reader-" + userID}); err != nil {
			return err
		}
		cw := tx.CatalogueWrites()
		miss.MissingSince = &missAt
		if err := cw.UpdateAsset(ctx, &miss); err != nil {
			return err
		}
		ret.MissingSince = &missAt
		ret.RetiredAt = &retAt
		return cw.UpdateAsset(ctx, &ret)
	}); err != nil {
		t.Fatalf("update missing/retired state: %v", err)
	}

	if _, err := db.Exec(ctx, `
		INSERT INTO asset_digests (asset_id, kind, value, is_current, reason, recorded_at)
		VALUES ($1, $2, $3, FALSE, $4, $5)
	`, avail.ID, string(domain.DigestKindXXH364Partial), "0000000000000001", string(domain.DigestReasonWriteBack), missAt); err != nil {
		t.Fatalf("insert historical digest: %v", err)
	}

	if _, err := db.Exec(ctx, `
		INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, device, source, completed, updated_at)
		VALUES ($1, $2, '{"type":"page","page":4}'::jsonb, 0.4, 'kobo', 'KOMGA', false, $3)
	`, userID, avail.VolumeID, retAt); err != nil {
		t.Fatalf("insert reading_progress: %v", err)
	}

	return seededRelocateFixture{
		lib:       lib,
		root:      root,
		avail:     avail,
		missing:   miss,
		retired:   ret,
		userID:    userID,
		availData: availData,
		missData:  missData,
		retData:   retData,
	}
}

func assertRelocateFixturePreserved(ctx context.Context, t *testing.T, db *testdb.TestDB, f seededRelocateFixture, wantPath []byte) {
	t.Helper()
	rootsList, err := postgres.NewLibraryRepository(db.Pool).ListLibraryRoots(ctx, f.lib.ID)
	if err != nil {
		t.Fatalf("ListLibraryRoots: %v", err)
	}
	if len(rootsList) != 1 || rootsList[0].ID != f.root.ID {
		t.Fatalf("roots = %+v, want single root %q", rootsList, f.root.ID)
	}
	if !bytes.Equal(rootsList[0].Path.Bytes, wantPath) {
		t.Fatalf("root.Path.Bytes = %q, want %q", rootsList[0].Path.Bytes, wantPath)
	}

	assets, err := postgres.NewCatalogueWriteRepository(db.Pool).ListAssetsByRoot(ctx, f.root.ID)
	if err != nil {
		t.Fatalf("ListAssetsByRoot: %v", err)
	}
	if len(assets) != 3 {
		t.Fatalf("expected 3 assets preserved, got %d", len(assets))
	}
	byID := make(map[string]domain.MediaAsset, 3)
	for _, a := range assets {
		byID[a.ID] = a
	}

	gotAvail, ok := byID[f.avail.ID]
	if !ok || !bytes.Equal(gotAvail.RelPathBytes, f.avail.RelPathBytes) || gotAvail.MissingSince != nil || gotAvail.RetiredAt != nil {
		t.Fatalf("available asset altered: %+v", gotAvail)
	}
	if len(gotAvail.Digests) != 1 || !gotAvail.Digests[0].IsCurrent {
		t.Fatalf("expected current digest preserved on avail asset, got %+v", gotAvail.Digests)
	}
	var digestCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM asset_digests WHERE asset_id = $1`, f.avail.ID).Scan(&digestCount); err != nil {
		t.Fatalf("counting asset_digests: %v", err)
	}
	if digestCount != 2 {
		t.Fatalf("expected 2 asset_digests rows (current+historical) preserved, got %d", digestCount)
	}
	gotMiss, ok := byID[f.missing.ID]
	if !ok || gotMiss.MissingSince == nil || gotMiss.RetiredAt != nil {
		t.Fatalf("missing asset state altered: %+v", gotMiss)
	}
	gotRet, ok := byID[f.retired.ID]
	if !ok || gotRet.MissingSince == nil || gotRet.RetiredAt == nil {
		t.Fatalf("retired asset state altered: %+v", gotRet)
	}

	var prog float64
	if err := db.QueryRow(ctx, `SELECT total_progression FROM reading_progress WHERE user_id = $1 AND volume_id = $2`,
		f.userID, f.avail.VolumeID).Scan(&prog); err != nil {
		t.Fatalf("reading_progress lost: %v", err)
	}
	if prog < 0.39 || prog > 0.41 {
		t.Fatalf("reading_progress = %v, want 0.4", prog)
	}
}

func TestRelocateRoot_VirtualAndManagedSuccessAndPreservation(t *testing.T) {
	t.Parallel()
	db, transactor, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name              string
		mode              domain.LibraryMode
		includeMissTarget bool
	}{
		{name: "VIRTUAL absent missing/retired allowed", mode: domain.LibraryModeVirtual, includeMissTarget: false},
		{name: "MANAGED present matching missing/retired verified", mode: domain.LibraryModeManaged, includeMissTarget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			canonBase, err := paths.CanonicalPath(base)
			if err != nil {
				t.Fatalf("CanonicalPath: %v", err)
			}
			oldRoot := filepath.Join(canonBase, "old")
			newRoot := filepath.Join(canonBase, "new")
			aliasNew := filepath.Join(canonBase, "alias-new")
			intakeDir := filepath.Join(canonBase, "intake")
			if err := os.MkdirAll(intakeDir, 0o755); err != nil {
				t.Fatalf("MkdirAll: %v", err)
			}

			fix := seedRelocateFixture(ctx, t, db, transactor, svc, admin, tc.mode, oldRoot, intakeDir)
			availDst := filepath.Join(newRoot, "Reloc/v01.cbz")
			writeRelocateTestFile(t, availDst, fix.availData)
			if fix.avail.FileMtime != nil {
				if err := os.Chtimes(availDst, *fix.avail.FileMtime, *fix.avail.FileMtime); err != nil {
					t.Fatalf("Chtimes(v01): %v", err)
				}
			}
			if tc.includeMissTarget {
				writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v02-missing.cbz"), fix.missData)
				writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v03-retired.cbz"), fix.retData)
			}
			if err := os.Symlink(newRoot, aliasNew); err != nil {
				t.Fatalf("Symlink: %v", err)
			}

			jobID, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
				RootID: fix.root.ID,
				Path:   []byte(aliasNew),
			})
			if err != nil {
				t.Fatalf("RelocateRoot: %v", err)
			}

			rel, err := roots.NewRelocator(transactor, roots.RelocatorOptions{})
			if err != nil {
				t.Fatalf("NewRelocator: %v", err)
			}
			jobRepo := postgres.NewJobRepository(db.Pool)
			runner := newRootsRunner(t, jobRepo, rel)
			if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot); err != nil || !claimed {
				t.Fatalf("ClaimAndRunNext = (%v, %v)", claimed, err)
			}

			gotJob, err := jobRepo.GetJobByID(ctx, jobID)
			if err != nil || gotJob.State != domain.JobStateSucceeded {
				t.Fatalf("relocate job = (%+v, %v), want SUCCEEDED", gotJob, err)
			}
			assertRelocateFixturePreserved(ctx, t, db, fix, []byte(newRoot))

			// Verify ordinary scan works cleanly on the relocated root.
			sc, err := scanner.NewScanner(transactor, scanner.Options{})
			if err != nil {
				t.Fatalf("NewScanner: %v", err)
			}
			scanRes, err := sc.ScanLibrary(ctx, fix.lib.ID, scanner.ScanOptions{})
			if err != nil || !scanRes.Complete {
				t.Fatalf("post-relocate ScanLibrary = (%+v, %v), want Complete", scanRes, err)
			}
			if tc.includeMissTarget && scanRes.RestoredCount != 2 {
				t.Fatalf("expected 2 missing/retired files revived by post-relocate scan, got %+v", scanRes)
			}
			if !tc.includeMissTarget && scanRes.UnchangedCount != 1 {
				t.Fatalf("expected 1 unchanged file on post-relocate scan, got %+v", scanRes)
			}
		})
	}
}

func TestRelocateRoot_VerificationAndOverlapFailures(t *testing.T) {
	t.Parallel()
	db, transactor, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}

	// Setup an unrelated library root + managed intake to test overlap rejection.
	otherRoot := filepath.Join(canonBase, "other-root")
	otherIntake := filepath.Join(canonBase, "other-intake")
	writeRelocateTestFile(t, filepath.Join(otherRoot, "x.cbz"), buildTestCBZBytes(t, "Other", "1", "1", "x"))
	if err := os.MkdirAll(otherIntake, 0o755); err != nil {
		t.Fatalf("MkdirAll(otherIntake): %v", err)
	}
	otherCreated, err := svc.CreateLibrary(ctx, admin, app.CreateLibraryRequest{
		Name:       "Other Managed Lib",
		Mode:       domain.LibraryModeManaged,
		IntakePath: []byte(otherIntake),
		Roots:      [][]byte{[]byte(otherRoot)},
	})
	if err != nil {
		t.Fatalf("CreateLibrary(Other): %v", err)
	}

	setupCase := func(t *testing.T) (seededRelocateFixture, string, string) {
		t.Helper()
		caseDir := filepath.Join(canonBase, "case-"+domain.MustNewID())
		oldRoot := filepath.Join(caseDir, "old")
		newRoot := filepath.Join(caseDir, "new")
		ownIntake := filepath.Join(caseDir, "intake")
		if err := os.MkdirAll(ownIntake, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		fix := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeManaged, oldRoot, ownIntake)
		return fix, newRoot, ownIntake
	}

	t.Run("enqueue rejects overlapping target paths", func(t *testing.T) {
		fix, _, ownIntake := setupCase(t)
		for _, target := range [][]byte{
			[]byte(otherRoot),
			[]byte(filepath.Join(otherRoot, "sub")),
			[]byte(filepath.Dir(otherRoot)),
			[]byte(otherIntake),
			[]byte(ownIntake),
		} {
			if _, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
				RootID: fix.root.ID,
				Path:   target,
			}); !errors.Is(err, app.ErrConflict) || !errors.Is(err, roots.ErrPathOverlap) {
				t.Fatalf("RelocateRoot(%q) = %v, want ErrConflict + ErrPathOverlap", target, err)
			}
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, fix.root.Path.Bytes)
	})

	runAndExpectFail := func(t *testing.T, fix seededRelocateFixture, newRoot string, relOpts roots.RelocatorOptions, wantErr error) {
		t.Helper()
		jobRepo := postgres.NewJobRepository(db.Pool)
		jobSvc := jobs.NewService(jobRepo, jobs.ServiceOptions{})
		jobID, err := roots.TriggerRelocateRoot(ctx, jobSvc, fix.lib.ID, fix.root.ID, []byte(newRoot), roots.TriggerRelocateOptions{})
		if err != nil {
			t.Fatalf("TriggerRelocateRoot: %v", err)
		}
		rel, err := roots.NewRelocator(transactor, relOpts)
		if err != nil {
			t.Fatalf("NewRelocator: %v", err)
		}
		runner := newRootsRunner(t, jobRepo, rel)
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext = (%v, %v)", claimed, err)
		}
		got, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil || got.State != domain.JobStateFailed {
			t.Fatalf("job = (%+v, %v), want FAILED", got, err)
		}
		if wantErr != nil && (got.LastError == nil || !bytes.Contains([]byte(*got.LastError), []byte(wantErr.Error()))) {
			t.Fatalf("job LastError = %v, want substring %q", got.LastError, wantErr.Error())
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, fix.root.Path.Bytes)
	}

	t.Run("missing required file fails", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		if err := os.MkdirAll(newRoot, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{}, roots.ErrMissingAssetFile)
	})

	t.Run("matching size and mtime with altered content fails fingerprint check", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		// Same length as availData, different bytes at the start of the ZIP header/comment.
		altered := append([]byte(nil), fix.availData...)
		altered[0] ^= 0xff
		if len(altered) != len(fix.availData) {
			t.Fatalf("size mismatch in test setup")
		}
		dst := filepath.Join(newRoot, "Reloc/v01.cbz")
		writeRelocateTestFile(t, dst, altered)
		if fix.avail.FileMtime != nil {
			if err := os.Chtimes(dst, *fix.avail.FileMtime, *fix.avail.FileMtime); err != nil {
				t.Fatalf("Chtimes: %v", err)
			}
		}
		if _, err := db.Exec(ctx, `UPDATE media_assets SET file_size = $1 WHERE id = $2`, int64(len(altered)), fix.avail.ID); err != nil {
			t.Fatalf("update file_size: %v", err)
		}
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{}, roots.ErrDigestMismatch)
	})

	t.Run("unreadable required file fails", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		dst := filepath.Join(newRoot, "Reloc/v01.cbz")
		writeRelocateTestFile(t, dst, fix.availData)
		if runtime.GOOS != "windows" {
			if err := os.Chmod(dst, 0o000); err != nil {
				t.Fatalf("Chmod: %v", err)
			}
			defer func() { _ = os.Chmod(dst, 0o600) }()
		}
		opts := roots.RelocatorOptions{}
		if os.Geteuid() == 0 {
			opts.Open = func(_ context.Context, _ []byte) (scanner.ReadAtCloser, error) {
				return nil, os.ErrPermission
			}
		}
		runAndExpectFail(t, fix, newRoot, opts, roots.ErrUnreadableAssetFile)
	})

	t.Run("present missing or retired asset with wrong digest fails", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v03-retired.cbz"), []byte("wrong-retired-bytes"))
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{}, roots.ErrDigestMismatch)
	})

	t.Run("present asset without verifiable XXH3_64_PARTIAL digest fails", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v02-missing.cbz"), fix.missData)
		if _, err := db.Exec(ctx, `DELETE FROM asset_digests WHERE asset_id = $1 AND kind = 'XXH3_64_PARTIAL'`, fix.missing.ID); err != nil {
			t.Fatalf("delete partial digest: %v", err)
		}
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{}, roots.ErrMissingVerificationDigest)
	})

	t.Run("overlap introduced during verification fails before activation", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)
		nested := filepath.Join(newRoot, "Reloc")
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{
			OnAfterVerify: func(hookCtx context.Context, _ domain.Job) error {
				_, addErr := svc.AddRoot(hookCtx, admin, app.AddLibraryRootRequest{
					LibraryID: otherCreated.Library.ID,
					Path:      []byte(nested),
				})
				return addErr
			},
		}, roots.ErrPathOverlap)
	})

	t.Run("verified file replaced in place with matching size and mtime after verification fails", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		dst := filepath.Join(newRoot, "Reloc/v01.cbz")
		writeRelocateTestFile(t, dst, fix.availData)
		if fix.avail.FileMtime != nil {
			if err := os.Chtimes(dst, *fix.avail.FileMtime, *fix.avail.FileMtime); err != nil {
				t.Fatalf("Chtimes: %v", err)
			}
		}
		altered := append([]byte(nil), fix.availData...)
		altered[0] ^= 0xff
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{
			OnAfterVerify: func(_ context.Context, _ domain.Job) error {
				if err := os.WriteFile(dst, altered, 0o600); err != nil {
					return err
				}
				if fix.avail.FileMtime != nil {
					return os.Chtimes(dst, *fix.avail.FileMtime, *fix.avail.FileMtime)
				}
				return nil
			},
		}, roots.ErrDigestMismatch)
	})

	t.Run("verified file replaced by atomic rename after verification fails", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		dst := filepath.Join(newRoot, "Reloc/v01.cbz")
		writeRelocateTestFile(t, dst, fix.availData)
		altered := buildTestCBZBytes(t, "Reloc", "Tampered", "1", "tampered-body-diff")
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{
			OnAfterVerify: func(_ context.Context, _ domain.Job) error {
				tmp := dst + ".tmp"
				if err := os.WriteFile(tmp, altered, 0o600); err != nil {
					return err
				}
				return os.Rename(tmp, dst)
			},
		}, roots.ErrDigestMismatch)
	})

	t.Run("verified required file removed after verification fails", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		dst := filepath.Join(newRoot, "Reloc/v01.cbz")
		writeRelocateTestFile(t, dst, fix.availData)
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{
			OnAfterVerify: func(_ context.Context, _ domain.Job) error {
				return os.Remove(dst)
			},
		}, roots.ErrMissingAssetFile)
	})

	t.Run("verified file replaced while waiting on contended global configuration lock fails verification", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		dst := filepath.Join(newRoot, "Reloc/v01.cbz")
		writeRelocateTestFile(t, dst, fix.availData)
		altered := append([]byte(nil), fix.availData...)
		altered[0] ^= 0xff

		lockAcquired := make(chan struct{})
		holderDone := make(chan error, 1)
		holderCtx, holderCancel := context.WithCancel(ctx)
		defer holderCancel()

		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{
			OnAfterVerify: func(_ context.Context, _ domain.Job) error {
				go func() {
					holderDone <- transactor.WithReadCommittedTx(holderCtx, func(tx storage.Tx) error {
						if err := tx.Libraries().LockConfiguration(holderCtx); err != nil {
							return err
						}
						close(lockAcquired)

						// Wait until Relocator's TryLockConfiguration observed contention and entered
						// waitForConfigLockRelease (blocked on AcquireLibraryConfigLock).
						deadline := time.Now().Add(8 * time.Second)
						for {
							var blocked int
							if err := db.QueryRow(holderCtx, `
								SELECT COUNT(*)
								FROM pg_stat_activity
								WHERE datname = current_database()
								  AND pid <> pg_backend_pid()
								  AND wait_event_type = 'Lock'
								  AND query ILIKE '%pg_advisory_xact_lock%'
							`).Scan(&blocked); err != nil {
								return err
							}
							if blocked > 0 {
								break
							}
							if time.Now().After(deadline) {
								return errors.New("timed out waiting for relocator to block on config lock release")
							}
							time.Sleep(15 * time.Millisecond)
						}

						if err := os.WriteFile(dst, altered, 0o600); err != nil {
							return err
						}
						if fix.avail.FileMtime != nil {
							return os.Chtimes(dst, *fix.avail.FileMtime, *fix.avail.FileMtime)
						}
						return nil
					})
				}()
				select {
				case <-lockAcquired:
					return nil
				case err := <-holderDone:
					return fmt.Errorf("config lock holder exited early: %w", err)
				case <-holderCtx.Done():
					return holderCtx.Err()
				}
			},
		}, roots.ErrDigestMismatch)

		if err := <-holderDone; err != nil {
			t.Fatalf("config lock holder err = %v", err)
		}
	})

	t.Run("previously absent optional file appearing after verification with wrong digest fails", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)
		missDst := filepath.Join(newRoot, "Reloc/v02-missing.cbz")
		runAndExpectFail(t, fix, newRoot, roots.RelocatorOptions{
			OnAfterVerify: func(_ context.Context, _ domain.Job) error {
				return os.WriteFile(missDst, []byte("appeared-with-bad-bytes"), 0o600)
			},
		}, roots.ErrDigestMismatch)
	})

	t.Run("previously absent optional file appearing after verification with valid digest re-verifies and activates", func(t *testing.T) {
		fix, newRoot, _ := setupCase(t)
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)
		missDst := filepath.Join(newRoot, "Reloc/v02-missing.cbz")

		jobRepo := postgres.NewJobRepository(db.Pool)
		jobSvc := jobs.NewService(jobRepo, jobs.ServiceOptions{})
		jobID, err := roots.TriggerRelocateRoot(ctx, jobSvc, fix.lib.ID, fix.root.ID, []byte(newRoot), roots.TriggerRelocateOptions{})
		if err != nil {
			t.Fatalf("TriggerRelocateRoot: %v", err)
		}
		rel, err := roots.NewRelocator(transactor, roots.RelocatorOptions{
			OnAfterVerify: func(_ context.Context, _ domain.Job) error {
				return os.WriteFile(missDst, fix.missData, 0o600)
			},
		})
		if err != nil {
			t.Fatalf("NewRelocator: %v", err)
		}
		runner := newRootsRunner(t, jobRepo, rel)
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext = (%v, %v)", claimed, err)
		}
		got, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil || got.State != domain.JobStateSucceeded {
			t.Fatalf("job = (%+v, %v), want SUCCEEDED", got, err)
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(newRoot))
	})
}

func TestRelocateRoot_CancellationAndCrashRecoveryBothSides(t *testing.T) {
	t.Parallel()
	db, transactor, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	jobRepo := postgres.NewJobRepository(db.Pool)
	jobSvc := jobs.NewService(jobRepo, jobs.ServiceOptions{})

	// 1. Cancellation before activation (during OnAfterVerify) leaves old path intact.
	t.Run("cancel before activation aborts without changing root", func(t *testing.T) {
		oldRoot := filepath.Join(canonBase, "cancel-old")
		newRoot := filepath.Join(canonBase, "cancel-new")
		fix := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldRoot, "")
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)

		jobID, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
			RootID: fix.root.ID,
			Path:   []byte(newRoot),
		})
		if err != nil {
			t.Fatalf("RelocateRoot: %v", err)
		}

		rel, err := roots.NewRelocator(transactor, roots.RelocatorOptions{
			OnAfterVerify: func(hookCtx context.Context, _ domain.Job) error {
				_, cErr := jobSvc.CancelJob(hookCtx, admin, jobID)
				return cErr
			},
		})
		if err != nil {
			t.Fatalf("NewRelocator: %v", err)
		}
		runner := newRootsRunner(t, jobRepo, rel)
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext = (%v, %v)", claimed, err)
		}
		got, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil || got.State != domain.JobStateCancelled {
			t.Fatalf("job = (%+v, %v), want CANCELLED", got, err)
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(oldRoot))
	})

	// 2. Crash BEFORE activation: Recover requeues, second attempt verifies and succeeds.
	t.Run("crash before activation recovers via requeue and succeeds on retry", func(t *testing.T) {
		oldRoot := filepath.Join(canonBase, "precrash-old")
		newRoot := filepath.Join(canonBase, "precrash-new")
		fix := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldRoot, "")
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)

		jobID, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
			RootID: fix.root.ID,
			Path:   []byte(newRoot),
		})
		if err != nil {
			t.Fatalf("RelocateRoot: %v", err)
		}

		crashCtx, crashCancel := context.WithCancel(ctx)
		relCrash, err := roots.NewRelocator(transactor, roots.RelocatorOptions{
			OnAfterVerify: func(_ context.Context, _ domain.Job) error {
				crashCancel()
				return context.Canceled
			},
		})
		if err != nil {
			t.Fatalf("NewRelocator(crash): %v", err)
		}
		runner1 := newRootsRunner(t, jobRepo, relCrash)
		if claimed, err := runner1.ClaimAndRunNext(crashCtx, domain.JobKindRelocateRoot); !claimed || !errors.Is(err, context.Canceled) {
			t.Fatalf("ClaimAndRunNext(crash) = (%v, %v), want (true, context.Canceled)", claimed, err)
		}

		// Old root must still be authoritative and job still RUNNING.
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(oldRoot))
		mid, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil || mid.State != domain.JobStateRunning || mid.Attempts != 1 {
			t.Fatalf("interrupted pre-activation job = (%+v, %v)", mid, err)
		}

		// Startup recovery requeues; attempt 2 runs cleanly and activates newRoot.
		relRecover, err := roots.NewRelocator(transactor, roots.RelocatorOptions{})
		if err != nil {
			t.Fatalf("NewRelocator(recover): %v", err)
		}
		runner2 := newRootsRunner(t, jobRepo, relRecover)
		afterRec, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil || afterRec.State != domain.JobStateQueued || string(afterRec.Checkpoint) != "{}" {
			t.Fatalf("recovered pre-activation job = (%+v, %v), want QUEUED checkpoint={}", afterRec, err)
		}
		if claimed, err := runner2.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext(attempt 2) = (%v, %v)", claimed, err)
		}
		final, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil || final.State != domain.JobStateSucceeded || final.Attempts != 2 {
			t.Fatalf("final job = (%+v, %v), want SUCCEEDED attempts=2", final, err)
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(newRoot))
	})

	// 3. Crash AFTER activation: Recover reconciles committed activation to SUCCEEDED.
	t.Run("crash after activation recovers directly as SUCCEEDED", func(t *testing.T) {
		oldRoot := filepath.Join(canonBase, "postcrash-old")
		newRoot := filepath.Join(canonBase, "postcrash-new")
		fix := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldRoot, "")
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)

		jobID, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
			RootID: fix.root.ID,
			Path:   []byte(newRoot),
		})
		if err != nil {
			t.Fatalf("RelocateRoot: %v", err)
		}

		crashCtx, crashCancel := context.WithCancel(ctx)
		relCrash, err := roots.NewRelocator(transactor, roots.RelocatorOptions{
			OnAfterActivate: func(hookCtx context.Context, _ domain.Job) error {
				// Even if cancel is requested right after activation committed, recovery must treat
				// the committed activation as SUCCEEDED.
				_, _ = jobSvc.CancelJob(hookCtx, admin, jobID)
				crashCancel()
				return context.Canceled
			},
		})
		if err != nil {
			t.Fatalf("NewRelocator(post-crash): %v", err)
		}
		runner1 := newRootsRunner(t, jobRepo, relCrash)
		if claimed, err := runner1.ClaimAndRunNext(crashCtx, domain.JobKindRelocateRoot); !claimed || !errors.Is(err, context.Canceled) {
			t.Fatalf("ClaimAndRunNext(post-crash) = (%v, %v)", claimed, err)
		}

		// Activation already committed new path while job remained RUNNING.
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(newRoot))

		relRecover, err := roots.NewRelocator(transactor, roots.RelocatorOptions{})
		if err != nil {
			t.Fatalf("NewRelocator(recover): %v", err)
		}
		_ = newRootsRunner(t, jobRepo, relRecover)

		final, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil || final.State != domain.JobStateSucceeded || final.Attempts != 1 {
			t.Fatalf("post-activation recovered job = (%+v, %v), want SUCCEEDED attempts=1", final, err)
		}
		// Dedupe key must be released.
		if _, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
			RootID: fix.root.ID,
			Path:   []byte(filepath.Join(canonBase, "postcrash-next")),
		}); err != nil {
			t.Fatalf("expected RELOCATE_ROOT reservation released after recovery, got %v", err)
		}
	})
}

func TestRelocateRoot_SameLibraryExclusionAndIndependentLibraries(t *testing.T) {
	t.Parallel()
	db, transactor, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}

	oldA := filepath.Join(canonBase, "libA-old")
	newA := filepath.Join(canonBase, "libA-new")
	oldB := filepath.Join(canonBase, "libB-old")
	newB := filepath.Join(canonBase, "libB-new")
	fixA := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldA, "")
	fixB := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldB, "")
	writeRelocateTestFile(t, filepath.Join(newA, "Reloc/v01.cbz"), fixA.availData)
	writeRelocateTestFile(t, filepath.Join(newB, "Reloc/v01.cbz"), fixB.availData)

	jobRepo := postgres.NewJobRepository(db.Pool)
	jobSvc := jobs.NewService(jobRepo, jobs.ServiceOptions{})

	// 1. Concurrent SCAN vs RelocateRoot race on libA: exactly one succeeds, one gets ErrConflict.
	start := make(chan struct{})
	var wg sync.WaitGroup
	var scanErr, relocErr error
	var scanID, relocID string
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		scanID, scanErr = scanner.TriggerScan(ctx, jobSvc, fixA.lib.ID, scanner.TriggerScanOptions{})
	}()
	go func() {
		defer wg.Done()
		<-start
		relocID, relocErr = svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
			RootID: fixA.root.ID,
			Path:   []byte(newA),
		})
	}()
	close(start)
	wg.Wait()

	if (scanErr == nil) == (relocErr == nil) {
		t.Fatalf("expected exactly one winner in SCAN vs RelocateRoot race: scan=(%q,%v) reloc=(%q,%v)",
			scanID, scanErr, relocID, relocErr)
	}
	if scanErr != nil && !errors.Is(scanErr, app.ErrConflict) {
		t.Fatalf("scanErr = %v, want ErrConflict", scanErr)
	}
	if relocErr != nil && !errors.Is(relocErr, app.ErrConflict) {
		t.Fatalf("relocErr = %v, want ErrConflict", relocErr)
	}

	// Independent library B proceeds without interference while libA is reserved!
	scanBID, err := scanner.TriggerScan(ctx, jobSvc, fixB.lib.ID, scanner.TriggerScanOptions{})
	if err != nil || scanBID == "" {
		t.Fatalf("independent libB TriggerScan while libA active = (%q, %v)", scanBID, err)
	}
	if _, err := jobRepo.RequestCancel(ctx, scanBID, time.Now().UTC()); err != nil {
		t.Fatalf("cancel libB scan: %v", err)
	}

	// Cancel whichever won on libA to reset, then test retry backoff exclusion.
	winnerID := scanID
	if relocID != "" {
		winnerID = relocID
	}
	if _, err := jobRepo.RequestCancel(ctx, winnerID, time.Now().UTC()); err != nil {
		t.Fatalf("cancel winner %q: %v", winnerID, err)
	}

	// 2. Retry backoff exclusion for RELOCATE_ROOT -> blocks SCAN & RelocateRoot on libA, allows libB.
	relocID2, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
		RootID: fixA.root.ID,
		Path:   []byte(newA),
	})
	if err != nil {
		t.Fatalf("RelocateRoot(libA): %v", err)
	}
	claimed, err := jobRepo.ClaimNext(ctx, domain.JobKindRelocateRoot, time.Now().UTC())
	if err != nil || claimed.ID != relocID2 {
		t.Fatalf("ClaimNext = (%+v, %v), want %q", claimed, err, relocID2)
	}
	if _, err := jobRepo.FailOrRetry(ctx, claimed.ID, claimed.Attempts, "backoff", time.Now().UTC().Add(time.Hour), time.Now().UTC()); err != nil {
		t.Fatalf("FailOrRetry: %v", err)
	}
	if _, err := scanner.TriggerScan(ctx, jobSvc, fixA.lib.ID, scanner.TriggerScanOptions{}); !errors.Is(err, app.ErrConflict) {
		t.Fatalf("TriggerScan(libA during RELOCATE_ROOT backoff) = %v, want ErrConflict", err)
	}
	if _, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
		RootID: fixA.root.ID,
		Path:   []byte(newA),
	}); !errors.Is(err, app.ErrConflict) {
		t.Fatalf("RelocateRoot(libA during RELOCATE_ROOT backoff) = %v, want ErrConflict", err)
	}
	if relocBID, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
		RootID: fixB.root.ID,
		Path:   []byte(newB),
	}); err != nil || relocBID == "" {
		t.Fatalf("RelocateRoot(libB while libA in backoff) = (%q, %v)", relocBID, err)
	}

	// Concurrent RelocateRoot vs RelocateRoot on libB (while libB already has QUEUED RELOCATE_ROOT) -> ErrConflict.
	var secondConflict atomic.Int32
	if _, err := svc.RelocateRoot(ctx, admin, app.RelocateRootRequest{
		RootID: fixB.root.ID,
		Path:   []byte(newB),
	}); errors.Is(err, app.ErrConflict) {
		secondConflict.Add(1)
	}
	if secondConflict.Load() != 1 {
		t.Fatalf("expected second RelocateRoot on libB to return ErrConflict")
	}

	// Clean up backoff job and verify repeat SCAN deduplication still works.
	if _, err := jobRepo.RequestCancel(ctx, relocID2, time.Now().UTC()); err != nil {
		t.Fatalf("cancel backoff job: %v", err)
	}
	s1, err := scanner.TriggerScan(ctx, jobSvc, fixA.lib.ID, scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan #1 after cancel: %v", err)
	}
	s2, err := scanner.TriggerScan(ctx, jobSvc, fixA.lib.ID, scanner.TriggerScanOptions{})
	if err != nil || s2 != s1 {
		t.Fatalf("repeat TriggerScan = (%q, %v), want %q", s2, err, s1)
	}
}

type lostCommitAckTransactor struct {
	inner             storage.Transactor
	activationCommits atomic.Int32
	injectOnce        atomic.Bool
	afterCommit       func(ctx context.Context) error
}

func (l *lostCommitAckTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return l.inner.WithTx(ctx, fn)
}

func (l *lostCommitAckTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	var sawActivation bool
	err := l.inner.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		return fn(&lostCommitAckTx{
			Tx:            tx,
			parent:        l,
			sawActivation: &sawActivation,
		})
	})
	if err != nil {
		return err
	}
	if sawActivation {
		l.activationCommits.Add(1)
		if l.afterCommit != nil {
			if hookErr := l.afterCommit(ctx); hookErr != nil {
				return hookErr
			}
		}
		if l.injectOnce.CompareAndSwap(true, false) {
			return errors.New("simulated lost commit acknowledgment")
		}
	}
	return nil
}

type lostCommitAckTx struct {
	storage.Tx
	parent        *lostCommitAckTransactor
	sawActivation *bool
}

func (t *lostCommitAckTx) Libraries() storage.LibraryRepository {
	return &lostCommitAckLibRepo{
		LibraryRepository: t.Tx.Libraries(),
		sawActivation:     t.sawActivation,
	}
}

type lostCommitAckLibRepo struct {
	storage.LibraryRepository
	sawActivation *bool
}

func (r *lostCommitAckLibRepo) UpdateLibraryRootPath(ctx context.Context, id string, path domain.PathIdentity) (*domain.LibraryRoot, error) {
	*r.sawActivation = true
	return r.LibraryRepository.UpdateLibraryRootPath(ctx, id, path)
}

func TestRelocateRoot_CommitAckAndInFlightOutcomeSettlement(t *testing.T) {
	t.Parallel()
	db, transactor, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	jobRepo := postgres.NewJobRepository(db.Pool)
	jobSvc := jobs.NewService(jobRepo, jobs.ServiceOptions{})

	t.Run("lost commit ACK with single attempt and concurrent cancel_requested settles SUCCEEDED", func(t *testing.T) {
		caseDir := filepath.Join(canonBase, "lost-ack-"+domain.MustNewID())
		oldRoot := filepath.Join(caseDir, "old")
		newRoot := filepath.Join(caseDir, "new")
		fix := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldRoot, "")
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)

		jobID, err := roots.TriggerRelocateRoot(ctx, jobSvc, fix.lib.ID, fix.root.ID, []byte(newRoot), roots.TriggerRelocateOptions{
			MaxAttempts: 1,
		})
		if err != nil {
			t.Fatalf("TriggerRelocateRoot: %v", err)
		}

		faultTx := &lostCommitAckTransactor{inner: transactor}
		faultTx.injectOnce.Store(true)
		faultTx.afterCommit = func(hookCtx context.Context) error {
			_, cErr := jobSvc.CancelJob(hookCtx, admin, jobID)
			return cErr
		}

		rel, err := roots.NewRelocator(faultTx, roots.RelocatorOptions{})
		if err != nil {
			t.Fatalf("NewRelocator: %v", err)
		}
		runner := newRootsRunner(t, jobRepo, rel)
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext = (%v, %v), want (true, nil)", claimed, err)
		}
		if claimedAgain, err := runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot); err != nil || claimedAgain {
			t.Fatalf("unexpected retry ClaimAndRunNext = (%v, %v)", claimedAgain, err)
		}
		if commits := faultTx.activationCommits.Load(); commits != 1 {
			t.Fatalf("activationCommits = %d, want 1", commits)
		}

		got, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJobByID: %v", err)
		}
		if got.State != domain.JobStateSucceeded || got.Attempts != 1 || got.LastError != nil {
			t.Fatalf("job = %+v, want SUCCEEDED attempts=1 last_error=nil", got)
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(newRoot))
	})

	runInFlightCase := func(t *testing.T, shouldCommit bool, maxAttempts int) (seededRelocateFixture, string, string, string, *jobs.Runner, *inFlightActivationTransactor) {
		t.Helper()
		caseDir := filepath.Join(canonBase, "inflight-"+domain.MustNewID())
		oldRoot := filepath.Join(caseDir, "old")
		newRoot := filepath.Join(caseDir, "new")
		fix := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldRoot, "")
		writeRelocateTestFile(t, filepath.Join(newRoot, "Reloc/v01.cbz"), fix.availData)

		jobID, err := roots.TriggerRelocateRoot(ctx, jobSvc, fix.lib.ID, fix.root.ID, []byte(newRoot), roots.TriggerRelocateOptions{
			MaxAttempts: maxAttempts,
		})
		if err != nil {
			t.Fatalf("TriggerRelocateRoot: %v", err)
		}

		faultTx := &inFlightActivationTransactor{
			inner:           transactor,
			testCtx:         ctx,
			shouldCommit:    shouldCommit,
			inFlightReady:   make(chan struct{}),
			releaseInFlight: make(chan struct{}),
			bgDone:          make(chan error, 1),
		}
		faultTx.injectInFlight.Store(true)
		defer faultTx.release()

		rel, err := roots.NewRelocator(faultTx, roots.RelocatorOptions{})
		if err != nil {
			t.Fatalf("NewRelocator: %v", err)
		}
		runner := newRootsRunner(t, jobRepo, rel)

		runDone := make(chan struct{})
		var claimed bool
		var runErr error
		go func() {
			defer close(runDone)
			claimed, runErr = runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot)
		}()

		select {
		case <-faultTx.inFlightReady:
		case <-runDone:
			t.Fatalf("ClaimAndRunNext exited before activation entered in-flight state: (%v, %v)", claimed, runErr)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for in-flight activation: %v", ctx.Err())
		}

		if err := waitForJobOutcomeLockWaiter(ctx, db, runDone); err != nil {
			gotNow, _ := jobRepo.GetJobByID(ctx, jobID)
			t.Fatalf("waitForJobOutcomeLockWaiter: %v (claimed=%v runErr=%v state=%+v)", err, claimed, runErr, gotNow)
		}
		faultTx.release()
		bgErr := <-faultTx.bgDone
		if shouldCommit && bgErr != nil {
			t.Fatalf("background activation commit err = %v", bgErr)
		}
		if !shouldCommit && bgErr == nil {
			t.Fatalf("expected rolled-back background activation error, got nil")
		}
		<-runDone

		if runErr != nil || !claimed {
			t.Fatalf("ClaimAndRunNext = (%v, %v), want (true, nil)", claimed, runErr)
		}
		return fix, jobID, oldRoot, newRoot, runner, faultTx
	}

	t.Run("in-flight activation commits while outcome handler waits on lock and settles SUCCEEDED", func(t *testing.T) {
		fix, jobID, _, newRoot, _, faultTx := runInFlightCase(t, true, 3)
		if faultTx.attemptsStarted.Load() != 1 {
			t.Fatalf("activation attemptsStarted = %d, want 1", faultTx.attemptsStarted.Load())
		}
		got, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJobByID: %v", err)
		}
		if got.State != domain.JobStateSucceeded || got.Attempts != 1 || got.LastError != nil {
			t.Fatalf("job = %+v, want SUCCEEDED attempts=1 last_error=nil", got)
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(newRoot))
	})

	t.Run("in-flight activation rolls back while outcome handler waits on lock then retries and succeeds", func(t *testing.T) {
		fix, jobID, oldRoot, newRoot, runner, faultTx := runInFlightCase(t, false, 2)
		mid, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJobByID(after rollback): %v", err)
		}
		if mid.State != domain.JobStateQueued || mid.Attempts != 1 || mid.LastError == nil {
			t.Fatalf("after rollback job = %+v, want QUEUED attempts=1 with error", mid)
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(oldRoot))

		if _, err := db.Exec(ctx, `UPDATE jobs SET run_after = NOW() WHERE id = $1`, jobID); err != nil {
			t.Fatalf("reset run_after: %v", err)
		}
		if claimed2, err := runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot); err != nil || !claimed2 {
			t.Fatalf("ClaimAndRunNext(attempt 2) = (%v, %v)", claimed2, err)
		}
		final, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJobByID(attempt 2): %v", err)
		}
		if final.State != domain.JobStateSucceeded || final.Attempts != 2 || final.LastError != nil {
			t.Fatalf("final job = %+v, want SUCCEEDED attempts=2 last_error=nil", final)
		}
		if faultTx.attemptsStarted.Load() != 2 {
			t.Fatalf("attemptsStarted = %d, want 2", faultTx.attemptsStarted.Load())
		}
		assertRelocateFixturePreserved(ctx, t, db, fix, []byte(newRoot))
	})
}

type inFlightActivationTransactor struct {
	inner             storage.Transactor
	testCtx           context.Context
	injectInFlight    atomic.Bool
	shouldCommit      bool
	attemptsStarted   atomic.Int32
	inFlightReady     chan struct{}
	inFlightReadyOnce sync.Once
	releaseInFlight   chan struct{}
	releaseOnce       sync.Once
	bgDone            chan error
}

func (f *inFlightActivationTransactor) release() {
	f.releaseOnce.Do(func() {
		close(f.releaseInFlight)
	})
}

func (f *inFlightActivationTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return f.inner.WithTx(ctx, fn)
}

func (f *inFlightActivationTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	var sawActivation bool
	var holdActivation bool
	err := f.inner.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		return fn(&inFlightDetectTx{
			Tx:            tx,
			sawActivation: &sawActivation,
			onCheckpoint: func() error {
				if !sawActivation || !f.injectInFlight.CompareAndSwap(true, false) {
					return nil
				}
				holdActivation = true
				return errors.New("yielding local tx to run held in-flight tx")
			},
		})
	})
	if !holdActivation {
		if sawActivation {
			f.attemptsStarted.Add(1)
		}
		return err
	}

	// Run the real activation inside an independent background transaction that holds
	// the jobs FOR UPDATE lock after UpdateLibraryRootPath + SaveCheckpoint until releaseInFlight.
	f.attemptsStarted.Add(1)
	go func() {
		bgCtx := f.testCtx
		if bgCtx == nil {
			bgCtx = context.WithoutCancel(ctx)
		}
		bgErr := f.inner.WithReadCommittedTx(bgCtx, func(bgTx storage.Tx) error {
			return fn(&inFlightBgTx{
				Tx: bgTx,
				afterSaveCheckpoint: func() error {
					f.inFlightReadyOnce.Do(func() { close(f.inFlightReady) })
					select {
					case <-f.releaseInFlight:
					case <-bgCtx.Done():
						return bgCtx.Err()
					}
					if !f.shouldCommit {
						return errors.New("simulated in-flight activation rollback")
					}
					return nil
				},
			})
		})
		f.bgDone <- bgErr
	}()

	select {
	case <-f.inFlightReady:
	case bgErr := <-f.bgDone:
		return fmt.Errorf("in-flight background tx ended early: %w", bgErr)
	case <-ctx.Done():
		return ctx.Err()
	}
	return errors.New("simulated connection drop while activation commit in flight")
}

type inFlightDetectTx struct {
	storage.Tx
	sawActivation *bool
	onCheckpoint  func() error
}

func (t *inFlightDetectTx) Libraries() storage.LibraryRepository {
	return &lostCommitAckLibRepo{
		LibraryRepository: t.Tx.Libraries(),
		sawActivation:     t.sawActivation,
	}
}

func (t *inFlightDetectTx) Jobs() storage.JobRepository {
	return &inFlightDetectJobRepo{
		JobRepository: t.Tx.Jobs(),
		onCheckpoint:  t.onCheckpoint,
	}
}

type inFlightDetectJobRepo struct {
	storage.JobRepository
	onCheckpoint func() error
}

func (r *inFlightDetectJobRepo) SaveCheckpoint(ctx context.Context, id string, attempt int, checkpoint json.RawMessage, now time.Time) (*domain.Job, error) {
	if err := r.onCheckpoint(); err != nil {
		return nil, err
	}
	return r.JobRepository.SaveCheckpoint(ctx, id, attempt, checkpoint, now)
}

type inFlightBgTx struct {
	storage.Tx
	afterSaveCheckpoint func() error
}

func (t *inFlightBgTx) Jobs() storage.JobRepository {
	return &inFlightBgJobRepo{
		JobRepository:       t.Tx.Jobs(),
		afterSaveCheckpoint: t.afterSaveCheckpoint,
	}
}

type inFlightBgJobRepo struct {
	storage.JobRepository
	afterSaveCheckpoint func() error
}

func (r *inFlightBgJobRepo) SaveCheckpoint(ctx context.Context, id string, attempt int, checkpoint json.RawMessage, now time.Time) (*domain.Job, error) {
	res, err := r.JobRepository.SaveCheckpoint(ctx, id, attempt, checkpoint, now)
	if err != nil {
		return nil, err
	}
	if err := r.afterSaveCheckpoint(); err != nil {
		return nil, err
	}
	return res, nil
}

func waitForJobOutcomeLockWaiter(ctx context.Context, db *testdb.TestDB, done <-chan struct{}) error {
	deadline := time.Now().Add(8 * time.Second)
	for {
		var count int
		if err := db.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock'
			  AND query ILIKE '%FROM jobs%FOR UPDATE%'
		`).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return nil
		}
		select {
		case <-done:
			return errors.New("runner outcome handling exited without waiting for jobs row lock")
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for outcome handler to block on jobs FOR UPDATE lock")
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func TestRelocateRoot_VerificationAndRecheckDoNotBlockOtherLibraryConfig(t *testing.T) {
	t.Parallel()
	db, transactor, svc, admin := setupConfigServiceTestDB(t, roots.ConfigServiceOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	oldA := filepath.Join(canonBase, "libA-old")
	newA := filepath.Join(canonBase, "libA-new")
	oldB := filepath.Join(canonBase, "libB-old")
	extraB1 := filepath.Join(canonBase, "libB-extra-1")
	extraB2 := filepath.Join(canonBase, "libB-extra-2")
	for _, dir := range []string{extraB1, extraB2} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", dir, err)
		}
	}

	fixA := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldA, "")
	fixB := seedRelocateFixture(ctx, t, db, transactor, svc, admin, domain.LibraryModeVirtual, oldB, "")
	availNewA := filepath.Join(newA, "Reloc/v01.cbz")
	writeRelocateTestFile(t, availNewA, fixA.availData)

	jobRepo := postgres.NewJobRepository(db.Pool)
	jobSvc := jobs.NewService(jobRepo, jobs.ServiceOptions{})
	jobID, err := roots.TriggerRelocateRoot(ctx, jobSvc, fixA.lib.ID, fixA.root.ID, []byte(newA), roots.TriggerRelocateOptions{})
	if err != nil {
		t.Fatalf("TriggerRelocateRoot: %v", err)
	}

	inOpenVerify := make(chan struct{})
	releaseOpenVerify := make(chan struct{})
	var openOnce sync.Once

	inAssetRecheck := make(chan struct{})
	releaseAssetRecheck := make(chan struct{})
	var recheckOnce sync.Once
	var openCompleted atomic.Bool

	rel, err := roots.NewRelocator(transactor, roots.RelocatorOptions{
		Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			if string(rawPath) == availNewA {
				openOnce.Do(func() {
					close(inOpenVerify)
					select {
					case <-releaseOpenVerify:
					case <-openCtx.Done():
					}
					openCompleted.Store(true)
				})
			}
			return os.Open(string(rawPath))
		},
		Stat: func(rawPath []byte) (os.FileInfo, error) {
			// Pause only on per-asset recheck after Open verification has completed.
			if openCompleted.Load() && string(rawPath) == availNewA {
				recheckOnce.Do(func() {
					close(inAssetRecheck)
					select {
					case <-releaseAssetRecheck:
					case <-ctx.Done():
					}
				})
			}
			return os.Stat(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("NewRelocator: %v", err)
	}
	runner := newRootsRunner(t, jobRepo, rel)

	runDone := make(chan struct{})
	var claimed bool
	var runErr error
	go func() {
		defer close(runDone)
		claimed, runErr = runner.ClaimAndRunNext(ctx, domain.JobKindRelocateRoot)
	}()

	// 1. Pause inside Open (fingerprint verification): unrelated libB config change must not block.
	select {
	case <-inOpenVerify:
	case <-runDone:
		t.Fatalf("relocator exited before Open pause: (%v, %v)", claimed, runErr)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Open pause: %v", ctx.Err())
	}
	if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: fixB.lib.ID,
		Path:      []byte(extraB1),
	}); err != nil {
		t.Fatalf("unrelated libB AddRoot blocked/failed during verification: %v", err)
	}
	close(releaseOpenVerify)

	// 2. Pause inside per-asset Stat recheck: unrelated libB config change must still not block.
	select {
	case <-inAssetRecheck:
	case <-runDone:
		t.Fatalf("relocator exited before per-asset Stat recheck pause: (%v, %v)", claimed, runErr)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for per-asset Stat recheck pause: %v", ctx.Err())
	}
	if _, err := svc.AddRoot(ctx, admin, app.AddLibraryRootRequest{
		LibraryID: fixB.lib.ID,
		Path:      []byte(extraB2),
	}); err != nil {
		t.Fatalf("unrelated libB AddRoot blocked/failed during per-asset recheck: %v", err)
	}
	close(releaseAssetRecheck)

	<-runDone
	if runErr != nil || !claimed {
		t.Fatalf("ClaimAndRunNext = (%v, %v), want (true, nil)", claimed, runErr)
	}
	got, err := jobRepo.GetJobByID(ctx, jobID)
	if err != nil || got.State != domain.JobStateSucceeded {
		t.Fatalf("relocate job = (%+v, %v), want SUCCEEDED", got, err)
	}
	assertRelocateFixturePreserved(ctx, t, db, fixA, []byte(newA))
}
