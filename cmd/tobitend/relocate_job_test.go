package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/library/roots"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedAdminUserForRelocateTest(ctx context.Context, t *testing.T, pool *pgxpool.Pool) *domain.Principal {
	t.Helper()
	userRepo := postgres.NewUserRepository(pool)
	u := domain.User{ID: domain.MustNewID(), Username: "reloc-admin-" + domain.MustNewID()[:8]}
	if err := userRepo.CreateUser(ctx, &u); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := userRepo.AssignUserRole(ctx, u.ID, domain.RoleAdminID); err != nil {
		t.Fatalf("AssignUserRole: %v", err)
	}
	var adminRole domain.Role
	for _, r := range domain.BuiltInRoles() {
		if r.ID == domain.RoleAdminID {
			adminRole = r
			break
		}
	}
	return domain.NewPrincipal(u, []domain.Role{adminRole}, domain.CredentialKindPassword, "cred-"+u.ID, nil)
}

func TestRunDaemonRelocateRoot_ProductionWiringExclusionAndPostRelocateScan(t *testing.T) {
	originalNewRelocator := newDaemonRelocator
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonRelocator = originalNewRelocator
		slog.SetDefault(originalLogger)
	})

	db := testdb.New(t)
	migrateStartupDB(t, db)
	dsPtr, workersPtr := captureDaemonAssembly(t)

	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	oldRootA := filepath.Join(canonBase, "libA-old")
	newRootA := filepath.Join(canonBase, "libA-new")
	rootB := filepath.Join(canonBase, "libB")
	cbzA := buildTestCBZ(t, "RelocDaemon", "One", "1")
	cbzB := buildTestCBZ(t, "OtherDaemon", "One", "1")
	writeTestFile(t, filepath.Join(oldRootA, "RelocDaemon/v01.cbz"), cbzA)
	writeTestFile(t, filepath.Join(newRootA, "RelocDaemon/v01.cbz"), cbzA)
	writeTestFile(t, filepath.Join(rootB, "OtherDaemon/v01.cbz"), cbzB)

	libA, rootA := seedTestLibrary(ctx, t, db.Pool, "Daemon Reloc A", oldRootA)
	libB, _ := seedTestLibrary(ctx, t, db.Pool, "Daemon Reloc B", rootB)
	admin := seedAdminUserForRelocateTest(ctx, t, db.Pool)

	// Hold RELOCATE_ROOT mid-verification briefly to test live RUNNING exclusion via production services.
	enteredVerify := make(chan struct{}, 1)
	releaseVerify := make(chan struct{})
	newDaemonRelocator = func(pool *pgxpool.Pool, _ *config.Config) (*roots.Relocator, error) {
		return roots.NewRelocator(postgres.NewTransactor(pool), roots.RelocatorOptions{
			OnBeforeVerify: func(vCtx context.Context, _ domain.Job) error {
				select {
				case enteredVerify <- struct{}{}:
				default:
				}
				select {
				case <-releaseVerify:
					return nil
				case <-vCtx.Done():
					return vCtx.Err()
				}
			},
		})
	}

	jobRepo := postgres.NewJobRepository(db.Pool)
	err = runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, appSvcs *app.Services) error {
		workers := *workersPtr
		ds := *dsPtr
		if sz, active := workers.runner.ActivePoolSize(domain.JobKindRelocateRoot); !active || sz != jobs.DefaultWorkerPoolSize {
			return fmt.Errorf("RELOCATE_ROOT ActivePoolSize = (%d, %v), want (%d, true)", sz, active, jobs.DefaultWorkerPoolSize)
		}

		// Initial scan on libA to create media_asset + digest + reading_progress.
		initScanID, err := appSvcs.Library.ScanLibrary(serveCtx, admin, libA.ID)
		if err != nil {
			return fmt.Errorf("initial ScanLibrary: %w", err)
		}
		waitDaemonJobState(serveCtx, t, jobRepo, initScanID, domain.JobStateSucceeded)

		var volID, assetID string
		if err := db.QueryRow(serveCtx, `SELECT id, volume_id FROM media_assets WHERE library_root_id = $1`, rootA.ID).Scan(&assetID, &volID); err != nil {
			return err
		}
		if _, err := db.Exec(serveCtx, `
			INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
			VALUES ($1, $2, '{"type":"page","page":3}'::jsonb, 0.5, 'KOMGA', false)
		`, admin.User.ID, volID); err != nil {
			return err
		}

		// Trigger RELOCATE_ROOT through LibraryConfigService; worker pauses at OnBeforeVerify.
		relocJobID, err := appSvcs.LibraryConfig.RelocateRoot(serveCtx, admin, app.RelocateRootRequest{
			RootID: rootA.ID,
			Path:   []byte(newRootA),
		})
		if err != nil {
			return fmt.Errorf("RelocateRoot: %w", err)
		}

		select {
		case <-enteredVerify:
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for RELOCATE_ROOT worker to enter verification")
		}

		// Same-library ScanLibrary and second RelocateRoot must fail with app.ErrConflict.
		if _, err := appSvcs.Library.ScanLibrary(serveCtx, admin, libA.ID); !errors.Is(err, app.ErrConflict) {
			return fmt.Errorf("ScanLibrary during active RELOCATE_ROOT = %v, want ErrConflict", err)
		}
		if _, err := appSvcs.LibraryConfig.RelocateRoot(serveCtx, admin, app.RelocateRootRequest{
			RootID: rootA.ID,
			Path:   []byte(newRootA),
		}); !errors.Is(err, app.ErrConflict) {
			return fmt.Errorf("second RelocateRoot during active RELOCATE_ROOT = %v, want ErrConflict", err)
		}

		// Independent library B must still scan and succeed while libA is relocating.
		scanBID, err := ds.triggerScan(serveCtx, libB.ID, scanner.TriggerScanOptions{})
		if err != nil {
			return fmt.Errorf("independent libB scan failed: %w", err)
		}
		waitDaemonJobState(serveCtx, t, jobRepo, scanBID, domain.JobStateSucceeded)

		close(releaseVerify)
		waitDaemonJobState(serveCtx, t, jobRepo, relocJobID, domain.JobStateSucceeded)

		// Verify root updated on same row, asset & reading_progress preserved, and post-relocate scan succeeds.
		gotRoot, err := postgres.NewLibraryRepository(db.Pool).GetLibraryRootByID(serveCtx, rootA.ID)
		if err != nil {
			return err
		}
		if !bytes.Equal(gotRoot.Path.Bytes, []byte(newRootA)) {
			return fmt.Errorf("relocated path = %q, want %q", gotRoot.Path.Bytes, newRootA)
		}
		var postScanAssetID string
		if err := db.QueryRow(serveCtx, `SELECT id FROM media_assets WHERE library_root_id = $1`, rootA.ID).Scan(&postScanAssetID); err != nil {
			return err
		}
		if postScanAssetID != assetID {
			return fmt.Errorf("asset ID changed across relocate: %q != %q", postScanAssetID, assetID)
		}
		var prog float64
		if err := db.QueryRow(serveCtx, `SELECT total_progression FROM reading_progress WHERE user_id = $1 AND volume_id = $2`, admin.User.ID, volID).Scan(&prog); err != nil {
			return err
		}
		if prog < 0.49 || prog > 0.51 {
			return fmt.Errorf("reading_progress = %v, want 0.5", prog)
		}

		postScanID, err := appSvcs.Library.ScanLibrary(serveCtx, admin, libA.ID)
		if err != nil {
			return fmt.Errorf("post-relocate ScanLibrary: %w", err)
		}
		waitDaemonJobState(serveCtx, t, jobRepo, postScanID, domain.JobStateSucceeded)
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}
}

func TestRelocateKillRecoveryHelperProcess(t *testing.T) {
	mode := os.Getenv("TOBITEN_RELOC_KILL_MODE")
	if mode == "" {
		return
	}
	dbURL := os.Getenv("TOBITEN_RELOC_KILL_DB_URL")
	libID := os.Getenv("TOBITEN_RELOC_KILL_LIB_ID")
	rootID := os.Getenv("TOBITEN_RELOC_KILL_ROOT_ID")
	newRoot := os.Getenv("TOBITEN_RELOC_KILL_TARGET")
	if dbURL == "" || libID == "" || rootID == "" || newRoot == "" {
		t.Fatal("missing TOBITEN_RELOC_KILL_* environment variables")
	}

	originalNewRelocator := newDaemonRelocator
	t.Cleanup(func() {
		newDaemonRelocator = originalNewRelocator
	})

	readyToKill := make(chan struct{})
	var once sync.Once
	signalReady := func() {
		once.Do(func() { close(readyToKill) })
	}

	newDaemonRelocator = func(pool *pgxpool.Pool, _ *config.Config) (*roots.Relocator, error) {
		opts := roots.RelocatorOptions{}
		switch mode {
		case "crash-before-activate":
			opts.OnAfterVerify = func(vCtx context.Context, _ domain.Job) error {
				signalReady()
				<-vCtx.Done()
				return vCtx.Err()
			}
		case "crash-after-activate":
			opts.OnAfterActivate = func(aCtx context.Context, _ domain.Job) error {
				signalReady()
				<-aCtx.Done()
				return aCtx.Err()
			}
		default:
			return nil, fmt.Errorf("unknown helper mode %q", mode)
		}
		return roots.NewRelocator(postgres.NewTransactor(pool), opts)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg := hostedConfig(t, dbURL)
	if err := runDaemon(ctx, cfg, func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, appSvcs *app.Services) error {
		jobRepo := postgres.NewJobRepository(nil)
		_ = jobRepo
		admin := &domain.Principal{
			User:        domain.User{ID: "", Username: "sys"},
			Permissions: []domain.Permission{domain.PermissionLibraryAdmin},
		}
		jobID, err := appSvcs.LibraryConfig.RelocateRoot(serveCtx, admin, app.RelocateRootRequest{
			RootID: rootID,
			Path:   []byte(newRoot),
		})
		if err != nil {
			return err
		}
		select {
		case <-readyToKill:
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for relocate helper ready")
		}
		fmt.Printf("RELOC_HELPER_READY %s\n", jobID)
		<-serveCtx.Done()
		return nil
	}); err != nil {
		t.Fatalf("helper runDaemon: %v", err)
	}
}

func TestRunDaemonRelocateRoot_SIGKILLBothSidesRecovery(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)

	exePath, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	jobRepo := postgres.NewJobRepository(db.Pool)

	runKillCase := func(t *testing.T, mode string, removeOldAfterKill bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
		defer cancel()

		base := t.TempDir()
		canonBase, err := paths.CanonicalPath(base)
		if err != nil {
			t.Fatalf("CanonicalPath: %v", err)
		}
		oldRoot := filepath.Join(canonBase, "old")
		newRoot := filepath.Join(canonBase, "new")
		cbz := buildTestCBZ(t, "KillSaga", "One", "1")
		writeTestFile(t, filepath.Join(oldRoot, "KillSaga/v01.cbz"), cbz)
		writeTestFile(t, filepath.Join(newRoot, "KillSaga/v01.cbz"), cbz)

		lib, root := seedTestLibrary(ctx, t, db.Pool, "Kill Lib "+mode, oldRoot)
		sc, err := scanner.NewScanner(postgres.NewTransactor(db.Pool), scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		if res, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil || !res.Complete || res.IndexedCount != 1 {
			t.Fatalf("seed ScanLibrary = (%+v, %v)", res, err)
		}

		cmd := exec.Command(exePath, "-test.run=^TestRelocateKillRecoveryHelperProcess$")
		cmd.Env = append(os.Environ(),
			"TOBITEN_TEST_DATABASE_URL="+db.URL,
			"TOBITEN_RELOC_KILL_MODE="+mode,
			"TOBITEN_RELOC_KILL_DB_URL="+db.URL,
			"TOBITEN_RELOC_KILL_LIB_ID="+lib.ID,
			"TOBITEN_RELOC_KILL_ROOT_ID="+root.ID,
			"TOBITEN_RELOC_KILL_TARGET="+newRoot,
		)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("StdoutPipe: %v", err)
		}
		var stderr syncBuffer
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start helper: %v", err)
		}
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		})

		readyCh := make(chan string, 1)
		go func() {
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				line := strings.TrimSpace(sc.Text())
				if id, ok := strings.CutPrefix(line, "RELOC_HELPER_READY "); ok {
					readyCh <- id
					return
				}
			}
			readyCh <- ""
		}()

		var jobID string
		select {
		case jobID = <-readyCh:
			if jobID == "" {
				_ = cmd.Wait()
				t.Fatalf("helper exited before ready; stderr:\n%s", stderr.String())
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("timed out waiting for helper ready; stderr:\n%s", stderr.String())
		}

		if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
			t.Fatalf("SIGKILL helper: %v", err)
		}
		if waitErr := cmd.Wait(); waitErr == nil {
			t.Fatal("expected non-nil error after SIGKILL")
		}

		if removeOldAfterKill {
			if err := os.RemoveAll(oldRoot); err != nil {
				t.Fatalf("RemoveAll(oldRoot): %v", err)
			}
		}

		mid, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil || mid.State != domain.JobStateRunning || mid.Attempts != 1 {
			t.Fatalf("interrupted job after SIGKILL = (%+v, %v), want RUNNING attempts=1", mid, err)
		}

		// Start normal daemon: startup recovery completes before serving and settles the job cleanly.
		if err := runDaemon(ctx, hostedConfig(t, db.URL), func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
			got := waitDaemonJobState(serveCtx, t, jobRepo, jobID, domain.JobStateSucceeded)
			wantAttempts := 2
			if mode == "crash-after-activate" {
				wantAttempts = 1
			}
			if got.Attempts != wantAttempts {
				return fmt.Errorf("mode %s final Attempts = %d, want %d", mode, got.Attempts, wantAttempts)
			}
			return nil
		}); err != nil {
			t.Fatalf("recovery runDaemon(%s): %v", mode, err)
		}

		gotRoot, err := postgres.NewLibraryRepository(db.Pool).GetLibraryRootByID(ctx, root.ID)
		if err != nil {
			t.Fatalf("GetLibraryRootByID: %v", err)
		}
		if !bytes.Equal(gotRoot.Path.Bytes, []byte(newRoot)) {
			t.Fatalf("final root path = %q, want %q", gotRoot.Path.Bytes, newRoot)
		}
	}

	t.Run("SIGKILL before activation requeues and succeeds", func(t *testing.T) {
		runKillCase(t, "crash-before-activate", false)
	})
	t.Run("SIGKILL after activation reconciles directly to SUCCEEDED", func(t *testing.T) {
		runKillCase(t, "crash-after-activate", true)
	})
}
