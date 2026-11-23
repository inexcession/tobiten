package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/api/middleware"
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

func TestRunDaemonRelocateRoot_HTTPAuthVisibilityAndConflict(t *testing.T) {
	originalNewRelocator := newDaemonRelocator
	originalNewScanner := newDaemonScanner
	originalLogger := slog.Default()
	t.Cleanup(func() {
		newDaemonRelocator = originalNewRelocator
		newDaemonScanner = originalNewScanner
		slog.SetDefault(originalLogger)
	})

	db := testdb.New(t)
	migrateStartupDB(t, db)
	dsPtr, _ := captureDaemonAssembly(t)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath: %v", err)
	}
	oldRootA := filepath.Join(canonBase, "http-libA-old")
	newRootA := filepath.Join(canonBase, "http-libA-new")
	rootB := filepath.Join(canonBase, "http-libB")
	newRootB := filepath.Join(canonBase, "http-libB-new")
	cbzA := buildTestCBZ(t, "RelocHTTP", "One", "1")
	cbzB := buildTestCBZ(t, "ScanConflict", "One", "1")
	writeTestFile(t, filepath.Join(oldRootA, "RelocHTTP/v01.cbz"), cbzA)
	writeTestFile(t, filepath.Join(newRootA, "RelocHTTP/v01.cbz"), cbzA)
	scanPauseFile := filepath.Join(rootB, "ScanConflict/v01.cbz")
	writeTestFile(t, scanPauseFile, cbzB)
	writeTestFile(t, filepath.Join(newRootB, "ScanConflict/v01.cbz"), cbzB)

	libA, rootA := seedTestLibrary(ctx, t, db.Pool, "HTTP Reloc A", oldRootA)
	libB, rootBRec := seedTestLibrary(ctx, t, db.Pool, "HTTP Reloc B", rootB)

	// Hold RELOCATE_ROOT mid-verification when armed so HTTP concurrency/visibility assertions run deterministically.
	var pauseRelocate atomic.Bool
	relocEntered := make(chan struct{}, 4)
	releaseReloc := make(chan struct{})
	newDaemonRelocator = func(pool *pgxpool.Pool, _ *config.Config) (*roots.Relocator, error) {
		return roots.NewRelocator(postgres.NewTransactor(pool), roots.RelocatorOptions{
			OnBeforeVerify: func(vCtx context.Context, _ domain.Job) error {
				if !pauseRelocate.Load() {
					return nil
				}
				select {
				case relocEntered <- struct{}{}:
				default:
				}
				select {
				case <-releaseReloc:
					return nil
				case <-vCtx.Done():
					return vCtx.Err()
				}
			},
		})
	}

	// Hold libB SCAN mid-open when armed so M1B-D7 active-SCAN conflict on POST /relocate is deterministic.
	var pauseScan atomic.Bool
	scanEntered := make(chan struct{}, 4)
	releaseScan := make(chan struct{})
	newDaemonScanner = func(pool *pgxpool.Pool, _ *config.Config) (*scanner.Scanner, error) {
		return scanner.NewScanner(postgres.NewTransactor(pool), scanner.Options{
			Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				if pauseScan.Load() && p == scanPauseFile {
					select {
					case scanEntered <- struct{}{}:
					default:
					}
					select {
					case <-releaseScan:
					case <-openCtx.Done():
						return nil, openCtx.Err()
					}
				}
				return os.Open(p)
			},
		})
	}

	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_ADMIN_USER=relocsysadmin",
		"TOBITEN_ADMIN_PASS=admin-secret-123",
		"TOBITEN_BASE_URL=http://localhost:8080/comics",
		"TOBITEN_LOG_FORMAT=json",
		"TOBITEN_SCANNER_WORKERS=1",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	userRepo := postgres.NewUserRepository(db.Pool)
	jobRepo := postgres.NewJobRepository(db.Pool)

	err = runDaemon(ctx, cfg, func(serveCtx context.Context, cfg *config.Config, logger *slog.Logger, services *app.Services) error {
		ds := *dsPtr
		reg := BuildRegistryWithServices(cfg, services)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		srv, err := api.NewServer(api.ServerOptions{
			Config:   cfg,
			Logger:   logger,
			Listener: ln,
			Registry: reg,
		})
		if err != nil {
			_ = ln.Close()
			return err
		}
		srvCtx, cancelSrv := context.WithCancel(serveCtx)
		srvDone := make(chan error, 1)
		go func() {
			srvDone <- srv.Run(srvCtx)
		}()
		defer func() {
			cancelSrv()
			if sErr := <-srvDone; sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
				t.Errorf("srv.Run: %v", sErr)
			}
		}()

		baseURL := srv.URL()
		httpClient := &http.Client{Timeout: 5 * time.Second}

		// Custom roles mirroring ADR-0017 §2 visibility:
		// - roleLibAdminNoJobsAll: library:read + library:admin + jobs:cancel_own (NO jobs:read_all)
		// - roleJobsReadAllOnly: library:read + jobs:read_all (NO library:admin)
		const (
			roleLibAdminNoJobsAllID = "019318a0-0000-7000-8000-000000000301"
			roleJobsReadAllOnlyID   = "019318a0-0000-7000-8000-000000000302"
		)
		if _, err := db.Exec(serveCtx, `
			INSERT INTO roles (id, name, built_in, requires_allow_labels) VALUES
				($1, 'Reloc Admin No ReadAll', FALSE, FALSE),
				($2, 'Reloc Auditor ReadAll', FALSE, FALSE)
		`, roleLibAdminNoJobsAllID, roleJobsReadAllOnlyID); err != nil {
			return err
		}
		if _, err := db.Exec(serveCtx, `
			INSERT INTO role_permissions (role_id, permission) VALUES
				($1, 'library:read'), ($1, 'library:admin'), ($1, 'jobs:cancel_own'),
				($2, 'library:read'), ($2, 'jobs:read_all')
		`, roleLibAdminNoJobsAllID, roleJobsReadAllOnlyID); err != nil {
			return err
		}

		sysadmin, err := userRepo.GetUserByUsername(serveCtx, "relocsysadmin")
		if err != nil {
			return err
		}
		creator := createTestUserWithPassword(serveCtx, t, userRepo, "reloc-creator", "creator-pass-123", roleLibAdminNoJobsAllID)
		otherAdmin := createTestUserWithPassword(serveCtx, t, userRepo, "reloc-other", "other-pass-123", roleLibAdminNoJobsAllID)
		auditor := createTestUserWithPassword(serveCtx, t, userRepo, "reloc-auditor", "auditor-pass-123", roleJobsReadAllOnlyID)
		reader := createTestUserWithPassword(serveCtx, t, userRepo, "reloc-reader", "reader-pass-123", domain.RoleReaderID)
		curator := createTestUserWithPassword(serveCtx, t, userRepo, "reloc-curator", "curator-pass-123", domain.RoleCuratorID)

		sysReadPAT := createTestPAT(serveCtx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeRead})
		sysLibAdminPAT := createTestPAT(serveCtx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeLibraryAdmin})
		sysAdminPAT := createTestPAT(serveCtx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeAdmin})

		doReq := func(method, path, body string, mutate func(*http.Request)) (int, http.Header, string) {
			var bodyReader io.Reader
			if body != "" {
				bodyReader = strings.NewReader(body)
			}
			req, reqErr := http.NewRequestWithContext(serveCtx, method, baseURL+path, bodyReader)
			if reqErr != nil {
				t.Fatalf("NewRequest(%s %s): %v", method, path, reqErr)
			}
			if body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			if mutate != nil {
				mutate(req)
			}
			resp, doErr := httpClient.Do(req)
			if doErr != nil {
				t.Fatalf("%s %s: %v", method, path, doErr)
			}
			defer resp.Body.Close()
			raw, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				t.Fatalf("ReadAll(%s %s): %v", method, path, readErr)
			}
			return resp.StatusCode, resp.Header, string(raw)
		}

		// Cookie login for creator to test CSRF enforcement and valid cookie-triggered relocate.
		loginCode, loginHdr, loginBody := doReq(http.MethodPost, "/api/v1/auth/login", `{"username":"reloc-creator","password":"creator-pass-123"}`, nil)
		if loginCode != http.StatusOK {
			return fmt.Errorf("creator login = %d (%s), want 200", loginCode, loginBody)
		}
		var creatorCookieVal string
		for _, sc := range loginHdr.Values("Set-Cookie") {
			if strings.HasPrefix(sc, app.NativeSessionCookieName+"=") {
				parts := strings.SplitN(sc, ";", 2)
				creatorCookieVal = strings.TrimPrefix(parts[0], app.NativeSessionCookieName+"=")
			}
		}
		creatorCSRF := loginHdr.Get("X-CSRF-Token")
		if creatorCookieVal == "" || creatorCSRF == "" {
			return errors.New("missing session cookie or CSRF token")
		}
		creatorCookie := &http.Cookie{Name: app.NativeSessionCookieName, Value: creatorCookieVal}

		// 1. Unauthenticated POST /relocate returns 401 (direct + subpath).
		for _, p := range []string{
			"/api/v1/admin/roots/" + rootA.ID + "/relocate",
			"/comics/api/v1/admin/roots/" + rootA.ID + "/relocate",
		} {
			code, hdr, _ := doReq(http.MethodPost, p, `{"path":"`+newRootA+`"}`, nil)
			if code != http.StatusUnauthorized || !strings.HasPrefix(hdr.Get("WWW-Authenticate"), "Basic") {
				return fmt.Errorf("unauthenticated POST %s = (%d, WWW-Authenticate=%q), want (401, Basic)", p, code, hdr.Get("WWW-Authenticate"))
			}
		}

		// 2. Missing library:admin or missing CSRF returns 403 and creates zero jobs.
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"reader basic", func(r *http.Request) { r.SetBasicAuth(reader.Username, "reader-pass-123") }},
			{"curator basic", func(r *http.Request) { r.SetBasicAuth(curator.Username, "curator-pass-123") }},
			{"auditor jobs:read_all basic", func(r *http.Request) { r.SetBasicAuth(auditor.Username, "auditor-pass-123") }},
			{"sysadmin read PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+sysReadPAT) }},
			{"cookie without CSRF", func(r *http.Request) { r.AddCookie(creatorCookie) }},
		} {
			code, _, body := doReq(http.MethodPost, "/api/v1/admin/roots/"+rootA.ID+"/relocate", `{"path":"`+newRootA+`"}`, tc.mutate)
			if code != http.StatusForbidden {
				return fmt.Errorf("%s POST /relocate = %d (%s), want 403", tc.name, code, body)
			}
		}

		// 3. Unknown root returns 404; overlapping target returns 409; 0 jobs created so far.
		code404, _, _ := doReq(http.MethodPost, "/api/v1/admin/roots/missing-root-id/relocate", `{"path":"`+newRootA+`"}`, func(r *http.Request) {
			r.SetBasicAuth(creator.Username, "creator-pass-123")
		})
		if code404 != http.StatusNotFound {
			return fmt.Errorf("unknown root POST /relocate = %d, want 404", code404)
		}
		codeOverlap, _, _ := doReq(http.MethodPost, "/api/v1/admin/roots/"+rootA.ID+"/relocate", `{"path":"`+rootB+`"}`, func(r *http.Request) {
			r.SetBasicAuth(creator.Username, "creator-pass-123")
		})
		if codeOverlap != http.StatusConflict {
			return fmt.Errorf("overlapping target POST /relocate = %d, want 409", codeOverlap)
		}
		var preJobs int
		if err := db.QueryRow(serveCtx, `SELECT COUNT(*) FROM jobs`).Scan(&preJobs); err != nil {
			return err
		}
		if preJobs != 0 {
			return fmt.Errorf("expected 0 jobs after denied/404/overlap requests, got %d", preJobs)
		}

		// 4. Initial scan on libA to create media_asset + asset_digest + reading_progress.
		initScanCode, _, initScanBody := doReq(http.MethodPost, "/api/v1/libraries/"+libA.ID+"/scan", "", func(r *http.Request) {
			r.SetBasicAuth(creator.Username, "creator-pass-123")
		})
		if initScanCode != http.StatusAccepted {
			return fmt.Errorf("initial POST /scan = %d (%s), want 202", initScanCode, initScanBody)
		}
		var initScanResp middleware.JobAcceptedResponse
		mustUnmarshal(t, initScanBody, &initScanResp)
		waitDaemonJobState(serveCtx, t, jobRepo, initScanResp.JobID, domain.JobStateSucceeded)

		var volID, assetID string
		if err := db.QueryRow(serveCtx, `SELECT id, volume_id FROM media_assets WHERE library_root_id = $1`, rootA.ID).Scan(&assetID, &volID); err != nil {
			return err
		}
		if _, err := db.Exec(serveCtx, `
			INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
			VALUES ($1, $2, '{"type":"page","page":2}'::jsonb, 0.4, 'KOMGA', false)
		`, reader.ID, volID); err != nil {
			return err
		}

		// 5. Creator triggers relocate via subpath + cookie + CSRF while paused at OnBeforeVerify -> 202 with job_id.
		pauseRelocate.Store(true)
		relocCode, relocHdr, relocBody := doReq(http.MethodPost, "/comics/api/v1/admin/roots/"+rootA.ID+"/relocate", `{"path":"`+newRootA+`"}`, func(r *http.Request) {
			r.AddCookie(creatorCookie)
			r.Header.Set("X-CSRF-Token", creatorCSRF)
		})
		if relocCode != http.StatusAccepted {
			return fmt.Errorf("creator POST /relocate = %d (%s), want 202", relocCode, relocBody)
		}
		if relocHdr.Get("Cache-Control") != "no-store" {
			return fmt.Errorf("POST /relocate Cache-Control = %q, want no-store", relocHdr.Get("Cache-Control"))
		}
		var relocResp middleware.JobAcceptedResponse
		mustUnmarshal(t, relocBody, &relocResp)
		if relocResp.JobID == "" {
			return errors.New("expected non-empty job_id from POST /relocate")
		}
		relocJobID := relocResp.JobID

		select {
		case <-relocEntered:
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for RELOCATE_ROOT worker to enter verification")
		}

		// M1B-D7: active RELOCATE_ROOT rejects another relocate AND scan on the same library with 409.
		if c, _, b := doReq(http.MethodPost, "/api/v1/admin/roots/"+rootA.ID+"/relocate", `{"path":"`+newRootA+`"}`, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+sysLibAdminPAT)
		}); c != http.StatusConflict {
			return fmt.Errorf("duplicate POST /relocate while RUNNING = %d (%s), want 409", c, b)
		}
		if c, _, b := doReq(http.MethodPost, "/api/v1/libraries/"+libA.ID+"/scan", "", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+sysLibAdminPAT)
		}); c != http.StatusConflict {
			return fmt.Errorf("POST /scan during active RELOCATE_ROOT = %d (%s), want 409", c, b)
		}

		// ADR-0017 §2 job visibility: creator and jobs:read_all see RUNNING job; other library:admin and reader get 404.
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"creator", func(r *http.Request) { r.SetBasicAuth(creator.Username, "creator-pass-123") }},
			{"auditor", func(r *http.Request) { r.SetBasicAuth(auditor.Username, "auditor-pass-123") }},
		} {
			c, _, b := doReq(http.MethodGet, "/api/v1/jobs/"+relocJobID, "", tc.mutate)
			if c != http.StatusOK {
				return fmt.Errorf("%s GET /jobs/%s = %d (%s), want 200", tc.name, relocJobID, c, b)
			}
			var info app.JobInfo
			mustUnmarshal(t, b, &info)
			if info.ID != relocJobID || info.Kind != string(domain.JobKindRelocateRoot) || info.State != string(domain.JobStateRunning) ||
				info.CreatedBy == nil || *info.CreatedBy != creator.ID {
				return fmt.Errorf("%s GET /jobs/%s snapshot = %+v", tc.name, relocJobID, info)
			}
		}
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"otherAdmin (no jobs:read_all)", func(r *http.Request) { r.SetBasicAuth(otherAdmin.Username, "other-pass-123") }},
			{"reader", func(r *http.Request) { r.SetBasicAuth(reader.Username, "reader-pass-123") }},
		} {
			if c, _, _ := doReq(http.MethodGet, "/api/v1/jobs/"+relocJobID, "", tc.mutate); c != http.StatusNotFound {
				return fmt.Errorf("%s GET /jobs/%s = %d, want 404", tc.name, relocJobID, c)
			}
		}

		// Release RELOCATE_ROOT and verify SUCCEEDED, updated root path, and preserved asset + reading_progress.
		pauseRelocate.Store(false)
		close(releaseReloc)
		waitDaemonJobState(serveCtx, t, jobRepo, relocJobID, domain.JobStateSucceeded)

		gotRootA, err := postgres.NewLibraryRepository(db.Pool).GetLibraryRootByID(serveCtx, rootA.ID)
		if err != nil {
			return err
		}
		if !bytes.Equal(gotRootA.Path.Bytes, []byte(newRootA)) {
			return fmt.Errorf("relocated rootA path = %q, want %q", gotRootA.Path.Bytes, newRootA)
		}
		var postAssetID string
		if err := db.QueryRow(serveCtx, `SELECT id FROM media_assets WHERE library_root_id = $1`, rootA.ID).Scan(&postAssetID); err != nil {
			return err
		}
		if postAssetID != assetID {
			return fmt.Errorf("asset ID changed across relocate: %q != %q", postAssetID, assetID)
		}
		var prog float64
		if err := db.QueryRow(serveCtx, `SELECT total_progression FROM reading_progress WHERE user_id = $1 AND volume_id = $2`, reader.ID, volID).Scan(&prog); err != nil {
			return err
		}
		if prog < 0.39 || prog > 0.41 {
			return fmt.Errorf("reading_progress = %v, want 0.4", prog)
		}

		// 6. Symmetric M1B-D7 conflict: active SCAN on libB rejects POST /relocate for rootB with 409.
		pauseScan.Store(true)
		scanBCode, _, scanBBody := doReq(http.MethodPost, "/api/v1/libraries/"+libB.ID+"/scan", "", func(r *http.Request) {
			r.SetBasicAuth(otherAdmin.Username, "other-pass-123")
		})
		if scanBCode != http.StatusAccepted {
			return fmt.Errorf("POST /scan(libB) = %d (%s), want 202", scanBCode, scanBBody)
		}
		var scanBResp middleware.JobAcceptedResponse
		mustUnmarshal(t, scanBBody, &scanBResp)

		select {
		case <-scanEntered:
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for libB scan to enter file open")
		}

		if c, _, b := doReq(http.MethodPost, "/api/v1/admin/roots/"+rootBRec.ID+"/relocate", `{"path":"`+newRootB+`"}`, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+sysLibAdminPAT)
		}); c != http.StatusConflict {
			return fmt.Errorf("POST /relocate during active SCAN = %d (%s), want 409", c, b)
		}

		pauseScan.Store(false)
		close(releaseScan)
		waitDaemonJobState(serveCtx, t, jobRepo, scanBResp.JobID, domain.JobStateSucceeded)

		// 7. Creatorless RELOCATE_ROOT (created_by = NULL) is visible only to jobs:read_all (ADR-0017 §2).
		autoRelocID, err := roots.TriggerRelocateRoot(serveCtx, ds.jobs, libB.ID, rootBRec.ID, []byte(newRootB), roots.TriggerRelocateOptions{})
		if err != nil {
			return fmt.Errorf("TriggerRelocateRoot(creatorless): %w", err)
		}
		waitDaemonJobState(serveCtx, t, jobRepo, autoRelocID, domain.JobStateSucceeded)

		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"auditor", func(r *http.Request) { r.SetBasicAuth(auditor.Username, "auditor-pass-123") }},
			{"sysadmin admin PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+sysAdminPAT) }},
		} {
			c, _, b := doReq(http.MethodGet, "/api/v1/jobs/"+autoRelocID, "", tc.mutate)
			if c != http.StatusOK {
				return fmt.Errorf("%s GET creatorless relocate job = %d (%s), want 200", tc.name, c, b)
			}
			var autoInfo app.JobInfo
			mustUnmarshal(t, b, &autoInfo)
			if autoInfo.CreatedBy != nil {
				return fmt.Errorf("creatorless relocate job CreatedBy = %v, want nil", *autoInfo.CreatedBy)
			}
		}
		for _, tc := range []struct {
			name   string
			mutate func(*http.Request)
		}{
			{"creator (library:admin without jobs:read_all)", func(r *http.Request) { r.SetBasicAuth(creator.Username, "creator-pass-123") }},
			{"sysadmin library:admin PAT", func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+sysLibAdminPAT) }},
			{"reader", func(r *http.Request) { r.SetBasicAuth(reader.Username, "reader-pass-123") }},
		} {
			if c, _, _ := doReq(http.MethodGet, "/api/v1/jobs/"+autoRelocID, "", tc.mutate); c != http.StatusNotFound {
				return fmt.Errorf("%s GET creatorless relocate job = %d, want 404", tc.name, c)
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon err = %v", err)
	}
}
