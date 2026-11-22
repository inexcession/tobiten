package scanner_test

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/container/cbz"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/events"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
)

func TestScanModuleMetadataAndTriggerDedupe(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx := t.Context()

	if _, err := scanner.NewModule(nil, scanner.ModuleOptions{}); err == nil {
		t.Fatal("NewModule(nil) expected error")
	}
	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := scanner.NewModule(sc, scanner.ModuleOptions{Workers: -1}); !errors.Is(err, scanner.ErrInvalidWorkerCount) {
		t.Fatalf("NewModule(Workers=-1) err = %v, want ErrInvalidWorkerCount", err)
	}

	mod, err := scanner.NewModule(sc, scanner.ModuleOptions{})
	if err != nil {
		t.Fatalf("NewModule: %v", err)
	}
	if mod.Name() != scanner.ModuleName {
		t.Fatalf("Name() = %q, want %q", mod.Name(), scanner.ModuleName)
	}
	if kinds := mod.JobKinds(); len(kinds) != 1 || kinds[0].Name() != string(domain.JobKindScan) || kinds[0].ResumePolicy() != domain.JobResumeRestart {
		t.Fatalf("JobKinds() = %+v, want [SCAN/RESTART]", kinds)
	}
	if fmts := mod.ContainerFormats(); len(fmts) != 1 || fmts[0].Name != cbz.FormatName {
		t.Fatalf("ContainerFormats() = %+v, want [%s]", fmts, cbz.FormatName)
	}

	jobRepo := postgres.NewJobRepository(db.Pool)
	jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

	if _, err := scanner.TriggerScan(ctx, nil, "lib-1", scanner.TriggerScanOptions{}); !errors.Is(err, app.ErrServiceBusy) {
		t.Fatalf("TriggerScan(nil) err = %v, want ErrServiceBusy", err)
	}
	if _, err := scanner.TriggerScan(ctx, jobService, "   ", scanner.TriggerScanOptions{}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("TriggerScan(blank) err = %v, want ErrInvalidInput", err)
	}
	if _, err := scanner.TriggerScan(ctx, jobService, " lib-1 ", scanner.TriggerScanOptions{}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("TriggerScan(whitespace-padded) err = %v, want ErrInvalidInput", err)
	}

	id1, err := scanner.TriggerScan(ctx, jobService, "lib-a", scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan(lib-a) #1: %v", err)
	}
	id2, err := scanner.TriggerScan(ctx, jobService, "lib-a", scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan(lib-a) #2: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("duplicate TriggerScan(lib-a) returned %q, want %q", id2, id1)
	}
	idB, err := scanner.TriggerScan(ctx, jobService, "lib-b", scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan(lib-b): %v", err)
	}
	if idB == id1 {
		t.Fatalf("TriggerScan(lib-b) collided with lib-a job ID %q", idB)
	}

	jA, err := jobRepo.GetJobByID(ctx, id1)
	if err != nil {
		t.Fatalf("GetJobByID(%s): %v", id1, err)
	}
	if jA.DedupeKey == nil || *jA.DedupeKey != "SCAN:lib-a" {
		t.Fatalf("lib-a DedupeKey = %v, want SCAN:lib-a", jA.DedupeKey)
	}
	var payload scanner.ScanJobPayload
	if err := json.Unmarshal(jA.Payload, &payload); err != nil || payload.LibraryID != "lib-a" {
		t.Fatalf("lib-a Payload = %s (err=%v), want library_id=lib-a", jA.Payload, err)
	}
}

func TestScanModuleRunner_CatalogueProgressMissingAndPermanentFailures(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	cbz1 := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Job Saga</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-1"), method: zip.Store},
	)
	cbz2 := buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Job Saga</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("page-2"), method: zip.Store},
	)
	file1 := filepath.Join(rootDir, "Job Saga/v01.cbz")
	file2 := filepath.Join(rootDir, "Job Saga/v02.cbz")
	writeDiskFile(t, file1, cbz1)
	writeDiskFile(t, file2, cbz2)

	var lib domain.Library
	var root domain.LibraryRoot
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Runner Scan Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seedLibraryAndRoots: %v", err)
	}

	sc, err := scanner.NewScanner(transactor, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	mod, err := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 2})
	if err != nil {
		t.Fatalf("NewModule: %v", err)
	}

	reg := module.NewRegistry()
	if err := reg.Register(mod); err != nil {
		t.Fatalf("reg.Register: %v", err)
	}

	hub, err := events.NewProgressHub(events.ProgressHubOptions{MinInterval: time.Nanosecond})
	if err != nil {
		t.Fatalf("NewProgressHub: %v", err)
	}
	adminPrincipal := &domain.Principal{
		User:        domain.User{ID: "admin-1", Username: "admin"},
		Permissions: []domain.Permission{domain.PermissionJobsReadAll},
	}
	tickCh, unsubTicks, err := hub.Subscribe(ctx, events.ProgressSubscribeOptions{
		Principal:  adminPrincipal,
		BufferSize: 16,
	})
	if err != nil {
		t.Fatalf("hub.Subscribe: %v", err)
	}
	defer unsubTicks()

	jobRepo := postgres.NewJobRepository(db.Pool)
	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{ProgressHub: hub})
	if err := runner.RegisterModuleKinds(reg); err != nil {
		t.Fatalf("RegisterModuleKinds: %v", err)
	}
	if err := runner.RegisterModuleHandlers(reg); err != nil {
		t.Fatalf("RegisterModuleHandlers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

	// 1. Trigger and run scan -> SUCCEEDED, catalogue populated, progress ticks emitted.
	scanJobID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan #1: %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext #1 = (%v, %v)", claimed, err)
	}
	got1, err := jobRepo.GetJobByID(ctx, scanJobID)
	if err != nil || got1.State != domain.JobStateSucceeded {
		t.Fatalf("job #1 = (%+v, %v), want SUCCEEDED", got1, err)
	}

	var ticks []domain.JobProgressTick
	for len(tickCh) > 0 {
		ticks = append(ticks, <-tickCh)
	}
	if len(ticks) < 2 {
		t.Fatalf("expected at least 2 progress ticks, got %d (%+v)", len(ticks), ticks)
	}
	if ticks[0].JobID != scanJobID || ticks[0].Kind != domain.JobKindScan || ticks[0].ScannedCount != 1 || ticks[0].CurrentPath == "" {
		t.Fatalf("unexpected first tick: %+v", ticks[0])
	}
	if ticks[len(ticks)-1].ScannedCount != 2 {
		t.Fatalf("last tick ScannedCount = %d, want 2", ticks[len(ticks)-1].ScannedCount)
	}

	assets := listAssetsForRoot(ctx, t, transactor, root.ID)
	if len(assets) != 2 {
		t.Fatalf("indexed assets = %d, want 2", len(assets))
	}

	// 2. Delete one file and run a second SCAN job -> missing-file reconciliation marks it missing.
	if err := os.Remove(file2); err != nil {
		t.Fatalf("Remove(%s): %v", file2, err)
	}
	scanJobID2, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan #2: %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext #2 = (%v, %v)", claimed, err)
	}
	got2, err := jobRepo.GetJobByID(ctx, scanJobID2)
	if err != nil || got2.State != domain.JobStateSucceeded {
		t.Fatalf("job #2 = (%+v, %v), want SUCCEEDED", got2, err)
	}
	assetsAfter := listAssetsForRoot(ctx, t, transactor, root.ID)
	var missingCount int
	for _, a := range assetsAfter {
		if a.MissingSince != nil {
			missingCount++
		}
	}
	if missingCount != 1 {
		t.Fatalf("missingCount after delete scan = %d, want 1", missingCount)
	}

	// 3. Unknown library fails permanently on attempt 1 without retry and releases dedupe key.
	missingLibJobID, err := scanner.TriggerScan(ctx, jobService, "unknown-library-id", scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan(unknown): %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext(unknown) = (%v, %v)", claimed, err)
	}
	gotMissing, err := jobRepo.GetJobByID(ctx, missingLibJobID)
	if err != nil {
		t.Fatalf("GetJobByID(unknown): %v", err)
	}
	if gotMissing.State != domain.JobStateFailed || gotMissing.Attempts != 1 {
		t.Fatalf("unknown library job = (%s, attempts=%d), want (FAILED, 1)", gotMissing.State, gotMissing.Attempts)
	}
	var failedEventCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.failed'`, missingLibJobID).Scan(&failedEventCount); err != nil {
		t.Fatalf("count job.failed for unknown library: %v", err)
	}
	if failedEventCount != 1 {
		t.Fatalf("job.failed count = %d, want 1", failedEventCount)
	}
	if nextID, err := scanner.TriggerScan(ctx, jobService, "unknown-library-id", scanner.TriggerScanOptions{}); err != nil || nextID == missingLibJobID {
		t.Fatalf("post-failure re-trigger = (%q, %v), want fresh job ID", nextID, err)
	}
	// Drain queued re-trigger or cancel it, and also verify empty payload fails permanently without retry.
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext(re-trigger) = (%v, %v)", claimed, err)
	}

	badPayloadJob := &domain.Job{
		Kind:        domain.JobKindScan,
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 3,
	}
	if _, err := jobRepo.Enqueue(ctx, badPayloadJob); err != nil {
		t.Fatalf("Enqueue badPayloadJob: %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext(badPayloadJob) = (%v, %v)", claimed, err)
	}
	gotBad, err := jobRepo.GetJobByID(ctx, badPayloadJob.ID)
	if err != nil || gotBad.State != domain.JobStateFailed || gotBad.Attempts != 1 {
		t.Fatalf("badPayloadJob = (%+v, %v), want FAILED attempts=1", gotBad, err)
	}
}

func TestScanModuleRunner_IncompleteWalkRetriesAndOfflineStops(t *testing.T) {
	t.Parallel()

	t.Run("incomplete ONLINE walk enters retry path and succeeds after recovery", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		blockedDir := filepath.Join(rootDir, "Blocked")
		writeDiskFile(t, filepath.Join(blockedDir, "v01.cbz"), buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Blocked Saga</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("page-1"), method: zip.Store},
		))
		writeDiskFile(t, filepath.Join(rootDir, "Healthy/v02.cbz"), buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Healthy Saga</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("page-2"), method: zip.Store},
		))

		var lib domain.Library
		var root domain.LibraryRoot
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Incomplete Walk Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seedLibraryAndRoots: %v", err)
		}

		var blockSubdir atomic.Bool
		blockSubdir.Store(true)
		sc, err := scanner.NewScanner(transactor, scanner.Options{
			ReadDir: func(rawDir []byte) ([]os.DirEntry, error) {
				if blockSubdir.Load() && string(rawDir) == blockedDir {
					return nil, os.ErrPermission
				}
				return os.ReadDir(string(rawDir))
			},
		})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		mod, err := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 2})
		if err != nil {
			t.Fatalf("NewModule: %v", err)
		}
		reg := module.NewRegistry()
		if err := reg.Register(mod); err != nil {
			t.Fatalf("reg.Register: %v", err)
		}
		jobRepo := postgres.NewJobRepository(db.Pool)
		runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{})
		if err := runner.RegisterModuleKinds(reg); err != nil {
			t.Fatalf("RegisterModuleKinds: %v", err)
		}
		if err := runner.RegisterModuleHandlers(reg); err != nil {
			t.Fatalf("RegisterModuleHandlers: %v", err)
		}
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

		jobID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 2})
		if err != nil {
			t.Fatalf("TriggerScan: %v", err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext #1 = (%v, %v)", claimed, err)
		}

		afterAttempt1, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJobByID after attempt 1: %v", err)
		}
		if afterAttempt1.State != domain.JobStateQueued || afterAttempt1.Attempts != 1 {
			t.Fatalf("job after incomplete walk = (state=%s, attempts=%d), want (QUEUED, 1)", afterAttempt1.State, afterAttempt1.Attempts)
		}
		if !afterAttempt1.RunAfter.After(afterAttempt1.UpdatedAt) {
			t.Fatalf("expected backoff RunAfter > UpdatedAt, got run_after=%v updated_at=%v", afterAttempt1.RunAfter, afterAttempt1.UpdatedAt)
		}
		if gotAssets := listAssetsForRoot(ctx, t, transactor, root.ID); len(gotAssets) != 0 {
			t.Fatalf("assets after incomplete walk = %d, want 0", len(gotAssets))
		}
		var failedEvents int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.failed'`, jobID).Scan(&failedEvents); err != nil {
			t.Fatalf("count job.failed: %v", err)
		}
		if failedEvents != 0 {
			t.Fatalf("job.failed count after retryable incomplete walk = %d, want 0", failedEvents)
		}

		// Recover subtree readability and make the retry immediately claimable.
		blockSubdir.Store(false)
		if _, err := db.Exec(ctx, `UPDATE jobs SET run_after = now() - interval '1 second' WHERE id = $1`, jobID); err != nil {
			t.Fatalf("backdate run_after: %v", err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext #2 = (%v, %v)", claimed, err)
		}
		afterAttempt2, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJobByID after attempt 2: %v", err)
		}
		if afterAttempt2.State != domain.JobStateSucceeded || afterAttempt2.Attempts != 2 {
			t.Fatalf("job after recovery = (state=%s, attempts=%d), want (SUCCEEDED, 2)", afterAttempt2.State, afterAttempt2.Attempts)
		}
		if gotAssets := listAssetsForRoot(ctx, t, transactor, root.ID); len(gotAssets) != 2 {
			t.Fatalf("assets after recovered walk = %d, want 2", len(gotAssets))
		}
	})

	t.Run("offline library stops cleanly without retrying (M1B-D10)", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		missingRoot := filepath.Join(t.TempDir(), "unmounted-share")
		var lib domain.Library
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			lib, _ = seedLibraryAndRoots(ctx, t, tx, "Offline Stop Lib", nil, []byte(missingRoot))
			return nil
		}); err != nil {
			t.Fatalf("seedLibraryAndRoots: %v", err)
		}

		sc, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		mod, err := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 1})
		if err != nil {
			t.Fatalf("NewModule: %v", err)
		}
		reg := module.NewRegistry()
		if err := reg.Register(mod); err != nil {
			t.Fatalf("reg.Register: %v", err)
		}
		jobRepo := postgres.NewJobRepository(db.Pool)
		runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{})
		if err := runner.RegisterModuleKinds(reg); err != nil {
			t.Fatalf("RegisterModuleKinds: %v", err)
		}
		if err := runner.RegisterModuleHandlers(reg); err != nil {
			t.Fatalf("RegisterModuleHandlers: %v", err)
		}
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

		jobID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 3})
		if err != nil {
			t.Fatalf("TriggerScan: %v", err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext = (%v, %v)", claimed, err)
		}
		got, err := jobRepo.GetJobByID(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJobByID: %v", err)
		}
		if got.State != domain.JobStateSucceeded || got.Attempts != 1 {
			t.Fatalf("offline library job = (state=%s, attempts=%d), want (SUCCEEDED, 1)", got.State, got.Attempts)
		}
	})
}

func TestScanModuleRunner_NonLibraryNotFoundRetries(t *testing.T) {
	t.Parallel()
	db, transactor := setupIndexerTestDB(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	rootDir := filepath.Join(t.TempDir(), "root")
	keepFile := filepath.Join(rootDir, "Keep/v01.cbz")
	vanishFile := filepath.Join(rootDir, "Vanish/v02.cbz")
	writeDiskFile(t, keepFile, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Keep Series</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("keep-1"), method: zip.Store},
	))
	writeDiskFile(t, vanishFile, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Vanish Series</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("vanish-2"), method: zip.Store},
	))

	var lib domain.Library
	var root domain.LibraryRoot
	if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		var roots []domain.LibraryRoot
		lib, roots = seedLibraryAndRoots(ctx, t, tx, "Non-Library ErrNotFound Retry Lib", nil, []byte(rootDir))
		root = roots[0]
		return nil
	}); err != nil {
		t.Fatalf("seedLibraryAndRoots: %v", err)
	}

	var vanishAssetID atomic.Value
	var simulateConcurrentDelete atomic.Bool
	sc, err := scanner.NewScanner(transactor, scanner.Options{
		Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
			if string(rawPath) == keepFile && simulateConcurrentDelete.CompareAndSwap(true, false) {
				id, _ := vanishAssetID.Load().(string)
				if _, delErr := db.Exec(openCtx, `DELETE FROM media_assets WHERE id = $1`, id); delErr != nil {
					return nil, delErr
				}
			}
			return os.Open(string(rawPath))
		},
	})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	mod, err := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 1})
	if err != nil {
		t.Fatalf("NewModule: %v", err)
	}
	reg := module.NewRegistry()
	if err := reg.Register(mod); err != nil {
		t.Fatalf("reg.Register: %v", err)
	}
	jobRepo := postgres.NewJobRepository(db.Pool)
	runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{})
	if err := runner.RegisterModuleKinds(reg); err != nil {
		t.Fatalf("RegisterModuleKinds: %v", err)
	}
	if err := runner.RegisterModuleHandlers(reg); err != nil {
		t.Fatalf("RegisterModuleHandlers: %v", err)
	}
	if _, err := runner.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

	// Baseline scan populates both assets.
	job1, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan #1: %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext #1 = (%v, %v)", claimed, err)
	}
	got1, err := jobRepo.GetJobByID(ctx, job1)
	if err != nil || got1.State != domain.JobStateSucceeded {
		t.Fatalf("baseline job = (%+v, %v), want SUCCEEDED", got1, err)
	}

	assets := listAssetsForRoot(ctx, t, transactor, root.ID)
	var foundVanish string
	for _, a := range assets {
		if a.RelPathDisplay == "Vanish/v02.cbz" {
			foundVanish = a.ID
			break
		}
	}
	if foundVanish == "" {
		t.Fatalf("failed to find baseline Vanish/v02.cbz asset among %+v", assets)
	}
	vanishAssetID.Store(foundVanish)

	// Remove Vanish/v02.cbz so reconciliation plans applyMarkMissing on it, and rewrite
	// Keep/v01.cbz so Open is invoked mid-walk, deleting the vanish asset in PostgreSQL
	// before withLockedAssetSnapshot locks it (producing a non-library storage.ErrNotFound).
	if err := os.Remove(vanishFile); err != nil {
		t.Fatalf("Remove(%s): %v", vanishFile, err)
	}
	writeDiskFile(t, keepFile, buildDeterministicZip(t, "",
		zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Keep Series</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
		zipEntry{name: "001.jpg", data: []byte("keep-1-modified"), method: zip.Store},
	))
	simulateConcurrentDelete.Store(true)

	job2, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 2})
	if err != nil {
		t.Fatalf("TriggerScan #2: %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext #2 attempt 1 = (%v, %v)", claimed, err)
	}
	afterAttempt1, err := jobRepo.GetJobByID(ctx, job2)
	if err != nil {
		t.Fatalf("GetJobByID #2 attempt 1: %v", err)
	}
	if afterAttempt1.State != domain.JobStateQueued || afterAttempt1.Attempts != 1 {
		t.Fatalf("job after non-library storage.ErrNotFound = (state=%s, attempts=%d), want (QUEUED, 1)", afterAttempt1.State, afterAttempt1.Attempts)
	}
	var failedEvents int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.failed'`, job2).Scan(&failedEvents); err != nil {
		t.Fatalf("count job.failed: %v", err)
	}
	if failedEvents != 0 {
		t.Fatalf("job.failed count after non-library ErrNotFound = %d, want 0", failedEvents)
	}

	if _, err := db.Exec(ctx, `UPDATE jobs SET run_after = now() - interval '1 second' WHERE id = $1`, job2); err != nil {
		t.Fatalf("backdate run_after: %v", err)
	}
	if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
		t.Fatalf("ClaimAndRunNext #2 attempt 2 = (%v, %v)", claimed, err)
	}
	afterAttempt2, err := jobRepo.GetJobByID(ctx, job2)
	if err != nil {
		t.Fatalf("GetJobByID #2 attempt 2: %v", err)
	}
	if afterAttempt2.State != domain.JobStateSucceeded || afterAttempt2.Attempts != 2 {
		t.Fatalf("job after retry = (state=%s, attempts=%d), want (SUCCEEDED, 2)", afterAttempt2.State, afterAttempt2.Attempts)
	}
}

type hookAfterAssetCommitTransactor struct {
	inner         storage.Transactor
	createdAssets atomic.Int32
	onAfterCommit func(createCount int32)
}

func (h *hookAfterAssetCommitTransactor) WithTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	return h.inner.WithTx(ctx, fn)
}

func (h *hookAfterAssetCommitTransactor) WithReadCommittedTx(ctx context.Context, fn func(tx storage.Tx) error) error {
	var created bool
	err := h.inner.WithReadCommittedTx(ctx, func(tx storage.Tx) error {
		return fn(&hookAfterAssetCommitTx{Tx: tx, created: &created})
	})
	if err == nil && created && h.onAfterCommit != nil {
		n := h.createdAssets.Add(1)
		h.onAfterCommit(n)
	}
	return err
}

type hookAfterAssetCommitTx struct {
	storage.Tx
	created *bool
}

func (h *hookAfterAssetCommitTx) CatalogueWrites() storage.CatalogueWriteRepository {
	return &hookAfterAssetCommitRepo{
		CatalogueWriteRepository: h.Tx.CatalogueWrites(),
		created:                  h.created,
	}
}

type hookAfterAssetCommitRepo struct {
	storage.CatalogueWriteRepository
	created *bool
}

func (r *hookAfterAssetCommitRepo) CreateAsset(ctx context.Context, asset *domain.MediaAsset) error {
	if err := r.CatalogueWriteRepository.CreateAsset(ctx, asset); err != nil {
		return err
	}
	*r.created = true
	return nil
}

func TestScanModuleRunner_DefaultMaxAttempts(t *testing.T) {
	t.Parallel()
	db, _ := setupIndexerTestDB(t)
	ctx := t.Context()

	if scanner.DefaultScanMaxAttempts != 3 {
		t.Fatalf("DefaultScanMaxAttempts = %d, want 3", scanner.DefaultScanMaxAttempts)
	}

	jobRepo := postgres.NewJobRepository(db.Pool)
	jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})
	jobID, err := scanner.TriggerScan(ctx, jobService, "lib-default-attempts", scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan: %v", err)
	}
	job, err := jobRepo.GetJobByID(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJobByID: %v", err)
	}
	if job.MaxAttempts != scanner.DefaultScanMaxAttempts {
		t.Fatalf("job.MaxAttempts = %d, want %d", job.MaxAttempts, scanner.DefaultScanMaxAttempts)
	}
}

func TestScanModuleRunner_CancelAtFileBoundary(t *testing.T) {
	t.Parallel()

	adminPrincipal := &domain.Principal{
		User:        domain.User{ID: "admin-1", Username: "admin"},
		Permissions: []domain.Permission{domain.PermissionJobsReadAll, domain.PermissionJobsCancelAll},
	}

	t.Run("cancel mid-walk stops at next file boundary without missing marks or partial move/modify", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		existingPath := filepath.Join(rootDir, "Existing/v01.cbz")
		modifyPath := filepath.Join(rootDir, "Modify/m01.cbz")
		oldMovePath := filepath.Join(rootDir, "Move/old.cbz")
		renamedMovePath := filepath.Join(rootDir, "Move/renamed.cbz")
		gonePath := filepath.Join(rootDir, "Vanish/gone.cbz")
		blockNewPath := filepath.Join(rootDir, "Zebra/01_block.cbz")
		pipelineNewPath := filepath.Join(rootDir, "Zebra/02_pipeline.cbz")
		unopenedNewPath := filepath.Join(rootDir, "Zebra/03_unopened.cbz")

		writeDiskFile(t, existingPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Existing</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("existing-1"), method: zip.Store},
		))
		writeDiskFile(t, modifyPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Modify</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("modify-v1"), method: zip.Store},
		))
		writeDiskFile(t, oldMovePath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Move</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("move-1"), method: zip.Store},
		))
		writeDiskFile(t, gonePath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Vanish</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("vanish-1"), method: zip.Store},
		))

		var lib domain.Library
		var root domain.LibraryRoot
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Cancel Walk Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seedLibraryAndRoots: %v", err)
		}

		var blockActive atomic.Bool
		enteredBlock := make(chan struct{}, 1)
		releaseBlock := make(chan struct{})
		var openMu sync.Mutex
		openedOnCancelScan := make(map[string]int)

		sc, err := scanner.NewScanner(transactor, scanner.Options{
			Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				if blockActive.Load() {
					openMu.Lock()
					openedOnCancelScan[p]++
					openMu.Unlock()
					if p == blockNewPath {
						select {
						case enteredBlock <- struct{}{}:
						default:
						}
						select {
						case <-releaseBlock:
						case <-openCtx.Done():
							return nil, openCtx.Err()
						}
					}
				}
				return os.Open(p)
			},
		})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		mod, err := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 1})
		if err != nil {
			t.Fatalf("NewModule: %v", err)
		}
		reg := module.NewRegistry()
		if err := reg.Register(mod); err != nil {
			t.Fatalf("reg.Register: %v", err)
		}
		jobRepo := postgres.NewJobRepository(db.Pool)
		runner := jobs.NewRunner(jobRepo, jobs.RunnerOptions{})
		if err := runner.RegisterModuleKinds(reg); err != nil {
			t.Fatalf("RegisterModuleKinds: %v", err)
		}
		if err := runner.RegisterModuleHandlers(reg); err != nil {
			t.Fatalf("RegisterModuleHandlers: %v", err)
		}
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

		// Baseline scan commits the 4 initial files.
		baseJobID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			t.Fatalf("TriggerScan baseline: %v", err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext baseline = (%v, %v)", claimed, err)
		}
		baseJob, err := jobRepo.GetJobByID(ctx, baseJobID)
		if err != nil || baseJob.State != domain.JobStateSucceeded {
			t.Fatalf("baseline job = (%+v, %v), want SUCCEEDED", baseJob, err)
		}

		var origModifyDigest string
		for _, a := range listAssetsForRoot(ctx, t, transactor, root.ID) {
			if a.RelPathDisplay == "Modify/m01.cbz" {
				if cur, ok := a.CurrentDigest(domain.DigestKindXXH364Partial); ok {
					origModifyDigest = cur.Value
				}
			}
		}
		if origModifyDigest == "" {
			t.Fatal("failed to record baseline Modify/m01.cbz digest")
		}

		// Mutate disk: move old.cbz, modify m01.cbz, delete gone.cbz, and add two new files.
		if err := os.Rename(oldMovePath, renamedMovePath); err != nil {
			t.Fatalf("Rename move file: %v", err)
		}
		writeDiskFile(t, modifyPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Modify</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("modify-v2-changed-content"), method: zip.Store},
		))
		if err := os.Remove(gonePath); err != nil {
			t.Fatalf("Remove gone file: %v", err)
		}
		writeDiskFile(t, blockNewPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Zebra</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("zebra-1"), method: zip.Store},
		))
		writeDiskFile(t, pipelineNewPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Zebra</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("zebra-2"), method: zip.Store},
		))
		writeDiskFile(t, unopenedNewPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Zebra</Series><Number>3</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("zebra-3"), method: zip.Store},
		))

		blockActive.Store(true)
		cancelJobID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			t.Fatalf("TriggerScan #2: %v", err)
		}

		runErrCh := make(chan error, 1)
		go func() {
			claimed, runErr := runner.ClaimAndRunNext(ctx, domain.JobKindScan)
			if runErr == nil && !claimed {
				runErr = errors.New("expected SCAN job to be claimed")
			}
			runErrCh <- runErr
		}()

		select {
		case <-enteredBlock:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for Open(Zebra/01_block.cbz)")
		}

		cancelInfo, err := jobService.CancelJob(ctx, adminPrincipal, cancelJobID)
		if err != nil {
			t.Fatalf("CancelJob: %v", err)
		}
		if cancelInfo.State != string(domain.JobStateRunning) || !cancelInfo.CancelRequested {
			t.Fatalf("CancelJob snapshot = %+v, want RUNNING with cancel_requested=true", cancelInfo)
		}
		close(releaseBlock)

		if runErr := <-runErrCh; runErr != nil {
			t.Fatalf("ClaimAndRunNext returned unexpected error: %v", runErr)
		}

		gotJob, err := jobRepo.GetJobByID(ctx, cancelJobID)
		if err != nil {
			t.Fatalf("GetJobByID: %v", err)
		}
		if gotJob.State != domain.JobStateCancelled || gotJob.FinishedAt == nil || gotJob.Attempts != 1 {
			t.Fatalf("cancelled job = %+v, want CANCELLED attempts=1 finished_at!=nil", gotJob)
		}

		var cancelEventCount int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.cancelled'`, cancelJobID).Scan(&cancelEventCount); err != nil {
			t.Fatalf("count job.cancelled: %v", err)
		}
		if cancelEventCount != 1 {
			t.Fatalf("job.cancelled count = %d, want 1", cancelEventCount)
		}

		openMu.Lock()
		pipelineCalls := openedOnCancelScan[pipelineNewPath]
		unopenedCalls := openedOnCancelScan[unopenedNewPath]
		existingCalls := openedOnCancelScan[existingPath]
		openMu.Unlock()
		if pipelineCalls != 0 {
			t.Fatalf("Zebra/02_pipeline.cbz was opened %d times after cancel with Workers=1, want 0", pipelineCalls)
		}
		if unopenedCalls != 0 {
			t.Fatalf("Zebra/03_unopened.cbz was opened %d times after cancel, want 0", unopenedCalls)
		}
		if existingCalls != 0 {
			t.Fatalf("Existing/v01.cbz unchanged file was re-opened %d times, want 0", existingCalls)
		}

		assetsAfter := listAssetsForRoot(ctx, t, transactor, root.ID)
		if len(assetsAfter) != 4 {
			t.Fatalf("assetsAfter = %d, want 4 baseline assets", len(assetsAfter))
		}
		byPath := make(map[string]domain.MediaAsset, len(assetsAfter))
		for _, a := range assetsAfter {
			byPath[a.RelPathDisplay] = a
		}
		if _, ok := byPath["Move/old.cbz"]; !ok {
			t.Fatalf("expected Move/old.cbz to remain unmoved after partial-walk cancel, got %+v", byPath)
		}
		modAsset := byPath["Modify/m01.cbz"]
		if cur, ok := modAsset.CurrentDigest(domain.DigestKindXXH364Partial); !ok || cur.Value != origModifyDigest {
			t.Fatalf("expected Modify/m01.cbz digest %q untouched, got %+v", origModifyDigest, cur)
		}
		if gone := byPath["Vanish/gone.cbz"]; gone.MissingSince != nil {
			t.Fatalf("expected Vanish/gone.cbz not marked missing on cancel, got %v", gone.MissingSince)
		}

		var badEvents int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE kind IN ('asset.moved', 'asset.modified', 'asset.missing')`).Scan(&badEvents); err != nil {
			t.Fatalf("count reconcile events: %v", err)
		}
		if badEvents != 0 {
			t.Fatalf("reconcile event count = %d, want 0 after partial-walk cancel", badEvents)
		}

		// Dedupe key must be released after CANCELLED.
		blockActive.Store(false)
		followUpID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			t.Fatalf("post-cancel TriggerScan: %v", err)
		}
		if followUpID == cancelJobID {
			t.Fatalf("post-cancel TriggerScan reused job ID %q", cancelJobID)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext followUp = (%v, %v)", claimed, err)
		}
		gotFollowUp, err := jobRepo.GetJobByID(ctx, followUpID)
		if err != nil || gotFollowUp.State != domain.JobStateSucceeded {
			t.Fatalf("followUp job = (%+v, %v), want SUCCEEDED", gotFollowUp, err)
		}
	})

	t.Run("cancel during write phase preserves committed files and skips missing marks", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		writeDiskFile(t, filepath.Join(rootDir, "Existing/v01.cbz"), buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Existing</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("existing-1"), method: zip.Store},
		))
		gonePath := filepath.Join(rootDir, "Vanish/gone.cbz")
		writeDiskFile(t, gonePath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Vanish</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("vanish-1"), method: zip.Store},
		))

		var lib domain.Library
		var root domain.LibraryRoot
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Cancel Write Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seedLibraryAndRoots: %v", err)
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

		var cancelTargetJobID atomic.Value
		var cancelOnNthCommit atomic.Int32
		hookTx := &hookAfterAssetCommitTransactor{
			inner: transactor,
			onAfterCommit: func(createCount int32) {
				if createCount == cancelOnNthCommit.Load() {
					if jid, ok := cancelTargetJobID.Load().(string); ok && jid != "" {
						if _, cErr := jobService.CancelJob(ctx, adminPrincipal, jid); cErr != nil {
							t.Errorf("CancelJob after commit %d: %v", createCount, cErr)
						}
					}
				}
			},
		}

		sc, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		mod, err := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 1})
		if err != nil {
			t.Fatalf("NewModule: %v", err)
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
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}

		// Baseline scan commits Existing + Vanish (2 CreateAsset commits).
		if _, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{}); err != nil {
			t.Fatalf("TriggerScan baseline: %v", err)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext baseline = (%v, %v)", claimed, err)
		}

		if err := os.Remove(gonePath); err != nil {
			t.Fatalf("Remove gonePath: %v", err)
		}
		for _, name := range []string{"New/a.cbz", "New/b.cbz", "New/c.cbz"} {
			writeDiskFile(t, filepath.Join(rootDir, name), buildDeterministicZip(t, "",
				zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>NewSaga</Series><Title>` + name + `</Title></ComicInfo>`), method: zip.Deflate},
				zipEntry{name: "001.jpg", data: []byte("bytes-" + name), method: zip.Store},
			))
		}

		cancelJobID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			t.Fatalf("TriggerScan #2: %v", err)
		}
		cancelTargetJobID.Store(cancelJobID)
		cancelOnNthCommit.Store(3) // 2 baseline + 1st new file (New/a.cbz)

		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext #2 = (%v, %v)", claimed, err)
		}
		gotJob, err := jobRepo.GetJobByID(ctx, cancelJobID)
		if err != nil {
			t.Fatalf("GetJobByID: %v", err)
		}
		if gotJob.State != domain.JobStateCancelled {
			t.Fatalf("job state = %s, want CANCELLED", gotJob.State)
		}

		assets := listAssetsForRoot(ctx, t, transactor, root.ID)
		if len(assets) != 3 {
			t.Fatalf("assets after write-phase cancel = %d, want 3 (2 baseline + New/a.cbz)", len(assets))
		}
		byPath := make(map[string]domain.MediaAsset, len(assets))
		for _, a := range assets {
			byPath[a.RelPathDisplay] = a
		}
		if _, ok := byPath["New/a.cbz"]; !ok {
			t.Fatalf("expected committed New/a.cbz intact, got %+v", byPath)
		}
		if gone := byPath["Vanish/gone.cbz"]; gone.MissingSince != nil {
			t.Fatalf("expected Vanish/gone.cbz not marked missing, got %v", gone.MissingSince)
		}
	})

	t.Run("cancel accepted during final write transaction ends CANCELLED with committed asset and released dedupe key", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		soloPath := filepath.Join(rootDir, "Solo/v01.cbz")
		writeDiskFile(t, soloPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Solo</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("solo-1"), method: zip.Store},
		))

		var lib domain.Library
		var root domain.LibraryRoot
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Final Write Cancel Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seedLibraryAndRoots: %v", err)
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

		var targetJobID atomic.Value
		var cancelIssued atomic.Bool
		hookTx := &hookAfterAssetCommitTransactor{
			inner: transactor,
			onAfterCommit: func(createCount int32) {
				if createCount == 1 && cancelIssued.CompareAndSwap(false, true) {
					jid, _ := targetJobID.Load().(string)
					snap, cErr := jobService.CancelJob(ctx, adminPrincipal, jid)
					if cErr != nil {
						t.Errorf("CancelJob during final write: %v", cErr)
					} else if snap.State != string(domain.JobStateRunning) || !snap.CancelRequested {
						t.Errorf("CancelJob snapshot = %+v, want RUNNING with cancel_requested=true", snap)
					}
				}
			},
		}

		var openMu sync.Mutex
		openedOnFollowUp := make(map[string]int)
		var trackOpens atomic.Bool
		sc, err := scanner.NewScanner(hookTx, scanner.Options{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				if trackOpens.Load() {
					openMu.Lock()
					openedOnFollowUp[p]++
					openMu.Unlock()
				}
				return os.Open(p)
			},
		})
		if err != nil {
			t.Fatalf("NewScanner: %v", err)
		}
		mod, err := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 1})
		if err != nil {
			t.Fatalf("NewModule: %v", err)
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
		if _, err := runner.Recover(ctx); err != nil {
			t.Fatalf("Recover: %v", err)
		}

		cancelJobID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			t.Fatalf("TriggerScan: %v", err)
		}
		targetJobID.Store(cancelJobID)

		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext = (%v, %v)", claimed, err)
		}
		got, err := jobRepo.GetJobByID(ctx, cancelJobID)
		if err != nil {
			t.Fatalf("GetJobByID: %v", err)
		}
		if got.State != domain.JobStateCancelled || got.FinishedAt == nil || got.Attempts != 1 {
			t.Fatalf("final-write cancelled job = %+v, want CANCELLED attempts=1 finished_at!=nil", got)
		}

		var cancelEvents int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.cancelled'`, cancelJobID).Scan(&cancelEvents); err != nil {
			t.Fatalf("count job.cancelled: %v", err)
		}
		if cancelEvents != 1 {
			t.Fatalf("job.cancelled count = %d, want 1", cancelEvents)
		}

		assets := listAssetsForRoot(ctx, t, transactor, root.ID)
		if len(assets) != 1 || assets[0].RelPathDisplay != "Solo/v01.cbz" {
			t.Fatalf("committed assets after final-write cancel = %+v, want [Solo/v01.cbz]", assets)
		}

		// Dedupe key released; clean rescan succeeds without re-opening Solo/v01.cbz.
		trackOpens.Store(true)
		followID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			t.Fatalf("follow-up TriggerScan: %v", err)
		}
		if followID == cancelJobID {
			t.Fatalf("dedupe key not released after final-write cancel: reused %q", cancelJobID)
		}
		if claimed, err := runner.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext follow-up = (%v, %v)", claimed, err)
		}
		gotFollow, err := jobRepo.GetJobByID(ctx, followID)
		if err != nil || gotFollow.State != domain.JobStateSucceeded {
			t.Fatalf("follow-up job = (%+v, %v), want SUCCEEDED", gotFollow, err)
		}
		openMu.Lock()
		soloOpens := openedOnFollowUp[soloPath]
		openMu.Unlock()
		if soloOpens != 0 {
			t.Fatalf("Solo/v01.cbz re-opened %d times on follow-up scan, want 0", soloOpens)
		}
	})
}

func TestScanModuleRunner_RestartRecoveryAndMaxAttemptsExhaustion(t *testing.T) {
	t.Parallel()

	t.Run("interrupted scan requeues on Recover and skips committed unchanged files", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		existingPath := filepath.Join(rootDir, "Existing/v01.cbz")
		newAPath := filepath.Join(rootDir, "New/a.cbz")
		newBPath := filepath.Join(rootDir, "New/b.cbz")
		writeDiskFile(t, existingPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Existing</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("existing-1"), method: zip.Store},
		))

		var lib domain.Library
		var root domain.LibraryRoot
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Restart Recover Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seedLibraryAndRoots: %v", err)
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

		setupRunner := func(sc *scanner.Scanner) *jobs.Runner {
			mod, mErr := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 1})
			if mErr != nil {
				t.Fatalf("NewModule: %v", mErr)
			}
			reg := module.NewRegistry()
			if rErr := reg.Register(mod); rErr != nil {
				t.Fatalf("reg.Register: %v", rErr)
			}
			r := jobs.NewRunner(jobRepo, jobs.RunnerOptions{})
			if err := r.RegisterModuleKinds(reg); err != nil {
				t.Fatalf("RegisterModuleKinds: %v", err)
			}
			if err := r.RegisterModuleHandlers(reg); err != nil {
				t.Fatalf("RegisterModuleHandlers: %v", err)
			}
			return r
		}

		// Baseline scan commits Existing/v01.cbz.
		scBase, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner baseline: %v", err)
		}
		runnerBase := setupRunner(scBase)
		if _, err := runnerBase.Recover(ctx); err != nil {
			t.Fatalf("Recover baseline: %v", err)
		}
		if _, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{}); err != nil {
			t.Fatalf("TriggerScan baseline: %v", err)
		}
		if claimed, err := runnerBase.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext baseline = (%v, %v)", claimed, err)
		}

		writeDiskFile(t, newAPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>New</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("new-a"), method: zip.Store},
		))
		writeDiskFile(t, newBPath, buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>New</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("new-b"), method: zip.Store},
		))

		run1Ctx, cancelRun1 := context.WithCancel(ctx)
		hookTx := &hookAfterAssetCommitTransactor{
			inner: transactor,
			onAfterCommit: func(createCount int32) {
				if createCount == 1 {
					cancelRun1()
				}
			},
		}
		sc1, err := scanner.NewScanner(hookTx, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner #1: %v", err)
		}
		runner1 := setupRunner(sc1)
		if _, err := runner1.Recover(ctx); err != nil {
			t.Fatalf("Recover #1: %v", err)
		}

		scanID, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			t.Fatalf("TriggerScan #2: %v", err)
		}
		if claimed, err := runner1.ClaimAndRunNext(run1Ctx, domain.JobKindScan); !claimed || !errors.Is(err, context.Canceled) {
			t.Fatalf("interrupted ClaimAndRunNext = (%v, %v), want (true, context.Canceled)", claimed, err)
		}

		midJob, err := jobRepo.GetJobByID(ctx, scanID)
		if err != nil {
			t.Fatalf("GetJobByID mid-interruption: %v", err)
		}
		if midJob.State != domain.JobStateRunning || midJob.Attempts != 1 {
			t.Fatalf("mid-interruption job = (state=%s, attempts=%d), want (RUNNING, 1)", midJob.State, midJob.Attempts)
		}

		// Recovery boot: track every file opened during the rerun.
		var openMu sync.Mutex
		openedOnRerun := make(map[string]int)
		sc2, err := scanner.NewScanner(transactor, scanner.Options{
			Open: func(_ context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
				p := string(rawPath)
				openMu.Lock()
				openedOnRerun[p]++
				openMu.Unlock()
				return os.Open(p)
			},
		})
		if err != nil {
			t.Fatalf("NewScanner #2: %v", err)
		}
		runner2 := setupRunner(sc2)
		report, err := runner2.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover #2: %v", err)
		}
		if report.TotalRunning != 1 || report.RequeuedRestart != 1 {
			t.Fatalf("Recover report = %+v, want TotalRunning=1 RequeuedRestart=1", report)
		}

		if claimed, err := runner2.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext rerun = (%v, %v)", claimed, err)
		}
		doneJob, err := jobRepo.GetJobByID(ctx, scanID)
		if err != nil {
			t.Fatalf("GetJobByID done: %v", err)
		}
		if doneJob.State != domain.JobStateSucceeded || doneJob.Attempts != 2 {
			t.Fatalf("doneJob = (state=%s, attempts=%d), want (SUCCEEDED, 2)", doneJob.State, doneJob.Attempts)
		}

		openMu.Lock()
		existingOpens := openedOnRerun[existingPath]
		newAOpens := openedOnRerun[newAPath]
		newBOpens := openedOnRerun[newBPath]
		openMu.Unlock()
		if existingOpens != 0 || newAOpens != 0 || newBOpens != 1 {
			t.Fatalf("rerun opens = (existing=%d, newA=%d, newB=%d), want (0, 0, 1)", existingOpens, newAOpens, newBOpens)
		}

		assets := listAssetsForRoot(ctx, t, transactor, root.ID)
		if len(assets) != 3 {
			t.Fatalf("assets = %d, want 3 (no duplicates)", len(assets))
		}
		for _, a := range assets {
			if a.MissingSince != nil {
				t.Fatalf("asset %s marked missing unexpectedly: %v", a.RelPathDisplay, a.MissingSince)
			}
		}
		var missingEvents int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE kind = 'asset.missing'`).Scan(&missingEvents); err != nil {
			t.Fatalf("count asset.missing: %v", err)
		}
		if missingEvents != 0 {
			t.Fatalf("asset.missing count = %d, want 0", missingEvents)
		}
	})

	t.Run("exhausting max_attempts by interruptions fails job cleanly and fresh trigger completes", func(t *testing.T) {
		t.Parallel()
		db, transactor := setupIndexerTestDB(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()

		rootDir := filepath.Join(t.TempDir(), "root")
		writeDiskFile(t, filepath.Join(rootDir, "Saga/v01.cbz"), buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Saga</Series><Number>1</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("saga-1"), method: zip.Store},
		))
		writeDiskFile(t, filepath.Join(rootDir, "Saga/v02.cbz"), buildDeterministicZip(t, "",
			zipEntry{name: "ComicInfo.xml", data: []byte(`<ComicInfo><Series>Saga</Series><Number>2</Number></ComicInfo>`), method: zip.Deflate},
			zipEntry{name: "001.jpg", data: []byte("saga-2"), method: zip.Store},
		))

		var lib domain.Library
		var root domain.LibraryRoot
		if err := transactor.WithTx(ctx, func(tx storage.Tx) error {
			var roots []domain.LibraryRoot
			lib, roots = seedLibraryAndRoots(ctx, t, tx, "Exhaust Interruption Lib", nil, []byte(rootDir))
			root = roots[0]
			return nil
		}); err != nil {
			t.Fatalf("seedLibraryAndRoots: %v", err)
		}

		jobRepo := postgres.NewJobRepository(db.Pool)
		jobService := jobs.NewService(jobRepo, jobs.ServiceOptions{})

		setupRunner := func(sc *scanner.Scanner) *jobs.Runner {
			mod, mErr := scanner.NewModule(sc, scanner.ModuleOptions{Workers: 1})
			if mErr != nil {
				t.Fatalf("NewModule: %v", mErr)
			}
			reg := module.NewRegistry()
			if rErr := reg.Register(mod); rErr != nil {
				t.Fatalf("reg.Register: %v", rErr)
			}
			r := jobs.NewRunner(jobRepo, jobs.RunnerOptions{})
			if err := r.RegisterModuleKinds(reg); err != nil {
				t.Fatalf("RegisterModuleKinds: %v", err)
			}
			if err := r.RegisterModuleHandlers(reg); err != nil {
				t.Fatalf("RegisterModuleHandlers: %v", err)
			}
			return r
		}

		jobID1, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{MaxAttempts: 2})
		if err != nil {
			t.Fatalf("TriggerScan #1: %v", err)
		}

		// Two interrupted attempts: attempt 1 commits v01.cbz and cancels before v02.cbz;
		// attempt 2 is interrupted mid-walk on v02.cbz.
		for attempt := 1; attempt <= 2; attempt++ {
			attemptCtx, cancelAttempt := context.WithCancel(ctx)
			scInterrupt, scErr := scanner.NewScanner(transactor, scanner.Options{
				Open: func(openCtx context.Context, rawPath []byte) (scanner.ReadAtCloser, error) {
					if filepath.Base(string(rawPath)) == "v02.cbz" {
						cancelAttempt()
						return nil, openCtx.Err()
					}
					return os.Open(string(rawPath))
				},
			})
			if scErr != nil {
				t.Fatalf("NewScanner attempt %d: %v", attempt, scErr)
			}
			r := setupRunner(scInterrupt)
			if _, err := r.Recover(ctx); err != nil {
				t.Fatalf("Recover attempt %d: %v", attempt, err)
			}
			if claimed, err := r.ClaimAndRunNext(attemptCtx, domain.JobKindScan); !claimed || !errors.Is(err, context.Canceled) {
				t.Fatalf("attempt %d ClaimAndRunNext = (%v, %v), want (true, context.Canceled)", attempt, claimed, err)
			}
		}

		// Recovery #3 sees attempts == max_attempts (2) and fails the job cleanly.
		scClean, err := scanner.NewScanner(transactor, scanner.Options{})
		if err != nil {
			t.Fatalf("NewScanner clean: %v", err)
		}
		runnerFinal := setupRunner(scClean)
		report, err := runnerFinal.Recover(ctx)
		if err != nil {
			t.Fatalf("Recover #3: %v", err)
		}
		if report.TotalRunning != 1 || report.Failed != 1 {
			t.Fatalf("Recover #3 report = %+v, want TotalRunning=1 Failed=1", report)
		}
		failedJob, err := jobRepo.GetJobByID(ctx, jobID1)
		if err != nil {
			t.Fatalf("GetJobByID failed: %v", err)
		}
		if failedJob.State != domain.JobStateFailed || failedJob.Attempts != 2 || failedJob.FinishedAt == nil {
			t.Fatalf("failedJob = %+v, want FAILED attempts=2 finished_at!=nil", failedJob)
		}
		var failedEvents int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE aggregate_id = $1 AND kind = 'job.failed'`, jobID1).Scan(&failedEvents); err != nil {
			t.Fatalf("count job.failed: %v", err)
		}
		if failedEvents != 1 {
			t.Fatalf("job.failed count = %d, want 1", failedEvents)
		}

		// Dedupe key is released; fresh trigger completes the scan.
		jobID2, err := scanner.TriggerScan(ctx, jobService, lib.ID, scanner.TriggerScanOptions{})
		if err != nil {
			t.Fatalf("fresh TriggerScan: %v", err)
		}
		if jobID2 == jobID1 {
			t.Fatalf("fresh TriggerScan reused exhausted job ID %q", jobID1)
		}
		if claimed, err := runnerFinal.ClaimAndRunNext(ctx, domain.JobKindScan); err != nil || !claimed {
			t.Fatalf("ClaimAndRunNext fresh = (%v, %v)", claimed, err)
		}
		succJob, err := jobRepo.GetJobByID(ctx, jobID2)
		if err != nil || succJob.State != domain.JobStateSucceeded {
			t.Fatalf("fresh job = (%+v, %v), want SUCCEEDED", succJob, err)
		}
		if assets := listAssetsForRoot(ctx, t, transactor, root.ID); len(assets) != 2 {
			t.Fatalf("final assets = %d, want 2", len(assets))
		}
	})
}
