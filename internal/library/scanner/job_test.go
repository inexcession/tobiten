package scanner_test

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
