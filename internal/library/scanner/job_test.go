package scanner_test

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
	trig := scanner.NewScanTrigger(jobService)

	if _, err := scanner.TriggerScan(ctx, nil, "lib-1", scanner.TriggerScanOptions{}); !errors.Is(err, app.ErrServiceBusy) {
		t.Fatalf("TriggerScan(nil) err = %v, want ErrServiceBusy", err)
	}
	if _, err := trig.TriggerScan(ctx, "   ", scanner.TriggerScanOptions{}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("TriggerScan(blank) err = %v, want ErrInvalidInput", err)
	}
	if _, err := trig.TriggerScan(ctx, " lib-1 ", scanner.TriggerScanOptions{}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatalf("TriggerScan(whitespace-padded) err = %v, want ErrInvalidInput", err)
	}

	id1, err := trig.TriggerScan(ctx, "lib-a", scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan(lib-a) #1: %v", err)
	}
	id2, err := trig.TriggerScan(ctx, "lib-a", scanner.TriggerScanOptions{})
	if err != nil {
		t.Fatalf("TriggerScan(lib-a) #2: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("duplicate TriggerScan(lib-a) returned %q, want %q", id2, id1)
	}
	idB, err := trig.TriggerScan(ctx, "lib-b", scanner.TriggerScanOptions{})
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

	trig := scanner.NewScanTrigger(jobs.NewService(jobRepo, jobs.ServiceOptions{}))

	// 1. Trigger and run scan -> SUCCEEDED, catalogue populated, progress ticks emitted.
	scanJobID, err := trig.TriggerScan(ctx, lib.ID, scanner.TriggerScanOptions{})
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
	scanJobID2, err := trig.TriggerScan(ctx, lib.ID, scanner.TriggerScanOptions{})
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
	missingLibJobID, err := trig.TriggerScan(ctx, "unknown-library-id", scanner.TriggerScanOptions{})
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
	if nextID, err := trig.TriggerScan(ctx, "unknown-library-id", scanner.TriggerScanOptions{}); err != nil || nextID == missingLibJobID {
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
