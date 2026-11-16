package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/jobs"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

func createTestPAT(ctx context.Context, t *testing.T, repo *postgres.UserRepository, userID string, scopes []domain.PATScope) string {
	t.Helper()
	patID := domain.MustNewID()
	token, hash, err := auth.NewPAT(patID)
	if err != nil {
		t.Fatalf("auth.NewPAT: %v", err)
	}
	scheme := auth.HashSchemeArgon2id
	exp := time.Now().UTC().Add(1 * time.Hour)
	if err := repo.CreateCredential(ctx, &domain.Credential{
		ID:         patID,
		UserID:     userID,
		Kind:       domain.CredentialKindPAT,
		Scopes:     scopes,
		SecretHash: &hash,
		HashScheme: &scheme,
		ExpiresAt:  &exp,
	}); err != nil {
		t.Fatalf("CreateCredential(PAT): %v", err)
	}
	return token
}

func createTestUserWithPassword(ctx context.Context, t *testing.T, repo *postgres.UserRepository, username, password, roleID string) domain.User {
	t.Helper()
	u := &domain.User{
		ID:       domain.MustNewID(),
		Username: username,
	}
	if err := repo.CreateUser(ctx, u); err != nil {
		t.Fatalf("CreateUser(%s): %v", username, err)
	}
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword(%s): %v", username, err)
	}
	scheme := auth.HashSchemeArgon2id
	if err := repo.CreateCredential(ctx, &domain.Credential{
		ID:         domain.MustNewID(),
		UserID:     u.ID,
		Kind:       domain.CredentialKindPassword,
		SecretHash: &hash,
		HashScheme: &scheme,
	}); err != nil {
		t.Fatalf("CreateCredential(%s): %v", username, err)
	}
	if roleID != "" {
		if err := repo.AssignUserRole(ctx, u.ID, roleID); err != nil {
			t.Fatalf("AssignUserRole(%s): %v", username, err)
		}
	}
	return *u
}

func TestDaemonJobsTriggerDetailAndCancelAPI(t *testing.T) {
	db := testdb.New(t)
	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_ADMIN_USER=sysadmin",
		"TOBITEN_ADMIN_PASS=admin-secret-123",
		"TOBITEN_BASE_URL=http://localhost:8080/comics",
		"TOBITEN_LOG_FORMAT=json",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	originalRegistry := buildDaemonRegistry
	originalConfigHook := configureDaemonJobRunner
	originalServer := startServer
	originalLogger := slog.Default()
	t.Cleanup(func() {
		buildDaemonRegistry = originalRegistry
		configureDaemonJobRunner = originalConfigHook
		startServer = originalServer
		slog.SetDefault(originalLogger)
	})

	var mu sync.Mutex
	var sharedRunner *jobs.Runner
	runningEntered := make(chan string, 4)
	releaseRunning := make(chan struct{})

	buildDaemonRegistry = func(cfg *config.Config, services *app.Services) module.Registry {
		reg := BuildRegistryWithServices(cfg, services)
		jobRepo := postgres.NewJobRepository(db.Pool)

		// Mount a test trigger endpoint using the shared 202 trigger helper + durable TriggerJob.
		triggerMod := &testJobRegistrarModule{
			SimpleModule: &module.SimpleModule{
				ModuleName: "job-api-probe",
				Jobs: []module.JobKind{
					module.JobKindSpec{Kind: domain.JobKindScan, Policy: domain.JobResumeCheckpoint, Workers: 1},
					module.JobKindSpec{Kind: domain.JobKindRepack, Policy: domain.JobResumeRestart, Workers: 1},
				},
				Mounts: []module.RouteMount{
					{
						ModuleName: "job-api-probe",
						Prefix:     "/api/v1/test-jobs",
						Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if r.Method != http.MethodPost || r.URL.Path != "/api/v1/test-jobs/trigger" {
								http.NotFound(w, r)
								return
							}
							p := domain.MustPrincipalFromContext(r.Context())
							var body struct {
								Kind      string  `json:"kind"`
								DedupeKey *string `json:"dedupe_key"`
								Automatic bool    `json:"automatic"`
							}
							if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
								middleware.WriteAppError(w, r, app.ErrInvalidInput)
								return
							}
							var createdBy *string
							if !body.Automatic {
								uid := p.User.ID
								createdBy = &uid
							}
							mu.Lock()
							notifier := sharedRunner
							mu.Unlock()
							middleware.HandleJobTrigger(w, r, func(ctx context.Context) (string, error) {
								return jobs.TriggerJob(ctx, jobRepo, notifier, jobs.TriggerRequest{
									Kind:      domain.JobKind(body.Kind),
									Payload:   json.RawMessage(`{"source":"probe"}`),
									DedupeKey: body.DedupeKey,
									CreatedBy: createdBy,
								}, time.Now().UTC())
							})
						}),
					},
				},
			},
			register: func(r *jobs.Runner) error {
				mu.Lock()
				sharedRunner = r
				mu.Unlock()

				// SCAN stays QUEUED unless manually claimed, or wait: use RunAfter in future for QUEUED tests,
				// and REPACK runs immediately and blocks until cancelled or released.
				if err := r.RegisterKind(jobs.KindConfig{
					Kind:         domain.JobKindScan,
					ResumePolicy: domain.JobResumeCheckpoint,
					Handler: jobs.HandlerFunc(func(ctx context.Context, exec *jobs.Execution) error {
						// Block until either cancelled or released.
						select {
						case runningEntered <- exec.Job().ID:
						default:
						}
						ticker := time.NewTicker(10 * time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-releaseRunning:
								return nil
							case <-ticker.C:
								cancelled, err := exec.CheckCancelled(ctx)
								if err == nil && cancelled {
									return jobs.ErrJobCancelled
								}
							}
						}
					}),
				}); err != nil {
					return err
				}
				return r.RegisterKind(jobs.KindConfig{
					Kind:         domain.JobKindRepack,
					ResumePolicy: domain.JobResumeRestart,
					Handler: jobs.HandlerFunc(func(ctx context.Context, exec *jobs.Execution) error {
						select {
						case runningEntered <- exec.Job().ID:
						default:
						}
						ticker := time.NewTicker(10 * time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-releaseRunning:
								return nil
							case <-ticker.C:
								cancelled, err := exec.CheckCancelled(ctx)
								if err == nil && cancelled {
									return jobs.ErrJobCancelled
								}
							}
						}
					}),
				})
			},
		}
		if err := reg.Register(triggerMod); err != nil {
			t.Fatalf("register probe module: %v", err)
		}
		return reg
	}

	startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger, services *app.Services) error {
		if services == nil || services.Jobs == nil {
			t.Fatal("expected daemon to wire services.Jobs")
		}
		reg := buildDaemonRegistry(cfg, services)
		srv, err := api.NewServer(api.ServerOptions{
			Config:   cfg,
			Logger:   logger,
			Registry: reg,
		})
		if err != nil {
			return err
		}
		defer func() { _ = srv.Close() }()

		userRepo := postgres.NewUserRepository(db.Pool)
		jobRepo := postgres.NewJobRepository(db.Pool)

		// Verify built-in role permissions seeded in PostgreSQL (0007_job_cancel_permissions.sql).
		roles, err := userRepo.ListRoles(ctx)
		if err != nil {
			t.Fatalf("ListRoles: %v", err)
		}
		roleMap := make(map[string]domain.Role, len(roles))
		for _, role := range roles {
			roleMap[role.ID] = role
		}
		for _, rid := range []string{domain.RoleAdminID, domain.RoleCuratorID, domain.RoleReaderID, domain.RoleRestrictedReaderID} {
			if !roleMap[rid].BuiltIn {
				t.Fatalf("missing built-in role %s", rid)
			}
		}

		sysadmin, err := userRepo.GetUserByUsername(ctx, "sysadmin")
		if err != nil {
			t.Fatalf("GetUserByUsername(sysadmin): %v", err)
		}
		alice := createTestUserWithPassword(ctx, t, userRepo, "alice-api", "alice-pass-123", domain.RoleReaderID)
		bob := createTestUserWithPassword(ctx, t, userRepo, "bob-api", "bob-pass-123", domain.RoleReaderID)
		tempUser := createTestUserWithPassword(ctx, t, userRepo, "temp-api", "temp-pass-123", domain.RoleReaderID)

		// Custom roles for fine-grained I-10 checks.
		const (
			roleReadAllOnlyID   = "019318a0-0000-7000-8000-000000000101"
			roleCancelAllOnlyID = "019318a0-0000-7000-8000-000000000102"
			roleNoCancelID      = "019318a0-0000-7000-8000-000000000103"
		)
		if _, err := db.Exec(ctx, `
			INSERT INTO roles (id, name, built_in, requires_allow_labels) VALUES
				($1, 'Job Auditor', FALSE, FALSE),
				($2, 'Blind Canceller', FALSE, FALSE),
				($3, 'Read Only Custom', FALSE, FALSE)
		`, roleReadAllOnlyID, roleCancelAllOnlyID, roleNoCancelID); err != nil {
			t.Fatalf("insert custom roles: %v", err)
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO role_permissions (role_id, permission) VALUES
				($1, 'library:read'), ($1, 'jobs:read_all'),
				($2, 'library:read'), ($2, 'jobs:cancel_all'),
				($3, 'library:read')
		`, roleReadAllOnlyID, roleCancelAllOnlyID, roleNoCancelID); err != nil {
			t.Fatalf("insert custom role permissions: %v", err)
		}

		auditor := createTestUserWithPassword(ctx, t, userRepo, "auditor-api", "auditor-pass-123", roleReadAllOnlyID)
		blindCanceller := createTestUserWithPassword(ctx, t, userRepo, "blind-api", "blind-pass-123", roleCancelAllOnlyID)

		adminReadPAT := createTestPAT(ctx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeRead})
		adminLibPAT := createTestPAT(ctx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeLibraryAdmin})
		adminAdminPAT := createTestPAT(ctx, t, userRepo, sysadmin.ID, []domain.PATScope{domain.ScopeAdmin})
		aliceReadPAT := createTestPAT(ctx, t, userRepo, alice.ID, []domain.PATScope{domain.ScopeRead})
		aliceMetaPAT := createTestPAT(ctx, t, userRepo, alice.ID, []domain.PATScope{domain.ScopeMetadataWrite})
		aliceAdminPAT := createTestPAT(ctx, t, userRepo, alice.ID, []domain.PATScope{domain.ScopeAdmin})
		_ = auditor
		_ = blindCanceller

		callBasic := func(method, path, body, user, pass string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			req.RemoteAddr = "198.51.100.55:4001"
			if body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			if user != "" {
				req.SetBasicAuth(user, pass)
			}
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			return rec
		}

		callPAT := func(method, path, body, token string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			req.RemoteAddr = "198.51.100.55:4002"
			if body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			return rec
		}

		// 1. Shared 202 trigger helper + deduplication + validation on real daemon.
		recBad := callBasic(http.MethodPost, "/api/v1/test-jobs/trigger", `{"kind":""}`, alice.Username, "alice-pass-123")
		if recBad.Code != http.StatusBadRequest {
			t.Fatalf("empty kind trigger status = %d, want 400 (body: %s)", recBad.Code, recBad.Body.String())
		}

		// Queue a future-scheduled SCAN job so it stays QUEUED until cancelled.
		future := time.Now().UTC().Add(1 * time.Hour)
		dedupeKey := "SCAN:lib-dedupe-1"
		queuedJob := &domain.Job{
			Kind:      domain.JobKindScan,
			Payload:   json.RawMessage(`{"library_id":"lib-dedupe-1"}`),
			CreatedBy: &alice.ID,
			DedupeKey: &dedupeKey,
			RunAfter:  future,
		}
		if created, err := jobRepo.Enqueue(ctx, queuedJob); err != nil || !created {
			t.Fatalf("Enqueue future queuedJob = (%v, %v)", created, err)
		}

		// Repeated trigger through HTTP 202 helper returns the exact same active job_id.
		recTrig1 := callBasic(http.MethodPost, "/api/v1/test-jobs/trigger", `{"kind":"SCAN","dedupe_key":"SCAN:lib-dedupe-1"}`, alice.Username, "alice-pass-123")
		if recTrig1.Code != http.StatusAccepted {
			t.Fatalf("trigger dedupe status = %d, want 202 (body: %s)", recTrig1.Code, recTrig1.Body.String())
		}
		var trigResp1 middleware.JobAcceptedResponse
		if err := json.Unmarshal(recTrig1.Body.Bytes(), &trigResp1); err != nil {
			t.Fatalf("decode trigger response: %v", err)
		}
		if trigResp1.JobID != queuedJob.ID {
			t.Fatalf("deduplicated trigger returned job_id=%q, want %q", trigResp1.JobID, queuedJob.ID)
		}

		// Enqueue an automatic (created_by = nil) future QUEUED job and a creator-deleted future QUEUED job.
		autoJob := &domain.Job{
			Kind:     domain.JobKindScan,
			Payload:  json.RawMessage(`{"auto":true}`),
			RunAfter: future,
		}
		if created, err := jobRepo.Enqueue(ctx, autoJob); err != nil || !created {
			t.Fatalf("Enqueue autoJob = (%v, %v)", created, err)
		}

		deletedCreatorJob := &domain.Job{
			Kind:      domain.JobKindScan,
			Payload:   json.RawMessage(`{"deleted_creator":true}`),
			CreatedBy: &tempUser.ID,
			RunAfter:  future,
		}
		if created, err := jobRepo.Enqueue(ctx, deletedCreatorJob); err != nil || !created {
			t.Fatalf("Enqueue deletedCreatorJob = (%v, %v)", created, err)
		}
		if _, err := db.Exec(ctx, `DELETE FROM users WHERE id = $1`, tempUser.ID); err != nil {
			t.Fatalf("delete tempUser: %v", err)
		}

		// Trigger a runnable REPACK job via the HTTP 202 helper (also via configured sub-path /comics) and wait for it to enter RUNNING.
		recRunTrig := callBasic(http.MethodPost, "/comics/api/v1/test-jobs/trigger", `{"kind":"REPACK"}`, alice.Username, "alice-pass-123")
		if recRunTrig.Code != http.StatusAccepted {
			t.Fatalf("runnable trigger status = %d, want 202 (body: %s)", recRunTrig.Code, recRunTrig.Body.String())
		}
		var runTrigResp middleware.JobAcceptedResponse
		if err := json.Unmarshal(recRunTrig.Body.Bytes(), &runTrigResp); err != nil {
			t.Fatalf("decode runnable trigger JSON: %v", err)
		}
		select {
		case enteredID := <-runningEntered:
			if enteredID != runTrigResp.JobID {
				t.Fatalf("worker entered job %q, want %q", enteredID, runTrigResp.JobID)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for worker to enter RUNNING")
		}

		// 2. Unauthenticated access must fail closed with 401.
		for _, tc := range []struct{ method, path string }{
			{http.MethodGet, "/api/v1/jobs/" + queuedJob.ID},
			{http.MethodPost, "/api/v1/jobs/" + queuedJob.ID + "/cancel"},
			{http.MethodGet, "/comics/api/v1/jobs/" + queuedJob.ID},
			{http.MethodPost, "/comics/api/v1/jobs/" + queuedJob.ID + "/cancel"},
		} {
			rec := callBasic(tc.method, tc.path, "", "", "")
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated %s %s status = %d, want 401", tc.method, tc.path, rec.Code)
			}
			if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic") {
				t.Errorf("unauthenticated %s %s missing Basic challenge", tc.method, tc.path)
			}
		}

		// 3. GET /api/v1/jobs/{job_id}: non-creator gets exact same 404 as absent ID; creator and jobs:read_all get details; automatic & deleted-creator jobs admin-only.
		recAbsentGet := callBasic(http.MethodGet, "/api/v1/jobs/019318a0-0000-7000-8000-999999999999", "", bob.Username, "bob-pass-123")
		if recAbsentGet.Code != http.StatusNotFound {
			t.Fatalf("absent GET status = %d, want 404", recAbsentGet.Code)
		}
		for _, hiddenPath := range []string{
			"/api/v1/jobs/" + queuedJob.ID,
			"/api/v1/jobs/" + autoJob.ID,
			"/api/v1/jobs/" + deletedCreatorJob.ID,
		} {
			recBob := callBasic(http.MethodGet, hiddenPath, "", bob.Username, "bob-pass-123")
			if recBob.Code != http.StatusNotFound || recBob.Body.String() != recAbsentGet.Body.String() || recBob.Header().Get("Content-Type") != recAbsentGet.Header().Get("Content-Type") {
				t.Fatalf("bob GET %s = (%d, %q), want identical 404 (%d, %q)", hiddenPath, recBob.Code, recBob.Body.String(), recAbsentGet.Code, recAbsentGet.Body.String())
			}
			recBlind := callBasic(http.MethodGet, hiddenPath, "", blindCanceller.Username, "blind-pass-123")
			if recBlind.Code != http.StatusNotFound || recBlind.Body.String() != recAbsentGet.Body.String() {
				t.Fatalf("blindCanceller GET %s = (%d, %q), want identical 404", hiddenPath, recBlind.Code, recBlind.Body.String())
			}
			recLibPAT := callPAT(http.MethodGet, hiddenPath, "", adminLibPAT)
			if recLibPAT.Code != http.StatusNotFound || recLibPAT.Body.String() != recAbsentGet.Body.String() {
				t.Fatalf("admin library:admin PAT GET %s = (%d, %q), want identical 404", hiddenPath, recLibPAT.Code, recLibPAT.Body.String())
			}
		}

		// Creator, auditor (jobs:read_all), and admin (admin PAT) can read permitted job details.
		if rec := callBasic(http.MethodGet, "/api/v1/jobs/"+queuedJob.ID, "", alice.Username, "alice-pass-123"); rec.Code != http.StatusOK {
			t.Fatalf("creator GET status = %d, want 200", rec.Code)
		}
		if rec := callBasic(http.MethodGet, "/api/v1/jobs/"+autoJob.ID, "", auditor.Username, "auditor-pass-123"); rec.Code != http.StatusOK {
			t.Fatalf("auditor (jobs:read_all) GET autoJob status = %d, want 200", rec.Code)
		}
		recDelRead := callPAT(http.MethodGet, "/comics/api/v1/jobs/"+deletedCreatorJob.ID, "", adminAdminPAT)
		if recDelRead.Code != http.StatusOK {
			t.Fatalf("admin PAT GET deletedCreatorJob status = %d, want 200", recDelRead.Code)
		}
		var delReadBody app.JobInfo
		if err := json.Unmarshal(recDelRead.Body.Bytes(), &delReadBody); err != nil {
			t.Fatalf("decode deletedCreatorJob JSON: %v", err)
		}
		if delReadBody.CreatedBy != nil {
			t.Fatalf("deleted creator job CreatedBy = %v, want nil", *delReadBody.CreatedBy)
		}

		// 4. POST /api/v1/jobs/{job_id}/cancel authorization + state matrix (I-10).
		recAbsentCancel := callBasic(http.MethodPost, "/api/v1/jobs/019318a0-0000-7000-8000-999999999999/cancel", "", bob.Username, "bob-pass-123")
		if recAbsentCancel.Code != http.StatusNotFound {
			t.Fatalf("absent cancel status = %d, want 404", recAbsentCancel.Code)
		}
		for _, hiddenCancel := range []string{
			"/api/v1/jobs/" + queuedJob.ID + "/cancel",
			"/api/v1/jobs/" + autoJob.ID + "/cancel",
			"/api/v1/jobs/" + deletedCreatorJob.ID + "/cancel",
		} {
			if rec := callBasic(http.MethodPost, hiddenCancel, "", bob.Username, "bob-pass-123"); rec.Code != http.StatusNotFound || rec.Body.String() != recAbsentCancel.Body.String() {
				t.Fatalf("bob POST %s = (%d, %q), want 404 matching absent", hiddenCancel, rec.Code, rec.Body.String())
			}
			if rec := callBasic(http.MethodPost, hiddenCancel, "", blindCanceller.Username, "blind-pass-123"); rec.Code != http.StatusNotFound || rec.Body.String() != recAbsentCancel.Body.String() {
				t.Fatalf("blindCanceller POST %s = (%d, %q), want 404 matching absent", hiddenCancel, rec.Code, rec.Body.String())
			}
			if rec := callPAT(http.MethodPost, hiddenCancel, "", adminReadPAT); rec.Code != http.StatusNotFound {
				t.Fatalf("admin read PAT POST %s = %d, want 404", hiddenCancel, rec.Code)
			}
		}

		// Visible without cancellation authority -> 403 Forbidden.
		for _, pat := range []string{aliceReadPAT, aliceMetaPAT} {
			if rec := callPAT(http.MethodPost, "/api/v1/jobs/"+queuedJob.ID+"/cancel", "", pat); rec.Code != http.StatusForbidden {
				t.Fatalf("alice non-admin PAT cancel own job status = %d, want 403", rec.Code)
			}
		}
		for _, visibleNoCancel := range []string{
			"/api/v1/jobs/" + queuedJob.ID + "/cancel",
			"/api/v1/jobs/" + autoJob.ID + "/cancel",
			"/api/v1/jobs/" + deletedCreatorJob.ID + "/cancel",
		} {
			if rec := callBasic(http.MethodPost, visibleNoCancel, "", auditor.Username, "auditor-pass-123"); rec.Code != http.StatusForbidden {
				t.Fatalf("auditor (jobs:read_all only) POST %s = %d, want 403", visibleNoCancel, rec.Code)
			}
		}

		// Creator with admin PAT (capped to Reader -> has jobs:cancel_own) cancels QUEUED job -> 200 CANCELLED.
		recCancelQueued := callPAT(http.MethodPost, "/comics/api/v1/jobs/"+queuedJob.ID+"/cancel", "", aliceAdminPAT)
		if recCancelQueued.Code != http.StatusOK {
			t.Fatalf("alice admin PAT cancel queued status = %d, want 200 (body: %s)", recCancelQueued.Code, recCancelQueued.Body.String())
		}
		var snapQueued app.JobInfo
		if err := json.Unmarshal(recCancelQueued.Body.Bytes(), &snapQueued); err != nil {
			t.Fatalf("decode snapQueued: %v", err)
		}
		if snapQueued.State != string(domain.JobStateCancelled) || !snapQueued.CancelRequested {
			t.Fatalf("queued cancel snapshot = %+v, want CANCELLED and cancel_requested=true", snapQueued)
		}

		// Re-triggering with same dedupe_key now yields a new job_id because cancelled released the key.
		recTrig2 := callBasic(http.MethodPost, "/api/v1/test-jobs/trigger", `{"kind":"SCAN","dedupe_key":"SCAN:lib-dedupe-1"}`, alice.Username, "alice-pass-123")
		if recTrig2.Code != http.StatusAccepted {
			t.Fatalf("post-cancel trigger status = %d, want 202", recTrig2.Code)
		}
		var trigResp2 middleware.JobAcceptedResponse
		if err := json.Unmarshal(recTrig2.Body.Bytes(), &trigResp2); err != nil {
			t.Fatalf("decode trigResp2: %v", err)
		}
		if trigResp2.JobID == "" || trigResp2.JobID == queuedJob.ID {
			t.Fatalf("post-cancel trigger returned %q, want new job_id distinct from %q", trigResp2.JobID, queuedJob.ID)
		}

		// Cancelling terminal job -> 409 Conflict.
		if rec := callBasic(http.MethodPost, "/api/v1/jobs/"+queuedJob.ID+"/cancel", "", alice.Username, "alice-pass-123"); rec.Code != http.StatusConflict {
			t.Fatalf("cancel already-cancelled job status = %d, want 409", rec.Code)
		}

		// Cookie session login + Cache-Control: no-store checks on GET/HEAD (200 and absent/invisible 404)
		// + CSRF check on POST /api/v1/jobs/{job_id}/cancel for RUNNING job.
		_, sessCookie, csrfTok := performLogin(t, srv.Handler(), "/api/v1/auth/login", `{"username":"alice-api","password":"alice-pass-123"}`, "198.51.100.55:4010", nil)
		callCookie := func(method, path string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(method, path, nil)
			req.RemoteAddr = "198.51.100.55:4011"
			req.AddCookie(sessCookie)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			return rec
		}

		for _, tc := range []struct {
			name   string
			method string
			path   string
		}{
			{name: "direct GET own snapshot", method: http.MethodGet, path: "/api/v1/jobs/" + runTrigResp.JobID},
			{name: "direct HEAD own snapshot", method: http.MethodHead, path: "/api/v1/jobs/" + runTrigResp.JobID},
			{name: "subpath GET own snapshot", method: http.MethodGet, path: "/comics/api/v1/jobs/" + runTrigResp.JobID},
			{name: "subpath HEAD own snapshot", method: http.MethodHead, path: "/comics/api/v1/jobs/" + runTrigResp.JobID},
		} {
			rec := callCookie(tc.method, tc.path)
			if rec.Code != http.StatusOK {
				t.Fatalf("cookie %s status = %d, want 200 (body: %s)", tc.name, rec.Code, rec.Body.String())
			}
			if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
				t.Fatalf("cookie %s Cache-Control = %q, want no-store", tc.name, cc)
			}
			if tc.method == http.MethodHead && rec.Body.Len() != 0 {
				t.Fatalf("cookie %s HEAD returned non-empty body %q", tc.name, rec.Body.String())
			}
		}

		// Cookie-authenticated GET and HEAD for absent vs invisible (automatic/deleted-creator) jobs:
		// must return identical 404 responses with Cache-Control: no-store.
		recCookieAbsentGet := callCookie(http.MethodGet, "/api/v1/jobs/019318a0-0000-7000-8000-999999999999")
		recCookieAbsentHead := callCookie(http.MethodHead, "/api/v1/jobs/019318a0-0000-7000-8000-999999999999")
		if recCookieAbsentGet.Code != http.StatusNotFound || recCookieAbsentHead.Code != http.StatusNotFound {
			t.Fatalf("cookie absent GET/HEAD status = %d/%d, want 404/404", recCookieAbsentGet.Code, recCookieAbsentHead.Code)
		}
		if recCookieAbsentGet.Header().Get("Cache-Control") != "no-store" || recCookieAbsentHead.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("cookie absent GET/HEAD Cache-Control = %q/%q, want no-store/no-store",
				recCookieAbsentGet.Header().Get("Cache-Control"), recCookieAbsentHead.Header().Get("Cache-Control"))
		}
		for _, invisiblePath := range []string{
			"/api/v1/jobs/" + autoJob.ID,
			"/comics/api/v1/jobs/" + deletedCreatorJob.ID,
		} {
			recInvGet := callCookie(http.MethodGet, invisiblePath)
			recInvHead := callCookie(http.MethodHead, invisiblePath)
			if recInvGet.Code != http.StatusNotFound || recInvHead.Code != http.StatusNotFound {
				t.Fatalf("cookie invisible %s GET/HEAD = %d/%d, want 404/404", invisiblePath, recInvGet.Code, recInvHead.Code)
			}
			if recInvGet.Header().Get("Cache-Control") != "no-store" || recInvHead.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("cookie invisible %s GET/HEAD Cache-Control = %q/%q, want no-store/no-store",
					invisiblePath, recInvGet.Header().Get("Cache-Control"), recInvHead.Header().Get("Cache-Control"))
			}
			if recInvGet.Body.String() != recCookieAbsentGet.Body.String() || recInvHead.Body.Len() != 0 {
				t.Fatalf("cookie invisible %s 404 body mismatch: get=%q absent=%q headLen=%d",
					invisiblePath, recInvGet.Body.String(), recCookieAbsentGet.Body.String(), recInvHead.Body.Len())
			}
		}

		// X-API-Key authentication also receives Cache-Control: no-store on job snapshots.
		reqAPIKey := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+runTrigResp.JobID, nil)
		reqAPIKey.RemoteAddr = "198.51.100.55:4012"
		reqAPIKey.Header.Set("X-API-Key", aliceReadPAT)
		recAPIKey := httptest.NewRecorder()
		srv.Handler().ServeHTTP(recAPIKey, reqAPIKey)
		if recAPIKey.Code != http.StatusOK || recAPIKey.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("X-API-Key GET snapshot = (%d, Cache-Control=%q), want (200, no-store)",
				recAPIKey.Code, recAPIKey.Header().Get("Cache-Control"))
		}

		reqNoCSRF := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+runTrigResp.JobID+"/cancel", nil)
		reqNoCSRF.AddCookie(sessCookie)
		recNoCSRF := httptest.NewRecorder()
		srv.Handler().ServeHTTP(recNoCSRF, reqNoCSRF)
		if recNoCSRF.Code != http.StatusForbidden {
			t.Fatalf("cookie cancel without CSRF status = %d, want 403", recNoCSRF.Code)
		}

		reqWithCSRF := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/"+runTrigResp.JobID+"/cancel", nil)
		reqWithCSRF.AddCookie(sessCookie)
		reqWithCSRF.Header.Set("X-CSRF-Token", csrfTok)
		recCancelRun := httptest.NewRecorder()
		srv.Handler().ServeHTTP(recCancelRun, reqWithCSRF)
		if recCancelRun.Code != http.StatusOK {
			t.Fatalf("cookie cancel RUNNING with CSRF status = %d, want 200 (body: %s)", recCancelRun.Code, recCancelRun.Body.String())
		}
		if cc := recCancelRun.Header().Get("Cache-Control"); cc != "no-store" {
			t.Fatalf("cookie cancel RUNNING Cache-Control = %q, want no-store", cc)
		}
		var snapRun app.JobInfo
		if err := json.Unmarshal(recCancelRun.Body.Bytes(), &snapRun); err != nil {
			t.Fatalf("decode snapRun: %v", err)
		}
		if snapRun.State != string(domain.JobStateRunning) || !snapRun.CancelRequested {
			t.Fatalf("running cancel snapshot = %+v, want RUNNING and cancel_requested=true", snapRun)
		}

		// Wait for real worker pool to observe cancel_requested and transition REPACK job to CANCELLED.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			recPoll := callBasic(http.MethodGet, "/api/v1/jobs/"+runTrigResp.JobID, "", alice.Username, "alice-pass-123")
			if recPoll.Code == http.StatusOK {
				var polled app.JobInfo
				if err := json.Unmarshal(recPoll.Body.Bytes(), &polled); err == nil && polled.State == string(domain.JobStateCancelled) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
		}
		if rec := callBasic(http.MethodPost, "/api/v1/jobs/"+runTrigResp.JobID+"/cancel", "", alice.Username, "alice-pass-123"); rec.Code != http.StatusConflict {
			t.Fatalf("cancel worker-cancelled job status = %d, want 409", rec.Code)
		}

		// Admin cancels automatic and creator-deleted jobs -> 200 CANCELLED.
		if rec := callPAT(http.MethodPost, "/api/v1/jobs/"+autoJob.ID+"/cancel", "", adminAdminPAT); rec.Code != http.StatusOK {
			t.Fatalf("admin PAT cancel autoJob status = %d, want 200", rec.Code)
		}
		if rec := callBasic(http.MethodPost, "/api/v1/jobs/"+deletedCreatorJob.ID+"/cancel", "", "sysadmin", "admin-secret-123"); rec.Code != http.StatusOK {
			t.Fatalf("admin Basic cancel deletedCreatorJob status = %d, want 200", rec.Code)
		}

		// Verify job.cancelled outbox events were recorded for cancelled jobs.
		var cancelledEvents int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM domain_events WHERE kind = 'job.cancelled' AND aggregate_id IN ($1, $2, $3, $4)`,
			queuedJob.ID, runTrigResp.JobID, autoJob.ID, deletedCreatorJob.ID).Scan(&cancelledEvents); err != nil {
			t.Fatalf("count job.cancelled outbox events: %v", err)
		}
		if cancelledEvents != 4 {
			t.Fatalf("job.cancelled outbox events = %d, want 4", cancelledEvents)
		}

		// 5. Role demotion immediately lowers an existing admin PAT's job permissions.
		// Warm cache for adminAdminPAT first (already used above), then demote sysadmin -> Reader.
		if err := userRepo.RemoveUserRole(ctx, sysadmin.ID, domain.RoleAdminID); err != nil {
			t.Fatalf("RemoveUserRole(sysadmin, Admin): %v", err)
		}
		if err := userRepo.AssignUserRole(ctx, sysadmin.ID, domain.RoleReaderID); err != nil {
			t.Fatalf("AssignUserRole(sysadmin, Reader): %v", err)
		}
		// Demoted user with existing admin PAT can no longer see/cancel alice's newly created job (404).
		if rec := callPAT(http.MethodGet, "/api/v1/jobs/"+trigResp2.JobID, "", adminAdminPAT); rec.Code != http.StatusNotFound {
			t.Fatalf("demoted admin PAT GET other job status = %d, want 404", rec.Code)
		}
		if rec := callPAT(http.MethodPost, "/api/v1/jobs/"+trigResp2.JobID+"/cancel", "", adminAdminPAT); rec.Code != http.StatusNotFound {
			t.Fatalf("demoted admin PAT cancel other job status = %d, want 404", rec.Code)
		}

		// Demote alice from Reader -> roleNoCancelID: alice's existing admin PAT loses jobs:cancel_own -> 403 on own job!
		aliceOwnJob := &domain.Job{Kind: domain.JobKindScan, CreatedBy: &alice.ID, RunAfter: future}
		if _, err := jobRepo.Enqueue(ctx, aliceOwnJob); err != nil {
			t.Fatalf("Enqueue aliceOwnJob: %v", err)
		}
		if err := userRepo.RemoveUserRole(ctx, alice.ID, domain.RoleReaderID); err != nil {
			t.Fatalf("RemoveUserRole(alice, Reader): %v", err)
		}
		if err := userRepo.AssignUserRole(ctx, alice.ID, roleNoCancelID); err != nil {
			t.Fatalf("AssignUserRole(alice, roleNoCancelID): %v", err)
		}
		if rec := callPAT(http.MethodGet, "/api/v1/jobs/"+aliceOwnJob.ID, "", aliceAdminPAT); rec.Code != http.StatusOK {
			t.Fatalf("demoted alice GET own job status = %d, want 200", rec.Code)
		}
		if rec := callPAT(http.MethodPost, "/api/v1/jobs/"+aliceOwnJob.ID+"/cancel", "", aliceAdminPAT); rec.Code != http.StatusForbidden {
			t.Fatalf("demoted alice admin PAT cancel own job status = %d, want 403", rec.Code)
		}

		close(releaseRunning)
		return nil
	}

	if err := runDaemon(context.Background(), cfg, startServer); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("runDaemon failed: %v", err)
	}
}
