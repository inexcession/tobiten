package openapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

func mustBuiltInRole(t *testing.T, id string) domain.Role {
	t.Helper()
	r, ok := domain.BuiltInRoleByID(id)
	if !ok {
		t.Fatalf("built-in role %q not found", id)
	}
	return r
}

func TestMiddlewareJobTriggerHelper(t *testing.T) {
	t.Parallel()

	t.Run("202 Accepted returns canonical job_id JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/scan", nil)
		rec := httptest.NewRecorder()
		middleware.HandleJobTrigger(rec, req, func(context.Context) (string, error) {
			return "job-scan-123", nil
		})
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 Accepted", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		var body middleware.JobAcceptedResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode JSON: %v", err)
		}
		if body.JobID != "job-scan-123" {
			t.Errorf("job_id = %q, want job-scan-123", body.JobID)
		}
	})

	t.Run("empty job_id fails closed with 500", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/scan", nil)
		rec := httptest.NewRecorder()
		middleware.HandleJobTrigger(rec, req, func(context.Context) (string, error) {
			return "   ", nil
		})
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", rec.Code)
		}
	})

	t.Run("nil trigger returns 503", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/scan", nil)
		rec := httptest.NewRecorder()
		middleware.HandleJobTrigger(rec, req, nil)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
	})

	t.Run("error mapping", func(t *testing.T) {
		cases := []struct {
			name     string
			err      error
			wantCode int
			wantAuth bool
		}{
			{name: "unauthorized", err: app.ErrUnauthorized, wantCode: http.StatusUnauthorized, wantAuth: true},
			{name: "forbidden", err: app.ErrForbidden, wantCode: http.StatusForbidden},
			{name: "not found", err: app.ErrNotFound, wantCode: http.StatusNotFound},
			{name: "conflict", err: app.ErrConflict, wantCode: http.StatusConflict},
			{name: "invalid input", err: app.ErrInvalidInput, wantCode: http.StatusBadRequest},
			{name: "too many requests", err: app.ErrTooManyRequests, wantCode: http.StatusTooManyRequests},
			{name: "service busy", err: app.ErrServiceBusy, wantCode: http.StatusServiceUnavailable},
			{name: "not implemented", err: app.ErrNotImplemented, wantCode: http.StatusNotImplemented},
			{name: "internal error redacts message", err: errors.New("db password leaked"), wantCode: http.StatusInternalServerError},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, "/api/v1/libraries/lib-1/scan", nil)
				rec := httptest.NewRecorder()
				middleware.HandleJobTrigger(rec, req, func(context.Context) (string, error) {
					return "", tc.err
				})
				if rec.Code != tc.wantCode {
					t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
				}
				if tc.wantAuth && !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic") {
					t.Errorf("expected WWW-Authenticate Basic header, got %q", rec.Header().Get("WWW-Authenticate"))
				}
				if strings.Contains(rec.Body.String(), "db password leaked") {
					t.Errorf("raw internal error leaked in body: %s", rec.Body.String())
				}
			})
		}
	})
}

func TestOpenAPIModule_JobDetailAndCancel(t *testing.T) {
	t.Parallel()

	aliceID := "u-alice"
	bobID := "u-bob"
	now := time.Now().UTC()

	stubAdmin := app.NewStubAdminService()
	stubAdmin.Jobs = []app.JobInfo{
		{
			ID:          "job-alice-queued",
			Kind:        string(domain.JobKindScan),
			State:       string(domain.JobStateQueued),
			MaxAttempts: 3,
			Checkpoint:  json.RawMessage(`{}`),
			CreatedBy:   &aliceID,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
		{
			ID:          "job-alice-running",
			Kind:        string(domain.JobKindRepack),
			State:       string(domain.JobStateRunning),
			Attempts:    1,
			MaxAttempts: 3,
			Checkpoint:  json.RawMessage(`{"progress_pct":40}`),
			CreatedBy:   &aliceID,
			CreatedAt:   now,
			StartedAt:   &now,
			UpdatedAt:   now,
		},
		{
			ID:          "job-alice-done",
			Kind:        string(domain.JobKindScrape),
			State:       string(domain.JobStateSucceeded),
			Attempts:    1,
			MaxAttempts: 3,
			ProgressPct: 100,
			Checkpoint:  json.RawMessage(`{}`),
			CreatedBy:   &aliceID,
			CreatedAt:   now,
			StartedAt:   &now,
			FinishedAt:  &now,
			UpdatedAt:   now,
		},
		{
			ID:          "job-auto-queued",
			Kind:        string(domain.JobKindScan),
			State:       string(domain.JobStateQueued),
			MaxAttempts: 3,
			Checkpoint:  json.RawMessage(`{}`),
			CreatedBy:   nil,
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}

	mod := NewModule(app.NewAuthorizingServices(&app.Services{Jobs: stubAdmin}))
	handler := mod.RouteMounts()[0].Handler

	readerRole := mustBuiltInRole(t, domain.RoleReaderID)
	adminRole := mustBuiltInRole(t, domain.RoleAdminID)
	readAllOnlyRole := domain.Role{ID: "r-readall", Name: "Auditor", Permissions: []domain.Permission{domain.PermissionJobsReadAll}}

	alice := domain.NewPrincipal(domain.User{ID: aliceID, Username: "alice"}, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-a", nil)
	aliceReadPAT := domain.NewPrincipal(domain.User{ID: aliceID, Username: "alice"}, []domain.Role{readerRole}, domain.CredentialKindPAT, "c-a-pat", []domain.PATScope{domain.ScopeRead})
	bob := domain.NewPrincipal(domain.User{ID: bobID, Username: "bob"}, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-b", nil)
	readAllOnly := domain.NewPrincipal(domain.User{ID: bobID, Username: "auditor"}, []domain.Role{readAllOnlyRole}, domain.CredentialKindPassword, "c-aud", nil)
	admin := domain.NewPrincipal(domain.User{ID: "u-admin", Username: "admin"}, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-adm", nil)

	doReq := func(method, path string, p *domain.Principal) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if p != nil {
			req = req.WithContext(domain.ContextWithPrincipal(req.Context(), p))
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	// Nil services return 503 when authenticated, 401 when unauthenticated.
	nilHandler := NewModule(nil).RouteMounts()[0].Handler
	{
		reqUnauth := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-alice-queued", nil)
		recUnauth := httptest.NewRecorder()
		nilHandler.ServeHTTP(recUnauth, reqUnauth)
		if recUnauth.Code != http.StatusUnauthorized {
			t.Fatalf("unauth GET /jobs status = %d, want 401", recUnauth.Code)
		}

		reqAuth := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-alice-queued", nil)
		reqAuth = reqAuth.WithContext(domain.ContextWithPrincipal(reqAuth.Context(), admin))
		recAuth := httptest.NewRecorder()
		nilHandler.ServeHTTP(recAuth, reqAuth)
		if recAuth.Code != http.StatusServiceUnavailable {
			t.Fatalf("nil-services GET /jobs status = %d, want 503", recAuth.Code)
		}
	}

	// Wrong methods return 405.
	if rec := doReq(http.MethodPost, "/api/v1/jobs/job-alice-queued", alice); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /jobs/{id} status = %d, want 405", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/api/v1/jobs/job-alice-queued/cancel", alice); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /jobs/{id}/cancel status = %d, want 405", rec.Code)
	}

	// Absent and invisible job IDs produce identical 404 responses on GET, HEAD, and POST cancel,
	// all marked Cache-Control: no-store.
	recAbsentGet := doReq(http.MethodGet, "/api/v1/jobs/does-not-exist", bob)
	recOtherGet := doReq(http.MethodGet, "/api/v1/jobs/job-alice-queued", bob)
	recAutoGet := doReq(http.MethodGet, "/api/v1/jobs/job-auto-queued", bob)
	if recAbsentGet.Code != http.StatusNotFound || recOtherGet.Code != http.StatusNotFound || recAutoGet.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for absent/other/auto GET, got %d/%d/%d", recAbsentGet.Code, recOtherGet.Code, recAutoGet.Code)
	}
	if recAbsentGet.Body.String() != recOtherGet.Body.String() || recAbsentGet.Body.String() != recAutoGet.Body.String() {
		t.Fatalf("GET 404 bodies differ: absent=%q other=%q auto=%q", recAbsentGet.Body.String(), recOtherGet.Body.String(), recAutoGet.Body.String())
	}
	for label, rec := range map[string]*httptest.ResponseRecorder{
		"absent GET": recAbsentGet,
		"other GET":  recOtherGet,
		"auto GET":   recAutoGet,
	} {
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("%s Cache-Control = %q, want no-store", label, cc)
		}
	}

	recAbsentHead := doReq(http.MethodHead, "/api/v1/jobs/does-not-exist", bob)
	recOtherHead := doReq(http.MethodHead, "/api/v1/jobs/job-alice-queued", bob)
	if recAbsentHead.Code != http.StatusNotFound || recOtherHead.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for absent/other HEAD, got %d/%d", recAbsentHead.Code, recOtherHead.Code)
	}
	if cc := recAbsentHead.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("absent HEAD Cache-Control = %q, want no-store", cc)
	}
	if cc := recOtherHead.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("other HEAD Cache-Control = %q, want no-store", cc)
	}

	recAbsentCancel := doReq(http.MethodPost, "/api/v1/jobs/does-not-exist/cancel", bob)
	recOtherCancel := doReq(http.MethodPost, "/api/v1/jobs/job-alice-queued/cancel", bob)
	if recAbsentCancel.Code != http.StatusNotFound || recOtherCancel.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for absent/other cancel, got %d/%d", recAbsentCancel.Code, recOtherCancel.Code)
	}
	if recAbsentCancel.Body.String() != recOtherCancel.Body.String() {
		t.Fatalf("POST cancel 404 bodies differ: absent=%q other=%q", recAbsentCancel.Body.String(), recOtherCancel.Body.String())
	}
	if recAbsentCancel.Header().Get("Cache-Control") != "no-store" || recOtherCancel.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("expected Cache-Control: no-store on 404 cancel responses, got absent=%q other=%q",
			recAbsentCancel.Header().Get("Cache-Control"), recOtherCancel.Header().Get("Cache-Control"))
	}

	// Creator and jobs:read_all get details with Cache-Control: no-store; HEAD returns empty body.
	recAliceGet := doReq(http.MethodGet, "/api/v1/jobs/job-alice-running", alice)
	if recAliceGet.Code != http.StatusOK {
		t.Fatalf("creator GET status = %d, want 200", recAliceGet.Code)
	}
	if cc := recAliceGet.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("creator GET Cache-Control = %q, want no-store", cc)
	}
	recHead := doReq(http.MethodHead, "/api/v1/jobs/job-alice-running", alice)
	if recHead.Code != http.StatusOK || recHead.Body.Len() != 0 {
		t.Fatalf("HEAD /jobs status=%d bodyLen=%d, want 200 and empty body", recHead.Code, recHead.Body.Len())
	}
	if cc := recHead.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("creator HEAD Cache-Control = %q, want no-store", cc)
	}
	if rec := doReq(http.MethodGet, "/api/v1/jobs/job-auto-queued", readAllOnly); rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("jobs:read_all GET automatic job = (%d, Cache-Control=%q), want (200, no-store)", rec.Code, rec.Header().Get("Cache-Control"))
	}

	// Visible without cancel authority -> 403.
	if rec := doReq(http.MethodPost, "/api/v1/jobs/job-alice-queued/cancel", aliceReadPAT); rec.Code != http.StatusForbidden {
		t.Fatalf("read PAT cancel own job status = %d, want 403", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/api/v1/jobs/job-auto-queued/cancel", readAllOnly); rec.Code != http.StatusForbidden {
		t.Fatalf("jobs:read_all-only cancel auto job status = %d, want 403", rec.Code)
	}

	// Creator cancels QUEUED -> 200 CANCELLED.
	recCancelQueued := doReq(http.MethodPost, "/api/v1/jobs/job-alice-queued/cancel", alice)
	if recCancelQueued.Code != http.StatusOK {
		t.Fatalf("cancel queued status = %d, want 200 (body: %s)", recCancelQueued.Code, recCancelQueued.Body.String())
	}
	var queuedOut app.JobInfo
	if err := json.Unmarshal(recCancelQueued.Body.Bytes(), &queuedOut); err != nil {
		t.Fatalf("decode cancel queued JSON: %v", err)
	}
	if queuedOut.State != string(domain.JobStateCancelled) || !queuedOut.CancelRequested {
		t.Fatalf("unexpected queued cancel response: %+v", queuedOut)
	}

	// Creator cancels RUNNING -> 200 RUNNING + cancel_requested: true.
	recCancelRunning := doReq(http.MethodPost, "/api/v1/jobs/job-alice-running/cancel", alice)
	if recCancelRunning.Code != http.StatusOK {
		t.Fatalf("cancel running status = %d, want 200", recCancelRunning.Code)
	}
	var runningOut app.JobInfo
	if err := json.Unmarshal(recCancelRunning.Body.Bytes(), &runningOut); err != nil {
		t.Fatalf("decode cancel running JSON: %v", err)
	}
	if runningOut.State != string(domain.JobStateRunning) || !runningOut.CancelRequested {
		t.Fatalf("unexpected running cancel response: %+v", runningOut)
	}

	// Authorized caller cancelling terminal job -> 409 Conflict.
	if rec := doReq(http.MethodPost, "/api/v1/jobs/job-alice-done/cancel", admin); rec.Code != http.StatusConflict {
		t.Fatalf("cancel terminal job status = %d, want 409", rec.Code)
	}
}
