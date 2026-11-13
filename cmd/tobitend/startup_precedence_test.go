package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestDaemonCredentialPrecedenceAndConflictsPostgres(t *testing.T) {
	db := testdb.New(t)
	t.Setenv("TOBITEN_STORAGE_MODE", "postgres")
	t.Setenv("TOBITEN_DATABASE_URL", db.URL)
	t.Setenv("TOBITEN_ADMIN_USER", "alice")
	t.Setenv("TOBITEN_ADMIN_PASS", "initial-secret-123")
	t.Setenv("TOBITEN_LOG_FORMAT", "json")

	originalServer, originalLogger := startServer, slog.Default()
	t.Cleanup(func() {
		startServer = originalServer
		slog.SetDefault(originalLogger)
	})

	startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger, services *app.Services) error {
		reg := BuildRegistryWithServices(cfg, services)
		probeMod := module.NewSimpleModule("probe")
		probeMod.Mounts = []module.RouteMount{
			{
				ModuleName: "probe",
				Prefix:     "/api/v1/probe",
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					p := domain.MustPrincipalFromContext(r.Context())
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(p)
				}),
			},
		}
		if err := reg.Register(probeMod); err != nil {
			t.Fatalf("register probe module: %v", err)
		}

		srv, err := api.NewServer(api.ServerOptions{
			Config:   cfg,
			Logger:   logger,
			Listener: httptest.NewUnstartedServer(nil).Listener,
			Registry: reg,
		})
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		defer func() { _ = srv.Close() }()

		repo := postgres.NewUserRepository(db.Pool)
		alice, err := repo.GetUserByUsername(ctx, "alice")
		if err != nil {
			t.Fatalf("GetUserByUsername: %v", err)
		}

		scheme := auth.HashSchemeArgon2id
		futureExp := time.Now().UTC().Add(time.Hour)

		pat1ID := domain.MustNewID()
		pat1Token, pat1Hash, err := auth.NewPAT(pat1ID)
		if err != nil {
			t.Fatalf("NewPAT(1): %v", err)
		}
		if err := repo.CreateCredential(ctx, &domain.Credential{
			ID:         pat1ID,
			UserID:     alice.ID,
			Kind:       domain.CredentialKindPAT,
			Scopes:     []domain.PATScope{domain.ScopeRead},
			SecretHash: &pat1Hash,
			HashScheme: &scheme,
			ExpiresAt:  &futureExp,
		}); err != nil {
			t.Fatalf("CreateCredential(pat1): %v", err)
		}

		pat2ID := domain.MustNewID()
		pat2Token, pat2Hash, err := auth.NewPAT(pat2ID)
		if err != nil {
			t.Fatalf("NewPAT(2): %v", err)
		}
		if err := repo.CreateCredential(ctx, &domain.Credential{
			ID:         pat2ID,
			UserID:     alice.ID,
			Kind:       domain.CredentialKindPAT,
			Scopes:     []domain.PATScope{domain.ScopeProgressWrite},
			SecretHash: &pat2Hash,
			HashScheme: &scheme,
			ExpiresAt:  &futureExp,
		}); err != nil {
			t.Fatalf("CreateCredential(pat2): %v", err)
		}

		doReq := func(path string, headers map[string][]string, remote string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.RemoteAddr = remote
			for k, vals := range headers {
				for _, v := range vals {
					req.Header.Add(k, v)
				}
			}
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			return rec
		}

		validBasic := basicAuthHeader("alice", "initial-secret-123")
		invalidBasic := basicAuthHeader("alice", "wrong-secret")

		// 1. Valid Basic + Valid PAT (same user Alice) MUST return 401 (no early-success escape, no grant union).
		rec := doReq("/api/v1/probe", map[string][]string{
			"Authorization": {validBasic, "Bearer " + pat1Token},
		}, "198.51.100.20:1001")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("Basic + Bearer PAT status = %d, want 401", rec.Code)
		}

		// 2. Valid Basic + Invalid X-API-Key MUST return 401 without updating PAT last_used_at.
		rec = doReq("/api/v1/probe", map[string][]string{
			"Authorization": {validBasic},
			"X-API-Key":     {"tobiten_pat_" + pat1ID + "_wrong-secret"},
		}, "198.51.100.20:1002")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("Basic + invalid X-API-Key status = %d, want 401", rec.Code)
		}

		// 3. Valid PAT + Invalid Basic MUST return 401.
		rec = doReq("/api/v1/probe", map[string][]string{
			"Authorization": {invalidBasic},
			"X-API-Key":     {pat1Token},
		}, "198.51.100.20:1003")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("Invalid Basic + valid X-API-Key status = %d, want 401", rec.Code)
		}

		// 4. Distinct Bearer PAT1 + X-API-Key PAT2 (same user Alice) MUST return 401.
		rec = doReq("/api/v1/probe", map[string][]string{
			"Authorization": {"Bearer " + pat1Token},
			"X-API-Key":     {pat2Token},
		}, "198.51.100.20:1004")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("Bearer PAT1 + X-API-Key PAT2 status = %d, want 401", rec.Code)
		}

		// Confirm none of the conflicted/rejected requests touched pat1 or pat2 last_used_at.
		for _, id := range []string{pat1ID, pat2ID} {
			c, err := repo.GetCredentialByID(ctx, id)
			if err != nil {
				t.Fatalf("GetCredentialByID(%s): %v", id, err)
			}
			if c.LastUsedAt != nil {
				t.Fatalf("credential %s last_used_at must remain nil after conflict, got %v", id, c.LastUsedAt)
			}
		}

		// 5. X-Auth-Token with no registered verifier MUST return 401 on protected and /healthz routes.
		for _, path := range []string{"/api/v1/probe", "/healthz"} {
			rec = doReq(path, map[string][]string{
				"X-Auth-Token": {"komga-session-token"},
			}, "198.51.100.20:1005")
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("unverified X-Auth-Token on %s status = %d, want 401", path, rec.Code)
			}
		}

		// 6. Repeated identical Basic headers succeed.
		rec = doReq("/api/v1/probe", map[string][]string{
			"Authorization": {validBasic, strings.ToLower(validBasic[:6]) + validBasic[6:]},
		}, "198.51.100.20:1006")
		if rec.Code != http.StatusOK {
			t.Fatalf("repeated identical Basic status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}

		// 7. Repeated identical Bearer + X-API-Key PAT succeeds without widening scopes.
		rec = doReq("/api/v1/probe", map[string][]string{
			"Authorization": {"Bearer " + pat1Token},
			"X-API-Key":     {pat1Token},
		}, "198.51.100.20:1007")
		if rec.Code != http.StatusOK {
			t.Fatalf("repeated identical Bearer + X-API-Key status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		var p domain.Principal
		if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
			t.Fatalf("decode principal: %v", err)
		}
		if p.CredentialID != pat1ID || !p.HasPermission(domain.PermissionLibraryRead) || p.HasPermission(domain.PermissionProgressWrite) {
			t.Fatalf("unexpected repeated PAT principal: %+v", p)
		}

		return nil
	}

	if code := runContext(t.Context(), nil); code != 0 {
		t.Fatalf("runContext exit code = %d, want 0", code)
	}
}
