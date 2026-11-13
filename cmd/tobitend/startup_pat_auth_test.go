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

func TestDaemonPATAuthRegistryAndPostgresLifecycle(t *testing.T) {
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
		if services == nil || services.PATAuth == nil {
			t.Fatal("expected daemon to wire PATAuth service")
		}

		// Build production registry without ServerOptions.Authenticators test injection.
		reg := BuildRegistryWithServices(cfg, services)
		var authNames []string
		for _, a := range reg.Authenticators() {
			authNames = append(authNames, a.Name())
		}
		if len(authNames) != 3 || authNames[0] != "basic" || authNames[1] != "pat" || authNames[2] != "native-session" {
			t.Fatalf("expected [basic pat native-session] authenticators in production registry, got %v", authNames)
		}

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

		doBearer := func(token, remoteAddr string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/probe", nil)
			req.RemoteAddr = remoteAddr
			req.Header.Set("Authorization", "Bearer "+token)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			return rec
		}

		doAPIKey := func(token, remoteAddr string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/probe", nil)
			req.RemoteAddr = remoteAddr
			req.Header.Set("X-API-Key", token)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			return rec
		}

		repo := postgres.NewUserRepository(db.Pool)
		alice, err := repo.GetUserByUsername(ctx, "alice")
		if err != nil {
			t.Fatalf("GetUserByUsername: %v", err)
		}

		// Create a valid PAT with progress:write scope (capped by Alice's Admin role).
		patID := domain.MustNewID()
		patToken, patHash, err := auth.NewPAT(patID)
		if err != nil {
			t.Fatalf("NewPAT: %v", err)
		}
		scheme := auth.HashSchemeArgon2id
		futureExp := time.Now().UTC().Add(1 * time.Hour)
		if err := repo.CreateCredential(ctx, &domain.Credential{
			ID:         patID,
			UserID:     alice.ID,
			Kind:       domain.CredentialKindPAT,
			Scopes:     []domain.PATScope{domain.ScopeProgressWrite},
			SecretHash: &patHash,
			HashScheme: &scheme,
			ExpiresAt:  &futureExp,
		}); err != nil {
			t.Fatalf("CreateCredential(valid PAT): %v", err)
		}

		// 1. Valid Bearer PAT resolves Principal with scoped permissions and advances last_used_at from NULL.
		rec := doBearer(patToken, "198.51.100.10:1001")
		if rec.Code != http.StatusOK {
			t.Fatalf("valid Bearer PAT status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		var p1 domain.Principal
		if err := json.Unmarshal(rec.Body.Bytes(), &p1); err != nil {
			t.Fatalf("decode principal: %v", err)
		}
		if p1.User.ID != alice.ID || p1.CredentialID != patID || p1.CredentialKind != domain.CredentialKindPAT {
			t.Fatalf("unexpected PAT principal: %+v", p1)
		}
		if !p1.HasPermission(domain.PermissionProgressWrite) || p1.HasPermission(domain.PermissionUsersAdmin) {
			t.Fatalf("expected progress:write-capped permissions, got %v", p1.Permissions)
		}

		credAfterFirst, err := repo.GetCredentialByID(ctx, patID)
		if err != nil {
			t.Fatalf("GetCredentialByID: %v", err)
		}
		if credAfterFirst.LastUsedAt == nil {
			t.Fatalf("expected last_used_at to be set after first PAT use")
		}

		// 2. Valid X-API-Key PAT (even on cache hit) advances last_used_at strictly forward.
		time.Sleep(5 * time.Millisecond)
		rec = doAPIKey(patToken, "198.51.100.10:1002")
		if rec.Code != http.StatusOK {
			t.Fatalf("valid X-API-Key PAT status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
		}
		credAfterSecond, err := repo.GetCredentialByID(ctx, patID)
		if err != nil {
			t.Fatalf("GetCredentialByID(second): %v", err)
		}
		if credAfterSecond.LastUsedAt == nil || !credAfterSecond.LastUsedAt.After(*credAfterFirst.LastUsedAt) {
			t.Fatalf("expected last_used_at to advance from %v, got %v", credAfterFirst.LastUsedAt, credAfterSecond.LastUsedAt)
		}

		// 3. Invalid PAT on /healthz returns 401 (no allowlist fallback).
		healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		healthReq.RemoteAddr = "198.51.100.11:1000"
		healthReq.Header.Set("Authorization", "Bearer tobiten_pat_"+patID+"_wrong-secret")
		healthRec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(healthRec, healthReq)
		if healthRec.Code != http.StatusUnauthorized {
			t.Fatalf("invalid PAT on /healthz status = %d, want 401", healthRec.Code)
		}

		// 4. Expired PAT returns 401 and does not advance last_used_at.
		expiredID := domain.MustNewID()
		expiredToken, expiredHash, err := auth.NewPAT(expiredID)
		if err != nil {
			t.Fatalf("NewPAT(expired): %v", err)
		}
		pastExp := time.Now().UTC().Add(-1 * time.Minute)
		if err := repo.CreateCredential(ctx, &domain.Credential{
			ID:         expiredID,
			UserID:     alice.ID,
			Kind:       domain.CredentialKindPAT,
			Scopes:     []domain.PATScope{domain.ScopeRead},
			SecretHash: &expiredHash,
			HashScheme: &scheme,
			ExpiresAt:  &pastExp,
		}); err != nil {
			t.Fatalf("CreateCredential(expired): %v", err)
		}
		rec = doBearer(expiredToken, "198.51.100.12:1000")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expired PAT status = %d, want 401", rec.Code)
		}
		if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic ") {
			t.Fatalf("missing WWW-Authenticate Basic header on 401: %q", rec.Header().Get("WWW-Authenticate"))
		}
		expiredCred, err := repo.GetCredentialByID(ctx, expiredID)
		if err != nil {
			t.Fatalf("GetCredentialByID(expired): %v", err)
		}
		if expiredCred.LastUsedAt != nil {
			t.Fatalf("expired PAT must not update last_used_at, got %v", expiredCred.LastUsedAt)
		}

		// 5. Revoking the valid warmed-cache PAT returns 401 immediately without updating last_used_at.
		if err := repo.RevokeCredential(ctx, patID, time.Now().UTC()); err != nil {
			t.Fatalf("RevokeCredential: %v", err)
		}
		rec = doBearer(patToken, "198.51.100.10:1003")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("revoked PAT status = %d, want 401", rec.Code)
		}
		rec = doAPIKey(patToken, "198.51.100.10:1004")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("revoked PAT via X-API-Key status = %d, want 401", rec.Code)
		}
		credAfterRevoke, err := repo.GetCredentialByID(ctx, patID)
		if err != nil {
			t.Fatalf("GetCredentialByID(revoked): %v", err)
		}
		if credAfterRevoke.LastUsedAt == nil || !credAfterRevoke.LastUsedAt.Equal(*credAfterSecond.LastUsedAt) {
			t.Fatalf("revoked PAT must not advance last_used_at: before=%v after=%v", credAfterSecond.LastUsedAt, credAfterRevoke.LastUsedAt)
		}

		return nil
	}

	if code := runContext(t.Context(), nil); code != 0 {
		t.Fatalf("runContext exit code = %d, want 0", code)
	}
}
