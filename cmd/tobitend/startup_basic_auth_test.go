package main

import (
	"context"
	"encoding/base64"
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

func basicAuthHeader(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

func TestDaemonBasicAuthRegistryAndPostgresLifecycle(t *testing.T) {
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
		if services == nil || services.BasicAuth == nil || services.PATAuth == nil {
			t.Fatal("expected daemon to wire BasicAuth and PATAuth services")
		}

		// Build production registry without ServerOptions.Authenticators test injection.
		reg := BuildRegistryWithServices(cfg, services)
		auths := reg.Authenticators()
		if len(auths) != 2 || auths[0].Name() != "basic" || auths[1].Name() != "pat" {
			t.Fatalf("expected [basic pat] authenticators in registry, got %v", auths)
		}

		// Register a protected probe module to observe Principal resolution over HTTP.
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

		doProbe := func(authHeader, remoteAddr string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/probe", nil)
			if remoteAddr != "" {
				req.RemoteAddr = remoteAddr
			}
			if authHeader != "" {
				req.Header.Set("Authorization", authHeader)
			}
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			return rec
		}

		// 1. Unauthenticated protected route returns 401 + WWW-Authenticate: Basic.
		rec := doProbe("", "198.51.100.1:1000")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated status = %d, want 401", rec.Code)
		}
		if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Basic ") {
			t.Fatalf("missing WWW-Authenticate Basic header: %q", rec.Header().Get("WWW-Authenticate"))
		}

		// 2. Valid Basic auth resolves Admin Principal from PostgreSQL and warms cache.
		for range 2 {
			rec = doProbe(basicAuthHeader("alice", "initial-secret-123"), "198.51.100.1:1001")
			if rec.Code != http.StatusOK {
				t.Fatalf("valid Basic auth status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
			}
			var p domain.Principal
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
				t.Fatalf("decode principal: %v", err)
			}
			if p.User.Username != "alice" || p.CredentialKind != domain.CredentialKindPassword || !p.HasPermission(domain.PermissionUsersAdmin) {
				t.Fatalf("unexpected principal: %+v", p)
			}
		}

		// 3. Invalid Basic credential on /healthz returns 401 (no allowlist fallback).
		healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		healthReq.RemoteAddr = "198.51.100.2:1000"
		healthReq.Header.Set("Authorization", basicAuthHeader("alice", "wrong-pass"))
		healthRec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(healthRec, healthReq)
		if healthRec.Code != http.StatusUnauthorized {
			t.Fatalf("invalid Basic on /healthz status = %d, want 401", healthRec.Code)
		}

		repo := postgres.NewUserRepository(db.Pool)
		alice, err := repo.GetUserByUsername(ctx, "alice")
		if err != nil {
			t.Fatalf("GetUserByUsername: %v", err)
		}

		// 4. Live role change in PostgreSQL reflects immediately even on cache hit.
		if err := repo.RemoveUserRole(ctx, alice.ID, domain.RoleAdminID); err != nil {
			t.Fatalf("RemoveUserRole: %v", err)
		}
		if err := repo.AssignUserRole(ctx, alice.ID, domain.RoleReaderID); err != nil {
			t.Fatalf("AssignUserRole: %v", err)
		}
		rec = doProbe(basicAuthHeader("alice", "initial-secret-123"), "198.51.100.1:1002")
		if rec.Code != http.StatusOK {
			t.Fatalf("role-updated auth status = %d, want 200", rec.Code)
		}
		var downgraded domain.Principal
		if err := json.Unmarshal(rec.Body.Bytes(), &downgraded); err != nil {
			t.Fatalf("decode principal: %v", err)
		}
		if downgraded.HasPermission(domain.PermissionUsersAdmin) {
			t.Fatalf("expected Admin permission removed after DB role change")
		}
		if !downgraded.HasPermission(domain.PermissionLibraryRead) {
			t.Fatalf("expected Reader permission retained after DB role change")
		}

		// 5. Password replacement in PostgreSQL invalidates warmed cache immediately.
		oldCred, err := repo.GetActivePasswordCredentialByUserID(ctx, alice.ID)
		if err != nil {
			t.Fatalf("GetActivePasswordCredentialByUserID: %v", err)
		}
		if err := repo.RevokeCredential(ctx, oldCred.ID, time.Now().UTC()); err != nil {
			t.Fatalf("RevokeCredential: %v", err)
		}
		newHash, err := auth.HashPassword("rotated-secret-456")
		if err != nil {
			t.Fatalf("HashPassword: %v", err)
		}
		scheme := auth.HashSchemeArgon2id
		newCredID := domain.MustNewID()
		if err := repo.CreateCredential(ctx, &domain.Credential{
			ID:         newCredID,
			UserID:     alice.ID,
			Kind:       domain.CredentialKindPassword,
			SecretHash: &newHash,
			HashScheme: &scheme,
		}); err != nil {
			t.Fatalf("CreateCredential: %v", err)
		}

		if rec = doProbe(basicAuthHeader("alice", "initial-secret-123"), "198.51.100.1:1003"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("old password must fail immediately after replacement, got %d", rec.Code)
		}
		if rec = doProbe(basicAuthHeader("alice", "rotated-secret-456"), "198.51.100.1:1004"); rec.Code != http.StatusOK {
			t.Fatalf("new password must succeed after replacement, got %d", rec.Code)
		}

		// 6. Expiry in PostgreSQL rejects immediately.
		if _, err := db.Exec(ctx, "UPDATE credentials SET expires_at = now() - interval '1 minute' WHERE id = $1", newCredID); err != nil {
			t.Fatalf("expire credential: %v", err)
		}
		if rec = doProbe(basicAuthHeader("alice", "rotated-secret-456"), "198.51.100.1:1005"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("expired credential must return 401 immediately, got %d", rec.Code)
		}

		// 7. Revocation in PostgreSQL rejects immediately.
		if _, err := db.Exec(ctx, "UPDATE credentials SET expires_at = NULL WHERE id = $1", newCredID); err != nil {
			t.Fatalf("unexpire credential: %v", err)
		}
		if rec = doProbe(basicAuthHeader("alice", "rotated-secret-456"), "198.51.100.1:1006"); rec.Code != http.StatusOK {
			t.Fatalf("valid credential should warm cache before revocation, got %d", rec.Code)
		}
		if err := repo.RevokeCredential(ctx, newCredID, time.Now().UTC()); err != nil {
			t.Fatalf("RevokeCredential(new): %v", err)
		}
		if rec = doProbe(basicAuthHeader("alice", "rotated-secret-456"), "198.51.100.1:1007"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("revoked credential must return 401 immediately, got %d", rec.Code)
		}

		// 8. Repeated failures trigger throttling (fast 401 without KDF) while leaving account unlocked in PostgreSQL.
		bobID := domain.MustNewID()
		bobHash, err := auth.HashPassword("bob-secret-789")
		if err != nil {
			t.Fatalf("HashPassword(bob): %v", err)
		}
		if err := repo.CreateUser(ctx, &domain.User{ID: bobID, Username: "bob"}); err != nil {
			t.Fatalf("CreateUser(bob): %v", err)
		}
		if err := repo.CreateCredential(ctx, &domain.Credential{
			ID:         domain.MustNewID(),
			UserID:     bobID,
			Kind:       domain.CredentialKindPassword,
			SecretHash: &bobHash,
			HashScheme: &scheme,
		}); err != nil {
			t.Fatalf("CreateCredential(bob): %v", err)
		}
		if err := repo.AssignUserRole(ctx, bobID, domain.RoleReaderID); err != nil {
			t.Fatalf("AssignUserRole(bob): %v", err)
		}
		for i := range auth.DefaultUserMaxFailures {
			rec = doProbe(basicAuthHeader("bob", "wrong-pass"), "198.51.100.50:2000")
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("failure %d status = %d, want 401", i+1, rec.Code)
			}
		}
		throttledStart := time.Now()
		rec = doProbe(basicAuthHeader("bob", "bob-secret-789"), "198.51.100.51:2001")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("throttled bob status = %d, want 401", rec.Code)
		}
		if elapsed := time.Since(throttledStart); elapsed > 50*time.Millisecond {
			t.Fatalf("throttled request took %v; expected fast rejection without Argon2id", elapsed)
		}

		return nil
	}

	if code := runContext(t.Context(), nil); code != 0 {
		t.Fatalf("runContext exit code = %d, want 0", code)
	}
}
