package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func registerSessionProbeModule(t *testing.T, reg module.Registry, called *bool) {
	t.Helper()
	probeMod := module.NewSimpleModule("probe")
	probeMod.Mounts = []module.RouteMount{
		{
			ModuleName: "probe",
			Prefix:     "/api/v1/probe",
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if called != nil {
					*called = true
				}
				p := domain.MustPrincipalFromContext(r.Context())
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(p)
			}),
		},
	}
	if err := reg.Register(probeMod); err != nil {
		t.Fatalf("register probe module: %v", err)
	}
}

func performLogin(t *testing.T, h http.Handler, path, body, remoteAddr string, headers map[string]string) (*httptest.ResponseRecorder, *http.Cookie, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	req.Host = "spoofed-host.invalid"
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var sessCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == app.NativeSessionCookieName {
			sessCookie = c
		}
	}
	return rec, sessCookie, rec.Header().Get("X-CSRF-Token")
}

func TestDaemonSessionLoginAttributesAndSubpath(t *testing.T) {
	cases := []struct {
		name       string
		baseURL    string
		loginPath  string
		probePath  string
		origin     string
		referer    string
		wantPath   string
		wantSecure bool
		fwdProto   string
	}{
		{
			name:       "direct-http",
			baseURL:    "http://localhost:8080",
			loginPath:  "/api/v1/auth/login",
			probePath:  "/api/v1/probe",
			origin:     "http://localhost:8080",
			referer:    "http://localhost:8080/login",
			wantPath:   "/",
			wantSecure: false,
			fwdProto:   "https",
		},
		{
			name:       "https-subpath",
			baseURL:    "https://comics.example.com:8443/comics%20vault",
			loginPath:  "/comics%20vault/api/v1/auth/login",
			probePath:  "/comics%20vault/api/v1/probe",
			origin:     "https://comics.example.com:8443",
			referer:    "https://comics.example.com:8443/comics%20vault/login",
			wantPath:   "/comics%20vault",
			wantSecure: true,
			fwdProto:   "http",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := testdb.New(t)
			t.Setenv("TOBITEN_STORAGE_MODE", "postgres")
			t.Setenv("TOBITEN_DATABASE_URL", db.URL)
			t.Setenv("TOBITEN_BASE_URL", tc.baseURL)
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
				var mutCalled bool
				registerSessionProbeModule(t, reg, &mutCalled)

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

				// Foreign / null / cross-site browser origins must 403 on login before reaching login logic.
				for _, hdrs := range []map[string]string{
					{"Origin": "null"},
					{"Origin": "https://attacker.invalid"},
					{"Referer": "https://attacker.invalid/form"},
					{"Sec-Fetch-Site": "cross-site"},
				} {
					rec, c, _ := performLogin(t, srv.Handler(), tc.loginPath, `{"username":"alice","password":"initial-secret-123"}`, "198.51.100.10:1001", hdrs)
					if rec.Code != http.StatusForbidden || c != nil {
						t.Fatalf("bad origin %v: code=%d cookie=%v", hdrs, rec.Code, c)
					}
				}

				// Same-origin login succeeds and issues cookie with resolved base URL attributes (ignoring X-Forwarded-Proto).
				rec, cookie, csrf := performLogin(t, srv.Handler(), tc.loginPath, `{"username":"alice","password":"initial-secret-123"}`, "198.51.100.10:1002", map[string]string{
					"Origin":            tc.origin,
					"Referer":           tc.referer,
					"Sec-Fetch-Site":    "same-origin",
					"X-Forwarded-Proto": tc.fwdProto,
					"X-Forwarded-Host":  "attacker.invalid",
				})
				if rec.Code != http.StatusOK {
					t.Fatalf("same-origin login status=%d body=%s", rec.Code, rec.Body.String())
				}
				if cookie == nil || !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode || cookie.Secure != tc.wantSecure || cookie.Path != tc.wantPath {
					t.Fatalf("unexpected cookie attributes: %+v (want secure=%v path=%q)", cookie, tc.wantSecure, tc.wantPath)
				}
				if csrf == "" {
					t.Fatal("expected non-empty X-CSRF-Token response header")
				}
				var body struct {
					CSRFToken string `json:"csrf_token"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.CSRFToken != csrf {
					t.Fatalf("unexpected csrf body=%+v header=%q err=%v", body, csrf, err)
				}

				// Non-browser login omitting browser-origin headers also succeeds without prior CSRF token.
				rec2, cookie2, _ := performLogin(t, srv.Handler(), tc.loginPath, `{"username":"alice","password":"initial-secret-123"}`, "198.51.100.10:1003", map[string]string{
					"X-Forwarded-Proto": tc.fwdProto,
				})
				if rec2.Code != http.StatusOK || cookie2 == nil || cookie2.Secure != tc.wantSecure || cookie2.Path != tc.wantPath {
					t.Fatalf("non-browser login status=%d cookie=%+v", rec2.Code, cookie2)
				}

				// Protected mutation works with matching cookie + CSRF + same-origin headers.
				mutReq := httptest.NewRequest(http.MethodPost, tc.probePath, nil)
				mutReq.RemoteAddr = "198.51.100.10:1004"
				mutReq.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: cookie.Value})
				mutReq.Header.Set("X-CSRF-Token", csrf)
				mutReq.Header.Set("Origin", tc.origin)
				mutReq.Header.Set("Sec-Fetch-Site", "same-origin")
				mutRec := httptest.NewRecorder()
				mutCalled = false
				srv.Handler().ServeHTTP(mutRec, mutReq)
				if mutRec.Code != http.StatusOK || !mutCalled {
					t.Fatalf("authenticated mutation status=%d called=%v", mutRec.Code, mutCalled)
				}
				return nil
			}

			if code := runContext(t.Context(), nil); code != 0 {
				t.Fatalf("runContext exit code = %d", code)
			}
		})
	}
}

func TestDaemonSessionCSRFAndKomgaCookieIsolation(t *testing.T) {
	db := testdb.New(t)
	t.Setenv("TOBITEN_STORAGE_MODE", "postgres")
	t.Setenv("TOBITEN_DATABASE_URL", db.URL)
	t.Setenv("TOBITEN_BASE_URL", "https://comics.example.com")
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
		var handlerCalled bool
		registerSessionProbeModule(t, reg, &handlerCalled)

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

		rec, cookie, csrf := performLogin(t, srv.Handler(), "/api/v1/auth/login", `{"username":"alice","password":"initial-secret-123"}`, "198.51.100.30:1001", nil)
		if rec.Code != http.StatusOK || cookie == nil || csrf == "" {
			t.Fatalf("login failed: code=%d cookie=%v csrf=%q", rec.Code, cookie, csrf)
		}

		// Komga cookies are rejected outside /komga/*.
		for _, komgaName := range []string{"KOMGA-SESSION", "komga-remember-me"} {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/probe", nil)
			r.AddCookie(&http.Cookie{Name: komgaName, Value: cookie.Value})
			w := httptest.NewRecorder()
			handlerCalled = false
			srv.Handler().ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized || handlerCalled {
				t.Fatalf("Komga cookie %q status=%d called=%v", komgaName, w.Code, handlerCalled)
			}
		}

		// Mutation missing X-CSRF-Token -> 403 before handler executes.
		for _, tok := range []string{"", "wrong-csrf"} {
			r := httptest.NewRequest(http.MethodPost, "/api/v1/probe", nil)
			r.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: cookie.Value})
			if tok != "" {
				r.Header.Set("X-CSRF-Token", tok)
			}
			w := httptest.NewRecorder()
			handlerCalled = false
			srv.Handler().ServeHTTP(w, r)
			if w.Code != http.StatusForbidden || handlerCalled {
				t.Fatalf("CSRF=%q status=%d called=%v", tok, w.Code, handlerCalled)
			}
		}

		// Invalid explicit Basic must return 401 without falling back to ambient session cookie.
		r := httptest.NewRequest(http.MethodGet, "/api/v1/probe", nil)
		r.SetBasicAuth("alice", "wrong-password")
		r.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: cookie.Value})
		w := httptest.NewRecorder()
		handlerCalled = false
		srv.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized || handlerCalled {
			t.Fatalf("invalid explicit + valid cookie status=%d called=%v", w.Code, handlerCalled)
		}

		// Valid explicit Basic + ambient cookie succeeds on mutation without X-CSRF-Token.
		r = httptest.NewRequest(http.MethodPost, "/api/v1/probe", nil)
		r.RemoteAddr = "198.51.100.30:1002"
		r.SetBasicAuth("alice", "initial-secret-123")
		r.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: cookie.Value})
		w = httptest.NewRecorder()
		handlerCalled = false
		srv.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusOK || !handlerCalled {
			t.Fatalf("valid explicit + cookie mutation status=%d called=%v", w.Code, handlerCalled)
		}
		return nil
	}

	if code := runContext(t.Context(), nil); code != 0 {
		t.Fatalf("runContext exit code = %d", code)
	}
}

func TestDaemonSessionCredentialLifecycleRoleSyncAndLoginThrottle(t *testing.T) {
	db := testdb.New(t)
	t.Setenv("TOBITEN_STORAGE_MODE", "postgres")
	t.Setenv("TOBITEN_DATABASE_URL", db.URL)
	t.Setenv("TOBITEN_BASE_URL", "http://localhost:8080")
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
		registerSessionProbeModule(t, reg, nil)

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

		rec, cookie, _ := performLogin(t, srv.Handler(), "/api/v1/auth/login", `{"username":"alice","password":"initial-secret-123"}`, "198.51.100.40:1001", nil)
		if rec.Code != http.StatusOK || cookie == nil {
			t.Fatalf("login failed: code=%d", rec.Code)
		}

		getPrincipal := func(wantStatus int) *domain.Principal {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/probe", nil)
			r.AddCookie(&http.Cookie{Name: app.NativeSessionCookieName, Value: cookie.Value})
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, r)
			if w.Code != wantStatus {
				t.Fatalf("probe status=%d want=%d body=%s", w.Code, wantStatus, w.Body.String())
			}
			if wantStatus != http.StatusOK {
				return nil
			}
			var p domain.Principal
			if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
				t.Fatalf("unmarshal principal: %v", err)
			}
			return &p
		}

		// Live role change in Postgres takes effect immediately on the active cookie session.
		p := getPrincipal(http.StatusOK)
		if !p.HasPermission(domain.PermissionUsersAdmin) {
			t.Fatalf("expected PermissionUsersAdmin initially, caps=%v", p.Permissions)
		}
		userRepo := postgres.NewUserRepository(db.Pool)
		adminRole, err := userRepo.GetRoleByName(ctx, "Admin")
		if err != nil {
			t.Fatalf("GetRoleByName Admin: %v", err)
		}
		readerRole, err := userRepo.GetRoleByName(ctx, "Reader")
		if err != nil {
			t.Fatalf("GetRoleByName Reader: %v", err)
		}
		if err := userRepo.RemoveUserRole(ctx, p.User.ID, adminRole.ID); err != nil {
			t.Fatalf("RemoveUserRole: %v", err)
		}
		if err := userRepo.AssignUserRole(ctx, p.User.ID, readerRole.ID); err != nil {
			t.Fatalf("AssignUserRole: %v", err)
		}
		p2 := getPrincipal(http.StatusOK)
		if p2.HasPermission(domain.PermissionUsersAdmin) || !p2.HasPermission(domain.PermissionLibraryRead) {
			t.Fatalf("expected live demotion to READER only, caps=%v", p2.Permissions)
		}

		// Deleting an active originating credential cascades in DB and immediately ends the HTTP session.
		if _, err := db.Exec(ctx, `DELETE FROM credentials WHERE id = $1`, p2.CredentialID); err != nil {
			t.Fatalf("delete credential: %v", err)
		}
		var sessCount int
		if err := db.QueryRow(ctx, `SELECT count(*) FROM sessions WHERE credential_id = $1`, p2.CredentialID).Scan(&sessCount); err != nil {
			t.Fatalf("count sessions: %v", err)
		}
		if sessCount != 0 {
			t.Fatalf("expected sessions row cascade-deleted, got count=%d", sessCount)
		}
		getPrincipal(http.StatusUnauthorized)

		// Provision a fresh password credential to verify RevokeCredential and in-place hash rotation.
		secondHash, err := auth.HashPassword("second-secret-123")
		if err != nil {
			t.Fatalf("HashPassword: %v", err)
		}
		scheme := auth.HashSchemeArgon2id
		if err := userRepo.CreateCredential(ctx, &domain.Credential{
			ID:         domain.MustNewID(),
			UserID:     p2.User.ID,
			Kind:       domain.CredentialKindPassword,
			SecretHash: &secondHash,
			HashScheme: &scheme,
		}); err != nil {
			t.Fatalf("CreateCredential: %v", err)
		}
		rec2, cookie2, _ := performLogin(t, srv.Handler(), "/api/v1/auth/login", `{"username":"alice","password":"second-secret-123"}`, "198.51.100.40:1002", nil)
		if rec2.Code != http.StatusOK || cookie2 == nil {
			t.Fatalf("second login failed: code=%d", rec2.Code)
		}
		cookie = cookie2
		p3 := getPrincipal(http.StatusOK)

		if _, err := db.Exec(ctx, `UPDATE credentials SET secret_hash = secret_hash || '-rotated' WHERE id = $1`, p3.CredentialID); err != nil {
			t.Fatalf("rotate secret_hash: %v", err)
		}
		getPrincipal(http.StatusUnauthorized)

		if _, err := db.Exec(ctx, `UPDATE credentials SET secret_hash = $2 WHERE id = $1`, p3.CredentialID, secondHash); err != nil {
			t.Fatalf("restore secret_hash: %v", err)
		}
		getPrincipal(http.StatusOK)

		if err := userRepo.RevokeCredential(ctx, p3.CredentialID, time.Now().UTC()); err != nil {
			t.Fatalf("RevokeCredential: %v", err)
		}
		getPrincipal(http.StatusUnauthorized)

		// Peer-IP login throttling (DefaultIPMaxFailures = 10) holds across changing source ports and spoofed X-Forwarded-For headers.
		for i := range 10 {
			r, _, _ := performLogin(t, srv.Handler(), "/api/v1/auth/login", `{"username":"probe-user-`+strconv.Itoa(i)+`","password":"wrong"}`, "198.51.100.77:"+strconv.Itoa(1000+i), map[string]string{
				"X-Forwarded-For": "203.0.113." + strconv.Itoa(10+i),
			})
			if r.Code != http.StatusUnauthorized {
				t.Fatalf("failure %d: status=%d want=401", i, r.Code)
			}
		}
		throttledRec, _, _ := performLogin(t, srv.Handler(), "/api/v1/auth/login", `{"username":"alice","password":"anything"}`, "198.51.100.77:9999", map[string]string{
			"X-Forwarded-For": "203.0.113.250",
		})
		if throttledRec.Code != http.StatusTooManyRequests {
			t.Fatalf("expected 429 throttle across port changes & X-Forwarded-For rotation, got %d body=%s", throttledRec.Code, throttledRec.Body.String())
		}
		return nil
	}

	if code := runContext(t.Context(), nil); code != 0 {
		t.Fatalf("runContext exit code = %d", code)
	}
}
