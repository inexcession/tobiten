package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/api"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

func readSetupRecord(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record["setup_token"] != nil {
			return record
		}
	}
	t.Fatal("no setup token logged")
	return nil
}

func setupHTTP(t *testing.T, handler http.Handler, path, body string, status int) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Host = "attacker.invalid"
	req.Header.Set("X-Forwarded-Host", "attacker.invalid")
	req.Header.Set("X-Forwarded-Proto", "http")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != status {
		t.Fatalf("%s status=%d want=%d body=%s", path, rec.Code, status, rec.Body.String())
	}
	return rec
}

func TestDaemonInteractiveSetupAndRestart(t *testing.T) {
	db := testdb.New(t)
	migrateStartupDB(t, db)
	putSetting(t, db, "server.base_url", "https://books.example.test/books%20shelf")
	t.Setenv("TOBITEN_STORAGE_MODE", "postgres")
	t.Setenv("TOBITEN_DATABASE_URL", db.URL)
	t.Setenv("TOBITEN_ADMIN_USER", "")
	t.Setenv("TOBITEN_ADMIN_PASS", "")
	t.Setenv("TOBITEN_BASE_URL", "")
	t.Setenv("TOBITEN_LOG_FORMAT", "json")
	t.Setenv("TOBITEN_LOG_LEVEL", "debug")
	originalServer := startServer
	t.Cleanup(func() { startServer = originalServer })
	var firstID string
	var retained app.SetupService
	calls := 0
	startServer = func(ctx context.Context, cfg *config.Config, logger *slog.Logger, services *app.Services) error {
		calls++
		if services == nil || services.Setup == nil || services.Admin != nil {
			t.Fatal("incorrect service composition")
		}
		retained = services.Setup
		listener := httptest.NewUnstartedServer(nil).Listener
		srv, err := api.NewServer(api.ServerOptions{Config: cfg, Logger: logger, Listener: listener, Registry: BuildRegistryWithServices(cfg, services)})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = srv.Close() }()
		if calls == 1 {
			record := readSetupRecord(t, os.Stdout.Name())
			if record["setup_api"] != "https://books.example.test/books%20shelf/api/v1/auth/setup" || record["setup_method"] != "POST" || record["level"] != "WARN" || record["service"] != "tobitend" || record["setup_url"] != nil {
				t.Fatalf("instructions=%v", record)
			}
			token := record["setup_token"].(string)
			entropy, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "tb_setup_"))
			if err != nil || len(entropy) != 32 {
				t.Fatalf("token entropy=%d err=%v", len(entropy), err)
			}
			setupHTTP(t, srv.Handler(), "/books%20shelf/api/v1/auth/setup", `{"setup_token":"wrong","username":"alice","password":"private-password"}`, 401)
			// Inject a real persistence error after user creation; the HTTP error must be stable.
			if _, err := db.Exec(ctx, "CREATE FUNCTION reject_http_setup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private DB detail'; END $$; CREATE TRIGGER reject_http_setup BEFORE INSERT ON credentials FOR EACH ROW EXECUTE FUNCTION reject_http_setup()"); err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(app.SetupAdminRequest{SetupToken: token, Username: "alice", Password: "private-password"})
			rec := setupHTTP(t, srv.Handler(), "/books%20shelf/api/v1/auth/setup", string(payload), 500)
			if strings.TrimSpace(rec.Body.String()) != `{"error":"setup failed"}` {
				t.Fatal(rec.Body.String())
			}
			if _, err := db.Exec(ctx, "DROP TRIGGER reject_http_setup ON credentials"); err != nil {
				t.Fatal(err)
			}
			rec = setupHTTP(t, srv.Handler(), "/books%20shelf/api/v1/auth/setup", string(payload), 201)
			var result struct {
				User domain.User `json:"user"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			firstID = result.User.ID
			if firstID == "" || result.User.Username != "alice" {
				t.Fatalf("created=%v", result)
			}
		}
		setupHTTP(t, srv.Handler(), "/books%20shelf/api/v1/auth/setup", `malformed`, 403)
		setupHTTP(t, srv.Handler(), "/api/v1/auth/setup", `{}`, 403)
		setupHTTP(t, srv.Handler(), "/api/v1/auth/login", `{"username":"alice","password":"anything"}`, 401)
		loginRec := setupHTTP(t, srv.Handler(), "/api/v1/auth/login", `{"username":"alice","password":"private-password"}`, 200)
		if len(loginRec.Result().Cookies()) == 0 || loginRec.Header().Get("X-CSRF-Token") == "" {
			t.Fatalf("expected session cookie and X-CSRF-Token on login, cookies=%v csrf=%q", loginRec.Result().Cookies(), loginRec.Header().Get("X-CSRF-Token"))
		}
		user, err := postgres.NewUserRepository(db.Pool).GetUserByID(ctx, firstID)
		if err != nil || user.Username != "alice" {
			t.Fatalf("persisted user=%v err=%v", user, err)
		}
		return nil
	}
	for i := range 2 {
		out, err := os.CreateTemp(t.TempDir(), "setup-log-")
		if err != nil {
			t.Fatal(err)
		}
		originalOut, originalLogger := os.Stdout, slog.Default()
		os.Stdout = out
		code := runContext(t.Context(), nil)
		os.Stdout = originalOut
		slog.SetDefault(originalLogger)
		if _, err := out.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(out)
		if err != nil {
			t.Fatal(err)
		}
		if err := out.Close(); err != nil {
			t.Fatal(err)
		}
		if code != 0 {
			t.Fatalf("start=%d logs=%s", code, data)
		}
		if bytes.Contains(data, []byte("private-password")) || bytes.Contains(data, []byte("private DB detail")) {
			t.Fatalf("secret in logs: %s", data)
		}
		if n := bytes.Count(data, []byte(`"setup_token"`)); n != 1-i {
			t.Fatalf("start %d token logs=%d", i, n)
		}
		if _, err := retained.SetupInitialAdmin(t.Context(), app.SetupAdminRequest{}); err != app.ErrSetupSealed {
			t.Fatalf("closed service err=%v", err)
		}
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}
