package bootstrap

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/auth"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupDB(t *testing.T) (*testdb.TestDB, *postgres.UserRepository) {
	t.Helper()
	db := testdb.New(t)
	if err := migrations.RunFS(t.Context(), db, migrations.CoreFS); err != nil {
		t.Fatal(err)
	}
	return db, postgres.NewUserRepository(db.Pool)
}

func newTestSetup(t *testing.T, repo Repository) (*Setup, string) {
	t.Helper()
	var out bytes.Buffer
	s, err := NewSetup(t.Context(), repo, slog.New(slog.NewJSONHandler(&out, nil)).With("service", "tobitend"), "https://books.example/sub/api/v1/auth/setup")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	var record struct {
		Token   string `json:"setup_token"`
		API     string `json:"setup_api"`
		Method  string `json:"setup_method"`
		Service string `json:"service"`
		Level   string `json:"level"`
	}
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.API != "https://books.example/sub/api/v1/auth/setup" || record.Method != "POST" || record.Service != "tobitend" || record.Level != "WARN" || strings.Contains(out.String(), "setup_url") {
		t.Fatalf("bad instructions: %s", out.String())
	}
	if !strings.HasPrefix(record.Token, "tb_setup_") {
		t.Fatal("missing token prefix")
	}
	entropy, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(record.Token, "tb_setup_"))
	if err != nil || len(entropy) != 32 {
		t.Fatalf("token entropy bytes=%d err=%v", len(entropy), err)
	}
	return s, record.Token
}

func assertErased(t *testing.T, s *Setup, backing []byte) {
	t.Helper()
	if !s.sealed || s.token != nil || !bytes.Equal(backing, make([]byte, len(backing))) {
		t.Fatal("token was not erased and sealed")
	}
}

func TestSetupTokenLifecycle(t *testing.T) {
	_, repo := setupDB(t)
	s, token := newTestSetup(t, repo)
	backing := s.token
	for _, bad := range []string{"", "wrong", token[:len(token)-1], token + "x"} {
		if _, err := s.SetupInitialAdmin(t.Context(), app.SetupAdminRequest{SetupToken: bad, Username: "alice", Password: "password"}); !errors.Is(err, app.ErrUnauthorized) {
			t.Fatalf("invalid token: %v", err)
		}
	}
	if _, err := s.SetupInitialAdmin(t.Context(), app.SetupAdminRequest{SetupToken: token, Username: " \t", Password: "password"}); !errors.Is(err, app.ErrInvalidInput) {
		t.Fatal(err)
	}
	if count, err := repo.CountUsers(t.Context()); err != nil || count != 0 {
		t.Fatalf("invalid requests wrote users: %d %v", count, err)
	}
	user, err := s.SetupInitialAdmin(t.Context(), app.SetupAdminRequest{SetupToken: token, Username: " alice ", Password: "password"})
	if err != nil || user.Username != "alice" {
		t.Fatalf("user=%v err=%v", user, err)
	}
	assertErased(t, s, backing)
	creds, err := repo.ListCredentialsByUserID(t.Context(), user.ID)
	if err != nil || len(creds) != 1 || creds[0].Kind != domain.CredentialKindPassword || creds[0].SecretHash == nil {
		t.Fatalf("credentials=%v err=%v", creds, err)
	}
	if ok, err := auth.VerifyPassword("password", *creds[0].SecretHash); err != nil || !ok {
		t.Fatalf("password verify=%v err=%v", ok, err)
	}
	roles, err := repo.ListUserRoles(t.Context(), user.ID)
	if err != nil || len(roles) != 1 || roles[0].ID != domain.RoleAdminID {
		t.Fatalf("roles=%v err=%v", roles, err)
	}
	if _, err := s.SetupInitialAdmin(t.Context(), app.SetupAdminRequest{SetupToken: token}); !errors.Is(err, app.ErrSetupSealed) {
		t.Fatal(err)
	}
	var out bytes.Buffer
	restarted, err := NewSetup(t.Context(), repo, slog.New(slog.NewJSONHandler(&out, nil)), "unused")
	if err != nil || out.Len() != 0 {
		t.Fatalf("restart err=%v logs=%s", err, out.String())
	}
	assertErased(t, restarted, nil)
	// No stored setting or later account deletion reopens this running instance.
	if err := repo.DeleteUser(t.Context(), user.ID); err != nil {
		t.Fatal(err)
	}
	if sealed, err := s.IsSetupSealed(t.Context()); err != nil || !sealed {
		t.Fatalf("seal=%v err=%v", sealed, err)
	}
}

func TestSetupRollbackRetainsToken(t *testing.T) {
	for _, target := range []string{"credentials", "user_roles"} {
		t.Run(target, func(t *testing.T) {
			db, repo := setupDB(t)
			s, token := newTestSetup(t, repo)
			if _, err := db.Exec(t.Context(), "CREATE FUNCTION reject_setup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private DB detail'; END $$; CREATE TRIGGER reject_setup BEFORE INSERT ON "+target+" FOR EACH ROW EXECUTE FUNCTION reject_setup()"); err != nil {
				t.Fatal(err)
			}
			req := app.SetupAdminRequest{SetupToken: token, Username: "alice", Password: "password"}
			if _, err := s.SetupInitialAdmin(t.Context(), req); err == nil {
				t.Fatal("expected rollback")
			}
			for _, table := range []string{"users", "credentials", "user_roles"} {
				var count int
				if err := db.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("%s count=%d err=%v", table, count, err)
				}
			}
			if string(s.token) != token || s.sealed {
				t.Fatal("rollback consumed token")
			}
			if _, err := db.Exec(t.Context(), "DROP TRIGGER reject_setup ON "+target); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SetupInitialAdmin(t.Context(), req); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSetupConcurrentServicesAndEnvironment(t *testing.T) {
	_, repo := setupDB(t)
	a, tokenA := newTestSetup(t, repo)
	b, tokenB := newTestSetup(t, repo)
	if tokenA == tokenB {
		t.Fatal("tokens reused")
	}
	oldA, oldB := a.token, b.token
	var wg sync.WaitGroup
	errs := make(chan error, 9)
	for i := range 8 {
		s, token := a, tokenA
		if i%2 != 0 {
			s, token = b, tokenB
		}
		wg.Go(func() {
			_, err := s.SetupInitialAdmin(t.Context(), app.SetupAdminRequest{SetupToken: token, Username: "alice", Password: "password"})
			errs <- err
		})
	}
	wg.Go(func() { errs <- ProvisionAdmin(t.Context(), repo, "environment", "password") })
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, app.ErrSetupSealed) {
			t.Fatal(err)
		}
	}
	if count, err := repo.CountUsers(t.Context()); err != nil || count != 1 {
		t.Fatalf("users=%d err=%v", count, err)
	}
	assertErased(t, a, oldA)
	assertErased(t, b, oldB)
}

func TestSetupExternalSealAndShutdown(t *testing.T) {
	_, repo := setupDB(t)
	s, _ := newTestSetup(t, repo)
	backing := s.token
	if err := ProvisionAdmin(t.Context(), repo, "environment", "password"); err != nil {
		t.Fatal(err)
	}
	if sealed, err := s.IsSetupSealed(t.Context()); err != nil || !sealed {
		t.Fatalf("seal=%v err=%v", sealed, err)
	}
	assertErased(t, s, backing)
	_, otherRepo := setupDB(t)
	other, _ := newTestSetup(t, otherRepo)
	backing = other.token
	other.Close()
	assertErased(t, other, backing)
}

type failedSetupRepo struct{}

func (failedSetupRepo) WithBootstrapTx(context.Context, func(storage.UserRepository) error) error {
	return errors.New("private database error")
}

func TestSetupFailedCheckNeverLogsToken(t *testing.T) {
	var out bytes.Buffer
	if s, err := NewSetup(t.Context(), failedSetupRepo{}, slog.New(slog.NewJSONHandler(&out, nil)), "unused"); err == nil || s != nil || out.Len() != 0 {
		t.Fatalf("setup=%v err=%v logs=%s", s, err, out.String())
	}
}
