package bootstrap_test

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/inexcession/tobiten/internal/bootstrap"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestMain(m *testing.M) { os.Exit(testdb.Run(m)) }

func bootstrapDB(t *testing.T) (*testdb.TestDB, *postgres.UserRepository) {
	t.Helper()
	db := testdb.New(t)
	if err := migrations.RunFS(t.Context(), db, migrations.CoreFS); err != nil {
		t.Fatal(err)
	}
	return db, postgres.NewUserRepository(db.Pool)
}

func TestProvisionAdminRollback(t *testing.T) {
	for _, target := range []string{"credentials", "user_roles"} {
		t.Run(target, func(t *testing.T) {
			db, repo := bootstrapDB(t)
			// Fail after the user insert, or after both user and password inserts.
			if _, err := db.Exec(t.Context(), "CREATE FUNCTION reject_bootstrap() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected bootstrap failure'; END $$; CREATE TRIGGER reject_bootstrap BEFORE INSERT ON "+target+" FOR EACH ROW EXECUTE FUNCTION reject_bootstrap()"); err != nil {
				t.Fatal(err)
			}
			if err := bootstrap.ProvisionAdmin(t.Context(), repo, "alice", "password"); err == nil {
				t.Fatal("expected rollback")
			}
			for _, table := range []string{"users", "credentials", "user_roles"} {
				var count int
				if err := db.QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("%s count=%d err=%v", table, count, err)
				}
			}
			if _, err := db.Exec(t.Context(), "DROP TRIGGER reject_bootstrap ON "+target); err != nil {
				t.Fatal(err)
			}
			if err := bootstrap.ProvisionAdmin(t.Context(), repo, "alice", "password"); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
		})
	}
}

func TestProvisionAdminConcurrent(t *testing.T) {
	_, repo := bootstrapDB(t)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Go(func() { errs <- bootstrap.ProvisionAdmin(t.Context(), repo, "alice", "password") })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	users, err := repo.ListUsers(t.Context())
	if err != nil || len(users) != 1 {
		t.Fatalf("users=%v err=%v", users, err)
	}
	creds, err := repo.ListCredentialsByUserID(t.Context(), users[0].ID)
	if err != nil || len(creds) != 1 {
		t.Fatalf("credentials=%d err=%v", len(creds), err)
	}
	roles, err := repo.ListUserRoles(t.Context(), users[0].ID)
	if err != nil || len(roles) != 1 {
		t.Fatalf("roles=%v err=%v", roles, err)
	}
}

func TestProvisionAdminMissingCredentials(t *testing.T) {
	_, repo := bootstrapDB(t)
	for _, values := range [][2]string{{"", ""}, {" ", "\t"}, {"alice", ""}, {"", "password"}} {
		err := bootstrap.ProvisionAdmin(t.Context(), repo, values[0], values[1])
		partial := values[0] == "alice" || values[1] == "password"
		if (err != nil) != partial {
			t.Fatalf("unexpected missing-credential result: %v", err)
		}
	}
	count, err := repo.CountUsers(context.Background())
	if err != nil || count != 0 {
		t.Fatalf("users=%d err=%v", count, err)
	}
}
