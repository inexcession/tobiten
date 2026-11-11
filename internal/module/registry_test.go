package module

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"testing"
	"testing/fstest"

	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestRegistry_RegistrationAndLookup(t *testing.T) {
	reg := NewRegistry()

	modA := NewSimpleModule("komga")
	if err := reg.Register(modA); err != nil {
		t.Fatalf("Register(komga) error: %v", err)
	}

	// Lookup case-insensitive
	if got := reg.Module("komga"); got == nil || got.Name() != "komga" {
		t.Errorf("Module(komga) = %v, want modA", got)
	}
	if got := reg.Module("KOMGA"); got == nil || got.Name() != "komga" {
		t.Errorf("Module(KOMGA) = %v, want modA", got)
	}

	// Duplicate registration rejected
	modADup := NewSimpleModule("komga")
	if err := reg.Register(modADup); err == nil {
		t.Errorf("Register duplicate module expected error, got nil")
	}

	// Case-insensitive duplicate rejected
	modACaps := NewSimpleModule("KOMGA")
	if err := reg.Register(modACaps); err == nil {
		t.Errorf("Register case-insensitive duplicate expected error, got nil")
	}

	// Nil and empty name rejected
	if err := reg.Register(nil); err == nil {
		t.Errorf("Register(nil) expected error, got nil")
	}
	if err := reg.Register(NewSimpleModule("")); err == nil {
		t.Errorf("Register(empty name) expected error, got nil")
	}

	// Order preserved
	modB := NewSimpleModule("opds")
	if err := reg.Register(modB); err != nil {
		t.Fatalf("Register(opds) error: %v", err)
	}

	all := reg.Modules()
	if len(all) != 2 || all[0].Name() != "komga" || all[1].Name() != "opds" {
		t.Errorf("Modules() = %v, want [komga, opds]", all)
	}
}

func TestRegistry_EnablementAndConfig(t *testing.T) {
	reg := NewRegistry()

	modA := NewSimpleModule("komga")
	modB := NewSimpleModule("opds")
	_ = reg.Register(modA)
	_ = reg.Register(modB)

	// Enabled by default
	if !reg.IsEnabled("komga") || !reg.IsEnabled("opds") {
		t.Errorf("expected both modules enabled by default")
	}

	// SetEnabled dynamically
	reg.SetEnabled("komga", false)
	if reg.IsEnabled("komga") {
		t.Errorf("expected komga disabled after SetEnabled(false)")
	}
	if !reg.IsEnabled("opds") {
		t.Errorf("expected opds still enabled")
	}

	enabled := reg.EnabledModules()
	if len(enabled) != 1 || enabled[0].Name() != "opds" {
		t.Errorf("EnabledModules() = %v, want [opds]", enabled)
	}

	// Re-enable
	reg.SetEnabled("komga", true)
	if !reg.IsEnabled("komga") {
		t.Errorf("expected komga re-enabled after SetEnabled(true)")
	}

	// ApplyConfig
	cfg := &config.Config{
		DisabledModules: []string{"OPDS"},
	}
	reg.ApplyConfig(cfg)
	if !reg.IsEnabled("komga") {
		t.Errorf("expected komga enabled under cfg")
	}
	if reg.IsEnabled("opds") {
		t.Errorf("expected opds disabled under cfg")
	}
}

func TestRegistry_SeamsCollection(t *testing.T) {
	reg := NewRegistry()

	handlerA := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	modA := &SimpleModule{
		ModuleName: "mod-a",
		Mounts: []RouteMount{
			{Prefix: "/a", Handler: handlerA},
		},
		Auths: []Authenticator{
			NamedAuthenticator("auth-a"),
		},
		Jobs: []JobKind{
			NamedJobKind("job-a"),
		},
		Subscribers: []EventSubscriber{
			NamedEventSubscriber("sub-a"),
		},
		Migrations: fstest.MapFS{
			"9001_mod_a.sql": &fstest.MapFile{Data: []byte("-- migration a")},
		},
	}

	handlerB := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	modB := &SimpleModule{
		ModuleName: "mod-b",
		Mounts: []RouteMount{
			{Prefix: "/b", Handler: handlerB},
		},
		Auths: []Authenticator{
			NamedAuthenticator("auth-b"),
		},
		Jobs: []JobKind{
			NamedJobKind("job-b"),
		},
		Subscribers: []EventSubscriber{
			NamedEventSubscriber("sub-b"),
		},
		Migrations: fstest.MapFS{
			"9002_mod_b.sql": &fstest.MapFile{Data: []byte("-- migration b")},
		},
	}

	if err := reg.Register(modA); err != nil {
		t.Fatalf("Register(modA) error: %v", err)
	}
	if err := reg.Register(modB); err != nil {
		t.Fatalf("Register(modB) error: %v", err)
	}

	// Both enabled
	routes := reg.Routes()
	if len(routes) != 2 {
		t.Errorf("Routes() len = %d, want 2", len(routes))
	}
	if routes[0].ModuleName != "mod-a" || routes[1].ModuleName != "mod-b" {
		t.Errorf("Routes() module names = %q, %q", routes[0].ModuleName, routes[1].ModuleName)
	}

	if auths := reg.Authenticators(); len(auths) != 2 {
		t.Errorf("Authenticators() len = %d, want 2", len(auths))
	}
	if jobs := reg.JobKinds(); len(jobs) != 2 {
		t.Errorf("JobKinds() len = %d, want 2", len(jobs))
	}
	if subs := reg.EventSubscribers(); len(subs) != 2 {
		t.Errorf("EventSubscribers() len = %d, want 2", len(subs))
	}
	if migs := reg.MigrationSources(); len(migs) != 2 {
		t.Errorf("MigrationSources() len = %d, want 2", len(migs))
	}

	// Disable mod-a: routes, auths, jobs, and subs from mod-a are excluded
	reg.SetEnabled("mod-a", false)

	routes = reg.Routes()
	if len(routes) != 1 || routes[0].ModuleName != "mod-b" {
		t.Errorf("Routes() with mod-a disabled = %v, want only mod-b", routes)
	}
	if auths := reg.Authenticators(); len(auths) != 1 || auths[0].Name() != "auth-b" {
		t.Errorf("Authenticators() with mod-a disabled = %v, want only auth-b", auths)
	}
	if jobs := reg.JobKinds(); len(jobs) != 1 || jobs[0].Name() != "job-b" {
		t.Errorf("JobKinds() with mod-a disabled = %v, want only job-b", jobs)
	}
	if subs := reg.EventSubscribers(); len(subs) != 1 || subs[0].Name() != "sub-b" {
		t.Errorf("EventSubscribers() with mod-a disabled = %v, want only sub-b", subs)
	}

	// CRITICAL: MigrationSources() MUST still return migrations for disabled modules (ADR-0018 §2)!
	migsRemaining := reg.MigrationSources()
	if len(migsRemaining) != 2 {
		t.Errorf("MigrationSources() with mod-a disabled len = %d, want 2 (disabled module migrations must not be dropped)", len(migsRemaining))
	}
}

func TestRegistry_ValidateMigrations(t *testing.T) {
	t.Run("valid migrations in module range", func(t *testing.T) {
		reg := NewRegistry()
		mod := &SimpleModule{
			ModuleName: "komga",
			MinVersion: ModuleKomgaMinVersion,
			MaxVersion: ModuleKomgaMaxVersion,
			Migrations: fstest.MapFS{
				"1001_komga_init.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			},
		}
		if err := reg.Register(mod); err != nil {
			t.Fatalf("Register error: %v", err)
		}
		if err := reg.ValidateMigrations(); err != nil {
			t.Errorf("ValidateMigrations() unexpected error: %v", err)
		}
	})

	t.Run("migration in core range is rejected", func(t *testing.T) {
		reg := NewRegistry()
		mod := &SimpleModule{
			ModuleName: "bad-mod",
			Migrations: fstest.MapFS{
				"0005_stealing_core.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			},
		}
		if err := reg.Register(mod); err != nil {
			t.Fatalf("Register error: %v", err)
		}
		err := reg.ValidateMigrations()
		if err == nil {
			t.Fatal("expected error for migration in core range, got nil")
		}
		if !errors.Is(err, ErrCoreRangeConflict) {
			t.Errorf("expected ErrCoreRangeConflict, got: %v", err)
		}
	})

	t.Run("migration outside declared range is rejected", func(t *testing.T) {
		reg := NewRegistry()
		mod := &SimpleModule{
			ModuleName: "komga",
			MinVersion: 1000,
			MaxVersion: 1999,
			Migrations: fstest.MapFS{
				"2001_wrong_range.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			},
		}
		if err := reg.Register(mod); err != nil {
			t.Fatalf("Register error: %v", err)
		}
		err := reg.ValidateMigrations()
		if err == nil {
			t.Fatal("expected error for migration outside declared range, got nil")
		}
		if !errors.Is(err, ErrInvalidMigrationRange) {
			t.Errorf("expected ErrInvalidMigrationRange, got: %v", err)
		}
	})

	t.Run("duplicate version between modules is rejected", func(t *testing.T) {
		reg := NewRegistry()
		modA := &SimpleModule{
			ModuleName: "mod-a",
			Migrations: fstest.MapFS{
				"9001_first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			},
		}
		modB := &SimpleModule{
			ModuleName: "mod-b",
			Migrations: fstest.MapFS{
				"9001_collision.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			},
		}
		_ = reg.Register(modA)
		_ = reg.Register(modB)

		err := reg.ValidateMigrations()
		if err == nil {
			t.Fatal("expected error for duplicate migration version, got nil")
		}
		if !errors.Is(err, ErrDuplicateMigration) {
			t.Errorf("expected ErrDuplicateMigration, got: %v", err)
		}
	})
}

func TestRegistry_DatabaseMigrationRunnerIntegration(t *testing.T) {
	ctx := context.Background()
	db := testdb.New(t)

	reg := NewRegistry()

	moduleSQL := `
CREATE TABLE module_test_items (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
`
	mod := &SimpleModule{
		ModuleName: "test-db-module",
		MinVersion: ModuleTestMinVersion,
		MaxVersion: ModuleTestMaxVersion,
		Migrations: fstest.MapFS{
			"9001_create_module_test_items.sql": &fstest.MapFile{Data: []byte(moduleSQL)},
		},
	}

	if err := reg.Register(mod); err != nil {
		t.Fatalf("Register error: %v", err)
	}

	// 1. Run migrations with core migrations + module migrations
	allSources := append([]fs.FS{migrations.CoreFS}, reg.MigrationSources()...)
	if err := migrations.RunFS(ctx, db, allSources...); err != nil {
		t.Fatalf("RunFS error: %v", err)
	}

	// Verify table was created by inserting a row
	_, err := db.Exec(ctx, "INSERT INTO module_test_items (id, name) VALUES ($1, $2)", "id-1", "test-item")
	if err != nil {
		t.Fatalf("inserting into module_test_items failed: %v", err)
	}

	// 2. Disable module by config
	cfg := &config.Config{
		DisabledModules: []string{"test-db-module"},
	}
	reg.ApplyConfig(cfg)
	if reg.IsEnabled("test-db-module") {
		t.Fatalf("expected test-db-module to be disabled")
	}

	// 3. Re-run migrations with disabled module
	// Crucial invariant: MigrationSources() STILL includes test-db-module's migration,
	// so RunFS does NOT return ErrMissingMigration!
	allSourcesAfterDisable := append([]fs.FS{migrations.CoreFS}, reg.MigrationSources()...)
	if err := migrations.RunFS(ctx, db, allSourcesAfterDisable...); err != nil {
		t.Fatalf("re-running migrations after disabling module failed: %v (disabling module must not cause ErrMissingMigration)", err)
	}

	// Applied table is still functional
	var name string
	err = db.QueryRow(ctx, "SELECT name FROM module_test_items WHERE id = $1", "id-1").Scan(&name)
	if err != nil {
		t.Fatalf("querying module_test_items after disable failed: %v", err)
	}
	if name != "test-item" {
		t.Errorf("name = %q, want %q", name, "test-item")
	}
}
