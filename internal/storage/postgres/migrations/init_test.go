package migrations_test

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
	"github.com/jackc/pgx/v5/pgconn"
)

func apply0001Init(t *testing.T) migrations.DB {
	t.Helper()
	db := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("failed to apply core migration: %v", err)
	}
	return db
}

func TestMigration0001_Applies(t *testing.T) {
	t.Parallel()
	db := apply0001Init(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify schema_migrations recorded 0001_init.sql
	applied, err := migrations.Applied(ctx, db)
	if err != nil {
		t.Fatalf("querying applied migrations: %v", err)
	}
	if len(applied) != 1 {
		t.Fatalf("expected 1 applied migration, got %d", len(applied))
	}
	if applied[0].Version != 1 || applied[0].Name != "0001_init.sql" {
		t.Fatalf("unexpected applied migration: %+v", applied[0])
	}

	// Verify all 12 M1A tables exist
	expectedTables := []string{
		"system_settings",
		"users",
		"credentials",
		"roles",
		"role_permissions",
		"user_roles",
		"labels",
		"item_labels",
		"user_label_policies",
		"domain_events",
		"event_subscribers",
		"jobs",
	}

	for _, table := range expectedTables {
		var exists bool
		err := db.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
			table,
		).Scan(&exists)
		if err != nil {
			t.Fatalf("checking table existence for %q: %v", table, err)
		}
		if !exists {
			t.Errorf("expected table %q to exist in database", table)
		}
	}

	// Verify built-in role seed rows and their permissions
	type roleExpected struct {
		name                string
		builtIn             bool
		requiresAllowLabels bool
		permissions         []string
	}

	expectedRoles := map[string]roleExpected{
		"019318a0-0000-7000-8000-000000000001": {
			name:                "Admin",
			builtIn:             true,
			requiresAllowLabels: false,
			permissions: []string{
				"collections:manage",
				"jobs:read_all",
				"library:admin",
				"library:read",
				"metadata:edit",
				"progress:write",
				"trash:purge",
				"users:admin",
			},
		},
		"019318a0-0000-7000-8000-000000000002": {
			name:                "Curator",
			builtIn:             true,
			requiresAllowLabels: false,
			permissions: []string{
				"collections:manage",
				"library:read",
				"metadata:edit",
				"progress:write",
			},
		},
		"019318a0-0000-7000-8000-000000000003": {
			name:                "Reader",
			builtIn:             true,
			requiresAllowLabels: false,
			permissions: []string{
				"library:read",
				"progress:write",
			},
		},
		"019318a0-0000-7000-8000-000000000004": {
			name:                "Restricted reader",
			builtIn:             true,
			requiresAllowLabels: true,
			permissions: []string{
				"library:read",
				"progress:write",
			},
		},
	}

	for id, expected := range expectedRoles {
		var name string
		var builtIn bool
		var requiresAllowLabels bool
		err := db.QueryRow(ctx, "SELECT name, built_in, requires_allow_labels FROM roles WHERE id = $1", id).Scan(&name, &builtIn, &requiresAllowLabels)
		if err != nil {
			t.Errorf("querying role %s: %v", id, err)
			continue
		}
		if name != expected.name || builtIn != expected.builtIn || requiresAllowLabels != expected.requiresAllowLabels {
			t.Errorf("role %s: got name=%q, built_in=%v, requires_allow_labels=%v; want name=%q, built_in=%v, requires_allow_labels=%v",
				id, name, builtIn, requiresAllowLabels, expected.name, expected.builtIn, expected.requiresAllowLabels)
		}

		rows, err := db.Query(ctx, "SELECT permission FROM role_permissions WHERE role_id = $1 ORDER BY permission ASC", id)
		if err != nil {
			t.Errorf("querying permissions for role %s: %v", id, err)
			continue
		}
		var perms []string
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				rows.Close()
				t.Fatalf("scanning permission: %v", err)
			}
			perms = append(perms, p)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("rows error: %v", err)
		}

		sort.Strings(expected.permissions)
		sort.Strings(perms)
		if strings.Join(perms, ",") != strings.Join(expected.permissions, ",") {
			t.Errorf("role %s (%s) permissions mismatch: got %v, want %v",
				id, expected.name, perms, expected.permissions)
		}
	}

	// Verify running migrations again is an idempotent no-op
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("re-running CoreFS failed: %v", err)
	}
}

func TestMigration0001_PATEmptyScopesCheck(t *testing.T) {
	t.Parallel()
	db := apply0001Init(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const userID = "019318a0-0000-7000-8000-000000000010"
	_, err := db.Exec(ctx, "INSERT INTO users (id, username) VALUES ($1, $2)", userID, "alice")
	if err != nil {
		t.Fatalf("inserting test user: %v", err)
	}

	// 1. A PASSWORD credential with empty scopes must be accepted.
	_, err = db.Exec(ctx, `
		INSERT INTO credentials (id, user_id, kind, scopes, secret_hash, hash_scheme)
		VALUES ($1, $2, 'PASSWORD', '{}', 'hash_pw', 'argon2id')`,
		"019318a0-0000-7000-8000-000000000011", userID)
	if err != nil {
		t.Fatalf("expected PASSWORD with empty scopes to succeed, got %v", err)
	}

	// 2. A PAT credential with empty scopes must be rejected by the CHECK constraint.
	_, err = db.Exec(ctx, `
		INSERT INTO credentials (id, user_id, kind, scopes, secret_hash, hash_scheme)
		VALUES ($1, $2, 'PAT', '{}', 'hash_pat', 'argon2id')`,
		"019318a0-0000-7000-8000-000000000012", userID)
	if err == nil {
		t.Fatal("expected PAT with empty scopes to be rejected by CHECK constraint, but it succeeded")
	}
	if !isCheckViolation(err) {
		t.Fatalf("expected check_violation error, got %v", err)
	}

	// 3. A PAT credential with non-empty scopes must be accepted.
	_, err = db.Exec(ctx, `
		INSERT INTO credentials (id, user_id, kind, scopes, secret_hash, hash_scheme)
		VALUES ($1, $2, 'PAT', '{"read"}', 'hash_pat2', 'argon2id')`,
		"019318a0-0000-7000-8000-000000000013", userID)
	if err != nil {
		t.Fatalf("expected PAT with non-empty scopes to succeed, got %v", err)
	}
}

func TestMigration0001_OneEffectPerLabelKey(t *testing.T) {
	t.Parallel()
	db := apply0001Init(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const userID = "019318a0-0000-7000-8000-000000000020"
	const labelID = "019318a0-0000-7000-8000-000000000021"

	_, err := db.Exec(ctx, "INSERT INTO users (id, username) VALUES ($1, $2)", userID, "bob")
	if err != nil {
		t.Fatalf("inserting test user: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO labels (id, scope, owner_id, name) VALUES ($1, 'SERVER', NULL, $2)", labelID, "kids-safe")
	if err != nil {
		t.Fatalf("inserting test server label: %v", err)
	}

	// 1. Inserting an ALLOW effect succeeds.
	_, err = db.Exec(ctx, "INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'ALLOW')", userID, labelID)
	if err != nil {
		t.Fatalf("inserting first policy row: %v", err)
	}

	// 2. Inserting a RESTRICT effect for the same (user_id, label_id) must be rejected
	// by duplicate key violation on the primary key (one effect per subject/label).
	_, err = db.Exec(ctx, "INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')", userID, labelID)
	if err == nil {
		t.Fatal("expected second policy for same (user_id, label_id) to be rejected, but it succeeded")
	}
	if !isUniqueViolation(err) {
		t.Fatalf("expected unique/primary key violation, got %v", err)
	}
}

func TestMigration0001_ServerOnlyPolicyTrigger(t *testing.T) {
	t.Parallel()
	db := apply0001Init(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const userID = "019318a0-0000-7000-8000-000000000030"
	const userLabelID = "019318a0-0000-7000-8000-000000000031"
	const serverLabelID = "019318a0-0000-7000-8000-000000000032"

	_, err := db.Exec(ctx, "INSERT INTO users (id, username) VALUES ($1, $2)", userID, "carol")
	if err != nil {
		t.Fatalf("inserting user: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO labels (id, scope, owner_id, name) VALUES ($1, 'USER', $2, 'my-favorite')", userLabelID, userID)
	if err != nil {
		t.Fatalf("inserting user label: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO labels (id, scope, owner_id, name) VALUES ($1, 'SERVER', NULL, 'mature-content')", serverLabelID)
	if err != nil {
		t.Fatalf("inserting server label: %v", err)
	}

	// 1. Attempting to attach a policy to a USER label must be rejected by the trigger.
	_, err = db.Exec(ctx, "INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'ALLOW')", userID, userLabelID)
	if err == nil {
		t.Fatal("expected user_label_policies with USER label to be rejected by trigger, but it succeeded")
	}
	if !strings.Contains(err.Error(), "only permit SERVER labels") {
		t.Fatalf("expected trigger exception mentioning SERVER labels, got: %v", err)
	}

	// 2. Attaching a policy to a SERVER label must succeed.
	_, err = db.Exec(ctx, "INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')", userID, serverLabelID)
	if err != nil {
		t.Fatalf("expected user_label_policies with SERVER label to succeed, got %v", err)
	}

	// 3. Attempting to change the scope of the referenced SERVER label to USER must be rejected.
	_, err = db.Exec(ctx, "UPDATE labels SET scope = 'USER', owner_id = $1 WHERE id = $2", userID, serverLabelID)
	if err == nil {
		t.Fatal("expected changing scope of referenced SERVER label to be rejected, but it succeeded")
	}
	if !strings.Contains(err.Error(), "Cannot change scope of label referenced in user_label_policies") {
		t.Fatalf("expected trigger exception on scope change, got: %v", err)
	}

	// 4. Attempting to delete the referenced SERVER label must fail via ON DELETE RESTRICT.
	_, err = db.Exec(ctx, "DELETE FROM labels WHERE id = $1", serverLabelID)
	if err == nil {
		t.Fatal("expected deleting referenced SERVER label to fail via ON DELETE RESTRICT, but it succeeded")
	}
	if !isForeignKeyViolation(err) {
		t.Fatalf("expected foreign key violation on delete, got: %v", err)
	}
}

func TestMigration0001_PublishedSequencePublishedAtPairing(t *testing.T) {
	t.Parallel()
	db := apply0001Init(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Both NULL (unsequenced outbox event) -> must succeed.
	_, err := db.Exec(ctx, `
		INSERT INTO domain_events (published_sequence, published_at, kind, aggregate_type, aggregate_id)
		VALUES (NULL, NULL, 'job.queued', 'job', '019318a0-0000-7000-8000-000000000041')`)
	if err != nil {
		t.Fatalf("expected both NULL to succeed, got %v", err)
	}

	// 2. Both NOT NULL (sequenced published event) -> must succeed.
	_, err = db.Exec(ctx, `
		INSERT INTO domain_events (published_sequence, published_at, kind, aggregate_type, aggregate_id)
		VALUES (1, now(), 'job.finished', 'job', '019318a0-0000-7000-8000-000000000042')`)
	if err != nil {
		t.Fatalf("expected both NOT NULL to succeed, got %v", err)
	}

	// 3. published_sequence is set, but published_at is NULL -> must be rejected by CHECK constraint.
	_, err = db.Exec(ctx, `
		INSERT INTO domain_events (published_sequence, published_at, kind, aggregate_type, aggregate_id)
		VALUES (2, NULL, 'job.started', 'job', '019318a0-0000-7000-8000-000000000043')`)
	if err == nil {
		t.Fatal("expected published_sequence set with published_at NULL to be rejected, but it succeeded")
	}
	if !isCheckViolation(err) {
		t.Fatalf("expected check violation, got %v", err)
	}

	// 4. published_sequence is NULL, but published_at is set -> must be rejected by CHECK constraint.
	_, err = db.Exec(ctx, `
		INSERT INTO domain_events (published_sequence, published_at, kind, aggregate_type, aggregate_id)
		VALUES (NULL, now(), 'job.started', 'job', '019318a0-0000-7000-8000-000000000044')`)
	if err == nil {
		t.Fatal("expected published_sequence NULL with published_at set to be rejected, but it succeeded")
	}
	if !isCheckViolation(err) {
		t.Fatalf("expected check violation, got %v", err)
	}
}

func TestMigration0001_JobsStateCheck(t *testing.T) {
	t.Parallel()
	db := apply0001Init(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	validStates := []string{"QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"}
	for i, state := range validStates {
		jobID := "019318a0-0000-7000-8000-00000000005" + string(rune('0'+i))
		_, err := db.Exec(ctx, `
			INSERT INTO jobs (id, kind, state)
			VALUES ($1, 'SCAN', $2)`, jobID, state)
		if err != nil {
			t.Fatalf("expected valid state %q to be accepted, got: %v", state, err)
		}
	}

	// Invalid state must be rejected by CHECK constraint
	_, err := db.Exec(ctx, `
		INSERT INTO jobs (id, kind, state)
		VALUES ('019318a0-0000-7000-8000-000000000059', 'SCAN', 'UNKNOWN_STATUS')`)
	if err == nil {
		t.Fatal("expected invalid state to be rejected by CHECK constraint, but it succeeded")
	}
	if !isCheckViolation(err) {
		t.Fatalf("expected check violation on invalid state, got: %v", err)
	}
}

func isCheckViolation(err error) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == "23514" // check_violation
	}
	return false
}

func isUniqueViolation(err error) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == "23505" // unique_violation
	}
	return false
}

func isForeignKeyViolation(err error) bool {
	if pgErr, ok := err.(*pgconn.PgError); ok {
		return pgErr.Code == "23503" || pgErr.Code == "23001" // foreign_key_violation or restrict_violation
	}
	return false
}
