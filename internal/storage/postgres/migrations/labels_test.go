package migrations_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func applyHeadMigrations(t *testing.T) migrations.DB {
	t.Helper()
	ctx := context.Background()
	db := testdb.New(t)
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("RunFS(CoreFS) failed: %v", err)
	}
	return db
}

func insertTestUser(ctx context.Context, t *testing.T, db migrations.DB, id, username string) {
	t.Helper()
	_, err := db.Exec(ctx, `INSERT INTO users (id, username) VALUES ($1, $2)`, id, username)
	if err != nil {
		t.Fatalf("insert test user %s: %v", id, err)
	}
}

func isPgSQLState(err error, codes ...string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return slices.Contains(codes, pgErr.Code)
}

func TestMigration0005_BuiltInMarkedForDeletionSeed(t *testing.T) {
	ctx := context.Background()
	db := applyHeadMigrations(t)

	for _, want := range domain.BuiltInServerLabels() {
		var (
			id      string
			scope   string
			ownerID *string
			name    string
			builtIn bool
		)
		err := db.QueryRow(ctx,
			`SELECT id, scope, owner_id, name, built_in FROM labels WHERE name = $1 AND scope = 'SERVER'`,
			want.Name,
		).Scan(&id, &scope, &ownerID, &name, &builtIn)
		if err != nil {
			t.Fatalf("query built-in server label %q: %v", want.Name, err)
		}
		if id != want.ID || scope != string(want.Scope) || ownerID != nil || name != want.Name || builtIn != want.BuiltIn {
			t.Fatalf("built-in server label mismatch: got (%q,%q,%v,%q,%v), want (%q,%q,%v,%q,%v)",
				id, scope, ownerID, name, builtIn,
				want.ID, want.Scope, want.OwnerID, want.Name, want.BuiltIn)
		}
	}

	// Re-running migrations must be idempotent and keep a single marked_for_deletion row.
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("second RunFS(CoreFS) failed: %v", err)
	}
	var count int
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM labels WHERE scope = 'SERVER' AND name = $1`,
		domain.LabelMarkedForDeletion,
	).Scan(&count); err != nil {
		t.Fatalf("count marked_for_deletion: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 marked_for_deletion label after idempotent rerun, got %d", count)
	}
}

func TestMigration0005_ServerOnlyPolicyAndRestrictDelete(t *testing.T) {
	ctx := context.Background()
	db := applyHeadMigrations(t)

	userID := domain.MustNewID()
	userLabelID := domain.MustNewID()
	serverLabelID := domain.MustNewID()
	insertTestUser(ctx, t, db, userID, "policy-tester")

	if _, err := db.Exec(ctx,
		`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'USER', $2, 'spoiler-free', FALSE)`,
		userLabelID, userID,
	); err != nil {
		t.Fatalf("insert USER label: %v", err)
	}
	if _, err := db.Exec(ctx,
		`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'SERVER', NULL, 'mature-content', FALSE)`,
		serverLabelID,
	); err != nil {
		t.Fatalf("insert SERVER label: %v", err)
	}

	// 1. Inserting a policy row targeting a USER label must be rejected by the DB trigger.
	_, err := db.Exec(ctx,
		`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')`,
		userID, userLabelID,
	)
	if err == nil || !strings.Contains(err.Error(), "only permit SERVER labels") {
		t.Fatalf("expected SERVER-only policy trigger error, got: %v", err)
	}

	// 2. Inserting a policy targeting a SERVER label succeeds.
	if _, err := db.Exec(ctx,
		`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')`,
		userID, serverLabelID,
	); err != nil {
		t.Fatalf("insert SERVER label policy: %v", err)
	}

	// 3. Referenced SERVER label cannot be deleted until its policies are removed (ON DELETE RESTRICT).
	_, err = db.Exec(ctx, `DELETE FROM labels WHERE id = $1`, serverLabelID)
	if !isPgSQLState(err, "23001", "23503") {
		t.Fatalf("expected ON DELETE RESTRICT error (23001/23503) deleting referenced SERVER label, got: %v", err)
	}

	// 4. Removing the policy allows deleting the SERVER label.
	if _, err := db.Exec(ctx,
		`DELETE FROM user_label_policies WHERE user_id = $1 AND label_id = $2`,
		userID, serverLabelID,
	); err != nil {
		t.Fatalf("delete user_label_policy: %v", err)
	}
	if _, err := db.Exec(ctx, `DELETE FROM labels WHERE id = $1`, serverLabelID); err != nil {
		t.Fatalf("delete unreferenced SERVER label: %v", err)
	}
}

func TestMigration0005_RepeatableRead_ScopeChangeVsPolicyInsert(t *testing.T) {
	ctx := context.Background()

	t.Run("PolicyCommitsFirst_RepeatableReadScopeChangeBlocked", func(t *testing.T) {
		db := applyHeadMigrations(t)
		userID := domain.MustNewID()
		labelID := domain.MustNewID()
		insertTestUser(ctx, t, db, userID, "rr-user-1")

		if _, err := db.Exec(ctx,
			`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'SERVER', NULL, 'restricted-tag', FALSE)`,
			labelID,
		); err != nil {
			t.Fatalf("insert SERVER label: %v", err)
		}

		// Pin a Repeatable Read snapshot BEFORE any policy row exists.
		txChange := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, txChange)
		pinRepeatableReadSnapshot(ctx, t, txChange)

		// Concurrently insert and commit a policy referencing labelID.
		txPolicy := beginTxWithIso(ctx, t, db, pgx.ReadCommitted)
		defer rollbackQuiet(ctx, txPolicy)
		if _, err := txPolicy.Exec(ctx,
			`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')`,
			userID, labelID,
		); err != nil {
			t.Fatalf("txPolicy insert: %v", err)
		}
		if err := txPolicy.Commit(ctx); err != nil {
			t.Fatalf("txPolicy commit: %v", err)
		}

		// txChange cannot see the policy row in its stale snapshot, but trg_check_user_label_policy_scope
		// updated the labels row version on commit, so converting scope to USER must abort with 40001.
		_, err := txChange.Exec(ctx,
			`UPDATE labels SET scope = 'USER', owner_id = $2 WHERE id = $1`,
			labelID, userID,
		)
		if !isPgSQLState(err, "40001") {
			t.Fatalf("expected 40001 serialization failure converting scope under stale RR snapshot, got: %v", err)
		}
		rollbackQuiet(ctx, txChange)

		var scope string
		if err := db.QueryRow(ctx, `SELECT scope FROM labels WHERE id = $1`, labelID).Scan(&scope); err != nil {
			t.Fatalf("verify label scope: %v", err)
		}
		if scope != "SERVER" {
			t.Fatalf("expected label scope to remain SERVER, got %q", scope)
		}
	})

	t.Run("ScopeChangeCommitsFirst_RepeatableReadPolicyInsertBlocked", func(t *testing.T) {
		db := applyHeadMigrations(t)
		userID := domain.MustNewID()
		labelID := domain.MustNewID()
		insertTestUser(ctx, t, db, userID, "rr-user-2")

		if _, err := db.Exec(ctx,
			`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'SERVER', NULL, 'converting-tag', FALSE)`,
			labelID,
		); err != nil {
			t.Fatalf("insert SERVER label: %v", err)
		}

		// Pin a Repeatable Read snapshot while labelID still has scope = 'SERVER'.
		txPolicy := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, txPolicy)
		pinRepeatableReadSnapshot(ctx, t, txPolicy)

		// Concurrently convert labelID from SERVER to USER and commit (no policies exist yet).
		txChange := beginTxWithIso(ctx, t, db, pgx.ReadCommitted)
		defer rollbackQuiet(ctx, txChange)
		if _, err := txChange.Exec(ctx,
			`UPDATE labels SET scope = 'USER', owner_id = $2 WHERE id = $1`,
			labelID, userID,
		); err != nil {
			t.Fatalf("txChange update scope: %v", err)
		}
		if err := txChange.Commit(ctx); err != nil {
			t.Fatalf("txChange commit: %v", err)
		}

		// txPolicy tries to insert a policy under its stale RR snapshot.
		// Updating the labels tuple inside the trigger detects the committed change and rejects with 40001.
		_, err := txPolicy.Exec(ctx,
			`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'ALLOW')`,
			userID, labelID,
		)
		if !isPgSQLState(err, "40001") {
			t.Fatalf("expected 40001 serialization failure inserting policy against concurrently converted label, got: %v", err)
		}
		rollbackQuiet(ctx, txPolicy)

		var policyCount int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM user_label_policies WHERE label_id = $1`, labelID).Scan(&policyCount); err != nil {
			t.Fatalf("count policies: %v", err)
		}
		if policyCount != 0 {
			t.Fatalf("expected 0 policies for converted USER label, got %d", policyCount)
		}
	})

	t.Run("ConcurrentInFlightPolicyInsertBlocksScopeChange", func(t *testing.T) {
		db := applyHeadMigrations(t)
		userID := domain.MustNewID()
		labelID := domain.MustNewID()
		insertTestUser(ctx, t, db, userID, "rc-user-3")

		if _, err := db.Exec(ctx,
			`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'SERVER', NULL, 'in-flight-tag', FALSE)`,
			labelID,
		); err != nil {
			t.Fatalf("insert SERVER label: %v", err)
		}

		txPolicy := beginTxWithIso(ctx, t, db, pgx.ReadCommitted)
		defer rollbackQuiet(ctx, txPolicy)
		if _, err := txPolicy.Exec(ctx,
			`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')`,
			userID, labelID,
		); err != nil {
			t.Fatalf("txPolicy insert: %v", err)
		}

		txChange := beginTxWithIso(ctx, t, db, pgx.RepeatableRead)
		defer rollbackQuiet(ctx, txChange)
		pinRepeatableReadSnapshot(ctx, t, txChange)

		started := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			close(started)
			_, err := txChange.Exec(ctx,
				`UPDATE labels SET scope = 'USER', owner_id = $2 WHERE id = $1`,
				labelID, userID,
			)
			done <- err
		}()

		<-started
		// Give txChange time to block on the row lock held by txPolicy.
		time.Sleep(75 * time.Millisecond)

		if err := txPolicy.Commit(ctx); err != nil {
			t.Fatalf("txPolicy commit: %v", err)
		}

		err := <-done
		if !isPgSQLState(err, "40001") {
			t.Fatalf("expected blocked RR scope-change to fail with 40001 after policy commit, got: %v", err)
		}
		rollbackQuiet(ctx, txChange)
	})
}
