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

func TestMigration0005_PersonalBuiltInBackfillAndTrashProtection(t *testing.T) {
	ctx := context.Background()
	db := testdb.New(t)

	coreMigs, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("Collect(CoreFS): %v", err)
	}
	var pre0005 []migrations.Migration
	for _, m := range coreMigs {
		if m.Version < 5 {
			pre0005 = append(pre0005, m)
		}
	}
	if err := migrations.Run(ctx, db, pre0005); err != nil {
		t.Fatalf("Run pre-0005 migrations: %v", err)
	}

	userEmptyID := domain.MustNewID()
	userPartialID := domain.MustNewID()
	existingWantID := domain.MustNewID()
	existingFavID := domain.MustNewID()
	legacyTrashID := domain.MustNewID()
	seriesID := domain.MustNewID()
	volumeID := domain.MustNewID()

	insertTestUser(ctx, t, db, userEmptyID, "pre-empty")
	insertTestUser(ctx, t, db, userPartialID, "pre-partial")

	// Partial user has want-to-read (built_in=FALSE) and favorite (built_in=TRUE), missing dnf.
	if _, err := db.Exec(ctx,
		`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'USER', $2, 'want-to-read', FALSE), ($3, 'USER', $2, 'favorite', TRUE)`,
		existingWantID, userPartialID, existingFavID,
	); err != nil {
		t.Fatalf("seed partial user labels: %v", err)
	}
	if _, err := db.Exec(ctx,
		`INSERT INTO item_labels (label_id, item_type, item_id) VALUES ($1, 'SERIES', $2)`,
		existingWantID, seriesID,
	); err != nil {
		t.Fatalf("seed pre-existing user item_labels: %v", err)
	}

	// Simulate pre-0005 SERVER marked_for_deletion under a legacy ID with staged marks & policy.
	if _, err := db.Exec(ctx,
		`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'SERVER', NULL, 'marked_for_deletion', FALSE)`,
		legacyTrashID,
	); err != nil {
		t.Fatalf("seed legacy trash label: %v", err)
	}
	if _, err := db.Exec(ctx,
		`INSERT INTO item_labels (label_id, item_type, item_id) VALUES ($1, 'VOLUME', $2)`,
		legacyTrashID, volumeID,
	); err != nil {
		t.Fatalf("seed legacy trash mark: %v", err)
	}
	if _, err := db.Exec(ctx,
		`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')`,
		userEmptyID, legacyTrashID,
	); err != nil {
		t.Fatalf("seed legacy trash policy: %v", err)
	}

	// Upgrade to head (runs 0005_labels.sql).
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("RunFS(CoreFS) upgrade failed: %v", err)
	}

	// Both empty and partial users must have all 3 personal built-ins with built_in=TRUE.
	for _, uid := range []string{userEmptyID, userPartialID} {
		for _, name := range domain.BuiltInUserLabelNames() {
			var (
				gotID   string
				builtIn bool
			)
			if err := db.QueryRow(ctx,
				`SELECT id, built_in FROM labels WHERE scope = 'USER' AND owner_id = $1 AND name = $2`,
				uid, name,
			).Scan(&gotID, &builtIn); err != nil {
				t.Fatalf("query backfilled label %q for user %s: %v", name, uid, err)
			}
			if gotID == "" || !builtIn {
				t.Fatalf("unexpected backfilled label %q for user %s: id=%q built_in=%v", name, uid, gotID, builtIn)
			}
			if uid == userPartialID && name == domain.LabelWantToRead && gotID != existingWantID {
				t.Fatalf("expected existing want-to-read ID %s to be preserved, got %s", existingWantID, gotID)
			}
			if uid == userPartialID && name == domain.LabelFavorite && gotID != existingFavID {
				t.Fatalf("expected existing favorite ID %s to be preserved, got %s", existingFavID, gotID)
			}
		}
	}

	// Existing personal item assignment must survive upgrade untouched.
	var wantCount int
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM item_labels WHERE label_id = $1 AND item_type = 'SERIES' AND item_id = $2`,
		existingWantID, seriesID,
	).Scan(&wantCount); err != nil || wantCount != 1 {
		t.Fatalf("expected pre-existing user item_labels mark to survive upgrade, count=%d err=%v", wantCount, err)
	}

	// Legacy trash label marks and policies must migrate onto the canonical trash label ID cleanly.
	var trashMarks int
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM item_labels WHERE label_id = $1 AND item_type = 'VOLUME' AND item_id = $2`,
		domain.LabelMarkedForDeletionID, volumeID,
	).Scan(&trashMarks); err != nil || trashMarks != 1 {
		t.Fatalf("expected legacy trash mark on canonical ID %s, count=%d err=%v", domain.LabelMarkedForDeletionID, trashMarks, err)
	}
	var trashPolicies int
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM user_label_policies WHERE user_id = $1 AND label_id = $2 AND effect = 'RESTRICT'`,
		userEmptyID, domain.LabelMarkedForDeletionID,
	).Scan(&trashPolicies); err != nil || trashPolicies != 1 {
		t.Fatalf("expected legacy trash policy on canonical ID %s, count=%d err=%v", domain.LabelMarkedForDeletionID, trashPolicies, err)
	}

	// Remove the policy so ON DELETE RESTRICT wouldn't block deletion, then prove DB trigger
	// still protects canonical trash label from generic renames, field edits, and deletion.
	if _, err := db.Exec(ctx,
		`DELETE FROM user_label_policies WHERE user_id = $1 AND label_id = $2`,
		userEmptyID, domain.LabelMarkedForDeletionID,
	); err != nil {
		t.Fatalf("delete trash policy: %v", err)
	}

	if _, err := db.Exec(ctx,
		`UPDATE labels SET name = 'renamed-trash' WHERE id = $1`,
		domain.LabelMarkedForDeletionID,
	); err == nil || !strings.Contains(err.Error(), "cannot modify protected built-in trash label") {
		t.Fatalf("expected protected trash rename error, got: %v", err)
	}
	if _, err := db.Exec(ctx,
		`UPDATE labels SET built_in = FALSE WHERE id = $1`,
		domain.LabelMarkedForDeletionID,
	); err == nil || !strings.Contains(err.Error(), "cannot modify protected built-in trash label") {
		t.Fatalf("expected protected trash built_in edit error, got: %v", err)
	}
	if _, err := db.Exec(ctx,
		`DELETE FROM labels WHERE id = $1`,
		domain.LabelMarkedForDeletionID,
	); err == nil || !strings.Contains(err.Error(), "cannot delete protected built-in trash label") {
		t.Fatalf("expected protected trash delete error, got: %v", err)
	}

	// Verify canonical trash row and its staged mark are completely untouched.
	var trashMarksAfter int
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM item_labels WHERE label_id = $1 AND item_type = 'VOLUME' AND item_id = $2`,
		domain.LabelMarkedForDeletionID, volumeID,
	).Scan(&trashMarksAfter); err != nil || trashMarksAfter != 1 {
		t.Fatalf("expected trash mark to survive attempted delete/rename, count=%d err=%v", trashMarksAfter, err)
	}
}

func applyPre0005Migrations(ctx context.Context, t *testing.T, db migrations.DB) {
	t.Helper()
	coreMigs, err := migrations.Collect(migrations.CoreFS)
	if err != nil {
		t.Fatalf("Collect(CoreFS): %v", err)
	}
	var pre0005 []migrations.Migration
	for _, m := range coreMigs {
		if m.Version < 5 {
			pre0005 = append(pre0005, m)
		}
	}
	if err := migrations.Run(ctx, db, pre0005); err != nil {
		t.Fatalf("Run pre-0005 migrations: %v", err)
	}
}

func TestMigration0005_RejectsUnrelatedLabelAtReservedTrashID(t *testing.T) {
	ctx := context.Background()

	t.Run("USERLabelOccupantWithLegacyTrash", func(t *testing.T) {
		db := testdb.New(t)
		applyPre0005Migrations(ctx, t, db)

		userID := domain.MustNewID()
		legacyTrashID := domain.MustNewID()
		seriesID := domain.MustNewID()
		volumeID := domain.MustNewID()
		insertTestUser(ctx, t, db, userID, "collision-user")

		// Seed an unrelated private USER label at the reserved canonical trash ID.
		if _, err := db.Exec(ctx,
			`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'USER', $2, 'private-bookmark', FALSE)`,
			domain.LabelMarkedForDeletionID, userID,
		); err != nil {
			t.Fatalf("seed unrelated USER occupant: %v", err)
		}
		if _, err := db.Exec(ctx,
			`INSERT INTO item_labels (label_id, item_type, item_id) VALUES ($1, 'SERIES', $2)`,
			domain.LabelMarkedForDeletionID, seriesID,
		); err != nil {
			t.Fatalf("seed unrelated USER item_labels: %v", err)
		}

		// Also seed an independent legacy SERVER trash label with its own assignment & policy.
		if _, err := db.Exec(ctx,
			`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'SERVER', NULL, 'marked_for_deletion', FALSE)`,
			legacyTrashID,
		); err != nil {
			t.Fatalf("seed legacy trash label: %v", err)
		}
		if _, err := db.Exec(ctx,
			`INSERT INTO item_labels (label_id, item_type, item_id) VALUES ($1, 'VOLUME', $2)`,
			legacyTrashID, volumeID,
		); err != nil {
			t.Fatalf("seed legacy trash item_labels: %v", err)
		}
		if _, err := db.Exec(ctx,
			`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')`,
			userID, legacyTrashID,
		); err != nil {
			t.Fatalf("seed legacy trash policy: %v", err)
		}

		// Migration 0005 must fail with an actionable error instead of repurposing the USER label.
		err := migrations.RunFS(ctx, db, migrations.CoreFS)
		if err == nil || !strings.Contains(err.Error(), "reserved trash label ID") {
			t.Fatalf("expected actionable reserved-trash-ID collision error, got: %v", err)
		}

		// Failed migration 5 must NOT be recorded in schema_migrations.
		applied, err := migrations.Applied(ctx, db)
		if err != nil {
			t.Fatalf("query Applied migrations: %v", err)
		}
		if len(applied) != 4 || applied[len(applied)-1].Version != 4 {
			t.Fatalf("expected applied migrations to stay at version 4, got: %+v", applied)
		}

		// Original USER occupant metadata, assignments, legacy trash, policy, and no partial built-ins survive.
		var (
			gotScope   string
			gotOwnerID *string
			gotName    string
			gotBuiltIn bool
		)
		if err := db.QueryRow(ctx,
			`SELECT scope, owner_id, name, built_in FROM labels WHERE id = $1`,
			domain.LabelMarkedForDeletionID,
		).Scan(&gotScope, &gotOwnerID, &gotName, &gotBuiltIn); err != nil {
			t.Fatalf("query unrelated occupant after rollback: %v", err)
		}
		if gotScope != "USER" || gotOwnerID == nil || *gotOwnerID != userID || gotName != "private-bookmark" || gotBuiltIn {
			t.Fatalf("unrelated occupant changed after rollback: scope=%q owner=%v name=%q built_in=%v", gotScope, gotOwnerID, gotName, gotBuiltIn)
		}

		var cnt int
		if err := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM item_labels WHERE label_id = $1 AND item_type = 'SERIES' AND item_id = $2`,
			domain.LabelMarkedForDeletionID, seriesID,
		).Scan(&cnt); err != nil || cnt != 1 {
			t.Fatalf("unrelated USER item assignment lost: cnt=%d err=%v", cnt, err)
		}

		if err := db.QueryRow(ctx,
			`SELECT scope, owner_id, name, built_in FROM labels WHERE id = $1`,
			legacyTrashID,
		).Scan(&gotScope, &gotOwnerID, &gotName, &gotBuiltIn); err != nil {
			t.Fatalf("query legacy trash after rollback: %v", err)
		}
		if gotScope != "SERVER" || gotOwnerID != nil || gotName != "marked_for_deletion" || gotBuiltIn {
			t.Fatalf("legacy trash changed after rollback: scope=%q owner=%v name=%q built_in=%v", gotScope, gotOwnerID, gotName, gotBuiltIn)
		}

		var gotEffect string
		if err := db.QueryRow(ctx,
			`SELECT effect FROM user_label_policies WHERE user_id = $1 AND label_id = $2`,
			userID, legacyTrashID,
		).Scan(&gotEffect); err != nil || gotEffect != "RESTRICT" {
			t.Fatalf("legacy trash policy lost or altered: effect=%q err=%v", gotEffect, err)
		}

		var builtInCount int
		if err := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM labels WHERE owner_id = $1 AND built_in = TRUE`,
			userID,
		).Scan(&builtInCount); err != nil || builtInCount != 0 {
			t.Fatalf("expected 0 backfilled built-ins after atomic rollback, got=%d err=%v", builtInCount, err)
		}
	})

	t.Run("SERVERLabelOccupantWithConflictingPolicies", func(t *testing.T) {
		db := testdb.New(t)
		applyPre0005Migrations(ctx, t, db)

		userID := domain.MustNewID()
		legacyTrashID := domain.MustNewID()
		seriesID := domain.MustNewID()
		insertTestUser(ctx, t, db, userID, "collision-server-user")

		// Seed an unrelated SERVER label at the reserved trash ID with an ALLOW policy.
		if _, err := db.Exec(ctx,
			`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'SERVER', NULL, 'mature-18', FALSE)`,
			domain.LabelMarkedForDeletionID,
		); err != nil {
			t.Fatalf("seed unrelated SERVER occupant: %v", err)
		}
		if _, err := db.Exec(ctx,
			`INSERT INTO item_labels (label_id, item_type, item_id) VALUES ($1, 'SERIES', $2)`,
			domain.LabelMarkedForDeletionID, seriesID,
		); err != nil {
			t.Fatalf("seed unrelated SERVER item_labels: %v", err)
		}
		if _, err := db.Exec(ctx,
			`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'ALLOW')`,
			userID, domain.LabelMarkedForDeletionID,
		); err != nil {
			t.Fatalf("seed unrelated SERVER policy: %v", err)
		}

		// Also seed an independent legacy SERVER trash label with a conflicting RESTRICT policy for the same user.
		if _, err := db.Exec(ctx,
			`INSERT INTO labels (id, scope, owner_id, name, built_in) VALUES ($1, 'SERVER', NULL, 'marked_for_deletion', FALSE)`,
			legacyTrashID,
		); err != nil {
			t.Fatalf("seed legacy trash label: %v", err)
		}
		if _, err := db.Exec(ctx,
			`INSERT INTO user_label_policies (user_id, label_id, effect) VALUES ($1, $2, 'RESTRICT')`,
			userID, legacyTrashID,
		); err != nil {
			t.Fatalf("seed legacy trash policy: %v", err)
		}

		err := migrations.RunFS(ctx, db, migrations.CoreFS)
		if err == nil || !strings.Contains(err.Error(), "reserved trash label ID") {
			t.Fatalf("expected actionable reserved-trash-ID collision error, got: %v", err)
		}

		applied, err := migrations.Applied(ctx, db)
		if err != nil {
			t.Fatalf("query Applied migrations: %v", err)
		}
		if len(applied) != 4 || applied[len(applied)-1].Version != 4 {
			t.Fatalf("expected applied migrations to stay at version 4, got: %+v", applied)
		}

		// Both labels, assignments, and both distinct policy effects survive atomically untouched.
		var gotName string
		var gotBuiltIn bool
		if err := db.QueryRow(ctx,
			`SELECT name, built_in FROM labels WHERE id = $1`,
			domain.LabelMarkedForDeletionID,
		).Scan(&gotName, &gotBuiltIn); err != nil || gotName != "mature-18" || gotBuiltIn {
			t.Fatalf("unrelated SERVER occupant altered: name=%q built_in=%v err=%v", gotName, gotBuiltIn, err)
		}

		var gotOccupantEffect, gotTrashEffect string
		if err := db.QueryRow(ctx,
			`SELECT effect FROM user_label_policies WHERE user_id = $1 AND label_id = $2`,
			userID, domain.LabelMarkedForDeletionID,
		).Scan(&gotOccupantEffect); err != nil || gotOccupantEffect != "ALLOW" {
			t.Fatalf("unrelated SERVER policy lost or altered: effect=%q err=%v", gotOccupantEffect, err)
		}
		if err := db.QueryRow(ctx,
			`SELECT effect FROM user_label_policies WHERE user_id = $1 AND label_id = $2`,
			userID, legacyTrashID,
		).Scan(&gotTrashEffect); err != nil || gotTrashEffect != "RESTRICT" {
			t.Fatalf("legacy trash policy lost or altered: effect=%q err=%v", gotTrashEffect, err)
		}

		var cnt int
		if err := db.QueryRow(ctx,
			`SELECT COUNT(*) FROM item_labels WHERE label_id = $1 AND item_type = 'SERIES' AND item_id = $2`,
			domain.LabelMarkedForDeletionID, seriesID,
		).Scan(&cnt); err != nil || cnt != 1 {
			t.Fatalf("unrelated SERVER assignment lost: cnt=%d err=%v", cnt, err)
		}
	})
}
