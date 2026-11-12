package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func setupSettingsTestDB(t *testing.T) (*testdb.TestDB, storage.SettingsRepository, storage.UserRepository) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupSettingsTestDB: applying migrations: %v", err)
	}

	settingsRepo := postgres.NewSettingsRepository(db.Pool)
	userRepo := postgres.NewUserRepository(db.Pool)
	return db, settingsRepo, userRepo
}

func TestSettingsRepository_CRUD(t *testing.T) {
	t.Parallel()
	_, repo, userRepo := setupSettingsTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create a user to test updated_by
	userID := domain.MustNewID()
	user := &domain.User{
		ID:       userID,
		Username: "admin_user",
	}
	if err := userRepo.CreateUser(ctx, user); err != nil {
		t.Fatalf("CreateUser failed: %v", err)
	}

	// 2. Get non-existent setting returns ErrNotFound
	_, err := repo.GetSetting(ctx, "non.existent.key")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetSetting(non.existent) err = %v, want ErrNotFound", err)
	}

	// 3. Insert new setting
	setting := &domain.SystemSetting{
		Key:       "oidc.default_role_id",
		Value:     []byte(`"019318a0-0000-7000-8000-000000000003"`),
		IsSecret:  false,
		UpdatedBy: &userID,
	}
	if err := repo.SetSetting(ctx, setting); err != nil {
		t.Fatalf("SetSetting failed: %v", err)
	}

	// 4. Read back
	got, err := repo.GetSetting(ctx, "oidc.default_role_id")
	if err != nil {
		t.Fatalf("GetSetting failed: %v", err)
	}
	if got.Key != setting.Key {
		t.Errorf("got Key = %q, want %q", got.Key, setting.Key)
	}
	if got.ValueString() != "019318a0-0000-7000-8000-000000000003" {
		t.Errorf("got ValueString = %q, want %q", got.ValueString(), "019318a0-0000-7000-8000-000000000003")
	}
	if got.IsSecret != false {
		t.Errorf("got IsSecret = %v, want false", got.IsSecret)
	}
	if got.UpdatedBy == nil || *got.UpdatedBy != userID {
		t.Errorf("got UpdatedBy = %v, want %q", got.UpdatedBy, userID)
	}

	// 5. Upsert: update existing setting
	updateSetting := &domain.SystemSetting{
		Key:      "oidc.default_role_id",
		Value:    []byte(`"019318a0-0000-7000-8000-000000000004"`),
		IsSecret: false,
	}
	if err := repo.SetSetting(ctx, updateSetting); err != nil {
		t.Fatalf("SetSetting (update) failed: %v", err)
	}

	gotUpdated, err := repo.GetSetting(ctx, "oidc.default_role_id")
	if err != nil {
		t.Fatalf("GetSetting after update failed: %v", err)
	}
	if gotUpdated.ValueString() != "019318a0-0000-7000-8000-000000000004" {
		t.Errorf("got ValueString = %q, want %q", gotUpdated.ValueString(), "019318a0-0000-7000-8000-000000000004")
	}

	// 6. Insert a secret setting and a second setting
	secretSetting := &domain.SystemSetting{
		Key:      "scraper.comicvine.key",
		Value:    []byte(`"encrypted_secret_data"`),
		IsSecret: true,
	}
	if err := repo.SetSetting(ctx, secretSetting); err != nil {
		t.Fatalf("SetSetting(secret) failed: %v", err)
	}

	intSetting := &domain.SystemSetting{
		Key:      "events.retention_days",
		Value:    []byte(`7`),
		IsSecret: false,
	}
	if err := repo.SetSetting(ctx, intSetting); err != nil {
		t.Fatalf("SetSetting(int) failed: %v", err)
	}

	// 7. ListSettings
	list, err := repo.ListSettings(ctx)
	if err != nil {
		t.Fatalf("ListSettings failed: %v", err)
	}
	if len(list) < 3 {
		t.Fatalf("ListSettings returned %d items, want at least 3", len(list))
	}

	// 8. GetSettingsMap: should exclude secrets
	m, err := repo.GetSettingsMap(ctx)
	if err != nil {
		t.Fatalf("GetSettingsMap failed: %v", err)
	}
	if m["events.retention_days"] != "7" {
		t.Errorf("GetSettingsMap[events.retention_days] = %q, want %q", m["events.retention_days"], "7")
	}
	if m["oidc.default_role_id"] != "019318a0-0000-7000-8000-000000000004" {
		t.Errorf("GetSettingsMap[oidc.default_role_id] = %q, want %q", m["oidc.default_role_id"], "019318a0-0000-7000-8000-000000000004")
	}
	if _, ok := m["scraper.comicvine.key"]; ok {
		t.Errorf("GetSettingsMap included secret key scraper.comicvine.key")
	}

	// 9. DeleteSetting
	if err := repo.DeleteSetting(ctx, "oidc.default_role_id"); err != nil {
		t.Fatalf("DeleteSetting failed: %v", err)
	}
	_, err = repo.GetSetting(ctx, "oidc.default_role_id")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("GetSetting after delete err = %v, want ErrNotFound", err)
	}
}

func TestSettingsRepository_ValidationAndConstraints(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupSettingsTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Nil setting
	if err := repo.SetSetting(ctx, nil); err == nil {
		t.Errorf("SetSetting(nil) expected error, got nil")
	}

	// Empty key
	if err := repo.SetSetting(ctx, &domain.SystemSetting{Key: "", Value: []byte(`"val"`)}); !errors.Is(err, domain.ErrEmptySettingKey) {
		t.Errorf("SetSetting(empty key) err = %v, want ErrEmptySettingKey", err)
	}

	// Empty value
	if err := repo.SetSetting(ctx, &domain.SystemSetting{Key: "test", Value: nil}); !errors.Is(err, domain.ErrEmptySettingVal) {
		t.Errorf("SetSetting(empty val) err = %v, want ErrEmptySettingVal", err)
	}

	// Non-existent user for updated_by FK constraint
	badUserID := domain.MustNewID()
	err := repo.SetSetting(ctx, &domain.SystemSetting{
		Key:       "test.fk",
		Value:     []byte(`"val"`),
		UpdatedBy: &badUserID,
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("SetSetting(bad updated_by) err = %v, want ErrNotFound", err)
	}
}

func TestSettingsRepository_WithTx(t *testing.T) {
	t.Parallel()
	db, _, _ := setupSettingsTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	repo := postgres.NewSettingsRepository(db.Pool)

	// Rollback test: error returned in WithTx should discard mutations
	rollbackErr := errors.New("simulated error")
	err := repo.WithTx(ctx, func(txRepo storage.SettingsRepository) error {
		if err := txRepo.SetSetting(ctx, &domain.SystemSetting{
			Key:   "rollback.test",
			Value: []byte(`"will_be_reverted"`),
		}); err != nil {
			return err
		}
		return rollbackErr
	})
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("WithTx error = %v, want %v", err, rollbackErr)
	}

	// Verify setting was rolled back
	_, err = repo.GetSetting(ctx, "rollback.test")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("GetSetting(rollback.test) after aborted tx err = %v, want ErrNotFound", err)
	}

	// Commit test
	err = repo.WithTx(ctx, func(txRepo storage.SettingsRepository) error {
		return txRepo.SetSetting(ctx, &domain.SystemSetting{
			Key:   "commit.test",
			Value: []byte(`"will_be_persisted"`),
		})
	})
	if err != nil {
		t.Fatalf("WithTx (commit) unexpected error: %v", err)
	}

	got, err := repo.GetSetting(ctx, "commit.test")
	if err != nil {
		t.Fatalf("GetSetting(commit.test) failed: %v", err)
	}
	if got.ValueString() != "will_be_persisted" {
		t.Errorf("got %q, want %q", got.ValueString(), "will_be_persisted")
	}
}
