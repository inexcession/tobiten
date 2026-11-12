package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/config"
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

func TestSettingsRepository_ConfigIntegration(t *testing.T) {
	t.Parallel()
	_, repo, _ := setupSettingsTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Seed PostgreSQL database with real stored settings
	seedSettings := []*domain.SystemSetting{
		{
			Key:      "server.port",
			Value:    []byte(`9090`),
			IsSecret: false,
		},
		{
			Key:      "log.level",
			Value:    []byte(`"debug"`),
			IsSecret: false,
		},
		{
			Key:      "storage.data_dir",
			Value:    []byte(`"/srv/tobiten"`),
			IsSecret: false,
		},
		{
			Key:      "scanner.workers",
			Value:    []byte(`4`),
			IsSecret: false,
		},
		{
			Key:      "oidc.default_role_id",
			Value:    []byte(`"019318a0-0000-7000-8000-000000000003"`),
			IsSecret: false,
		},
		{
			Key:      "oidc.default_labels",
			Value:    []byte(`["label-1", "label-2"]`),
			IsSecret: false,
		},
		{
			Key:      "scraper.comicvine.key",
			Value:    []byte(`"top_secret_ciphertext"`),
			IsSecret: true,
		},
		{
			Key:      "events.retention_days",
			Value:    []byte(`14`),
			IsSecret: false,
		},
	}

	for _, s := range seedSettings {
		if err := repo.SetSetting(ctx, s); err != nil {
			t.Fatalf("SetSetting(%q) failed: %v", s.Key, err)
		}
	}

	// 2. Read stored rows through SettingsRepository.GetSettingsMap
	settingsMap, err := repo.GetSettingsMap(ctx)
	if err != nil {
		t.Fatalf("GetSettingsMap failed: %v", err)
	}

	// Secret setting must be excluded
	if _, ok := settingsMap["scraper.comicvine.key"]; ok {
		t.Errorf("GetSettingsMap unexpectedly included secret row scraper.comicvine.key")
	}

	// 3. Propose slice done_when proof:
	// "A setting stored in system_settings is read when no flag/env sets it,
	// env still wins, and the effective config reports env_locked for pinned keys."

	t.Run("stored_values_win_over_defaults_with_proper_provenance_and_locks", func(t *testing.T) {
		cfg, err := config.Load(nil, config.WithEnviron(nil), config.WithoutEnvFile(), config.WithSystemSettings(settingsMap))
		if err != nil {
			t.Fatalf("config.Load failed: %v", err)
		}

		// Stored values win over compiled defaults
		if cfg.Port != 9090 {
			t.Errorf("Port = %d, want 9090 from DB", cfg.Port)
		}
		if cfg.LogLevel != "debug" {
			t.Errorf("LogLevel = %q, want 'debug' from DB", cfg.LogLevel)
		}
		if cfg.DataDir != "/srv/tobiten" {
			t.Errorf("DataDir = %q, want '/srv/tobiten' from DB", cfg.DataDir)
		}
		if cfg.ScannerWorkers != 4 {
			t.Errorf("ScannerWorkers = %d, want 4 from DB", cfg.ScannerWorkers)
		}
		if cfg.OIDCDefaultRole != "019318a0-0000-7000-8000-000000000003" {
			t.Errorf("OIDCDefaultRole = %q, want role id from DB", cfg.OIDCDefaultRole)
		}
		if cfg.OIDCDefaultLabels != "label-1,label-2" {
			t.Errorf("OIDCDefaultLabels = %q, want 'label-1,label-2' from DB", cfg.OIDCDefaultLabels)
		}

		// Rebased default paths derived from DB data root
		if cfg.CoversDir != "/srv/tobiten/covers" {
			t.Errorf("CoversDir = %q, want '/srv/tobiten/covers'", cfg.CoversDir)
		}
		if cfg.CacheDir != "/srv/tobiten/cache" {
			t.Errorf("CacheDir = %q, want '/srv/tobiten/cache'", cfg.CacheDir)
		}
		if cfg.PostgresDir != "/srv/tobiten/postgres" {
			t.Errorf("PostgresDir = %q, want '/srv/tobiten/postgres'", cfg.PostgresDir)
		}

		// Unmapped setting accessible via SystemSetting
		if val, ok := cfg.SystemSetting("events.retention_days"); !ok || val != "14" {
			t.Errorf("SystemSetting(events.retention_days) = %q, %v; want '14', true", val, ok)
		}

		// Provenance and lock reporting: unpinned DB values are not env_locked
		if cfg.SourceOf("TOBITEN_PORT") != config.SourceSystemSettings {
			t.Errorf("SourceOf(TOBITEN_PORT) = %v, want SourceSystemSettings", cfg.SourceOf("TOBITEN_PORT"))
		}
		if cfg.IsEnvLocked("TOBITEN_PORT") {
			t.Errorf("IsEnvLocked(TOBITEN_PORT) = true, want false")
		}
		if cfg.IsEnvLocked("port") {
			t.Errorf("IsEnvLocked(port) = true, want false")
		}
		if cfg.IsEnvLocked("server.port") {
			t.Errorf("IsEnvLocked(server.port) = true, want false")
		}

		if cfg.SourceOf("TOBITEN_DATA_DIR") != config.SourceSystemSettings {
			t.Errorf("SourceOf(TOBITEN_DATA_DIR) = %v, want SourceSystemSettings", cfg.SourceOf("TOBITEN_DATA_DIR"))
		}
		if cfg.IsEnvLocked("TOBITEN_DATA_DIR") {
			t.Errorf("IsEnvLocked(TOBITEN_DATA_DIR) = true, want false")
		}
		if cfg.IsEnvLocked("data.dir") {
			t.Errorf("IsEnvLocked(data.dir) = true, want false")
		}
		if cfg.IsEnvLocked("storage.data_dir") {
			t.Errorf("IsEnvLocked(storage.data_dir) = true, want false")
		}

		// Default-derived paths have SourceDefault and are not env_locked
		if cfg.SourceOf("TOBITEN_COVERS_DIR") != config.SourceDefault {
			t.Errorf("SourceOf(TOBITEN_COVERS_DIR) = %v, want SourceDefault", cfg.SourceOf("TOBITEN_COVERS_DIR"))
		}
		if cfg.IsEnvLocked("TOBITEN_COVERS_DIR") {
			t.Errorf("IsEnvLocked(TOBITEN_COVERS_DIR) = true, want false")
		}

		// EffectiveSettings report
		eff := cfg.EffectiveConfig().Settings
		portEff := eff["TOBITEN_PORT"]
		if portEff.Value != 9090 || portEff.Source != config.SourceSystemSettings || portEff.EnvLocked {
			t.Errorf("portEff = %+v, want Value=9090, Source=system_settings, EnvLocked=false", portEff)
		}
		dataEff := eff["TOBITEN_DATA_DIR"]
		if dataEff.Value != "/srv/tobiten" || dataEff.Source != config.SourceSystemSettings || dataEff.EnvLocked {
			t.Errorf("dataEff = %+v, want Value=/srv/tobiten, Source=system_settings, EnvLocked=false", dataEff)
		}
		eventsEff := eff["events.retention_days"]
		if eventsEff.Value != "14" || eventsEff.Source != config.SourceSystemSettings || eventsEff.EnvLocked {
			t.Errorf("eventsEff = %+v, want Value='14', Source=system_settings, EnvLocked=false", eventsEff)
		}

		// Setting lookup through aliases
		sPort, ok := cfg.Setting("server.port")
		if !ok || sPort.Value != 9090 || sPort.Source != config.SourceSystemSettings || sPort.EnvLocked {
			t.Errorf("Setting(server.port) = %+v, %v", sPort, ok)
		}
		sData, ok := cfg.Setting("data.dir")
		if !ok || sData.Value != "/srv/tobiten" || sData.Source != config.SourceSystemSettings || sData.EnvLocked {
			t.Errorf("Setting(data.dir) = %+v, %v", sData, ok)
		}
	})

	t.Run("env_and_flags_win_over_stored_values_and_report_env_locked", func(t *testing.T) {
		flags := []string{"--port=8080"}
		env := []string{
			"TOBITEN_DATA_DIR=/env/root",
			"TOBITEN_COVERS_DIR=/env/covers",
		}

		cfg, err := config.Load(flags, config.WithEnviron(env), config.WithoutEnvFile(), config.WithSystemSettings(settingsMap))
		if err != nil {
			t.Fatalf("config.Load failed: %v", err)
		}

		// 1. Flag wins over DB
		if cfg.Port != 8080 {
			t.Errorf("Port = %d, want 8080 from flag (wins over DB 9090)", cfg.Port)
		}
		if cfg.SourceOf("TOBITEN_PORT") != config.SourceFlag {
			t.Errorf("SourceOf(TOBITEN_PORT) = %v, want SourceFlag", cfg.SourceOf("TOBITEN_PORT"))
		}
		if !cfg.IsEnvLocked("TOBITEN_PORT") {
			t.Errorf("IsEnvLocked(TOBITEN_PORT) = false, want true")
		}
		if !cfg.IsEnvLocked("server.port") {
			t.Errorf("IsEnvLocked(server.port) = false, want true")
		}
		sPort, ok := cfg.Setting("server.port")
		if !ok || sPort.Value != 8080 || sPort.Source != config.SourceFlag || !sPort.EnvLocked {
			t.Errorf("Setting(server.port) = %+v, %v; want flag value locked", sPort, ok)
		}

		// 2. Env wins over DB
		if cfg.DataDir != "/env/root" {
			t.Errorf("DataDir = %q, want /env/root from env (wins over DB /srv/tobiten)", cfg.DataDir)
		}
		if cfg.SourceOf("TOBITEN_DATA_DIR") != config.SourceEnv {
			t.Errorf("SourceOf(TOBITEN_DATA_DIR) = %v, want SourceEnv", cfg.SourceOf("TOBITEN_DATA_DIR"))
		}
		if !cfg.IsEnvLocked("TOBITEN_DATA_DIR") {
			t.Errorf("IsEnvLocked(TOBITEN_DATA_DIR) = false, want true")
		}
		if !cfg.IsEnvLocked("data.dir") {
			t.Errorf("IsEnvLocked(data.dir) = false, want true")
		}
		sData, ok := cfg.Setting("data.dir")
		if !ok || sData.Value != "/env/root" || sData.Source != config.SourceEnv || !sData.EnvLocked {
			t.Errorf("Setting(data.dir) = %+v, %v; want env value locked", sData, ok)
		}

		// 3. Env covers dir preserved, cache dir rebased with env root
		if cfg.CoversDir != "/env/covers" {
			t.Errorf("CoversDir = %q, want /env/covers", cfg.CoversDir)
		}
		if cfg.SourceOf("TOBITEN_COVERS_DIR") != config.SourceEnv {
			t.Errorf("SourceOf(TOBITEN_COVERS_DIR) = %v, want SourceEnv", cfg.SourceOf("TOBITEN_COVERS_DIR"))
		}
		if !cfg.IsEnvLocked("TOBITEN_COVERS_DIR") {
			t.Errorf("IsEnvLocked(TOBITEN_COVERS_DIR) = false, want true")
		}
		if cfg.CacheDir != "/env/root/cache" {
			t.Errorf("CacheDir = %q, want /env/root/cache", cfg.CacheDir)
		}

		// 4. Stored unpinned settings still win over defaults
		if cfg.LogLevel != "debug" {
			t.Errorf("LogLevel = %q, want 'debug' from DB", cfg.LogLevel)
		}
		if cfg.SourceOf("TOBITEN_LOG_LEVEL") != config.SourceSystemSettings {
			t.Errorf("SourceOf(TOBITEN_LOG_LEVEL) = %v, want SourceSystemSettings", cfg.SourceOf("TOBITEN_LOG_LEVEL"))
		}
		if cfg.IsEnvLocked("TOBITEN_LOG_LEVEL") {
			t.Errorf("IsEnvLocked(TOBITEN_LOG_LEVEL) = true, want false")
		}
	})

	t.Run("ApplySystemSettings_equivalent_to_Load_with_stored_rows", func(t *testing.T) {
		cfgDefault, err := config.Load(nil, config.WithEnviron(nil), config.WithoutEnvFile())
		if err != nil {
			t.Fatalf("config.Load failed: %v", err)
		}

		if err := cfgDefault.ApplySystemSettings(settingsMap); err != nil {
			t.Fatalf("ApplySystemSettings failed: %v", err)
		}

		cfgLoaded, err := config.Load(nil, config.WithEnviron(nil), config.WithoutEnvFile(), config.WithSystemSettings(settingsMap))
		if err != nil {
			t.Fatalf("config.Load(WithSystemSettings) failed: %v", err)
		}

		if cfgDefault.DataDir != cfgLoaded.DataDir {
			t.Errorf("DataDir mismatch: applied=%q, loaded=%q", cfgDefault.DataDir, cfgLoaded.DataDir)
		}
		if cfgDefault.CoversDir != cfgLoaded.CoversDir {
			t.Errorf("CoversDir mismatch: applied=%q, loaded=%q", cfgDefault.CoversDir, cfgLoaded.CoversDir)
		}
		if cfgDefault.CacheDir != cfgLoaded.CacheDir {
			t.Errorf("CacheDir mismatch: applied=%q, loaded=%q", cfgDefault.CacheDir, cfgLoaded.CacheDir)
		}
		if cfgDefault.Port != cfgLoaded.Port {
			t.Errorf("Port mismatch: applied=%d, loaded=%d", cfgDefault.Port, cfgLoaded.Port)
		}
		if cfgDefault.LogLevel != cfgLoaded.LogLevel {
			t.Errorf("LogLevel mismatch: applied=%q, loaded=%q", cfgDefault.LogLevel, cfgLoaded.LogLevel)
		}

		effApplied := cfgDefault.EffectiveSettings()
		effLoaded := cfgLoaded.EffectiveSettings()
		for k, sApplied := range effApplied {
			sLoaded, ok := effLoaded[k]
			if !ok {
				t.Errorf("key %q present in applied but not loaded", k)
				continue
			}
			if sApplied != sLoaded {
				t.Errorf("EffectiveSetting mismatch for %q: applied=%+v, loaded=%+v", k, sApplied, sLoaded)
			}
		}
	})
}
