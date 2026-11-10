package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type precedenceTestCase struct {
	envKey      string
	flagName    string
	defaultVal  any
	envFileVal  string
	envVal      string
	flagVal     string
	expectedEnv any
	expectedFlg any
	getVal      func(c *Config) any
	// extraEnv or extraFlags needed to make semantic validation pass (e.g. for DATABASE_URL or STORAGE_MODE)
	extraEnv   []string
	extraFlags []string
}

func TestPrecedence_TableDriven(t *testing.T) {
	testCases := []precedenceTestCase{
		{
			envKey:      "TOBITEN_HOST",
			flagName:    "host",
			defaultVal:  "0.0.0.0",
			envFileVal:  "127.0.0.1",
			envVal:      "10.0.0.1",
			flagVal:     "192.168.1.1",
			expectedEnv: "10.0.0.1",
			expectedFlg: "192.168.1.1",
			getVal:      func(c *Config) any { return c.Host },
		},
		{
			envKey:      "TOBITEN_PORT",
			flagName:    "port",
			defaultVal:  8080,
			envFileVal:  "9000",
			envVal:      "9001",
			flagVal:     "9002",
			expectedEnv: 9001,
			expectedFlg: 9002,
			getVal:      func(c *Config) any { return c.Port },
		},
		{
			envKey:      "TOBITEN_BASE_URL",
			flagName:    "base-url",
			defaultVal:  "http://localhost:8080",
			envFileVal:  "http://books.local:8080",
			envVal:      "https://books.example.com",
			flagVal:     "https://books.example.com/tobiten",
			expectedEnv: "https://books.example.com",
			expectedFlg: "https://books.example.com/tobiten",
			getVal:      func(c *Config) any { return c.BaseURL },
		},
		{
			envKey:      "TOBITEN_DATA_DIR",
			flagName:    "data-dir",
			defaultVal:  "./data",
			envFileVal:  "/var/lib/tobiten-file",
			envVal:      "/var/lib/tobiten-env",
			flagVal:     "/var/lib/tobiten-flag",
			expectedEnv: "/var/lib/tobiten-env",
			expectedFlg: "/var/lib/tobiten-flag",
			getVal:      func(c *Config) any { return c.DataDir },
		},
		{
			envKey:      "TOBITEN_COVERS_DIR",
			flagName:    "covers-dir",
			defaultVal:  filepath.Join(DefaultDataDir, "covers"),
			envFileVal:  "/mnt/covers-file",
			envVal:      "/mnt/covers-env",
			flagVal:     "/mnt/covers-flag",
			expectedEnv: "/mnt/covers-env",
			expectedFlg: "/mnt/covers-flag",
			getVal:      func(c *Config) any { return c.CoversDir },
		},
		{
			envKey:      "TOBITEN_CACHE_DIR",
			flagName:    "cache-dir",
			defaultVal:  filepath.Join(DefaultDataDir, "cache"),
			envFileVal:  "/tmp/cache-file",
			envVal:      "/tmp/cache-env",
			flagVal:     "/tmp/cache-flag",
			expectedEnv: "/tmp/cache-env",
			expectedFlg: "/tmp/cache-flag",
			getVal:      func(c *Config) any { return c.CacheDir },
		},
		{
			envKey:      "TOBITEN_THUMB_CACHE_MAX_MB",
			flagName:    "thumb-cache-max-mb",
			defaultVal:  2048,
			envFileVal:  "1024",
			envVal:      "512",
			flagVal:     "256",
			expectedEnv: 512,
			expectedFlg: 256,
			getVal:      func(c *Config) any { return c.ThumbCacheMaxMB },
		},
		{
			envKey:      "TOBITEN_EXTRACT_CACHE_MAX_MB",
			flagName:    "extract-cache-max-mb",
			defaultVal:  4096,
			envFileVal:  "2048",
			envVal:      "1024",
			flagVal:     "512",
			expectedEnv: 1024,
			expectedFlg: 512,
			getVal:      func(c *Config) any { return c.ExtractCacheMaxMB },
		},
		{
			envKey:      "TOBITEN_ADMIN_USER",
			flagName:    "admin-user",
			defaultVal:  "",
			envFileVal:  "admin_file",
			envVal:      "admin_env",
			flagVal:     "admin_flag",
			expectedEnv: "admin_env",
			expectedFlg: "admin_flag",
			getVal:      func(c *Config) any { return c.AdminUser },
		},
		{
			envKey:      "TOBITEN_ADMIN_PASS",
			flagName:    "admin-pass",
			defaultVal:  "",
			envFileVal:  "pass_file",
			envVal:      "pass_env",
			flagVal:     "pass_flag",
			expectedEnv: "pass_env",
			expectedFlg: "pass_flag",
			getVal:      func(c *Config) any { return c.AdminPass },
		},
		{
			envKey:      "TOBITEN_SECRET_KEY",
			flagName:    "secret-key",
			defaultVal:  "",
			envFileVal:  "secret_file",
			envVal:      "secret_env",
			flagVal:     "secret_flag",
			expectedEnv: "secret_env",
			expectedFlg: "secret_flag",
			getVal:      func(c *Config) any { return c.SecretKey },
		},
		{
			envKey:      "TOBITEN_TRUSTED_PROXIES",
			flagName:    "trusted-proxies",
			defaultVal:  "",
			envFileVal:  "10.0.0.0/8",
			envVal:      "172.16.0.0/12",
			flagVal:     "192.168.0.0/16",
			expectedEnv: "172.16.0.0/12",
			expectedFlg: "192.168.0.0/16",
			getVal:      func(c *Config) any { return c.TrustedProxies },
		},
		{
			envKey:      "TOBITEN_OIDC_DEFAULT_ROLE",
			flagName:    "oidc-default-role",
			defaultVal:  "",
			envFileVal:  "Reader",
			envVal:      "Curator",
			flagVal:     "Admin",
			expectedEnv: "Curator",
			expectedFlg: "Admin",
			getVal:      func(c *Config) any { return c.OIDCDefaultRole },
		},
		{
			envKey:      "TOBITEN_OIDC_DEFAULT_LABELS",
			flagName:    "oidc-default-labels",
			defaultVal:  "",
			envFileVal:  "label1",
			envVal:      "label2",
			flagVal:     "label3",
			expectedEnv: "label2",
			expectedFlg: "label3",
			getVal:      func(c *Config) any { return c.OIDCDefaultLabels },
		},
		{
			envKey:      "TOBITEN_API_KEY",
			flagName:    "api-key",
			defaultVal:  "",
			envFileVal:  "key_file",
			envVal:      "key_env",
			flagVal:     "key_flag",
			expectedEnv: "key_env",
			expectedFlg: "key_flag",
			getVal:      func(c *Config) any { return c.APIKey },
		},
		{
			envKey:      "TOBITEN_LOG_LEVEL",
			flagName:    "log-level",
			defaultVal:  "info",
			envFileVal:  "debug",
			envVal:      "warn",
			flagVal:     "error",
			expectedEnv: "warn",
			expectedFlg: "error",
			getVal:      func(c *Config) any { return c.LogLevel },
		},
		{
			envKey:      "TOBITEN_LOG_FORMAT",
			flagName:    "log-format",
			defaultVal:  "json",
			envFileVal:  "text",
			envVal:      "table",
			flagVal:     "text",
			expectedEnv: "table",
			expectedFlg: "text",
			getVal:      func(c *Config) any { return c.LogFormat },
		},
		{
			envKey:      "TOBITEN_STORAGE_MODE",
			flagName:    "storage-mode",
			defaultVal:  "local",
			envFileVal:  "postgres",
			envVal:      "postgres",
			flagVal:     "postgres",
			expectedEnv: "postgres",
			expectedFlg: "postgres",
			getVal:      func(c *Config) any { return c.StorageMode },
			extraEnv:    []string{"TOBITEN_DATABASE_URL=postgres://localhost:5432/tobiten"},
			extraFlags:  []string{"-database-url=postgres://localhost:5432/tobiten"},
		},
		{
			envKey:      "TOBITEN_DATABASE_URL",
			flagName:    "database-url",
			defaultVal:  "",
			envFileVal:  "postgres://localhost:5432/file_db",
			envVal:      "postgres://localhost:5432/env_db",
			flagVal:     "postgres://localhost:5432/flag_db",
			expectedEnv: "postgres://localhost:5432/env_db",
			expectedFlg: "postgres://localhost:5432/flag_db",
			getVal:      func(c *Config) any { return c.DatabaseURL },
			extraEnv:    []string{"TOBITEN_STORAGE_MODE=postgres"},
			extraFlags:  []string{"-storage-mode=postgres"},
		},
		{
			envKey:      "TOBITEN_PG_BIN",
			flagName:    "pg-bin",
			defaultVal:  "",
			envFileVal:  "/usr/lib/postgresql/18/bin",
			envVal:      "/opt/local/lib/postgresql18/bin",
			flagVal:     "/custom/pg/bin",
			expectedEnv: "/opt/local/lib/postgresql18/bin",
			expectedFlg: "/custom/pg/bin",
			getVal:      func(c *Config) any { return c.PgBin },
		},
		{
			envKey:      "TOBITEN_LIBRARY_MODE",
			flagName:    "library-mode",
			defaultVal:  "virtual",
			envFileVal:  "managed",
			envVal:      "virtual",
			flagVal:     "managed",
			expectedEnv: "virtual",
			expectedFlg: "managed",
			getVal:      func(c *Config) any { return c.LibraryMode },
		},
		{
			envKey:      "TOBITEN_SCANNER_WORKERS",
			flagName:    "scanner-workers",
			defaultVal:  0,
			envFileVal:  "2",
			envVal:      "4",
			flagVal:     "8",
			expectedEnv: 4,
			expectedFlg: 8,
			getVal:      func(c *Config) any { return c.ScannerWorkers },
		},
		{
			envKey:      "TOBITEN_INTAKE_MODE",
			flagName:    "intake-mode",
			defaultVal:  "hardlink",
			envFileVal:  "copy",
			envVal:      "move",
			flagVal:     "hardlink",
			expectedEnv: "move",
			expectedFlg: "hardlink",
			getVal:      func(c *Config) any { return c.IntakeMode },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.envKey, func(t *testing.T) {
			// 1. Default precedence: when no flags, no env, no env_file
			t.Run("default", func(t *testing.T) {
				var extraEnv []string
				var extraFlags []string
				if tc.envKey != "TOBITEN_STORAGE_MODE" && tc.envKey != "TOBITEN_DATABASE_URL" {
					extraEnv = tc.extraEnv
					extraFlags = tc.extraFlags
				}

				cfg, err := Load(extraFlags, WithEnviron(extraEnv), WithoutEnvFile())
				if err != nil {
					t.Fatalf("Load() unexpected error: %v", err)
				}
				if got := tc.getVal(cfg); got != tc.defaultVal {
					t.Errorf("%s default = %v, want %v", tc.envKey, got, tc.defaultVal)
				}
				if src := cfg.SourceOf(tc.envKey); src != SourceDefault {
					t.Errorf("%s SourceOf() = %v, want %v", tc.envKey, src, SourceDefault)
				}
				if tc.envKey != "TOBITEN_COVERS_DIR" && tc.envKey != "TOBITEN_CACHE_DIR" {
					if cfg.IsEnvLocked(tc.envKey) {
						t.Errorf("%s IsEnvLocked() = true, want false for default", tc.envKey)
					}
				}
			})

			// 2. .env file overrides compiled default
			t.Run("env_file_overrides_default", func(t *testing.T) {
				envFileContent := fmt.Sprintf("%s=%s\n", tc.envKey, tc.envFileVal)
				for _, extra := range tc.extraEnv {
					envFileContent += extra + "\n"
				}

				cfg, err := Load(nil, WithEnviron(nil), WithEnvFileReader(strings.NewReader(envFileContent)))
				if err != nil {
					t.Fatalf("Load() unexpected error: %v", err)
				}
				if src := cfg.SourceOf(tc.envKey); src != SourceEnvFile {
					t.Errorf("%s SourceOf() = %v, want %v", tc.envKey, src, SourceEnvFile)
				}
				if !cfg.IsEnvLocked(tc.envKey) {
					t.Errorf("%s IsEnvLocked() = false, want true for env_file", tc.envKey)
				}
			})

			// 3. OS environment overrides .env file and compiled default
			t.Run("env_overrides_env_file", func(t *testing.T) {
				envFileContent := fmt.Sprintf("%s=%s\n", tc.envKey, tc.envFileVal)
				envList := append([]string{fmt.Sprintf("%s=%s", tc.envKey, tc.envVal)}, tc.extraEnv...)

				cfg, err := Load(nil, WithEnviron(envList), WithEnvFileReader(strings.NewReader(envFileContent)))
				if err != nil {
					t.Fatalf("Load() unexpected error: %v", err)
				}
				if got := tc.getVal(cfg); got != tc.expectedEnv {
					t.Errorf("%s env = %v, want %v", tc.envKey, got, tc.expectedEnv)
				}
				if src := cfg.SourceOf(tc.envKey); src != SourceEnv {
					t.Errorf("%s SourceOf() = %v, want %v", tc.envKey, src, SourceEnv)
				}
				if !cfg.IsEnvLocked(tc.envKey) {
					t.Errorf("%s IsEnvLocked() = false, want true for env", tc.envKey)
				}
			})

			// 4. CLI flag overrides OS environment, .env file, and compiled default
			t.Run("flag_overrides_all", func(t *testing.T) {
				envFileContent := fmt.Sprintf("%s=%s\n", tc.envKey, tc.envFileVal)
				envList := append([]string{fmt.Sprintf("%s=%s", tc.envKey, tc.envVal)}, tc.extraEnv...)
				flagList := append([]string{fmt.Sprintf("-%s=%s", tc.flagName, tc.flagVal)}, tc.extraFlags...)

				cfg, err := Load(flagList, WithEnviron(envList), WithEnvFileReader(strings.NewReader(envFileContent)))
				if err != nil {
					t.Fatalf("Load() unexpected error: %v", err)
				}
				if got := tc.getVal(cfg); got != tc.expectedFlg {
					t.Errorf("%s flag = %v, want %v", tc.envKey, got, tc.expectedFlg)
				}
				if src := cfg.SourceOf(tc.envKey); src != SourceFlag {
					t.Errorf("%s SourceOf() = %v, want %v", tc.envKey, src, SourceFlag)
				}
				if !cfg.IsEnvLocked(tc.envKey) {
					t.Errorf("%s IsEnvLocked() = false, want true for flag", tc.envKey)
				}
			})
		})
	}
}

func TestDocumentedKeysCoverage(t *testing.T) {
	// Parse .env.example from repository root
	bytes, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatalf("failed to read .env.example: %v", err)
	}

	envVars, err := parseEnv(strings.NewReader(string(bytes)))
	if err != nil {
		t.Fatalf("failed to parse .env.example: %v", err)
	}

	keyMap := make(map[string]bool)
	for _, k := range DocumentedEnvKeys {
		keyMap[k] = true
	}

	for k := range envVars {
		if !strings.HasPrefix(k, "TOBITEN_") {
			continue
		}
		if !keyMap[k] {
			t.Errorf(".env.example contains %q which is missing from DocumentedEnvKeys", k)
		}
	}

	if len(DocumentedEnvKeys) != 23 {
		t.Errorf("DocumentedEnvKeys length = %d, want 23", len(DocumentedEnvKeys))
	}
}

func TestDataDirDerivedPaths(t *testing.T) {
	t.Run("default data directory", func(t *testing.T) {
		cfg, err := Load(nil, WithEnviron(nil), WithoutEnvFile())
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}

		if cfg.DataDir != "./data" {
			t.Errorf("DataDir = %q, want %q", cfg.DataDir, "./data")
		}
		wantPostgres := filepath.Join(cfg.DataDir, "postgres")
		if cfg.PostgresDir != wantPostgres {
			t.Errorf("PostgresDir = %q, want %q", cfg.PostgresDir, wantPostgres)
		}
		wantBackups := filepath.Join(cfg.DataDir, "backups")
		if cfg.BackupsDir != wantBackups {
			t.Errorf("BackupsDir = %q, want %q", cfg.BackupsDir, wantBackups)
		}
		wantSecretKey := filepath.Join(cfg.DataDir, "secret.key")
		if cfg.SecretKeyPath != wantSecretKey {
			t.Errorf("SecretKeyPath = %q, want %q", cfg.SecretKeyPath, wantSecretKey)
		}
		wantCovers := filepath.Join(cfg.DataDir, "covers")
		if cfg.CoversDir != wantCovers {
			t.Errorf("CoversDir = %q, want %q", cfg.CoversDir, wantCovers)
		}
		wantCache := filepath.Join(cfg.DataDir, "cache")
		if cfg.CacheDir != wantCache {
			t.Errorf("CacheDir = %q, want %q", cfg.CacheDir, wantCache)
		}
		wantThumbs := filepath.Join(cfg.CacheDir, "thumbs")
		if cfg.ThumbsCacheDir() != wantThumbs {
			t.Errorf("ThumbsCacheDir() = %q, want %q", cfg.ThumbsCacheDir(), wantThumbs)
		}
		wantRepack := filepath.Join(cfg.CacheDir, "repack")
		if cfg.RepackCacheDir() != wantRepack {
			t.Errorf("RepackCacheDir() = %q, want %q", cfg.RepackCacheDir(), wantRepack)
		}
		wantExtract := filepath.Join(cfg.CacheDir, "extract")
		if cfg.ExtractCacheDir() != wantExtract {
			t.Errorf("ExtractCacheDir() = %q, want %q", cfg.ExtractCacheDir(), wantExtract)
		}
	})

	t.Run("custom data directory derives all paths", func(t *testing.T) {
		cfg, err := Load([]string{"-data-dir=/srv/tobiten"}, WithEnviron(nil), WithoutEnvFile())
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}

		if cfg.DataDir != "/srv/tobiten" {
			t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/srv/tobiten")
		}
		if cfg.PostgresDir != "/srv/tobiten/postgres" {
			t.Errorf("PostgresDir = %q, want %q", cfg.PostgresDir, "/srv/tobiten/postgres")
		}
		if cfg.BackupsDir != "/srv/tobiten/backups" {
			t.Errorf("BackupsDir = %q, want %q", cfg.BackupsDir, "/srv/tobiten/backups")
		}
		if cfg.SecretKeyPath != "/srv/tobiten/secret.key" {
			t.Errorf("SecretKeyPath = %q, want %q", cfg.SecretKeyPath, "/srv/tobiten/secret.key")
		}
		if cfg.CoversDir != "/srv/tobiten/covers" {
			t.Errorf("CoversDir = %q, want %q", cfg.CoversDir, "/srv/tobiten/covers")
		}
		if cfg.CacheDir != "/srv/tobiten/cache" {
			t.Errorf("CacheDir = %q, want %q", cfg.CacheDir, "/srv/tobiten/cache")
		}
	})

	t.Run("explicit covers and cache dir override derived defaults", func(t *testing.T) {
		cfg, err := Load([]string{
			"-data-dir=/srv/tobiten",
			"-covers-dir=/fast-nvme/covers",
			"-cache-dir=/scratch/cache",
		}, WithEnviron(nil), WithoutEnvFile())
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}

		if cfg.CoversDir != "/fast-nvme/covers" {
			t.Errorf("CoversDir = %q, want %q", cfg.CoversDir, "/fast-nvme/covers")
		}
		if cfg.CacheDir != "/scratch/cache" {
			t.Errorf("CacheDir = %q, want %q", cfg.CacheDir, "/scratch/cache")
		}
		if cfg.ThumbsCacheDir() != "/scratch/cache/thumbs" {
			t.Errorf("ThumbsCacheDir() = %q, want %q", cfg.ThumbsCacheDir(), "/scratch/cache/thumbs")
		}
		// Postgres and backup directories remain derived from DataDir
		if cfg.PostgresDir != "/srv/tobiten/postgres" {
			t.Errorf("PostgresDir = %q, want %q", cfg.PostgresDir, "/srv/tobiten/postgres")
		}
		if cfg.BackupsDir != "/srv/tobiten/backups" {
			t.Errorf("BackupsDir = %q, want %q", cfg.BackupsDir, "/srv/tobiten/backups")
		}
	})
}

func TestStorageModeValidation(t *testing.T) {
	t.Run("local mode with database URL refuses startup", func(t *testing.T) {
		cfg, err := Load([]string{
			"-storage-mode=local",
			"-database-url=postgres://user:pass@localhost:5432/tobiten",
		}, WithEnviron(nil), WithoutEnvFile())

		if err == nil {
			t.Fatalf("expected error, got nil (cfg = %+v)", cfg)
		}
		if !errors.Is(err, ErrLocalStorageConflict) {
			t.Errorf("error = %v, want ErrLocalStorageConflict", err)
		}
		if !strings.Contains(err.Error(), "storage mode 'local' requires an empty TOBITEN_DATABASE_URL") {
			t.Errorf("error message = %q, want documented message", err.Error())
		}
	})

	t.Run("postgres mode without database URL refuses startup", func(t *testing.T) {
		cfg, err := Load([]string{
			"-storage-mode=postgres",
			"-database-url=",
		}, WithEnviron(nil), WithoutEnvFile())

		if err == nil {
			t.Fatalf("expected error, got nil (cfg = %+v)", cfg)
		}
		if !errors.Is(err, ErrPostgresStorageConflict) {
			t.Errorf("error = %v, want ErrPostgresStorageConflict", err)
		}
		if !strings.Contains(err.Error(), "storage mode 'postgres' requires a non-empty TOBITEN_DATABASE_URL") {
			t.Errorf("error message = %q, want documented message", err.Error())
		}
	})

	t.Run("local mode with empty database URL succeeds", func(t *testing.T) {
		cfg, err := Load([]string{
			"-storage-mode=local",
			"-database-url=",
		}, WithEnviron(nil), WithoutEnvFile())

		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}
		if cfg.StorageMode != "local" {
			t.Errorf("StorageMode = %q, want local", cfg.StorageMode)
		}
	})

	t.Run("postgres mode with database URL succeeds", func(t *testing.T) {
		cfg, err := Load([]string{
			"-storage-mode=postgres",
			"-database-url=postgres://user:pass@localhost:5432/tobiten",
		}, WithEnviron(nil), WithoutEnvFile())

		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}
		if cfg.StorageMode != "postgres" {
			t.Errorf("StorageMode = %q, want postgres", cfg.StorageMode)
		}
	})

	t.Run("invalid storage mode refuses startup", func(t *testing.T) {
		invalidModes := []string{"sqlite", "mysql", "unknown"}
		for _, m := range invalidModes {
			_, err := Load([]string{"-storage-mode=" + m}, WithEnviron(nil), WithoutEnvFile())
			if err == nil {
				t.Fatalf("storage-mode=%q expected error, got nil", m)
			}
			if !errors.Is(err, ErrInvalidStorageMode) {
				t.Errorf("storage-mode=%q error = %v, want ErrInvalidStorageMode", m, err)
			}
		}
	})
}

func TestOtherValidation(t *testing.T) {
	validPostgresArgs := []string{
		"-storage-mode=postgres",
		"-database-url=postgres://user:pass@localhost:5432/tobiten",
	}

	t.Run("invalid port numbers", func(t *testing.T) {
		invalidPorts := []string{"-port=0", "-port=-1", "-port=65536"}
		for _, p := range invalidPorts {
			args := append([]string{p}, validPostgresArgs...)
			_, err := Load(args, WithEnviron(nil), WithoutEnvFile())
			if err == nil || !errors.Is(err, ErrInvalidPort) {
				t.Errorf("expected ErrInvalidPort for %s, got %v", p, err)
			}
		}
	})

	t.Run("invalid library mode", func(t *testing.T) {
		_, err := Load([]string{"-library-mode=invalid"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil || !errors.Is(err, ErrInvalidLibraryMode) {
			t.Errorf("expected ErrInvalidLibraryMode, got %v", err)
		}
	})

	t.Run("invalid intake mode", func(t *testing.T) {
		_, err := Load([]string{"-intake-mode=invalid"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil || !errors.Is(err, ErrInvalidIntakeMode) {
			t.Errorf("expected ErrInvalidIntakeMode, got %v", err)
		}
	})

	t.Run("invalid log level", func(t *testing.T) {
		_, err := Load([]string{"-log-level=verbose"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil || !errors.Is(err, ErrInvalidLogLevel) {
			t.Errorf("expected ErrInvalidLogLevel, got %v", err)
		}
	})

	t.Run("invalid log format", func(t *testing.T) {
		_, err := Load([]string{"-log-format=xml"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil || !errors.Is(err, ErrInvalidLogFormat) {
			t.Errorf("expected ErrInvalidLogFormat, got %v", err)
		}
	})

	t.Run("invalid base URL", func(t *testing.T) {
		invalidURLs := []string{"ftp://localhost", "not-a-url", "http://"}
		for _, u := range invalidURLs {
			_, err := Load([]string{"-base-url=" + u}, WithEnviron(nil), WithoutEnvFile())
			if err == nil || !errors.Is(err, ErrInvalidBaseURL) {
				t.Errorf("expected ErrInvalidBaseURL for %q, got %v", u, err)
			}
		}
	})

	t.Run("negative cache and worker budgets", func(t *testing.T) {
		_, err := Load([]string{"-thumb-cache-max-mb=-1"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil {
			t.Error("expected error for negative thumb-cache-max-mb")
		}

		_, err = Load([]string{"-extract-cache-max-mb=-1"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil {
			t.Error("expected error for negative extract-cache-max-mb")
		}

		_, err = Load([]string{"-scanner-workers=-1"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil {
			t.Error("expected error for negative scanner-workers")
		}
	})

	t.Run("non-integer env var values", func(t *testing.T) {
		_, err := Load(nil, WithEnviron([]string{"TOBITEN_PORT=abc"}), WithoutEnvFile())
		if err == nil {
			t.Error("expected error for non-integer TOBITEN_PORT")
		}
	})
}

func TestEnvFileParser(t *testing.T) {
	content := `
# Full-line comment
TOBITEN_HOST=127.0.0.1
export TOBITEN_PORT=9090
TOBITEN_BASE_URL="http://quoted.example.com"
TOBITEN_DATA_DIR='/single/quoted/dir'
TOBITEN_LOG_LEVEL=debug # inline comment
`
	parsed, err := parseEnv(strings.NewReader(content))
	if err != nil {
		t.Fatalf("parseEnv() error: %v", err)
	}

	if parsed["TOBITEN_HOST"] != "127.0.0.1" {
		t.Errorf("TOBITEN_HOST = %q, want 127.0.0.1", parsed["TOBITEN_HOST"])
	}
	if parsed["TOBITEN_PORT"] != "9090" {
		t.Errorf("TOBITEN_PORT = %q, want 9090", parsed["TOBITEN_PORT"])
	}
	if parsed["TOBITEN_BASE_URL"] != "http://quoted.example.com" {
		t.Errorf("TOBITEN_BASE_URL = %q, want http://quoted.example.com", parsed["TOBITEN_BASE_URL"])
	}
	if parsed["TOBITEN_DATA_DIR"] != "/single/quoted/dir" {
		t.Errorf("TOBITEN_DATA_DIR = %q, want /single/quoted/dir", parsed["TOBITEN_DATA_DIR"])
	}
	if parsed["TOBITEN_LOG_LEVEL"] != "debug" {
		t.Errorf("TOBITEN_LOG_LEVEL = %q, want debug", parsed["TOBITEN_LOG_LEVEL"])
	}
}
