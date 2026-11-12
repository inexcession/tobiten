package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type precedenceTestCase struct {
	envKey      string
	flagName    string // empty if no CLI flag exists (I-7: secrets)
	defaultVal  any
	envFileVal  string
	envVal      string
	flagVal     string
	expectedEnv any
	expectedFlg any
	getVal      func(c *Config) any
	extraEnv    []string
	extraFlags  []string
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
			flagName:    "", // I-7: secret, no CLI flag
			defaultVal:  "",
			envFileVal:  "pass_file",
			envVal:      "pass_env",
			flagVal:     "",
			expectedEnv: "pass_env",
			expectedFlg: "",
			getVal:      func(c *Config) any { return c.AdminPass },
		},
		{
			envKey:      "TOBITEN_SECRET_KEY",
			flagName:    "", // I-7: secret, no CLI flag
			defaultVal:  "",
			envFileVal:  "secret_file",
			envVal:      "secret_env",
			flagVal:     "",
			expectedEnv: "secret_env",
			expectedFlg: "",
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
			extraFlags:  []string{},
		},
		{
			envKey:      "TOBITEN_DATABASE_URL",
			flagName:    "", // I-7: secret, no CLI flag
			defaultVal:  "",
			envFileVal:  "postgres://localhost:5432/file_db",
			envVal:      "postgres://localhost:5432/env_db",
			flagVal:     "",
			expectedEnv: "postgres://localhost:5432/env_db",
			expectedFlg: "",
			getVal:      func(c *Config) any { return c.DatabaseURL },
			extraEnv:    []string{"TOBITEN_STORAGE_MODE=postgres"},
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

			// 4. CLI flag overrides OS environment, .env file, and compiled default (if flag exists)
			if tc.flagName != "" {
				t.Run("flag_overrides_all", func(t *testing.T) {
					envFileContent := fmt.Sprintf("%s=%s\n", tc.envKey, tc.envFileVal)
					envList := append([]string{fmt.Sprintf("%s=%s", tc.envKey, tc.envVal)}, tc.extraEnv...)
					flagList := append([]string{fmt.Sprintf("--%s=%s", tc.flagName, tc.flagVal)}, tc.extraFlags...)

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
			} else {
				// I-7: Assert that secret keys cannot be passed via CLI flag
				t.Run("no_cli_flag_allowed", func(t *testing.T) {
					for _, prefix := range []string{"--", "-"} {
						flagArg := fmt.Sprintf("%s%s=secret", prefix, strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(tc.envKey, "TOBITEN_"), "_", "-")))
						_, err := Load([]string{flagArg}, WithEnviron(nil), WithoutEnvFile())
						if err == nil || !errors.Is(err, ErrInvalidFlag) {
							t.Errorf("expected ErrInvalidFlag when passing %s, got %v", flagArg, err)
						}
					}
				})
			}
		})
	}
}

func TestBlankValuesUnset(t *testing.T) {
	// I-6: A TOBITEN_* key that is empty or whitespace-only in env or .env
	// is treated exactly as absent: falls through to default, source is default, IsEnvLocked is false.

	t.Run("blank TOBITEN_DATA_DIR in env gives ./data and ./data/postgres", func(t *testing.T) {
		cfg, err := Load(nil, WithEnviron([]string{"TOBITEN_DATA_DIR=   "}), WithoutEnvFile())
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.DataDir != "./data" {
			t.Errorf("DataDir = %q, want ./data", cfg.DataDir)
		}
		wantPG := filepath.Join("./data", "postgres")
		if cfg.PostgresDir != wantPG {
			t.Errorf("PostgresDir = %q, want %q", cfg.PostgresDir, wantPG)
		}
		if cfg.SourceOf("TOBITEN_DATA_DIR") != SourceDefault {
			t.Errorf("SourceOf(TOBITEN_DATA_DIR) = %v, want SourceDefault", cfg.SourceOf("TOBITEN_DATA_DIR"))
		}
		if cfg.IsEnvLocked("TOBITEN_DATA_DIR") {
			t.Errorf("IsEnvLocked(TOBITEN_DATA_DIR) = true, want false for blank env")
		}
	})

	t.Run("blank integer in env falls through to default", func(t *testing.T) {
		cfg, err := Load(nil, WithEnviron([]string{"TOBITEN_PORT="}), WithoutEnvFile())
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.Port != 8080 {
			t.Errorf("Port = %d, want 8080", cfg.Port)
		}
		if cfg.IsEnvLocked("TOBITEN_PORT") {
			t.Errorf("IsEnvLocked(TOBITEN_PORT) = true, want false for blank env")
		}
	})

	t.Run("loading .env.example locks only its non-blank keys", func(t *testing.T) {
		bytes, err := os.ReadFile("../../.env.example")
		if err != nil {
			t.Fatalf("failed to read .env.example: %v", err)
		}

		cfg, err := Load(nil, WithEnviron(nil), WithEnvFileReader(strings.NewReader(string(bytes))))
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}

		// Non-blank keys in .env.example MUST be locked
		nonBlankKeys := []string{
			"TOBITEN_HOST",
			"TOBITEN_PORT",
			"TOBITEN_BASE_URL",
			"TOBITEN_DATA_DIR",
			"TOBITEN_THUMB_CACHE_MAX_MB",
			"TOBITEN_EXTRACT_CACHE_MAX_MB",
			"TOBITEN_LOG_LEVEL",
			"TOBITEN_LOG_FORMAT",
			"TOBITEN_STORAGE_MODE",
			"TOBITEN_LIBRARY_MODE",
			"TOBITEN_INTAKE_MODE",
		}
		for _, k := range nonBlankKeys {
			if !cfg.IsEnvLocked(k) {
				t.Errorf(".env.example key %s is non-blank but IsEnvLocked() = false", k)
			}
			if src := cfg.SourceOf(k); src != SourceEnvFile {
				t.Errorf(".env.example key %s SourceOf() = %v, want SourceEnvFile", k, src)
			}
		}

		// Blank or commented-out keys in .env.example MUST NOT be locked
		blankKeys := []string{
			"TOBITEN_COVERS_DIR",
			"TOBITEN_CACHE_DIR",
			"TOBITEN_ADMIN_USER",
			"TOBITEN_ADMIN_PASS",
			"TOBITEN_SECRET_KEY",
			"TOBITEN_TRUSTED_PROXIES",
			"TOBITEN_OIDC_DEFAULT_ROLE",
			"TOBITEN_OIDC_DEFAULT_LABELS",
			"TOBITEN_DATABASE_URL",
			"TOBITEN_PG_BIN",
			"TOBITEN_SCANNER_WORKERS",
		}
		for _, k := range blankKeys {
			if cfg.IsEnvLocked(k) {
				t.Errorf(".env.example key %s is blank but IsEnvLocked() = true", k)
			}
			if src := cfg.SourceOf(k); src != SourceDefault {
				t.Errorf(".env.example key %s SourceOf() = %v, want SourceDefault", k, src)
			}
		}
	})
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

	daemonKeyMap := make(map[string]bool)
	for _, k := range DaemonEnvKeys {
		daemonKeyMap[k] = true
	}

	cliKeyMap := make(map[string]bool)
	for _, k := range CLIEnvKeys {
		cliKeyMap[k] = true
	}

	for k := range envVars {
		if !strings.HasPrefix(k, "TOBITEN_") {
			continue
		}
		isDaemon := daemonKeyMap[k]
		isCLI := cliKeyMap[k]
		if !isDaemon && !isCLI {
			t.Errorf(".env.example contains %q which is neither in DaemonEnvKeys nor in CLIEnvKeys", k)
		}
	}

	// Verify exact counts (22 daemon keys + 1 CLI key = 23 total)
	if len(DaemonEnvKeys) != 22 {
		t.Errorf("DaemonEnvKeys length = %d, want 22", len(DaemonEnvKeys))
	}
	if len(CLIEnvKeys) != 1 || CLIEnvKeys[0] != "TOBITEN_API_KEY" {
		t.Errorf("CLIEnvKeys = %v, want [TOBITEN_API_KEY]", CLIEnvKeys)
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
		cfg, err := Load([]string{"--data-dir=/srv/tobiten"}, WithEnviron(nil), WithoutEnvFile())
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
			"--data-dir=/srv/tobiten",
			"--covers-dir=/fast-nvme/covers",
			"--cache-dir=/scratch/cache",
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
	t.Run("local mode with database URL refuses startup without password leakage", func(t *testing.T) {
		secretURL := "postgres://user:super_secret_password@localhost:5432/tobiten"
		_, err := Load(nil, WithEnviron([]string{
			"TOBITEN_STORAGE_MODE=local",
			"TOBITEN_DATABASE_URL=" + secretURL,
		}), WithoutEnvFile())

		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !errors.Is(err, ErrLocalStorageConflict) {
			t.Errorf("error = %v, want ErrLocalStorageConflict", err)
		}
		errStr := err.Error()
		if !strings.Contains(errStr, "storage mode 'local' requires an empty TOBITEN_DATABASE_URL") {
			t.Errorf("error message = %q, want documented message", errStr)
		}
		// Item 3: Never echo TOBITEN_DATABASE_URL in errors
		if strings.Contains(errStr, "super_secret_password") || strings.Contains(errStr, "postgres://") {
			t.Errorf("error message leaked database URL/credentials: %q", errStr)
		}
	})

	t.Run("postgres mode without database URL refuses startup", func(t *testing.T) {
		_, err := Load(nil, WithEnviron([]string{
			"TOBITEN_STORAGE_MODE=postgres",
			"TOBITEN_DATABASE_URL=",
		}), WithoutEnvFile())

		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		if !errors.Is(err, ErrPostgresStorageConflict) {
			t.Errorf("error = %v, want ErrPostgresStorageConflict", err)
		}
		if !strings.Contains(err.Error(), "storage mode 'postgres' requires a non-empty TOBITEN_DATABASE_URL") {
			t.Errorf("error message = %q, want documented message", err.Error())
		}
	})

	t.Run("local mode with empty database URL succeeds", func(t *testing.T) {
		cfg, err := Load([]string{"--storage-mode=local"}, WithEnviron([]string{"TOBITEN_DATABASE_URL="}), WithoutEnvFile())
		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}
		if cfg.StorageMode != "local" {
			t.Errorf("StorageMode = %q, want local", cfg.StorageMode)
		}
	})

	t.Run("postgres mode with database URL succeeds", func(t *testing.T) {
		cfg, err := Load([]string{"--storage-mode=postgres"}, WithEnviron([]string{
			"TOBITEN_DATABASE_URL=postgres://user:pass@localhost:5432/tobiten",
		}), WithoutEnvFile())

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
			_, err := Load([]string{"--storage-mode=" + m}, WithEnviron(nil), WithoutEnvFile())
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
	validPostgresEnv := []string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=postgres://user:pass@localhost:5432/tobiten",
	}

	t.Run("invalid port numbers", func(t *testing.T) {
		invalidPorts := []string{"--port=0", "--port=-1", "--port=65536"}
		for _, p := range invalidPorts {
			_, err := Load([]string{p}, WithEnviron(validPostgresEnv), WithoutEnvFile())
			if err == nil || !errors.Is(err, ErrInvalidPort) {
				t.Errorf("expected ErrInvalidPort for %s, got %v", p, err)
			}
		}
	})

	t.Run("invalid library mode", func(t *testing.T) {
		_, err := Load([]string{"--library-mode=invalid"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil || !errors.Is(err, ErrInvalidLibraryMode) {
			t.Errorf("expected ErrInvalidLibraryMode, got %v", err)
		}
	})

	t.Run("invalid intake mode", func(t *testing.T) {
		_, err := Load([]string{"--intake-mode=invalid"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil || !errors.Is(err, ErrInvalidIntakeMode) {
			t.Errorf("expected ErrInvalidIntakeMode, got %v", err)
		}
	})

	t.Run("invalid log level", func(t *testing.T) {
		_, err := Load([]string{"--log-level=verbose"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil || !errors.Is(err, ErrInvalidLogLevel) {
			t.Errorf("expected ErrInvalidLogLevel, got %v", err)
		}
	})

	t.Run("invalid log format", func(t *testing.T) {
		_, err := Load([]string{"--log-format=xml"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil || !errors.Is(err, ErrInvalidLogFormat) {
			t.Errorf("expected ErrInvalidLogFormat, got %v", err)
		}
	})

	t.Run("invalid base URL", func(t *testing.T) {
		invalidURLs := []string{
			"ftp://localhost",
			"not-a-url",
			"http://",
			"http://localhost:8080?query=1",   // query rejected
			"http://localhost:8080#section",   // fragment rejected
			"http://user:pass@localhost:8080", // userinfo rejected
			"https://user@books.example.com",  // user rejected
		}
		for _, u := range invalidURLs {
			_, err := Load([]string{"--base-url=" + u}, WithEnviron(nil), WithoutEnvFile())
			if err == nil || !errors.Is(err, ErrInvalidBaseURL) {
				t.Errorf("expected ErrInvalidBaseURL for %q, got %v", u, err)
			}
		}
	})

	t.Run("invalid base URL with credentials redacts password in error", func(t *testing.T) {
		secretPass := "my_super_secret_password_987"
		rawURL := fmt.Sprintf("http://user:%s@localhost:8080/books", secretPass)
		_, err := Load([]string{"--base-url=" + rawURL}, WithEnviron(nil), WithoutEnvFile())
		if err == nil {
			t.Fatalf("expected error for base URL with credentials, got nil")
		}
		if !errors.Is(err, ErrInvalidBaseURL) {
			t.Errorf("error = %v, want ErrInvalidBaseURL", err)
		}
		errStr := err.Error()
		if strings.Contains(errStr, secretPass) {
			t.Errorf("error message leaked base URL password: %q", errStr)
		}
		if !strings.Contains(errStr, "http://user:xxxxx@localhost:8080/books") {
			t.Errorf("error message should contain redacted URL, got: %q", errStr)
		}
	})

	t.Run("negative cache and worker budgets", func(t *testing.T) {
		_, err := Load([]string{"--thumb-cache-max-mb=-1"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil {
			t.Error("expected error for negative thumb-cache-max-mb")
		}

		_, err = Load([]string{"--extract-cache-max-mb=-1"}, WithEnviron(nil), WithoutEnvFile())
		if err == nil {
			t.Error("expected error for negative extract-cache-max-mb")
		}

		_, err = Load([]string{"--scanner-workers=-1"}, WithEnviron(nil), WithoutEnvFile())
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

func TestEnvFileDoubleQuotedUnescape(t *testing.T) {
	// Item 4: unescape in a single left-to-right pass so \\n yields literal \n, not newline
	tests := []struct {
		input string
		want  string
	}{
		{
			input: `KEY="hello\nworld"`,
			want:  "hello\nworld",
		},
		{
			input: `KEY="hello\\nworld"`,
			want:  "hello\\nworld", // literal backslash + n
		},
		{
			input: `KEY="path\\to\\file"`,
			want:  "path\\to\\file",
		},
		{
			input: `KEY="tab\there"`,
			want:  "tab\there",
		},
		{
			input: `KEY="carriage\rreturn"`,
			want:  "carriage\rreturn",
		},
		{
			input: `KEY="with\"escaped\"quote"`,
			want:  `with"escaped"quote`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			parsed, err := parseEnv(strings.NewReader(tc.input))
			if err != nil {
				t.Fatalf("parseEnv() error: %v", err)
			}
			if got := parsed["KEY"]; got != tc.want {
				t.Errorf("parseEnv(%s) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestPrintUsage(t *testing.T) {
	var buf bytes.Buffer
	PrintUsage(&buf)
	out := buf.String()

	if !strings.Contains(out, "Usage of tobitend:\n") {
		t.Errorf("PrintUsage output missing header: %q", out)
	}

	// Canonical double-dash long options must be presented (ADR-0014 §2, I-8)
	expectedFlags := []string{
		"--admin-user",
		"--base-url",
		"--cache-dir",
		"--covers-dir",
		"--data-dir",
		"--env-file",
		"--extract-cache-max-mb",
		"--help",
		"--host",
		"--intake-mode",
		"--library-mode",
		"--log-format",
		"--log-level",
		"--oidc-default-labels",
		"--oidc-default-role",
		"--pg-bin",
		"--port",
		"--scanner-workers",
		"--storage-mode",
		"--thumb-cache-max-mb",
		"--trusted-proxies",
		"--version",
	}
	for _, flg := range expectedFlags {
		if !strings.Contains(out, flg) {
			t.Errorf("PrintUsage output missing canonical long option %q", flg)
		}
	}

	// -v alias must be omitted from canonical help
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "-v\t") || strings.HasPrefix(trimmed, "-v ") || trimmed == "-v" {
			t.Errorf("PrintUsage output contains omitted alias -v: %q", line)
		}
	}

	// Secret keys and client API key must NOT have CLI flags (I-7)
	for _, forbidden := range []string{"admin-pass", "secret-key", "database-url", "api-key"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("PrintUsage output contains forbidden flag %q", forbidden)
		}
	}
}

func TestSingleDashAliasAccepted(t *testing.T) {
	// Existing parser aliases remain accepted (ADR-0014 §2, I-8)
	cfg, err := Load([]string{"-port=9090", "-host=127.0.0.1"}, WithEnviron(nil), WithoutEnvFile())
	if err != nil {
		t.Fatalf("Load() unexpected error for single-dash flag alias: %v", err)
	}
	if cfg.Port != 9090 {
		t.Errorf("Port = %d, want 9090", cfg.Port)
	}
	if cfg.Host != "127.0.0.1" {
		t.Errorf("Host = %q, want 127.0.0.1", cfg.Host)
	}

	// -v alias returns ShowVersion
	cfgV, err := Load([]string{"-v"}, WithEnviron(nil), WithoutEnvFile())
	if err != nil {
		t.Fatalf("Load() unexpected error for -v: %v", err)
	}
	if !cfgV.ShowVersion {
		t.Errorf("ShowVersion = false, want true for -v alias")
	}
}

func TestDisabledModules(t *testing.T) {
	t.Run("default has no disabled modules", func(t *testing.T) {
		cfg, err := Load(nil, WithEnviron(nil), WithoutEnvFile())
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if !cfg.IsModuleEnabled("komga") {
			t.Errorf("expected komga to be enabled by default")
		}
		if !cfg.IsModuleEnabled("opds") {
			t.Errorf("expected opds to be enabled by default")
		}
	})

	t.Run("WithDisabledModules disables specified modules", func(t *testing.T) {
		cfg, err := Load(nil, WithEnviron(nil), WithoutEnvFile(), WithDisabledModules("komga", "TEST-MOD"))
		if err != nil {
			t.Fatalf("Load() error: %v", err)
		}
		if cfg.IsModuleEnabled("komga") {
			t.Errorf("expected komga to be disabled")
		}
		if cfg.IsModuleEnabled("KOMGA") {
			t.Errorf("expected KOMGA (case-insensitive) to be disabled")
		}
		if cfg.IsModuleEnabled("test-mod") {
			t.Errorf("expected test-mod to be disabled")
		}
		if !cfg.IsModuleEnabled("opds") {
			t.Errorf("expected opds to remain enabled")
		}
	})

	t.Run("DisableModule and EnableModule mutate list dynamically", func(t *testing.T) {
		cfg := &Config{}
		if !cfg.IsModuleEnabled("komga") {
			t.Errorf("expected komga enabled initially")
		}

		cfg.DisableModule("komga")
		if cfg.IsModuleEnabled("komga") {
			t.Errorf("expected komga disabled after DisableModule")
		}

		// Idempotent disable
		cfg.DisableModule("komga")
		if len(cfg.DisabledModules) != 1 {
			t.Errorf("len(DisabledModules) = %d, want 1", len(cfg.DisabledModules))
		}

		cfg.EnableModule("komga")
		if !cfg.IsModuleEnabled("komga") {
			t.Errorf("expected komga enabled after EnableModule")
		}
		if len(cfg.DisabledModules) != 0 {
			t.Errorf("len(DisabledModules) = %d, want 0", len(cfg.DisabledModules))
		}
	})
}

// TestSystemSettings_Precedence tests the slice acceptance criterion (M1-T1.4, ADR-0014 §2):
// "A setting stored in system_settings is read when no flag/env sets it, env still wins,
// and the effective config reports env_locked for pinned keys."
func TestSystemSettings_Precedence(t *testing.T) {
	t.Run("system_settings_read_when_no_flag_or_env_sets_it", func(t *testing.T) {
		settings := map[string]string{
			"server.port":       "9090",
			"log.level":         "debug",
			"scanner.workers":   "4",
			"library.mode":      "managed",
			"oidc.default_role": "Reader",
		}

		cfg, err := Load(nil, WithEnviron(nil), WithoutEnvFile(), WithSystemSettings(settings))
		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}

		if cfg.Port != 9090 {
			t.Errorf("Port = %d, want 9090 from system_settings", cfg.Port)
		}
		if cfg.LogLevel != "debug" {
			t.Errorf("LogLevel = %q, want 'debug' from system_settings", cfg.LogLevel)
		}
		if cfg.ScannerWorkers != 4 {
			t.Errorf("ScannerWorkers = %d, want 4 from system_settings", cfg.ScannerWorkers)
		}
		if cfg.LibraryMode != "managed" {
			t.Errorf("LibraryMode = %q, want 'managed' from system_settings", cfg.LibraryMode)
		}
		if cfg.OIDCDefaultRole != "Reader" {
			t.Errorf("OIDCDefaultRole = %q, want 'Reader' from system_settings", cfg.OIDCDefaultRole)
		}

		// Provenance and lock status: system_settings values are NOT env_locked
		for _, key := range []string{"TOBITEN_PORT", "TOBITEN_LOG_LEVEL", "TOBITEN_SCANNER_WORKERS", "TOBITEN_LIBRARY_MODE", "TOBITEN_OIDC_DEFAULT_ROLE"} {
			if src := cfg.SourceOf(key); src != SourceSystemSettings {
				t.Errorf("%s SourceOf() = %v, want %v", key, src, SourceSystemSettings)
			}
			if cfg.IsEnvLocked(key) {
				t.Errorf("%s IsEnvLocked() = true, want false for system_settings", key)
			}
		}

		// EffectiveConfig reporting
		eff := cfg.EffectiveConfig()
		for _, key := range []string{"TOBITEN_PORT", "TOBITEN_LOG_LEVEL", "TOBITEN_SCANNER_WORKERS", "TOBITEN_LIBRARY_MODE", "TOBITEN_OIDC_DEFAULT_ROLE"} {
			s, ok := eff.Settings[key]
			if !ok {
				t.Errorf("EffectiveConfig missing setting %q", key)
				continue
			}
			if s.EnvLocked {
				t.Errorf("%s in EffectiveConfig has EnvLocked = true, want false", key)
			}
			if s.Source != SourceSystemSettings {
				t.Errorf("%s in EffectiveConfig has Source = %v, want %v", key, s.Source, SourceSystemSettings)
			}
		}
	})

	t.Run("env_still_wins_over_system_settings", func(t *testing.T) {
		settings := map[string]string{
			"server.port":     "9090",
			"log.level":       "debug",
			"scanner.workers": "4",
		}
		env := []string{
			"TOBITEN_PORT=8088",
			"TOBITEN_LOG_LEVEL=warn",
		}

		cfg, err := Load(nil, WithEnviron(env), WithoutEnvFile(), WithSystemSettings(settings))
		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}

		// Env overrides system_settings
		if cfg.Port != 8088 {
			t.Errorf("Port = %d, want 8088 from env (env still wins)", cfg.Port)
		}
		if cfg.LogLevel != "warn" {
			t.Errorf("LogLevel = %q, want 'warn' from env (env still wins)", cfg.LogLevel)
		}
		// Unset in env still falls back to system_settings
		if cfg.ScannerWorkers != 4 {
			t.Errorf("ScannerWorkers = %d, want 4 from system_settings", cfg.ScannerWorkers)
		}

		// Env-pinned keys must report env_locked = true
		if !cfg.IsEnvLocked("TOBITEN_PORT") {
			t.Errorf("IsEnvLocked(TOBITEN_PORT) = false, want true for env-pinned key")
		}
		if !cfg.IsEnvLocked("TOBITEN_LOG_LEVEL") {
			t.Errorf("IsEnvLocked(TOBITEN_LOG_LEVEL) = false, want true for env-pinned key")
		}
		if cfg.SourceOf("TOBITEN_PORT") != SourceEnv {
			t.Errorf("SourceOf(TOBITEN_PORT) = %v, want %v", cfg.SourceOf("TOBITEN_PORT"), SourceEnv)
		}
		if cfg.SourceOf("TOBITEN_LOG_LEVEL") != SourceEnv {
			t.Errorf("SourceOf(TOBITEN_LOG_LEVEL) = %v, want %v", cfg.SourceOf("TOBITEN_LOG_LEVEL"), SourceEnv)
		}

		// system_settings key remains unlocked
		if cfg.IsEnvLocked("TOBITEN_SCANNER_WORKERS") {
			t.Errorf("IsEnvLocked(TOBITEN_SCANNER_WORKERS) = true, want false")
		}
		if cfg.SourceOf("TOBITEN_SCANNER_WORKERS") != SourceSystemSettings {
			t.Errorf("SourceOf(TOBITEN_SCANNER_WORKERS) = %v, want %v", cfg.SourceOf("TOBITEN_SCANNER_WORKERS"), SourceSystemSettings)
		}

		// Verify via EffectiveConfig
		eff := cfg.EffectiveConfig()
		if !eff.Settings["TOBITEN_PORT"].EnvLocked {
			t.Errorf("EffectiveConfig TOBITEN_PORT EnvLocked = false, want true")
		}
		if !eff.Settings["TOBITEN_LOG_LEVEL"].EnvLocked {
			t.Errorf("EffectiveConfig TOBITEN_LOG_LEVEL EnvLocked = false, want true")
		}
		if eff.Settings["TOBITEN_SCANNER_WORKERS"].EnvLocked {
			t.Errorf("EffectiveConfig TOBITEN_SCANNER_WORKERS EnvLocked = true, want false")
		}
	})

	t.Run("flag_still_wins_over_system_settings_and_env", func(t *testing.T) {
		settings := map[string]string{
			"server.port": "9090",
		}
		env := []string{
			"TOBITEN_PORT=8088",
		}
		flags := []string{
			"--port=7070",
		}

		cfg, err := Load(flags, WithEnviron(env), WithoutEnvFile(), WithSystemSettings(settings))
		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}

		if cfg.Port != 7070 {
			t.Errorf("Port = %d, want 7070 from flag (flag overrides all)", cfg.Port)
		}
		if cfg.SourceOf("TOBITEN_PORT") != SourceFlag {
			t.Errorf("SourceOf(TOBITEN_PORT) = %v, want %v", cfg.SourceOf("TOBITEN_PORT"), SourceFlag)
		}
		if !cfg.IsEnvLocked("TOBITEN_PORT") {
			t.Errorf("IsEnvLocked(TOBITEN_PORT) = false, want true for flag")
		}
	})

	t.Run("env_file_still_wins_over_system_settings", func(t *testing.T) {
		settings := map[string]string{
			"server.port": "9090",
		}
		envFile := "TOBITEN_PORT=8085\n"

		cfg, err := Load(nil, WithEnviron(nil), WithEnvFileReader(strings.NewReader(envFile)), WithSystemSettings(settings))
		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}

		if cfg.Port != 8085 {
			t.Errorf("Port = %d, want 8085 from .env file", cfg.Port)
		}
		if cfg.SourceOf("TOBITEN_PORT") != SourceEnvFile {
			t.Errorf("SourceOf(TOBITEN_PORT) = %v, want %v", cfg.SourceOf("TOBITEN_PORT"), SourceEnvFile)
		}
		if !cfg.IsEnvLocked("TOBITEN_PORT") {
			t.Errorf("IsEnvLocked(TOBITEN_PORT) = false, want true for env_file")
		}
	})

	t.Run("blank_env_falls_through_to_system_settings", func(t *testing.T) {
		// I-6: blank environment variable is treated as absent and falls through to system_settings
		settings := map[string]string{
			"server.port": "9090",
			"log.level":   "debug",
		}
		env := []string{
			"TOBITEN_PORT=",
			"TOBITEN_LOG_LEVEL=   ",
		}

		cfg, err := Load(nil, WithEnviron(env), WithoutEnvFile(), WithSystemSettings(settings))
		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}

		if cfg.Port != 9090 {
			t.Errorf("Port = %d, want 9090 from system_settings when env is blank", cfg.Port)
		}
		if cfg.LogLevel != "debug" {
			t.Errorf("LogLevel = %q, want 'debug' from system_settings when env is blank", cfg.LogLevel)
		}
		if cfg.IsEnvLocked("TOBITEN_PORT") {
			t.Errorf("IsEnvLocked(TOBITEN_PORT) = true, want false when env was blank")
		}
		if cfg.IsEnvLocked("TOBITEN_LOG_LEVEL") {
			t.Errorf("IsEnvLocked(TOBITEN_LOG_LEVEL) = true, want false when env was blank")
		}
	})

	t.Run("blank_system_settings_falls_through_to_default", func(t *testing.T) {
		settings := map[string]string{
			"server.port": "   ",
			"log.level":   "",
		}

		cfg, err := Load(nil, WithEnviron(nil), WithoutEnvFile(), WithSystemSettings(settings))
		if err != nil {
			t.Fatalf("Load() unexpected error: %v", err)
		}

		if cfg.Port != DefaultPort {
			t.Errorf("Port = %d, want default %d when system_settings is blank", cfg.Port, DefaultPort)
		}
		if cfg.LogLevel != DefaultLogLevel {
			t.Errorf("LogLevel = %q, want default %q when system_settings is blank", cfg.LogLevel, DefaultLogLevel)
		}
		if cfg.SourceOf("TOBITEN_PORT") != SourceDefault {
			t.Errorf("SourceOf(TOBITEN_PORT) = %v, want %v", cfg.SourceOf("TOBITEN_PORT"), SourceDefault)
		}
		if cfg.IsEnvLocked("TOBITEN_PORT") {
			t.Errorf("IsEnvLocked(TOBITEN_PORT) = true, want false for default")
		}
	})
}

func TestSystemSettings_KeyAliases(t *testing.T) {
	tests := []struct {
		name       string
		settings   map[string]string
		checkField func(*testing.T, *Config)
	}{
		{
			name:     "oidc.default_role_id alias",
			settings: map[string]string{"oidc.default_role_id": "019318a0-0000-7000-8000-000000000003"},
			checkField: func(t *testing.T, c *Config) {
				if c.OIDCDefaultRole != "019318a0-0000-7000-8000-000000000003" {
					t.Errorf("OIDCDefaultRole = %q, want role id", c.OIDCDefaultRole)
				}
				if c.SourceOf("TOBITEN_OIDC_DEFAULT_ROLE") != SourceSystemSettings {
					t.Errorf("SourceOf = %v, want %v", c.SourceOf("TOBITEN_OIDC_DEFAULT_ROLE"), SourceSystemSettings)
				}
			},
		},
		{
			name:     "oidc.default_labels alias",
			settings: map[string]string{"oidc.default_labels": "restricted,family"},
			checkField: func(t *testing.T, c *Config) {
				if c.OIDCDefaultLabels != "restricted,family" {
					t.Errorf("OIDCDefaultLabels = %q, want 'restricted,family'", c.OIDCDefaultLabels)
				}
			},
		},
		{
			name:     "server.base_url alias",
			settings: map[string]string{"server.base_url": "https://books.example.com/tobiten"},
			checkField: func(t *testing.T, c *Config) {
				if c.BaseURL != "https://books.example.com/tobiten" {
					t.Errorf("BaseURL = %q, want 'https://books.example.com/tobiten'", c.BaseURL)
				}
			},
		},
		{
			name:     "cache.thumb_max_mb alias",
			settings: map[string]string{"cache.thumb_max_mb": "1024"},
			checkField: func(t *testing.T, c *Config) {
				if c.ThumbCacheMaxMB != 1024 {
					t.Errorf("ThumbCacheMaxMB = %d, want 1024", c.ThumbCacheMaxMB)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load(nil, WithEnviron(nil), WithoutEnvFile(), WithSystemSettings(tt.settings))
			if err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}
			tt.checkField(t, cfg)
		})
	}
}

func TestSystemSettings_WithSystemSettingsJSON(t *testing.T) {
	rawJSON := map[string][]byte{
		"server.port":           []byte(`9090`),
		"log.level":             []byte(`"debug"`),
		"oidc.default_labels":   []byte(`["label-1", "label-2"]`),
		"events.retention_days": []byte(`14`),
	}

	cfg, err := Load(nil, WithEnviron(nil), WithoutEnvFile(), WithSystemSettingsJSON(rawJSON))
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	if cfg.Port != 9090 {
		t.Errorf("Port = %d, want 9090", cfg.Port)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want 'debug'", cfg.LogLevel)
	}
	if cfg.OIDCDefaultLabels != "label-1,label-2" {
		t.Errorf("OIDCDefaultLabels = %q, want 'label-1,label-2'", cfg.OIDCDefaultLabels)
	}

	// Unmapped setting accessible via SystemSetting
	val, ok := cfg.SystemSetting("events.retention_days")
	if !ok || val != "14" {
		t.Errorf("SystemSetting(events.retention_days) = %q, %v; want '14', true", val, ok)
	}
}

func TestSystemSettings_ApplySystemSettings(t *testing.T) {
	env := []string{
		"TOBITEN_PORT=8080",
	}
	cfg, err := Load(nil, WithEnviron(env), WithoutEnvFile())
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	// Initial state
	if !cfg.IsEnvLocked("TOBITEN_PORT") {
		t.Errorf("expected TOBITEN_PORT to be env-locked")
	}
	if cfg.IsEnvLocked("TOBITEN_LOG_LEVEL") {
		t.Errorf("expected TOBITEN_LOG_LEVEL not to be env-locked")
	}

	// Apply settings: port is locked, log.level is not
	err = cfg.ApplySystemSettings(map[string]string{
		"server.port": "9090",
		"log.level":   "debug",
	})
	if err != nil {
		t.Fatalf("ApplySystemSettings failed: %v", err)
	}

	// Port should remain 8080 ("env still wins")
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want 8080 (env-locked key must not be modified)", cfg.Port)
	}
	// LogLevel should be updated to debug
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want 'debug' from applied system_settings", cfg.LogLevel)
	}
	if cfg.SourceOf("TOBITEN_LOG_LEVEL") != SourceSystemSettings {
		t.Errorf("SourceOf(TOBITEN_LOG_LEVEL) = %v, want %v", cfg.SourceOf("TOBITEN_LOG_LEVEL"), SourceSystemSettings)
	}
	if cfg.IsEnvLocked("TOBITEN_LOG_LEVEL") {
		t.Errorf("IsEnvLocked(TOBITEN_LOG_LEVEL) = true, want false")
	}

	// Apply invalid setting: port out of range must fail validation and keep cfg intact
	err = cfg.ApplySystemSettings(map[string]string{
		"log.level": "invalid_level",
	})
	if err == nil {
		t.Errorf("expected error applying invalid log level, got nil")
	}
	// Previous valid LogLevel must still be debug
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel corrupted after failed ApplySystemSettings: got %q, want 'debug'", cfg.LogLevel)
	}
}

func TestEffectiveConfig_Reporting(t *testing.T) {
	flags := []string{"--host=127.0.0.1"}
	env := []string{"TOBITEN_PORT=9090"}
	settings := map[string]string{
		"log.level":             "warn",
		"events.retention_days": "7",
	}

	cfg, err := Load(flags, WithEnviron(env), WithoutEnvFile(), WithSystemSettings(settings))
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	eff := cfg.EffectiveConfig()

	// 1. Flag setting: host
	hostSetting := eff.Settings["TOBITEN_HOST"]
	if !hostSetting.EnvLocked || hostSetting.Source != SourceFlag || hostSetting.Value != "127.0.0.1" {
		t.Errorf("hostSetting = %+v, want Source=flag, EnvLocked=true, Value=127.0.0.1", hostSetting)
	}

	// 2. Env setting: port
	portSetting := eff.Settings["TOBITEN_PORT"]
	if !portSetting.EnvLocked || portSetting.Source != SourceEnv || portSetting.Value != 9090 {
		t.Errorf("portSetting = %+v, want Source=env, EnvLocked=true, Value=9090", portSetting)
	}

	// 3. SystemSettings setting: log.level
	logSetting := eff.Settings["TOBITEN_LOG_LEVEL"]
	if logSetting.EnvLocked || logSetting.Source != SourceSystemSettings || logSetting.Value != "warn" {
		t.Errorf("logSetting = %+v, want Source=system_settings, EnvLocked=false, Value=warn", logSetting)
	}

	// 4. Default setting: library.mode
	libSetting := eff.Settings["TOBITEN_LIBRARY_MODE"]
	if libSetting.EnvLocked || libSetting.Source != SourceDefault || libSetting.Value != "virtual" {
		t.Errorf("libSetting = %+v, want Source=default, EnvLocked=false, Value=virtual", libSetting)
	}

	// 5. Unmapped setting: events.retention_days
	unmappedSetting := eff.Settings["events.retention_days"]
	if unmappedSetting.EnvLocked || unmappedSetting.Source != SourceSystemSettings || unmappedSetting.Value != "7" {
		t.Errorf("unmappedSetting = %+v, want Source=system_settings, EnvLocked=false, Value=7", unmappedSetting)
	}

	// 6. Secret setting: admin pass
	passSetting := eff.Settings["TOBITEN_ADMIN_PASS"]
	if !passSetting.IsSecret {
		t.Errorf("passSetting.IsSecret = false, want true")
	}
	if passSetting.Value != "" {
		t.Errorf("unset passSetting.Value = %q, want empty string", passSetting.Value)
	}

	// 7. Setting helper lookup by alias
	s, ok := cfg.Setting("port")
	if !ok || s.Key != "TOBITEN_PORT" || !s.EnvLocked {
		t.Errorf("Setting('port') = %+v, %v; want TOBITEN_PORT locked", s, ok)
	}
}
