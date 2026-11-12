package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Predefined compiled defaults.
const (
	DefaultHost              = "0.0.0.0"
	DefaultPort              = 8080
	DefaultBaseURL           = "http://localhost:8080"
	DefaultDataDir           = "./data"
	DefaultThumbCacheMaxMB   = 2048
	DefaultExtractCacheMaxMB = 4096
	DefaultLogLevel          = "info"
	DefaultLogFormat         = "json"
	DefaultStorageMode       = "local"
	DefaultLibraryMode       = "virtual"
	DefaultIntakeMode        = "hardlink"
	DefaultScannerWorkers    = 0
)

// DaemonEnvKeys contains all 22 TOBITEN_* environment variables recognized by tobitend (I-7).
var DaemonEnvKeys = []string{
	"TOBITEN_HOST",
	"TOBITEN_PORT",
	"TOBITEN_BASE_URL",
	"TOBITEN_DATA_DIR",
	"TOBITEN_COVERS_DIR",
	"TOBITEN_CACHE_DIR",
	"TOBITEN_THUMB_CACHE_MAX_MB",
	"TOBITEN_EXTRACT_CACHE_MAX_MB",
	"TOBITEN_ADMIN_USER",
	"TOBITEN_ADMIN_PASS",
	"TOBITEN_SECRET_KEY",
	"TOBITEN_TRUSTED_PROXIES",
	"TOBITEN_OIDC_DEFAULT_ROLE",
	"TOBITEN_OIDC_DEFAULT_LABELS",
	"TOBITEN_LOG_LEVEL",
	"TOBITEN_LOG_FORMAT",
	"TOBITEN_STORAGE_MODE",
	"TOBITEN_DATABASE_URL",
	"TOBITEN_PG_BIN",
	"TOBITEN_LIBRARY_MODE",
	"TOBITEN_SCANNER_WORKERS",
	"TOBITEN_INTAKE_MODE",
}

// CLIEnvKeys contains environment variables owned by the CLI rather than the daemon (I-7).
var CLIEnvKeys = []string{
	"TOBITEN_API_KEY",
}

// Sentinel errors for configuration and validation failures.
var (
	ErrLocalStorageConflict    = errors.New("storage mode 'local' requires an empty TOBITEN_DATABASE_URL; for a remote database, set TOBITEN_STORAGE_MODE=postgres")
	ErrPostgresStorageConflict = errors.New("storage mode 'postgres' requires a non-empty TOBITEN_DATABASE_URL")
	ErrInvalidStorageMode      = errors.New("unsupported storage mode: supported modes are 'local' and 'postgres'")
	ErrInvalidLibraryMode      = errors.New("unsupported library mode: supported modes are 'virtual' and 'managed'")
	ErrInvalidIntakeMode       = errors.New("unsupported intake mode: supported modes are 'hardlink', 'copy', and 'move'")
	ErrInvalidLogLevel         = errors.New("unsupported log level: supported levels are 'debug', 'info', 'warn', and 'error'")
	ErrInvalidLogFormat        = errors.New("unsupported log format: supported formats are 'json', 'text', and 'table'")
	ErrInvalidPort             = errors.New("invalid port: port must be between 1 and 65535")
	ErrInvalidBaseURL          = errors.New("invalid base URL: must be an absolute URL with http or https scheme, without query, fragment, or userinfo")
	ErrInvalidFlag             = errors.New("invalid CLI flag")
)

// Source represents the origin of a configuration value.
type Source string

const (
	SourceDefault        Source = "default"
	SourceSystemSettings Source = "system_settings"
	SourceEnvFile        Source = "env_file"
	SourceEnv            Source = "env"
	SourceFlag           Source = "flag"
)

// EffectiveSetting describes the resolved value, source, and lock status of a configuration setting (ADR-0014 §2).
type EffectiveSetting struct {
	Key       string `json:"key"`
	Value     any    `json:"value"`
	Source    Source `json:"source"`
	EnvLocked bool   `json:"env_locked"`
	IsSecret  bool   `json:"is_secret,omitempty"`
}

// EffectiveConfig holds the complete resolved configuration report.
type EffectiveConfig struct {
	Settings map[string]EffectiveSetting `json:"settings"`
}

// Config holds the complete resolved runtime configuration for tobitend.
type Config struct {
	// Network & Server
	Host    string `json:"host"`
	Port    int    `json:"port"`
	BaseURL string `json:"base_url"`

	// Storage Directories & Independent Volume Redirection (ADR-0014, ADR-0015)
	DataDir           string `json:"data_dir"`
	CoversDir         string `json:"covers_dir"`
	CacheDir          string `json:"cache_dir"`
	PostgresDir       string `json:"postgres_dir"`
	BackupsDir        string `json:"backups_dir"`
	SecretKeyPath     string `json:"secret_key_path"`
	ThumbCacheMaxMB   int    `json:"thumb_cache_max_mb"`
	ExtractCacheMaxMB int    `json:"extract_cache_max_mb"`

	// First-Run Admin Bootstrap & Security (ADR-0006, ADR-0014)
	AdminUser         string `json:"admin_user"`
	AdminPass         string `json:"-"`
	SecretKey         string `json:"-"`
	TrustedProxies    string `json:"trusted_proxies"`
	OIDCDefaultRole   string `json:"oidc_default_role"`
	OIDCDefaultLabels string `json:"oidc_default_labels"`

	// Logging (ADR-0013)
	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`

	// Storage Engine (ADR-0001)
	StorageMode string `json:"storage_mode"`
	DatabaseURL string `json:"-"`
	PgBin       string `json:"pg_bin"`

	// Library & Scanner (ADR-0002, ADR-0003, ADR-0019)
	LibraryMode    string `json:"library_mode"`
	ScannerWorkers int    `json:"scanner_workers"`
	IntakeMode     string `json:"intake_mode"`

	// Operational flags
	ShowVersion bool `json:"-"`

	// DisabledModules contains names of modules disabled at runtime (ADR-0018 §1).
	DisabledModules []string `json:"disabled_modules,omitempty"`

	// sources tracks the origin of each setting (flag > env > env_file > system_settings > default)
	sources map[string]Source

	// rawSystemSettings retains raw entries loaded from PostgreSQL system_settings (ADR-0014 §2).
	rawSystemSettings map[string]string
}

// IsModuleEnabled returns true if the module with the given name is enabled.
func (c *Config) IsModuleEnabled(name string) bool {
	if c == nil {
		return true
	}
	target := strings.ToLower(strings.TrimSpace(name))
	for _, m := range c.DisabledModules {
		if strings.ToLower(strings.TrimSpace(m)) == target {
			return false
		}
	}
	return true
}

// DisableModule adds a module name to the DisabledModules list.
func (c *Config) DisableModule(name string) {
	if !c.IsModuleEnabled(name) {
		return
	}
	trimmed := strings.TrimSpace(name)
	if trimmed != "" {
		c.DisabledModules = append(c.DisabledModules, trimmed)
	}
}

// EnableModule removes a module name from the DisabledModules list if present.
func (c *Config) EnableModule(name string) {
	target := strings.ToLower(strings.TrimSpace(name))
	if target == "" {
		return
	}
	filtered := make([]string, 0, len(c.DisabledModules))
	for _, m := range c.DisabledModules {
		if strings.ToLower(strings.TrimSpace(m)) != target {
			filtered = append(filtered, m)
		}
	}
	c.DisabledModules = filtered
}

// ThumbsCacheDir returns the directory for ephemeral preview thumbnails (ADR-0015).
func (c *Config) ThumbsCacheDir() string {
	return filepath.Join(c.CacheDir, "thumbs")
}

// RepackCacheDir returns the scratch directory for archive repacks (ADR-0015).
func (c *Config) RepackCacheDir() string {
	return filepath.Join(c.CacheDir, "repack")
}

// ExtractCacheDir returns the directory for extracted pages of solid RAR/7z archives (ADR-0004, ADR-0015).
func (c *Config) ExtractCacheDir() string {
	return filepath.Join(c.CacheDir, "extract")
}

// SourceOf returns the origin source for a given TOBITEN_* environment variable key or alias.
func (c *Config) SourceOf(envKey string) Source {
	if c == nil || c.sources == nil {
		return SourceDefault
	}
	trimmed := strings.TrimSpace(envKey)
	if src, ok := c.sources[trimmed]; ok {
		return src
	}
	if canonical := canonicalEnvKey(trimmed); canonical != "" {
		if src, ok := c.sources[canonical]; ok {
			return src
		}
	}
	if src, ok := c.sources[envKey]; ok {
		return src
	}
	return SourceDefault
}

// IsEnvLocked returns true if the setting was explicitly configured via CLI flags
// or environment / .env variables (ADR-0014 §2, I-6).
func (c *Config) IsEnvLocked(envKey string) bool {
	src := c.SourceOf(envKey)
	return src == SourceFlag || src == SourceEnv || src == SourceEnvFile
}

// EffectiveSettings returns a map of all known daemon configuration keys and raw system settings
// to their effective resolved state (ADR-0014 §2).
// Sensitive secrets (ADMIN_PASS, SECRET_KEY, DATABASE_URL) are masked as "[REDACTED]" if set.
func (c *Config) EffectiveSettings() map[string]EffectiveSetting {
	m := make(map[string]EffectiveSetting)
	if c == nil {
		return m
	}

	for _, envKey := range DaemonEnvKeys {
		var val any
		isSecret := false
		switch envKey {
		case "TOBITEN_HOST":
			val = c.Host
		case "TOBITEN_PORT":
			val = c.Port
		case "TOBITEN_BASE_URL":
			val = c.BaseURL
		case "TOBITEN_DATA_DIR":
			val = c.DataDir
		case "TOBITEN_COVERS_DIR":
			val = c.CoversDir
		case "TOBITEN_CACHE_DIR":
			val = c.CacheDir
		case "TOBITEN_THUMB_CACHE_MAX_MB":
			val = c.ThumbCacheMaxMB
		case "TOBITEN_EXTRACT_CACHE_MAX_MB":
			val = c.ExtractCacheMaxMB
		case "TOBITEN_ADMIN_USER":
			val = c.AdminUser
		case "TOBITEN_ADMIN_PASS":
			isSecret = true
			if c.AdminPass != "" {
				val = "[REDACTED]"
			} else {
				val = ""
			}
		case "TOBITEN_SECRET_KEY":
			isSecret = true
			if c.SecretKey != "" {
				val = "[REDACTED]"
			} else {
				val = ""
			}
		case "TOBITEN_TRUSTED_PROXIES":
			val = c.TrustedProxies
		case "TOBITEN_OIDC_DEFAULT_ROLE":
			val = c.OIDCDefaultRole
		case "TOBITEN_OIDC_DEFAULT_LABELS":
			val = c.OIDCDefaultLabels
		case "TOBITEN_LOG_LEVEL":
			val = c.LogLevel
		case "TOBITEN_LOG_FORMAT":
			val = c.LogFormat
		case "TOBITEN_STORAGE_MODE":
			val = c.StorageMode
		case "TOBITEN_DATABASE_URL":
			isSecret = true
			if c.DatabaseURL != "" {
				val = "[REDACTED]"
			} else {
				val = ""
			}
		case "TOBITEN_PG_BIN":
			val = c.PgBin
		case "TOBITEN_LIBRARY_MODE":
			val = c.LibraryMode
		case "TOBITEN_SCANNER_WORKERS":
			val = c.ScannerWorkers
		case "TOBITEN_INTAKE_MODE":
			val = c.IntakeMode
		}

		m[envKey] = EffectiveSetting{
			Key:       envKey,
			Value:     val,
			Source:    c.SourceOf(envKey),
			EnvLocked: c.IsEnvLocked(envKey),
			IsSecret:  isSecret,
		}
	}

	for k, v := range c.rawSystemSettings {
		if canonical := canonicalEnvKey(k); canonical == "" {
			if _, exists := m[k]; !exists {
				m[k] = EffectiveSetting{
					Key:       k,
					Value:     v,
					Source:    c.SourceOf(k),
					EnvLocked: c.IsEnvLocked(k),
				}
			}
		}
	}

	return m
}

// EffectiveConfig returns the complete effective configuration report (ADR-0014 §2).
func (c *Config) EffectiveConfig() *EffectiveConfig {
	return &EffectiveConfig{
		Settings: c.EffectiveSettings(),
	}
}

// Setting returns the EffectiveSetting for the given key (accepting TOBITEN_* keys or aliases).
func (c *Config) Setting(key string) (EffectiveSetting, bool) {
	if c == nil {
		return EffectiveSetting{}, false
	}
	eff := c.EffectiveSettings()
	trimmed := strings.TrimSpace(key)
	if canonical := canonicalEnvKey(trimmed); canonical != "" {
		if s, ok := eff[canonical]; ok {
			return s, true
		}
	}
	if s, ok := eff[trimmed]; ok {
		return s, true
	}
	if s, ok := eff[key]; ok {
		return s, true
	}
	return EffectiveSetting{}, false
}

// SystemSettings returns a copy of all loaded system_settings.
func (c *Config) SystemSettings() map[string]string {
	if c == nil || c.rawSystemSettings == nil {
		return nil
	}
	out := make(map[string]string, len(c.rawSystemSettings))
	for k, v := range c.rawSystemSettings {
		out[k] = v
	}
	return out
}

// SystemSetting returns the setting value from system_settings for the given key, if present.
func (c *Config) SystemSetting(key string) (string, bool) {
	if c == nil || c.rawSystemSettings == nil {
		return "", false
	}
	if val, ok := c.rawSystemSettings[key]; ok {
		return val, true
	}
	trimmed := strings.TrimSpace(key)
	if val, ok := c.rawSystemSettings[trimmed]; ok {
		return val, true
	}
	if canonical := canonicalEnvKey(trimmed); canonical != "" {
		return lookupSystemSetting(c.rawSystemSettings, canonical)
	}
	return "", false
}

// ApplySystemSettings applies settings from system_settings to an existing Config.
// Any setting already pinned by a CLI flag, environment variable, or .env file is locked
// (env still wins) and is not modified (ADR-0014 §2).
type settingApplier struct {
	envKey string
	apply  func(c *Config, val string) error
}

var systemSettingAppliers = []settingApplier{
	{
		envKey: "TOBITEN_HOST",
		apply: func(c *Config, val string) error {
			c.Host = val
			return nil
		},
	},
	{
		envKey: "TOBITEN_PORT",
		apply: func(c *Config, val string) error {
			n, err := strconv.Atoi(strings.TrimSpace(val))
			if err != nil {
				return fmt.Errorf("invalid integer value for TOBITEN_PORT in system_settings: %q", val)
			}
			c.Port = n
			return nil
		},
	},
	{
		envKey: "TOBITEN_BASE_URL",
		apply: func(c *Config, val string) error {
			c.BaseURL = strings.TrimRight(val, "/")
			if c.BaseURL == "" {
				c.BaseURL = DefaultBaseURL
			}
			return nil
		},
	},
	{
		envKey: "TOBITEN_DATA_DIR",
		apply: func(c *Config, val string) error {
			c.DataDir = val
			c.PostgresDir = filepath.Join(c.DataDir, "postgres")
			c.BackupsDir = filepath.Join(c.DataDir, "backups")
			c.SecretKeyPath = filepath.Join(c.DataDir, "secret.key")
			return nil
		},
	},
	{
		envKey: "TOBITEN_COVERS_DIR",
		apply: func(c *Config, val string) error {
			c.CoversDir = val
			return nil
		},
	},
	{
		envKey: "TOBITEN_CACHE_DIR",
		apply: func(c *Config, val string) error {
			c.CacheDir = val
			return nil
		},
	},
	{
		envKey: "TOBITEN_THUMB_CACHE_MAX_MB",
		apply: func(c *Config, val string) error {
			n, err := strconv.Atoi(strings.TrimSpace(val))
			if err != nil {
				return fmt.Errorf("invalid integer value for TOBITEN_THUMB_CACHE_MAX_MB in system_settings: %q", val)
			}
			c.ThumbCacheMaxMB = n
			return nil
		},
	},
	{
		envKey: "TOBITEN_EXTRACT_CACHE_MAX_MB",
		apply: func(c *Config, val string) error {
			n, err := strconv.Atoi(strings.TrimSpace(val))
			if err != nil {
				return fmt.Errorf("invalid integer value for TOBITEN_EXTRACT_CACHE_MAX_MB in system_settings: %q", val)
			}
			c.ExtractCacheMaxMB = n
			return nil
		},
	},
	{
		envKey: "TOBITEN_ADMIN_USER",
		apply: func(c *Config, val string) error {
			c.AdminUser = val
			return nil
		},
	},
	{
		envKey: "TOBITEN_TRUSTED_PROXIES",
		apply: func(c *Config, val string) error {
			c.TrustedProxies = val
			return nil
		},
	},
	{
		envKey: "TOBITEN_OIDC_DEFAULT_ROLE",
		apply: func(c *Config, val string) error {
			c.OIDCDefaultRole = val
			return nil
		},
	},
	{
		envKey: "TOBITEN_OIDC_DEFAULT_LABELS",
		apply: func(c *Config, val string) error {
			c.OIDCDefaultLabels = val
			return nil
		},
	},
	{
		envKey: "TOBITEN_LOG_LEVEL",
		apply: func(c *Config, val string) error {
			c.LogLevel = strings.ToLower(val)
			return nil
		},
	},
	{
		envKey: "TOBITEN_LOG_FORMAT",
		apply: func(c *Config, val string) error {
			c.LogFormat = strings.ToLower(val)
			return nil
		},
	},
	{
		envKey: "TOBITEN_STORAGE_MODE",
		apply: func(c *Config, val string) error {
			c.StorageMode = strings.ToLower(val)
			return nil
		},
	},
	{
		envKey: "TOBITEN_PG_BIN",
		apply: func(c *Config, val string) error {
			c.PgBin = val
			return nil
		},
	},
	{
		envKey: "TOBITEN_LIBRARY_MODE",
		apply: func(c *Config, val string) error {
			c.LibraryMode = strings.ToLower(val)
			return nil
		},
	},
	{
		envKey: "TOBITEN_SCANNER_WORKERS",
		apply: func(c *Config, val string) error {
			n, err := strconv.Atoi(strings.TrimSpace(val))
			if err != nil {
				return fmt.Errorf("invalid integer value for TOBITEN_SCANNER_WORKERS in system_settings: %q", val)
			}
			c.ScannerWorkers = n
			return nil
		},
	},
	{
		envKey: "TOBITEN_INTAKE_MODE",
		apply: func(c *Config, val string) error {
			c.IntakeMode = strings.ToLower(val)
			return nil
		},
	},
}

// Blank values in system_settings are treated as absent (I-6).
func (c *Config) ApplySystemSettings(settings map[string]string) error {
	if c == nil || len(settings) == 0 {
		return nil
	}

	clone := *c
	clone.sources = make(map[string]Source, len(c.sources))
	for k, v := range c.sources {
		clone.sources[k] = v
	}
	clone.rawSystemSettings = make(map[string]string, len(c.rawSystemSettings)+len(settings))
	for k, v := range c.rawSystemSettings {
		clone.rawSystemSettings[k] = v
	}
	for k, v := range settings {
		clone.rawSystemSettings[k] = v
	}

	for _, entry := range systemSettingAppliers {
		if c.IsEnvLocked(entry.envKey) {
			continue
		}
		val, ok := lookupSystemSetting(settings, entry.envKey)
		if !ok || strings.TrimSpace(val) == "" {
			continue
		}
		if err := entry.apply(&clone, val); err != nil {
			return err
		}
		clone.sources[entry.envKey] = SourceSystemSettings
	}

	clone.rebaseDefaultPaths(c)

	for k := range settings {
		canonical := canonicalEnvKey(k)
		if canonical == "" && !c.IsEnvLocked(k) {
			clone.sources[k] = SourceSystemSettings
		}
	}

	if err := clone.Validate(); err != nil {
		return err
	}

	*c = clone
	return nil
}

// ApplySystemSettingsJSON unmarshals raw JSONB settings and applies them.
func (c *Config) ApplySystemSettingsJSON(raw map[string][]byte) error {
	return c.ApplySystemSettings(ParseSystemSettingsJSON(raw))
}

// rebaseDefaultPaths rebases default-derived cover and cache directories when DataDir changes,
// while preserving explicit flag/env/.env/DB overrides and their provenance (ADR-0014 §2).
func (c *Config) rebaseDefaultPaths(orig *Config) {
	if !orig.IsEnvLocked("TOBITEN_COVERS_DIR") && c.sources["TOBITEN_COVERS_DIR"] == SourceDefault {
		c.CoversDir = filepath.Join(c.DataDir, "covers")
		c.sources["TOBITEN_COVERS_DIR"] = SourceDefault
	}
	if !orig.IsEnvLocked("TOBITEN_CACHE_DIR") && c.sources["TOBITEN_CACHE_DIR"] == SourceDefault {
		c.CacheDir = filepath.Join(c.DataDir, "cache")
		c.sources["TOBITEN_CACHE_DIR"] = SourceDefault
	}
}

// Validate checks configuration invariants and storage-mode conflict rules.
func (c *Config) Validate() error {
	// Storage mode conflict validation (ADR-0001 §3, ROADMAP M1-AC2, M1-T1, I-6/I-7)
	switch strings.ToLower(c.StorageMode) {
	case "local":
		if strings.TrimSpace(c.DatabaseURL) != "" {
			// Never echo database URL or credentials in errors (item 3)
			return ErrLocalStorageConflict
		}
	case "postgres":
		if strings.TrimSpace(c.DatabaseURL) == "" {
			return ErrPostgresStorageConflict
		}
	default:
		return fmt.Errorf("%w: %q (supported modes: 'local', 'postgres')", ErrInvalidStorageMode, c.StorageMode)
	}

	// Library mode validation (ADR-0002, ADR-0019)
	switch strings.ToLower(c.LibraryMode) {
	case "virtual", "managed":
	default:
		return fmt.Errorf("%w: %q (supported modes: 'virtual', 'managed')", ErrInvalidLibraryMode, c.LibraryMode)
	}

	// Intake mode validation (ADR-0002)
	switch strings.ToLower(c.IntakeMode) {
	case "hardlink", "copy", "move":
	default:
		return fmt.Errorf("%w: %q (supported modes: 'hardlink', 'copy', 'move')", ErrInvalidIntakeMode, c.IntakeMode)
	}

	// Log level validation (ADR-0013)
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("%w: %q (supported levels: 'debug', 'info', 'warn', 'error')", ErrInvalidLogLevel, c.LogLevel)
	}

	// Log format validation (ADR-0013)
	switch strings.ToLower(c.LogFormat) {
	case "json", "text", "table":
	default:
		return fmt.Errorf("%w: %q (supported formats: 'json', 'text', 'table')", ErrInvalidLogFormat, c.LogFormat)
	}

	// Port validation
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("%w: %d", ErrInvalidPort, c.Port)
	}

	// Cache MB validation
	if c.ThumbCacheMaxMB < 0 {
		return fmt.Errorf("thumb_cache_max_mb cannot be negative: %d", c.ThumbCacheMaxMB)
	}
	if c.ExtractCacheMaxMB < 0 {
		return fmt.Errorf("extract_cache_max_mb cannot be negative: %d", c.ExtractCacheMaxMB)
	}
	if c.ScannerWorkers < 0 {
		return fmt.Errorf("scanner_workers cannot be negative: %d", c.ScannerWorkers)
	}

	// BaseURL validation (ADR-0014, ADR-0020): absolute URL, http/https, no query, no fragment, no userinfo
	parsed, err := url.Parse(c.BaseURL)
	if err != nil {
		return fmt.Errorf("%w: invalid URL format", ErrInvalidBaseURL)
	}
	displayURL := parsed.Redacted()
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("%w: %q", ErrInvalidBaseURL, displayURL)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return fmt.Errorf("%w: %q (base URL must not contain query parameters, fragment, or user credentials)", ErrInvalidBaseURL, displayURL)
	}

	return nil
}

// Option configures how configuration is loaded.
type Option func(*loadOptions)

type loadOptions struct {
	environ         []string
	hasCustomEnv    bool
	envFile         string
	hasEnvFile      bool
	envFileReader   io.Reader
	skipEnvFile     bool
	disabledModules []string
	systemSettings  map[string]string
}

// WithEnviron specifies a custom environment list (e.g. for testing).
func WithEnviron(env []string) Option {
	return func(o *loadOptions) {
		o.environ = env
		o.hasCustomEnv = true
	}
}

// WithEnvFile specifies a custom path to a .env file.
func WithEnvFile(path string) Option {
	return func(o *loadOptions) {
		o.envFile = path
		o.hasEnvFile = true
	}
}

// WithEnvFileReader supplies .env contents from an io.Reader (e.g. for testing).
func WithEnvFileReader(r io.Reader) Option {
	return func(o *loadOptions) {
		o.envFileReader = r
	}
}

// WithoutEnvFile disables loading of any .env file.
func WithoutEnvFile() Option {
	return func(o *loadOptions) {
		o.skipEnvFile = true
	}
}

// WithDisabledModules specifies module names to disable when loading config (ADR-0018 §1).
func WithDisabledModules(names ...string) Option {
	return func(o *loadOptions) {
		for _, name := range names {
			trimmed := strings.TrimSpace(name)
			if trimmed != "" {
				o.disabledModules = append(o.disabledModules, trimmed)
			}
		}
	}
}

// WithSystemSettings specifies settings loaded from PostgreSQL system_settings table (ADR-0014 §2).
func WithSystemSettings(settings map[string]string) Option {
	return func(o *loadOptions) {
		if o.systemSettings == nil {
			o.systemSettings = make(map[string]string)
		}
		for k, v := range settings {
			o.systemSettings[k] = v
		}
	}
}

// WithSystemSettingsJSON specifies raw JSON settings loaded from PostgreSQL system_settings table.
func WithSystemSettingsJSON(settings map[string][]byte) Option {
	return func(o *loadOptions) {
		parsed := ParseSystemSettingsJSON(settings)
		if o.systemSettings == nil {
			o.systemSettings = make(map[string]string)
		}
		for k, v := range parsed {
			o.systemSettings[k] = v
		}
	}
}

type flagValues struct {
	host              string
	port              int
	baseURL           string
	dataDir           string
	coversDir         string
	cacheDir          string
	thumbCacheMaxMB   int
	extractCacheMaxMB int
	adminUser         string
	trustedProxies    string
	oidcDefaultRole   string
	oidcDefaultLabels string
	logLevel          string
	logFormat         string
	storageMode       string
	pgBin             string
	libraryMode       string
	scannerWorkers    int
	intakeMode        string
	envFile           string
	version           bool
	v                 bool
}

func registerFlags(fs *flag.FlagSet, fv *flagValues) {
	fs.StringVar(&fv.host, "host", "", "server listen host")
	fs.IntVar(&fv.port, "port", 0, "server listen port")
	fs.StringVar(&fv.baseURL, "base-url", "", "public base URL for absolute links")
	fs.StringVar(&fv.dataDir, "data-dir", "", "path to data directory")
	fs.StringVar(&fv.coversDir, "covers-dir", "", "path to persistent covers directory")
	fs.StringVar(&fv.cacheDir, "cache-dir", "", "path to ephemeral cache directory")
	fs.IntVar(&fv.thumbCacheMaxMB, "thumb-cache-max-mb", 0, "max disk budget for preview thumbnails in MB")
	fs.IntVar(&fv.extractCacheMaxMB, "extract-cache-max-mb", 0, "max disk budget for extracted pages in MB")
	fs.StringVar(&fv.adminUser, "admin-user", "", "first-run admin bootstrap username")
	fs.StringVar(&fv.trustedProxies, "trusted-proxies", "", "trusted reverse-proxy CIDRs")
	fs.StringVar(&fv.oidcDefaultRole, "oidc-default-role", "", "default role for auto-provisioned OIDC users")
	fs.StringVar(&fv.oidcDefaultLabels, "oidc-default-labels", "", "default SERVER label names for auto-provisioned OIDC users")
	fs.StringVar(&fv.logLevel, "log-level", "", "log level (debug, info, warn, error)")
	fs.StringVar(&fv.logFormat, "log-format", "", "log format (json, text, table)")
	fs.StringVar(&fv.storageMode, "storage-mode", "", "storage mode (local, postgres)")
	fs.StringVar(&fv.pgBin, "pg-bin", "", "directory containing PostgreSQL binaries for local mode")
	fs.StringVar(&fv.libraryMode, "library-mode", "", "default mode for new libraries (virtual, managed)")
	fs.IntVar(&fv.scannerWorkers, "scanner-workers", 0, "scanner worker concurrency")
	fs.StringVar(&fv.intakeMode, "intake-mode", "", "managed intake mode (hardlink, copy, move)")
	fs.StringVar(&fv.envFile, "env-file", "", "path to .env file")
	fs.BoolVar(&fv.version, "version", false, "print version and exit")
	fs.BoolVar(&fv.v, "v", false, "print version and exit")
}

// PrintUsage writes daemon command-line flag usage with canonical double-dash options
// to the given writer (ADR-0014 §2, I-8).
func PrintUsage(w io.Writer) {
	fs := flag.NewFlagSet("tobitend", flag.ContinueOnError)
	fs.SetOutput(w)
	var fv flagValues
	registerFlags(fs, &fv)
	fmt.Fprintf(w, "Usage of %s:\n", fs.Name())
	printDefaults(w, fs)
}

func printDefaults(w io.Writer, fs *flag.FlagSet) {
	var printedHelp bool
	fs.VisitAll(func(fl *flag.Flag) {
		if fl.Name == "v" {
			return // Omit short alias from canonical help (M1-T1.5)
		}
		if !printedHelp && fl.Name > "help" {
			printedHelp = true
			fmt.Fprint(w, "  --help\n    \tprint help and exit\n")
		}
		var b strings.Builder
		prefix := "--"
		if len(fl.Name) == 1 {
			prefix = "-"
		}
		fmt.Fprintf(&b, "  %s%s", prefix, fl.Name)
		name, usage := flag.UnquoteUsage(fl)
		if len(name) > 0 {
			b.WriteString(" ")
			b.WriteString(name)
		}
		if b.Len() <= 4 {
			b.WriteString("\t")
		} else {
			b.WriteString("\n    \t")
		}
		b.WriteString(strings.ReplaceAll(usage, "\n", "\n    \t"))
		if fl.DefValue != "" && fl.DefValue != "0" && fl.DefValue != "false" {
			fmt.Fprintf(&b, " (default %v)", fl.DefValue)
		}
		fmt.Fprint(w, b.String(), "\n")
	})
	if !printedHelp {
		fmt.Fprint(w, "  --help\n    \tprint help and exit\n")
	}
}

func parseCLIFlags(args []string) (*flagValues, map[string]bool, error) {
	fs := flag.NewFlagSet("tobitend", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage of %s:\n", fs.Name())
		printDefaults(fs.Output(), fs)
	}

	var fv flagValues
	registerFlags(fs, &fv)

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalidFlag, err)
	}

	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})
	return &fv, visited, nil
}

func resolveEnvFile(lo *loadOptions, fv *flagValues, visited map[string]bool) (map[string]string, error) {
	if visited["env-file"] && strings.TrimSpace(fv.envFile) != "" {
		lo.envFile = fv.envFile
		lo.hasEnvFile = true
	}
	if lo.skipEnvFile {
		return make(map[string]string), nil
	}
	if lo.envFileReader != nil {
		parsed, err := parseEnv(lo.envFileReader)
		if err != nil {
			return nil, fmt.Errorf("failed to parse env file reader: %w", err)
		}
		return parsed, nil
	}
	if lo.envFile == "" {
		return make(map[string]string), nil
	}
	parsed, err := loadEnvFile(lo.envFile)
	if err != nil {
		if lo.hasEnvFile {
			return nil, fmt.Errorf("failed to load env file %q: %w", lo.envFile, err)
		}
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read env file %q: %w", lo.envFile, err)
		}
		return make(map[string]string), nil
	}
	return parsed, nil
}

func resolveEnvMap(lo *loadOptions) map[string]string {
	envMap := make(map[string]string)
	rawEnv := lo.environ
	if !lo.hasCustomEnv {
		rawEnv = os.Environ()
	}
	for _, kv := range rawEnv {
		idx := strings.Index(kv, "=")
		if idx != -1 {
			envMap[kv[:idx]] = kv[idx+1:]
		}
	}
	return envMap
}

// Load loads, resolves, and validates configuration according to precedence:
// CLI Flags > Environment Variables / .env > Compiled Defaults.
// Empty or whitespace-only values in env/.env are treated as absent (I-6).
// Secrets (admin-pass, secret-key, database-url) have no CLI flags (I-7).
func Load(args []string, opts ...Option) (*Config, error) {
	lo := &loadOptions{
		envFile: ".env",
	}
	for _, opt := range opts {
		opt(lo)
	}

	fv, visited, err := parseCLIFlags(args)
	if err != nil {
		return nil, err
	}
	if fv.version || fv.v {
		return &Config{ShowVersion: true}, nil
	}

	envFileMap, err := resolveEnvFile(lo, fv, visited)
	if err != nil {
		return nil, err
	}

	envMap := resolveEnvMap(lo)

	cfg := &Config{
		sources: make(map[string]Source),
	}

	// 4. Resolve settings by precedence: Flag > Env > EnvFile > SystemSettings > Default
	// Blank values in env/.env/system_settings are treated as absent (I-6).
	cfg.Host = resolveString(visited["host"], fv.host, "TOBITEN_HOST", envMap, envFileMap, lo.systemSettings, DefaultHost, cfg.sources)
	cfg.Port, err = resolveInt(visited["port"], fv.port, "TOBITEN_PORT", envMap, envFileMap, lo.systemSettings, DefaultPort, cfg.sources)
	if err != nil {
		return nil, err
	}

	cfg.BaseURL = resolveString(visited["base-url"], fv.baseURL, "TOBITEN_BASE_URL", envMap, envFileMap, lo.systemSettings, DefaultBaseURL, cfg.sources)
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}

	// Storage Directories & Derived Paths (ADR-0014 §2, ADR-0015 §1, I-6)
	cfg.DataDir = resolveString(visited["data-dir"], fv.dataDir, "TOBITEN_DATA_DIR", envMap, envFileMap, lo.systemSettings, DefaultDataDir, cfg.sources)

	rawCoversDir := resolveString(visited["covers-dir"], fv.coversDir, "TOBITEN_COVERS_DIR", envMap, envFileMap, lo.systemSettings, "", cfg.sources)
	if strings.TrimSpace(rawCoversDir) == "" {
		cfg.CoversDir = filepath.Join(cfg.DataDir, "covers")
		cfg.sources["TOBITEN_COVERS_DIR"] = SourceDefault
	} else {
		cfg.CoversDir = rawCoversDir
	}

	rawCacheDir := resolveString(visited["cache-dir"], fv.cacheDir, "TOBITEN_CACHE_DIR", envMap, envFileMap, lo.systemSettings, "", cfg.sources)
	if strings.TrimSpace(rawCacheDir) == "" {
		cfg.CacheDir = filepath.Join(cfg.DataDir, "cache")
		cfg.sources["TOBITEN_CACHE_DIR"] = SourceDefault
	} else {
		cfg.CacheDir = rawCacheDir
	}

	// Derived paths fixed from DataDir (ADR-0001 §3, §5, ADR-0014 §2, §3)
	cfg.PostgresDir = filepath.Join(cfg.DataDir, "postgres")
	cfg.BackupsDir = filepath.Join(cfg.DataDir, "backups")
	cfg.SecretKeyPath = filepath.Join(cfg.DataDir, "secret.key")

	cfg.ThumbCacheMaxMB, err = resolveInt(visited["thumb-cache-max-mb"], fv.thumbCacheMaxMB, "TOBITEN_THUMB_CACHE_MAX_MB", envMap, envFileMap, lo.systemSettings, DefaultThumbCacheMaxMB, cfg.sources)
	if err != nil {
		return nil, err
	}

	cfg.ExtractCacheMaxMB, err = resolveInt(visited["extract-cache-max-mb"], fv.extractCacheMaxMB, "TOBITEN_EXTRACT_CACHE_MAX_MB", envMap, envFileMap, lo.systemSettings, DefaultExtractCacheMaxMB, cfg.sources)
	if err != nil {
		return nil, err
	}

	// First-run admin & security
	cfg.AdminUser = resolveString(visited["admin-user"], fv.adminUser, "TOBITEN_ADMIN_USER", envMap, envFileMap, lo.systemSettings, "", cfg.sources)

	// Secrets have NO flags (I-7)
	cfg.AdminPass = resolveSecret("TOBITEN_ADMIN_PASS", envMap, envFileMap, cfg.sources)
	cfg.SecretKey = resolveSecret("TOBITEN_SECRET_KEY", envMap, envFileMap, cfg.sources)

	cfg.TrustedProxies = resolveString(visited["trusted-proxies"], fv.trustedProxies, "TOBITEN_TRUSTED_PROXIES", envMap, envFileMap, lo.systemSettings, "", cfg.sources)
	cfg.OIDCDefaultRole = resolveString(visited["oidc-default-role"], fv.oidcDefaultRole, "TOBITEN_OIDC_DEFAULT_ROLE", envMap, envFileMap, lo.systemSettings, "", cfg.sources)
	cfg.OIDCDefaultLabels = resolveString(visited["oidc-default-labels"], fv.oidcDefaultLabels, "TOBITEN_OIDC_DEFAULT_LABELS", envMap, envFileMap, lo.systemSettings, "", cfg.sources)

	// Logging
	cfg.LogLevel = resolveString(visited["log-level"], fv.logLevel, "TOBITEN_LOG_LEVEL", envMap, envFileMap, lo.systemSettings, DefaultLogLevel, cfg.sources)
	cfg.LogFormat = resolveString(visited["log-format"], fv.logFormat, "TOBITEN_LOG_FORMAT", envMap, envFileMap, lo.systemSettings, DefaultLogFormat, cfg.sources)

	// Storage Engine
	cfg.StorageMode = resolveString(visited["storage-mode"], fv.storageMode, "TOBITEN_STORAGE_MODE", envMap, envFileMap, lo.systemSettings, DefaultStorageMode, cfg.sources)
	// DatabaseURL has NO flag (I-7)
	cfg.DatabaseURL = resolveSecret("TOBITEN_DATABASE_URL", envMap, envFileMap, cfg.sources)
	cfg.PgBin = resolveString(visited["pg-bin"], fv.pgBin, "TOBITEN_PG_BIN", envMap, envFileMap, lo.systemSettings, "", cfg.sources)

	// Library & Scanner
	cfg.LibraryMode = resolveString(visited["library-mode"], fv.libraryMode, "TOBITEN_LIBRARY_MODE", envMap, envFileMap, lo.systemSettings, DefaultLibraryMode, cfg.sources)
	cfg.ScannerWorkers, err = resolveInt(visited["scanner-workers"], fv.scannerWorkers, "TOBITEN_SCANNER_WORKERS", envMap, envFileMap, lo.systemSettings, DefaultScannerWorkers, cfg.sources)
	if err != nil {
		return nil, err
	}

	cfg.IntakeMode = resolveString(visited["intake-mode"], fv.intakeMode, "TOBITEN_INTAKE_MODE", envMap, envFileMap, lo.systemSettings, DefaultIntakeMode, cfg.sources)

	// Normalize casing
	cfg.StorageMode = strings.ToLower(cfg.StorageMode)
	cfg.LibraryMode = strings.ToLower(cfg.LibraryMode)
	cfg.IntakeMode = strings.ToLower(cfg.IntakeMode)
	cfg.LogLevel = strings.ToLower(cfg.LogLevel)
	cfg.LogFormat = strings.ToLower(cfg.LogFormat)

	// Retain loaded system_settings map
	if len(lo.systemSettings) > 0 {
		cfg.rawSystemSettings = make(map[string]string, len(lo.systemSettings))
		for k, v := range lo.systemSettings {
			cfg.rawSystemSettings[k] = v
			canonical := canonicalEnvKey(k)
			if canonical == "" {
				if _, ok := cfg.sources[k]; !ok {
					cfg.sources[k] = SourceSystemSettings
				}
			}
		}
	}

	// Disabled modules
	if len(lo.disabledModules) > 0 {
		cfg.DisabledModules = append(cfg.DisabledModules, lo.disabledModules...)
	}

	// 5. Semantic Validation
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func resolveString(flagVisited bool, flagVal string, envKey string, envMap, envFileMap, systemSettings map[string]string, defaultVal string, sources map[string]Source) string {
	if flagVisited && strings.TrimSpace(flagVal) != "" {
		sources[envKey] = SourceFlag
		return flagVal
	}
	if val, ok := envMap[envKey]; ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			sources[envKey] = SourceEnv
			return val
		}
	}
	if val, ok := envFileMap[envKey]; ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			sources[envKey] = SourceEnvFile
			return val
		}
	}
	if val, ok := lookupSystemSetting(systemSettings, envKey); ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			sources[envKey] = SourceSystemSettings
			return val
		}
	}
	sources[envKey] = SourceDefault
	return defaultVal
}

func resolveInt(flagVisited bool, flagVal int, envKey string, envMap, envFileMap, systemSettings map[string]string, defaultVal int, sources map[string]Source) (int, error) {
	if flagVisited {
		sources[envKey] = SourceFlag
		return flagVal, nil
	}
	if val, ok := envMap[envKey]; ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			n, err := strconv.Atoi(trimmed)
			if err != nil {
				return 0, fmt.Errorf("invalid integer value for %s: %q", envKey, val)
			}
			sources[envKey] = SourceEnv
			return n, nil
		}
	}
	if val, ok := envFileMap[envKey]; ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			n, err := strconv.Atoi(trimmed)
			if err != nil {
				return 0, fmt.Errorf("invalid integer value for %s in env file: %q", envKey, val)
			}
			sources[envKey] = SourceEnvFile
			return n, nil
		}
	}
	if val, ok := lookupSystemSetting(systemSettings, envKey); ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			n, err := strconv.Atoi(trimmed)
			if err != nil {
				return 0, fmt.Errorf("invalid integer value for %s in system_settings: %q", envKey, val)
			}
			sources[envKey] = SourceSystemSettings
			return n, nil
		}
	}
	sources[envKey] = SourceDefault
	return defaultVal, nil
}

func resolveSecret(envKey string, envMap, envFileMap map[string]string, sources map[string]Source) string {
	if val, ok := envMap[envKey]; ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			sources[envKey] = SourceEnv
			return val
		}
	}
	if val, ok := envFileMap[envKey]; ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			sources[envKey] = SourceEnvFile
			return val
		}
	}
	sources[envKey] = SourceDefault
	return ""
}

var envKeyAliases = map[string][]string{
	"TOBITEN_HOST":                 {"server.host", "host"},
	"TOBITEN_PORT":                 {"server.port", "port"},
	"TOBITEN_BASE_URL":             {"server.base_url", "base_url"},
	"TOBITEN_DATA_DIR":             {"storage.data_dir", "data_dir"},
	"TOBITEN_COVERS_DIR":           {"storage.covers_dir", "covers_dir"},
	"TOBITEN_CACHE_DIR":            {"storage.cache_dir", "cache_dir"},
	"TOBITEN_THUMB_CACHE_MAX_MB":   {"cache.thumb_max_mb", "thumb_cache_max_mb"},
	"TOBITEN_EXTRACT_CACHE_MAX_MB": {"cache.extract_max_mb", "extract_cache_max_mb"},
	"TOBITEN_ADMIN_USER":           {"admin.user", "admin_user"},
	"TOBITEN_ADMIN_PASS":           {"admin.pass", "admin_pass"},
	"TOBITEN_SECRET_KEY":           {"security.secret_key", "secret_key"},
	"TOBITEN_TRUSTED_PROXIES":      {"security.trusted_proxies", "trusted_proxies"},
	"TOBITEN_OIDC_DEFAULT_ROLE":    {"oidc.default_role_id", "oidc.default_role", "oidc_default_role"},
	"TOBITEN_OIDC_DEFAULT_LABELS":  {"oidc.default_labels", "oidc.default_label_ids", "oidc_default_labels"},
	"TOBITEN_LOG_LEVEL":            {"log.level", "logging.level", "log_level"},
	"TOBITEN_LOG_FORMAT":           {"log.format", "logging.format", "log_format"},
	"TOBITEN_STORAGE_MODE":         {"storage.mode", "storage_mode"},
	"TOBITEN_DATABASE_URL":         {"storage.database_url", "database_url"},
	"TOBITEN_PG_BIN":               {"storage.pg_bin", "pg_bin"},
	"TOBITEN_LIBRARY_MODE":         {"library.mode", "library_mode"},
	"TOBITEN_SCANNER_WORKERS":      {"scanner.workers", "scanner_workers"},
	"TOBITEN_INTAKE_MODE":          {"intake.mode", "intake_mode"},
}

// envKeyAcceptedAliases maps each canonical TOBITEN_* environment variable key
// to its complete ordered list of accepted alias spellings.
var envKeyAcceptedAliases = func() map[string][]string {
	m := make(map[string][]string, len(envKeyAliases))
	for envKey, aliases := range envKeyAliases {
		seen := make(map[string]bool)
		var list []string
		add := func(s string) {
			norm := strings.ToLower(strings.TrimSpace(s))
			if norm != "" && !seen[norm] {
				seen[norm] = true
				list = append(list, norm)
			}
		}
		add(envKey)
		for _, a := range aliases {
			add(a)
		}
		snake := strings.ToLower(strings.TrimPrefix(envKey, "TOBITEN_"))
		add(snake)
		dotted := strings.ReplaceAll(snake, "_", ".")
		add(dotted)
		m[envKey] = list
	}
	return m
}()

// aliasToEnvKey maps every accepted alias (in lowercase) back to its canonical TOBITEN_* key.
var aliasToEnvKey = func() map[string]string {
	m := make(map[string]string)
	for envKey, aliases := range envKeyAcceptedAliases {
		for _, alias := range aliases {
			m[alias] = envKey
		}
	}
	return m
}()

func canonicalEnvKey(key string) string {
	k := strings.ToLower(strings.TrimSpace(key))
	if envKey, ok := aliasToEnvKey[k]; ok {
		return envKey
	}
	upper := strings.ToUpper(strings.TrimSpace(key))
	if strings.HasPrefix(upper, "TOBITEN_") {
		return upper
	}
	return ""
}

func lookupSystemSetting(systemSettings map[string]string, envKey string) (string, bool) {
	if len(systemSettings) == 0 {
		return "", false
	}
	if val, ok := systemSettings[envKey]; ok {
		trimmed := strings.TrimSpace(val)
		if trimmed != "" {
			return trimmed, true
		}
	}
	for _, alias := range envKeyAcceptedAliases[envKey] {
		if val, ok := systemSettings[alias]; ok {
			trimmed := strings.TrimSpace(val)
			if trimmed != "" {
				return trimmed, true
			}
		}
	}
	return "", false
}

// ParseSystemSettingsJSON converts a map of raw JSONB setting values into a map of string values.
func ParseSystemSettingsJSON(raw map[string][]byte) map[string]string {
	if raw == nil {
		return nil
	}
	m := make(map[string]string, len(raw))
	for k, v := range raw {
		trimmed := bytes.TrimSpace(v)
		if len(trimmed) == 0 {
			continue
		}
		var strVal string
		if err := json.Unmarshal(trimmed, &strVal); err == nil {
			m[k] = strVal
			continue
		}
		var arrVal []string
		if err := json.Unmarshal(trimmed, &arrVal); err == nil {
			m[k] = strings.Join(arrVal, ",")
			continue
		}
		m[k] = string(trimmed)
	}
	return m
}
