package config

import (
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
	SourceDefault Source = "default"
	SourceEnvFile Source = "env_file"
	SourceEnv     Source = "env"
	SourceFlag    Source = "flag"
)

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

	// sources tracks the origin of each setting (flag > env > env_file > default)
	sources map[string]Source
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

// SourceOf returns the origin source for a given TOBITEN_* environment variable key.
func (c *Config) SourceOf(envKey string) Source {
	if c.sources == nil {
		return SourceDefault
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
	environ       []string
	hasCustomEnv  bool
	envFile       string
	hasEnvFile    bool
	envFileReader io.Reader
	skipEnvFile   bool
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
	fs.VisitAll(func(fl *flag.Flag) {
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

	// 1. Parse CLI flags
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
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidFlag, err)
	}

	visited := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})

	// Override env file if set via CLI flag
	if visited["env-file"] && strings.TrimSpace(fv.envFile) != "" {
		lo.envFile = fv.envFile
		lo.hasEnvFile = true
	}

	// 2. Parse .env file
	envFileMap := make(map[string]string)
	if !lo.skipEnvFile {
		if lo.envFileReader != nil {
			parsed, err := parseEnv(lo.envFileReader)
			if err != nil {
				return nil, fmt.Errorf("failed to parse env file reader: %w", err)
			}
			envFileMap = parsed
		} else if lo.envFile != "" {
			parsed, err := loadEnvFile(lo.envFile)
			if err != nil {
				// If custom env file path was explicitly requested, fail on error.
				if lo.hasEnvFile {
					return nil, fmt.Errorf("failed to load env file %q: %w", lo.envFile, err)
				}
				// Default .env is optional; ignore not-exist errors.
				if !os.IsNotExist(err) {
					return nil, fmt.Errorf("failed to read env file %q: %w", lo.envFile, err)
				}
			} else {
				envFileMap = parsed
			}
		}
	}

	// 3. Parse environment variables
	envMap := make(map[string]string)
	rawEnv := lo.environ
	if !lo.hasCustomEnv {
		rawEnv = os.Environ()
	}
	for _, kv := range rawEnv {
		idx := strings.Index(kv, "=")
		if idx != -1 {
			k := kv[:idx]
			v := kv[idx+1:]
			envMap[k] = v
		}
	}

	cfg := &Config{
		sources: make(map[string]Source),
	}

	if fv.version || fv.v {
		cfg.ShowVersion = true
		return cfg, nil
	}

	var err error

	// 4. Resolve settings by precedence: Flag > Env > EnvFile > Default
	// Blank values in env/.env are treated as absent (I-6).
	cfg.Host = resolveString(visited["host"], fv.host, "TOBITEN_HOST", envMap, envFileMap, DefaultHost, cfg.sources)
	cfg.Port, err = resolveInt(visited["port"], fv.port, "TOBITEN_PORT", envMap, envFileMap, DefaultPort, cfg.sources)
	if err != nil {
		return nil, err
	}

	cfg.BaseURL = resolveString(visited["base-url"], fv.baseURL, "TOBITEN_BASE_URL", envMap, envFileMap, DefaultBaseURL, cfg.sources)
	cfg.BaseURL = strings.TrimRight(cfg.BaseURL, "/")
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}

	// Storage Directories & Derived Paths (ADR-0014 §2, ADR-0015 §1, I-6)
	cfg.DataDir = resolveString(visited["data-dir"], fv.dataDir, "TOBITEN_DATA_DIR", envMap, envFileMap, DefaultDataDir, cfg.sources)

	rawCoversDir := resolveString(visited["covers-dir"], fv.coversDir, "TOBITEN_COVERS_DIR", envMap, envFileMap, "", cfg.sources)
	if strings.TrimSpace(rawCoversDir) == "" {
		cfg.CoversDir = filepath.Join(cfg.DataDir, "covers")
		cfg.sources["TOBITEN_COVERS_DIR"] = SourceDefault
	} else {
		cfg.CoversDir = rawCoversDir
	}

	rawCacheDir := resolveString(visited["cache-dir"], fv.cacheDir, "TOBITEN_CACHE_DIR", envMap, envFileMap, "", cfg.sources)
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

	cfg.ThumbCacheMaxMB, err = resolveInt(visited["thumb-cache-max-mb"], fv.thumbCacheMaxMB, "TOBITEN_THUMB_CACHE_MAX_MB", envMap, envFileMap, DefaultThumbCacheMaxMB, cfg.sources)
	if err != nil {
		return nil, err
	}

	cfg.ExtractCacheMaxMB, err = resolveInt(visited["extract-cache-max-mb"], fv.extractCacheMaxMB, "TOBITEN_EXTRACT_CACHE_MAX_MB", envMap, envFileMap, DefaultExtractCacheMaxMB, cfg.sources)
	if err != nil {
		return nil, err
	}

	// First-run admin & security
	cfg.AdminUser = resolveString(visited["admin-user"], fv.adminUser, "TOBITEN_ADMIN_USER", envMap, envFileMap, "", cfg.sources)

	// Secrets have NO flags (I-7)
	cfg.AdminPass = resolveSecret("TOBITEN_ADMIN_PASS", envMap, envFileMap, cfg.sources)
	cfg.SecretKey = resolveSecret("TOBITEN_SECRET_KEY", envMap, envFileMap, cfg.sources)

	cfg.TrustedProxies = resolveString(visited["trusted-proxies"], fv.trustedProxies, "TOBITEN_TRUSTED_PROXIES", envMap, envFileMap, "", cfg.sources)
	cfg.OIDCDefaultRole = resolveString(visited["oidc-default-role"], fv.oidcDefaultRole, "TOBITEN_OIDC_DEFAULT_ROLE", envMap, envFileMap, "", cfg.sources)
	cfg.OIDCDefaultLabels = resolveString(visited["oidc-default-labels"], fv.oidcDefaultLabels, "TOBITEN_OIDC_DEFAULT_LABELS", envMap, envFileMap, "", cfg.sources)

	// Logging
	cfg.LogLevel = resolveString(visited["log-level"], fv.logLevel, "TOBITEN_LOG_LEVEL", envMap, envFileMap, DefaultLogLevel, cfg.sources)
	cfg.LogFormat = resolveString(visited["log-format"], fv.logFormat, "TOBITEN_LOG_FORMAT", envMap, envFileMap, DefaultLogFormat, cfg.sources)

	// Storage Engine
	cfg.StorageMode = resolveString(visited["storage-mode"], fv.storageMode, "TOBITEN_STORAGE_MODE", envMap, envFileMap, DefaultStorageMode, cfg.sources)
	// DatabaseURL has NO flag (I-7)
	cfg.DatabaseURL = resolveSecret("TOBITEN_DATABASE_URL", envMap, envFileMap, cfg.sources)
	cfg.PgBin = resolveString(visited["pg-bin"], fv.pgBin, "TOBITEN_PG_BIN", envMap, envFileMap, "", cfg.sources)

	// Library & Scanner
	cfg.LibraryMode = resolveString(visited["library-mode"], fv.libraryMode, "TOBITEN_LIBRARY_MODE", envMap, envFileMap, DefaultLibraryMode, cfg.sources)
	cfg.ScannerWorkers, err = resolveInt(visited["scanner-workers"], fv.scannerWorkers, "TOBITEN_SCANNER_WORKERS", envMap, envFileMap, DefaultScannerWorkers, cfg.sources)
	if err != nil {
		return nil, err
	}

	cfg.IntakeMode = resolveString(visited["intake-mode"], fv.intakeMode, "TOBITEN_INTAKE_MODE", envMap, envFileMap, DefaultIntakeMode, cfg.sources)

	// Normalize casing
	cfg.StorageMode = strings.ToLower(cfg.StorageMode)
	cfg.LibraryMode = strings.ToLower(cfg.LibraryMode)
	cfg.IntakeMode = strings.ToLower(cfg.IntakeMode)
	cfg.LogLevel = strings.ToLower(cfg.LogLevel)
	cfg.LogFormat = strings.ToLower(cfg.LogFormat)

	// 5. Semantic Validation
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func resolveString(flagVisited bool, flagVal string, envKey string, envMap, envFileMap map[string]string, defaultVal string, sources map[string]Source) string {
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
	sources[envKey] = SourceDefault
	return defaultVal
}

func resolveInt(flagVisited bool, flagVal int, envKey string, envMap, envFileMap map[string]string, defaultVal int, sources map[string]Source) (int, error) {
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
