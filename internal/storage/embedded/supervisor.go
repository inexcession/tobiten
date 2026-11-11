package embedded

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// Sentinel errors returned by the supervisor.
var (
	ErrRootForbidden     = errors.New("embedded postgresql cannot run as root: run as a non-root user")
	ErrAlreadyRunning    = errors.New("embedded postgresql is already running")
	ErrNotRunning        = errors.New("embedded postgresql is not running")
	ErrSocketPathTooLong = errors.New("unix domain socket path too long")
)

func defaultIsRoot() bool {
	return os.Geteuid() == 0
}

// Options contains configuration settings for creating an embedded PostgreSQL supervisor.
type Options struct {
	// BinDir specifies the path to PostgreSQL binaries. If empty, binaries are discovered via FindBinDir.
	BinDir string
	// DataDir specifies the database cluster directory (${TOBITEN_DATA_DIR}/postgres). Required.
	DataDir string
	// SocketDir specifies the directory for the UNIX domain socket. If empty, defaults to DataDir
	// (or a short fallback in os.TempDir() if the path would exceed 104 bytes).
	SocketDir string
	// Port specifies the port identifier for the socket (default 5432).
	Port int
	// LogFile specifies the file where PostgreSQL logs are written. If empty, defaults to DataDir/postgresql.log.
	LogFile string
	// NoSync instructs initdb to skip syncing to disk (--no-sync), useful for fast test runs.
	NoSync bool

	// isRoot overrides the root-detection check (for testing root refusal).
	isRoot func() bool
}

// Supervisor manages the lifecycle of an embedded PostgreSQL subprocess on a UNIX domain socket.
type Supervisor struct {
	mu         sync.Mutex
	opts       Options
	binDir     string
	dataDir    string
	socketDir  string
	port       int
	pid        int
	running    bool
	tempSocket bool
}

// New creates a new embedded PostgreSQL supervisor with the given options.
func New(opts Options) (*Supervisor, error) {
	if opts.DataDir == "" {
		return nil, errors.New("data directory is required")
	}

	port := opts.Port
	if port <= 0 {
		port = 5432
	}

	return &Supervisor{
		opts:    opts,
		dataDir: filepath.Clean(opts.DataDir),
		port:    port,
	}, nil
}

// Start initializes the database cluster if necessary, writes the configuration,
// and starts the supervised PostgreSQL instance on a UNIX domain socket.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return ErrAlreadyRunning
	}

	isRoot := s.opts.isRoot
	if isRoot == nil {
		isRoot = defaultIsRoot
	}
	if isRoot() {
		return ErrRootForbidden
	}

	// Maximum sun_path size for macOS is 104 bytes (Linux is 108).
	const maxSunPath = 104
	if s.opts.SocketDir != "" {
		cleanSocketDir := filepath.Clean(s.opts.SocketDir)
		sockPath := filepath.Join(cleanSocketDir, fmt.Sprintf(".s.PGSQL.%d", s.port))
		if len(sockPath) >= maxSunPath {
			return fmt.Errorf("%w: path %q is %d bytes (maximum is %d)", ErrSocketPathTooLong, sockPath, len(sockPath), maxSunPath-1)
		}
		s.socketDir = cleanSocketDir
	}

	binDir := s.opts.BinDir
	if binDir == "" {
		var err error
		binDir, err = FindBinDir("")
		if err != nil {
			return err
		}
	}
	s.binDir = binDir

	if err := os.MkdirAll(s.dataDir, 0700); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	if s.opts.SocketDir == "" {
		sockPath := filepath.Join(s.dataDir, fmt.Sprintf(".s.PGSQL.%d", s.port))
		if len(sockPath) < maxSunPath {
			s.socketDir = s.dataDir
		} else {
			h := sha256.Sum256([]byte(s.dataDir))
			tmpSockDir := filepath.Join(os.TempDir(), fmt.Sprintf("tbt-%x", h[:4]))
			if err := os.MkdirAll(tmpSockDir, 0700); err != nil {
				return fmt.Errorf("failed to create fallback socket directory: %w", err)
			}
			s.socketDir = tmpSockDir
			s.tempSocket = true
		}
	}

	if err := os.MkdirAll(s.socketDir, 0700); err != nil {
		return fmt.Errorf("failed to create socket directory: %w", err)
	}

	// Check if cluster initialized (PG_VERSION exists)
	pgVersionFile := filepath.Join(s.dataDir, "PG_VERSION")
	if _, err := os.Stat(pgVersionFile); os.IsNotExist(err) {
		initdbArgs := []string{
			"-D", s.dataDir,
			"-U", "postgres",
			"-A", "trust",
			"-E", "UTF8",
		}
		if s.opts.NoSync {
			initdbArgs = append(initdbArgs, "--no-sync")
		}

		cmd := exec.CommandContext(ctx, filepath.Join(s.binDir, "initdb"), initdbArgs...)
		out, initErr := cmd.CombinedOutput()
		if initErr != nil {
			return fmt.Errorf("initdb failed: %w (output: %s)", initErr, strings.TrimSpace(string(out)))
		}
	}

	// Write postgresql.conf block
	confPath := filepath.Join(s.dataDir, "postgresql.conf")
	settings := ConfigSettings{
		ListenAddresses:       "''",
		UnixSocketDirectories: s.socketDir,
		UnixSocketPermissions: "0700",
		Port:                  s.port,
	}
	if err := WriteConfigFile(confPath, settings); err != nil {
		return fmt.Errorf("failed to update postgresql.conf: %w", err)
	}

	// Start PostgreSQL via pg_ctl
	logFile := s.opts.LogFile
	if logFile == "" {
		logFile = filepath.Join(s.dataDir, "postgresql.log")
	}

	startCmd := exec.CommandContext(ctx, filepath.Join(s.binDir, "pg_ctl"),
		"-w",
		"-D", s.dataDir,
		"-l", logFile,
		"start",
	)
	out, err := startCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_ctl start failed: %w (output: %s)", err, strings.TrimSpace(string(out)))
	}

	// Read postmaster.pid
	pidBytes, err := os.ReadFile(filepath.Join(s.dataDir, "postmaster.pid"))
	if err == nil {
		lines := strings.Split(string(pidBytes), "\n")
		if len(lines) > 0 {
			s.pid, _ = strconv.Atoi(strings.TrimSpace(lines[0]))
		}
	}

	s.running = true
	return nil
}

// Stop shuts down the supervised PostgreSQL instance cleanly.
// It is idempotent and safe to call multiple times.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	// Attempt graceful fast stop first
	stopCmd := exec.CommandContext(ctx, filepath.Join(s.binDir, "pg_ctl"),
		"-w",
		"-D", s.dataDir,
		"-m", "fast",
		"stop",
	)
	if err := stopCmd.Run(); err != nil {
		// Fall back to immediate stop
		stopImm := exec.Command(filepath.Join(s.binDir, "pg_ctl"),
			"-w",
			"-D", s.dataDir,
			"-m", "immediate",
			"stop",
		)
		_ = stopImm.Run()
	}

	// If postmaster is still running, kill it
	if s.pid > 0 {
		if err := syscall.Kill(s.pid, 0); err == nil {
			_ = syscall.Kill(s.pid, syscall.SIGKILL)
		}
	}

	if s.tempSocket && s.socketDir != "" {
		_ = os.RemoveAll(s.socketDir)
	}

	s.running = false
	s.pid = 0
	return nil
}

// Close stops the supervisor, satisfying io.Closer.
func (s *Supervisor) Close() error {
	return s.Stop(context.Background())
}

// Running reports whether the supervisor is currently running.
func (s *Supervisor) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// PID returns the postmaster process ID, or 0 if not running.
func (s *Supervisor) PID() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pid
}

// BinDir returns the PostgreSQL binary directory in use.
func (s *Supervisor) BinDir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binDir
}

// DataDir returns the cluster data directory.
func (s *Supervisor) DataDir() string {
	return s.dataDir
}

// SocketDir returns the directory containing the UNIX domain socket.
func (s *Supervisor) SocketDir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.socketDir
}

// Port returns the port identifier.
func (s *Supervisor) Port() int {
	return s.port
}

// DatabaseURL returns a postgresql connection URL for connecting to the embedded instance over UNIX socket.
func (s *Supervisor) DatabaseURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := &url.URL{
		Scheme: "postgres",
		User:   url.User("postgres"),
		Path:   "/postgres",
	}
	q := url.Values{}
	q.Set("host", s.socketDir)
	q.Set("port", strconv.Itoa(s.port))
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}

// ConnString returns a key-value PostgreSQL connection string for connecting over UNIX socket.
func (s *Supervisor) ConnString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable",
		s.socketDir, s.port)
}
