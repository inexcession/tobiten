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
	"time"
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
	logFile    string
	port       int
	pid        int
	running    bool
	tempSocket bool
}

// New creates a new embedded PostgreSQL supervisor with the given options.
// All directory and file paths are resolved to absolute paths.
func New(opts Options) (*Supervisor, error) {
	if opts.DataDir == "" {
		return nil, errors.New("data directory is required")
	}

	absDataDir, err := filepath.Abs(opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve absolute data directory: %w", err)
	}

	var absBinDir string
	if opts.BinDir != "" {
		absBinDir, err = filepath.Abs(opts.BinDir)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve absolute bin directory: %w", err)
		}
	}

	var absSocketDir string
	if opts.SocketDir != "" {
		absSocketDir, err = filepath.Abs(opts.SocketDir)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve absolute socket directory: %w", err)
		}
	}

	var absLogFile string
	if opts.LogFile != "" {
		absLogFile, err = filepath.Abs(opts.LogFile)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve absolute log file: %w", err)
		}
	}

	port := opts.Port
	if port <= 0 {
		port = 5432
	}

	return &Supervisor{
		opts:      opts,
		dataDir:   absDataDir,
		binDir:    absBinDir,
		socketDir: absSocketDir,
		logFile:   absLogFile,
		port:      port,
	}, nil
}

func readPostmasterPID(dataDir string) (int, error) {
	pidBytes, err := os.ReadFile(filepath.Join(dataDir, "postmaster.pid"))
	if err != nil {
		return 0, err
	}
	lines := strings.Split(string(pidBytes), "\n")
	if len(lines) == 0 {
		return 0, errors.New("empty postmaster.pid file")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("invalid PID in postmaster.pid: %w", err)
	}
	return pid, nil
}

func confirmProcessExit(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	killed := false

	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}

		if !killed {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			killed = true
		}

		time.Sleep(50 * time.Millisecond)
	}

	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return nil
	}

	return fmt.Errorf("process %d is still alive", pid)
}

func deriveBoundedContext(parent context.Context, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if _, ok := parent.Deadline(); ok {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, defaultTimeout)
}

func (s *Supervisor) cleanupFailedStart() {
	pid, _ := readPostmasterPID(s.dataDir)
	if s.binDir != "" {
		pgCtl := filepath.Join(s.binDir, "pg_ctl")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopCmd := exec.CommandContext(ctx, pgCtl, "-w", "-D", s.dataDir, "-m", "immediate", "stop")
		_ = stopCmd.Run()
	}

	if pid > 0 {
		_ = confirmProcessExit(pid, 3*time.Second)
	}

	if s.tempSocket && s.socketDir != "" {
		_ = os.RemoveAll(s.socketDir)
	}

	s.running = false
	s.pid = 0
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
	if s.socketDir != "" {
		sockPath := filepath.Join(s.socketDir, fmt.Sprintf(".s.PGSQL.%d", s.port))
		if len(sockPath) >= maxSunPath {
			return fmt.Errorf("%w: path %q is %d bytes (maximum is %d)", ErrSocketPathTooLong, sockPath, len(sockPath), maxSunPath-1)
		}
	}

	// 1. Validate complete PostgreSQL 18 binary set (pg_ctl, initdb, postgres) before data mutation
	binDir := s.binDir
	if binDir == "" {
		discovered, err := FindBinDir("")
		if err != nil {
			return err
		}
		binDir = discovered
	}
	if err := ValidateBinDir(binDir, func(m int) bool { return m == PinnedMajor }); err != nil {
		return fmt.Errorf("invalid postgresql binary set: %w", err)
	}
	s.binDir = binDir

	// 2. Initialize cluster into clean data directory BEFORE creating nested socket directories
	if err := os.MkdirAll(s.dataDir, 0700); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

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

	// 3. Set up and create socket directory (which may be nested in dataDir or fallback in os.TempDir())
	if s.socketDir == "" {
		sockPath := filepath.Join(s.dataDir, fmt.Sprintf(".s.PGSQL.%d", s.port))
		if len(sockPath) < maxSunPath {
			s.socketDir = s.dataDir
		} else {
			h := sha256.Sum256([]byte(s.dataDir))
			tmpSockDir := filepath.Join(os.TempDir(), fmt.Sprintf("tbt-%x", h[:4]))
			s.socketDir = tmpSockDir
			s.tempSocket = true
		}
	}

	if err := os.MkdirAll(s.socketDir, 0700); err != nil {
		return fmt.Errorf("failed to create socket directory: %w", err)
	}

	// 4. Write postgresql.conf block
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

	// 5. Start PostgreSQL via pg_ctl with startup cleanup on error or cancellation
	logFile := s.logFile
	if logFile == "" {
		logFile = filepath.Join(s.dataDir, "postgresql.log")
	}

	startCmd := exec.CommandContext(ctx, filepath.Join(s.binDir, "pg_ctl"),
		"-w",
		"-D", s.dataDir,
		"-l", logFile,
		"start",
	)
	out, startErr := startCmd.CombinedOutput()
	if startErr != nil {
		s.cleanupFailedStart()
		return fmt.Errorf("pg_ctl start failed: %w (output: %s)", startErr, strings.TrimSpace(string(out)))
	}

	// Read postmaster.pid
	pid, err := readPostmasterPID(s.dataDir)
	if err != nil || pid <= 0 {
		s.cleanupFailedStart()
		return fmt.Errorf("server started but failed to read postmaster.pid: %w", err)
	}
	s.pid = pid

	// Check if context was cancelled while waiting
	if ctx.Err() != nil {
		s.cleanupFailedStart()
		return ctx.Err()
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

	pid := s.pid
	if pid <= 0 {
		pid, _ = readPostmasterPID(s.dataDir)
	}

	pgCtl := filepath.Join(s.binDir, "pg_ctl")

	// 1. Attempt graceful fast stop bounded by ctx (or default 10s if unbounded ctx)
	fastCtx, fastCancel := deriveBoundedContext(ctx, 10*time.Second)
	defer fastCancel()

	stopCmd := exec.CommandContext(fastCtx, pgCtl,
		"-w",
		"-D", s.dataDir,
		"-m", "fast",
		"stop",
	)
	if runErr := stopCmd.Run(); runErr != nil {
		// 2. Fall back to immediate stop bounded by 5s
		immCtx, immCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer immCancel()

		immCmd := exec.CommandContext(immCtx, pgCtl,
			"-w",
			"-D", s.dataDir,
			"-m", "immediate",
			"stop",
		)
		_ = immCmd.Run()
	}

	// 3. Confirm owned-process exit
	if pid > 0 {
		if err := confirmProcessExit(pid, 5*time.Second); err != nil {
			// Process failed to exit! Propagate failure and do NOT clear running state
			return fmt.Errorf("failed to confirm postmaster process %d exit: %w", pid, err)
		}
	}

	// 4. Process exit confirmed: clean up temporary socket if applicable and clear state
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
		EscapeConnStringValue(s.socketDir), s.port)
}
