package embedded

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
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

	// Logger specifies the logger used by the supervisor. If nil, slog.Default() is used.
	Logger *slog.Logger

	// isRoot overrides the root-detection check (for testing root refusal).
	isRoot func() bool

	// fsProbe overrides filesystem detection (for testing network filesystem warnings).
	fsProbe func(path string) (fsType string, isNetwork bool, err error)

	// compressionProbe overrides compression capability detection (for testing compression tuning and warnings).
	compressionProbe func(ctx context.Context, binDir, dataDir string) (CompressionCapabilities, error)

	// postLaunchHook is an optional hook executed immediately after postmaster is launched,
	// before readiness/state finalization. Used for testing post-launch failures/cancellations.
	postLaunchHook func(s *Supervisor, pid int) error

	// confirmExit overrides the process exit confirmation check (for testing cleanup failures).
	confirmExit func(pid int, timeout time.Duration) error

	// resolvePIDTimeout overrides the timeout for resolving a pending postmaster PID (default 1500ms).
	resolvePIDTimeout time.Duration
}

// Supervisor manages the lifecycle of an embedded PostgreSQL subprocess on a UNIX domain socket.
type Supervisor struct {
	mu                sync.Mutex
	opts              Options
	binDir            string
	dataDir           string
	socketDir         string
	logFile           string
	port              int
	pid               int
	running           bool
	pendingLaunch     bool
	tempSocket        bool
	logger            *slog.Logger
	isNetworkFS       bool
	fsType            string
	fsProbe           func(path string) (fsType string, isNetwork bool, err error)
	compressionCaps   CompressionCapabilities
	compressionErr    error
	compressionProbe  func(ctx context.Context, binDir, dataDir string) (CompressionCapabilities, error)
	confirmExit       func(pid int, timeout time.Duration) error
	resolvePIDTimeout time.Duration
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

	confirmFn := opts.confirmExit
	if confirmFn == nil {
		confirmFn = confirmProcessExit
	}

	resolveTimeout := opts.resolvePIDTimeout
	if resolveTimeout <= 0 {
		resolveTimeout = 1500 * time.Millisecond
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Supervisor{
		opts:              opts,
		dataDir:           absDataDir,
		binDir:            absBinDir,
		socketDir:         absSocketDir,
		logFile:           absLogFile,
		port:              port,
		logger:            logger,
		fsProbe:           opts.fsProbe,
		compressionProbe:  opts.compressionProbe,
		confirmExit:       confirmFn,
		resolvePIDTimeout: resolveTimeout,
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

func (s *Supervisor) resolvePostmasterPID(ctx context.Context, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		pid, err := readPostmasterPID(s.dataDir)
		if err == nil && pid > 0 && syscall.Kill(pid, 0) == nil {
			return pid, nil
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		if time.Now().After(deadline) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if pid, err := readPostmasterPID(s.dataDir); err == nil && pid > 0 && syscall.Kill(pid, 0) == nil {
		return pid, nil
	}
	return 0, os.ErrNotExist
}

func (s *Supervisor) cleanupLaunchedPostmaster(pid int) error {
	if pid > 0 {
		s.pid = pid
	}
	targetPID := s.pid
	if targetPID <= 0 {
		return nil
	}

	s.running = true

	var stopErrs []error

	if s.binDir != "" {
		pgCtl := filepath.Join(s.binDir, "pg_ctl")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopCmd := exec.CommandContext(ctx, pgCtl, "-w", "-D", s.dataDir, "-m", "immediate", "stop")
		if err := stopCmd.Run(); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("immediate stop failed: %w", err))
		}
	}

	if exitErr := s.confirmExit(targetPID, 3*time.Second); exitErr != nil {
		stopErrs = append(stopErrs, fmt.Errorf("failed to confirm exit for postmaster process %d: %w", targetPID, exitErr))
	}

	if len(stopErrs) > 0 {
		// Retain owned PID, retryable state, and socket directory when cleanup fails
		s.pendingLaunch = true
		return errors.Join(stopErrs...)
	}

	if s.tempSocket && s.socketDir != "" {
		_ = os.RemoveAll(s.socketDir)
	}

	s.pendingLaunch = false
	s.running = false
	s.pid = 0
	return nil
}

func (s *Supervisor) validateBinaries() error {
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
	return nil
}

func (s *Supervisor) ensureClusterInitialized(ctx context.Context) error {
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
	return nil
}

func (s *Supervisor) setupSocketDir() error {
	const maxSunPath = 104
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
	return nil
}

// Start initializes the database cluster if necessary, writes the configuration,
// and starts the supervised PostgreSQL instance on a UNIX domain socket.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running || s.pendingLaunch {
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
	if err := s.validateBinaries(); err != nil {
		return err
	}

	// 2. Check for pre-existing running server before mutating data or configuration
	prePID, _ := readPostmasterPID(s.dataDir)
	if prePID > 0 && syscall.Kill(prePID, 0) == nil {
		return fmt.Errorf("embedded postgresql cannot start: another server (PID %d) is already running in %s", prePID, s.dataDir)
	}

	// 3. Inspect existing cluster version before mutating configuration or starting PostgreSQL (ADR-0001 §4)
	if err := ValidateClusterVersion(s.dataDir); err != nil {
		return err
	}

	// 4. Check whether data directory is located on a network filesystem (ADR-0001 §3)
	s.checkNetworkFilesystem()

	// 5. Initialize cluster into clean data directory BEFORE creating nested socket directories
	if err := s.ensureClusterInitialized(ctx); err != nil {
		return err
	}

	// 6. Set up and create socket directory (which may be nested in dataDir or fallback in os.TempDir())
	if err := s.setupSocketDir(); err != nil {
		return err
	}

	// 7. Write postgresql.conf block with probed compression settings (ADR-0001 §2)
	confPath := filepath.Join(s.dataDir, "postgresql.conf")
	settings := ConfigSettings{
		ListenAddresses:       "''",
		UnixSocketDirectories: s.socketDir,
		UnixSocketPermissions: "0700",
		Port:                  s.port,
	}
	s.applyCompressionSettings(ctx, &settings)
	if err := WriteConfigFile(confPath, settings); err != nil {
		return fmt.Errorf("failed to update postgresql.conf: %w", err)
	}

	// 8. Start PostgreSQL via pg_ctl with startup cleanup on error or cancellation
	if err := ctx.Err(); err != nil {
		return err
	}

	logFile := s.logFile
	if logFile == "" {
		logFile = filepath.Join(s.dataDir, "postgresql.log")
	}

	// Mark launch as pending from the moment we initiate startCmd
	s.pendingLaunch = true

	startCmd := exec.CommandContext(ctx, filepath.Join(s.binDir, "pg_ctl"),
		"-w",
		"-D", s.dataDir,
		"-l", logFile,
		"start",
	)
	out, startErr := startCmd.CombinedOutput()
	if startErr != nil {
		// pg_ctl failed or was cancelled while running.
		// A child process may have been forked by pg_ctl before it failed or was killed.
		// One missing postmaster.pid read must not discard a possibly launched child!
		// Resolve the pending launch with bounded, retryable cleanup until its child is accounted for.
		resolveCtx, resolveCancel := context.WithTimeout(context.Background(), s.resolvePIDTimeout+500*time.Millisecond)
		postPID, resolveErr := s.resolvePostmasterPID(resolveCtx, s.resolvePIDTimeout)
		resolveCancel()

		if postPID > 0 {
			cleanupErr := s.cleanupLaunchedPostmaster(postPID)
			if cleanupErr != nil {
				return errors.Join(fmt.Errorf("pg_ctl start failed: %w (output: %s)", startErr, strings.TrimSpace(string(out))), cleanupErr)
			}
			// Child has been accounted for and confirmed dead!
			if ctx.Err() != nil {
				return errors.Join(ctx.Err(), fmt.Errorf("pg_ctl start failed: %w (output: %s)", startErr, strings.TrimSpace(string(out))))
			}
			return fmt.Errorf("pg_ctl start failed: %w (output: %s)", startErr, strings.TrimSpace(string(out)))
		}

		// Child not yet accounted for! Retain pending launch with retryable cleanup!
		s.running = true
		s.pendingLaunch = true
		startFailure := fmt.Errorf("pg_ctl start failed: %w (output: %s)", startErr, strings.TrimSpace(string(out)))
		if ctx.Err() != nil {
			startFailure = errors.Join(ctx.Err(), startFailure)
		}
		return errors.Join(startFailure, fmt.Errorf("pending postmaster launch not yet accounted for: %w", resolveErr))
	}

	// Read postmaster.pid after pg_ctl start succeeded
	resolveCtx, resolveCancel := context.WithTimeout(context.Background(), s.resolvePIDTimeout+500*time.Millisecond)
	pid, err := s.resolvePostmasterPID(resolveCtx, s.resolvePIDTimeout)
	resolveCancel()
	if err != nil || pid <= 0 {
		s.running = true
		s.pendingLaunch = true
		return fmt.Errorf("server reported started but failed to resolve postmaster.pid: %w", err)
	}
	s.pid = pid
	s.running = true
	s.pendingLaunch = false

	// Post-launch checks: context cancellation or hook failure
	if ctx.Err() != nil {
		cleanupErr := s.cleanupLaunchedPostmaster(pid)
		return errors.Join(ctx.Err(), cleanupErr)
	}

	if s.opts.postLaunchHook != nil {
		if hookErr := s.opts.postLaunchHook(s, pid); hookErr != nil {
			cleanupErr := s.cleanupLaunchedPostmaster(pid)
			return errors.Join(hookErr, cleanupErr)
		}
	}

	if ctx.Err() != nil {
		cleanupErr := s.cleanupLaunchedPostmaster(pid)
		return errors.Join(ctx.Err(), cleanupErr)
	}

	return nil
}

// Stop shuts down the supervised PostgreSQL instance cleanly.
// It is idempotent and safe to call multiple times.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running && !s.pendingLaunch {
		return nil
	}

	pid := s.pid
	if pid <= 0 {
		resolveCtx, resolveCancel := deriveBoundedContext(ctx, 3*time.Second)
		resolvedPID, _ := s.resolvePostmasterPID(resolveCtx, s.resolvePIDTimeout)
		resolveCancel()
		if resolvedPID > 0 {
			pid = resolvedPID
			s.pid = resolvedPID
		}
	}

	pgCtl := filepath.Join(s.binDir, "pg_ctl")
	var stopErrs []error

	if pid > 0 {
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
			if immErr := immCmd.Run(); immErr != nil {
				stopErrs = append(stopErrs, fmt.Errorf("immediate stop failed: %w", immErr))
			}
		}

		// 3. Confirm owned-process exit
		if err := s.confirmExit(pid, 5*time.Second); err != nil {
			stopErrs = append(stopErrs, fmt.Errorf("failed to confirm postmaster process %d exit: %w", pid, err))
			// Process failed to exit! Propagate failure and retain running state, PID, and pending launch
			s.running = true
			s.pendingLaunch = true
			return errors.Join(stopErrs...)
		}
	} else {
		// Unresolved PID: attempt immediate stop on dataDir just in case
		if s.binDir != "" {
			immCtx, immCancel := deriveBoundedContext(ctx, 3*time.Second)
			defer immCancel()
			immCmd := exec.CommandContext(immCtx, pgCtl, "-w", "-D", s.dataDir, "-m", "immediate", "stop")
			if immErr := immCmd.Run(); immErr != nil {
				stopErrs = append(stopErrs, fmt.Errorf("immediate stop failed: %w", immErr))
			}
		}
		stopErrs = append(stopErrs, errors.New("postmaster PID unresolved: child launch remains pending"))
		// Unresolved PID / failed cleanup: retain pending state, running state, and socket
		s.running = true
		s.pendingLaunch = true
		return errors.Join(stopErrs...)
	}

	// 4. Process exit confirmed: child is accounted for! Clean up temporary socket if applicable and clear state
	if s.tempSocket && s.socketDir != "" {
		_ = os.RemoveAll(s.socketDir)
	}

	s.pendingLaunch = false
	s.running = false
	s.pid = 0
	return nil
}

// Close stops the supervisor, satisfying io.Closer.
func (s *Supervisor) Close() error {
	return s.Stop(context.Background())
}

// Running reports whether the supervisor is currently running or has a pending launch.
func (s *Supervisor) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running || s.pendingLaunch
}

// PendingLaunch reports whether a launch attempt is pending resolution or cleanup.
func (s *Supervisor) PendingLaunch() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingLaunch
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

// IsNetworkFS reports whether the data directory resides on a network filesystem (e.g. NFS, SMB/CIFS).
func (s *Supervisor) IsNetworkFS() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isNetworkFS
}

// FilesystemType returns the detected filesystem type of the data directory.
func (s *Supervisor) FilesystemType() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fsType
}

// CheckFilesystem inspects the data directory's filesystem and returns its information.
func (s *Supervisor) CheckFilesystem() (FilesystemInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	probe := s.fsProbe
	if probe == nil {
		probe = defaultFSProbe
	}

	targetPath := findExistingAncestor(s.dataDir)
	fsType, isNetwork, err := probe(targetPath)
	if err != nil {
		return FilesystemInfo{}, err
	}
	s.fsType = fsType
	s.isNetworkFS = isNetwork
	return FilesystemInfo{Type: fsType, IsNetwork: isNetwork}, nil
}

func (s *Supervisor) checkNetworkFilesystem() {
	probe := s.fsProbe
	if probe == nil {
		probe = defaultFSProbe
	}

	targetPath := findExistingAncestor(s.dataDir)
	fsType, isNetwork, err := probe(targetPath)
	if err != nil {
		s.logger.Debug("failed to inspect data directory filesystem",
			slog.String("data_dir", s.dataDir),
			slog.String("target_path", targetPath),
			slog.Any("error", err),
		)
		return
	}

	s.fsType = fsType
	s.isNetworkFS = isNetwork

	if isNetwork {
		s.logger.Warn("embedded postgresql data directory is located on a network filesystem (NFS/SMB); PostgreSQL on network storage is prone to data corruption, file locking issues, and high latency - a local disk is strongly recommended",
			slog.String("data_dir", s.dataDir),
			slog.String("fs_type", fsType),
		)
	}
}

func defaultFSProbe(path string) (string, bool, error) {
	info, err := DetectFilesystem(path)
	if err != nil {
		return "", false, err
	}
	return info.Type, info.IsNetwork, nil
}

// CompressionCapabilities returns the detected compression capabilities of the PostgreSQL binary.
func (s *Supervisor) CompressionCapabilities() CompressionCapabilities {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compressionCaps
}

// CompressionError returns any error encountered during compression capability probing.
func (s *Supervisor) CompressionError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compressionErr
}

// CheckCompression inspects the PostgreSQL binary's compression capabilities and updates cached state.
func (s *Supervisor) CheckCompression(ctx context.Context) (CompressionCapabilities, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	probe := s.compressionProbe
	if probe == nil {
		probe = defaultCompressionProbe
	}

	caps, err := probe(ctx, s.binDir, s.dataDir)
	s.compressionErr = err
	if err != nil {
		s.compressionCaps = CompressionCapabilities{}
		return CompressionCapabilities{}, err
	}
	s.compressionCaps = caps
	return caps, nil
}

func (s *Supervisor) applyCompressionSettings(ctx context.Context, settings *ConfigSettings) {
	probe := s.compressionProbe
	if probe == nil {
		probe = defaultCompressionProbe
	}

	caps, err := probe(ctx, s.binDir, s.dataDir)
	s.compressionErr = err
	if err != nil {
		s.logger.Warn("failed to probe postgresql compression capabilities; omitting compression tuning and using server defaults",
			slog.Any("error", err),
		)
		s.compressionCaps = CompressionCapabilities{}
		return
	}

	s.compressionCaps = caps

	if caps.HasZstdWAL {
		settings.WALCompression = "zstd"
	} else {
		s.logger.Warn("local postgresql build lacks zstd WAL compression support; omitting wal_compression tuning and using server default",
			slog.String("setting", "wal_compression"),
			slog.String("unsupported_method", "zstd"),
		)
	}

	if caps.HasLZ4Toast {
		settings.DefaultToastCompression = "lz4"
	} else {
		s.logger.Warn("local postgresql build lacks lz4 TOAST compression support; omitting default_toast_compression tuning and using server default",
			slog.String("setting", "default_toast_compression"),
			slog.String("unsupported_method", "lz4"),
		)
	}
}

func defaultCompressionProbe(ctx context.Context, binDir, dataDir string) (CompressionCapabilities, error) {
	return ProbeCompression(ctx, binDir, dataDir)
}
