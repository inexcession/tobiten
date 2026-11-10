// Package testdb provides a real PostgreSQL 18 test database harness.
// It provisions a real PostgreSQL 18 instance (either via system binaries
// such as MacPorts or TOBITEN_PG_BIN, or via TOBITEN_TEST_DATABASE_URL)
// and gives each test or test package a fresh, isolated database.
// The PostgreSQL server is automatically turned off when testing completes.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	globalMu      sync.Mutex
	currentServer *serverInstance
	sigOnce       sync.Once
)

type serverInstance struct {
	mu          sync.Mutex
	binDir      string
	dataDir     string
	port        int
	adminURL    string
	isExternal  bool
	managedByM  bool
	activeTests int
	adminPool   *pgxpool.Pool
	pid         int
	stopOnce    sync.Once
}

func setupSignalHandler() {
	sigOnce.Do(func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
		go func() {
			<-c
			Stop()
			os.Exit(1)
		}()
	})
}

// findPgBinDir searches for PostgreSQL 18+ binaries.
// Candidates checked in order:
// 1. TOBITEN_PG_BIN environment variable
// 2. MacPorts default: /opt/local/lib/postgresql18/bin
// 3. Homebrew Apple Silicon: /opt/homebrew/opt/postgresql@18/bin
// 4. Homebrew Intel: /usr/local/opt/postgresql@18/bin
// 5. Debian/Ubuntu: /usr/lib/postgresql/18/bin
// 6. PATH lookup for pg_ctl
func findPgBinDir() (string, error) {
	var candidates []string

	if envBin := os.Getenv("TOBITEN_PG_BIN"); envBin != "" {
		candidates = append(candidates, envBin)
	}

	candidates = append(candidates,
		"/opt/local/lib/postgresql18/bin",
		"/opt/homebrew/opt/postgresql@18/bin",
		"/usr/local/opt/postgresql@18/bin",
		"/usr/lib/postgresql/18/bin",
	)

	if pathBin, err := exec.LookPath("pg_ctl"); err == nil {
		candidates = append(candidates, filepath.Dir(pathBin))
	}

	for _, dir := range candidates {
		if isValidPg18BinDir(dir) {
			return dir, nil
		}
	}

	return "", errors.New("postgresql 18 binaries not found: set TOBITEN_PG_BIN, install postgresql 18 (e.g. via MacPorts in /opt/local/lib/postgresql18/bin), or provide TOBITEN_TEST_DATABASE_URL")
}

func isValidPg18BinDir(dir string) bool {
	pgCtl := filepath.Join(dir, "pg_ctl")
	initdb := filepath.Join(dir, "initdb")

	if fi, err := os.Stat(pgCtl); err != nil || fi.IsDir() || (fi.Mode().Perm()&0111) == 0 {
		return false
	}
	if fi, err := os.Stat(initdb); err != nil || fi.IsDir() || (fi.Mode().Perm()&0111) == 0 {
		return false
	}

	// Verify major version is at least 18.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, pgCtl, "--version")
	out, err := cmd.Output()
	if err != nil {
		return false
	}

	major, err := parsePgMajorVersion(string(out))
	if err != nil || major < 18 {
		return false
	}

	return true
}

var versionRegex = regexp.MustCompile(`(?i)(?:postgres(?:ql)?|pg_ctl|initdb)\s+(?:\([^\)]+\)\s+)?(\d+)(?:\.(\d+))?`)

func parsePgMajorVersion(versionStr string) (int, error) {
	matches := versionRegex.FindStringSubmatch(versionStr)
	if len(matches) < 2 {
		return 0, fmt.Errorf("unable to parse postgres version string: %q", versionStr)
	}
	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("invalid major version number in %q: %w", versionStr, err)
	}
	return major, nil
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("failed to allocate free port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func getOrStartServer(managedByM bool) (*serverInstance, error) {
	globalMu.Lock()
	defer globalMu.Unlock()

	setupSignalHandler()

	if currentServer != nil {
		if managedByM {
			currentServer.managedByM = true
		}
		return currentServer, nil
	}

	srv, err := startServer(managedByM)
	if err != nil {
		return nil, err
	}
	currentServer = srv
	return currentServer, nil
}

func startServer(managedByM bool) (*serverInstance, error) {
	// 1. Check for existing external database URL.
	if testURL := os.Getenv("TOBITEN_TEST_DATABASE_URL"); testURL != "" {
		srv, err := connectExternalServer(testURL, managedByM)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to TOBITEN_TEST_DATABASE_URL: %w", err)
		}
		return srv, nil
	}

	// 2. Discover local PostgreSQL 18 binaries.
	binDir, err := findPgBinDir()
	if err != nil {
		return nil, err
	}

	port, err := getFreePort()
	if err != nil {
		return nil, err
	}

	dataDir, err := os.MkdirTemp("", "tobiten-test-pg-data-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary postgres data dir: %w", err)
	}

	initdbPath := filepath.Join(binDir, "initdb")
	pgCtlPath := filepath.Join(binDir, "pg_ctl")

	// Initialize the cluster.
	initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer initCancel()

	initCmd := exec.CommandContext(initCtx, initdbPath,
		"-D", dataDir,
		"-U", "postgres",
		"-A", "trust",
		"--no-sync",
	)
	if out, initErr := initCmd.CombinedOutput(); initErr != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("initdb failed: %w (output: %s)", initErr, string(out))
	}

	// Start the postgres server using pg_ctl.
	pgOptions := fmt.Sprintf(
		"-p %d -h 127.0.0.1 -k '' -c fsync=off -c synchronous_commit=off -c full_page_writes=off -c shared_buffers=32MB -c max_connections=50",
		port,
	)
	logFile := filepath.Join(dataDir, "pg.log")

	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()

	startCmd := exec.CommandContext(startCtx, pgCtlPath,
		"-w",
		"-D", dataDir,
		"-l", logFile,
		"-o", pgOptions,
		"start",
	)
	if out, startErr := startCmd.CombinedOutput(); startErr != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("pg_ctl start failed: %w (output: %s)", startErr, string(out))
	}

	// Read postmaster PID if available.
	pid := 0
	if pidBytes, pidErr := os.ReadFile(filepath.Join(dataDir, "postmaster.pid")); pidErr == nil {
		lines := strings.Split(string(pidBytes), "\n")
		if len(lines) > 0 {
			pid, _ = strconv.Atoi(strings.TrimSpace(lines[0]))
		}
	}

	adminURL := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		stopInstance(binDir, dataDir, pid)
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}
	poolCfg.MaxConns = 10

	adminPool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		stopInstance(binDir, dataDir, pid)
		return nil, fmt.Errorf("failed to connect admin pool: %w", err)
	}

	if pingErr := adminPool.Ping(ctx); pingErr != nil {
		adminPool.Close()
		stopInstance(binDir, dataDir, pid)
		return nil, fmt.Errorf("failed to ping postgres admin pool: %w", pingErr)
	}

	return &serverInstance{
		binDir:     binDir,
		dataDir:    dataDir,
		port:       port,
		adminURL:   adminURL,
		isExternal: false,
		managedByM: managedByM,
		adminPool:  adminPool,
		pid:        pid,
	}, nil
}

func connectExternalServer(testURL string, managedByM bool) (*serverInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(testURL)
	if err != nil {
		return nil, fmt.Errorf("invalid TOBITEN_TEST_DATABASE_URL: %w", err)
	}
	poolCfg.MaxConns = 10

	adminPool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to external postgres: %w", err)
	}

	if pingErr := adminPool.Ping(ctx); pingErr != nil {
		adminPool.Close()
		return nil, fmt.Errorf("failed to ping external postgres: %w", pingErr)
	}

	// Verify server_version_num >= 180000.
	var versionNum int
	if vErr := adminPool.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&versionNum); vErr != nil {
		adminPool.Close()
		return nil, fmt.Errorf("failed to query external postgres server version: %w", vErr)
	}
	if versionNum < 180000 {
		adminPool.Close()
		return nil, fmt.Errorf("external postgresql version must be 18+ (got server_version_num %d)", versionNum)
	}

	return &serverInstance{
		adminURL:   testURL,
		isExternal: true,
		managedByM: managedByM,
		adminPool:  adminPool,
	}, nil
}

func stopInstance(binDir, dataDir string, pid int) {
	stopped := false
	if binDir != "" && dataDir != "" {
		pgCtlPath := filepath.Join(binDir, "pg_ctl")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stopCmd := exec.CommandContext(ctx, pgCtlPath, "-w", "-D", dataDir, "-m", "immediate", "stop")
		if err := stopCmd.Run(); err == nil {
			stopped = true
		}
	}

	if !stopped && pid > 0 {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}

	if dataDir != "" {
		_ = os.RemoveAll(dataDir)
	}
}

func (s *serverInstance) stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.adminPool != nil {
			s.adminPool.Close()
			s.adminPool = nil
		}

		if !s.isExternal {
			stopInstance(s.binDir, s.dataDir, s.pid)
		}
	})
}

func (s *serverInstance) createFreshDB(ctx context.Context) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.adminPool == nil {
		return "", "", errors.New("admin pool is closed")
	}

	// Generate a unique database name.
	randBytes := make([]byte, 4)
	if _, err := rand.Read(randBytes); err != nil {
		return "", "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	dbName := fmt.Sprintf("tobiten_test_%d_%s", time.Now().UnixNano(), hex.EncodeToString(randBytes))

	query := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
	if _, err := s.adminPool.Exec(ctx, query); err != nil {
		return "", "", fmt.Errorf("failed to create fresh database %q: %w", dbName, err)
	}

	targetURL, err := s.buildTargetURL(dbName)
	if err != nil {
		_ = s.dropDBLocked(ctx, dbName)
		return "", "", err
	}

	return dbName, targetURL, nil
}

func (s *serverInstance) dropDB(ctx context.Context, dbName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropDBLocked(ctx, dbName)
}

func (s *serverInstance) dropDBLocked(ctx context.Context, dbName string) error {
	if s.adminPool == nil {
		return nil
	}
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", pgx.Identifier{dbName}.Sanitize())
	_, err := s.adminPool.Exec(ctx, query)
	return err
}

func (s *serverInstance) buildTargetURL(dbName string) (string, error) {
	u, err := url.Parse(s.adminURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse admin URL: %w", err)
	}
	u.Path = "/" + dbName
	return u.String(), nil
}
