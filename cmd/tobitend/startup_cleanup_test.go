package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/storage/embedded"
)

// delayedStop preserves the real supervisor and its locks while exit confirmation
// is deliberately delayed. The second attempt performs the real Stop.
type delayedStop struct {
	*embedded.Supervisor
	attempts    int
	failStartup bool
	beforeStop  func()
}

func (s *delayedStop) Start(ctx context.Context) error {
	if err := s.Supervisor.Start(ctx); err != nil {
		return err
	}
	if s.failStartup {
		return errors.New("injected failure after child launch")
	}
	return nil
}

func (s *delayedStop) Stop(ctx context.Context) error {
	s.attempts++
	if s.beforeStop != nil {
		s.beforeStop()
	}
	if s.attempts == 1 {
		return errors.New("injected delayed exit confirmation")
	}
	return s.Supervisor.Stop(ctx)
}

func cleanupFixture(t *testing.T, failStartup bool) (*config.Config, **delayedStop) {
	t.Helper()
	if _, err := embedded.FindBinDir(""); err != nil {
		t.Skipf("host PostgreSQL binaries required: %v", err)
	}
	original := newLocalPostgres
	t.Cleanup(func() { newLocalPostgres = original })
	socketDir, err := os.MkdirTemp("/tmp", "tbt-cleanup-socket-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(socketDir); err != nil {
			t.Error(err)
		}
	})
	var local *delayedStop
	newLocalPostgres = func(opts embedded.Options) (localPostgres, error) {
		opts.SocketDir = socketDir
		supervisor, err := embedded.New(opts)
		if err != nil {
			return nil, err
		}
		local = &delayedStop{Supervisor: supervisor, failStartup: failStartup}
		return local, nil
	}
	cfg, err := config.Load([]string{"--data-dir=" + t.TempDir()}, config.WithEnviron(nil), config.WithoutEnvFile())
	if err != nil {
		t.Fatal(err)
	}
	return cfg, &local
}

func TestCleanupRetainsDatabaseAndLocalOwnershipOnFailedStop(t *testing.T) {
	cfg, local := cleanupFixture(t, false)
	d, err := openDaemonStorage(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.close(); err != nil {
			t.Error(err)
		}
	})
	// Pause the first stop attempt while a hosted successor tries to acquire ownership.
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	// A stop attempt must never release the DB session before confirming exit.
	(*local).beforeStop = func() {
		if d.owner.IsClosed() {
			t.Error("database owner closed before Stop")
		}
		if (*local).attempts == 1 {
			close(entered)
			<-release
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- d.close() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop was not reached")
	}
	during, err := openDaemonStorage(t.Context(), hostedConfig(t, (*local).DatabaseURL()))
	if during != nil {
		_ = during.close()
	}
	close(release)
	closeErr := <-closed
	if err == nil {
		t.Fatal("hosted successor acquired ownership while Stop was delayed")
	}
	if closeErr == nil {
		t.Fatal("expected delayed stop failure")
	}
	if d.owner.IsClosed() || !(*local).Running() || !(*local).DataDirLocked() {
		t.Fatal("cleanup discarded active ownership")
	}
	// A hosted-mode successor must still be excluded from this live engine.
	successor, err := openDaemonStorage(t.Context(), hostedConfig(t, (*local).DatabaseURL()))
	if successor != nil {
		_ = successor.close()
	}
	if err == nil {
		t.Fatal("successor acquired database during failed Stop")
	}
	contender, err := embedded.New(embedded.Options{DataDir: cfg.PostgresDir})
	if err != nil {
		t.Fatal(err)
	}
	if err := contender.Start(t.Context()); !errors.Is(err, embedded.ErrDataDirLocked) {
		t.Fatalf("local successor not excluded: %v", err)
	}
	if err := d.close(); err != nil {
		t.Fatal(err)
	}
	if !d.owner.IsClosed() || (*local).Running() || (*local).DataDirLocked() || (*local).SocketDirLocked() {
		t.Fatal("successful cleanup retained ownership")
	}
	// close remains safe to retry after successful cleanup too.
	if err := d.close(); err != nil {
		t.Fatal(err)
	}
}

func TestStartupFailureReturnsRetryableCleanupHandle(t *testing.T) {
	cfg, local := cleanupFixture(t, true)
	d, err := openDaemonStorage(t.Context(), cfg)
	if err == nil || d == nil {
		t.Fatalf("failed startup lost its cleanup handle: d=%v err=%v", d, err)
	}
	t.Cleanup(func() {
		if err := d.close(); err != nil {
			t.Error(err)
		}
	})
	if !(*local).Running() || !(*local).DataDirLocked() || !(*local).SocketDirLocked() {
		t.Fatal("failed startup released data/socket ownership")
	}
	if err := d.close(); err != nil {
		t.Fatal(err)
	}
	if (*local).Running() || (*local).DataDirLocked() || (*local).SocketDirLocked() {
		t.Fatal("retry did not complete cleanup")
	}
}

func TestRunDaemonRetriesFailedCleanupBeforeReturning(t *testing.T) {
	cfg, local := cleanupFixture(t, true)
	original := slog.Default()
	t.Cleanup(func() { slog.SetDefault(original) })
	slog.SetDefault(bootstrapLogger())
	called := false
	err := runDaemon(t.Context(), cfg, func(context.Context, *config.Config, *slog.Logger) error { called = true; return nil })
	if err == nil || called {
		t.Fatalf("startup result err=%v served=%t", err, called)
	}
	if (*local).attempts != 2 || (*local).Running() || (*local).DataDirLocked() {
		t.Fatal("daemon returned before retry completed")
	}
}

func TestRunDaemonRetriesShutdownUntilExitConfirmed(t *testing.T) {
	cfg, local := cleanupFixture(t, false)
	original := slog.Default()
	t.Cleanup(func() { slog.SetDefault(original) })
	err := runDaemon(t.Context(), cfg, func(context.Context, *config.Config, *slog.Logger) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if (*local).attempts != 2 || (*local).Running() || (*local).DataDirLocked() || (*local).SocketDirLocked() {
		t.Fatal("shutdown returned before cleanup completed")
	}
}
