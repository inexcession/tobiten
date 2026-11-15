package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/testdb"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type daemonProcess struct {
	cmd     *exec.Cmd
	port    int
	output  *syncBuffer
	done    chan struct{}
	waitErr error
}

func (p *daemonProcess) healthzURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d/healthz", p.port)
}

func findGoTool(t *testing.T) string {
	t.Helper()
	if goBin := os.Getenv("GO"); goBin != "" {
		if path, err := exec.LookPath(goBin); err == nil {
			return path
		}
		return goBin
	}
	if path, err := exec.LookPath("go"); err == nil {
		return path
	}
	for _, candidate := range []string{
		"/opt/local/bin/go",
		"/usr/local/go/bin/go",
		"/usr/local/bin/go",
		"/usr/bin/go",
	} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	t.Fatal("go binary not found")
	return ""
}

func buildDaemonBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "tobitend")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, findGoTool(t), "build", "-o", binPath, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build tobitend: %v\n%s", err, out)
	}
	return binPath
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved port listener: %v", err)
	}
	return port
}

func startDaemonProcess(t *testing.T, binPath, dbURL string) *daemonProcess {
	t.Helper()
	port := reserveTCPPort(t)
	workDir := t.TempDir()
	dataDir := filepath.Join(workDir, "data")
	cmd := exec.Command(
		binPath,
		"--storage-mode=postgres",
		"--host=127.0.0.1",
		fmt.Sprintf("--port=%d", port),
		"--data-dir="+dataDir,
		"--log-format=json",
		"--log-level=info",
	)
	cmd.Dir = workDir
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"TOBITEN_DATABASE_URL=" + dbURL,
		"TOBITEN_ADMIN_USER=alice",
		"TOBITEN_ADMIN_PASS=initial-password",
	}
	out := &syncBuffer{}
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start tobitend: %v", err)
	}
	proc := &daemonProcess{
		cmd:    cmd,
		port:   port,
		output: out,
		done:   make(chan struct{}),
	}
	go func() {
		proc.waitErr = cmd.Wait()
		close(proc.done)
	}()
	t.Cleanup(func() {
		select {
		case <-proc.done:
			return
		default:
		}
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-proc.done:
				return
			case <-time.After(5 * time.Second):
			}
			_ = cmd.Process.Kill()
			<-proc.done
		}
	})
	return proc
}

func waitForDaemonServing(t *testing.T, proc *daemonProcess) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-proc.done:
			t.Fatalf("daemon exited before serving: err=%v\noutput:\n%s", proc.waitErr, proc.output.String())
		default:
		}
		resp, err := client.Get(proc.healthzURL())
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK && strings.TrimSpace(string(body)) == "ok" {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for daemon at %s to serve\noutput:\n%s", proc.healthzURL(), proc.output.String())
}

func waitForDaemonExit(t *testing.T, proc *daemonProcess, timeout time.Duration, label string) error {
	t.Helper()
	select {
	case <-proc.done:
		return proc.waitErr
	case <-time.After(timeout):
		t.Fatalf("%s did not exit within %v\noutput:\n%s", label, timeout, proc.output.String())
		return nil
	}
}

func queryDaemonLockPIDs(t *testing.T, db *testdb.TestDB) []int32 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	rows, err := db.Query(
		ctx,
		`SELECT pid
		 FROM pg_locks
		 WHERE locktype = 'advisory'
		   AND granted
		   AND classid = $1::oid
		   AND objid = $2::oid
		   AND objsubid = 1`,
		int64(uint32(daemonLockID>>32)),
		int64(uint32(daemonLockID&0xffffffff)),
	)
	if err != nil {
		t.Fatalf("query daemon advisory lock: %v", err)
	}
	defer rows.Close()
	var pids []int32
	for rows.Next() {
		var pid int32
		if err := rows.Scan(&pid); err != nil {
			t.Fatalf("scan lock pid: %v", err)
		}
		pids = append(pids, pid)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate lock rows: %v", err)
	}
	return pids
}

func assertExitCode(t *testing.T, err error, want int, output string) {
	t.Helper()
	if want == 0 {
		if err != nil {
			t.Fatalf("expected clean exit 0, got %v\noutput:\n%s", err, output)
		}
		return
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected exit code %d, got err=%v\noutput:\n%s", want, err, output)
	}
	if exitErr.ExitCode() != want {
		t.Fatalf("exit code = %d, want %d\noutput:\n%s", exitErr.ExitCode(), want, output)
	}
}

func TestTwoActualDaemonsOwnershipConflictLossAndSuccessor(t *testing.T) {
	db := testdb.New(t)
	binPath := buildDaemonBinary(t)

	// 1. Start the first actual daemon against an unmigrated hosted DB and wait for it to serve.
	owner := startDaemonProcess(t, binPath, db.URL)
	waitForDaemonServing(t, owner)

	ownerPIDs := queryDaemonLockPIDs(t, db)
	if len(ownerPIDs) != 1 {
		t.Fatalf("expected 1 daemon lock session, got %v", ownerPIDs)
	}
	ownerLockPID := ownerPIDs[0]

	// Verify migrations ran under MigrationLockID without blocking on daemonLockID.
	var userCount int
	if err := db.QueryRow(t.Context(), "SELECT COUNT(*) FROM users WHERE username = 'alice'").Scan(&userCount); err != nil {
		t.Fatalf("query provisioned admin: %v", err)
	}
	if userCount != 1 {
		t.Fatalf("provisioned admin count = %d, want 1", userCount)
	}

	// 2. Start a second actual daemon against the same database; it must fail startup clearly.
	contender := startDaemonProcess(t, binPath, db.URL)
	contenderErr := waitForDaemonExit(t, contender, 15*time.Second, "contender daemon")
	contenderOut := contender.output.String()
	assertExitCode(t, contenderErr, 1, contenderOut)
	if !strings.Contains(contenderOut, "another tobitend already owns this database") {
		t.Fatalf("contender output missing clear ownership conflict:\n%s", contenderOut)
	}

	// Owner must remain healthy and hold the same dedicated lock session.
	resp, err := (&http.Client{Timeout: time.Second}).Get(owner.healthzURL())
	if err != nil {
		t.Fatalf("owner /healthz after contender failure: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("owner /healthz status = %d, want 200", resp.StatusCode)
	}

	// 3. Terminate ONLY the owner's dedicated advisory-lock session (not its application pool).
	if _, err := db.Exec(t.Context(), "SELECT pg_terminate_backend($1)", ownerLockPID); err != nil {
		t.Fatalf("terminate owner dedicated lock session: %v", err)
	}
	ownerErr := waitForDaemonExit(t, owner, 10*time.Second, "owner daemon")
	ownerOut := owner.output.String()
	assertExitCode(t, ownerErr, 1, ownerOut)
	if !strings.Contains(ownerOut, "database ownership connection lost") {
		t.Fatalf("owner output missing ownership loss error:\n%s", ownerOut)
	}
	if resp, err := (&http.Client{Timeout: 500 * time.Millisecond}).Get(owner.healthzURL()); err == nil {
		_ = resp.Body.Close()
		t.Fatalf("owner still serving /healthz after exit (status=%d)", resp.StatusCode)
	}

	// 4. A successor daemon can now acquire ownership and serve cleanly.
	successor := startDaemonProcess(t, binPath, db.URL)
	waitForDaemonServing(t, successor)

	successorPIDs := queryDaemonLockPIDs(t, db)
	if len(successorPIDs) != 1 || successorPIDs[0] == ownerLockPID {
		t.Fatalf("expected successor to hold new lock session (old=%d), got %v", ownerLockPID, successorPIDs)
	}

	if err := successor.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal successor SIGTERM: %v", err)
	}
	successorErr := waitForDaemonExit(t, successor, 10*time.Second, "successor daemon")
	assertExitCode(t, successorErr, 0, successor.output.String())
}
