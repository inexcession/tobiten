package main

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestVersionString(t *testing.T) {
	got := versionString()
	want := "tobitend 0.1.0-dev"
	if got != want {
		t.Fatalf("versionString() = %q, want %q", got, want)
	}
}

func TestRunVersion(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "default execution",
			args: []string{},
			want: "tobitend 0.1.0-dev\n",
		},
		{
			name: "flag -version",
			args: []string{"-version"},
			want: "tobitend 0.1.0-dev\n",
		},
		{
			name: "flag -v",
			args: []string{"-v"},
			want: "tobitend 0.1.0-dev\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe() error: %v", err)
			}

			origStdout := os.Stdout
			os.Stdout = w

			exitCode := run(tc.args)

			_ = w.Close()
			os.Stdout = origStdout

			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			_ = r.Close()

			if exitCode != 0 {
				t.Errorf("run(%v) exit code = %d, want 0", tc.args, exitCode)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("run(%v) output = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

func TestRunInvalidFlag(t *testing.T) {
	exitCode := run([]string{"-invalid-unknown-flag"})
	if exitCode == 0 {
		t.Errorf("run(-invalid-unknown-flag) exit code = 0, want non-zero")
	}
}

func TestRunStorageModeRefusal(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantExit   int
		wantErrSub string
	}{
		{
			name:       "local mode with database URL refuses startup",
			args:       []string{"-storage-mode=local", "-database-url=postgres://localhost:5432/db"},
			wantExit:   1,
			wantErrSub: "storage mode 'local' requires an empty TOBITEN_DATABASE_URL",
		},
		{
			name:       "postgres mode without database URL refuses startup",
			args:       []string{"-storage-mode=postgres", "-database-url="},
			wantExit:   1,
			wantErrSub: "storage mode 'postgres' requires a non-empty TOBITEN_DATABASE_URL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe() error: %v", err)
			}

			origStderr := os.Stderr
			os.Stderr = w

			exitCode := run(tc.args)

			_ = w.Close()
			os.Stderr = origStderr

			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			_ = r.Close()

			if exitCode != tc.wantExit {
				t.Errorf("run(%v) exit code = %d, want %d", tc.args, exitCode, tc.wantExit)
			}
			if !bytes.Contains(buf.Bytes(), []byte(tc.wantErrSub)) {
				t.Errorf("run(%v) stderr = %q, want it to contain %q", tc.args, buf.String(), tc.wantErrSub)
			}
		})
	}
}
