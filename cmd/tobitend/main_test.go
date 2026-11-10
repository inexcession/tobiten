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

func TestRunHelp(t *testing.T) {
	for _, flag := range []string{"-h", "-help"} {
		t.Run(flag, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("os.Pipe() error: %v", err)
			}

			origStderr := os.Stderr
			os.Stderr = w

			exitCode := run([]string{flag})

			_ = w.Close()
			os.Stderr = origStderr

			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			_ = r.Close()

			if exitCode != 0 {
				t.Errorf("run(%s) exit code = %d, want 0", flag, exitCode)
			}
			out := buf.String()
			if !bytes.Contains(buf.Bytes(), []byte("Usage of tobitend:")) {
				t.Errorf("run(%s) stderr = %q, want usage output", flag, out)
			}
			// I-7: secrets and client API key must NOT have CLI flags
			for _, forbidden := range []string{"-admin-pass", "-secret-key", "-database-url", "-api-key"} {
				if bytes.Contains(buf.Bytes(), []byte(forbidden)) {
					t.Errorf("run(%s) usage contains forbidden flag %q", flag, forbidden)
				}
			}
		})
	}
}

func TestRunInvalidFlag(t *testing.T) {
	invalidFlags := []string{
		"-invalid-unknown-flag",
		"-admin-pass=secret",
		"-secret-key=secret",
		"-database-url=postgres://localhost/db",
		"-api-key=token",
	}
	for _, flg := range invalidFlags {
		t.Run(flg, func(t *testing.T) {
			exitCode := run([]string{flg})
			if exitCode != 2 {
				t.Errorf("run(%s) exit code = %d, want 2", flg, exitCode)
			}
		})
	}
}

func TestRunStorageModeRefusal(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		envKey     string
		envVal     string
		wantExit   int
		wantErrSub string
		wantNotSub string
	}{
		{
			name:       "local mode with database URL refuses startup",
			args:       []string{"-storage-mode=local"},
			envKey:     "TOBITEN_DATABASE_URL",
			envVal:     "postgres://user:super_secret_password@localhost:5432/db",
			wantExit:   1,
			wantErrSub: "storage mode 'local' requires an empty TOBITEN_DATABASE_URL",
			wantNotSub: "super_secret_password",
		},
		{
			name:       "postgres mode without database URL refuses startup",
			args:       []string{"-storage-mode=postgres"},
			envKey:     "TOBITEN_DATABASE_URL",
			envVal:     "",
			wantExit:   1,
			wantErrSub: "storage mode 'postgres' requires a non-empty TOBITEN_DATABASE_URL",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.envKey, tc.envVal)

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
			if tc.wantNotSub != "" && bytes.Contains(buf.Bytes(), []byte(tc.wantNotSub)) {
				t.Errorf("run(%v) stderr = %q, must NOT contain secret %q", tc.args, buf.String(), tc.wantNotSub)
			}
		})
	}
}
