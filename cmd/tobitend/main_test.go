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
