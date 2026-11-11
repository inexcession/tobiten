package embedded

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEscapePgConfString(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"", "''"},
		{"/var/run/postgresql", "'/var/run/postgresql'"},
		{"/path with spaces/data", "'/path with spaces/data'"},
		{"/tmp/bob's data", "'/tmp/bob''s data'"},
		{`C:\pg\data`, `'C:\\pg\\data'`},
	}

	for _, tc := range tests {
		out := EscapePgConfString(tc.input)
		if out != tc.expect {
			t.Errorf("EscapePgConfString(%q) = %q, expected %q", tc.input, out, tc.expect)
		}
	}
}

func TestEscapeConnStringValue(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"", "''"},
		{"simple", "simple"},
		{"5432", "5432"},
		{"/tmp/socket", "/tmp/socket"},
		{"/tmp/path with spaces", "'/tmp/path with spaces'"},
		{"/tmp/bob's socket", `'/'tmp/'bob\'s socket'`},
		{`\escaped\path`, `'\\escaped\\path'`},
	}

	for _, tc := range tests {
		out := EscapeConnStringValue(tc.input)
		if tc.input == "/tmp/bob's socket" {
			// Expected format with escaped single quote inside single quotes
			if out != `'/tmp/bob\'s socket'` {
				t.Errorf("EscapeConnStringValue(%q) = %q, expected %q", tc.input, out, `'/tmp/bob\'s socket'`)
			}
			continue
		}
		if out != tc.expect {
			t.Errorf("EscapeConnStringValue(%q) = %q, expected %q", tc.input, out, tc.expect)
		}
	}
}

func TestGenerateConfig(t *testing.T) {
	settings := ConfigSettings{
		ListenAddresses:         "''",
		UnixSocketDirectories:   "/tmp/test socket with 'quotes'",
		UnixSocketPermissions:   "0700",
		Port:                    5432,
		WALCompression:          "zstd",
		DefaultToastCompression: "lz4",
		ExtraSettings: map[string]string{
			"shared_buffers":  "'64MB'",
			"max_connections": "50",
		},
	}

	conf := GenerateConfig(settings)

	expectedSnippets := []string{
		confBeginMarker,
		"listen_addresses = ''",
		"unix_socket_directories = '/tmp/test socket with ''quotes'''",
		"unix_socket_permissions = 0700",
		"port = 5432",
		"wal_compression = 'zstd'",
		"default_toast_compression = 'lz4'",
		"max_connections = 50",
		"shared_buffers = '64MB'",
		confEndMarker,
	}

	for _, snippet := range expectedSnippets {
		if !strings.Contains(conf, snippet) {
			t.Errorf("expected generated config to contain %q, but got:\n%s", snippet, conf)
		}
	}
}

func TestWriteConfigFile_NewFile(t *testing.T) {
	tmpDir := t.TempDir()
	confPath := filepath.Join(tmpDir, "postgresql.conf")

	settings := ConfigSettings{
		ListenAddresses:       "''",
		UnixSocketDirectories: "/tmp/sock",
		UnixSocketPermissions: "0700",
		Port:                  5432,
	}

	if err := WriteConfigFile(confPath, settings); err != nil {
		t.Fatalf("WriteConfigFile failed: %v", err)
	}

	bytes, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to read written config file: %v", err)
	}

	content := string(bytes)
	if !strings.Contains(content, "unix_socket_directories = '/tmp/sock'") {
		t.Errorf("unexpected content: %s", content)
	}
}

func TestWriteConfigFile_AppendAndReplace(t *testing.T) {
	tmpDir := t.TempDir()
	confPath := filepath.Join(tmpDir, "postgresql.conf")

	initialContent := "# System configuration\nlc_messages = 'C'\n"
	if err := os.WriteFile(confPath, []byte(initialContent), 0600); err != nil {
		t.Fatalf("failed to write initial file: %v", err)
	}

	// First write: appends block
	settings1 := ConfigSettings{
		ListenAddresses:       "''",
		UnixSocketDirectories: "/tmp/sock-1",
		Port:                  5432,
	}
	if err := WriteConfigFile(confPath, settings1); err != nil {
		t.Fatalf("first WriteConfigFile failed: %v", err)
	}

	bytes1, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to read config file: %v", err)
	}
	content1 := string(bytes1)
	if !strings.Contains(content1, "lc_messages = 'C'") {
		t.Errorf("expected initial content preserved, got:\n%s", content1)
	}
	if !strings.Contains(content1, "/tmp/sock-1") {
		t.Errorf("expected sock-1 in content, got:\n%s", content1)
	}

	// Second write: replaces block in place without duplicate markers
	settings2 := ConfigSettings{
		ListenAddresses:       "''",
		UnixSocketDirectories: "/tmp/sock-2",
		Port:                  5433,
	}
	if err := WriteConfigFile(confPath, settings2); err != nil {
		t.Fatalf("second WriteConfigFile failed: %v", err)
	}

	bytes2, err := os.ReadFile(confPath)
	if err != nil {
		t.Fatalf("failed to read config file: %v", err)
	}
	content2 := string(bytes2)
	if !strings.Contains(content2, "lc_messages = 'C'") {
		t.Errorf("expected initial content preserved after update, got:\n%s", content2)
	}
	if strings.Contains(content2, "/tmp/sock-1") {
		t.Errorf("expected old sock-1 removed, but found in:\n%s", content2)
	}
	if !strings.Contains(content2, "/tmp/sock-2") {
		t.Errorf("expected updated sock-2, got:\n%s", content2)
	}

	// Verify markers appear exactly once
	if strings.Count(content2, confBeginMarker) != 1 {
		t.Errorf("expected exactly 1 begin marker, got %d", strings.Count(content2, confBeginMarker))
	}
	if strings.Count(content2, confEndMarker) != 1 {
		t.Errorf("expected exactly 1 end marker, got %d", strings.Count(content2, confEndMarker))
	}
}
