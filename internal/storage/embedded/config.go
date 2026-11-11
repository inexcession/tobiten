package embedded

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	confBeginMarker = "# --- BEGIN TOBITEN EMBEDDED CONFIGURATION ---"
	confEndMarker   = "# --- END TOBITEN EMBEDDED CONFIGURATION ---"
)

// ConfigSettings contains PostgreSQL configuration parameters tuned for embedded execution.
type ConfigSettings struct {
	// ListenAddresses specifies TCP listening addresses (typically '' for UNIX-socket-only).
	ListenAddresses string
	// UnixSocketDirectories specifies directory or comma-separated directories for UNIX domain sockets.
	UnixSocketDirectories string
	// UnixSocketPermissions specifies permissions for the UNIX domain socket (e.g. 0700).
	UnixSocketPermissions string
	// Port specifies the port number used for the socket identifier (default 5432).
	Port int
	// WALCompression specifies WAL compression method (e.g. 'zstd'). Omitted if empty.
	WALCompression string
	// DefaultToastCompression specifies TOAST compression method (e.g. 'lz4'). Omitted if empty.
	DefaultToastCompression string
	// ExtraSettings contains additional configuration parameters.
	ExtraSettings map[string]string
}

// GenerateConfig generates the Tobiten embedded configuration block for postgresql.conf.
func GenerateConfig(settings ConfigSettings) string {
	var sb strings.Builder
	sb.WriteString(confBeginMarker + "\n")
	sb.WriteString("# Generated automatically by Tobiten. Do not edit manually.\n")

	listen := settings.ListenAddresses
	if listen == "" {
		listen = "''"
	} else if !strings.HasPrefix(listen, "'") {
		listen = fmt.Sprintf("'%s'", listen)
	}
	fmt.Fprintf(&sb, "listen_addresses = %s\n", listen)

	if settings.UnixSocketDirectories != "" {
		sockDir := settings.UnixSocketDirectories
		if !strings.HasPrefix(sockDir, "'") {
			sockDir = fmt.Sprintf("'%s'", sockDir)
		}
		fmt.Fprintf(&sb, "unix_socket_directories = %s\n", sockDir)
	}

	if settings.UnixSocketPermissions != "" {
		fmt.Fprintf(&sb, "unix_socket_permissions = %s\n", settings.UnixSocketPermissions)
	} else {
		sb.WriteString("unix_socket_permissions = 0700\n")
	}

	port := settings.Port
	if port <= 0 {
		port = 5432
	}
	fmt.Fprintf(&sb, "port = %d\n", port)

	if settings.WALCompression != "" {
		wal := settings.WALCompression
		if !strings.HasPrefix(wal, "'") {
			wal = fmt.Sprintf("'%s'", wal)
		}
		fmt.Fprintf(&sb, "wal_compression = %s\n", wal)
	}

	if settings.DefaultToastCompression != "" {
		toast := settings.DefaultToastCompression
		if !strings.HasPrefix(toast, "'") {
			toast = fmt.Sprintf("'%s'", toast)
		}
		fmt.Fprintf(&sb, "default_toast_compression = %s\n", toast)
	}

	if len(settings.ExtraSettings) > 0 {
		keys := make([]string, 0, len(settings.ExtraSettings))
		for k := range settings.ExtraSettings {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s = %s\n", k, settings.ExtraSettings[k])
		}
	}

	sb.WriteString(confEndMarker + "\n")
	return sb.String()
}

// WriteConfigFile writes or updates the Tobiten configuration block within the specified postgresql.conf.
// If the block already exists, it is updated in place. If not, it is appended to the file.
func WriteConfigFile(confPath string, settings ConfigSettings) error {
	block := GenerateConfig(settings)

	existingBytes, err := os.ReadFile(confPath)
	if err != nil {
		if os.IsNotExist(err) {
			return os.WriteFile(confPath, []byte(block), 0600)
		}
		return fmt.Errorf("failed to read postgresql.conf: %w", err)
	}

	content := string(existingBytes)
	startIdx := strings.Index(content, confBeginMarker)
	endIdx := strings.Index(content, confEndMarker)

	var newContent string
	if startIdx != -1 && endIdx != -1 && endIdx >= startIdx {
		// Replace existing block
		before := content[:startIdx]
		after := content[endIdx+len(confEndMarker):]
		after = strings.TrimPrefix(after, "\n")
		newContent = before + block + after
	} else {
		// Append block
		if len(content) > 0 && !strings.HasSuffix(content, "\n") {
			content += "\n"
		}
		newContent = content + "\n" + block
	}

	// Write atomically via a temp file in the same directory
	dir := filepath.Dir(confPath)
	tmpFile, err := os.CreateTemp(dir, "postgresql.conf.tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary postgresql.conf: %w", err)
	}
	tmpName := tmpFile.Name()

	if _, err := tmpFile.WriteString(newContent); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("failed to write temporary postgresql.conf: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("failed to sync temporary postgresql.conf: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("failed to close temporary postgresql.conf: %w", err)
	}

	if err := os.Rename(tmpName, confPath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("failed to replace postgresql.conf: %w", err)
	}

	return nil
}
