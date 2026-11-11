//go:build darwin

package embedded

import (
	"strings"
	"syscall"
)

func statfsInfo(path string) (FilesystemInfo, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return FilesystemInfo{}, err
	}

	nameBytes := make([]byte, 0, len(stat.Fstypename))
	for _, b := range stat.Fstypename {
		if b == 0 {
			break
		}
		nameBytes = append(nameBytes, byte(b))
	}
	typeName := strings.ToLower(string(nameBytes))

	// On Darwin, MNT_LOCAL (0x1000) indicates local filesystem storage.
	const mntLocal = 0x1000
	isLocal := (stat.Flags & mntLocal) != 0

	isNetwork := false
	switch typeName {
	case "nfs", "smbfs", "cifs", "afpfs":
		isNetwork = true
	default:
		if !isLocal && typeName != "" {
			isNetwork = true
		}
	}

	return FilesystemInfo{
		Type:      typeName,
		IsNetwork: isNetwork,
	}, nil
}
