//go:build linux

package mount

import (
	"fmt"
	"syscall"
)

func statfsInfo(path string) (FilesystemInfo, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return FilesystemInfo{}, err
	}

	magic := uint32(stat.Type)
	switch magic {
	case LinuxNFSSuperMagic:
		return FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
	case LinuxSMBSuperMagic:
		return FilesystemInfo{Type: "smb", IsNetwork: true}, nil
	case LinuxCIFSMagicNumber:
		return FilesystemInfo{Type: "cifs", IsNetwork: true}, nil
	case LinuxSMB2MagicNumber:
		return FilesystemInfo{Type: "smb2", IsNetwork: true}, nil
	case LinuxAFSSuperMagic:
		return FilesystemInfo{Type: "afs", IsNetwork: true}, nil
	case LinuxCephSuperMagic:
		return FilesystemInfo{Type: "ceph", IsNetwork: true}, nil
	case LinuxCodaSuperMagic:
		return FilesystemInfo{Type: "coda", IsNetwork: true}, nil
	case LinuxExtSuperMagic:
		return FilesystemInfo{Type: "ext4", IsNetwork: false}, nil
	case LinuxBtrfsSuperMagic:
		return FilesystemInfo{Type: "btrfs", IsNetwork: false}, nil
	case LinuxXFSSuperMagic:
		return FilesystemInfo{Type: "xfs", IsNetwork: false}, nil
	case LinuxTmpfsMagic:
		return FilesystemInfo{Type: "tmpfs", IsNetwork: false}, nil
	case LinuxZFSSuperMagic:
		return FilesystemInfo{Type: "zfs", IsNetwork: false}, nil
	default:
		return FilesystemInfo{Type: fmt.Sprintf("0x%x", magic), IsNetwork: false}, nil
	}
}
