//go:build linux

package embedded

import (
	"fmt"
	"syscall"
)

const (
	nfsSuperMagic   uint32 = 0x6969
	smbSuperMagic   uint32 = 0x517B
	cifsMagicNumber uint32 = 0xFF534D42
	smb2MagicNumber uint32 = 0xFE534D42
	codaSuperMagic  uint32 = 0x73757245
	afsSuperMagic   uint32 = 0x5346414F
	cephSuperMagic  uint32 = 0x00C36400
)

func statfsInfo(path string) (FilesystemInfo, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return FilesystemInfo{}, err
	}

	magic := uint32(stat.Type)
	switch magic {
	case nfsSuperMagic:
		return FilesystemInfo{Type: "nfs", IsNetwork: true}, nil
	case smbSuperMagic:
		return FilesystemInfo{Type: "smb", IsNetwork: true}, nil
	case cifsMagicNumber:
		return FilesystemInfo{Type: "cifs", IsNetwork: true}, nil
	case smb2MagicNumber:
		return FilesystemInfo{Type: "smb2", IsNetwork: true}, nil
	case afsSuperMagic:
		return FilesystemInfo{Type: "afs", IsNetwork: true}, nil
	case cephSuperMagic:
		return FilesystemInfo{Type: "ceph", IsNetwork: true}, nil
	case codaSuperMagic:
		return FilesystemInfo{Type: "coda", IsNetwork: true}, nil
	case 0xEF53:
		return FilesystemInfo{Type: "ext4", IsNetwork: false}, nil
	case 0x9123683E:
		return FilesystemInfo{Type: "btrfs", IsNetwork: false}, nil
	case 0x58465342:
		return FilesystemInfo{Type: "xfs", IsNetwork: false}, nil
	case 0x01021994:
		return FilesystemInfo{Type: "tmpfs", IsNetwork: false}, nil
	case 0x2FC12FC1:
		return FilesystemInfo{Type: "zfs", IsNetwork: false}, nil
	default:
		return FilesystemInfo{Type: fmt.Sprintf("0x%x", magic), IsNetwork: false}, nil
	}
}
