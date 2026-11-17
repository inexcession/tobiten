//go:build linux

package roots

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	linuxExtSuperMagic   uint32 = 0xEF53
	linuxTmpfsMagic      uint32 = 0x01021994
	linuxBtrfsSuperMagic uint32 = 0x9123683E
	linuxCasefoldFlag           = 0x40000000 // FS_CASEFOLD_FL
)

func directoryLookupSemantics(dir string) lookupSemantics {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return lookupSemantics{caseInsensitive: true, normInsensitive: true}
	}

	switch uint32(st.Type) {
	case linuxExtSuperMagic:
		f, err := os.Open(dir)
		if err != nil {
			return lookupSemantics{caseInsensitive: true, normInsensitive: true}
		}
		flags, ioctlErr := unix.IoctlGetInt(int(f.Fd()), unix.FS_IOC_GETFLAGS)
		_ = f.Close()
		if ioctlErr != nil || (flags&linuxCasefoldFlag) != 0 {
			return lookupSemantics{caseInsensitive: true, normInsensitive: true}
		}
		return lookupSemantics{caseInsensitive: false, normInsensitive: false}
	case linuxTmpfsMagic, linuxBtrfsSuperMagic:
		return lookupSemantics{caseInsensitive: false, normInsensitive: false}
	default:
		// Network, FUSE, overlay, or foreign mounts can alias case or normalization
		// depending on backing storage or server settings; fail closed without probing.
		return lookupSemantics{caseInsensitive: true, normInsensitive: true}
	}
}
