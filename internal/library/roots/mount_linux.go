//go:build linux

package roots

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func directoryLookupSemantics(dir string) lookupSemantics {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return lookupSemantics{
			caseInsensitive:        true,
			normInsensitive:        true,
			ignoreDefaultIgnorable: true,
		}
	}

	magic := uint32(st.Type)
	if magic == linuxExtSuperMagic {
		f, err := os.Open(dir)
		if err != nil {
			return linuxStatfsSemantics(magic, 0, err)
		}
		flags, ioctlErr := unix.IoctlGetInt(int(f.Fd()), unix.FS_IOC_GETFLAGS)
		_ = f.Close()
		return linuxStatfsSemantics(magic, flags, ioctlErr)
	}
	return linuxStatfsSemantics(magic, 0, nil)
}

func sameFileIdentityAndChangeTime(a, b os.FileInfo) bool {
	if !os.SameFile(a, b) {
		return false
	}
	sa, okA := a.Sys().(*syscall.Stat_t)
	sb, okB := b.Sys().(*syscall.Stat_t)
	if !okA || !okB {
		return true
	}
	return sa.Dev == sb.Dev && sa.Ino == sb.Ino && sa.Ctim == sb.Ctim
}
