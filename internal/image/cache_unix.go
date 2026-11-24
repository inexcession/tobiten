//go:build unix

package image

import "golang.org/x/sys/unix"

func canModifyDir(dir string) bool {
	return unix.Access(dir, unix.W_OK) == nil
}
