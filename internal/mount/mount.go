// Package mount detects filesystem mount types using OS statfs calls.
package mount

import (
	"bytes"
	"errors"
	"fmt"
)

// Linux statfs filesystem magic numbers shared across mount detection and
// path-lookup semantics.
const (
	LinuxNFSSuperMagic   uint32 = 0x6969
	LinuxSMBSuperMagic   uint32 = 0x517B
	LinuxCIFSMagicNumber uint32 = 0xFF534D42
	LinuxSMB2MagicNumber uint32 = 0xFE534D42
	LinuxCodaSuperMagic  uint32 = 0x73757245
	LinuxAFSSuperMagic   uint32 = 0x5346414F
	LinuxCephSuperMagic  uint32 = 0x00C36400
	LinuxExtSuperMagic   uint32 = 0xEF53
	LinuxBtrfsSuperMagic uint32 = 0x9123683E
	LinuxXFSSuperMagic   uint32 = 0x58465342
	LinuxTmpfsMagic      uint32 = 0x01021994
	LinuxZFSSuperMagic   uint32 = 0x2FC12FC1
)

// ErrInvalidPath is returned when a raw path is empty or contains a NUL byte.
var ErrInvalidPath = errors.New("mount: path must be non-empty and free of NUL bytes")

// FilesystemInfo contains information about a filesystem mount.
type FilesystemInfo struct {
	Type      string
	IsNetwork bool
}

// DetectFilesystem returns filesystem information for path using statfs.
func DetectFilesystem(path string) (FilesystemInfo, error) {
	return statfsInfo(path)
}

// DetectFilesystemBytes returns filesystem information for a raw OS byte path
// without requiring UTF-8 validity.
func DetectFilesystemBytes(rawPath []byte) (FilesystemInfo, error) {
	if len(rawPath) == 0 || bytes.IndexByte(rawPath, 0) >= 0 {
		return FilesystemInfo{}, fmt.Errorf("%w", ErrInvalidPath)
	}
	return statfsInfo(string(rawPath))
}
