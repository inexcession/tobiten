// Package paths implements raw-byte POSIX path joining and file access helpers (ADR-0003 §5).
package paths

import (
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/inexcession/tobiten/internal/domain"
)

// ErrInvalidRawPath is returned when a raw byte path is empty or contains a NUL byte.
var ErrInvalidRawPath = errors.New("raw path must be non-empty and NUL-free")

// JoinRootRelative joins an absolute root byte path and a validated root-relative
// byte path using the POSIX separator without UTF-8 conversion, Unicode normalization,
// or lexical cleaning of valid filename bytes (ADR-0003 §5).
func JoinRootRelative(rootBytes, relBytes []byte) ([]byte, error) {
	if err := domain.ValidateRootPathBytes(rootBytes); err != nil {
		return nil, err
	}
	if err := domain.ValidateRelativePathBytes(relBytes); err != nil {
		return nil, err
	}

	root := rootBytes
	for len(root) > 1 && root[len(root)-1] == '/' {
		root = root[:len(root)-1]
	}

	needSlash := root[len(root)-1] != '/'
	extra := 0
	if needSlash {
		extra = 1
	}

	joined := make([]byte, 0, len(root)+extra+len(relBytes))
	joined = append(joined, root...)
	if needSlash {
		joined = append(joined, '/')
	}
	joined = append(joined, relBytes...)
	return joined, nil
}

// OpenRaw opens a file by its exact raw POSIX path bytes without UTF-8 validation
// or Unicode normalization.
func OpenRaw(rawPath []byte) (*os.File, error) {
	if len(rawPath) == 0 || bytes.IndexByte(rawPath, 0) >= 0 {
		return nil, ErrInvalidRawPath
	}
	return os.Open(string(rawPath))
}

// OpenRootRelative joins rootBytes and relBytes and opens the target file using the
// resulting raw POSIX bytes.
func OpenRootRelative(rootBytes, relBytes []byte) (*os.File, error) {
	joined, err := JoinRootRelative(rootBytes, relBytes)
	if err != nil {
		return nil, err
	}
	return os.Open(string(joined))
}

// ReadRootRelative joins rootBytes and relBytes and reads the target file's contents
// using the resulting raw POSIX bytes.
func ReadRootRelative(rootBytes, relBytes []byte) ([]byte, error) {
	f, err := OpenRootRelative(rootBytes, relBytes)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	return io.ReadAll(f)
}

// StatRootRelative joins rootBytes and relBytes and returns os.Stat info using the
// resulting raw POSIX bytes.
func StatRootRelative(rootBytes, relBytes []byte) (os.FileInfo, error) {
	joined, err := JoinRootRelative(rootBytes, relBytes)
	if err != nil {
		return nil, err
	}
	return os.Stat(string(joined))
}

// LstatRootRelative joins rootBytes and relBytes and returns os.Lstat info using the
// resulting raw POSIX bytes.
func LstatRootRelative(rootBytes, relBytes []byte) (os.FileInfo, error) {
	joined, err := JoinRootRelative(rootBytes, relBytes)
	if err != nil {
		return nil, err
	}
	return os.Lstat(string(joined))
}
