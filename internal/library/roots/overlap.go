// Package roots implements library root and managed intake path validation,
// canonical overlap detection, and configuration services (ADR-0002, ADR-0003, ADR-0019).
package roots

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/storage"
)

// ErrPathOverlap is returned when a candidate root or intake path is equal to,
// inside, or contains an existing configured library root or intake path.
var ErrPathOverlap = errors.New("path overlaps an existing library root or intake")

// PathKind identifies whether an ExistingPath came from a library root or managed intake.
type PathKind string

const (
	// PathKindRoot identifies a configured library root directory.
	PathKindRoot PathKind = "root"
	// PathKindIntake identifies a managed library's configured intake directory.
	PathKindIntake PathKind = "intake"
)

// ExistingPath describes a configured library root or intake path for overlap checking.
type ExistingPath struct {
	LibraryID string
	RootID    string
	Kind      PathKind
	Path      []byte
}

// CollectOptions excludes specific existing entries when validating a replacement
// (such as replacing a library's own intake path or relocating a specific root).
type CollectOptions struct {
	ExcludeRootID          string
	ExcludeIntakeLibraryID string
}

// CollectExistingPaths loads all configured root and intake paths from repo,
// optionally excluding a target root or intake being replaced.
func CollectExistingPaths(ctx context.Context, repo storage.LibraryRepository, opts CollectOptions) ([]ExistingPath, error) {
	if repo == nil {
		return nil, errors.New("library repository is required")
	}

	roots, err := repo.ListAllLibraryRoots(ctx)
	if err != nil {
		return nil, err
	}
	libs, err := repo.ListLibraries(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]ExistingPath, 0, len(roots)+len(libs))
	for _, r := range roots {
		if opts.ExcludeRootID != "" && r.ID == opts.ExcludeRootID {
			continue
		}
		out = append(out, ExistingPath{
			LibraryID: r.LibraryID,
			RootID:    r.ID,
			Kind:      PathKindRoot,
			Path:      slices.Clone(r.Path.Bytes),
		})
	}
	for _, lib := range libs {
		if lib.IntakePath == nil {
			continue
		}
		if opts.ExcludeIntakeLibraryID != "" && lib.ID == opts.ExcludeIntakeLibraryID {
			continue
		}
		out = append(out, ExistingPath{
			LibraryID: lib.ID,
			Kind:      PathKindIntake,
			Path:      slices.Clone(lib.IntakePath.Bytes),
		})
	}
	return out, nil
}

// ResolveDirectoryPath validates raw as a non-empty, NUL-free path, canonicalizes it
// through paths.CanonicalPathBytes without modifying the filesystem, and rejects any
// existing non-directory path. Missing directories are permitted.
func ResolveDirectoryPath(raw []byte) ([]byte, error) {
	if len(raw) == 0 || bytes.IndexByte(raw, 0) >= 0 {
		return nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, paths.ErrInvalidRawPath)
	}
	canon, err := paths.CanonicalPathBytes(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	fi, err := os.Lstat(string(canon))
	if err != nil {
		if os.IsNotExist(err) {
			return canon, nil
		}
		return nil, fmt.Errorf("%w: inspecting path %q: %w", app.ErrInvalidInput, string(canon), err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("%w: path %q is not a directory", app.ErrInvalidInput, string(canon))
	}
	return canon, nil
}

// SplitCanonicalComponents splits a clean canonical POSIX absolute path into its
// path components. The root path "/" yields an empty slice.
func SplitCanonicalComponents(canonicalPath []byte) ([][]byte, error) {
	if len(canonicalPath) == 0 || bytes.IndexByte(canonicalPath, 0) >= 0 || canonicalPath[0] != '/' {
		return nil, fmt.Errorf("%w: canonical path must be non-empty, absolute, and NUL-free", app.ErrInvalidInput)
	}
	var comps [][]byte
	start := 1
	for start <= len(canonicalPath) {
		rel := bytes.IndexByte(canonicalPath[start:], '/')
		var seg []byte
		if rel < 0 {
			seg = canonicalPath[start:]
			start = len(canonicalPath) + 1
		} else {
			seg = canonicalPath[start : start+rel]
			start += rel + 1
		}
		if len(seg) == 0 {
			continue
		}
		if len(seg) == 1 && seg[0] == '.' || len(seg) == 2 && seg[0] == '.' && seg[1] == '.' {
			return nil, fmt.Errorf("%w: canonical path cannot contain dot segments", app.ErrInvalidInput)
		}
		comps = append(comps, seg)
	}
	return comps, nil
}

// CanonicalPathsOverlap reports whether two canonical absolute POSIX paths are equal,
// or one contains the other, comparing by path component rather than string prefix.
func CanonicalPathsOverlap(a, b []byte) (bool, error) {
	ca, err := SplitCanonicalComponents(a)
	if err != nil {
		return false, err
	}
	cb, err := SplitCanonicalComponents(b)
	if err != nil {
		return false, err
	}
	return hasComponentPrefix(ca, cb) || hasComponentPrefix(cb, ca), nil
}

func hasComponentPrefix(prefix, target [][]byte) bool {
	if len(prefix) > len(target) {
		return false
	}
	for i := range prefix {
		if !bytes.Equal(prefix[i], target[i]) {
			return false
		}
	}
	return true
}

// CheckPathOverlap resolves candidateRaw to its canonical POSIX path and rejects it
// with app.ErrConflict / ErrPathOverlap if it equals, lies inside, or contains any
// canonicalized path in existing.
func CheckPathOverlap(candidateRaw []byte, existing []ExistingPath) ([]byte, error) {
	canonCandidate, err := ResolveDirectoryPath(candidateRaw)
	if err != nil {
		return nil, err
	}

	for _, item := range existing {
		canonExisting, err := paths.CanonicalPathBytes(item.Path)
		if err != nil {
			return nil, fmt.Errorf("canonicalizing existing %s path for library %s: %w", item.Kind, item.LibraryID, err)
		}
		overlap, err := CanonicalPathsOverlap(canonCandidate, canonExisting)
		if err != nil {
			return nil, err
		}
		if overlap {
			if item.LibraryID != "" {
				return nil, fmt.Errorf("%w: %w (%s in library %s)", app.ErrConflict, ErrPathOverlap, item.Kind, item.LibraryID)
			}
			return nil, fmt.Errorf("%w: %w (%s)", app.ErrConflict, ErrPathOverlap, item.Kind)
		}
	}

	return canonCandidate, nil
}
