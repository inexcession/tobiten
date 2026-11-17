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
	"unicode/utf8"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/library/paths"
	"github.com/inexcession/tobiten/internal/storage"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
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
	canon, _, err := resolveAndInspectDirectory(raw)
	return canon, err
}

func resolveAndInspectDirectory(raw []byte) ([]byte, *pathIdentityInfo, error) {
	if len(raw) == 0 || bytes.IndexByte(raw, 0) >= 0 {
		return nil, nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, paths.ErrInvalidRawPath)
	}
	canon, err := paths.CanonicalPathBytes(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", app.ErrInvalidInput, err)
	}
	info, err := inspectPathIdentity(canon, true)
	if err != nil {
		return nil, nil, err
	}
	return canon, info, nil
}

type lookupSemantics struct {
	caseInsensitive bool
	normInsensitive bool
}

type pathIdentityInfo struct {
	ancestors []os.FileInfo
	leafPath  string
	tail      [][]byte
}

func (p *pathIdentityInfo) leaf() os.FileInfo {
	return p.ancestors[len(p.ancestors)-1]
}

func inspectPathIdentity(canonicalPath []byte, requireDir bool) (*pathIdentityInfo, error) {
	comps, err := SplitCanonicalComponents(canonicalPath)
	if err != nil {
		return nil, err
	}

	rootInfo, err := os.Lstat("/")
	if err != nil {
		return nil, fmt.Errorf("%w: inspecting root path %q: %w", app.ErrInvalidInput, "/", err)
	}

	ancestors := make([]os.FileInfo, 1, 1+len(comps))
	ancestors[0] = rootInfo
	cur := "/"
	leafPath := "/"
	var tail [][]byte

	for i, comp := range comps {
		if cur == "/" {
			cur = "/" + string(comp)
		} else {
			cur = cur + "/" + string(comp)
		}

		fi, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) {
				tail = comps[i:]
				break
			}
			return nil, fmt.Errorf("%w: inspecting path %q: %w", app.ErrInvalidInput, cur, err)
		}
		if requireDir && !fi.IsDir() {
			return nil, fmt.Errorf("%w: path %q is not a directory", app.ErrInvalidInput, cur)
		}
		ancestors = append(ancestors, fi)
		leafPath = cur
	}

	return &pathIdentityInfo{
		ancestors: ancestors,
		leafPath:  leafPath,
		tail:      tail,
	}, nil
}

func anySameFile(infos []os.FileInfo, target os.FileInfo) bool {
	for _, fi := range infos {
		if os.SameFile(fi, target) {
			return true
		}
	}
	return false
}

func identitiesOverlap(a, b *pathIdentityInfo) bool {
	switch {
	case len(a.tail) == 0 && len(b.tail) == 0:
		return anySameFile(a.ancestors, b.leaf()) || anySameFile(b.ancestors, a.leaf())
	case len(a.tail) == 0:
		return anySameFile(b.ancestors, a.leaf())
	case len(b.tail) == 0:
		return anySameFile(a.ancestors, b.leaf())
	default:
		if !os.SameFile(a.leaf(), b.leaf()) {
			return false
		}
		sem := directoryLookupSemantics(a.leafPath)
		return hasComponentPrefix(a.tail, b.tail, sem) || hasComponentPrefix(b.tail, a.tail, sem)
	}
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

// CanonicalPathsOverlap reports whether two canonical absolute POSIX paths refer to the
// same directory or one contains the other, using filesystem identity for existing
// directories/ancestors and component-prefix comparison for non-existent suffixes.
func CanonicalPathsOverlap(a, b []byte) (bool, error) {
	infoA, err := inspectPathIdentity(a, false)
	if err != nil {
		return false, err
	}
	infoB, err := inspectPathIdentity(b, false)
	if err != nil {
		return false, err
	}
	return identitiesOverlap(infoA, infoB), nil
}

func hasComponentPrefix(prefix, target [][]byte, sem lookupSemantics) bool {
	if len(prefix) > len(target) {
		return false
	}
	for i := range prefix {
		if !componentEqual(prefix[i], target[i], sem) {
			return false
		}
	}
	return true
}

func componentEqual(a, b []byte, sem lookupSemantics) bool {
	if bytes.Equal(a, b) {
		return true
	}
	if !sem.caseInsensitive && !sem.normInsensitive {
		return false
	}
	if !utf8.Valid(a) || !utf8.Valid(b) {
		if sem.caseInsensitive {
			return asciiFoldEqual(a, b)
		}
		return false
	}
	sa, sb := string(a), string(b)
	if sem.normInsensitive {
		sa = norm.NFC.String(sa)
		sb = norm.NFC.String(sb)
	}
	if sem.caseInsensitive {
		fold := cases.Fold()
		sa = fold.String(sa)
		sb = fold.String(sb)
		if sem.normInsensitive {
			sa = norm.NFC.String(sa)
			sb = norm.NFC.String(sb)
		}
	}
	return sa == sb
}

func asciiFoldEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// CheckPathOverlap resolves candidateRaw to its canonical POSIX path and rejects it
// with app.ErrConflict / ErrPathOverlap if it equals, lies inside, or contains any
// canonicalized path in existing (including filesystem case/normalization aliases).
func CheckPathOverlap(candidateRaw []byte, existing []ExistingPath) ([]byte, error) {
	canonCandidate, candInfo, err := resolveAndInspectDirectory(candidateRaw)
	if err != nil {
		return nil, err
	}

	for _, item := range existing {
		canonExisting, err := paths.CanonicalPathBytes(item.Path)
		if err != nil {
			return nil, fmt.Errorf("canonicalizing existing %s path for library %s: %w", item.Kind, item.LibraryID, err)
		}
		itemInfo, err := inspectPathIdentity(canonExisting, false)
		if err != nil {
			return nil, err
		}
		if identitiesOverlap(candInfo, itemInfo) {
			if item.LibraryID != "" {
				return nil, fmt.Errorf("%w: %w (%s in library %s)", app.ErrConflict, ErrPathOverlap, item.Kind, item.LibraryID)
			}
			return nil, fmt.Errorf("%w: %w (%s)", app.ErrConflict, ErrPathOverlap, item.Kind)
		}
	}

	return canonCandidate, nil
}
