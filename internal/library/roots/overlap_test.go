package roots

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/library/paths"
)

type treeEntrySnapshot struct {
	relPath string
	mode    fs.FileMode
	size    int64
	modTime time.Time
	content []byte
	target  string
}

func snapshotTree(t *testing.T, root string) []treeEntrySnapshot {
	t.Helper()
	var entries []treeEntrySnapshot
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		fi, err := os.Lstat(p)
		if err != nil {
			return err
		}
		snap := treeEntrySnapshot{
			relPath: rel,
			mode:    fi.Mode(),
			size:    fi.Size(),
			modTime: fi.ModTime(),
		}
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			snap.target = target
		case !fi.IsDir():
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			snap.content = data
		}
		entries = append(entries, snap)
		return nil
	})
	if err != nil {
		t.Fatalf("snapshotTree(%q): %v", root, err)
	}
	return entries
}

func assertTreeUnchanged(t *testing.T, root string, before []treeEntrySnapshot) {
	t.Helper()
	after := snapshotTree(t, root)
	if len(after) != len(before) {
		t.Fatalf("tree entry count changed: before=%d after=%d", len(before), len(after))
	}
	for i := range before {
		b, a := before[i], after[i]
		if b.relPath != a.relPath || b.mode != a.mode || b.size != a.size || !b.modTime.Equal(a.modTime) ||
			b.target != a.target || !bytes.Equal(b.content, a.content) {
			t.Fatalf("fixture tree mutated at %q: before=%+v after=%+v", b.relPath, b, a)
		}
	}
}

func TestCanonicalPathsOverlap_ComponentBoundaries(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		a    []byte
		b    []byte
		want bool
	}{
		{name: "equal paths", a: []byte("/mnt/a"), b: []byte("/mnt/a"), want: true},
		{name: "child inside parent", a: []byte("/mnt/a/sub"), b: []byte("/mnt/a"), want: true},
		{name: "deep child inside parent", a: []byte("/mnt/a/sub/deep"), b: []byte("/mnt/a"), want: true},
		{name: "parent contains child", a: []byte("/mnt/a"), b: []byte("/mnt/a/sub"), want: true},
		{name: "root contains everything", a: []byte("/"), b: []byte("/mnt/a"), want: true},
		{name: "both root", a: []byte("/"), b: []byte("/"), want: true},
		{name: "sibling with name prefix accepted", a: []byte("/mnt/a"), b: []byte("/mnt/ab"), want: false},
		{name: "sibling reversed accepted", a: []byte("/mnt/comics-2"), b: []byte("/mnt/comics"), want: false},
		{name: "sibling under same parent accepted", a: []byte("/mnt/a/vol1"), b: []byte("/mnt/a/vol2"), want: false},
		{name: "disjoint top levels accepted", a: []byte("/mnt/manga"), b: []byte("/srv/manga"), want: false},
		{name: "invalid utf8 nested overlaps", a: []byte{'/', 'm', 'n', 't', '/', 0xff}, b: []byte{'/', 'm', 'n', 't', '/', 0xff, '/', 'x'}, want: true},
		{name: "invalid utf8 distinct accepted", a: []byte{'/', 'm', 'n', 't', '/', 0xff}, b: []byte{'/', 'm', 'n', 't', '/', 0xfe}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CanonicalPathsOverlap(tc.a, tc.b)
			if err != nil {
				t.Fatalf("CanonicalPathsOverlap(%q, %q) err = %v", tc.a, tc.b, err)
			}
			if got != tc.want {
				t.Fatalf("CanonicalPathsOverlap(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}

	for _, bad := range [][]byte{nil, []byte("relative/path"), []byte("/mnt/\x00/a"), []byte("/mnt/../a")} {
		if _, err := SplitCanonicalComponents(bad); !errors.Is(err, app.ErrInvalidInput) {
			t.Fatalf("SplitCanonicalComponents(%q) err = %v, want ErrInvalidInput", bad, err)
		}
	}
}

func TestCheckPathOverlap_SymlinksRelativeDotDotAndTreeUntouched(t *testing.T) {
	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath(base): %v", err)
	}

	// Build a realistic fixture tree with files, subdirs, and symlinks.
	mntDir := filepath.Join(canonBase, "mnt")
	dirA := filepath.Join(mntDir, "a")
	dirASub := filepath.Join(dirA, "sub")
	dirAB := filepath.Join(mntDir, "ab")
	dirIntake := filepath.Join(mntDir, "intake")
	if err := os.MkdirAll(dirASub, 0o755); err != nil {
		t.Fatalf("MkdirAll(dirASub): %v", err)
	}
	if err := os.MkdirAll(dirAB, 0o755); err != nil {
		t.Fatalf("MkdirAll(dirAB): %v", err)
	}
	if err := os.MkdirAll(dirIntake, 0o755); err != nil {
		t.Fatalf("MkdirAll(dirIntake): %v", err)
	}

	sampleFile := filepath.Join(dirASub, "chapter01.cbz")
	if err := os.WriteFile(sampleFile, []byte("PK\x03\x04fixture-cbz-bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile(sampleFile): %v", err)
	}
	regFileRoot := filepath.Join(mntDir, "not-a-dir.cbz")
	if err := os.WriteFile(regFileRoot, []byte("file"), 0o600); err != nil {
		t.Fatalf("WriteFile(regFileRoot): %v", err)
	}

	// Backdate mtime to make any accidental directory/file mtime bump obvious.
	past := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, p := range []string{sampleFile, regFileRoot, dirASub, dirA, dirAB, dirIntake, mntDir} {
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatalf("Chtimes(%q): %v", p, err)
		}
	}

	aliasA := filepath.Join(canonBase, "alias-a")
	if err := os.Symlink(dirA, aliasA); err != nil {
		t.Fatalf("Symlink(aliasA): %v", err)
	}
	aliasMnt := filepath.Join(canonBase, "alias-mnt")
	if err := os.Symlink(mntDir, aliasMnt); err != nil {
		t.Fatalf("Symlink(aliasMnt): %v", err)
	}
	aliasFile := filepath.Join(canonBase, "alias-file")
	if err := os.Symlink(regFileRoot, aliasFile); err != nil {
		t.Fatalf("Symlink(aliasFile): %v", err)
	}

	before := snapshotTree(t, canonBase)
	t.Chdir(canonBase)

	existing := []ExistingPath{
		{LibraryID: "lib-1", RootID: "root-1", Kind: PathKindRoot, Path: []byte(dirA)},
		{LibraryID: "lib-1", Kind: PathKindIntake, Path: []byte(dirIntake)},
	}

	rejectCases := []struct {
		name string
		raw  []byte
	}{
		{name: "direct equal root", raw: []byte(dirA)},
		{name: "direct child of root", raw: []byte(dirASub)},
		{name: "direct parent of root", raw: []byte(mntDir)},
		{name: "symlink equal root", raw: []byte(aliasA)},
		{name: "symlink child of root", raw: []byte(filepath.Join(aliasA, "sub"))},
		{name: "symlink ancestor child of root", raw: []byte(filepath.Join(aliasMnt, "a", "new-child"))},
		{name: "symlink parent containing root", raw: []byte(aliasMnt)},
		{name: "relative equal root", raw: []byte("./mnt/a")},
		{name: "relative dotdot into root", raw: []byte("mnt/ab/../a/sub")},
		{name: "relative symlink with dotdot", raw: []byte("./alias-a/../a")},
		{name: "equal intake", raw: []byte(dirIntake)},
		{name: "inside intake via symlink", raw: []byte(filepath.Join(aliasMnt, "intake", "drop"))},
	}

	for _, tc := range rejectCases {
		t.Run("reject_"+tc.name, func(t *testing.T) {
			_, err := CheckPathOverlap(tc.raw, existing)
			if !errors.Is(err, app.ErrConflict) || !errors.Is(err, ErrPathOverlap) {
				t.Fatalf("CheckPathOverlap(%q) err = %v, want ErrConflict + ErrPathOverlap", tc.raw, err)
			}
		})
	}

	// Non-directory files (direct or via symlink) fail with ErrInvalidInput.
	for _, badFile := range [][]byte{[]byte(regFileRoot), []byte(aliasFile)} {
		if _, err := CheckPathOverlap(badFile, existing); !errors.Is(err, app.ErrInvalidInput) {
			t.Fatalf("CheckPathOverlap(%q) err = %v, want ErrInvalidInput", badFile, err)
		}
	}

	// Siblings sharing a name prefix (/mnt/a vs /mnt/ab) and relative/symlink spellings of /mnt/ab succeed.
	for _, okPath := range [][]byte{
		[]byte(dirAB),
		[]byte("./mnt/ab"),
		[]byte("mnt/a/../ab"),
		[]byte(filepath.Join(aliasMnt, "ab")),
		[]byte(filepath.Join(canonBase, "nonexistent-sibling")),
	} {
		canon, err := CheckPathOverlap(okPath, existing)
		if err != nil {
			t.Fatalf("CheckPathOverlap(%q) unexpected error: %v", okPath, err)
		}
		if len(canon) == 0 || canon[0] != '/' {
			t.Fatalf("CheckPathOverlap(%q) returned non-absolute canonical path %q", okPath, canon)
		}
	}

	// Also verify existing non-canonical symlink path in ExistingPath catches direct candidate.
	aliasExisting := []ExistingPath{
		{LibraryID: "lib-alias", RootID: "root-alias", Kind: PathKindRoot, Path: slices.Clone([]byte(aliasA))},
	}
	if _, err := CheckPathOverlap([]byte(dirASub), aliasExisting); !errors.Is(err, ErrPathOverlap) {
		t.Fatalf("expected direct path inside existing symlink root to be rejected, got %v", err)
	}

	assertTreeUnchanged(t, canonBase, before)
}

func TestCheckPathOverlap_CaseAndNormalizationAliases(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	canonBase, err := paths.CanonicalPath(base)
	if err != nil {
		t.Fatalf("CanonicalPath(base): %v", err)
	}

	dirCaseUpper := filepath.Join(canonBase, "Manga")
	dirCaseUpperSub := filepath.Join(dirCaseUpper, "existing-sub")
	dirNFD := filepath.Join(canonBase, "Cafe\u0301")
	dirNFDSub := filepath.Join(dirNFD, "existing-sub")
	for _, d := range []string{dirCaseUpperSub, dirNFDSub} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", d, err)
		}
	}

	dirCaseLower := filepath.Join(canonBase, "manga")
	dirNFC := filepath.Join(canonBase, "Caf\u00e9")

	upperInfo, err := os.Lstat(dirCaseUpper)
	if err != nil {
		t.Fatalf("Lstat(%q): %v", dirCaseUpper, err)
	}
	lowerInfo, lowerErr := os.Lstat(dirCaseLower)
	caseAliased := lowerErr == nil && os.SameFile(upperInfo, lowerInfo)

	nfdInfo, err := os.Lstat(dirNFD)
	if err != nil {
		t.Fatalf("Lstat(%q): %v", dirNFD, err)
	}
	nfcInfo, nfcErr := os.Lstat(dirNFC)
	normAliased := nfcErr == nil && os.SameFile(nfdInfo, nfcInfo)

	if !caseAliased {
		if err := os.MkdirAll(filepath.Join(dirCaseLower, "existing-sub"), 0o755); err != nil {
			t.Fatalf("MkdirAll(distinct lowercase): %v", err)
		}
	}
	if !normAliased {
		if err := os.MkdirAll(filepath.Join(dirNFC, "existing-sub"), 0o755); err != nil {
			t.Fatalf("MkdirAll(distinct NFC): %v", err)
		}
	}

	before := snapshotTree(t, canonBase)

	existingParents := []ExistingPath{
		{LibraryID: "lib-case", RootID: "r-case", Kind: PathKindRoot, Path: []byte(dirCaseUpper)},
		{LibraryID: "lib-nfd", RootID: "r-nfd", Kind: PathKindRoot, Path: []byte(dirNFD)},
	}
	existingChildren := []ExistingPath{
		{LibraryID: "lib-case-sub", RootID: "r-case-sub", Kind: PathKindRoot, Path: []byte(dirCaseUpperSub)},
		{LibraryID: "lib-nfd-sub", RootID: "r-nfd-sub", Kind: PathKindRoot, Path: []byte(dirNFDSub)},
	}

	if caseAliased {
		for _, candidate := range [][]byte{
			[]byte(dirCaseLower),
			[]byte(filepath.Join(dirCaseLower, "existing-sub")),
			[]byte(filepath.Join(dirCaseLower, "future-sub")),
		} {
			if _, err := CheckPathOverlap(candidate, existingParents); !errors.Is(err, ErrPathOverlap) {
				t.Fatalf("case-aliased candidate %q against parent: err = %v, want ErrPathOverlap", candidate, err)
			}
		}
		if _, err := CheckPathOverlap([]byte(dirCaseLower), existingChildren); !errors.Is(err, ErrPathOverlap) {
			t.Fatalf("case-aliased ancestor %q against child: err = %v, want ErrPathOverlap", dirCaseLower, err)
		}
	} else {
		canonLower, err := CheckPathOverlap([]byte(dirCaseLower), existingParents)
		if err != nil {
			t.Fatalf("distinct case directory %q rejected on case-sensitive fs: %v", dirCaseLower, err)
		}
		if string(canonLower) != dirCaseLower {
			t.Fatalf("raw bytes changed: got %q want %q", canonLower, dirCaseLower)
		}
		if _, err := CheckPathOverlap([]byte(filepath.Join(dirCaseLower, "existing-sub")), []ExistingPath{
			{LibraryID: "lib-lower", RootID: "r-lower", Kind: PathKindRoot, Path: canonLower},
		}); !errors.Is(err, ErrPathOverlap) {
			t.Fatalf("descendant under distinct lowercase dir must still be rejected, got %v", err)
		}
	}

	if normAliased {
		for _, candidate := range [][]byte{
			[]byte(dirNFC),
			[]byte(filepath.Join(dirNFC, "existing-sub")),
			[]byte(filepath.Join(dirNFC, "future-sub")),
		} {
			if _, err := CheckPathOverlap(candidate, existingParents); !errors.Is(err, ErrPathOverlap) {
				t.Fatalf("normalization-aliased candidate %q against parent: err = %v, want ErrPathOverlap", candidate, err)
			}
		}
		if _, err := CheckPathOverlap([]byte(dirNFC), existingChildren); !errors.Is(err, ErrPathOverlap) {
			t.Fatalf("normalization-aliased ancestor %q against child: err = %v, want ErrPathOverlap", dirNFC, err)
		}
	} else {
		canonNFC, err := CheckPathOverlap([]byte(dirNFC), existingParents)
		if err != nil {
			t.Fatalf("distinct NFC directory %q rejected on normalization-sensitive fs: %v", dirNFC, err)
		}
		if string(canonNFC) != dirNFC {
			t.Fatalf("raw bytes changed: got %q want %q", canonNFC, dirNFC)
		}
		if _, err := CheckPathOverlap([]byte(filepath.Join(dirNFC, "existing-sub")), []ExistingPath{
			{LibraryID: "lib-nfc", RootID: "r-nfc", Kind: PathKindRoot, Path: canonNFC},
		}); !errors.Is(err, ErrPathOverlap) {
			t.Fatalf("descendant under distinct NFC dir must still be rejected, got %v", err)
		}
	}

	// Distinct sibling sharing a prefix remains accepted regardless of case/normalization behavior.
	if _, err := CheckPathOverlap([]byte(filepath.Join(canonBase, "manga2")), existingParents); err != nil {
		t.Fatalf("distinct sibling manga2 rejected: %v", err)
	}

	assertTreeUnchanged(t, canonBase, before)
}
