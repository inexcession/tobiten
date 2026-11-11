package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	apiPackagePrefix     = "github.com/inexcession/tobiten/internal/api"
	storagePackagePrefix = "github.com/inexcession/tobiten/internal/storage"
)

type packageInfo struct {
	Dir        string   `json:"Dir"`
	ImportPath string   `json:"ImportPath"`
	Name       string   `json:"Name"`
	Imports    []string `json:"Imports"`
	Deps       []string `json:"Deps"`
	Error      *struct {
		Err string `json:"Err"`
	} `json:"Error,omitempty"`
}

type boundaryViolation struct {
	AdapterPackage string
	StoragePackage string
	ImportChain    []string
}

func (v boundaryViolation) String() string {
	if len(v.ImportChain) > 0 {
		return fmt.Sprintf("adapter package %q imports forbidden storage package %q via chain: %s",
			v.AdapterPackage, v.StoragePackage, strings.Join(v.ImportChain, " -> "))
	}
	return fmt.Sprintf("adapter package %q imports forbidden storage package %q",
		v.AdapterPackage, v.StoragePackage)
}

func isAPIPackage(pkg string) bool {
	return pkg == apiPackagePrefix || strings.HasPrefix(pkg, apiPackagePrefix+"/")
}

func isStoragePackage(pkg string) bool {
	return pkg == storagePackagePrefix || strings.HasPrefix(pkg, storagePackagePrefix+"/")
}

func findRepoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate go.mod from working directory %s", dir)
		}
		dir = parent
	}
}

func findGoBinary() (string, error) {
	if goBin := os.Getenv("GO"); goBin != "" {
		if p, err := exec.LookPath(goBin); err == nil {
			return p, nil
		}
		return goBin, nil
	}

	if p, err := exec.LookPath("go"); err == nil {
		return p, nil
	}

	for _, candidate := range []string{
		"/opt/local/bin/go",
		"/usr/local/go/bin/go",
		"/usr/local/bin/go",
		"/usr/bin/go",
	} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("go binary not found on PATH or standard locations")
}

func runGoListDepsJSON(ctx context.Context, goBin, repoRoot, overlayPath, pattern string) ([]packageInfo, error) {
	args := []string{"list"}
	if overlayPath != "" {
		args = append(args, "-overlay", overlayPath)
	}
	args = append(args, "-deps", "-json", pattern)

	cmd := exec.CommandContext(ctx, goBin, args...)
	cmd.Dir = repoRoot

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("go list failed: %w (stderr: %s)", err, stderr.String())
	}

	var packages []packageInfo
	dec := json.NewDecoder(&stdout)
	for dec.More() {
		var p packageInfo
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("failed to decode package info: %w", err)
		}
		packages = append(packages, p)
	}

	return packages, nil
}

func runGoListDepsPlain(ctx context.Context, goBin, repoRoot, overlayPath, pattern string) ([]string, error) {
	args := []string{"list"}
	if overlayPath != "" {
		args = append(args, "-overlay", overlayPath)
	}
	args = append(args, "-deps", pattern)

	cmd := exec.CommandContext(ctx, goBin, args...)
	cmd.Dir = repoRoot

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("go list failed: %w (stderr: %s)", err, stderr.String())
	}

	var lines []string
	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan go list output: %w", err)
	}

	return lines, nil
}

func findImportChain(importsMap map[string][]string, start, target string) []string {
	if start == target {
		return []string{start}
	}

	queue := [][]string{{start}}
	visited := map[string]bool{start: true}

	for len(queue) > 0 {
		currPath := queue[0]
		queue = queue[1:]
		last := currPath[len(currPath)-1]

		if last == target {
			return currPath
		}

		for _, next := range importsMap[last] {
			if !visited[next] {
				visited[next] = true
				nextPath := make([]string, len(currPath)+1)
				copy(nextPath, currPath)
				nextPath[len(currPath)] = next
				queue = append(queue, nextPath)
			}
		}
	}

	return nil
}

func validateBoundaries(packages []packageInfo) []boundaryViolation {
	importsMap := make(map[string][]string, len(packages))
	for _, p := range packages {
		importsMap[p.ImportPath] = p.Imports
	}

	var violations []boundaryViolation
	seen := make(map[string]bool)

	for _, p := range packages {
		if !isAPIPackage(p.ImportPath) {
			continue
		}

		// Check direct imports first.
		for _, imp := range p.Imports {
			if isStoragePackage(imp) {
				key := p.ImportPath + "=>" + imp
				if !seen[key] {
					seen[key] = true
					violations = append(violations, boundaryViolation{
						AdapterPackage: p.ImportPath,
						StoragePackage: imp,
						ImportChain:    []string{p.ImportPath, imp},
					})
				}
			}
		}

		// Check transitive dependencies via Deps.
		for _, dep := range p.Deps {
			if isStoragePackage(dep) {
				key := p.ImportPath + "=>" + dep
				if !seen[key] {
					seen[key] = true
					chain := findImportChain(importsMap, p.ImportPath, dep)
					if len(chain) == 0 {
						chain = []string{p.ImportPath, dep}
					}
					violations = append(violations, boundaryViolation{
						AdapterPackage: p.ImportPath,
						StoragePackage: dep,
						ImportChain:    chain,
					})
				}
			}
		}
	}

	return violations
}

func checkAPIDependencies(ctx context.Context, goBin, repoRoot, overlayPath string) ([]packageInfo, []boundaryViolation, error) {
	pkgs, err := runGoListDepsJSON(ctx, goBin, repoRoot, overlayPath, "./internal/api/...")
	if err != nil {
		return nil, nil, err
	}
	violations := validateBoundaries(pkgs)
	return pkgs, violations, nil
}

func createOverlayConfigFile(t testing.TB, replacements map[string]string) string {
	t.Helper()

	type overlayJSON struct {
		Replace map[string]string `json:"Replace"`
	}

	payload, err := json.Marshal(overlayJSON{Replace: replacements})
	if err != nil {
		t.Fatalf("failed to marshal overlay: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "tobiten-overlay-*.json")
	if err != nil {
		t.Fatalf("failed to create overlay file: %v", err)
	}

	if _, err := tmpFile.Write(payload); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		t.Fatalf("failed to write overlay file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpFile.Name())
		t.Fatalf("failed to close overlay file: %v", err)
	}

	t.Cleanup(func() {
		_ = os.Remove(tmpFile.Name())
	})

	return tmpFile.Name()
}

func TestImportBoundary_CurrentTree(t *testing.T) {
	goBin, err := findGoBinary()
	if err != nil {
		t.Fatalf("go binary not found: %v", err)
	}
	repoRoot := findRepoRoot(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pkgs, violations, err := checkAPIDependencies(ctx, goBin, repoRoot, "")
	if err != nil {
		t.Fatalf("checkAPIDependencies failed on current tree: %v", err)
	}

	var apiPkgs []string
	for _, p := range pkgs {
		if isAPIPackage(p.ImportPath) {
			apiPkgs = append(apiPkgs, p.ImportPath)
		}
	}

	if len(apiPkgs) == 0 {
		t.Fatal("expected at least one package under internal/api, found none")
	}

	// Verify that internal/api and internal/api/middleware are present.
	if !slices.Contains(apiPkgs, "github.com/inexcession/tobiten/internal/api") {
		t.Errorf("expected github.com/inexcession/tobiten/internal/api in checked packages, got %v", apiPkgs)
	}
	if !slices.Contains(apiPkgs, "github.com/inexcession/tobiten/internal/api/middleware") {
		t.Errorf("expected github.com/inexcession/tobiten/internal/api/middleware in checked packages, got %v", apiPkgs)
	}

	if len(violations) > 0 {
		for _, v := range violations {
			t.Errorf("unexpected boundary violation on current tree: %s", v)
		}
	}

	// Verify plain go list -deps output as well.
	lines, err := runGoListDepsPlain(ctx, goBin, repoRoot, "", "./internal/api/...")
	if err != nil {
		t.Fatalf("runGoListDepsPlain failed on current tree: %v", err)
	}

	for _, line := range lines {
		if isStoragePackage(line) {
			t.Errorf("storage package %q appeared in go list -deps ./internal/api/...", line)
		}
	}

	// Verify test-fixture isolation:
	// internal/module/registry_test.go imports storage/postgres/migrations and testdb,
	// but those test fixtures must NOT be part of internal/api production dependencies.
	for _, line := range lines {
		if strings.Contains(line, "migrations") || strings.Contains(line, "testdb") {
			t.Errorf("test fixture package %q leaked into internal/api production dependencies", line)
		}
	}
}

func TestImportBoundary_InjectedDirectImportFails(t *testing.T) {
	goBin, err := findGoBinary()
	if err != nil {
		t.Fatalf("go binary not found: %v", err)
	}
	repoRoot := findRepoRoot(t)

	// Create a temporary Go source file that injects an import of internal/storage.
	injectedSrc, err := os.CreateTemp("", "injected-*.go")
	if err != nil {
		t.Fatalf("failed to create temp source: %v", err)
	}
	defer os.Remove(injectedSrc.Name())

	code := []byte("package api\n\nimport _ \"github.com/inexcession/tobiten/internal/storage\"\n")
	if _, err := injectedSrc.Write(code); err != nil {
		_ = injectedSrc.Close()
		t.Fatalf("failed to write injected source: %v", err)
	}
	if err := injectedSrc.Close(); err != nil {
		t.Fatalf("failed to close injected source: %v", err)
	}

	targetPath := filepath.Join(repoRoot, "internal", "api", "injected_storage_test_probe.go")
	overlayFile := createOverlayConfigFile(t, map[string]string{
		targetPath: injectedSrc.Name(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, violations, err := checkAPIDependencies(ctx, goBin, repoRoot, overlayFile)
	if err != nil {
		t.Fatalf("checkAPIDependencies failed with overlay: %v", err)
	}

	if len(violations) == 0 {
		t.Fatal("expected boundary violation when storage import is injected, got none")
	}

	foundOffender := false
	for _, v := range violations {
		if v.AdapterPackage == "github.com/inexcession/tobiten/internal/api" &&
			v.StoragePackage == "github.com/inexcession/tobiten/internal/storage" {
			foundOffender = true
			break
		}
	}

	if !foundOffender {
		t.Errorf("expected violation from github.com/inexcession/tobiten/internal/api to github.com/inexcession/tobiten/internal/storage, got: %v", violations)
	}

	// Verify plain go list -deps also sees the violation.
	lines, err := runGoListDepsPlain(ctx, goBin, repoRoot, overlayFile, "./internal/api/...")
	if err != nil {
		t.Fatalf("runGoListDepsPlain failed with overlay: %v", err)
	}
	if !slices.Contains(lines, "github.com/inexcession/tobiten/internal/storage") {
		t.Errorf("expected github.com/inexcession/tobiten/internal/storage in go list -deps output, got %v", lines)
	}
}

func TestImportBoundary_InjectedSubpackageStorageImportFails(t *testing.T) {
	goBin, err := findGoBinary()
	if err != nil {
		t.Fatalf("go binary not found: %v", err)
	}
	repoRoot := findRepoRoot(t)

	// Simulate an adapter subpackage (e.g. internal/api/komga) injecting internal/storage/postgres.
	injectedSrc, err := os.CreateTemp("", "injected-komga-*.go")
	if err != nil {
		t.Fatalf("failed to create temp source: %v", err)
	}
	defer os.Remove(injectedSrc.Name())

	code := []byte("package komga\n\nimport _ \"github.com/inexcession/tobiten/internal/storage/postgres\"\n")
	if _, err := injectedSrc.Write(code); err != nil {
		_ = injectedSrc.Close()
		t.Fatalf("failed to write injected source: %v", err)
	}
	if err := injectedSrc.Close(); err != nil {
		t.Fatalf("failed to close injected source: %v", err)
	}

	targetPath := filepath.Join(repoRoot, "internal", "api", "komga", "adapter_probe.go")
	overlayFile := createOverlayConfigFile(t, map[string]string{
		targetPath: injectedSrc.Name(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, violations, err := checkAPIDependencies(ctx, goBin, repoRoot, overlayFile)
	if err != nil {
		t.Fatalf("checkAPIDependencies failed with overlay: %v", err)
	}

	if len(violations) == 0 {
		t.Fatal("expected boundary violation when adapter subpackage imports storage, got none")
	}

	foundOffender := false
	for _, v := range violations {
		if v.AdapterPackage == "github.com/inexcession/tobiten/internal/api/komga" &&
			v.StoragePackage == "github.com/inexcession/tobiten/internal/storage/postgres" {
			foundOffender = true
			break
		}
	}

	if !foundOffender {
		t.Errorf("expected violation from internal/api/komga to internal/storage/postgres, got: %v", violations)
	}
}

func TestImportBoundary_UnitLogic(t *testing.T) {
	tests := []struct {
		name          string
		packages      []packageInfo
		wantCount     int
		wantViolators []string
	}{
		{
			name: "clean dependencies",
			packages: []packageInfo{
				{
					ImportPath: "github.com/inexcession/tobiten/internal/api",
					Imports:    []string{"net/http", "github.com/inexcession/tobiten/internal/module"},
					Deps:       []string{"net/http", "github.com/inexcession/tobiten/internal/module", "github.com/inexcession/tobiten/internal/config"},
				},
				{
					ImportPath: "github.com/inexcession/tobiten/internal/module",
					Imports:    []string{"github.com/inexcession/tobiten/internal/config"},
					Deps:       []string{"github.com/inexcession/tobiten/internal/config"},
				},
			},
			wantCount: 0,
		},
		{
			name: "direct storage import",
			packages: []packageInfo{
				{
					ImportPath: "github.com/inexcession/tobiten/internal/api/komga",
					Imports:    []string{"github.com/inexcession/tobiten/internal/storage/postgres"},
					Deps:       []string{"github.com/inexcession/tobiten/internal/storage/postgres"},
				},
				{
					ImportPath: "github.com/inexcession/tobiten/internal/storage/postgres",
					Imports:    []string{},
					Deps:       []string{},
				},
			},
			wantCount:     1,
			wantViolators: []string{"github.com/inexcession/tobiten/internal/api/komga"},
		},
		{
			name: "transitive storage import via intermediate package",
			packages: []packageInfo{
				{
					ImportPath: "github.com/inexcession/tobiten/internal/api/opds",
					Imports:    []string{"github.com/inexcession/tobiten/internal/app/service"},
					Deps:       []string{"github.com/inexcession/tobiten/internal/app/service", "github.com/inexcession/tobiten/internal/storage"},
				},
				{
					ImportPath: "github.com/inexcession/tobiten/internal/app/service",
					Imports:    []string{"github.com/inexcession/tobiten/internal/storage"},
					Deps:       []string{"github.com/inexcession/tobiten/internal/storage"},
				},
				{
					ImportPath: "github.com/inexcession/tobiten/internal/storage",
					Imports:    []string{},
					Deps:       []string{},
				},
			},
			wantCount:     1,
			wantViolators: []string{"github.com/inexcession/tobiten/internal/api/opds"},
		},
		{
			name: "non-api package importing storage is allowed",
			packages: []packageInfo{
				{
					ImportPath: "github.com/inexcession/tobiten/cmd/tobitend",
					Imports:    []string{"github.com/inexcession/tobiten/internal/storage/postgres"},
					Deps:       []string{"github.com/inexcession/tobiten/internal/storage/postgres"},
				},
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			violations := validateBoundaries(tt.packages)
			if len(violations) != tt.wantCount {
				t.Fatalf("expected %d violations, got %d (%v)", tt.wantCount, len(violations), violations)
			}
			for i, wantV := range tt.wantViolators {
				if violations[i].AdapterPackage != wantV {
					t.Errorf("violation %d: expected adapter %s, got %s", i, wantV, violations[i].AdapterPackage)
				}
			}
		})
	}
}

func TestImportBoundary_ChainResolution(t *testing.T) {
	importsMap := map[string][]string{
		"github.com/inexcession/tobiten/internal/api/komga":   {"github.com/inexcession/tobiten/internal/app/library"},
		"github.com/inexcession/tobiten/internal/app/library": {"github.com/inexcession/tobiten/internal/storage/postgres"},
	}

	chain := findImportChain(importsMap,
		"github.com/inexcession/tobiten/internal/api/komga",
		"github.com/inexcession/tobiten/internal/storage/postgres",
	)

	expected := []string{
		"github.com/inexcession/tobiten/internal/api/komga",
		"github.com/inexcession/tobiten/internal/app/library",
		"github.com/inexcession/tobiten/internal/storage/postgres",
	}

	if !slices.Equal(chain, expected) {
		t.Errorf("expected chain %v, got %v", expected, chain)
	}

	v := boundaryViolation{
		AdapterPackage: chain[0],
		StoragePackage: chain[len(chain)-1],
		ImportChain:    chain,
	}
	s := v.String()
	if !strings.Contains(s, "adapter package \"github.com/inexcession/tobiten/internal/api/komga\"") {
		t.Errorf("expected adapter package in string, got %s", s)
	}
	if !strings.Contains(s, "forbidden storage package \"github.com/inexcession/tobiten/internal/storage/postgres\"") {
		t.Errorf("expected storage package in string, got %s", s)
	}
	if !strings.Contains(s, "via chain: github.com/inexcession/tobiten/internal/api/komga -> github.com/inexcession/tobiten/internal/app/library -> github.com/inexcession/tobiten/internal/storage/postgres") {
		t.Errorf("expected chain in string, got %s", s)
	}
}
