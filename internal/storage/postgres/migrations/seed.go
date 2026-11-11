package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	pathPkg "path"
	"sort"
	"strings"
)

// SeedsFS embeds all schema seed fixtures located in testdata/seeds.
//
//go:embed testdata/seeds/*.sql
var SeedsFS embed.FS

// SanitizeDumpSQL prepares a plain SQL script produced by pg_dump for execution
// via a standard SQL connection (such as pgx or database/sql).
//
// In particular:
//  1. It strips psql client meta-commands (lines starting with `\`, such as `\restrict`
//     and `\unrestrict` introduced in PostgreSQL 17/18).
//  2. It ensures the session search_path is reset at the end of execution, since pg_dump
//     sets `search_path = ”` during dumping for security.
func SanitizeDumpSQL(rawSQL string) string {
	lines := strings.Split(rawSQL, "\n")
	var builder strings.Builder
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, `\`) {
			// Skip psql backslash commands
			continue
		}
		builder.WriteString(line)
		builder.WriteByte('\n')
	}
	builder.WriteString("\nRESET search_path;\n")
	return builder.String()
}

// RestoreSeed restores a schema seed fixture into the target database.
// The rawSQL string is sanitized to strip psql meta-commands and reset search_path.
func RestoreSeed(ctx context.Context, db DB, rawSQL string) error {
	cleanSQL := SanitizeDumpSQL(rawSQL)
	if _, err := db.Exec(ctx, cleanSQL); err != nil {
		return fmt.Errorf("restoring seed SQL: %w", err)
	}
	return nil
}

// RestoreSeedFS reads a seed fixture file from an fs.FS and restores it into the target database.
func RestoreSeedFS(ctx context.Context, db DB, fsys fs.FS, filename string) error {
	content, err := fs.ReadFile(fsys, filename)
	if err != nil {
		return fmt.Errorf("reading seed fixture %q: %w", filename, err)
	}
	return RestoreSeed(ctx, db, string(content))
}

// ListSeeds discovers and returns all seed fixture filenames from the given filesystem,
// sorted in natural ascending order.
func ListSeeds(fsys fs.FS, rootDir string) ([]string, error) {
	entries, err := fs.ReadDir(fsys, rootDir)
	if err != nil {
		return nil, fmt.Errorf("reading seeds directory %q: %w", rootDir, err)
	}

	var seeds []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".sql") {
			seeds = append(seeds, pathPkg.Join(rootDir, name))
		}
	}

	sort.Strings(seeds)
	return seeds, nil
}
