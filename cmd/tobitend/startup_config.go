package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/storage/embedded"
)

type storageIdentity struct {
	dataDir  string
	mode     string
	binaries [3]string
}

// canonicalPath also resolves symlinked ancestors when the final path is not yet created.
func canonicalPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	parent := filepath.Dir(absolute)
	if parent == absolute {
		return "", err
	}
	resolvedParent, err := canonicalPath(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(resolvedParent, filepath.Base(absolute)), nil
}

func identifyStorage(cfg *config.Config) (storageIdentity, error) {
	identity := storageIdentity{mode: cfg.StorageMode}
	var err error
	identity.dataDir, err = canonicalPath(cfg.DataDir)
	if err != nil {
		return identity, fmt.Errorf("resolving data directory: %w", err)
	}
	if cfg.StorageMode == "local" {
		binDir, err := embedded.FindBinDir(cfg.PgBin)
		if err != nil {
			return identity, err
		}
		for i, name := range []string{"pg_ctl", "initdb", "postgres"} {
			identity.binaries[i], err = filepath.EvalSymlinks(filepath.Join(binDir, name))
			if err != nil {
				return identity, fmt.Errorf("resolving PostgreSQL binary: %w", err)
			}
		}
	}
	return identity, nil
}

func applyStartupSettings(cfg *config.Config, settings map[string]string, frozen storageIdentity) error {
	candidate := *cfg
	if err := candidate.ApplySystemSettings(settings); err != nil {
		return fmt.Errorf("persisted startup settings are invalid (configure bootstrap flags/environment/.env and restart): %w", err)
	}
	identity, err := identifyStorage(&candidate)
	if err != nil {
		return fmt.Errorf("persisted storage identity is invalid (configure bootstrap flags/environment/.env and restart): %w", err)
	}
	if identity != frozen {
		return fmt.Errorf("persisted storage identity conflicts with the opened instance: configure TOBITEN_DATA_DIR, TOBITEN_STORAGE_MODE, and TOBITEN_PG_BIN via bootstrap flags/environment/.env and restart")
	}
	*cfg = candidate
	return nil
}
