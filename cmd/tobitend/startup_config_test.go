package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/config"
)

func fakePGBinaries(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"pg_ctl", "initdb", "postgres"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\necho 'postgres (PostgreSQL) 18.1'\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestStartupStorageIdentity(t *testing.T) {
	// No environment pin on PG_BIN: exercise effective persisted values and equivalent discovery.
	binDir := fakePGBinaries(t)
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	// FindBinDir checks platform directories before PATH, so capture its actual selection.
	base, err := config.Load(nil, config.WithEnviron(nil), config.WithoutEnvFile())
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := identifyStorage(base)
	if err != nil {
		t.Fatal(err)
	}
	equivalentBin := filepath.Join(t.TempDir(), "bin-alias")
	if err := os.Symlink(filepath.Dir(frozen.binaries[0]), equivalentBin); err != nil {
		t.Fatal(err)
	}
	equivalentData := filepath.Join(t.TempDir(), "data-alias")
	// Canonicalization resolves aliases in the ancestor even before ./data exists.
	parent, err := filepath.Abs(filepath.Dir(base.DataDir))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(parent, equivalentData); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		settings map[string]string
		conflict bool
	}{
		{"equivalent absolute data directory", map[string]string{"storage.data_dir": frozen.dataDir}, false},
		{"equivalent symlink ancestor", map[string]string{"data_dir": filepath.Join(equivalentData, filepath.Base(base.DataDir))}, false},
		{"normalized storage mode", map[string]string{"storage.mode": "LOCAL"}, false},
		{"equivalent binary alias", map[string]string{"storage.pg_bin": equivalentBin}, false},
		{"different data directory", map[string]string{"storage.data_dir": t.TempDir()}, true},
		{"different storage mode", map[string]string{"TOBITEN_STORAGE_MODE": "postgres"}, true},
		{"different resolved binaries", map[string]string{"pg_bin": binDir}, binDir != filepath.Dir(frozen.binaries[0])},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.Load(nil, config.WithEnviron(nil), config.WithoutEnvFile())
			if err != nil {
				t.Fatal(err)
			}
			original := *cfg
			err = applyStartupSettings(cfg, tc.settings, frozen)
			if (err != nil) != tc.conflict {
				t.Fatalf("conflict=%t err=%v", tc.conflict, err)
			}
			if tc.conflict {
				if !strings.Contains(err.Error(), "restart") {
					t.Fatalf("not actionable: %v", err)
				}
				if !reflect.DeepEqual(*cfg, original) {
					t.Fatal("failed config application mutated original")
				}
			} else {
				actual, err := identifyStorage(cfg)
				if err != nil || actual != frozen {
					t.Fatalf("identity=%v err=%v", actual, err)
				}
			}
		})
	}
}

func TestStartupIdentityPinnedSources(t *testing.T) {
	for _, source := range []string{"flag", "env", "env_file"} {
		t.Run(source, func(t *testing.T) {
			dataDir, binDir := t.TempDir(), fakePGBinaries(t)
			args := []string(nil)
			options := []config.Option{config.WithEnviron(nil), config.WithoutEnvFile()}
			switch source {
			case "flag":
				args = []string{"--data-dir=" + dataDir, "--storage-mode=local", "--pg-bin=" + binDir}
			case "env":
				options = []config.Option{config.WithEnviron([]string{"TOBITEN_DATA_DIR=" + dataDir, "TOBITEN_STORAGE_MODE=local", "TOBITEN_PG_BIN=" + binDir}), config.WithoutEnvFile()}
			case "env_file":
				options = []config.Option{config.WithEnviron(nil), config.WithEnvFileReader(strings.NewReader("TOBITEN_DATA_DIR=" + dataDir + "\nTOBITEN_STORAGE_MODE=local\nTOBITEN_PG_BIN=" + binDir + "\n"))}
			}
			cfg, err := config.Load(args, options...)
			if err != nil {
				t.Fatal(err)
			}
			frozen, err := identifyStorage(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := applyStartupSettings(cfg, map[string]string{"storage.data_dir": "/different-data", "storage.mode": "postgres", "storage.pg_bin": "/missing-binaries"}, frozen); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"TOBITEN_DATA_DIR", "TOBITEN_STORAGE_MODE", "TOBITEN_PG_BIN"} {
				if !cfg.IsEnvLocked(key) || string(cfg.SourceOf(key)) != source {
					t.Fatalf("%s source=%v locked=%t", key, cfg.SourceOf(key), cfg.IsEnvLocked(key))
				}
			}
		})
	}
}
