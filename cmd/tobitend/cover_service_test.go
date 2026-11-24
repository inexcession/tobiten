package main

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/bootstrap"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/domain"
	tobitenimage "github.com/inexcession/tobiten/internal/image"
	"github.com/inexcession/tobiten/internal/library/curation"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestRunDaemon_OwnsCoverPoolWithConfiguredWaitTimeout(t *testing.T) {
	db := testdb.New(t)
	coversDir := filepath.Join(t.TempDir(), "covers")
	rootDir := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	var pngBuf bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 64, 96))
	for y := range 96 {
		for x := range 64 {
			img.SetRGBA(x, y, color.RGBA{R: 20, G: 120, B: 220, A: 255})
		}
	}
	if err := png.Encode(&pngBuf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	w, err := zw.Create("001.png")
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}
	if _, err := w.Write(pngBuf.Bytes()); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, "daemon-cover.cbz"), zipBuf.Bytes(), 0o644); err != nil {
		t.Fatalf("write cbz: %v", err)
	}

	cfg, err := config.Load(nil, config.WithoutEnvFile(), config.WithEnviron([]string{
		"TOBITEN_STORAGE_MODE=postgres",
		"TOBITEN_DATABASE_URL=" + db.URL,
		"TOBITEN_ADMIN_USER=alice",
		"TOBITEN_ADMIN_PASS=initial-password",
		"TOBITEN_COVERS_DIR=" + coversDir,
		"TOBITEN_THUMBNAIL_WAIT_TIMEOUT=2s",
	}))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	var capturedCover *curation.CoverService
	origBuild := buildDaemonServices
	t.Cleanup(func() { buildDaemonServices = origBuild })
	buildDaemonServices = func(pool *pgxpool.Pool, c *config.Config, setup *bootstrap.Setup, userRepo *postgres.UserRepository) (*daemonServices, error) {
		ds, bErr := origBuild(pool, c, setup, userRepo)
		if bErr != nil {
			return nil, bErr
		}
		capturedCover = ds.cover
		return ds, nil
	}

	err = runDaemon(t.Context(), cfg, func(serveCtx context.Context, _ *config.Config, _ *slog.Logger, _ *app.Services) error {
		if capturedCover == nil {
			t.Fatal("daemonServices.cover was not initialized")
		}
		if got := capturedCover.WaitTimeout(); got != 2*time.Second {
			t.Fatalf("CoverService.WaitTimeout() = %s, want 2s", got)
		}

		tx := postgres.NewTransactor(db.Pool)
		libID, err := domain.NewID()
		if err != nil {
			return err
		}
		rootID, err := domain.NewID()
		if err != nil {
			return err
		}
		if err := tx.WithTx(serveCtx, func(st storage.Tx) error {
			if err := st.Libraries().CreateLibrary(serveCtx, &domain.Library{
				ID:            libID,
				Name:          "daemon-cover-lib-" + libID,
				Mode:          domain.LibraryModeVirtual,
				ResolverChain: domain.DefaultResolverChain(),
			}); err != nil {
				return err
			}
			return st.Libraries().CreateLibraryRoot(serveCtx, &domain.LibraryRoot{
				ID:        rootID,
				LibraryID: libID,
				Path:      domain.NewPathIdentity([]byte(rootDir)),
				State:     domain.LibraryRootStateOnline,
			})
		}); err != nil {
			return err
		}

		sc, err := scanner.NewScanner(tx, scanner.Options{})
		if err != nil {
			return err
		}
		if _, err := sc.ScanLibrary(serveCtx, libID, scanner.ScanOptions{}); err != nil {
			return err
		}

		var assets []domain.MediaAsset
		if err := tx.WithTx(serveCtx, func(st storage.Tx) error {
			assets, err = st.CatalogueWrites().ListAssetsByRoot(serveCtx, rootID)
			return err
		}); err != nil {
			return err
		}
		if len(assets) != 1 {
			t.Fatalf("assets = %d, want 1", len(assets))
		}

		entry, err := capturedCover.GetOrGenerateAssetCover(serveCtx, assets[0].ID)
		if err != nil {
			return err
		}
		if entry.ContentType != tobitenimage.CodecJPEG.ContentType() || len(entry.Data) == 0 {
			t.Fatalf("unexpected cover entry: contentType=%q len=%d", entry.ContentType, len(entry.Data))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon: %v", err)
	}

	// After runDaemon exits, the daemon-owned pool must be stopped.
	if _, err := capturedCover.GetOrGenerateAssetCover(t.Context(), "any-asset"); !errors.Is(err, curation.ErrCoverPoolClosed) {
		t.Fatalf("after runDaemon exit err = %v, want ErrCoverPoolClosed", err)
	}
}
