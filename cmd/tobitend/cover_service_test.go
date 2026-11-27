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

	// Seed an interrupted cover temp and a foreign temp in a configured leaf shard
	// before runDaemon starts; startup must sweep the cover temp and keep the foreign temp.
	seedCache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache seed: %v", err)
	}
	samplePath, err := seedCache.PathFor(tobitenimage.ArchiveCoverKey("5566778899aabbcc", 0, tobitenimage.DefaultCoverOptions()))
	if err != nil {
		t.Fatalf("PathFor seed: %v", err)
	}
	shardDir := filepath.Dir(samplePath)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		t.Fatalf("mkdir leaf shard: %v", err)
	}
	interruptedCoverTemp := filepath.Join(shardDir, ".tobiten-tmp-cover-interrupted")
	foreignTemp := filepath.Join(shardDir, ".tobiten-tmp-job42-foreign")
	for _, p := range []string{interruptedCoverTemp, foreignTemp} {
		if err := os.WriteFile(p, []byte("leftover"), 0o600); err != nil {
			t.Fatalf("seed temp %s: %v", p, err)
		}
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
		if _, statErr := os.Stat(interruptedCoverTemp); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("interrupted cover temp %s still exists after daemon startup: %v", interruptedCoverTemp, statErr)
		}
		if _, statErr := os.Stat(foreignTemp); statErr != nil {
			t.Fatalf("foreign temp %s did not survive daemon startup: %v", foreignTemp, statErr)
		}
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
		var adminUser domain.User
		if err := tx.WithTx(serveCtx, func(st storage.Tx) error {
			var listErr error
			assets, listErr = st.CatalogueWrites().ListAssetsByRoot(serveCtx, rootID)
			if listErr != nil {
				return listErr
			}
			u, getErr := st.Users().GetUserByUsername(serveCtx, "alice")
			if getErr != nil {
				return getErr
			}
			adminUser = *u
			return nil
		}); err != nil {
			return err
		}
		if len(assets) != 1 {
			t.Fatalf("assets = %d, want 1", len(assets))
		}
		adminRole, ok := domain.BuiltInRoleByName(domain.RoleAdminName)
		if !ok {
			return errors.New("missing Admin role")
		}
		principal := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-daemon-cover", nil)
		volCtx := app.ContextWithCoverTargetKind(serveCtx, domain.ItemTypeVolume)

		if !errors.Is(capturedCover.Start(serveCtx), curation.ErrCoverPoolAlreadyRunning) {
			return errors.New("expected daemon-owned cover pool to be running during serve")
		}

		rendition, err := capturedCover.GetCover(volCtx, principal, assets[0].VolumeID)
		if err != nil {
			return err
		}
		if rendition.ContentType != tobitenimage.CodecJPEG.ContentType() || len(rendition.Data) == 0 {
			t.Fatalf("unexpected cover rendition: contentType=%q len=%d", rendition.ContentType, len(rendition.Data))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("runDaemon: %v", err)
	}

	// After runDaemon exits, the daemon-owned pool must already be stopped
	// (the daemon DB pool is closed, so verify lifecycle directly via Start/Stop).
	if err := capturedCover.Start(t.Context()); err != nil {
		t.Fatalf("after runDaemon exit Start err = %v, want nil (pool stopped during shutdown)", err)
	}
	capturedCover.Stop()
}
