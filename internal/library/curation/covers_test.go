package curation_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/container"
	"github.com/inexcession/tobiten/internal/container/cbz"
	"github.com/inexcession/tobiten/internal/domain"
	tobitenimage "github.com/inexcession/tobiten/internal/image"
	"github.com/inexcession/tobiten/internal/image/purego"
	"github.com/inexcession/tobiten/internal/library/curation"
	"github.com/inexcession/tobiten/internal/library/scanner"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestMain(m *testing.M) {
	os.Exit(testdb.Run(m))
}

type cbzEntry struct {
	name string
	data []byte
}

func makeSolidPNG(t *testing.T, w, h int, c color.RGBA) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			img.SetRGBA(x, y, c)
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return buf.Bytes()
}

func buildCBZWithComment(t *testing.T, comment string, entries ...cbzEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, e := range entries {
		w, err := zw.Create(e.name)
		if err != nil {
			t.Fatalf("zip create %q: %v", e.name, err)
		}
		if _, err := w.Write(e.data); err != nil {
			t.Fatalf("zip write %q: %v", e.name, err)
		}
	}
	if comment != "" {
		if err := zw.SetComment(comment); err != nil {
			t.Fatalf("zip set comment: %v", err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

func buildCBZ(t *testing.T, entries ...cbzEntry) []byte {
	t.Helper()
	return buildCBZWithComment(t, "", entries...)
}

func buildSameSizeCBZPair(t *testing.T, entriesA, entriesB []cbzEntry) ([]byte, []byte) {
	t.Helper()
	rawA := buildCBZ(t, entriesA...)
	rawB := buildCBZ(t, entriesB...)
	switch {
	case len(rawA) < len(rawB):
		rawA = buildCBZWithComment(t, strings.Repeat(" ", len(rawB)-len(rawA)), entriesA...)
	case len(rawB) < len(rawA):
		rawB = buildCBZWithComment(t, strings.Repeat(" ", len(rawA)-len(rawB)), entriesB...)
	}
	if len(rawA) != len(rawB) {
		t.Fatalf("expected equal CBZ lengths, got %d vs %d", len(rawA), len(rawB))
	}
	return rawA, rawB
}

func writeCBZ(t *testing.T, pathBytes []byte, entries ...cbzEntry) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(string(pathBytes)), 0o755); err != nil {
		t.Fatalf("mkdir parent for %q: %v", string(pathBytes), err)
	}
	if err := os.WriteFile(string(pathBytes), buildCBZ(t, entries...), 0o644); err != nil {
		t.Fatalf("write cbz %q: %v", string(pathBytes), err)
	}
}

func decodeJPEGCenterColor(t *testing.T, data []byte) color.RGBA {
	t.Helper()
	img, err := jpeg.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode rendered jpeg: %v", err)
	}
	b := img.Bounds()
	r, g, bl, a := img.At(b.Min.X+b.Dx()/2, b.Min.Y+b.Dy()/2).RGBA()
	return color.RGBA{
		R: uint8(r >> 8),
		G: uint8(g >> 8),
		B: uint8(bl >> 8),
		A: uint8(a >> 8),
	}
}

func assertDominantColor(t *testing.T, jpegData []byte, wantR, wantG, wantB uint8) {
	t.Helper()
	got := decodeJPEGCenterColor(t, jpegData)
	diff := func(a, b uint8) int {
		if a > b {
			return int(a - b)
		}
		return int(b - a)
	}
	const tol = 25
	if diff(got.R, wantR) > tol || diff(got.G, wantG) > tol || diff(got.B, wantB) > tol {
		t.Fatalf("rendered JPEG center color = %+v, want near RGB(%d,%d,%d)", got, wantR, wantG, wantB)
	}
}

type spyImagePipeline struct {
	inner       *purego.Pipeline
	calls       atomic.Int32
	active      atomic.Int32
	maxActive   atomic.Int32
	onRender    func(ctx context.Context) error
	hookMu      sync.Mutex
	afterRender func(ctx context.Context) error
}

func newSpyImagePipeline(t *testing.T, onRender func(context.Context) error) (*spyImagePipeline, *tobitenimage.Registry) {
	t.Helper()
	inner, err := purego.New(purego.Options{})
	if err != nil {
		t.Fatalf("purego.New: %v", err)
	}
	spy := &spyImagePipeline{
		inner:    inner,
		onRender: onRender,
	}
	reg := tobitenimage.NewRegistry()
	if err := reg.Register(spy); err != nil {
		t.Fatalf("registry.Register: %v", err)
	}
	return spy, reg
}

func (s *spyImagePipeline) setAfterRender(fn func(context.Context) error) {
	s.hookMu.Lock()
	s.afterRender = fn
	s.hookMu.Unlock()
}

func (s *spyImagePipeline) Name() string                        { return s.inner.Name() }
func (s *spyImagePipeline) DecodeFormats() []tobitenimage.Codec { return s.inner.DecodeFormats() }
func (s *spyImagePipeline) EncodeFormats() []tobitenimage.Codec { return s.inner.EncodeFormats() }
func (s *spyImagePipeline) PlaceholderFormats() []tobitenimage.Codec {
	return s.inner.PlaceholderFormats()
}

func (s *spyImagePipeline) RenderCover(ctx context.Context, src io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
	s.calls.Add(1)
	cur := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		prev := s.maxActive.Load()
		if cur <= prev || s.maxActive.CompareAndSwap(prev, cur) {
			break
		}
	}
	if s.onRender != nil {
		if err := s.onRender(ctx); err != nil {
			return tobitenimage.Result{}, err
		}
	}
	res, err := s.inner.RenderCover(ctx, src, opts)
	if err != nil {
		return tobitenimage.Result{}, err
	}
	s.hookMu.Lock()
	after := s.afterRender
	s.hookMu.Unlock()
	if after != nil {
		if err := after(ctx); err != nil {
			return tobitenimage.Result{}, err
		}
	}
	return res, nil
}

func setupDBAndLibrary(t *testing.T, rootBytes []byte) (*postgres.Transactor, *domain.Library, *domain.LibraryRoot) {
	t.Helper()
	db := testdb.New(t)
	ctx := t.Context()
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("migrations.RunFS: %v", err)
	}
	tx := postgres.NewTransactor(db.Pool)

	rootID, err := domain.NewID()
	if err != nil {
		t.Fatalf("domain.NewID: %v", err)
	}
	libID, err := domain.NewID()
	if err != nil {
		t.Fatalf("domain.NewID: %v", err)
	}
	lib := &domain.Library{
		ID:                 libID,
		Name:               "curation-test-" + libID,
		Mode:               domain.LibraryModeVirtual,
		ResolverChain:      domain.DefaultResolverChain(),
		MetadataPrecedence: domain.MetadataPrecedenceEmbeddedOverOnline,
		DefaultCoverPolicy: domain.CoverPolicyFirst,
	}
	root := &domain.LibraryRoot{
		ID:        rootID,
		LibraryID: libID,
		Path:      domain.NewPathIdentity(rootBytes),
		State:     domain.LibraryRootStateOnline,
	}
	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		if err := st.Libraries().CreateLibrary(ctx, lib); err != nil {
			return err
		}
		return st.Libraries().CreateLibraryRoot(ctx, root)
	}); err != nil {
		t.Fatalf("seed library and root: %v", err)
	}
	return tx, lib, root
}

func listRootAssets(t *testing.T, tx storage.Transactor, rootID string) []domain.MediaAsset {
	t.Helper()
	var assets []domain.MediaAsset
	if err := tx.WithTx(t.Context(), func(st storage.Tx) error {
		var err error
		assets, err = st.CatalogueWrites().ListAssetsByRoot(t.Context(), rootID)
		return err
	}); err != nil {
		t.Fatalf("ListAssetsByRoot: %v", err)
	}
	return assets
}

func newTestCoverService(
	t *testing.T,
	readTx storage.ReadTransactor,
	reg *tobitenimage.Registry,
	waitTimeout time.Duration,
	configure ...func(*curation.CoverServiceOptions),
) (*curation.CoverService, *tobitenimage.CoverCache) {
	t.Helper()
	cache, err := tobitenimage.NewCoverCache(filepath.Join(t.TempDir(), "covers"))
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}
	opts := curation.CoverServiceOptions{
		ReadTx:      readTx,
		Cache:       cache,
		Pipelines:   reg,
		Workers:     2,
		WaitTimeout: waitTimeout,
	}
	for _, fn := range configure {
		if fn != nil {
			fn(&opts)
		}
	}
	svc, err := curation.NewCoverService(opts)
	if err != nil {
		t.Fatalf("NewCoverService: %v", err)
	}
	if err := svc.Start(t.Context()); err != nil {
		t.Fatalf("svc.Start: %v", err)
	}
	t.Cleanup(svc.Stop)
	return svc, cache
}

func seedReaderPrincipal(t *testing.T, tx storage.Transactor) *domain.Principal {
	t.Helper()
	readerRole, ok := domain.BuiltInRoleByName(domain.RoleReaderName)
	if !ok {
		t.Fatal("missing Reader role")
	}
	userID, err := domain.NewID()
	if err != nil {
		t.Fatalf("domain.NewID: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	user := domain.User{
		ID:        userID,
		Username:  "reader-" + userID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := tx.WithTx(t.Context(), func(st storage.Tx) error {
		if err := st.Users().CreateUser(t.Context(), &user); err != nil {
			return err
		}
		return st.Users().AssignUserRole(t.Context(), user.ID, readerRole.ID)
	}); err != nil {
		t.Fatalf("seed reader principal: %v", err)
	}
	return domain.NewPrincipal(user, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-"+userID, nil)
}

func assertCachedCoverPageIndex(t *testing.T, cache *tobitenimage.CoverCache, asset domain.MediaAsset, wantPage int) {
	t.Helper()
	digest, ok := asset.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok {
		t.Fatalf("asset %s missing XXH3_64_PARTIAL digest", asset.ID)
	}
	key := tobitenimage.ArchiveCoverKey(digest.Value, wantPage, tobitenimage.DefaultCoverOptions())
	if _, err := cache.Stat(t.Context(), key); err != nil {
		t.Fatalf("Stat cover cache for asset %s page %d: %v", asset.ID, wantPage, err)
	}
}

func TestCoverService_ExportedMethodsRequirePrincipal(t *testing.T) {
	t.Parallel()
	svcType := reflect.TypeFor[*curation.CoverService]()
	principalType := reflect.TypeFor[*domain.Principal]()
	lifecycleMethods := map[string]bool{
		"Start":       true,
		"Stop":        true,
		"WaitTimeout": true,
	}

	var checked int
	for i := range svcType.NumMethod() {
		m := svcType.Method(i)
		if lifecycleMethods[m.Name] {
			for argIdx := 1; argIdx < m.Type.NumIn(); argIdx++ {
				if m.Type.In(argIdx).Kind() == reflect.String {
					t.Fatalf("lifecycle method CoverService.%s unexpectedly accepts string parameter %d", m.Name, argIdx)
				}
			}
			continue
		}
		checked++
		hasPrincipal := false
		for argIdx := 1; argIdx < m.Type.NumIn(); argIdx++ {
			if m.Type.In(argIdx) == principalType {
				hasPrincipal = true
				break
			}
		}
		if !hasPrincipal {
			t.Fatalf("exported method CoverService.%s does not accept *domain.Principal", m.Name)
		}
	}
	if checked == 0 {
		t.Fatal("expected at least one exported service method on *curation.CoverService")
	}
	for _, forbidden := range []string{"GetOrGenerateAssetCover", "GetOrGenerateCover", "Workers"} {
		if _, ok := svcType.MethodByName(forbidden); ok {
			t.Fatalf("CoverService must not export %s", forbidden)
		}
	}
}

func TestCoverService_CoverPageSelectionNeverSuppressed(t *testing.T) {
	t.Parallel()
	rootDir := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	red := makeSolidPNG(t, 80, 120, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	green := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	blue := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 40, B: 220, A: 255})

	// 1. ComicInfo FrontCover on page 2 (while page 0 is Deleted) -> blue
	writeCBZ(t, []byte(filepath.Join(rootDir, "frontcover.cbz")),
		cbzEntry{name: "001.png", data: red},
		cbzEntry{name: "002.png", data: green},
		cbzEntry{name: "003.png", data: blue},
		cbzEntry{
			name: "ComicInfo.xml",
			data: []byte(`<ComicInfo><Series>Front</Series><Number>1</Number><Pages><Page Image="0" Type="Deleted"/><Page Image="2" Type="FrontCover"/></Pages></ComicInfo>`),
		},
	)

	// 2. No FrontCover, page 0 is Deleted -> first displayable page 1 (green)
	writeCBZ(t, []byte(filepath.Join(rootDir, "suppressed_first.cbz")),
		cbzEntry{name: "001.png", data: red},
		cbzEntry{name: "002.png", data: green},
		cbzEntry{
			name: "ComicInfo.xml",
			data: []byte(`<ComicInfo><Series>SuppressedFirst</Series><Number>1</Number><Pages><Page Image="0" Type="Deleted"/></Pages></ComicInfo>`),
		},
	)

	// 3. Plain CBZ with no ComicInfo -> page 0 (red)
	writeCBZ(t, []byte(filepath.Join(rootDir, "plain.cbz")),
		cbzEntry{name: "001.png", data: red},
		cbzEntry{name: "002.png", data: green},
	)

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	byRel := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byRel[a.RelPathDisplay] = a
	}

	_, reg := newSpyImagePipeline(t, nil)
	svc, cache := newTestCoverService(t, tx, reg, 5*time.Second)
	principal := seedReaderPrincipal(t, tx)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)

	fcAsset := byRel["frontcover.cbz"]
	if fcAsset.CoverPageIndex != 2 {
		t.Fatalf("frontcover CoverPageIndex = %d, want 2", fcAsset.CoverPageIndex)
	}
	fcRendition, err := svc.GetCover(volCtx, principal, fcAsset.VolumeID)
	if err != nil {
		t.Fatalf("GetCover(frontcover): %v", err)
	}
	assertCachedCoverPageIndex(t, cache, fcAsset, 2)
	assertDominantColor(t, fcRendition.Data, 30, 40, 220)

	supAsset := byRel["suppressed_first.cbz"]
	if supAsset.CoverPageIndex != 1 {
		t.Fatalf("suppressed_first CoverPageIndex = %d, want 1", supAsset.CoverPageIndex)
	}
	supRendition, err := svc.GetCover(volCtx, principal, supAsset.VolumeID)
	if err != nil {
		t.Fatalf("GetCover(suppressed_first): %v", err)
	}
	assertCachedCoverPageIndex(t, cache, supAsset, 1)
	assertDominantColor(t, supRendition.Data, 30, 210, 30)

	plainAsset := byRel["plain.cbz"]
	plainRendition, err := svc.GetCover(volCtx, principal, plainAsset.VolumeID)
	if err != nil {
		t.Fatalf("GetCover(plain): %v", err)
	}
	assertCachedCoverPageIndex(t, cache, plainAsset, 0)
	assertDominantColor(t, plainRendition.Data, 220, 30, 30)

	// If a DB asset later has its recorded cover page marked suppressed (e.g. via HidePage),
	// CoverService must never serve the suppressed page and instead fall back or fail unavailable.
	if err := tx.WithTx(t.Context(), func(st storage.Tx) error {
		a, getErr := st.CatalogueWrites().GetAssetByIDForUpdate(t.Context(), plainAsset.ID)
		if getErr != nil {
			return getErr
		}
		a.SuppressedPages = []int{0}
		return st.CatalogueWrites().UpdateAssetContent(t.Context(), a)
	}); err != nil {
		t.Fatalf("update suppressed_pages: %v", err)
	}

	fallbackRendition, err := svc.GetCover(volCtx, principal, plainAsset.VolumeID)
	if err != nil {
		t.Fatalf("GetCover(plain after suppressing page 0): %v", err)
	}
	assertCachedCoverPageIndex(t, cache, plainAsset, 1)
	assertDominantColor(t, fallbackRendition.Data, 30, 210, 30)

	// Suppressing all remaining pages returns a typed unavailable error.
	if err := tx.WithTx(t.Context(), func(st storage.Tx) error {
		a, getErr := st.CatalogueWrites().GetAssetByIDForUpdate(t.Context(), plainAsset.ID)
		if getErr != nil {
			return getErr
		}
		a.SuppressedPages = []int{0, 1}
		return st.CatalogueWrites().UpdateAssetContent(t.Context(), a)
	}); err != nil {
		t.Fatalf("suppress all pages: %v", err)
	}
	if _, err := svc.GetCover(volCtx, principal, plainAsset.VolumeID); !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("all-suppressed error = %v, want ErrCoverUnavailable", err)
	}
}

func TestCoverService_RawBytePathsNFCNFDAndInvalidUTF8(t *testing.T) {
	t.Parallel()
	rootDir := filepath.Join(t.TempDir(), "raw-root")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	red := makeSolidPNG(t, 64, 96, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	green := makeSolidPNG(t, 64, 96, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	blue := makeSolidPNG(t, 64, 96, color.RGBA{R: 30, G: 40, B: 220, A: 255})

	// Separate subdirectories avoid APFS NFC/NFD directory-entry collision on Darwin.
	nfcRel := []byte("nfc/Caf\xc3\xa9_01.cbz")
	nfdRel := []byte("nfd/Cafe\xcc\x81_02.cbz")
	writeCBZ(t, append(append([]byte(rootDir), '/'), nfcRel...), cbzEntry{name: "001.png", data: red})
	writeCBZ(t, append(append([]byte(rootDir), '/'), nfdRel...), cbzEntry{name: "001.png", data: green})

	invalidCreated := false
	invalidRel := []byte("bad_\xff\xfe/Vol_\x80_03.cbz")
	invalidDir := append(append([]byte(rootDir), '/'), []byte("bad_\xff\xfe")...)
	if err := os.MkdirAll(string(invalidDir), 0o755); err != nil {
		if runtime.GOOS == "linux" || (!errors.Is(err, syscall.EILSEQ) && !errors.Is(err, syscall.EINVAL)) {
			t.Fatalf("mkdir invalid UTF-8 dir: %v", err)
		}
	} else {
		invalidAbs := append(append([]byte(rootDir), '/'), invalidRel...)
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		w, err := zw.Create("001.png")
		if err != nil {
			t.Fatalf("zip create: %v", err)
		}
		if _, err := w.Write(blue); err != nil {
			t.Fatalf("zip write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zip close: %v", err)
		}
		if err := os.WriteFile(string(invalidAbs), buf.Bytes(), 0o644); err != nil {
			if runtime.GOOS == "linux" || (!errors.Is(err, syscall.EILSEQ) && !errors.Is(err, syscall.EINVAL)) {
				t.Fatalf("write invalid UTF-8 file: %v", err)
			}
		} else {
			invalidCreated = true
		}
	}

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	findByBytes := func(rel []byte) domain.MediaAsset {
		for _, a := range assets {
			if bytes.Equal(a.RelPathBytes, rel) {
				return a
			}
		}
		t.Fatalf("missing indexed asset for raw path %x (%q)", rel, string(rel))
		return domain.MediaAsset{}
	}

	_, reg := newSpyImagePipeline(t, nil)
	svc, _ := newTestCoverService(t, tx, reg, 5*time.Second)
	principal := seedReaderPrincipal(t, tx)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)

	nfcRendition, err := svc.GetCover(volCtx, principal, findByBytes(nfcRel).VolumeID)
	if err != nil {
		t.Fatalf("NFC cover error: %v", err)
	}
	assertDominantColor(t, nfcRendition.Data, 220, 30, 30)

	nfdRendition, err := svc.GetCover(volCtx, principal, findByBytes(nfdRel).VolumeID)
	if err != nil {
		t.Fatalf("NFD cover error: %v", err)
	}
	assertDominantColor(t, nfdRendition.Data, 30, 210, 30)

	if invalidCreated {
		invRendition, err := svc.GetCover(volCtx, principal, findByBytes(invalidRel).VolumeID)
		if err != nil {
			t.Fatalf("invalid UTF-8 cover error: %v", err)
		}
		assertDominantColor(t, invRendition.Data, 30, 40, 220)
	}
}

func TestCoverService_BoundedPoolSharedWaitAndTimeoutContinuation(t *testing.T) {
	t.Parallel()
	rootDir := filepath.Join(t.TempDir(), "pool-root")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	img := makeSolidPNG(t, 80, 120, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	const numBooks = 6
	for i := range numBooks {
		name := filepath.Join(rootDir, "book_"+string(rune('a'+i))+".cbz")
		// Distinct page bytes give distinct XXH3_64_PARTIAL digests.
		writeCBZ(t, []byte(name), cbzEntry{name: "001.png", data: append(slices.Clone(img), byte(i))})
	}

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}
	assets := listRootAssets(t, tx, root.ID)
	if len(assets) != numBooks {
		t.Fatalf("indexed assets = %d, want %d", len(assets), numBooks)
	}

	principal := seedReaderPrincipal(t, tx)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)

	t.Run("bounded workers cap concurrent generation", func(t *testing.T) {
		gate := make(chan struct{})
		twoStarted := make(chan struct{})
		var startedCount atomic.Int32

		spy, reg := newSpyImagePipeline(t, func(ctx context.Context) error {
			if startedCount.Add(1) == 2 {
				close(twoStarted)
			}
			select {
			case <-gate:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		svc, _ := newTestCoverService(t, tx, reg, 5*time.Second)

		var wg sync.WaitGroup
		errs := make([]error, numBooks)
		for i := range numBooks {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = svc.GetCover(volCtx, principal, assets[idx].VolumeID)
			}(i)
		}

		select {
		case <-twoStarted:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for 2 bounded workers to start")
		}
		// Give any hypothetical third worker a moment to violate the bound.
		time.Sleep(25 * time.Millisecond)
		if got := spy.active.Load(); got != 2 {
			close(gate)
			t.Fatalf("active workers = %d, want 2", got)
		}

		close(gate)
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Fatalf("asset %d cover err: %v", i, err)
			}
		}
		if got := spy.maxActive.Load(); got != 2 {
			t.Fatalf("maxActive = %d, want 2", got)
		}
		if got := spy.calls.Load(); got != numBooks {
			t.Fatalf("total render calls = %d, want %d", got, numBooks)
		}
	})

	t.Run("concurrent requests for same key share one generation and timeout continues caching", func(t *testing.T) {
		gate := make(chan struct{})
		renderStarted := make(chan struct{})
		var startOnce sync.Once

		spy, reg := newSpyImagePipeline(t, func(ctx context.Context) error {
			startOnce.Do(func() { close(renderStarted) })
			select {
			case <-gate:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		svc, cache := newTestCoverService(t, &armingReadTx{inner: tx}, reg, 500*time.Millisecond)
		target := assets[0]
		curDigest, ok := target.CurrentDigest(domain.DigestKindXXH364Partial)
		if !ok {
			t.Fatal("missing XXH3_64_PARTIAL digest")
		}
		cacheKey := tobitenimage.ArchiveCoverKey(curDigest.Value, target.CoverPageIndex, tobitenimage.DefaultCoverOptions())

		// Caller A triggers generation and times out while generation stays active.
		timeoutErrCh := make(chan error, 1)
		go func() {
			_, err := svc.GetCover(volCtx, principal, target.VolumeID)
			timeoutErrCh <- err
		}()

		select {
		case <-renderStarted:
		case <-time.After(3 * time.Second):
			t.Fatal("render never started")
		}

		// Caller A must receive a typed timeout error while generation stays active.
		select {
		case err := <-timeoutErrCh:
			if !errors.Is(err, curation.ErrCoverWaitTimeout) {
				close(gate)
				t.Fatalf("caller A error = %v, want ErrCoverWaitTimeout", err)
			}
			var timeoutErr *curation.CoverWaitTimeoutError
			if !errors.As(err, &timeoutErr) || !timeoutErr.IsTimeout() {
				close(gate)
				t.Fatalf("caller A error %T does not match *CoverWaitTimeoutError", err)
			}
		case <-time.After(3 * time.Second):
			close(gate)
			t.Fatal("caller A did not time out")
		}

		// Before gate closes, cache must not have published a partial entry.
		if _, err := cache.Stat(t.Context(), cacheKey); !errors.Is(err, tobitenimage.ErrCacheMiss) {
			close(gate)
			t.Fatalf("expected ErrCacheMiss before generation finishes, got %v", err)
		}

		// Launch 6 concurrent callers joining the exact same in-flight task started by caller A.
		const sharedCallers = 6
		sharedResults := make([]*app.CoverRendition, sharedCallers)
		sharedErrs := make([]error, sharedCallers)
		var joinedCount atomic.Int32
		allJoined := make(chan struct{})
		var wg sync.WaitGroup
		for i := range sharedCallers {
			wg.Add(1)
			waitCtx := &waitNotifyContext{
				Context: volCtx,
				onWait: func() {
					if joinedCount.Add(1) == sharedCallers {
						close(allJoined)
					}
				},
			}
			go func(idx int, ctx context.Context) {
				defer wg.Done()
				sharedResults[idx], sharedErrs[idx] = svc.GetCover(ctx, principal, target.VolumeID)
			}(i, waitCtx)
		}

		select {
		case <-allJoined:
		case <-time.After(3 * time.Second):
			close(gate)
			wg.Wait()
			t.Fatalf("timed out waiting for shared callers to join task wait (%d/%d)", joinedCount.Load(), sharedCallers)
		}

		// Release generation; all shared callers receive the single generated cover and it is cached.
		close(gate)
		wg.Wait()

		for i := range sharedCallers {
			if sharedErrs[i] != nil {
				t.Fatalf("shared caller %d error: %v", i, sharedErrs[i])
			}
			if !bytes.Equal(sharedResults[i].Data, sharedResults[0].Data) {
				t.Fatalf("shared caller %d bytes differed from caller 0", i)
			}
		}
		if got := spy.calls.Load(); got != 1 {
			t.Fatalf("render calls across timeout + %d shared callers = %d, want 1", sharedCallers, got)
		}
		if _, err := cache.Stat(t.Context(), cacheKey); err != nil {
			t.Fatalf("expected cached cover after timed-out caller, Stat err = %v", err)
		}

		// Subsequent retry by caller A hits cache with zero extra renders.
		retryRendition, err := svc.GetCover(volCtx, principal, target.VolumeID)
		if err != nil {
			t.Fatalf("retry after cache completion err: %v", err)
		}
		if !bytes.Equal(retryRendition.Data, sharedResults[0].Data) {
			t.Fatal("retry rendition bytes differed from shared result")
		}
		if got := spy.calls.Load(); got != 1 {
			t.Fatalf("render calls after cache hit = %d, want 1", got)
		}
	})

	t.Run("GetCover shares single caller wait across successive fallback candidates", func(t *testing.T) {
		writeCBZ(t, []byte(filepath.Join(rootDir, "wait_alpha.cbz")), cbzEntry{name: "001.png", data: []byte("\x89PNG\r\n\x1a\ncorrupt-wait-1")})
		writeCBZ(t, []byte(filepath.Join(rootDir, "wait_beta.cbz")), cbzEntry{name: "001.png", data: []byte("\x89PNG\r\n\x1a\ncorrupt-wait-2")})
		healthyImg := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 210, B: 30, A: 255})
		writeCBZ(t, []byte(filepath.Join(rootDir, "wait_gamma.cbz")), cbzEntry{name: "001.png", data: healthyImg})

		if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
			t.Fatalf("ScanLibrary fallback fixtures: %v", err)
		}
		allAssets := listRootAssets(t, tx, root.ID)
		byRel := make(map[string]domain.MediaAsset, len(allAssets))
		for _, a := range allAssets {
			byRel[a.RelPathDisplay] = a
		}

		var readerUser domain.User
		var readerRole domain.Role
		now := time.Now().UTC().Truncate(time.Microsecond)
		if err := tx.WithTx(t.Context(), func(st storage.Tx) error {
			var ok bool
			readerRole, ok = domain.BuiltInRoleByName(domain.RoleReaderName)
			if !ok {
				return errors.New("missing Reader role")
			}
			readerUser = domain.User{ID: "u-wait-reader", Username: "waitreader", CreatedAt: now, UpdatedAt: now}
			if err := st.Users().CreateUser(t.Context(), &readerUser); err != nil {
				return err
			}
			if err := st.Users().AssignUserRole(t.Context(), readerUser.ID, readerRole.ID); err != nil {
				return err
			}
			ser := domain.Series{
				ID: "ser-shared-wait", LibraryID: lib.ID, Title: "Shared Wait", TitleSort: "shared wait",
				Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now,
			}
			if err := st.CatalogueWrites().CreateSeries(t.Context(), &ser); err != nil {
				return err
			}
			seenVols := make(map[string]struct{}, 3)
			for idx, relName := range []string{"wait_alpha.cbz", "wait_beta.cbz", "wait_gamma.cbz"} {
				pos := float64(idx + 1)
				tmpl, exists := byRel[relName]
				if !exists {
					return fmt.Errorf("missing scanned asset %q", relName)
				}
				if _, dup := seenVols[tmpl.VolumeID]; dup {
					return fmt.Errorf("expected distinct volume for %q (got duplicate %s)", relName, tmpl.VolumeID)
				}
				seenVols[tmpl.VolumeID] = struct{}{}
				if err := st.SeriesMemberships().SetVolumeMemberships(t.Context(), tmpl.VolumeID, []domain.SeriesMembership{{
					VolumeID: tmpl.VolumeID, SeriesID: ser.ID, IsPrimary: true, Kind: domain.MembershipKindSeries,
					Position: &pos, Source: domain.MembershipSourceManual,
				}}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("seed shared-wait series: %v", err)
		}

		okAsset := byRel["wait_gamma.cbz"]
		okDigest, ok := okAsset.CurrentDigest(domain.DigestKindXXH364Partial)
		if !ok {
			t.Fatal("missing wait-ok digest")
		}
		cand3Key := tobitenimage.ArchiveCoverKey(okDigest.Value, 0, tobitenimage.DefaultCoverOptions())

		gate := make(chan struct{})
		cand3Started := make(chan struct{})
		cand3Finished := make(chan struct{})
		var attempt atomic.Int32
		var cand3StartOnce, cand3DoneOnce sync.Once

		spy, reg := newSpyImagePipeline(t, func(ctx context.Context) error {
			switch attempt.Add(1) {
			case 1, 2:
				// Each corrupt candidate consumes ~80ms before failing decode.
				timer := time.NewTimer(80 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			case 3:
				cand3StartOnce.Do(func() { close(cand3Started) })
				select {
				case <-gate:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			default:
				return nil
			}
		})
		spy.setAfterRender(func(_ context.Context) error {
			if attempt.Load() >= 3 {
				cand3DoneOnce.Do(func() { close(cand3Finished) })
			}
			return nil
		})

		const callerWait = 250 * time.Millisecond
		svc, cache := newTestCoverService(t, tx, reg, callerWait)
		principal := domain.NewPrincipal(readerUser, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-wait", nil)
		seriesCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeSeries)

		start := time.Now()
		_, err := svc.GetCover(seriesCtx, principal, "ser-shared-wait")
		elapsed := time.Since(start)

		if !errors.Is(err, app.ErrServiceBusy) || !errors.Is(err, curation.ErrCoverWaitTimeout) {
			close(gate)
			t.Fatalf("GetCover shared wait err = %v, want ErrServiceBusy + ErrCoverWaitTimeout", err)
		}
		var timeoutErr *curation.CoverWaitTimeoutError
		if !errors.As(err, &timeoutErr) || !timeoutErr.IsTimeout() {
			close(gate)
			t.Fatalf("GetCover err %T does not unwrap *CoverWaitTimeoutError", err)
		}
		if timeoutErr.Timeout >= 180*time.Millisecond {
			close(gate)
			t.Fatalf("third candidate wait timeout = %s, want shared remaining budget < 180ms (of %s)", timeoutErr.Timeout, callerWait)
		}
		if elapsed >= 450*time.Millisecond {
			close(gate)
			t.Fatalf("GetCover total elapsed = %s, want single shared wait near %s", elapsed, callerWait)
		}

		select {
		case <-cand3Started:
		case <-time.After(time.Second):
			close(gate)
			t.Fatal("expected candidate 3 worker to start before shared wait expired")
		}
		if _, statErr := cache.Stat(t.Context(), cand3Key); !errors.Is(statErr, tobitenimage.ErrCacheMiss) {
			close(gate)
			t.Fatalf("expected ErrCacheMiss while worker blocked on gate, got %v", statErr)
		}

		// Releasing the worker finishes generation in background and caches candidate 3.
		close(gate)
		select {
		case <-cand3Finished:
		case <-time.After(3 * time.Second):
			t.Fatal("candidate 3 background worker did not finish")
		}
		// Give cache.Put a moment after RenderCover returns inside the worker.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, statErr := cache.Stat(t.Context(), cand3Key); statErr == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if _, statErr := cache.Stat(t.Context(), cand3Key); statErr != nil {
			t.Fatalf("expected cached cand3 entry after background worker completion: %v", statErr)
		}

		callsBeforeRetry := spy.calls.Load()
		volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)
		gotVol3, err := svc.GetCover(volCtx, principal, okAsset.VolumeID)
		if err != nil {
			t.Fatalf("retry wait-ok volume after background cache completion: %v", err)
		}
		assertDominantColor(t, gotVol3.Data, 30, 210, 30)
		if spy.calls.Load() != callsBeforeRetry {
			t.Fatalf("wait-ok volume triggered %d extra renders after background cache completion, want 0", spy.calls.Load()-callsBeforeRetry)
		}
	})
}

func TestCoverService_MoveReusesCacheAndExternalModifyRegenerates(t *testing.T) {
	t.Parallel()
	rootDir := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	origPath := filepath.Join(rootDir, "dir1", "vol1.cbz")
	movedPath := filepath.Join(rootDir, "dir2", "renamed_vol1.cbz")
	coldPath := filepath.Join(rootDir, "dir1", "cold.cbz")
	revertedPath := filepath.Join(rootDir, "dir1", "reverted.cbz")
	red := makeSolidPNG(t, 80, 120, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	green := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	blue := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 30, B: 220, A: 255})

	coldRedCBZ := buildCBZ(t, cbzEntry{name: "001.png", data: red}, cbzEntry{name: "002.png", data: blue})
	coldGreenCBZ := buildCBZ(t, cbzEntry{name: "001.png", data: green}, cbzEntry{name: "002.png", data: red})
	rev1CBZ, rev2CBZ := buildSameSizeCBZPair(
		t,
		[]cbzEntry{{name: "001.png", data: red}, {name: "002.png", data: green}},
		[]cbzEntry{{name: "001.png", data: green}, {name: "002.png", data: blue}},
	)

	writeCBZ(t, []byte(origPath), cbzEntry{name: "001.png", data: red})
	if err := os.WriteFile(coldPath, coldRedCBZ, 0o644); err != nil {
		t.Fatalf("write cold cbz: %v", err)
	}
	if err := os.WriteFile(revertedPath, rev1CBZ, 0o644); err != nil {
		t.Fatalf("write reverted cbz: %v", err)
	}

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("initial ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	if len(assets) != 3 {
		t.Fatalf("initial assets = %d, want 3", len(assets))
	}
	byRel := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byRel[a.RelPathDisplay] = a
	}
	vol1 := byRel["dir1/vol1.cbz"]
	cold := byRel["dir1/cold.cbz"]
	reverted := byRel["dir1/reverted.cbz"]
	digest1, ok := vol1.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok {
		t.Fatal("missing initial XXH3_64_PARTIAL digest for vol1")
	}
	coldDigest1, ok := cold.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok {
		t.Fatal("missing initial XXH3_64_PARTIAL digest for cold")
	}
	oldColdKey := tobitenimage.ArchiveCoverKey(coldDigest1.Value, 0, tobitenimage.DefaultCoverOptions())
	revDigest1, ok := reverted.CurrentDigest(domain.DigestKindXXH364Partial)
	if !ok {
		t.Fatal("missing initial XXH3_64_PARTIAL digest for reverted")
	}
	revDigest2, err := scanner.PartialFingerprint(bytes.NewReader(rev2CBZ), int64(len(rev2CBZ)))
	if err != nil || revDigest2 == revDigest1.Value {
		t.Fatalf("expected distinct same-size rev2 digest (err=%v, d1=%s, d2=%s)", err, revDigest1.Value, revDigest2)
	}
	oldRevertedKey := tobitenimage.ArchiveCoverKey(revDigest1.Value, 0, tobitenimage.DefaultCoverOptions())

	var beforeOpenMu sync.Mutex
	var beforeOpenFn func() error
	setBeforeOpen := func(fn func() error) {
		beforeOpenMu.Lock()
		beforeOpenFn = fn
		beforeOpenMu.Unlock()
	}

	baseCBZ := cbz.Format()
	wrappedCBZ := baseCBZ
	wrappedCBZ.Open = func(ra io.ReaderAt, size int64) (container.ContainerReader, error) {
		beforeOpenMu.Lock()
		fn := beforeOpenFn
		beforeOpenMu.Unlock()
		if fn != nil {
			if hookErr := fn(); hookErr != nil {
				return nil, hookErr
			}
		}
		return baseCBZ.Open(ra, size)
	}
	formats := container.NewFormatRegistry()
	if err := formats.Register(wrappedCBZ); err != nil {
		t.Fatalf("register wrapped cbz format: %v", err)
	}

	spy, reg := newSpyImagePipeline(t, nil)
	svc, cache := newTestCoverService(t, tx, reg, 5*time.Second, func(o *curation.CoverServiceOptions) {
		o.Formats = formats
	})
	principal := seedReaderPrincipal(t, tx)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)

	entry1, err := svc.GetCover(volCtx, principal, vol1.VolumeID)
	if err != nil {
		t.Fatalf("initial GetCover: %v", err)
	}
	assertDominantColor(t, entry1.Data, 220, 30, 30)
	if spy.calls.Load() != 1 {
		t.Fatalf("initial render calls = %d, want 1", spy.calls.Load())
	}

	// Move/rename the file on disk and rescan (M1-AC8).
	if err := os.MkdirAll(filepath.Dir(movedPath), 0o755); err != nil {
		t.Fatalf("mkdir moved dir: %v", err)
	}
	if err := os.Rename(origPath, movedPath); err != nil {
		t.Fatalf("rename cbz: %v", err)
	}
	scanMove, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("move ScanLibrary: %v", err)
	}
	if scanMove.MovedCount != 1 {
		t.Fatalf("MovedCount = %d, want 1", scanMove.MovedCount)
	}

	entryMoved, err := svc.GetCover(volCtx, principal, vol1.VolumeID)
	if err != nil {
		t.Fatalf("moved GetCover: %v", err)
	}
	if !bytes.Equal(entryMoved.Data, entry1.Data) {
		t.Fatal("moved file cover differed from cached entry")
	}
	if spy.calls.Load() != 1 {
		t.Fatalf("render calls after move/rename = %d, want 1 (must reuse cache)", spy.calls.Load())
	}

	// Externally modify the moved file with new cover artwork and distinct size/mtime, then rescan.
	writeCBZ(t, []byte(movedPath),
		cbzEntry{name: "001.png", data: green},
		cbzEntry{name: "002.png", data: red},
	)
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(movedPath, future, future); err != nil {
		t.Fatalf("chtimes modified file: %v", err)
	}

	scanMod, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("modify ScanLibrary: %v", err)
	}
	if scanMod.ModifiedCount != 1 {
		t.Fatalf("ModifiedCount = %d, want 1", scanMod.ModifiedCount)
	}

	assetsAfterMod := listRootAssets(t, tx, root.ID)
	var digest2 domain.AssetDigest
	for _, a := range assetsAfterMod {
		if a.ID == vol1.ID {
			digest2, ok = a.CurrentDigest(domain.DigestKindXXH364Partial)
			break
		}
	}
	if !ok || digest2.Value == digest1.Value {
		t.Fatalf("expected new digest after external modify, old=%s new=%+v", digest1.Value, digest2)
	}

	entryMod, err := svc.GetCover(volCtx, principal, vol1.VolumeID)
	if err != nil {
		t.Fatalf("modified GetCover: %v", err)
	}
	assertDominantColor(t, entryMod.Data, 30, 210, 30)
	if spy.calls.Load() != 2 {
		t.Fatalf("render calls after external modify = %d, want 2", spy.calls.Load())
	}
	if _, err := cache.Stat(t.Context(), tobitenimage.ArchiveCoverKey(digest2.Value, 0, tobitenimage.DefaultCoverOptions())); err != nil {
		t.Fatalf("Stat new digest cache entry: %v", err)
	}

	// Cold request after an unscanned external edit must fail verification and leave old key empty.
	if err := os.WriteFile(coldPath, coldGreenCBZ, 0o644); err != nil {
		t.Fatalf("overwrite cold cbz before cold request: %v", err)
	}
	if _, err := svc.GetCover(volCtx, principal, cold.VolumeID); !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("cold modified before rescan err = %v, want ErrCoverUnavailable", err)
	} else {
		var unavailErr *curation.CoverUnavailableError
		if !errors.As(err, &unavailErr) || unavailErr.Reason != "digest_mismatch" {
			t.Fatalf("cold modified unavailErr = %+v, want Reason=digest_mismatch", unavailErr)
		}
	}
	if spy.calls.Load() != 2 {
		t.Fatalf("render calls after pre-extraction mismatch = %d, want 2", spy.calls.Load())
	}
	if _, err := cache.Stat(t.Context(), oldColdKey); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Fatalf("old key Stat after pre-extraction mismatch = %v, want ErrCacheMiss", err)
	}

	// Restore matching cold archive bytes, then overwrite mid-generation (after RenderCover, before cache.Put).
	if err := os.WriteFile(coldPath, coldRedCBZ, 0o644); err != nil {
		t.Fatalf("restore cold cbz: %v", err)
	}
	afterEntered := make(chan struct{})
	releaseAfter := make(chan struct{})
	spy.setAfterRender(func(ctx context.Context) error {
		close(afterEntered)
		select {
		case <-releaseAfter:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	midGenErrCh := make(chan error, 1)
	go func() {
		_, gErr := svc.GetCover(volCtx, principal, cold.VolumeID)
		midGenErrCh <- gErr
	}()

	select {
	case <-afterEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for cold generation render completion")
	}

	future2 := future.Add(2 * time.Second)
	if err := os.WriteFile(coldPath, coldGreenCBZ, 0o644); err != nil {
		close(releaseAfter)
		t.Fatalf("overwrite cold cbz mid-generation: %v", err)
	}
	if err := os.Chtimes(coldPath, future2, future2); err != nil {
		close(releaseAfter)
		t.Fatalf("chtimes cold cbz mid-generation: %v", err)
	}
	close(releaseAfter)

	if err := <-midGenErrCh; !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("mid-generation write err = %v, want ErrCoverUnavailable", err)
	} else {
		var unavailErr *curation.CoverUnavailableError
		if !errors.As(err, &unavailErr) || unavailErr.Reason != "digest_mismatch" {
			t.Fatalf("mid-generation write unavailErr = %+v, want Reason=digest_mismatch", unavailErr)
		}
	}
	if _, err := cache.Stat(t.Context(), oldColdKey); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Fatalf("old key Stat after mid-generation rejection = %v, want ErrCacheMiss", err)
	}

	// Clear hook and rescan: regeneration succeeds under the new digest while the old key stays empty.
	spy.setAfterRender(nil)
	scanColdMod, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("cold modify ScanLibrary: %v", err)
	}
	if scanColdMod.ModifiedCount != 1 {
		t.Fatalf("cold ModifiedCount = %d, want 1", scanColdMod.ModifiedCount)
	}

	assetsAfterColdMod := listRootAssets(t, tx, root.ID)
	var coldDigest2 domain.AssetDigest
	for _, a := range assetsAfterColdMod {
		if a.ID == cold.ID {
			coldDigest2, ok = a.CurrentDigest(domain.DigestKindXXH364Partial)
			break
		}
	}
	if !ok || coldDigest2.Value == coldDigest1.Value {
		t.Fatalf("expected new digest for cold cbz after rescan, old=%s new=%+v", coldDigest1.Value, coldDigest2)
	}

	entryColdMod, err := svc.GetCover(volCtx, principal, cold.VolumeID)
	if err != nil {
		t.Fatalf("cold post-scan GetCover: %v", err)
	}
	assertDominantColor(t, entryColdMod.Data, 30, 210, 30)
	if _, err := cache.Stat(t.Context(), tobitenimage.ArchiveCoverKey(coldDigest2.Value, 0, tobitenimage.DefaultCoverOptions())); err != nil {
		t.Fatalf("Stat regenerated cold digest entry: %v", err)
	}
	if _, err := cache.Stat(t.Context(), oldColdKey); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Fatalf("old key Stat after post-scan regeneration = %v, want ErrCacheMiss", err)
	}

	// Same-size edit written before archive parsing and reverted back to the original
	// bytes (with changed mtime) before publication must be rejected and leave the old key empty.
	revMtime2 := future2.Add(2 * time.Second)
	revMtime3 := revMtime2.Add(2 * time.Second)
	setBeforeOpen(func() error {
		if err := os.WriteFile(revertedPath, rev2CBZ, 0o644); err != nil {
			return err
		}
		return os.Chtimes(revertedPath, revMtime2, revMtime2)
	})
	spy.setAfterRender(func(context.Context) error {
		if err := os.WriteFile(revertedPath, rev1CBZ, 0o644); err != nil {
			return err
		}
		return os.Chtimes(revertedPath, revMtime3, revMtime3)
	})
	defer setBeforeOpen(nil)
	defer spy.setAfterRender(nil)

	if _, err := svc.GetCover(volCtx, principal, reverted.VolumeID); !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("reverted same-size edit err = %v, want ErrCoverUnavailable", err)
	} else {
		var unavailErr *curation.CoverUnavailableError
		if !errors.As(err, &unavailErr) || unavailErr.Reason != "source_changed" {
			t.Fatalf("reverted same-size edit unavailErr = %+v, want Reason=source_changed", unavailErr)
		}
	}
	if _, err := cache.Stat(t.Context(), oldRevertedKey); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Fatalf("old key Stat after reverted same-size edit = %v, want ErrCacheMiss", err)
	}
}

func TestCoverService_UnavailableAndNotFoundErrors(t *testing.T) {
	t.Parallel()
	rootDir := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	red := makeSolidPNG(t, 64, 96, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	green := makeSolidPNG(t, 64, 96, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	fileA := filepath.Join(rootDir, "a.cbz")
	fileB := filepath.Join(rootDir, "b.cbz")
	writeCBZ(t, []byte(fileA), cbzEntry{name: "001.png", data: red})
	writeCBZ(t, []byte(fileB), cbzEntry{name: "001.png", data: green})

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	if len(assets) != 2 {
		t.Fatalf("assets = %d, want 2", len(assets))
	}
	byName := map[string]domain.MediaAsset{
		assets[0].RelPathDisplay: assets[0],
		assets[1].RelPathDisplay: assets[1],
	}

	_, reg := newSpyImagePipeline(t, nil)
	svc, _ := newTestCoverService(t, tx, reg, 5*time.Second)
	principal := seedReaderPrincipal(t, tx)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)

	// Unknown volume returns app.ErrNotFound.
	if _, err := svc.GetCover(volCtx, principal, "nonexistent-volume"); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("unknown volume error = %v, want app.ErrNotFound", err)
	}

	// Prime cache for a.cbz before it goes missing/retired.
	if _, err := svc.GetCover(volCtx, principal, byName["a.cbz"].VolumeID); err != nil {
		t.Fatalf("prime a.cbz cover: %v", err)
	}

	// Remove b.cbz on disk WITHOUT rescanning -> file_missing unavailable error.
	if err := os.Remove(fileB); err != nil {
		t.Fatalf("remove b.cbz: %v", err)
	}
	_, err = svc.GetCover(volCtx, principal, byName["b.cbz"].VolumeID)
	if !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("unscanned missing file error = %v, want ErrCoverUnavailable", err)
	}
	var unavailErr *curation.CoverUnavailableError
	if !errors.As(err, &unavailErr) || unavailErr.Reason != "file_missing" {
		t.Fatalf("unavailErr = %+v, want Reason=file_missing", unavailErr)
	}

	// Delete a.cbz and rescan so a.cbz is marked missing -> app.ErrNotFound (R6-D3).
	if err := os.Remove(fileA); err != nil {
		t.Fatalf("remove a.cbz: %v", err)
	}
	writeCBZ(t, []byte(fileB), cbzEntry{name: "001.png", data: green}) // keep root non-empty so it stays ONLINE
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("rescan with missing a.cbz: %v", err)
	}

	if _, err := svc.GetCover(volCtx, principal, byName["a.cbz"].VolumeID); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("missing asset error = %v, want app.ErrNotFound", err)
	}

	// Retire a.cbz via grace-period expiry -> app.ErrNotFound (R6-D3).
	past := time.Now().Add(-10 * 24 * time.Hour)
	if err := tx.WithTx(t.Context(), func(st storage.Tx) error {
		a, getErr := st.CatalogueWrites().GetAssetByIDForUpdate(t.Context(), byName["a.cbz"].ID)
		if getErr != nil {
			return getErr
		}
		a.MissingSince = &past
		return st.CatalogueWrites().UpdateAsset(t.Context(), a)
	}); err != nil {
		t.Fatalf("backdate missing_since: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("retire ScanLibrary: %v", err)
	}
	if _, err := svc.GetCover(volCtx, principal, byName["a.cbz"].VolumeID); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("retired asset error = %v, want app.ErrNotFound", err)
	}

	// Prime cache for b.cbz, then make the root directory OFFLINE -> ErrServiceBusy + ErrCoverUnavailable ("root_offline").
	if _, err := svc.GetCover(volCtx, principal, byName["b.cbz"].VolumeID); err != nil {
		t.Fatalf("prime b.cbz cover: %v", err)
	}
	if err := os.RemoveAll(rootDir); err != nil {
		t.Fatalf("remove rootDir: %v", err)
	}
	scanOffline, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{})
	if err != nil {
		t.Fatalf("offline ScanLibrary: %v", err)
	}
	if !scanOffline.Offline {
		t.Fatalf("expected library OFFLINE after root removal, got %+v", scanOffline)
	}

	_, err = svc.GetCover(volCtx, principal, byName["b.cbz"].VolumeID)
	if !errors.Is(err, app.ErrServiceBusy) || !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("offline root asset error = %v, want ErrServiceBusy + ErrCoverUnavailable", err)
	}
	if !errors.As(err, &unavailErr) || unavailErr.Reason != "root_offline" {
		t.Fatalf("offline unavailErr = %+v, want Reason=root_offline", unavailErr)
	}
}

func TestCoverService_PoolLifecycleAndRestartRegeneration(t *testing.T) {
	t.Parallel()
	_, reg := newSpyImagePipeline(t, nil)
	cache, err := tobitenimage.NewCoverCache(filepath.Join(t.TempDir(), "covers"))
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	if _, err := curation.NewCoverService(curation.CoverServiceOptions{Pipelines: reg}); !errors.Is(err, curation.ErrMissingCoverCache) {
		t.Fatalf("missing cache err = %v, want ErrMissingCoverCache", err)
	}
	if _, err := curation.NewCoverService(curation.CoverServiceOptions{Cache: cache}); !errors.Is(err, curation.ErrMissingImageRegistry) {
		t.Fatalf("missing registry err = %v, want ErrMissingImageRegistry", err)
	}
	if _, err := curation.NewCoverService(curation.CoverServiceOptions{Cache: cache, Pipelines: reg, Workers: -1}); !errors.Is(err, curation.ErrInvalidCoverWorkers) {
		t.Fatalf("negative workers err = %v, want ErrInvalidCoverWorkers", err)
	}
	if _, err := curation.NewCoverService(curation.CoverServiceOptions{Cache: cache, Pipelines: reg, WaitTimeout: -time.Second}); !errors.Is(err, curation.ErrInvalidCoverWaitTimeout) {
		t.Fatalf("negative wait timeout err = %v, want ErrInvalidCoverWaitTimeout", err)
	}

	rootDir := filepath.Join(t.TempDir(), "lifecycle-root")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	red := makeSolidPNG(t, 64, 96, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	writeCBZ(t, []byte(filepath.Join(rootDir, "restart.cbz")), cbzEntry{name: "001.png", data: red})

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}
	assets := listRootAssets(t, tx, root.ID)
	if len(assets) != 1 {
		t.Fatalf("assets = %d, want 1", len(assets))
	}
	volID := assets[0].VolumeID
	principal := seedReaderPrincipal(t, tx)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)

	// Not started yet -> ErrCoverPoolNotRunning.
	svc1, err := curation.NewCoverService(curation.CoverServiceOptions{ReadTx: tx, Cache: cache, Pipelines: reg})
	if err != nil {
		t.Fatalf("NewCoverService: %v", err)
	}
	if _, err := svc1.GetCover(volCtx, principal, volID); !errors.Is(err, curation.ErrCoverPoolNotRunning) {
		t.Fatalf("before Start err = %v, want ErrCoverPoolNotRunning", err)
	}

	// Interrupt mid-generation by cancelling daemon lifecycle context / stopping svcInterrupted.
	renderEntered := make(chan struct{})
	gate := make(chan struct{})
	_, blockingReg := newSpyImagePipeline(t, func(ctx context.Context) error {
		close(renderEntered)
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	svcInterrupted, err := curation.NewCoverService(curation.CoverServiceOptions{
		ReadTx:    tx,
		Cache:     cache,
		Pipelines: blockingReg,
		Workers:   1,
	})
	if err != nil {
		t.Fatalf("NewCoverService blocking: %v", err)
	}
	if err := svcInterrupted.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, gErr := svcInterrupted.GetCover(volCtx, principal, volID)
		errCh <- gErr
	}()
	select {
	case <-renderEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("interrupted render did not start")
	}
	svcInterrupted.Stop()
	if gotErr := <-errCh; gotErr == nil {
		t.Fatal("expected error after Stop interrupted generation")
	}
	if _, err := svcInterrupted.GetCover(volCtx, principal, volID); !errors.Is(err, curation.ErrCoverPoolClosed) {
		t.Fatalf("after Stop err = %v, want ErrCoverPoolClosed", err)
	}

	// Simulated daemon restart with a new CoverService instance regenerates cleanly.
	svcRestarted, err := curation.NewCoverService(curation.CoverServiceOptions{
		ReadTx:    tx,
		Cache:     cache,
		Pipelines: reg,
	})
	if err != nil {
		t.Fatalf("NewCoverService restarted: %v", err)
	}
	if err := svcRestarted.Start(t.Context()); err != nil {
		t.Fatalf("Start restarted: %v", err)
	}
	defer svcRestarted.Stop()

	rendition, err := svcRestarted.GetCover(volCtx, principal, volID)
	if err != nil {
		t.Fatalf("restarted GetCover: %v", err)
	}
	assertDominantColor(t, rendition.Data, 220, 30, 30)
}

func TestCoverService_StoppedPoolPreservesNotFoundForHiddenAndAbsentTargets(t *testing.T) {
	t.Parallel()
	rootDir := filepath.Join(t.TempDir(), "stopped-pool-root")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	red := makeSolidPNG(t, 64, 96, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	green := makeSolidPNG(t, 64, 96, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	writeCBZ(t, []byte(filepath.Join(rootDir, "visible.cbz")), cbzEntry{name: "001.png", data: red})
	writeCBZ(t, []byte(filepath.Join(rootDir, "restricted.cbz")), cbzEntry{name: "001.png", data: green})

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	ctx := t.Context()
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	byRel := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byRel[a.RelPathDisplay] = a
	}
	visAsset := byRel["visible.cbz"]
	restrAsset := byRel["restricted.cbz"]

	var adminUser, readerUser domain.User
	var adminRole, readerRole domain.Role
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		var ok bool
		if adminRole, ok = domain.BuiltInRoleByName(domain.RoleAdminName); !ok {
			return errors.New("missing Admin role")
		}
		if readerRole, ok = domain.BuiltInRoleByName(domain.RoleReaderName); !ok {
			return errors.New("missing Reader role")
		}
		adminUser = domain.User{ID: "u-stop-admin", Username: "stopadmin", CreatedAt: now, UpdatedAt: now}
		readerUser = domain.User{ID: "u-stop-reader", Username: "stopreader", CreatedAt: now, UpdatedAt: now}
		for _, u := range []struct {
			user *domain.User
			role string
		}{
			{&adminUser, adminRole.ID},
			{&readerUser, readerRole.ID},
		} {
			if err := st.Users().CreateUser(ctx, u.user); err != nil {
				return err
			}
			if err := st.Users().AssignUserRole(ctx, u.user.ID, u.role); err != nil {
				return err
			}
		}

		cw := st.CatalogueWrites()
		for _, s := range []domain.Series{
			{ID: "ser-stop-vis", LibraryID: lib.ID, Title: "Visible", TitleSort: "visible", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-stop-restr", LibraryID: lib.ID, Title: "Restricted", TitleSort: "restricted", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
		} {
			ser := s
			if err := cw.CreateSeries(ctx, &ser); err != nil {
				return err
			}
		}
		p1 := 1.0
		if err := st.SeriesMemberships().SetVolumeMemberships(ctx, visAsset.VolumeID, []domain.SeriesMembership{{
			VolumeID: visAsset.VolumeID, SeriesID: "ser-stop-vis", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual,
		}}); err != nil {
			return err
		}
		if err := st.SeriesMemberships().SetVolumeMemberships(ctx, restrAsset.VolumeID, []domain.SeriesMembership{{
			VolumeID: restrAsset.VolumeID, SeriesID: "ser-stop-restr", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual,
		}}); err != nil {
			return err
		}

		labels := st.Labels()
		lbl := domain.Label{ID: "lbl-stop-nsfw", Name: "NSFW-Stop", Scope: domain.LabelScopeServer}
		if err := labels.CreateLabel(ctx, &lbl); err != nil {
			return err
		}
		if err := labels.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{UserID: readerUser.ID, LabelID: lbl.ID, Effect: domain.PolicyEffectRestrict}); err != nil {
			return err
		}
		for _, il := range []domain.ItemLabel{
			{LabelID: lbl.ID, ItemType: domain.ItemTypeSeries, ItemID: "ser-stop-restr"},
			{LabelID: lbl.ID, ItemType: domain.ItemTypeVolume, ItemID: restrAsset.VolumeID},
		} {
			item := il
			if err := labels.AssignItemLabel(ctx, &item); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed stopped-pool fixture: %v", err)
	}

	adminPrincipal := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-stop-adm", nil)
	readerPrincipal := domain.NewPrincipal(readerUser, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-stop-rdr", nil)

	_, reg := newSpyImagePipeline(t, nil)
	cache, err := tobitenimage.NewCoverCache(filepath.Join(t.TempDir(), "covers"))
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}
	svc, err := curation.NewCoverService(curation.CoverServiceOptions{
		ReadTx:    tx,
		Cache:     cache,
		Pipelines: reg,
	})
	if err != nil {
		t.Fatalf("NewCoverService: %v", err)
	}

	assertOutcomes := func(phase string, wantPoolErr error) {
		t.Helper()
		for _, tc := range []struct {
			name      string
			principal *domain.Principal
			kind      domain.ItemType
			targetID  string
			wantErrs  []error
		}{
			{"absent volume", readerPrincipal, domain.ItemTypeVolume, "vol-does-not-exist", []error{app.ErrNotFound}},
			{"absent series", readerPrincipal, domain.ItemTypeSeries, "ser-does-not-exist", []error{app.ErrNotFound}},
			{"hidden volume for reader", readerPrincipal, domain.ItemTypeVolume, restrAsset.VolumeID, []error{app.ErrNotFound}},
			{"hidden series for reader", readerPrincipal, domain.ItemTypeSeries, "ser-stop-restr", []error{app.ErrNotFound}},
			{"visible volume for reader", readerPrincipal, domain.ItemTypeVolume, visAsset.VolumeID, []error{app.ErrServiceBusy, wantPoolErr}},
			{"visible series for reader", readerPrincipal, domain.ItemTypeSeries, "ser-stop-vis", []error{app.ErrServiceBusy, wantPoolErr}},
			{"restricted volume visible to admin", adminPrincipal, domain.ItemTypeVolume, restrAsset.VolumeID, []error{app.ErrServiceBusy, wantPoolErr}},
			{"restricted series visible to admin", adminPrincipal, domain.ItemTypeSeries, "ser-stop-restr", []error{app.ErrServiceBusy, wantPoolErr}},
		} {
			_, gotErr := svc.GetCover(app.ContextWithCoverTargetKind(ctx, tc.kind), tc.principal, tc.targetID)
			for _, wantErr := range tc.wantErrs {
				if !errors.Is(gotErr, wantErr) {
					t.Fatalf("%s (%s): GetCover = %v, want errors.Is(_, %v)", phase, tc.name, gotErr, wantErr)
				}
			}
			if errors.Is(tc.wantErrs[0], app.ErrNotFound) && errors.Is(gotErr, app.ErrServiceBusy) {
				t.Fatalf("%s (%s): GetCover leaked ErrServiceBusy: %v", phase, tc.name, gotErr)
			}
		}
	}

	// Before Start: hidden and absent targets return ErrNotFound; visible targets return ErrServiceBusy + ErrCoverPoolNotRunning.
	assertOutcomes("before Start", curation.ErrCoverPoolNotRunning)

	// After Start and Stop: hidden and absent targets still return ErrNotFound; visible targets return ErrServiceBusy + ErrCoverPoolClosed.
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("svc.Start: %v", err)
	}
	svc.Stop()
	assertOutcomes("after Stop", curation.ErrCoverPoolClosed)
}

func TestCoverService_GetCover_LabelEnforcementAndSeriesFallback(t *testing.T) {
	t.Parallel()

	rootDir := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	red := makeSolidPNG(t, 80, 120, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	green := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	blue := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 40, B: 220, A: 255})
	yellow := makeSolidPNG(t, 80, 120, color.RGBA{R: 220, G: 210, B: 30, A: 255})
	purple := makeSolidPNG(t, 80, 120, color.RGBA{R: 180, G: 40, B: 200, A: 255})

	writeCBZ(t, []byte(filepath.Join(rootDir, "alpha-1-nsfw.cbz")), cbzEntry{name: "001.png", data: blue})
	writeCBZ(t, []byte(filepath.Join(rootDir, "alpha-1-clean.cbz")), cbzEntry{name: "001.png", data: red})
	writeCBZ(t, []byte(filepath.Join(rootDir, "alpha-2-clean.cbz")), cbzEntry{name: "001.png", data: green})
	writeCBZ(t, []byte(filepath.Join(rootDir, "kids-1.cbz")), cbzEntry{name: "001.png", data: yellow})
	writeCBZ(t, []byte(filepath.Join(rootDir, "offline-only.cbz")), cbzEntry{name: "001.png", data: red})
	writeCBZ(t, []byte(filepath.Join(rootDir, "offline-pref.cbz")), cbzEntry{name: "001.png", data: blue})
	writeCBZ(t, []byte(filepath.Join(rootDir, "offline-fallback-online.cbz")), cbzEntry{name: "001.png", data: purple})
	writeCBZ(t, []byte(filepath.Join(rootDir, "corrupt-cover.cbz")), cbzEntry{name: "001.png", data: []byte("corrupt-not-a-valid-png")})

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	ctx := t.Context()

	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(ctx, lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	offlineRootDir := filepath.Join(t.TempDir(), "offline-root")
	offlineRootID, err := domain.NewID()
	if err != nil {
		t.Fatalf("NewID offline root: %v", err)
	}
	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		return st.Libraries().CreateLibraryRoot(ctx, &domain.LibraryRoot{
			ID:        offlineRootID,
			LibraryID: lib.ID,
			Path:      domain.NewPathIdentity([]byte(offlineRootDir)),
			State:     domain.LibraryRootStateOffline,
		})
	}); err != nil {
		t.Fatalf("create offline root: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	byName := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byName[a.RelPathDisplay] = a
	}

	var (
		adminUser, readerUser, kidUser, blankKidUser domain.User
		adminRole, readerRole, restrictedRole        domain.Role
	)
	now := time.Now().UTC().Truncate(time.Microsecond)

	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		users := st.Users()
		var ok bool
		if adminRole, ok = domain.BuiltInRoleByName(domain.RoleAdminName); !ok {
			return errors.New("missing Admin role")
		}
		if readerRole, ok = domain.BuiltInRoleByName(domain.RoleReaderName); !ok {
			return errors.New("missing Reader role")
		}
		if restrictedRole, ok = domain.BuiltInRoleByName(domain.RoleRestrictedReaderName); !ok {
			return errors.New("missing Restricted reader role")
		}

		for _, u := range []struct {
			dst  *domain.User
			id   string
			name string
			role string
		}{
			{&adminUser, "u-cov-admin", "covadmin", adminRole.ID},
			{&readerUser, "u-cov-reader", "covreader", readerRole.ID},
			{&kidUser, "u-cov-kid", "covkid", restrictedRole.ID},
			{&blankKidUser, "u-cov-blank", "covblank", restrictedRole.ID},
		} {
			*u.dst = domain.User{ID: u.id, Username: u.name, CreatedAt: now, UpdatedAt: now}
			if err := users.CreateUser(ctx, u.dst); err != nil {
				return err
			}
			if err := users.AssignUserRole(ctx, u.id, u.role); err != nil {
				return err
			}
		}

		cw := st.CatalogueWrites()
		for _, s := range []domain.Series{
			{ID: "ser-cov-alpha", LibraryID: lib.ID, Title: "Alpha", TitleSort: "alpha", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-cov-fallback", LibraryID: lib.ID, Title: "Fallback", TitleSort: "fallback", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-cov-kids", LibraryID: lib.ID, Title: "Kids", TitleSort: "kids", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-cov-trashed", LibraryID: lib.ID, Title: "Trashed", TitleSort: "trashed", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-cov-offline-first", LibraryID: lib.ID, Title: "Offline First", TitleSort: "offline first", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-cov-corrupt-fallback", LibraryID: lib.ID, Title: "Corrupt Fallback", TitleSort: "corrupt fallback", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-cov-corrupt-only", LibraryID: lib.ID, Title: "Corrupt Only", TitleSort: "corrupt only", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
		} {
			ser := s
			if err := cw.CreateSeries(ctx, &ser); err != nil {
				return err
			}
		}

		prefNSFW := byName["alpha-1-nsfw.cbz"].ID
		prefOffline := byName["offline-pref.cbz"].ID
		for _, v := range []domain.Volume{
			{ID: "vol-cov-multi", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PreferredAssetID: &prefNSFW, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-cov-only-restricted", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-cov-nofile", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PageCount: 0, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-cov-offline", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-cov-offline-with-online", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PreferredAssetID: &prefOffline, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-cov-corrupt", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-cov-clean2", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-cov-kids", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
			{ID: "vol-cov-trashed", LibraryID: lib.ID, Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now},
		} {
			vol := v
			if err := cw.CreateVolume(ctx, &vol); err != nil {
				return err
			}
		}

		sm := st.SeriesMemberships()
		p1, p2, p3, p4 := 1.0, 2.0, 3.0, 4.0
		for _, m := range []struct {
			volID string
			list  []domain.SeriesMembership
		}{
			{"vol-cov-only-restricted", []domain.SeriesMembership{{VolumeID: "vol-cov-only-restricted", SeriesID: "ser-cov-alpha", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual}}},
			{"vol-cov-clean2", []domain.SeriesMembership{
				{VolumeID: "vol-cov-clean2", SeriesID: "ser-cov-alpha", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p2, Source: domain.MembershipSourceManual},
				{VolumeID: "vol-cov-clean2", SeriesID: "ser-cov-offline-first", IsPrimary: false, Kind: domain.MembershipKindSeries, Position: &p2, Source: domain.MembershipSourceManual},
				{VolumeID: "vol-cov-clean2", SeriesID: "ser-cov-corrupt-fallback", IsPrimary: false, Kind: domain.MembershipKindSeries, Position: &p2, Source: domain.MembershipSourceManual},
			}},
			{"vol-cov-offline-with-online", []domain.SeriesMembership{{VolumeID: "vol-cov-offline-with-online", SeriesID: "ser-cov-offline-first", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual}}},
			{"vol-cov-corrupt", []domain.SeriesMembership{
				{VolumeID: "vol-cov-corrupt", SeriesID: "ser-cov-corrupt-fallback", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual},
				{VolumeID: "vol-cov-corrupt", SeriesID: "ser-cov-corrupt-only", IsPrimary: false, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual},
			}},
			{"vol-cov-nofile", []domain.SeriesMembership{{VolumeID: "vol-cov-nofile", SeriesID: "ser-cov-fallback", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual}}},
			{"vol-cov-offline", []domain.SeriesMembership{{VolumeID: "vol-cov-offline", SeriesID: "ser-cov-fallback", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p2, Source: domain.MembershipSourceManual}}},
			{"vol-cov-trashed", []domain.SeriesMembership{{VolumeID: "vol-cov-trashed", SeriesID: "ser-cov-fallback", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p3, Source: domain.MembershipSourceManual}}},
			{"vol-cov-multi", []domain.SeriesMembership{{VolumeID: "vol-cov-multi", SeriesID: "ser-cov-fallback", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p4, Source: domain.MembershipSourceManual}}},
			{"vol-cov-kids", []domain.SeriesMembership{{VolumeID: "vol-cov-kids", SeriesID: "ser-cov-kids", IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual}}},
		} {
			if err := sm.SetVolumeMemberships(ctx, m.volID, m.list); err != nil {
				return err
			}
		}

		moveAsset := func(assetName, volID string, rootIDOverride string) error {
			a, err := cw.GetAssetByIDForUpdate(ctx, byName[assetName].ID)
			if err != nil {
				return err
			}
			a.VolumeID = volID
			if rootIDOverride != "" {
				a.LibraryRootID = rootIDOverride
			}
			return cw.UpdateAsset(ctx, a)
		}
		if err := moveAsset("alpha-1-nsfw.cbz", "vol-cov-multi", ""); err != nil {
			return err
		}
		if err := moveAsset("alpha-1-clean.cbz", "vol-cov-multi", ""); err != nil {
			return err
		}
		if err := moveAsset("alpha-2-clean.cbz", "vol-cov-clean2", ""); err != nil {
			return err
		}
		if err := moveAsset("kids-1.cbz", "vol-cov-kids", ""); err != nil {
			return err
		}
		if err := moveAsset("offline-only.cbz", "vol-cov-offline", offlineRootID); err != nil {
			return err
		}
		if err := moveAsset("offline-pref.cbz", "vol-cov-offline-with-online", offlineRootID); err != nil {
			return err
		}
		if err := moveAsset("offline-fallback-online.cbz", "vol-cov-offline-with-online", ""); err != nil {
			return err
		}
		if err := moveAsset("corrupt-cover.cbz", "vol-cov-corrupt", ""); err != nil {
			return err
		}

		// Create a dedicated restricted-only asset for vol-cov-only-restricted.
		nsfwAsset := byName["alpha-1-nsfw.cbz"]
		nsfwDigest, _ := nsfwAsset.CurrentDigest(domain.DigestKindXXH364Partial)
		copyPath := filepath.Join(rootDir, "only-restr-copy.cbz")
		origBytes, err := os.ReadFile(filepath.Join(rootDir, "alpha-1-nsfw.cbz"))
		if err != nil {
			return err
		}
		if err := os.WriteFile(copyPath, origBytes, 0o644); err != nil {
			return err
		}
		restrAsset := domain.MediaAsset{
			ID:             "a-cov-only-restr",
			VolumeID:       "vol-cov-only-restricted",
			LibraryRootID:  root.ID,
			RelPathBytes:   []byte("only-restr-copy.cbz"),
			RelPathDisplay: "only-restr-copy.cbz",
			RelPathMatch:   "only-restr-copy.cbz",
			FileSize:       nsfwAsset.FileSize,
			Format:         domain.MediaFormatCBZ,
			PageCount:      1,
			CoverPageIndex: 0,
			Digests: []domain.AssetDigest{{
				AssetID:   "a-cov-only-restr",
				Kind:      domain.DigestKindXXH364Partial,
				Value:     nsfwDigest.Value,
				IsCurrent: true,
				Reason:    domain.DigestReasonScan,
			}},
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := cw.CreateAsset(ctx, &restrAsset); err != nil {
			return err
		}

		// Seed a trashed series asset (vol in trashed series / trashed volume).
		trashedPath := filepath.Join(rootDir, "trashed-vol.cbz")
		if err := os.WriteFile(trashedPath, origBytes, 0o644); err != nil {
			return err
		}
		trashedAsset := domain.MediaAsset{
			ID:             "a-cov-trashed-vol",
			VolumeID:       "vol-cov-trashed",
			LibraryRootID:  root.ID,
			RelPathBytes:   []byte("trashed-vol.cbz"),
			RelPathDisplay: "trashed-vol.cbz",
			RelPathMatch:   "trashed-vol.cbz",
			FileSize:       int64(len(origBytes)),
			Format:         domain.MediaFormatCBZ,
			PageCount:      1,
			CoverPageIndex: 0,
			Digests: []domain.AssetDigest{{
				AssetID:   "a-cov-trashed-vol",
				Kind:      domain.DigestKindXXH364Partial,
				Value:     nsfwDigest.Value,
				IsCurrent: true,
				Reason:    domain.DigestReasonScan,
			}},
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := cw.CreateAsset(ctx, &trashedAsset); err != nil {
			return err
		}

		labels := st.Labels()
		if err := labels.EnsureBuiltInServerLabels(ctx); err != nil {
			return err
		}
		for _, lbl := range []domain.Label{
			{ID: "lbl-cov-nsfw", Name: "NSFW", Scope: domain.LabelScopeServer},
			{ID: "lbl-cov-kids", Name: "Kids", Scope: domain.LabelScopeServer},
		} {
			l := lbl
			if err := labels.CreateLabel(ctx, &l); err != nil {
				return err
			}
		}
		if err := labels.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{UserID: readerUser.ID, LabelID: "lbl-cov-nsfw", Effect: domain.PolicyEffectRestrict}); err != nil {
			return err
		}
		if err := labels.SetUserLabelPolicy(ctx, &domain.UserLabelPolicy{UserID: kidUser.ID, LabelID: "lbl-cov-kids", Effect: domain.PolicyEffectAllow}); err != nil {
			return err
		}
		for _, il := range []domain.ItemLabel{
			{LabelID: "lbl-cov-nsfw", ItemType: domain.ItemTypeAsset, ItemID: byName["alpha-1-nsfw.cbz"].ID},
			{LabelID: "lbl-cov-nsfw", ItemType: domain.ItemTypeAsset, ItemID: "a-cov-only-restr"},
			{LabelID: "lbl-cov-kids", ItemType: domain.ItemTypeSeries, ItemID: "ser-cov-kids"},
			{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeSeries, ItemID: "ser-cov-trashed"},
			{LabelID: domain.LabelMarkedForDeletionID, ItemType: domain.ItemTypeVolume, ItemID: "vol-cov-trashed"},
		} {
			item := il
			if err := labels.AssignItemLabel(ctx, &item); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed cover test fixture: %v", err)
	}

	_, reg := newSpyImagePipeline(t, nil)
	svc, _ := newTestCoverService(t, tx, reg, 2*time.Second)

	adminPrincipal := domain.NewPrincipal(adminUser, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-adm", nil)
	readerPrincipal := domain.NewPrincipal(readerUser, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-rdr", nil)
	kidPrincipal := domain.NewPrincipal(kidUser, []domain.Role{restrictedRole}, domain.CredentialKindPassword, "c-kid", nil)
	blankPrincipal := domain.NewPrincipal(blankKidUser, []domain.Role{restrictedRole}, domain.CredentialKindPassword, "c-blk", nil)

	// 1. Cross-principal volume asset selection: Admin gets NSFW blue; Reader gets Clean red and a different ETag.
	admVolCover, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeVolume), adminPrincipal, "vol-cov-multi")
	if err != nil {
		t.Fatalf("admin GetCover(vol-cov-multi): %v", err)
	}
	assertDominantColor(t, admVolCover.Data, 30, 40, 220)

	rdrVolCover, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeVolume), readerPrincipal, "vol-cov-multi")
	if err != nil {
		t.Fatalf("reader GetCover(vol-cov-multi): %v", err)
	}
	assertDominantColor(t, rdrVolCover.Data, 220, 30, 30)
	if admVolCover.ETag == rdrVolCover.ETag || admVolCover.ETag == "" || strings.HasPrefix(admVolCover.ETag, "W/") {
		t.Fatalf("expected distinct strong ETags for admin (%q) vs reader (%q)", admVolCover.ETag, rdrVolCover.ETag)
	}

	// 2. Target-kind isolation: series ID on volume kind and volume ID on series kind return ErrNotFound.
	if _, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeVolume), adminPrincipal, "ser-cov-alpha"); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("series ID with volume target kind = %v, want ErrNotFound", err)
	}
	if _, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), adminPrincipal, "vol-cov-multi"); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("volume ID with series target kind = %v, want ErrNotFound", err)
	}

	// 3. Series FIRST selection & cross-principal fallback on ser-cov-alpha:
	//    Admin gets vol-cov-only-restricted (blue); Reader falls back to vol-cov-clean2 (green).
	admSerAlpha, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), adminPrincipal, "ser-cov-alpha")
	if err != nil {
		t.Fatalf("admin GetCover(ser-cov-alpha): %v", err)
	}
	assertDominantColor(t, admSerAlpha.Data, 30, 40, 220)

	rdrSerAlpha, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), readerPrincipal, "ser-cov-alpha")
	if err != nil {
		t.Fatalf("reader GetCover(ser-cov-alpha): %v", err)
	}
	assertDominantColor(t, rdrSerAlpha.Data, 30, 210, 30)
	if admSerAlpha.ETag == rdrSerAlpha.ETag {
		t.Fatalf("expected distinct ETags on ser-cov-alpha: both %q", admSerAlpha.ETag)
	}

	// 4. Series FIRST fallback across empty, offline-root, and trashed volumes on ser-cov-fallback:
	//    pos 1 (vol-cov-nofile) -> pos 2 (vol-cov-offline) -> pos 3 (vol-cov-trashed) -> pos 4 (vol-cov-multi).
	//    Admin gets vol-cov-multi's NSFW blue; Reader gets vol-cov-multi's clean red!
	admFallback, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), adminPrincipal, "ser-cov-fallback")
	if err != nil {
		t.Fatalf("admin GetCover(ser-cov-fallback): %v", err)
	}
	assertDominantColor(t, admFallback.Data, 30, 40, 220)

	rdrFallback, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), readerPrincipal, "ser-cov-fallback")
	if err != nil {
		t.Fatalf("reader GetCover(ser-cov-fallback): %v", err)
	}
	assertDominantColor(t, rdrFallback.Data, 220, 30, 30)

	// 5. Trashed targets return ErrNotFound even for Admin; Restricted reader sees ALLOW series only, blank kid fails closed.
	for _, tc := range []struct {
		name      string
		principal *domain.Principal
		kind      domain.ItemType
		targetID  string
		wantErr   error
	}{
		{"nil principal", nil, domain.ItemTypeVolume, "vol-cov-multi", app.ErrUnauthorized},
		{"admin trashed volume", adminPrincipal, domain.ItemTypeVolume, "vol-cov-trashed", app.ErrNotFound},
		{"admin trashed series", adminPrincipal, domain.ItemTypeSeries, "ser-cov-trashed", app.ErrNotFound},
		{"admin nofile volume", adminPrincipal, domain.ItemTypeVolume, "vol-cov-nofile", app.ErrNotFound},
		{"admin offline volume", adminPrincipal, domain.ItemTypeVolume, "vol-cov-offline", app.ErrServiceBusy},
		{"reader only-restricted volume", readerPrincipal, domain.ItemTypeVolume, "vol-cov-only-restricted", app.ErrNotFound},
		{"kid unallowed series", kidPrincipal, domain.ItemTypeSeries, "ser-cov-alpha", app.ErrNotFound},
		{"kid unallowed volume", kidPrincipal, domain.ItemTypeVolume, "vol-cov-multi", app.ErrNotFound},
		{"blank kid zero ALLOW", blankPrincipal, domain.ItemTypeSeries, "ser-cov-kids", app.ErrNotFound},
	} {
		_, gErr := svc.GetCover(app.ContextWithCoverTargetKind(ctx, tc.kind), tc.principal, tc.targetID)
		if !errors.Is(gErr, tc.wantErr) {
			t.Fatalf("%s: GetCover = %v, want %v", tc.name, gErr, tc.wantErr)
		}
	}

	kidSerCover, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), kidPrincipal, "ser-cov-kids")
	if err != nil {
		t.Fatalf("kid GetCover(ser-cov-kids): %v", err)
	}
	assertDominantColor(t, kidSerCover.Data, 220, 210, 30)

	// 6. Worker timeout on candidate 1 returns ErrServiceBusy rather than falling back to candidate 2.
	_, slowReg := newSpyImagePipeline(t, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	slowSvc, _ := newTestCoverService(t, tx, slowReg, 40*time.Millisecond, func(o *curation.CoverServiceOptions) {
		o.Workers = 1
	})
	if _, err := slowSvc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), readerPrincipal, "ser-cov-alpha"); !errors.Is(err, app.ErrServiceBusy) {
		t.Fatalf("slow worker series GetCover = %v, want ErrServiceBusy", err)
	}

	// 7. Offline preferred asset with a visible online variant stays on the same volume (both volume & FIRST series).
	offlineMultiVol, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeVolume), readerPrincipal, "vol-cov-offline-with-online")
	if err != nil {
		t.Fatalf("GetCover(vol-cov-offline-with-online): %v", err)
	}
	assertDominantColor(t, offlineMultiVol.Data, 180, 40, 200)

	offlineFirstSer, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), readerPrincipal, "ser-cov-offline-first")
	if err != nil {
		t.Fatalf("GetCover(ser-cov-offline-first): %v", err)
	}
	assertDominantColor(t, offlineFirstSer.Data, 180, 40, 200)

	// 8. Structurally valid CBZ with damaged cover image falls back to next volume, or returns ErrNotFound (404) when none is usable.
	corruptFallback, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), readerPrincipal, "ser-cov-corrupt-fallback")
	if err != nil {
		t.Fatalf("GetCover(ser-cov-corrupt-fallback): %v", err)
	}
	assertDominantColor(t, corruptFallback.Data, 30, 210, 30)

	if _, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeVolume), readerPrincipal, "vol-cov-corrupt"); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("GetCover(vol-cov-corrupt) = %v, want ErrNotFound", err)
	}
	if _, err := svc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), readerPrincipal, "ser-cov-corrupt-only"); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("GetCover(ser-cov-corrupt-only) = %v, want ErrNotFound", err)
	}

	// 9. Infrastructure error (ErrNoPipeline) is preserved rather than masked as 404 fallback.
	noPipeSvc, _ := newTestCoverService(t, tx, tobitenimage.NewRegistry(), 2*time.Second)
	if _, err := noPipeSvc.GetCover(app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries), readerPrincipal, "ser-cov-alpha"); !errors.Is(err, tobitenimage.ErrNoPipeline) {
		t.Fatalf("no-pipeline GetCover = %v, want ErrNoPipeline", err)
	}
}

type countingTxBeginner struct {
	pool      *pgxpool.Pool
	queries   atomic.Int64
	begins    atomic.Int64
	onBeginTx func(call int64)
}

func (c *countingTxBeginner) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.queries.Add(1)
	return c.pool.Exec(ctx, sql, args...)
}

func (c *countingTxBeginner) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.queries.Add(1)
	return c.pool.Query(ctx, sql, args...)
}

func (c *countingTxBeginner) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	c.queries.Add(1)
	return c.pool.QueryRow(ctx, sql, args...)
}

func (c *countingTxBeginner) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &countingPgxTx{Tx: tx, queries: &c.queries}, nil
}

func (c *countingTxBeginner) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	call := c.begins.Add(1)
	if c.onBeginTx != nil {
		c.onBeginTx(call)
	}
	tx, err := c.pool.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}
	return &countingPgxTx{Tx: tx, queries: &c.queries}, nil
}

type countingPgxTx struct {
	pgx.Tx
	queries *atomic.Int64
}

func (c *countingPgxTx) Begin(ctx context.Context) (pgx.Tx, error) {
	sub, err := c.Tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &countingPgxTx{Tx: sub, queries: c.queries}, nil
}

func (c *countingPgxTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.queries.Add(1)
	return c.Tx.Exec(ctx, sql, args...)
}

func (c *countingPgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	c.queries.Add(1)
	return c.Tx.Query(ctx, sql, args...)
}

func (c *countingPgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	c.queries.Add(1)
	return c.Tx.QueryRow(ctx, sql, args...)
}

func TestCoverService_GetCover_LongSeriesBoundedWorkAndBatchedFallback(t *testing.T) {
	t.Parallel()

	rootDir := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	green := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	blue := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 40, B: 220, A: 255})
	orange := makeSolidPNG(t, 80, 120, color.RGBA{R: 230, G: 130, B: 20, A: 255})

	writeCBZ(t, []byte(filepath.Join(rootDir, "long-01.cbz")), cbzEntry{name: "001.png", data: blue})
	writeCBZ(t, []byte(filepath.Join(rootDir, "batch-healthy.cbz")), cbzEntry{name: "001.png", data: green})
	writeCBZ(t, []byte(filepath.Join(rootDir, "shift-healthy.cbz")), cbzEntry{name: "001.png", data: orange})
	writeCBZ(t, []byte(filepath.Join(rootDir, "batch-corrupt.cbz")), cbzEntry{name: "001.png", data: []byte("corrupt-png")})

	db := testdb.New(t)
	ctx := t.Context()
	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("migrations.RunFS: %v", err)
	}
	tx := postgres.NewTransactor(db.Pool)

	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		if err := st.Libraries().CreateLibrary(ctx, &domain.Library{
			ID:            "lib-long",
			Name:          "Long Series Lib",
			Mode:          domain.LibraryModeVirtual,
			ResolverChain: domain.DefaultResolverChain(),
		}); err != nil {
			return err
		}
		return st.Libraries().CreateLibraryRoot(ctx, &domain.LibraryRoot{
			ID:        "root-long",
			LibraryID: "lib-long",
			Path:      domain.NewPathIdentity([]byte(rootDir)),
			State:     domain.LibraryRootStateOnline,
		})
	}); err != nil {
		t.Fatalf("seed lib/root: %v", err)
	}

	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(ctx, "lib-long", scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, "root-long")
	byName := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byName[a.RelPathDisplay] = a
	}

	var readerUser domain.User
	var readerRole domain.Role
	now := time.Now().UTC().Truncate(time.Microsecond)

	if err := tx.WithTx(ctx, func(st storage.Tx) error {
		var ok bool
		readerRole, ok = domain.BuiltInRoleByName(domain.RoleReaderName)
		if !ok {
			return errors.New("missing Reader role")
		}
		readerUser = domain.User{ID: "u-long-reader", Username: "longreader", CreatedAt: now, UpdatedAt: now}
		if err := st.Users().CreateUser(ctx, &readerUser); err != nil {
			return err
		}
		if err := st.Users().AssignUserRole(ctx, readerUser.ID, readerRole.ID); err != nil {
			return err
		}

		cw := st.CatalogueWrites()
		sm := st.SeriesMemberships()
		for _, s := range []domain.Series{
			{ID: "ser-long-1", LibraryID: "lib-long", Title: "One Vol", TitleSort: "one vol", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-long-64", LibraryID: "lib-long", Title: "Sixty Four", TitleSort: "sixty four", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-batch-11", LibraryID: "lib-long", Title: "Batched Fallback", TitleSort: "batched fallback", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
			{ID: "ser-batch-shift", LibraryID: "lib-long", Title: "Batched Shift", TitleSort: "batched shift", Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now},
		} {
			ser := s
			if err := cw.CreateSeries(ctx, &ser); err != nil {
				return err
			}
		}

		seedVolWithAsset := func(seriesID, volID string, pos float64, tmplName string) error {
			vol := domain.Volume{ID: volID, LibraryID: "lib-long", Kind: domain.VolumeKindVolume, PageCount: 1, CreatedAt: now, UpdatedAt: now}
			if err := cw.CreateVolume(ctx, &vol); err != nil {
				return err
			}
			if err := sm.SetVolumeMemberships(ctx, volID, []domain.SeriesMembership{{
				VolumeID:  volID,
				SeriesID:  seriesID,
				IsPrimary: true,
				Kind:      domain.MembershipKindSeries,
				Position:  &pos,
				Source:    domain.MembershipSourceManual,
			}}); err != nil {
				return err
			}
			tmpl := byName[tmplName]
			dig, _ := tmpl.CurrentDigest(domain.DigestKindXXH364Partial)
			relName := fmt.Sprintf("%s-%s.cbz", seriesID, volID)
			raw, rErr := os.ReadFile(filepath.Join(rootDir, tmplName))
			if rErr != nil {
				return rErr
			}
			if wErr := os.WriteFile(filepath.Join(rootDir, relName), raw, 0o644); wErr != nil {
				return wErr
			}
			asset := domain.MediaAsset{
				ID:             "a-" + volID,
				VolumeID:       volID,
				LibraryRootID:  "root-long",
				RelPathBytes:   []byte(relName),
				RelPathDisplay: relName,
				RelPathMatch:   relName,
				FileSize:       int64(len(raw)),
				Format:         domain.MediaFormatCBZ,
				PageCount:      1,
				CoverPageIndex: 0,
				Digests: []domain.AssetDigest{{
					AssetID:   "a-" + volID,
					Kind:      domain.DigestKindXXH364Partial,
					Value:     dig.Value,
					IsCurrent: true,
					Reason:    domain.DigestReasonScan,
				}},
				CreatedAt: now,
				UpdatedAt: now,
			}
			return cw.CreateAsset(ctx, &asset)
		}

		if err := seedVolWithAsset("ser-long-1", "v-one-1", 1, "long-01.cbz"); err != nil {
			return err
		}
		for i := 1; i <= 64; i++ {
			if err := seedVolWithAsset("ser-long-64", fmt.Sprintf("v64-%02d", i), float64(i), "long-01.cbz"); err != nil {
				return err
			}
		}
		// 10 corrupt volumes (> seriesCoverBatchSize=8) followed by 1 healthy volume at position 11.
		for i := 1; i <= 10; i++ {
			if err := seedVolWithAsset("ser-batch-11", fmt.Sprintf("vb-%02d", i), float64(i), "batch-corrupt.cbz"); err != nil {
				return err
			}
		}
		if err := seedVolWithAsset("ser-batch-11", "vb-11", 11, "batch-healthy.cbz"); err != nil {
			return err
		}
		// Exactly 8 corrupt volumes (full batch 1) + 1 healthy volume at position 9 (batch 2).
		for i := 1; i <= 8; i++ {
			if err := seedVolWithAsset("ser-batch-shift", fmt.Sprintf("vs-%02d", i), float64(i), "batch-corrupt.cbz"); err != nil {
				return err
			}
		}
		return seedVolWithAsset("ser-batch-shift", "vs-09", 9, "shift-healthy.cbz")
	}); err != nil {
		t.Fatalf("seed long series fixture: %v", err)
	}

	counter := &countingTxBeginner{pool: db.Pool}
	countingTx := postgres.NewTransactor(counter)
	spy, reg := newSpyImagePipeline(t, nil)
	svc, _ := newTestCoverService(t, countingTx, reg, 2*time.Second)

	readerPrincipal := domain.NewPrincipal(readerUser, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-long", nil)
	seriesCtx := app.ContextWithCoverTargetKind(ctx, domain.ItemTypeSeries)

	// Warm both 1-volume and 64-volume first covers.
	if _, err := svc.GetCover(seriesCtx, readerPrincipal, "ser-long-1"); err != nil {
		t.Fatalf("warm ser-long-1: %v", err)
	}
	if _, err := svc.GetCover(seriesCtx, readerPrincipal, "ser-long-64"); err != nil {
		t.Fatalf("warm ser-long-64: %v", err)
	}

	// Warm first-cover lookup must execute the exact same bounded SQL query count for 1 and 64 volumes, with 0 renders.
	rendersBefore := spy.calls.Load()
	counter.queries.Store(0)
	if _, err := svc.GetCover(seriesCtx, readerPrincipal, "ser-long-1"); err != nil {
		t.Fatalf("warm hit ser-long-1: %v", err)
	}
	queriesOne := counter.queries.Load()

	counter.queries.Store(0)
	got64, err := svc.GetCover(seriesCtx, readerPrincipal, "ser-long-64")
	if err != nil {
		t.Fatalf("warm hit ser-long-64: %v", err)
	}
	queries64 := counter.queries.Load()
	if spy.calls.Load() != rendersBefore {
		t.Fatalf("warm first-cover triggered %d renders, want 0", spy.calls.Load()-rendersBefore)
	}
	if queries64 != queriesOne || queries64 > 6 {
		t.Fatalf("warm 64-volume series executed %d queries (1-vol=%d), want identical bounded count <= 6", queries64, queriesOne)
	}
	assertDominantColor(t, got64.Data, 30, 40, 220)

	// Batched fallback across 10 corrupt volumes (> batch size 8) reaches healthy position 11.
	gotBatch, err := svc.GetCover(seriesCtx, readerPrincipal, "ser-batch-11")
	if err != nil {
		t.Fatalf("ser-batch-11 GetCover: %v", err)
	}
	assertDominantColor(t, gotBatch.Data, 30, 210, 30)

	// Earlier candidate disappearing between batch 1 and batch 2 snapshots must not shift and skip vs-09.
	counter.begins.Store(0)
	counter.onBeginTx = func(call int64) {
		if call == 2 {
			if _, execErr := db.Exec(ctx, `UPDATE media_assets SET missing_since = NOW() WHERE id = 'a-vs-01'`); execErr != nil {
				t.Errorf("mark a-vs-01 missing between batches: %v", execErr)
			}
		}
	}
	gotShift, err := svc.GetCover(seriesCtx, readerPrincipal, "ser-batch-shift")
	counter.onBeginTx = nil
	if err != nil {
		t.Fatalf("ser-batch-shift GetCover after snapshot mutation: %v", err)
	}
	if begins := counter.begins.Load(); begins != 2 {
		t.Fatalf("ser-batch-shift read tx begins = %d, want 2 batches", begins)
	}
	assertDominantColor(t, gotShift.Data, 230, 130, 20)
}

type armingReadTx struct {
	inner storage.ReadTransactor
}

func (a *armingReadTx) WithRepeatableReadTx(ctx context.Context, fn func(tx storage.ReadTx) error) error {
	err := a.inner.WithRepeatableReadTx(ctx, fn)
	if w, ok := ctx.(*waitNotifyContext); ok {
		w.arm()
	}
	return err
}

type waitNotifyContext struct {
	context.Context
	armed  atomic.Bool
	once   sync.Once
	onWait func()
}

func (c *waitNotifyContext) arm() {
	c.armed.Store(true)
}

func (c *waitNotifyContext) Done() <-chan struct{} {
	if c.armed.Load() {
		c.once.Do(func() {
			if c.onWait != nil {
				c.onWait()
			}
		})
	}
	return c.Context.Done()
}

func TestCoverService_PipelinePanicCompletesTaskForJoinedCallersAndNextKeySucceeds(t *testing.T) {
	t.Parallel()
	rootDir := filepath.Join(t.TempDir(), "panic-root")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	red := makeSolidPNG(t, 80, 120, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	green := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	writeCBZ(t, []byte(filepath.Join(rootDir, "01_panic.cbz")), cbzEntry{name: "001.png", data: red})
	writeCBZ(t, []byte(filepath.Join(rootDir, "02_healthy.cbz")), cbzEntry{name: "001.png", data: green})

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	byRel := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byRel[a.RelPathDisplay] = a
	}
	panicAsset := byRel["01_panic.cbz"]
	healthyAsset := byRel["02_healthy.cbz"]

	var readerUser domain.User
	var readerRole domain.Role
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := tx.WithTx(t.Context(), func(st storage.Tx) error {
		var ok bool
		readerRole, ok = domain.BuiltInRoleByName(domain.RoleReaderName)
		if !ok {
			return errors.New("missing Reader role")
		}
		readerUser = domain.User{ID: "u-panic-reader", Username: "panicreader", CreatedAt: now, UpdatedAt: now}
		if err := st.Users().CreateUser(t.Context(), &readerUser); err != nil {
			return err
		}
		return st.Users().AssignUserRole(t.Context(), readerUser.ID, readerRole.ID)
	}); err != nil {
		t.Fatalf("seed reader user: %v", err)
	}

	renderStarted := make(chan struct{})
	releasePanic := make(chan struct{})
	var startOnce sync.Once
	var shouldPanic atomic.Bool
	shouldPanic.Store(true)

	spy, reg := newSpyImagePipeline(t, func(ctx context.Context) error {
		if shouldPanic.Load() {
			startOnce.Do(func() { close(renderStarted) })
			select {
			case <-releasePanic:
			case <-ctx.Done():
				return ctx.Err()
			}
			panic("boom: pipeline decoder panic")
		}
		return nil
	})

	svc, _ := newTestCoverService(t, &armingReadTx{inner: tx}, reg, 3*time.Second, func(o *curation.CoverServiceOptions) {
		o.Workers = 1
	})

	principal := domain.NewPrincipal(readerUser, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-panic", nil)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)

	const totalCallers = 4
	errs := make([]error, totalCallers)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errs[0] = svc.GetCover(volCtx, principal, panicAsset.VolumeID)
	}()

	select {
	case <-renderStarted:
	case <-time.After(3 * time.Second):
		close(releasePanic)
		t.Fatal("timed out waiting for first cover render to start")
	}

	var joinedCount atomic.Int32
	allJoined := make(chan struct{})
	for i := 1; i < totalCallers; i++ {
		wg.Add(1)
		waitCtx := &waitNotifyContext{
			Context: volCtx,
			onWait: func() {
				if joinedCount.Add(1) == totalCallers-1 {
					close(allJoined)
				}
			},
		}
		go func(idx int, ctx context.Context) {
			defer wg.Done()
			_, errs[idx] = svc.GetCover(ctx, principal, panicAsset.VolumeID)
		}(i, waitCtx)
	}

	select {
	case <-allJoined:
	case <-time.After(3 * time.Second):
		close(releasePanic)
		wg.Wait()
		t.Fatalf("timed out waiting for joined callers to reach task wait (joined=%d/%d)", joinedCount.Load(), totalCallers-1)
	}
	close(releasePanic)
	wg.Wait()

	if got := spy.calls.Load(); got != 1 {
		t.Fatalf("render calls across %d joined callers = %d, want 1", totalCallers, got)
	}
	for i, callerErr := range errs {
		if !errors.Is(callerErr, curation.ErrCoverUnavailable) {
			t.Fatalf("caller %d err = %v, want errors.Is(_, ErrCoverUnavailable)", i, callerErr)
		}
		var unavail *curation.CoverUnavailableError
		if !errors.As(callerErr, &unavail) {
			t.Fatalf("caller %d err %T does not match *CoverUnavailableError", i, callerErr)
		}
	}

	// In-flight entry was deleted, so a subsequent request for the same key runs anew rather than hanging.
	if _, err := svc.GetCover(volCtx, principal, panicAsset.VolumeID); !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("repeat request after panic err = %v, want ErrCoverUnavailable", err)
	}
	if got := spy.calls.Load(); got != 2 {
		t.Fatalf("render calls after repeat panic request = %d, want 2", got)
	}

	// Same single worker must remain alive and serve the next request for another key.
	shouldPanic.Store(false)
	gotHealthy, err := svc.GetCover(volCtx, principal, healthyAsset.VolumeID)
	if err != nil {
		t.Fatalf("next key GetCover after panic err = %v, want nil", err)
	}
	assertDominantColor(t, gotHealthy.Data, 30, 210, 30)
	if got := spy.calls.Load(); got != 3 {
		t.Fatalf("render calls after next key = %d, want 3", got)
	}
}

func writeOversizedPageCBZ(t *testing.T, path string, totalChunkBytes int) {
	t.Helper()
	basePNG := makeSolidPNG(t, 10, 10, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	const ihdrEnd = 8 + 4 + 4 + 13 + 4

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer func() {
		_ = f.Close()
	}()

	zw := zip.NewWriter(f)
	w, err := zw.Create("001.png")
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}
	if _, err := w.Write(basePNG[:ihdrEnd]); err != nil {
		t.Fatalf("write PNG header: %v", err)
	}

	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(totalChunkBytes))
	copy(hdr[4:8], "tEXt")
	if _, err := w.Write(hdr[:]); err != nil {
		t.Fatalf("write tEXt header: %v", err)
	}

	crc := crc32.NewIEEE()
	_, _ = crc.Write(hdr[4:8])
	chunk := bytes.Repeat([]byte{'a'}, 64*1024)
	remaining := totalChunkBytes
	for remaining > 0 {
		n := min(remaining, len(chunk))
		if _, err := w.Write(chunk[:n]); err != nil {
			t.Fatalf("write tEXt payload: %v", err)
		}
		_, _ = crc.Write(chunk[:n])
		remaining -= n
	}

	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc.Sum32())
	if _, err := w.Write(crcBuf[:]); err != nil {
		t.Fatalf("write tEXt crc: %v", err)
	}
	if _, err := w.Write(basePNG[ihdrEnd:]); err != nil {
		t.Fatalf("write PNG body: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("file close: %v", err)
	}
}

func TestCoverService_OversizedPageStreamUnavailableAndFallsBack(t *testing.T) {
	t.Parallel()
	rootDir := filepath.Join(t.TempDir(), "oversized-root")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}

	writeOversizedPageCBZ(t, filepath.Join(rootDir, "01_oversized.cbz"), (64<<20)+1024)
	green := makeSolidPNG(t, 80, 120, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	writeCBZ(t, []byte(filepath.Join(rootDir, "02_healthy.cbz")), cbzEntry{name: "001.png", data: green})

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	byRel := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byRel[a.RelPathDisplay] = a
	}
	oversizedAsset := byRel["01_oversized.cbz"]
	healthyAsset := byRel["02_healthy.cbz"]

	var readerUser domain.User
	var readerRole domain.Role
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := tx.WithTx(t.Context(), func(st storage.Tx) error {
		var ok bool
		readerRole, ok = domain.BuiltInRoleByName(domain.RoleReaderName)
		if !ok {
			return errors.New("missing Reader role")
		}
		readerUser = domain.User{ID: "u-oversized-reader", Username: "oversizedreader", CreatedAt: now, UpdatedAt: now}
		if err := st.Users().CreateUser(t.Context(), &readerUser); err != nil {
			return err
		}
		if err := st.Users().AssignUserRole(t.Context(), readerUser.ID, readerRole.ID); err != nil {
			return err
		}
		ser := domain.Series{
			ID: "ser-oversized-fallback", LibraryID: lib.ID, Title: "Oversized Fallback", TitleSort: "oversized fallback",
			Status: domain.SeriesStatusOngoing, Genres: []string{}, Tags: []string{}, CreatedAt: now, UpdatedAt: now,
		}
		if err := st.CatalogueWrites().CreateSeries(t.Context(), &ser); err != nil {
			return err
		}
		p1, p2 := 1.0, 2.0
		if err := st.SeriesMemberships().SetVolumeMemberships(t.Context(), oversizedAsset.VolumeID, []domain.SeriesMembership{
			{VolumeID: oversizedAsset.VolumeID, SeriesID: ser.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p1, Source: domain.MembershipSourceManual},
		}); err != nil {
			return err
		}
		return st.SeriesMemberships().SetVolumeMemberships(t.Context(), healthyAsset.VolumeID, []domain.SeriesMembership{
			{VolumeID: healthyAsset.VolumeID, SeriesID: ser.ID, IsPrimary: true, Kind: domain.MembershipKindSeries, Position: &p2, Source: domain.MembershipSourceManual},
		})
	}); err != nil {
		t.Fatalf("seed series and reader: %v", err)
	}

	_, reg := newSpyImagePipeline(t, nil)
	svc, _ := newTestCoverService(t, tx, reg, 10*time.Second)

	principal := domain.NewPrincipal(readerUser, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-oversized", nil)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)
	_, err = svc.GetCover(volCtx, principal, oversizedAsset.VolumeID)
	if !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("GetCover(oversized) err = %v, want ErrCoverUnavailable", err)
	}
	var unavail *curation.CoverUnavailableError
	if !errors.As(err, &unavail) || unavail.Reason != "page_too_large" {
		t.Fatalf("GetCover(oversized) unavail = %+v, want Reason=page_too_large", unavail)
	}

	seriesCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeSeries)
	got, err := svc.GetCover(seriesCtx, principal, "ser-oversized-fallback")
	if err != nil {
		t.Fatalf("GetCover(series fallback) err = %v, want nil", err)
	}
	assertDominantColor(t, got.Data, 30, 210, 30)
}

type countingPageStream struct {
	prefix               []byte
	totalBytes           int64
	pos                  int64
	bytesRead            atomic.Int64
	readCalls            atomic.Int32
	injectNoProgressAtEO bool
	injectedNoProgress   bool
}

func (s *countingPageStream) Read(p []byte) (int, error) {
	s.readCalls.Add(1)
	if len(p) == 0 {
		return 0, nil
	}
	if s.pos >= s.totalBytes {
		if s.injectNoProgressAtEO && !s.injectedNoProgress {
			s.injectedNoProgress = true
			return 0, nil
		}
		return 0, io.EOF
	}
	avail := s.totalBytes - s.pos
	if int64(len(p)) > avail {
		p = p[:avail]
	}
	for i := range p {
		off := s.pos + int64(i)
		if off < int64(len(s.prefix)) {
			p[i] = s.prefix[off]
		} else {
			p[i] = 'a'
		}
	}
	s.pos += int64(len(p))
	s.bytesRead.Add(int64(len(p)))
	return len(p), nil
}

func (s *countingPageStream) Close() error { return nil }

type streamOverrideReader struct {
	container.ContainerReader
	openFn func() io.ReadCloser
}

func (r *streamOverrideReader) Open(_ int) (io.ReadCloser, error) {
	return r.openFn(), nil
}

type countingStreamPipeline struct {
	renderFn func(ctx context.Context, src io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error)
}

func (p *countingStreamPipeline) Name() string { return "counting-stream" }
func (p *countingStreamPipeline) DecodeFormats() []tobitenimage.Codec {
	return []tobitenimage.Codec{tobitenimage.CodecPNG}
}
func (p *countingStreamPipeline) EncodeFormats() []tobitenimage.Codec {
	return []tobitenimage.Codec{tobitenimage.CodecJPEG}
}
func (p *countingStreamPipeline) RenderCover(ctx context.Context, src io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
	return p.renderFn(ctx, src, opts)
}

func TestCoverService_PageLimiterBoundaryAndRepeatedReadsAfterOverrun(t *testing.T) {
	t.Parallel()
	const pageLimit = int64(64 << 20)

	rootDir := filepath.Join(t.TempDir(), "limiter-root")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	pngData := makeSolidPNG(t, 16, 16, color.RGBA{R: 30, G: 210, B: 30, A: 255})
	writeCBZ(t, []byte(filepath.Join(rootDir, "exact.cbz")), cbzEntry{name: "001.png", data: pngData})
	writeCBZ(t, []byte(filepath.Join(rootDir, "overrun.cbz")), cbzEntry{name: "001.png", data: pngData})

	tx, lib, root := setupDBAndLibrary(t, []byte(rootDir))
	sc, err := scanner.NewScanner(tx, scanner.Options{})
	if err != nil {
		t.Fatalf("NewScanner: %v", err)
	}
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("ScanLibrary: %v", err)
	}

	assets := listRootAssets(t, tx, root.ID)
	byRel := make(map[string]domain.MediaAsset, len(assets))
	for _, a := range assets {
		byRel[a.RelPathDisplay] = a
	}
	principal := seedReaderPrincipal(t, tx)
	volCtx := app.ContextWithCoverTargetKind(t.Context(), domain.ItemTypeVolume)

	pngPrefix := []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")

	t.Run("exact boundary accepts empty reads, no-progress (0, nil), and EOF", func(t *testing.T) {
		stream := &countingPageStream{
			prefix:               pngPrefix,
			totalBytes:           pageLimit,
			injectNoProgressAtEO: true,
		}
		baseCBZ := cbz.Format()
		formats := container.NewFormatRegistry()
		if err := formats.Register(container.Format{
			Name:     "cbz",
			Priority: 10,
			Sniff:    baseCBZ.Sniff,
			Open: func(ra io.ReaderAt, size int64) (container.ContainerReader, error) {
				cr, err := baseCBZ.Open(ra, size)
				if err != nil {
					return nil, err
				}
				return &streamOverrideReader{
					ContainerReader: cr,
					openFn:          func() io.ReadCloser { return stream },
				}, nil
			},
		}); err != nil {
			t.Fatalf("register format: %v", err)
		}

		reg := tobitenimage.NewRegistry()
		if err := reg.Register(&countingStreamPipeline{
			renderFn: func(_ context.Context, src io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
				buf := make([]byte, 64*1024)
				var total int64
				for total < pageLimit {
					toRead := min(int64(len(buf)), pageLimit-total)
					n, readErr := src.Read(buf[:toRead])
					total += int64(n)
					if readErr != nil {
						return tobitenimage.Result{}, fmt.Errorf("unexpected read error before boundary at %d: %w", total, readErr)
					}
				}
				// Empty read at exact boundary must not probe upstream.
				callsBeforeEmpty := stream.readCalls.Load()
				nEmpty, errEmpty := src.Read(nil)
				if nEmpty != 0 || errEmpty != nil {
					return tobitenimage.Result{}, fmt.Errorf("empty read at boundary = (%d, %v), want (0, nil)", nEmpty, errEmpty)
				}
				if gotCalls := stream.readCalls.Load(); gotCalls != callsBeforeEmpty {
					return tobitenimage.Result{}, fmt.Errorf("empty read probed upstream (%d -> %d calls)", callsBeforeEmpty, gotCalls)
				}
				// First non-empty read at exact boundary surfaces upstream (0, nil).
				nZero, errZero := src.Read(buf[:1])
				if nZero != 0 || errZero != nil {
					return tobitenimage.Result{}, fmt.Errorf("upstream (0, nil) became (%d, %v)", nZero, errZero)
				}
				// Next read surfaces io.EOF.
				nEOF, errEOF := src.Read(buf[:1])
				if nEOF != 0 || !errors.Is(errEOF, io.EOF) {
					return tobitenimage.Result{}, fmt.Errorf("EOF read = (%d, %v), want (0, io.EOF)", nEOF, errEOF)
				}
				return tobitenimage.Result{
					Data:        []byte("exact-boundary-jpeg"),
					Format:      opts.OutputFormat,
					ContentType: opts.OutputFormat.ContentType(),
					SourceCodec: tobitenimage.CodecPNG,
				}, nil
			},
		}); err != nil {
			t.Fatalf("register pipeline: %v", err)
		}

		svc, _ := newTestCoverService(t, tx, reg, 5*time.Second, func(o *curation.CoverServiceOptions) {
			o.Formats = formats
		})
		rendition, err := svc.GetCover(volCtx, principal, byRel["exact.cbz"].VolumeID)
		if err != nil {
			t.Fatalf("GetCover(exact boundary) err = %v", err)
		}
		if string(rendition.Data) != "exact-boundary-jpeg" {
			t.Fatalf("rendition.Data = %q, want exact-boundary-jpeg", rendition.Data)
		}
		if got := stream.bytesRead.Load(); got != pageLimit {
			t.Fatalf("underlying bytesRead = %d, want %d", got, pageLimit)
		}
	})

	t.Run("repeated reads after overrun latch error without reading further bytes", func(t *testing.T) {
		stream := &countingPageStream{
			prefix:     pngPrefix,
			totalBytes: pageLimit + 1024,
		}
		baseCBZ := cbz.Format()
		formats := container.NewFormatRegistry()
		if err := formats.Register(container.Format{
			Name:     "cbz",
			Priority: 10,
			Sniff:    baseCBZ.Sniff,
			Open: func(ra io.ReaderAt, size int64) (container.ContainerReader, error) {
				cr, err := baseCBZ.Open(ra, size)
				if err != nil {
					return nil, err
				}
				return &streamOverrideReader{
					ContainerReader: cr,
					openFn:          func() io.ReadCloser { return stream },
				}, nil
			},
		}); err != nil {
			t.Fatalf("register format: %v", err)
		}

		var bytesAtOverrun int64
		var callsAtOverrun int32
		var bytesAfterRepeated int64
		var callsAfterRepeated int32

		reg := tobitenimage.NewRegistry()
		if err := reg.Register(&countingStreamPipeline{
			renderFn: func(_ context.Context, src io.Reader, _ tobitenimage.RenderOptions) (tobitenimage.Result, error) {
				buf := make([]byte, 64*1024)
				var firstErr error
				for {
					_, readErr := src.Read(buf)
					if readErr != nil {
						firstErr = readErr
						break
					}
				}
				bytesAtOverrun = stream.bytesRead.Load()
				callsAtOverrun = stream.readCalls.Load()

				for i := range 5 {
					n, repErr := src.Read(buf)
					if n != 0 || repErr == nil {
						return tobitenimage.Result{}, fmt.Errorf("repeat read %d = (%d, %v), want (0, latched error)", i, n, repErr)
					}
				}
				if n, repErr := src.Read(nil); n != 0 || repErr == nil {
					return tobitenimage.Result{}, fmt.Errorf("empty read after overrun = (%d, %v), want (0, latched error)", n, repErr)
				}
				bytesAfterRepeated = stream.bytesRead.Load()
				callsAfterRepeated = stream.readCalls.Load()
				return tobitenimage.Result{}, firstErr
			},
		}); err != nil {
			t.Fatalf("register pipeline: %v", err)
		}

		svc, _ := newTestCoverService(t, tx, reg, 5*time.Second, func(o *curation.CoverServiceOptions) {
			o.Formats = formats
		})
		_, err := svc.GetCover(volCtx, principal, byRel["overrun.cbz"].VolumeID)
		if !errors.Is(err, curation.ErrCoverUnavailable) {
			t.Fatalf("GetCover(overrun) err = %v, want ErrCoverUnavailable", err)
		}
		var unavail *curation.CoverUnavailableError
		if !errors.As(err, &unavail) || unavail.Reason != "page_too_large" {
			t.Fatalf("unavail = %+v, want Reason=page_too_large", unavail)
		}
		if bytesAtOverrun != pageLimit+1 {
			t.Fatalf("bytesAtOverrun = %d, want %d", bytesAtOverrun, pageLimit+1)
		}
		if bytesAfterRepeated != bytesAtOverrun || callsAfterRepeated != callsAtOverrun {
			t.Fatalf("repeated reads after overrun touched underlying stream: bytes %d->%d, calls %d->%d",
				bytesAtOverrun, bytesAfterRepeated, callsAtOverrun, callsAfterRepeated)
		}
	})
}
