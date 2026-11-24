package curation_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

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
	tx storage.Transactor,
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
		Tx:          tx,
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
	svc, _ := newTestCoverService(t, tx, reg, 5*time.Second)

	fcAsset := byRel["frontcover.cbz"]
	if fcAsset.CoverPageIndex != 2 {
		t.Fatalf("frontcover CoverPageIndex = %d, want 2", fcAsset.CoverPageIndex)
	}
	fcEntry, err := svc.GetOrGenerateAssetCover(t.Context(), fcAsset.ID)
	if err != nil {
		t.Fatalf("GetOrGenerateAssetCover(frontcover): %v", err)
	}
	if fcEntry.Key.PageIndex != 2 {
		t.Fatalf("frontcover Key.PageIndex = %d, want 2", fcEntry.Key.PageIndex)
	}
	assertDominantColor(t, fcEntry.Data, 30, 40, 220)

	supAsset := byRel["suppressed_first.cbz"]
	if supAsset.CoverPageIndex != 1 {
		t.Fatalf("suppressed_first CoverPageIndex = %d, want 1", supAsset.CoverPageIndex)
	}
	supEntry, err := svc.GetOrGenerateAssetCover(t.Context(), supAsset.ID)
	if err != nil {
		t.Fatalf("GetOrGenerateAssetCover(suppressed_first): %v", err)
	}
	if supEntry.Key.PageIndex != 1 {
		t.Fatalf("suppressed_first Key.PageIndex = %d, want 1", supEntry.Key.PageIndex)
	}
	assertDominantColor(t, supEntry.Data, 30, 210, 30)

	plainAsset := byRel["plain.cbz"]
	plainEntry, err := svc.GetOrGenerateAssetCover(t.Context(), plainAsset.ID)
	if err != nil {
		t.Fatalf("GetOrGenerateAssetCover(plain): %v", err)
	}
	if plainEntry.Key.PageIndex != 0 {
		t.Fatalf("plain Key.PageIndex = %d, want 0", plainEntry.Key.PageIndex)
	}
	assertDominantColor(t, plainEntry.Data, 220, 30, 30)

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

	fallbackEntry, err := svc.GetOrGenerateAssetCover(t.Context(), plainAsset.ID)
	if err != nil {
		t.Fatalf("GetOrGenerateAssetCover(plain after suppressing page 0): %v", err)
	}
	if fallbackEntry.Key.PageIndex != 1 {
		t.Fatalf("fallback Key.PageIndex = %d, want 1", fallbackEntry.Key.PageIndex)
	}
	assertDominantColor(t, fallbackEntry.Data, 30, 210, 30)

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
	if _, err := svc.GetOrGenerateAssetCover(t.Context(), plainAsset.ID); !errors.Is(err, curation.ErrCoverUnavailable) {
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

	nfcEntry, err := svc.GetOrGenerateAssetCover(t.Context(), findByBytes(nfcRel).ID)
	if err != nil {
		t.Fatalf("NFC cover error: %v", err)
	}
	assertDominantColor(t, nfcEntry.Data, 220, 30, 30)

	nfdEntry, err := svc.GetOrGenerateAssetCover(t.Context(), findByBytes(nfdRel).ID)
	if err != nil {
		t.Fatalf("NFD cover error: %v", err)
	}
	assertDominantColor(t, nfdEntry.Data, 30, 210, 30)

	if invalidCreated {
		invEntry, err := svc.GetOrGenerateAssetCover(t.Context(), findByBytes(invalidRel).ID)
		if err != nil {
			t.Fatalf("invalid UTF-8 cover error: %v", err)
		}
		assertDominantColor(t, invEntry.Data, 30, 40, 220)
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
				_, errs[idx] = svc.GetOrGenerateAssetCover(t.Context(), assets[idx].ID)
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
		svc, cache := newTestCoverService(t, tx, reg, 40*time.Millisecond)
		target := assets[0]
		curDigest, ok := target.CurrentDigest(domain.DigestKindXXH364Partial)
		if !ok {
			t.Fatal("missing XXH3_64_PARTIAL digest")
		}
		cacheKey := tobitenimage.ArchiveCoverKey(curDigest.Value, target.CoverPageIndex, tobitenimage.DefaultCoverOptions())

		// Caller A triggers generation and times out past 40ms.
		timeoutErrCh := make(chan error, 1)
		go func() {
			_, err := svc.GetOrGenerateAssetCover(t.Context(), target.ID)
			timeoutErrCh <- err
		}()

		select {
		case <-renderStarted:
		case <-time.After(3 * time.Second):
			t.Fatal("render never started")
		}

		// Launch 6 concurrent callers sharing the exact same in-flight key with longer wait.
		const sharedCallers = 6
		sharedResults := make([]tobitenimage.CoverEntry, sharedCallers)
		sharedErrs := make([]error, sharedCallers)
		var wg sync.WaitGroup
		for i := range sharedCallers {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				sharedResults[idx], sharedErrs[idx] = svc.GetOrGenerateAssetCover(
					t.Context(),
					target.ID,
					curation.CoverRequestOptions{WaitTimeout: 3 * time.Second},
				)
			}(i)
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
		retryEntry, err := svc.GetOrGenerateAssetCover(t.Context(), target.ID)
		if err != nil {
			t.Fatalf("retry after cache completion err: %v", err)
		}
		if !bytes.Equal(retryEntry.Data, sharedResults[0].Data) {
			t.Fatal("retry entry bytes differed from shared result")
		}
		if got := spy.calls.Load(); got != 1 {
			t.Fatalf("render calls after cache hit = %d, want 1", got)
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

	entry1, err := svc.GetOrGenerateAssetCover(t.Context(), vol1.ID)
	if err != nil {
		t.Fatalf("initial GetOrGenerateAssetCover: %v", err)
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

	entryMoved, err := svc.GetOrGenerateAssetCover(t.Context(), vol1.ID)
	if err != nil {
		t.Fatalf("moved GetOrGenerateAssetCover: %v", err)
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

	entryMod, err := svc.GetOrGenerateAssetCover(t.Context(), vol1.ID)
	if err != nil {
		t.Fatalf("modified GetOrGenerateAssetCover: %v", err)
	}
	assertDominantColor(t, entryMod.Data, 30, 210, 30)
	if spy.calls.Load() != 2 {
		t.Fatalf("render calls after external modify = %d, want 2", spy.calls.Load())
	}
	if entryMod.Key.Digest != digest2.Value {
		t.Fatalf("modified entry digest = %s, want %s", entryMod.Key.Digest, digest2.Value)
	}
	if _, err := cache.Stat(t.Context(), tobitenimage.ArchiveCoverKey(digest2.Value, 0, tobitenimage.DefaultCoverOptions())); err != nil {
		t.Fatalf("Stat new digest cache entry: %v", err)
	}

	// Cold request after an unscanned external edit must fail verification and leave old key empty.
	if err := os.WriteFile(coldPath, coldGreenCBZ, 0o644); err != nil {
		t.Fatalf("overwrite cold cbz before cold request: %v", err)
	}
	if _, err := svc.GetOrGenerateAssetCover(t.Context(), cold.ID); !errors.Is(err, curation.ErrCoverUnavailable) {
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
		_, gErr := svc.GetOrGenerateAssetCover(t.Context(), cold.ID)
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

	entryColdMod, err := svc.GetOrGenerateAssetCover(t.Context(), cold.ID)
	if err != nil {
		t.Fatalf("cold post-scan GetOrGenerateAssetCover: %v", err)
	}
	assertDominantColor(t, entryColdMod.Data, 30, 210, 30)
	if entryColdMod.Key.Digest != coldDigest2.Value {
		t.Fatalf("cold regenerated digest = %s, want %s", entryColdMod.Key.Digest, coldDigest2.Value)
	}
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

	if _, err := svc.GetOrGenerateAssetCover(t.Context(), reverted.ID); !errors.Is(err, curation.ErrCoverUnavailable) {
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

	// Unknown asset returns app.ErrNotFound.
	if _, err := svc.GetOrGenerateAssetCover(t.Context(), "nonexistent-asset"); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("unknown asset error = %v, want app.ErrNotFound", err)
	}

	// Prime cache for a.cbz before it goes missing/retired.
	if _, err := svc.GetOrGenerateAssetCover(t.Context(), byName["a.cbz"].ID); err != nil {
		t.Fatalf("prime a.cbz cover: %v", err)
	}

	// Remove b.cbz on disk WITHOUT rescanning -> file_missing unavailable error.
	if err := os.Remove(fileB); err != nil {
		t.Fatalf("remove b.cbz: %v", err)
	}
	_, err = svc.GetOrGenerateAssetCover(t.Context(), byName["b.cbz"].ID)
	if !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("unscanned missing file error = %v, want ErrCoverUnavailable", err)
	}
	var unavailErr *curation.CoverUnavailableError
	if !errors.As(err, &unavailErr) || unavailErr.Reason != "file_missing" {
		t.Fatalf("unavailErr = %+v, want Reason=file_missing", unavailErr)
	}

	// Delete a.cbz and rescan so a.cbz is marked missing -> ErrCoverUnavailable ("missing").
	if err := os.Remove(fileA); err != nil {
		t.Fatalf("remove a.cbz: %v", err)
	}
	writeCBZ(t, []byte(fileB), cbzEntry{name: "001.png", data: green}) // keep root non-empty so it stays ONLINE
	if _, err := sc.ScanLibrary(t.Context(), lib.ID, scanner.ScanOptions{}); err != nil {
		t.Fatalf("rescan with missing a.cbz: %v", err)
	}

	_, err = svc.GetOrGenerateAssetCover(t.Context(), byName["a.cbz"].ID)
	if !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("missing asset error = %v, want ErrCoverUnavailable", err)
	}
	if !errors.As(err, &unavailErr) || unavailErr.Reason != "missing" {
		t.Fatalf("missing unavailErr = %+v, want Reason=missing", unavailErr)
	}

	// Retire a.cbz via grace-period expiry -> ErrCoverUnavailable ("retired").
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
	_, err = svc.GetOrGenerateAssetCover(t.Context(), byName["a.cbz"].ID)
	if !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("retired asset error = %v, want ErrCoverUnavailable", err)
	}
	if !errors.As(err, &unavailErr) || unavailErr.Reason != "retired" {
		t.Fatalf("retired unavailErr = %+v, want Reason=retired", unavailErr)
	}

	// Prime cache for b.cbz, then make the root directory OFFLINE -> ErrCoverUnavailable ("root_offline").
	if _, err := svc.GetOrGenerateAssetCover(t.Context(), byName["b.cbz"].ID); err != nil {
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

	_, err = svc.GetOrGenerateAssetCover(t.Context(), byName["b.cbz"].ID)
	if !errors.Is(err, curation.ErrCoverUnavailable) {
		t.Fatalf("offline root asset error = %v, want ErrCoverUnavailable", err)
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

	rootDir := t.TempDir()
	cbzPath := filepath.Join(rootDir, "restart.cbz")
	red := makeSolidPNG(t, 64, 96, color.RGBA{R: 220, G: 30, B: 30, A: 255})
	cbzBytes := buildCBZ(t, cbzEntry{name: "001.png", data: red})
	if err := os.WriteFile(cbzPath, cbzBytes, 0o644); err != nil {
		t.Fatalf("write restart cbz: %v", err)
	}
	restartDigest, err := scanner.PartialFingerprint(bytes.NewReader(cbzBytes), int64(len(cbzBytes)))
	if err != nil {
		t.Fatalf("PartialFingerprint: %v", err)
	}

	src := curation.CoverAssetSource{
		AssetID:        "a-1",
		LibraryRootID:  "r-1",
		RootPathBytes:  []byte(rootDir),
		RootState:      domain.LibraryRootStateOnline,
		RelPathBytes:   []byte("restart.cbz"),
		Format:         domain.MediaFormatCBZ,
		DigestKind:     domain.DigestKindXXH364Partial,
		Digest:         restartDigest,
		PageCount:      1,
		CoverPageIndex: 0,
	}

	// Not started yet -> ErrCoverPoolNotRunning.
	svc1, err := curation.NewCoverService(curation.CoverServiceOptions{Cache: cache, Pipelines: reg})
	if err != nil {
		t.Fatalf("NewCoverService: %v", err)
	}
	if _, err := svc1.GetOrGenerateCover(t.Context(), src); !errors.Is(err, curation.ErrCoverPoolNotRunning) {
		t.Fatalf("before Start err = %v, want ErrCoverPoolNotRunning", err)
	}

	// Interrupt mid-generation by cancelling daemon lifecycle context / stopping svc1.
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
		_, gErr := svcInterrupted.GetOrGenerateCover(t.Context(), src)
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
	if _, err := svcInterrupted.GetOrGenerateCover(t.Context(), src); !errors.Is(err, curation.ErrCoverPoolClosed) {
		t.Fatalf("after Stop err = %v, want ErrCoverPoolClosed", err)
	}

	// Simulated daemon restart with a new CoverService instance regenerates cleanly.
	svcRestarted, err := curation.NewCoverService(curation.CoverServiceOptions{
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

	entry, err := svcRestarted.GetOrGenerateCover(t.Context(), src)
	if err != nil {
		t.Fatalf("restarted GetOrGenerateCover: %v", err)
	}
	assertDominantColor(t, entry.Data, 220, 30, 30)
}
