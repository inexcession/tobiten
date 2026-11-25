package image_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	stdimage "image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	tobitenimage "github.com/inexcession/tobiten/internal/image"
	"github.com/inexcession/tobiten/internal/image/purego"
	"github.com/inexcession/tobiten/internal/library/scanner"
)

func TestCoverCache_TwoLevelShardedPaths(t *testing.T) {
	ctx := context.Background()
	coversDir := t.TempDir()
	cache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	key := tobitenimage.ArchiveCoverKey("ABCD1234EF567890", 0, tobitenimage.DefaultCoverOptions())
	payload := []byte("fake-jpeg-bytes")

	putEntry, err := cache.Put(ctx, key, payload)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	rel, err := filepath.Rel(coversDir, putEntry.Path)
	if err != nil {
		t.Fatalf("filepath.Rel: %v", err)
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 3 {
		t.Fatalf("relative path %q = %v, want 3 components (<s1>/<s2>/<file>)", rel, parts)
	}
	if parts[0] != "ab" || parts[1] != "cd" {
		t.Errorf("shards = (%q, %q), want (\"ab\", \"cd\")", parts[0], parts[1])
	}
	if filepath.Ext(parts[2]) != ".jpg" {
		t.Errorf("extension = %q, want .jpg", filepath.Ext(parts[2]))
	}
	if putEntry.ContentType != "image/jpeg" {
		t.Errorf("ContentType = %q, want image/jpeg", putEntry.ContentType)
	}

	// Lookup with lowercase digest and equivalent defaults must hit the exact same entry.
	lookupKey := tobitenimage.CoverKey{
		SourceKind: "xxh3_64_partial",
		Digest:     "abcd1234ef567890",
		PageIndex:  0,
		Rendition: tobitenimage.RenderOptions{
			MaxWidth:          400,
			MaxHeight:         tobitenimage.DefaultCoverMaxHeight,
			OutputFormat:      "jpg",
			Quality:           0,
			MaxPixels:         999,
			MaxPreflightBytes: 512,
			SourceCodec:       tobitenimage.CodecPNG,
		},
	}
	got, err := cache.Get(ctx, lookupKey)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Path != putEntry.Path {
		t.Errorf("Get.Path = %q, want %q", got.Path, putEntry.Path)
	}
	if !bytes.Equal(got.Data, payload) {
		t.Errorf("Get.Data = %q, want %q", got.Data, payload)
	}

	info, err := cache.Stat(ctx, lookupKey)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len(payload)) || info.Path != putEntry.Path {
		t.Errorf("Stat = %+v, want path %q size %d", info, putEntry.Path, len(payload))
	}
}

func TestCoverCache_ArchiveAndImageSameHexNeverCollide(t *testing.T) {
	ctx := context.Background()
	coversDir := t.TempDir()
	cache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	const sharedHex = "0123456789abcdef0123456789abcdef"
	opts := tobitenimage.DefaultCoverOptions()

	archiveKey := tobitenimage.ArchiveCoverKey(sharedHex, 0, opts)
	// Also construct the image key using a bare struct literal (PageIndex == 0)
	// to ensure zero-valued page index never collides with archive page 0.
	imageKey := tobitenimage.CoverKey{
		SourceKind: tobitenimage.SourceKeyImageXXH3128Full,
		Digest:     sharedHex,
		Rendition:  opts,
	}

	archiveBytes := []byte("archive-cover-rendition")
	imageBytes := []byte("uploaded-sidecar-rendition")

	arcEntry, err := cache.Put(ctx, archiveKey, archiveBytes)
	if err != nil {
		t.Fatalf("Put(archive): %v", err)
	}
	imgEntry, err := cache.Put(ctx, imageKey, imageBytes)
	if err != nil {
		t.Fatalf("Put(image): %v", err)
	}
	if arcEntry.Path == imgEntry.Path {
		t.Fatalf("archive and image paths collided at %q", arcEntry.Path)
	}
	if filepath.Dir(arcEntry.Path) != filepath.Dir(imgEntry.Path) {
		t.Errorf("expected both keys in same shard dir %q vs %q", filepath.Dir(arcEntry.Path), filepath.Dir(imgEntry.Path))
	}

	gotArc, err := cache.Get(ctx, archiveKey)
	if err != nil {
		t.Fatalf("Get(archive): %v", err)
	}
	if !bytes.Equal(gotArc.Data, archiveBytes) {
		t.Errorf("archive data = %q, want %q", gotArc.Data, archiveBytes)
	}

	gotImg, err := cache.Get(ctx, imageKey)
	if err != nil {
		t.Fatalf("Get(image): %v", err)
	}
	if !bytes.Equal(gotImg.Data, imageBytes) {
		t.Errorf("image data = %q, want %q", gotImg.Data, imageBytes)
	}
	if gotImg.Key.PageIndex != tobitenimage.NoPageIndex {
		t.Errorf("image PageIndex = %d, want NoPageIndex (%d)", gotImg.Key.PageIndex, tobitenimage.NoPageIndex)
	}

	// Re-keying archive source must leave same-hex image entry untouched.
	moved, err := cache.Rekey(ctx, tobitenimage.ArchiveSource(sharedHex), tobitenimage.ArchiveSource("fedcba9876543210fedcba9876543210"))
	if err != nil {
		t.Fatalf("Rekey(archive): %v", err)
	}
	if moved != 1 {
		t.Fatalf("Rekey moved = %d, want 1", moved)
	}
	if _, err := cache.Get(ctx, imageKey); err != nil {
		t.Fatalf("image entry disappeared after archive Rekey: %v", err)
	}

	// Invalidating image source must leave other keys intact.
	removed, err := cache.Invalidate(ctx, tobitenimage.CoverImageSource(sharedHex))
	if err != nil {
		t.Fatalf("Invalidate(image): %v", err)
	}
	if removed != 1 {
		t.Fatalf("Invalidate removed = %d, want 1", removed)
	}
	if _, err := cache.Get(ctx, imageKey); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Errorf("Get(image) after Invalidate = %v, want ErrCacheMiss", err)
	}
	rekeyedArc := tobitenimage.ArchiveCoverKey("fedcba9876543210fedcba9876543210", 0, opts)
	if _, err := cache.Get(ctx, rekeyedArc); err != nil {
		t.Errorf("rekeyed archive entry disappeared after image Invalidate: %v", err)
	}
}

func TestCoverCache_IdentityIncludesPageRenditionAndOutputFormat(t *testing.T) {
	ctx := context.Background()
	coversDir := t.TempDir()
	cache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	const digest = "deadbeefcafebabe"
	variants := []struct {
		name    string
		key     tobitenimage.CoverKey
		payload []byte
		wantCT  string
		wantExt string
	}{
		{
			name:    "page0_w400_q85_jpeg",
			key:     tobitenimage.ArchiveCoverKey(digest, 0, tobitenimage.RenderOptions{MaxWidth: 400, Quality: 85, OutputFormat: tobitenimage.CodecJPEG}),
			payload: []byte("p0-w400-q85-jpeg"),
			wantCT:  "image/jpeg",
			wantExt: ".jpg",
		},
		{
			name:    "page1_w400_q85_jpeg",
			key:     tobitenimage.ArchiveCoverKey(digest, 1, tobitenimage.RenderOptions{MaxWidth: 400, Quality: 85, OutputFormat: tobitenimage.CodecJPEG}),
			payload: []byte("p1-w400-q85-jpeg"),
			wantCT:  "image/jpeg",
			wantExt: ".jpg",
		},
		{
			name:    "page0_w800_q85_jpeg",
			key:     tobitenimage.ArchiveCoverKey(digest, 0, tobitenimage.RenderOptions{MaxWidth: 800, Quality: 85, OutputFormat: tobitenimage.CodecJPEG}),
			payload: []byte("p0-w800-q85-jpeg"),
			wantCT:  "image/jpeg",
			wantExt: ".jpg",
		},
		{
			name:    "page0_w400_h600_q85_jpeg",
			key:     tobitenimage.ArchiveCoverKey(digest, 0, tobitenimage.RenderOptions{MaxWidth: 400, MaxHeight: 600, Quality: 85, OutputFormat: tobitenimage.CodecJPEG}),
			payload: []byte("p0-w400-h600-q85-jpeg"),
			wantCT:  "image/jpeg",
			wantExt: ".jpg",
		},
		{
			name:    "page0_w400_q70_jpeg",
			key:     tobitenimage.ArchiveCoverKey(digest, 0, tobitenimage.RenderOptions{MaxWidth: 400, Quality: 70, OutputFormat: tobitenimage.CodecJPEG}),
			payload: []byte("p0-w400-q70-jpeg"),
			wantCT:  "image/jpeg",
			wantExt: ".jpg",
		},
		{
			name:    "page0_w400_q85_webp",
			key:     tobitenimage.ArchiveCoverKey(digest, 0, tobitenimage.RenderOptions{MaxWidth: 400, Quality: 85, OutputFormat: tobitenimage.CodecWebP}),
			payload: []byte("p0-w400-q85-webp"),
			wantCT:  "image/webp",
			wantExt: ".webp",
		},
		{
			name:    "page0_w400_q85_avif",
			key:     tobitenimage.ArchiveCoverKey(digest, 0, tobitenimage.RenderOptions{MaxWidth: 400, Quality: 85, OutputFormat: tobitenimage.CodecAVIF}),
			payload: []byte("p0-w400-q85-avif"),
			wantCT:  "image/avif",
			wantExt: ".avif",
		},
	}

	seenPaths := make(map[string]string)
	for _, v := range variants {
		entry, err := cache.Put(ctx, v.key, v.payload)
		if err != nil {
			t.Fatalf("Put(%s): %v", v.name, err)
		}
		if prev, dup := seenPaths[entry.Path]; dup {
			t.Fatalf("%s collided on path %q with %s", v.name, entry.Path, prev)
		}
		seenPaths[entry.Path] = v.name
		if entry.ContentType != v.wantCT {
			t.Errorf("%s ContentType = %q, want %q", v.name, entry.ContentType, v.wantCT)
		}
		if filepath.Ext(entry.Path) != v.wantExt {
			t.Errorf("%s extension = %q, want %q", v.name, filepath.Ext(entry.Path), v.wantExt)
		}
	}

	for _, v := range variants {
		got, err := cache.Get(ctx, v.key)
		if err != nil {
			t.Fatalf("Get(%s): %v", v.name, err)
		}
		if !bytes.Equal(got.Data, v.payload) {
			t.Errorf("Get(%s) = %q, want %q", v.name, got.Data, v.payload)
		}
	}
}

func TestCoverCache_RekeyMovesWithoutReencoding(t *testing.T) {
	ctx := context.Background()
	coversDir := t.TempDir()
	cache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	oldDigest := "1234567890abcdef"
	newDigest := "fedcba0987654321"
	sameShardDigest := "1234ffffffffffff"

	keys := []tobitenimage.CoverKey{
		tobitenimage.ArchiveCoverKey(oldDigest, 0, tobitenimage.DefaultCoverOptions()),
		tobitenimage.ArchiveCoverKey(oldDigest, 0, tobitenimage.RenderOptions{MaxWidth: 800, OutputFormat: tobitenimage.CodecWebP}),
		tobitenimage.ArchiveCoverKey(oldDigest, 2, tobitenimage.DefaultCoverOptions()),
	}
	payloads := [][]byte{
		[]byte("rendition-0-jpeg-exact-bytes"),
		[]byte("rendition-0-webp-exact-bytes"),
		[]byte("rendition-2-jpeg-exact-bytes"),
	}

	oldInfos := make([]os.FileInfo, len(keys))
	for i, k := range keys {
		entry, err := cache.Put(ctx, k, payloads[i])
		if err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		fi, err := os.Stat(entry.Path)
		if err != nil {
			t.Fatalf("Stat(%s): %v", entry.Path, err)
		}
		oldInfos[i] = fi
	}

	// Unrelated key in same shard must remain unaffected.
	unrelated := tobitenimage.ArchiveCoverKey("1234000000000000", 0, tobitenimage.DefaultCoverOptions())
	if _, err := cache.Put(ctx, unrelated, []byte("keep-me")); err != nil {
		t.Fatalf("Put(unrelated): %v", err)
	}

	// Rekey across shards: 12/34 -> fe/dc.
	moved, err := cache.Rekey(ctx, tobitenimage.ArchiveSource(oldDigest), tobitenimage.ArchiveSource(newDigest))
	if err != nil {
		t.Fatalf("Rekey(cross-shard): %v", err)
	}
	if moved != len(keys) {
		t.Fatalf("Rekey moved = %d, want %d", moved, len(keys))
	}

	for i, k := range keys {
		if _, err := cache.Get(ctx, k); !errors.Is(err, tobitenimage.ErrCacheMiss) {
			t.Errorf("old key %d still present: %v", i, err)
		}
		nk := k
		nk.Digest = newDigest
		got, err := cache.Get(ctx, nk)
		if err != nil {
			t.Fatalf("Get(rekeyed %d): %v", i, err)
		}
		if !bytes.Equal(got.Data, payloads[i]) {
			t.Errorf("rekeyed payload %d mismatch", i)
		}
		newInfo, err := os.Stat(got.Path)
		if err != nil {
			t.Fatalf("Stat(rekeyed %s): %v", got.Path, err)
		}
		if !os.SameFile(oldInfos[i], newInfo) {
			t.Errorf("entry %d was not moved in place (different inode/file identity)", i)
		}
	}

	// Rekey within the same shard: fe/dc -> 12/34 -> wait, test same shard fe/dc -> fe/dc.
	sameShardTarget := "fedc000011112222"
	movedSame, err := cache.Rekey(ctx, tobitenimage.ArchiveSource(newDigest), tobitenimage.ArchiveSource(sameShardTarget))
	if err != nil {
		t.Fatalf("Rekey(same-shard): %v", err)
	}
	if movedSame != len(keys) {
		t.Fatalf("same-shard Rekey moved = %d, want %d", movedSame, len(keys))
	}
	for i, k := range keys {
		nk := k
		nk.Digest = sameShardTarget
		got, err := cache.Get(ctx, nk)
		if err != nil {
			t.Fatalf("Get(same-shard %d): %v", i, err)
		}
		if !bytes.Equal(got.Data, payloads[i]) {
			t.Errorf("same-shard payload %d mismatch", i)
		}
	}

	// Unrelated entry must still exist.
	if _, err := cache.Get(ctx, unrelated); err != nil {
		t.Errorf("unrelated entry lost after Rekey: %v", err)
	}

	// Rekey absent source is a clean no-op.
	if n, err := cache.Rekey(ctx, tobitenimage.ArchiveSource(sameShardDigest), tobitenimage.ArchiveSource("aaaa111122223333")); err != nil || n != 0 {
		t.Errorf("absent Rekey = (%d, %v), want (0, nil)", n, err)
	}
}

func TestCoverCache_RekeyPreservesExistingDestinationRendition(t *testing.T) {
	ctx := context.Background()
	coversDir := t.TempDir()
	cache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	const oldDigest = "1111222233334444"
	const newDigest = "9999888877776666"

	opts400 := tobitenimage.DefaultCoverOptions()
	opts800 := tobitenimage.RenderOptions{MaxWidth: 800, OutputFormat: tobitenimage.CodecJPEG}

	old400 := tobitenimage.ArchiveCoverKey(oldDigest, 0, opts400)
	old800 := tobitenimage.ArchiveCoverKey(oldDigest, 0, opts800)
	new400 := tobitenimage.ArchiveCoverKey(newDigest, 0, opts400)
	new800 := tobitenimage.ArchiveCoverKey(newDigest, 0, opts800)

	if _, err := cache.Put(ctx, old400, []byte("old-400-bytes")); err != nil {
		t.Fatalf("Put(old400): %v", err)
	}
	old800Entry, err := cache.Put(ctx, old800, []byte("old-800-bytes"))
	if err != nil {
		t.Fatalf("Put(old800): %v", err)
	}
	old800Info, err := os.Stat(old800Entry.Path)
	if err != nil {
		t.Fatalf("Stat(old800): %v", err)
	}

	existingDestBytes := []byte("existing-destination-400-bytes")
	destEntry, err := cache.Put(ctx, new400, existingDestBytes)
	if err != nil {
		t.Fatalf("Put(new400): %v", err)
	}
	destInfo, err := os.Stat(destEntry.Path)
	if err != nil {
		t.Fatalf("Stat(new400): %v", err)
	}

	// old400 collides with existing new400 (must preserve new400 and remove old400);
	// old800 has no existing destination (must move in place to new800).
	moved, err := cache.Rekey(ctx, tobitenimage.ArchiveSource(oldDigest), tobitenimage.ArchiveSource(newDigest))
	if err != nil {
		t.Fatalf("Rekey: %v", err)
	}
	if moved != 1 {
		t.Fatalf("Rekey moved = %d, want 1", moved)
	}

	gotDest, err := cache.Get(ctx, new400)
	if err != nil {
		t.Fatalf("Get(new400): %v", err)
	}
	if !bytes.Equal(gotDest.Data, existingDestBytes) {
		t.Errorf("new400 bytes = %q, want preserved %q", gotDest.Data, existingDestBytes)
	}
	gotDestInfo, err := os.Stat(gotDest.Path)
	if err != nil {
		t.Fatalf("Stat(gotDest): %v", err)
	}
	if !os.SameFile(destInfo, gotDestInfo) {
		t.Errorf("existing destination file was replaced instead of preserved")
	}

	gotMoved, err := cache.Get(ctx, new800)
	if err != nil {
		t.Fatalf("Get(new800): %v", err)
	}
	if !bytes.Equal(gotMoved.Data, []byte("old-800-bytes")) {
		t.Errorf("new800 bytes = %q, want old-800-bytes", gotMoved.Data)
	}
	gotMovedInfo, err := os.Stat(gotMoved.Path)
	if err != nil {
		t.Fatalf("Stat(new800): %v", err)
	}
	if !os.SameFile(old800Info, gotMovedInfo) {
		t.Errorf("non-colliding entry was not moved in place")
	}

	if _, err := cache.Get(ctx, old400); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Errorf("superseded old400 still present: %v", err)
	}
	if _, err := cache.Get(ctx, old800); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Errorf("moved old800 still present: %v", err)
	}

	// An unusable destination (such as a directory) must fail and retain the valid source.
	const blockedOldDigest = "2222333344445555"
	const blockedNewDigest = "7777666655554444"
	blockedOldKey := tobitenimage.ArchiveCoverKey(blockedOldDigest, 0, opts400)
	blockedOldBytes := []byte("valid-source-preserved-on-dir-dest")
	if _, err := cache.Put(ctx, blockedOldKey, blockedOldBytes); err != nil {
		t.Fatalf("Put(blockedOld): %v", err)
	}
	blockedDestPath, err := cache.PathFor(tobitenimage.ArchiveCoverKey(blockedNewDigest, 0, opts400))
	if err != nil {
		t.Fatalf("PathFor(blockedDest): %v", err)
	}
	if err := os.MkdirAll(blockedDestPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(blockedDestPath): %v", err)
	}
	if _, err := cache.Rekey(ctx, tobitenimage.ArchiveSource(blockedOldDigest), tobitenimage.ArchiveSource(blockedNewDigest)); err == nil {
		t.Fatal("expected Rekey with directory destination to fail")
	}
	gotPreserved, err := cache.Get(ctx, blockedOldKey)
	if err != nil {
		t.Fatalf("source entry lost after invalid destination Rekey: %v", err)
	}
	if !bytes.Equal(gotPreserved.Data, blockedOldBytes) {
		t.Errorf("preserved source bytes = %q, want %q", gotPreserved.Data, blockedOldBytes)
	}
	if info, err := os.Stat(blockedDestPath); err != nil || !info.IsDir() {
		t.Errorf("destination directory altered: info=%v err=%v", info, err)
	}

	// Same-shard collision-only re-key preserves destination inode/bytes and removes source.
	const sameOldDigest = "3344111122223333"
	const sameNewDigest = "3344999988887777"
	sameOldKey := tobitenimage.ArchiveCoverKey(sameOldDigest, 0, opts400)
	sameNewKey := tobitenimage.ArchiveCoverKey(sameNewDigest, 0, opts400)
	if _, err := cache.Put(ctx, sameOldKey, []byte("same-shard-old")); err != nil {
		t.Fatalf("Put(sameOld): %v", err)
	}
	sameDestEntry, err := cache.Put(ctx, sameNewKey, []byte("same-shard-dest"))
	if err != nil {
		t.Fatalf("Put(sameNew): %v", err)
	}
	sameDestInfo, err := os.Stat(sameDestEntry.Path)
	if err != nil {
		t.Fatalf("Stat(sameNew): %v", err)
	}
	if movedSame, err := cache.Rekey(ctx, tobitenimage.ArchiveSource(sameOldDigest), tobitenimage.ArchiveSource(sameNewDigest)); err != nil || movedSame != 0 {
		t.Fatalf("same-shard collision Rekey = (%d, %v), want (0, nil)", movedSame, err)
	}
	gotSameDest, err := cache.Get(ctx, sameNewKey)
	if err != nil || !bytes.Equal(gotSameDest.Data, []byte("same-shard-dest")) {
		t.Fatalf("Get(sameNew) = (%v, %v)", gotSameDest, err)
	}
	gotSameDestInfo, err := os.Stat(gotSameDest.Path)
	if err != nil || !os.SameFile(sameDestInfo, gotSameDestInfo) {
		t.Errorf("same-shard destination file replaced or missing: err=%v", err)
	}
	if _, err := cache.Get(ctx, sameOldKey); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Errorf("same-shard superseded source still present: %v", err)
	}
}

func TestCoverCache_InvalidateRemovesEveryRendition(t *testing.T) {
	ctx := context.Background()
	coversDir := t.TempDir()
	cache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	const targetDigest = "a1b2c3d4e5f60718"
	const peerDigest = "a1b2ffffffffffff"

	targetKeys := []tobitenimage.CoverKey{
		tobitenimage.ArchiveCoverKey(targetDigest, 0, tobitenimage.DefaultCoverOptions()),
		tobitenimage.ArchiveCoverKey(targetDigest, 0, tobitenimage.RenderOptions{MaxWidth: 800, OutputFormat: tobitenimage.CodecJPEG}),
		tobitenimage.ArchiveCoverKey(targetDigest, 0, tobitenimage.RenderOptions{MaxWidth: 400, OutputFormat: tobitenimage.CodecWebP}),
		tobitenimage.ArchiveCoverKey(targetDigest, 1, tobitenimage.DefaultCoverOptions()),
		tobitenimage.ArchiveCoverKey(targetDigest, 1, tobitenimage.RenderOptions{MaxWidth: 600, OutputFormat: tobitenimage.CodecAVIF}),
	}
	for i, k := range targetKeys {
		if _, err := cache.Put(ctx, k, []byte{byte('a' + i)}); err != nil {
			t.Fatalf("Put(target %d): %v", i, err)
		}
	}

	peerKey := tobitenimage.ArchiveCoverKey(peerDigest, 0, tobitenimage.DefaultCoverOptions())
	if _, err := cache.Put(ctx, peerKey, []byte("peer")); err != nil {
		t.Fatalf("Put(peer): %v", err)
	}
	sameHexImageKey := tobitenimage.CoverImageKey(targetDigest, tobitenimage.DefaultCoverOptions())
	if _, err := cache.Put(ctx, sameHexImageKey, []byte("image-same-hex")); err != nil {
		t.Fatalf("Put(sameHexImage): %v", err)
	}

	// InvalidatePage removes only page 0 renditions.
	removedPage0, err := cache.InvalidatePage(ctx, tobitenimage.ArchiveSource(targetDigest), 0)
	if err != nil {
		t.Fatalf("InvalidatePage(0): %v", err)
	}
	if removedPage0 != 3 {
		t.Fatalf("InvalidatePage(0) removed = %d, want 3", removedPage0)
	}
	for i, k := range targetKeys[:3] {
		if _, err := cache.Get(ctx, k); !errors.Is(err, tobitenimage.ErrCacheMiss) {
			t.Errorf("page 0 rendition %d still present after InvalidatePage: %v", i, err)
		}
	}
	for i, k := range targetKeys[3:] {
		if _, err := cache.Get(ctx, k); err != nil {
			t.Errorf("page 1 rendition %d missing after InvalidatePage(0): %v", i, err)
		}
	}

	// Full Invalidate removes remaining renditions of targetDigest.
	removedRest, err := cache.Invalidate(ctx, tobitenimage.ArchiveSource(targetDigest))
	if err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if removedRest != 2 {
		t.Fatalf("Invalidate removed = %d, want 2", removedRest)
	}
	for i, k := range targetKeys {
		if _, err := cache.Get(ctx, k); !errors.Is(err, tobitenimage.ErrCacheMiss) {
			t.Errorf("rendition %d still present after Invalidate: %v", i, err)
		}
	}

	// Peer key in same shard and image key with same hex must remain intact.
	if _, err := cache.Get(ctx, peerKey); err != nil {
		t.Errorf("peer entry lost after Invalidate: %v", err)
	}
	if _, err := cache.Get(ctx, sameHexImageKey); err != nil {
		t.Errorf("same-hex image entry lost after archive Invalidate: %v", err)
	}
}

func TestCoverCache_IgnoresAndRemovesLeftoverTempFiles(t *testing.T) {
	ctx := context.Background()
	coversDir := t.TempDir()
	cache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	key := tobitenimage.ArchiveCoverKey("5566778899aabbcc", 0, tobitenimage.DefaultCoverOptions())
	finalPath, err := cache.PathFor(key)
	if err != nil {
		t.Fatalf("PathFor: %v", err)
	}
	shardDir := filepath.Dir(finalPath)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	coverTempPaths := []string{
		filepath.Join(shardDir, ".tobiten-tmp-cover-12345"),
		filepath.Join(shardDir, ".tobiten-tmp-cover-"+filepath.Base(finalPath)),
	}
	foreignTempPaths := []string{
		filepath.Join(coversDir, ".tobiten-tmp-job42-repack"),
		filepath.Join(coversDir, ".tobiten-tmp-cover-outside-leaf"),
		filepath.Join(coversDir, "55", ".tobiten-tmp-job42-shard1"),
		filepath.Join(coversDir, "55", ".tobiten-tmp-cover-outside-leaf"),
		filepath.Join(shardDir, ".tobiten-tmp-job42-leaf"),
	}
	for _, tp := range append(slices.Clone(coverTempPaths), foreignTempPaths...) {
		if err := os.WriteFile(tp, []byte("incomplete-temp"), 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", tp, err)
		}
	}

	// Leftover temp files must be ignored by Get, Stat, Rekey, and Invalidate.
	if _, err := cache.Get(ctx, key); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Fatalf("Get with only temp files = %v, want ErrCacheMiss", err)
	}
	if _, err := cache.Stat(ctx, key); !errors.Is(err, tobitenimage.ErrCacheMiss) {
		t.Fatalf("Stat with only temp files = %v, want ErrCacheMiss", err)
	}
	if moved, err := cache.Rekey(ctx, key.Source(), tobitenimage.ArchiveSource("0011223344556677")); err != nil || moved != 0 {
		t.Fatalf("Rekey ignored temps = (%d, %v), want (0, nil)", moved, err)
	}
	if removed, err := cache.Invalidate(ctx, key.Source()); err != nil || removed != 0 {
		t.Fatalf("Invalidate ignored temps = (%d, %v), want (0, nil)", removed, err)
	}

	// Also place a valid complete entry in the same shard directory to ensure CleanupTemp preserves it.
	completeKey := tobitenimage.ArchiveCoverKey("5566000011112222", 0, tobitenimage.DefaultCoverOptions())
	if _, err := cache.Put(ctx, completeKey, []byte("valid-entry")); err != nil {
		t.Fatalf("Put(complete): %v", err)
	}

	removed, err := cache.CleanupTemp(ctx)
	if err != nil {
		t.Fatalf("CleanupTemp: %v", err)
	}
	if removed != len(coverTempPaths) {
		t.Fatalf("CleanupTemp removed = %d, want %d", removed, len(coverTempPaths))
	}
	for _, tp := range coverTempPaths {
		if _, err := os.Stat(tp); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("cover temp file %q still exists after CleanupTemp: %v", tp, err)
		}
	}
	for _, fp := range foreignTempPaths {
		if _, err := os.Stat(fp); err != nil {
			t.Errorf("foreign temp file %q did not survive CleanupTemp: %v", fp, err)
		}
	}
	if got, err := cache.Get(ctx, completeKey); err != nil || !bytes.Equal(got.Data, []byte("valid-entry")) {
		t.Errorf("complete entry damaged by CleanupTemp: (%v, %v)", got, err)
	}
}

func TestCoverCache_HitAfterSourceFileMovedOrRenamed(t *testing.T) {
	ctx := context.Background()
	coversDir := t.TempDir()
	mediaDir := t.TempDir()

	cache, err := tobitenimage.NewCoverCache(coversDir)
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}
	pipe, err := purego.New(purego.Options{})
	if err != nil {
		t.Fatalf("purego.New: %v", err)
	}

	// Build a valid CBZ containing a synthetic PNG cover page.
	pngBytes := makeTestPNGBytes(t, 600, 900, color.RGBA{R: 30, G: 120, B: 210, A: 255})
	cbzBytes := makeTestCBZBytes(t, "001.png", pngBytes)

	origCBZ := filepath.Join(mediaDir, "Series v001.cbz")
	if err := os.WriteFile(origCBZ, cbzBytes, 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", origCBZ, err)
	}

	fpBefore := fingerprintFile(t, origCBZ)
	rendered, err := pipe.RenderCover(ctx, bytes.NewReader(pngBytes), tobitenimage.DefaultCoverOptions())
	if err != nil {
		t.Fatalf("RenderCover: %v", err)
	}

	archiveKey := tobitenimage.ArchiveCoverKey(fpBefore, 0, tobitenimage.DefaultCoverOptions())
	if _, err := cache.Put(ctx, archiveKey, rendered.Data); err != nil {
		t.Fatalf("Put(archive): %v", err)
	}

	// Move and rename the CBZ into a nested subdirectory.
	movedCBZ := filepath.Join(mediaDir, "Series", "Renamed Volume 01.cbz")
	if err := os.MkdirAll(filepath.Dir(movedCBZ), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.Rename(origCBZ, movedCBZ); err != nil {
		t.Fatalf("os.Rename: %v", err)
	}

	fpAfter := fingerprintFile(t, movedCBZ)
	if fpAfter != fpBefore {
		t.Fatalf("fingerprint changed across move: %q -> %q", fpBefore, fpAfter)
	}

	hit, err := cache.Get(ctx, tobitenimage.ArchiveCoverKey(fpAfter, 0, tobitenimage.DefaultCoverOptions()))
	if err != nil {
		t.Fatalf("Get after CBZ move: %v", err)
	}
	if !bytes.Equal(hit.Data, rendered.Data) {
		t.Fatalf("cached JPEG bytes changed after CBZ move")
	}

	// Also verify image-byte-keyed sidecars survive file moves/renames.
	sidecarOrig := filepath.Join(mediaDir, "cover.png")
	if err := os.WriteFile(sidecarOrig, pngBytes, 0o600); err != nil {
		t.Fatalf("WriteFile(sidecar): %v", err)
	}
	imgDigestBefore := hashImageFile(t, sidecarOrig)
	imgKeyBefore := tobitenimage.CoverImageKey(imgDigestBefore, tobitenimage.DefaultCoverOptions())
	if _, err := cache.Put(ctx, imgKeyBefore, rendered.Data); err != nil {
		t.Fatalf("Put(sidecar): %v", err)
	}

	sidecarMoved := filepath.Join(mediaDir, "Series", "poster.png")
	if err := os.Rename(sidecarOrig, sidecarMoved); err != nil {
		t.Fatalf("Rename(sidecar): %v", err)
	}
	imgDigestAfter := hashImageFile(t, sidecarMoved)
	if imgDigestAfter != imgDigestBefore {
		t.Fatalf("CoverImageDigest changed across move: %q -> %q", imgDigestBefore, imgDigestAfter)
	}
	if imgDigestAfter != tobitenimage.CoverImageDigest(pngBytes) {
		t.Fatalf("CoverImageDigestReader = %q, want CoverImageDigest = %q", imgDigestAfter, tobitenimage.CoverImageDigest(pngBytes))
	}
	if _, err := cache.Get(ctx, tobitenimage.CoverImageKey(imgDigestAfter, tobitenimage.DefaultCoverOptions())); err != nil {
		t.Fatalf("Get after sidecar move: %v", err)
	}
}

func TestCoverCache_ValidationAndErrors(t *testing.T) {
	ctx := context.Background()

	if _, err := tobitenimage.NewCoverCache("   "); !errors.Is(err, tobitenimage.ErrInvalidCacheDir) {
		t.Errorf("empty rootDir err = %v, want ErrInvalidCacheDir", err)
	}
	fileAsDir := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(fileAsDir, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := tobitenimage.NewCoverCache(fileAsDir); !errors.Is(err, tobitenimage.ErrInvalidCacheDir) {
		t.Errorf("file rootDir err = %v, want ErrInvalidCacheDir", err)
	}

	cache, err := tobitenimage.NewCoverCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	invalidKeys := []tobitenimage.CoverKey{
		{SourceKind: "", Digest: "0123456789abcdef", PageIndex: 0, Rendition: tobitenimage.DefaultCoverOptions()},
		{SourceKind: "BAD-KIND", Digest: "0123456789abcdef", PageIndex: 0, Rendition: tobitenimage.DefaultCoverOptions()},
		{SourceKind: tobitenimage.SourceKeyArchiveXXH364Partial, Digest: "abc", PageIndex: 0, Rendition: tobitenimage.DefaultCoverOptions()},
		{SourceKind: tobitenimage.SourceKeyArchiveXXH364Partial, Digest: "../etc/passwd", PageIndex: 0, Rendition: tobitenimage.DefaultCoverOptions()},
		{SourceKind: tobitenimage.SourceKeyArchiveXXH364Partial, Digest: "0123456789abcdef", PageIndex: -1, Rendition: tobitenimage.DefaultCoverOptions()},
		{SourceKind: tobitenimage.SourceKeyImageXXH3128Full, Digest: "0123456789abcdef", PageIndex: 1, Rendition: tobitenimage.DefaultCoverOptions()},
		{SourceKind: tobitenimage.SourceKeyImageXXH3128Full, Digest: "0123456789abcdef", PageIndex: -2, Rendition: tobitenimage.DefaultCoverOptions()},
		{SourceKind: tobitenimage.SourceKeyArchiveXXH364Partial, Digest: "0123456789abcdef", PageIndex: 0, Rendition: tobitenimage.RenderOptions{}},
		{SourceKind: tobitenimage.SourceKeyArchiveXXH364Partial, Digest: "0123456789abcdef", PageIndex: 0, Rendition: tobitenimage.RenderOptions{MaxWidth: 400, OutputFormat: "bad-fmt"}},
	}
	for i, k := range invalidKeys {
		if _, err := cache.PathFor(k); err == nil {
			t.Errorf("invalid key %d (%+v) unexpectedly succeeded", i, k)
		}
		if _, err := cache.Put(ctx, k, []byte("x")); err == nil {
			t.Errorf("Put with invalid key %d unexpectedly succeeded", i)
		}
	}

	valid := tobitenimage.ArchiveCoverKey("0123456789abcdef", 0, tobitenimage.DefaultCoverOptions())
	if _, err := cache.Put(ctx, valid, nil); !errors.Is(err, tobitenimage.ErrEmptyCoverData) {
		t.Errorf("Put(empty) err = %v, want ErrEmptyCoverData", err)
	}
	if _, err := cache.Rekey(ctx, tobitenimage.ArchiveSource("0123456789abcdef"), tobitenimage.CoverImageSource("0123456789abcdef")); !errors.Is(err, tobitenimage.ErrInvalidCacheKey) {
		t.Errorf("cross-kind Rekey err = %v, want ErrInvalidCacheKey", err)
	}
	if _, err := tobitenimage.CoverImageDigestReader(nil); !errors.Is(err, tobitenimage.ErrCorrupt) {
		t.Errorf("CoverImageDigestReader(nil) err = %v, want ErrCorrupt", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := cache.Get(canceled, valid); !errors.Is(err, context.Canceled) {
		t.Errorf("Get(canceled) err = %v, want context.Canceled", err)
	}
}

func TestCoverCache_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	cache, err := tobitenimage.NewCoverCache(t.TempDir())
	if err != nil {
		t.Fatalf("NewCoverCache: %v", err)
	}

	const workers = 8
	const opsPerWorker = 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*opsPerWorker)

	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range opsPerWorker {
				digest := fmt.Sprintf("aa%02x%012x", workerID%4, i)
				key := tobitenimage.ArchiveCoverKey(digest, i%3, tobitenimage.DefaultCoverOptions())
				payload := []byte(fmt.Sprintf("worker-%d-iter-%d", workerID, i))

				if _, err := cache.Put(ctx, key, payload); err != nil {
					errs <- err
					return
				}
				if _, err := cache.Get(ctx, key); err != nil && !errors.Is(err, tobitenimage.ErrCacheMiss) {
					errs <- err
					return
				}
				if i%5 == 0 {
					if _, err := cache.Invalidate(ctx, key.Source()); err != nil {
						errs <- err
						return
					}
				}
				if i%7 == 0 {
					newDigest := fmt.Sprintf("bb%02x%012x", workerID%4, i)
					if _, err := cache.Rekey(ctx, key.Source(), tobitenimage.ArchiveSource(newDigest)); err != nil {
						errs <- err
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent CoverCache error: %v", err)
	}
	if _, err := cache.CleanupTemp(ctx); err != nil {
		t.Fatalf("CleanupTemp: %v", err)
	}
}

func TestCoverCache_TraversalOnlyExternalAncestor(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses 0111 directory permissions")
	}

	ctx := context.Background()
	layouts := []struct {
		name           string
		preprovisioned bool
	}{
		{name: "pre_provisioned_covers_root", preprovisioned: true},
		{name: "lazily_created_covers_root_under_writable_child", preprovisioned: false},
	}

	for _, tc := range layouts {
		t.Run(tc.name, func(t *testing.T) {
			extDir := filepath.Join(t.TempDir(), "ext")
			var coversDir string
			if tc.preprovisioned {
				coversDir = filepath.Join(extDir, "covers")
				if err := os.MkdirAll(coversDir, 0o755); err != nil {
					t.Fatalf("MkdirAll: %v", err)
				}
			} else {
				writable := filepath.Join(extDir, "writable")
				if err := os.MkdirAll(writable, 0o755); err != nil {
					t.Fatalf("MkdirAll: %v", err)
				}
				coversDir = filepath.Join(writable, "covers")
			}

			if err := os.Chmod(extDir, 0o111); err != nil {
				t.Fatalf("Chmod(0111): %v", err)
			}
			t.Cleanup(func() {
				_ = os.Chmod(extDir, 0o755)
			})

			cache, err := tobitenimage.NewCoverCache(coversDir)
			if err != nil {
				t.Fatalf("NewCoverCache: %v", err)
			}

			key := tobitenimage.ArchiveCoverKey("1122334455667788", 0, tobitenimage.DefaultCoverOptions())
			payload := []byte("cover-under-0111-ancestor")
			if _, err := cache.Put(ctx, key, payload); err != nil {
				t.Fatalf("Put: %v", err)
			}
			got, err := cache.Get(ctx, key)
			if err != nil || !bytes.Equal(got.Data, payload) {
				t.Fatalf("Get = (%v, %v), want %q", got, err, payload)
			}

			newDigest := "99aabbccddeeff00"
			if moved, err := cache.Rekey(ctx, key.Source(), tobitenimage.ArchiveSource(newDigest)); err != nil || moved != 1 {
				t.Fatalf("Rekey = (%d, %v), want (1, nil)", moved, err)
			}
			if _, err := cache.Get(ctx, tobitenimage.ArchiveCoverKey(newDigest, 0, tobitenimage.DefaultCoverOptions())); err != nil {
				t.Fatalf("Get after Rekey: %v", err)
			}
			if removed, err := cache.Invalidate(ctx, tobitenimage.ArchiveSource(newDigest)); err != nil || removed != 1 {
				t.Fatalf("Invalidate = (%d, %v), want (1, nil)", removed, err)
			}
		})
	}

	t.Run("writable_unreadable_parent_0300_requires_sync", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "writable-unreadable")
		if err := os.MkdirAll(parent, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.Chmod(parent, 0o300); err != nil {
			t.Fatalf("Chmod(0300): %v", err)
		}
		t.Cleanup(func() {
			_ = os.Chmod(parent, 0o755)
		})

		coversDir := filepath.Join(parent, "covers")
		cache, err := tobitenimage.NewCoverCache(coversDir)
		if err != nil {
			t.Fatalf("NewCoverCache: %v", err)
		}

		key := tobitenimage.ArchiveCoverKey("1122334455667788", 0, tobitenimage.DefaultCoverOptions())
		if _, err := cache.Put(ctx, key, []byte("cover")); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("Put under 0300 parent err = %v, want os.ErrPermission", err)
		}
		if _, err := os.Stat(coversDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("coversDir survived rollback under 0300 parent: %v", err)
		}

		// Even when coversDir already exists under the 0300 parent (such as after
		// an interrupted creation), reopened Put must not bypass the parent sync.
		if err := os.Mkdir(coversDir, 0o755); err != nil {
			t.Fatalf("Mkdir(coversDir): %v", err)
		}
		reopened, err := tobitenimage.NewCoverCache(coversDir)
		if err != nil {
			t.Fatalf("reopen NewCoverCache: %v", err)
		}
		if _, err := reopened.Put(ctx, key, []byte("cover")); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("reopened Put with existing coversDir under 0300 parent err = %v, want os.ErrPermission", err)
		}

		if err := os.Chmod(parent, 0o755); err != nil {
			t.Fatalf("Chmod(0755): %v", err)
		}
		if _, err := reopened.Put(ctx, key, []byte("cover")); err != nil {
			t.Fatalf("Put after Chmod(0755): %v", err)
		}
	})
}

func fingerprintFile(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat(%s): %v", path, err)
	}
	fp, err := scanner.PartialFingerprint(f, info.Size())
	if err != nil {
		t.Fatalf("PartialFingerprint(%s): %v", path, err)
	}
	return fp
}

func hashImageFile(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	defer f.Close()
	d, err := tobitenimage.CoverImageDigestReader(f)
	if err != nil {
		t.Fatalf("CoverImageDigestReader(%s): %v", path, err)
	}
	return d
}

func makeTestPNGBytes(t *testing.T, w, h int, c color.RGBA) []byte {
	t.Helper()
	img := stdimage.NewRGBA(stdimage.Rect(0, 0, w, h))
	for y := range h {
		for x := range w {
			img.SetRGBA(x, y, c)
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("png.Encode: %v", err)
	}
	return buf.Bytes()
}

func makeTestCBZBytes(t *testing.T, entryName string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create(entryName)
	if err != nil {
		t.Fatalf("zip.Create: %v", err)
	}
	if _, err := w.Write(content); err != nil {
		t.Fatalf("zip.Write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip.Close: %v", err)
	}
	return buf.Bytes()
}
