package image

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/zeebo/xxh3"
)

// SourceKeyKind identifies the source-digest namespace of a cover cache entry
// (ADR-0015 §2, ADR-0018 S8).
type SourceKeyKind string

const (
	// SourceKeyArchiveXXH364Partial keys cover renditions derived from an
	// archive's current XXH3_64_PARTIAL fingerprint.
	SourceKeyArchiveXXH364Partial SourceKeyKind = "XXH3_64_PARTIAL"

	// SourceKeyImageXXH3128Full keys cover renditions derived from full sidecar
	// or uploaded image bytes using XXH3-128.
	SourceKeyImageXXH3128Full SourceKeyKind = "IMAGE_XXH3_128_FULL"
)

// NoPageIndex marks cover keys whose source has no page dimension (such as
// standalone sidecar or uploaded images keyed by SourceKeyImageXXH3128Full).
const NoPageIndex = -1

const (
	coverTempPrefix = ".tobiten-tmp-cover-"
	tempFilePattern = coverTempPrefix + "*"
	minDigestHexLen = 4
)

// Sentinel errors returned by CoverCache operations.
var (
	// ErrInvalidCacheDir means the configured covers directory path is invalid.
	ErrInvalidCacheDir = errors.New("image: invalid cover cache directory")
	// ErrInvalidCacheKey means a cover cache key or source is invalid.
	ErrInvalidCacheKey = errors.New("image: invalid cover cache key")
	// ErrEmptyCoverData means an empty encoded cover payload was passed to Put.
	ErrEmptyCoverData = errors.New("image: cover data cannot be empty")
	// ErrCacheMiss means no cached cover entry exists for the requested key.
	ErrCacheMiss = errors.New("image: cover cache miss")
)

// NormalizeSourceKeyKind canonicalizes a source key kind name.
func NormalizeSourceKeyKind(kind string) SourceKeyKind {
	return SourceKeyKind(strings.ToUpper(strings.TrimSpace(kind)))
}

// CoverImageDigest computes the canonical lowercase hex IMAGE_XXH3_128_FULL
// digest over raw image bytes (ADR-0015 §2).
func CoverImageDigest(data []byte) string {
	sum := xxh3.Hash128(data).Bytes()
	return hex.EncodeToString(sum[:])
}

// CoverImageDigestReader computes the canonical lowercase hex
// IMAGE_XXH3_128_FULL digest by streaming image bytes from r (ADR-0015 §2).
func CoverImageDigestReader(r io.Reader) (string, error) {
	if r == nil {
		return "", fmt.Errorf("%w: nil image reader", ErrCorrupt)
	}
	h := xxh3.New128()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("image: hash image bytes: %w", err)
	}
	sum := h.Sum128().Bytes()
	return hex.EncodeToString(sum[:]), nil
}

// CoverSource identifies a source namespace and content digest in CoverCache.
type CoverSource struct {
	Kind   SourceKeyKind
	Digest string
}

// ArchiveSource returns a CoverSource for an archive XXH3_64_PARTIAL fingerprint.
func ArchiveSource(digest string) CoverSource {
	return CoverSource{
		Kind:   SourceKeyArchiveXXH364Partial,
		Digest: digest,
	}
}

// CoverImageSource returns a CoverSource for an IMAGE_XXH3_128_FULL image digest.
func CoverImageSource(digest string) CoverSource {
	return CoverSource{
		Kind:   SourceKeyImageXXH3128Full,
		Digest: digest,
	}
}

// Normalize validates and canonicalizes s.
func (s CoverSource) Normalize() (CoverSource, error) {
	kind := NormalizeSourceKeyKind(string(s.Kind))
	if !isValidSlug(string(kind), 'A', 'Z') {
		return CoverSource{}, fmt.Errorf("%w: invalid source key kind %q", ErrInvalidCacheKey, s.Kind)
	}
	digest, err := normalizeHexDigest(s.Digest)
	if err != nil {
		return CoverSource{}, err
	}
	return CoverSource{
		Kind:   kind,
		Digest: digest,
	}, nil
}

func (s CoverSource) shards() (string, string) {
	return s.Digest[0:2], s.Digest[2:4]
}

// CoverKey uniquely identifies a cached cover rendition on disk (ADR-0015 §2,
// ADR-0018 S8).
type CoverKey struct {
	SourceKind SourceKeyKind
	Digest     string
	PageIndex  int
	Rendition  RenderOptions
}

// ArchiveCoverKey constructs a CoverKey for an archive page rendition.
func ArchiveCoverKey(digest string, pageIndex int, rendition RenderOptions) CoverKey {
	return CoverKey{
		SourceKind: SourceKeyArchiveXXH364Partial,
		Digest:     digest,
		PageIndex:  pageIndex,
		Rendition:  rendition,
	}
}

// CoverImageKey constructs a CoverKey for a sidecar or uploaded image rendition.
func CoverImageKey(digest string, rendition RenderOptions) CoverKey {
	return CoverKey{
		SourceKind: SourceKeyImageXXH3128Full,
		Digest:     digest,
		PageIndex:  NoPageIndex,
		Rendition:  rendition,
	}
}

// Source returns the CoverSource portion of k.
func (k CoverKey) Source() CoverSource {
	return CoverSource{
		Kind:   k.SourceKind,
		Digest: k.Digest,
	}
}

// Normalize validates and canonicalizes k, including its source key, page
// applicability, and rendition parameters.
func (k CoverKey) Normalize() (CoverKey, error) {
	src, err := k.Source().Normalize()
	if err != nil {
		return CoverKey{}, err
	}

	pageIndex, err := normalizePageIndex(src.Kind, k.PageIndex)
	if err != nil {
		return CoverKey{}, err
	}

	rend, err := k.Rendition.Validate()
	if err != nil {
		return CoverKey{}, err
	}
	if !isValidSlug(string(rend.OutputFormat), 'a', 'z') {
		return CoverKey{}, fmt.Errorf("%w: invalid output format %q", ErrInvalidCacheKey, rend.OutputFormat)
	}
	// Strip input-side decode hints/guards so rendition identity depends solely
	// on output parameters.
	rend.MaxPixels = 0
	rend.MaxPreflightBytes = 0
	rend.SourceCodec = ""

	return CoverKey{
		SourceKind: src.Kind,
		Digest:     src.Digest,
		PageIndex:  pageIndex,
		Rendition:  rend,
	}, nil
}

func normalizePageIndex(kind SourceKeyKind, page int) (int, error) {
	if kind == SourceKeyImageXXH3128Full {
		if page != 0 && page != NoPageIndex {
			return 0, fmt.Errorf("%w: page index %d is not applicable to %s", ErrInvalidCacheKey, page, kind)
		}
		return NoPageIndex, nil
	}
	if page < 0 {
		return 0, fmt.Errorf("%w: page index %d cannot be negative for %s", ErrInvalidCacheKey, page, kind)
	}
	return page, nil
}

func isValidSlug(s string, minAlpha, maxAlpha byte) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		c := s[i]
		isAlpha := c >= minAlpha && c <= maxAlpha
		isDigit := c >= '0' && c <= '9'
		if !isAlpha && !isDigit && c != '_' {
			return false
		}
	}
	return true
}

func normalizeHexDigest(raw string) (string, error) {
	d := strings.ToLower(strings.TrimSpace(raw))
	if len(d) < minDigestHexLen {
		return "", fmt.Errorf("%w: digest %q must be at least %d hex characters", ErrInvalidCacheKey, raw, minDigestHexLen)
	}
	for i := range len(d) {
		c := d[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("%w: digest %q must be hexadecimal", ErrInvalidCacheKey, raw)
		}
	}
	return d, nil
}

// CoverEntry describes a stored cover cache entry and its encoded image bytes.
type CoverEntry struct {
	Key         CoverKey
	Path        string
	ContentType string
	Size        int64
	Data        []byte
}

// CoverEntryInfo describes a stored cover cache entry without reading its payload.
type CoverEntryInfo struct {
	Key         CoverKey
	Path        string
	ContentType string
	Size        int64
}

type fsOps struct {
	createTemp func(dir, pattern string) (*os.File, error)
	writeAll   func(f *os.File, data []byte) error
	syncFile   func(f *os.File) error
	closeFile  func(f *os.File) error
	rename     func(oldPath, newPath string) error
	syncDir    func(dir string) error
	remove     func(path string) error
}

func defaultFSOps() fsOps {
	return fsOps{
		createTemp: os.CreateTemp,
		writeAll:   writeFullFile,
		syncFile:   func(f *os.File) error { return f.Sync() },
		closeFile:  func(f *os.File) error { return f.Close() },
		rename:     os.Rename,
		syncDir:    syncDirectory,
		remove:     os.Remove,
	}
}

func writeFullFile(f *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := f.Write(data)
		data = data[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// CoverCache stores persistent, content-addressed cover renditions in a
// two-level sharded directory tree under TOBITEN_COVERS_DIR (ADR-0015 §2, ADR-0018 S8).
type CoverCache struct {
	rootDir   string
	shards    [256]sync.Mutex
	rootMu    sync.Mutex
	dirtyMu   sync.Mutex
	dirtyDirs map[string]bool
	ops       fsOps
}

// NewCoverCache constructs a CoverCache rooted at rootDir (TOBITEN_COVERS_DIR).
func NewCoverCache(rootDir string) (*CoverCache, error) {
	cleaned := strings.TrimSpace(rootDir)
	if cleaned == "" {
		return nil, fmt.Errorf("%w: covers directory cannot be empty", ErrInvalidCacheDir)
	}
	cleaned = filepath.Clean(cleaned)
	info, err := os.Stat(cleaned)
	if err == nil && !info.IsDir() {
		return nil, fmt.Errorf("%w: %q is not a directory", ErrInvalidCacheDir, cleaned)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: stat %q: %v", ErrInvalidCacheDir, cleaned, err)
	}
	return &CoverCache{
		rootDir:   cleaned,
		dirtyDirs: make(map[string]bool),
		ops:       defaultFSOps(),
	}, nil
}

// RootDir returns the cleaned root directory path of the cover cache.
func (c *CoverCache) RootDir() string {
	return c.rootDir
}

// PathFor returns the deterministic two-level sharded file path for key under RootDir.
func (c *CoverCache) PathFor(key CoverKey) (string, error) {
	norm, err := key.Normalize()
	if err != nil {
		return "", err
	}
	return c.entryPath(norm), nil
}

func (c *CoverCache) entryPath(norm CoverKey) string {
	s1, s2 := norm.Source().shards()
	return filepath.Join(c.rootDir, s1, s2, entryFilename(norm))
}

func entryFilename(norm CoverKey) string {
	pagePart := "pna"
	if norm.PageIndex >= 0 {
		pagePart = "p" + strconv.Itoa(norm.PageIndex)
	}
	codec := norm.Rendition.OutputFormat
	ext := codec.Extension()
	if ext == "" {
		ext = "." + string(codec)
	}
	return fmt.Sprintf(
		"%s-%s-%s-w%d-h%d-q%d-%s%s",
		strings.ToLower(string(norm.SourceKind)),
		norm.Digest,
		pagePart,
		norm.Rendition.MaxWidth,
		norm.Rendition.MaxHeight,
		norm.Rendition.Quality,
		string(codec),
		ext,
	)
}

func parseEntryFilename(name string) (CoverKey, bool) {
	if name == "" || strings.HasPrefix(name, ".") {
		return CoverKey{}, false
	}
	parts := strings.Split(name, "-")
	if len(parts) != 7 {
		return CoverKey{}, false
	}

	kind := NormalizeSourceKeyKind(parts[0])
	digest := parts[1]

	pageIndex, ok := parsePagePart(parts[2])
	if !ok {
		return CoverKey{}, false
	}
	maxW, ok := parsePrefixedUint(parts[3], "w")
	if !ok {
		return CoverKey{}, false
	}
	maxH, ok := parsePrefixedUint(parts[4], "h")
	if !ok {
		return CoverKey{}, false
	}
	quality, ok := parsePrefixedUint(parts[5], "q")
	if !ok {
		return CoverKey{}, false
	}

	ext := filepath.Ext(parts[6])
	if ext == "" {
		return CoverKey{}, false
	}
	codecName := strings.TrimSuffix(parts[6], ext)

	candidate := CoverKey{
		SourceKind: kind,
		Digest:     digest,
		PageIndex:  pageIndex,
		Rendition: RenderOptions{
			MaxWidth:     maxW,
			MaxHeight:    maxH,
			Quality:      quality,
			OutputFormat: Codec(codecName),
		},
	}
	norm, err := candidate.Normalize()
	if err != nil {
		return CoverKey{}, false
	}
	if entryFilename(norm) != name {
		return CoverKey{}, false
	}
	return norm, true
}

func parsePagePart(part string) (int, bool) {
	if part == "pna" {
		return NoPageIndex, true
	}
	return parsePrefixedUint(part, "p")
}

func parsePrefixedUint(part, prefix string) (int, bool) {
	if !strings.HasPrefix(part, prefix) {
		return 0, false
	}
	v, err := strconv.Atoi(strings.TrimPrefix(part, prefix))
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

func shardIndex(digest string) int {
	b0 := hexNibble(digest[0])
	b1 := hexNibble(digest[1])
	return int(b0<<4 | b1)
}

func hexNibble(c byte) byte {
	if c >= '0' && c <= '9' {
		return c - '0'
	}
	return c - 'a' + 10
}

func parseShardByte(name string) (int, bool) {
	if len(name) != 2 {
		return 0, false
	}
	for i := range 2 {
		c := name[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return 0, false
		}
	}
	return shardIndex(name), true
}

// Get looks up and reads the cached cover rendition for key. Returns
// ErrCacheMiss when no complete entry exists.
func (c *CoverCache) Get(ctx context.Context, key CoverKey) (CoverEntry, error) {
	if err := ctx.Err(); err != nil {
		return CoverEntry{}, err
	}
	norm, err := key.Normalize()
	if err != nil {
		return CoverEntry{}, err
	}

	idx := shardIndex(norm.Digest)
	c.shards[idx].Lock()
	defer c.shards[idx].Unlock()

	path := c.entryPath(norm)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CoverEntry{}, ErrCacheMiss
		}
		return CoverEntry{}, fmt.Errorf("image: read cover cache entry %s: %w", path, err)
	}
	return CoverEntry{
		Key:         norm,
		Path:        path,
		ContentType: norm.Rendition.OutputFormat.ContentType(),
		Size:        int64(len(data)),
		Data:        data,
	}, nil
}

// Stat checks whether a cached cover entry exists for key without reading its
// image bytes. Returns ErrCacheMiss when absent.
func (c *CoverCache) Stat(ctx context.Context, key CoverKey) (CoverEntryInfo, error) {
	if err := ctx.Err(); err != nil {
		return CoverEntryInfo{}, err
	}
	norm, err := key.Normalize()
	if err != nil {
		return CoverEntryInfo{}, err
	}

	idx := shardIndex(norm.Digest)
	c.shards[idx].Lock()
	defer c.shards[idx].Unlock()

	path := c.entryPath(norm)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CoverEntryInfo{}, ErrCacheMiss
		}
		return CoverEntryInfo{}, fmt.Errorf("image: stat cover cache entry %s: %w", path, err)
	}
	if info.IsDir() {
		return CoverEntryInfo{}, ErrCacheMiss
	}
	return CoverEntryInfo{
		Key:         norm,
		Path:        path,
		ContentType: norm.Rendition.OutputFormat.ContentType(),
		Size:        info.Size(),
	}, nil
}

// Put writes a cover rendition atomically using the crash-safe protocol
// (temp file -> fsync -> rename -> directory fsync).
func (c *CoverCache) Put(ctx context.Context, key CoverKey, data []byte) (CoverEntry, error) {
	if err := ctx.Err(); err != nil {
		return CoverEntry{}, err
	}
	if len(data) == 0 {
		return CoverEntry{}, ErrEmptyCoverData
	}
	norm, err := key.Normalize()
	if err != nil {
		return CoverEntry{}, err
	}

	idx := shardIndex(norm.Digest)
	c.shards[idx].Lock()
	defer c.shards[idx].Unlock()

	s1, s2 := norm.Source().shards()
	shard2Dir, err := c.ensureShardDir(s1, s2)
	if err != nil {
		return CoverEntry{}, err
	}

	finalPath := filepath.Join(shard2Dir, entryFilename(norm))
	if err := c.writeCrashSafe(shard2Dir, finalPath, data); err != nil {
		return CoverEntry{}, err
	}

	copied := make([]byte, len(data))
	copy(copied, data)
	return CoverEntry{
		Key:         norm,
		Path:        finalPath,
		ContentType: norm.Rendition.OutputFormat.ContentType(),
		Size:        int64(len(copied)),
		Data:        copied,
	}, nil
}

func (c *CoverCache) writeCrashSafe(dir, finalPath string, data []byte) error {
	f, err := c.ops.createTemp(dir, tempFilePattern)
	if err != nil {
		return fmt.Errorf("image: create cover temp file: %w", err)
	}
	tmpPath := f.Name()
	closed := false
	cleanup := func() {
		if !closed {
			_ = c.ops.closeFile(f)
			closed = true
		}
		_ = c.ops.remove(tmpPath)
	}

	if err := c.ops.writeAll(f, data); err != nil {
		cleanup()
		return fmt.Errorf("image: write cover temp file: %w", err)
	}
	if err := c.ops.syncFile(f); err != nil {
		cleanup()
		return fmt.Errorf("image: fsync cover temp file: %w", err)
	}
	if err := c.ops.closeFile(f); err != nil {
		closed = true
		_ = c.ops.remove(tmpPath)
		return fmt.Errorf("image: close cover temp file: %w", err)
	}
	closed = true

	if err := c.ops.rename(tmpPath, finalPath); err != nil {
		_ = c.ops.remove(tmpPath)
		return fmt.Errorf("image: rename cover cache entry: %w", err)
	}
	c.markDirtyDir(dir)
	if err := c.syncDirDurable(dir); err != nil {
		return fmt.Errorf("image: fsync cover shard directory: %w", err)
	}
	return nil
}

func (c *CoverCache) markDirtyDir(dir string) {
	c.dirtyMu.Lock()
	c.dirtyDirs[dir] = true
	c.dirtyMu.Unlock()
}

func (c *CoverCache) clearDirtyDir(dir string) {
	c.dirtyMu.Lock()
	delete(c.dirtyDirs, dir)
	c.dirtyMu.Unlock()
}

func (c *CoverCache) isDirtyDir(dir string) bool {
	c.dirtyMu.Lock()
	dirty := c.dirtyDirs[dir]
	c.dirtyMu.Unlock()
	return dirty
}

func (c *CoverCache) hasDirtyDirs() bool {
	c.dirtyMu.Lock()
	has := len(c.dirtyDirs) > 0
	c.dirtyMu.Unlock()
	return has
}

func (c *CoverCache) syncDirDurable(dir string) error {
	if err := c.ops.syncDir(dir); err != nil {
		return err
	}
	c.clearDirtyDir(dir)
	return nil
}

func (c *CoverCache) flushDirtyDirIfPresent(dir string) error {
	if !c.isDirtyDir(dir) {
		return nil
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		c.clearDirtyDir(dir)
		return nil
	}
	return c.syncDirDurable(dir)
}

func (c *CoverCache) flushDirtyAncestors(target string) error {
	if !c.hasDirtyDirs() {
		return nil
	}
	var chain []string
	for cur := filepath.Clean(target); ; {
		if c.isDirtyDir(cur) {
			chain = append(chain, cur)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	for i := len(chain) - 1; i >= 0; i-- {
		if err := c.flushDirtyDirIfPresent(chain[i]); err != nil {
			return err
		}
	}
	return nil
}

func (c *CoverCache) ensureRootDir() error {
	c.rootMu.Lock()
	defer c.rootMu.Unlock()

	var missing []string
	cur := c.rootDir
	for {
		info, err := os.Stat(cur)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("%w: %q is not a directory", ErrInvalidCacheDir, cur)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("image: stat covers directory %s: %w", cur, err)
		}
		missing = append(missing, cur)
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}

	if err := c.flushDirtyAncestors(cur); err != nil {
		return fmt.Errorf("image: fsync covers ancestor directory: %w", err)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		dir := missing[i]
		if err := c.mkdirSynced(dir, filepath.Dir(dir)); err != nil {
			return fmt.Errorf("image: create covers directory %s: %w", dir, err)
		}
	}
	return nil
}

func (c *CoverCache) ensureShardDir(s1, s2 string) (string, error) {
	if err := c.ensureRootDir(); err != nil {
		return "", err
	}
	shard1Dir := filepath.Join(c.rootDir, s1)
	if err := c.mkdirSynced(shard1Dir, c.rootDir); err != nil {
		return "", fmt.Errorf("image: create cover shard directory %s: %w", shard1Dir, err)
	}
	shard2Dir := filepath.Join(shard1Dir, s2)
	if err := c.mkdirSynced(shard2Dir, shard1Dir); err != nil {
		return "", fmt.Errorf("image: create cover shard directory %s: %w", shard2Dir, err)
	}
	return shard2Dir, nil
}

func (c *CoverCache) mkdirSynced(dir, parent string) error {
	err := os.Mkdir(dir, 0o755)
	if err == nil {
		c.markDirtyDir(parent)
		return c.syncDirDurable(parent)
	}
	if errors.Is(err, os.ErrExist) {
		return c.flushDirtyDirIfPresent(parent)
	}
	return err
}

// Rekey moves cached renditions for from to to without re-encoding their
// image payloads. When a rendition already exists at the destination key, the
// existing destination entry is preserved and the superseded source entry is
// removed (not counted as moved). Returns the number of moved entries.
func (c *CoverCache) Rekey(ctx context.Context, from, to CoverSource) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	normFrom, err := from.Normalize()
	if err != nil {
		return 0, err
	}
	normTo, err := to.Normalize()
	if err != nil {
		return 0, err
	}
	if normFrom.Kind != normTo.Kind {
		return 0, fmt.Errorf("%w: cannot rekey across source kinds %s -> %s", ErrInvalidCacheKey, normFrom.Kind, normTo.Kind)
	}
	if normFrom == normTo {
		return 0, nil
	}

	idx1 := shardIndex(normFrom.Digest)
	idx2 := shardIndex(normTo.Digest)
	if idx1 == idx2 {
		c.shards[idx1].Lock()
		defer c.shards[idx1].Unlock()
	} else {
		lo, hi := min(idx1, idx2), max(idx1, idx2)
		c.shards[lo].Lock()
		c.shards[hi].Lock()
		defer c.shards[hi].Unlock()
		defer c.shards[lo].Unlock()
	}

	return c.rekeyLocked(ctx, normFrom, normTo)
}

func (c *CoverCache) rekeyLocked(ctx context.Context, from, to CoverSource) (int, error) {
	fromS1, fromS2 := from.shards()
	toS1, toS2 := to.shards()
	fromDir := filepath.Join(c.rootDir, fromS1, fromS2)
	toDir := filepath.Join(c.rootDir, toS1, toS2)

	matched, err := c.matchingKeysInDir(fromDir, from, nil)
	if err != nil {
		return 0, err
	}
	if len(matched) == 0 {
		if err := c.flushRekeyPending(fromS1, fromS2, toS1, toS2); err != nil {
			return 0, err
		}
		return 0, nil
	}

	moved := 0
	fromModified := false
	toModified := false
	toDirEnsured := false

	for _, k := range matched {
		if err := ctx.Err(); err != nil {
			c.syncMovedDirs(fromDir, toDir, fromModified, toModified)
			return moved, err
		}
		oldPath := filepath.Join(fromDir, entryFilename(k))
		newKey := k
		newKey.Digest = to.Digest
		newPath := filepath.Join(toDir, entryFilename(newKey))

		exists, err := fileExists(newPath)
		if err != nil {
			c.syncMovedDirs(fromDir, toDir, fromModified, toModified)
			return moved, fmt.Errorf("image: stat rekey destination %s: %w", newPath, err)
		}
		if exists {
			c.markDirtyDir(fromDir)
			if err := c.ops.remove(oldPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				c.syncMovedDirs(fromDir, toDir, fromModified, toModified)
				return moved, fmt.Errorf("image: remove superseded cover entry %s: %w", oldPath, err)
			}
			fromModified = true
			continue
		}

		if !toDirEnsured {
			if _, err := c.ensureShardDir(toS1, toS2); err != nil {
				c.syncMovedDirs(fromDir, toDir, fromModified, toModified)
				return moved, err
			}
			toDirEnsured = true
		}

		c.markDirtyDir(toDir)
		if fromDir != toDir {
			c.markDirtyDir(fromDir)
		}
		if err := c.ops.rename(oldPath, newPath); err != nil {
			c.syncMovedDirs(fromDir, toDir, fromModified, toModified)
			return moved, fmt.Errorf("image: rekey cover entry %s -> %s: %w", oldPath, newPath, err)
		}
		moved++
		toModified = true
		fromModified = true
	}

	if err := c.finishRekeySyncs(fromS1, fromS2, toS1, toS2, fromModified, toModified); err != nil {
		return moved, err
	}
	return moved, nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (c *CoverCache) matchingKeysInDir(dir string, source CoverSource, match func(CoverKey) bool) ([]CoverKey, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("image: read shard directory %s: %w", dir, err)
	}
	var matched []CoverKey
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		parsed, ok := parseEntryFilename(ent.Name())
		if !ok || parsed.Source() != source {
			continue
		}
		if match != nil && !match(parsed) {
			continue
		}
		matched = append(matched, parsed)
	}
	return matched, nil
}

func (c *CoverCache) finishRekeySyncs(fromS1, fromS2, toS1, toS2 string, fromModified, toModified bool) error {
	fromDir := filepath.Join(c.rootDir, fromS1, fromS2)
	toDir := filepath.Join(c.rootDir, toS1, toS2)

	if toModified || c.isDirtyDir(toDir) {
		if err := c.flushDirtyAncestors(toDir); err != nil {
			return fmt.Errorf("image: fsync target shard directory %s: %w", toDir, err)
		}
	}
	if fromDir != toDir {
		if fromModified || c.isDirtyDir(fromDir) {
			if err := c.flushDirtyDirIfPresent(fromDir); err != nil {
				return fmt.Errorf("image: fsync source shard directory %s: %w", fromDir, err)
			}
		}
		if err := c.flushDirtyDirIfPresent(filepath.Join(c.rootDir, fromS1)); err != nil {
			return fmt.Errorf("image: fsync source shard directory %s: %w", filepath.Join(c.rootDir, fromS1), err)
		}
		if err := c.pruneEmptyShardDirs(fromS1, fromS2); err != nil {
			return err
		}
		c.rootMu.Lock()
		err := c.flushDirtyDirIfPresent(c.rootDir)
		c.rootMu.Unlock()
		if err != nil {
			return fmt.Errorf("image: fsync covers root directory %s: %w", c.rootDir, err)
		}
	}
	return nil
}

func (c *CoverCache) flushRekeyPending(fromS1, fromS2, toS1, toS2 string) error {
	if !c.hasDirtyDirs() {
		return nil
	}
	return c.finishRekeySyncs(fromS1, fromS2, toS1, toS2, false, false)
}

func (c *CoverCache) syncMovedDirs(fromDir, toDir string, fromModified, toModified bool) {
	if toModified {
		_ = c.syncDirDurable(toDir)
	}
	if fromModified && fromDir != toDir {
		_ = c.syncDirDurable(fromDir)
	}
}

// Invalidate removes every cached rendition (across all pages, sizes, and
// output formats) belonging to source, and returns the number of removed entries.
func (c *CoverCache) Invalidate(ctx context.Context, source CoverSource) (int, error) {
	return c.invalidateMatching(ctx, source, nil)
}

// InvalidatePage removes every cached rendition belonging to source and pageIndex,
// and returns the number of removed entries.
func (c *CoverCache) InvalidatePage(ctx context.Context, source CoverSource, pageIndex int) (int, error) {
	normSrc, err := source.Normalize()
	if err != nil {
		return 0, err
	}
	normPage, err := normalizePageIndex(normSrc.Kind, pageIndex)
	if err != nil {
		return 0, err
	}
	return c.invalidateMatching(ctx, normSrc, func(k CoverKey) bool {
		return k.PageIndex == normPage
	})
}

func (c *CoverCache) invalidateMatching(ctx context.Context, source CoverSource, match func(CoverKey) bool) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	norm, err := source.Normalize()
	if err != nil {
		return 0, err
	}

	idx := shardIndex(norm.Digest)
	c.shards[idx].Lock()
	defer c.shards[idx].Unlock()

	s1, s2 := norm.shards()
	shard2Dir := filepath.Join(c.rootDir, s1, s2)
	matched, err := c.matchingKeysInDir(shard2Dir, norm, match)
	if err != nil {
		return 0, err
	}
	if len(matched) == 0 {
		if err := c.flushShardPending(s1, s2); err != nil {
			return 0, err
		}
		return 0, nil
	}

	removed := 0
	for _, k := range matched {
		if err := ctx.Err(); err != nil {
			if removed > 0 {
				_ = c.syncDirDurable(shard2Dir)
			}
			return removed, err
		}
		p := filepath.Join(shard2Dir, entryFilename(k))
		c.markDirtyDir(shard2Dir)
		if err := c.ops.remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			if removed > 0 {
				_ = c.syncDirDurable(shard2Dir)
			}
			return removed, fmt.Errorf("image: remove cover cache entry %s: %w", p, err)
		}
		removed++
	}

	if err := c.syncDirDurable(shard2Dir); err != nil {
		return removed, fmt.Errorf("image: fsync cover shard directory %s: %w", shard2Dir, err)
	}
	if err := c.pruneEmptyShardDirs(s1, s2); err != nil {
		return removed, err
	}
	return removed, nil
}

func (c *CoverCache) flushShardPending(s1, s2 string) error {
	if !c.hasDirtyDirs() {
		return nil
	}
	shard2Dir := filepath.Join(c.rootDir, s1, s2)
	if err := c.flushDirtyDirIfPresent(shard2Dir); err != nil {
		return fmt.Errorf("image: fsync cover shard directory %s: %w", shard2Dir, err)
	}
	shard1Dir := filepath.Join(c.rootDir, s1)
	if err := c.flushDirtyDirIfPresent(shard1Dir); err != nil {
		return fmt.Errorf("image: fsync cover shard directory %s: %w", shard1Dir, err)
	}
	if err := c.pruneEmptyShardDirs(s1, s2); err != nil {
		return err
	}
	c.rootMu.Lock()
	err := c.flushDirtyDirIfPresent(c.rootDir)
	c.rootMu.Unlock()
	if err != nil {
		return fmt.Errorf("image: fsync covers root directory %s: %w", c.rootDir, err)
	}
	return nil
}

func (c *CoverCache) removeEmptyShard1Dir(shard1Dir string) error {
	c.rootMu.Lock()
	defer c.rootMu.Unlock()
	if err := c.ops.remove(shard1Dir); err == nil {
		c.clearDirtyDir(shard1Dir)
		c.markDirtyDir(c.rootDir)
		if syncErr := c.syncDirDurable(c.rootDir); syncErr != nil {
			return fmt.Errorf("image: fsync covers root directory %s: %w", c.rootDir, syncErr)
		}
	}
	return nil
}

func (c *CoverCache) pruneEmptyShardDirs(s1, s2 string) error {
	shard1Dir := filepath.Join(c.rootDir, s1)
	shard2Dir := filepath.Join(shard1Dir, s2)
	if entries, err := os.ReadDir(shard2Dir); err == nil && len(entries) == 0 {
		if err := c.ops.remove(shard2Dir); err == nil {
			c.clearDirtyDir(shard2Dir)
			c.markDirtyDir(shard1Dir)
			if syncErr := c.syncDirDurable(shard1Dir); syncErr != nil {
				return fmt.Errorf("image: fsync cover shard directory %s: %w", shard1Dir, syncErr)
			}
		}
	}
	if entries, err := os.ReadDir(shard1Dir); err == nil && len(entries) == 0 {
		return c.removeEmptyShard1Dir(shard1Dir)
	}
	return nil
}

// CleanupTemp removes leftover cover-cache temporary files (.tobiten-tmp-cover-*)
// inside leaf shard directories (<RootDir>/<s1>/<s2>/), leaving foreign temporary
// files untouched, and returns the number of removed files.
func (c *CoverCache) CleanupTemp(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	topEntries, err := os.ReadDir(c.rootDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("image: read cover cache root %s: %w", c.rootDir, err)
	}

	removed := 0
	for _, ent := range topEntries {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if !ent.IsDir() {
			continue
		}
		idx, ok := parseShardByte(ent.Name())
		if !ok {
			continue
		}
		n, err := c.cleanupShardTemp(ctx, ent.Name(), idx)
		removed += n
		if err != nil {
			return removed, err
		}
	}
	if c.hasDirtyDirs() {
		c.rootMu.Lock()
		err := c.flushDirtyDirIfPresent(c.rootDir)
		c.rootMu.Unlock()
		if err != nil {
			return removed, fmt.Errorf("image: fsync covers root directory %s: %w", c.rootDir, err)
		}
	}
	return removed, nil
}

func (c *CoverCache) cleanupShardTemp(ctx context.Context, s1 string, idx int) (int, error) {
	c.shards[idx].Lock()
	defer c.shards[idx].Unlock()

	shard1Dir := filepath.Join(c.rootDir, s1)
	entries, err := os.ReadDir(shard1Dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("image: read cover shard directory %s: %w", shard1Dir, err)
	}

	removed := 0
	for _, ent := range entries {
		if err := ctx.Err(); err != nil {
			return removed, err
		}
		if !ent.IsDir() {
			continue
		}
		if _, ok := parseShardByte(ent.Name()); !ok {
			continue
		}
		n, err := c.cleanupLeafShardTemp(ctx, s1, ent.Name())
		removed += n
		if err != nil {
			return removed, err
		}
	}

	if err := c.flushDirtyDirIfPresent(shard1Dir); err != nil {
		return removed, fmt.Errorf("image: fsync cover shard directory %s: %w", shard1Dir, err)
	}
	return removed, nil
}

func (c *CoverCache) cleanupLeafShardTemp(ctx context.Context, s1, s2 string) (int, error) {
	dir := filepath.Join(c.rootDir, s1, s2)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("image: read cover shard directory %s: %w", dir, err)
	}

	removed := 0
	for _, ent := range entries {
		if err := ctx.Err(); err != nil {
			if removed > 0 {
				_ = c.syncDirDurable(dir)
			}
			return removed, err
		}
		if ent.IsDir() || !strings.HasPrefix(ent.Name(), coverTempPrefix) {
			continue
		}
		p := filepath.Join(dir, ent.Name())
		c.markDirtyDir(dir)
		if err := c.ops.remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			if removed > 0 {
				_ = c.syncDirDurable(dir)
			}
			return removed, fmt.Errorf("image: remove leftover cover temp %s: %w", p, err)
		}
		removed++
	}

	if removed > 0 {
		if err := c.syncDirDurable(dir); err != nil {
			return removed, fmt.Errorf("image: fsync cover shard directory %s: %w", dir, err)
		}
		if err := c.pruneEmptyShardDirs(s1, s2); err != nil {
			return removed, err
		}
	} else if err := c.flushShardPending(s1, s2); err != nil {
		return 0, err
	}
	return removed, nil
}
