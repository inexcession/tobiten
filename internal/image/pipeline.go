// Package image defines the two-tier ImagePipeline seam and codec registry
// (ADR-0004, ADR-0009, ADR-0015, ADR-0018 S8). Concrete backends such as
// internal/image/purego advertise their supported decode and encode codecs at
// runtime and render cover and thumbnail renditions.
package image

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"slices"
	"sort"
	"strings"
	"sync"
)

// Codec identifies a canonical raster image format.
type Codec string

// Canonical image codecs used across container diagnostics and image pipelines.
const (
	CodecJPEG Codec = "jpeg"
	CodecPNG  Codec = "png"
	CodecWebP Codec = "webp"
	CodecJXL  Codec = "jxl"
	CodecAVIF Codec = "avif"
)

// Default rendition and safety limits.
const (
	DefaultCoverMaxWidth     = 400
	DefaultJPEGQuality       = 85
	DefaultMaxPixels         = int64(100_000_000)
	DefaultMaxPreflightBytes = int64(1 << 20)

	// MaxJPEGDimension is the maximum width or height representable in a JPEG SOF marker.
	MaxJPEGDimension = 65535

	initialSniffBytes = 12
	maxSniffBytes     = 512
)

// Sentinel errors shared by image pipeline registries and backends.
var (
	// ErrDuplicatePipeline means a pipeline name is already registered.
	ErrDuplicatePipeline = errors.New("image: pipeline already registered")
	// ErrNoPipeline means no pipeline is registered in the registry.
	ErrNoPipeline = errors.New("image: no pipeline available")
	// ErrInvalidOptions means render options are invalid.
	ErrInvalidOptions = errors.New("image: invalid render options")
	// ErrUnsupportedFormat means the source image format is recognized but not supported.
	ErrUnsupportedFormat = errors.New("image: unsupported image format")
	// ErrUnsupportedEncodeFormat means the requested output codec is not supported.
	ErrUnsupportedEncodeFormat = errors.New("image: unsupported encode format")
	// ErrCorrupt means the image header or pixel stream is damaged or invalid.
	ErrCorrupt = errors.New("image: corrupt image")
	// ErrPixelLimitExceeded means the source dimensions exceed the configured pixel limit.
	ErrPixelLimitExceeded = errors.New("image: pixel limit exceeded")
	// ErrPreflightLimitExceeded means header sniffing/config probing exceeded the preflight byte budget.
	ErrPreflightLimitExceeded = errors.New("image: preflight header limit exceeded")
)

// NormalizeCodec canonicalizes codec names, common aliases, and MIME types.
func NormalizeCodec(name string) Codec {
	n := strings.ToLower(strings.TrimSpace(name))
	switch n {
	case "jpg", "jpe", "jpeg", "image/jpeg", "image/jpg":
		return CodecJPEG
	case "png", "image/png":
		return CodecPNG
	case "webp", "image/webp":
		return CodecWebP
	case "jxl", "image/jxl":
		return CodecJXL
	case "avif", "image/avif":
		return CodecAVIF
	default:
		return Codec(n)
	}
}

// ContentType returns the canonical HTTP MIME type for the codec, or "" if unknown.
func (c Codec) ContentType() string {
	switch NormalizeCodec(string(c)) {
	case CodecJPEG:
		return "image/jpeg"
	case CodecPNG:
		return "image/png"
	case CodecWebP:
		return "image/webp"
	case CodecJXL:
		return "image/jxl"
	case CodecAVIF:
		return "image/avif"
	default:
		return ""
	}
}

// Extension returns the canonical file extension (including leading dot), or "" if unknown.
func (c Codec) Extension() string {
	switch NormalizeCodec(string(c)) {
	case CodecJPEG:
		return ".jpg"
	case CodecPNG:
		return ".png"
	case CodecWebP:
		return ".webp"
	case CodecJXL:
		return ".jxl"
	case CodecAVIF:
		return ".avif"
	default:
		return ""
	}
}

// RenderOptions controls cover/thumbnail rendition sizing and encoding.
type RenderOptions struct {
	// MaxWidth and MaxHeight bound the output dimensions while preserving
	// aspect ratio. At least one must be positive; zero leaves that axis unbounded.
	// Images already within the requested bounds are not upscaled.
	MaxWidth  int
	MaxHeight int

	// OutputFormat selects the target encoded codec. Empty defaults to CodecJPEG.
	OutputFormat Codec

	// Quality controls lossy encoder quality in [1, 100]. Zero selects DefaultJPEGQuality.
	Quality int

	// MaxPixels overrides the pipeline pixel limit (width * height) when positive
	// and stricter than the pipeline's configured limit.
	MaxPixels int64

	// MaxPreflightBytes caps header bytes read and buffered during preflight
	// config probing when positive and stricter than the pipeline's limit.
	MaxPreflightBytes int64

	// SourceCodec is an optional caller hint (for example from an entry extension).
	// Content sniffing takes precedence when magic bytes identify the input format.
	SourceCodec Codec
}

// DefaultCoverOptions returns standard cover rendition options (400px max width, JPEG).
func DefaultCoverOptions() RenderOptions {
	return RenderOptions{
		MaxWidth:     DefaultCoverMaxWidth,
		OutputFormat: CodecJPEG,
		Quality:      DefaultJPEGQuality,
	}
}

// Validate checks bounds and normalizes defaulted fields on o.
func (o RenderOptions) Validate() (RenderOptions, error) {
	if o.MaxWidth < 0 || o.MaxHeight < 0 || (o.MaxWidth == 0 && o.MaxHeight == 0) {
		return RenderOptions{}, fmt.Errorf("%w: max width/height must have at least one positive bound", ErrInvalidOptions)
	}
	if o.Quality < 0 || o.Quality > 100 {
		return RenderOptions{}, fmt.Errorf("%w: quality %d must be in [0, 100]", ErrInvalidOptions, o.Quality)
	}
	if o.MaxPixels < 0 {
		return RenderOptions{}, fmt.Errorf("%w: max pixels cannot be negative", ErrInvalidOptions)
	}
	if o.MaxPreflightBytes < 0 {
		return RenderOptions{}, fmt.Errorf("%w: max preflight bytes cannot be negative", ErrInvalidOptions)
	}

	out := o
	if strings.TrimSpace(string(out.OutputFormat)) == "" {
		out.OutputFormat = CodecJPEG
	} else {
		out.OutputFormat = NormalizeCodec(string(out.OutputFormat))
	}
	if out.Quality == 0 {
		out.Quality = DefaultJPEGQuality
	}
	if out.SourceCodec != "" {
		out.SourceCodec = NormalizeCodec(string(out.SourceCodec))
	}
	return out, nil
}

// Result holds an encoded rendition and metadata about how it was produced.
type Result struct {
	Data        []byte
	Format      Codec
	ContentType string
	Width       int
	Height      int
	SourceCodec Codec
	Placeholder bool
}

// ImagePipeline decodes, resizes, and encodes cover and thumbnail renditions
// and advertises its supported decode and encode codecs at runtime (ADR-0004, ADR-0018 S8).
//
//nolint:revive // name is fixed by ADR-0004 and ADR-0018 S8, which call the seam ImagePipeline
type ImagePipeline interface {
	// Name returns the unique, canonical backend name (for example "purego" or "vips").
	Name() string
	// DecodeFormats returns the source image codecs this pipeline can decode.
	DecodeFormats() []Codec
	// EncodeFormats returns the target image codecs this pipeline can encode.
	EncodeFormats() []Codec
	// RenderCover decodes src, fits it within opts bounds preserving aspect ratio,
	// and encodes the rendition in the requested output format.
	RenderCover(ctx context.Context, src io.Reader, opts RenderOptions) (Result, error)
}

// PrioritizedPipeline is an optional interface implemented by an ImagePipeline
// to declare selection priority in a Registry. Higher values are preferred;
// ties preserve registration order. Pipelines that do not implement it default to 0.
type PrioritizedPipeline interface {
	ImagePipeline
	Priority() int
}

// PlaceholderProvider is an optional interface implemented by an ImagePipeline
// to advertise source codecs for which it can render a fallback placeholder
// when no registered pipeline can decode them (ADR-0004 §1, ADR-0018 S8).
type PlaceholderProvider interface {
	ImagePipeline
	PlaceholderFormats() []Codec
}

// Registry manages registered ImagePipeline implementations, advertises their
// aggregate decode and encode codecs, and routes cover rendering requests.
type Registry struct {
	mu        sync.RWMutex
	pipelines []ImagePipeline
}

// NewRegistry constructs an empty image pipeline registry.
func NewRegistry() *Registry {
	return &Registry{}
}

func pipelinePriority(p ImagePipeline) int {
	if pp, ok := p.(PrioritizedPipeline); ok {
		return pp.Priority()
	}
	return 0
}

func validatePipelineFormats(name string, codecs []Codec, kind string) error {
	for _, c := range codecs {
		if NormalizeCodec(string(c)) == "" {
			return fmt.Errorf("image: pipeline %q has empty %s format", name, kind)
		}
	}
	return nil
}

// Register adds an ImagePipeline to the registry. It rejects nil pipelines,
// empty names, empty codec entries, and duplicate names (case-insensitive).
func (r *Registry) Register(p ImagePipeline) error {
	if p == nil {
		return errors.New("image: cannot register nil pipeline")
	}
	name := strings.ToLower(strings.TrimSpace(p.Name()))
	if name == "" {
		return errors.New("image: pipeline name cannot be empty")
	}
	if err := validatePipelineFormats(p.Name(), p.DecodeFormats(), "decode"); err != nil {
		return err
	}
	if err := validatePipelineFormats(p.Name(), p.EncodeFormats(), "encode"); err != nil {
		return err
	}
	if pp, ok := p.(PlaceholderProvider); ok {
		if err := validatePipelineFormats(p.Name(), pp.PlaceholderFormats(), "placeholder"); err != nil {
			return err
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, existing := range r.pipelines {
		if strings.ToLower(strings.TrimSpace(existing.Name())) == name {
			return fmt.Errorf("%w: %q", ErrDuplicatePipeline, p.Name())
		}
	}

	r.pipelines = append(r.pipelines, p)
	sort.SliceStable(r.pipelines, func(i, j int) bool {
		return pipelinePriority(r.pipelines[i]) > pipelinePriority(r.pipelines[j])
	})
	return nil
}

// Pipelines returns all registered pipelines in priority order.
func (r *Registry) Pipelines() []ImagePipeline {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ImagePipeline, len(r.pipelines))
	copy(out, r.pipelines)
	return out
}

// Pipeline returns the registered pipeline with the given name (case-insensitive),
// or nil if not found.
func (r *Registry) Pipeline(name string) ImagePipeline {
	target := strings.ToLower(strings.TrimSpace(name))
	if target == "" {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.pipelines {
		if strings.ToLower(strings.TrimSpace(p.Name())) == target {
			return p
		}
	}
	return nil
}

// DecodeFormats returns the deduplicated list of decode codecs advertised by
// all registered pipelines in deterministic priority/registration order.
func (r *Registry) DecodeFormats() []Codec {
	return r.collectFormats(func(p ImagePipeline) []Codec { return p.DecodeFormats() })
}

// EncodeFormats returns the deduplicated list of encode codecs advertised by
// all registered pipelines in deterministic priority/registration order.
func (r *Registry) EncodeFormats() []Codec {
	return r.collectFormats(func(p ImagePipeline) []Codec { return p.EncodeFormats() })
}

// PlaceholderFormats returns the deduplicated list of source codecs for which
// registered pipelines advertise placeholder fallback support.
func (r *Registry) PlaceholderFormats() []Codec {
	return r.collectFormats(func(p ImagePipeline) []Codec {
		if pp, ok := p.(PlaceholderProvider); ok {
			return pp.PlaceholderFormats()
		}
		return nil
	})
}

func (r *Registry) collectFormats(get func(ImagePipeline) []Codec) []Codec {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[Codec]bool)
	var out []Codec
	for _, p := range r.pipelines {
		for _, raw := range get(p) {
			c := NormalizeCodec(string(raw))
			if c == "" || seen[c] {
				continue
			}
			seen[c] = true
			out = append(out, c)
		}
	}
	return out
}

// SupportsDecode reports whether at least one registered pipeline advertises
// decoding support for codec.
func (r *Registry) SupportsDecode(codec Codec) bool {
	target := NormalizeCodec(string(codec))
	if target == "" {
		return false
	}
	return slices.Contains(r.DecodeFormats(), target)
}

// SupportsEncode reports whether at least one registered pipeline advertises
// encoding support for codec.
func (r *Registry) SupportsEncode(codec Codec) bool {
	target := NormalizeCodec(string(codec))
	if target == "" {
		return false
	}
	return slices.Contains(r.EncodeFormats(), target)
}

// SupportsPlaceholder reports whether at least one registered pipeline
// advertises placeholder fallback support for codec.
func (r *Registry) SupportsPlaceholder(codec Codec) bool {
	target := NormalizeCodec(string(codec))
	if target == "" {
		return false
	}
	return slices.Contains(r.PlaceholderFormats(), target)
}

func pipelineSupports(codecs []Codec, target Codec) bool {
	for _, c := range codecs {
		if NormalizeCodec(string(c)) == target {
			return true
		}
	}
	return false
}

// Select chooses the highest-priority registered pipeline that can encode
// dstCodec and either decode srcCodec or provide a placeholder for srcCodec.
func (r *Registry) Select(srcCodec, dstCodec Codec) (ImagePipeline, error) {
	encodeCapable, err := r.encodeCapable(dstCodec)
	if err != nil {
		return nil, err
	}
	return selectFromCandidates(encodeCapable, srcCodec)
}

func selectFromCandidates(encodeCapable []ImagePipeline, srcCodec Codec) (ImagePipeline, error) {
	src := NormalizeCodec(string(srcCodec))
	if src == "" {
		return encodeCapable[0], nil
	}

	for _, p := range encodeCapable {
		if pipelineSupports(p.DecodeFormats(), src) {
			return p, nil
		}
	}

	for _, p := range encodeCapable {
		if pp, ok := p.(PlaceholderProvider); ok && pipelineSupports(pp.PlaceholderFormats(), src) {
			return p, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrUnsupportedFormat, src)
}

// EffectivePreflightBytes returns DefaultMaxPreflightBytes or a tighter
// positive per-call override.
func EffectivePreflightBytes(override int64) int64 {
	if override > 0 && override < DefaultMaxPreflightBytes {
		return override
	}
	return DefaultMaxPreflightBytes
}

// RenderCover validates options, sniffs the source codec from leading bytes
// within the shared preflight budget (or tighter per-call override), selects an
// appropriate registered pipeline, and renders the cover rendition.
func (r *Registry) RenderCover(ctx context.Context, src io.Reader, opts RenderOptions) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	norm, err := opts.Validate()
	if err != nil {
		return Result{}, err
	}
	if src == nil {
		return Result{}, fmt.Errorf("%w: nil image reader", ErrCorrupt)
	}

	encodeCapable, err := r.encodeCapable(norm.OutputFormat)
	if err != nil {
		return Result{}, err
	}

	budget := EffectivePreflightBytes(norm.MaxPreflightBytes)
	codec, replay, err := SniffReaderBounded(src, budget)
	if err != nil {
		return Result{}, err
	}
	if codec == "" {
		return Result{}, fmt.Errorf("%w: unrecognized image header", ErrCorrupt)
	}

	p, err := selectFromCandidates(encodeCapable, codec)
	if err != nil {
		return Result{}, err
	}
	norm.SourceCodec = codec
	return p.RenderCover(ctx, replay, norm)
}

func (r *Registry) encodeCapable(dstCodec Codec) ([]ImagePipeline, error) {
	pipelines := r.Pipelines()
	if len(pipelines) == 0 {
		return nil, ErrNoPipeline
	}

	dst := NormalizeCodec(string(dstCodec))
	if dst == "" {
		dst = CodecJPEG
	}

	var out []ImagePipeline
	for _, p := range pipelines {
		if pipelineSupports(p.EncodeFormats(), dst) {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedEncodeFormat, dst)
	}
	return out, nil
}

// SniffHeader identifies an image codec from its leading magic bytes, or
// returns "" when the header does not match a known image signature.
func SniffHeader(header []byte) Codec {
	switch {
	case len(header) >= 3 && header[0] == 0xFF && header[1] == 0xD8 && header[2] == 0xFF:
		return CodecJPEG
	case bytes.HasPrefix(header, []byte("\x89PNG\r\n\x1a\n")):
		return CodecPNG
	case len(header) >= 12 && bytes.Equal(header[0:4], []byte("RIFF")) && bytes.Equal(header[8:12], []byte("WEBP")):
		return CodecWebP
	case len(header) >= 2 && header[0] == 0xFF && header[1] == 0x0A:
		return CodecJXL
	case bytes.HasPrefix(header, []byte("\x00\x00\x00\x0cJXL \r\n\x87\n")):
		return CodecJXL
	case isAVIFHeader(header):
		return CodecAVIF
	case isISOBMFFFtyp(header):
		return Codec("heif")
	case bytes.HasPrefix(header, []byte("GIF87a")) || bytes.HasPrefix(header, []byte("GIF89a")):
		return Codec("gif")
	case len(header) >= 2 && header[0] == 'B' && header[1] == 'M':
		return Codec("bmp")
	case bytes.HasPrefix(header, []byte("II*\x00")) || bytes.HasPrefix(header, []byte("MM\x00*")):
		return Codec("tiff")
	default:
		return ""
	}
}

func isISOBMFFFtyp(header []byte) bool {
	return len(header) >= 12 && bytes.Equal(header[4:8], []byte("ftyp"))
}

func isAVIFBrand(b []byte) bool {
	return len(b) == 4 && (bytes.Equal(b, []byte("avif")) || bytes.Equal(b, []byte("avis")))
}

func isAVIFHeader(header []byte) bool {
	if !isISOBMFFFtyp(header) {
		return false
	}
	if isAVIFBrand(header[8:12]) {
		return true
	}
	boxSize := int(binary.BigEndian.Uint32(header[0:4]))
	end := len(header)
	if boxSize >= 16 && boxSize < end {
		end = boxSize
	}
	for off := 16; off+4 <= end; off += 4 {
		if isAVIFBrand(header[off : off+4]) {
			return true
		}
	}
	return false
}

func readUpTo(r io.Reader, limit int) ([]byte, bool, error) {
	buf := make([]byte, 0, limit)
	tmp := make([]byte, limit)
	for len(buf) < limit {
		n, err := r.Read(tmp[:limit-len(buf)])
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if errors.Is(err, io.EOF) {
			return buf, true, nil
		}
		if err != nil {
			return buf, false, err
		}
		if n == 0 {
			break
		}
	}
	return buf, false, nil
}

// SniffReader reads the minimum leading bytes needed to detect the image
// codec and returns a replay io.Reader covering the full stream from byte 0.
func SniffReader(r io.Reader) (Codec, io.Reader, error) {
	return SniffReaderBounded(r, 0)
}

type replayReader struct {
	codec Codec
	buf   []byte
	off   int
	eof   bool
	rest  io.Reader
}

func (rr *replayReader) Read(p []byte) (int, error) {
	if rr.off < len(rr.buf) {
		n := copy(p, rr.buf[rr.off:])
		rr.off += n
		return n, nil
	}
	if rr.eof || rr.rest == nil {
		return 0, io.EOF
	}
	return rr.rest.Read(p)
}

// SniffReaderBounded behaves like SniffReader while enforcing maxBytes (when
// positive) as an upper bound on header bytes read during sniffing.
func SniffReaderBounded(r io.Reader, maxBytes int64) (Codec, io.Reader, error) {
	if r == nil {
		return "", nil, fmt.Errorf("%w: nil image reader", ErrCorrupt)
	}
	if maxBytes < 0 {
		return "", nil, fmt.Errorf("%w: max preflight bytes cannot be negative", ErrInvalidOptions)
	}
	if rr, ok := r.(*replayReader); ok && rr.off == 0 {
		if maxBytes > 0 && int64(len(rr.buf)) > maxBytes {
			return "", nil, fmt.Errorf("%w: header exceeds %d bytes", ErrPreflightLimitExceeded, maxBytes)
		}
		return rr.codec, rr, nil
	}

	initLimit := initialSniffBytes
	if maxBytes > 0 && maxBytes < int64(initLimit) {
		initLimit = int(maxBytes)
	}
	buf, eof, err := readUpTo(r, initLimit)
	if err != nil {
		return "", nil, fmt.Errorf("%w: read header: %v", ErrCorrupt, err)
	}
	if len(buf) == 0 {
		return "", nil, fmt.Errorf("%w: empty image", ErrCorrupt)
	}

	// For ISOBMFF containers whose major brand is generic (e.g. mif1), inspect
	// the rest of the ftyp box up to maxSniffBytes (and maxBytes) for compatible brands.
	if !eof && isISOBMFFFtyp(buf) && !isAVIFBrand(buf[8:12]) && !bytes.HasPrefix(buf, []byte("\x00\x00\x00\x0cJXL \r\n\x87\n")) {
		target := maxSniffBytes
		if boxSize := int(binary.BigEndian.Uint32(buf[0:4])); boxSize > len(buf) && boxSize < target {
			target = boxSize
		}
		budgetExceeded := false
		if maxBytes > 0 && int64(target) > maxBytes {
			target = int(maxBytes)
			budgetExceeded = true
		}
		if target > len(buf) {
			extra, extraEOF, extraErr := readUpTo(r, target-len(buf))
			if extraErr != nil {
				return "", nil, fmt.Errorf("%w: read container header: %v", ErrCorrupt, extraErr)
			}
			buf = append(buf, extra...)
			eof = extraEOF
		}
		if budgetExceeded && !eof && SniffHeader(buf) == Codec("heif") {
			return "", nil, fmt.Errorf("%w: container header exceeds %d bytes", ErrPreflightLimitExceeded, maxBytes)
		}
	}

	codec := SniffHeader(buf)
	if maxBytes > 0 && int64(len(buf)) >= maxBytes && !eof && codec == "" {
		return "", nil, fmt.Errorf("%w: header exceeds %d bytes", ErrPreflightLimitExceeded, maxBytes)
	}
	return codec, &replayReader{
		codec: codec,
		buf:   buf,
		eof:   eof,
		rest:  r,
	}, nil
}

func cmpMul64(a, b, c, d uint64) int {
	hi1, lo1 := bits.Mul64(a, b)
	hi2, lo2 := bits.Mul64(c, d)
	if hi1 != hi2 {
		if hi1 < hi2 {
			return -1
		}
		return 1
	}
	if lo1 != lo2 {
		if lo1 < lo2 {
			return -1
		}
		return 1
	}
	return 0
}

// mulDivRoundHalfUp computes round_half_up(a * b / den) using exact 128-bit
// arithmetic. Callers must ensure b <= den and a <= math.MaxInt so the result
// fits in int without overflow.
func mulDivRoundHalfUp(a, b, den uint64) int {
	hi, lo := bits.Mul64(a, b)
	lo, carry := bits.Add64(lo, den/2, 0)
	hi, _ = bits.Add64(hi, 0, carry)
	q, _ := bits.Div64(hi, lo, den)
	return int(q)
}

// FitDimensions computes aspect-preserving target dimensions bounded by maxW
// and maxH without upscaling. Zero on either axis leaves that axis unbounded.
func FitDimensions(srcW, srcH, maxW, maxH int) (int, int) {
	if srcW <= 0 || srcH <= 0 {
		return 0, 0
	}
	if maxW <= 0 && maxH <= 0 {
		return srcW, srcH
	}
	if (maxW <= 0 || srcW <= maxW) && (maxH <= 0 || srcH <= maxH) {
		return srcW, srcH
	}

	effW := srcW
	if maxW > 0 && maxW < effW {
		effW = maxW
	}
	effH := srcH
	if maxH > 0 && maxH < effH {
		effH = maxH
	}

	// Compare effW/srcW vs effH/srcH via 128-bit unsigned products to avoid
	// signed integer overflow on large bounds.
	if cmpMul64(uint64(effW), uint64(srcH), uint64(effH), uint64(srcW)) <= 0 {
		dstH := mulDivRoundHalfUp(uint64(srcH), uint64(effW), uint64(srcW))
		return effW, max(1, min(effH, dstH))
	}
	dstW := mulDivRoundHalfUp(uint64(srcW), uint64(effH), uint64(srcH))
	return max(1, min(effW, dstW)), effH
}

// FitCodecDimensions computes aspect-preserving target dimensions bounded by
// both the requested limits and any format-specific encoder axis limits
// (such as JPEG's 65,535-pixel per-axis maximum).
func FitCodecDimensions(codec Codec, srcW, srcH, maxW, maxH int) (int, int) {
	if NormalizeCodec(string(codec)) == CodecJPEG {
		if maxW <= 0 || maxW > MaxJPEGDimension {
			maxW = MaxJPEGDimension
		}
		if maxH <= 0 || maxH > MaxJPEGDimension {
			maxH = MaxJPEGDimension
		}
	}
	return FitDimensions(srcW, srcH, maxW, maxH)
}

// ExceedsPixelLimit reports whether width * height exceeds maxPixels without
// risking signed integer overflow.
func ExceedsPixelLimit(width, height int, maxPixels int64) bool {
	if width <= 0 || height <= 0 || maxPixels <= 0 {
		return false
	}
	w, h := int64(width), int64(height)
	return w > maxPixels/h
}
