// Package purego implements the Tier 1 pure-Go ImagePipeline using the Go
// standard library and golang.org/x/image (ADR-0004 §1). It decodes JPEG, PNG,
// and WebP, resizes covers deterministically while preserving aspect ratio,
// encodes thumbnails as JPEG, and returns a deterministic placeholder for JXL
// and AVIF sources.
package purego

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"

	"golang.org/x/image/draw"
	"golang.org/x/image/webp"

	tobitenimage "github.com/inexcession/tobiten/internal/image"
)

// PipelineName is the canonical registered name of the pure-Go Tier 1 pipeline.
const PipelineName = "purego"

const (
	placeholderBaseWidth  = 400
	placeholderBaseHeight = 600
)

// Options configures the pure-Go Tier 1 pipeline.
type Options struct {
	// MaxPixels caps source image pixel count (width * height) before full
	// decode. Zero selects image.DefaultMaxPixels. Negative values are rejected.
	MaxPixels int64
}

// Pipeline is the Tier 1 pure-Go implementation of image.ImagePipeline.
type Pipeline struct {
	maxPixels int64
}

var (
	_ tobitenimage.ImagePipeline       = (*Pipeline)(nil)
	_ tobitenimage.PlaceholderProvider = (*Pipeline)(nil)
)

// New constructs a Tier 1 pure-Go ImagePipeline with the given options.
func New(opts Options) (*Pipeline, error) {
	if opts.MaxPixels < 0 {
		return nil, fmt.Errorf("%w: max pixels cannot be negative", tobitenimage.ErrInvalidOptions)
	}
	maxPixels := opts.MaxPixels
	if maxPixels == 0 {
		maxPixels = tobitenimage.DefaultMaxPixels
	}
	return &Pipeline{
		maxPixels: maxPixels,
	}, nil
}

// Name implements image.ImagePipeline.
func (p *Pipeline) Name() string {
	return PipelineName
}

// DecodeFormats implements image.ImagePipeline.
func (p *Pipeline) DecodeFormats() []tobitenimage.Codec {
	return []tobitenimage.Codec{
		tobitenimage.CodecJPEG,
		tobitenimage.CodecPNG,
		tobitenimage.CodecWebP,
	}
}

// EncodeFormats implements image.ImagePipeline.
func (p *Pipeline) EncodeFormats() []tobitenimage.Codec {
	return []tobitenimage.Codec{
		tobitenimage.CodecJPEG,
	}
}

// PlaceholderFormats implements image.PlaceholderProvider.
func (p *Pipeline) PlaceholderFormats() []tobitenimage.Codec {
	return []tobitenimage.Codec{
		tobitenimage.CodecJXL,
		tobitenimage.CodecAVIF,
	}
}

// MaxPixels returns the configured maximum source pixel count.
func (p *Pipeline) MaxPixels() int64 {
	return p.maxPixels
}

func (p *Pipeline) effectiveMaxPixels(override int64) int64 {
	if override > 0 && override < p.maxPixels {
		return override
	}
	return p.maxPixels
}

// RenderCover implements image.ImagePipeline.
func (p *Pipeline) RenderCover(ctx context.Context, src io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
	if err := ctx.Err(); err != nil {
		return tobitenimage.Result{}, err
	}
	norm, err := opts.Validate()
	if err != nil {
		return tobitenimage.Result{}, err
	}
	if norm.OutputFormat != tobitenimage.CodecJPEG {
		return tobitenimage.Result{}, fmt.Errorf("%w: %s", tobitenimage.ErrUnsupportedEncodeFormat, norm.OutputFormat)
	}
	if src == nil {
		return tobitenimage.Result{}, fmt.Errorf("%w: nil image reader", tobitenimage.ErrCorrupt)
	}

	preflightLimit := tobitenimage.EffectivePreflightBytes(norm.MaxPreflightBytes)
	codec, replay, err := tobitenimage.SniffReaderBounded(src, preflightLimit)
	if err != nil {
		return tobitenimage.Result{}, err
	}

	switch codec {
	case tobitenimage.CodecJXL, tobitenimage.CodecAVIF:
		return renderPlaceholder(codec, norm)
	case tobitenimage.CodecJPEG, tobitenimage.CodecPNG, tobitenimage.CodecWebP:
		return p.renderSupported(ctx, codec, replay, norm, preflightLimit)
	case "":
		return tobitenimage.Result{}, fmt.Errorf("%w: unrecognized image header", tobitenimage.ErrCorrupt)
	default:
		return tobitenimage.Result{}, fmt.Errorf("%w: %s", tobitenimage.ErrUnsupportedFormat, codec)
	}
}

func (p *Pipeline) renderSupported(
	ctx context.Context,
	codec tobitenimage.Codec,
	r io.Reader,
	opts tobitenimage.RenderOptions,
	preflightLimit int64,
) (tobitenimage.Result, error) {
	maxPixels := p.effectiveMaxPixels(opts.MaxPixels)
	img, err := decodeBounded(ctx, codec, r, maxPixels, preflightLimit)
	if err != nil {
		return tobitenimage.Result{}, err
	}

	if err := ctx.Err(); err != nil {
		return tobitenimage.Result{}, err
	}

	bounds := img.Bounds()
	dstW, dstH := tobitenimage.FitCodecDimensions(opts.OutputFormat, bounds.Dx(), bounds.Dy(), opts.MaxWidth, opts.MaxHeight)
	dst := resizeOrFlatten(img, dstW, dstH)

	if err := ctx.Err(); err != nil {
		return tobitenimage.Result{}, err
	}

	data, err := encodeJPEG(dst, opts.Quality)
	if err != nil {
		return tobitenimage.Result{}, err
	}

	return tobitenimage.Result{
		Data:        data,
		Format:      tobitenimage.CodecJPEG,
		ContentType: tobitenimage.CodecJPEG.ContentType(),
		Width:       dstW,
		Height:      dstH,
		SourceCodec: codec,
		Placeholder: false,
	}, nil
}

type preflightLimitedReader struct {
	r         io.Reader
	buf       bytes.Buffer
	remaining int64
	exceeded  bool
}

func (pr *preflightLimitedReader) Read(dst []byte) (int, error) {
	if pr.remaining <= 0 {
		pr.exceeded = true
		return 0, tobitenimage.ErrPreflightLimitExceeded
	}
	if int64(len(dst)) > pr.remaining {
		dst = dst[:pr.remaining]
	}
	n, err := pr.r.Read(dst)
	if n > 0 {
		pr.buf.Write(dst[:n])
		pr.remaining -= int64(n)
	}
	return n, err
}

func decodeBounded(
	ctx context.Context,
	codec tobitenimage.Codec,
	r io.Reader,
	maxPixels int64,
	maxPreflightBytes int64,
) (image.Image, error) {
	pr := &preflightLimitedReader{
		r:         r,
		remaining: maxPreflightBytes,
	}

	cfg, err := decodeConfig(codec, pr)
	if pr.exceeded || errors.Is(err, tobitenimage.ErrPreflightLimitExceeded) {
		return nil, fmt.Errorf("%w: exceeded %d bytes", tobitenimage.ErrPreflightLimitExceeded, maxPreflightBytes)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", tobitenimage.ErrCorrupt, err)
	}
	if cfg.Width <= 0 || cfg.Height <= 0 {
		return nil, fmt.Errorf("%w: invalid dimensions %dx%d", tobitenimage.ErrCorrupt, cfg.Width, cfg.Height)
	}
	if tobitenimage.ExceedsPixelLimit(cfg.Width, cfg.Height, maxPixels) {
		return nil, fmt.Errorf("%w: %dx%d exceeds limit %d",
			tobitenimage.ErrPixelLimitExceeded, cfg.Width, cfg.Height, maxPixels)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	full := io.MultiReader(bytes.NewReader(pr.buf.Bytes()), r)
	img, err := decodeImage(codec, full)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", tobitenimage.ErrCorrupt, err)
	}

	b := img.Bounds()
	if b.Dx() <= 0 || b.Dy() <= 0 {
		return nil, fmt.Errorf("%w: empty decoded image bounds", tobitenimage.ErrCorrupt)
	}
	if tobitenimage.ExceedsPixelLimit(b.Dx(), b.Dy(), maxPixels) {
		return nil, fmt.Errorf("%w: %dx%d exceeds limit %d",
			tobitenimage.ErrPixelLimitExceeded, b.Dx(), b.Dy(), maxPixels)
	}
	return img, nil
}

func decodeConfig(codec tobitenimage.Codec, r io.Reader) (image.Config, error) {
	switch codec {
	case tobitenimage.CodecJPEG:
		return jpeg.DecodeConfig(r)
	case tobitenimage.CodecPNG:
		return png.DecodeConfig(r)
	case tobitenimage.CodecWebP:
		return webp.DecodeConfig(r)
	default:
		return image.Config{}, fmt.Errorf("%w: %s", tobitenimage.ErrUnsupportedFormat, codec)
	}
}

func decodeImage(codec tobitenimage.Codec, r io.Reader) (image.Image, error) {
	switch codec {
	case tobitenimage.CodecJPEG:
		return jpeg.Decode(r)
	case tobitenimage.CodecPNG:
		return png.Decode(r)
	case tobitenimage.CodecWebP:
		return webp.Decode(r)
	default:
		return nil, fmt.Errorf("%w: %s", tobitenimage.ErrUnsupportedFormat, codec)
	}
}

func isOpaque(img image.Image) bool {
	type opaquer interface {
		Opaque() bool
	}
	if o, ok := img.(opaquer); ok {
		return o.Opaque()
	}
	return false
}

func resizeOrFlatten(src image.Image, dstW, dstH int) image.Image {
	srcBounds := src.Bounds()
	if srcBounds.Dx() == dstW && srcBounds.Dy() == dstH && isOpaque(src) {
		return src
	}

	dst := image.NewRGBA(image.Rect(0, 0, dstW, dstH))
	draw.Draw(dst, dst.Bounds(), image.White, image.Point{}, draw.Src)
	if srcBounds.Dx() == dstW && srcBounds.Dy() == dstH {
		draw.Draw(dst, dst.Bounds(), src, srcBounds.Min, draw.Over)
		return dst
	}
	draw.CatmullRom.Scale(dst, dst.Bounds(), src, srcBounds, draw.Over, nil)
	return dst
}

func encodeJPEG(img image.Image, quality int) ([]byte, error) {
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		return nil, fmt.Errorf("image/purego: encode jpeg: %w", err)
	}
	return buf.Bytes(), nil
}

func renderPlaceholder(codec tobitenimage.Codec, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
	w, h := tobitenimage.FitCodecDimensions(opts.OutputFormat, placeholderBaseWidth, placeholderBaseHeight, opts.MaxWidth, opts.MaxHeight)
	canvas := drawPlaceholderCard(w, h)
	data, err := encodeJPEG(canvas, opts.Quality)
	if err != nil {
		return tobitenimage.Result{}, err
	}
	return tobitenimage.Result{
		Data:        data,
		Format:      tobitenimage.CodecJPEG,
		ContentType: tobitenimage.CodecJPEG.ContentType(),
		Width:       w,
		Height:      h,
		SourceCodec: codec,
		Placeholder: true,
	}, nil
}

func drawPlaceholderCard(w, h int) image.Image {
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	bg := color.RGBA{R: 236, G: 240, B: 245, A: 255}
	border := color.RGBA{R: 180, G: 190, B: 204, A: 255}
	card := color.RGBA{R: 222, G: 228, B: 236, A: 255}

	draw.Draw(dst, dst.Bounds(), image.NewUniform(bg), image.Point{}, draw.Src)

	margin := max(1, min(w, h)/16)
	inner := image.Rect(margin, margin, max(margin+1, w-margin), max(margin+1, h-margin))
	draw.Draw(dst, inner, image.NewUniform(border), image.Point{}, draw.Src)

	inset := max(1, margin/4)
	core := image.Rect(
		min(inner.Max.X-1, inner.Min.X+inset),
		min(inner.Max.Y-1, inner.Min.Y+inset),
		max(inner.Min.X+1, inner.Max.X-inset),
		max(inner.Min.Y+1, inner.Max.Y-inset),
	)
	if core.Dx() > 0 && core.Dy() > 0 {
		draw.Draw(dst, core, image.NewUniform(card), image.Point{}, draw.Src)
	}
	return dst
}
