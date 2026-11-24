package image_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"

	tobitenimage "github.com/inexcession/tobiten/internal/image"
)

type stubPipeline struct {
	name         string
	priority     int
	decodes      []tobitenimage.Codec
	encodes      []tobitenimage.Codec
	placeholders []tobitenimage.Codec
	maxPreflight int64
	renderFn     func(ctx context.Context, src io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error)
}

func (s *stubPipeline) Name() string                             { return s.name }
func (s *stubPipeline) Priority() int                            { return s.priority }
func (s *stubPipeline) DecodeFormats() []tobitenimage.Codec      { return s.decodes }
func (s *stubPipeline) EncodeFormats() []tobitenimage.Codec      { return s.encodes }
func (s *stubPipeline) PlaceholderFormats() []tobitenimage.Codec { return s.placeholders }
func (s *stubPipeline) MaxPreflightBytes() int64                 { return s.maxPreflight }
func (s *stubPipeline) RenderCover(ctx context.Context, src io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
	if s.renderFn != nil {
		return s.renderFn(ctx, src, opts)
	}
	return tobitenimage.Result{
		Data:        []byte("ok:" + s.name),
		Format:      opts.OutputFormat,
		ContentType: opts.OutputFormat.ContentType(),
		SourceCodec: opts.SourceCodec,
	}, nil
}

type plainPipeline struct {
	name    string
	decodes []tobitenimage.Codec
	encodes []tobitenimage.Codec
}

func (p plainPipeline) Name() string                        { return p.name }
func (p plainPipeline) DecodeFormats() []tobitenimage.Codec { return p.decodes }
func (p plainPipeline) EncodeFormats() []tobitenimage.Codec { return p.encodes }
func (p plainPipeline) RenderCover(context.Context, io.Reader, tobitenimage.RenderOptions) (tobitenimage.Result, error) {
	return tobitenimage.Result{}, nil
}

func TestRegistry_RegisterValidationAndAdvertisement(t *testing.T) {
	reg := tobitenimage.NewRegistry()

	if err := reg.Register(nil); err == nil {
		t.Error("expected nil pipeline to be rejected")
	}
	if err := reg.Register(&stubPipeline{name: "  "}); err == nil {
		t.Error("expected empty pipeline name to be rejected")
	}
	if err := reg.Register(&stubPipeline{name: "bad-decode", decodes: []tobitenimage.Codec{""}}); err == nil {
		t.Error("expected empty decode codec to be rejected")
	}
	if err := reg.Register(&stubPipeline{name: "bad-encode", encodes: []tobitenimage.Codec{" "}}); err == nil {
		t.Error("expected empty encode codec to be rejected")
	}
	if err := reg.Register(&stubPipeline{name: "bad-placeholder", placeholders: []tobitenimage.Codec{""}}); err == nil {
		t.Error("expected empty placeholder codec to be rejected")
	}

	p1 := &stubPipeline{
		name:    "Tier1",
		decodes: []tobitenimage.Codec{tobitenimage.CodecJPEG, tobitenimage.CodecPNG, tobitenimage.CodecWebP},
		encodes: []tobitenimage.Codec{tobitenimage.CodecJPEG},
	}
	if err := reg.Register(p1); err != nil {
		t.Fatalf("Register(Tier1): %v", err)
	}

	err := reg.Register(&stubPipeline{name: " tier1 "})
	if !errors.Is(err, tobitenimage.ErrDuplicatePipeline) {
		t.Errorf("duplicate Register error = %v, want ErrDuplicatePipeline", err)
	}

	if got := reg.Pipeline("TIER1"); got != p1 {
		t.Errorf("Pipeline(TIER1) = %v, want %v", got, p1)
	}
	if got := reg.Pipeline("missing"); got != nil {
		t.Errorf("Pipeline(missing) = %v, want nil", got)
	}
	if got := reg.Pipeline(" "); got != nil {
		t.Errorf("Pipeline(blank) = %v, want nil", got)
	}

	gotDec := reg.DecodeFormats()
	wantDec := []tobitenimage.Codec{tobitenimage.CodecJPEG, tobitenimage.CodecPNG, tobitenimage.CodecWebP}
	if len(gotDec) != len(wantDec) {
		t.Fatalf("DecodeFormats() = %v, want %v", gotDec, wantDec)
	}
	for i := range wantDec {
		if gotDec[i] != wantDec[i] {
			t.Fatalf("DecodeFormats() = %v, want %v", gotDec, wantDec)
		}
	}

	gotEnc := reg.EncodeFormats()
	wantEnc := []tobitenimage.Codec{tobitenimage.CodecJPEG}
	if len(gotEnc) != 1 || gotEnc[0] != wantEnc[0] {
		t.Fatalf("EncodeFormats() = %v, want %v", gotEnc, wantEnc)
	}

	if !reg.SupportsDecode("jpg") || !reg.SupportsDecode("PNG") || !reg.SupportsDecode("image/webp") {
		t.Error("expected SupportsDecode to match normalized JPEG/PNG/WebP")
	}
	if reg.SupportsDecode(tobitenimage.CodecJXL) || reg.SupportsDecode(tobitenimage.CodecAVIF) {
		t.Error("expected Tier 1 registry not to advertise JXL/AVIF decode")
	}
	if !reg.SupportsEncode("jpeg") || reg.SupportsEncode(tobitenimage.CodecWebP) {
		t.Error("expected SupportsEncode to advertise JPEG only")
	}
}

func TestRegistry_PriorityAndSelection(t *testing.T) {
	reg := tobitenimage.NewRegistry()
	if _, err := reg.Select(tobitenimage.CodecJPEG, tobitenimage.CodecJPEG); !errors.Is(err, tobitenimage.ErrNoPipeline) {
		t.Fatalf("empty Select err = %v, want ErrNoPipeline", err)
	}

	tier1 := &stubPipeline{
		name:         "purego",
		priority:     0,
		decodes:      []tobitenimage.Codec{tobitenimage.CodecJPEG, tobitenimage.CodecPNG, tobitenimage.CodecWebP},
		encodes:      []tobitenimage.Codec{tobitenimage.CodecJPEG},
		placeholders: []tobitenimage.Codec{tobitenimage.CodecJXL, tobitenimage.CodecAVIF},
	}
	jpegOnly := &stubPipeline{
		name:     "jpeg-only",
		priority: 10,
		decodes:  []tobitenimage.Codec{tobitenimage.CodecJPEG},
		encodes:  []tobitenimage.Codec{tobitenimage.CodecJPEG},
	}
	tie := plainPipeline{
		name:    "purego-alt",
		decodes: []tobitenimage.Codec{tobitenimage.CodecJPEG},
		encodes: []tobitenimage.Codec{tobitenimage.CodecJPEG},
	}

	for _, p := range []tobitenimage.ImagePipeline{tier1, jpegOnly, tie} {
		if err := reg.Register(p); err != nil {
			t.Fatal(err)
		}
	}

	list := reg.Pipelines()
	if len(list) != 3 || list[0].Name() != "jpeg-only" || list[1].Name() != "purego" || list[2].Name() != "purego-alt" {
		t.Fatalf("Pipelines() order = [%s, %s, %s], want [jpeg-only, purego, purego-alt]",
			list[0].Name(), list[1].Name(), list[2].Name())
	}

	// Higher-priority JPEG-only provider cannot decode or placeholder JXL/AVIF,
	// so placeholder routing must fall back to the capable purego provider.
	for _, codec := range []tobitenimage.Codec{tobitenimage.CodecJXL, tobitenimage.CodecAVIF} {
		sel, err := reg.Select(codec, tobitenimage.CodecJPEG)
		if err != nil || sel.Name() != "purego" {
			t.Fatalf("Select(%s, jpeg) without decoder = (%v, %v), want purego", codec, sel, err)
		}
	}
	if sel, err := reg.Select(tobitenimage.CodecJPEG, tobitenimage.CodecJPEG); err != nil || sel.Name() != "jpeg-only" {
		t.Fatalf("Select(jpeg, jpeg) = (%v, %v), want jpeg-only", sel, err)
	}
	if !reg.SupportsPlaceholder(tobitenimage.CodecJXL) || !reg.SupportsPlaceholder(tobitenimage.CodecAVIF) || reg.SupportsPlaceholder(tobitenimage.CodecPNG) {
		t.Fatalf("unexpected PlaceholderFormats(): %v", reg.PlaceholderFormats())
	}

	// When a higher-priority compatible decoder is registered, it wins over placeholder fallback.
	tier2 := &stubPipeline{
		name:     "vips",
		priority: 20,
		decodes:  []tobitenimage.Codec{tobitenimage.CodecJPEG, tobitenimage.CodecPNG, tobitenimage.CodecWebP, tobitenimage.CodecJXL, tobitenimage.CodecAVIF},
		encodes:  []tobitenimage.Codec{tobitenimage.CodecJPEG},
	}
	if err := reg.Register(tier2); err != nil {
		t.Fatal(err)
	}
	for _, codec := range []tobitenimage.Codec{tobitenimage.CodecJXL, tobitenimage.CodecAVIF} {
		sel, err := reg.Select(codec, tobitenimage.CodecJPEG)
		if err != nil || sel.Name() != "vips" {
			t.Fatalf("Select(%s, jpeg) with decoder = (%v, %v), want vips", codec, sel, err)
		}
	}

	if _, err := reg.Select(tobitenimage.CodecJPEG, tobitenimage.CodecWebP); !errors.Is(err, tobitenimage.ErrUnsupportedEncodeFormat) {
		t.Fatalf("Select(jpeg, webp) err = %v, want ErrUnsupportedEncodeFormat", err)
	}
	if _, err := reg.Select(tobitenimage.Codec("gif"), tobitenimage.CodecJPEG); !errors.Is(err, tobitenimage.ErrUnsupportedFormat) {
		t.Fatalf("Select(gif, jpeg) err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestRegistry_RenderCoverRouting(t *testing.T) {
	reg := tobitenimage.NewRegistry()
	var seenBytes []byte
	var seenCodec tobitenimage.Codec
	jpegOnly := &stubPipeline{
		name:     "jpeg-only",
		priority: 10,
		decodes:  []tobitenimage.Codec{tobitenimage.CodecJPEG},
		encodes:  []tobitenimage.Codec{tobitenimage.CodecJPEG},
		renderFn: func(_ context.Context, _ io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
			return tobitenimage.Result{}, errors.New("jpeg-only provider should not handle JXL/AVIF placeholders")
		},
	}
	p := &stubPipeline{
		name:         "purego",
		priority:     0,
		decodes:      []tobitenimage.Codec{tobitenimage.CodecJPEG, tobitenimage.CodecPNG, tobitenimage.CodecWebP},
		encodes:      []tobitenimage.Codec{tobitenimage.CodecJPEG},
		placeholders: []tobitenimage.Codec{tobitenimage.CodecJXL, tobitenimage.CodecAVIF},
		renderFn: func(_ context.Context, src io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
			b, err := io.ReadAll(src)
			if err != nil {
				return tobitenimage.Result{}, err
			}
			seenBytes = b
			seenCodec = opts.SourceCodec
			return tobitenimage.Result{
				Data:        b,
				Format:      opts.OutputFormat,
				SourceCodec: opts.SourceCodec,
				Placeholder: opts.SourceCodec == tobitenimage.CodecJXL || opts.SourceCodec == tobitenimage.CodecAVIF,
			}, nil
		},
	}
	if err := reg.Register(jpegOnly); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(p); err != nil {
		t.Fatal(err)
	}

	// JXL and AVIF route past the higher-priority jpeg-only provider to purego
	// for placeholder generation and preserve the original source codec.
	jxlInput := append([]byte("\x00\x00\x00\x0cJXL \r\n\x87\n"), []byte("payload")...)
	res, err := reg.RenderCover(context.Background(), bytes.NewReader(jxlInput), tobitenimage.DefaultCoverOptions())
	if err != nil {
		t.Fatalf("RenderCover(jxl) err = %v", err)
	}
	if !res.Placeholder || res.SourceCodec != tobitenimage.CodecJXL || seenCodec != tobitenimage.CodecJXL {
		t.Fatalf("unexpected jxl result: %+v (seenCodec=%q)", res, seenCodec)
	}
	if !bytes.Equal(seenBytes, jxlInput) {
		t.Fatal("SniffReader replay modified input bytes")
	}

	avifInput := []byte("\x00\x00\x00\x10ftypavif\x00\x00\x00\x00")
	res, err = reg.RenderCover(context.Background(), bytes.NewReader(avifInput), tobitenimage.DefaultCoverOptions())
	if err != nil {
		t.Fatalf("RenderCover(avif) err = %v", err)
	}
	if !res.Placeholder || res.SourceCodec != tobitenimage.CodecAVIF || seenCodec != tobitenimage.CodecAVIF {
		t.Fatalf("unexpected avif result: %+v (seenCodec=%q)", res, seenCodec)
	}

	// Corrupt/unrecognized bytes fail with ErrCorrupt before invoking pipeline.
	if _, err := reg.RenderCover(context.Background(), bytes.NewReader([]byte("not-an-image")), tobitenimage.DefaultCoverOptions()); !errors.Is(err, tobitenimage.ErrCorrupt) {
		t.Fatalf("RenderCover(corrupt) err = %v, want ErrCorrupt", err)
	}

	// Invalid options fail with ErrInvalidOptions.
	if _, err := reg.RenderCover(context.Background(), bytes.NewReader(jxlInput), tobitenimage.RenderOptions{}); !errors.Is(err, tobitenimage.ErrInvalidOptions) {
		t.Fatalf("RenderCover(zero bounds) err = %v, want ErrInvalidOptions", err)
	}

	// Provider preflight limit is enforced before Registry sniffing reads past it.
	regLimited := tobitenimage.NewRegistry()
	if err := regLimited.Register(&stubPipeline{
		name:         "limited",
		decodes:      []tobitenimage.Codec{tobitenimage.CodecJPEG},
		encodes:      []tobitenimage.Codec{tobitenimage.CodecJPEG},
		placeholders: []tobitenimage.Codec{tobitenimage.CodecAVIF},
		maxPreflight: 24,
		renderFn: func(_ context.Context, _ io.Reader, opts tobitenimage.RenderOptions) (tobitenimage.Result, error) {
			return tobitenimage.Result{
				Format:      opts.OutputFormat,
				SourceCodec: opts.SourceCodec,
				Placeholder: true,
			}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	ftyp64 := make([]byte, 64)
	binary.BigEndian.PutUint32(ftyp64[0:4], 64)
	copy(ftyp64[4:8], "ftyp")
	copy(ftyp64[8:12], "mif1")
	copy(ftyp64[16:20], "avif")
	cr := &countingReader{r: bytes.NewReader(ftyp64)}
	res, err = regLimited.RenderCover(context.Background(), cr, tobitenimage.DefaultCoverOptions())
	if err != nil {
		t.Fatalf("RenderCover(64B ftyp early avif) err = %v", err)
	}
	if !res.Placeholder || res.SourceCodec != tobitenimage.CodecAVIF {
		t.Fatalf("unexpected result: %+v", res)
	}
	if cr.n != 24 {
		t.Errorf("registry sniff consumed %d bytes, want 24", cr.n)
	}

	// Late avif brand past 24-byte budget stops at 24 bytes with ErrPreflightLimitExceeded.
	ftypLate := make([]byte, 64)
	binary.BigEndian.PutUint32(ftypLate[0:4], 64)
	copy(ftypLate[4:8], "ftyp")
	copy(ftypLate[8:12], "mif1")
	copy(ftypLate[32:36], "avif")
	crLate := &countingReader{r: bytes.NewReader(ftypLate)}
	if _, err := regLimited.RenderCover(context.Background(), crLate, tobitenimage.DefaultCoverOptions()); !errors.Is(err, tobitenimage.ErrPreflightLimitExceeded) {
		t.Fatalf("RenderCover(64B ftyp late avif) err = %v, want ErrPreflightLimitExceeded", err)
	}
	if crLate.n != 24 {
		t.Errorf("registry sniff consumed %d bytes on late brand, want 24", crLate.n)
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func TestSniffHeaderAndReader(t *testing.T) {
	// ISOBMFF ftyp with generic major brand mif1 and avif in compatible brands.
	ftyp := make([]byte, 24)
	binary.BigEndian.PutUint32(ftyp[0:4], 24)
	copy(ftyp[4:8], "ftyp")
	copy(ftyp[8:12], "mif1")
	copy(ftyp[16:20], "heic")
	copy(ftyp[20:24], "avif")

	cases := []struct {
		name string
		data []byte
		want tobitenimage.Codec
	}{
		{"jpeg", []byte{0xff, 0xd8, 0xff, 0xe0, 0x00}, tobitenimage.CodecJPEG},
		{"png", []byte("\x89PNG\r\n\x1a\nxxxx"), tobitenimage.CodecPNG},
		{"webp", []byte("RIFF\x20\x00\x00\x00WEBPVP8 "), tobitenimage.CodecWebP},
		{"jxl-codestream", []byte{0xff, 0x0a, 0x00, 0x00}, tobitenimage.CodecJXL},
		{"jxl-container", []byte("\x00\x00\x00\x0cJXL \r\n\x87\n\x00\x00"), tobitenimage.CodecJXL},
		{"avif-major", []byte("\x00\x00\x00\x10ftypavif\x00\x00\x00\x00"), tobitenimage.CodecAVIF},
		{"avif-compat", ftyp, tobitenimage.CodecAVIF},
		{"gif", []byte("GIF89a......"), tobitenimage.Codec("gif")},
		{"bmp", []byte("BM.........."), tobitenimage.Codec("bmp")},
		{"tiff-le", []byte("II*\x00........"), tobitenimage.Codec("tiff")},
		{"unknown", []byte("hello world!"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tobitenimage.SniffHeader(tc.data); got != tc.want {
				t.Errorf("SniffHeader = %q, want %q", got, tc.want)
			}
			got, replay, err := tobitenimage.SniffReader(bytes.NewReader(tc.data))
			if err != nil {
				t.Fatalf("SniffReader err = %v", err)
			}
			if got != tc.want {
				t.Errorf("SniffReader codec = %q, want %q", got, tc.want)
			}
			all, err := io.ReadAll(replay)
			if err != nil {
				t.Fatalf("ReadAll(replay) err = %v", err)
			}
			if !bytes.Equal(all, tc.data) {
				t.Errorf("replay bytes = %x, want %x", all, tc.data)
			}
		})
	}
}

func TestFitDimensionsAndPixelLimit(t *testing.T) {
	fitCases := []struct {
		srcW, srcH, maxW, maxH int
		wantW, wantH           int
	}{
		{800, 1200, 400, 0, 400, 600},
		{1200, 800, 400, 400, 400, 267},
		{800, 1600, 400, 400, 200, 400},
		{300, 450, 400, 600, 300, 450}, // no upscale
		{1000, 500, 0, 250, 500, 250},
		{10000, 1, 100, 100, 100, 1}, // floor >= 1
		{10, 45, 7, 0, 7, 32},        // exact half-up rounding (31.5 -> 32)
		{math.MaxInt, math.MaxInt, math.MaxInt - 1, 0, math.MaxInt - 1, math.MaxInt - 1},
		{100, 300, 40, math.MaxInt, 40, 120},  // large unbound axis must not overflow
		{100, 300, math.MaxInt, 120, 40, 120}, // large width bound must not overflow
		{100, 300, math.MaxInt, math.MaxInt, 100, 300},
		{math.MaxInt, 1, 100, math.MaxInt, 100, 1},
	}
	for _, tc := range fitCases {
		gotW, gotH := tobitenimage.FitDimensions(tc.srcW, tc.srcH, tc.maxW, tc.maxH)
		if gotW != tc.wantW || gotH != tc.wantH {
			t.Errorf("FitDimensions(%d,%d,%d,%d) = (%d,%d), want (%d,%d)",
				tc.srcW, tc.srcH, tc.maxW, tc.maxH, gotW, gotH, tc.wantW, tc.wantH)
		}
	}

	jpegFitCases := []struct {
		srcW, srcH, maxW, maxH int
		wantW, wantH           int
	}{
		{8, 65536, 400, 0, 8, 65535},
		{65536, 8, 0, 400, 65535, 8},
		{100, 300, 40, math.MaxInt, 40, 120},
	}
	for _, tc := range jpegFitCases {
		gotW, gotH := tobitenimage.FitCodecDimensions(tobitenimage.CodecJPEG, tc.srcW, tc.srcH, tc.maxW, tc.maxH)
		if gotW != tc.wantW || gotH != tc.wantH {
			t.Errorf("FitCodecDimensions(jpeg, %d,%d,%d,%d) = (%d,%d), want (%d,%d)",
				tc.srcW, tc.srcH, tc.maxW, tc.maxH, gotW, gotH, tc.wantW, tc.wantH)
		}
	}

	if !tobitenimage.ExceedsPixelLimit(1<<30, 1<<30, 100_000_000) {
		t.Error("expected overflow-safe pixel limit check to reject 2^60 pixels")
	}
	if tobitenimage.ExceedsPixelLimit(5, 2, 10) {
		t.Error("expected exact boundary 5*2 <= 10 to pass")
	}
	if !tobitenimage.ExceedsPixelLimit(6, 2, 10) {
		t.Error("expected 6*2 > 10 to exceed limit")
	}
}
