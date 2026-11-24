package purego_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"testing"

	"golang.org/x/image/webp"

	tobitenimage "github.com/inexcession/tobiten/internal/image"
	"github.com/inexcession/tobiten/internal/image/purego"
)

func makeTestRGBA(w, h int, withAlpha bool) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			a := uint8(255)
			if withAlpha && x < w/4 {
				a = 0
			}
			img.SetRGBA(x, y, color.RGBA{
				R: uint8((x * 255) / max(1, w-1)),
				G: uint8((y * 255) / max(1, h-1)),
				B: 160,
				A: a,
			})
		}
	}
	return img
}

func encodeTestJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, makeTestRGBA(w, h, false), &jpeg.Options{Quality: 90}); err != nil {
		t.Fatalf("jpeg.Encode: %v", err)
	}
	return buf.Bytes()
}

func encodeTestPNG(t *testing.T, w, h int, withAlpha bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, makeTestRGBA(w, h, withAlpha)); err != nil {
		t.Fatalf("png.Encode: %v", err)
	}
	return buf.Bytes()
}

// bitWriter writes LSB-first bits for constructing valid VP8L lossless WebP fixtures.
type bitWriter struct {
	buf []byte
	acc uint64
	n   uint
}

func (bw *bitWriter) writeBits(val uint32, count uint) {
	bw.acc |= uint64(val) << bw.n
	bw.n += count
	for bw.n >= 8 {
		bw.buf = append(bw.buf, byte(bw.acc&0xff))
		bw.acc >>= 8
		bw.n -= 8
	}
}

func (bw *bitWriter) bytes() []byte {
	out := append([]byte(nil), bw.buf...)
	if bw.n > 0 {
		out = append(out, byte(bw.acc&0xff))
	}
	return out
}

func writeSimple2SymbolCode(bw *bitWriter, sym0, sym1 byte) {
	bw.writeBits(1, 1) // is_simple_code = 1
	bw.writeBits(1, 1) // num_symbols_minus_1 = 1 (2 symbols)
	bw.writeBits(1, 1) // is_first_8bits = 1
	bw.writeBits(uint32(sym0), 8)
	bw.writeBits(uint32(sym1), 8)
}

func writeSimple1SymbolCode(bw *bitWriter, sym0 byte) {
	bw.writeBits(1, 1) // is_simple_code = 1
	bw.writeBits(0, 1) // num_symbols_minus_1 = 0 (1 symbol)
	bw.writeBits(1, 1) // is_first_8bits = 1
	bw.writeBits(uint32(sym0), 8)
}

// encodeTestWebP builds a valid lossless VP8L WebP image with two alternating colors.
func encodeTestWebP(t *testing.T, w, h int) []byte {
	t.Helper()
	var bw bitWriter
	bw.writeBits(0, 1) // has_transforms = 0
	bw.writeBits(0, 1) // color_cache_used = 0
	bw.writeBits(0, 1) // is_meta_huffman = 0

	// 5 Huffman trees: Green+Length, Red, Blue, Alpha, Distance.
	writeSimple2SymbolCode(&bw, 40, 220) // Green
	writeSimple2SymbolCode(&bw, 200, 30) // Red
	writeSimple1SymbolCode(&bw, 120)     // Blue
	writeSimple1SymbolCode(&bw, 255)     // Alpha
	writeSimple1SymbolCode(&bw, 0)       // Distance

	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			bit := uint32((x/16 + y/16) % 2)
			bw.writeBits(bit, 1) // Green bit
			bw.writeBits(bit, 1) // Red bit
		}
	}

	bitstream := bw.bytes()
	payload := make([]byte, 5+len(bitstream))
	payload[0] = 0x2f
	hdr := uint32(w-1) | (uint32(h-1) << 14) | (0 << 28) | (0 << 29)
	binary.LittleEndian.PutUint32(payload[1:5], hdr)
	copy(payload[5:], bitstream)

	paddedLen := len(payload)
	if paddedLen%2 == 1 {
		paddedLen++
	}
	riff := make([]byte, 12+8+paddedLen)
	copy(riff[0:4], "RIFF")
	binary.LittleEndian.PutUint32(riff[4:8], uint32(4+8+paddedLen))
	copy(riff[8:12], "WEBP")
	copy(riff[12:16], "VP8L")
	binary.LittleEndian.PutUint32(riff[16:20], uint32(len(payload)))
	copy(riff[20:], payload)

	// Self-check that golang.org/x/image/webp decodes our fixture cleanly.
	cfg, err := webp.DecodeConfig(bytes.NewReader(riff))
	if err != nil || cfg.Width != w || cfg.Height != h {
		t.Fatalf("webp fixture self-check failed: cfg=%+v err=%v", cfg, err)
	}
	return riff
}

// buildForgedPNGHeader returns a 33-byte PNG signature + IHDR chunk with no IDAT.
func buildForgedPNGHeader(w, h uint32) []byte {
	var ihdr [13]byte
	binary.BigEndian.PutUint32(ihdr[0:4], w)
	binary.BigEndian.PutUint32(ihdr[4:8], h)
	ihdr[8] = 8 // bit depth
	ihdr[9] = 2 // RGB

	var buf bytes.Buffer
	buf.WriteString("\x89PNG\r\n\x1a\n")
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], 13)
	buf.Write(lenBuf[:])
	buf.WriteString("IHDR")
	buf.Write(ihdr[:])
	crc := crc32.NewIEEE()
	crc.Write([]byte("IHDR"))
	crc.Write(ihdr[:])
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc.Sum32())
	buf.Write(crcBuf[:])
	return buf.Bytes()
}

// buildForgedJPEGHeader returns a JPEG SOI + APP0 (JFIF) + SOF0 header declaring
// w x h with no scan data.
func buildForgedJPEGHeader(w, h uint16) []byte {
	buf := []byte{
		0xff, 0xd8, // SOI
		0xff, 0xe0, // APP0 (JFIF)
		0x00, 0x10, // length = 16
		'J', 'F', 'I', 'F', 0x00,
		0x01, 0x01, // version 1.1
		0x00,       // units
		0x00, 0x01, // X density
		0x00, 0x01, // Y density
		0x00, 0x00, // thumbnail 0x0
		0xff, 0xc0, // SOF0
		0x00, 0x11, // length = 17
		0x08,       // precision = 8
		0x00, 0x00, // height
		0x00, 0x00, // width
		0x03,             // 3 components
		0x01, 0x11, 0x00, // Y
		0x02, 0x11, 0x00, // Cb
		0x03, 0x11, 0x00, // Cr
	}
	binary.BigEndian.PutUint16(buf[25:27], h)
	binary.BigEndian.PutUint16(buf[27:29], w)
	return buf
}

// buildForgedWebPHeader returns a minimal VP8L header declaring w x h with no pixel bitstream.
func buildForgedWebPHeader(w, h uint32) []byte {
	payload := make([]byte, 8)
	payload[0] = 0x2f
	binary.LittleEndian.PutUint32(payload[1:5], (w-1)|((h-1)<<14))
	out := make([]byte, 12+8+len(payload))
	copy(out[0:4], "RIFF")
	binary.LittleEndian.PutUint32(out[4:8], uint32(4+8+len(payload)))
	copy(out[8:12], "WEBP")
	copy(out[12:16], "VP8L")
	binary.LittleEndian.PutUint32(out[16:20], uint32(len(payload)))
	copy(out[20:], payload)
	return out
}

// failPastOffsetReader yields data[:limit] normally and fails with errBodyRead if
// any read attempts to advance past limit, proving full decode was never reached.
type failPastOffsetReader struct {
	header   []byte
	pos      int
	bodyRead bool
}

var errBodyRead = errors.New("unexpected read past header")

func (r *failPastOffsetReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.header) {
		r.bodyRead = true
		return 0, errBodyRead
	}
	n := copy(p, r.header[r.pos:])
	r.pos += n
	return n, nil
}

func TestRegistryAdvertisesPureGoFormats(t *testing.T) {
	p, err := purego.New(purego.Options{})
	if err != nil {
		t.Fatalf("purego.New: %v", err)
	}
	reg := tobitenimage.NewRegistry()
	if err := reg.Register(p); err != nil {
		t.Fatalf("Register(purego): %v", err)
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
	if len(gotEnc) != 1 || gotEnc[0] != tobitenimage.CodecJPEG {
		t.Fatalf("EncodeFormats() = %v, want [jpeg]", gotEnc)
	}
	if !reg.SupportsDecode(tobitenimage.CodecJPEG) || !reg.SupportsDecode(tobitenimage.CodecPNG) || !reg.SupportsDecode(tobitenimage.CodecWebP) {
		t.Fatal("expected JPEG, PNG, and WebP decode support to be advertised")
	}
	if reg.SupportsDecode(tobitenimage.CodecJXL) || reg.SupportsDecode(tobitenimage.CodecAVIF) {
		t.Fatal("Tier 1 must not advertise JXL or AVIF decode support")
	}
	if !reg.SupportsEncode(tobitenimage.CodecJPEG) {
		t.Fatal("expected JPEG encode support to be advertised")
	}
}

func TestRenderCover_DecodeResizeAndDeterministicEncode(t *testing.T) {
	p, err := purego.New(purego.Options{})
	if err != nil {
		t.Fatal(err)
	}
	reg := tobitenimage.NewRegistry()
	if err := reg.Register(p); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name      string
		src       []byte
		wantCodec tobitenimage.Codec
		opts      tobitenimage.RenderOptions
		wantW     int
		wantH     int
	}{
		{
			name:      "jpeg portrait width-bounded",
			src:       encodeTestJPEG(t, 600, 900),
			wantCodec: tobitenimage.CodecJPEG,
			opts:      tobitenimage.RenderOptions{MaxWidth: 300, Quality: 85},
			wantW:     300,
			wantH:     450,
		},
		{
			name:      "png alpha landscape box-bounded",
			src:       encodeTestPNG(t, 800, 400, true),
			wantCodec: tobitenimage.CodecPNG,
			opts:      tobitenimage.RenderOptions{MaxWidth: 400, MaxHeight: 400, Quality: 85},
			wantW:     400,
			wantH:     200,
		},
		{
			name:      "webp portrait box-bounded",
			src:       encodeTestWebP(t, 320, 640),
			wantCodec: tobitenimage.CodecWebP,
			opts:      tobitenimage.RenderOptions{MaxWidth: 300, MaxHeight: 300, Quality: 85},
			wantW:     150,
			wantH:     300,
		},
		{
			name:      "png smaller than bound is not upscaled",
			src:       encodeTestPNG(t, 120, 180, false),
			wantCodec: tobitenimage.CodecPNG,
			opts:      tobitenimage.RenderOptions{MaxWidth: 400, MaxHeight: 600, Quality: 85},
			wantW:     120,
			wantH:     180,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res1, err := reg.RenderCover(context.Background(), bytes.NewReader(tc.src), tc.opts)
			if err != nil {
				t.Fatalf("first RenderCover: %v", err)
			}
			res2, err := p.RenderCover(context.Background(), bytes.NewReader(tc.src), tc.opts)
			if err != nil {
				t.Fatalf("second RenderCover: %v", err)
			}

			if res1.Placeholder {
				t.Error("expected non-placeholder result")
			}
			if res1.SourceCodec != tc.wantCodec {
				t.Errorf("SourceCodec = %q, want %q", res1.SourceCodec, tc.wantCodec)
			}
			if res1.Format != tobitenimage.CodecJPEG || res1.ContentType != "image/jpeg" {
				t.Errorf("Format/ContentType = (%q, %q), want (jpeg, image/jpeg)", res1.Format, res1.ContentType)
			}
			if res1.Width != tc.wantW || res1.Height != tc.wantH {
				t.Errorf("dimensions = %dx%d, want %dx%d", res1.Width, res1.Height, tc.wantW, tc.wantH)
			}
			if !bytes.Equal(res1.Data, res2.Data) {
				t.Error("expected identical encoded bytes across repeated runs for fixed input")
			}

			cfg, err := jpeg.DecodeConfig(bytes.NewReader(res1.Data))
			if err != nil {
				t.Fatalf("output is not valid JPEG: %v", err)
			}
			if cfg.Width != tc.wantW || cfg.Height != tc.wantH {
				t.Errorf("decoded JPEG dimensions = %dx%d, want %dx%d", cfg.Width, cfg.Height, tc.wantW, tc.wantH)
			}
		})
	}
}

func TestRenderCover_JXLAndAVIFReturnPlaceholderWithCodec(t *testing.T) {
	p, err := purego.New(purego.Options{})
	if err != nil {
		t.Fatal(err)
	}
	reg := tobitenimage.NewRegistry()
	if err := reg.Register(p); err != nil {
		t.Fatal(err)
	}

	avifCompat := make([]byte, 24)
	binary.BigEndian.PutUint32(avifCompat[0:4], 24)
	copy(avifCompat[4:8], "ftyp")
	copy(avifCompat[8:12], "mif1")
	copy(avifCompat[20:24], "avif")

	cases := []struct {
		name      string
		data      []byte
		wantCodec tobitenimage.Codec
	}{
		{"jxl codestream", []byte{0xff, 0x0a, 0x00, 0x01, 0x02}, tobitenimage.CodecJXL},
		{"jxl container", []byte("\x00\x00\x00\x0cJXL \r\n\x87\n\x00\x00"), tobitenimage.CodecJXL},
		{"avif major", []byte("\x00\x00\x00\x10ftypavif\x00\x00\x00\x00"), tobitenimage.CodecAVIF},
		{"avif compat", avifCompat, tobitenimage.CodecAVIF},
	}

	opts := tobitenimage.RenderOptions{MaxWidth: 200, MaxHeight: 200, Quality: 80}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := reg.RenderCover(context.Background(), bytes.NewReader(tc.data), opts)
			if err != nil {
				t.Fatalf("RenderCover(%s): %v", tc.name, err)
			}
			if !res.Placeholder {
				t.Error("expected Placeholder = true")
			}
			if res.SourceCodec != tc.wantCodec {
				t.Errorf("SourceCodec = %q, want %q", res.SourceCodec, tc.wantCodec)
			}
			if res.Format != tobitenimage.CodecJPEG || res.ContentType != "image/jpeg" {
				t.Errorf("Format/ContentType = (%q, %q), want (jpeg, image/jpeg)", res.Format, res.ContentType)
			}
			if res.Width > 200 || res.Height > 200 || res.Width <= 0 || res.Height <= 0 {
				t.Errorf("unexpected placeholder dimensions %dx%d", res.Width, res.Height)
			}

			cfg, err := jpeg.DecodeConfig(bytes.NewReader(res.Data))
			if err != nil {
				t.Fatalf("placeholder is not valid JPEG: %v", err)
			}
			if cfg.Width != res.Width || cfg.Height != res.Height {
				t.Errorf("decoded placeholder dimensions %dx%d != Result %dx%d", cfg.Width, cfg.Height, res.Width, res.Height)
			}

			resDirect, err := p.RenderCover(context.Background(), bytes.NewReader(tc.data), opts)
			if err != nil {
				t.Fatalf("direct RenderCover: %v", err)
			}
			if !bytes.Equal(res.Data, resDirect.Data) {
				t.Error("expected deterministic placeholder bytes")
			}
		})
	}
}

func TestRenderCover_CorruptAndPixelLimitFailBeforeFullDecode(t *testing.T) {
	if _, err := purego.New(purego.Options{MaxPixels: -1}); !errors.Is(err, tobitenimage.ErrInvalidOptions) {
		t.Fatalf("negative MaxPixels error = %v, want ErrInvalidOptions", err)
	}

	p, err := purego.New(purego.Options{MaxPixels: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	opts := tobitenimage.DefaultCoverOptions()

	t.Run("pixel limit fails before reading body", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			header []byte
		}{
			{"png", buildForgedPNGHeader(200, 100)},     // 20,000 > 10,000
			{"webp", buildForgedWebPHeader(200, 100)},   // 20,000 > 10,000
			{"jpeg", buildForgedJPEGHeader(1000, 1000)}, // 1,000,000 > 10,000
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := &failPastOffsetReader{header: tc.header}
				_, err := p.RenderCover(context.Background(), r, opts)
				if !errors.Is(err, tobitenimage.ErrPixelLimitExceeded) {
					t.Fatalf("err = %v, want ErrPixelLimitExceeded", err)
				}
				if r.bodyRead {
					t.Error("expected pixel limit check to reject before reading past header")
				}
			})
		}

		// Large JPEG with valid SOF0 in first chunk and poison after 4KB proves
		// DecodeConfig stops before full decode of scan entropy data.
		jpegHeader := buildForgedJPEGHeader(500, 500)
		paddedHeader := make([]byte, 4096)
		copy(paddedHeader, jpegHeader)
		r := &failPastOffsetReader{header: paddedHeader}
		if _, err := p.RenderCover(context.Background(), r, opts); !errors.Is(err, tobitenimage.ErrPixelLimitExceeded) {
			t.Fatalf("padded jpeg err = %v, want ErrPixelLimitExceeded", err)
		}
		if r.bodyRead {
			t.Error("expected jpeg pixel limit check to fail before reading beyond header buffer")
		}
	})

	t.Run("exact boundary succeeds and per-call override applies", func(t *testing.T) {
		exact := encodeTestPNG(t, 100, 100, false) // 10,000 == 10,000
		if _, err := p.RenderCover(context.Background(), bytes.NewReader(exact), opts); err != nil {
			t.Fatalf("exact boundary failed: %v", err)
		}

		tight := opts
		tight.MaxPixels = 9_999
		if _, err := p.RenderCover(context.Background(), bytes.NewReader(exact), tight); !errors.Is(err, tobitenimage.ErrPixelLimitExceeded) {
			t.Fatalf("override limit err = %v, want ErrPixelLimitExceeded", err)
		}
	})

	t.Run("corrupt inputs fail with ErrCorrupt", func(t *testing.T) {
		corruptPNG := append(buildForgedPNGHeader(10, 10), []byte("bad-idat-chunk")...)
		cases := []struct {
			name string
			r    io.Reader
		}{
			{"nil reader", nil},
			{"empty", bytes.NewReader(nil)},
			{"garbage", bytes.NewReader([]byte("not an image at all"))},
			{"truncated png header", bytes.NewReader([]byte("\x89PNG\r\n\x1a\n\x00\x00"))},
			{"corrupt png body", bytes.NewReader(corruptPNG)},
			{"corrupt jpeg", bytes.NewReader([]byte{0xff, 0xd8, 0xff, 0xd9})},
			{"corrupt webp", bytes.NewReader([]byte("RIFF\x10\x00\x00\x00WEBPVP8X\x00\x00"))},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := p.RenderCover(context.Background(), tc.r, opts)
				if !errors.Is(err, tobitenimage.ErrCorrupt) {
					t.Fatalf("%s: err = %v, want ErrCorrupt", tc.name, err)
				}
			})
		}
	})

	t.Run("unsupported decode and encode formats fail with typed errors", func(t *testing.T) {
		gifBytes := []byte("GIF89a\x01\x00\x01\x00\x00\x00\x00;")
		if _, err := p.RenderCover(context.Background(), bytes.NewReader(gifBytes), opts); !errors.Is(err, tobitenimage.ErrUnsupportedFormat) {
			t.Fatalf("gif err = %v, want ErrUnsupportedFormat", err)
		}

		badOut := opts
		badOut.OutputFormat = tobitenimage.CodecWebP
		if _, err := p.RenderCover(context.Background(), bytes.NewReader(encodeTestJPEG(t, 10, 10)), badOut); !errors.Is(err, tobitenimage.ErrUnsupportedEncodeFormat) {
			t.Fatalf("webp encode err = %v, want ErrUnsupportedEncodeFormat", err)
		}
	})

	t.Run("canceled context fails immediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := p.RenderCover(ctx, bytes.NewReader(encodeTestJPEG(t, 10, 10)), opts); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled ctx err = %v, want context.Canceled", err)
		}
	})
}
