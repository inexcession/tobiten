package domain

import (
	"bytes"
	"errors"
	"testing"
	"unicode/utf8"
)

func TestPathIdentity_NFCAndNFD(t *testing.T) {
	cases := []struct {
		name      string
		nfc       []byte
		nfd       []byte
		wantMatch string
	}{
		{
			name:      "hiragana dakuten",
			nfc:       []byte("Series/\u304c/v01.cbz"),       // が (U+304C)
			nfd:       []byte("Series/\u304b\u3099/v01.cbz"), // か + U+3099
			wantMatch: "Series/\u304c/v01.cbz",
		},
		{
			name:      "latin acute accent",
			nfc:       []byte("Caf\u00e9/Tome 1.cbz"),  // é (U+00E9)
			nfd:       []byte("Cafe\u0301/Tome 1.cbz"), // e + U+0301
			wantMatch: "Caf\u00e9/Tome 1.cbz",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if bytes.Equal(tc.nfc, tc.nfd) {
				t.Fatal("test setup error: NFC and NFD byte slices must differ")
			}

			nfcID, err := NewRelativePathIdentity(tc.nfc)
			if err != nil {
				t.Fatalf("NewRelativePathIdentity(nfc) failed: %v", err)
			}
			nfdID, err := NewRelativePathIdentity(tc.nfd)
			if err != nil {
				t.Fatalf("NewRelativePathIdentity(nfd) failed: %v", err)
			}

			if bytes.Equal(nfcID.Bytes, nfdID.Bytes) {
				t.Fatalf("expected distinct raw bytes for NFC (%x) and NFD (%x)", nfcID.Bytes, nfdID.Bytes)
			}
			if !bytes.Equal(nfcID.Bytes, tc.nfc) {
				t.Fatalf("expected NFC raw bytes %x, got %x", tc.nfc, nfcID.Bytes)
			}
			if !bytes.Equal(nfdID.Bytes, tc.nfd) {
				t.Fatalf("expected NFD raw bytes %x, got %x", tc.nfd, nfdID.Bytes)
			}

			if nfcID.InvalidUTF8 || nfdID.InvalidUTF8 {
				t.Fatalf("expected InvalidUTF8=false for valid NFC/NFD paths, got nfc=%v nfd=%v", nfcID.InvalidUTF8, nfdID.InvalidUTF8)
			}
			if nfcID.Display != string(tc.nfc) {
				t.Fatalf("expected valid NFC name unchanged in Display %q, got %q", string(tc.nfc), nfcID.Display)
			}
			if nfdID.Display != string(tc.nfd) {
				t.Fatalf("expected valid NFD name unchanged in Display %q, got %q", string(tc.nfd), nfdID.Display)
			}
			if nfcID.Match != tc.wantMatch || nfdID.Match != tc.wantMatch {
				t.Fatalf("expected shared NFC match key %q, got nfc=%q nfd=%q", tc.wantMatch, nfcID.Match, nfdID.Match)
			}
		})
	}
}

func TestPathIdentity_InvalidUTF8DisplayAndValidPassThrough(t *testing.T) {
	cases := []struct {
		name        string
		raw         []byte
		wantDisplay string
		wantMatch   string
		wantInvalid bool
	}{
		{
			name:        "leading invalid bytes",
			raw:         []byte{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', '.', 'c', 'b', 'z'},
			wantDisplay: `\xff\xfe/book.cbz`,
			wantMatch:   `\xff\xfe/book.cbz`,
			wantInvalid: true,
		},
		{
			name:        "truncated utf8 sequence before ascii",
			raw:         []byte{'s', 'u', 'b', '/', 0xc3, 'a', '.', 'c', 'b', 'z'},
			wantDisplay: `sub/\xc3a.cbz`,
			wantMatch:   `sub/\xc3a.cbz`,
			wantInvalid: true,
		},
		{
			name:        "invalid 3-byte prefix and NFD suffix",
			raw:         append([]byte{0xe3, 0x81, '/'}, []byte("\u304b\u3099.cbz")...),
			wantDisplay: `\xe3\x81/` + "\u304b\u3099.cbz",
			wantMatch:   `\xe3\x81/` + "\u304c.cbz",
			wantInvalid: true,
		},
		{
			name:        "valid literal backslash-xhh passes through unchanged",
			raw:         []byte(`\xff/book.cbz`),
			wantDisplay: `\xff/book.cbz`,
			wantMatch:   `\xff/book.cbz`,
			wantInvalid: false,
		},
		{
			name:        "valid POSIX punctuation preserved unchanged",
			raw:         []byte(`dir/a:..\sample.cbz`),
			wantDisplay: `dir/a:..\sample.cbz`,
			wantMatch:   `dir/a:..\sample.cbz`,
			wantInvalid: false,
		},
		{
			name:        "valid encoded U+FFFD replacement character",
			raw:         []byte("\ufffd/book.cbz"),
			wantDisplay: "\ufffd/book.cbz",
			wantMatch:   "\ufffd/book.cbz",
			wantInvalid: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := NewPathIdentity(tc.raw)
			if !bytes.Equal(id.Bytes, tc.raw) {
				t.Fatalf("expected raw bytes %x preserved, got %x", tc.raw, id.Bytes)
			}
			if !utf8.ValidString(id.Display) {
				t.Fatalf("expected valid UTF-8 display text, got %x", []byte(id.Display))
			}
			if !utf8.ValidString(id.Match) {
				t.Fatalf("expected valid UTF-8 match key, got %x", []byte(id.Match))
			}
			if id.Display != tc.wantDisplay {
				t.Fatalf("expected Display %q, got %q", tc.wantDisplay, id.Display)
			}
			if id.Match != tc.wantMatch {
				t.Fatalf("expected Match %q, got %q", tc.wantMatch, id.Match)
			}
			if id.InvalidUTF8 != tc.wantInvalid {
				t.Fatalf("expected InvalidUTF8=%v, got %v", tc.wantInvalid, id.InvalidUTF8)
			}
		})
	}
}

func TestPathIdentity_ClonesInputBytes(t *testing.T) {
	raw := []byte("Series/v01.cbz")
	id, err := NewRelativePathIdentity(raw)
	if err != nil {
		t.Fatalf("NewRelativePathIdentity failed: %v", err)
	}
	raw[0] = 'X'
	if string(id.Bytes) != "Series/v01.cbz" {
		t.Fatalf("expected cloned bytes unaffected by caller mutation, got %q", id.Bytes)
	}
}

func TestValidateRelativePathBytes_MatchesMediaAssetTable(t *testing.T) {
	goodPaths := [][]byte{
		[]byte("Series/v01.cbz"),
		[]byte("a:sample.cbz"),
		[]byte(`\sample.cbz`),
		[]byte(`..\sample.cbz`),
		[]byte(`dir/a:sample.cbz`),
		[]byte(`dir/\sample.cbz`),
		[]byte(".hidden.cbz"),
		[]byte("..dots.cbz"),
		{0xff, 0xfe, '/', 'b', 'o', 'o', 'k', '.', 'c', 'b', 'z'},
	}
	for _, p := range goodPaths {
		if err := ValidateRelativePathBytes(p); err != nil {
			t.Errorf("ValidateRelativePathBytes(%q) unexpected error: %v", p, err)
		}
		if _, err := NewRelativePathIdentity(p); err != nil {
			t.Errorf("NewRelativePathIdentity(%q) unexpected error: %v", p, err)
		}
	}

	badPaths := [][]byte{
		nil,
		{},
		[]byte("/abs/path.cbz"),
		[]byte("../escape.cbz"),
		[]byte("sub/../../escape.cbz"),
		[]byte("sub/.."),
		[]byte("."),
		[]byte("./book.cbz"),
		[]byte("sub//book.cbz"),
		[]byte("sub/"),
		[]byte("nul\x00byte.cbz"),
		[]byte(".."),
		[]byte("sub/./book.cbz"),
	}
	for _, p := range badPaths {
		if err := ValidateRelativePathBytes(p); !errors.Is(err, ErrInvalidAssetPath) {
			t.Errorf("ValidateRelativePathBytes(%q) expected ErrInvalidAssetPath, got %v", p, err)
		}
		if _, err := NewRelativePathIdentity(p); !errors.Is(err, ErrInvalidAssetPath) {
			t.Errorf("NewRelativePathIdentity(%q) expected ErrInvalidAssetPath, got %v", p, err)
		}
	}
}

func TestValidateRootPathBytes(t *testing.T) {
	goodRoots := [][]byte{
		[]byte("/"),
		[]byte("/var/media"),
		[]byte("/var/media/"),
		{'/', 'm', 'n', 't', '/', 0xff, 0xfe},
	}
	for _, r := range goodRoots {
		id, err := NewRootPathIdentity(r)
		if err != nil {
			t.Errorf("NewRootPathIdentity(%x) unexpected error: %v", r, err)
		}
		if !bytes.Equal(id.Bytes, r) {
			t.Errorf("expected root bytes %x preserved, got %x", r, id.Bytes)
		}
	}

	badRoots := [][]byte{
		nil,
		{},
		[]byte("relative/root"),
		[]byte("."),
		[]byte("/var/\x00media"),
	}
	for _, r := range badRoots {
		if err := ValidateRootPathBytes(r); !errors.Is(err, ErrInvalidRootPath) {
			t.Errorf("ValidateRootPathBytes(%q) expected ErrInvalidRootPath, got %v", r, err)
		}
		if _, err := NewRootPathIdentity(r); !errors.Is(err, ErrInvalidRootPath) {
			t.Errorf("NewRootPathIdentity(%q) expected ErrInvalidRootPath, got %v", r, err)
		}
	}
}
