package domain

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLocatorJSONRoundTrip(t *testing.T) {
	cfi := "epubcfi(/6/4!/2/1:0)"
	digest := &LocatorDigest{
		Kind:  DigestKindXXH364Partial,
		Value: "0123456789abcdef",
	}

	tests := []struct {
		name         string
		locator      Locator
		wantContains []string
		wantAbsent   []string
	}{
		{
			name:    "page locator with zero-valued position and page count",
			locator: NewPageLocator(0, 0, "asset-page-0", digest),
			wantContains: []string{
				`"type":"page"`,
				`"page":0`,
				`"page_count":0`,
				`"asset_id":"asset-page-0"`,
				`"kind":"XXH3_64_PARTIAL"`,
				`"value":"0123456789abcdef"`,
			},
			wantAbsent: []string{`"href"`, `"progression"`, `"cfi"`},
		},
		{
			name:    "page locator with non-zero position",
			locator: NewPageLocator(12, 190, "asset-page-1", digest),
			wantContains: []string{
				`"type":"page"`,
				`"page":12`,
				`"page_count":190`,
			},
			wantAbsent: []string{`"href"`, `"progression"`, `"cfi"`},
		},
		{
			name:    "text locator with zero-valued progression",
			locator: NewTextLocator("OEBPS/ch01.xhtml", 0.0, nil, "asset-text-0", digest),
			wantContains: []string{
				`"type":"text"`,
				`"href":"OEBPS/ch01.xhtml"`,
				`"progression":0`,
				`"asset_id":"asset-text-0"`,
			},
			wantAbsent: []string{`"page"`, `"page_count"`, `"cfi"`},
		},
		{
			name:    "text locator with non-zero progression and CFI",
			locator: NewTextLocator("OEBPS/ch05.xhtml", 0.425, &cfi, "asset-text-1", digest),
			wantContains: []string{
				`"type":"text"`,
				`"href":"OEBPS/ch05.xhtml"`,
				`"progression":0.425`,
				`"cfi":"epubcfi(/6/4!/2/1:0)"`,
			},
			wantAbsent: []string{`"page"`, `"page_count"`},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.locator.Validate(); err != nil {
				t.Fatalf("input Locator.Validate() error: %v", err)
			}

			raw, err := json.Marshal(tc.locator)
			if err != nil {
				t.Fatalf("json.Marshal error: %v", err)
			}
			encoded := string(raw)

			for _, substr := range tc.wantContains {
				if !strings.Contains(encoded, substr) {
					t.Errorf("JSON %s missing expected %s", encoded, substr)
				}
			}
			for _, substr := range tc.wantAbsent {
				if strings.Contains(encoded, substr) {
					t.Errorf("JSON %s unexpectedly contains %s", encoded, substr)
				}
			}

			var decoded Locator
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("json.Unmarshal error: %v", err)
			}
			if err := decoded.Validate(); err != nil {
				t.Fatalf("decoded Locator.Validate() error: %v", err)
			}
			if !reflect.DeepEqual(decoded, tc.locator) {
				t.Fatalf("round-trip mismatch:\n got: %+v\nwant: %+v", decoded, tc.locator)
			}
		})
	}
}

func TestLocatorValidationAndMatchesAsset(t *testing.T) {
	t.Run("missing zero-required fields rejected after JSON unmarshal", func(t *testing.T) {
		var pageMissing Locator
		if err := json.Unmarshal([]byte(`{"type":"page","page_count":10}`), &pageMissing); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(pageMissing.Validate(), ErrMissingPagePosition) {
			t.Fatalf("expected ErrMissingPagePosition, got %v", pageMissing.Validate())
		}

		var textMissingProg Locator
		if err := json.Unmarshal([]byte(`{"type":"text","href":"ch1.xhtml"}`), &textMissingProg); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(textMissingProg.Validate(), ErrMissingTextProgression) {
			t.Fatalf("expected ErrMissingTextProgression, got %v", textMissingProg.Validate())
		}

		var mixed Locator
		if err := json.Unmarshal([]byte(`{"type":"page","page":0,"href":"ch1.xhtml"}`), &mixed); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(mixed.Validate(), ErrUnexpectedLocatorField) {
			t.Fatalf("expected ErrUnexpectedLocatorField, got %v", mixed.Validate())
		}
	})

	t.Run("MatchesAsset detects matching vs replaced digest", func(t *testing.T) {
		loc := NewPageLocator(5, 20, "asset-1", &LocatorDigest{
			Kind:  DigestKindXXH364Partial,
			Value: "abc123",
		})
		same := &AssetDigest{AssetID: "asset-1", Kind: DigestKindXXH364Partial, Value: "abc123", IsCurrent: true}
		replaced := &AssetDigest{AssetID: "asset-1", Kind: DigestKindXXH364Partial, Value: "def456", IsCurrent: true}

		if !loc.MatchesAsset("asset-1", same) {
			t.Error("expected MatchesAsset == true for same asset and digest")
		}
		if loc.MatchesAsset("asset-1", replaced) {
			t.Error("expected MatchesAsset == false when content digest changed")
		}
		if loc.MatchesAsset("asset-2", same) {
			t.Error("expected MatchesAsset == false for different asset ID")
		}
	})
}

func TestReadingProgressValidate(t *testing.T) {
	now := time.Now().UTC()
	valid := ReadingProgress{
		UserID:           "user-1",
		VolumeID:         "vol-1",
		Locator:          NewPageLocator(0, 100, "asset-1", nil),
		TotalProgression: 0.0,
		Source:           "KOMGA",
		UpdatedAt:        now,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	badProg := valid
	badProg.TotalProgression = 1.5
	if !errors.Is(badProg.Validate(), ErrInvalidTotalProgression) {
		t.Fatalf("expected ErrInvalidTotalProgression, got %v", badProg.Validate())
	}

	noSource := valid
	noSource.Source = ""
	if !errors.Is(noSource.Validate(), ErrEmptyProgressSource) {
		t.Fatalf("expected ErrEmptyProgressSource, got %v", noSource.Validate())
	}
}
