package resolver

import (
	"math"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/metadata/comicinfo"
)

func int32Ptr(v int32) *int32 {
	return &v
}

func relPathID(t *testing.T, raw []byte) domain.PathIdentity {
	t.Helper()
	id, err := domain.NewRelativePathIdentity(raw)
	if err != nil {
		t.Fatalf("NewRelativePathIdentity(%q) failed: %v", raw, err)
	}
	return id
}

func assertNumberPtr(t *testing.T, got *float64, wantNil bool, wantVal float64) {
	t.Helper()
	if wantNil {
		if got != nil {
			t.Fatalf("Number = %v, want nil", *got)
		}
		return
	}
	if got == nil {
		t.Fatalf("Number = nil, want %v", wantVal)
	}
	if *got != wantVal || math.Signbit(*got) != math.Signbit(wantVal) {
		t.Fatalf("Number = %v (signbit=%v), want %v (signbit=%v)", *got, math.Signbit(*got), wantVal, math.Signbit(wantVal))
	}
}

func assertInt32Ptr(t *testing.T, name string, got *int32, wantNil bool, wantVal int32) {
	t.Helper()
	if wantNil {
		if got != nil {
			t.Fatalf("%s = %d, want nil", name, *got)
		}
		return
	}
	if got == nil {
		t.Fatalf("%s = nil, want %d", name, wantVal)
	}
	if *got != wantVal {
		t.Fatalf("%s = %d, want %d", name, *got, wantVal)
	}
}

func TestResolve_ChainPrecedenceAndFallback(t *testing.T) {
	folderFirst := []domain.ResolverName{domain.ResolverFolder, domain.ResolverMetadata, domain.ResolverFilename}
	folderFilenameMeta := []domain.ResolverName{domain.ResolverFolder, domain.ResolverFilename, domain.ResolverMetadata}
	filenameFirst := []domain.ResolverName{domain.ResolverFilename, domain.ResolverMetadata, domain.ResolverFolder}

	tests := []struct {
		name           string
		meta           *comicinfo.Metadata
		relPath        string
		chain          []domain.ResolverName
		wantStandalone bool
		wantSource     domain.ResolverName
		wantTitle      string
		wantRunNil     bool
		wantRun        int32
		wantKind       domain.VolumeKind
		wantNumNil     bool
		wantNum        float64
		wantNumSource  domain.ResolverName
	}{
		{
			name: "default chain chooses metadata first on identical input",
			meta: &comicinfo.Metadata{
				Series: "Meta Series",
				Volume: int32Ptr(2018),
				Number: "1.5",
			},
			relPath:       "Folder Series/Filename Series v03.cbz",
			chain:         domain.DefaultResolverChain(),
			wantSource:    domain.ResolverMetadata,
			wantTitle:     "Meta Series",
			wantRun:       2018,
			wantKind:      domain.VolumeKindVolume,
			wantNum:       1.5,
			wantNumSource: domain.ResolverMetadata,
		},
		{
			name: "folder-first chain resolves Komga-style and fills missing number from metadata",
			meta: &comicinfo.Metadata{
				Series: "Meta Series",
				Volume: int32Ptr(2018),
				Number: "1.5",
			},
			relPath:       "Folder Series/Filename Series v03.cbz",
			chain:         folderFirst,
			wantSource:    domain.ResolverFolder,
			wantTitle:     "Folder Series",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindVolume,
			wantNum:       1.5,
			wantNumSource: domain.ResolverMetadata,
		},
		{
			name: "folder-first with filename second fills number from filename",
			meta: &comicinfo.Metadata{
				Series: "Meta Series",
				Number: "1.5",
			},
			relPath:       "Folder Series/Filename Series c012.cbz",
			chain:         folderFilenameMeta,
			wantSource:    domain.ResolverFolder,
			wantTitle:     "Folder Series",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindChapter,
			wantNum:       12,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name: "filename-first keeps filename series and number over metadata",
			meta: &comicinfo.Metadata{
				Series: "Meta Series",
				Volume: int32Ptr(2018),
				Number: "9",
			},
			relPath:       "Folder Series/Filename Series v03.cbz",
			chain:         filenameFirst,
			wantSource:    domain.ResolverFilename,
			wantTitle:     "Filename Series",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindVolume,
			wantNum:       3,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name: "default chain falls back to folder when metadata has no series",
			meta: &comicinfo.Metadata{
				Title: "Just A Chapter",
			},
			relPath:       "Folder Series/Filename Series c004.cbz",
			chain:         domain.DefaultResolverChain(),
			wantSource:    domain.ResolverFolder,
			wantTitle:     "Folder Series",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindChapter,
			wantNum:       4,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name: "default chain falls back to filename when directly under root without metadata",
			meta: &comicinfo.Metadata{
				Summary: "No series tag",
			},
			relPath:       "Filename Series #007.cbz",
			chain:         domain.DefaultResolverChain(),
			wantSource:    domain.ResolverFilename,
			wantTitle:     "Filename Series",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindIssue,
			wantNum:       7,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name: "metadata winner with empty number falls back to filename while keeping metadata run",
			meta: &comicinfo.Metadata{
				Series: "Example",
				Volume: int32Ptr(2018),
				Number: "",
			},
			relPath:       "Other Folder/Other Series c012.5.cbz",
			chain:         domain.DefaultResolverChain(),
			wantSource:    domain.ResolverMetadata,
			wantTitle:     "Example",
			wantRun:       2018,
			wantKind:      domain.VolumeKindChapter,
			wantNum:       12.5,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name: "metadata winner with unusable number falls back to filename while keeping series",
			meta: &comicinfo.Metadata{
				Series: "Example",
				Volume: int32Ptr(2018),
				Number: "1/2",
			},
			relPath:       "Other Folder/Other Series v05.cbz",
			chain:         domain.DefaultResolverChain(),
			wantSource:    domain.ResolverMetadata,
			wantTitle:     "Example",
			wantRun:       2018,
			wantKind:      domain.VolumeKindVolume,
			wantNum:       5,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name: "metadata winner with number does not let filename overwrite number or add run",
			meta: &comicinfo.Metadata{
				Series: "Example",
				Number: "4",
			},
			relPath:       "Example/Example v09.cbz",
			chain:         domain.DefaultResolverChain(),
			wantSource:    domain.ResolverMetadata,
			wantTitle:     "Example",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindVolume,
			wantNum:       4,
			wantNumSource: domain.ResolverMetadata,
		},
		{
			name: "metadata winner with run and no usable number and no filename marker stays unnumbered",
			meta: &comicinfo.Metadata{
				Series: "Example",
				Volume: int32Ptr(2018),
			},
			relPath:    "Example/Example.cbz",
			chain:      domain.DefaultResolverChain(),
			wantSource: domain.ResolverMetadata,
			wantTitle:  "Example",
			wantRun:    2018,
			wantKind:   domain.VolumeKindVolume,
			wantNumNil: true,
		},
		{
			name:          "folder winner fills number from marker-only filename like v05.cbz",
			meta:          nil,
			relPath:       "Example/v05.cbz",
			chain:         domain.DefaultResolverChain(),
			wantSource:    domain.ResolverFolder,
			wantTitle:     "Example",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindVolume,
			wantNum:       5,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name:          "filename-first falls through marker-only filename to folder and keeps filename number",
			meta:          nil,
			relPath:       "Example/v05.cbz",
			chain:         filenameFirst,
			wantSource:    domain.ResolverFolder,
			wantTitle:     "Example",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindVolume,
			wantNum:       5,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name: "metadata number fills folder winner even when metadata precedes folder without series",
			meta: &comicinfo.Metadata{
				Number: "42",
			},
			relPath:       "Example/Example v05.cbz",
			chain:         domain.DefaultResolverChain(),
			wantSource:    domain.ResolverFolder,
			wantTitle:     "Example",
			wantRunNil:    true,
			wantKind:      domain.VolumeKindVolume,
			wantNum:       42,
			wantNumSource: domain.ResolverMetadata,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Resolve(tc.meta, relPathID(t, []byte(tc.relPath)), tc.chain)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if res.Standalone != tc.wantStandalone {
				t.Fatalf("Standalone = %v, want %v", res.Standalone, tc.wantStandalone)
			}
			if tc.wantStandalone {
				if res.Series != nil {
					t.Fatalf("Series = %+v, want nil", res.Series)
				}
				return
			}
			if res.Series == nil {
				t.Fatal("Series = nil, want non-nil")
			}
			if res.Series.Source != tc.wantSource {
				t.Errorf("Series.Source = %q, want %q", res.Series.Source, tc.wantSource)
			}
			if res.Series.Title != tc.wantTitle {
				t.Errorf("Series.Title = %q, want %q", res.Series.Title, tc.wantTitle)
			}
			assertInt32Ptr(t, "Series.MetadataRun", res.Series.MetadataRun, tc.wantRunNil, tc.wantRun)
			if res.VolumeKind != tc.wantKind {
				t.Errorf("VolumeKind = %q, want %q", res.VolumeKind, tc.wantKind)
			}
			assertNumberPtr(t, res.Number, tc.wantNumNil, tc.wantNum)
			if res.NumberSource != tc.wantNumSource {
				t.Errorf("NumberSource = %q, want %q", res.NumberSource, tc.wantNumSource)
			}
		})
	}
}

func TestResolve_StandalonesAndRootFiles(t *testing.T) {
	tests := []struct {
		name          string
		meta          *comicinfo.Metadata
		relPath       string
		chain         []domain.ResolverName
		wantKind      domain.VolumeKind
		wantNumNil    bool
		wantNum       float64
		wantNumSource domain.ResolverName
	}{
		{
			name:       "plain file directly under root is standalone",
			relPath:    "Akira.cbz",
			chain:      domain.DefaultResolverChain(),
			wantKind:   domain.VolumeKindVolume,
			wantNumNil: true,
		},
		{
			name:       "file directly under root with bracketed tags only is standalone",
			relPath:    "[Katsuhiro Otomo] Akira [2024].cbz",
			chain:      domain.DefaultResolverChain(),
			wantKind:   domain.VolumeKindVolume,
			wantNumNil: true,
		},
		{
			name:       "file directly under root has no folder series in folder-only chain",
			relPath:    "One Piece v01.cbz",
			chain:      []domain.ResolverName{domain.ResolverFolder},
			wantKind:   domain.VolumeKindVolume,
			wantNumNil: true,
		},
		{
			name: "file directly under root with metadata number but empty series is standalone with number",
			meta: &comicinfo.Metadata{
				Number: "2",
			},
			relPath:       "Standalone.cbz",
			chain:         domain.DefaultResolverChain(),
			wantKind:      domain.VolumeKindVolume,
			wantNum:       2,
			wantNumSource: domain.ResolverMetadata,
		},
		{
			name:          "marker-only file directly under root is standalone",
			relPath:       "v01.cbz",
			chain:         domain.DefaultResolverChain(),
			wantKind:      domain.VolumeKindVolume,
			wantNum:       1,
			wantNumSource: domain.ResolverFilename,
		},
		{
			name:       "whitespace-only parent directory yields no folder series",
			relPath:    "   /Akira.cbz",
			chain:      []domain.ResolverName{domain.ResolverFolder},
			wantKind:   domain.VolumeKindVolume,
			wantNumNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Resolve(tc.meta, relPathID(t, []byte(tc.relPath)), tc.chain)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if !res.Standalone {
				t.Fatalf("Standalone = false (series=%+v), want true", res.Series)
			}
			if res.Series != nil {
				t.Fatalf("Series = %+v, want nil", res.Series)
			}
			if res.VolumeKind != tc.wantKind {
				t.Errorf("VolumeKind = %q, want %q", res.VolumeKind, tc.wantKind)
			}
			assertNumberPtr(t, res.Number, tc.wantNumNil, tc.wantNum)
			if res.NumberSource != tc.wantNumSource {
				t.Errorf("NumberSource = %q, want %q", res.NumberSource, tc.wantNumSource)
			}
		})
	}
}

func TestResolve_FilenameMarkersAndBracketTags(t *testing.T) {
	filenameOnly := []domain.ResolverName{domain.ResolverFilename}

	tests := []struct {
		name           string
		relPath        string
		wantStandalone bool
		wantTitle      string
		wantKind       domain.VolumeKind
		wantNum        float64
	}{
		{
			name:      "v01 volume marker",
			relPath:   "One Piece v01.cbz",
			wantTitle: "One Piece",
			wantKind:  domain.VolumeKindVolume,
			wantNum:   1,
		},
		{
			name:      "V001 uppercase volume marker with fractional",
			relPath:   "One Piece V012.5.cbz",
			wantTitle: "One Piece",
			wantKind:  domain.VolumeKindVolume,
			wantNum:   12.5,
		},
		{
			name:      "c001 chapter marker",
			relPath:   "One Piece c001.cbz",
			wantTitle: "One Piece",
			wantKind:  domain.VolumeKindChapter,
			wantNum:   1,
		},
		{
			name:      "fractional chapter marker c1090.5",
			relPath:   "One Piece c1090.5.cbz",
			wantTitle: "One Piece",
			wantKind:  domain.VolumeKindChapter,
			wantNum:   1090.5,
		},
		{
			name:      "hash issue marker #001",
			relPath:   "Batman #001.cbz",
			wantTitle: "Batman",
			wantKind:  domain.VolumeKindIssue,
			wantNum:   1,
		},
		{
			name:      "issue marker with parenthesized year",
			relPath:   "Batman #001 (2016) [Digital].cbz",
			wantTitle: "Batman",
			wantKind:  domain.VolumeKindIssue,
			wantNum:   1,
		},
		{
			name:      "SP01 special marker with subtitle",
			relPath:   "Gintama SP01 Jump Festa.cbz",
			wantTitle: "Gintama",
			wantKind:  domain.VolumeKindSpecial,
			wantNum:   1,
		},
		{
			name:      "lowercase sp marker",
			relPath:   "Gintama sp02.5.cbz",
			wantTitle: "Gintama",
			wantKind:  domain.VolumeKindSpecial,
			wantNum:   2.5,
		},
		{
			name:      "leading middle and trailing bracketed tags ignored",
			relPath:   "[Group] One Piece [Digital] v01 [a1b2c3d4].cbz",
			wantTitle: "One Piece",
			wantKind:  domain.VolumeKindVolume,
			wantNum:   1,
		},
		{
			name:      "nested bracketed tags and fake markers inside brackets ignored",
			relPath:   "One Piece [[v99]] c001 [c999].cbz",
			wantTitle: "One Piece",
			wantKind:  domain.VolumeKindChapter,
			wantNum:   1,
		},
		{
			name:           "fake marker only inside brackets is standalone",
			relPath:        "Standalone [v01].cbz",
			wantStandalone: true,
		},
		{
			name:      "hyphen separator before marker cleaned",
			relPath:   "One Piece - v01.cbz",
			wantTitle: "One Piece",
			wantKind:  domain.VolumeKindVolume,
			wantNum:   1,
		},
		{
			name:      "internal hyphen in title preserved",
			relPath:   "D.Gray-man v01.cbz",
			wantTitle: "D.Gray-man",
			wantKind:  domain.VolumeKindVolume,
			wantNum:   1,
		},
		{
			name:      "underscore separated filename",
			relPath:   "One_Piece_v01.cbz",
			wantTitle: "One_Piece",
			wantKind:  domain.VolumeKindVolume,
			wantNum:   1,
		},
		{
			name:      "numeric words in series title not treated as markers",
			relPath:   "Mob Psycho 100 v01.cbz",
			wantTitle: "Mob Psycho 100",
			wantKind:  domain.VolumeKindVolume,
			wantNum:   1,
		},
		{
			name:      "series starting with V not confused with v01",
			relPath:   "V for Vendetta v01.cbz",
			wantTitle: "V for Vendetta",
			wantKind:  domain.VolumeKindVolume,
			wantNum:   1,
		},
		{
			name:      "non-extension fractional suffix without extension preserved",
			relPath:   "One Piece c1090.5",
			wantTitle: "One Piece",
			wantKind:  domain.VolumeKindChapter,
			wantNum:   1090.5,
		},
		{
			name:           "marker letters inside a word do not match",
			relPath:        "Dev01.cbz",
			wantStandalone: true,
		},
		{
			name:           "bare issue number without ADR-0019 marker does not match",
			relPath:        "Batman 001.cbz",
			wantStandalone: true,
		},
		{
			name:           "Chapter word without ADR-0019 marker does not match",
			relPath:        "One Piece - Chapter 1.cbz",
			wantStandalone: true,
		},
		{
			name:           "marker with trailing letter suffix does not match",
			relPath:        "One Piece v01a.cbz",
			wantStandalone: true,
		},
		{
			name:           "marker immediately followed by dot-word does not match",
			relPath:        "One Piece v01.Part1.cbz",
			wantStandalone: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Resolve(nil, relPathID(t, []byte(tc.relPath)), filenameOnly)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if res.Standalone != tc.wantStandalone {
				t.Fatalf("Standalone = %v, want %v (series=%+v)", res.Standalone, tc.wantStandalone, res.Series)
			}
			if tc.wantStandalone {
				return
			}
			if res.Series == nil {
				t.Fatal("Series = nil, want non-nil")
			}
			if res.Series.Source != domain.ResolverFilename {
				t.Errorf("Series.Source = %q, want filename", res.Series.Source)
			}
			if res.Series.Title != tc.wantTitle {
				t.Errorf("Series.Title = %q, want %q", res.Series.Title, tc.wantTitle)
			}
			if res.VolumeKind != tc.wantKind {
				t.Errorf("VolumeKind = %q, want %q", res.VolumeKind, tc.wantKind)
			}
			assertNumberPtr(t, res.Number, false, tc.wantNum)
		})
	}
}

func TestResolve_M1BD5SeriesKeys(t *testing.T) {
	defaultChain := domain.DefaultResolverChain()
	folderOnly := []domain.ResolverName{domain.ResolverFolder}

	t.Run("metadata title normalization shares keys across NFC, NFD, case, and whitespace", func(t *testing.T) {
		a, err := Resolve(&comicinfo.Metadata{Series: "  \u3000\u00c9lodie   Wei\u00df!  "}, relPathID(t, []byte("a.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Resolve(&comicinfo.Metadata{Series: "e\u0301LODIE\t\nWEISS!"}, relPathID(t, []byte("b.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		if a.Series.Key != b.Series.Key {
			t.Fatalf("keys differ: %q vs %q", a.Series.Key, b.Series.Key)
		}
		if a.Series.Title != "\u00c9lodie   Wei\u00df!" {
			t.Errorf("source display title not preserved: %q", a.Series.Title)
		}
		if b.Series.Title != "e\u0301LODIE\t\nWEISS!" {
			t.Errorf("source display title not preserved: %q", b.Series.Title)
		}
	})

	t.Run("punctuation and edition words remain distinct", func(t *testing.T) {
		plain, err := Resolve(&comicinfo.Metadata{Series: "Dragon Ball"}, relPathID(t, []byte("1.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		punct, err := Resolve(&comicinfo.Metadata{Series: "Dragon Ball!"}, relPathID(t, []byte("2.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		edition, err := Resolve(&comicinfo.Metadata{Series: "Dragon Ball (Kanzenban)"}, relPathID(t, []byte("3.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		if plain.Series.Key == punct.Series.Key {
			t.Errorf("punctuation collapsed unexpectedly: %q", plain.Series.Key)
		}
		if plain.Series.Key == edition.Series.Key {
			t.Errorf("edition words collapsed unexpectedly: %q", plain.Series.Key)
		}
	})

	t.Run("metadata runs stay separate while absent and negative share unspecified run", func(t *testing.T) {
		unspecNil, err := Resolve(&comicinfo.Metadata{Series: "Example"}, relPathID(t, []byte("1.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		unspecNeg, err := Resolve(&comicinfo.Metadata{Series: "Example", Volume: int32Ptr(-1)}, relPathID(t, []byte("2.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		runZero, err := Resolve(&comicinfo.Metadata{Series: "Example", Volume: int32Ptr(0)}, relPathID(t, []byte("3.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		run2018, err := Resolve(&comicinfo.Metadata{Series: "Example", Volume: int32Ptr(2018)}, relPathID(t, []byte("4.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		fromFilename, err := Resolve(nil, relPathID(t, []byte("Example v01.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}

		if unspecNil.Series.Key != unspecNeg.Series.Key {
			t.Errorf("nil and negative Volume should share unspecified run key: %q vs %q", unspecNil.Series.Key, unspecNeg.Series.Key)
		}
		if unspecNil.Series.MetadataRun != nil || unspecNeg.Series.MetadataRun != nil {
			t.Errorf("expected nil MetadataRun for absent/negative volume")
		}
		if unspecNil.Series.Key != fromFilename.Series.Key {
			t.Errorf("metadata unspecified run and filename winner should share namespace: %q vs %q", unspecNil.Series.Key, fromFilename.Series.Key)
		}
		if unspecNil.Series.Key == runZero.Series.Key {
			t.Errorf("unspecified run collided with Volume=0: %q", runZero.Series.Key)
		}
		if runZero.Series.Key == run2018.Series.Key {
			t.Errorf("Volume=0 collided with Volume=2018: %q", runZero.Series.Key)
		}
		assertInt32Ptr(t, "runZero.MetadataRun", runZero.Series.MetadataRun, false, 0)
		assertInt32Ptr(t, "run2018.MetadataRun", run2018.Series.MetadataRun, false, 2018)
	})

	t.Run("folder keys use separate namespace, full parent path, NFC equivalence, and raw-byte invalid UTF-8", func(t *testing.T) {
		metaBatman, err := Resolve(&comicinfo.Metadata{Series: "Batman"}, relPathID(t, []byte("a.cbz")), defaultChain)
		if err != nil {
			t.Fatal(err)
		}
		folderBatman, err := Resolve(nil, relPathID(t, []byte("Batman/a.cbz")), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		nestedDC, err := Resolve(nil, relPathID(t, []byte("DC/Batman/a.cbz")), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		nestedMarvel, err := Resolve(nil, relPathID(t, []byte("Marvel/Batman/a.cbz")), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		lowerBatman, err := Resolve(nil, relPathID(t, []byte("batman/a.cbz")), folderOnly)
		if err != nil {
			t.Fatal(err)
		}

		if metaBatman.Series.Key == folderBatman.Series.Key {
			t.Errorf("metadata and folder keys collided: %q", metaBatman.Series.Key)
		}
		if folderBatman.Series.Key == nestedDC.Series.Key {
			t.Errorf("root-child and nested folder collided: %q", folderBatman.Series.Key)
		}
		if nestedDC.Series.Key == nestedMarvel.Series.Key {
			t.Errorf("nested folders with same leaf name collided: %q", nestedDC.Series.Key)
		}
		if nestedDC.Series.Title != "Batman" || nestedMarvel.Series.Title != "Batman" {
			t.Errorf("nested folder display titles = %q / %q, want Batman", nestedDC.Series.Title, nestedMarvel.Series.Title)
		}
		if folderBatman.Series.Key == lowerBatman.Series.Key {
			t.Errorf("POSIX folder case differences collapsed unexpectedly: %q", folderBatman.Series.Key)
		}

		// NFC and NFD parent paths across roots share one folder key.
		nfcParent, err := Resolve(nil, relPathID(t, []byte("Manga/\u00c9lodie/1.cbz")), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		nfdParent, err := Resolve(nil, relPathID(t, []byte("Manga/e\u0301lodie/2.cbz")), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		nfdCapParent, err := Resolve(nil, relPathID(t, []byte("Manga/E\u0301lodie/2.cbz")), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		if nfcParent.Series.Key == nfdParent.Series.Key {
			t.Errorf("case difference in NFC/NFD folder key collapsed: %q", nfcParent.Series.Key)
		}
		if nfcParent.Series.Key != nfdCapParent.Series.Key {
			t.Errorf("NFC and NFD folder keys should match: %q vs %q", nfcParent.Series.Key, nfdCapParent.Series.Key)
		}

		// Invalid-UTF-8 parent paths stay distinct by raw bytes and never collide with valid escaped names.
		invalidFF, err := Resolve(nil, relPathID(t, []byte{'M', 'a', 'n', 'g', 'a', '/', 0xff, '/', '1', '.', 'c', 'b', 'z'}), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		invalidFFAgain, err := Resolve(nil, relPathID(t, []byte{'M', 'a', 'n', 'g', 'a', '/', 0xff, '/', '2', '.', 'c', 'b', 'z'}), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		invalidFE, err := Resolve(nil, relPathID(t, []byte{'M', 'a', 'n', 'g', 'a', '/', 0xfe, '/', '1', '.', 'c', 'b', 'z'}), folderOnly)
		if err != nil {
			t.Fatal(err)
		}
		literalEscaped, err := Resolve(nil, relPathID(t, []byte(`Manga/\xff/1.cbz`)), folderOnly)
		if err != nil {
			t.Fatal(err)
		}

		if invalidFF.Series.Key != invalidFFAgain.Series.Key {
			t.Errorf("identical invalid-UTF-8 parent paths should match: %q vs %q", invalidFF.Series.Key, invalidFFAgain.Series.Key)
		}
		if invalidFF.Series.Key == invalidFE.Series.Key {
			t.Errorf("distinct invalid-UTF-8 parent paths collided: %q", invalidFF.Series.Key)
		}
		if invalidFF.Series.Key == literalEscaped.Series.Key {
			t.Errorf("invalid-UTF-8 parent collided with literal escaped valid UTF-8: %q", invalidFF.Series.Key)
		}
		if invalidFF.Series.Title != `\xff` {
			t.Errorf("invalid-UTF-8 leaf display title = %q, want %q", invalidFF.Series.Title, `\xff`)
		}
	})
}

func TestResolve_ComicInfoNumberParsing(t *testing.T) {
	defaultChain := domain.DefaultResolverChain()

	validCases := []struct {
		raw  string
		want float64
	}{
		{raw: "1", want: 1},
		{raw: "001", want: 1},
		{raw: "1.5", want: 1.5},
		{raw: "-1", want: -1},
		{raw: "+2.25", want: 2.25},
		{raw: "0", want: 0},
		{raw: "-0", want: 0},
		{raw: ".5", want: 0.5},
		{raw: "  42.75\t", want: 42.75},
	}

	for _, tc := range validCases {
		t.Run("valid_"+tc.raw, func(t *testing.T) {
			res, err := Resolve(&comicinfo.Metadata{
				Series: "Example",
				Number: tc.raw,
			}, relPathID(t, []byte("Example v99.cbz")), defaultChain)
			if err != nil {
				t.Fatal(err)
			}
			assertNumberPtr(t, res.Number, false, tc.want)
			if res.NumberSource != domain.ResolverMetadata {
				t.Errorf("NumberSource = %q, want metadata", res.NumberSource)
			}
		})
	}

	unusableCases := []string{
		"",
		"   ",
		"1/2",
		"1-2",
		"v01",
		"#1",
		"One",
		"NaN",
		"Inf",
		"+Inf",
		"-Inf",
		"Infinity",
		"0x10",
		"0x1.fp3",
		"1_000",
		"1e5",
		"1.",
		"+",
		"-",
		strings.Repeat("9", 400),
	}

	for _, raw := range unusableCases {
		name := raw
		if len(name) > 20 {
			name = name[:20] + "..."
		}
		t.Run("unusable_with_fallback_"+name, func(t *testing.T) {
			// With filename fallback available, unusable ComicInfo Number falls back to filename.
			withFallback, err := Resolve(&comicinfo.Metadata{
				Series: "Example",
				Volume: int32Ptr(2018),
				Number: raw,
			}, relPathID(t, []byte("Example v03.cbz")), defaultChain)
			if err != nil {
				t.Fatal(err)
			}
			assertNumberPtr(t, withFallback.Number, false, 3)
			if withFallback.NumberSource != domain.ResolverFilename {
				t.Errorf("NumberSource = %q, want filename", withFallback.NumberSource)
			}
			assertInt32Ptr(t, "MetadataRun", withFallback.Series.MetadataRun, false, 2018)

			// Without filename marker, unusable ComicInfo Number remains nil.
			noFallback, err := Resolve(&comicinfo.Metadata{
				Series: "Example",
				Volume: int32Ptr(2018),
				Number: raw,
			}, relPathID(t, []byte("Example.cbz")), defaultChain)
			if err != nil {
				t.Fatal(err)
			}
			assertNumberPtr(t, noFallback.Number, true, 0)
			if noFallback.NumberSource != "" {
				t.Errorf("NumberSource = %q, want empty", noFallback.NumberSource)
			}
		})
	}
}

func TestResolve_BaselineTitleSort(t *testing.T) {
	tests := []struct {
		name     string
		a        string
		b        string
		wantSame bool
		wantSort string
	}{
		{
			name:     "case and whitespace normalization",
			a:        "  One   Piece  ",
			b:        "ONE\tPIECE",
			wantSame: true,
			wantSort: "one piece",
		},
		{
			name:     "NFC and NFD equivalence with case folding",
			a:        "\u00c9lodie",
			b:        "e\u0301LODIE",
			wantSame: true,
			wantSort: "\u00e9lodie",
		},
		{
			name:     "sharp s case folding",
			a:        "Wei\u00df Kreuz",
			b:        "WEISS KREUZ",
			wantSame: true,
			wantSort: "weiss kreuz",
		},
		{
			name:     "punctuation preserved deterministically",
			a:        "One Piece!",
			b:        "One Piece",
			wantSame: false,
			wantSort: "one piece!",
		},
		{
			name:     "edition words preserved deterministically",
			a:        "Dragon Ball (Kanzenban)",
			b:        "Dragon Ball",
			wantSame: false,
			wantSort: "dragon ball (kanzenban)",
		},
		{
			name:     "invalid UTF-8 escaped folder display produces valid UTF-8 sort key",
			a:        `\xff`,
			b:        `\xff`,
			wantSame: true,
			wantSort: `\xff`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sortA1 := BaselineTitleSort(tc.a)
			sortA2 := BaselineTitleSort(tc.a)
			sortB := BaselineTitleSort(tc.b)
			if sortA1 != sortA2 {
				t.Fatalf("BaselineTitleSort not deterministic: %q vs %q", sortA1, sortA2)
			}
			if sortA1 != tc.wantSort {
				t.Errorf("BaselineTitleSort(%q) = %q, want %q", tc.a, sortA1, tc.wantSort)
			}
			if (sortA1 == sortB) != tc.wantSame {
				t.Errorf("BaselineTitleSort(%q)=%q vs BaselineTitleSort(%q)=%q, wantSame=%v", tc.a, sortA1, tc.b, sortB, tc.wantSame)
			}
		})
	}
}

func TestResolve_ValidationErrors(t *testing.T) {
	validPath := relPathID(t, []byte("Series/Book v01.cbz"))

	invalidPaths := [][]byte{
		nil,
		{},
		[]byte("/abs/Book.cbz"),
		[]byte("../Book.cbz"),
		[]byte("./Book.cbz"),
		[]byte("Series//Book.cbz"),
		[]byte("Series/Book\x00.cbz"),
	}
	for _, raw := range invalidPaths {
		if _, err := Resolve(nil, domain.PathIdentity{Bytes: raw}, domain.DefaultResolverChain()); err == nil {
			t.Errorf("expected error for invalid relPath %q, got nil", raw)
		}
	}

	invalidChains := [][]domain.ResolverName{
		nil,
		{},
		{domain.ResolverName("unknown")},
		{domain.ResolverMetadata, domain.ResolverMetadata},
	}
	for _, chain := range invalidChains {
		if _, err := Resolve(nil, validPath, chain); err == nil {
			t.Errorf("expected error for invalid chain %v, got nil", chain)
		}
	}
}
