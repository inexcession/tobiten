package comicinfo

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func parse(t *testing.T, s string, mode Mode) (Metadata, []Diagnostic, error) {
	t.Helper()
	return Parse(strings.NewReader(s), mode)
}
func TestFixtures(t *testing.T) {
	for _, file := range []string{"v20.xml", "v21.xml"} {
		for _, mode := range []Mode{Strict, Lenient} {
			t.Run(file+string(rune('0'+mode)), func(t *testing.T) {
				data, err := os.ReadFile("testdata/" + file)
				if err != nil {
					t.Fatal(err)
				}
				got, diags, err := Parse(strings.NewReader("\ufeff"+string(data)), mode)
				if err != nil || len(diags) != 0 {
					t.Fatalf("parse: %v %v", err, diags)
				}
				if file == "v20.xml" {
					if got.Series != "Example" || got.Title != "First" || got.Number != "10.5" || got.Volume == nil || *got.Volume != 0 || got.Count == nil || *got.Count != 20 || got.Day == nil || *got.Day != 29 || got.Manga != "YesAndRightToLeft" || got.AgeRating != "Teen" || !reflect.DeepEqual(got.Writer, []string{"A", "B", "C"}) || !reflect.DeepEqual(got.Characters, []string{"X", "Y"}) || len(got.Pages) != 3 || !reflect.DeepEqual(got.Pages[0].Type, []string{"FrontCover", "Story"}) || got.Pages[1].Type[0] != "Deleted" || got.Pages[2].Type[0] != "Story" {
						t.Fatalf("unexpected metadata: %+v", got)
					}
				} else if got.GTIN != "001234" || got.Number != "001.25" || got.CoverImage != "cover.jpg" || !reflect.DeepEqual(got.Translator, []string{"A", "B"}) || !reflect.DeepEqual(got.Tags, []string{"one", "two"}) {
					t.Fatalf("unexpected metadata: %+v", got)
				}
			})
		}
	}
}
func TestDocumentErrors(t *testing.T) {
	cases := []string{"", "<ComicInfo>", "<ComicInfo><Title>x</ComicInfo>", "<ComicInfo/><ComicInfo/>", "x<ComicInfo/>", "<Other/>", "<ComicInfo xmlns=\"urn:x\"/>", "<!DOCTYPE ComicInfo><ComicInfo/>", "<?xml version=\"1.0\" encoding=\"iso-8859-1\"?><ComicInfo/>", "<ComicInfo>\xff</ComicInfo>", "<?xml nonsense?><ComicInfo/>", "<?xml version=\"1.0\"?><ComicInfo/><?xml version=\"1.0\"?>"}
	for _, s := range cases {
		for _, mode := range []Mode{Strict, Lenient} {
			got, diags, err := parse(t, s, mode)
			if err == nil || !reflect.DeepEqual(got, Metadata{}) || diags != nil {
				t.Errorf("%q mode %v: %+v %v %v", s, mode, got, diags, err)
			}
		}
	}
}
func TestStrictInvalid(t *testing.T) {
	cases := []string{"<series>x</series>", "<Title>a</Title><Title>b</Title>", "<Volume>-2</Volume>", "<Count>2147483648</Count>", "<Month>13</Month>", "<Year>2023</Year><Month>2</Month><Day>29</Day>", "<Manga>yes</Manga>", "<AgeRating>unknown value</AgeRating>", "<Pages><Page Image=\"-1\"/></Pages>", "<Pages><Page Image=\"0\"/><Page Image=\"0\"/></Pages>", "<Pages><Page Image=\"0\" Type=\"FrontCover bad\"/></Pages>", "<Title><b>x</b></Title>"}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			got, diags, err := parse(t, "<ComicInfo>"+s+"</ComicInfo>", Strict)
			if err == nil || !reflect.DeepEqual(got, Metadata{}) || diags != nil {
				t.Fatalf("%+v %v %v", got, diags, err)
			}
		})
	}
}
func TestLenientRecovery(t *testing.T) {
	s := `<comicinfo><volume>bad</volume><Volume>2</Volume><Volume>3</Volume><Manga>yesandrighttoleft</Manga><Year>2023</Year><Month>2</Month><Day>29</Day><Pages><page image="bad"/><Page Image="4" Type="frontcover bad deleted"/><Page Image="4"/><Page Image="5" Type="bad"/></Pages></comicinfo>`
	got, diags, err := parse(t, s, Lenient)
	if err != nil {
		t.Fatal(err)
	}
	if got.Volume == nil || *got.Volume != 2 || got.Manga != "YesAndRightToLeft" || got.Day != nil || len(got.Pages) != 2 || !reflect.DeepEqual(got.Pages[0].Type, []string{"FrontCover", "Deleted"}) || len(got.Pages[1].Type) != 0 || len(diags) != 7 {
		t.Fatalf("%+v diagnostics %+v", got, diags)
	}
}

func TestBoundariesAndDuplicates(t *testing.T) {
	cases := []struct {
		body  string
		valid bool
	}{
		{`<Volume>-1</Volume><Count>2147483647</Count><Year>-1</Year>`, true},
		{`<Month>2</Month><Day>29</Day>`, true},
		{`<Year>1900</Year><Month>2</Month><Day>29</Day>`, false},
		{`<Title/><Title>x</Title>`, false},
		{`<Pages><Page Image="0" Image="1"/></Pages>`, false},
		{`<Pages><Page image="0"/></Pages>`, false},
		{`<Unused><Series>ignored</Series></Unused><Series>ok</Series>`, true},
	}
	for _, tc := range cases {
		_, _, err := parse(t, "<ComicInfo>"+tc.body+"</ComicInfo>", Strict)
		if (err == nil) != tc.valid {
			t.Errorf("%s: %v", tc.body, err)
		}
	}
}
