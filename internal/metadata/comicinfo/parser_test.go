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
	if got.Volume == nil || *got.Volume != 2 || got.Manga != "YesAndRightToLeft" || got.Day != nil || len(got.Pages) != 2 || !reflect.DeepEqual(got.Pages[0].Type, []string{"FrontCover", "Deleted"}) || len(got.Pages[1].Type) != 0 || len(diags) != 8 {
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

func TestDocumentStructure(t *testing.T) {
	fixtures := []struct {
		name  string
		valid bool
	}{
		{"document-valid.xml", true}, {"document-invalid-prefix.xml", false}, {"document-invalid-outside.xml", false},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + fixture.name)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []Mode{Strict, Lenient} {
				got, diags, err := parse(t, string(data), mode)
				if fixture.valid {
					if err != nil || len(diags) != 0 || got.Title != "Retained" {
						t.Fatalf("mode %v: %+v %v %v", mode, got, diags, err)
					}
				} else if err == nil || !reflect.DeepEqual(got, Metadata{}) || diags != nil {
					t.Fatalf("mode %v: %+v %v %v", mode, got, diags, err)
				}
			}
		})
	}
}

func TestDocumentSyntaxCases(t *testing.T) {
	cases := []struct {
		name, doc string
		valid     bool
	}{
		{"literal whitespace and comments", " \n<!-- hi --><ComicInfo/>\r\n<?end ok?>", true},
		{"declared in place", `<ComicInfo><x:extension xmlns:x="urn:x" x:key="v"/></ComicInfo>`, true},
		{"nested default namespace", `<ComicInfo><Unused xmlns="urn:x"><Any/></Unused><Title>ok</Title></ComicInfo>`, true},
		{"character reference before", `&#32;<ComicInfo/>`, false},
		{"character reference after", `<ComicInfo/>&#x20;`, false},
		{"CDATA after", `<ComicInfo/><![CDATA[ ]]>`, false},
		{"empty CDATA after", `<ComicInfo/><![CDATA[]]>`, false},
		{"undeclared attribute", `<ComicInfo><Extra u:a="x"/></ComicInfo>`, false},
		{"scope ends", `<ComicInfo><Extra xmlns:x="urn:x"/><x:Extra/></ComicInfo>`, false},
		{"prefix cleared", `<ComicInfo xmlns:x="urn:x"><Extra xmlns:x=""><x:E/></Extra></ComicInfo>`, false},
		{"reserved prefix", `<ComicInfo xmlns:xml="urn:wrong"/>`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []Mode{Strict, Lenient} {
				got, diags, err := parse(t, tc.doc, mode)
				if tc.valid {
					if err != nil {
						t.Fatalf("mode %v: %v", mode, err)
					}
				} else if err == nil || !reflect.DeepEqual(got, Metadata{}) || diags != nil {
					t.Fatalf("mode %v: %+v %v %v", mode, got, diags, err)
				}
			}
		})
	}
}

func TestLenientDuplicateAfterInvalid(t *testing.T) {
	got, diags, err := parse(t, `<ComicInfo><Volume>invalid</Volume><Volume>2</Volume></ComicInfo>`, Lenient)
	if err != nil || got.Volume == nil || *got.Volume != 2 {
		t.Fatalf("metadata %+v: %v", got, err)
	}
	want := []Diagnostic{{Field: "Volume", Reason: "invalid integer"}, {Field: "Volume", Reason: "duplicate singleton"}}
	if !reflect.DeepEqual(diags, want) {
		t.Fatalf("diagnostics: got %+v, want %+v", diags, want)
	}
}

func TestStartTagAndQualifiedNames(t *testing.T) {
	cases := []struct {
		name, doc string
		valid     bool
	}{
		{"adjacent root attributes", `<ComicInfo a="x"b="y"><Title>retained</Title></ComicInfo>`, false},
		{"adjacent nested attributes", `<ComicInfo><Title>retained</Title><Extension a='x'b='y'/></ComicInfo>`, false},
		{"adjacent namespace declarations", `<ComicInfo xmlns:x="urn:x"xmlns:y="urn:y"><Title>retained</Title></ComicInfo>`, false},
		{"invalid local element", `<ComicInfo xmlns:x="urn:x"><Title>retained</Title><x:1bad/></ComicInfo>`, false},
		{"invalid local attribute", `<ComicInfo xmlns:x="urn:x"><Title>retained</Title><Other x:1bad="x"/></ComicInfo>`, false},
		{"invalid namespace name", `<ComicInfo xmlns:1bad="urn:x"><Title>retained</Title></ComicInfo>`, false},
		{"invalid prefix", `<ComicInfo xmlns:1x="urn:x"><Title>retained</Title><1x:ext/></ComicInfo>`, false},
		{"valid declared unicode and separators", "<ComicInfo xmlns:画=\"urn:x\"\n a = 'x>y'\t画:題=\"'&quot;\"><Title>retained</Title><画:頁 画:番号='1'/></ComicInfo>", true},
		{"valid prefixed name characters", `<ComicInfo xmlns:x="urn:x"><Title>retained</Title><x:_a-1.b x:a1="2" /></ComicInfo>`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []Mode{Strict, Lenient} {
				got, diagnostics, err := parse(t, tc.doc, mode)
				if tc.valid {
					if err != nil || got.Title != "retained" || len(diagnostics) != 0 {
						t.Fatalf("mode %v: %+v %v %v", mode, got, diagnostics, err)
					}
				} else if err == nil || !reflect.DeepEqual(got, Metadata{}) || diagnostics != nil {
					t.Fatalf("mode %v: %+v %v %v", mode, got, diagnostics, err)
				}
			}
		})
	}
}

func TestCharactersAndProcessingInstructions(t *testing.T) {
	cases := []struct {
		name, doc, wantTitle string
		valid                bool
	}{
		{"hex surrogate in title", `<ComicInfo><Title>&#xD800;</Title></ComicInfo>`, "", false},
		{"hex low surrogate with leading zeros", `<ComicInfo><Title>&#x00DFFF;</Title></ComicInfo>`, "", false},
		{"decimal surrogate in title", `<ComicInfo><Title>&#55296;</Title></ComicInfo>`, "", false},
		{"surrogate in attribute", `<ComicInfo note="&#xD800;"><Title>retained</Title></ComicInfo>`, "", false},
		{"noncharacter ref", `<ComicInfo><Title>&#xFFFE;</Title></ComicInfo>`, "", false},
		{"out of range ref", `<ComicInfo><Title>&#x110000;</Title></ComicInfo>`, "", false},
		{"nul in comment", "<!-- \x00 --><ComicInfo><Title>retained</Title></ComicInfo>", "", false},
		{"vertical tab in comment", "<!-- \x0b --><ComicInfo><Title>retained</Title></ComicInfo>", "", false},
		{"control char in comment", "<!-- \x1f --><ComicInfo><Title>retained</Title></ComicInfo>", "", false},
		{"noncharacter in comment", "<!-- \ufffe --><ComicInfo><Title>retained</Title></ComicInfo>", "", false},
		{"control char in PI", "<?tool \x0c?><ComicInfo><Title>retained</Title></ComicInfo>", "", false},
		{"PI missing separator", `<?tool!data?><ComicInfo><Title>retained</Title></ComicInfo>`, "", false},
		{"PI equals without separator", `<?tool=1?><ComicInfo><Title>retained</Title></ComicInfo>`, "", false},
		{"xmlversion without separator", `<?xmlversion="1.0"?><ComicInfo><Title>retained</Title></ComicInfo>`, "", false},
		{"PI target with colon", `<?ns:tool ok?><ComicInfo><Title>retained</Title></ComicInfo>`, "", false},
		{
			"valid U+FFFD, legal refs, literal ref-like text, and valid PIs",
			"<?tool?><?tool   ?><?tool &#xD800; &#0; ?>\n<!-- &#xD800; &#0; \ufffd -->\n<ComicInfo note=\"\ufffd&#xFFFD;\"><Title>\ufffd&#xFFFD;&#65533;&#xD7FF;&#xE000;&#x10FFFF;&#9;&amp;#xD800;<![CDATA[&#xD800;&#0;\ufffd]]></Title></ComicInfo>",
			"\ufffd\ufffd\ufffd\uD7FF\uE000\U0010FFFF\t&#xD800;&#xD800;&#0;\ufffd",
			true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []Mode{Strict, Lenient} {
				got, diagnostics, err := parse(t, tc.doc, mode)
				if tc.valid {
					if err != nil || got.Title != tc.wantTitle || len(diagnostics) != 0 {
						t.Fatalf("mode %v: %+v %v %v", mode, got, diagnostics, err)
					}
				} else if err == nil || !reflect.DeepEqual(got, Metadata{}) || diagnostics != nil {
					t.Fatalf("mode %v: %+v %v %v", mode, got, diagnostics, err)
				}
			}
		})
	}
}
