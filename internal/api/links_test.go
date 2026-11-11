package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/inexcession/tobiten/internal/config"
)

func TestNewURLBuilder_Validation(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{
			name:    "valid localhost default",
			raw:     "http://localhost:8080",
			wantErr: false,
		},
		{
			name:    "valid https domain",
			raw:     "https://books.example.com",
			wantErr: false,
		},
		{
			name:    "valid https sub-path",
			raw:     "https://example.com/tobiten",
			wantErr: false,
		},
		{
			name:    "valid https sub-path with trailing slash",
			raw:     "https://example.com/tobiten/",
			wantErr: false,
		},
		{
			name:    "valid ip and port with multi-segment sub-path",
			raw:     "http://192.168.1.100:8080/manga/library",
			wantErr: false,
		},
		{
			name:    "empty string",
			raw:     "",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			raw:     "   ",
			wantErr: true,
		},
		{
			name:    "missing scheme",
			raw:     "localhost:8080",
			wantErr: true,
		},
		{
			name:    "invalid scheme ftp",
			raw:     "ftp://example.com/tobiten",
			wantErr: true,
		},
		{
			name:    "missing host",
			raw:     "http:///tobiten",
			wantErr: true,
		},
		{
			name:    "contains query parameter",
			raw:     "http://localhost:8080?foo=bar",
			wantErr: true,
		},
		{
			name:    "contains fragment",
			raw:     "http://localhost:8080#section",
			wantErr: true,
		},
		{
			name:    "contains user credentials",
			raw:     "http://admin:pass@localhost:8080",
			wantErr: true,
		},
		{
			name:    "malformed url",
			raw:     "://invalid-url",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ub, err := NewURLBuilder(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewURLBuilder(%q) succeeded, expected error", tt.raw)
				}
				if !errors.Is(err, config.ErrInvalidBaseURL) {
					t.Fatalf("NewURLBuilder(%q) error = %v, want error wrapping ErrInvalidBaseURL", tt.raw, err)
				}
			} else {
				if err != nil {
					t.Fatalf("NewURLBuilder(%q) unexpected error: %v", tt.raw, err)
				}
				if ub == nil {
					t.Fatalf("NewURLBuilder(%q) returned nil builder", tt.raw)
				}
			}
		})
	}
}

func TestMustNewURLBuilder(t *testing.T) {
	t.Run("valid does not panic", func(t *testing.T) {
		ub := MustNewURLBuilder("http://localhost:8080")
		if ub.BaseURL() != "http://localhost:8080" {
			t.Errorf("got %q, want http://localhost:8080", ub.BaseURL())
		}
	})

	t.Run("invalid panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected MustNewURLBuilder to panic for invalid URL")
			}
		}()
		_ = MustNewURLBuilder("invalid-url")
	})
}

func TestNewURLBuilderFromConfig(t *testing.T) {
	t.Run("nil config returns error", func(t *testing.T) {
		_, err := NewURLBuilderFromConfig(nil)
		if err == nil {
			t.Errorf("expected error for nil config")
		}
	})

	t.Run("valid config succeeds", func(t *testing.T) {
		cfg := &config.Config{BaseURL: "https://example.com/books"}
		ub, err := NewURLBuilderFromConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ub.BaseURL() != "https://example.com/books" {
			t.Errorf("got %q, want https://example.com/books", ub.BaseURL())
		}
	})
}

func TestURLBuilder_RootBase(t *testing.T) {
	ub := MustNewURLBuilder("http://localhost:8080")

	if got := ub.BaseURL(); got != "http://localhost:8080" {
		t.Errorf("BaseURL() = %q, want %q", got, "http://localhost:8080")
	}
	if got := ub.SubPath(); got != "" {
		t.Errorf("SubPath() = %q, want empty string", got)
	}
	if got := ub.Path("/komga"); got != "/komga" {
		t.Errorf("Path(/komga) = %q, want /komga", got)
	}
	if got := ub.Path(""); got != "" {
		t.Errorf("Path(\"\") = %q, want empty string", got)
	}
	if got := ub.Path("/"); got != "/" {
		t.Errorf("Path(/) = %q, want /", got)
	}

	tests := []struct {
		ref  string
		want string
	}{
		{ref: "", want: "http://localhost:8080"},
		{ref: "/", want: "http://localhost:8080/"},
		{ref: "/api/v1/auth/setup", want: "http://localhost:8080/api/v1/auth/setup"},
		{ref: "api/v1/auth/setup", want: "http://localhost:8080/api/v1/auth/setup"},
		{ref: "/komga", want: "http://localhost:8080/komga"},
		{ref: "/komga/", want: "http://localhost:8080/komga/"},
		{ref: "/komga/opds/v1.2/catalog", want: "http://localhost:8080/komga/opds/v1.2/catalog"},
		{ref: "/komga/api/v1/series?page=0&size=20", want: "http://localhost:8080/komga/api/v1/series?page=0&size=20"},
		{ref: "/docs#header", want: "http://localhost:8080/docs#header"},
		{ref: "/books/1/file/Chapter%201.cbz", want: "http://localhost:8080/books/1/file/Chapter%201.cbz"},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := ub.URL(tt.ref)
			if got != tt.want {
				t.Errorf("URL(%q) = %q, want %q", tt.ref, got, tt.want)
			}
		})
	}
}

func TestURLBuilder_SubPathBase(t *testing.T) {
	ub := MustNewURLBuilder("https://example.com/tobiten")

	if got := ub.BaseURL(); got != "https://example.com/tobiten" {
		t.Errorf("BaseURL() = %q, want %q", got, "https://example.com/tobiten")
	}
	if got := ub.SubPath(); got != "/tobiten" {
		t.Errorf("SubPath() = %q, want /tobiten", got)
	}
	if got := ub.Path("/komga"); got != "/tobiten/komga" {
		t.Errorf("Path(/komga) = %q, want /tobiten/komga", got)
	}
	if got := ub.Path("komga"); got != "/tobiten/komga" {
		t.Errorf("Path(komga) = %q, want /tobiten/komga", got)
	}
	if got := ub.Path(""); got != "/tobiten" {
		t.Errorf("Path(\"\") = %q, want /tobiten", got)
	}
	if got := ub.Path("/"); got != "/tobiten/" {
		t.Errorf("Path(/) = %q, want /tobiten/", got)
	}
	if got := ub.Path("/tobiten/komga"); got != "/tobiten/tobiten/komga" {
		t.Errorf("Path(/tobiten/komga) = %q, want /tobiten/tobiten/komga", got)
	}

	tests := []struct {
		ref  string
		want string
	}{
		{ref: "", want: "https://example.com/tobiten"},
		{ref: "/", want: "https://example.com/tobiten/"},
		{ref: "/api/v1/auth/setup", want: "https://example.com/tobiten/api/v1/auth/setup"},
		{ref: "api/v1/auth/setup", want: "https://example.com/tobiten/api/v1/auth/setup"},
		{ref: "/komga", want: "https://example.com/tobiten/komga"},
		{ref: "/komga/", want: "https://example.com/tobiten/komga/"},
		{ref: "/komga/opds/v1.2/catalog", want: "https://example.com/tobiten/komga/opds/v1.2/catalog"},
		{ref: "/komga/api/v1/series?page=0&size=20", want: "https://example.com/tobiten/komga/api/v1/series?page=0&size=20"},
		{ref: "/docs#header", want: "https://example.com/tobiten/docs#header"},
		{ref: "/books/1/file/Chapter%201.cbz", want: "https://example.com/tobiten/books/1/file/Chapter%201.cbz"},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := ub.URL(tt.ref)
			if got != tt.want {
				t.Errorf("URL(%q) = %q, want %q", tt.ref, got, tt.want)
			}
		})
	}

	// Verify multi-segment sub-path with trailing slash normalized
	ubMulti := MustNewURLBuilder("https://books.example.com:8443/manga/library/")
	if got := ubMulti.BaseURL(); got != "https://books.example.com:8443/manga/library" {
		t.Errorf("BaseURL() = %q, want https://books.example.com:8443/manga/library", got)
	}
	if got := ubMulti.SubPath(); got != "/manga/library" {
		t.Errorf("SubPath() = %q, want /manga/library", got)
	}
	if got := ubMulti.URL("/api/v1/healthz"); got != "https://books.example.com:8443/manga/library/api/v1/healthz" {
		t.Errorf("URL(/api/v1/healthz) = %q, want https://books.example.com:8443/manga/library/api/v1/healthz", got)
	}
}

func TestURLBuilder_URLWithQuery(t *testing.T) {
	ub := MustNewURLBuilder("https://example.com/tobiten")

	t.Run("empty query", func(t *testing.T) {
		got := ub.URLWithQuery("/api/v1/books", nil)
		want := "https://example.com/tobiten/api/v1/books"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("query added to path without existing query", func(t *testing.T) {
		q := url.Values{}
		q.Set("page", "1")
		q.Set("size", "20")
		got := ub.URLWithQuery("/api/v1/books", q)
		want := "https://example.com/tobiten/api/v1/books?page=1&size=20"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("query appended to path with existing query", func(t *testing.T) {
		q := url.Values{}
		q.Set("sort", "name")
		got := ub.URLWithQuery("/api/v1/books?filter=all", q)
		want := "https://example.com/tobiten/api/v1/books?filter=all&sort=name"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}

func TestURLBuilder_ForeignHostRejected(t *testing.T) {
	ub := MustNewURLBuilder("https://example.com/tobiten")

	foreignCases := []string{
		"http://evil.com/phish",
		"https://evil.com/phish",
		"//evil.com/phish",
		"ftp://evil.com/phish",
	}

	for _, fc := range foreignCases {
		t.Run(fc, func(t *testing.T) {
			_, err := ub.Parse(fc)
			if err == nil {
				t.Fatalf("Parse(%q) succeeded, expected error", fc)
			}
			if !errors.Is(err, ErrForeignURL) {
				t.Fatalf("Parse(%q) error = %v, want ErrForeignURL", fc, err)
			}
			if got := ub.URL(fc); got != "" {
				t.Errorf("URL(%q) = %q, want empty string", fc, got)
			}
		})
	}
}

func TestURLBuilder_URLFromRequest_ForwardedHeadersIgnored(t *testing.T) {
	baseConfigs := []struct {
		name    string
		baseURL string
		subPath string
	}{
		{
			name:    "root base http",
			baseURL: "http://localhost:8080",
			subPath: "",
		},
		{
			name:    "root base https",
			baseURL: "https://books.example.com",
			subPath: "",
		},
		{
			name:    "sub-path base https",
			baseURL: "https://example.com/tobiten",
			subPath: "/tobiten",
		},
		{
			name:    "multi-segment sub-path base https",
			baseURL: "https://media.internal:8443/apps/tobiten",
			subPath: "/apps/tobiten",
		},
	}

	pathsToTest := []struct {
		ref          string
		expectedPath string
	}{
		{
			ref:          "/api/v1/auth/setup",
			expectedPath: "/api/v1/auth/setup",
		},
		{
			ref:          "/komga",
			expectedPath: "/komga",
		},
		{
			ref:          "/komga/opds/v1.2/catalog",
			expectedPath: "/komga/opds/v1.2/catalog",
		},
		{
			ref:          "/komga/api/v1/series?page=0&size=20",
			expectedPath: "/komga/api/v1/series?page=0&size=20",
		},
	}

	for _, bc := range baseConfigs {
		bc := bc
		t.Run(bc.name, func(t *testing.T) {
			ub := MustNewURLBuilder(bc.baseURL)

			for _, pt := range pathsToTest {
				pt := pt
				t.Run(pt.ref, func(t *testing.T) {
					// Construct incoming request with hostile/forwarded headers
					req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:55555/dummy", nil)
					req.Host = "internal-docker-host:8080"
					req.Header.Set("X-Forwarded-Host", "attacker.evil.com")
					req.Header.Set("X-Forwarded-Proto", "https")
					req.Header.Set("X-Forwarded-For", "203.0.113.195, 70.41.3.18")
					req.Header.Set("X-Forwarded-Port", "9999")
					req.Header.Set("X-Forwarded-Prefix", "/spoofed-prefix")
					req.Header.Set("Forwarded", "for=192.0.2.60;proto=https;by=203.0.113.43;host=forwarded.example.org")

					// Call ub.URLFromRequest
					link1 := ub.URLFromRequest(req, pt.ref)

					// Call package-level URLFromRequest with context
					ctxReq := req.WithContext(WithURLBuilder(req.Context(), ub))
					link2 := URLFromRequest(ctxReq, pt.ref)

					expectedFullURL := bc.baseURL + pt.expectedPath

					if link1 != expectedFullURL {
						t.Errorf("ub.URLFromRequest produced %q, want %q (forwarded headers were consulted!)", link1, expectedFullURL)
					}
					if link2 != expectedFullURL {
						t.Errorf("URLFromRequest produced %q, want %q (forwarded headers were consulted!)", link2, expectedFullURL)
					}
				})
			}
		})
	}
}

func TestURLBuilder_ContextAndMiddleware(t *testing.T) {
	ub := MustNewURLBuilder("https://example.com/tobiten")

	t.Run("nil request URLFromRequest returns empty", func(t *testing.T) {
		if got := URLFromRequest(nil, "/api/v1"); got != "" {
			t.Errorf("got %q, want empty string", got)
		}
	})

	t.Run("request without builder returns empty", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		if got := URLFromRequest(req, "/api/v1"); got != "" {
			t.Errorf("got %q, want empty string", got)
		}
	})

	t.Run("middleware attaches builder to context", func(t *testing.T) {
		var capturedLink string
		handler := URLBuilderMiddleware(ub)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedLink = URLFromRequest(r, "/api/v1/auth/setup")
			w.WriteHeader(http.StatusOK)
		}))

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Forwarded-Host", "evil.com")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("handler returned status %d, want 200", rec.Code)
		}
		want := "https://example.com/tobiten/api/v1/auth/setup"
		if capturedLink != want {
			t.Errorf("capturedLink = %q, want %q", capturedLink, want)
		}
	})
}

func TestURLBuilder_PrefixMatching_PreservesBoth(t *testing.T) {
	t.Run("base url /komga with /komga routes preserves both", func(t *testing.T) {
		ub := MustNewURLBuilder("https://example.com/komga")

		cases := []struct {
			ref      string
			wantURL  string
			wantPath string
		}{
			{
				ref:      "/komga",
				wantURL:  "https://example.com/komga/komga",
				wantPath: "/komga/komga",
			},
			{
				ref:      "/komga/",
				wantURL:  "https://example.com/komga/komga/",
				wantPath: "/komga/komga/",
			},
			{
				ref:      "/komga/opds/v1.2/catalog",
				wantURL:  "https://example.com/komga/komga/opds/v1.2/catalog",
				wantPath: "/komga/komga/opds/v1.2/catalog",
			},
			{
				ref:      "/komga/api/v1/series",
				wantURL:  "https://example.com/komga/komga/api/v1/series",
				wantPath: "/komga/komga/api/v1/series",
			},
			{
				ref:      "/api/v1/auth/setup",
				wantURL:  "https://example.com/komga/api/v1/auth/setup",
				wantPath: "/komga/api/v1/auth/setup",
			},
		}

		for _, tc := range cases {
			if got := ub.URL(tc.ref); got != tc.wantURL {
				t.Errorf("URL(%q) = %q, want %q", tc.ref, got, tc.wantURL)
			}
			if got := ub.Path(tc.ref); got != tc.wantPath {
				t.Errorf("Path(%q) = %q, want %q", tc.ref, got, tc.wantPath)
			}
		}
	})

	t.Run("base url /api with /api routes preserves both", func(t *testing.T) {
		ub := MustNewURLBuilder("https://example.com/api")

		cases := []struct {
			ref      string
			wantURL  string
			wantPath string
		}{
			{
				ref:      "/api",
				wantURL:  "https://example.com/api/api",
				wantPath: "/api/api",
			},
			{
				ref:      "/api/",
				wantURL:  "https://example.com/api/api/",
				wantPath: "/api/api/",
			},
			{
				ref:      "/api/v1/auth/setup",
				wantURL:  "https://example.com/api/api/v1/auth/setup",
				wantPath: "/api/api/v1/auth/setup",
			},
			{
				ref:      "/komga/opds/v1.2/catalog",
				wantURL:  "https://example.com/api/komga/opds/v1.2/catalog",
				wantPath: "/api/komga/opds/v1.2/catalog",
			},
		}

		for _, tc := range cases {
			if got := ub.URL(tc.ref); got != tc.wantURL {
				t.Errorf("URL(%q) = %q, want %q", tc.ref, got, tc.wantURL)
			}
			if got := ub.Path(tc.ref); got != tc.wantPath {
				t.Errorf("Path(%q) = %q, want %q", tc.ref, got, tc.wantPath)
			}
		}
	})
}

func TestURLBuilder_URLEscapingIntact(t *testing.T) {
	testCases := []struct {
		name        string
		rawBase     string
		wantBaseURL string
		wantSubPath string
		ref         string
		wantURL     string
		wantPath    string
	}{
		{
			name:        "space escaped as %20",
			rawBase:     "https://example.com/sub%20path",
			wantBaseURL: "https://example.com/sub%20path",
			wantSubPath: "/sub%20path",
			ref:         "/komga",
			wantURL:     "https://example.com/sub%20path/komga",
			wantPath:    "/sub%20path/komga",
		},
		{
			name:        "question mark escaped as %3F",
			rawBase:     "https://example.com/foo%3Fbar",
			wantBaseURL: "https://example.com/foo%3Fbar",
			wantSubPath: "/foo%3Fbar",
			ref:         "/komga",
			wantURL:     "https://example.com/foo%3Fbar/komga",
			wantPath:    "/foo%3Fbar/komga",
		},
		{
			name:        "hash escaped as %23",
			rawBase:     "https://example.com/baz%23qux",
			wantBaseURL: "https://example.com/baz%23qux",
			wantSubPath: "/baz%23qux",
			ref:         "/komga",
			wantURL:     "https://example.com/baz%23qux/komga",
			wantPath:    "/baz%23qux/komga",
		},
		{
			name:        "combined %20, %3F, %23 with trailing slash normalized",
			rawBase:     "https://example.com/sub%20path/foo%3Fbar/baz%23qux/",
			wantBaseURL: "https://example.com/sub%20path/foo%3Fbar/baz%23qux",
			wantSubPath: "/sub%20path/foo%3Fbar/baz%23qux",
			ref:         "/komga/opds/v1.2/catalog",
			wantURL:     "https://example.com/sub%20path/foo%3Fbar/baz%23qux/komga/opds/v1.2/catalog",
			wantPath:    "/sub%20path/foo%3Fbar/baz%23qux/komga/opds/v1.2/catalog",
		},
		{
			name:        "base ending with %2F keeps encoding",
			rawBase:     "https://example.com/books%2F",
			wantBaseURL: "https://example.com/books%2F",
			wantSubPath: "/books%2F",
			ref:         "/komga",
			wantURL:     "https://example.com/books%2F/komga",
			wantPath:    "/books%2F/komga",
		},
		{
			name:        "base ending with %2F/ normalizes literal trailing slash and keeps %2F",
			rawBase:     "https://example.com/books%2F/",
			wantBaseURL: "https://example.com/books%2F",
			wantSubPath: "/books%2F",
			ref:         "/komga",
			wantURL:     "https://example.com/books%2F/komga",
			wantPath:    "/books%2F/komga",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ub := MustNewURLBuilder(tc.rawBase)
			if got := ub.BaseURL(); got != tc.wantBaseURL {
				t.Errorf("BaseURL() = %q, want %q", got, tc.wantBaseURL)
			}
			if got := ub.SubPath(); got != tc.wantSubPath {
				t.Errorf("SubPath() = %q, want %q", got, tc.wantSubPath)
			}
			if got := ub.URL(tc.ref); got != tc.wantURL {
				t.Errorf("URL(%q) = %q, want %q", tc.ref, got, tc.wantURL)
			}
			if got := ub.Path(tc.ref); got != tc.wantPath {
				t.Errorf("Path(%q) = %q, want %q", tc.ref, got, tc.wantPath)
			}
		})
	}
}

func TestURLBuilder_Percent2F_Regressions(t *testing.T) {
	bases := []struct {
		name    string
		rawBase string
	}{
		{
			name:    "ending in %2F",
			rawBase: "https://example.com/books%2F",
		},
		{
			name:    "ending in %2F/",
			rawBase: "https://example.com/books%2F/",
		},
		{
			name:    "ending in %2F/// multiple trailing slashes",
			rawBase: "https://example.com/books%2F///",
		},
	}

	for _, b := range bases {
		t.Run(b.name, func(t *testing.T) {
			ub := MustNewURLBuilder(b.rawBase)

			if got := ub.BaseURL(); got != "https://example.com/books%2F" {
				t.Errorf("BaseURL() = %q, want https://example.com/books%%2F", got)
			}
			if got := ub.SubPath(); got != "/books%2F" {
				t.Errorf("SubPath() = %q, want /books%%2F", got)
			}

			// URL() checks
			urlCases := []struct {
				ref  string
				want string
			}{
				{ref: "", want: "https://example.com/books%2F"},
				{ref: "/", want: "https://example.com/books%2F/"},
				{ref: "/komga", want: "https://example.com/books%2F/komga"},
				{ref: "komga", want: "https://example.com/books%2F/komga"},
				{ref: "/komga/", want: "https://example.com/books%2F/komga/"},
				{ref: "/komga/opds/v1.2/catalog", want: "https://example.com/books%2F/komga/opds/v1.2/catalog"},
				{ref: "/api/v1/auth/setup", want: "https://example.com/books%2F/api/v1/auth/setup"},
			}
			for _, c := range urlCases {
				if got := ub.URL(c.ref); got != c.want {
					t.Errorf("URL(%q) = %q, want %q", c.ref, got, c.want)
				}
			}

			// Path() checks
			pathCases := []struct {
				ref  string
				want string
			}{
				{ref: "", want: "/books%2F"},
				{ref: "/", want: "/books%2F/"},
				{ref: "/komga", want: "/books%2F/komga"},
				{ref: "komga", want: "/books%2F/komga"},
				{ref: "/komga/", want: "/books%2F/komga/"},
				{ref: "/komga/opds/v1.2/catalog", want: "/books%2F/komga/opds/v1.2/catalog"},
				{ref: "/api/v1/auth/setup", want: "/books%2F/api/v1/auth/setup"},
			}
			for _, c := range pathCases {
				if got := ub.Path(c.ref); got != c.want {
					t.Errorf("Path(%q) = %q, want %q", c.ref, got, c.want)
				}
			}
		})
	}
}
