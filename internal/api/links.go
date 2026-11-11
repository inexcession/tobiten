package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/inexcession/tobiten/internal/config"
)

// ErrForeignURL is returned when an attempt is made to resolve an absolute URL
// with a foreign host or scheme against the configured base URL.
var ErrForeignURL = errors.New("cannot resolve absolute URL with foreign host or scheme against base URL")

// URLBuilder builds absolute URLs based exclusively on the resolved TOBITEN_BASE_URL.
// It preserves any configured sub-path and guarantees that forwarded headers
// (e.g. X-Forwarded-Host, X-Forwarded-Proto, X-Forwarded-For, X-Forwarded-Prefix, Forwarded)
// or request Host values are never consulted for public link generation
// (ADR-0014 §2, ADR-0020 §6).
type URLBuilder struct {
	base       *url.URL
	rawBaseURL string // normalized without trailing slash, e.g. "https://example.com/tobiten"
	subPath    string // sub-path component only, e.g. "/tobiten" or ""
}

// LinkBuilder is an alias for URLBuilder for consistency across architecture and spec references.
type LinkBuilder = URLBuilder

// NewURLBuilder creates a new URLBuilder from a raw base URL string.
// The base URL must be an absolute URL with http or https scheme, non-empty host,
// and must not contain user credentials, query parameters, or fragments.
func NewURLBuilder(rawBaseURL string) (*URLBuilder, error) {
	trimmed := strings.TrimSpace(rawBaseURL)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: base URL cannot be empty", config.ErrInvalidBaseURL)
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid URL format: %v", config.ErrInvalidBaseURL, err)
	}

	displayURL := parsed.Redacted()
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("%w: %q (scheme must be http or https, and host must not be empty)", config.ErrInvalidBaseURL, displayURL)
	}

	if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return nil, fmt.Errorf("%w: %q (base URL must not contain query parameters, fragment, or user credentials)", config.ErrInvalidBaseURL, displayURL)
	}

	// Normalize sub-path: remove trailing slashes.
	cleanPath := strings.TrimRight(parsed.Path, "/")
	cleanRawPath := strings.TrimRight(parsed.RawPath, "/")

	baseURLStr := fmt.Sprintf("%s://%s%s", parsed.Scheme, parsed.Host, cleanPath)

	canonicalURL := &url.URL{
		Scheme:  parsed.Scheme,
		Host:    parsed.Host,
		Path:    cleanPath,
		RawPath: cleanRawPath,
	}

	return &URLBuilder{
		base:       canonicalURL,
		rawBaseURL: baseURLStr,
		subPath:    cleanPath,
	}, nil
}

// MustNewURLBuilder calls NewURLBuilder and panics on error.
func MustNewURLBuilder(rawBaseURL string) *URLBuilder {
	ub, err := NewURLBuilder(rawBaseURL)
	if err != nil {
		panic(err)
	}
	return ub
}

// NewURLBuilderFromConfig creates a URLBuilder from the BaseURL field of a Config.
func NewURLBuilderFromConfig(cfg *config.Config) (*URLBuilder, error) {
	if cfg == nil {
		return nil, errors.New("config is required")
	}
	return NewURLBuilder(cfg.BaseURL)
}

// BaseURL returns the normalized public base URL without a trailing slash.
func (ub *URLBuilder) BaseURL() string {
	return ub.rawBaseURL
}

// SubPath returns the sub-path component of the base URL, or empty string if root.
// For example, for "https://example.com/tobiten", it returns "/tobiten".
// For "http://localhost:8080", it returns "".
func (ub *URLBuilder) SubPath() string {
	return ub.subPath
}

// Path returns the path with the base URL's sub-path prepended.
// Useful for constructing cookie paths or internal mount routes that must preserve sub-paths.
func (ub *URLBuilder) Path(ref string) string {
	if ref == "" {
		return ub.subPath
	}
	// If ref already starts with subPath, avoid duplicating it.
	if ub.subPath != "" {
		if ref == ub.subPath {
			return ub.subPath
		}
		if strings.HasPrefix(ref, ub.subPath+"/") {
			return ref
		}
	}
	cleanRef := strings.TrimLeft(ref, "/")
	if ub.subPath == "" {
		if strings.HasPrefix(ref, "/") {
			return "/" + cleanRef
		}
		return cleanRef
	}
	if ref == "/" {
		return ub.subPath + "/"
	}
	return ub.subPath + "/" + cleanRef
}

// Parse resolves ref against the base URL, preserving the base URL's sub-path.
// If ref contains a foreign host or scheme, ErrForeignURL is returned.
func (ub *URLBuilder) Parse(ref string) (*url.URL, error) {
	if ref == "" {
		u := *ub.base
		return &u, nil
	}

	parsed, err := url.Parse(ref)
	if err != nil {
		return nil, fmt.Errorf("invalid path or reference %q: %w", ref, err)
	}

	// Reject foreign schemes or hosts
	if (parsed.Host != "" && parsed.Host != ub.base.Host) ||
		(parsed.Scheme != "" && parsed.Scheme != ub.base.Scheme) {
		return nil, fmt.Errorf("%w: %q", ErrForeignURL, ref)
	}

	refPath := parsed.Path
	refRawPath := parsed.RawPath

	// If ref already starts with subPath, strip it so joining does not duplicate it.
	if ub.subPath != "" {
		if refPath == ub.subPath {
			refPath = ""
		} else if strings.HasPrefix(refPath, ub.subPath+"/") {
			refPath = strings.TrimPrefix(refPath, ub.subPath)
		}
		if refRawPath != "" {
			subRaw := ub.base.RawPath
			if subRaw == "" {
				subRaw = ub.subPath
			}
			if refRawPath == subRaw {
				refRawPath = ""
			} else if strings.HasPrefix(refRawPath, subRaw+"/") {
				refRawPath = strings.TrimPrefix(refRawPath, subRaw)
			}
		}
	}

	var joinedPath, joinedRawPath string

	switch refPath {
	case "":
		joinedPath = ub.subPath
		joinedRawPath = ub.base.RawPath
	case "/":
		if ub.subPath == "" {
			joinedPath = "/"
			joinedRawPath = "/"
		} else {
			joinedPath = ub.subPath + "/"
			if ub.base.RawPath != "" {
				joinedRawPath = ub.base.RawPath + "/"
			}
		}
	default:
		cleanRef := strings.TrimLeft(refPath, "/")
		if ub.subPath == "" {
			joinedPath = "/" + cleanRef
		} else {
			joinedPath = ub.subPath + "/" + cleanRef
		}

		if refRawPath != "" || ub.base.RawPath != "" {
			rRaw := refRawPath
			if rRaw == "" {
				rRaw = refPath
			}
			bRaw := ub.base.RawPath
			if bRaw == "" {
				bRaw = ub.subPath
			}
			cleanRawRef := strings.TrimLeft(rRaw, "/")
			if bRaw == "" {
				joinedRawPath = "/" + cleanRawRef
			} else {
				joinedRawPath = bRaw + "/" + cleanRawRef
			}
		}
	}

	result := &url.URL{
		Scheme:   ub.base.Scheme,
		Host:     ub.base.Host,
		Path:     joinedPath,
		RawPath:  joinedRawPath,
		RawQuery: parsed.RawQuery,
		Fragment: parsed.Fragment,
	}

	return result, nil
}

// URL resolves ref against the base URL, preserving the base URL's sub-path,
// and returns the absolute URL string. If ref cannot be parsed or contains a
// foreign host, it returns an empty string.
func (ub *URLBuilder) URL(ref string) string {
	u, err := ub.Parse(ref)
	if err != nil {
		return ""
	}
	return u.String()
}

// URLWithQuery resolves ref against the base URL and appends encoded query parameters.
func (ub *URLBuilder) URLWithQuery(ref string, query url.Values) string {
	u, err := ub.Parse(ref)
	if err != nil {
		return ""
	}
	if len(query) > 0 {
		encoded := query.Encode()
		if u.RawQuery != "" {
			u.RawQuery = u.RawQuery + "&" + encoded
		} else {
			u.RawQuery = encoded
		}
	}
	return u.String()
}

// URLFromRequest builds an absolute URL on the configured TOBITEN_BASE_URL.
// It accepts *http.Request for handler convenience, but deliberately and strictly
// ignores any forwarded headers (such as X-Forwarded-Host, X-Forwarded-Proto,
// X-Forwarded-For, X-Forwarded-Port, X-Forwarded-Prefix, Forwarded) or r.Host.
// Per ADR-0014 §2 and ADR-0020 §6, Tobiten never consults forwarded headers to build URLs.
func (ub *URLBuilder) URLFromRequest(r *http.Request, ref string) string {
	// Forwarded headers and request Host are intentionally ignored.
	return ub.URL(ref)
}

type contextKey struct{}

var urlBuilderContextKey = contextKey{}

// WithURLBuilder returns a context with ub attached.
func WithURLBuilder(ctx context.Context, ub *URLBuilder) context.Context {
	return context.WithValue(ctx, urlBuilderContextKey, ub)
}

// URLBuilderFromContext retrieves the URLBuilder from ctx, or nil if absent.
func URLBuilderFromContext(ctx context.Context) *URLBuilder {
	if ctx == nil {
		return nil
	}
	if ub, ok := ctx.Value(urlBuilderContextKey).(*URLBuilder); ok {
		return ub
	}
	return nil
}

// URLBuilderFromRequest retrieves the URLBuilder from r's context, or nil if absent.
func URLBuilderFromRequest(r *http.Request) *URLBuilder {
	if r == nil {
		return nil
	}
	return URLBuilderFromContext(r.Context())
}

// URLFromRequest resolves ref using the URLBuilder stored in r's context.
// Like URLBuilder.URLFromRequest, forwarded headers and request Host are never consulted.
// If r is nil or has no URLBuilder in context, it returns an empty string.
func URLFromRequest(r *http.Request, ref string) string {
	ub := URLBuilderFromRequest(r)
	if ub == nil {
		return ""
	}
	return ub.URL(ref)
}

// URLBuilderMiddleware attaches ub to every request's context.
func URLBuilderMiddleware(ub *URLBuilder) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := WithURLBuilder(r.Context(), ub)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
