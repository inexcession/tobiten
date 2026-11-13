package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
)

// SubPathResolver resolves sub-paths configured in a base URL.
type SubPathResolver interface {
	SubPath() string
	SubPathDecoded() string
}

// OriginResolver resolves the canonical public origin and cookie security attributes
// from the configured TOBITEN_BASE_URL (never from forwarded request headers).
type OriginResolver interface {
	Origin() string
	IsHTTPS() bool
	NativeCookiePath() string
}

type originResolverCtxKey struct{}

// WithOriginResolver attaches an OriginResolver to ctx.
func WithOriginResolver(ctx context.Context, r OriginResolver) context.Context {
	if r == nil {
		return ctx
	}
	return context.WithValue(ctx, originResolverCtxKey{}, r)
}

// OriginResolverFromContext extracts the configured OriginResolver from ctx, if present.
func OriginResolverFromContext(ctx context.Context) OriginResolver {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(originResolverCtxKey{}).(OriginResolver)
	return r
}

// AuthOptions configures the mandatory authentication middleware (ADR-0006 §1, ADR-0018 S1).
type AuthOptions struct {
	Registry        module.Registry
	Authenticators  []module.Authenticator
	SubPathResolver SubPathResolver
	OriginResolver  OriginResolver
	Logger          *slog.Logger
}

type allowlistTarget struct {
	path           string
	allowedMethods []string
}

// Closed allowlist of unauthenticated routes (ADR-0006 §1, ADR-0014 §1).
// Adding a new unauthenticated route requires an ADR amendment.
var closedAllowlist = []allowlistTarget{
	{
		path:           "/healthz",
		allowedMethods: []string{http.MethodGet, http.MethodHead},
	},
	{
		path:           "/api/v1/auth/login",
		allowedMethods: []string{http.MethodPost},
	},
	{
		path:           "/api/v1/auth/setup",
		allowedMethods: []string{http.MethodPost},
	},
}

// WriteUnauthorized writes a standard 401 Unauthorized response with WWW-Authenticate: Basic
// and never an empty 200 (ADR-0006 §1, KOMGA-ADAPTER §8).
func WriteUnauthorized(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", `Basic realm="tobiten"`)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusUnauthorized)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(`{"error":"unauthorized"}` + "\n"))
	}
}

// WriteForbidden writes a 403 Forbidden JSON response without downstream handler execution.
func WriteForbidden(w http.ResponseWriter, r *http.Request, msg string) {
	if msg == "" {
		msg = "forbidden"
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusForbidden)
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{Error: msg})
	}
}

type csrfUnverifiedCtxKey struct{}

// WithCSRFUnverified marks ctx as carrying a cookie-authenticated request whose
// session-bound CSRF token has not been verified (deferred exclusively to genuine
// public login/setup handlers).
func WithCSRFUnverified(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, csrfUnverifiedCtxKey{}, true)
}

// IsCSRFUnverified reports whether ctx carries an unverified cookie CSRF state.
func IsCSRFUnverified(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(csrfUnverifiedCtxKey{}).(bool)
	return v
}

// RequirePrincipal wraps an http.Handler to ensure a verified domain.Principal exists in context
// and that any cookie-authenticated CSRF token has been verified.
func RequirePrincipal(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := domain.PrincipalFromContext(r.Context())
		if !ok || p == nil {
			WriteUnauthorized(w, r)
			return
		}
		if IsCSRFUnverified(r.Context()) {
			WriteForbidden(w, r, "invalid csrf token")
			return
		}
		h.ServeHTTP(w, r)
	})
}

func isMethodAllowed(method string, allowed []string) bool {
	for _, m := range allowed {
		if method == m {
			return true
		}
	}
	return false
}

func writeMethodNotAllowed(w http.ResponseWriter, r *http.Request, allowedMethods []string) {
	w.Header().Set("Allow", strings.Join(allowedMethods, ", "))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusMethodNotAllowed)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(`{"error":"method not allowed"}` + "\n"))
	}
}

func matchAllowlistTarget(reqPath, subDecoded, subEscaped string, allowDecoded bool) (allowlistTarget, bool) {
	for _, target := range closedAllowlist {
		if reqPath == target.path {
			return target, true
		}
		if subEscaped != "" && reqPath == subEscaped+target.path {
			return target, true
		}
		if allowDecoded && subDecoded != "" && reqPath == subDecoded+target.path {
			return target, true
		}
	}
	return allowlistTarget{}, false
}

func matchRequestAllowlist(r *http.Request, subDecoded, subEscaped string) (allowlistTarget, bool) {
	hasEncodedSlash := strings.Contains(strings.ToUpper(subEscaped), "%2F")

	if r.URL.RawPath != "" {
		if target, matched := matchAllowlistTarget(r.URL.RawPath, subDecoded, subEscaped, false); matched {
			return target, true
		}
		if hasEncodedSlash {
			return allowlistTarget{}, false
		}
	}

	return matchAllowlistTarget(r.URL.Path, subDecoded, subEscaped, !hasEncodedSlash)
}

func collectAuthenticators(opts AuthOptions) []module.Authenticator {
	var auths []module.Authenticator
	if opts.Registry != nil {
		auths = append(auths, opts.Registry.Authenticators()...)
	}
	auths = append(auths, opts.Authenticators...)
	return auths
}

var (
	errInvalidExplicitCredential     = errors.New("invalid explicit credential")
	errConflictingExplicitCredential = errors.New("conflicting explicit credentials")
	errUnsupportedExplicitScheme     = errors.New("unsupported authorization scheme")
	errUnhandledExplicitCredential   = errors.New("explicit credential not handled")
)

type explicitSourceKind uint8

const (
	explicitNone explicitSourceKind = iota
	explicitBasic
	explicitPAT
	explicitXAuthToken
)

type explicitClassification struct {
	kind explicitSourceKind
	err  error
}

type explicitClassifierState struct {
	sawBasic      bool
	basicPayloads []string
	basicSyntax   bool

	sawPAT   bool
	firstPAT string

	sawXAuth   bool
	firstXAuth string
}

func recordExplicitToken(val string, first *string, has *bool) error {
	if val == "" {
		return errInvalidExplicitCredential
	}
	if !*has {
		*first = val
		*has = true
		return nil
	}
	if subtle.ConstantTimeCompare([]byte(*first), []byte(val)) != 1 {
		return errConflictingExplicitCredential
	}
	return nil
}

func classifyAuthorizationHeaders(values []string, st *explicitClassifierState) error {
	for _, hdr := range values {
		trimmed := strings.TrimSpace(hdr)
		if trimmed == "" {
			return errInvalidExplicitCredential
		}
		scheme, rest, found := strings.Cut(trimmed, " ")
		val := strings.TrimSpace(rest)

		switch {
		case strings.EqualFold(scheme, "Basic"):
			st.sawBasic = true
			if !found || val == "" {
				st.basicSyntax = true
				continue
			}
			st.basicPayloads = append(st.basicPayloads, val)
		case strings.EqualFold(scheme, "Bearer"):
			if !found {
				st.sawPAT = true
				return errInvalidExplicitCredential
			}
			if err := recordExplicitToken(val, &st.firstPAT, &st.sawPAT); err != nil {
				return err
			}
		default:
			return errUnsupportedExplicitScheme
		}
	}
	return nil
}

func validateRepeatedBasic(payloads []string, hasSyntaxError bool) error {
	if len(payloads) <= 1 && (len(payloads) == 0 || !hasSyntaxError) {
		return nil
	}
	if hasSyntaxError {
		return errInvalidExplicitCredential
	}
	var firstUser, firstPass string
	for i, payload := range payloads {
		decoded, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return errInvalidExplicitCredential
		}
		user, pass, ok := strings.Cut(string(decoded), ":")
		if !ok || user == "" || pass == "" || strings.ContainsRune(user, 0) || strings.ContainsRune(pass, 0) {
			return errInvalidExplicitCredential
		}
		if i == 0 {
			firstUser = user
			firstPass = pass
			continue
		}
		if subtle.ConstantTimeCompare([]byte(firstUser), []byte(user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(firstPass), []byte(pass)) != 1 {
			return errConflictingExplicitCredential
		}
	}
	return nil
}

func classifyExplicitRequest(r *http.Request) explicitClassification {
	var st explicitClassifierState

	if err := classifyAuthorizationHeaders(r.Header.Values("Authorization"), &st); err != nil {
		return explicitClassification{err: err}
	}
	for _, hdr := range r.Header.Values("X-API-Key") {
		if err := recordExplicitToken(strings.TrimSpace(hdr), &st.firstPAT, &st.sawPAT); err != nil {
			return explicitClassification{err: err}
		}
	}
	for _, hdr := range r.Header.Values("X-Auth-Token") {
		if err := recordExplicitToken(strings.TrimSpace(hdr), &st.firstXAuth, &st.sawXAuth); err != nil {
			return explicitClassification{err: err}
		}
	}

	kinds := 0
	var kind explicitSourceKind
	if st.sawBasic {
		kinds++
		kind = explicitBasic
	}
	if st.sawPAT {
		kinds++
		kind = explicitPAT
	}
	if st.sawXAuth {
		kinds++
		kind = explicitXAuthToken
	}

	if kinds > 1 {
		return explicitClassification{err: errConflictingExplicitCredential}
	}
	if st.sawBasic {
		if err := validateRepeatedBasic(st.basicPayloads, st.basicSyntax); err != nil {
			return explicitClassification{kind: explicitBasic, err: err}
		}
	}

	return explicitClassification{kind: kind}
}

type partitionedAuthenticators struct {
	explicit    []module.Authenticator
	proxy       []module.Authenticator
	cookie      []module.Authenticator
	unspecified []module.Authenticator
}

func partitionAuthenticators(auths []module.Authenticator) partitionedAuthenticators {
	var p partitionedAuthenticators
	for _, a := range auths {
		if a == nil {
			continue
		}
		tier := module.CredentialTierUnspecified
		if tiered, ok := a.(module.TieredAuthenticator); ok {
			tier = tiered.Tier()
		}
		switch tier {
		case module.CredentialTierExplicit:
			p.explicit = append(p.explicit, a)
		case module.CredentialTierTrustedProxy:
			p.proxy = append(p.proxy, a)
		case module.CredentialTierCookie:
			p.cookie = append(p.cookie, a)
		default:
			p.unspecified = append(p.unspecified, a)
		}
	}
	return p
}

func sameCredentialPrincipal(a, b *domain.Principal, requireCredID bool) bool {
	if a == nil || b == nil {
		return false
	}
	if requireCredID && (a.CredentialID == "" || b.CredentialID == "") {
		return false
	}
	if a.User.ID != b.User.ID || a.CredentialID != b.CredentialID || a.CredentialKind != b.CredentialKind {
		return false
	}
	return slices.Equal(a.Scopes, b.Scopes) && slices.Equal(a.Permissions, b.Permissions)
}

type authOutcome struct {
	res  module.AuthResult
	tier module.CredentialTier
	err  error
}

func authenticateTier(r *http.Request, auths []module.Authenticator, requireCredID bool) (module.AuthResult, error) {
	var matched module.AuthResult
	for _, a := range auths {
		res, err := a.Authenticate(r)
		if err != nil {
			return module.AuthResult{}, err
		}
		if !res.Handled {
			continue
		}
		if res.Principal == nil {
			return module.AuthResult{}, errors.New("handled without principal")
		}
		if !matched.Handled {
			matched = res
			continue
		}
		if !sameCredentialPrincipal(matched.Principal, res.Principal, requireCredID) ||
			matched.RequiresCSRF != res.RequiresCSRF ||
			matched.CSRFTokenHash != res.CSRFTokenHash {
			return module.AuthResult{}, errConflictingExplicitCredential
		}
	}
	return matched, nil
}

func authenticateChainOutcome(r *http.Request, auths []module.Authenticator) authOutcome {
	explicit := classifyExplicitRequest(r)
	if explicit.err != nil {
		return authOutcome{tier: module.CredentialTierExplicit, err: explicit.err}
	}

	tiers := partitionAuthenticators(auths)

	// Explicit credentials (X-API-Key, Bearer, Basic, X-Auth-Token) take strict precedence
	// over trusted-proxy, cookie, and unspecified providers and must never fall back (ADR-0006 §3).
	if explicit.kind != explicitNone {
		res, err := authenticateTier(r, tiers.explicit, true)
		if err != nil {
			return authOutcome{tier: module.CredentialTierExplicit, err: err}
		}
		if res.Handled {
			return authOutcome{res: res, tier: module.CredentialTierExplicit}
		}
		return authOutcome{tier: module.CredentialTierExplicit, err: errUnhandledExplicitCredential}
	}

	for _, tier := range []struct {
		kind          module.CredentialTier
		auths         []module.Authenticator
		requireCredID bool
	}{
		{kind: module.CredentialTierExplicit, auths: tiers.explicit, requireCredID: true},
		{kind: module.CredentialTierTrustedProxy, auths: tiers.proxy, requireCredID: true},
		{kind: module.CredentialTierCookie, auths: tiers.cookie, requireCredID: true},
		{kind: module.CredentialTierUnspecified, auths: tiers.unspecified, requireCredID: false},
	} {
		res, err := authenticateTier(r, tier.auths, tier.requireCredID)
		if err != nil {
			return authOutcome{tier: tier.kind, err: err}
		}
		if res.Handled {
			return authOutcome{res: res, tier: tier.kind}
		}
	}

	return authOutcome{}
}

// CanonicalOrigin normalizes an http/https scheme and host[:port] to its canonical origin
// string ("scheme://host" or "scheme://host:port"), stripping default ports :80 and :443.
func CanonicalOrigin(scheme, host string) string {
	s := strings.ToLower(strings.TrimSpace(scheme))
	if s != "http" && s != "https" {
		return ""
	}
	h := strings.TrimSpace(host)
	if h == "" {
		return ""
	}
	hostPart, port, err := net.SplitHostPort(h)
	if err != nil {
		// No port component; verify host has no stray port delimiter outside IPv6 brackets.
		if strings.HasPrefix(h, "[") && strings.HasSuffix(h, "]") {
			return s + "://" + strings.ToLower(h)
		}
		if strings.Contains(h, ":") {
			return ""
		}
		return s + "://" + strings.ToLower(h)
	}
	hostPart = strings.ToLower(hostPart)
	if hostPart == "" || port == "" {
		return ""
	}
	if (s == "http" && port == "80") || (s == "https" && port == "443") {
		if strings.Contains(hostPart, ":") {
			return s + "://[" + hostPart + "]"
		}
		return s + "://" + hostPart
	}
	return s + "://" + net.JoinHostPort(hostPart, port)
}

func isSafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}

func validateBrowserOriginAndFetchMetadata(r *http.Request, expectedOrigin string) bool {
	secFetch := r.Header.Values("Sec-Fetch-Site")
	origins := r.Header.Values("Origin")
	referers := r.Header.Values("Referer")

	// Allow non-browser clients that omit all browser-origin/fetch-metadata headers.
	if len(secFetch) == 0 && len(origins) == 0 && len(referers) == 0 {
		return true
	}
	if expectedOrigin == "" {
		return false
	}

	if len(secFetch) > 0 {
		if len(secFetch) != 1 || !strings.EqualFold(strings.TrimSpace(secFetch[0]), "same-origin") {
			return false
		}
	}

	if len(origins) > 0 {
		if len(origins) != 1 {
			return false
		}
		raw := strings.TrimSpace(origins[0])
		if raw == "" || strings.EqualFold(raw, "null") {
			return false
		}
		u, err := url.Parse(raw)
		if err != nil || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return false
		}
		if CanonicalOrigin(u.Scheme, u.Host) != expectedOrigin {
			return false
		}
	}

	if len(referers) > 0 {
		if len(referers) != 1 {
			return false
		}
		raw := strings.TrimSpace(referers[0])
		if raw == "" {
			return false
		}
		u, err := url.Parse(raw)
		if err != nil || u.User != nil {
			return false
		}
		if CanonicalOrigin(u.Scheme, u.Host) != expectedOrigin {
			return false
		}
	}

	return true
}

func verifySessionCSRF(r *http.Request, expectedHash string) bool {
	if expectedHash == "" {
		return false
	}
	vals := r.Header.Values("X-CSRF-Token")
	if len(vals) != 1 || vals[0] == "" {
		return false
	}
	sum := sha256.Sum256([]byte(vals[0]))
	got := hex.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(got), []byte(expectedHash)) == 1
}

func publicAllowlistOwnerActive(reg module.Registry, targetPath string) bool {
	if reg == nil {
		return true
	}
	switch targetPath {
	case "/healthz":
		return reg.Module("system") != nil && reg.IsEnabled("system")
	case "/api/v1/auth/login", "/api/v1/auth/setup":
		return reg.Module("openapi") != nil && reg.IsEnabled("openapi")
	default:
		return false
	}
}

func enforceAuthenticatedMutationPolicy(
	w http.ResponseWriter,
	r *http.Request,
	expectedOrigin string,
	res module.AuthResult,
	deferPublicCSRF bool,
) (ok bool, csrfUnverified bool) {
	if isSafeMethod(r.Method) {
		return true, false
	}
	if !validateBrowserOriginAndFetchMetadata(r, expectedOrigin) {
		WriteForbidden(w, r, "forbidden")
		return false, false
	}
	if res.RequiresCSRF {
		if !verifySessionCSRF(r, res.CSRFTokenHash) {
			if deferPublicCSRF {
				return true, true
			}
			WriteForbidden(w, r, "invalid csrf token")
			return false, false
		}
	}
	return true, false
}

// Auth returns HTTP middleware that enforces Tobiten's mandatory authentication contract
// (ADR-0006 §1, ADR-0018 S1).
//
// All requests outside the closed unauthenticated allowlist must resolve a verified
// domain.Principal through the authenticator chain before reaching downstream handlers.
// Unauthenticated or invalid requests receive 401 Unauthorized with WWW-Authenticate: Basic
// and never an empty 200.
func Auth(opts AuthOptions) func(http.Handler) http.Handler {
	var subDecoded, subEscaped, expectedOrigin string
	if opts.SubPathResolver != nil {
		subDecoded = opts.SubPathResolver.SubPathDecoded()
		subEscaped = opts.SubPathResolver.SubPath()
	}
	if opts.OriginResolver != nil {
		expectedOrigin = opts.OriginResolver.Origin()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if opts.OriginResolver != nil {
				r = r.WithContext(WithOriginResolver(r.Context(), opts.OriginResolver))
			}

			target, matched := matchRequestAllowlist(r, subDecoded, subEscaped)
			methodAllowed := matched && isMethodAllowed(r.Method, target.allowedMethods)
			ownerActive := matched && publicAllowlistOwnerActive(opts.Registry, target.path)

			outcome := authenticateChainOutcome(r, collectAuthenticators(opts))
			if outcome.err != nil {
				// Allow stale/expired session cookies on active allowlisted routes (e.g. POST /api/v1/auth/login
				// or GET /healthz) so users can always re-authenticate, while still blocking invalid
				// explicit credentials and blocking invalid cookies on all protected routes.
				if matched && ownerActive && outcome.tier == module.CredentialTierCookie {
					outcome = authOutcome{}
				} else {
					WriteUnauthorized(w, r)
					return
				}
			}

			if outcome.res.Handled && outcome.res.Principal != nil {
				ok, csrfUnverified := enforceAuthenticatedMutationPolicy(
					w,
					r,
					expectedOrigin,
					outcome.res,
					methodAllowed && ownerActive,
				)
				if !ok {
					return
				}
				ctx := domain.ContextWithPrincipal(r.Context(), outcome.res.Principal)
				if csrfUnverified {
					ctx = WithCSRFUnverified(ctx)
				}
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			if matched {
				if !methodAllowed {
					writeMethodNotAllowed(w, r, target.allowedMethods)
					return
				}
				if !isSafeMethod(r.Method) && !validateBrowserOriginAndFetchMetadata(r, expectedOrigin) {
					WriteForbidden(w, r, "forbidden")
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			WriteUnauthorized(w, r)
		})
	}
}
