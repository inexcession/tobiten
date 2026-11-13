package middleware

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
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

// AuthOptions configures the mandatory authentication middleware (ADR-0006 §1, ADR-0018 S1).
type AuthOptions struct {
	Registry        module.Registry
	Authenticators  []module.Authenticator
	SubPathResolver SubPathResolver
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

// RequirePrincipal wraps an http.Handler to ensure a verified domain.Principal exists in context.
// Unauthenticated requests receive 401 Unauthorized with WWW-Authenticate: Basic and never an empty 200.
func RequirePrincipal(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := domain.PrincipalFromContext(r.Context())
		if !ok || p == nil {
			WriteUnauthorized(w, r)
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

func authenticateTier(r *http.Request, auths []module.Authenticator, requireCredID bool) (*domain.Principal, bool, error) {
	var matched *domain.Principal
	for _, a := range auths {
		res, err := a.Authenticate(r)
		if err != nil {
			return nil, false, err
		}
		if !res.Handled {
			continue
		}
		if res.Principal == nil {
			return nil, false, errors.New("handled without principal")
		}
		if matched == nil {
			matched = res.Principal
			continue
		}
		if !sameCredentialPrincipal(matched, res.Principal, requireCredID) {
			return nil, false, errConflictingExplicitCredential
		}
	}
	if matched != nil {
		return matched, true, nil
	}
	return nil, false, nil
}

func authenticateChain(r *http.Request, auths []module.Authenticator) (*domain.Principal, bool, error) {
	explicit := classifyExplicitRequest(r)
	if explicit.err != nil {
		return nil, false, explicit.err
	}

	tiers := partitionAuthenticators(auths)

	// Explicit credentials (X-API-Key, Bearer, Basic, X-Auth-Token) take strict precedence
	// over trusted-proxy and cookie tiers and must never fall back (ADR-0006 §3).
	if explicit.kind != explicitNone {
		if p, handled, err := authenticateTier(r, tiers.explicit, true); err != nil || handled {
			return p, handled, err
		}
		if p, handled, err := authenticateTier(r, tiers.unspecified, false); err != nil || handled {
			return p, handled, err
		}
		return nil, false, errUnhandledExplicitCredential
	}

	for _, tier := range []struct {
		auths         []module.Authenticator
		requireCredID bool
	}{
		{auths: tiers.explicit, requireCredID: true},
		{auths: tiers.proxy, requireCredID: true},
		{auths: tiers.cookie, requireCredID: true},
		{auths: tiers.unspecified, requireCredID: false},
	} {
		if p, handled, err := authenticateTier(r, tier.auths, tier.requireCredID); err != nil || handled {
			return p, handled, err
		}
	}

	return nil, false, nil
}

// Auth returns HTTP middleware that enforces Tobiten's mandatory authentication contract
// (ADR-0006 §1, ADR-0018 S1).
//
// All requests outside the closed unauthenticated allowlist must resolve a verified
// domain.Principal through the authenticator chain before reaching downstream handlers.
// Unauthenticated or invalid requests receive 401 Unauthorized with WWW-Authenticate: Basic
// and never an empty 200.
func Auth(opts AuthOptions) func(http.Handler) http.Handler {
	var subDecoded, subEscaped string
	if opts.SubPathResolver != nil {
		subDecoded = opts.SubPathResolver.SubPathDecoded()
		subEscaped = opts.SubPathResolver.SubPath()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auths := collectAuthenticators(opts)
			principal, ok, err := authenticateChain(r, auths)
			if err != nil {
				WriteUnauthorized(w, r)
				return
			}
			if ok && principal != nil {
				ctx := domain.ContextWithPrincipal(r.Context(), principal)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			target, matched := matchRequestAllowlist(r, subDecoded, subEscaped)
			if matched {
				if isMethodAllowed(r.Method, target.allowedMethods) {
					next.ServeHTTP(w, r)
					return
				}
				writeMethodNotAllowed(w, r, target.allowedMethods)
				return
			}

			WriteUnauthorized(w, r)
		})
	}
}
