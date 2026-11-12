package middleware

import (
	"errors"
	"log/slog"
	"net/http"
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

func authenticateChain(r *http.Request, auths []module.Authenticator) (*domain.Principal, bool, error) {
	for _, a := range auths {
		res, err := a.Authenticate(r)
		if err != nil {
			return nil, false, err
		}
		if res.Handled {
			if res.Principal != nil {
				return res.Principal, true, nil
			}
			return nil, false, errors.New("handled without principal")
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
