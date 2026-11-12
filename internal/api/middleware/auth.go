package middleware

import (
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

func matchAllowlistTarget(reqPath, subDecoded, subEscaped string) (allowlistTarget, bool) {
	for _, target := range closedAllowlist {
		if reqPath == target.path {
			return target, true
		}
		if subDecoded != "" && reqPath == subDecoded+target.path {
			return target, true
		}
		if subEscaped != "" && subEscaped != subDecoded && reqPath == subEscaped+target.path {
			return target, true
		}
	}
	return allowlistTarget{}, false
}

func matchRequestAllowlist(r *http.Request, subDecoded, subEscaped string) (allowlistTarget, bool) {
	target, matched := matchAllowlistTarget(r.URL.Path, subDecoded, subEscaped)
	if !matched && r.URL.RawPath != "" {
		target, matched = matchAllowlistTarget(r.URL.RawPath, subDecoded, subEscaped)
	}
	return target, matched
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

func writeUnauthorized(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("WWW-Authenticate", `Basic realm="tobiten"`)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusUnauthorized)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(`{"error":"unauthorized"}` + "\n"))
	}
}

func collectAuthenticators(opts AuthOptions) []module.Authenticator {
	var auths []module.Authenticator
	if opts.Registry != nil {
		auths = append(auths, opts.Registry.Authenticators()...)
	}
	auths = append(auths, opts.Authenticators...)
	return auths
}

func authenticateChain(r *http.Request, auths []module.Authenticator) (*domain.Principal, bool) {
	for _, a := range auths {
		res, err := a.Authenticate(r)
		if err != nil {
			return nil, false
		}
		if res.Handled {
			if res.Principal != nil {
				return res.Principal, true
			}
			return nil, false
		}
	}
	return nil, false
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
			target, matched := matchRequestAllowlist(r, subDecoded, subEscaped)
			if matched {
				if isMethodAllowed(r.Method, target.allowedMethods) {
					next.ServeHTTP(w, r)
					return
				}
				writeMethodNotAllowed(w, r, target.allowedMethods)
				return
			}

			auths := collectAuthenticators(opts)
			principal, ok := authenticateChain(r, auths)
			if ok && principal != nil {
				ctx := domain.ContextWithPrincipal(r.Context(), principal)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			writeUnauthorized(w, r)
		})
	}
}
