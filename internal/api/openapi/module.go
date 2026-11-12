package openapi

import (
	"net/http"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/module"
)

// Module implements the native REST API module under /api/v1 (ADR-0005, ADR-0018).
type Module struct {
	services *app.Services
	router   http.Handler
}

// NewModule constructs a new native REST API module wired with application services.
// If services (or services.Admin) is nil, endpoints fail closed / return 503 Service Unavailable.
func NewModule(services *app.Services) *Module {
	m := &Module{
		services: services,
	}
	m.router = m.buildRouter()
	return m
}

// Name returns the canonical module name for the native REST API (ADR-0018 §1).
func (m *Module) Name() string {
	return "openapi"
}

// RouteMounts declares HTTP routes under /api/v1 provided by this module.
func (m *Module) RouteMounts() []module.RouteMount {
	return []module.RouteMount{
		{
			ModuleName: "openapi",
			Prefix:     "/api/v1",
			Handler:    m.router,
		},
	}
}

func (m *Module) buildRouter() http.Handler {
	mux := http.NewServeMux()

	// Auth routes (closed unauthenticated allowlist, ADR-0006 §1, ADR-0014 §1).
	// Only POST is allowlisted; other methods return 405 Method Not Allowed.
	mux.HandleFunc("POST /api/v1/auth/login", m.handleLogin)
	mux.HandleFunc("POST /api/v1/auth/setup", m.handleSetup)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath := r.URL.Path
		if reqPath == "/api/v1/auth/login" || reqPath == "/api/v1/auth/setup" {
			mux.ServeHTTP(w, r)
			return
		}

		p, ok := domain.PrincipalFromContext(r.Context())
		if !ok || p == nil {
			middleware.WriteUnauthorized(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
