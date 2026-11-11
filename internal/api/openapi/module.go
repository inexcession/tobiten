package openapi

import (
	"net/http"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/module"
)

// Module implements the native REST API module under /api/v1 (ADR-0005, ADR-0018).
type Module struct {
	services *app.Services
	router   http.Handler
}

// NewModule constructs a new native REST API module wired with application services.
func NewModule(services *app.Services) *Module {
	if services == nil {
		services = app.NewStubServices()
	}
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

	// Auth routes (closed unauthenticated allowlist, ADR-0006 §1, ADR-0014 §1)
	mux.HandleFunc("POST /api/v1/auth/login", m.handleLogin)
	mux.HandleFunc("POST /api/v1/auth/setup", m.handleSetup)
	mux.HandleFunc("GET /api/v1/auth/setup", m.handleSetupStatus)
	mux.HandleFunc("HEAD /api/v1/auth/setup", m.handleSetupStatus)

	return mux
}
