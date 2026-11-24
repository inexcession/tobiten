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

	// Job detail and cancellation routes (ADR-0017 §2, decision I-10).
	mux.HandleFunc("GET /api/v1/jobs/{job_id}", m.handleGetJob)
	mux.HandleFunc("POST /api/v1/jobs/{job_id}/cancel", m.handleCancelJob)

	// Shared authenticated SSE event stream (ADR-0013 §3, ADR-0018 S3).
	mux.HandleFunc("GET /api/v1/events", m.handleEvents)

	// Native library and catalogue read routes (M1-T21.4, ADR-0019, ADR-0020).
	mux.HandleFunc("GET /api/v1/libraries", m.handleListLibraries)
	mux.HandleFunc("GET /api/v1/libraries/{id}", m.handleGetLibrary)
	mux.HandleFunc("GET /api/v1/libraries/{id}/folders", m.handleBrowseLibraryFolder)
	mux.HandleFunc("GET /api/v1/series", m.handleListSeries)
	mux.HandleFunc("GET /api/v1/series/{id}", m.handleGetSeries)
	mux.HandleFunc("GET /api/v1/series/{id}/thumbnail", m.handleGetSeriesThumbnail)
	mux.HandleFunc("GET /api/v1/volumes", m.handleListVolumes)
	mux.HandleFunc("GET /api/v1/volumes/{id}", m.handleGetVolume)
	mux.HandleFunc("GET /api/v1/volumes/{id}/thumbnail", m.handleGetVolumeThumbnail)

	// Native library provisioning, scan, and root management routes (M1-T21.5, M1-T10.7, M1-T22.5, ADR-0017, ADR-0019).
	mux.HandleFunc("POST /api/v1/libraries", m.handleCreateLibrary)
	mux.HandleFunc("POST /api/v1/libraries/{id}/roots", m.handleAddLibraryRoot)
	mux.HandleFunc("DELETE /api/v1/libraries/{id}/roots/{root_id}", m.handleRemoveLibraryRoot)
	mux.HandleFunc("POST /api/v1/libraries/{id}/scan", m.handleScanLibrary)
	mux.HandleFunc("POST /api/v1/admin/roots/{id}/relocate", m.handleRelocateRoot)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath := r.URL.Path
		if reqPath == "/api/v1/auth/login" || reqPath == "/api/v1/auth/setup" {
			if r.Method == http.MethodPost {
				mux.ServeHTTP(w, r)
				return
			}
			// Only POST handlers are registered on /auth/login and /auth/setup; let ServeMux
			// answer 405 for safe methods (e.g. direct module GET/HEAD calls) while failing closed
			// on any other unsafe method without a verified Principal and CSRF check.
			if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
				mux.ServeHTTP(w, r)
				return
			}
		}

		p, ok := domain.PrincipalFromContext(r.Context())
		if !ok || p == nil {
			middleware.WriteUnauthorized(w, r)
			return
		}
		if middleware.IsCSRFUnverified(r.Context()) {
			middleware.WriteForbidden(w, r, "invalid csrf token")
			return
		}
		mux.ServeHTTP(w, r)
	})
}
