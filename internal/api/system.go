package api

import (
	"net/http"

	"github.com/inexcession/tobiten/internal/module"
)

// SystemModule provides core daemon liveness routes (/healthz) through the module registry (ADR-0018 §1).
type SystemModule struct{}

// NewSystemModule constructs a new SystemModule.
func NewSystemModule() *SystemModule {
	return &SystemModule{}
}

func (m *SystemModule) Name() string {
	return "system"
}

func (m *SystemModule) RouteMounts() []module.RouteMount {
	return []module.RouteMount{
		{
			ModuleName: "system",
			Prefix:     "/healthz",
			Handler:    HealthzHandler(),
		},
	}
}

// HealthzHandler returns the unauthenticated GET/HEAD /healthz liveness handler.
// It returns 200 OK indicating liveness only, with no version or details (ADR-0006 §1).
func HealthzHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("method not allowed\n"))
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte("ok\n"))
		}
	})
}
