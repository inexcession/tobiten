package openapi

import (
	"encoding/json"
	"net/http"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

func (m *Module) jobService() app.JobService {
	if m.services == nil {
		return nil
	}
	if m.services.Jobs != nil {
		return m.services.Jobs
	}
	return m.services.Admin
}

func (m *Module) handleGetJob(w http.ResponseWriter, r *http.Request) {
	p, ok := domain.PrincipalFromContext(r.Context())
	if !ok || p == nil {
		middleware.WriteUnauthorized(w, r)
		return
	}

	svc := m.jobService()
	if svc == nil {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}

	jobID := r.PathValue("job_id")
	info, err := svc.GetJob(r.Context(), p, jobID)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(info)
	}
}

func (m *Module) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	p, ok := domain.PrincipalFromContext(r.Context())
	if !ok || p == nil {
		middleware.WriteUnauthorized(w, r)
		return
	}

	svc := m.jobService()
	if svc == nil {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}

	jobID := r.PathValue("job_id")
	info, err := svc.CancelJob(r.Context(), p, jobID)
	if err != nil {
		middleware.WriteAppError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, info)
}
