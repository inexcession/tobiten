package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/inexcession/tobiten/internal/app"
)

// JobAcceptedResponse is the canonical 202 Accepted payload returned by async job triggers (ADR-0017 §2).
type JobAcceptedResponse struct {
	JobID string `json:"job_id"`
}

func writeJSONResponse(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if r != nil && r.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func writeErrorJSON(w http.ResponseWriter, r *http.Request, status int, msg string) {
	writeJSONResponse(w, r, status, struct {
		Error string `json:"error"`
	}{Error: msg})
}

// WriteJobAccepted writes a 202 Accepted JSON response containing {"job_id": "..."} (ADR-0017 §2).
// It fails closed with 500 Internal Server Error if jobID is blank.
func WriteJobAccepted(w http.ResponseWriter, r *http.Request, jobID string) {
	if strings.TrimSpace(jobID) == "" {
		writeErrorJSON(w, r, http.StatusInternalServerError, "missing job id")
		return
	}
	writeJSONResponse(w, r, http.StatusAccepted, JobAcceptedResponse{JobID: jobID})
}

// WriteAppError maps application-layer sentinel errors to canonical HTTP status responses
// without leaking internal error strings to clients.
func WriteAppError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, app.ErrUnauthorized):
		WriteUnauthorized(w, r)
	case errors.Is(err, app.ErrForbidden):
		WriteForbidden(w, r, "forbidden")
	case errors.Is(err, app.ErrNotFound):
		writeErrorJSON(w, r, http.StatusNotFound, "not found")
	case errors.Is(err, app.ErrConflict):
		writeErrorJSON(w, r, http.StatusConflict, "conflict")
	case errors.Is(err, app.ErrInvalidInput):
		writeErrorJSON(w, r, http.StatusBadRequest, "invalid request")
	case errors.Is(err, app.ErrTooManyRequests):
		writeErrorJSON(w, r, http.StatusTooManyRequests, "too many requests")
	case errors.Is(err, app.ErrServiceBusy):
		writeErrorJSON(w, r, http.StatusServiceUnavailable, "service unavailable")
	case errors.Is(err, app.ErrNotImplemented):
		writeErrorJSON(w, r, http.StatusNotImplemented, "not implemented")
	default:
		writeErrorJSON(w, r, http.StatusInternalServerError, "internal server error")
	}
}

// HandleJobTrigger runs trigger, maps any application error to its HTTP status,
// and returns 202 Accepted with {"job_id": "..."} on success (ADR-0017 §2).
func HandleJobTrigger(w http.ResponseWriter, r *http.Request, trigger func(context.Context) (string, error)) {
	if trigger == nil {
		writeErrorJSON(w, r, http.StatusServiceUnavailable, "service unavailable")
		return
	}
	jobID, err := trigger(r.Context())
	if err != nil {
		WriteAppError(w, r, err)
		return
	}
	WriteJobAccepted(w, r, jobID)
}
