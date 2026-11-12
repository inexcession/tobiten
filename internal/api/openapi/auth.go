package openapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/inexcession/tobiten/internal/app"
)

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	User domainUserResponse `json:"user"`
}

type domainUserResponse struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

type setupResponse struct {
	User    domainUserResponse `json:"user"`
	Message string             `json:"message"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

func (m *Module) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if m.services == nil || m.services.Admin == nil {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}

	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password are required")
		return
	}

	principal, err := m.services.Admin.AuthenticateUser(r.Context(), req.Username, req.Password)
	if err != nil {
		if errors.Is(err, app.ErrUnauthorized) {
			writeError(w, http.StatusUnauthorized, "invalid username or password")
			return
		}
		writeError(w, http.StatusUnauthorized, "authentication failed")
		return
	}

	writeJSON(w, http.StatusOK, loginResponse{
		User: domainUserResponse{
			ID:       principal.User.ID,
			Username: principal.User.Username,
		},
	})
}

func (m *Module) handleSetup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	service := m.setupService()
	if service == nil {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}

	// Check if setup is already sealed (permanent seal: users > 0, ADR-0014 §1)
	sealed, err := service.IsSetupSealed(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to check setup status")
		return
	}
	if sealed {
		writeError(w, http.StatusForbidden, "first-run setup is permanently sealed")
		return
	}

	var req app.SetupAdminRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password are required")
		return
	}

	user, err := service.SetupInitialAdmin(r.Context(), req)
	if err != nil {
		if errors.Is(err, app.ErrSetupSealed) {
			writeError(w, http.StatusForbidden, "first-run setup is permanently sealed")
			return
		}
		if errors.Is(err, app.ErrInvalidInput) {
			writeError(w, http.StatusBadRequest, "invalid setup parameters")
			return
		}
		if errors.Is(err, app.ErrUnauthorized) {
			writeError(w, http.StatusUnauthorized, "invalid setup token")
			return
		}
		writeError(w, http.StatusInternalServerError, "setup failed")
		return
	}

	writeJSON(w, http.StatusCreated, setupResponse{
		User: domainUserResponse{
			ID:       user.ID,
			Username: user.Username,
		},
		Message: "initial admin created",
	})
}

// Existing AdminService implementations include setup; composition can wire bootstrap
// independently while account authentication and management remain unavailable.
func (m *Module) setupService() app.SetupService {
	if m.services == nil {
		return nil
	}
	if m.services.Setup != nil {
		return m.services.Setup
	}
	return m.services.Admin
}
