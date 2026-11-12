package openapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

type failingSetup struct{ checkErr, createErr error }

func (s failingSetup) IsSetupSealed(context.Context) (bool, error) { return false, s.checkErr }
func (s failingSetup) SetupInitialAdmin(context.Context, app.SetupAdminRequest) (*domain.User, error) {
	return nil, s.createErr
}

func TestSetupPublicErrors(t *testing.T) {
	private := errors.New("postgres://operator:secret@internal:5432/database SQL private detail")
	for _, tc := range []struct {
		name    string
		service app.SetupService
		status  int
		message string
	}{
		{"check failure", failingSetup{checkErr: private}, 500, "failed to check setup status"},
		{"create failure", failingSetup{createErr: private}, 500, "setup failed"},
		{"invalid token", failingSetup{createErr: app.ErrUnauthorized}, 401, "invalid setup token"},
		{"bad input", failingSetup{createErr: app.ErrInvalidInput}, 400, "invalid setup parameters"},
		{"concurrent seal", failingSetup{createErr: app.ErrSetupSealed}, 403, "first-run setup is permanently sealed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := NewModule(&app.Services{Setup: tc.service}).RouteMounts()[0].Handler
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/setup", strings.NewReader(`{"setup_token":"secret-token","username":"alice","password":"secret-password"}`))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.status || strings.TrimSpace(rec.Body.String()) != `{"error":"`+tc.message+`"}` {
				t.Fatalf("response=%d %s", rec.Code, rec.Body.String())
			}
		})
	}
}
