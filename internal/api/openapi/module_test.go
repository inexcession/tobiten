package openapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/inexcession/tobiten/internal/app"
)

func TestOpenAPIModule_AuthRoutes(t *testing.T) {
	stubServices := app.NewStubServices()
	mod := NewModule(stubServices)

	mounts := mod.RouteMounts()
	if len(mounts) != 1 {
		t.Fatalf("expected 1 RouteMount, got %d", len(mounts))
	}
	if mounts[0].Prefix != "/api/v1" {
		t.Errorf("prefix = %q, want /api/v1", mounts[0].Prefix)
	}

	handler := mounts[0].Handler

	t.Run("GET /api/v1/auth/setup status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/setup", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}

		var res setupStatusResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("json decode failed: %v", err)
		}
		if res.Sealed {
			t.Errorf("expected sealed=false on initial run")
		}
	})

	t.Run("POST /api/v1/auth/setup creates initial admin", func(t *testing.T) {
		body, _ := json.Marshal(app.SetupAdminRequest{
			SetupToken: "tb_setup_test_token",
			Username:   "sysadmin",
			Password:   "correcthorsebatterystaple",
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/setup", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 Created (body: %s)", rec.Code, rec.Body.String())
		}

		var res setupResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("json decode failed: %v", err)
		}
		if res.User.Username != "sysadmin" {
			t.Errorf("username = %q, want sysadmin", res.User.Username)
		}
	})

	t.Run("POST /api/v1/auth/setup is permanently sealed after user creation", func(t *testing.T) {
		body, _ := json.Marshal(app.SetupAdminRequest{
			SetupToken: "tb_setup_test_token_2",
			Username:   "sysadmin2",
			Password:   "correcthorsebatterystaple",
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/setup", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403 Forbidden after seal", rec.Code)
		}
	})

	t.Run("POST /api/v1/auth/login with valid credentials succeeds", func(t *testing.T) {
		body, _ := json.Marshal(loginRequest{
			Username: "sysadmin",
			Password: "correcthorsebatterystaple",
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 OK (body: %s)", rec.Code, rec.Body.String())
		}

		var res loginResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("json decode failed: %v", err)
		}
		if res.User.Username != "sysadmin" {
			t.Errorf("username = %q, want sysadmin", res.User.Username)
		}
	})

	t.Run("POST /api/v1/auth/login with invalid credentials fails with 401", func(t *testing.T) {
		body, _ := json.Marshal(loginRequest{
			Username: "",
			Password: "",
		})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("empty credentials status = %d, want 400 Bad Request", rec.Code)
		}
	})
}
