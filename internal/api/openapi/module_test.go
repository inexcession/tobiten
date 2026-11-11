package openapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/inexcession/tobiten/internal/app"
)

func TestOpenAPIModule_NilServicesFailClosed(t *testing.T) {
	// Daemon and nil-service defaults must fail closed/unavailable until real services land
	for _, tc := range []struct {
		name     string
		services *app.Services
	}{
		{name: "nil services", services: nil},
		{name: "empty services", services: &app.Services{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mod := NewModule(tc.services)
			mounts := mod.RouteMounts()
			if len(mounts) != 1 {
				t.Fatalf("expected 1 RouteMount, got %d", len(mounts))
			}
			handler := mounts[0].Handler

			// POST /api/v1/auth/login fails closed with 503 Service Unavailable
			bodyLogin, _ := json.Marshal(loginRequest{Username: "admin", Password: "pwd"})
			reqLogin := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(bodyLogin))
			recLogin := httptest.NewRecorder()
			handler.ServeHTTP(recLogin, reqLogin)
			if recLogin.Code != http.StatusServiceUnavailable {
				t.Errorf("login status = %d, want 503 Service Unavailable", recLogin.Code)
			}

			// POST /api/v1/auth/setup fails closed with 503 Service Unavailable
			bodySetup, _ := json.Marshal(app.SetupAdminRequest{Username: "admin", Password: "pwd", SetupToken: "tok"})
			reqSetup := httptest.NewRequest(http.MethodPost, "/api/v1/auth/setup", bytes.NewReader(bodySetup))
			recSetup := httptest.NewRecorder()
			handler.ServeHTTP(recSetup, reqSetup)
			if recSetup.Code != http.StatusServiceUnavailable {
				t.Errorf("setup status = %d, want 503 Service Unavailable", recSetup.Code)
			}

			// Anonymous GET /api/v1/auth/setup must be denied / 405 Method Not Allowed
			reqGetSetup := httptest.NewRequest(http.MethodGet, "/api/v1/auth/setup", nil)
			recGetSetup := httptest.NewRecorder()
			handler.ServeHTTP(recGetSetup, reqGetSetup)
			if recGetSetup.Code != http.StatusMethodNotAllowed {
				t.Errorf("GET setup status = %d, want 405 Method Not Allowed (denied)", recGetSetup.Code)
			}

			// Anonymous HEAD /api/v1/auth/setup must be denied / 405 Method Not Allowed
			reqHeadSetup := httptest.NewRequest(http.MethodHead, "/api/v1/auth/setup", nil)
			recHeadSetup := httptest.NewRecorder()
			handler.ServeHTTP(recHeadSetup, reqHeadSetup)
			if recHeadSetup.Code != http.StatusMethodNotAllowed {
				t.Errorf("HEAD setup status = %d, want 405 Method Not Allowed (denied)", recHeadSetup.Code)
			}
		})
	}
}

func TestOpenAPIModule_ExplicitFakeServices(t *testing.T) {
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

	t.Run("GET /api/v1/auth/setup denied (not allowlisted)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/setup", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405 Method Not Allowed", rec.Code)
		}
	})

	t.Run("HEAD /api/v1/auth/setup denied (not allowlisted)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodHead, "/api/v1/auth/setup", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405 Method Not Allowed", rec.Code)
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

	t.Run("POST /api/v1/auth/login with invalid credentials fails with 400 or 401", func(t *testing.T) {
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
