package openapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

func TestOpenAPIModule_EventsSSE(t *testing.T) {
	t.Parallel()

	readerRole := mustBuiltInRole(t, domain.RoleReaderID)
	alice := domain.NewPrincipal(domain.User{ID: "u-alice", Username: "alice"}, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-a", nil)

	t.Run("unauthenticated returns 401", func(t *testing.T) {
		mod := NewModule(app.NewStubServices())
		h := mod.RouteMounts()[0].Handler

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("nil services returns 503", func(t *testing.T) {
		mod := NewModule(nil)
		h := mod.RouteMounts()[0].Handler

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), alice))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("Cache-Control = %q, want no-store", cc)
		}
	})

	t.Run("wrong method returns 405", func(t *testing.T) {
		mod := NewModule(app.NewStubServices())
		h := mod.RouteMounts()[0].Handler

		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), alice))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", rec.Code)
		}
	})

	t.Run("HEAD returns 200 SSE headers with empty body", func(t *testing.T) {
		mod := NewModule(app.NewStubServices())
		h := mod.RouteMounts()[0].Handler

		req := httptest.NewRequest(http.MethodHead, "/api/v1/events", nil)
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), alice))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
			t.Errorf("Content-Type = %q, want text/event-stream", ct)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("HEAD body len = %d, want 0", rec.Body.Len())
		}
	})

	t.Run("GET streams durable ID and ephemeral progress without ID", func(t *testing.T) {
		stubEvents := app.NewStubEventService()
		stubEvents.Events = []app.StreamEvent{
			{
				ID:   "10",
				Name: "job.started",
				Data: json.RawMessage(`{"sequence":10,"kind":"job.started"}`),
			},
			{
				ID:   "",
				Name: domain.ProgressTickEventName,
				Data: json.RawMessage(`{"job_id":"j1","scanned_count":3}`),
			},
		}
		mod := NewModule(app.NewAuthorizingServices(&app.Services{Events: stubEvents}))
		h := mod.RouteMounts()[0].Handler

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events?job_id=j1", nil).WithContext(ctx)
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), alice))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if !rec.Flushed {
			t.Error("expected SSE response to be flushed")
		}

		scanner := bufio.NewScanner(strings.NewReader(rec.Body.String()))
		var lines []string
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		want := []string{
			"id: 10",
			"event: job.started",
			`data: {"sequence":10,"kind":"job.started"}`,
			"",
			"event: job.progress",
			`data: {"job_id":"j1","scanned_count":3}`,
			"",
		}
		if len(lines) != len(want) {
			t.Fatalf("lines = %#v, want %#v", lines, want)
		}
		for i := range want {
			if lines[i] != want[i] {
				t.Errorf("line[%d] = %q, want %q", i, lines[i], want[i])
			}
		}
	})
}
