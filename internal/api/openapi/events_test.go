package openapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/api/middleware"
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

type failingSSEFlushWriter struct {
	*httptest.ResponseRecorder
	flushCount int
	failAfter  int
	flushErr   error
}

func (w *failingSSEFlushWriter) FlushError() error {
	w.Flush()
	w.flushCount++
	if w.flushCount > w.failAfter {
		return w.flushErr
	}
	return nil
}

type blockingDeadlineSSEWriter struct {
	*httptest.ResponseRecorder
	mu            sync.Mutex
	deadline      time.Time
	deadlineSetCh chan time.Time
	blockingCh    chan struct{}
	flushCount    int
}

func newBlockingDeadlineSSEWriter() *blockingDeadlineSSEWriter {
	return &blockingDeadlineSSEWriter{
		ResponseRecorder: httptest.NewRecorder(),
		deadlineSetCh:    make(chan time.Time, 8),
		blockingCh:       make(chan struct{}, 1),
	}
}

func (w *blockingDeadlineSSEWriter) SetWriteDeadline(d time.Time) error {
	w.mu.Lock()
	w.deadline = d
	w.mu.Unlock()
	select {
	case w.deadlineSetCh <- d:
	default:
	}
	return nil
}

func (w *blockingDeadlineSSEWriter) FlushError() error {
	w.Flush()
	w.mu.Lock()
	w.flushCount++
	count := w.flushCount
	w.mu.Unlock()
	if count == 1 {
		// Header flush succeeds immediately.
		return nil
	}
	// Block on the frame flush until write deadline expires (either periodic bound or shutdown abort).
	select {
	case w.blockingCh <- struct{}{}:
	default:
	}
	for {
		w.mu.Lock()
		d := w.deadline
		w.mu.Unlock()
		if !d.IsZero() {
			wait := time.Until(d)
			if wait <= 0 {
				return os.ErrDeadlineExceeded
			}
			select {
			case <-time.After(wait):
			case d = <-w.deadlineSetCh:
				if !d.IsZero() && !d.After(time.Now()) {
					return os.ErrDeadlineExceeded
				}
			}
			continue
		}
		d = <-w.deadlineSetCh
		if !d.IsZero() && !d.After(time.Now()) {
			return os.ErrDeadlineExceeded
		}
	}
}

type captureEmitErrEventService struct {
	events       []app.StreamEvent
	onAttachedFn func()
	emitErrCh    chan error
}

func (s *captureEmitErrEventService) Stream(_ context.Context, _ *domain.Principal, _ app.EventStreamFilter, onAttached func() error, emit func(app.StreamEvent) error) error {
	if err := onAttached(); err != nil {
		return err
	}
	if s.onAttachedFn != nil {
		s.onAttachedFn()
	}
	for _, ev := range s.events {
		if err := emit(ev); err != nil {
			s.emitErrCh <- err
			return err
		}
	}
	s.emitErrCh <- nil
	return nil
}

func TestOpenAPIModule_EventsSSE_FlushErrorAndBlockedWriteShutdown(t *testing.T) {
	t.Parallel()

	readerRole := mustBuiltInRole(t, domain.RoleReaderID)
	alice := domain.NewPrincipal(domain.User{ID: "u-alice", Username: "alice"}, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-a", nil)

	t.Run("flush error propagates to emit caller", func(t *testing.T) {
		wantErr := errors.New("broken pipe")
		svc := &captureEmitErrEventService{
			events: []app.StreamEvent{
				{ID: "1", Name: "job.started", Data: json.RawMessage(`{"sequence":1}`)},
			},
			emitErrCh: make(chan error, 1),
		}
		mod := NewModule(app.NewAuthorizingServices(&app.Services{Events: svc}))
		h := mod.RouteMounts()[0].Handler

		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req = req.WithContext(domain.ContextWithPrincipal(req.Context(), alice))
		rec := &failingSSEFlushWriter{
			ResponseRecorder: httptest.NewRecorder(),
			failAfter:        1, // header flush succeeds, first event flush fails
			flushErr:         wantErr,
		}
		h.ServeHTTP(rec, req)

		select {
		case gotErr := <-svc.emitErrCh:
			if !errors.Is(gotErr, wantErr) {
				t.Fatalf("emit error = %v, want %v", gotErr, wantErr)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for emit error")
		}
	})

	t.Run("shutdown cancellation aborts blocked SSE flush via write deadline", func(t *testing.T) {
		svc := &captureEmitErrEventService{
			events: []app.StreamEvent{
				{ID: "2", Name: "job.started", Data: json.RawMessage(`{"sequence":2}`)},
			},
			emitErrCh: make(chan error, 1),
		}
		mod := NewModule(app.NewAuthorizingServices(&app.Services{Events: svc}))
		h := mod.RouteMounts()[0].Handler

		shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
		req = req.WithContext(middleware.WithShutdownContext(domain.ContextWithPrincipal(req.Context(), alice), shutdownCtx))
		rec := newBlockingDeadlineSSEWriter()

		done := make(chan struct{})
		go func() {
			defer close(done)
			h.ServeHTTP(rec, req)
		}()

		// Wait until frame flush is actively blocked.
		select {
		case <-rec.blockingCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for SSE flush to block")
		}

		// Cancelling shutdownCtx must abort the blocked write immediately via SetWriteDeadline.
		start := time.Now()
		cancelShutdown()

		select {
		case gotErr := <-svc.emitErrCh:
			if !errors.Is(gotErr, os.ErrDeadlineExceeded) {
				t.Fatalf("emit error after shutdown abort = %v, want %v", gotErr, os.ErrDeadlineExceeded)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for blocked SSE write to abort on shutdown")
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for handleEvents to return")
		}
		if elapsed := time.Since(start); elapsed >= defaultSSEWriteTimeout {
			t.Fatalf("shutdown unblock took %v, expected immediate abort well below %v", elapsed, defaultSSEWriteTimeout)
		}
	})
}
