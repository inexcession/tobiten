package openapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

func (m *Module) eventService() app.EventService {
	if m.services == nil {
		return nil
	}
	return m.services.Events
}

func setSSEHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("X-Accel-Buffering", "no")
}

func sanitizeSSEField(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' {
			return -1
		}
		return r
	}, s)
}

func writeSSEFrame(w io.Writer, ev app.StreamEvent) error {
	if id := sanitizeSSEField(ev.ID); id != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
			return err
		}
	}
	if name := sanitizeSSEField(ev.Name); name != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", name); err != nil {
			return err
		}
	}

	raw := bytes.TrimSpace(ev.Data)
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	for line := range bytes.SplitSeq(raw, []byte("\n")) {
		clean := bytes.TrimSuffix(line, []byte("\r"))
		if _, err := fmt.Fprintf(w, "data: %s\n", clean); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "\n")
	return err
}

func mergeStreamContext(reqCtx, shutdownCtx context.Context) (context.Context, context.CancelFunc) {
	if shutdownCtx == nil {
		return reqCtx, func() {}
	}
	merged, cancel := context.WithCancel(reqCtx)
	stop := context.AfterFunc(shutdownCtx, cancel)
	return merged, func() {
		stop()
		cancel()
	}
}

func (m *Module) handleEvents(w http.ResponseWriter, r *http.Request) {
	p, ok := domain.PrincipalFromContext(r.Context())
	if !ok || p == nil {
		w.Header().Set("Cache-Control", "no-store")
		middleware.WriteUnauthorized(w, r)
		return
	}

	svc := m.eventService()
	if svc == nil {
		w.Header().Set("Cache-Control", "no-store")
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}

	if r.Method == http.MethodHead {
		setSSEHeaders(w)
		w.WriteHeader(http.StatusOK)
		return
	}

	jobID := strings.TrimSpace(r.URL.Query().Get("job_id"))
	rc := http.NewResponseController(w)
	streamCtx, cancelStream := mergeStreamContext(r.Context(), middleware.ShutdownContextFromContext(r.Context()))
	defer cancelStream()

	attached := false
	err := svc.Stream(
		streamCtx,
		p,
		app.EventStreamFilter{JobID: jobID},
		func() error {
			attached = true
			setSSEHeaders(w)
			w.WriteHeader(http.StatusOK)
			_ = rc.Flush()
			return nil
		},
		func(ev app.StreamEvent) error {
			if err := writeSSEFrame(w, ev); err != nil {
				return err
			}
			_ = rc.Flush()
			return nil
		},
	)
	if err != nil && !attached {
		w.Header().Set("Cache-Control", "no-store")
		middleware.WriteAppError(w, r, err)
	}
}
