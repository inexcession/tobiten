package openapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/app"
	"github.com/inexcession/tobiten/internal/domain"
)

const defaultSSEWriteTimeout = 5 * time.Second

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

type sseWriter struct {
	w         http.ResponseWriter
	rc        *http.ResponseController
	streamCtx context.Context
	timeout   time.Duration

	mu      sync.Mutex
	writing bool
	closed  bool
}

func newSSEWriter(streamCtx context.Context, w http.ResponseWriter, timeout time.Duration) *sseWriter {
	if timeout <= 0 {
		timeout = defaultSSEWriteTimeout
	}
	return &sseWriter{
		w:         w,
		rc:        http.NewResponseController(w),
		streamCtx: streamCtx,
		timeout:   timeout,
	}
}

func ignoreUnsupportedErr(err error) error {
	switch {
	case err == nil, errors.Is(err, http.ErrNotSupported):
		return nil
	default:
		return err
	}
}

func (s *sseWriter) setDeadline(deadline time.Time) error {
	return ignoreUnsupportedErr(s.rc.SetWriteDeadline(deadline))
}

func (s *sseWriter) beginWrite() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return context.Canceled
	}
	if err := s.streamCtx.Err(); err != nil {
		return err
	}
	if err := s.setDeadline(time.Now().Add(s.timeout)); err != nil {
		return err
	}
	s.writing = true
	return nil
}

func (s *sseWriter) endWrite() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writing = false
	if s.closed || s.streamCtx.Err() != nil {
		return
	}
	_ = s.setDeadline(time.Time{})
}

func (s *sseWriter) abortBlockedWrite() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.writing {
		_ = s.setDeadline(time.Unix(1, 0))
	}
}

func (s *sseWriter) flush() error {
	return ignoreUnsupportedErr(s.rc.Flush())
}

func (s *sseWriter) writeHeaders(onWrite func()) error {
	if err := s.beginWrite(); err != nil {
		return err
	}
	defer s.endWrite()

	onWrite()
	setSSEHeaders(s.w)
	s.w.WriteHeader(http.StatusOK)
	return s.flush()
}

func (s *sseWriter) writeEvent(ev app.StreamEvent) error {
	if err := s.beginWrite(); err != nil {
		return err
	}
	defer s.endWrite()

	if err := writeSSEFrame(s.w, ev); err != nil {
		return err
	}
	return s.flush()
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
	streamCtx, cancelStream := mergeStreamContext(r.Context(), middleware.ShutdownContextFromContext(r.Context()))
	defer cancelStream()

	sw := newSSEWriter(streamCtx, w, defaultSSEWriteTimeout)
	stopAbort := context.AfterFunc(streamCtx, sw.abortBlockedWrite)
	defer stopAbort()

	attached := false
	err := svc.Stream(
		streamCtx,
		p,
		app.EventStreamFilter{JobID: jobID},
		func() error {
			return sw.writeHeaders(func() {
				attached = true
			})
		},
		sw.writeEvent,
	)
	if err != nil && !attached && streamCtx.Err() == nil {
		w.Header().Set("Cache-Control", "no-store")
		middleware.WriteAppError(w, r, err)
	}
}
