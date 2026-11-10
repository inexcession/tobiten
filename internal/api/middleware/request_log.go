package middleware

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/inexcession/tobiten/internal/logging"
)

// responseRecorder wraps an http.ResponseWriter to capture the HTTP status code
// and response size in bytes.
type responseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
	wroteHeader  bool
}

func (rw *responseRecorder) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.statusCode = code
		rw.wroteHeader = true
		rw.ResponseWriter.WriteHeader(code)
	}
}

func (rw *responseRecorder) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

func (rw *responseRecorder) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (rw *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
}

func (rw *responseRecorder) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// RequestLogger returns HTTP middleware that logs request execution with slog.
// It records method, path, status, duration_ms, bytes, and remote_addr.
// In accordance with ADR-0006 and slice M1-T1.2 decrees:
// - User-controlled data is kept strictly in attributes, never in the log message.
// - Credentials are NEVER recorded: no Authorization, Cookie, X-API-Key, or X-Auth-Token headers.
// - Query strings are NEVER recorded (logs URL path only, as query strings may carry tokens).
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			rec := &responseRecorder{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next.ServeHTTP(rec, r)

			duration := time.Since(start)

			path := r.URL.Path
			if path == "" {
				path = "/"
			}

			attrs := []any{
				slog.String("method", r.Method),
				slog.String("path", path),
				slog.Int("status", rec.statusCode),
				logging.Duration(duration),
				slog.Int64("bytes", rec.bytesWritten),
				slog.String("remote_addr", r.RemoteAddr),
			}

			const msg = "http request"

			switch {
			case rec.statusCode >= 500:
				logger.ErrorContext(r.Context(), msg, attrs...)
			case rec.statusCode >= 400:
				logger.WarnContext(r.Context(), msg, attrs...)
			default:
				logger.InfoContext(r.Context(), msg, attrs...)
			}
		})
	}
}
