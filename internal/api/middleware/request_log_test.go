package middleware

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type flusherRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flusherRecorder) Flush() {
	f.flushed = true
	f.ResponseRecorder.Flush()
}

func TestRequestLogger_NeverLogsCredentials(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("response payload"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/healthz?token=supersecret_token&apiKey=12345", nil)
	req.Header.Set("Authorization", "Bearer my-secret-jwt")
	req.Header.Set("Cookie", "session_id=secret-session-cookie")
	req.Header.Set("X-API-Key", "my-secret-api-key")
	req.Header.Set("X-Auth-Token", "my-secret-auth-token")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	logOutput := buf.String()

	var logMap map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logMap); err != nil {
		t.Fatalf("log is not valid JSON: %v, raw output: %s", err, logOutput)
	}

	// 1. Msg must be constant string "http request"
	if logMap["msg"] != "http request" {
		t.Errorf("msg = %v, want 'http request'", logMap["msg"])
	}

	// 2. Path must be "/healthz" and MUST NOT include query string
	if logMap["path"] != "/healthz" {
		t.Errorf("path = %v, want '/healthz'", logMap["path"])
	}

	// 3. Status must be 200
	if status, ok := logMap["status"].(float64); !ok || int(status) != http.StatusOK {
		t.Errorf("status = %v, want %d", logMap["status"], http.StatusOK)
	}

	// 4. duration_ms must be present
	if _, ok := logMap["duration_ms"]; !ok {
		t.Errorf("duration_ms missing from log: %s", logOutput)
	}

	// 5. Bytes must equal length of "response payload" (16)
	if b, ok := logMap["bytes"].(float64); !ok || int(b) != 16 {
		t.Errorf("bytes = %v, want 16", logMap["bytes"])
	}

	// 6. NONE of the secret credentials or tokens may appear anywhere in the log output
	for _, secret := range []string{
		"supersecret_token",
		"12345",
		"my-secret-jwt",
		"secret-session-cookie",
		"my-secret-api-key",
		"my-secret-auth-token",
		"token=",
		"apiKey=",
	} {
		if strings.Contains(logOutput, secret) {
			t.Errorf("log output leaked sensitive credential or query parameter %q: %s", secret, logOutput)
		}
	}
}

func TestRequestLogger_LogLevels(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantLevel  string
	}{
		{name: "200 OK -> INFO", statusCode: http.StatusOK, wantLevel: "INFO"},
		{name: "201 Created -> INFO", statusCode: http.StatusCreated, wantLevel: "INFO"},
		{name: "302 Found -> INFO", statusCode: http.StatusFound, wantLevel: "INFO"},
		{name: "400 Bad Request -> WARN", statusCode: http.StatusBadRequest, wantLevel: "WARN"},
		{name: "404 Not Found -> WARN", statusCode: http.StatusNotFound, wantLevel: "WARN"},
		{name: "500 Internal Error -> ERROR", statusCode: http.StatusInternalServerError, wantLevel: "ERROR"},
		{name: "503 Service Unavailable -> ERROR", statusCode: http.StatusServiceUnavailable, wantLevel: "ERROR"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.statusCode)
			}))

			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			var logMap map[string]any
			if err := json.Unmarshal(buf.Bytes(), &logMap); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}
			if logMap["level"] != tc.wantLevel {
				t.Errorf("level = %v, want %q", logMap["level"], tc.wantLevel)
			}
		})
	}
}

func TestRequestLogger_FlushForwarding(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	flushed := false
	handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("responseRecorder does not implement http.Flusher")
		}
		f.Flush()
	}))

	rec := &flusherRecorder{
		ResponseRecorder: httptest.NewRecorder(),
	}
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	handler.ServeHTTP(rec, req)

	flushed = rec.flushed
	if !flushed {
		t.Errorf("expected Flush() to be forwarded to underlying ResponseWriter")
	}
}

func TestRequestLogger_FlushRecordsImplicit200AndIgnoresLateWriteHeader(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NewResponseController(w).Flush() //nolint:errcheck
		// Late WriteHeader after Flush must not alter final recorded status.
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("event: ping\n\n"))
		http.NewResponseController(w).Flush() //nolint:errcheck
	}))

	rec := &flusherRecorder{
		ResponseRecorder: httptest.NewRecorder(),
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	handler.ServeHTTP(rec, req)

	if !rec.flushed {
		t.Fatal("expected Flush() to be forwarded")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("underlying status = %d, want 200", rec.Code)
	}

	var logMap map[string]any
	if err := json.Unmarshal(buf.Bytes(), &logMap); err != nil {
		t.Fatalf("invalid JSON log: %v", err)
	}
	if status, ok := logMap["status"].(float64); !ok || int(status) != http.StatusOK {
		t.Errorf("logged status = %v, want 200", logMap["status"])
	}
	if b, ok := logMap["bytes"].(float64); !ok || int(b) != len("event: ping\n\n") {
		t.Errorf("logged bytes = %v, want %d", logMap["bytes"], len("event: ping\n\n"))
	}
}

type informationalStatusRecorder struct {
	*httptest.ResponseRecorder
	codes []int
}

func (r *informationalStatusRecorder) WriteHeader(code int) {
	r.codes = append(r.codes, code)
	if code >= 200 {
		r.ResponseRecorder.WriteHeader(code)
	}
}

func TestRequestLogger_Informational1xxNotFinal(t *testing.T) {
	t.Run("103 followed by 201 records 201", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, nil))

		handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusEarlyHints)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("created"))
		}))

		rec := &informationalStatusRecorder{ResponseRecorder: httptest.NewRecorder()}
		req := httptest.NewRequest(http.MethodPost, "/items", nil)
		handler.ServeHTTP(rec, req)

		if len(rec.codes) != 2 || rec.codes[0] != http.StatusEarlyHints || rec.codes[1] != http.StatusCreated {
			t.Fatalf("forwarded codes = %v, want [103 201]", rec.codes)
		}

		var logMap map[string]any
		if err := json.Unmarshal(buf.Bytes(), &logMap); err != nil {
			t.Fatalf("invalid JSON log: %v", err)
		}
		if status, ok := logMap["status"].(float64); !ok || int(status) != http.StatusCreated {
			t.Errorf("logged status = %v, want 201", logMap["status"])
		}
		if logMap["level"] != "INFO" {
			t.Errorf("logged level = %v, want INFO", logMap["level"])
		}
	})

	t.Run("100 followed by Write records 200", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, nil))

		handler := RequestLogger(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusContinue)
			_, _ = w.Write([]byte("ok"))
		}))

		rec := &informationalStatusRecorder{ResponseRecorder: httptest.NewRecorder()}
		req := httptest.NewRequest(http.MethodGet, "/stream", nil)
		handler.ServeHTTP(rec, req)

		if len(rec.codes) != 2 || rec.codes[0] != http.StatusContinue || rec.codes[1] != http.StatusOK {
			t.Fatalf("forwarded codes = %v, want [100 200]", rec.codes)
		}

		var logMap map[string]any
		if err := json.Unmarshal(buf.Bytes(), &logMap); err != nil {
			t.Fatalf("invalid JSON log: %v", err)
		}
		if status, ok := logMap["status"].(float64); !ok || int(status) != http.StatusOK {
			t.Errorf("logged status = %v, want 200", logMap["status"])
		}
	})
}
