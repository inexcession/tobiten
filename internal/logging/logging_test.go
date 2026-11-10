package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var updateGolden = flag.Bool("update", false, "update golden files with test output")

func deterministicTimeFunc(start time.Time) func() time.Time {
	curr := start
	var mu sync.Mutex
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t := curr
		curr = curr.Add(time.Second)
		return t
	}
}

func logStandardSequence(logger *slog.Logger) {
	// Record 1: Basic info log with no extra attributes
	logger.Info("starting server daemon")

	// Record 2: Record with domain attributes (series_id, volume_id, duration_ms, job_id)
	logger.Info("volume scanned",
		SeriesID("01931535-6490-7d7b-9c76-2e860bc81eb2"),
		VolumeID("01931535-6490-7d7b-9c76-2e860bc81eb3"),
		DurationMS(142),
		JobID("01931535-6490-7d7b-9c76-2e860bc81eb4"),
	)

	// Record 3: Warning log with error attribute
	logger.Warn("retryable intake conflict", Err(errors.New("permission denied")))

	// Record 4: Error log with connection error
	logger.Error("database connection failed", Err(errors.New("connection refused")))

	// Record 5: Group attributes
	logger.Info("database pool configured",
		slog.Group("db", slog.String("host", "localhost"), slog.Int("port", 5432)),
	)

	// Record 6: Scoped logger with service override
	scoped := logger.With(Service("scanner"))
	scoped.Info("worker started")

	// Record 7: Nested WithGroup and WithAttrs
	grouped := logger.WithGroup("job").With(JobID("01931535-6490-7d7b-9c76-2e860bc81eb5"))
	grouped.Info("task completed", DurationMS(450))
}

func checkOrUpdateGolden(t *testing.T, goldenName string, actual []byte) {
	t.Helper()
	goldenPath := filepath.Join("testdata", goldenName)

	if *updateGolden {
		if err := os.MkdirAll("testdata", 0755); err != nil {
			t.Fatalf("failed to create testdata dir: %v", err)
		}
		if err := os.WriteFile(goldenPath, actual, 0644); err != nil {
			t.Fatalf("failed to write golden file %q: %v", goldenPath, err)
		}
		return
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file %q (run with -update to create): %v", goldenPath, err)
	}

	if !bytes.Equal(actual, expected) {
		t.Errorf("output does not match golden file %q\n--- Expected ---\n%s\n--- Actual ---\n%s",
			goldenPath, string(expected), string(actual))
	}
}

func TestGolden_JSON(t *testing.T) {
	baseTime := time.Date(2026, 11, 10, 10, 30, 0, 0, time.UTC)
	var buf bytes.Buffer

	logger, err := New(Options{
		Writer:   &buf,
		Format:   FormatJSON,
		Level:    "info",
		TimeFunc: deterministicTimeFunc(baseTime),
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	logStandardSequence(logger)
	actual := buf.Bytes()

	// Assertions for M1-T2.1:
	// 1. Output matches golden file
	checkOrUpdateGolden(t, "golden_json.golden", actual)

	// 2. The JSON test asserts one line per record with time, level, service, msg keys.
	lines := strings.Split(strings.TrimRight(string(actual), "\n"), "\n")
	if len(lines) != 7 {
		t.Fatalf("expected 7 log records, got %d lines", len(lines))
	}

	for i, line := range lines {
		if strings.Contains(line, "\r") {
			t.Errorf("line %d contains carriage return", i)
		}

		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("line %d is not valid single-line JSON: %v\nLine: %s", i, err, line)
		}

		// Required standard keys per ADR-0013 §1 / ROADMAP M1-T2.1
		for _, requiredKey := range []string{KeyTime, KeyLevel, KeyService, KeyMsg} {
			val, ok := m[requiredKey]
			if !ok {
				t.Errorf("line %d missing required key %q; keys present: %v", i, requiredKey, m)
				continue
			}
			strVal, isStr := val.(string)
			if !isStr || strVal == "" {
				t.Errorf("line %d key %q has empty or non-string value: %v", i, requiredKey, val)
			}
		}
	}
}

func TestGolden_Text(t *testing.T) {
	baseTime := time.Date(2026, 11, 10, 10, 30, 0, 0, time.UTC)
	var buf bytes.Buffer

	logger, err := New(Options{
		Writer:   &buf,
		Format:   FormatText,
		Level:    "info",
		TimeFunc: deterministicTimeFunc(baseTime),
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	logStandardSequence(logger)
	checkOrUpdateGolden(t, "golden_text.golden", buf.Bytes())
}

func TestGolden_Table(t *testing.T) {
	baseTime := time.Date(2026, 11, 10, 10, 30, 0, 0, time.UTC)
	var buf bytes.Buffer

	logger, err := New(Options{
		Writer:   &buf,
		Format:   FormatTable,
		Level:    "info",
		NoColor:  true,
		TimeFunc: deterministicTimeFunc(baseTime),
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	logStandardSequence(logger)
	checkOrUpdateGolden(t, "golden_table.golden", buf.Bytes())
}

func TestGolden_TableColor(t *testing.T) {
	baseTime := time.Date(2026, 11, 10, 10, 30, 0, 0, time.UTC)
	var buf bytes.Buffer

	logger, err := New(Options{
		Writer:   &buf,
		Format:   FormatTable,
		Level:    "info",
		NoColor:  false,
		TimeFunc: deterministicTimeFunc(baseTime),
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	logStandardSequence(logger)
	checkOrUpdateGolden(t, "golden_table_color.golden", buf.Bytes())
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"", FormatJSON, false},
		{"   ", FormatJSON, false},
		{"json", FormatJSON, false},
		{"JSON", FormatJSON, false},
		{"text", FormatText, false},
		{"TEXT", FormatText, false},
		{"table", FormatTable, false},
		{"Table", FormatTable, false},
		{"syslog", "", true},
		{"xml", "", true},
		{"yaml", "", true},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			got, err := ParseFormat(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseFormat(%q) expected error, got nil", tc.input)
				}
				if !errors.Is(err, ErrInvalidLogFormat) {
					t.Errorf("ParseFormat(%q) error = %v, want ErrInvalidLogFormat", tc.input, err)
				}
			} else {
				if err != nil {
					t.Errorf("ParseFormat(%q) unexpected error: %v", tc.input, err)
				}
				if got != tc.want {
					t.Errorf("ParseFormat(%q) = %q, want %q", tc.input, got, tc.want)
				}
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input   string
		want    slog.Level
		wantErr bool
	}{
		{"", slog.LevelInfo, false},
		{"   ", slog.LevelInfo, false},
		{"debug", slog.LevelDebug, false},
		{"DEBUG", slog.LevelDebug, false},
		{"info", slog.LevelInfo, false},
		{"INFO", slog.LevelInfo, false},
		{"warn", slog.LevelWarn, false},
		{"WARN", slog.LevelWarn, false},
		{"error", slog.LevelError, false},
		{"ERROR", slog.LevelError, false},
		{"trace", slog.LevelInfo, true},
		{"fatal", slog.LevelInfo, true},
		{"unknown", slog.LevelInfo, true},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("input=%q", tc.input), func(t *testing.T) {
			got, err := ParseLevel(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseLevel(%q) expected error, got nil", tc.input)
				}
				if !errors.Is(err, ErrInvalidLogLevel) {
					t.Errorf("ParseLevel(%q) error = %v, want ErrInvalidLogLevel", tc.input, err)
				}
			} else {
				if err != nil {
					t.Errorf("ParseLevel(%q) unexpected error: %v", tc.input, err)
				}
				if got != tc.want {
					t.Errorf("ParseLevel(%q) = %v, want %v", tc.input, got, tc.want)
				}
			}
		})
	}
}

func TestLevelFiltering(t *testing.T) {
	formats := []string{FormatJSON, FormatText, FormatTable}

	for _, fmtName := range formats {
		t.Run(fmtName, func(t *testing.T) {
			var buf bytes.Buffer
			logger, err := New(Options{
				Writer:  &buf,
				Format:  fmtName,
				Level:   "warn",
				NoColor: true,
			})
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}

			logger.Debug("debug message")
			logger.Info("info message")
			logger.Warn("warn message")
			logger.Error("error message")

			out := buf.String()
			if strings.Contains(out, "debug message") {
				t.Errorf("%s: debug message was logged despite Level=warn", fmtName)
			}
			if strings.Contains(out, "info message") {
				t.Errorf("%s: info message was logged despite Level=warn", fmtName)
			}
			if !strings.Contains(out, "warn message") {
				t.Errorf("%s: warn message missing from output", fmtName)
			}
			if !strings.Contains(out, "error message") {
				t.Errorf("%s: error message missing from output", fmtName)
			}
		})
	}
}

func TestRecordServiceOverride(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(Options{
		Writer:   &buf,
		Format:   FormatJSON,
		Level:    "info",
		Service:  "default-srv",
		TimeFunc: func() time.Time { return time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	logger.Info("default service message")
	logger.Info("overridden service message", slog.String(KeyService, "custom-srv"))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}

	var m1, m2 map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &m1)
	_ = json.Unmarshal([]byte(lines[1]), &m2)

	if m1["service"] != "default-srv" {
		t.Errorf("m1 service = %v, want default-srv", m1["service"])
	}
	if m2["service"] != "custom-srv" {
		t.Errorf("m2 service = %v, want custom-srv", m2["service"])
	}
}

func TestReplaceAttr(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(Options{
		Writer:  &buf,
		Format:  FormatJSON,
		Level:   "info",
		NoColor: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == "secret" {
				return slog.String("secret", "[REDACTED]")
			}
			return a
		},
		TimeFunc: func() time.Time { return time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	logger.Info("test message", slog.String("secret", "top-secret-password"))

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if m["secret"] != "[REDACTED]" {
		t.Errorf("secret = %v, want [REDACTED]", m["secret"])
	}
}

func TestConcurrency(t *testing.T) {
	for _, fmtName := range []string{FormatJSON, FormatText, FormatTable} {
		t.Run(fmtName, func(t *testing.T) {
			logger, err := New(Options{
				Writer:  io.Discard,
				Format:  fmtName,
				Level:   "info",
				NoColor: true,
			})
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}

			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					l := logger.With(slog.Int("worker", id))
					for j := 0; j < 50; j++ {
						l.Info("concurrent log line",
							SeriesID("series-1"),
							DurationMS(int64(j)),
						)
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

func TestDomainHelpers(t *testing.T) {
	if a := Service("test-srv"); a.Key != KeyService || a.Value.String() != "test-srv" {
		t.Errorf("Service() = %v", a)
	}
	if a := SeriesID("s-1"); a.Key != KeySeriesID || a.Value.String() != "s-1" {
		t.Errorf("SeriesID() = %v", a)
	}
	if a := VolumeID("v-1"); a.Key != KeyVolumeID || a.Value.String() != "v-1" {
		t.Errorf("VolumeID() = %v", a)
	}
	if a := DurationMS(250); a.Key != KeyDurationMS || a.Value.Int64() != 250 {
		t.Errorf("DurationMS() = %v", a)
	}
	if a := Duration(500 * time.Millisecond); a.Key != KeyDurationMS || a.Value.Int64() != 500 {
		t.Errorf("Duration() = %v", a)
	}
	if a := JobID("job-123"); a.Key != KeyJobID || a.Value.String() != "job-123" {
		t.Errorf("JobID() = %v", a)
	}
	if a := Err(errors.New("boom")); a.Key != KeyError || a.Value.String() != "boom" {
		t.Errorf("Err(boom) = %v", a)
	}
	if a := Err(nil); a.Key != KeyError || a.Value.String() != "<nil>" {
		t.Errorf("Err(nil) = %v", a)
	}
}

func TestIsTerminal(t *testing.T) {
	if IsTerminal(nil) {
		t.Errorf("IsTerminal(nil) = true, want false")
	}
	var buf bytes.Buffer
	if IsTerminal(&buf) {
		t.Errorf("IsTerminal(&bytes.Buffer) = true, want false")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error: %v", err)
	}
	defer r.Close()
	defer w.Close()
	if IsTerminal(r) {
		t.Errorf("IsTerminal(pipeReader) = true, want false")
	}
	if IsTerminal(w) {
		t.Errorf("IsTerminal(pipeWriter) = true, want false")
	}
}
