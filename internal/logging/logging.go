// Package logging provides structured log/slog handlers for Tobiten (ADR-0013).
// It supports single-line JSON output (default), logfmt text output, and
// vertically aligned colored tabular output for interactive development.
package logging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Supported log output formats (ADR-0013 §1).
const (
	FormatJSON  = "json"
	FormatText  = "text"
	FormatTable = "table"
)

// DefaultService is the default service identifier attached to log records.
const DefaultService = "tobitend"

// Standard attribute keys (ADR-0013 §1).
const (
	KeyTime       = slog.TimeKey
	KeyLevel      = slog.LevelKey
	KeyMsg        = slog.MessageKey
	KeyService    = "service"
	KeySeriesID   = "series_id"
	KeyVolumeID   = "volume_id"
	KeyDurationMS = "duration_ms"
	KeyJobID      = "job_id"
	KeyError      = "error"
)

// Sentinel errors for invalid logging configuration.
var (
	ErrInvalidLogFormat = errors.New("unsupported log format: supported formats are 'json', 'text', and 'table'")
	ErrInvalidLogLevel  = errors.New("unsupported log level: supported levels are 'debug', 'info', 'warn', and 'error'")
)

// ANSI escape codes for interactive table output.
const (
	ansiReset   = "\x1b[0m"
	ansiDim     = "\x1b[90m"
	ansiRed     = "\x1b[31m"
	ansiGreen   = "\x1b[32m"
	ansiYellow  = "\x1b[33m"
	ansiMagenta = "\x1b[35m"
	ansiCyan    = "\x1b[36m"
)

// Column widths for table formatting.
const (
	tableTimeFormat   = "2006-01-02 15:04:05.000"
	tableLevelWidth   = 5
	tableServiceWidth = 12
	tableMsgWidth     = 28
)

type defaultWriter struct{}

func (defaultWriter) Write(p []byte) (int, error) {
	return os.Stdout.Write(p)
}

// Options configures a structured logger.
type Options struct {
	// Writer is the destination for log output. Defaults to os.Stdout.
	Writer io.Writer
	// Format selects the output format: "json" (default), "text", or "table".
	Format string
	// Level sets the minimum log level: "debug", "info" (default), "warn", or "error".
	Level string
	// Service sets the default "service" attribute key. Defaults to DefaultService ("tobitend").
	Service string
	// NoColor disables ANSI color sequences in "table" format.
	NoColor bool
	// TimeFunc optionally overrides record timestamps (used for deterministic tests).
	TimeFunc func() time.Time
	// ReplaceAttr optionally transforms attributes before emission.
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

// Service returns a slog.Attr for the "service" key.
func Service(name string) slog.Attr {
	return slog.String(KeyService, name)
}

// SeriesID returns a slog.Attr for the "series_id" domain key.
func SeriesID(id string) slog.Attr {
	return slog.String(KeySeriesID, id)
}

// VolumeID returns a slog.Attr for the "volume_id" domain key.
func VolumeID(id string) slog.Attr {
	return slog.String(KeyVolumeID, id)
}

// DurationMS returns a slog.Attr for the "duration_ms" domain key.
func DurationMS(ms int64) slog.Attr {
	return slog.Int64(KeyDurationMS, ms)
}

// Duration returns a slog.Attr for the "duration_ms" domain key from a time.Duration.
func Duration(d time.Duration) slog.Attr {
	return slog.Int64(KeyDurationMS, d.Milliseconds())
}

// JobID returns a slog.Attr for the "job_id" domain key.
func JobID(id string) slog.Attr {
	return slog.String(KeyJobID, id)
}

// Err returns a slog.Attr for the "error" domain key.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.String(KeyError, "<nil>")
	}
	return slog.String(KeyError, err.Error())
}

// ParseFormat validates and normalizes a log format string.
// An empty string defaults to FormatJSON ("json").
func ParseFormat(format string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(format))
	if normalized == "" {
		return FormatJSON, nil
	}
	switch normalized {
	case FormatJSON, FormatText, FormatTable:
		return normalized, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidLogFormat, format)
	}
}

// ParseLevel validates and converts a log level string into a slog.Level.
// An empty string defaults to slog.LevelInfo.
func ParseLevel(level string) (slog.Level, error) {
	normalized := strings.ToLower(strings.TrimSpace(level))
	if normalized == "" {
		return slog.LevelInfo, nil
	}
	switch normalized {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("%w: %q", ErrInvalidLogLevel, level)
	}
}

// New constructs a *slog.Logger from Options.
func New(opts Options) (*slog.Logger, error) {
	h, err := NewHandler(opts)
	if err != nil {
		return nil, err
	}
	return slog.New(h), nil
}

// NewHandler constructs a slog.Handler for the configured format and level.
func NewHandler(opts Options) (slog.Handler, error) {
	format, err := ParseFormat(opts.Format)
	if err != nil {
		return nil, err
	}
	level, err := ParseLevel(opts.Level)
	if err != nil {
		return nil, err
	}

	w := opts.Writer
	if w == nil {
		w = defaultWriter{}
	}

	service := strings.TrimSpace(opts.Service)
	if service == "" {
		service = DefaultService
	}

	if format == FormatTable {
		return &tableHandler{
			w:           w,
			mu:          &sync.Mutex{},
			level:       level,
			service:     service,
			noColor:     opts.NoColor,
			timeFunc:    opts.TimeFunc,
			replaceAttr: opts.ReplaceAttr,
		}, nil
	}

	h := &stdHandler{
		w:           w,
		mu:          &sync.Mutex{},
		format:      format,
		level:       level,
		service:     service,
		timeFunc:    opts.TimeFunc,
		replaceAttr: opts.ReplaceAttr,
	}
	h.inner = h.buildInner(service)
	return h, nil
}

type handlerStep struct {
	group string
	attrs []slog.Attr
}

// stdHandler wraps slog.JSONHandler and slog.TextHandler to guarantee the
// presence and non-duplication of the top-level "service" attribute and UTC timestamps.
type stdHandler struct {
	w           io.Writer
	mu          *sync.Mutex
	format      string
	level       slog.Level
	service     string
	timeFunc    func() time.Time
	replaceAttr func(groups []string, a slog.Attr) slog.Attr
	steps       []handlerStep
	inner       slog.Handler
}

func (h *stdHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *stdHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.timeFunc != nil {
		r.Time = h.timeFunc().UTC()
	} else if r.Time.IsZero() {
		r.Time = time.Now().UTC()
	} else {
		r.Time = r.Time.UTC()
	}

	if !h.hasGroups() {
		if overrideService, hasOverride := extractRecordService(r); hasOverride {
			cloned := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == KeyService {
					return true
				}
				cloned.AddAttrs(a)
				return true
			})
			tmpInner := h.buildInner(overrideService)
			h.mu.Lock()
			defer h.mu.Unlock()
			return tmpInner.Handle(ctx, cloned)
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	return h.inner.Handle(ctx, r)
}

func (h *stdHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	service := h.service
	filtered := make([]slog.Attr, 0, len(attrs))
	atRoot := !h.hasGroups()

	for _, a := range attrs {
		if atRoot && a.Key == KeyService {
			if s := strings.TrimSpace(a.Value.Resolve().String()); s != "" {
				service = s
			}
			continue
		}
		filtered = append(filtered, a)
	}

	steps := slices.Clone(h.steps)
	if len(filtered) > 0 {
		steps = append(steps, handlerStep{attrs: filtered})
	}

	next := &stdHandler{
		w:           h.w,
		mu:          h.mu,
		format:      h.format,
		level:       h.level,
		service:     service,
		timeFunc:    h.timeFunc,
		replaceAttr: h.replaceAttr,
		steps:       steps,
	}
	next.inner = next.buildInner(service)
	return next
}

func (h *stdHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	steps := slices.Clone(h.steps)
	steps = append(steps, handlerStep{group: name})

	next := &stdHandler{
		w:           h.w,
		mu:          h.mu,
		format:      h.format,
		level:       h.level,
		service:     h.service,
		timeFunc:    h.timeFunc,
		replaceAttr: h.replaceAttr,
		steps:       steps,
	}
	next.inner = next.buildInner(h.service)
	return next
}

func (h *stdHandler) hasGroups() bool {
	for _, s := range h.steps {
		if s.group != "" {
			return true
		}
	}
	return false
}

func (h *stdHandler) buildInner(service string) slog.Handler {
	hop := &slog.HandlerOptions{
		Level:       h.level,
		ReplaceAttr: h.replaceAttr,
	}
	var base slog.Handler
	if h.format == FormatText {
		base = slog.NewTextHandler(h.w, hop)
	} else {
		base = slog.NewJSONHandler(h.w, hop)
	}
	base = base.WithAttrs([]slog.Attr{slog.String(KeyService, service)})
	for _, s := range h.steps {
		if s.group != "" {
			base = base.WithGroup(s.group)
		} else if len(s.attrs) > 0 {
			base = base.WithAttrs(s.attrs)
		}
	}
	return base
}

func extractRecordService(r slog.Record) (string, bool) {
	var found string
	var ok bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == KeyService && a.Value.Kind() != slog.KindGroup {
			if s := strings.TrimSpace(a.Value.Resolve().String()); s != "" {
				found = s
				ok = true
			}
		}
		return true
	})
	return found, ok
}

type scopedAttr struct {
	groups []string
	prefix string
	attr   slog.Attr
}

// tableHandler implements slog.Handler for clean, vertically aligned, colored
// tabular output during interactive terminal sessions (ADR-0013 §1).
type tableHandler struct {
	w           io.Writer
	mu          *sync.Mutex
	level       slog.Level
	service     string
	noColor     bool
	timeFunc    func() time.Time
	replaceAttr func(groups []string, a slog.Attr) slog.Attr
	groups      []string
	groupPrefix string
	attrs       []scopedAttr
}

func (h *tableHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *tableHandler) Handle(_ context.Context, r slog.Record) error {
	if h.timeFunc != nil {
		r.Time = h.timeFunc().UTC()
	} else if r.Time.IsZero() {
		r.Time = time.Now().UTC()
	} else {
		r.Time = r.Time.UTC()
	}

	service := h.service
	collected := make([]scopedAttr, 0, len(h.attrs)+r.NumAttrs())
	collected = append(collected, h.attrs...)

	atRoot := len(h.groups) == 0
	r.Attrs(func(a slog.Attr) bool {
		h.collectAttr(&collected, &service, atRoot, h.groups, h.groupPrefix, a)
		return true
	})

	timeStr := r.Time.Format(tableTimeFormat)
	if h.replaceAttr != nil {
		ta := h.replaceAttr(nil, slog.Time(KeyTime, r.Time))
		if ta.Key != "" {
			if ta.Value.Kind() == slog.KindTime {
				timeStr = ta.Value.Time().UTC().Format(tableTimeFormat)
			} else {
				timeStr = ta.Value.String()
			}
		}
	}

	lvl := r.Level
	levelStr := levelLabel(lvl)
	if h.replaceAttr != nil {
		la := h.replaceAttr(nil, slog.Any(KeyLevel, lvl))
		if la.Key != "" && la.Value.String() != "" {
			levelStr = la.Value.String()
		}
	}
	levelRaw := padRight(levelStr, tableLevelWidth)

	if h.replaceAttr != nil {
		sa := h.replaceAttr(nil, slog.String(KeyService, service))
		if sa.Key != "" {
			service = sa.Value.String()
		}
	}
	serviceRaw := padRight(sanitizeSingleLine(service), tableServiceWidth)

	msg := r.Message
	if h.replaceAttr != nil {
		ma := h.replaceAttr(nil, slog.String(KeyMsg, msg))
		if ma.Key != "" {
			msg = ma.Value.String()
		}
	}
	msgClean := sanitizeSingleLine(msg)

	// Render key=value pairs
	renderedAttrs := make([]string, 0, len(collected))
	for _, sa := range collected {
		a := sa.attr
		if h.replaceAttr != nil {
			a = h.replaceAttr(sa.groups, a)
			if a.Equal(slog.Attr{}) || a.Key == "" {
				continue
			}
		}
		key := sa.prefix + a.Key
		val := formatAttrValue(a.Value.Resolve())
		if h.noColor {
			renderedAttrs = append(renderedAttrs, key+"="+val)
		} else {
			renderedAttrs = append(renderedAttrs, ansiDim+key+"="+ansiReset+val)
		}
	}

	var buf bytes.Buffer
	if h.noColor {
		buf.WriteString(timeStr)
		buf.WriteString(" | ")
		buf.WriteString(levelRaw)
		buf.WriteString(" | ")
		buf.WriteString(serviceRaw)
		buf.WriteString(" | ")
	} else {
		buf.WriteString(ansiDim + timeStr + ansiReset)
		buf.WriteString(" | ")
		buf.WriteString(levelColor(lvl) + levelRaw + ansiReset)
		buf.WriteString(" | ")
		buf.WriteString(ansiMagenta + serviceRaw + ansiReset)
		buf.WriteString(" | ")
	}

	if len(renderedAttrs) > 0 {
		buf.WriteString(padRight(msgClean, tableMsgWidth))
		buf.WriteString(" | ")
		buf.WriteString(strings.Join(renderedAttrs, " "))
	} else {
		buf.WriteString(msgClean)
	}
	buf.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(buf.Bytes())
	return err
}

func (h *tableHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	service := h.service
	collected := slices.Clone(h.attrs)
	atRoot := len(h.groups) == 0
	for _, a := range attrs {
		h.collectAttr(&collected, &service, atRoot, h.groups, h.groupPrefix, a)
	}

	return &tableHandler{
		w:           h.w,
		mu:          h.mu,
		level:       h.level,
		service:     service,
		noColor:     h.noColor,
		timeFunc:    h.timeFunc,
		replaceAttr: h.replaceAttr,
		groups:      slices.Clone(h.groups),
		groupPrefix: h.groupPrefix,
		attrs:       collected,
	}
}

func (h *tableHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	groups := slices.Clone(h.groups)
	groups = append(groups, name)

	return &tableHandler{
		w:           h.w,
		mu:          h.mu,
		level:       h.level,
		service:     h.service,
		noColor:     h.noColor,
		timeFunc:    h.timeFunc,
		replaceAttr: h.replaceAttr,
		groups:      groups,
		groupPrefix: h.groupPrefix + name + ".",
		attrs:       slices.Clone(h.attrs),
	}
}

func (h *tableHandler) collectAttr(dst *[]scopedAttr, service *string, atRoot bool, groups []string, prefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		subAttrs := a.Value.Group()
		if len(subAttrs) == 0 {
			return
		}
		nextGroups := groups
		nextPrefix := prefix
		nextRoot := atRoot
		if a.Key != "" {
			nextGroups = append(slices.Clone(groups), a.Key)
			nextPrefix = prefix + a.Key + "."
			nextRoot = false
		}
		for _, sub := range subAttrs {
			h.collectAttr(dst, service, nextRoot, nextGroups, nextPrefix, sub)
		}
		return
	}
	if a.Key == "" {
		return
	}
	if atRoot && a.Key == KeyService {
		if s := strings.TrimSpace(a.Value.String()); s != "" {
			*service = s
		}
		return
	}
	*dst = append(*dst, scopedAttr{
		groups: groups,
		prefix: prefix,
		attr:   a,
	})
}

func levelLabel(level slog.Level) string {
	switch {
	case level < slog.LevelInfo:
		return "DEBUG"
	case level < slog.LevelWarn:
		return "INFO"
	case level < slog.LevelError:
		return "WARN"
	default:
		return "ERROR"
	}
}

func levelColor(level slog.Level) string {
	switch {
	case level < slog.LevelInfo:
		return ansiCyan
	case level < slog.LevelWarn:
		return ansiGreen
	case level < slog.LevelError:
		return ansiYellow
	default:
		return ansiRed
	}
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func sanitizeSingleLine(s string) string {
	s = strings.ReplaceAll(s, "\r", `\r`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func formatAttrValue(v slog.Value) string {
	var s string
	switch v.Kind() {
	case slog.KindString:
		s = v.String()
	case slog.KindInt64:
		return strconv.FormatInt(v.Int64(), 10)
	case slog.KindUint64:
		return strconv.FormatUint(v.Uint64(), 10)
	case slog.KindFloat64:
		return strconv.FormatFloat(v.Float64(), 'g', -1, 64)
	case slog.KindBool:
		return strconv.FormatBool(v.Bool())
	case slog.KindDuration:
		s = v.Duration().String()
	case slog.KindTime:
		s = v.Time().UTC().Format(time.RFC3339Nano)
	case slog.KindAny:
		if err, ok := v.Any().(error); ok {
			s = err.Error()
		} else {
			s = fmt.Sprint(v.Any())
		}
	default:
		s = v.String()
	}

	if needsQuoting(s) {
		return strconv.Quote(s)
	}
	return s
}

func needsQuoting(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= ' ' || c == '"' || c == '=' || c == '\\' || c >= 0x7f {
			return true
		}
	}
	return false
}
