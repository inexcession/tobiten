package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/api/openapi"
	"github.com/inexcession/tobiten/internal/config"
	"github.com/inexcession/tobiten/internal/module"
)

// DefaultShutdownTimeout is the maximum duration to wait for in-flight requests
// to complete during graceful shutdown.
const DefaultShutdownTimeout = 10 * time.Second

// ServerOptions configures the Tobiten HTTP server.
type ServerOptions struct {
	Config   *config.Config
	Logger   *slog.Logger
	Listener net.Listener    // optional; if nil, binds Config.Host:Config.Port
	Registry module.Registry // optional; if nil, module.NewRegistry() is used
}

// Server represents the Tobiten HTTP server daemon.
type Server struct {
	cfg        *config.Config
	logger     *slog.Logger
	listener   net.Listener
	httpServer *http.Server
	mux        *http.ServeMux
	urlBuilder *URLBuilder
	registry   module.Registry
}

// NewServer initializes a new Server with configured routes and middleware.
func NewServer(opts ServerOptions) (*Server, error) {
	if opts.Config == nil {
		return nil, errors.New("server config is required")
	}
	cfg := opts.Config

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = config.DefaultBaseURL
	}
	ub, err := NewURLBuilder(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	reg := opts.Registry
	if reg == nil {
		reg = module.NewRegistry()
		_ = reg.Register(NewSystemModule())
		_ = reg.Register(openapi.NewModule(nil))
	} else if reg.Module("system") == nil {
		_ = reg.Register(NewSystemModule())
	}
	reg.ApplyConfig(cfg)

	listener := opts.Listener
	if listener == nil {
		addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		listener = ln
	}

	mux := http.NewServeMux()

	// Mount module routes from registry (ADR-0018 §1, ADR-0020 §6).
	// Core liveness (/healthz) and native REST (/api/v1) resolve through the registry.
	mountModuleRoutes(mux, reg, ub, logger)

	// Wrap root router with global RequestLogger and URLBuilder middleware
	handler := middleware.RequestLogger(logger)(URLBuilderMiddleware(ub)(mux))

	httpServer := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	return &Server{
		cfg:        cfg,
		logger:     logger,
		listener:   listener,
		httpServer: httpServer,
		mux:        mux,
		urlBuilder: ub,
		registry:   reg,
	}, nil
}

// stripSubPath wraps h to strip the base URL's sub-path from incoming requests.
// It consistently strips the decoded Path and escaped RawPath so encoded bases
// (such as %20 and %2F) do not cause 404s (ADR-0020 §6).
func stripSubPath(ub *URLBuilder, h http.Handler) http.Handler {
	if ub == nil || ub.SubPath() == "" {
		return h
	}

	decodedSubPath := ub.SubPathDecoded()
	escapedSubPath := ub.SubPath()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, decodedSubPath)
		if len(p) >= len(r.URL.Path) {
			http.NotFound(w, r)
			return
		}

		var rp string
		if r.URL.RawPath != "" {
			rp = strings.TrimPrefix(r.URL.RawPath, escapedSubPath)
			if len(rp) >= len(r.URL.RawPath) {
				http.NotFound(w, r)
				return
			}
		}

		if p == "" {
			p = "/"
		}
		if r.URL.RawPath != "" && rp == "" {
			rp = "/"
		}

		r2 := new(http.Request)
		*r2 = *r
		r2.URL = new(url.URL)
		*r2.URL = *r.URL
		r2.URL.Path = p
		r2.URL.RawPath = rp
		h.ServeHTTP(w, r2)
	})
}

type compositeHandler struct {
	handlers []http.Handler
}

func (c *compositeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if len(c.handlers) == 0 {
		http.NotFound(w, r)
		return
	}
	if len(c.handlers) == 1 {
		c.handlers[0].ServeHTTP(w, r)
		return
	}

	var first404 *fallbackRecorder

	for i, h := range c.handlers {
		if i == len(c.handlers)-1 {
			rec := newFallbackRecorder(w)
			h.ServeHTTP(rec, r)
			if !rec.is404 {
				rec.flush()
				return
			}
			if first404 != nil {
				first404.flush()
			} else {
				rec.flush()
			}
			return
		}

		rec := newFallbackRecorder(w)
		h.ServeHTTP(rec, r)
		if !rec.is404 {
			rec.flush()
			return
		}
		if first404 == nil {
			first404 = rec
		}
	}
}

type fallbackRecorder struct {
	underlying http.ResponseWriter
	header     http.Header
	buf        bytes.Buffer
	statusCode int
	is404      bool
	written    bool
}

func newFallbackRecorder(w http.ResponseWriter) *fallbackRecorder {
	return &fallbackRecorder{
		underlying: w,
		header:     make(http.Header),
		statusCode: http.StatusOK,
	}
}

func (r *fallbackRecorder) Header() http.Header {
	return r.header
}

func (r *fallbackRecorder) WriteHeader(statusCode int) {
	if r.written {
		return
	}
	r.written = true
	r.statusCode = statusCode
	if statusCode == http.StatusNotFound {
		r.is404 = true
	}
}

func (r *fallbackRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.WriteHeader(http.StatusOK)
	}
	return r.buf.Write(b)
}

func (r *fallbackRecorder) flush() {
	for k, vv := range r.header {
		for _, v := range vv {
			r.underlying.Header().Add(k, v)
		}
	}
	r.underlying.WriteHeader(r.statusCode)
	_, _ = r.underlying.Write(r.buf.Bytes())
}

// mountModuleRoutes registers routes from enabled modules in the registry onto mux.
// It ensures module routes agree with generated public URLs and sub-path reverse proxies (ADR-0020 §6),
// and composes direct and BASE_URL-prefixed aliases without duplicate ServeMux pattern panics (ADR-0018 §1).
func mountModuleRoutes(mux *http.ServeMux, reg module.Registry, ub *URLBuilder, logger *slog.Logger) {
	if reg == nil {
		return
	}

	patternHandlers := make(map[string]*compositeHandler)

	registerPattern := func(pattern string, h http.Handler, isDirect bool) {
		if comp, exists := patternHandlers[pattern]; exists {
			if isDirect {
				// Direct module routes precede subpath aliases in fallback resolution order
				comp.handlers = append([]http.Handler{h}, comp.handlers...)
			} else {
				comp.handlers = append(comp.handlers, h)
			}
		} else {
			comp := &compositeHandler{handlers: []http.Handler{h}}
			patternHandlers[pattern] = comp
			mux.Handle(pattern, comp)
		}
	}

	for _, mount := range reg.Routes() {
		prefix := "/" + strings.Trim(mount.Prefix, "/")
		handler := mount.Handler
		if handler == nil {
			continue
		}

		moduleName := mount.ModuleName
		guardedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !reg.IsEnabled(moduleName) {
				http.NotFound(w, r)
				return
			}
			handler.ServeHTTP(w, r)
		})

		logger.Debug("mounting module route",
			slog.String("module", moduleName),
			slog.String("prefix", prefix),
		)

		if prefix == "/" {
			registerPattern("/", guardedHandler, true)
			if ub != nil && ub.SubPath() != "" {
				subPath := ub.SubPath()
				subPathHandler := stripSubPath(ub, guardedHandler)
				registerPattern(subPath, subPathHandler, false)
				registerPattern(subPath+"/", subPathHandler, false)
			}
			continue
		}

		registerPattern(prefix, guardedHandler, true)
		registerPattern(prefix+"/", guardedHandler, true)

		if ub != nil && ub.SubPath() != "" {
			subPrefix := ub.Path(prefix)
			subPrefixHandler := stripSubPath(ub, guardedHandler)
			registerPattern(subPrefix, subPrefixHandler, false)
			registerPattern(subPrefix+"/", subPrefixHandler, false)
		}
	}
}

// Addr returns the network address the server is listening on.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.httpServer.Addr
}

// URL returns the listener-derived local HTTP base URL to communicate with this server in tests.
// Do not use this or Server.Addr() to build public links; use PublicURL() or URLBuilder() instead (ADR-0014 §2, ADR-0020 §6).
func (s *Server) URL() string {
	addr := s.Addr()
	host, port, err := net.SplitHostPort(addr)
	if err == nil {
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		return fmt.Sprintf("http://%s:%s", host, port)
	}
	return "http://" + addr
}

// URLBuilder returns the URLBuilder configured for this server.
func (s *Server) URLBuilder() *URLBuilder {
	return s.urlBuilder
}

// BaseURL returns the configured public base URL without trailing slashes.
func (s *Server) BaseURL() string {
	return s.urlBuilder.BaseURL()
}

// PublicURL builds an absolute public URL using the server's configured TOBITEN_BASE_URL.
// It never consults the listener address, server port, or forwarded headers (ADR-0014 §2, ADR-0020 §6).
func (s *Server) PublicURL(ref string) string {
	return s.urlBuilder.URL(ref)
}

// Mux returns the underlying *http.ServeMux used by the server.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// Registry returns the module registry wired to this server.
func (s *Server) Registry() module.Registry {
	return s.registry
}

// Handler returns the root HTTP handler of the server (including middleware).
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

// Serve starts accepting connections on the server's listener.
// It blocks until the server is closed or Shutdown is invoked.
// On clean shutdown, it returns http.ErrServerClosed.
func (s *Server) Serve() error {
	return s.httpServer.Serve(s.listener)
}

// Shutdown gracefully shuts down the server without interrupting active connections,
// waiting until ctx expires for in-flight requests to complete.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Close immediately closes the listener and active connections.
func (s *Server) Close() error {
	return s.httpServer.Close()
}

// Run serves incoming requests until ctx is cancelled, at which point it initiates
// graceful shutdown with DefaultShutdownTimeout.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("starting http server",
			slog.String("addr", s.Addr()),
		)
		if err := s.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.logger.Info("shutting down http server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
		defer cancel()
		if err := s.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http server shutdown: %w", err)
		}
		s.logger.Info("http server stopped cleanly")
		return nil
	}
}
