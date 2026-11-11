package api

import (
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

	// Register /healthz liveness endpoint (ADR-0006 §1 closed allowlist)
	registerHealthz(mux, ub)

	// Mount module routes (ADR-0018 §1, ADR-0020 §6)
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

// registerHealthz attaches the unauthenticated GET/HEAD /healthz endpoint.
// It returns 200 OK indicating liveness only, with no version or details (ADR-0006 §1).
func registerHealthz(mux *http.ServeMux, ub *URLBuilder) {
	healthz := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte("ok\n"))
		}
	}

	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("HEAD /healthz", healthz)

	if ub != nil && ub.SubPath() != "" {
		subHealthz := ub.Path("/healthz")
		mux.HandleFunc("GET "+subHealthz, healthz)
		mux.HandleFunc("HEAD "+subHealthz, healthz)
	}
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

// mountModuleRoutes registers routes from enabled modules in the registry onto mux.
// It ensures module routes agree with generated public URLs and sub-path reverse proxies (ADR-0020 §6).
func mountModuleRoutes(mux *http.ServeMux, reg module.Registry, ub *URLBuilder, logger *slog.Logger) {
	if reg == nil {
		return
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
			mux.Handle("/", guardedHandler)
			if ub != nil && ub.SubPath() != "" {
				subPath := ub.SubPath()
				subPathHandler := stripSubPath(ub, guardedHandler)
				mux.Handle(subPath, subPathHandler)
				mux.Handle(subPath+"/", subPathHandler)
			}
			continue
		}

		mux.Handle(prefix, guardedHandler)
		mux.Handle(prefix+"/", guardedHandler)

		if ub != nil && ub.SubPath() != "" {
			subPrefix := ub.Path(prefix)
			subPrefixHandler := stripSubPath(ub, guardedHandler)
			mux.Handle(subPrefix, subPrefixHandler)
			mux.Handle(subPrefix+"/", subPrefixHandler)
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
