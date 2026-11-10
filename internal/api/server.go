package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/inexcession/tobiten/internal/api/middleware"
	"github.com/inexcession/tobiten/internal/config"
)

// DefaultShutdownTimeout is the maximum duration to wait for in-flight requests
// to complete during graceful shutdown.
const DefaultShutdownTimeout = 10 * time.Second

// ServerOptions configures the Tobiten HTTP server.
type ServerOptions struct {
	Config   *config.Config
	Logger   *slog.Logger
	Listener net.Listener // optional; if nil, binds Config.Host:Config.Port
}

// Server represents the Tobiten HTTP server daemon.
type Server struct {
	cfg        *config.Config
	logger     *slog.Logger
	listener   net.Listener
	httpServer *http.Server
	mux        *http.ServeMux
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
	registerHealthz(mux)

	// Wrap root router with global RequestLogger middleware
	handler := middleware.RequestLogger(logger)(mux)

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
	}, nil
}

// registerHealthz attaches the unauthenticated GET/HEAD /healthz endpoint.
// It returns 200 OK indicating liveness only, with no version or details (ADR-0006 §1).
func registerHealthz(mux *http.ServeMux) {
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
}

// Addr returns the network address the server is listening on.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.httpServer.Addr
}

// URL returns the local HTTP base URL to communicate with this server.
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

// Mux returns the underlying *http.ServeMux used by the server.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
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
