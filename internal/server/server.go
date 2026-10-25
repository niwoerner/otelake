package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"

	"github.com/niwoerner/otelake/internal/config"
)

type Server struct {
	cfg    *config.Config
	logger *slog.Logger
	http   *http.Server
}

func New(cfg *config.Config, logger *slog.Logger) *Server {
	s := &Server{
		cfg:    cfg,
		logger: logger,
	}
	s.http = &http.Server{Handler: s.routes()}
	return s
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /v1/traces", s.handleTraces)
	mux.HandleFunc("POST /v1/metrics", s.handleMetrics)
	mux.HandleFunc("POST /v1/logs", s.handleLogs)
	return mux
}

// Run binds the listener and serves in the background; it returns once the
// server is accepting connections.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.HTTP.Address)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", s.cfg.HTTP.Address, err)
	}
	s.logger.Info("listening", "address", ln.Addr().String())

	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("http server error", "error", err)
		}
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("ok"))
}

// readBody reads the request body up to the configured limit and writes the
// error response if that fails.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.HTTP.MaxBodyBytes))
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return nil, false
	}
	return body, true
}
