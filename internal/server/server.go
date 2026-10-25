package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
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

// readBody reads the request body, decompressing it according to
// Content-Encoding. The size limit applies before and after decompression.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	limit := s.cfg.HTTP.MaxBodyBytes
	body := http.MaxBytesReader(w, r.Body, limit)

	var reader io.Reader
	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
		reader = body
	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, false
		}
		defer func() { _ = gz.Close() }()
		reader = gz
	case "zstd":
		zr, err := zstd.NewReader(body, zstd.WithDecoderMaxMemory(uint64(limit)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return nil, false
		}
		defer zr.Close()
		reader = zr
	default:
		http.Error(w, "unsupported Content-Encoding", http.StatusUnsupportedMediaType)
		return nil, false
	}

	// Read one byte past the limit to detect an oversized decompressed body.
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return nil, false
	}
	if int64(len(data)) > limit {
		http.Error(w, "decompressed body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	return data, true
}
