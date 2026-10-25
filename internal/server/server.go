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
	"github.com/niwoerner/otelake/internal/batcher"
	"github.com/niwoerner/otelake/internal/config"
	"github.com/niwoerner/otelake/internal/schemas"
	"github.com/niwoerner/otelake/internal/storage"
	"github.com/niwoerner/otelake/internal/writer"
)

type Server struct {
	cfg    *config.Config
	logger *slog.Logger
	http   *http.Server
	spans  *batcher.Batcher
}

func New(cfg *config.Config, logger *slog.Logger) (*Server, error) {
	bucket, err := storage.New(cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("creating storage: %w", err)
	}

	s := &Server{
		cfg:    cfg,
		logger: logger,
		spans: batcher.New(
			writer.New(bucket, schemas.Spans, "spans", "start_time", "service_name"),
			logger.With("signal", "spans"),
		),
	}
	s.http = &http.Server{Handler: s.routes()}
	return s, nil
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
	go s.spans.Run()

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

// Shutdown stops accepting requests, waits for in-flight ones and flushes
// the batchers.
func (s *Server) Shutdown(ctx context.Context) error {
	return errors.Join(s.http.Shutdown(ctx), s.spans.Close(ctx))
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
			writeError(w, err.Error(), http.StatusBadRequest)
			return nil, false
		}
		defer func() { _ = gz.Close() }()
		reader = gz
	case "zstd":
		zr, err := zstd.NewReader(body, zstd.WithDecoderMaxMemory(uint64(limit)))
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return nil, false
		}
		defer zr.Close()
		reader = zr
	default:
		writeError(w, "unsupported Content-Encoding", http.StatusUnsupportedMediaType)
		return nil, false
	}

	// Read one byte past the limit to detect an oversized decompressed body.
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		code := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		writeError(w, err.Error(), code)
		return nil, false
	}
	if int64(len(data)) > limit {
		writeError(w, "decompressed body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}
	return data, true
}
