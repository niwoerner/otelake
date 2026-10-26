package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/niwoerner/otelake/internal/batcher"
	"github.com/niwoerner/otelake/internal/config"
	"github.com/niwoerner/otelake/internal/query"
	"github.com/niwoerner/otelake/internal/schemas"
	"github.com/niwoerner/otelake/internal/storage"
	"github.com/niwoerner/otelake/internal/writer"
	"golang.org/x/sync/semaphore"
)

var errBodyTooLarge = errors.New("decompressed body too large")

type Server struct {
	cfg       *config.Config
	logger    *slog.Logger
	http      *http.Server
	budget    *semaphore.Weighted // bytes of requests in memory, see readBody
	queries   *query.Engine
	queryGate *semaphore.Weighted

	spans   *batcher.Batcher
	logs    *batcher.Batcher
	metrics *batcher.Batcher
}

func New(cfg *config.Config, logger *slog.Logger) (*Server, error) {
	if cfg.HTTP.MemoryLimitBytes < cfg.HTTP.MaxBodyBytes*memoryPerBodyByte {
		return nil, fmt.Errorf("memory limit %d must hold one request of the max body size (%d bytes)",
			cfg.HTTP.MemoryLimitBytes, cfg.HTTP.MaxBodyBytes*memoryPerBodyByte)
	}

	bucket, err := storage.New(cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("creating storage: %w", err)
	}

	var queries *query.Engine
	if cfg.Storage.Type == "local" {
		queries, err = query.New(cfg.Storage.Local.Dir, cfg.Query)
		if err != nil {
			return nil, fmt.Errorf("creating query engine: %w", err)
		}
	}

	// One batcher and file writer per signal: <signal>/hour=YYYYMMDDHH/<file>.parquet
	newBatcher := func(signal string, schema *arrow.Schema, timeColumns ...string) *batcher.Batcher {
		files := writer.New(bucket, schema, signal, timeColumns, "service_name")
		return batcher.New(files, logger.With("signal", signal))
	}

	s := &Server{
		cfg:       cfg,
		logger:    logger,
		budget:    semaphore.NewWeighted(cfg.HTTP.MemoryLimitBytes),
		queries:   queries,
		queryGate: semaphore.NewWeighted(int64(cfg.Query.MaxConcurrent)),
		spans:     newBatcher("spans", schemas.Spans, "start_time"),
		logs:      newBatcher("logs", schemas.Logs, "time", "observed_time"),
		metrics:   newBatcher("metrics", schemas.Metrics, "time"),
	}
	s.http = &http.Server{Handler: s.routes()}
	return s, nil
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /v1/traces", requireKey(s.cfg.IngestKeys, s.handleTraces))
	mux.HandleFunc("POST /v1/metrics", requireKey(s.cfg.IngestKeys, s.handleMetrics))
	mux.HandleFunc("POST /v1/logs", requireKey(s.cfg.IngestKeys, s.handleLogs))
	mux.HandleFunc("POST /api/query", requireKey(s.cfg.ReadKeys, s.handleQuery))
	return mux
}

// Run binds the listener and serves in the background; it returns once the
// server is accepting connections.
func (s *Server) Run(ctx context.Context) error {
	go s.spans.Run()
	go s.logs.Run()
	go s.metrics.Run()

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

// Shutdown stops accepting requests, waits for in-flight ones and then
// flushes all batchers at the same time, so each gets the whole budget.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.http.Shutdown(ctx)

	batchers := []*batcher.Batcher{s.spans, s.logs, s.metrics}
	errs := make([]error, len(batchers))
	var wg sync.WaitGroup
	for i, b := range batchers {
		wg.Go(func() { errs[i] = b.Close(ctx) })
	}
	wg.Wait()
	if s.queries != nil {
		errs = append(errs, s.queries.Close())
	}
	return errors.Join(append(errs, err)...)
}

// store hands rec to b and reports whether its rows were stored; if not, it
// has already written the error response.
func (s *Server) store(w http.ResponseWriter, b *batcher.Batcher, rec arrow.RecordBatch, signal string) bool {
	switch err := b.Write(rec); {
	case err == nil:
		return true
	case errors.Is(err, batcher.ErrFull):
		writeBusy(w)
	default: // a failed flush (logged by the batcher) or shutting down
		writeError(w, "temporarily unable to store "+signal, http.StatusServiceUnavailable)
	}
	return false
}

// readBody reads the request body, decompressing it according to
// Content-Encoding. The size limit applies before and after decompression.
// The request's memory stays reserved in the budget until release is called.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) (body []byte, release func(), ok bool) {
	limit := s.cfg.HTTP.MaxBodyBytes
	raw := http.MaxBytesReader(w, r.Body, limit)

	var reader io.Reader
	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
		reader = raw
	case "gzip":
		gz, err := gzip.NewReader(raw)
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return nil, nil, false
		}
		defer func() { _ = gz.Close() }()
		reader = gz
	case "zstd":
		zr, err := zstd.NewReader(raw, zstd.WithDecoderMaxMemory(uint64(limit)))
		if err != nil {
			writeError(w, err.Error(), http.StatusBadRequest)
			return nil, nil, false
		}
		defer zr.Close()
		reader = zr
	default:
		writeError(w, "unsupported Content-Encoding", http.StatusUnsupportedMediaType)
		return nil, nil, false
	}

	counted := &budgetReader{r: reader, budget: s.budget}
	release = func() { s.budget.Release(counted.reserved) }

	// Read one byte past the limit to detect an oversized decompressed body.
	data, err := io.ReadAll(io.LimitReader(counted, limit+1))
	if err == nil && int64(len(data)) > limit {
		err = errBodyTooLarge
	}
	// Reserve what decoding and flattening the body will take on top of it.
	if err == nil {
		extra := int64(len(data)) * (memoryPerBodyByte - 1)
		if !s.budget.TryAcquire(extra) {
			err = errOverBudget
		} else {
			counted.reserved += extra
		}
	}

	if err != nil {
		release()
		var tooLarge *http.MaxBytesError
		switch {
		case errors.Is(err, errOverBudget):
			writeBusy(w)
		case errors.As(err, &tooLarge), errors.Is(err, errBodyTooLarge):
			writeError(w, "request body too large", http.StatusRequestEntityTooLarge)
		default:
			writeError(w, err.Error(), http.StatusBadRequest)
		}
		return nil, nil, false
	}
	return data, release, true
}
