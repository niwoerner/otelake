package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/niwoerner/otelake/internal/config"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

const maxBody = 1 << 16

// newTestServer runs a server storing into a temp dir, with a memory budget
// that holds exactly one maximum-size request.
func newTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	cfg := config.NewConfig()
	cfg.HTTP.Address = "127.0.0.1:0"
	cfg.HTTP.MaxBodyBytes = maxBody
	cfg.HTTP.MemoryLimitBytes = maxBody * memoryPerBodyByte
	cfg.Storage.Local.Dir = t.TempDir()

	s, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	return s, cfg.Storage.Local.Dir
}

func tracesBody(t *testing.T) []byte {
	t.Helper()
	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("GET /cart")
	span.SetStartTimestamp(1)
	body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

func post(s *Server, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	return rec
}

func TestStoresTracesAndReleasesBudget(t *testing.T) {
	s, dir := newTestServer(t)

	resp := post(s, tracesBody(t))

	if resp.Code != http.StatusOK || resp.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("response = %d %q, want 200 application/x-protobuf", resp.Code, resp.Header().Get("Content-Type"))
	}
	files, _ := filepath.Glob(filepath.Join(dir, "spans", "hour=*", "*.parquet"))
	if len(files) != 1 {
		t.Errorf("parquet files = %v, want 1", files)
	}
	limit := s.cfg.HTTP.MemoryLimitBytes
	if !s.budget.TryAcquire(limit) {
		t.Fatal("budget not fully released after the request")
	}
	s.budget.Release(limit)
}

func TestRejectsRequestsOverMemoryBudget(t *testing.T) {
	s, _ := newTestServer(t)
	limit := s.cfg.HTTP.MemoryLimitBytes
	s.budget.TryAcquire(limit) // other requests hold the whole budget
	defer s.budget.Release(limit)

	resp := post(s, tracesBody(t))

	if resp.Code != http.StatusTooManyRequests || resp.Header().Get("Retry-After") == "" {
		t.Errorf("response = %d, Retry-After %q, want 429 with Retry-After", resp.Code, resp.Header().Get("Retry-After"))
	}
}

func TestRejectsOversizedBodies(t *testing.T) {
	s, _ := newTestServer(t)

	resp := post(s, make([]byte, maxBody+1))

	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("response = %d, want 413", resp.Code)
	}
	limit := s.cfg.HTTP.MemoryLimitBytes
	if !s.budget.TryAcquire(limit) {
		t.Fatal("budget not released after a rejected request")
	}
	s.budget.Release(limit)
}

func TestNewRejectsMemoryLimitBelowOneRequest(t *testing.T) {
	cfg := config.NewConfig()
	cfg.HTTP.MemoryLimitBytes = cfg.HTTP.MaxBodyBytes

	if _, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("New accepted a memory limit that can't hold one maximum-size request")
	}
}
