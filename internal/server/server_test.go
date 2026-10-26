package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/niwoerner/otelake/internal/config"
	"github.com/niwoerner/otelake/internal/query"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

const maxBody = 1 << 16

// newTestServer runs a server storing into a temp dir, with a memory budget
// that holds exactly one maximum-size request.
func newTestServer(t *testing.T, configure ...func(*config.Config)) (*Server, string) {
	t.Helper()
	cfg := config.NewConfig()
	cfg.IngestKeys = []string{"test-ingest-key", "rotated-ingest-key"}
	cfg.ReadKeys = []string{"test-read-key"}
	cfg.HTTP.Address = "127.0.0.1:0"
	cfg.HTTP.MaxBodyBytes = maxBody
	cfg.HTTP.MemoryLimitBytes = maxBody * memoryPerBodyByte
	cfg.Storage.Local.Dir = t.TempDir()
	for _, apply := range configure {
		apply(cfg)
	}

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
	req.Header.Set("Authorization", "Bearer test-ingest-key")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	return rec
}

func TestIngestAuthentication(t *testing.T) {
	s, _ := newTestServer(t)
	for _, path := range []string{"/v1/traces", "/v1/logs", "/v1/metrics"} {
		t.Run(path, func(t *testing.T) {
			for _, tt := range []struct {
				name   string
				values []string
				status int
			}{
				{"missing", nil, http.StatusUnauthorized},
				{"unknown", []string{"Bearer unknown"}, http.StatusUnauthorized},
				{"read-only key", []string{"Bearer test-read-key"}, http.StatusUnauthorized},
				{"empty", []string{"Bearer "}, http.StatusUnauthorized},
				{"wrong scheme", []string{"Basic test-ingest-key"}, http.StatusUnauthorized},
				{"extra token", []string{"Bearer test-ingest-key extra"}, http.StatusUnauthorized},
				{"duplicate", []string{"Bearer test-ingest-key", "Bearer unknown"}, http.StatusUnauthorized},
				{"combined", []string{"Bearer test-ingest-key, Bearer unknown"}, http.StatusUnauthorized},
				{"valid", []string{"Bearer test-ingest-key"}, http.StatusOK},
				{"rotated", []string{"Bearer rotated-ingest-key"}, http.StatusOK},
				{"case insensitive scheme", []string{"bearer test-ingest-key"}, http.StatusOK},
			} {
				t.Run(tt.name, func(t *testing.T) {
					// Empty protobuf is a valid, empty export for all three signals.
					req := httptest.NewRequest(http.MethodPost, path, nil)
					req.Header.Set("Content-Type", "application/x-protobuf")
					for _, value := range tt.values {
						req.Header.Add("Authorization", value)
					}
					rec := httptest.NewRecorder()
					s.http.Handler.ServeHTTP(rec, req)
					if rec.Code != tt.status {
						t.Fatalf("status = %d, want %d", rec.Code, tt.status)
					}
					if tt.status == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
						t.Error("missing bearer challenge")
					}
				})
			}
		})
	}

	for _, keys := range [][]string{nil, {}, {""}} {
		s.cfg.IngestKeys = keys
		s.http.Handler = s.routes()
		if resp := post(s, nil); resp.Code != http.StatusUnauthorized {
			t.Errorf("keys %q: status = %d, want 401", keys, resp.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("health = %d %q, want 200 ok", rec.Code, rec.Body.String())
	}
}

func TestUnauthorizedRequestsDoNotReadBody(t *testing.T) {
	s, dir := newTestServer(t)
	for _, path := range []string{"/v1/traces", "/v1/logs", "/v1/metrics", "/api/query"} {
		body := bytes.NewBuffer(make([]byte, maxBody+1))
		req := httptest.NewRequest(http.MethodPost, path, body)
		req.Header.Set("Content-Encoding", "gzip") // invalid gzip must not be decoded
		rec := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized || body.Len() != maxBody+1 {
			t.Errorf("%s: status = %d, unread bytes = %d, want 401 and %d", path, rec.Code, body.Len(), maxBody+1)
		}
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*", "hour=*", "*.parquet"))
	if len(files) != 0 {
		t.Errorf("unauthorized requests stored files: %v", files)
	}
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

func postQuery(t *testing.T, s *Server, statement string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]string{"sql": statement})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/api/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-read-key")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	return rec
}

func TestQueryIngestedTelemetry(t *testing.T) {
	s, _ := newTestServer(t)
	// Discovering views before ingestion must not prevent later data appearing.
	if resp := postQuery(t, s, "SELECT table_name FROM information_schema.views WHERE table_name='spans'"); resp.Code != 200 || !strings.Contains(resp.Body.String(), `"rows":[]`) {
		t.Fatalf("before ingestion: %d %s", resp.Code, resp.Body.String())
	}
	at := time.Date(2026, 10, 26, 12, 0, 0, 123456789, time.UTC)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i := 0; i < 3; i++ {
		span := spans.AppendEmpty()
		span.SetName("POST /cart")
		span.SetTraceID(pcommon.TraceID{15: 42})
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(i))))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(i) + time.Millisecond)))
		span.Attributes().PutInt("exact", 9007199254740993)
		span.Status().SetCode(ptrace.StatusCodeError)
	}
	body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= 2; round++ {
		if resp := post(s, body); resp.Code != 200 {
			t.Fatalf("ingest: %d %s", resp.Code, resp.Body.String())
		}
		resp := postQuery(t, s, fmt.Sprintf(`WITH selected AS (SELECT * FROM spans WHERE epoch_ns(start_time) >= %d AND epoch_ns(start_time) < %d)
			SELECT service_name, count(*) AS n FROM selected WHERE status_code=2 GROUP BY service_name`, at.UnixNano(), at.UnixNano()+2))
		want := fmt.Sprintf(`{"columns":["service_name","n"],"rows":[["checkout",%d]],"truncated":false}`, round*2)
		if resp.Code != 200 || resp.Body.String() != want {
			t.Fatalf("aggregation round %d: %d %s, want %s", round, resp.Code, resp.Body.String(), want)
		}
	}
	resp := postQuery(t, s, `SELECT start_time, epoch_ns(start_time) AS ns, attrs_int, hex(trace_id) AS id, parent_span_id FROM spans ORDER BY start_time LIMIT 1`)
	want := fmt.Sprintf(`{"columns":["start_time","ns","attrs_int","id","parent_span_id"],"rows":[["2026-10-26 12:00:00.123456789+00",%d,{"exact":9007199254740993},"0000000000000000000000000000002A",null]],"truncated":false}`, at.UnixNano())
	if resp.Code != 200 || resp.Body.String() != want {
		t.Fatalf("precision/nested/null: %d %s, want %s", resp.Code, resp.Body.String(), want)
	}
	if resp := postQuery(t, s, `SELECT * FROM spans LIMIT 1`); resp.Code != 200 || !json.Valid(resp.Body.Bytes()) {
		t.Fatalf("all stored columns: %d %s", resp.Code, resp.Body.String())
	}
	logs := plog.NewLogs()
	lr := logs.ResourceLogs().AppendEmpty()
	lr.Resource().Attributes().PutStr("service.name", "checkout")
	lr.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("payment declined")
	logBody, err := plogotlp.NewExportRequestFromLogs(logs).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "checkout")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("cart.total")
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(12.5)
	metricBody, err := pmetricotlp.NewExportRequestFromMetrics(metrics).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	for path, payload := range map[string][]byte{"/v1/logs": logBody, "/v1/metrics": metricBody} {
		req := httptest.NewRequest("POST", path, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Authorization", "Bearer test-ingest-key")
		rec := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("%s: %d %s", path, rec.Code, rec.Body.String())
		}
	}
	resp = postQuery(t, s, `SELECT l.body_str, m.name, m.value_f64 FROM logs l JOIN metrics m USING (service_name)`)
	if resp.Code != 200 || resp.Body.String() != `{"columns":["body_str","name","value_f64"],"rows":[["payment declined","cart.total",12.5]],"truncated":false}` {
		t.Fatalf("cross-signal join: %d %s", resp.Code, resp.Body.String())
	}
}

func TestQueryValidationAndAuthentication(t *testing.T) {
	s, _ := newTestServer(t)
	for _, tt := range []struct {
		body, key, contentType string
		status                 int
	}{
		{`{"sql":"SELECT 42 AS answer"}`, "test-read-key", "application/json", 200},
		{`{"sql":"SELECT 42"}`, "test-ingest-key", "application/json", 401},
		{`{"sql":"SELECT 42"}`, "", "application/json", 401},
		{`{"sql":"SELECT 42"}`, "test-read-key", "text/plain", 415},
		{`{}`, "test-read-key", "application/json", 400},
		{`null`, "test-read-key", "application/json", 400},
		{`{"sql":"SELECT 1","unknown":true}`, "test-read-key", "application/json", 400},
		{`{"sql":"SELECT 1"} {}`, "test-read-key", "application/json", 400},
		{`{"sql":"` + strings.Repeat("x", maxQueryBodyBytes) + `"}`, "test-read-key", "application/json", 413},
	} {
		req := httptest.NewRequest("POST", "/api/query", strings.NewReader(tt.body))
		req.Header.Set("Authorization", "Bearer "+tt.key)
		req.Header.Set("Content-Type", tt.contentType)
		rec := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(rec, req)
		if rec.Code != tt.status {
			t.Errorf("body %.80s key %q: %d %s, want %d", tt.body, tt.key, rec.Code, rec.Body.String(), tt.status)
		}
	}
	for _, keys := range [][]string{nil, {}, {""}} {
		s.cfg.ReadKeys = keys
		s.http.Handler = s.routes()
		if resp := postQuery(t, s, "SELECT 42"); resp.Code != 401 {
			t.Errorf("read keys %q: %d, want 401", keys, resp.Code)
		}
	}
}

func TestQueryRejectsWritesAndExternalAccess(t *testing.T) {
	s, dir := newTestServer(t)
	outside := filepath.Join(t.TempDir(), "private.txt")
	if err := os.WriteFile(outside, []byte("private fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "written.csv")
	for _, statement := range []string{
		"SELECT 1; SELECT 2", "CREATE TABLE injected(i INT); SELECT 1",
		"CREATE TABLE injected(i INT)", "DROP VIEW spans", "DELETE FROM spans",
		"SET enable_external_access=true", "PRAGMA threads=8", "INSTALL httpfs", "LOAD httpfs",
		fmt.Sprintf("COPY (SELECT 1) TO '%s'", output),
		fmt.Sprintf("SELECT * FROM read_text('%s')", outside),
		fmt.Sprintf("SELECT * FROM read_text('%s/../%s/private.txt')", dir, filepath.Base(filepath.Dir(outside))),
		"SELECT * FROM read_text('https://example.com/private.txt')",
		"SELECT * FROM query('SET enable_external_access=true')",
	} {
		resp := postQuery(t, s, statement)
		if resp.Code != 400 {
			t.Errorf("%s: %d %s, want 400", statement, resp.Code, resp.Body.String())
		}
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Errorf("COPY created a file or failed to stat: %v", err)
	}
	resp := postQuery(t, s, `SELECT table_name FROM information_schema.tables WHERE table_name='injected'`)
	if resp.Code != 200 || !strings.Contains(resp.Body.String(), `"rows":[]`) {
		t.Fatalf("multi-statement side effect: %d %s", resp.Code, resp.Body.String())
	}
}

func TestQueryResultLimits(t *testing.T) {
	s, _ := newTestServer(t)
	for _, n := range []int{0, 500, 501} {
		resp := postQuery(t, s, fmt.Sprintf("SELECT i FROM range(%d) t(i) ORDER BY i", n))
		var result query.Result
		if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if resp.Code != 200 || !reflect.DeepEqual(result.Columns, []string{"i"}) || len(result.Rows) != min(n, 500) || result.Rows == nil || result.Truncated != (n > 500) {
			t.Fatalf("range(%d): %d rows %d truncated %t", n, resp.Code, len(result.Rows), result.Truncated)
		}
		if n != 0 && (string(result.Rows[0][0]) != "0" || string(result.Rows[499][0]) != "499") {
			t.Fatal("unexpected row values or order")
		}
	}
	for _, tt := range []struct {
		sql    string
		status int
	}{
		{`SELECT repeat('x', 1048576)`, 422},
		{`SELECT repeat('<', 200000)`, 422}, // JSON escaping also counts toward the cap.
		{`SELECT 7 AS answer`, 200},
		{`SELECT 'NaN'::DOUBLE`, 400},
	} {
		if resp := postQuery(t, s, tt.sql); resp.Code != tt.status {
			t.Errorf("%s: %d %s, want %d", tt.sql, resp.Code, resp.Body.String(), tt.status)
		}
	}
}

func TestQueryConcurrencyDoesNotBlockIngestion(t *testing.T) {
	s, _ := newTestServer(t)
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	req := httptest.NewRequest("POST", "/api/query", reader)
	req.Header.Set("Authorization", "Bearer test-read-key")
	req.Header.Set("Content-Type", "application/json")
	done := make(chan struct{})
	go func() { defer close(done); s.http.Handler.ServeHTTP(httptest.NewRecorder(), req) }()
	if _, err := writer.Write([]byte(`{"sql":`)); err != nil {
		t.Fatal(err)
	}
	resp := postQuery(t, s, "SELECT 42")
	if resp.Code != 429 || resp.Header().Get("Retry-After") == "" {
		t.Errorf("busy query: %d", resp.Code)
	}
	if resp := post(s, nil); resp.Code != 200 {
		t.Errorf("ingest during query: %d", resp.Code)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked query did not exit")
	}
	if resp := postQuery(t, s, "SELECT 42"); resp.Code != 200 {
		t.Errorf("query after release: %d", resp.Code)
	}
}

func TestQueryConcurrentReaders(t *testing.T) {
	s, _ := newTestServer(t, func(cfg *config.Config) { cfg.Query.MaxConcurrent = 2 })
	if resp := post(s, tracesBody(t)); resp.Code != 200 {
		t.Fatal(resp.Body.String())
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for range 20 {
				resp := postQuery(t, s, "SELECT count(*) AS n FROM spans")
				if resp.Code != 200 || resp.Body.String() != `{"columns":["n"],"rows":[[1]],"truncated":false}` {
					t.Errorf("concurrent query: %d %s", resp.Code, resp.Body.String())
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestQueryCancellationAndUnsupportedStorage(t *testing.T) {
	s, _ := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest("POST", "/api/query", strings.NewReader(`{"sql":"SELECT 42"}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer test-read-key")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, req)
	if rec.Code != 408 {
		t.Errorf("canceled query: %d", rec.Code)
	}
	s.cfg.Query.Timeout = 20 * time.Millisecond
	if resp := postQuery(t, s, "SELECT sum(a.i*b.i) FROM range(1000000) a(i), range(1000000) b(i)"); resp.Code != 504 {
		t.Errorf("expensive query: %d %s", resp.Code, resp.Body.String())
	}
	s.cfg.Query.Timeout = time.Second
	if resp := postQuery(t, s, "SELECT 42"); resp.Code != 200 {
		t.Errorf("after cancellation: %d %s", resp.Code, resp.Body.String())
	}
	s3, _ := newTestServer(t, func(cfg *config.Config) {
		cfg.Storage.Type = "s3"
		cfg.Storage.S3.Endpoint = "localhost:9000"
		cfg.Storage.S3.Bucket = "test"
	})
	if resp := postQuery(t, s3, "SELECT 42"); resp.Code != 503 {
		t.Errorf("S3 retrieval: %d", resp.Code)
	}
	if resp := post(s3, nil); resp.Code != 200 {
		t.Errorf("S3 empty ingest: %d", resp.Code)
	}
}

func TestQuerySlowBodyDeadline(t *testing.T) {
	s, _ := newTestServer(t)
	s.cfg.Query.Timeout = 100 * time.Millisecond
	httpServer := httptest.NewServer(s.http.Handler)
	defer httpServer.Close()
	conn, err := net.Dial("tcp", httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = io.WriteString(conn, "POST /api/query HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer test-read-key\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"sql\":")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 504 {
		t.Errorf("slow body status %d, want 504", resp.StatusCode)
	}
}

func TestQuerySlowReaderReleasesSlot(t *testing.T) {
	s, _ := newTestServer(t)
	s.cfg.Query.Timeout = 100 * time.Millisecond
	httpServer := httptest.NewUnstartedServer(s.http.Handler)
	httpServer.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateNew {
			if err := conn.(*net.TCPConn).SetWriteBuffer(1024); err != nil {
				t.Error(err)
			}
		}
	}
	httpServer.Start()
	defer httpServer.Close()
	conn, err := net.DialTCP("tcp", nil, httpServer.Listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	body := `{"sql":"SELECT repeat('x', 900000) AS value"}`
	_, err = fmt.Fprintf(conn, "POST /api/query HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer test-read-key\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	if err != nil {
		t.Fatal(err)
	}
	// Read only the headers, then leave the large response blocked on TCP.
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("large query status %d, want 200", resp.StatusCode)
	}
	time.Sleep(5 * s.cfg.Query.Timeout)
	if next := postQuery(t, s, "SELECT 42 AS answer"); next.Code != 200 || next.Body.String() != `{"columns":["answer"],"rows":[[42]],"truncated":false}` {
		t.Fatalf("query while stalled client remains connected: %d %s", next.Code, next.Body.String())
	}
}

func TestQueryWriteDeadlineAllowsConnectionReuse(t *testing.T) {
	s, _ := newTestServer(t)
	s.cfg.Query.Timeout = 100 * time.Millisecond
	httpServer := httptest.NewServer(s.http.Handler)
	defer httpServer.Close()
	conn, err := net.Dial("tcp", httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	body := `{"sql":"SELECT 42 AS answer"}`
	_, err = fmt.Fprintf(conn, "POST /api/query HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer test-read-key\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(resp.Body)
	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || resp.StatusCode != 200 || string(data) != `{"columns":["answer"],"rows":[[42]],"truncated":false}` {
		t.Fatalf("query response: %d %s, error %v", resp.StatusCode, data, err)
	}
	time.Sleep(2 * s.cfg.Query.Timeout)
	// Health does not set a write deadline, so a stale query deadline would fail.
	if _, err := io.WriteString(conn, "GET /health HTTP/1.1\r\nHost: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err = http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err = io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 || string(data) != "ok" {
		t.Fatalf("reused connection: %d %s, error %v", resp.StatusCode, data, err)
	}
}
