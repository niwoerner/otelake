package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/niwoerner/otelake/internal/benchdata"
	"github.com/niwoerner/otelake/internal/config"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

// TestHTTP measures two distinct boundaries over real loopback HTTP:
// receive ends after unmarshalling; e2e ends after local Parquet storage.
func TestHTTP(t *testing.T) {
	if os.Getenv("OTELAKE_BENCH_HTTP") != "1" {
		t.Skip("set OTELAKE_BENCH_HTTP=1 to measure HTTP and E2E throughput")
	}
	duration := 5 * time.Second
	if value := os.Getenv("OTELAKE_BENCH_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration <= 0 {
			t.Fatal("OTELAKE_BENCH_DURATION must be a positive duration")
		}
	}
	const rows, clients = 1000, 128
	for _, stage := range []string{"receive", "e2e"} {
		for _, name := range benchdata.Names {
			t.Run(stage+"/"+name, func(t *testing.T) {
				payloads := make([][]byte, clients)
				var d benchdata.Data
				var inputBytes int
				for worker := range clients {
					var err error
					d, err = benchdata.New(name, rows, 1, worker*rows)
					if err != nil {
						t.Fatal(err)
					}
					payloads[worker] = d.Body
					inputBytes += len(d.Body)
				}
				cfg := config.NewConfig()
				cfg.IngestKeys = []string{"benchmark-ingest-key"}
				cfg.Storage.Local.Dir = t.TempDir()
				s, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
				if err != nil {
					t.Fatal(err)
				}
				handler := s.http.Handler
				path := d.Prefix
				if path == "spans" {
					path = "traces"
				}
				if stage == "receive" {
					// An isolated benchmark handler, not a production endpoint:
					// use the real body limits/budget and fresh OTLP decoder, then stop.
					mux := http.NewServeMux()
					mux.HandleFunc("POST /v1/"+path, func(w http.ResponseWriter, r *http.Request) {
						body, release, ok := s.readBody(w, r)
						if !ok {
							return
						}
						defer release()
						var count int
						var err error
						switch d.Prefix {
						case "spans":
							req := ptraceotlp.NewExportRequest()
							err = req.UnmarshalProto(body)
							count = req.Traces().SpanCount()
						case "logs":
							req := plogotlp.NewExportRequest()
							err = req.UnmarshalProto(body)
							count = req.Logs().LogRecordCount()
						case "metrics":
							req := pmetricotlp.NewExportRequest()
							err = req.UnmarshalProto(body)
							count = req.Metrics().DataPointCount()
						}
						if err != nil || count != rows {
							http.Error(w, "invalid benchmark payload", http.StatusBadRequest)
							return
						}
						writeResponse(w, nil) // successful OTLP export response is empty protobuf
					})
					handler = mux
				} else {
					go s.spans.Run()
					go s.logs.Run()
					go s.metrics.Run()
					t.Cleanup(func() {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						if err := s.Shutdown(ctx); err != nil {
							t.Error(err)
						}
					})
				}
				host := httptest.NewServer(handler)
				t.Cleanup(host.Close)
				client := host.Client()
				client.Timeout = 15 * time.Second
				transport := client.Transport.(*http.Transport)
				transport.MaxIdleConns = clients
				transport.MaxIdleConnsPerHost = clients
				t.Cleanup(transport.CloseIdleConnections)
				var mu sync.Mutex
				var requests int
				var totalLatency time.Duration
				statuses := map[int]int{}
				transportErrors := 0
				var wg sync.WaitGroup
				start := time.Now()
				deadline := start.Add(duration)
				for _, body := range payloads {
					wg.Go(func() {
						for time.Now().Before(deadline) {
							req, err := http.NewRequest(http.MethodPost, host.URL+"/v1/"+path, bytes.NewReader(body))
							if err != nil {
								t.Error(err)
								return
							}
							req.Header.Set("Content-Type", "application/x-protobuf")
							req.Header.Set("Authorization", "Bearer "+cfg.IngestKeys[0])
							sent := time.Now()
							resp, err := client.Do(req)
							code := 0
							if err == nil {
								code = resp.StatusCode
								_, err = io.Copy(io.Discard, resp.Body)
								if closeErr := resp.Body.Close(); closeErr != nil {
									err = closeErr
								}
							}
							latency := time.Since(sent)
							mu.Lock()
							if err != nil {
								transportErrors++
							} else {
								statuses[code]++
								if code == http.StatusOK {
									requests++
									totalLatency += latency
								}
							}
							mu.Unlock()
						}
					})
				}
				wg.Wait()
				elapsed := time.Since(start)
				if requests == 0 {
					t.Fatalf("no successes: statuses=%v transport_errors=%d", statuses, transportErrors)
				}
				if stage == "e2e" {
					// Decode stored Parquet outside the load window, so a fast
					// acknowledgement without storage cannot pass the benchmark.
					files, err := filepath.Glob(filepath.Join(cfg.Storage.Local.Dir, d.Prefix, "hour=*", "*.parquet"))
					if err != nil {
						t.Fatal(err)
					}
					var stored int64
					for _, path := range files {
						data, err := os.ReadFile(path)
						if err != nil {
							t.Fatal(err)
						}
						table, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data), parquet.NewReaderProperties(nil), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
						if err != nil {
							t.Fatal(err)
						}
						stored += table.NumRows()
						table.Release()
					}
					if stored != int64(requests*rows) {
						t.Errorf("stored rows = %d, acknowledged rows = %d", stored, requests*rows)
					}
					t.Logf("verified_stored_records=%d files=%d", stored, len(files))
				}
				t.Logf("requests/s=%.2f records/s=%.0f mean_latency_ms=%.2f requests=%d clients=%d records/request=%d mean_request_KiB=%.1f elapsed=%s statuses=%v transport_errors=%d",
					float64(requests)/elapsed.Seconds(), float64(requests*rows)/elapsed.Seconds(), float64(totalLatency)/float64(requests)/float64(time.Millisecond), requests, clients, rows, float64(inputBytes)/clients/1024, elapsed, statuses, transportErrors)
				if transportErrors != 0 || len(statuses) != 1 || statuses[http.StatusOK] != requests {
					t.Error("failed requests; not a clean throughput result")
				}
			})
		}
	}
}
