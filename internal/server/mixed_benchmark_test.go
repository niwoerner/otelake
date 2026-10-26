package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/niwoerner/otelake/internal/benchdata"
	"github.com/niwoerner/otelake/internal/config"
)

// TestMixedSignals offers a fixed load, rather than waiting for each response
// before sending more work. It tests one scenario, not maximum capacity.
func TestMixedSignals(t *testing.T) {
	if os.Getenv("OTELAKE_BENCH_MIXED") != "1" {
		t.Skip("set OTELAKE_BENCH_MIXED=1 to run the mixed-signal load scenario")
	}
	duration := 2 * time.Minute
	if value := os.Getenv("OTELAKE_BENCH_DURATION"); value != "" {
		var err error
		duration, err = time.ParseDuration(value)
		if err != nil || duration < time.Second || duration%time.Second != 0 {
			t.Fatal("OTELAKE_BENCH_DURATION must be a positive whole number of seconds")
		}
	}
	const rows, rate, concurrency = 200, 50, 256
	const interval = time.Second / rate
	warmup := min(10*time.Second, duration)
	mix := []string{"traces", "logs-text", "traces", "metrics-numeric", "logs-text", "traces", "logs-structured", "traces", "metrics-buckets", "logs-text"}
	type request struct {
		body     []byte
		prefix   string
		at       int64
		code     int
		err      error
		lag      time.Duration
		latency  time.Duration
		finished time.Time
	}
	requests := make([]request, int((warmup+duration)/interval))
	warmupRequests := int(warmup / interval)
	base := time.Now().Add(-warmup - duration - time.Second).Truncate(interval)
	var rawBytes, wireBytes int64
	// Generation and compression are outside the load window. No request is replayed.
	for i := range requests {
		name := mix[i%len(mix)]
		at := base.Add(time.Duration(i) * interval)
		if i%19 == 0 {
			// Keep late timestamps outside the normal range even on long runs.
			at = at.Add(-warmup - duration - 10*time.Minute)
		}
		body, err := benchdata.Mixed(name, rows, i*rows, at)
		if err != nil {
			t.Fatal(err)
		}
		var compressed bytes.Buffer
		gz := gzip.NewWriter(&compressed)
		if _, err := gz.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
		prefix := "metrics"
		switch name {
		case "traces":
			prefix = "spans"
		case "logs-text", "logs-structured":
			prefix = "logs"
		}
		requests[i] = request{body: compressed.Bytes(), prefix: prefix, at: at.UnixNano()}
		if i >= warmupRequests {
			rawBytes += int64(len(body))
			wireBytes += int64(compressed.Len())
		}
	}
	cfg := config.NewConfig()
	cfg.IngestKeys = []string{"benchmark-ingest-key"}
	cfg.Storage.Local.Dir = t.TempDir()
	s, err := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
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
	host := httptest.NewServer(s.http.Handler)
	t.Cleanup(host.Close)
	client := host.Client()
	client.Timeout = 15 * time.Second
	transport := client.Transport.(*http.Transport)
	transport.MaxIdleConns, transport.MaxIdleConnsPerHost = concurrency, concurrency
	t.Cleanup(transport.CloseIdleConnections)

	var pending, completed atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)
	runtime.GC()
	start := time.Now().Add(100 * time.Millisecond)
	measurementStart, end := start.Add(warmup), start.Add(warmup+duration)
	type sample struct {
		elapsed            time.Duration
		rss, heap          uint64
		pending, completed int64
	}
	var samples []sample
	stopSampling, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			var rss uint64
			if data, err := os.ReadFile("/proc/self/statm"); err == nil {
				fields := strings.Fields(string(data))
				if len(fields) > 1 {
					pages, _ := strconv.ParseUint(fields[1], 10, 64)
					rss = pages * uint64(os.Getpagesize())
				}
			}
			samples = append(samples, sample{time.Since(measurementStart), rss, mem.HeapAlloc, pending.Load(), completed.Load()})
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
			}
		}
	}()
	missed := 0
	for i := range requests {
		due := start.Add(time.Duration(i) * interval)
		time.Sleep(time.Until(due))
		// Do not turn generator stalls into a catch-up burst or hide overload
		// by slowing down the offered rate. Missed slots invalidate a clean run.
		if time.Since(due) >= interval {
			missed++
			continue
		}
		select {
		case sem <- struct{}{}:
		default:
			missed++
			continue
		}
		pending.Add(1)
		wg.Go(func() {
			defer func() { <-sem; pending.Add(-1) }()
			r := &requests[i]
			path := r.prefix
			if path == "spans" {
				path = "traces"
			}
			req, err := http.NewRequest(http.MethodPost, host.URL+"/v1/"+path, bytes.NewReader(r.body))
			if err == nil {
				req.Header.Set("Content-Type", "application/x-protobuf")
				req.Header.Set("Content-Encoding", "gzip")
				req.Header.Set("Authorization", "Bearer "+cfg.IngestKeys[0])
				r.lag = time.Since(due)
				var resp *http.Response
				resp, err = client.Do(req)
				if err == nil {
					r.code = resp.StatusCode
					_, err = io.Copy(io.Discard, resp.Body)
					if closeErr := resp.Body.Close(); closeErr != nil {
						err = closeErr
					}
				}
			}
			r.err, r.finished = err, time.Now()
			r.latency = r.finished.Sub(due)
			if i >= warmupRequests && err == nil && r.code == http.StatusOK {
				completed.Add(rows)
			}
		})
	}
	time.Sleep(time.Until(end))
	pendingAtEnd := pending.Load()
	wg.Wait() // each request has a 15-second client deadline
	drain := time.Since(end)
	close(stopSampling)
	<-sampled

	var latencies, lags []time.Duration
	statuses := map[int]int{}
	var acknowledged, inWindow, transportErrors, warmupFailures int
	for i, r := range requests {
		if r.err == nil && r.code == http.StatusOK && !r.finished.Before(measurementStart) && !r.finished.After(end) {
			inWindow++
		}
		if i < warmupRequests {
			if r.err != nil || r.code != http.StatusOK {
				warmupFailures++
			}
			continue
		}
		statuses[r.code]++
		if r.err != nil {
			transportErrors++
		}
		if r.err == nil && r.code == http.StatusOK {
			acknowledged++
			latencies = append(latencies, r.latency)
			lags = append(lags, r.lag)
		}
	}
	t.Logf("target_records/s=%d measured_window=%s warmup=%s acknowledged_records/s_in_window=%.1f acknowledged_records/s_including_drain=%.1f offered_requests=%d acknowledged_requests=%d missed_slots=%d statuses=%v transport_errors=%d warmup_failures=%d pending_at_end=%d drain=%s",
		rows*rate, duration, warmup, float64(inWindow*rows)/duration.Seconds(), float64(acknowledged*rows)/(duration+drain).Seconds(), len(requests)-warmupRequests, acknowledged, missed, statuses, transportErrors, warmupFailures, pendingAtEnd, drain)
	if len(latencies) > 0 {
		slices.Sort(latencies)
		slices.Sort(lags)
		for _, p := range []int{50, 95, 99} {
			index := (len(latencies)*p+99)/100 - 1
			t.Logf("p%d_latency=%s p%d_dispatch_lag=%s", p, latencies[index], p, lags[index])
		}
	}
	var peakRSS, peakHeap uint64
	var peakPending int64
	for _, sample := range samples {
		peakRSS, peakHeap = max(peakRSS, sample.rss), max(peakHeap, sample.heap)
		peakPending = max(peakPending, sample.pending)
		if sample.elapsed >= 0 {
			t.Logf("sample elapsed=%s completed_records=%d in_flight_requests=%d process_RSS_MiB=%.1f process_heap_MiB=%.1f", sample.elapsed.Round(time.Second), sample.completed, sample.pending, float64(sample.rss)/(1<<20), float64(sample.heap)/(1<<20))
		}
	}
	t.Logf("process_baseline_RSS_MiB=%.1f process_peak_RSS_MiB=%.1f process_peak_heap_MiB=%.1f sampled_peak_in_flight=%d raw_MiB/s=%.2f gzip_MiB/s=%.2f GOMAXPROCS=%d",
		float64(samples[0].rss)/(1<<20), float64(peakRSS)/(1<<20), float64(peakHeap)/(1<<20), peakPending, float64(rawBytes)/duration.Seconds()/(1<<20), float64(wireBytes)/duration.Seconds()/(1<<20), runtime.GOMAXPROCS(0))
	if missed != 0 || transportErrors != 0 || warmupFailures != 0 || acknowledged != len(requests)-warmupRequests {
		t.Error("load was not delivered and acknowledged cleanly; not a sustainable-load result")
	}

	// Decode every file and verify every acknowledged record exactly once.
	// Counting rows alone could hide one dropped request plus one duplicate.
	requestByTime := make(map[int64]int, len(requests))
	for i, r := range requests {
		requestByTime[r.at] = i
	}
	seen := make([]bool, len(requests)*rows)
	var stored, files int
	var diskBytes int64
	for _, prefix := range []string{"spans", "logs", "metrics"} {
		paths, err := filepath.Glob(filepath.Join(cfg.Storage.Local.Dir, prefix, "hour=*", "*.parquet"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			diskBytes += int64(len(data))
			files++
			table, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data), parquet.NewReaderProperties(nil), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
			if err != nil {
				t.Fatal(err)
			}
			column := "time"
			if prefix == "spans" {
				column = "start_time"
			}
			indices := table.Schema().FieldIndices(column)
			if len(indices) != 1 {
				table.Release()
				t.Fatalf("missing timestamp column in %s", path)
			}
			for _, chunk := range table.Column(indices[0]).Data().Chunks() {
				values := chunk.(*array.Timestamp)
				for row := 0; row < values.Len(); row++ {
					ts := int64(values.Value(row))
					at := ts - ts%int64(interval)
					i, ok := requestByTime[at]
					ordinal := int(ts - at)
					if values.IsNull(row) || !ok || ordinal >= rows || requests[i].prefix != prefix {
						table.Release()
						t.Fatalf("unexpected record timestamp %d in %s", ts, path)
					}
					id := i*rows + ordinal
					if seen[id] {
						table.Release()
						t.Fatalf("duplicate record %d in %s", id, path)
					}
					seen[id] = true
					stored++
				}
			}
			table.Release()
		}
	}
	for i, r := range requests {
		for row := range rows {
			if seen[i*rows+row] != (r.err == nil && r.code == http.StatusOK) {
				t.Fatalf("record %d storage does not match acknowledgement", i*rows+row)
			}
		}
	}
	t.Logf("verified_unique_records=%d files=%d parquet_MiB=%.2f (includes warmup)", stored, files, float64(diskBytes)/(1<<20))
}
