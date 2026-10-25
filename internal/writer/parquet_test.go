package writer

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/niwoerner/otelake/internal/logs"
	"github.com/niwoerner/otelake/internal/schemas"
	"github.com/niwoerner/otelake/internal/traces"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var hour14 = time.Date(2026, 10, 25, 14, 0, 0, 0, time.UTC)

// memBucket is an in-memory Bucket whose Put fails from the failAt-th call on.
type memBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    int
	failAt  int // 0 = never fail
}

func newMemBucket() *memBucket { return &memBucket{objects: map[string][]byte{}} }

func (m *memBucket) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts++
	if m.failAt > 0 && m.puts >= m.failAt {
		return errors.New("put failed")
	}
	m.objects[key] = data
	return nil
}

func (m *memBucket) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *memBucket) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

type span struct {
	service string
	start   time.Time
}

func newWriter(t *testing.T, bucket *memBucket) (*Writer, memory.Allocator) {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	w := New(bucket, schemas.Spans, "spans", []string{"start_time"}, "service_name")
	w.mem = mem
	return w, mem
}

func record(t *testing.T, mem memory.Allocator, spans ...span) arrow.RecordBatch {
	t.Helper()
	td := ptrace.NewTraces()
	for _, s := range spans {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", s.service)
		sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		if !s.start.IsZero() {
			sp.SetStartTimestamp(pcommon.NewTimestampFromTime(s.start))
		}
	}
	rec, err := traces.Flatten(mem, td)
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	return rec
}

// readSpans decodes a stored Parquet file into (service, start) pairs in file order.
func readSpans(t *testing.T, data []byte) []span {
	t.Helper()
	tbl, err := pqarrow.ReadTable(context.Background(), bytes.NewReader(data),
		parquet.NewReaderProperties(nil), pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("reading parquet: %v", err)
	}
	defer tbl.Release()

	tr := array.NewTableReader(tbl, -1)
	defer tr.Release()
	var spans []span
	for tr.Next() {
		rec := tr.RecordBatch()
		services := rec.Column(rec.Schema().FieldIndices("service_name")[0]).(*array.Dictionary)
		names := services.Dictionary().(*array.String)
		starts := rec.Column(rec.Schema().FieldIndices("start_time")[0]).(*array.Timestamp)
		for i := range int(rec.NumRows()) {
			spans = append(spans, span{
				service: names.Value(services.GetValueIndex(i)),
				start:   time.Unix(0, int64(starts.Value(i))).UTC(),
			})
		}
	}
	return spans
}

func TestWriteStoresOneSortedFilePerHour(t *testing.T) {
	bucket := newMemBucket()
	w, mem := newWriter(t, bucket)

	first := record(t, mem,
		span{"payments", hour14.Add(2 * time.Minute)},
		span{"checkout", hour14.Add(3 * time.Minute)},
	)
	defer first.Release()
	second := record(t, mem,
		span{"checkout", hour14.Add(1 * time.Minute)},
		span{"checkout", hour14.Add(time.Hour)}, // next hour
	)
	defer second.Release()

	if err := w.Write([]arrow.RecordBatch{first, second}, hour14.Add(2*time.Hour)); err != nil {
		t.Fatalf("write: %v", err)
	}

	keys := bucket.keys()
	if len(keys) != 2 || !strings.HasPrefix(keys[0], "spans/hour=2026102514/") || !strings.HasPrefix(keys[1], "spans/hour=2026102515/") {
		t.Fatalf("keys = %v, want one file for hour 14 and one for hour 15", keys)
	}
	want := []span{
		{"checkout", hour14.Add(1 * time.Minute)},
		{"checkout", hour14.Add(3 * time.Minute)},
		{"payments", hour14.Add(2 * time.Minute)},
	}
	if got := readSpans(t, bucket.objects[keys[0]]); !slices.Equal(got, want) {
		t.Errorf("hour 14 rows = %v, want %v", got, want)
	}
}

func TestWritePartitionsInvalidTimesIntoCurrentHour(t *testing.T) {
	bucket := newMemBucket()
	w, mem := newWriter(t, bucket)

	now := hour14.Add(30 * time.Minute)
	rec := record(t, mem,
		span{"checkout", time.Time{}},             // unset start time
		span{"checkout", now.Add(48 * time.Hour)}, // too far in the future
	)
	defer rec.Release()

	if err := w.Write([]arrow.RecordBatch{rec}, now); err != nil {
		t.Fatalf("write: %v", err)
	}
	keys := bucket.keys()
	if len(keys) != 1 || !strings.HasPrefix(keys[0], "spans/hour=2026102514/") {
		t.Fatalf("keys = %v, want one file in the current hour", keys)
	}
	if got := len(readSpans(t, bucket.objects[keys[0]])); got != 2 {
		t.Errorf("rows = %d, want 2", got)
	}
}

func TestFailedWriteRollsBackStoredFiles(t *testing.T) {
	bucket := newMemBucket()
	bucket.failAt = 2
	w, mem := newWriter(t, bucket)

	rec := record(t, mem,
		span{"checkout", hour14},
		span{"checkout", hour14.Add(time.Hour)},
	)
	defer rec.Release()

	if err := w.Write([]arrow.RecordBatch{rec}, hour14); err == nil {
		t.Fatal("write succeeded, want error")
	}
	if keys := bucket.keys(); len(keys) != 0 {
		t.Errorf("objects left after failed write: %v", keys)
	}
}

func TestWriteFallsBackToTheNextTimeColumn(t *testing.T) {
	bucket := newMemBucket()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	w := New(bucket, schemas.Logs, "logs", []string{"time", "observed_time"}, "service_name")
	w.mem = mem

	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(hour14.Add(5 * time.Minute))) // time unset
	rec, err := logs.Flatten(mem, ld)
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	defer rec.Release()

	if err := w.Write([]arrow.RecordBatch{rec}, hour14.Add(3*time.Hour)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if keys := bucket.keys(); len(keys) != 1 || !strings.HasPrefix(keys[0], "logs/hour=2026102514/") {
		t.Errorf("keys = %v, want the observed hour 14, not the hour of now", keys)
	}
}
