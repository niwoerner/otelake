package batcher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// fakeWriter records the rows of every batch it is asked to store.
type fakeWriter struct {
	mu      sync.Mutex
	batches []int64 // rows per Write call
	err     error
}

func (f *fakeWriter) Write(recs []arrow.RecordBatch, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	var rows int64
	for _, rec := range recs {
		rows += rec.NumRows()
	}
	f.batches = append(f.batches, rows)
	return nil
}

func (f *fakeWriter) stored() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.batches...)
}

func newBatcher(t *testing.T, files FileWriter) *Batcher {
	t.Helper()
	return New(files, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// record builds a one-column record with n rows.
func record(t *testing.T, n int) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	for i := range n {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
	}
	return rb.NewRecordBatch()
}

func TestWriteAcksOnceStored(t *testing.T) {
	files := &fakeWriter{}
	b := newBatcher(t, files)
	b.flushInterval = 10 * time.Millisecond
	go b.Run()
	defer func() { _ = b.Close(context.Background()) }()

	rec := record(t, 3)
	defer rec.Release()

	if err := b.Write(rec); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := files.stored(); len(got) != 1 || got[0] != 3 {
		t.Errorf("stored batches = %v, want [3] before the ack", got)
	}
}

func TestWriteReportsFailedFlush(t *testing.T) {
	files := &fakeWriter{err: errors.New("put failed")}
	b := newBatcher(t, files)
	b.flushInterval = 10 * time.Millisecond
	go b.Run()
	defer func() { _ = b.Close(context.Background()) }()

	rec := record(t, 1)
	defer rec.Release()

	if err := b.Write(rec); !errors.Is(err, files.err) {
		t.Fatalf("Write = %v, want the flush error", err)
	}
}

func TestWriteFlushesEarlyAtMaxRows(t *testing.T) {
	files := &fakeWriter{}
	b := newBatcher(t, files)
	b.flushInterval = time.Hour // only maxRows triggers the flush
	b.maxRows = 2
	go b.Run()
	defer func() { _ = b.Close(context.Background()) }()

	rec := record(t, 2)
	defer rec.Release()

	if err := b.Write(rec); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func TestWriteReturnsErrFullWhenQueueIsFull(t *testing.T) {
	b := newBatcher(t, &fakeWriter{})
	b.reqs = make(chan request) // no room and no Run reading it

	rec := record(t, 1)
	defer rec.Release()

	if err := b.Write(rec); !errors.Is(err, ErrFull) {
		t.Fatalf("Write = %v, want ErrFull", err)
	}
}

func TestCloseFlushesPendingAndIsIdempotent(t *testing.T) {
	files := &fakeWriter{}
	b := newBatcher(t, files)
	b.flushInterval = time.Hour // only Close flushes
	go b.Run()

	rec := record(t, 1)
	rec.Retain() // the batcher's reference, as Write would take it
	ack := make(chan error, 1)
	b.reqs <- request{rec: rec, ack: ack}

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-ack; err != nil {
		t.Errorf("pending request ack = %v", err)
	}
	if got := files.stored(); len(got) != 1 {
		t.Errorf("stored batches after Close = %v, want 1", got)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := b.Write(rec); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Close = %v, want ErrClosed", err)
	}
	rec.Release()
}

func TestCloseReturnsAtDeadline(t *testing.T) {
	b := newBatcher(t, &fakeWriter{}) // Run never started, so it never finishes
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if err := b.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close = %v, want DeadlineExceeded", err)
	}
}
