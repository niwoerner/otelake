package batcher

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

const (
	defaultFlushInterval = time.Second
	defaultMaxRows       = 100_000 // flush early once this many rows are pending
	queueSize            = 1024    // pending requests before Write returns ErrFull
)

var (
	ErrFull   = errors.New("batcher queue is full")
	ErrClosed = errors.New("batcher is closed")
)

// FileWriter stores one batch of records; writer.Writer implements it.
type FileWriter interface {
	Write(recs []arrow.RecordBatch, now time.Time) error
}

// Batcher collects the records of many requests and hands them to the file
// writer as one batch, every flush interval or max rows. Requests are acked
// once their rows are stored. Delivery is at-least-once: a client that retries
// after a timeout or a failed flush may store the same rows twice.
type Batcher struct {
	files  FileWriter
	logger *slog.Logger

	flushInterval time.Duration
	maxRows       int64

	reqs      chan request
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

type request struct {
	rec arrow.RecordBatch
	ack chan error
}

func New(files FileWriter, logger *slog.Logger) *Batcher {
	return &Batcher{
		files:         files,
		logger:        logger,
		flushInterval: defaultFlushInterval,
		maxRows:       defaultMaxRows,
		reqs:          make(chan request, queueSize),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
}

// Write queues rec and waits until it is stored or the flush failed. Once
// queued, the rows are written even if the caller gives up, so Write waits for
// the outcome instead of reporting a failure for stored rows. The caller keeps
// its own reference to rec and releases it as usual.
func (b *Batcher) Write(rec arrow.RecordBatch) error {
	select {
	case <-b.stop:
		return ErrClosed
	default:
	}

	rec.Retain()
	ack := make(chan error, 1)
	select {
	case b.reqs <- request{rec: rec, ack: ack}:
	default:
		rec.Release()
		return ErrFull
	}

	select {
	case err := <-ack:
		return err
	case <-b.done:
		select {
		case err := <-ack:
			return err
		default: // queued after the final drain: never written
			rec.Release()
			return ErrClosed
		}
	}
}

// Run flushes until Close is called. It must run in its own goroutine.
func (b *Batcher) Run() {
	defer close(b.done)
	tick := time.NewTicker(b.flushInterval)
	defer tick.Stop()

	var pending []request
	var rows int64
	flush := func() {
		b.flush(pending)
		pending, rows = nil, 0
	}

	for {
		select {
		case req := <-b.reqs:
			pending = append(pending, req)
			rows += req.rec.NumRows()
			if rows >= b.maxRows {
				flush()
			}
		case <-tick.C:
			flush()
		case <-b.stop:
			for {
				select {
				case req := <-b.reqs:
					pending = append(pending, req)
				default:
					flush()
					return
				}
			}
		}
	}
}

// Close stops accepting writes and waits until pending rows are flushed or
// ctx ends. It is safe to call more than once.
func (b *Batcher) Close(ctx context.Context) error {
	b.closeOnce.Do(func() { close(b.stop) })
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher) flush(pending []request) {
	if len(pending) == 0 {
		return
	}
	recs := make([]arrow.RecordBatch, len(pending))
	for i, req := range pending {
		recs[i] = req.rec
	}

	err := b.files.Write(recs, time.Now())
	if err != nil {
		b.logger.Error("flush failed", "error", err)
	}
	for _, req := range pending {
		req.ack <- err
		req.rec.Release()
	}
}
