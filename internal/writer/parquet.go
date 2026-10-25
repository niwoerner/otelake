package writer

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/niwoerner/otelake/internal/storage"
)

const (
	nsPerHour = int64(time.Hour)
	// Event times further ahead than this are treated as invalid.
	maxFutureSkew = 24 * time.Hour
	putTimeout    = 5 * time.Second
)

// Writer stores Arrow records as Parquet files in a bucket, one file per event
// hour: <prefix>/hour=YYYYMMDDHH/<file>.parquet, rows sorted by the sort
// column, then event time.
type Writer struct {
	bucket     storage.Bucket
	schema     *arrow.Schema
	prefix     string // e.g. "spans"
	timeColumn string // event time, used for the hour partition and sorting
	sortColumn string // dictionary-encoded string column sorted before time
	mem        memory.Allocator
	seq        atomic.Uint64
}

func New(bucket storage.Bucket, schema *arrow.Schema, prefix, timeColumn, sortColumn string) *Writer {
	return &Writer{
		bucket:     bucket,
		schema:     schema,
		prefix:     prefix,
		timeColumn: timeColumn,
		sortColumn: sortColumn,
		mem:        memory.DefaultAllocator,
	}
}

// Write stores recs as one Parquet file per event hour, sorted by the sort
// column and time. It is all-or-nothing: if any file fails, the files already
// stored by this call are deleted again, so a retried batch is not stored twice.
func (w *Writer) Write(recs []arrow.RecordBatch, now time.Time) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), putTimeout)
	defer cancel()

	all, err := w.concat(recs)
	if err != nil {
		return fmt.Errorf("concatenating records: %w", err)
	}
	defer all.Release()

	var stored []string
	defer func() {
		if err != nil {
			err = errors.Join(err, w.rollback(stored))
		}
	}()

	for hour, rows := range w.rowsByHour(all, now) {
		w.sortRows(all, rows)
		part, err := w.take(ctx, all, rows)
		if err != nil {
			return fmt.Errorf("selecting rows of hour %s: %w", hour, err)
		}
		data, err := encodeParquet(part)
		part.Release()
		if err != nil {
			return fmt.Errorf("encoding parquet: %w", err)
		}

		key := fmt.Sprintf("%s/hour=%s/%d-%06d.parquet", w.prefix, hour, now.UnixNano(), w.seq.Add(1))
		if err := w.bucket.Put(ctx, key, data); err != nil {
			return err
		}
		stored = append(stored, key)
	}
	return nil
}

// rollback deletes the files a failed write already stored.
func (w *Writer) rollback(keys []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), putTimeout)
	defer cancel()

	var errs error
	for _, key := range keys {
		if err := w.bucket.Delete(ctx, key); err != nil {
			errs = errors.Join(errs, fmt.Errorf("rolling back %s: %w", key, err))
		}
	}
	return errs
}

func (w *Writer) concat(recs []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(recs) == 1 {
		recs[0].Retain()
		return recs[0], nil
	}

	var rows int64
	cols := make([]arrow.Array, w.schema.NumFields())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	for i := range cols {
		parts := make([]arrow.Array, len(recs))
		for j, rec := range recs {
			parts[j] = rec.Column(i)
		}
		col, err := array.Concatenate(parts, w.mem)
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}
	for _, rec := range recs {
		rows += rec.NumRows()
	}
	return array.NewRecordBatch(w.schema, cols, rows), nil
}

// rowsByHour groups row indices by event hour, formatted YYYYMMDDHH (UTC).
// Rows without a valid event time (unset, or more than maxFutureSkew ahead)
// go to the hour of now, so they don't end up in bogus partitions.
func (w *Writer) rowsByHour(rec arrow.RecordBatch, now time.Time) map[string][]int32 {
	times := rec.Column(w.schema.FieldIndices(w.timeColumn)[0]).(*array.Timestamp)
	latest := now.Add(maxFutureSkew).UnixNano()
	hours := map[string][]int32{}
	for i, ts := range times.Values() {
		t := int64(ts)
		if t <= 0 || t > latest {
			t = now.UnixNano()
		}
		hour := time.Unix(0, t/nsPerHour*nsPerHour).UTC().Format("2006010215")
		hours[hour] = append(hours[hour], int32(i))
	}
	return hours
}

// sortRows orders row indices by the sort column, then event time.
func (w *Writer) sortRows(rec arrow.RecordBatch, rows []int32) {
	keys := rec.Column(w.schema.FieldIndices(w.sortColumn)[0]).(*array.Dictionary)
	names := keys.Dictionary().(*array.String)
	times := rec.Column(w.schema.FieldIndices(w.timeColumn)[0]).(*array.Timestamp)

	slices.SortStableFunc(rows, func(x, y int32) int {
		return cmp.Or(
			cmp.Compare(names.Value(keys.GetValueIndex(int(x))), names.Value(keys.GetValueIndex(int(y)))),
			cmp.Compare(times.Value(int(x)), times.Value(int(y))),
		)
	})
}

func (w *Writer) take(ctx context.Context, rec arrow.RecordBatch, rows []int32) (arrow.RecordBatch, error) {
	ib := array.NewInt32Builder(w.mem)
	defer ib.Release()
	ib.AppendValues(rows, nil)
	indices := ib.NewInt32Array()
	defer indices.Release()

	cols := make([]arrow.Array, rec.NumCols())
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	for i := range cols {
		col, err := compute.TakeArray(ctx, rec.Column(i), indices)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", rec.ColumnName(i), err)
		}
		cols[i] = col
	}
	return array.NewRecordBatch(w.schema, cols, int64(len(rows))), nil
}

func encodeParquet(rec arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(true),
		parquet.WithStats(true),
	)
	fw, err := pqarrow.NewFileWriter(rec.Schema(), &buf, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return nil, err
	}
	if err := fw.Write(rec); err != nil {
		_ = fw.Close()
		return nil, err
	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
