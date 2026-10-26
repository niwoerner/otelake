package benchmarks

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/niwoerner/otelake/internal/benchdata"
	"github.com/niwoerner/otelake/internal/writer"
)

func BenchmarkFlatten(b *testing.B) {
	for _, name := range benchdata.Names {
		b.Run(name, func(b *testing.B) {
			const rows = 1000
			d, err := benchdata.New(name, rows, 1, 0)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				rec, err := d.Flatten()
				if err != nil {
					b.Fatal(err)
				}
				if rec.NumRows() != rows {
					b.Fatalf("rows = %d, want %d", rec.NumRows(), rows)
				}
				rec.Release()
			}
			b.ReportMetric(float64(b.N*rows)/b.Elapsed().Seconds(), "records/s")
		})
	}
}

// countingBucket measures encoding, not disk or network I/O. Output is not retained.
type countingBucket struct{ bytes, files int }

func (s *countingBucket) Put(_ context.Context, _ string, data []byte) error {
	s.bytes += len(data)
	s.files++
	return nil
}

func (s *countingBucket) Delete(context.Context, string) error { return nil }

func BenchmarkParquet(b *testing.B) {
	for _, name := range benchdata.Names {
		b.Run(name, func(b *testing.B) {
			// The production batcher flushes at 100,000 records. Use 100
			// distinct 1,000-record requests, all within one event hour.
			const rows = 100_000
			var recs []arrow.RecordBatch
			b.Cleanup(func() {
				for _, rec := range recs {
					rec.Release()
				}
			})
			var d benchdata.Data
			for offset := 0; offset < rows; offset += 1000 {
				var err error
				d, err = benchdata.New(name, 1000, 1, offset)
				if err != nil {
					b.Fatal(err)
				}
				rec, err := d.Flatten()
				if err != nil {
					b.Fatal(err)
				}
				if rec.NumRows() != 1000 {
					b.Fatalf("rows = %d, want 1000", rec.NumRows())
				}
				recs = append(recs, rec)
			}
			sink := &countingBucket{}
			w := writer.New(sink, d.Schema, d.Prefix, d.TimeColumns, "service_name")
			b.ReportAllocs()
			for b.Loop() {
				sink.bytes, sink.files = 0, 0
				if err := w.Write(recs, benchdata.Now); err != nil {
					b.Fatal(err)
				}
				if sink.files != 1 {
					b.Fatalf("files = %d, want 1", sink.files)
				}
			}
			b.ReportMetric(float64(b.N*rows)/b.Elapsed().Seconds(), "records/s")
			b.ReportMetric(float64(sink.bytes)/rows, "parquet-B/record")
		})
	}
}
