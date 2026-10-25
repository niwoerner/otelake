package metrics

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/flatten"
	"github.com/niwoerner/otelake/internal/schemas"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Flatten turns decoded OTLP metrics into one row per data point in the
// schemas.Metrics layout; resource, scope and metric are copied onto every row
// and the columns of other data point types are null.
func Flatten(mem memory.Allocator, md pmetric.Metrics) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(mem, schemas.Metrics)
	defer b.Release()

	row := flatten.NewRow(b)
	for _, rm := range md.ResourceMetrics().All() {
		service := flatten.ServiceName(rm.Resource())
		for _, sm := range rm.ScopeMetrics().All() {
			for _, m := range sm.Metrics().All() {
				mr := metricRow{row: row, service: service, rm: rm, sm: sm, m: m}
				if err := mr.appendPoints(); err != nil {
					return nil, err
				}
			}
		}
	}
	return b.NewRecordBatch(), nil
}

// point is the part all data point types share.
type point interface {
	Attributes() pcommon.Map
	StartTimestamp() pcommon.Timestamp
	Timestamp() pcommon.Timestamp
	Flags() pmetric.DataPointFlags
}

type metricRow struct {
	row     *flatten.Row
	service string
	rm      pmetric.ResourceMetrics
	sm      pmetric.ScopeMetrics
	m       pmetric.Metric
}

func (mr metricRow) appendPoints() error {
	row := mr.row
	switch mr.m.Type() {
	case pmetric.MetricTypeGauge:
		for _, dp := range mr.m.Gauge().DataPoints().All() {
			mr.begin(dp, schemas.MetricTypeGauge, pmetric.AggregationTemporalityUnspecified, false)
			appendNumber(row, dp)
			if err := row.Done(); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSum:
		sum := mr.m.Sum()
		for _, dp := range sum.DataPoints().All() {
			mr.begin(dp, schemas.MetricTypeSum, sum.AggregationTemporality(), sum.IsMonotonic())
			appendNumber(row, dp)
			if err := row.Done(); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeHistogram:
		h := mr.m.Histogram()
		for _, dp := range h.DataPoints().All() {
			mr.begin(dp, schemas.MetricTypeHistogram, h.AggregationTemporality(), false)
			appendHistogram(row, dp)
			if err := row.Done(); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeExponentialHistogram:
		h := mr.m.ExponentialHistogram()
		for _, dp := range h.DataPoints().All() {
			mr.begin(dp, schemas.MetricTypeExponentialHistogram, h.AggregationTemporality(), false)
			appendExponentialHistogram(row, dp)
			if err := row.Done(); err != nil {
				return err
			}
		}
	case pmetric.MetricTypeSummary:
		for _, dp := range mr.m.Summary().DataPoints().All() {
			mr.begin(dp, schemas.MetricTypeSummary, pmetric.AggregationTemporalityUnspecified, false)
			appendSummary(row, dp)
			if err := row.Done(); err != nil {
				return err
			}
		}
	}
	return nil
}

// begin starts a row and fills every column up to the type-specific values.
func (mr metricRow) begin(dp point, typ int8, temporality pmetric.AggregationTemporality, monotonic bool) {
	row := mr.row
	row.Reset()
	row.ResourceAndScope(mr.service, mr.rm.Resource(), mr.rm.SchemaUrl(), mr.sm.Scope(), mr.sm.SchemaUrl())
	row.Dict("name", mr.m.Name())
	row.Dict("description", mr.m.Description())
	row.Dict("unit", mr.m.Unit())
	row.Attrs("metadata_", mr.m.Metadata())
	row.I8("type", typ)
	row.I8("temporality", int8(temporality))
	row.Bool("is_monotonic", monotonic)
	row.Attrs("", dp.Attributes())
	row.Timestamp("start_time", dp.StartTimestamp())
	row.Timestamp("time", dp.Timestamp())
	row.U32("flags", uint32(dp.Flags()))
}

func appendNumber(row *flatten.Row, dp pmetric.NumberDataPoint) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		row.F64("value_f64", dp.DoubleValue())
		row.Null("value_int")
	case pmetric.NumberDataPointValueTypeInt:
		row.Null("value_f64")
		row.I64("value_int", dp.IntValue())
	default:
		row.Nulls("value_f64", "value_int")
	}
	row.Nulls("count", "sum", "min", "max", "bucket_counts", "explicit_bounds",
		"scale", "zero_count", "zero_threshold", "positive_offset", "positive_bucket_counts",
		"negative_offset", "negative_bucket_counts", "quantile_values")
	appendExemplars(row, dp.Exemplars())
}

func appendHistogram(row *flatten.Row, dp pmetric.HistogramDataPoint) {
	row.Nulls("value_f64", "value_int")
	row.U64("count", dp.Count())
	optionalF64(row, "sum", dp.HasSum(), dp.Sum())
	optionalF64(row, "min", dp.HasMin(), dp.Min())
	optionalF64(row, "max", dp.HasMax(), dp.Max())
	row.U64s("bucket_counts", dp.BucketCounts())
	row.F64s("explicit_bounds", dp.ExplicitBounds())
	row.Nulls("scale", "zero_count", "zero_threshold", "positive_offset", "positive_bucket_counts",
		"negative_offset", "negative_bucket_counts", "quantile_values")
	appendExemplars(row, dp.Exemplars())
}

func appendExponentialHistogram(row *flatten.Row, dp pmetric.ExponentialHistogramDataPoint) {
	row.Nulls("value_f64", "value_int")
	row.U64("count", dp.Count())
	optionalF64(row, "sum", dp.HasSum(), dp.Sum())
	optionalF64(row, "min", dp.HasMin(), dp.Min())
	optionalF64(row, "max", dp.HasMax(), dp.Max())
	row.Nulls("bucket_counts", "explicit_bounds")
	row.I32("scale", dp.Scale())
	row.U64("zero_count", dp.ZeroCount())
	row.F64("zero_threshold", dp.ZeroThreshold())
	row.I32("positive_offset", dp.Positive().Offset())
	row.U64s("positive_bucket_counts", dp.Positive().BucketCounts())
	row.I32("negative_offset", dp.Negative().Offset())
	row.U64s("negative_bucket_counts", dp.Negative().BucketCounts())
	row.Null("quantile_values")
	appendExemplars(row, dp.Exemplars())
}

func appendSummary(row *flatten.Row, dp pmetric.SummaryDataPoint) {
	row.Nulls("value_f64", "value_int")
	row.U64("count", dp.Count())
	row.F64("sum", dp.Sum())
	row.Nulls("min", "max", "bucket_counts", "explicit_bounds",
		"scale", "zero_count", "zero_threshold", "positive_offset", "positive_bucket_counts",
		"negative_offset", "negative_bucket_counts")
	quantiles := row.List("quantile_values")
	for _, q := range dp.QuantileValues().All() {
		quantiles.Next()
		quantiles.F64("quantile", q.Quantile())
		quantiles.F64("value", q.Value())
	}
	row.List("exemplars") // summaries carry no exemplars
}

func appendExemplars(row *flatten.Row, exemplars pmetric.ExemplarSlice) {
	l := row.List("exemplars")
	for _, e := range exemplars.All() {
		l.Next()
		l.Attrs("filtered_", e.FilteredAttributes())
		l.Timestamp("time", e.Timestamp())
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			l.F64("value_f64", e.DoubleValue())
			l.Null("value_int")
		case pmetric.ExemplarValueTypeInt:
			l.Null("value_f64")
			l.I64("value_int", e.IntValue())
		default:
			l.Nulls("value_f64", "value_int")
		}
		l.SpanID("span_id", e.SpanID())
		l.TraceID("trace_id", e.TraceID())
	}
}

func optionalF64(row *flatten.Row, name string, ok bool, v float64) {
	if ok {
		row.F64(name, v)
		return
	}
	row.Null(name)
}
