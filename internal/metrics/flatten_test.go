package metrics

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/niwoerner/otelake/internal/flatten/flattentest"
	"github.com/niwoerner/otelake/internal/schemas"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Rows of testMetrics, one data point per metric type.
const (
	gaugeRow = iota
	sumRow
	histogramRow
	expHistogramRow
	summaryRow
)

func testMetrics() pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "checkout")
	ms := rm.ScopeMetrics().AppendEmpty().Metrics()

	gauge := ms.AppendEmpty()
	gauge.SetName("cpu.usage")
	gauge.SetUnit("1")
	gauge.Metadata().PutStr("source", "host")
	dp := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(0.75)
	dp.SetTimestamp(1_000)
	dp.Attributes().PutStr("cpu", "0")

	sum := ms.AppendEmpty()
	sum.SetName("requests")
	s := sum.SetEmptySum()
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	s.SetIsMonotonic(true)
	s.DataPoints().AppendEmpty().SetIntValue(7)

	hist := ms.AppendEmpty()
	hist.SetName("latency")
	h := hist.SetEmptyHistogram()
	h.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	hdp := h.DataPoints().AppendEmpty()
	hdp.SetCount(3)
	hdp.SetSum(12.5)
	hdp.BucketCounts().FromRaw([]uint64{1, 2})
	hdp.ExplicitBounds().FromRaw([]float64{5})
	hdp.Exemplars().AppendEmpty().SetIntValue(4)

	exp := ms.AppendEmpty()
	exp.SetName("latency.exp")
	edp := exp.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
	edp.SetCount(4)
	edp.SetScale(2)
	edp.SetZeroCount(1)
	edp.Positive().SetOffset(-1)
	edp.Positive().BucketCounts().FromRaw([]uint64{1, 2})

	summary := ms.AppendEmpty()
	summary.SetName("latency.summary")
	sdp := summary.SetEmptySummary().DataPoints().AppendEmpty()
	sdp.SetCount(10)
	sdp.SetSum(100)
	q := sdp.QuantileValues().AppendEmpty()
	q.SetQuantile(0.99)
	q.SetValue(42)
	sdp.QuantileValues().AppendEmpty().SetQuantile(0.5)
	return md
}

func flattenMetrics(t *testing.T) arrow.RecordBatch {
	t.Helper()
	rec, err := Flatten(flattentest.Allocator(t), testMetrics())
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	t.Cleanup(rec.Release)
	return rec
}

func TestFlattenMatchesSchema(t *testing.T) {
	rec := flattenMetrics(t)

	if !rec.Schema().Equal(schemas.Metrics) {
		t.Fatalf("schema differs:\n got %v\nwant %v", rec.Schema(), schemas.Metrics)
	}
	if rec.NumRows() != 5 {
		t.Fatalf("rows = %d, want one per data point (5)", rec.NumRows())
	}
}

func TestFlattenStoresOwnMetricTypes(t *testing.T) {
	rec := flattenMetrics(t)

	types := flattentest.Column[*array.Int8](t, rec, "type")
	want := []int8{
		schemas.MetricTypeGauge, schemas.MetricTypeSum, schemas.MetricTypeHistogram,
		schemas.MetricTypeExponentialHistogram, schemas.MetricTypeSummary,
	}
	for row, typ := range want {
		if got := types.Value(row); got != typ {
			t.Errorf("row %d type = %d, want %d", row, got, typ)
		}
	}
}

func TestFlattenMetricColumns(t *testing.T) {
	rec := flattenMetrics(t)

	if got := flattentest.Dict(t, rec, "name", gaugeRow); got != "cpu.usage" {
		t.Errorf("name = %q", got)
	}
	if got := flattentest.Map(t, rec, "metadata_attrs_str", gaugeRow)["source"]; got != "host" {
		t.Errorf("metadata source = %v", got)
	}
	if got := flattentest.Map(t, rec, "attrs_str", gaugeRow)["cpu"]; got != "0" {
		t.Errorf("data point attribute cpu = %v", got)
	}
	if got := flattentest.Dict(t, rec, "service_name", summaryRow); got != "checkout" {
		t.Errorf("service_name = %q", got)
	}
}

func TestFlattenNumberPoints(t *testing.T) {
	rec := flattenMetrics(t)
	f64 := flattentest.Column[*array.Float64](t, rec, "value_f64")
	i64 := flattentest.Column[*array.Int64](t, rec, "value_int")

	if f64.Value(gaugeRow) != 0.75 || !i64.IsNull(gaugeRow) {
		t.Errorf("gauge: value_f64 = %v, value_int null = %v", f64.Value(gaugeRow), i64.IsNull(gaugeRow))
	}
	if !f64.IsNull(sumRow) || i64.Value(sumRow) != 7 {
		t.Errorf("sum: value_f64 null = %v, value_int = %d", f64.IsNull(sumRow), i64.Value(sumRow))
	}
	if got := flattentest.Column[*array.Int8](t, rec, "temporality").Value(sumRow); got != int8(pmetric.AggregationTemporalityCumulative) {
		t.Errorf("sum temporality = %d", got)
	}
	if !flattentest.Column[*array.Boolean](t, rec, "is_monotonic").Value(sumRow) {
		t.Error("sum must be monotonic")
	}
	if !flattentest.Column[*array.Uint64](t, rec, "count").IsNull(gaugeRow) {
		t.Error("gauge count must be null")
	}
}

func TestFlattenHistograms(t *testing.T) {
	rec := flattenMetrics(t)
	count := flattentest.Column[*array.Uint64](t, rec, "count")
	sum := flattentest.Column[*array.Float64](t, rec, "sum")

	if count.Value(histogramRow) != 3 || sum.Value(histogramRow) != 12.5 {
		t.Errorf("histogram count = %d, sum = %v", count.Value(histogramRow), sum.Value(histogramRow))
	}
	if !flattentest.Column[*array.Float64](t, rec, "min").IsNull(histogramRow) {
		t.Error("unset histogram min must be null")
	}
	if got := flattentest.Len(t, rec, "bucket_counts", histogramRow); got != 2 {
		t.Errorf("bucket_counts = %d, want 2", got)
	}
	if got := flattentest.Len(t, rec, "exemplars", histogramRow); got != 1 {
		t.Errorf("exemplars = %d, want 1", got)
	}
	if !flattentest.Column[*array.List](t, rec, "bucket_counts").IsNull(expHistogramRow) {
		t.Error("exponential histogram bucket_counts must be null")
	}

	if got := flattentest.Column[*array.Int32](t, rec, "scale").Value(expHistogramRow); got != 2 {
		t.Errorf("scale = %d", got)
	}
	if got := flattentest.Column[*array.Int32](t, rec, "positive_offset").Value(expHistogramRow); got != -1 {
		t.Errorf("positive_offset = %d", got)
	}
	if got := flattentest.Len(t, rec, "positive_bucket_counts", expHistogramRow); got != 2 {
		t.Errorf("positive_bucket_counts = %d, want 2", got)
	}
	if !flattentest.Column[*array.Int32](t, rec, "scale").IsNull(histogramRow) {
		t.Error("histogram scale must be null")
	}
}

func TestFlattenSummary(t *testing.T) {
	rec := flattenMetrics(t)

	if got := flattentest.Column[*array.Uint64](t, rec, "count").Value(summaryRow); got != 10 {
		t.Errorf("count = %d", got)
	}
	if got := flattentest.Len(t, rec, "quantile_values", summaryRow); got != 2 {
		t.Fatalf("quantile_values = %d, want 2", got)
	}
	quantiles := flattentest.Column[*array.List](t, rec, "quantile_values")
	start, _ := quantiles.ValueOffsets(summaryRow)
	q := quantiles.ListValues().(*array.Struct)
	if got := q.Field(0).(*array.Float64).Value(int(start)); got != 0.99 {
		t.Errorf("first quantile = %v", got)
	}
	if got := flattentest.Len(t, rec, "exemplars", summaryRow); got != 0 {
		t.Errorf("summary exemplars = %d, want 0", got)
	}
	if !quantiles.IsNull(gaugeRow) {
		t.Error("gauge quantile_values must be null")
	}
}
