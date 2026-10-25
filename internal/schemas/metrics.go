package schemas

import (
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
)

var (
	f64 = arrow.PrimitiveTypes.Float64
	u64 = arrow.PrimitiveTypes.Uint64
	i32 = arrow.PrimitiveTypes.Int32
)

// Values of the metrics "type" column. They are part of the stored contract, so
// they never change, independent of any library's enum.
const (
	MetricTypeGauge                int8 = 1
	MetricTypeSum                  int8 = 2
	MetricTypeHistogram            int8 = 3
	MetricTypeExponentialHistogram int8 = 4
	MetricTypeSummary              int8 = 5
)

// One row per data point; columns of other data point types are null.
var Metrics = otlpArrowSchema(resourceAndScope(), []arrow.Field{
	{Name: "name", Type: dictString},
	{Name: "description", Type: dictString},
	{Name: "unit", Type: dictString},
}, attributes("metadata_"), []arrow.Field{
	{Name: "type", Type: arrow.PrimitiveTypes.Int8}, // one of the MetricType constants
	{Name: "temporality", Type: arrow.PrimitiveTypes.Int8},
	{Name: "is_monotonic", Type: arrow.FixedWidthTypes.Boolean},
}, attributes(""), []arrow.Field{
	{Name: "start_time", Type: timestamp},
	{Name: "time", Type: timestamp},
	{Name: "flags", Type: u32},

	// gauge, sum
	{Name: "value_f64", Type: f64, Nullable: true},
	{Name: "value_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},

	// histogram, exponential_histogram, summary
	{Name: "count", Type: u64, Nullable: true},
	{Name: "sum", Type: f64, Nullable: true},
	{Name: "min", Type: f64, Nullable: true},
	{Name: "max", Type: f64, Nullable: true},

	// histogram
	{Name: "bucket_counts", Type: arrow.ListOf(u64), Nullable: true},
	{Name: "explicit_bounds", Type: arrow.ListOf(f64), Nullable: true},

	// exponential_histogram
	{Name: "scale", Type: i32, Nullable: true},
	{Name: "zero_count", Type: u64, Nullable: true},
	{Name: "zero_threshold", Type: f64, Nullable: true},
	{Name: "positive_offset", Type: i32, Nullable: true},
	{Name: "positive_bucket_counts", Type: arrow.ListOf(u64), Nullable: true},
	{Name: "negative_offset", Type: i32, Nullable: true},
	{Name: "negative_bucket_counts", Type: arrow.ListOf(u64), Nullable: true},

	// summary
	{Name: "quantile_values", Type: arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "quantile", Type: f64},
		arrow.Field{Name: "value", Type: f64},
	)), Nullable: true},

	{Name: "exemplars", Type: arrow.ListOf(arrow.StructOf(slices.Concat(
		attributes("filtered_"),
		[]arrow.Field{
			{Name: "time", Type: timestamp},
			{Name: "value_f64", Type: f64, Nullable: true},
			{Name: "value_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "span_id", Type: spanID, Nullable: true},
			{Name: "trace_id", Type: traceID, Nullable: true},
		},
	)...))},
})
