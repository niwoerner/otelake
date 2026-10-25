package schemas

import (
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
)

var Spans = otlpArrowSchema(resourceAndScope(), []arrow.Field{
	{Name: "trace_id", Type: traceID, Nullable: true},
	{Name: "span_id", Type: spanID, Nullable: true},
	{Name: "trace_state", Type: str},
	{Name: "parent_span_id", Type: spanID, Nullable: true},
	{Name: "flags", Type: u32},
	{Name: "name", Type: dictString},
	{Name: "kind", Type: arrow.PrimitiveTypes.Int8},
	{Name: "start_time", Type: timestamp},
	{Name: "end_time", Type: timestamp},
	{Name: "duration_ns", Type: arrow.PrimitiveTypes.Int64}, // derived
	{Name: "status_code", Type: arrow.PrimitiveTypes.Int8},
	{Name: "status_message", Type: str},
}, attributes(""), []arrow.Field{
	{Name: "dropped_attributes_count", Type: u32},
	{Name: "events", Type: arrow.ListOf(arrow.StructOf(slices.Concat(
		[]arrow.Field{
			{Name: "time", Type: timestamp},
			{Name: "name", Type: str},
		},
		attributes(""),
		[]arrow.Field{
			{Name: "dropped_attributes_count", Type: u32},
		},
	)...))},
	{Name: "dropped_events_count", Type: u32},
	{Name: "links", Type: arrow.ListOf(arrow.StructOf(slices.Concat(
		[]arrow.Field{
			{Name: "trace_id", Type: traceID, Nullable: true},
			{Name: "span_id", Type: spanID, Nullable: true},
			{Name: "trace_state", Type: str},
			{Name: "flags", Type: u32},
		},
		attributes(""),
		[]arrow.Field{
			{Name: "dropped_attributes_count", Type: u32},
		},
	)...))},
	{Name: "dropped_links_count", Type: u32},
})
