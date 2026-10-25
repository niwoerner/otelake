package schemas

import "github.com/apache/arrow-go/v18/arrow"

var Logs = otlpArrowSchema(resourceAndScope(), []arrow.Field{
	{Name: "time", Type: timestamp},
	{Name: "observed_time", Type: timestamp},
	{Name: "severity_number", Type: arrow.PrimitiveTypes.Int8},
	{Name: "severity_text", Type: dictString},
	{Name: "body_str", Type: str, Nullable: true},  // string bodies
	{Name: "body_json", Type: str, Nullable: true}, // every other body type as OTLP JSON
	{Name: "event_name", Type: dictString},
	{Name: "flags", Type: u32},
	{Name: "trace_id", Type: traceID, Nullable: true},
	{Name: "span_id", Type: spanID, Nullable: true},
}, attributes(""), []arrow.Field{
	{Name: "dropped_attributes_count", Type: u32},
})
