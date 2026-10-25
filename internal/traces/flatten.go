package traces

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/flatten"
	"github.com/niwoerner/otelake/internal/schemas"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Flatten turns decoded OTLP traces into one row per span in the
// schemas.Spans layout; resource and scope are copied onto every row.
func Flatten(mem memory.Allocator, td ptrace.Traces) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(mem, schemas.Spans)
	defer b.Release()

	row := flatten.NewRow(b)
	for _, rs := range td.ResourceSpans().All() {
		service := flatten.ServiceName(rs.Resource())
		for _, ss := range rs.ScopeSpans().All() {
			for _, span := range ss.Spans().All() {
				row.Reset()
				row.ResourceAndScope(service, rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())
				appendSpan(row, span)
				if err := row.Done(); err != nil {
					return nil, err
				}
			}
		}
	}
	return b.NewRecordBatch(), nil
}

func appendSpan(row *flatten.Row, span ptrace.Span) {
	start, end := span.StartTimestamp(), span.EndTimestamp()

	row.TraceID("trace_id", span.TraceID())
	row.SpanID("span_id", span.SpanID())
	row.Str("trace_state", span.TraceState().AsRaw())
	row.SpanID("parent_span_id", span.ParentSpanID())
	row.U32("flags", span.Flags())
	row.Dict("name", span.Name())
	row.I8("kind", int8(span.Kind()))
	row.Timestamp("start_time", start)
	row.Timestamp("end_time", end)
	row.I64("duration_ns", max(int64(end)-int64(start), 0))
	row.I8("status_code", int8(span.Status().Code()))
	row.Str("status_message", span.Status().Message())
	row.Attrs("", span.Attributes())
	row.U32("dropped_attributes_count", span.DroppedAttributesCount())

	events := row.List("events")
	for _, e := range span.Events().All() {
		events.Next()
		events.Timestamp("time", e.Timestamp())
		events.Str("name", e.Name())
		events.Attrs("", e.Attributes())
		events.U32("dropped_attributes_count", e.DroppedAttributesCount())
	}
	row.U32("dropped_events_count", span.DroppedEventsCount())

	links := row.List("links")
	for _, l := range span.Links().All() {
		links.Next()
		links.TraceID("trace_id", l.TraceID())
		links.SpanID("span_id", l.SpanID())
		links.Str("trace_state", l.TraceState().AsRaw())
		links.U32("flags", l.Flags())
		links.Attrs("", l.Attributes())
		links.U32("dropped_attributes_count", l.DroppedAttributesCount())
	}
	row.U32("dropped_links_count", span.DroppedLinksCount())
}
