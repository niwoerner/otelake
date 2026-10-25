package traces

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/schemas"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/xpdata/entity"
)

const unknownService = "unknown_service"

// Flatten turns decoded OTLP traces into one row per span in the
// schemas.Spans layout; resource and scope are copied onto every row.
func Flatten(mem memory.Allocator, td ptrace.Traces) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(mem, schemas.Spans)
	defer b.Release()

	row := newColumns(b.Fields(), schemas.Spans.Fields())
	for _, rs := range td.ResourceSpans().All() {
		res := rs.Resource()
		service := unknownService
		if v, ok := res.Attributes().Get("service.name"); ok && v.Type() == pcommon.ValueTypeStr {
			service = v.Str()
		}
		for _, ss := range rs.ScopeSpans().All() {
			scope := ss.Scope()
			for _, span := range ss.Spans().All() {
				row.reset()
				row.dict("service_name", service)
				row.dict("resource_schema_url", rs.SchemaUrl())
				row.attrs("resource_", res.Attributes())
				row.u32("resource_dropped_attributes_count", res.DroppedAttributesCount())
				row.entityRefs("resource_entity_refs", entity.ResourceEntityRefs(res))
				row.dict("scope_schema_url", ss.SchemaUrl())
				row.dict("scope_name", scope.Name())
				row.dict("scope_version", scope.Version())
				row.attrs("scope_", scope.Attributes())
				row.u32("scope_dropped_attributes_count", scope.DroppedAttributesCount())
				appendSpan(row, span)
				if err := row.done(); err != nil {
					return nil, err
				}
			}
		}
	}
	return b.NewRecordBatch(), nil
}

func appendSpan(row *columns, span ptrace.Span) {
	start, end := span.StartTimestamp(), span.EndTimestamp()

	row.traceID("trace_id", span.TraceID())
	row.spanID("span_id", span.SpanID())
	row.str("trace_state", span.TraceState().AsRaw())
	row.spanID("parent_span_id", span.ParentSpanID())
	row.u32("flags", span.Flags())
	row.dict("name", span.Name())
	row.i8("kind", int8(span.Kind()))
	row.timestamp("start_time", start)
	row.timestamp("end_time", end)
	row.i64("duration_ns", max(int64(end)-int64(start), 0))
	row.i8("status_code", int8(span.Status().Code()))
	row.str("status_message", span.Status().Message())
	row.attrs("", span.Attributes())
	row.u32("dropped_attributes_count", span.DroppedAttributesCount())

	events := row.list("events")
	for _, e := range span.Events().All() {
		events.next()
		events.timestamp("time", e.Timestamp())
		events.str("name", e.Name())
		events.attrs("", e.Attributes())
		events.u32("dropped_attributes_count", e.DroppedAttributesCount())
	}
	row.u32("dropped_events_count", span.DroppedEventsCount())

	links := row.list("links")
	for _, l := range span.Links().All() {
		links.next()
		links.traceID("trace_id", l.TraceID())
		links.spanID("span_id", l.SpanID())
		links.str("trace_state", l.TraceState().AsRaw())
		links.u32("flags", l.Flags())
		links.attrs("", l.Attributes())
		links.u32("dropped_attributes_count", l.DroppedAttributesCount())
	}
	row.u32("dropped_links_count", span.DroppedLinksCount())
}
