package logs

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/flatten"
	"github.com/niwoerner/otelake/internal/schemas"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// Flatten turns decoded OTLP logs into one row per log record in the
// schemas.Logs layout; resource and scope are copied onto every row.
func Flatten(mem memory.Allocator, ld plog.Logs) (arrow.RecordBatch, error) {
	b := array.NewRecordBuilder(mem, schemas.Logs)
	defer b.Release()

	row := flatten.NewRow(b)
	for _, rl := range ld.ResourceLogs().All() {
		service := flatten.ServiceName(rl.Resource())
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				row.Reset()
				row.ResourceAndScope(service, rl.Resource(), rl.SchemaUrl(), sl.Scope(), sl.SchemaUrl())
				appendLogRecord(row, lr)
				if err := row.Done(); err != nil {
					return nil, err
				}
			}
		}
	}
	return b.NewRecordBatch(), nil
}

func appendLogRecord(row *flatten.Row, lr plog.LogRecord) {
	row.Timestamp("time", lr.Timestamp())
	row.Timestamp("observed_time", lr.ObservedTimestamp())
	row.I8("severity_number", int8(lr.SeverityNumber()))
	row.Dict("severity_text", lr.SeverityText())

	// String bodies stay text; every other body type keeps its structure as OTLP JSON.
	switch body := lr.Body(); body.Type() {
	case pcommon.ValueTypeEmpty:
		row.Null("body_str")
		row.Null("body_json")
	case pcommon.ValueTypeStr:
		row.Str("body_str", body.Str())
		row.Null("body_json")
	default:
		row.Null("body_str")
		row.JSON("body_json", body)
	}

	row.Dict("event_name", lr.EventName())
	row.U32("flags", uint32(lr.Flags()))
	row.TraceID("trace_id", lr.TraceID())
	row.SpanID("span_id", lr.SpanID())
	row.Attrs("", lr.Attributes())
	row.U32("dropped_attributes_count", lr.DroppedAttributesCount())
}
