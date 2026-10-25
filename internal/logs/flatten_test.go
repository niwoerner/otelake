package logs

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/niwoerner/otelake/internal/flatten"
	"github.com/niwoerner/otelake/internal/flatten/flattentest"
	"github.com/niwoerner/otelake/internal/schemas"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

var (
	traceID = pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID  = pcommon.SpanID{1, 1, 1, 1, 1, 1, 1, 1}
)

// testLogs builds one resource and scope with three log records: a string
// body, a structured body and no body.
func testLogs() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "checkout")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("app")

	text := sl.LogRecords().AppendEmpty()
	text.SetTimestamp(1_000)
	text.SetObservedTimestamp(2_000)
	text.SetSeverityNumber(plog.SeverityNumberError)
	text.SetSeverityText("ERROR")
	text.Body().SetStr("payment failed")
	text.SetEventName("payment.failed")
	text.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
	text.SetTraceID(traceID)
	text.SetSpanID(spanID)
	text.Attributes().PutInt("order.id", 42)
	text.SetDroppedAttributesCount(3)

	structured := sl.LogRecords().AppendEmpty()
	structured.Body().SetEmptyMap().PutStr("user", "ada")

	sl.LogRecords().AppendEmpty() // no body
	return ld
}

func flattenLogs(t *testing.T, ld plog.Logs) arrow.RecordBatch {
	t.Helper()
	rec, err := Flatten(flattentest.Allocator(t), ld)
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	t.Cleanup(rec.Release)
	return rec
}

func TestFlattenMatchesSchema(t *testing.T) {
	rec := flattenLogs(t, testLogs())

	if !rec.Schema().Equal(schemas.Logs) {
		t.Fatalf("schema differs:\n got %v\nwant %v", rec.Schema(), schemas.Logs)
	}
	if rec.NumRows() != 3 {
		t.Fatalf("rows = %d, want 3", rec.NumRows())
	}
	for row := range 3 {
		if got := flattentest.Dict(t, rec, "service_name", row); got != "checkout" {
			t.Errorf("row %d service_name = %q", row, got)
		}
	}
}

func TestFlattenStoresBodyByType(t *testing.T) {
	rec := flattenLogs(t, testLogs())
	text := flattentest.Column[*array.String](t, rec, "body_str")
	structured := flattentest.Column[*array.String](t, rec, "body_json")

	if text.Value(0) != "payment failed" || !structured.IsNull(0) {
		t.Errorf("string body: body_str = %q, body_json null = %v", text.Value(0), structured.IsNull(0))
	}
	if want := `{"kvlistValue":{"values":[{"key":"user","value":{"stringValue":"ada"}}]}}`; !text.IsNull(1) || structured.Value(1) != want {
		t.Errorf("map body: body_str null = %v, body_json = %s, want %s", text.IsNull(1), structured.Value(1), want)
	}
	if !text.IsNull(2) || !structured.IsNull(2) {
		t.Error("empty body must leave body_str and body_json null")
	}
}

func TestFlattenLogRecordFields(t *testing.T) {
	rec := flattenLogs(t, testLogs())

	if got := flattentest.Column[*array.Timestamp](t, rec, "time").Value(0); got != 1_000 {
		t.Errorf("time = %d", got)
	}
	if got := flattentest.Column[*array.Timestamp](t, rec, "observed_time").Value(0); got != 2_000 {
		t.Errorf("observed_time = %d", got)
	}
	if got := flattentest.Column[*array.Int8](t, rec, "severity_number").Value(0); got != int8(plog.SeverityNumberError) {
		t.Errorf("severity_number = %d", got)
	}
	if got := flattentest.Dict(t, rec, "severity_text", 0); got != "ERROR" {
		t.Errorf("severity_text = %q", got)
	}
	if got := flattentest.Dict(t, rec, "event_name", 0); got != "payment.failed" {
		t.Errorf("event_name = %q", got)
	}
	if got := flattentest.Column[*array.Uint32](t, rec, "flags").Value(0); got != 1 {
		t.Errorf("flags = %d, want 1 (sampled)", got)
	}
	if got := pcommon.TraceID(flattentest.Column[*array.FixedSizeBinary](t, rec, "trace_id").Value(0)); got != traceID {
		t.Errorf("trace_id = %v", got)
	}
	if !flattentest.Column[*array.FixedSizeBinary](t, rec, "span_id").IsNull(1) {
		t.Error("empty span_id must be null")
	}
	if got := flattentest.Map(t, rec, "attrs_int", 0)["order.id"]; got != int64(42) {
		t.Errorf("attrs_int[order.id] = %v", got)
	}
	if got := flattentest.Column[*array.Uint32](t, rec, "dropped_attributes_count").Value(0); got != 3 {
		t.Errorf("dropped_attributes_count = %d", got)
	}
}

func TestFlattenDefaultsMissingServiceName(t *testing.T) {
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	rec := flattenLogs(t, ld)

	if got := flattentest.Dict(t, rec, "service_name", 0); got != flatten.UnknownService {
		t.Errorf("service_name = %q, want %q", got, flatten.UnknownService)
	}
}
