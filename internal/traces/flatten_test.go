package traces

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/schemas"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/xpdata/entity"
)

var (
	traceID = pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID  = pcommon.SpanID{1, 1, 1, 1, 1, 1, 1, 1}
	childID = pcommon.SpanID{2, 2, 2, 2, 2, 2, 2, 2}
)

// testTraces builds one resource with every attribute value type, one scope
// and two spans: a root span with an event and a link, and an error child.
func testTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.37.0")
	res := rs.Resource()
	res.Attributes().PutStr("service.name", "checkout")
	res.Attributes().PutInt("process.pid", 42)
	res.SetDroppedAttributesCount(1)
	ref := entity.ResourceEntityRefs(res).AppendEmpty()
	ref.SetType("service")
	ref.IdKeys().Append("service.name")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("http")
	ss.Scope().SetVersion("1.2.0")
	ss.Scope().Attributes().PutBool("scope.enabled", true)

	root := ss.Spans().AppendEmpty()
	root.SetTraceID(traceID)
	root.SetSpanID(spanID)
	root.TraceState().FromRaw("vendor=value")
	root.SetName("GET /cart")
	root.SetKind(ptrace.SpanKindServer)
	root.SetStartTimestamp(1_000)
	root.SetEndTimestamp(3_000)
	attrs := root.Attributes()
	attrs.PutStr("http.route", "/cart")
	attrs.PutInt("http.response.status_code", 200)
	attrs.PutDouble("ratio", 0.5)
	attrs.PutBool("cached", true)
	attrs.PutEmptyBytes("raw").FromRaw([]byte{0xca, 0xfe})
	attrs.PutEmptySlice("tags").AppendEmpty().SetStr("a")
	attrs.PutEmpty("unset")
	ev := root.Events().AppendEmpty()
	ev.SetName("cache.hit")
	ev.SetTimestamp(2_000)
	ev.Attributes().PutStr("key", "cart")
	link := root.Links().AppendEmpty()
	link.SetTraceID(traceID)
	link.SetSpanID(childID)

	child := ss.Spans().AppendEmpty()
	child.SetTraceID(traceID)
	child.SetSpanID(childID)
	child.SetParentSpanID(spanID)
	child.SetName("SELECT cart")
	child.SetStartTimestamp(2_000)
	child.SetEndTimestamp(1_500) // ends before it starts
	child.Status().SetCode(ptrace.StatusCodeError)
	child.Status().SetMessage("timeout")
	return td
}

func flatten(t *testing.T, td ptrace.Traces) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	rec, err := Flatten(mem, td)
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	t.Cleanup(rec.Release)
	return rec
}

func column[T arrow.Array](t *testing.T, rec arrow.RecordBatch, name string) T {
	t.Helper()
	idx := rec.Schema().FieldIndices(name)
	if len(idx) == 0 {
		t.Fatalf("no column %q", name)
	}
	return rec.Column(idx[0]).(T)
}

func dictValue(t *testing.T, rec arrow.RecordBatch, name string, row int) string {
	t.Helper()
	d := column[*array.Dictionary](t, rec, name)
	return d.Dictionary().(*array.String).Value(d.GetValueIndex(row))
}

// mapEntries returns the key/value pairs of one row of a map column.
func mapEntries(t *testing.T, rec arrow.RecordBatch, name string, row int) map[string]any {
	t.Helper()
	m := column[*array.Map](t, rec, name)
	start, end := m.ValueOffsets(row)
	keys := m.Keys().(*array.String)
	entries := map[string]any{}
	for i := int(start); i < int(end); i++ {
		entries[keys.Value(i)] = m.Items().GetOneForMarshal(i)
	}
	return entries
}

func TestFlattenMatchesSchema(t *testing.T) {
	rec := flatten(t, testTraces())

	if !rec.Schema().Equal(schemas.Spans) {
		t.Fatalf("schema differs:\n got %v\nwant %v", rec.Schema(), schemas.Spans)
	}
	if rec.NumRows() != 2 {
		t.Fatalf("rows = %d, want 2", rec.NumRows())
	}
}

func TestFlattenCopiesResourceAndScope(t *testing.T) {
	rec := flatten(t, testTraces())

	for row := range 2 {
		if got := dictValue(t, rec, "service_name", row); got != "checkout" {
			t.Errorf("row %d service_name = %q", row, got)
		}
		if got := dictValue(t, rec, "scope_name", row); got != "http" {
			t.Errorf("row %d scope_name = %q", row, got)
		}
		if got := mapEntries(t, rec, "resource_attrs_int", row)["process.pid"]; got != int64(42) {
			t.Errorf("row %d resource process.pid = %v", row, got)
		}
		if got := mapEntries(t, rec, "scope_attrs_bool", row)["scope.enabled"]; got != true {
			t.Errorf("row %d scope.enabled = %v", row, got)
		}
	}
	if got := column[*array.Uint32](t, rec, "resource_dropped_attributes_count").Value(0); got != 1 {
		t.Errorf("resource_dropped_attributes_count = %d", got)
	}
	refs := column[*array.List](t, rec, "resource_entity_refs")
	if start, end := refs.ValueOffsets(0); end-start != 1 {
		t.Errorf("entity refs = %d, want 1", end-start)
	}
}

func TestFlattenSpanFields(t *testing.T) {
	rec := flatten(t, testTraces())

	ids := column[*array.FixedSizeBinary](t, rec, "trace_id")
	if got := pcommon.TraceID(ids.Value(0)); got != traceID {
		t.Errorf("trace_id = %v", got)
	}
	parents := column[*array.FixedSizeBinary](t, rec, "parent_span_id")
	if !parents.IsNull(0) {
		t.Error("empty parent_span_id must be null")
	}
	if got := pcommon.SpanID(parents.Value(1)); got != spanID {
		t.Errorf("child parent_span_id = %v", got)
	}
	if got := column[*array.String](t, rec, "trace_state").Value(0); got != "vendor=value" {
		t.Errorf("trace_state = %q", got)
	}
	if got := column[*array.Int8](t, rec, "kind").Value(0); got != int8(ptrace.SpanKindServer) {
		t.Errorf("kind = %d", got)
	}

	durations := column[*array.Int64](t, rec, "duration_ns")
	if got := durations.Value(0); got != 2_000 {
		t.Errorf("duration_ns = %d, want 2000", got)
	}
	if got := durations.Value(1); got != 0 {
		t.Errorf("negative duration_ns = %d, want 0", got)
	}
	if got := column[*array.Timestamp](t, rec, "end_time").Value(1); got != 1_500 {
		t.Errorf("end_time kept as sent = %d, want 1500", got)
	}
	if got := column[*array.Int8](t, rec, "status_code").Value(1); got != int8(ptrace.StatusCodeError) {
		t.Errorf("status_code = %d", got)
	}
	if got := column[*array.String](t, rec, "status_message").Value(1); got != "timeout" {
		t.Errorf("status_message = %q", got)
	}
}

func TestFlattenSplitsAttributesByType(t *testing.T) {
	rec := flatten(t, testTraces())

	want := map[string]map[string]any{
		"attrs_str":   {"http.route": "/cart"},
		"attrs_int":   {"http.response.status_code": int64(200)},
		"attrs_f64":   {"ratio": 0.5},
		"attrs_bool":  {"cached": true},
		"attrs_bytes": {"raw": []byte{0xca, 0xfe}},
		"attrs_any":   {"tags": `{"arrayValue":{"values":[{"stringValue":"a"}]}}`, "unset": `{}`},
	}
	for col, entries := range want {
		got := mapEntries(t, rec, col, 0)
		if len(got) != len(entries) {
			t.Errorf("%s = %v, want %v", col, got, entries)
			continue
		}
		for k, v := range entries {
			if b, ok := v.([]byte); ok {
				if string(got[k].([]byte)) != string(b) {
					t.Errorf("%s[%s] = %v, want %v", col, k, got[k], v)
				}
				continue
			}
			if got[k] != v {
				t.Errorf("%s[%s] = %v, want %v", col, k, got[k], v)
			}
		}
	}
}

func TestFlattenEventsAndLinks(t *testing.T) {
	rec := flatten(t, testTraces())

	events := column[*array.List](t, rec, "events")
	if start, end := events.ValueOffsets(0); end-start != 1 {
		t.Fatalf("root events = %d, want 1", end-start)
	}
	if start, end := events.ValueOffsets(1); end-start != 0 {
		t.Errorf("child events = %d, want 0", end-start)
	}
	ev := events.ListValues().(*array.Struct)
	if got := ev.Field(1).(*array.String).Value(0); got != "cache.hit" {
		t.Errorf("event name = %q", got)
	}

	links := column[*array.List](t, rec, "links")
	link := links.ListValues().(*array.Struct)
	if got := pcommon.SpanID(link.Field(1).(*array.FixedSizeBinary).Value(0)); got != childID {
		t.Errorf("link span_id = %v", got)
	}
}

func TestFlattenDefaultsMissingServiceName(t *testing.T) {
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()

	rec := flatten(t, td)

	if got := dictValue(t, rec, "service_name", 0); got != unknownService {
		t.Errorf("service_name = %q, want %q", got, unknownService)
	}
	if !column[*array.FixedSizeBinary](t, rec, "trace_id").IsNull(0) {
		t.Error("empty trace_id must be null")
	}
}
