package schemas

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// mapping assigns every field of an OTLP message to the columns storing it.
// Nested columns use dotted paths, e.g. "events.time" for a field inside the
// events list. Every field must map to at least one column or marker.
type mapping map[string][]string

// Markers for fields without a column of their own.
const (
	nested       = "(nested message, mapped on its own)"
	perRow       = "(one row per element)"
	profilesOnly = "(only used by the profiles signal)"
)

func TestTracesOtlpConformance(t *testing.T) {
	checkConformance(t, Spans, &tracepb.ResourceSpans{}, mapping{
		"resource":    {nested},
		"scope_spans": {perRow},
		"schema_url":  {"resource_schema_url"},
	})
	checkConformance(t, Spans, &tracepb.ScopeSpans{}, mapping{
		"scope":      {nested},
		"spans":      {perRow},
		"schema_url": {"scope_schema_url"},
	})
	checkResourceAndScope(t, Spans)
	checkAttributeValues(t, Spans)
	checkConformance(t, Spans, &tracepb.Span{}, mapping{
		"trace_id":                 {"trace_id"},
		"span_id":                  {"span_id"},
		"trace_state":              {"trace_state"},
		"parent_span_id":           {"parent_span_id"},
		"flags":                    {"flags"},
		"name":                     {"name"},
		"kind":                     {"kind"},
		"start_time_unix_nano":     {"start_time"},
		"end_time_unix_nano":       {"end_time"},
		"attributes":               attrs(""),
		"dropped_attributes_count": {"dropped_attributes_count"},
		"events":                   {"events"},
		"dropped_events_count":     {"dropped_events_count"},
		"links":                    {"links"},
		"dropped_links_count":      {"dropped_links_count"},
		"status":                   {nested},
	})
	checkConformance(t, Spans, &tracepb.Span_Event{}, mapping{
		"time_unix_nano":           {"events.time"},
		"name":                     {"events.name"},
		"attributes":               attrs("events."),
		"dropped_attributes_count": {"events.dropped_attributes_count"},
	})
	checkConformance(t, Spans, &tracepb.Span_Link{}, mapping{
		"trace_id":                 {"links.trace_id"},
		"span_id":                  {"links.span_id"},
		"trace_state":              {"links.trace_state"},
		"attributes":               attrs("links."),
		"dropped_attributes_count": {"links.dropped_attributes_count"},
		"flags":                    {"links.flags"},
	})
	checkConformance(t, Spans, &tracepb.Status{}, mapping{
		"message": {"status_message"},
		"code":    {"status_code"},
	})
}

func TestLogsOtlpConformance(t *testing.T) {
	checkConformance(t, Logs, &logspb.ResourceLogs{}, mapping{
		"resource":   {nested},
		"scope_logs": {perRow},
		"schema_url": {"resource_schema_url"},
	})
	checkConformance(t, Logs, &logspb.ScopeLogs{}, mapping{
		"scope":       {nested},
		"log_records": {perRow},
		"schema_url":  {"scope_schema_url"},
	})
	checkResourceAndScope(t, Logs)
	checkAttributeValues(t, Logs)
	checkConformance(t, Logs, &logspb.LogRecord{}, mapping{
		"time_unix_nano":           {"time"},
		"observed_time_unix_nano":  {"observed_time"},
		"severity_number":          {"severity_number"},
		"severity_text":            {"severity_text"},
		"body":                     {"body_str", "body_json"},
		"attributes":               attrs(""),
		"dropped_attributes_count": {"dropped_attributes_count"},
		"flags":                    {"flags"},
		"trace_id":                 {"trace_id"},
		"span_id":                  {"span_id"},
		"event_name":               {"event_name"},
	})
}

func TestMetricsOtlpConformance(t *testing.T) {
	checkConformance(t, Metrics, &metricspb.ResourceMetrics{}, mapping{
		"resource":      {nested},
		"scope_metrics": {perRow},
		"schema_url":    {"resource_schema_url"},
	})
	checkConformance(t, Metrics, &metricspb.ScopeMetrics{}, mapping{
		"scope":      {nested},
		"metrics":    {perRow},
		"schema_url": {"scope_schema_url"},
	})
	checkResourceAndScope(t, Metrics)
	checkAttributeValues(t, Metrics)
	checkConformance(t, Metrics, &metricspb.Metric{}, mapping{
		"name":                  {"name"},
		"description":           {"description"},
		"unit":                  {"unit"},
		"metadata":              attrs("metadata_"),
		"gauge":                 {"type", nested},
		"sum":                   {"type", nested},
		"histogram":             {"type", nested},
		"exponential_histogram": {"type", nested},
		"summary":               {"type", nested},
	})
	checkConformance(t, Metrics, &metricspb.Gauge{}, mapping{
		"data_points": {perRow},
	})
	checkConformance(t, Metrics, &metricspb.Sum{}, mapping{
		"data_points":             {perRow},
		"aggregation_temporality": {"temporality"},
		"is_monotonic":            {"is_monotonic"},
	})
	checkConformance(t, Metrics, &metricspb.Histogram{}, mapping{
		"data_points":             {perRow},
		"aggregation_temporality": {"temporality"},
	})
	checkConformance(t, Metrics, &metricspb.ExponentialHistogram{}, mapping{
		"data_points":             {perRow},
		"aggregation_temporality": {"temporality"},
	})
	checkConformance(t, Metrics, &metricspb.Summary{}, mapping{
		"data_points": {perRow},
	})
	checkConformance(t, Metrics, &metricspb.NumberDataPoint{}, mapping{
		"attributes":           attrs(""),
		"start_time_unix_nano": {"start_time"},
		"time_unix_nano":       {"time"},
		"as_double":            {"value_f64"},
		"as_int":               {"value_int"},
		"exemplars":            {"exemplars"},
		"flags":                {"flags"},
	})
	checkConformance(t, Metrics, &metricspb.HistogramDataPoint{}, mapping{
		"attributes":           attrs(""),
		"start_time_unix_nano": {"start_time"},
		"time_unix_nano":       {"time"},
		"count":                {"count"},
		"sum":                  {"sum"},
		"bucket_counts":        {"bucket_counts"},
		"explicit_bounds":      {"explicit_bounds"},
		"exemplars":            {"exemplars"},
		"flags":                {"flags"},
		"min":                  {"min"},
		"max":                  {"max"},
	})
	checkConformance(t, Metrics, &metricspb.ExponentialHistogramDataPoint{}, mapping{
		"attributes":           attrs(""),
		"start_time_unix_nano": {"start_time"},
		"time_unix_nano":       {"time"},
		"count":                {"count"},
		"sum":                  {"sum"},
		"scale":                {"scale"},
		"zero_count":           {"zero_count"},
		"positive":             {nested},
		"negative":             {nested},
		"flags":                {"flags"},
		"exemplars":            {"exemplars"},
		"min":                  {"min"},
		"max":                  {"max"},
		"zero_threshold":       {"zero_threshold"},
	})
	checkConformance(t, Metrics, &metricspb.ExponentialHistogramDataPoint_Buckets{}, mapping{
		"offset":        {"positive_offset", "negative_offset"},
		"bucket_counts": {"positive_bucket_counts", "negative_bucket_counts"},
	})
	checkConformance(t, Metrics, &metricspb.SummaryDataPoint{}, mapping{
		"attributes":           attrs(""),
		"start_time_unix_nano": {"start_time"},
		"time_unix_nano":       {"time"},
		"count":                {"count"},
		"sum":                  {"sum"},
		"quantile_values":      {"quantile_values"},
		"flags":                {"flags"},
	})
	checkConformance(t, Metrics, &metricspb.SummaryDataPoint_ValueAtQuantile{}, mapping{
		"quantile": {"quantile_values.quantile"},
		"value":    {"quantile_values.value"},
	})
	checkConformance(t, Metrics, &metricspb.Exemplar{}, mapping{
		"filtered_attributes": attrs("exemplars.filtered_"),
		"time_unix_nano":      {"exemplars.time"},
		"as_double":           {"exemplars.value_f64"},
		"as_int":              {"exemplars.value_int"},
		"span_id":             {"exemplars.span_id"},
		"trace_id":            {"exemplars.trace_id"},
	})
}

// checkResourceAndScope checks the resource and scope columns shared by all signals.
func checkResourceAndScope(t *testing.T, schema *arrow.Schema) {
	t.Helper()
	checkConformance(t, schema, &resourcepb.Resource{}, mapping{
		"attributes":               append(attrs("resource_"), "service_name"),
		"dropped_attributes_count": {"resource_dropped_attributes_count"},
		"entity_refs":              {"resource_entity_refs"},
	})
	checkConformance(t, schema, &commonpb.EntityRef{}, mapping{
		"schema_url":       {"resource_entity_refs.schema_url"},
		"type":             {"resource_entity_refs.type"},
		"id_keys":          {"resource_entity_refs.id_keys"},
		"description_keys": {"resource_entity_refs.description_keys"},
	})
	checkConformance(t, schema, &commonpb.InstrumentationScope{}, mapping{
		"name":                     {"scope_name"},
		"version":                  {"scope_version"},
		"attributes":               attrs("scope_"),
		"dropped_attributes_count": {"scope_dropped_attributes_count"},
	})
}

// checkAttributeValues checks that every attribute value type has a column.
func checkAttributeValues(t *testing.T, schema *arrow.Schema) {
	t.Helper()
	checkConformance(t, schema, &commonpb.KeyValue{}, mapping{
		"key":          attrs(""),
		"value":        {nested},
		"key_strindex": {profilesOnly},
	})
	checkConformance(t, schema, &commonpb.AnyValue{}, mapping{
		"string_value":          {"attrs_str"},
		"bool_value":            {"attrs_bool"},
		"int_value":             {"attrs_int"},
		"double_value":          {"attrs_f64"},
		"array_value":           {"attrs_any"},
		"kvlist_value":          {"attrs_any"},
		"bytes_value":           {"attrs_bytes"},
		"string_value_strindex": {profilesOnly},
	})
	checkConformance(t, schema, &commonpb.ArrayValue{}, mapping{
		"values": {"attrs_any"},
	})
	checkConformance(t, schema, &commonpb.KeyValueList{}, mapping{
		"values": {"attrs_any"},
	})
}

func attrs(prefix string) []string {
	return []string{
		prefix + "attrs_str",
		prefix + "attrs_int",
		prefix + "attrs_f64",
		prefix + "attrs_bool",
		prefix + "attrs_bytes",
		prefix + "attrs_any",
	}
}

// checkConformance fails for OTLP fields missing from the mapping or mapped to
// nothing, for mapping entries that are not OTLP fields, and for mapped columns
// the schema lacks.
func checkConformance(t *testing.T, schema *arrow.Schema, msg protoreflect.ProtoMessage, m mapping) {
	t.Helper()
	desc := msg.ProtoReflect().Descriptor()
	fields := desc.Fields()

	for i := range fields.Len() {
		name := string(fields.Get(i).Name())
		if _, ok := m[name]; !ok {
			t.Errorf("%s.%s is not mapped to a column", desc.Name(), name)
		}
	}
	for name, columns := range m {
		if fields.ByName(protoreflect.Name(name)) == nil {
			t.Errorf("%s has no field %q", desc.Name(), name)
		}
		if len(columns) == 0 {
			t.Errorf("%s.%s maps to no column; every OTLP field must be stored", desc.Name(), name)
		}
		for _, column := range columns {
			if !strings.HasPrefix(column, "(") && !hasColumn(schema.Fields(), column) {
				t.Errorf("%s.%s maps to column %q, which the schema lacks", desc.Name(), name, column)
			}
		}
	}
}

// hasColumn resolves a dotted column path through list and struct types.
func hasColumn(fields []arrow.Field, path string) bool {
	name, rest, nestedPath := strings.Cut(path, ".")
	for _, f := range fields {
		if f.Name != name {
			continue
		}
		if !nestedPath {
			return true
		}
		return hasColumn(childFields(f.Type), rest)
	}
	return false
}

func childFields(t arrow.DataType) []arrow.Field {
	switch t := t.(type) {
	case *arrow.ListType:
		return childFields(t.Elem())
	case *arrow.StructType:
		return t.Fields()
	}
	return nil
}
