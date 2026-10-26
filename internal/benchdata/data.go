// Package benchdata supplies deterministic synthetic telemetry for benchmarks.
// It is not a production workload model.
package benchdata

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/niwoerner/otelake/internal/logs"
	"github.com/niwoerner/otelake/internal/metrics"
	"github.com/niwoerner/otelake/internal/schemas"
	"github.com/niwoerner/otelake/internal/traces"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

var Names = []string{"traces", "logs-text", "logs-structured", "metrics-numeric", "metrics-buckets"}

var Now = time.Date(2026, 10, 25, 14, 0, 0, 0, time.UTC)

// Data describes one request. Flatten uses already-decoded pdata; Body is OTLP protobuf.
type Data struct {
	Schema      *arrow.Schema
	Prefix      string
	TimeColumns []string
	Body        []byte
	Flatten     func() (arrow.RecordBatch, error)
}

// New creates n rows across 16 services and hours event hours. Offset varies
// rows between requests. Events arrive in deliberately unsorted time order.
func New(name string, n, hours, offset int) (Data, error) {
	td, ld, md := ptrace.NewTraces(), plog.NewLogs(), pmetric.NewMetrics()
	for service := 0; service < 16; service++ {
		resource := pcommon.NewResource()
		resource.Attributes().PutStr("service.name", fmt.Sprintf("service-%02d", service))
		resource.Attributes().PutStr("deployment.environment.name", "benchmark")
		resource.Attributes().PutInt("process.pid", int64(1000+service))
		rs := td.ResourceSpans().AppendEmpty()
		resource.CopyTo(rs.Resource())
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("benchmark.http")
		rl := ld.ResourceLogs().AppendEmpty()
		resource.CopyTo(rl.Resource())
		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("benchmark.app")
		rm := md.ResourceMetrics().AppendEmpty()
		resource.CopyTo(rm.Resource())
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("benchmark.metrics")
		ms := sm.Metrics()
		for i := service; i < n; i += 16 {
			id := uint64(offset + i + 1)
			var traceID pcommon.TraceID
			var spanID pcommon.SpanID
			binary.LittleEndian.PutUint64(traceID[:8], id)
			binary.LittleEndian.PutUint64(traceID[8:], id*6364136223846793005)
			binary.LittleEndian.PutUint64(spanID[:], id)
			ts := pcommon.NewTimestampFromTime(Now.Add(-time.Duration(i%hours)*time.Hour + time.Duration((i*7919)%3_600_000)*time.Millisecond))
			attrs := pcommon.NewMap()
			attrs.PutStr("http.route", fmt.Sprintf("/items/%d", i%32))
			attrs.PutInt("request.id", int64(id))
			attrs.PutDouble("sample.ratio", float64(i%100)/100)
			attrs.PutBool("cache.hit", i%3 == 0)
			switch name {
			case "traces":
				sp := ss.Spans().AppendEmpty()
				sp.SetTraceID(traceID)
				sp.SetSpanID(spanID)
				sp.SetName(fmt.Sprintf("GET /route-%d", i%32))
				sp.SetKind(ptrace.SpanKindServer)
				sp.SetStartTimestamp(ts)
				sp.SetEndTimestamp(ts + 1_000_000)
				attrs.CopyTo(sp.Attributes())
				if i%4 == 0 {
					e := sp.Events().AppendEmpty()
					e.SetName("cache.lookup")
					e.SetTimestamp(ts + 500_000)
					e.Attributes().PutBool("hit", i%3 == 0)
					link := sp.Links().AppendEmpty()
					link.SetTraceID(traceID)
					link.SetSpanID(spanID)
				}
			case "logs-text", "logs-structured":
				lr := sl.LogRecords().AppendEmpty()
				lr.SetTimestamp(ts)
				lr.SetObservedTimestamp(ts + 2_000_000)
				lr.SetTraceID(traceID)
				lr.SetSpanID(spanID)
				lr.SetSeverityNumber(plog.SeverityNumberInfo)
				lr.SetSeverityText("INFO")
				attrs.CopyTo(lr.Attributes())
				if name == "logs-text" {
					lr.Body().SetStr(fmt.Sprintf("request %d completed for customer %d: fetched inventory and calculated price", id, i%997))
				} else {
					body := lr.Body().SetEmptyMap()
					body.PutStr("message", "request completed")
					body.PutInt("customer", int64(i%997))
					body.PutEmptySlice("items").AppendEmpty().SetInt(int64(id))
				}
			case "metrics-numeric", "metrics-buckets":
				// Five metrics per resource: reuse each metric for subsequent points.
				kind := (i / 16) % 5
				for ms.Len() <= kind {
					m := ms.AppendEmpty()
					m.SetName(fmt.Sprintf("measurement.%d", ms.Len()-1))
					m.SetUnit("ms")
					if name == "metrics-numeric" {
						if ms.Len()%2 == 0 {
							m.SetEmptySum().SetIsMonotonic(true)
						} else {
							m.SetEmptyGauge()
						}
					} else {
						switch (ms.Len() - 1) % 3 {
						case 0:
							m.SetEmptyHistogram()
						case 1:
							m.SetEmptyExponentialHistogram()
						case 2:
							m.SetEmptySummary()
						}
					}
				}
				m := ms.At(kind)
				switch m.Type() {
				case pmetric.MetricTypeGauge, pmetric.MetricTypeSum:
					var points pmetric.NumberDataPointSlice
					if m.Type() == pmetric.MetricTypeSum {
						points = m.Sum().DataPoints()
					} else {
						points = m.Gauge().DataPoints()
					}
					dp := points.AppendEmpty()
					dp.SetTimestamp(ts)
					if i%2 == 0 {
						dp.SetIntValue(int64(id))
					} else {
						dp.SetDoubleValue(float64(id) / 7)
					}
					attrs.CopyTo(dp.Attributes())
				case pmetric.MetricTypeHistogram:
					dp := m.Histogram().DataPoints().AppendEmpty()
					dp.SetTimestamp(ts)
					dp.SetCount(36)
					dp.SetSum(float64(id))
					dp.ExplicitBounds().FromRaw([]float64{1, 2, 4, 8, 16, 32, 64})
					dp.BucketCounts().FromRaw([]uint64{1, 2, 3, 4, 5, 6, 7, 8})
					attrs.CopyTo(dp.Attributes())
					e := dp.Exemplars().AppendEmpty()
					e.SetTimestamp(ts)
					e.SetDoubleValue(float64(id))
					e.SetTraceID(traceID)
				case pmetric.MetricTypeExponentialHistogram:
					dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
					dp.SetTimestamp(ts)
					dp.SetCount(36)
					dp.SetScale(2)
					dp.Positive().BucketCounts().FromRaw([]uint64{1, 2, 3, 4, 5, 6, 7, 8})
					attrs.CopyTo(dp.Attributes())
				case pmetric.MetricTypeSummary:
					dp := m.Summary().DataPoints().AppendEmpty()
					dp.SetTimestamp(ts)
					dp.SetCount(100)
					dp.SetSum(float64(id))
					attrs.CopyTo(dp.Attributes())
					for _, q := range []float64{0.5, 0.9, 0.99} {
						v := dp.QuantileValues().AppendEmpty()
						v.SetQuantile(q)
						v.SetValue(float64(id) * q)
					}
				}
			default:
				return Data{}, fmt.Errorf("unknown fixture %q", name)
			}
		}
	}
	d := Data{}
	var err error
	switch name {
	case "traces":
		d.Schema, d.Prefix, d.TimeColumns = schemas.Spans, "spans", []string{"start_time"}
		d.Flatten = func() (arrow.RecordBatch, error) { return traces.Flatten(memory.DefaultAllocator, td) }
		d.Body, err = ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	case "logs-text", "logs-structured":
		d.Schema, d.Prefix, d.TimeColumns = schemas.Logs, "logs", []string{"time", "observed_time"}
		d.Flatten = func() (arrow.RecordBatch, error) { return logs.Flatten(memory.DefaultAllocator, ld) }
		d.Body, err = plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	default:
		d.Schema, d.Prefix, d.TimeColumns = schemas.Metrics, "metrics", []string{"time"}
		d.Flatten = func() (arrow.RecordBatch, error) { return metrics.Flatten(memory.DefaultAllocator, md) }
		d.Body, err = pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	}
	return d, err
}

// Mixed builds one distinct request for the mixed-signal load scenario. Event
// timestamps identify individual records without adding unique metric labels.
func Mixed(name string, n, offset int, at time.Time) ([]byte, error) {
	d, err := New(name, n, 1, offset)
	if err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewPCG(uint64(offset+1), 42))
	row := 0
	switch name {
	case "traces":
		req := ptraceotlp.NewExportRequest()
		if err := req.UnmarshalProto(d.Body); err != nil {
			return nil, err
		}
		for _, rs := range req.Traces().ResourceSpans().All() {
			for _, ss := range rs.ScopeSpans().All() {
				for _, span := range ss.Spans().All() {
					ts := pcommon.NewTimestampFromTime(at.Add(time.Duration(row)))
					row++
					duration := pcommon.Timestamp((1 + rng.IntN(100)) * 1_000_000)
					span.SetStartTimestamp(ts)
					span.SetEndTimestamp(ts + duration)
					span.Attributes().PutStr("http.request.method", "GET")
					span.Attributes().PutStr("server.address", "inventory.internal")
					span.Attributes().PutInt("http.response.status_code", 200)
					span.Attributes().PutInt("customer.id", int64(rng.IntN(10_000)))
					if rng.IntN(100) == 0 {
						span.Status().SetCode(ptrace.StatusCodeError)
						span.Status().SetMessage("upstream timeout")
						span.Attributes().PutInt("http.response.status_code", 504)
					}
					for _, event := range span.Events().All() {
						event.SetTimestamp(ts + duration/2)
					}
				}
			}
		}
		return req.MarshalProto()
	case "logs-text", "logs-structured":
		req := plogotlp.NewExportRequest()
		if err := req.UnmarshalProto(d.Body); err != nil {
			return nil, err
		}
		words := []string{"request", "completed", "inventory", "customer", "checkout", "payment", "retry", "cache", "upstream", "connection", "order", "product", "worker", "response", "duration", "success"}
		for _, rl := range req.Logs().ResourceLogs().All() {
			for _, sl := range rl.ScopeLogs().All() {
				for _, lr := range sl.LogRecords().All() {
					ts := pcommon.NewTimestampFromTime(at.Add(time.Duration(row)))
					row++
					lr.SetTimestamp(ts)
					lr.SetObservedTimestamp(ts + 2_000_000)
					size := 256
					switch p := rng.IntN(100); {
					case p == 0:
						size = 8192
					case p < 20:
						size = 1024
					}
					var message strings.Builder
					fmt.Fprintf(&message, "request=%d customer=%d ", offset+row, rng.IntN(10_000))
					for message.Len() < size {
						fmt.Fprintf(&message, "%s=%d ", words[rng.IntN(len(words))], rng.IntN(1000))
					}
					if name == "logs-text" {
						lr.Body().SetStr(message.String())
					} else {
						body := lr.Body().SetEmptyMap()
						body.PutStr("message", message.String())
						body.PutEmptyMap("order").PutInt("id", int64(offset+row))
						body.PutEmptySlice("tags").AppendEmpty().SetStr("checkout")
					}
					if rng.IntN(100) < 5 {
						lr.SetSeverityNumber(plog.SeverityNumberError)
						lr.SetSeverityText("ERROR")
					}
				}
			}
		}
		return req.MarshalProto()
	default:
		req := pmetricotlp.NewExportRequest()
		if err := req.UnmarshalProto(d.Body); err != nil {
			return nil, err
		}
		for _, rm := range req.Metrics().ResourceMetrics().All() {
			for _, sm := range rm.ScopeMetrics().All() {
				for _, m := range sm.Metrics().All() {
					switch m.Type() {
					case pmetric.MetricTypeGauge, pmetric.MetricTypeSum:
						var points pmetric.NumberDataPointSlice
						if m.Type() == pmetric.MetricTypeSum {
							m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
							points = m.Sum().DataPoints()
						} else {
							points = m.Gauge().DataPoints()
						}
						for _, dp := range points.All() {
							dp.SetTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(row))))
							row++
							dp.Attributes().Remove("request.id")
							if m.Type() == pmetric.MetricTypeGauge {
								dp.SetDoubleValue(rng.Float64() * 100)
							}
						}
					case pmetric.MetricTypeHistogram:
						m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
						for _, dp := range m.Histogram().DataPoints().All() {
							dp.SetTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(row))))
							row++
							dp.Attributes().Remove("request.id")
							dp.SetSum(1024 + rng.Float64()*32)
							for _, e := range dp.Exemplars().All() {
								e.SetTimestamp(dp.Timestamp())
								e.SetDoubleValue(32)
							}
						}
					case pmetric.MetricTypeExponentialHistogram:
						m.ExponentialHistogram().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
						for _, dp := range m.ExponentialHistogram().DataPoints().All() {
							dp.SetTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(row))))
							row++
							dp.Attributes().Remove("request.id")
							dp.SetSum(96)
						}
					case pmetric.MetricTypeSummary:
						for _, dp := range m.Summary().DataPoints().All() {
							dp.SetTimestamp(pcommon.NewTimestampFromTime(at.Add(time.Duration(row))))
							row++
							dp.Attributes().Remove("request.id")
							dp.SetSum(1500)
							for i, q := range dp.QuantileValues().All() {
								q.SetValue([]float64{10, 30, 60}[i])
							}
						}
					}
				}
			}
		}
		return req.MarshalProto()
	}
}
