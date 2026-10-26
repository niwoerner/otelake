# Benchmarks

- [Mixed-signal E2E](#mixed-signal-e2e)
- [Results](#results)
- [What each benchmark measures](#what-each-benchmark-measures)
- [Workload and environment](#workload-and-environment)
- [Run](#run)

## Mixed-signal E2E

One synthetic small-team scenario, using all three production ingestion paths
at once: HTTP → gzip/OTLP decode → Arrow → Parquet → local fsync → acknowledgement.
This tests whether the server keeps up with a chosen load, **not its maximum capacity**.

| Incoming telemetry | Records/second |
| :--- | ---: |
| Spans | 4,000 |
| Logs | 4,000 |
| Metric points | 2,000 |
| **Total** | **10,000** |

### Measured results

Three two-minute runs on the environment below. Values are rounded medians of
the three runs; request and record counts were identical in every run.

| Measurement | Result |
| :--- | ---: |
| Acknowledged during the steady window | **10,000 records/s** |
| Measured requests, including final drain | 9,925 records/s |
| Successful measured requests | 6,000 / 6,000 |
| Median acknowledgement latency | 550 ms |
| p95 acknowledgement latency | 990 ms |
| p99 acknowledgement latency | 1,032 ms |
| Failed requests / missed sends | 0 / 0 |
| Outstanding requests at cutoff → after drain | 5 → 0 |
| Unique stored records, including warmup | 1,300,000 |

**Interpretation:** the server kept up with this offered load in all three runs.
p95 means 95% of requests were acknowledged within about 990 ms. At this rate,
the one-second batch timer dominates latency; it is not a maximum-throughput test.
One-second samples stayed at 50–51 outstanding requests throughout the measured
windows, with no observed backlog growth. The last five requests drained in
about 0.91 seconds. The steady-window rate counts acknowledgements arriving
inside that window, including requests sent just before it; the drain-inclusive
rate counts only measured requests and includes their final tail time.

Whole-process RSS was **361 MiB before load and peaked at 604 MiB** (median peaks;
individual peaks 602–607 MiB); peak live heap was 531 MiB. These include client
fixtures and are **not server-only memory requirements**. Offered traffic averaged
4.03 MiB/s before gzip and 0.81 MiB/s on the wire, excluding HTTP headers.

### Method and limits

Requests contain 200 records: 50 gzip-compressed protobuf requests/second.
Each run warms up for 10 seconds, then measures two minutes, plus final drain time.
Logs are 75% text and 25% structured; metric points are half numeric and half
histograms, exponential histograms and summaries.
Fixtures are generated and compressed before timing; every request is distinct.
There are 16 services, varying span durations and errors, and log messages around
256 B (80%), 1 KiB (19%) or 8 KiB (1%), with varying words and numbers.
Metric label sets repeat across exports; request IDs are not added as metric labels.
About 5% of requests carry older event timestamps.

Arrivals are scheduled every 20 ms independently of responses, with at most 256
requests in flight. A full client concurrency limit or the scheduler falling
behind by a full 20 ms slot counts as a missed send and fails the run; the generator
does not silently reduce the offered load. There are no retries. Latency starts at the
scheduled send time and ends after reading the acknowledgement, including any
client scheduling delay. The test decodes every output file and checks every
acknowledged record identity exactly once, including warmup records.

The load generator and server still share one process and CPU over loopback.
RSS and live heap are sampled once per second for that **whole process, including
pre-generated client payloads**, not isolated server RAM. Outstanding requests
include requests waiting for the next batch flush, not just work queued behind I/O.
This is synthetic ingestion only: no S3, TLS, network distance, concurrent queries,
retries, failure recovery or multi-hour soak. It is not a comparison with other backends.

## Results

The following isolated stage results use a different workload from the mixed run.
**One request contains 1,000 records**: spans, log records or metric data points.
Higher throughput is better. HTTP rates are rounded to whole requests/second;
Arrow rates are rounded to the nearest 1,000 records/second.

### 1. HTTP receive

Receive the HTTP body and unmarshal OTLP. No flattening or storage.

| Workload | Requests/second |
| :--- | ---: |
| Traces | 2,759 |
| Text logs | 2,963 |
| Structured logs | 1,998 |
| Numeric metrics | 3,311 |
| Histograms and summaries | 2,557 |

### 2. Flatten OTLP → Arrow

Convert already-decoded telemetry into Arrow. No HTTP or Parquet encoding.

| Workload | Records/second |
| :--- | ---: |
| Traces | 315,000 |
| Text logs | 368,000 |
| Structured logs | 274,000 |
| Numeric metrics | 259,000 |
| Histograms and summaries | 234,000 |

### 3. Write Arrow → Parquet

Combine, sort and encode Arrow into compressed Parquet. No disk or S3 I/O.

| Workload | Records/second |
| :--- | ---: |
| Traces | 217,000 |
| Text logs | 257,000 |
| Structured logs | 235,000 |
| Numeric metrics | 270,000 |
| Histograms and summaries | 202,000 |

### 4. E2E → local storage

The full path: HTTP → OTLP → Arrow → Parquet → local disk → acknowledgement.

| Workload | Requests/second |
| :--- | ---: |
| Traces | 151 |
| Text logs | 183 |
| Structured logs | 161 |
| Numeric metrics | 178 |
| Histograms and summaries | 136 |

For example, **151 trace requests/second means about 151,000 spans stored/second**.

These are separate experiments, not four timings to add together. HTTP and E2E
use 128 concurrent clients; the isolated Arrow stages run one operation at a time.
These are measured throughputs at that load, **not proven maximums**.

## What each benchmark measures

- **HTTP receive:** a benchmark-only handler uses the production body reader and
  the same fresh OTLP decoder as the real handler, then responds before flattening.
  Production body limits and the memory budget are included.
  Requests are uncompressed protobuf. Every decoded request must contain 1,000 records.
- **OTLP → Arrow:** one already-decoded 1,000-record payload per operation,
  including release of the resulting Arrow record.
- **Arrow → Parquet:** one flush combines 100 distinct 1,000-record payloads
  (100,000 records), matching the production early-flush threshold. All events
  belong to one hour. Includes hour partitioning, sorting and Zstd encoding;
  Arrow construction is excluded. Output bytes are counted but not retained.
- **E2E:** uses production handlers and local file/directory fsync, with the
  default one-second/100,000-record batching policy. There are
  enough clients to reach the row threshold rather than only wait for the timer.
  Throughput includes draining requests started before the measurement deadline.
  After timing, every Parquet file is decoded and stored records are checked
  against successful acknowledgements. All measured requests returned 200;
  there were no transport errors or stored-row mismatches. No retries are used.

## Workload and environment

Measured 2026-10-26: Go 1.26.0, linux/amd64, Intel Xeon @ 2.60 GHz,
four visible CPUs, GOMAXPROCS=4, ext4. CPU quota was unavailable.
Stage table values are medians: five runs for Arrow stages; three five-second load
windows per HTTP/E2E case, plus drain time. Fixture generation is outside timing.

The stage fixtures have 16 services, three resource attributes, four row attributes,
varied IDs and unsorted timestamps. Traces include an event/link every fourth span.
Logs separate text and structured bodies. Numeric metrics mix gauges/sums;
the other metrics mix histograms, exponential histograms and summaries.
See [`internal/benchdata/data.go`](internal/benchdata/data.go) for exact fixtures.

In the stage HTTP tests, each client has its own ID range and replays its payload.
Timestamps and low-cardinality values repeat, affecting compression. Client and server share
CPU in the same process over loopback; this is not an independent load generator.
These short, warm-process runs are a baseline, not production sizing or
crash-durability proof. Filesystem performance is specific to the test machine.

## Run

```bash
# Mixed-signal E2E (opt-in; 10s warmup + 2m measurement per run)
OTELAKE_BENCH_MIXED=1 go test ./internal/server -run '^TestMixedSignals$' -v -count 3 -timeout 15m

# OTLP → Arrow and Arrow → Parquet
go test ./internal/benchmarks -run '^$' -bench . -benchmem -count 5

# HTTP receive and E2E (opt-in)
OTELAKE_BENCH_HTTP=1 go test ./internal/server -run '^TestHTTP$' -v -count 3
```

Use `-run '^TestHTTP/receive/'` or `-run '^TestHTTP/e2e/'` to select one HTTP stage.
`OTELAKE_BENCH_DURATION` overrides the measured window: for example, `30s` makes
the stage load tests longer, or `2s` gives the mixed test a quick smoke run.
The mixed test requires whole seconds and caps warmup at the measured duration.
Normal `go test ./...` skips the load tests. Transport compression comparisons
and the old size/hour matrix are intentionally excluded from this baseline.

Raw Go output still includes `ns/op`, `B/op` and `allocs/op` for profiling;
the human-facing tables focus on throughput. Allocations are not peak RAM.
When updating results, keep raw output and record the environment; do not compare
runs from different machines as though they were a code improvement.
