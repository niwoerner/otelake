# otelake — Architecture (v1)

An open-source, self-hosted OpenTelemetry backend optimized for **agent retrieval**: agents (and humans) query telemetry through SQL and MCP tools designed for compact, reproducible answers.

## Goals

1. **Robustness** — no acked data is lost on process crash, restart or deploy; overload is survivable.
2. **Convenience** — one Docker image, near-zero config, runs on a laptop (local directory) or on Hetzner (object storage).
3. **Agent-friendly retrieval** — SQL + MCP tools with compact, reproducible output.
4. **Verifiable and maintainable** — the project is mostly agent-developed, so every component has an automated pass/fail check, and state is kept local and simple.
5. **Scale later** — not a v1 goal, but roles (ingest / query / compactor) are separable from day one.

## Non-goals (v1)

OTAP/OTel Arrow ingest, auth, multi-tenancy (only a `tenant` column), querying unflushed data on ingesters, replicated ingest, precomputed rollups, dashboards UI, Prometheus API, log templating, baselines, entity graph, embeddings.

## Decisions

| Topic | Decision | Why |
|---|---|---|
| Language | **Rust** | Arrow, Parquet, DataFusion, `object_store` are native; strict compiler catches many agent mistakes; single static binary |
| Query engine | **Apache DataFusion** | Embeddable Rust query engine; we only supply the file list; async, built for concurrent server queries |
| Test oracle / interop | **DuckDB** | Independent engine to verify results; users can query the bucket's Parquet directly |
| Wire protocol | **OTLP only** (gRPC + HTTP, protobuf + JSON) | Every SDK and Collector speaks it. OTAP users run a Collector in front (`otelarrow` receiver → `otlp` exporter) |
| File format | **Parquet** (zstd) | Columnar, compressed, readable by every analytics tool |
| Storage | **Object storage** via `object_store` (S3 / Hetzner / MinIO / R2), **local directory** for dev | Cheap, unlimited, survives losing any machine. Same code path for both |
| Ingest durability | **Local WAL + async upload** (default), **ack after upload** (optional) | ms acks with seconds of local state; fully stateless mode via one switch |
| Catalog | **Per-hour generation files** in the bucket, single writer (compactor) | No database, no global log; all state is local to one hour |
| Schema | **One denormalized table per signal** | No joins, no fingerprint tables; Parquet compresses the repetition |
| License | **Apache-2.0** | Dependencies below are Apache-2.0 / MIT. AGPL projects (OpenObserve, Parseable) are inspiration only, no code reuse |
| `ollygarden/otlp-wire` | **Not applicable** | Go library; otelake is Rust. Decoding uses `prost` via `opentelemetry-proto` |

---

## The core constraint

1. **Data arrives as many small writes.** Collectors send a request every ~0.2–1 s with a few to a few thousand items; across services that is hundreds to thousands of requests/s.
2. **Queries need large, sorted, columnar files.** `WHERE service_name = 'checkout' AND duration_ns > 1e9` is only fast if data is stored per column, sorted, with per-chunk stats that let the engine skip what doesn't match.

You cannot write a big sorted file per 50-span request. Every stage below exists to convert (1) into (2) without losing data.

### Two background concepts

**Columnar files.** Parquet stores each column contiguously. A file contains row groups (~1M rows); each row group stores each column separately; the footer holds the schema plus min/max stats per row group and column.
- Compresses well: values in a column share a type and repeat (dictionary + run-length encoding, deltas for sorted timestamps, then zstd) → ~10×.
- Queries read only needed columns (e.g. 2 of 25) and skip row groups via stats. On object storage, readers use HTTP range GETs: footer first, then only the needed column chunks.
- Downside: writing one row touches every column → rows must be batched; fetching one full row by ID needs bloom filters.

**Timestamped immutable files.** Files are written once and never modified. Changes = write new files + commit a new generation + delete old files later.
- Object storage cannot edit or append to objects, so this works identically on S3 and local disk.
- Readers never see half-written files; caches never go stale; checksums stay valid.
- Every file belongs to one event-time hour, so queries select files by time range without opening them, and retention deletes whole hours.

---

## Overview

```
                     OTLP gRPC :4317 · OTLP HTTP :4318
                                   │
                    Load balancer (L7 / HTTP/2-aware)
                                   │
        ┌──────────────────────────┼──────────────────────────┐
   ingest-1                    ingest-2                   ingest-N
   decode → group commit → append wal/<signal>/<seg>.seg + fsync → ACK (ms)
   every ~2 s: rotate segment → Parquet per event hour → PUT → delete segment
        │
        ▼
 ┌──────────────────────── Object storage bucket ─────────────────────────┐
 │ l0/spans/hour=2026102514/<node_id>-000042.parquet      from ingesters  │
 │ data/spans/hour=2026102514/<uuid>.parquet              from compactor  │
 │ data/spans/hour=2026102514/gen-000003.json             commit (1/hour) │
 └────────────────────────────────────────────────────────────────────────┘
        ▲                                                  ▲
   compactor (exactly 1)                              query-1..N
   per hour: merge · dedup · commit gen · GC          DataFusion + local cache
                                                           ▲
                                               agents (MCP) · HTTP · CLI
```

All roles run from one image: `otelake --role=all|ingest|query|compactor`. A hobby setup runs `--role=all` in one container.

**End-to-end flow:**

```
OTLP request
 → decode + flatten into rows                              (1, 2)
 → grouped with concurrent requests                        (3)
 → append to current WAL segment + fsync                   (4)
 → reply 200 OK                                            ← durable on local disk
 → every ~2 s: segment closed → Parquet per event hour
     → PUT l0/<signal>/hour=H/<node_id>-<seg>.parquet
     → delete segment                                      ← durable in bucket, visible
 → compactor, per hour: ≥10 small files or hour closed
     → merge (dedup adjacent duplicates) → PUT data files
     → PUT gen-N+1.json {files, consumed_l0}
     → after grace: delete consumed L0 + files of older gens
 → after N days: retention deletes hour=H/ prefixes

memory / disk / upload backlog over limit → 429 / RESOURCE_EXHAUSTED   (6)
```

---

# Ingestion

## 1. Receivers

- OTLP gRPC on `:4317` (Trace, Logs, Metrics services).
- OTLP HTTP on `:4318`: `/v1/traces`, `/v1/logs`, `/v1/metrics`, protobuf and JSON, gzip/zstd.
- Max request size 16 MB (configurable).
- Responses use OTLP `PartialSuccess` (`rejected_*` count + message). Data is only rejected if undecodable; anything decodable is sanitized and accepted.

## 2. Decode and normalize

**Why:** OTLP is nested to avoid repeating resource data on the wire:

```
ExportTraceServiceRequest
 └─ ResourceSpans   resource: {service.name=checkout, host.name=abc, k8s.pod.name=xyz, ...}
     └─ ScopeSpans  scope: {name=io.opentelemetry.http, version=1.2}
         └─ Span[]  {trace_id, span_id, name, start, end, status, attributes{...}}
```

A span alone doesn't know its service. Query engines need flat tables, so each request is flattened into **one row per span / log record / data point**, with resource and scope data copied onto each row.

### Tables (one per signal, fully denormalized)

`spans`:
```
tenant, trace_id (16 bytes), span_id (8 bytes), parent_span_id, trace_state,
name, kind, start_time_ns, end_time_ns, duration_ns,
status_code, status_message, is_root, is_error,
service_name, resource_attrs (map), scope_name, scope_version,
attrs_str, attrs_int, attrs_f64, attrs_bool   (maps),
events (list), links (list),
received_at_ns, ingest_flags
```

`logs`:
```
tenant, time_ns, observed_time_ns, severity_number, severity_text,
body_str, body_json, event_name, trace_id, span_id, flags,
service_name, resource_attrs (map), scope_name, scope_version,
attrs_* (maps), received_at_ns, ingest_flags
```

`metrics` (one row per data point):
```
tenant, name, unit, type, temporality, is_monotonic,
series_id = hash(name, unit, type, temporality, resource, scope, sorted attributes),
service_name, resource_attrs (map), scope_name, scope_version, attrs_* (maps),
time_ns, start_time_ns, value_f64 | value_i64,
histogram: count, sum, min, max, bounds, bucket_counts,
exp. histogram: scale, zero_count, pos_offset, pos_counts, neg_offset, neg_counts,
summary: quantiles, exemplars, received_at_ns, ingest_flags
```

### Column choices

| Choice | Reason |
|---|---|
| Everything denormalized (resource, scope, metric labels on every row) | No joins, no fingerprint tables, no cross-table dedup. Rows are sorted by service / series, so repeated values sit next to each other and dictionary + run-length encoding shrink them to a few bytes. Expected cost +10–30% storage, measured in M1 |
| `service_name` as its own column | Most common filter; used in the sort key |
| `series_id` column on metrics | Groups points of one series for sorting and `rate()`; no separate series table |
| Span attributes in typed maps | Keys are unbounded, so no column per key. Typed maps keep `status_code >= 500` numeric; a key sent as int by one service and string by another doesn't break anything |
| `resource_attrs` as one string map | Resource attributes are almost always strings |
| `trace_id` as 16 raw bytes | Half the size of hex; faster comparisons and bloom filters |
| Precomputed `duration_ns`, `is_error`, `is_root` | Most common predicates: compute once at write. `is_error` keeps semconv rules in one place |

### Normalization rules

- `service.name` missing → `unknown_service`.
- `is_error`: span status ERROR, or server span with `http.response.status_code >= 500`, or log severity ≥ ERROR.
- `is_root`: `parent_span_id` empty.
- Sanitization (never reject): invalid UTF-8 → U+FFFD; values > 64 KB truncated; timestamp 0 → `received_at`; > 1 day in the future → clamped. Each sets a bit in `ingest_flags` and increments a self-metric.

## 3. Batching (group commit)

**Problem:** durability on local disk requires `fsync()` (force the OS page cache to the physical disk). One fsync costs ~0.1–1 ms on NVMe, 1–10 ms on SATA SSD / network block volumes. One fsync per request caps throughput at a few hundred req/s on slow disks.

**Solution:** all requests arriving while the previous fsync is in progress are written together with **one fsync**, then all are acked. Throughput is bound by disk bandwidth, not fsync latency. Ack latency stays in the low milliseconds.

## 4. Local WAL: one segment = one upload unit

**Problem:** good Parquet files need many rows, and uploading to object storage takes 20–200 ms per PUT. Acking only after an upload adds latency per request. Acking before anything is durable means a crash loses data the Collector already dropped.

**Solution:** append the raw request to a local log segment, fsync, ack. Every ~2 s the segment is closed, converted to Parquet and uploaded; then the segment file is deleted. **A segment file that still exists has not been uploaded** — no LSNs, markers or bookkeeping.

### Layout and format

```
wal/node_id                 stable random ID, generated on first start
wal/spans/000042.seg        one directory per signal, one open segment each
wal/logs/000017.seg
wal/metrics/000031.seg
```

- Record: `[u32 len][u32 crc32c][payload: zstd(raw OTLP request)]`. Raw OTLP means a decoder bug can be fixed and the data rebuilt.
- The open segment's rows are also kept in memory as Arrow, so normal uploads don't re-decode.
- Location: Docker named volume or Hetzner Volume. Holds only seconds of data in normal operation.

### Segment lifecycle

1. **Rotate** every `upload_interval` (default 2 s) or at 32 MB: close the current segment, open the next number, fsync the directory.
2. **Convert:** rows of the closed segment → one Parquet file **per event-time hour** they belong to (normally one; late data produces a second file for an older hour).
3. **Upload:** PUT `l0/<signal>/hour=<H>/<node_id>-<seg>.parquet`. The name is deterministic, so a retried upload overwrites the same object — idempotent, never duplicated.
4. **Delete** the segment file after all its PUTs succeeded; data is now visible to queries.
5. Upload failures: segment stays, upload retries with backoff, newer segments queue behind it.

### Recovery

1. Truncate the newest segment at the first CRC or length mismatch (a torn write that was never acked; the Collector retries it).
2. Every segment on disk is not yet uploaded: decode, convert, upload (same names → idempotent), delete.

### Graceful shutdown

On SIGTERM: readiness → false, stop accepting requests, close and upload all segments, exit. Deploys and scale-downs lose nothing and leave an empty WAL.

### Failure behavior

| Event | Data loss |
|---|---|
| Deploy / scale-down / `docker stop` | None (drained) |
| Process crash, same host | None (segments re-uploaded) |
| VM lost, WAL on Hetzner Volume (network block storage) | None (reattach volume to new VM) |
| VM lost, WAL on local disk | ≤ one upload interval (~2 s) of acked data |

## 5. Ack after upload (optional, fully stateless)

Same pipeline, one switch: `ingest.ack_on = "upload"` (default `"fsync"`).

- Segments rotate every ~250 ms; requests are acked when their segment's upload succeeded.
- No local state survives anything, so any container can disappear without loss.
- Ack latency ~0.4–0.6 s (WarpStream-like). Throughput is unaffected: one PUT per signal per segment, regardless of request count, and uploads overlap. Collectors keep more requests in flight (`throughput ≈ num_consumers × batch_size / ack_latency`).
- More, smaller L0 files; the compactor handles them.

## 6. Backpressure

**Problem:** the input rate is outside our control (incident log storms 10–100×, Collector backlogs after outages, buggy services). Unbounded intake leads to OOM kill → data loss → every Collector retries at once → crash loop.

**Solution:**
- One memory budget (`memory_limit`, default 1 GB) covering in-flight decoding, open-segment rows and upload buffers.
- At 80%: rotate early. At 100%: reject with gRPC `RESOURCE_EXHAUSTED` + `RetryInfo` / HTTP 429 + `Retry-After`. OTLP defines these as retryable; the Collector's `sending_queue` and `retry_on_failure` keep the data and back off.
- **Upload backlog** (bucket slow or down): closed segments accumulate; when they exceed `wal_max_bytes` (default 10 GB) or free disk < 5%, reject ingest. Queries keep running.
- `/readyz` reports the reason.

---

# Storage

## 7. Bucket layout

```
l0/spans/hour=2026102514/<node_id>-000042.parquet      uploaded by ingesters
data/spans/hour=2026102514/<uuid>.parquet              written by compactor
data/spans/hour=2026102514/gen-000003.json             generation (commit) for this hour
```

Same for `logs` and `metrics`. Hours are event-time hours (UTC).

Local disk per container: `wal/` (ingest only) and `cache/` (query; safe to lose).

## 8. Parquet files

- zstd level 3 for L0, higher (e.g. 9) for compacted files; dictionary encoding; page index; ~1 MB pages; ~128 MB row groups after compaction.
- Bloom filters on `trace_id` (spans, logs) and `series_id` (metrics).
- Sort keys:
  - spans: `(service_name, start_time_ns, trace_id, span_id)`
  - logs: `(service_name, time_ns)`
  - metrics: `(series_id, time_ns)`
- Each file carries its own schema, so schema changes need no migration.

## 9. Per-hour generations (catalog)

Each `(signal, hour)` has its own tiny commit history. There is no global catalog.

`data/spans/hour=H/gen-000003.json`:
```json
{
  "gen": 3,
  "files": [{"path": "…/<uuid>.parquet", "rows": 1203344, "bytes": 51023311,
             "min_time": …, "max_time": …, "blake3": "…"}],
  "consumed_l0": ["l0/spans/hour=H/node-a-000040.parquet", "…"]
}
```

- **Single writer: the compactor.** Only it writes `gen-*.json`, so generations never conflict. Enforced by running exactly one compactor (`replicas: 1`); if the bucket supports conditional PUT (`If-None-Match: *`), it is used as a safety net against a second compactor.
- A generation is committed by a single PUT of the next `gen-N.json` after its data files are uploaded. PUT is atomic, so a generation is either fully visible or not at all.

### What a query sees for one hour

```
visible files(H) = files in latest gen-*.json
                 ∪ (L0 files under l0/<signal>/hour=H/  −  consumed_l0 of that gen)
```

Query nodes LIST the hours a query touches (cached ~2 s). Consumed L0 files and files of older generations are deleted only after a grace period (10 min) longer than the cache TTL plus the max query duration. So a query never double-counts (consumed L0 is excluded) and never misses data (old files exist until every reader has moved on).

## 10. Compaction (single compactor, per hour)

**Why:** 2 s uploads create ~1,800 small files per hour per signal per ingester. Many small files mean more requests, worse compression and weaker skipping.

**One rule per hour**, checked every minute:
- the hour has **≥ 10 uncompacted small files**, or
- the hour is **closed** (end + 10 min grace for late data) and has any uncompacted files.

Then:
1. Read the selected files with a **streaming k-way merge**. Inputs are already sorted by the sort key, so memory holds one chunk per input file regardless of hour size.
2. **Dedup spans:** duplicates (from Collector retries) have identical sort keys, so they end up adjacent. Compare each row with the previous one; on a match keep the latest `received_at`. Constant memory, no hash set.
3. Write the merged data files; PUT `gen-N+1.json` listing the new file set and the consumed L0 files.
4. After grace: delete consumed L0 files and files no longer referenced.

- Size-tiered: small files merge into medium ones while the hour is open; the final pass at hour close produces the sorted hourly file set.
- **Late data** lands as new L0 files under the old hour (see §4) and is picked up by the next pass for that hour.
- Every job is idempotent: a crash mid-job leaves unreferenced objects and no new generation; the job reruns and GC removes the leftovers.
- Compactor down → ingest and queries keep working (queries slow down as L0 accumulates); on restart it continues from each hour's latest generation.

## 11. Retention and GC

- **Time:** delete `l0/<signal>/hour=H/` and `data/<signal>/hour=H/` prefixes older than `retention.<signal>` (defaults: spans/logs 7 d, metrics 30 d).
- **Size:** when total bytes > `max_bytes`, delete the oldest hours across signals until < 90%.
- **GC (per hour):** delete objects that are neither in the latest generation nor unconsumed L0, once older than the grace period; delete older `gen-*.json` files.

## 12. Integrity: `fsck`

- For every hour: all files in the latest generation exist and match their blake3 hash.
- Lists unreferenced objects and L0 files older than expected.
- Verifies local WAL segment checksums (ingest role).
- `--repair`: delete unreferenced objects after grace, truncate torn WAL tails.

---

# Retrieval

## 13. Query engine

**Apache DataFusion** — a Rust library that parses SQL, optimizes the plan (pushes filters and needed columns into the scan) and executes it vectorized (Arrow arrays of thousands of values) across all cores, streaming, with spill-to-disk. Our code only decides **which files** make up a table; DataFusion's built-in Parquet scan does pruning, reading and execution.

### How a query runs

```sql
SELECT service_name, count(*) AS errors
FROM spans
WHERE start_time_ns > ago('1h') AND is_error
GROUP BY service_name ORDER BY errors DESC LIMIT 10
```

```
1. MCP / HTTP      validate: read-only (SELECT / WITH), limits
2. DataFusion      parse → plan: Scan(spans) → Filter → Aggregate → Sort → Limit
3. Optimizer       needed columns: service_name, start_time_ns, is_error
                   filters pushed into scan: time > now-1h, is_error = true
4. TableProvider   hours in range → visible files per hour (§9)
5. Parquet scan    footer (cached) → skip row groups by stats
                   → range GETs for 3 of ~25 column chunks → local cache
6. Execution       parallel filter → partial counts per core → merge → top 10
7. Formatter       table + {files, bytes_scanned, elapsed_ms, sql}
```

Rough latency: tens of ms with a warm cache; a few hundred ms cold from the bucket.

- Limits: 30 s timeout; max estimated scan bytes (from file sizes, checked before execution); default 1000 result rows; capped concurrent range GETs per container.
- Deterministic output: every result has a tie-break order.
- Error messages written for agents, e.g.:
  - `unknown column "status"; did you mean "status_code"?`
  - `query would scan 12 GB (limit 2 GB); add a filter on start_time_ns`
- One query runs inside one container; scale by adding query containers.

### Views and functions

All computed live from the base tables; no precomputed rollups in v1.

| View | Definition |
|---|---|
| `spans`, `logs`, `metrics` | Base tables |
| `traces` | `GROUP BY trace_id` over `spans`: root name/service, start, end, span count, error count, services |
| `service_red` | Per service / span name / minute: count, errors, latency percentiles over `spans` |

Functions: `attr(row, 'key')`, `ago('1h')`, `ts('2026-10-25 14:00')`, `hex(trace_id)` / `unhex(...)`, `rate()`, `histogram_quantile()`.

## 14. MCP tools

| Tool | Input | Output |
|---|---|---|
| `catalog` | optional `service` | Tables/columns with types and descriptions; services with counts and last-seen; available time range; top attribute keys per service; example queries |
| `query` | `sql`, optional `limit`, `format` (md/csv/json) | Rows, `truncated`, `rows_total`, `bytes_scanned`, `elapsed_ms` |
| `trace` | `trace_id`, optional `time_hint` | Indented span tree; errors highlighted; repeated siblings collapsed (`×37 SELECT users, p50 2ms`); capped at ~200 spans. With `time_hint` only nearby hours are checked; without, search newest-first and stop at first match |
| `overview` | `window` (default 1h) | Per service: rate, error %, p50/p95 vs the previous window; top errors; new `service.version` values |

Every response includes the SQL it ran.

## 15. HTTP and CLI

- HTTP: `POST /api/v1/query` (`{sql, format}` → JSON / CSV / Arrow IPC), `GET /api/v1/traces/{id}`, `/mcp` (streamable HTTP MCP), `/healthz`, `/readyz`.
- CLI: `otelake run [--role=...]`, `otelake query "<sql>"`, `otelake trace <id>`, `otelake fsck [--repair]`, `otelake demo`.
- Config: optional `otelake.toml`; every key overridable via `OTELAKE_*` env vars.

```toml
[storage]
backend  = "s3"                                    # or "local"
endpoint = "https://fsn1.your-objectstorage.com"
bucket   = "otelake"

[ingest]
ack_on          = "fsync"                          # or "upload"
upload_interval = "2s"
memory_limit    = "1GB"
wal_max_bytes   = "10GB"

[query]
cache_size = "20GB"

[retention]
spans   = "7d"
logs    = "7d"
metrics = "30d"
```

---

# Dependencies

Reuse well-known crates for everything that isn't otelake's core logic. All are Apache-2.0 or MIT.

| Area | Crate | Used for |
|---|---|---|
| Runtime | `tokio` | Async runtime, signals, graceful shutdown |
| OTLP types | `opentelemetry-proto` (+ `prost`) | Generated OTLP protobuf types, gRPC services, JSON via serde |
| gRPC server | `tonic` | OTLP gRPC receiver, gzip/zstd |
| HTTP server | `axum`, `tower`, `tower-http` | OTLP HTTP, query API, MCP transport; decompression, concurrency limits, load shedding |
| Columnar | `arrow`, `parquet` (arrow-rs) | In-memory rows, Parquet read/write, bloom filters |
| Query | `datafusion` | SQL, planning, execution, Parquet scan with pruning |
| Object storage | `object_store` | S3 / Hetzner / MinIO / R2 / local dir; retries, multipart, range reads |
| MCP | `rmcp` | Official Rust MCP SDK |
| Compression / hashing | `zstd`, `crc32c`, `blake3` | WAL payloads, record checksums, file checksums |
| CLI / config | `clap`, `serde`, `toml`, `figment` | Commands, TOML + env merging |
| Errors | `thiserror`, `anyhow` | Library and binary errors |
| Self-telemetry | `tracing`, `opentelemetry`, `opentelemetry-otlp`, `tracing-opentelemetry` | otelake's own logs, traces, metrics |
| Allocator | `mimalloc` | Lower fragmentation under steady ingest |
| Testing | `proptest`, `insta`, `cargo-nextest`, `cargo-fuzz`, `criterion`, `testcontainers` | Property tests, snapshot (golden) tests, fast runner, fuzzing, benchmarks, MinIO in tests |
| Oracle | `duckdb` (dev-only, bundled) | Independent query results for comparison |

**Written ourselves** (the core, kept small): WAL segments (~500 lines; no widely trusted crate fits the segment-as-upload-unit model), OTLP → Arrow flattening, per-hour generations, compactor, table provider, MCP tools.

**Considered and not used in v1:**
- `deltalake` (delta-rs) / `iceberg` — mature table formats with Rust support, but they bring back a global commit log and checkpoints, and safe commits need conditional writes (undocumented on Hetzner). Per-hour generations are simpler and keep plain Parquet that DuckDB reads with a glob.
- OTel Arrow Rust crates (`otap-dataflow`) — experimental; OTAP is out of scope.
- Simulation frameworks (`turmoil`, `madsim`) — deferred with deterministic simulation testing.

---

# Deployment

## Topology (Hetzner)

```
apps (OTel SDK)
  └─ OTel Collector (persistent queue: file_storage + retry_on_failure)
        │ OTLP over Hetzner private network or Tailscale
        ▼
Hetzner Cloud VM(s), same location as the bucket (e.g. fsn1)
  ├─ otelake containers (docker compose)
  ├─ Caddy / Traefik (TLS, auth for MCP/HTTP, L7 load balancing)
  └─ Tailscale (optional, private agent access)
        │ S3 API, same network zone (free traffic)
        ▼
Hetzner Object Storage bucket
```

```yaml
# docker-compose.yml (sketch)
services:
  ingest:
    image: otelake
    command: run --role=ingest
    volumes: [wal:/var/lib/otelake/wal]
    stop_grace_period: 60s
  query:
    image: otelake
    command: run --role=query
    deploy: { replicas: 2 }
  compactor:
    image: otelake
    command: run --role=compactor
    deploy: { replicas: 1 }          # must be exactly 1
  caddy:
    image: caddy
volumes:
  wal: {}
```

Hobby: a single `otelake run --role=all` container with a `wal` volume and a bucket (or a local directory as "bucket").

**Rules:**
- WAL on local SSD or a Hetzner Volume; **never NFS/SMB** (fsync semantics).
- Put a Collector in front; its persistent queue covers backpressure and restarts.
- Use OTLP/HTTP or an HTTP/2-aware L7 load balancer; a TCP balancer pins long-lived gRPC connections to one container.
- No auth in v1: keep OTLP on a private network; expose MCP/HTTP via Tailscale or a reverse proxy with auth.

## Failure scenarios

| Failure | Behavior | Data loss |
|---|---|---|
| Ingest container restart / deploy | Drain or re-upload segments | None |
| VM lost (WAL on Hetzner Volume) | Reattach volume, re-upload | None |
| VM lost (WAL on local disk) | New container | ≤ ~2 s |
| Query container lost | Others serve; cold cache on restart | None |
| Compactor down | Ingest and queries continue; L0 accumulates | None |
| Bucket unavailable | Segments queue locally until `wal_max_bytes`, then 429; uploads retry; queries on bucket data fail with a clear error | None |
| Whole location down | Collector queues buffer | None if Collector queue is large enough |

## Hetzner Object Storage limits

From Hetzner docs (verify current values):

- Pricing: €6.49/month base incl. 1 TB storage + 1 TB egress; then ~€6.35/TB/month. No per-request fees; internal traffic free.
- 750 requests/s per bucket and per source IP; 10 Gbit/s per bucket → cap concurrent range GETs, rely on the cache.
- 50M objects per bucket → GC must delete consumed L0 files (each ingester creates ~130k L0 objects/day at 2 s × 3 signals).
- Minimum billable object size 64 KB → small L0 files are billed as 64 KB; negligible since they live ~1 h.
- Objects are immutable (matches our design).
- Conditional PUT: listed as unsupported on versioned buckets; unversioned support undocumented → tested in M0; design doesn't depend on it.
- LIST read-after-write consistency → tested in M0.

---

# Capacity (estimates, to be replaced by M1 benchmarks)

Ingest is limited by **items/s and bytes/item**, not requests/s.

| | Raw OTLP | On disk (Parquet + zstd) |
|---|---|---|
| Span | ~500 B | ~40–80 B (+10–30% for denormalization, to be measured) |
| Log record | ~300 B | ~30–60 B |
| Metric point | ~16 B + labels | ~3–6 B (≪1 B with a multi-point layout, see Deferred) |

CPU: ~50–150k items/s per core for decode + WAL + Parquet encode; compaction costs about the same again.

| | Homelab | Small team | Single-node ceiling |
|---|---|---|---|
| Workload | 10 services, 50 spans/s, 50 logs/s, 5k series | 50 services, 2k spans/s, 2k logs/s, 100k series | 50k spans/s, 50k logs/s, 1M series |
| Items/s | ~430 | ~10k | ~170k |
| Storage/day | ~0.5 GB | ~18 GB | ~400 GB |
| Total (7 d spans/logs, 30 d metrics) | ~6 GB | ~180 GB | ~3.5 TB |
| Hetzner | Small ARM VM + bucket base | 4–8 vCPU / 8–16 GB + bucket base | Dedicated 8–16 cores / 64 GB + ~3.5 TB bucket |
| Rough cost | ~€10–15/month | ~€15–30/month | ~€70–120/month |

Practical v1 target: ~10–20k items/s with headroom. Query cost depends on data scanned, not on data stored, so longer retention does not slow down time-bounded queries. Long-window aggregates (e.g. 30 d RED) are computed live in v1 and get faster once precomputed rollups are added.

**Storage levers** (deferred): attribute promotion to columns (20–40%), higher zstd on compacted files (10–20%), multi-point metric layout (large), log templating (50%+ on logs), age-based downsampling (60–90% of old spans), dropping noisy attributes in the Collector.

---

# Code layout

Dependencies point downward only; every boundary is an interface, so an agent working on one module needs only that module's context.

| Module | Role |
|---|---|
| `otlp` | Receivers, decode + flatten to Arrow |
| `wal` | Segments, checksums, fsync, rotation, recovery |
| `ingest` | Group commit, upload loop, `ack_on`, backpressure, drain |
| `store` | `object_store` wrapper (local / S3), Parquet read/write, cache |
| `catalog` | Per-hour generations, visibility rules |
| `compactor` | Merge, dedup, retention, GC |
| `query` | Table provider, views, functions |
| `mcp` | MCP tools |
| `server` | Binary, roles, config, CLI |
| `testkit` | Generator, oracle, crash harness, fault-injecting store, fixtures (dev-only) |

```
server → ingest → otlp, wal, store
server → compactor → catalog, store
server → mcp → query → catalog, store
testkit → everything
```

# Verification

| Check | What it proves |
|---|---|
| **Seeded generator** | Realistic, reproducible input: fan-out / async / messaging topologies, correlated logs, all metric types, edge cases (Unicode, huge attributes, duplicates, late data, zero timestamps, deep traces) |
| **DuckDB oracle** | Same OTLP loaded naively into DuckDB; same SQL on both. Independent engine: catches storage, catalog and DataFusion bugs |
| **Property tests** | Every acked write becomes queryable; compaction preserves results; retention only removes data outside its window; re-uploading a segment produces the same objects |
| **Crash loop** | Load with recorded acks; `kill -9` at random (incl. during upload and compaction); restart; every acked item present exactly once after compaction; `fsck` clean |
| **Fault-injecting object store** | Errors, latency, partial uploads, slow LIST on every operation; MinIO in CI for real S3 behavior |
| **Destroy-and-restore test** | Drain an ingest container, kill compactor and query nodes, start fresh ones from the bucket only; all data present |
| **Golden fixtures** | `testdata/otlp/*.json` → `expected/*.csv` for normalization and MCP output (`insta` snapshots) |
| **Hetzner compatibility test** | LIST after PUT consistency, conditional PUT, range GETs, request limits against a real bucket |

`just verify` runs format, lint, unit, property, oracle tests and a short crash loop in < 2 min. Must pass before every commit. `just verify-deep` runs long crash loops, fault injection and MinIO tests.

# Milestones

| | Scope | Gate |
|---|---|---|
| M0 | Generator, oracle, crash harness, fault-injecting store, CI, AGENTS.md, Hetzner compatibility test | Harness green on a stub |
| M1 | Traces: OTLP → WAL → L0 upload (local dir backend) → DataFusion SQL, `--role=all` | Oracle + round-trip + crash loop pass; first benchmarks |
| M2 | Logs; per-hour generations; compactor (merge, dedup); retention; GC | Compaction preserves results; destroy-and-restore passes |
| M3 | MCP tools, HTTP, CLI; S3 backend on Hetzner; roles; `ack_on = "upload"`; docker compose | Golden outputs; first agent eval on OTel Demo |
| M4 | Metrics | Oracle over full metrics data model |
| M5 | Performance and storage levers (precomputed rollups, attribute promotion, per-file summaries) | Benchmarks; storage per item reduced |

# Deferred (fits behind existing interfaces)

Precomputed rollups and trace summaries; per-file summaries for skipping; age-based downsampling; querying unflushed ingester data; external log WAL (NATS JetStream / Redpanda, Mimir-3 style) for zero-loss without volumes; OTAP; auth; multi-tenancy; multi-point metric layout; adaptive attribute promotion; learned partitioning; log templating; error fingerprints; baselines; entity graph; embeddings; `diff` / `explain` / `what_changed` tools; token budgets + result handles; Prometheus API; distributed query execution; deterministic simulation testing; mutation testing.

# Prior art

| System | What we took |
|---|---|
| OpenObserve | Rust + DataFusion + Parquet on object storage; local WAL with async upload. We upload faster (2 s vs up to minutes) and don't query ingesters |
| Loki / Mimir / Tempo | Separate ingest / query / compactor roles; Kafka-based ingest as a future option |
| WarpStream | Stateless agents acking after an S3 PUT → `ack_on = "upload"` |
| Bronto | Immutable timestamped files, adaptive columnar layout, summaries for skipping, precomputation, SQL + MCP across signals |
| Delta Lake / Iceberg | Commit-file idea, scoped down to one hour instead of a global log |
| ClickHouse async inserts | Server-side group commit |

# Open decisions

1. **Attribute storage:** typed maps (proposed) vs single `Map<String,String>`.
2. **First signal:** traces (proposed) vs logs.
3. **Upload interval default:** 2 s (proposed); trade-off is visibility vs number of L0 files.
