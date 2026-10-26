# otelake

An experiment of building an observability backend for agents with agents.

## What

An open-source, self-hosted OpenTelemetry backend targeting **agent retrieval**.
Today it supports OTLP ingestion and SQL queries over local telemetry through
HTTP. An MCP interface is planned.

Goals: no acked data lost · one container, near-zero config · compact, reproducible query results.
Non-goals (for now): OTel Arrow ingest, multi-tenancy, dashboards.

## Core Concepts

### Ingestion

Go HTTP server (OTLP/HTTP, `:4318`) -> Flatten + Normalize into Arrow format -> Write to Parquet file -> Upload to S3

- Batching: requests are buffered in memory and flushed every ~1 s (or N rows).
- Response: `200` only after the request's data is in a Parquet file; on failure `503`, so the Collector retries.
- Overload: bounded buffer, `429` when full. Max request size 64 MiB (`413` above).

### Schema

One fixed, denormalized table per signal (one row per span / log record / data point; resource and scope copied onto each row).
Attributes are stored in typed maps (`attrs_str`, `attrs_int`, `attrs_f64`, `attrs_bool`).
Schema changes: only add nullable columns; never rename, retype or change meaning.

### Storage

Stored in S3-like bucket -> swappable (local directory for dev).
Files are immutable, one folder per signal and event hour: `spans/hour=YYYYMMDDHH/*.parquet`, sorted by service and time.

### Retrieval

`POST /api/query` accepts `{"sql":"SELECT ..."}` on the existing HTTP server
(`:4318`). Embedded DuckDB queries local Parquet files directly through the
`spans`, `logs`, and `metrics` views. Retrieval requires a `ReadKeys` bearer
credential; ingest keys cannot read. S3 ingestion remains supported, but
retrieval with S3 storage returns `503`.

```bash
curl -sS localhost:4318/api/query \
  -H 'Authorization: Bearer <configured-read-key>' \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT service_name, count(*) AS errors FROM spans WHERE status_code = 2 GROUP BY service_name ORDER BY errors DESC"}'
```

Example response:

```json
{"columns":["service_name","errors"],"rows":[["checkout",12]],"truncated":false}
```

Use SQL for time filters, trace lookup, attributes, joins, and aggregations;
there is no separate filter language or discovery API. For example:

```sql
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'spans';
SELECT name, duration_ns / 1e6 AS duration_ms FROM spans
WHERE start_time >= TIMESTAMPTZ '2026-10-26 12:00:00+00'
  AND start_time < TIMESTAMPTZ '2026-10-26 13:00:00+00'
ORDER BY duration_ns DESC LIMIT 20;
SELECT body_str FROM logs WHERE attrs_str['http.route'] = '/cart';
SELECT name, avg(value_f64) FROM metrics GROUP BY name;
```

Send **one SELECT per request**, including CTEs. DuckDB's `query()` function
rejects multiple statements and non-SELECT commands; SQL is passed as a bound
value, not interpolated. Views expose the stored columns without signal-specific
conversion. Timestamps are DuckDB strings with nanosecond precision; use
`epoch_ns(start_time)` for integer nanoseconds and `hex(trace_id)` for readable
IDs. Maps and lists become JSON objects and arrays; missing values stay null.
Integers are emitted without float conversion (clients must also preserve large
integers). Cast NaN and infinity to VARCHAR before returning them as JSON.

A signal's view appears after its first completed Parquet file; querying a
signal that has never received data returns a missing-table error. New files
appear on subsequent queries. Results are not snapshots across requests; late
data can change a repeated query. Use an explicit `ORDER BY` for ordered results.

Defaults: one concurrent query, a 10-second deadline, 256 MiB of DuckDB-managed
memory, at most 500 returned rows, 16 KiB request bodies, and 1 MiB successful
JSON responses. `truncated` means more than 500 result rows exist; it does not
indicate an incomplete aggregation. There is no custom time-window or file-scan
budget. Disk spill and automatic extension downloads are disabled. DuckDB's
memory limit is not a total process memory cap; provision separately for
ingestion and Go.

DuckDB is configured to allow file reads only inside the local telemetry
directory, disable other external access, and lock those settings. Keep only
telemetry there, and do not allow untrusted users to write files or symlinks
there. **This endpoint is for trusted read-key holders, not a hostile-SQL
sandbox or a multi-tenant security boundary.** Use process/container isolation
for that threat model.

Invalid SQL/JSON returns `400`, wrong content type `415`, oversized bodies
`413`, excess concurrency `429` with `Retry-After`, response-budget excess `422`,
cancellation `408`, and deadline expiry `504`. Successful responses are JSON;
errors are plain text except auth failures, which retain the OTLP protobuf
`401` format. MCP and server-managed pagination are not implemented.

## Run

Set `OTELAKE_INGEST_KEYS` (write access) and `OTELAKE_READ_KEYS` (query access)
in the runtime environment. Each accepts a comma-separated list for key rotation;
whitespace around keys is trimmed. Unset or empty values deny access.
Read keys do not grant ingestion access. Do not commit secrets.
Building requires CGO and a C/C++ toolchain for embedded DuckDB.

```bash
go build -o bin/otelake .
OTELAKE_INGEST_KEYS='<write-secret>' OTELAKE_READ_KEYS='<read-secret>' ./bin/otelake
```

Health: `curl localhost:4318/health` (no authentication required).

All three OTLP endpoints require a single `Authorization: Bearer <key>` header.
Missing, malformed, or invalid credentials return `401` before the body is read.
Multiple ingest keys can be configured to overlap during rotation.

Test data (replace `<configured-ingest-key>` with a configured key):

```bash
telemetrygen traces --otlp-http --otlp-insecure --otlp-endpoint localhost:4318 \
  --otlp-header 'Authorization="Bearer <configured-ingest-key>"'
```

## Deployment

The Docker image builds with CGO on Debian and includes DuckDB's native runtime
dependencies. Multi-platform releases build natively per target (with QEMU in
CI). Authentication keys are loaded from environment variables at startup;
other configuration is currently compiled from `config.go`, not loaded from a file.
When using local storage, mount a directory writable by the container's UID 10001
and set `Storage.Local.Dir` to that mount path.

Use TLS at a reverse proxy before exposing ingestion outside a trusted network,
and prevent direct access to the backend port. Bearer authentication does not
encrypt traffic or provide tenant isolation.

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for ingestion results, methodology and run commands.
