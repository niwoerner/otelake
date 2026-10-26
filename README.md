# otelake

An experiment of building an observability backend for agents with agents.

## What

An open-source, self-hosted OpenTelemetry backend optimized for **agent retrieval**: agents (and humans) query telemetry via SQL and MCP.

Goals: no acked data lost · one container, near-zero config · compact, reproducible query results.
Non-goals (for now): OTel Arrow ingest, auth, multi-tenancy, dashboards.

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

SQL or MCP. Query engine open: DuckDB (via go-duckdb) is the main candidate.

## Run

```bash
go build -o bin/otelake .
./bin/otelake
```

Health: `curl localhost:4318/health` · Test data: `telemetrygen traces --otlp-http --otlp-insecure --otlp-endpoint localhost:4318`

## Deployment

Docker container with config file

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for ingestion results, methodology and run commands.
