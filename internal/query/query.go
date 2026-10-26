// Package query executes SQL over persisted local telemetry using DuckDB.
package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/niwoerner/otelake/internal/config"
)

const (
	MaxRows          = 500
	MaxResponseBytes = 1 << 20
)

var ErrResultLimit = errors.New("result exceeds 1 MiB; select fewer columns or rows")

type Result struct {
	Columns   []string            `json:"columns"`
	Rows      [][]json.RawMessage `json:"rows"`
	Truncated bool                `json:"truncated"`
}

type Engine struct {
	db  *sql.DB
	dir string
}

func New(dir string, cfg config.QueryConfig) (*Engine, error) {
	if cfg.Timeout <= 0 || cfg.MaxConcurrent < 1 || cfg.MemoryLimitBytes < 1 {
		return nil, errors.New("query limits must be positive")
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", fmt.Sprintf("?memory_limit=%dB&threads=1&temp_directory=&autoload_known_extensions=false&autoinstall_known_extensions=false", cfg.MemoryLimitBytes))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MaxConcurrent)
	db.SetMaxIdleConns(cfg.MaxConcurrent)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	// These settings are database-wide, including connections opened later.
	// Allow only the telemetry directory, disable other external access, and
	// prevent SQL from changing this policy. This is not a hostile-SQL sandbox.
	_, err = db.ExecContext(ctx, `SET allowed_directories = ['`+strings.ReplaceAll(dir+string(filepath.Separator), "'", "''")+`'];
		SET enable_external_access = false; SET lock_configuration = true`)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Engine{db: db, dir: dir}, nil
}

func (e *Engine) Close() error { return e.db.Close() }

func (e *Engine) Query(ctx context.Context, statement string) (Result, error) {
	result := Result{Rows: [][]json.RawMessage{}}
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = conn.Close() }()
	for _, signal := range []string{"spans", "logs", "metrics"} {
		pattern := filepath.Join(e.dir, signal, "hour=*", "*.parquet")
		files, err := filepath.Glob(pattern)
		if err != nil {
			return result, err
		}
		if len(files) == 0 {
			continue // read_parquet cannot bind a glob until its first file exists.
		}
		// Connection-local views avoid catalog write conflicts between readers.
		// DuckDB expands the glob on each query, so subsequent files are visible.
		_, err = conn.ExecContext(ctx, `CREATE OR REPLACE TEMP VIEW `+signal+` AS SELECT * FROM read_parquet('`+
			strings.ReplaceAll(pattern, "'", "''")+`', union_by_name=true)`)
		if err != nil {
			return result, err
		}
	}
	// query(?) parses exactly one SELECT; SQL is a bound value, never interpolated.
	// Encode in DuckDB to support nested values and nanosecond timestamps. Cast
	// JSON to VARCHAR to bypass the driver's lossy float64 JSON decoding.
	rows, err := conn.QueryContext(ctx, `SELECT coalesce(to_json(COLUMNS(*))::VARCHAR, 'null') AS "\0" FROM query(?) LIMIT ?`, statement, MaxRows+1)
	if err != nil {
		return result, err
	}
	defer func() { _ = rows.Close() }()
	result.Columns, err = rows.Columns()
	if err != nil {
		return result, err
	}
	values := make([]string, len(result.Columns))
	dest := make([]any, len(values))
	for i := range values {
		dest[i] = &values[i]
	}
	var size int
	for rows.Next() {
		if len(result.Rows) == MaxRows {
			result.Truncated = true
			break
		}
		if err := rows.Scan(dest...); err != nil {
			return result, err
		}
		row := make([]json.RawMessage, len(values))
		for i, value := range values {
			size += len(value)
			if size > MaxResponseBytes {
				return result, ErrResultLimit
			}
			row[i] = json.RawMessage(value)
		}
		result.Rows = append(result.Rows, row)
	}
	return result, rows.Err()
}
