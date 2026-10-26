package config

import (
	"os"
	"strings"
	"time"
)

const (
	defaultHTTPAddress  = ":4318"
	defaultMaxBodyBytes = 67108864   // 64 MiB, the OTLP spec's recommended default
	defaultMemoryLimit  = 1073741824 // 1 GiB for requests being decoded, flattened and queued

	defaultStorageType = "local"
	defaultLocalDir    = "data"
)

// Config is the root configuration structure
type Config struct {
	HTTP    HTTPConfig
	Storage StorageConfig
	Query   QueryConfig
	// Keys are loaded from OTELAKE_INGEST_KEYS. Empty keys deny access.
	IngestKeys []string
	// ReadKeys grant retrieval access, not ingestion access.
	ReadKeys []string
}

// QueryConfig bounds local Parquet retrieval independently of ingestion.
type QueryConfig struct {
	Timeout          time.Duration
	MaxConcurrent    int
	MemoryLimitBytes int64
}

// HTTPConfig holds the OTLP/HTTP server settings
type HTTPConfig struct {
	Address      string
	MaxBodyBytes int64
	// MemoryLimitBytes bounds the memory of all requests in flight; requests
	// beyond it get 429 and are retried by the client.
	MemoryLimitBytes int64
}

// StorageConfig selects where Parquet files are stored: "local" or "s3".
type StorageConfig struct {
	Type  string
	Local LocalConfig
	S3    S3Config
}

// LocalConfig stores files in a directory on disk.
type LocalConfig struct {
	Dir string
}

// S3Config stores files in an S3-compatible bucket (AWS, Hetzner, MinIO, R2).
type S3Config struct {
	Endpoint        string // host[:port], without scheme
	Bucket          string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Insecure        bool // plain HTTP, e.g. for a local MinIO
}

func NewConfig() *Config {
	cfg := &Config{
		IngestKeys: strings.Split(os.Getenv("OTELAKE_INGEST_KEYS"), ","),
		ReadKeys:   strings.Split(os.Getenv("OTELAKE_READ_KEYS"), ","),
		HTTP: HTTPConfig{
			Address:          defaultHTTPAddress,
			MaxBodyBytes:     defaultMaxBodyBytes,
			MemoryLimitBytes: defaultMemoryLimit,
		},
		Storage: StorageConfig{
			Type:  defaultStorageType,
			Local: LocalConfig{Dir: defaultLocalDir},
		},
		Query: QueryConfig{
			Timeout:          10 * time.Second,
			MaxConcurrent:    1,
			MemoryLimitBytes: 256 << 20,
		},
	}
	for _, keys := range [][]string{cfg.IngestKeys, cfg.ReadKeys} {
		for i := range keys {
			keys[i] = strings.TrimSpace(keys[i])
		}
	}
	return cfg
}
