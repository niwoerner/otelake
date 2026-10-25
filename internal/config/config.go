package config

const (
	defaultHTTPAddress  = ":4318"
	defaultMaxBodyBytes = 67108864 // 64 MiB, the OTLP spec's recommended default

	defaultStorageType = "local"
	defaultLocalDir    = "data"
)

// Config is the root configuration structure
type Config struct {
	HTTP    HTTPConfig
	Storage StorageConfig
}

// HTTPConfig holds the OTLP/HTTP server settings
type HTTPConfig struct {
	Address      string
	MaxBodyBytes int64
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
	return &Config{
		HTTP: HTTPConfig{
			Address:      defaultHTTPAddress,
			MaxBodyBytes: defaultMaxBodyBytes,
		},
		Storage: StorageConfig{
			Type:  defaultStorageType,
			Local: LocalConfig{Dir: defaultLocalDir},
		},
	}
}
