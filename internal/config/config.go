package config

const (
	defaultHTTPAddress  = ":4318"
	defaultMaxBodyBytes = 67108864 // 64 MiB, the OTLP spec's recommended default
)

// Config is the root configuration structure
type Config struct {
	HTTP HTTPConfig
}

// HTTPConfig holds the OTLP/HTTP server settings
type HTTPConfig struct {
	Address      string
	MaxBodyBytes int64
}

func NewConfig() *Config {
	return &Config{
		HTTP: HTTPConfig{
			Address:      defaultHTTPAddress,
			MaxBodyBytes: defaultMaxBodyBytes,
		},
	}
}
