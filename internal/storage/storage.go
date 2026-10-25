package storage

import (
	"context"
	"fmt"

	"github.com/niwoerner/otelake/internal/config"
)

// Bucket stores finished objects, on local disk or in S3, chosen by config.
type Bucket interface {
	// Put stores the object atomically: readers see all of it or nothing.
	Put(ctx context.Context, key string, data []byte) error
	// Delete removes the object; deleting a missing object is not an error.
	Delete(ctx context.Context, key string) error
}

func New(cfg config.StorageConfig) (Bucket, error) {
	switch cfg.Type {
	case "local":
		return &Local{dir: cfg.Local.Dir}, nil
	case "s3":
		return newS3(cfg.S3)
	default:
		return nil, fmt.Errorf("unknown storage type %q", cfg.Type)
	}
}
