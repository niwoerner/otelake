package config

import (
	"os"
	"reflect"
	"testing"
)

func TestNewConfigAuthKeys(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ingest string
		read   string
	}{
		{name: "unset"},
		{name: "empty"},
		{name: "configured", ingest: "write-one, write-two ", read: " read-only "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTELAKE_INGEST_KEYS", tc.ingest)
			t.Setenv("OTELAKE_READ_KEYS", tc.read)
			if tc.name == "unset" {
				for _, name := range []string{"OTELAKE_INGEST_KEYS", "OTELAKE_READ_KEYS"} {
					if err := os.Unsetenv(name); err != nil {
						t.Fatal(err)
					}
				}
			}
			cfg := NewConfig()
			wantIngest, wantRead := []string{""}, []string{""}
			if tc.name == "configured" {
				wantIngest, wantRead = []string{"write-one", "write-two"}, []string{"read-only"}
			}
			if !reflect.DeepEqual(cfg.IngestKeys, wantIngest) || !reflect.DeepEqual(cfg.ReadKeys, wantRead) {
				t.Fatalf("auth keys = %q, %q; want %q, %q", cfg.IngestKeys, cfg.ReadKeys, wantIngest, wantRead)
			}
		})
	}
}
