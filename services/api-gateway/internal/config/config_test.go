package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	keys := []string{
		"FRESHFLOW_ENV",
		"FRESHFLOW_LOG_LEVEL",
		"FRESHFLOW_HTTP_ADDR",
		"FRESHFLOW_POSTGRES_DSN",
		"FRESHFLOW_REDIS_ADDR",
		"FRESHFLOW_REDIS_PASSWORD",
		"FRESHFLOW_REDIS_DB",
		"FRESHFLOW_KAFKA_BROKERS",
		"FRESHFLOW_STARTUP_TIMEOUT",
		"FRESHFLOW_DEPENDENCY_CHECK_TIMEOUT",
		"FRESHFLOW_SHUTDOWN_TIMEOUT",
		"FRESHFLOW_ANALYTICS_SERVICE_URL",
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.DependencyCheckTimeout != 2*time.Second {
		t.Fatalf("DependencyCheckTimeout = %v, want 2s", cfg.DependencyCheckTimeout)
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Fatalf("KafkaBrokers = %#v", cfg.KafkaBrokers)
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	t.Setenv("FRESHFLOW_DEPENDENCY_CHECK_TIMEOUT", "soon")
	if _, err := Load(); err == nil {
		t.Fatal("Load() expected an error")
	}
}

func TestLoadSplitsKafkaBrokers(t *testing.T) {
	t.Setenv("FRESHFLOW_DEPENDENCY_CHECK_TIMEOUT", "")
	t.Setenv("FRESHFLOW_KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "kafka-2:9092" {
		t.Fatalf("KafkaBrokers = %#v", cfg.KafkaBrokers)
	}
}
