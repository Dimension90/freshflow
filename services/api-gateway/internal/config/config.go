package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Environment            string
	LogLevel               slog.Level
	HTTPAddr               string
	CatalogServiceURL      string
	CartServiceURL         string
	OrderServiceURL        string
	DeliveryServiceURL     string
	AnalyticsServiceURL    string
	PostgresDSN            string
	RedisAddr              string
	RedisPassword          string
	RedisDB                int
	KafkaBrokers           []string
	StartupTimeout         time.Duration
	DependencyCheckTimeout time.Duration
	ShutdownTimeout        time.Duration
}

func Load() (Config, error) {
	redisDB, err := intEnv("FRESHFLOW_REDIS_DB", 0)
	if err != nil {
		return Config{}, err
	}

	logLevel, err := parseLogLevel(env("FRESHFLOW_LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}

	startupTimeout, err := durationEnv("FRESHFLOW_STARTUP_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}
	checkTimeout, err := durationEnv("FRESHFLOW_DEPENDENCY_CHECK_TIMEOUT", 2*time.Second)
	if err != nil {
		return Config{}, err
	}
	shutdownTimeout, err := durationEnv("FRESHFLOW_SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	brokers := splitNonEmpty(env("FRESHFLOW_KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("FRESHFLOW_KAFKA_BROKERS must contain at least one broker")
	}

	return Config{
		Environment:            env("FRESHFLOW_ENV", "local"),
		LogLevel:               logLevel,
		HTTPAddr:               env("FRESHFLOW_HTTP_ADDR", ":8080"),
		CatalogServiceURL:      env("FRESHFLOW_CATALOG_SERVICE_URL", "http://localhost:8081"),
		CartServiceURL:         env("FRESHFLOW_CART_SERVICE_URL", "http://localhost:8082"),
		OrderServiceURL:        env("FRESHFLOW_ORDER_SERVICE_URL", "http://localhost:8083"),
		DeliveryServiceURL:     env("FRESHFLOW_DELIVERY_SERVICE_URL", "http://localhost:8084"),
		AnalyticsServiceURL:    env("FRESHFLOW_ANALYTICS_SERVICE_URL", "http://localhost:8087"),
		PostgresDSN:            env("FRESHFLOW_POSTGRES_DSN", "postgres://freshflow:freshflow@localhost:5432/freshflow?sslmode=disable"),
		RedisAddr:              env("FRESHFLOW_REDIS_ADDR", "localhost:6379"),
		RedisPassword:          os.Getenv("FRESHFLOW_REDIS_PASSWORD"),
		RedisDB:                redisDB,
		KafkaBrokers:           brokers,
		StartupTimeout:         startupTimeout,
		DependencyCheckTimeout: checkTimeout,
		ShutdownTimeout:        shutdownTimeout,
	}, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return value, nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return value, nil
}

func parseLogLevel(raw string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(raw))); err != nil {
		return 0, fmt.Errorf("FRESHFLOW_LOG_LEVEL is invalid: %w", err)
	}
	return level, nil
}

func splitNonEmpty(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}
