package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/envconfig"
	"github.com/freshflow/freshflow/pkg/platform/health"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/outbox"
	"github.com/freshflow/freshflow/pkg/platform/runhttp"
	"github.com/freshflow/freshflow/services/catalog-service/internal/catalog"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := runhttp.JSONLogger("catalog-service", envconfig.String("FRESHFLOW_ENV", "local"))
	slog.SetDefault(logger)
	postgres, err := pgxpool.New(context.Background(), envconfig.String("FRESHFLOW_POSTGRES_DSN", "postgres://freshflow:freshflow@localhost:5432/freshflow?sslmode=disable"))
	if err != nil {
		logger.Error("create PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: envconfig.String("FRESHFLOW_REDIS_ADDR", "localhost:6379")})
	defer redisClient.Close()
	kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(envconfig.CSV("FRESHFLOW_KAFKA_BROKERS", "localhost:9092")...), kgo.ClientID("catalog-service"))
	if err != nil {
		logger.Error("create Kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()
	relay, err := outbox.NewRelay(postgres, kafkaClient, "catalog", logger)
	if err != nil {
		logger.Error("create outbox relay", "error", err)
		os.Exit(1)
	}
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	go relay.Run(relayCtx)

	store := catalog.NewStore(postgres, redisClient, 30*time.Second)
	handler := catalog.NewHandler(store, logger)
	mux := http.NewServeMux()
	health.Register(mux, "catalog-service", logger, 2*time.Second, []health.Checker{
		{Name: "postgres", Check: postgres.Ping},
		{Name: "redis", Check: func(ctx context.Context) error { return redisClient.Ping(ctx).Err() }},
		{Name: "kafka", Check: kafkaClient.Ping},
	})
	handler.Register(mux)

	if err := runhttp.Run(envconfig.String("FRESHFLOW_HTTP_ADDR", ":8081"), httpx.Wrap("catalog-service", logger, mux), logger, 10*time.Second); err != nil {
		logger.Error("service stopped with error", "error", err)
		os.Exit(1)
	}
}
