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
	"github.com/freshflow/freshflow/services/delivery-service/internal/delivery"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := runhttp.JSONLogger("delivery-service", envconfig.String("FRESHFLOW_ENV", "local"))
	slog.SetDefault(logger)
	postgres, err := pgxpool.New(context.Background(), envconfig.String("FRESHFLOW_POSTGRES_DSN", "postgres://freshflow:freshflow@localhost:5432/freshflow?sslmode=disable"))
	if err != nil {
		logger.Error("create PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: envconfig.String("FRESHFLOW_REDIS_ADDR", "localhost:6379")})
	defer redisClient.Close()
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(envconfig.CSV("FRESHFLOW_KAFKA_BROKERS", "localhost:9092")...),
		kgo.ClientID("delivery-service"),
		kgo.ConsumerGroup(envconfig.String("FRESHFLOW_KAFKA_GROUP", "freshflow.delivery-service.v1")),
		kgo.ConsumeTopics("freshflow.order.events.v1", "freshflow.courier.location.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		logger.Error("create Kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	relay, err := outbox.NewRelay(postgres, kafkaClient, "delivery", logger)
	if err != nil {
		logger.Error("create outbox relay", "error", err)
		os.Exit(1)
	}
	backgroundCtx, cancelBackground := context.WithCancel(context.Background())
	defer cancelBackground()
	go relay.Run(backgroundCtx)
	consumer := delivery.NewConsumer(postgres, redisClient, kafkaClient, logger, 55.0302, 82.9204)
	go consumer.Run(backgroundCtx)
	etaClient := delivery.NewHTTPETAClient(envconfig.String("FRESHFLOW_ETA_SERVICE_URL", "http://localhost:8090"), 2*time.Second)
	go delivery.NewPredictionWorker(postgres, etaClient, logger, 3*time.Second).Run(backgroundCtx)

	handler := delivery.NewHandler(postgres, logger)
	mux := http.NewServeMux()
	health.Register(mux, "delivery-service", logger, 2*time.Second, []health.Checker{
		{Name: "postgres", Check: postgres.Ping},
		{Name: "redis", Check: func(ctx context.Context) error { return redisClient.Ping(ctx).Err() }},
		{Name: "kafka", Check: kafkaClient.Ping},
		{Name: "eta-service", Check: etaClient.Ready},
	})
	handler.Register(mux)
	if err := runhttp.Run(envconfig.String("FRESHFLOW_HTTP_ADDR", ":8084"), httpx.Wrap("delivery-service", logger, mux), logger, 10*time.Second); err != nil {
		logger.Error("service stopped with error", "error", err)
		os.Exit(1)
	}
}
