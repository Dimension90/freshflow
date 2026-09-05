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
	"github.com/freshflow/freshflow/pkg/platform/runhttp"
	"github.com/freshflow/freshflow/services/notification-worker/internal/notification"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := runhttp.JSONLogger("notification-worker", envconfig.String("FRESHFLOW_ENV", "local"))
	slog.SetDefault(logger)
	postgres, err := pgxpool.New(context.Background(), envconfig.String("FRESHFLOW_POSTGRES_DSN", "postgres://freshflow:freshflow@localhost:5432/freshflow?sslmode=disable"))
	if err != nil {
		logger.Error("create PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(envconfig.CSV("FRESHFLOW_KAFKA_BROKERS", "localhost:9092")...),
		kgo.ClientID("notification-worker"),
		kgo.ConsumerGroup(envconfig.String("FRESHFLOW_KAFKA_GROUP", "freshflow.notification-worker.v1")),
		kgo.ConsumeTopics("freshflow.order.events.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		logger.Error("create Kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	consumer := notification.NewConsumer(postgres, kafkaClient, logger)
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	go consumer.Run(consumerCtx)

	mux := http.NewServeMux()
	health.Register(mux, "notification-worker", logger, 2*time.Second, []health.Checker{
		{Name: "postgres", Check: postgres.Ping},
		{Name: "kafka", Check: kafkaClient.Ping},
	})
	if err := runhttp.Run(envconfig.String("FRESHFLOW_HTTP_ADDR", ":8086"), httpx.Wrap("notification-worker", logger, mux), logger, 10*time.Second); err != nil {
		logger.Error("service stopped with error", "error", err)
		os.Exit(1)
	}
}
