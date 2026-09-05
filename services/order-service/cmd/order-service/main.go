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
	"github.com/freshflow/freshflow/services/order-service/internal/order"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	logger := runhttp.JSONLogger("order-service", envconfig.String("FRESHFLOW_ENV", "local"))
	slog.SetDefault(logger)
	postgres, err := pgxpool.New(context.Background(), envconfig.String("FRESHFLOW_POSTGRES_DSN", "postgres://freshflow:freshflow@localhost:5432/freshflow?sslmode=disable"))
	if err != nil {
		logger.Error("create PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(envconfig.CSV("FRESHFLOW_KAFKA_BROKERS", "localhost:9092")...),
		kgo.ClientID("order-service"),
		kgo.ConsumerGroup(envconfig.String("FRESHFLOW_KAFKA_GROUP", "freshflow.order-service.v1")),
		kgo.ConsumeTopics("freshflow.delivery.events.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		logger.Error("create Kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()
	relay, err := outbox.NewRelay(postgres, kafkaClient, "orders", logger)
	if err != nil {
		logger.Error("create outbox relay", "error", err)
		os.Exit(1)
	}
	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	go relay.Run(relayCtx)
	eventConsumer := order.NewEventConsumer(postgres, kafkaClient, logger)
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	go eventConsumer.Run(consumerCtx)

	client := &http.Client{Timeout: 3 * time.Second, Transport: otelhttp.NewTransport(http.DefaultTransport)}
	service := order.NewService(
		postgres,
		order.NewCartClient(client, envconfig.String("FRESHFLOW_CART_SERVICE_URL", "http://localhost:8082")),
		order.NewCatalogClient(client, envconfig.String("FRESHFLOW_CATALOG_SERVICE_URL", "http://localhost:8081")),
		10*time.Minute,
	)
	handler := order.NewHandler(service, logger)
	mux := http.NewServeMux()
	health.Register(mux, "order-service", logger, 2*time.Second, []health.Checker{
		{Name: "postgres", Check: postgres.Ping},
		{Name: "kafka", Check: kafkaClient.Ping},
	})
	handler.Register(mux)

	if err := runhttp.Run(envconfig.String("FRESHFLOW_HTTP_ADDR", ":8083"), httpx.Wrap("order-service", logger, mux), logger, 10*time.Second); err != nil {
		logger.Error("service stopped with error", "error", err)
		os.Exit(1)
	}
}
