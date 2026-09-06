package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/freshflow/freshflow/pkg/platform/envconfig"
	"github.com/freshflow/freshflow/pkg/platform/health"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/runhttp"
	"github.com/freshflow/freshflow/services/analytics-worker/internal/analytics"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	logger := runhttp.JSONLogger("analytics-worker", envconfig.String("FRESHFLOW_ENV", "local"))
	slog.SetDefault(logger)
	clickhouseConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{envconfig.String("FRESHFLOW_CLICKHOUSE_ADDR", "localhost:9000")},
		Auth: clickhouse.Auth{
			Database: envconfig.String("FRESHFLOW_CLICKHOUSE_DATABASE", "freshflow"),
			Username: envconfig.String("FRESHFLOW_CLICKHOUSE_USER", "freshflow"),
			Password: envconfig.String("FRESHFLOW_CLICKHOUSE_PASSWORD", "freshflow"),
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("create ClickHouse connection", "error", err)
		os.Exit(1)
	}
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(envconfig.CSV("FRESHFLOW_KAFKA_BROKERS", "localhost:9092")...),
		kgo.ClientID("analytics-worker"),
		kgo.ConsumerGroup(envconfig.String("FRESHFLOW_KAFKA_GROUP", "freshflow.analytics-worker.v1")),
		kgo.ConsumeTopics(
			"freshflow.order.events.v1", "freshflow.inventory.events.v1",
			"freshflow.delivery.events.v1", "freshflow.courier.location.v1",
		),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		logger.Error("create Kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	store := analytics.NewStore(clickhouseConn)
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	go analytics.NewConsumer(store, kafkaClient, logger).Run(consumerCtx)
	go analytics.NewSLOReporter(store, logger, 30*time.Second).Run(consumerCtx)

	mux := http.NewServeMux()
	analytics.NewHandler(store, logger).Register(mux)
	health.Register(mux, "analytics-worker", logger, 2*time.Second, []health.Checker{
		{Name: "clickhouse", Check: clickhouseConn.Ping},
		{Name: "kafka", Check: kafkaClient.Ping},
	})
	if err := runhttp.Run(envconfig.String("FRESHFLOW_HTTP_ADDR", ":8087"),
		httpx.Wrap("analytics-worker", logger, mux), logger, 10*time.Second); err != nil {
		logger.Error("service stopped with error", "error", err)
		os.Exit(1)
	}
}
