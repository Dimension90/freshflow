package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/envconfig"
	"github.com/freshflow/freshflow/pkg/platform/health"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/runhttp"
	"github.com/freshflow/freshflow/services/courier-simulator/internal/simulator"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	logger := runhttp.JSONLogger("courier-simulator", envconfig.String("FRESHFLOW_ENV", "local"))
	slog.SetDefault(logger)
	redisClient := redis.NewClient(&redis.Options{Addr: envconfig.String("FRESHFLOW_REDIS_ADDR", "localhost:6379")})
	defer redisClient.Close()
	kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(envconfig.CSV("FRESHFLOW_KAFKA_BROKERS", "localhost:9092")...), kgo.ClientID("courier-simulator"))
	if err != nil {
		logger.Error("create Kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()
	deliveryURL := strings.TrimRight(envconfig.String("FRESHFLOW_DELIVERY_SERVICE_URL", "http://localhost:8084"), "/")
	httpClient := &http.Client{Timeout: 2 * time.Second, Transport: otelhttp.NewTransport(http.DefaultTransport)}
	runner := simulator.New(redisClient, kafkaClient, httpClient, deliveryURL, logger)
	backgroundCtx, cancelBackground := context.WithCancel(context.Background())
	defer cancelBackground()
	go runner.Run(backgroundCtx)

	mux := http.NewServeMux()
	health.Register(mux, "courier-simulator", logger, 2*time.Second, []health.Checker{
		{Name: "redis", Check: func(ctx context.Context) error { return redisClient.Ping(ctx).Err() }},
		{Name: "kafka", Check: kafkaClient.Ping},
		{Name: "delivery-service", Check: func(ctx context.Context) error {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, deliveryURL+"/readyz", nil)
			if err != nil {
				return err
			}
			response, err := httpClient.Do(request)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return &simulator.ReadinessError{Status: response.StatusCode}
			}
			return nil
		}},
	})
	if err := runhttp.Run(envconfig.String("FRESHFLOW_HTTP_ADDR", ":8085"), httpx.Wrap("courier-simulator", logger, mux), logger, 10*time.Second); err != nil {
		logger.Error("service stopped with error", "error", err)
		os.Exit(1)
	}
}
