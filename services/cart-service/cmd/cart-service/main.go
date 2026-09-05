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
	"github.com/freshflow/freshflow/services/cart-service/internal/cart"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := runhttp.JSONLogger("cart-service", envconfig.String("FRESHFLOW_ENV", "local"))
	slog.SetDefault(logger)
	postgres, err := pgxpool.New(context.Background(), envconfig.String("FRESHFLOW_POSTGRES_DSN", "postgres://freshflow:freshflow@localhost:5432/freshflow?sslmode=disable"))
	if err != nil {
		logger.Error("create PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()
	redisClient := redis.NewClient(&redis.Options{Addr: envconfig.String("FRESHFLOW_REDIS_ADDR", "localhost:6379")})
	defer redisClient.Close()

	store := cart.NewStore(postgres, redisClient, 24*time.Hour)
	handler := cart.NewHandler(store, logger)
	mux := http.NewServeMux()
	health.Register(mux, "cart-service", logger, 2*time.Second, []health.Checker{
		{Name: "postgres", Check: postgres.Ping},
		{Name: "redis", Check: func(ctx context.Context) error { return redisClient.Ping(ctx).Err() }},
	})
	handler.Register(mux)

	if err := runhttp.Run(envconfig.String("FRESHFLOW_HTTP_ADDR", ":8082"), httpx.Wrap("cart-service", logger, mux), logger, 10*time.Second); err != nil {
		logger.Error("service stopped with error", "error", err)
		os.Exit(1)
	}
}
