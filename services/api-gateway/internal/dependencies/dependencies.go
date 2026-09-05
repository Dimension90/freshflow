package dependencies

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/freshflow/freshflow/services/api-gateway/internal/config"
	"github.com/freshflow/freshflow/services/api-gateway/internal/httpapi"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Dependencies struct {
	Postgres *pgxpool.Pool
	Redis    *redis.Client
	Kafka    *kgo.Client
}

func Open(ctx context.Context, cfg config.Config) (*Dependencies, error) {
	postgres, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	kafkaClient, err := kgo.NewClient(kgo.SeedBrokers(cfg.KafkaBrokers...))
	if err != nil {
		postgres.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}

	deps := &Dependencies{Postgres: postgres, Redis: redisClient, Kafka: kafkaClient}
	return deps, nil
}

func (d *Dependencies) Checkers() []httpapi.Checker {
	return []httpapi.Checker{
		{Name: "postgres", Check: d.Postgres.Ping},
		{Name: "redis", Check: func(ctx context.Context) error {
			return d.Redis.Ping(ctx).Err()
		}},
		{Name: "kafka", Check: d.Kafka.Ping},
	}
}

func HTTPChecker(name, baseURL string) httpapi.Checker {
	client := &http.Client{}
	return httpapi.Checker{Name: name, Check: func(ctx context.Context) error {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/readyz", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("readiness status %d", response.StatusCode)
		}
		return nil
	}}
}

func (d *Dependencies) Close() {
	if d.Kafka != nil {
		d.Kafka.Close()
	}
	if d.Redis != nil {
		_ = d.Redis.Close()
	}
	if d.Postgres != nil {
		d.Postgres.Close()
	}
}
