package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	postgres *pgxpool.Pool
	kafka    *kgo.Client
	logger   *slog.Logger
}

func NewConsumer(postgres *pgxpool.Pool, kafka *kgo.Client, logger *slog.Logger) *Consumer {
	return &Consumer{postgres: postgres, kafka: kafka, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.kafka.PollRecords(ctx, 50)
		if err := fetches.Err(); err != nil {
			if ctx.Err() == nil {
				c.logger.Warn("poll Kafka", "error", err)
			}
			continue
		}
		for _, record := range fetches.Records() {
			for {
				processed, err := c.handle(ctx, record.Value)
				if err == nil {
					if processed {
						c.logger.Info("mock notification recorded", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
					}
					break
				}
				c.logger.Warn("process Kafka event; partition is paused by retry", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			if err := c.kafka.CommitRecords(ctx, record); err != nil && ctx.Err() == nil {
				c.logger.Warn("commit Kafka offset", "error", err, "topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
			}
		}
		telemetry.ObserveKafkaFetches("notification-worker", fetches)
	}
}

func (c *Consumer) handle(ctx context.Context, value []byte) (bool, error) {
	var envelope events.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return false, fmt.Errorf("decode event envelope: %w", err)
	}
	if envelope.EventID == "" || envelope.AggregateID == "" || envelope.EventType == "" {
		return false, fmt.Errorf("event envelope is missing required fields")
	}
	ctx = httpx.WithCorrelationID(ctx, envelope.CorrelationID)
	ctx, span := telemetry.StartConsumerSpan(ctx, "notification-worker", envelope.EventType, envelope.TraceID, envelope.SpanID)
	defer span.End()
	message, supported := messageFor(envelope.EventType)
	if !supported {
		return false, nil
	}
	tx, err := c.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `INSERT INTO notifications.processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`, envelope.EventID)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO notifications.notification_log (event_id, order_id, event_type, message, correlation_id)
		VALUES ($1, $2, $3, $4, $5)`, envelope.EventID, envelope.AggregateID, envelope.EventType, message, envelope.CorrelationID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func messageFor(eventType string) (string, bool) {
	switch eventType {
	case "order.created":
		return "Заказ создан и ожидает подтверждения", true
	case "order.confirmed":
		return "Заказ подтверждён", true
	case "order.status_changed":
		return "Статус заказа изменился", true
	default:
		return "", false
	}
}
