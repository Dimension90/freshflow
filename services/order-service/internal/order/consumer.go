package order

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/kafkax"
	"github.com/freshflow/freshflow/pkg/platform/outbox"
	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type EventConsumer struct {
	postgres *pgxpool.Pool
	kafka    *kgo.Client
	logger   *slog.Logger
}

func NewEventConsumer(postgres *pgxpool.Pool, kafka *kgo.Client, logger *slog.Logger) *EventConsumer {
	return &EventConsumer{postgres: postgres, kafka: kafka, logger: logger}
}

func (c *EventConsumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.kafka.PollRecords(ctx, 50)
		if err := fetches.Err(); err != nil {
			if ctx.Err() == nil {
				c.logger.Warn("poll delivery events", "error", err)
			}
			continue
		}
		for _, record := range fetches.Records() {
			if _, err := kafkax.Process(ctx, "order-service", c.kafka, record, c.logger, func(callCtx context.Context) error {
				return c.handle(callCtx, record.Value)
			}); err != nil {
				return
			}
			if err := c.kafka.CommitRecords(ctx, record); err != nil && ctx.Err() == nil {
				c.logger.Warn("commit delivery event offset", "error", err)
			}
		}
		telemetry.ObserveKafkaFetches("order-service", fetches)
	}
}

func (c *EventConsumer) handle(ctx context.Context, value []byte) error {
	var envelope events.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return fmt.Errorf("decode delivery event: %w", err)
	}
	ctx = httpx.WithCorrelationID(ctx, envelope.CorrelationID)
	ctx, span := telemetry.StartConsumerSpan(ctx, "order-service", envelope.EventType, envelope.TraceID, envelope.SpanID)
	defer span.End()
	targetStatus := ""
	switch envelope.EventType {
	case "delivery.assigned":
		targetStatus = "confirmed"
	case "delivery.status_changed":
		var payload struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return err
		}
		if payload.Status == "assembling" || payload.Status == "delivering" {
			targetStatus = payload.Status
		}
	case "delivery.completed":
		targetStatus = "delivered"
	default:
		return nil
	}
	if targetStatus == "" {
		return fmt.Errorf("unsupported delivery target status in %s", envelope.EventType)
	}
	tx, err := c.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `INSERT INTO orders.processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`, envelope.EventID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	var currentStatus string
	if err := tx.QueryRow(ctx, `SELECT status FROM orders.orders WHERE id = $1 FOR UPDATE`, envelope.AggregateID).Scan(&currentStatus); err != nil {
		return err
	}
	if currentStatus == targetStatus {
		return tx.Commit(ctx)
	}
	// A cancellation is terminal. Delivery events that were already in flight
	// are harmless and must not poison the consumer with an endless retry.
	if currentStatus == "cancelled" {
		return tx.Commit(ctx)
	}
	if !validTransition(currentStatus, targetStatus) {
		return fmt.Errorf("invalid order transition %s -> %s", currentStatus, targetStatus)
	}
	changedAt := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE orders.orders SET status = $2, updated_at = $3 WHERE id = $1`, envelope.AggregateID, targetStatus, changedAt); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO orders.order_status_history (order_id, previous_status, new_status, changed_at) VALUES ($1, $2, $3, $4)`, envelope.AggregateID, currentStatus, targetStatus, changedAt); err != nil {
		return err
	}
	if envelope.EventType == "delivery.assigned" {
		confirmed, err := events.New(ctx, "order.confirmed", "order-service", envelope.AggregateID, map[string]any{"order_id": envelope.AggregateID, "confirmed_at": changedAt})
		if err != nil {
			return err
		}
		if err := outbox.Insert(ctx, tx, "orders", "freshflow.order.events.v1", envelope.AggregateID, confirmed); err != nil {
			return err
		}
	}
	statusChanged, err := events.New(ctx, "order.status_changed", "order-service", envelope.AggregateID, map[string]any{
		"order_id": envelope.AggregateID, "previous_status": currentStatus, "new_status": targetStatus, "changed_at": changedAt,
	})
	if err != nil {
		return err
	}
	if err := outbox.Insert(ctx, tx, "orders", "freshflow.order.events.v1", envelope.AggregateID, statusChanged); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validTransition(current, target string) bool {
	allowed := map[string]map[string]bool{
		"created":    {"confirmed": true},
		"confirmed":  {"assembling": true, "delivered": true}, // operator completion is an explicit fast-forward.
		"assembling": {"delivering": true, "delivered": true},
		"delivering": {"delivered": true},
	}
	return allowed[current][target]
}
