package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
	"github.com/freshflow/freshflow/pkg/platform/outbox"
	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

const courierGeoKey = "local:couriers:geo"

type Consumer struct {
	postgres        *pgxpool.Pool
	redis           *redis.Client
	kafka           *kgo.Client
	logger          *slog.Logger
	pickupLatitude  float64
	pickupLongitude float64
}

func NewConsumer(postgres *pgxpool.Pool, redisClient *redis.Client, kafka *kgo.Client, logger *slog.Logger, pickupLatitude, pickupLongitude float64) *Consumer {
	return &Consumer{postgres: postgres, redis: redisClient, kafka: kafka, logger: logger, pickupLatitude: pickupLatitude, pickupLongitude: pickupLongitude}
}

func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.kafka.PollRecords(ctx, 100)
		if err := fetches.Err(); err != nil {
			if ctx.Err() == nil {
				c.logger.Warn("poll logistics events", "error", err)
			}
			continue
		}
		for _, record := range fetches.Records() {
			for {
				if err := c.handle(ctx, record.Value); err == nil {
					break
				} else {
					c.logger.Warn("process logistics event; retrying", "error", err, "topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			if err := c.kafka.CommitRecords(ctx, record); err != nil && ctx.Err() == nil {
				c.logger.Warn("commit logistics offset", "error", err)
			}
		}
		telemetry.ObserveKafkaFetches("delivery-service", fetches)
	}
}

func (c *Consumer) handle(ctx context.Context, value []byte) error {
	var envelope events.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return fmt.Errorf("decode event envelope: %w", err)
	}
	ctx = httpx.WithCorrelationID(ctx, envelope.CorrelationID)
	ctx, span := telemetry.StartConsumerSpan(ctx, "delivery-service", envelope.EventType, envelope.TraceID, envelope.SpanID)
	defer span.End()
	switch envelope.EventType {
	case "order.created":
		var payload orderCreatedPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return err
		}
		return c.assign(ctx, envelope, payload)
	case "order.cancelled":
		return c.cancel(ctx, envelope)
	case "courier.location_updated":
		var payload courierLocationPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return err
		}
		return c.updateLocation(ctx, envelope, payload)
	default:
		return nil
	}
}

func (c *Consumer) cancel(ctx context.Context, envelope events.Envelope) error {
	tx, err := c.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	inserted, err := markProcessed(ctx, tx, envelope.EventID)
	if err != nil || !inserted {
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	var deliveryID, courierID, status string
	err = tx.QueryRow(ctx, `SELECT id::text, courier_id::text, status FROM delivery.deliveries WHERE order_id = $1 FOR UPDATE`, envelope.AggregateID).Scan(&deliveryID, &courierID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx) // cancellation can legitimately precede assignment.
	}
	if err != nil {
		return err
	}
	if status == "completed" || status == "cancelled" {
		return tx.Commit(ctx)
	}
	cancelledAt := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE delivery.deliveries SET status = 'cancelled', updated_at = $2 WHERE id = $1`, deliveryID, cancelledAt); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE delivery.couriers SET status = 'available', updated_at = $2 WHERE id = $1`, courierID, cancelledAt); err != nil {
		return err
	}
	event, err := events.New(ctx, "delivery.cancelled", "delivery-service", envelope.AggregateID, map[string]any{
		"delivery_id": deliveryID, "order_id": envelope.AggregateID, "courier_id": courierID, "cancelled_at": cancelledAt,
	})
	if err != nil {
		return err
	}
	if err := outbox.Insert(ctx, tx, "delivery", "freshflow.delivery.events.v1", envelope.AggregateID, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Consumer) assign(ctx context.Context, envelope events.Envelope, payload orderCreatedPayload) error {
	if payload.ItemCount < 1 {
		payload.ItemCount = 1
	}
	candidates, err := c.redis.GeoSearch(ctx, courierGeoKey, &redis.GeoSearchQuery{
		Longitude: c.pickupLongitude, Latitude: c.pickupLatitude, Radius: 50, RadiusUnit: "km", Sort: "ASC", Count: 10,
	}).Result()
	if err != nil {
		return fmt.Errorf("find nearest couriers: %w", err)
	}
	if len(candidates) == 0 {
		return fmt.Errorf("no courier locations available")
	}
	tx, err := c.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	inserted, err := markProcessed(ctx, tx, envelope.EventID)
	if err != nil || !inserted {
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	var courierID string
	for _, candidate := range candidates {
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM delivery.couriers WHERE id = $1 FOR UPDATE`, candidate).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if status == "available" {
			courierID = candidate
			break
		}
	}
	if courierID == "" {
		return fmt.Errorf("all nearby couriers are busy")
	}
	deliveryID, err := id.NewUUID()
	if err != nil {
		return err
	}
	assignedAt := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE delivery.couriers SET status = 'assigned', updated_at = now() WHERE id = $1`, courierID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery.deliveries
		(id, order_id, courier_id, status, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude,
		 correlation_id, assigned_at, item_count, trace_id, origin_span_id)
		VALUES ($1, $2, $3, 'assigned', $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		deliveryID, payload.OrderID, courierID, c.pickupLatitude, c.pickupLongitude, payload.DeliveryLatitude,
		payload.DeliveryLongitude, envelope.CorrelationID, assignedAt, payload.ItemCount, envelope.TraceID, envelope.SpanID); err != nil {
		return err
	}
	assigned, err := events.New(ctx, "delivery.assigned", "delivery-service", payload.OrderID, map[string]any{
		"delivery_id": deliveryID, "order_id": payload.OrderID, "courier_id": courierID, "assigned_at": assignedAt,
	})
	if err != nil {
		return err
	}
	if err := outbox.Insert(ctx, tx, "delivery", "freshflow.delivery.events.v1", payload.OrderID, assigned); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *Consumer) updateLocation(ctx context.Context, envelope events.Envelope, payload courierLocationPayload) error {
	tx, err := c.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	inserted, err := markProcessed(ctx, tx, envelope.EventID)
	if err != nil || !inserted {
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	result, err := tx.Exec(ctx, `
		UPDATE delivery.couriers SET latitude = $2, longitude = $3, location_sequence = $4,
		last_seen_at = $5, updated_at = now() WHERE id = $1 AND location_sequence < $4`,
		payload.CourierID, payload.Latitude, payload.Longitude, payload.Sequence, payload.RecordedAt)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if payload.DeliveryID != "" {
		if err := c.applyPhase(ctx, tx, envelope, payload); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	_ = c.redis.GeoAdd(ctx, courierGeoKey, &redis.GeoLocation{Name: payload.CourierID, Longitude: payload.Longitude, Latitude: payload.Latitude}).Err()
	return nil
}

func (c *Consumer) applyPhase(ctx context.Context, tx pgx.Tx, source events.Envelope, payload courierLocationPayload) error {
	var orderID, courierID, currentStatus, correlationID, traceID, spanID string
	var assignedAt time.Time
	err := tx.QueryRow(ctx, `SELECT order_id::text, courier_id::text, status, correlation_id, trace_id, origin_span_id, assigned_at FROM delivery.deliveries WHERE id = $1 FOR UPDATE`, payload.DeliveryID).
		Scan(&orderID, &courierID, &currentStatus, &correlationID, &traceID, &spanID, &assignedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if courierID != payload.CourierID {
		return fmt.Errorf("delivery courier mismatch")
	}
	ctx = httpx.WithCorrelationID(ctx, correlationID)
	ctx, phaseSpan := telemetry.StartConsumerSpan(ctx, "delivery-service", "delivery.phase."+payload.Phase, traceID, spanID)
	defer phaseSpan.End()
	targetStatus := payload.Phase
	if targetStatus == "available" || targetStatus == currentStatus {
		return nil
	}
	if !validDeliveryTransition(currentStatus, targetStatus) {
		return fmt.Errorf("invalid delivery transition %s -> %s", currentStatus, targetStatus)
	}
	now := payload.RecordedAt.UTC()
	switch targetStatus {
	case "assembling":
		_, err = tx.Exec(ctx, `UPDATE delivery.deliveries SET status = 'assembling', updated_at = $2 WHERE id = $1`, payload.DeliveryID, now)
	case "delivering":
		_, err = tx.Exec(ctx, `UPDATE delivery.deliveries SET status = 'delivering', started_at = COALESCE(started_at, $2), updated_at = $2 WHERE id = $1`, payload.DeliveryID, now)
	case "completed":
		_, err = tx.Exec(ctx, `UPDATE delivery.deliveries SET status = 'completed', completed_at = $2, updated_at = $2 WHERE id = $1`, payload.DeliveryID, now)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE delivery.couriers SET status = 'available', updated_at = $2 WHERE id = $1`, courierID, now)
		}
	}
	if err != nil {
		return err
	}
	eventType := "delivery.status_changed"
	eventPayload := map[string]any{"delivery_id": payload.DeliveryID, "order_id": orderID, "courier_id": courierID, "status": targetStatus, "changed_at": now}
	if targetStatus == "completed" {
		eventType = "delivery.completed"
		eventPayload["assigned_at"] = assignedAt
		eventPayload["completed_at"] = now
		actualETA := int(now.Sub(assignedAt).Seconds())
		eventPayload["actual_eta_seconds"] = actualETA
		if _, err := tx.Exec(ctx, `
			UPDATE delivery.eta_predictions
			SET actual_eta_seconds = GREATEST(0, EXTRACT(EPOCH FROM ($2 - predicted_at))::integer), completed_at = $2
			WHERE delivery_id = $1`, payload.DeliveryID, now); err != nil {
			return err
		}
		var predictedETA int
		var modelVersion string
		if predictionErr := tx.QueryRow(ctx, `
			SELECT predicted_eta_seconds, model_version FROM delivery.eta_predictions
			WHERE delivery_id = $1 AND stage = 'confirmed'`, payload.DeliveryID).Scan(&predictedETA, &modelVersion); predictionErr == nil {
			eventPayload["predicted_eta_seconds"] = predictedETA
			eventPayload["eta_model_version"] = modelVersion
			eventPayload["on_time"] = actualETA <= predictedETA
		} else if !errors.Is(predictionErr, pgx.ErrNoRows) {
			return predictionErr
		}
	}
	event, err := events.New(ctx, eventType, "delivery-service", orderID, eventPayload)
	if err != nil {
		return err
	}
	return outbox.Insert(ctx, tx, "delivery", "freshflow.delivery.events.v1", orderID, event)
}

func markProcessed(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	result, err := tx.Exec(ctx, `INSERT INTO delivery.processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`, eventID)
	return err == nil && result.RowsAffected() == 1, err
}

func validDeliveryTransition(current, target string) bool {
	return map[string]string{"assigned": "assembling", "assembling": "delivering", "delivering": "completed"}[current] == target
}
