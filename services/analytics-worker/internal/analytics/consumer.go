package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
)

type projectionStore interface {
	Insert(context.Context, Projection) error
}

type Consumer struct {
	store  projectionStore
	kafka  *kgo.Client
	logger *slog.Logger
}

func NewConsumer(store projectionStore, kafka *kgo.Client, logger *slog.Logger) *Consumer {
	return &Consumer{store: store, kafka: kafka, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.kafka.PollRecords(ctx, 100)
		if err := fetches.Err(); err != nil {
			if ctx.Err() == nil {
				c.logger.Warn("poll Kafka", "error", err)
			}
			continue
		}
		for _, record := range fetches.Records() {
			for {
				err := c.handle(ctx, record.Value)
				if err == nil {
					break
				}
				c.logger.Warn("project Kafka event; partition paused by retry", "topic", record.Topic,
					"partition", record.Partition, "offset", record.Offset, "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			if err := c.kafka.CommitRecords(ctx, record); err != nil && ctx.Err() == nil {
				c.logger.Warn("commit Kafka offset", "error", err, "topic", record.Topic,
					"partition", record.Partition, "offset", record.Offset)
			}
		}
		telemetry.ObserveKafkaFetches("analytics-worker", fetches)
	}
}

func (c *Consumer) handle(ctx context.Context, value []byte) error {
	var envelope events.Envelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return fmt.Errorf("decode event envelope: %w", err)
	}
	ctx = httpx.WithCorrelationID(ctx, envelope.CorrelationID)
	ctx, span := telemetry.StartConsumerSpan(ctx, "analytics-worker", envelope.EventType, envelope.TraceID, envelope.SpanID)
	defer span.End()
	projection, err := Project(envelope)
	if err != nil {
		return err
	}
	if projection.Delivery == nil && projection.Order == nil {
		return nil
	}
	if err := c.store.Insert(ctx, projection); err != nil {
		return err
	}
	c.logger.Info("analytics event projected", "event_id", envelope.EventID,
		"event_type", envelope.EventType, "correlation_id", envelope.CorrelationID, "trace_id", envelope.TraceID)
	return nil
}
