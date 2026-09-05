package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

var validSchema = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

type executor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func Insert(ctx context.Context, tx executor, schema, topic, key string, envelope events.Envelope) error {
	if !validSchema.MatchString(schema) {
		return fmt.Errorf("invalid outbox schema %q", schema)
	}
	encoded, err := envelope.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event envelope: %w", err)
	}
	query := fmt.Sprintf(`
		INSERT INTO %s.outbox
		(event_id, topic, event_key, event_type, aggregate_id, correlation_id, trace_id, envelope, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, schema)
	if _, err := tx.Exec(ctx, query, envelope.EventID, topic, key, envelope.EventType, envelope.AggregateID,
		envelope.CorrelationID, envelope.TraceID, encoded, envelope.OccurredAt); err != nil {
		return fmt.Errorf("insert %s outbox event: %w", schema, err)
	}
	return nil
}

type Relay struct {
	postgres     *pgxpool.Pool
	kafka        *kgo.Client
	schema       string
	logger       *slog.Logger
	batchSize    int
	pollInterval time.Duration
}

func NewRelay(postgres *pgxpool.Pool, kafka *kgo.Client, schema string, logger *slog.Logger) (*Relay, error) {
	if !validSchema.MatchString(schema) {
		return nil, fmt.Errorf("invalid outbox schema %q", schema)
	}
	return &Relay{postgres: postgres, kafka: kafka, schema: schema, logger: logger, batchSize: 50, pollInterval: 500 * time.Millisecond}, nil
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		if err := r.publishBatch(ctx); err != nil && ctx.Err() == nil {
			r.logger.Warn("outbox publish batch failed", "schema", r.schema, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type record struct {
	EventID       string
	Topic         string
	Key           string
	EventType     string
	CorrelationID string
	TraceID       string
	Envelope      []byte
}

func (r *Relay) publishBatch(ctx context.Context) error {
	tx, err := r.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	query := fmt.Sprintf(`
		SELECT event_id::text, topic, event_key, event_type, correlation_id, trace_id, envelope
		FROM %s.outbox WHERE published_at IS NULL
		ORDER BY occurred_at FOR UPDATE SKIP LOCKED LIMIT $1`, r.schema)
	rows, err := tx.Query(ctx, query, r.batchSize)
	if err != nil {
		return err
	}
	records := make([]record, 0, r.batchSize)
	for rows.Next() {
		var item record
		if err := rows.Scan(&item.EventID, &item.Topic, &item.Key, &item.EventType, &item.CorrelationID, &item.TraceID, &item.Envelope); err != nil {
			rows.Close()
			return err
		}
		records = append(records, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range records {
		headers := []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(item.EventID)},
			{Key: "event_type", Value: []byte(item.EventType)},
			{Key: "correlation_id", Value: []byte(item.CorrelationID)},
		}
		if item.TraceID != "" {
			headers = append(headers, kgo.RecordHeader{Key: "trace_id", Value: []byte(item.TraceID)})
		}
		var envelope events.Envelope
		if json.Unmarshal(item.Envelope, &envelope) == nil && len(envelope.TraceID) == 32 && len(envelope.SpanID) == 16 {
			headers = append(headers,
				kgo.RecordHeader{Key: "span_id", Value: []byte(envelope.SpanID)},
				kgo.RecordHeader{Key: "traceparent", Value: []byte("00-" + envelope.TraceID + "-" + envelope.SpanID + "-01")},
			)
		}
		result := r.kafka.ProduceSync(ctx, &kgo.Record{Topic: item.Topic, Key: []byte(item.Key), Value: item.Envelope, Headers: headers})
		if err := result.FirstErr(); err != nil {
			update := fmt.Sprintf(`UPDATE %s.outbox SET attempts = attempts + 1, last_error = $2 WHERE event_id = $1`, r.schema)
			_, _ = tx.Exec(ctx, update, item.EventID, truncate(err.Error(), 1000))
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return fmt.Errorf("publish error %v; commit attempt error: %w", err, commitErr)
			}
			return err
		}
		update := fmt.Sprintf(`UPDATE %s.outbox SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE event_id = $1`, r.schema)
		if _, err := tx.Exec(ctx, update, item.EventID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func Decode(data []byte) (events.Envelope, error) {
	var envelope events.Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return events.Envelope{}, err
	}
	return envelope, nil
}
