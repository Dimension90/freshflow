package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
	"go.opentelemetry.io/otel/trace"
)

type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  int             `json:"event_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Producer      string          `json:"producer"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	TraceID       string          `json:"trace_id,omitempty"`
	SpanID        string          `json:"span_id,omitempty"`
	AggregateID   string          `json:"aggregate_id"`
	Payload       json.RawMessage `json:"payload"`
}

func New(ctx context.Context, eventType, producer, aggregateID string, payload any) (Envelope, error) {
	eventID, err := id.NewUUID()
	if err != nil {
		return Envelope{}, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal event payload: %w", err)
	}
	spanContext := trace.SpanContextFromContext(ctx)
	envelope := Envelope{
		EventID: eventID, EventType: eventType, EventVersion: 1, OccurredAt: time.Now().UTC(),
		Producer: producer, CorrelationID: httpx.CorrelationID(ctx), AggregateID: aggregateID, Payload: encoded,
	}
	if spanContext.IsValid() {
		envelope.TraceID = spanContext.TraceID().String()
		envelope.SpanID = spanContext.SpanID().String()
	}
	return envelope, nil
}

func (e Envelope) Marshal() ([]byte, error) {
	return json.Marshal(e)
}
