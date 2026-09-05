package events

import (
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestNewEnvelope(t *testing.T) {
	envelope, err := New(context.Background(), "order.created", "order-service", "30000000-0000-4000-8000-000000000001", map[string]string{"status": "created"})
	if err != nil {
		t.Fatal(err)
	}
	if envelope.EventVersion != 1 || envelope.EventID == "" || envelope.EventType != "order.created" {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}
	var payload map[string]string
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil || payload["status"] != "created" {
		t.Fatalf("unexpected payload: %#v, error=%v", payload, err)
	}
}

func TestNewEnvelopeCarriesTraceContext(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)
	envelope, err := New(ctx, "order.created", "order-service", "30000000-0000-4000-8000-000000000001", map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if envelope.TraceID != traceID.String() || envelope.SpanID != spanID.String() {
		t.Fatalf("trace context not copied: %#v", envelope)
	}
}
