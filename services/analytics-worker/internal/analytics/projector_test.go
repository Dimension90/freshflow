package analytics

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
)

func TestProjectOrderCreated(t *testing.T) {
	envelope := events.Envelope{
		EventID: "11111111-1111-4111-8111-111111111111", EventType: "order.created",
		AggregateID: "22222222-2222-4222-8222-222222222222", OccurredAt: time.Now(),
		Payload: json.RawMessage(`{
			"order_id":"22222222-2222-4222-8222-222222222222",
			"total_amount_minor":1299,"currency":"RUB",
			"items":[{"product_id":"33333333-3333-4333-8333-333333333333","product_name":"Яблоки","quantity":2}]
		}`),
	}

	projection, err := Project(envelope)
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if projection.Delivery != nil {
		t.Fatal("order event unexpectedly produced delivery fact")
	}
	if projection.Order == nil || projection.Order.Status != "created" {
		t.Fatalf("unexpected order projection: %#v", projection.Order)
	}
	if len(projection.Order.ProductIDs) != 1 || projection.Order.ItemQuantities[0] != 2 {
		t.Fatalf("items not projected: %#v", projection.Order)
	}
}

func TestProjectDeliveryCompleted(t *testing.T) {
	envelope := events.Envelope{
		EventID: "11111111-1111-4111-8111-111111111111", EventType: "delivery.completed",
		AggregateID: "44444444-4444-4444-8444-444444444444", OccurredAt: time.Now(),
		Payload: json.RawMessage(`{
			"delivery_id":"44444444-4444-4444-8444-444444444444",
			"order_id":"22222222-2222-4222-8222-222222222222",
			"courier_id":"55555555-5555-4555-8555-555555555555",
			"status":"completed","actual_eta_seconds":901
		}`),
	}

	projection, err := Project(envelope)
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if projection.Delivery == nil || projection.Order == nil {
		t.Fatalf("missing projections: %#v", projection)
	}
	if projection.Order.ActualETASeconds == nil || *projection.Order.ActualETASeconds != 901 {
		t.Fatalf("actual ETA not projected: %#v", projection.Order)
	}
}

func TestProjectIgnoresInventoryEvent(t *testing.T) {
	projection, err := Project(events.Envelope{
		EventID: "11111111-1111-4111-8111-111111111111", EventType: "inventory.reserved",
		OccurredAt: time.Now(), Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if projection.Order != nil || projection.Delivery != nil {
		t.Fatalf("unexpected projection: %#v", projection)
	}
}
