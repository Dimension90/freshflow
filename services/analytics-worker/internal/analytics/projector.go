package analytics

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/google/uuid"
)

type eventPayload struct {
	OrderID             string   `json:"order_id"`
	DeliveryID          string   `json:"delivery_id"`
	CourierID           string   `json:"courier_id"`
	Status              string   `json:"status"`
	PreviousStatus      string   `json:"previous_status"`
	NewStatus           string   `json:"new_status"`
	Currency            string   `json:"currency"`
	TotalAmountMinor    uint64   `json:"total_amount_minor"`
	Latitude            *float64 `json:"latitude"`
	Longitude           *float64 `json:"longitude"`
	PredictedETASeconds *uint32  `json:"predicted_eta_seconds"`
	ActualETASeconds    *uint32  `json:"actual_eta_seconds"`
	Items               []struct {
		ProductID   string `json:"product_id"`
		ProductName string `json:"product_name"`
		Quantity    uint32 `json:"quantity"`
	} `json:"items"`
}

func Project(envelope events.Envelope) (Projection, error) {
	if _, err := uuid.Parse(envelope.EventID); err != nil {
		return Projection{}, fmt.Errorf("invalid event_id: %w", err)
	}
	if envelope.OccurredAt.IsZero() {
		envelope.OccurredAt = time.Now().UTC()
	}
	var payload eventPayload
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return Projection{}, fmt.Errorf("decode %s payload: %w", envelope.EventType, err)
	}

	orderID := parseOptionalUUID(payload.OrderID)
	if orderID == nil && isOrderEvent(envelope.EventType) {
		orderID = parseOptionalUUID(envelope.AggregateID)
	}
	deliveryID := parseOptionalUUID(payload.DeliveryID)
	courierID := parseOptionalUUID(payload.CourierID)
	status := payload.Status
	if payload.NewStatus != "" {
		status = payload.NewStatus
	}

	projection := Projection{}
	if isDeliveryEvent(envelope.EventType) {
		projection.Delivery = &DeliveryFact{
			EventID: envelope.EventID, EventType: envelope.EventType, OrderID: orderID,
			DeliveryID: deliveryID, CourierID: courierID, Status: status,
			Latitude: payload.Latitude, Longitude: payload.Longitude,
			PredictedETASeconds: payload.PredictedETASeconds, ActualETASeconds: payload.ActualETASeconds,
			OccurredAt: envelope.OccurredAt.UTC(), CorrelationID: envelope.CorrelationID,
			TraceID: envelope.TraceID, Payload: envelope.Payload,
		}
	}
	if orderID != nil && (isOrderEvent(envelope.EventType) || isDeliveryEvent(envelope.EventType)) {
		fact := &OrderFact{
			EventID: envelope.EventID, OrderID: orderID.String(), EventType: envelope.EventType,
			Status: status, PreviousStatus: payload.PreviousStatus,
			TotalAmountMinor: payload.TotalAmountMinor, Currency: payload.Currency,
			PredictedETASeconds: payload.PredictedETASeconds, ActualETASeconds: payload.ActualETASeconds,
			OccurredAt: envelope.OccurredAt.UTC(), CorrelationID: envelope.CorrelationID, TraceID: envelope.TraceID,
		}
		if envelope.EventType == "order.created" {
			fact.Status = "created"
			for _, item := range payload.Items {
				productID, err := uuid.Parse(item.ProductID)
				if err != nil {
					return Projection{}, fmt.Errorf("invalid product_id: %w", err)
				}
				fact.ProductIDs = append(fact.ProductIDs, productID)
				fact.ProductNames = append(fact.ProductNames, item.ProductName)
				fact.ItemQuantities = append(fact.ItemQuantities, item.Quantity)
			}
		}
		if envelope.EventType == "order.confirmed" {
			fact.Status = "confirmed"
		}
		projection.Order = fact
	}
	return projection, nil
}

func parseOptionalUUID(value string) *uuid.UUID {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return nil
	}
	return &parsed
}

func isOrderEvent(eventType string) bool {
	switch eventType {
	case "order.created", "order.confirmed", "order.status_changed":
		return true
	default:
		return false
	}
}

func isDeliveryEvent(eventType string) bool {
	switch eventType {
	case "delivery.assigned", "delivery.status_changed", "delivery.completed", "courier.location_updated":
		return true
	default:
		return false
	}
}
