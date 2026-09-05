package analytics

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type DeliveryFact struct {
	EventID, EventType, Status, CorrelationID, TraceID string
	OrderID, DeliveryID, CourierID                     *uuid.UUID
	Latitude, Longitude                                *float64
	PredictedETASeconds, ActualETASeconds              *uint32
	OccurredAt                                         time.Time
	Payload                                            json.RawMessage
}

type OrderFact struct {
	EventID, OrderID, EventType, Status, PreviousStatus string
	TotalAmountMinor                                    uint64
	Currency                                            string
	ProductIDs                                          []uuid.UUID
	ProductNames                                        []string
	ItemQuantities                                      []uint32
	PredictedETASeconds, ActualETASeconds               *uint32
	OccurredAt                                          time.Time
	CorrelationID, TraceID                              string
}

type Projection struct {
	Delivery *DeliveryFact
	Order    *OrderFact
}

type HourBucket struct {
	Hour   time.Time `json:"hour"`
	Orders uint64    `json:"orders"`
}

type ETAStats struct {
	AveragePredictedETASeconds *float64 `json:"average_predicted_eta_seconds"`
	AverageActualETASeconds    *float64 `json:"average_actual_eta_seconds"`
}

type ProductStat struct {
	ProductID   string `json:"product_id"`
	ProductName string `json:"product_name"`
	Quantity    uint64 `json:"quantity"`
}

type StatusDuration struct {
	Status                 string  `json:"status"`
	AverageDurationSeconds float64 `json:"average_duration_seconds"`
}

type Summary struct {
	OrdersByHour    []HourBucket     `json:"orders_by_hour"`
	ETA             ETAStats         `json:"eta"`
	OnTimeRatio     *float64         `json:"on_time_ratio"`
	Cancellations   uint64           `json:"cancellations"`
	PopularProducts []ProductStat    `json:"popular_products"`
	StatusDurations []StatusDuration `json:"status_durations"`
}
