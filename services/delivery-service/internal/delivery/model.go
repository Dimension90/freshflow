package delivery

import "time"

type Delivery struct {
	ID                   string     `json:"id"`
	OrderID              string     `json:"order_id"`
	CourierID            string     `json:"courier_id"`
	CourierName          string     `json:"courier_name"`
	Status               string     `json:"status"`
	PickupLatitude       float64    `json:"pickup_latitude"`
	PickupLongitude      float64    `json:"pickup_longitude"`
	DestinationLatitude  float64    `json:"destination_latitude"`
	DestinationLongitude float64    `json:"destination_longitude"`
	AssignedAt           time.Time  `json:"assigned_at"`
	StartedAt            *time.Time `json:"started_at,omitempty"`
	CompletedAt          *time.Time `json:"completed_at,omitempty"`
	CourierLatitude      float64    `json:"courier_latitude"`
	CourierLongitude     float64    `json:"courier_longitude"`
	PredictedETASeconds  *int       `json:"predicted_eta_seconds,omitempty"`
	ETAModelVersion      *string    `json:"eta_model_version,omitempty"`
	ETAUpdatedAt         *time.Time `json:"eta_updated_at,omitempty"`
}

type Assignment struct {
	Delivery
	CorrelationID string `json:"correlation_id"`
}

type orderCreatedPayload struct {
	OrderID           string  `json:"order_id"`
	DeliveryLatitude  float64 `json:"delivery_latitude"`
	DeliveryLongitude float64 `json:"delivery_longitude"`
	ItemCount         int     `json:"item_count"`
}

type courierLocationPayload struct {
	CourierID      string    `json:"courier_id"`
	DeliveryID     string    `json:"delivery_id,omitempty"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
	HeadingDegrees float64   `json:"heading_degrees"`
	SpeedMPS       float64   `json:"speed_mps"`
	RecordedAt     time.Time `json:"recorded_at"`
	Sequence       int64     `json:"sequence"`
	Phase          string    `json:"phase"`
}
