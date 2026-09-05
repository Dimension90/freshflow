package order

import "time"

type CheckoutRequest struct {
	UserID      string `json:"user_id"`
	CartVersion int64  `json:"cart_version"`
}

type CartItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type Cart struct {
	UserID  string     `json:"user_id"`
	Version int64      `json:"version"`
	Items   []CartItem `json:"items"`
}

type ReserveRequest struct {
	CheckoutAttemptID string     `json:"checkout_attempt_id"`
	Items             []CartItem `json:"items"`
	TTLSeconds        int        `json:"ttl_seconds"`
}

type ReservedItem struct {
	ProductID      string `json:"product_id"`
	ProductName    string `json:"product_name"`
	Quantity       int    `json:"quantity"`
	UnitPriceMinor int64  `json:"unit_price_minor"`
	Currency       string `json:"currency"`
}

type Reservation struct {
	ReservationID    string         `json:"reservation_id"`
	Status           string         `json:"status"`
	Items            []ReservedItem `json:"items"`
	TotalAmountMinor int64          `json:"total_amount_minor"`
	Currency         string         `json:"currency"`
}

type Order struct {
	ID                string         `json:"id"`
	UserID            string         `json:"user_id"`
	ReservationID     string         `json:"reservation_id"`
	CartVersion       int64          `json:"cart_version"`
	Status            string         `json:"status"`
	Items             []ReservedItem `json:"items"`
	TotalAmountMinor  int64          `json:"total_amount_minor"`
	Currency          string         `json:"currency"`
	DeliveryLatitude  float64        `json:"delivery_latitude"`
	DeliveryLongitude float64        `json:"delivery_longitude"`
	CreatedAt         time.Time      `json:"created_at"`
}
