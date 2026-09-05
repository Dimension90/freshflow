package catalog

import "time"

type Product struct {
	ID                string `json:"id"`
	SKU               string `json:"sku"`
	Name              string `json:"name"`
	Description       string `json:"description"`
	PriceMinor        int64  `json:"price_minor"`
	Currency          string `json:"currency"`
	AvailableQuantity int    `json:"available_quantity"`
}

type ReservationItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type ReserveRequest struct {
	CheckoutAttemptID string            `json:"checkout_attempt_id"`
	Items             []ReservationItem `json:"items"`
	TTLSeconds        int               `json:"ttl_seconds"`
}

type ReservedItem struct {
	ProductID      string `json:"product_id"`
	ProductName    string `json:"product_name"`
	Quantity       int    `json:"quantity"`
	UnitPriceMinor int64  `json:"unit_price_minor"`
	Currency       string `json:"currency"`
}

type Shortage struct {
	ProductID string `json:"product_id"`
	Requested int    `json:"requested"`
	Available int    `json:"available"`
}

type ReservationResponse struct {
	ReservationID    string         `json:"reservation_id"`
	Status           string         `json:"status"`
	Items            []ReservedItem `json:"items,omitempty"`
	Shortages        []Shortage     `json:"shortages,omitempty"`
	TotalAmountMinor int64          `json:"total_amount_minor,omitempty"`
	Currency         string         `json:"currency,omitempty"`
	ExpiresAt        *time.Time     `json:"expires_at,omitempty"`
}
