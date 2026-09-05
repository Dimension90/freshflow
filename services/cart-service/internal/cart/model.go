package cart

import "time"

type Item struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type Cart struct {
	UserID    string    `json:"user_id"`
	Version   int64     `json:"version"`
	Items     []Item    `json:"items"`
	UpdatedAt time.Time `json:"updated_at"`
}

type SetItemRequest struct {
	Quantity int `json:"quantity"`
}
