package order

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
)

type CartReader interface {
	Get(context.Context, string) (Cart, error)
}

type Inventory interface {
	Reserve(context.Context, ReserveRequest) (Reservation, error)
	Release(context.Context, string) error
}

type UpstreamError struct {
	Status int
	Code   string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("upstream status %d (%s)", e.Status, e.Code)
}

type CartClient struct {
	client  *http.Client
	baseURL string
}

func NewCartClient(client *http.Client, baseURL string) *CartClient {
	return &CartClient{client: client, baseURL: strings.TrimRight(baseURL, "/")}
}

func (c *CartClient) Get(ctx context.Context, userID string) (Cart, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/internal/v1/carts/"+userID, nil)
	if err != nil {
		return Cart{}, err
	}
	request.Header.Set(httpx.CorrelationHeader, httpx.CorrelationID(ctx))
	response, err := c.client.Do(request)
	if err != nil {
		return Cart{}, fmt.Errorf("cart request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Cart{}, &UpstreamError{Status: response.StatusCode, Code: "cart_unavailable"}
	}
	var value Cart
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		return Cart{}, fmt.Errorf("decode cart response: %w", err)
	}
	return value, nil
}

type CatalogClient struct {
	client  *http.Client
	baseURL string
}

func NewCatalogClient(client *http.Client, baseURL string) *CatalogClient {
	return &CatalogClient{client: client, baseURL: strings.TrimRight(baseURL, "/")}
}

func (c *CatalogClient) Reserve(ctx context.Context, value ReserveRequest) (Reservation, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return Reservation{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/v1/reservations", bytes.NewReader(body))
	if err != nil {
		return Reservation{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(httpx.CorrelationHeader, httpx.CorrelationID(ctx))
	response, err := c.client.Do(request)
	if err != nil {
		return Reservation{}, fmt.Errorf("catalog reserve request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict {
		return Reservation{}, &UpstreamError{Status: response.StatusCode, Code: "inventory_insufficient"}
	}
	if response.StatusCode != http.StatusCreated {
		return Reservation{}, &UpstreamError{Status: response.StatusCode, Code: "catalog_unavailable"}
	}
	var reservation Reservation
	if err := json.NewDecoder(response.Body).Decode(&reservation); err != nil {
		return Reservation{}, fmt.Errorf("decode reservation response: %w", err)
	}
	return reservation, nil
}

func (c *CatalogClient) Release(ctx context.Context, reservationID string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/v1/reservations/"+reservationID+"/release", nil)
	if err != nil {
		return err
	}
	request.Header.Set(httpx.CorrelationHeader, httpx.CorrelationID(ctx))
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("catalog release request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return &UpstreamError{Status: response.StatusCode, Code: "reservation_release_failed"}
	}
	return nil
}
