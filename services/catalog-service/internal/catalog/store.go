package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/freshflow/freshflow/pkg/platform/id"
	"github.com/freshflow/freshflow/pkg/platform/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const productsCacheKey = "local:catalog:products:v1"

type Store struct {
	postgres *pgxpool.Pool
	redis    *redis.Client
	cacheTTL time.Duration
}

func NewStore(postgres *pgxpool.Pool, redisClient *redis.Client, cacheTTL time.Duration) *Store {
	return &Store{postgres: postgres, redis: redisClient, cacheTTL: cacheTTL}
}

func (s *Store) ListProducts(ctx context.Context) ([]Product, error) {
	if cached, err := s.redis.Get(ctx, productsCacheKey).Bytes(); err == nil {
		var products []Product
		if json.Unmarshal(cached, &products) == nil {
			return products, nil
		}
	}
	rows, err := s.postgres.Query(ctx, `
		SELECT id::text, sku, name, description, price_minor, currency, available_quantity
		FROM catalog.products WHERE active = true ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("query products: %w", err)
	}
	defer rows.Close()
	products := make([]Product, 0)
	for rows.Next() {
		var product Product
		if err := rows.Scan(&product.ID, &product.SKU, &product.Name, &product.Description, &product.PriceMinor, &product.Currency, &product.AvailableQuantity); err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		products = append(products, product)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate products: %w", err)
	}
	if encoded, err := json.Marshal(products); err == nil {
		_ = s.redis.Set(ctx, productsCacheKey, encoded, s.cacheTTL).Err()
	}
	return products, nil
}

func (s *Store) Reserve(ctx context.Context, request ReserveRequest) (ReservationResponse, error) {
	items := append([]ReservationItem(nil), request.Items...)
	sort.Slice(items, func(i, j int) bool { return items[i].ProductID < items[j].ProductID })
	tx, err := s.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ReservationResponse{}, fmt.Errorf("begin reserve transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, request.CheckoutAttemptID); err != nil {
		return ReservationResponse{}, fmt.Errorf("lock checkout attempt: %w", err)
	}
	var existingJSON []byte
	err = tx.QueryRow(ctx, `SELECT response FROM catalog.inventory_reservations WHERE checkout_attempt_id = $1`, request.CheckoutAttemptID).Scan(&existingJSON)
	if err == nil {
		var existing ReservationResponse
		if err := json.Unmarshal(existingJSON, &existing); err != nil {
			return ReservationResponse{}, fmt.Errorf("decode existing reservation: %w", err)
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ReservationResponse{}, fmt.Errorf("find existing reservation: %w", err)
	}

	reserved := make([]ReservedItem, 0, len(items))
	shortages := make([]Shortage, 0)
	var total int64
	currency := ""
	for _, item := range items {
		var product ReservedItem
		var available int
		err := tx.QueryRow(ctx, `
			SELECT id::text, name, price_minor, currency, available_quantity
			FROM catalog.products WHERE id = $1 AND active = true FOR UPDATE`, item.ProductID).
			Scan(&product.ProductID, &product.ProductName, &product.UnitPriceMinor, &product.Currency, &available)
		if errors.Is(err, pgx.ErrNoRows) {
			shortages = append(shortages, Shortage{ProductID: item.ProductID, Requested: item.Quantity, Available: 0})
			continue
		}
		if err != nil {
			return ReservationResponse{}, fmt.Errorf("lock product %s: %w", item.ProductID, err)
		}
		if available < item.Quantity {
			shortages = append(shortages, Shortage{ProductID: item.ProductID, Requested: item.Quantity, Available: available})
			continue
		}
		if currency != "" && currency != product.Currency {
			return ReservationResponse{}, fmt.Errorf("mixed currencies are not supported")
		}
		currency = product.Currency
		product.Quantity = item.Quantity
		reserved = append(reserved, product)
		total += product.UnitPriceMinor * int64(item.Quantity)
	}

	reservationID, err := id.NewUUID()
	if err != nil {
		return ReservationResponse{}, err
	}
	response := ReservationResponse{ReservationID: reservationID, Status: "failed", Shortages: shortages}
	if len(shortages) == 0 {
		expiresAt := time.Now().UTC().Add(time.Duration(request.TTLSeconds) * time.Second)
		response = ReservationResponse{ReservationID: reservationID, Status: "active", Items: reserved, TotalAmountMinor: total, Currency: currency, ExpiresAt: &expiresAt}
		for _, item := range reserved {
			if _, err := tx.Exec(ctx, `UPDATE catalog.products SET available_quantity = available_quantity - $2, updated_at = now() WHERE id = $1`, item.ProductID, item.Quantity); err != nil {
				return ReservationResponse{}, fmt.Errorf("decrement inventory: %w", err)
			}
		}
	}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		return ReservationResponse{}, fmt.Errorf("encode reservation: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO catalog.inventory_reservations (id, checkout_attempt_id, status, response, expires_at)
		VALUES ($1, $2, $3, $4, $5)`, reservationID, request.CheckoutAttemptID, response.Status, responseJSON, response.ExpiresAt)
	if err != nil {
		return ReservationResponse{}, fmt.Errorf("insert reservation: %w", err)
	}
	if response.Status == "active" {
		for _, item := range reserved {
			if _, err := tx.Exec(ctx, `INSERT INTO catalog.inventory_reservation_items (reservation_id, product_id, quantity) VALUES ($1, $2, $3)`, reservationID, item.ProductID, item.Quantity); err != nil {
				return ReservationResponse{}, fmt.Errorf("insert reservation item: %w", err)
			}
		}
	}
	eventType := "inventory.reservation_failed"
	payload := any(map[string]any{
		"checkout_attempt_id": request.CheckoutAttemptID,
		"reservation_id":      response.ReservationID,
		"shortages":           response.Shortages,
		"failed_at":           time.Now().UTC(),
	})
	if response.Status == "active" {
		eventType = "inventory.reserved"
		payload = map[string]any{
			"reservation_id":      response.ReservationID,
			"checkout_attempt_id": request.CheckoutAttemptID,
			"items":               request.Items,
			"expires_at":          response.ExpiresAt,
		}
	}
	envelope, err := events.New(ctx, eventType, "catalog-service", response.ReservationID, payload)
	if err != nil {
		return ReservationResponse{}, err
	}
	if err := outbox.Insert(ctx, tx, "catalog", "freshflow.inventory.events.v1", response.ReservationID, envelope); err != nil {
		return ReservationResponse{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ReservationResponse{}, fmt.Errorf("commit reservation: %w", err)
	}
	_ = s.redis.Del(ctx, productsCacheKey).Err()
	if response.ExpiresAt != nil {
		_ = s.redis.Set(ctx, "local:reservation:expiry:"+reservationID, request.CheckoutAttemptID, time.Until(*response.ExpiresAt)).Err()
	}
	return response, nil
}

func (s *Store) Release(ctx context.Context, reservationID string) error {
	tx, err := s.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin release transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM catalog.inventory_reservations WHERE id = $1 FOR UPDATE`, reservationID).Scan(&status); err != nil {
		return err
	}
	if status != "active" {
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE catalog.products p SET available_quantity = p.available_quantity + i.quantity, updated_at = now()
		FROM catalog.inventory_reservation_items i
		WHERE i.reservation_id = $1 AND i.product_id = p.id`, reservationID); err != nil {
		return fmt.Errorf("restore inventory: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE catalog.inventory_reservations SET status = 'released', updated_at = now() WHERE id = $1`, reservationID); err != nil {
		return fmt.Errorf("mark reservation released: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit release: %w", err)
	}
	_ = s.redis.Del(ctx, productsCacheKey, "local:reservation:expiry:"+reservationID).Err()
	return nil
}
