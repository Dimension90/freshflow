package order

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
	"github.com/freshflow/freshflow/pkg/platform/outbox"
	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	postgres       *pgxpool.Pool
	carts          CartReader
	inventory      Inventory
	reservationTTL time.Duration
}

func NewService(postgres *pgxpool.Pool, carts CartReader, inventory Inventory, reservationTTL time.Duration) *Service {
	return &Service{postgres: postgres, carts: carts, inventory: inventory, reservationTTL: reservationTTL}
}

func (s *Service) Create(ctx context.Context, key string, request CheckoutRequest) (Order, bool, *httpx.APIError) {
	hash := requestHash(request)
	connection, err := s.postgres.Acquire(ctx)
	if err != nil {
		return Order{}, false, internalError()
	}
	defer connection.Release()
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended($1, 0))`, key); err != nil {
		return Order{}, false, internalError()
	}
	defer connection.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key)

	attemptID, existing, apiErr := s.prepareIdempotency(ctx, connection, key, hash)
	if apiErr != nil {
		return Order{}, false, apiErr
	}
	if existing != nil {
		return *existing, true, nil
	}

	var deliveryLatitude, deliveryLongitude float64
	if err := connection.QueryRow(ctx, `SELECT delivery_latitude, delivery_longitude FROM orders.users WHERE id = $1`, request.UserID).Scan(&deliveryLatitude, &deliveryLongitude); errors.Is(err, pgx.ErrNoRows) {
		return Order{}, false, httpx.Error(http.StatusNotFound, "user_not_found", "user was not found", nil)
	} else if err != nil {
		return Order{}, false, internalError()
	}

	cart, err := s.carts.Get(ctx, request.UserID)
	if err != nil {
		return Order{}, false, httpx.Error(http.StatusServiceUnavailable, "cart_unavailable", "cart service is unavailable", nil)
	}
	if cart.Version != request.CartVersion {
		return Order{}, false, httpx.Error(http.StatusConflict, "cart_version_mismatch", "cart changed before checkout", map[string]any{"current_version": cart.Version})
	}
	if len(cart.Items) == 0 {
		return Order{}, false, httpx.Error(http.StatusConflict, "cart_empty", "cart is empty", nil)
	}

	reservation, err := s.inventory.Reserve(ctx, ReserveRequest{CheckoutAttemptID: attemptID, Items: cart.Items, TTLSeconds: int(s.reservationTTL.Seconds())})
	if err != nil {
		var upstream *UpstreamError
		if errors.As(err, &upstream) && upstream.Code == "inventory_insufficient" {
			return Order{}, false, httpx.Error(http.StatusConflict, "inventory_insufficient", "not enough stock for one or more items", nil)
		}
		return Order{}, false, httpx.Error(http.StatusServiceUnavailable, "catalog_unavailable", "catalog service is unavailable", nil)
	}

	created, err := s.persistOrder(ctx, connection, key, request, reservation, deliveryLatitude, deliveryLongitude)
	if err != nil {
		_ = s.inventory.Release(context.WithoutCancel(ctx), reservation.ReservationID)
		return Order{}, false, internalError()
	}
	return created, false, nil
}

func (s *Service) Get(ctx context.Context, orderID string) (Order, error) {
	var value Order
	err := s.postgres.QueryRow(ctx, `
		SELECT id::text, user_id::text, reservation_id::text, cart_version, status, total_amount_minor, currency,
		       delivery_latitude, delivery_longitude, created_at
		FROM orders.orders WHERE id = $1`, orderID).
		Scan(&value.ID, &value.UserID, &value.ReservationID, &value.CartVersion, &value.Status, &value.TotalAmountMinor, &value.Currency,
			&value.DeliveryLatitude, &value.DeliveryLongitude, &value.CreatedAt)
	if err != nil {
		return Order{}, err
	}
	rows, err := s.postgres.Query(ctx, `
		SELECT product_id::text, product_name, quantity, unit_price_minor
		FROM orders.order_items WHERE order_id = $1 ORDER BY product_id`, orderID)
	if err != nil {
		return Order{}, err
	}
	defer rows.Close()
	value.Items = make([]ReservedItem, 0)
	for rows.Next() {
		var item ReservedItem
		if err := rows.Scan(&item.ProductID, &item.ProductName, &item.Quantity, &item.UnitPriceMinor); err != nil {
			return Order{}, err
		}
		item.Currency = value.Currency
		value.Items = append(value.Items, item)
	}
	return value, rows.Err()
}

// Cancel is a small saga: while the order row is locked, it releases the active
// inventory reservation, then atomically records the terminal order state and
// puts an order.cancelled command into the outbox for delivery-service.
func (s *Service) Cancel(ctx context.Context, orderID string) (Order, *httpx.APIError) {
	tx, err := s.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Order{}, internalError()
	}
	defer tx.Rollback(ctx)

	var currentStatus, reservationID string
	err = tx.QueryRow(ctx, `SELECT status, reservation_id::text FROM orders.orders WHERE id = $1 FOR UPDATE`, orderID).Scan(&currentStatus, &reservationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, httpx.Error(http.StatusNotFound, "order_not_found", "order was not found", nil)
	}
	if err != nil {
		return Order{}, internalError()
	}
	if currentStatus == "cancelled" {
		if err := tx.Commit(ctx); err != nil {
			return Order{}, internalError()
		}
		value, err := s.Get(ctx, orderID)
		if err != nil {
			return Order{}, internalError()
		}
		return value, nil
	}
	if !canCancel(currentStatus) {
		return Order{}, httpx.Error(http.StatusConflict, "order_not_cancellable", "only a created or confirmed order can be cancelled", map[string]any{"status": currentStatus})
	}

	// Holding the row lock prevents a delivery event from advancing the order
	// while its corresponding reservation is being compensated.
	if err := s.inventory.Release(ctx, reservationID); err != nil {
		return Order{}, httpx.Error(http.StatusServiceUnavailable, "inventory_release_failed", "could not release the product reservation", nil)
	}
	changedAt := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE orders.orders SET status = 'cancelled', updated_at = $2 WHERE id = $1`, orderID, changedAt); err != nil {
		return Order{}, internalError()
	}
	if _, err := tx.Exec(ctx, `INSERT INTO orders.order_status_history (order_id, previous_status, new_status, changed_at) VALUES ($1, $2, 'cancelled', $3)`, orderID, currentStatus, changedAt); err != nil {
		return Order{}, internalError()
	}
	cancelled, err := events.New(ctx, "order.cancelled", "order-service", orderID, map[string]any{
		"order_id": orderID, "previous_status": currentStatus, "cancelled_at": changedAt,
	})
	if err != nil {
		return Order{}, internalError()
	}
	if err := outbox.Insert(ctx, tx, "orders", "freshflow.order.events.v1", orderID, cancelled); err != nil {
		return Order{}, internalError()
	}
	statusChanged, err := events.New(ctx, "order.status_changed", "order-service", orderID, map[string]any{
		"order_id": orderID, "previous_status": currentStatus, "new_status": "cancelled", "changed_at": changedAt,
	})
	if err != nil {
		return Order{}, internalError()
	}
	if err := outbox.Insert(ctx, tx, "orders", "freshflow.order.events.v1", orderID, statusChanged); err != nil {
		return Order{}, internalError()
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, internalError()
	}
	value, err := s.Get(ctx, orderID)
	if err != nil {
		return Order{}, internalError()
	}
	return value, nil
}

func (s *Service) prepareIdempotency(ctx context.Context, connection *pgxpool.Conn, key, hash string) (string, *Order, *httpx.APIError) {
	var storedHash, attemptID, state string
	var responseJSON []byte
	err := connection.QueryRow(ctx, `SELECT request_hash, checkout_attempt_id::text, state, response FROM orders.idempotency_keys WHERE key = $1`, key).
		Scan(&storedHash, &attemptID, &state, &responseJSON)
	if err == nil {
		if storedHash != hash {
			return "", nil, httpx.Error(http.StatusConflict, "idempotency_key_reused", "Idempotency-Key was already used with a different request", nil)
		}
		if state == "completed" {
			var existing Order
			if json.Unmarshal(responseJSON, &existing) != nil {
				return "", nil, internalError()
			}
			return attemptID, &existing, nil
		}
		return attemptID, nil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", nil, internalError()
	}
	attemptID, err = id.NewUUID()
	if err != nil {
		return "", nil, internalError()
	}
	if _, err := connection.Exec(ctx, `INSERT INTO orders.idempotency_keys (key, request_hash, checkout_attempt_id, state) VALUES ($1, $2, $3, 'processing')`, key, hash, attemptID); err != nil {
		return "", nil, internalError()
	}
	return attemptID, nil, nil
}

func (s *Service) persistOrder(ctx context.Context, connection *pgxpool.Conn, key string, request CheckoutRequest, reservation Reservation, deliveryLatitude, deliveryLongitude float64) (Order, error) {
	orderID, err := id.NewUUID()
	if err != nil {
		return Order{}, err
	}
	tx, err := connection.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Order{}, err
	}
	defer tx.Rollback(ctx)
	created := Order{
		ID: orderID, UserID: request.UserID, ReservationID: reservation.ReservationID,
		CartVersion: request.CartVersion, Status: "created", Items: reservation.Items,
		TotalAmountMinor: reservation.TotalAmountMinor, Currency: reservation.Currency,
		DeliveryLatitude: deliveryLatitude, DeliveryLongitude: deliveryLongitude, CreatedAt: time.Now().UTC(),
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO orders.orders (id, user_id, reservation_id, cart_version, status, total_amount_minor, currency,
		                           delivery_latitude, delivery_longitude, created_at, updated_at)
		VALUES ($1, $2, $3, $4, 'created', $5, $6, $7, $8, $9, $9)`,
		created.ID, created.UserID, created.ReservationID, created.CartVersion, created.TotalAmountMinor, created.Currency,
		created.DeliveryLatitude, created.DeliveryLongitude, created.CreatedAt); err != nil {
		return Order{}, err
	}
	for _, item := range created.Items {
		if _, err := tx.Exec(ctx, `
			INSERT INTO orders.order_items (order_id, product_id, product_name, quantity, unit_price_minor)
			VALUES ($1, $2, $3, $4, $5)`, created.ID, item.ProductID, item.ProductName, item.Quantity, item.UnitPriceMinor); err != nil {
			return Order{}, err
		}
	}
	eventPayload := map[string]any{
		"order_id":           created.ID,
		"user_id":            created.UserID,
		"reservation_id":     created.ReservationID,
		"currency":           created.Currency,
		"total_amount_minor": created.TotalAmountMinor,
		"item_count":         len(created.Items),
		"items":              created.Items,
		"created_at":         created.CreatedAt,
		"delivery_latitude":  created.DeliveryLatitude,
		"delivery_longitude": created.DeliveryLongitude,
	}
	envelope, err := events.New(ctx, "order.created", "order-service", created.ID, eventPayload)
	if err != nil {
		return Order{}, err
	}
	if err := outbox.Insert(ctx, tx, "orders", "freshflow.order.events.v1", created.ID, envelope); err != nil {
		return Order{}, err
	}
	responseJSON, err := json.Marshal(created)
	if err != nil {
		return Order{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE orders.idempotency_keys SET state = 'completed', status_code = 201, response = $2, updated_at = now() WHERE key = $1`, key, responseJSON); err != nil {
		return Order{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, err
	}
	telemetry.IncOrdersCreated()
	return created, nil
}

func canCancel(status string) bool {
	return status == "created" || status == "confirmed"
}

func requestHash(request CheckoutRequest) string {
	encoded, _ := json.Marshal(request)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func internalError() *httpx.APIError {
	return httpx.Error(http.StatusInternalServerError, "internal_error", "internal server error", nil)
}
