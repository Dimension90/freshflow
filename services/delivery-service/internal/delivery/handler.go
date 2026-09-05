package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
	"github.com/freshflow/freshflow/pkg/platform/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handler struct {
	postgres *pgxpool.Pool
	logger   *slog.Logger
}

func NewHandler(postgres *pgxpool.Pool, logger *slog.Logger) *Handler {
	return &Handler{postgres: postgres, logger: logger}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /internal/v1/deliveries/order/{orderID}", h.getByOrder)
	mux.HandleFunc("GET /internal/v1/deliveries/order/{orderID}/stream", h.streamByOrder)
	mux.HandleFunc("GET /internal/v1/simulator/assignments", h.assignments)
	mux.HandleFunc("GET /internal/v1/operations/couriers", h.couriers)
	mux.HandleFunc("POST /internal/v1/operations/deliveries/{deliveryID}/actions", h.action)
}

type operationAction struct {
	Action string `json:"action"`
}

func (h *Handler) couriers(w http.ResponseWriter, r *http.Request) {
	rows, err := h.postgres.Query(r.Context(), `
		SELECT c.id::text, c.name, c.status, c.latitude, c.longitude, c.last_seen_at,
		       d.id::text, d.order_id::text, d.status
		FROM delivery.couriers c
		LEFT JOIN delivery.deliveries d ON d.courier_id = c.id AND d.status NOT IN ('completed', 'cancelled')
		ORDER BY c.name`)
	if err != nil {
		httpx.WriteError(w, r, nil)
		return
	}
	defer rows.Close()
	values := make([]Courier, 0)
	for rows.Next() {
		var value Courier
		if err := rows.Scan(&value.ID, &value.Name, &value.Status, &value.Latitude, &value.Longitude, &value.LastSeenAt,
			&value.ActiveDeliveryID, &value.ActiveOrderID, &value.DeliveryStatus); err != nil {
			httpx.WriteError(w, r, nil)
			return
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"couriers": values})
}

func (h *Handler) action(w http.ResponseWriter, r *http.Request) {
	deliveryID := r.PathValue("deliveryID")
	if !id.IsUUID(deliveryID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_delivery_id", "delivery_id must be a UUID", nil))
		return
	}
	var request operationAction
	if apiErr := httpx.DecodeJSON(w, r, &request); apiErr != nil {
		httpx.WriteError(w, r, apiErr)
		return
	}
	var err error
	switch request.Action {
	case "delay":
		err = h.delay(r.Context(), deliveryID)
	case "complete":
		err = h.complete(r.Context(), deliveryID)
	default:
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_operation_action", "action must be delay or complete", nil))
		return
	}
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.WriteError(w, r, httpx.Error(http.StatusNotFound, "delivery_not_found", "delivery was not found", nil))
		return
	}
	if errors.Is(err, errTerminalDelivery) {
		httpx.WriteError(w, r, httpx.Error(http.StatusConflict, "delivery_not_actionable", "delivery is already terminal", nil))
		return
	}
	if err != nil {
		h.logger.Error("apply delivery operation", "delivery_id", deliveryID, "action", request.Action, "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	value, err := h.deliveryByID(r.Context(), deliveryID)
	if err != nil {
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}

var errTerminalDelivery = errors.New("terminal delivery")

func (h *Handler) delay(ctx context.Context, deliveryID string) error {
	tx, err := h.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var orderID, status string
	if err := tx.QueryRow(ctx, `SELECT order_id::text, status FROM delivery.deliveries WHERE id = $1 FOR UPDATE`, deliveryID).Scan(&orderID, &status); err != nil {
		return err
	}
	if status == "completed" || status == "cancelled" {
		return errTerminalDelivery
	}
	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE delivery.deliveries SET simulation_delay_seconds = LEAST(600, simulation_delay_seconds + 10), predicted_eta_seconds = CASE WHEN predicted_eta_seconds IS NULL THEN NULL ELSE predicted_eta_seconds + 10 END, eta_updated_at = now(), updated_at = now() WHERE id = $1`, deliveryID); err != nil {
		return err
	}
	event, err := events.New(ctx, "delivery.delay_injected", "delivery-service", orderID, map[string]any{"delivery_id": deliveryID, "order_id": orderID, "delay_seconds": 10, "changed_at": now})
	if err != nil {
		return err
	}
	if err := outbox.Insert(ctx, tx, "delivery", "freshflow.delivery.events.v1", orderID, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (h *Handler) complete(ctx context.Context, deliveryID string) error {
	tx, err := h.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var orderID, courierID, status, correlationID string
	var assignedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT order_id::text, courier_id::text, status, correlation_id, assigned_at FROM delivery.deliveries WHERE id = $1 FOR UPDATE`, deliveryID).Scan(&orderID, &courierID, &status, &correlationID, &assignedAt); err != nil {
		return err
	}
	if status == "completed" || status == "cancelled" {
		return errTerminalDelivery
	}
	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE delivery.deliveries SET status = 'completed', completed_at = $2, updated_at = $2 WHERE id = $1`, deliveryID, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE delivery.couriers SET status = 'available', updated_at = $2 WHERE id = $1`, courierID, now); err != nil {
		return err
	}
	actualETA := int(now.Sub(assignedAt).Seconds())
	if _, err := tx.Exec(ctx, `UPDATE delivery.eta_predictions SET actual_eta_seconds = GREATEST(0, EXTRACT(EPOCH FROM ($2 - predicted_at))::integer), completed_at = $2 WHERE delivery_id = $1`, deliveryID, now); err != nil {
		return err
	}
	eventCtx := httpx.WithCorrelationID(ctx, correlationID)
	event, err := events.New(eventCtx, "delivery.completed", "delivery-service", orderID, map[string]any{"delivery_id": deliveryID, "order_id": orderID, "courier_id": courierID, "assigned_at": assignedAt, "completed_at": now, "actual_eta_seconds": actualETA, "completed_by": "operator"})
	if err != nil {
		return err
	}
	if err := outbox.Insert(ctx, tx, "delivery", "freshflow.delivery.events.v1", orderID, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// streamByOrder emits a pending event until assignment, then a delivery snapshot
// every second so the UI can animate the simulated courier without polling.
func (h *Handler) streamByOrder(w http.ResponseWriter, r *http.Request) {
	orderID := r.PathValue("orderID")
	if !id.IsUUID(orderID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_order_id", "order_id must be a UUID", nil))
		return
	}
	responseController := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		value, err := h.deliveryByOrder(r.Context(), orderID)
		if errors.Is(err, pgx.ErrNoRows) {
			_, _ = fmt.Fprint(w, "event: pending\ndata: {}\n\n")
			if responseController.Flush() != nil {
				return
			}
		} else if err != nil {
			h.logger.Warn("stream delivery", "order_id", orderID, "error", err)
			return
		} else {
			encoded, marshalErr := json.Marshal(value)
			if marshalErr != nil {
				return
			}
			_, _ = fmt.Fprintf(w, "event: delivery\ndata: %s\n\n", encoded)
			if responseController.Flush() != nil {
				return
			}
			if value.Status == "completed" || value.Status == "cancelled" {
				return
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *Handler) getByOrder(w http.ResponseWriter, r *http.Request) {
	orderID := r.PathValue("orderID")
	if !id.IsUUID(orderID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_order_id", "order_id must be a UUID", nil))
		return
	}
	value, err := h.deliveryByOrder(r.Context(), orderID)
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.WriteError(w, r, httpx.Error(http.StatusNotFound, "delivery_not_found", "delivery was not found", nil))
		return
	}
	if err != nil {
		h.logger.Error("get delivery", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}

func (h *Handler) deliveryByOrder(ctx context.Context, orderID string) (Delivery, error) {
	return scanDelivery(h.postgres.QueryRow(ctx, `
		SELECT d.id::text, d.order_id::text, d.courier_id::text, c.name, d.status, d.pickup_latitude, d.pickup_longitude,
		       d.destination_latitude, d.destination_longitude, d.assigned_at, d.started_at, d.completed_at,
		       c.latitude, c.longitude, d.predicted_eta_seconds, d.eta_model_version, d.eta_updated_at, d.simulation_delay_seconds
		FROM delivery.deliveries d JOIN delivery.couriers c ON c.id = d.courier_id WHERE d.order_id = $1`, orderID))
}

func (h *Handler) deliveryByID(ctx context.Context, deliveryID string) (Delivery, error) {
	return scanDelivery(h.postgres.QueryRow(ctx, `
		SELECT d.id::text, d.order_id::text, d.courier_id::text, c.name, d.status, d.pickup_latitude, d.pickup_longitude,
		       d.destination_latitude, d.destination_longitude, d.assigned_at, d.started_at, d.completed_at,
		       c.latitude, c.longitude, d.predicted_eta_seconds, d.eta_model_version, d.eta_updated_at, d.simulation_delay_seconds
		FROM delivery.deliveries d JOIN delivery.couriers c ON c.id = d.courier_id WHERE d.id = $1`, deliveryID))
}

func (h *Handler) assignments(w http.ResponseWriter, r *http.Request) {
	rows, err := h.postgres.Query(r.Context(), `
		SELECT d.id::text, d.order_id::text, d.courier_id::text, c.name, d.status, d.pickup_latitude, d.pickup_longitude,
		       d.destination_latitude, d.destination_longitude, d.assigned_at, d.started_at, d.completed_at,
		       c.latitude, c.longitude, d.predicted_eta_seconds, d.eta_model_version, d.eta_updated_at, d.correlation_id, d.simulation_delay_seconds
		FROM delivery.deliveries d JOIN delivery.couriers c ON c.id = d.courier_id
		WHERE d.status NOT IN ('completed', 'cancelled') ORDER BY d.assigned_at`)
	if err != nil {
		httpx.WriteError(w, r, nil)
		return
	}
	defer rows.Close()
	result := make([]Assignment, 0)
	for rows.Next() {
		var value Assignment
		if err := rows.Scan(&value.ID, &value.OrderID, &value.CourierID, &value.CourierName, &value.Status, &value.PickupLatitude, &value.PickupLongitude,
			&value.DestinationLatitude, &value.DestinationLongitude, &value.AssignedAt, &value.StartedAt, &value.CompletedAt,
			&value.CourierLatitude, &value.CourierLongitude, &value.PredictedETASeconds, &value.ETAModelVersion, &value.ETAUpdatedAt,
			&value.CorrelationID, &value.SimulationDelaySeconds); err != nil {
			httpx.WriteError(w, r, nil)
			return
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		h.logger.Error("iterate active assignments", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"assignments": result})
}

type rowScanner interface{ Scan(...any) error }

func scanDelivery(row rowScanner) (Delivery, error) {
	var value Delivery
	err := row.Scan(&value.ID, &value.OrderID, &value.CourierID, &value.CourierName, &value.Status, &value.PickupLatitude, &value.PickupLongitude,
		&value.DestinationLatitude, &value.DestinationLongitude, &value.AssignedAt, &value.StartedAt, &value.CompletedAt,
		&value.CourierLatitude, &value.CourierLongitude, &value.PredictedETASeconds, &value.ETAModelVersion, &value.ETAUpdatedAt, &value.SimulationDelaySeconds)
	return value, err
}
