package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
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
		       c.latitude, c.longitude, d.predicted_eta_seconds, d.eta_model_version, d.eta_updated_at
		FROM delivery.deliveries d JOIN delivery.couriers c ON c.id = d.courier_id WHERE d.order_id = $1`, orderID))
}

func (h *Handler) assignments(w http.ResponseWriter, r *http.Request) {
	rows, err := h.postgres.Query(r.Context(), `
		SELECT d.id::text, d.order_id::text, d.courier_id::text, c.name, d.status, d.pickup_latitude, d.pickup_longitude,
		       d.destination_latitude, d.destination_longitude, d.assigned_at, d.started_at, d.completed_at,
		       c.latitude, c.longitude, d.predicted_eta_seconds, d.eta_model_version, d.eta_updated_at, d.correlation_id
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
			&value.CorrelationID); err != nil {
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
		&value.CourierLatitude, &value.CourierLongitude, &value.PredictedETASeconds, &value.ETAModelVersion, &value.ETAUpdatedAt)
	return value, err
}
