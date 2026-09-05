package catalog

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
	"github.com/jackc/pgx/v5"
)

type Handler struct {
	store  *Store
	logger *slog.Logger
}

func NewHandler(store *Store, logger *slog.Logger) *Handler {
	return &Handler{store: store, logger: logger}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /internal/v1/products", h.listProducts)
	mux.HandleFunc("POST /internal/v1/reservations", h.reserve)
	mux.HandleFunc("POST /internal/v1/reservations/{reservationID}/release", h.release)
}

func (h *Handler) listProducts(w http.ResponseWriter, r *http.Request) {
	products, err := h.store.ListProducts(r.Context())
	if err != nil {
		h.logger.Error("list products", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"products": products})
}

func (h *Handler) reserve(w http.ResponseWriter, r *http.Request) {
	var request ReserveRequest
	if apiErr := httpx.DecodeJSON(w, r, &request); apiErr != nil {
		httpx.WriteError(w, r, apiErr)
		return
	}
	if apiErr := validateReserve(request); apiErr != nil {
		httpx.WriteError(w, r, apiErr)
		return
	}
	response, err := h.store.Reserve(r.Context(), request)
	if err != nil {
		h.logger.Error("reserve inventory", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	if response.Status == "failed" {
		httpx.WriteError(w, r, httpx.Error(http.StatusConflict, "inventory_insufficient", "not enough stock for one or more items", map[string]any{"reservation_id": response.ReservationID, "shortages": response.Shortages}))
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, response)
}

func (h *Handler) release(w http.ResponseWriter, r *http.Request) {
	reservationID := r.PathValue("reservationID")
	if !id.IsUUID(reservationID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_reservation_id", "reservation_id must be a UUID", nil))
		return
	}
	if err := h.store.Release(r.Context(), reservationID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.Error(http.StatusNotFound, "reservation_not_found", "reservation was not found", nil))
			return
		}
		h.logger.Error("release reservation", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validateReserve(request ReserveRequest) *httpx.APIError {
	if !id.IsUUID(request.CheckoutAttemptID) {
		return httpx.Error(http.StatusBadRequest, "invalid_checkout_attempt_id", "checkout_attempt_id must be a UUID", nil)
	}
	if len(request.Items) == 0 || len(request.Items) > 100 {
		return httpx.Error(http.StatusBadRequest, "invalid_items", "items must contain between 1 and 100 products", nil)
	}
	if request.TTLSeconds < 60 || request.TTLSeconds > int((30*time.Minute).Seconds()) {
		return httpx.Error(http.StatusBadRequest, "invalid_reservation_ttl", "ttl_seconds must be between 60 and 1800", nil)
	}
	seen := make(map[string]struct{}, len(request.Items))
	for _, item := range request.Items {
		if !id.IsUUID(item.ProductID) || item.Quantity < 1 || item.Quantity > 99 {
			return httpx.Error(http.StatusBadRequest, "invalid_items", "each item requires a UUID product_id and quantity between 1 and 99", nil)
		}
		if _, exists := seen[item.ProductID]; exists {
			return httpx.Error(http.StatusBadRequest, "duplicate_product", "items must not contain duplicate product_id values", nil)
		}
		seen[item.ProductID] = struct{}{}
	}
	return nil
}
