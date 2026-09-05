package order

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
	"github.com/jackc/pgx/v5"
)

var validIdempotencyKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type Handler struct {
	service *Service
	logger  *slog.Logger
}

func NewHandler(service *Service, logger *slog.Logger) *Handler {
	return &Handler{service: service, logger: logger}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/v1/orders", h.create)
	mux.HandleFunc("GET /internal/v1/orders/{orderID}", h.get)
	mux.HandleFunc("DELETE /internal/v1/orders/{orderID}", h.cancel)
	mux.HandleFunc("GET /internal/v1/orders/{orderID}/stream", h.stream)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	orderID := r.PathValue("orderID")
	if !id.IsUUID(orderID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_order_id", "order_id must be a UUID", nil))
		return
	}
	value, apiErr := h.service.Cancel(r.Context(), orderID)
	if apiErr != nil {
		httpx.WriteError(w, r, apiErr)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}

// stream sends the initial order snapshot and each subsequent status change.
// The database remains the source of truth; SSE only avoids client-side polling.
func (h *Handler) stream(w http.ResponseWriter, r *http.Request) {
	orderID := r.PathValue("orderID")
	if !id.IsUUID(orderID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_order_id", "order_id must be a UUID", nil))
		return
	}
	responseController := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	lastStatus := ""
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		value, err := h.service.Get(r.Context(), orderID)
		if errors.Is(err, pgx.ErrNoRows) {
			if lastStatus == "" {
				httpx.WriteError(w, r, httpx.Error(http.StatusNotFound, "order_not_found", "order was not found", nil))
			}
			return
		}
		if err != nil {
			h.logger.Warn("stream order", "order_id", orderID, "error", err)
			return
		}
		if value.Status != lastStatus {
			encoded, err := json.Marshal(value)
			if err != nil {
				return
			}
			_, _ = fmt.Fprintf(w, "event: order\ndata: %s\n\n", encoded)
			if responseController.Flush() != nil {
				return
			}
			lastStatus = value.Status
		}
		if value.Status == "delivered" || value.Status == "cancelled" {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if !validIdempotencyKey.MatchString(key) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key is required and must contain 1-128 safe characters", nil))
		return
	}
	var request CheckoutRequest
	if apiErr := httpx.DecodeJSON(w, r, &request); apiErr != nil {
		httpx.WriteError(w, r, apiErr)
		return
	}
	if !id.IsUUID(request.UserID) || request.CartVersion < 1 {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_checkout", "user_id must be a UUID and cart_version must be positive", nil))
		return
	}
	created, replayed, apiErr := h.service.Create(r.Context(), key, request)
	if apiErr != nil {
		httpx.WriteError(w, r, apiErr)
		return
	}
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	httpx.WriteJSON(w, http.StatusCreated, created)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	orderID := r.PathValue("orderID")
	if !id.IsUUID(orderID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_order_id", "order_id must be a UUID", nil))
		return
	}
	value, err := h.service.Get(r.Context(), orderID)
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.WriteError(w, r, httpx.Error(http.StatusNotFound, "order_not_found", "order was not found", nil))
		return
	}
	if err != nil {
		h.logger.Error("get order", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}
