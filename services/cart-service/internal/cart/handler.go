package cart

import (
	"log/slog"
	"net/http"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
)

type Handler struct {
	store  *Store
	logger *slog.Logger
}

func NewHandler(store *Store, logger *slog.Logger) *Handler {
	return &Handler{store: store, logger: logger}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /internal/v1/carts/{userID}", h.get)
	mux.HandleFunc("PUT /internal/v1/carts/{userID}/items/{productID}", h.setItem)
	mux.HandleFunc("DELETE /internal/v1/carts/{userID}/items/{productID}", h.deleteItem)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userID")
	if !id.IsUUID(userID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_user_id", "user_id must be a UUID", nil))
		return
	}
	value, err := h.store.Get(r.Context(), userID)
	if err != nil {
		h.logger.Error("get cart", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}

func (h *Handler) setItem(w http.ResponseWriter, r *http.Request) {
	userID, productID := r.PathValue("userID"), r.PathValue("productID")
	if !id.IsUUID(userID) || !id.IsUUID(productID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_identifier", "user_id and product_id must be UUID values", nil))
		return
	}
	var request SetItemRequest
	if apiErr := httpx.DecodeJSON(w, r, &request); apiErr != nil {
		httpx.WriteError(w, r, apiErr)
		return
	}
	if request.Quantity < 1 || request.Quantity > 99 {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_quantity", "quantity must be between 1 and 99", nil))
		return
	}
	value, err := h.store.SetItem(r.Context(), userID, productID, request.Quantity)
	if err != nil {
		h.logger.Error("set cart item", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}

func (h *Handler) deleteItem(w http.ResponseWriter, r *http.Request) {
	userID, productID := r.PathValue("userID"), r.PathValue("productID")
	if !id.IsUUID(userID) || !id.IsUUID(productID) {
		httpx.WriteError(w, r, httpx.Error(http.StatusBadRequest, "invalid_identifier", "user_id and product_id must be UUID values", nil))
		return
	}
	value, err := h.store.DeleteItem(r.Context(), userID, productID)
	if err != nil {
		h.logger.Error("delete cart item", "error", err)
		httpx.WriteError(w, r, nil)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}
