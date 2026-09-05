package analytics

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
)

type reader interface {
	Summary(context.Context) (Summary, error)
	OrdersByHour(context.Context) ([]HourBucket, error)
	ETA(context.Context) (ETAStats, error)
	OnTimeRatio(context.Context) (*float64, error)
	Cancellations(context.Context) (uint64, error)
	PopularProducts(context.Context) ([]ProductStat, error)
	StatusDurations(context.Context) ([]StatusDuration, error)
}

type Handler struct {
	store  reader
	logger *slog.Logger
}

func NewHandler(store reader, logger *slog.Logger) *Handler {
	return &Handler{store: store, logger: logger}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /internal/v1/analytics/summary", h.summary)
	mux.HandleFunc("GET /internal/v1/analytics/orders-by-hour", h.ordersByHour)
	mux.HandleFunc("GET /internal/v1/analytics/eta", h.eta)
	mux.HandleFunc("GET /internal/v1/analytics/on-time", h.onTime)
	mux.HandleFunc("GET /internal/v1/analytics/cancellations", h.cancellations)
	mux.HandleFunc("GET /internal/v1/analytics/popular-products", h.popularProducts)
	mux.HandleFunc("GET /internal/v1/analytics/status-durations", h.statusDurations)
}

func (h *Handler) summary(w http.ResponseWriter, r *http.Request) {
	value, err := h.store.Summary(r.Context())
	if err != nil {
		h.failed(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}

func (h *Handler) ordersByHour(w http.ResponseWriter, r *http.Request) {
	value, err := h.store.OrdersByHour(r.Context())
	if err != nil {
		h.failed(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"orders_by_hour": value})
}

func (h *Handler) eta(w http.ResponseWriter, r *http.Request) {
	value, err := h.store.ETA(r.Context())
	if err != nil {
		h.failed(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, value)
}

func (h *Handler) onTime(w http.ResponseWriter, r *http.Request) {
	value, err := h.store.OnTimeRatio(r.Context())
	if err != nil {
		h.failed(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"on_time_ratio": value})
}

func (h *Handler) cancellations(w http.ResponseWriter, r *http.Request) {
	value, err := h.store.Cancellations(r.Context())
	if err != nil {
		h.failed(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"cancellations": value})
}

func (h *Handler) popularProducts(w http.ResponseWriter, r *http.Request) {
	value, err := h.store.PopularProducts(r.Context())
	if err != nil {
		h.failed(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"popular_products": value})
}

func (h *Handler) statusDurations(w http.ResponseWriter, r *http.Request) {
	value, err := h.store.StatusDurations(r.Context())
	if err != nil {
		h.failed(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"status_durations": value})
}

func (h *Handler) failed(w http.ResponseWriter, r *http.Request, err error) {
	h.logger.Error("query analytics", "error", err, "correlation_id", httpx.CorrelationID(r.Context()))
	httpx.WriteError(w, r, httpx.Error(http.StatusInternalServerError, "analytics_query_failed", "analytics query failed", nil))
}
