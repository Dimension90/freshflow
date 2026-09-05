package cart

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
)

func TestSetItemRejectsInvalidQuantityBeforeStore(t *testing.T) {
	handler := NewHandler(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mux := http.NewServeMux()
	handler.Register(mux)
	request := httptest.NewRequest(http.MethodPut, "/internal/v1/carts/00000000-0000-4000-8000-000000000001/items/10000000-0000-4000-8000-000000000001", strings.NewReader(`{"quantity":0}`))
	response := httptest.NewRecorder()
	httpx.Wrap("cart-service", slog.Default(), mux).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
}
