package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHealth(t *testing.T) {
	handler := testHandler(nil)
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if response.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("unexpected content type: %q", response.Header().Get("Content-Type"))
	}
	if response.Header().Get(correlationHeader) == "" {
		t.Fatal("correlation header is empty")
	}
}

func TestCorrelationIDIsPreserved(t *testing.T) {
	handler := testHandler(nil)
	request := httptest.NewRequest(http.MethodGet, "/api/v1", nil)
	request.Header.Set(correlationHeader, "interview-demo-42")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if got := response.Header().Get(correlationHeader); got != "interview-demo-42" {
		t.Fatalf("correlation header = %q", got)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	handler := testHandler(nil)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "http_server_request_duration_seconds") {
		t.Fatalf("metrics endpoint status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestInvalidCorrelationIDIsReplaced(t *testing.T) {
	handler := testHandler(nil)
	request := httptest.NewRequest(http.MethodGet, "/api/v1", nil)
	request.Header.Set(correlationHeader, "not valid because spaces")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if got := response.Header().Get(correlationHeader); got == "" || got == "not valid because spaces" {
		t.Fatalf("correlation header was not replaced: %q", got)
	}
}

func TestReady(t *testing.T) {
	handler := testHandler([]Checker{
		{Name: "postgres", Check: func(context.Context) error { return nil }},
		{Name: "redis", Check: func(context.Context) error { return nil }},
	})
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var body struct {
		Status string                 `json:"status"`
		Checks map[string]checkResult `json:"checks"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "ready" || body.Checks["postgres"].Status != "up" {
		t.Fatalf("unexpected response: %#v", body)
	}
}

func TestReadyReturnsServiceUnavailable(t *testing.T) {
	handler := testHandler([]Checker{
		{Name: "kafka", Check: func(context.Context) error { return context.DeadlineExceeded }},
	})
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestCatalogProxyRewritesPathAndPropagatesCorrelationID(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/products" {
			t.Errorf("upstream path = %q", r.URL.Path)
		}
		if r.Header.Get(correlationHeader) != "proxy-test" {
			t.Errorf("upstream correlation ID = %q", r.Header.Get(correlationHeader))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"products":[]}`))
	}))
	defer upstream.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := New(Options{Logger: logger, CatalogServiceURL: upstream.URL})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/catalog/products", nil)
	request.Header.Set(correlationHeader, "proxy-test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "products") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func testHandler(checkers []Checker) http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(Options{Logger: logger, CheckTimeout: 50 * time.Millisecond, Checkers: checkers})
}
