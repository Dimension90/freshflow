package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	platformhttpx "github.com/freshflow/freshflow/pkg/platform/httpx"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const correlationHeader = "X-Correlation-ID"

type Checker struct {
	Name  string
	Check func(context.Context) error
}

type Options struct {
	Logger              *slog.Logger
	CheckTimeout        time.Duration
	Checkers            []Checker
	CatalogServiceURL   string
	CartServiceURL      string
	OrderServiceURL     string
	DeliveryServiceURL  string
	AnalyticsServiceURL string
}

type server struct {
	logger       *slog.Logger
	checkTimeout time.Duration
	checkers     []Checker
}

func New(options Options) http.Handler {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.CheckTimeout <= 0 {
		options.CheckTimeout = 2 * time.Second
	}

	s := &server{
		logger:       options.Logger,
		checkTimeout: options.CheckTimeout,
		checkers:     append([]Checker(nil), options.Checkers...),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /api/v1", s.apiRoot)
	mux.Handle("/api/v1/catalog/", s.reverseProxy(options.CatalogServiceURL, "/api/v1/catalog", "/internal/v1"))
	mux.Handle("/api/v1/carts/", s.reverseProxy(options.CartServiceURL, "/api/v1/carts", "/internal/v1/carts"))
	mux.Handle("/api/v1/orders", s.reverseProxy(options.OrderServiceURL, "/api/v1/orders", "/internal/v1/orders"))
	mux.Handle("/api/v1/orders/", s.reverseProxy(options.OrderServiceURL, "/api/v1/orders", "/internal/v1/orders"))
	mux.Handle("/api/v1/deliveries/", s.reverseProxy(options.DeliveryServiceURL, "/api/v1/deliveries", "/internal/v1/deliveries"))
	mux.Handle("/api/v1/analytics/", s.reverseProxy(options.AnalyticsServiceURL, "/api/v1/analytics", "/internal/v1/analytics"))

	return platformhttpx.Wrap("api-gateway", options.Logger, mux)
}

func (s *server) reverseProxy(rawURL, publicPrefix, internalPrefix string) http.Handler {
	target, err := url.Parse(rawURL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			s.logger.Error("invalid upstream URL", "url", rawURL)
			writeAPIError(w, http.StatusServiceUnavailable, "upstream_unavailable", "service is unavailable", platformhttpx.CorrelationID(r.Context()))
		})
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = otelhttp.NewTransport(http.DefaultTransport)
	originalDirector := proxy.Director
	proxy.Director = func(request *http.Request) {
		originalDirector(request)
		request.URL.Path = internalPrefix + strings.TrimPrefix(request.URL.Path, publicPrefix)
		request.Header.Set(correlationHeader, platformhttpx.CorrelationID(request.Context()))
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, proxyErr error) {
		s.logger.Warn("upstream request failed", "upstream", target.Host, "error", proxyErr, "correlation_id", platformhttpx.CorrelationID(r.Context()))
		writeAPIError(w, http.StatusServiceUnavailable, "upstream_unavailable", "service is unavailable", platformhttpx.CorrelationID(r.Context()))
	}
	return proxy
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "up",
		"service": "api-gateway",
	})
}

func (s *server) apiRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":    "FreshFlow API",
		"version": "v1",
		"status":  "bootstrapped",
	})
}

type checkResult struct {
	Status     string `json:"status"`
	DurationMS int64  `json:"duration_ms"`
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	type namedResult struct {
		name   string
		result checkResult
	}

	results := make(chan namedResult, len(s.checkers))
	var wg sync.WaitGroup
	for _, checker := range s.checkers {
		checker := checker
		wg.Add(1)
		go func() {
			defer wg.Done()
			started := time.Now()
			ctx, cancel := context.WithTimeout(r.Context(), s.checkTimeout)
			defer cancel()
			status := "up"
			if err := checker.Check(ctx); err != nil {
				status = "down"
				s.logger.Warn("dependency readiness check failed", "dependency", checker.Name, "error", err)
			}
			results <- namedResult{
				name:   checker.Name,
				result: checkResult{Status: status, DurationMS: time.Since(started).Milliseconds()},
			}
		}()
	}
	wg.Wait()
	close(results)

	statusCode := http.StatusOK
	overall := "ready"
	checks := make(map[string]checkResult, len(s.checkers))
	for item := range results {
		checks[item.name] = item.result
		if item.result.Status != "up" {
			statusCode = http.StatusServiceUnavailable
			overall = "not_ready"
		}
	}

	writeJSON(w, statusCode, map[string]any{"status": overall, "checks": checks})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAPIError(w http.ResponseWriter, status int, code, message, correlationID string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":           code,
			"message":        message,
			"correlation_id": correlationID,
		},
	})
}
