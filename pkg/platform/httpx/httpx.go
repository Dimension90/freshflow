package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const CorrelationHeader = "X-Correlation-ID"

var validCorrelationID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type contextKey string

const correlationKey contextKey = "correlation_id"

type APIError struct {
	Status  int
	Code    string
	Message string
	Details any
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func Error(status int, code, message string, details any) *APIError {
	return &APIError{Status: status, Code: code, Message: message, Details: details}
}

func DecodeJSON(w http.ResponseWriter, r *http.Request, destination any) *APIError {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return Error(http.StatusBadRequest, "invalid_json", "request body is not valid JSON", nil)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Error(http.StatusBadRequest, "invalid_json", "request body must contain one JSON value", nil)
	}
	return nil
}

func WriteJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func WriteError(w http.ResponseWriter, r *http.Request, apiErr *APIError) {
	if apiErr == nil {
		apiErr = Error(http.StatusInternalServerError, "internal_error", "internal server error", nil)
	}
	payload := map[string]any{
		"code":           apiErr.Code,
		"message":        apiErr.Message,
		"correlation_id": CorrelationID(r.Context()),
	}
	if apiErr.Details != nil {
		payload["details"] = apiErr.Details
	}
	WriteJSON(w, apiErr.Status, map[string]any{"error": payload})
}

func CorrelationID(ctx context.Context) string {
	value, _ := ctx.Value(correlationKey).(string)
	return value
}

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationKey, correlationID)
}

func Wrap(service string, logger *slog.Logger, next http.Handler) http.Handler {
	if err := telemetry.Init(context.Background(), service, os.Getenv("FRESHFLOW_ENV"), logger); err != nil {
		logger.Error("initialize telemetry", "error", err)
	}
	withMetrics := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			telemetry.MetricsHandler().ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
	observed := recoverPanic(service, logger, accessLog(service, logger, withMetrics))
	return correlation(otelhttp.NewHandler(observed, service+".http"))
}

func correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := r.Header.Get(CorrelationHeader)
		if !validCorrelationID.MatchString(correlationID) {
			correlationID = randomID()
		}
		w.Header().Set(CorrelationHeader, correlationID)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), correlationKey, correlationID)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func accessLog(service string, logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		duration := time.Since(started)
		telemetry.ObserveHTTP(service, route, r.Method, recorder.status, duration)
		trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("http.route", route))
		fields := []any{"service", service, "method", r.Method, "path", r.URL.Path, "route", route,
			"status", recorder.status, "duration_ms", duration.Milliseconds(), "correlation_id", CorrelationID(r.Context())}
		fields = append(fields, telemetry.TraceFields(r.Context())...)
		logger.Info("http request completed", fields...)
	})
}

func recoverPanic(service string, logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("panic recovered", "service", service, "panic", recovered, "correlation_id", CorrelationID(r.Context()))
				WriteError(w, r, Error(http.StatusInternalServerError, "internal_error", "internal server error", nil))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func randomID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(value)
}
