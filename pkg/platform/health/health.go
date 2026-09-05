package health

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
)

type Checker struct {
	Name  string
	Check func(context.Context) error
}

func Register(mux *http.ServeMux, service string, logger *slog.Logger, timeout time.Duration, checkers []Checker) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"status": "up", "service": service})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		type result struct {
			Name       string
			Status     string
			DurationMS int64
		}
		channel := make(chan result, len(checkers))
		var wait sync.WaitGroup
		for _, checker := range checkers {
			checker := checker
			wait.Add(1)
			go func() {
				defer wait.Done()
				started := time.Now()
				ctx, cancel := context.WithTimeout(r.Context(), timeout)
				defer cancel()
				status := "up"
				if err := checker.Check(ctx); err != nil {
					status = "down"
					logger.Warn("dependency readiness check failed", "service", service, "dependency", checker.Name, "error", err)
				}
				channel <- result{Name: checker.Name, Status: status, DurationMS: time.Since(started).Milliseconds()}
			}()
		}
		wait.Wait()
		close(channel)

		statusCode := http.StatusOK
		overall := "ready"
		checks := make(map[string]any, len(checkers))
		for item := range channel {
			checks[item.Name] = map[string]any{"status": item.Status, "duration_ms": item.DurationMS}
			if item.Status != "up" {
				statusCode = http.StatusServiceUnavailable
				overall = "not_ready"
			}
		}
		httpx.WriteJSON(w, statusCode, map[string]any{"status": overall, "checks": checks})
	})
}
