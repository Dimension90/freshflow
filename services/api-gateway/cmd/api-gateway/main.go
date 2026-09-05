package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/freshflow/freshflow/services/api-gateway/internal/config"
	"github.com/freshflow/freshflow/services/api-gateway/internal/dependencies"
	"github.com/freshflow/freshflow/services/api-gateway/internal/httpapi"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	logger = logger.With("service", "api-gateway", "environment", cfg.Environment)
	slog.SetDefault(logger)

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), cfg.StartupTimeout)
	defer cancelStartup()

	deps, err := dependencies.Open(startupCtx, cfg)
	if err != nil {
		logger.Error("failed to initialize dependencies", "error", err)
		os.Exit(1)
	}
	defer deps.Close()

	checkers := append(deps.Checkers(),
		dependencies.HTTPChecker("catalog-service", cfg.CatalogServiceURL),
		dependencies.HTTPChecker("cart-service", cfg.CartServiceURL),
		dependencies.HTTPChecker("order-service", cfg.OrderServiceURL),
		dependencies.HTTPChecker("delivery-service", cfg.DeliveryServiceURL),
		dependencies.HTTPChecker("analytics-worker", cfg.AnalyticsServiceURL),
	)
	handler := httpapi.New(httpapi.Options{
		Logger:              logger,
		CheckTimeout:        cfg.DependencyCheckTimeout,
		Checkers:            checkers,
		CatalogServiceURL:   cfg.CatalogServiceURL,
		CartServiceURL:      cfg.CartServiceURL,
		OrderServiceURL:     cfg.OrderServiceURL,
		DeliveryServiceURL:  cfg.DeliveryServiceURL,
		AnalyticsServiceURL: cfg.AnalyticsServiceURL,
	})

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server started", "address", cfg.HTTPAddr)
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
		close(errCh)
	}()

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case serveErr := <-errCh:
		if serveErr != nil {
			logger.Error("http server failed", "error", serveErr)
			os.Exit(1)
		}
	case <-signalCtx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
		os.Exit(1)
	}
	if err := telemetry.Shutdown(shutdownCtx); err != nil {
		logger.Warn("flush telemetry", "error", err)
	}
	logger.Info("http server stopped")
}
