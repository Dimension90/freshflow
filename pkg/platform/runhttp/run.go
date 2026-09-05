package runhttp

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
)

func Run(addr string, handler http.Handler, logger *slog.Logger, shutdownTimeout time.Duration) error {
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := telemetry.Shutdown(ctx); err != nil {
			logger.Warn("flush telemetry", "error", err)
		}
	}()
	server := &http.Server{
		Addr: addr, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}
	errChannel := make(chan error, 1)
	go func() {
		logger.Info("http server started", "address", addr)
		errChannel <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-errChannel:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

func JSONLogger(service, environment string) *slog.Logger {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", service, "environment", environment)
	if err := telemetry.Init(context.Background(), service, environment, logger); err != nil {
		logger.Error("initialize telemetry", "error", err)
	}
	return logger
}
