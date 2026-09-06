package analytics

import (
	"context"
	"log/slog"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/telemetry"
)

const deliverySLOWindow = "24h"

type sloStore interface {
	DeliverySLO(context.Context) (*float64, uint64, error)
}

// SLOReporter reads ClickHouse periodically and exposes a bounded, low-cardinality
// delivery SLO to Prometheus. It never blocks Kafka consumption.
type SLOReporter struct {
	store    sloStore
	logger   *slog.Logger
	interval time.Duration
}

func NewSLOReporter(store sloStore, logger *slog.Logger, interval time.Duration) *SLOReporter {
	return &SLOReporter{store: store, logger: logger, interval: interval}
}

func (r *SLOReporter) Run(ctx context.Context) {
	r.collect(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.collect(ctx)
		}
	}
}

func (r *SLOReporter) collect(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	ratio, completed, err := r.store.DeliverySLO(ctx)
	if err != nil {
		r.logger.Warn("collect delivery SLO", "error", err)
		return
	}
	telemetry.ObserveDeliveryOnTimeRatio("analytics-worker", deliverySLOWindow, ratio, completed)
}
