package analytics

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

type fakeSLOStore struct {
	called    bool
	ratio     *float64
	completed uint64
}

func (s *fakeSLOStore) DeliverySLO(context.Context) (*float64, uint64, error) {
	s.called = true
	return s.ratio, s.completed, nil
}

func TestSLOReporterCollectsDeliverySLO(t *testing.T) {
	ratio := 0.95
	store := &fakeSLOStore{ratio: &ratio, completed: 20}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	NewSLOReporter(store, logger, 0).collect(context.Background())

	if !store.called {
		t.Fatal("DeliverySLO was not collected")
	}
}
