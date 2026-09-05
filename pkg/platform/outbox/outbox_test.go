package outbox

import (
	"context"
	"testing"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeExecutor struct {
	query string
	args  []any
}

func (f *fakeExecutor) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	f.query, f.args = query, args
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func TestInsertPersistsCompleteEnvelope(t *testing.T) {
	envelope, err := events.New(context.Background(), "order.created", "order-service", "30000000-0000-4000-8000-000000000001", map[string]string{"status": "created"})
	if err != nil {
		t.Fatal(err)
	}
	executor := &fakeExecutor{}
	if err := Insert(context.Background(), executor, "orders", "freshflow.order.events.v1", envelope.AggregateID, envelope); err != nil {
		t.Fatal(err)
	}
	if executor.query == "" || len(executor.args) != 9 || executor.args[0] != envelope.EventID {
		t.Fatalf("unexpected insert: query=%q args=%#v", executor.query, executor.args)
	}
}

func TestInsertRejectsUnsafeSchema(t *testing.T) {
	err := Insert(context.Background(), &fakeExecutor{}, "orders; DROP SCHEMA orders", "topic", "key", events.Envelope{})
	if err == nil {
		t.Fatal("unsafe schema was accepted")
	}
}
