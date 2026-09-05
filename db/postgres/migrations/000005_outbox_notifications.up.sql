CREATE TABLE catalog.outbox (
    event_id uuid PRIMARY KEY,
    topic text NOT NULL,
    event_key text NOT NULL,
    event_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    correlation_id text NOT NULL DEFAULT '',
    trace_id text NOT NULL DEFAULT '',
    envelope jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    published_at timestamptz,
    last_error text
);
CREATE INDEX catalog_outbox_pending_idx ON catalog.outbox (occurred_at) WHERE published_at IS NULL;

CREATE TABLE orders.outbox (
    event_id uuid PRIMARY KEY,
    topic text NOT NULL,
    event_key text NOT NULL,
    event_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    correlation_id text NOT NULL DEFAULT '',
    trace_id text NOT NULL DEFAULT '',
    envelope jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    published_at timestamptz,
    last_error text
);
CREATE INDEX orders_outbox_pending_idx ON orders.outbox (occurred_at) WHERE published_at IS NULL;

CREATE TABLE notifications.processed_events (
    event_id uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications.notification_log (
    id bigserial PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE REFERENCES notifications.processed_events(event_id),
    order_id uuid NOT NULL,
    event_type text NOT NULL,
    message text NOT NULL,
    correlation_id text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

