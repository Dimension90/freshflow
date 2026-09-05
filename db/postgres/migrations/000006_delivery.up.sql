ALTER TABLE orders.users
    ADD COLUMN delivery_latitude double precision NOT NULL DEFAULT 55.0415,
    ADD COLUMN delivery_longitude double precision NOT NULL DEFAULT 82.9346;

ALTER TABLE orders.orders
    ADD COLUMN delivery_latitude double precision NOT NULL DEFAULT 55.0415,
    ADD COLUMN delivery_longitude double precision NOT NULL DEFAULT 82.9346;

CREATE TABLE orders.order_status_history (
    id bigserial PRIMARY KEY,
    order_id uuid NOT NULL REFERENCES orders.orders(id) ON DELETE CASCADE,
    previous_status text NOT NULL,
    new_status text NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders.processed_events (
    event_id uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE delivery.couriers (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    status text NOT NULL CHECK (status IN ('available', 'assigned', 'offline')),
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    location_sequence bigint NOT NULL DEFAULT 0,
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE delivery.deliveries (
    id uuid PRIMARY KEY,
    order_id uuid NOT NULL UNIQUE,
    courier_id uuid NOT NULL REFERENCES delivery.couriers(id),
    status text NOT NULL CHECK (status IN ('assigned', 'assembling', 'delivering', 'completed')),
    pickup_latitude double precision NOT NULL,
    pickup_longitude double precision NOT NULL,
    destination_latitude double precision NOT NULL,
    destination_longitude double precision NOT NULL,
    correlation_id text NOT NULL DEFAULT '',
    assigned_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX deliveries_active_courier_idx ON delivery.deliveries (courier_id) WHERE status <> 'completed';

CREATE TABLE delivery.processed_events (
    event_id uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE delivery.outbox (
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
CREATE INDEX delivery_outbox_pending_idx ON delivery.outbox (occurred_at) WHERE published_at IS NULL;

INSERT INTO delivery.couriers (id, name, status, latitude, longitude)
VALUES
    ('40000000-0000-4000-8000-000000000001', 'Courier North', 'available', 55.0411, 82.9207),
    ('40000000-0000-4000-8000-000000000002', 'Courier Center', 'available', 55.0299, 82.9234),
    ('40000000-0000-4000-8000-000000000003', 'Courier East', 'available', 55.0378, 82.9581);
