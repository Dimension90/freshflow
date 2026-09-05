CREATE TABLE orders.users (
    id uuid PRIMARY KEY,
    display_name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders.orders (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES orders.users(id),
    reservation_id uuid NOT NULL UNIQUE,
    cart_version bigint NOT NULL,
    status text NOT NULL CHECK (status IN ('created', 'confirmed', 'assembling', 'delivering', 'delivered', 'cancelled')),
    total_amount_minor bigint NOT NULL CHECK (total_amount_minor >= 0),
    currency char(3) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders.order_items (
    order_id uuid NOT NULL REFERENCES orders.orders(id) ON DELETE CASCADE,
    product_id uuid NOT NULL,
    product_name text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    unit_price_minor bigint NOT NULL CHECK (unit_price_minor >= 0),
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE orders.idempotency_keys (
    key text PRIMARY KEY CHECK (length(key) BETWEEN 1 AND 128),
    request_hash char(64) NOT NULL,
    checkout_attempt_id uuid NOT NULL,
    state text NOT NULL CHECK (state IN ('processing', 'completed')),
    status_code integer,
    response jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO orders.users (id, display_name)
VALUES ('00000000-0000-4000-8000-000000000001', 'Demo User');

