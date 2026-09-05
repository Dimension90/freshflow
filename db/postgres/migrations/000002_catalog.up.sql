CREATE TABLE catalog.products (
    id uuid PRIMARY KEY,
    sku text NOT NULL UNIQUE,
    name text NOT NULL,
    description text NOT NULL DEFAULT '',
    price_minor bigint NOT NULL CHECK (price_minor >= 0),
    currency char(3) NOT NULL DEFAULT 'RUB',
    available_quantity integer NOT NULL CHECK (available_quantity >= 0),
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE catalog.inventory_reservations (
    id uuid PRIMARY KEY,
    checkout_attempt_id uuid NOT NULL UNIQUE,
    status text NOT NULL CHECK (status IN ('active', 'failed', 'released', 'consumed')),
    response jsonb NOT NULL,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE catalog.inventory_reservation_items (
    reservation_id uuid NOT NULL REFERENCES catalog.inventory_reservations(id) ON DELETE CASCADE,
    product_id uuid NOT NULL REFERENCES catalog.products(id),
    quantity integer NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (reservation_id, product_id)
);

INSERT INTO catalog.products (id, sku, name, description, price_minor, currency, available_quantity)
VALUES
    ('10000000-0000-4000-8000-000000000001', 'APL-GALA-1KG', 'Яблоки Gala', 'Сладкие яблоки, упаковка 1 кг', 24900, 'RUB', 100),
    ('10000000-0000-4000-8000-000000000002', 'MLK-32-1L', 'Молоко 3.2%', 'Пастеризованное молоко, 1 л', 10900, 'RUB', 80),
    ('10000000-0000-4000-8000-000000000003', 'BRD-WHT-400', 'Хлеб пшеничный', 'Свежий пшеничный хлеб, 400 г', 7900, 'RUB', 60),
    ('10000000-0000-4000-8000-000000000004', 'CHS-GOU-200', 'Сыр Gouda', 'Полутвёрдый сыр, 200 г', 32900, 'RUB', 40),
    ('10000000-0000-4000-8000-000000000005', 'TMT-PNK-600', 'Томаты розовые', 'Упаковка 600 г', 29900, 'RUB', 50);

