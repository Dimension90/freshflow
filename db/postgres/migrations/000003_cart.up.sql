CREATE TABLE cart.carts (
    user_id uuid PRIMARY KEY,
    version bigint NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE cart.cart_items (
    user_id uuid NOT NULL REFERENCES cart.carts(user_id) ON DELETE CASCADE,
    product_id uuid NOT NULL,
    quantity integer NOT NULL CHECK (quantity BETWEEN 1 AND 99),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, product_id)
);

