ALTER TABLE delivery.deliveries
    ADD COLUMN item_count integer NOT NULL DEFAULT 1 CHECK (item_count > 0),
    ADD COLUMN predicted_eta_seconds integer CHECK (predicted_eta_seconds > 0),
    ADD COLUMN eta_model_version text,
    ADD COLUMN eta_updated_at timestamptz;

CREATE TABLE delivery.eta_predictions (
    id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES delivery.deliveries(id) ON DELETE CASCADE,
    order_id uuid NOT NULL,
    stage text NOT NULL CHECK (stage IN ('created', 'confirmed', 'assembling', 'delivering')),
    distance_km double precision NOT NULL CHECK (distance_km >= 0),
    item_count integer NOT NULL CHECK (item_count > 0),
    district_load double precision NOT NULL CHECK (district_load BETWEEN 0.5 AND 3.0),
    available_couriers integer NOT NULL CHECK (available_couriers >= 0),
    predicted_eta_seconds integer NOT NULL CHECK (predicted_eta_seconds > 0),
    actual_eta_seconds integer CHECK (actual_eta_seconds >= 0),
    model_version text NOT NULL,
    predicted_at timestamptz NOT NULL,
    completed_at timestamptz,
    UNIQUE (delivery_id, stage)
);
CREATE INDEX eta_predictions_evaluation_idx
    ON delivery.eta_predictions (model_version, predicted_at)
    WHERE actual_eta_seconds IS NOT NULL;
