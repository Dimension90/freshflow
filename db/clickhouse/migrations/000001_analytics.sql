CREATE TABLE IF NOT EXISTS delivery_events
(
    event_id UUID,
    event_type LowCardinality(String),
    order_id Nullable(UUID),
    delivery_id Nullable(UUID),
    courier_id Nullable(UUID),
    status LowCardinality(String),
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    predicted_eta_seconds Nullable(UInt32),
    actual_eta_seconds Nullable(UInt32),
    occurred_at DateTime64(3, 'UTC'),
    correlation_id String,
    trace_id String,
    payload_json String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (event_id);

CREATE TABLE IF NOT EXISTS order_analytics
(
    event_id UUID,
    order_id UUID,
    event_type LowCardinality(String),
    status LowCardinality(String),
    previous_status LowCardinality(String),
    total_amount_minor UInt64,
    currency LowCardinality(String),
    product_ids Array(UUID),
    product_names Array(String),
    item_quantities Array(UInt32),
    predicted_eta_seconds Nullable(UInt32),
    actual_eta_seconds Nullable(UInt32),
    occurred_at DateTime64(3, 'UTC'),
    correlation_id String,
    trace_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (event_id);
