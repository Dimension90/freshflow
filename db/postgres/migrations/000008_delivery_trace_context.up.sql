ALTER TABLE delivery.deliveries
    ADD COLUMN trace_id text NOT NULL DEFAULT '',
    ADD COLUMN origin_span_id text NOT NULL DEFAULT '';
