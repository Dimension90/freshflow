ALTER TABLE delivery.deliveries
    ADD COLUMN simulation_delay_seconds integer NOT NULL DEFAULT 0
    CHECK (simulation_delay_seconds BETWEEN 0 AND 600);
