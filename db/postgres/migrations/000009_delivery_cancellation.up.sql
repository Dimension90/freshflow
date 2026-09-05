ALTER TABLE delivery.deliveries
    DROP CONSTRAINT deliveries_status_check;

ALTER TABLE delivery.deliveries
    ADD CONSTRAINT deliveries_status_check
    CHECK (status IN ('assigned', 'assembling', 'delivering', 'completed', 'cancelled'));

DROP INDEX delivery.deliveries_active_courier_idx;
CREATE INDEX deliveries_active_courier_idx
    ON delivery.deliveries (courier_id)
    WHERE status NOT IN ('completed', 'cancelled');
