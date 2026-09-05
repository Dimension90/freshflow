DROP TABLE delivery.eta_predictions;
ALTER TABLE delivery.deliveries
    DROP COLUMN eta_updated_at,
    DROP COLUMN eta_model_version,
    DROP COLUMN predicted_eta_seconds,
    DROP COLUMN item_count;
