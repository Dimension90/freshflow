DROP TABLE delivery.outbox;
DROP TABLE delivery.processed_events;
DROP TABLE delivery.deliveries;
DROP TABLE delivery.couriers;
DROP TABLE orders.processed_events;
DROP TABLE orders.order_status_history;
ALTER TABLE orders.orders DROP COLUMN delivery_longitude, DROP COLUMN delivery_latitude;
ALTER TABLE orders.users DROP COLUMN delivery_longitude, DROP COLUMN delivery_latitude;

