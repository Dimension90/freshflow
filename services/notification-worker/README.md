# notification-worker

Consumes order events with manual Kafka offset commits and records mock notifications without external email/SMS providers. `notifications.processed_events` and `notification_log` are written atomically, so redelivery cannot create a duplicate side effect.
