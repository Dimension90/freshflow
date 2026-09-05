# delivery-service

Owns couriers, assignments and delivery state. It consumes `order.created`, `order.cancelled` and courier locations with inbox deduplication, searches Redis GEO for nearby candidates, locks the final courier in PostgreSQL and publishes delivery events through its transactional outbox. Cancellation releases an assigned courier; the public stream endpoint provides pending/position snapshots via SSE.

Отдельный recovery worker на каждой стадии находит доставки без прогноза, вызывает ETA service вне OLTP-транзакции и атомарно сохраняет feature vector и prediction. При завершении он связывает все прогнозы доставки с фактическим оставшимся временем. Public snapshot включает текущие координаты, последний ETA и версию модели.
