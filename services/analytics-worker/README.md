# analytics-worker

Consumer group `freshflow.analytics-worker.v1` читает все доменные topics и строит в ClickHouse две проекции: поток логистических событий и денормализованные факты заказов. Повторная доставка безопасна: строки имеют стабильный `event_id`, таблицы используют `ReplacingMergeTree`, а все dashboard-запросы читают `FINAL`.

HTTP API на `:8087`:

- `GET /internal/v1/analytics/summary` — единый snapshot dashboard;
- `GET /internal/v1/analytics/orders-by-hour`;
- `GET /internal/v1/analytics/eta`;
- `GET /internal/v1/analytics/on-time`;
- `GET /internal/v1/analytics/cancellations`;
- `GET /internal/v1/analytics/popular-products`;
- `GET /internal/v1/analytics/status-durations`.

SQL для самостоятельного исследования лежит в `db/clickhouse/queries`.
