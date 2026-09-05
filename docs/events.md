# Kafka event contracts

## Delivery semantics

FreshFlow использует at-least-once delivery. Producer-ы записывают domain change и outbox row в одной PostgreSQL-транзакции. Relay может публиковать событие повторно, поэтому каждый consumer обязан дедуплицировать `event_id` до применения side effect.

Реализация этапа 4 хранит полный JSON envelope в `catalog.outbox` и `orders.outbox`. Relay передаёт `event_id`, `event_type`, `correlation_id` и, когда доступен, `trace_id` также в Kafka headers. `notification-worker` фиксирует inbox marker и mock notification одной PostgreSQL-транзакцией, после чего вручную commit-ит Kafka offset.

Порядок гарантируется только внутри aggregate:

| Topic | Partition key |
|---|---|
| `freshflow.order.events.v1` | `order_id` |
| `freshflow.inventory.events.v1` | `reservation_id` |
| `freshflow.delivery.events.v1` | `order_id` |
| `freshflow.courier.location.v1` | `courier_id` |

## Envelope v1

```json
{
  "event_id": "018f7f62-dbc4-7a15-b41f-5dbb23c99f42",
  "event_type": "order.created",
  "event_version": 1,
  "occurred_at": "2026-09-05T12:00:00Z",
  "producer": "order-service",
  "correlation_id": "01J6Y7A8B9C0D1E2F3G4H5J6K7",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "aggregate_id": "018f7f62-dbc4-7a15-b41f-5dbb23c99f40",
  "payload": {}
}
```

Правила:

- неизвестные поля игнорируются;
- обязательные поля не меняют смысл внутри версии;
- breaking change создаёт `event_version: 2` и период совместного чтения;
- `occurred_at` всегда UTC RFC 3339;
- sensitive personal data в events не публикуется;
- `traceparent` передаётся Kafka header по W3C Trace Context; `trace_id`/`span_id` в envelope позволяют восстановить parent после хранения события в outbox и упрощают offline correlation.

## Event catalog

### `order.created`

Producer: order-service. Consumer-ы: delivery-service, analytics-worker, notification-worker.

Payload: `order_id`, `user_id`, `reservation_id`, `currency`, `total_amount_minor`, `item_count`, массив snapshot-позиций (`product_id`, `product_name`, `quantity`, `unit_price_minor`, `currency`), `created_at`.

### `order.confirmed`

Producer: order-service. Consumer-ы: analytics-worker, notification-worker.

Payload: `order_id`, `confirmed_at`.

### `order.status_changed`

Producer: order-service. Consumer-ы: delivery-service, analytics-worker, notification-worker.

Payload: `order_id`, `previous_status`, `new_status`, `changed_at`, `reason` nullable.

### `inventory.reserved`

Producer: catalog-service. Consumer-ы: analytics-worker, order-service reconciliation.

Payload: `reservation_id`, `checkout_attempt_id`, позиции с `product_id` и `quantity`, `expires_at`.

### `inventory.reservation_failed`

Producer: catalog-service. Consumer-ы: analytics-worker, order-service reconciliation.

Payload: `checkout_attempt_id`, массив shortages (`product_id`, `requested`, `available`), `failed_at`.

### `delivery.assigned`

Producer: delivery-service. Consumer-ы: order-service, analytics-worker, notification-worker.

Payload: `delivery_id`, `order_id`, `courier_id`, `assigned_at`. Прогноз сохраняется асинхронно после назначения, чтобы HTTP inference не удерживал PostgreSQL lock.

### `delivery.status_changed`

Producer: delivery-service. Consumer-ы: order-service, analytics-worker.

Payload: `delivery_id`, `order_id`, `courier_id`, `status` (`assembling` или `delivering`), `changed_at`.

### `courier.location_updated`

Producer: courier-simulator (в будущем courier app). Consumer-ы: delivery-service и analytics-worker.

Payload: `courier_id`, `delivery_id` nullable, `latitude`, `longitude`, `heading_degrees`, `speed_mps`, `recorded_at`, `sequence`.

События могут быть sampled/throttled для аналитики, но Redis всегда содержит последнюю принятую позицию. `sequence` подавляет запоздавшие обновления одного курьера.

### `delivery.completed`

Producer: delivery-service. Consumer-ы: order-service, analytics-worker, notification-worker.

Payload: `delivery_id`, `order_id`, `courier_id`, `status`, `assigned_at`, `completed_at`, `actual_eta_seconds`; при наличии начального прогноза также `predicted_eta_seconds`, `eta_model_version` и `on_time`.

## Retry and DLQ

Consumer обрабатывает временные ошибки с bounded exponential backoff и jitter. После исчерпания попыток исходный envelope и sanitized error metadata публикуются в `<source-topic>.dlq`. Offset исходного сообщения фиксируется только после успешной обработки или успешной записи в DLQ.

DLQ — диагностический механизм, а не автоматический источник повторной публикации. Возврат сообщений выполняется отдельной явной командой после устранения причины.
