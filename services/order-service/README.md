# order-service

Owns idempotent checkout and order state. An advisory lock serializes concurrent requests for the same key; the request hash rejects key reuse with another payload. Catalog reservations are compensated if order persistence fails. Order, idempotency response and `order.created` outbox envelope commit in one transaction. Created and confirmed orders can be cancelled: the order row remains locked while the reservation is released and `order.cancelled` is saved to the outbox. It also exposes an SSE stream of authoritative order state changes.
