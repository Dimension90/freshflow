# Integration tests

`checkout_test.go` exercises gateway routing, catalog, durable cart updates, transactional inventory reservation, order creation, response replay and idempotency-key conflict. It then observes correlated `inventory.reserved` and `order.created` Kafka events, republishes the same order envelope and proves that notification-worker records exactly one side effect.

The same scenario waits for Redis GEO assignment and the simulated `confirmed → assembling → delivering → delivered` lifecycle, then verifies the completed delivery snapshot.

```powershell
$env:FRESHFLOW_INTEGRATION = '1'
go test ./tests/integration -count=1
```
