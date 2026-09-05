# Load testing

`k6/checkout.js` combines concurrent read traffic with a single serial checkout
flow. The separate checkout VU deliberately avoids cart-version conflicts: the
scenario measures the normal reservation/order path, while read VUs stress
gateway routing, ClickHouse analytics and the dispatcher snapshot.

Run it against an already started local stack:

```powershell
$env:FRESHFLOW_LOAD_DURATION = '30s'
$env:FRESHFLOW_LOAD_READ_VUS = '8'
docker compose --profile load run --rm k6
```

The Compose profile connects to `api-gateway` inside the Docker network; no
host-specific address is required. Useful output includes HTTP p95, request
failure rate, `freshflow_checkout_orders_created` and
`freshflow_checkout_failure_rate`. The default thresholds fail the run when
HTTP failures reach 3%, checkout failures reach 5%, or HTTP p95 exceeds one
second. Treat this as a reproducible local regression check, not a production
capacity claim.
