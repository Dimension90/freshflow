# FreshFlow web

Dependency-free ES module SPA served by nginx. It talks only to the public API gateway:

- catalog and durable cart;
- idempotent checkout;
- live order status and courier-position updates through Server-Sent Events;
- cancellation of created or confirmed orders;
- dispatcher screen with courier fleet, active assignments and demo delivery controls;
- ClickHouse analytics dashboard.

The Compose URL is `http://localhost:8089`. For a static-only preview run
`python -m http.server 4173 -d web`; API calls intentionally remain unavailable
unless the page is served through nginx or another same-origin reverse proxy.

Run the frontend unit tests with:

```bash
node --test web/tests/*.test.js
```

The UI does not keep a shadow cart. PostgreSQL/Redis-backed cart responses remain
the source of truth. Only the demo user ID and last opened order ID are stored in
the browser.
