# api-gateway

Public REST/SSE edge for FreshFlow. The stage 2 implementation provides:

- `GET /healthz` for liveness;
- `GET /readyz` with concurrent PostgreSQL, Redis and Kafka checks;
- `GET /api/v1` as the versioned API root;
- validated/generated `X-Correlation-ID` propagation;
- structured JSON access and lifecycle logs;
- bounded HTTP timeouts and graceful shutdown;
- a uniform JSON error foundation.

Domain routing, rate limiting, OpenAPI UI and SSE are added alongside their owning stages.
