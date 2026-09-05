# PostgreSQL migrations

Versioned `golang-migrate` up/down scripts create service schemas, domain tables, outboxes, consumer inboxes and ETA evaluation data. Compose runs `postgres-migrate` once PostgreSQL is healthy and blocks domain services until migrations complete.
