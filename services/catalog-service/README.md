# catalog-service

Owns products, prices, inventory and durable reservations. PostgreSQL row locks serialize stock changes; Redis caches the catalog and indexes reservation expiry without becoming the source of truth. Reservation result and inventory event are committed atomically through `catalog.outbox`.
