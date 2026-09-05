# cart-service

Owns durable versioned carts in PostgreSQL with a Redis hot cache. Mutations commit to PostgreSQL before invalidating cached snapshots.
