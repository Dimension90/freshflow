# courier-simulator

Produces deterministic courier movement without an external map API. It publishes one versioned location event per courier per second, stores latest coordinates/state in Redis, and advances active assignments through assembling, delivering and completed phases in roughly 15 seconds.
