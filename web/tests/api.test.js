import test from "node:test";
import assert from "node:assert/strict";
import { api, APIError } from "../api.js";

test("checkout sends cart version and idempotency key", async () => {
  const originalFetch = globalThis.fetch;
  let captured;
  globalThis.fetch = async (path, options) => {
    captured = { path, options };
    return new Response(JSON.stringify({ id: "order-1", status: "created" }), {
      status: 201,
      headers: { "Content-Type": "application/json" },
    });
  };
  try {
    const order = await api.checkout("user-1", 7, "checkout-1");
    assert.equal(order.id, "order-1");
    assert.equal(captured.path, "/api/v1/orders");
    assert.equal(captured.options.headers["Idempotency-Key"], "checkout-1");
    assert.deepEqual(JSON.parse(captured.options.body), { user_id: "user-1", cart_version: 7 });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("API error preserves backend code and correlation ID", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(JSON.stringify({
    error: { code: "inventory_insufficient", message: "not enough stock", correlation_id: "corr-42" },
  }), { status: 409, headers: { "Content-Type": "application/json" } });
  try {
    await assert.rejects(
      () => api.catalog(),
      (error) => error instanceof APIError
        && error.status === 409
        && error.code === "inventory_insufficient"
        && error.correlationID === "corr-42",
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});
