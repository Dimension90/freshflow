import http from "k6/http";
import { check, group, sleep } from "k6";
import { Counter, Rate } from "k6/metrics";

const baseURL = (__ENV.BASE_URL || "http://api-gateway:8080").replace(/\/$/, "");
const userID = __ENV.FRESHFLOW_DEMO_USER_ID || "00000000-0000-4000-8000-000000000001";
const productID = __ENV.FRESHFLOW_PRODUCT_ID || "10000000-0000-4000-8000-000000000001";
// K6_* is reserved by k6 for its own CLI configuration. Using it here would
// replace the scenarios below with k6's legacy default executor.
const duration = __ENV.FRESHFLOW_LOAD_DURATION || "30s";
const readVUs = Number(__ENV.FRESHFLOW_LOAD_READ_VUS || 8);

const createdOrders = new Counter("freshflow_checkout_orders_created");
const checkoutFailures = new Rate("freshflow_checkout_failure_rate");

export const options = {
  discardResponseBodies: false,
  thresholds: {
    http_req_failed: ["rate<0.03"],
    http_req_duration: ["p(95)<1000"],
    freshflow_checkout_failure_rate: ["rate<0.05"],
  },
  scenarios: {
    browse_catalog: {
      executor: "constant-vus",
      exec: "browse",
      vus: readVUs,
      duration,
    },
    checkout_flow: {
      executor: "constant-vus",
      exec: "checkout",
      vus: 1,
      duration,
      startTime: "1s",
    },
  },
};

function correlationID(prefix) {
  return `${prefix}-${__VU}-${__ITER}-${Date.now()}`;
}

function jsonParams(prefix) {
  return {
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Correlation-ID": correlationID(prefix),
    },
    tags: { endpoint: prefix },
  };
}

export function browse() {
  group("browse", () => {
    const response = http.batch([
      ["GET", `${baseURL}/api/v1/catalog/products`, null, { headers: { Accept: "application/json" }, tags: { endpoint: "catalog" } }],
      ["GET", `${baseURL}/api/v1/analytics/summary`, null, { headers: { Accept: "application/json" }, tags: { endpoint: "analytics" } }],
      ["GET", `${baseURL}/api/v1/operations/couriers`, null, { headers: { Accept: "application/json" }, tags: { endpoint: "operations" } }],
    ]);
    check(response[0], { "catalog is successful": (item) => item.status === 200 });
    check(response[1], { "analytics is successful": (item) => item.status === 200 });
    check(response[2], { "operations is successful": (item) => item.status === 200 });
  });
  sleep(0.4);
}

export function checkout() {
  group("checkout", () => {
    const cartResponse = http.put(
      `${baseURL}/api/v1/carts/${userID}/items/${productID}`,
      JSON.stringify({ quantity: 1 }),
      jsonParams("cart-update"),
    );
    if (!check(cartResponse, { "cart update is successful": (item) => item.status === 200 })) {
      checkoutFailures.add(1);
      return;
    }
    const cart = cartResponse.json();
    const idempotencyKey = `k6-${__VU}-${__ITER}-${Date.now()}`;
    const params = jsonParams("checkout");
    params.headers["Idempotency-Key"] = idempotencyKey;
    const orderResponse = http.post(
      `${baseURL}/api/v1/orders`,
      JSON.stringify({ user_id: userID, cart_version: cart.version }),
      params,
    );
    const created = check(orderResponse, { "order is created": (item) => item.status === 201 });
    checkoutFailures.add(!created);
    if (created) createdOrders.add(1);
  });
  // One checkout VU prevents cart-version contention from obscuring latency
  // while browse traffic still exercises concurrent gateway reads.
  sleep(2);
}
