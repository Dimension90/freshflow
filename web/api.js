const DEFAULT_TIMEOUT_MS = 10000;

export class APIError extends Error {
  constructor(message, { status = 0, code = "request_failed", correlationID = "" } = {}) {
    super(message);
    this.name = "APIError";
    this.status = status;
    this.code = code;
    this.correlationID = correlationID;
  }
}

async function request(path, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeout ?? DEFAULT_TIMEOUT_MS);
  try {
    const response = await fetch(path, {
      ...options,
      headers: {
        Accept: "application/json",
        ...(options.body ? { "Content-Type": "application/json" } : {}),
        ...options.headers,
      },
      signal: controller.signal,
    });
    const body = response.status === 204 ? null : await response.json().catch(() => null);
    if (!response.ok) {
      const apiError = body?.error;
      throw new APIError(apiError?.message || `HTTP ${response.status}`, {
        status: response.status,
        code: apiError?.code,
        correlationID: apiError?.correlation_id || response.headers.get("X-Correlation-ID") || "",
      });
    }
    return body;
  } catch (error) {
    if (error instanceof APIError) throw error;
    if (error.name === "AbortError") throw new APIError("Сервис не ответил вовремя", { code: "timeout" });
    throw new APIError("Не удалось связаться с FreshFlow API", { code: "network_error" });
  } finally {
    clearTimeout(timer);
  }
}

export const api = {
  readiness: () => request("/readyz", { timeout: 3000 }),
  catalog: () => request("/api/v1/catalog/products"),
  cart: (userID) => request(`/api/v1/carts/${encodeURIComponent(userID)}`),
  setCartItem: (userID, productID, quantity) => request(
    `/api/v1/carts/${encodeURIComponent(userID)}/items/${encodeURIComponent(productID)}`,
    { method: "PUT", body: JSON.stringify({ quantity }) },
  ),
  removeCartItem: (userID, productID) => request(
    `/api/v1/carts/${encodeURIComponent(userID)}/items/${encodeURIComponent(productID)}`,
    { method: "DELETE" },
  ),
  checkout: (userID, cartVersion, idempotencyKey) => request("/api/v1/orders", {
    method: "POST",
    headers: { "Idempotency-Key": idempotencyKey },
    body: JSON.stringify({ user_id: userID, cart_version: cartVersion }),
  }),
  order: (orderID) => request(`/api/v1/orders/${encodeURIComponent(orderID)}`),
  cancelOrder: (orderID) => request(`/api/v1/orders/${encodeURIComponent(orderID)}`, { method: "DELETE" }),
  delivery: (orderID) => request(`/api/v1/deliveries/order/${encodeURIComponent(orderID)}`),
  analytics: () => request("/api/v1/analytics/summary"),
  couriers: () => request("/api/v1/operations/couriers"),
  deliveryAction: (deliveryID, action) => request(`/api/v1/operations/deliveries/${encodeURIComponent(deliveryID)}/actions`, {
    method: "POST",
    body: JSON.stringify({ action }),
  }),
};
