import test from "node:test";
import assert from "node:assert/strict";
import { courierPosition, duration, money, percentage, shortID } from "../format.js";

test("formats domain values for the Russian UI", () => {
  assert.match(money(24900), /249/);
  assert.equal(duration(90), "2 мин");
  assert.equal(duration(3660), "1 ч 1 мин");
  assert.equal(percentage(0.875), "88%");
  assert.equal(shortID("12345678-abcd"), "12345678");
});

test("courier marker always stays inside the simulated map", () => {
  const position = courierPosition({
    pickup_latitude: 55,
    pickup_longitude: 82,
    destination_latitude: 56,
    destination_longitude: 83,
    courier_latitude: 100,
    courier_longitude: 100,
  });
  assert.ok(position.x >= 8 && position.x <= 92);
  assert.ok(position.y >= 8 && position.y <= 92);
});
