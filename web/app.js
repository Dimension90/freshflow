import { api, APIError } from "./api.js";
import { courierPosition, duration, money, ORDER_STEPS, percentage, shortID, STATUS_LABELS } from "./format.js";

const DEMO_USER_ID = "00000000-0000-4000-8000-000000000001";
const state = {
  products: [],
  cart: { user_id: DEMO_USER_ID, version: 0, items: [] },
  order: null,
  delivery: null,
  analytics: null,
	operations: null,
	operationBusy: new Set(),
  busyProducts: new Set(),
  checkoutBusy: false,
  orderStream: null,
  deliveryStream: null,
  cancelBusy: false,
};

const elements = {
  views: [...document.querySelectorAll("[data-view]")],
  nav: [...document.querySelectorAll("[data-nav]")],
  serviceState: document.querySelector("#service-state"),
  productGrid: document.querySelector("#product-grid"),
  catalogNotice: document.querySelector("#catalog-notice"),
  cartPanel: document.querySelector("#cart-panel"),
  cartContent: document.querySelector("#cart-content"),
  cartCount: document.querySelector("#cart-count"),
  cartToggle: document.querySelector("#cart-toggle"),
  closeCart: document.querySelector("#close-cart"),
  mobileScrim: document.querySelector("#mobile-scrim"),
  orderContent: document.querySelector("#order-content"),
  orderNotice: document.querySelector("#order-notice"),
  analyticsContent: document.querySelector("#analytics-content"),
  analyticsNotice: document.querySelector("#analytics-notice"),
	operationsContent: document.querySelector("#operations-content"),
	operationsNotice: document.querySelector("#operations-notice"),
  toastRegion: document.querySelector("#toast-region"),
};

const productVisuals = {
  "APL-GALA-1KG": ["🍎", "#ec5a45"],
  "MLK-32-1L": ["🥛", "#70a6d8"],
  "BRD-WHT-400": ["🥖", "#d9954f"],
  "CHS-GOU-200": ["🧀", "#eab92d"],
  "TMT-PNK-600": ["🍅", "#df5c4c"],
};

function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setNotice(element, message = "", kind = "error") {
  element.textContent = message;
  element.className = message ? `notice ${kind}` : "notice hidden";
}

function readableError(error) {
  if (!(error instanceof APIError)) return "Что-то пошло не так";
  const suffix = error.correlationID ? ` Код обращения: ${error.correlationID}` : "";
  return `${error.message}.${suffix}`;
}

function toast(message, kind = "success") {
  const item = document.createElement("div");
  item.className = `toast ${kind}`;
  item.textContent = message;
  elements.toastRegion.append(item);
  window.setTimeout(() => item.remove(), 3200);
}

function quantityFor(productID) {
  return state.cart.items.find((item) => item.product_id === productID)?.quantity ?? 0;
}

function cartDetails() {
  return state.cart.items.map((item) => ({
    ...item,
    product: state.products.find((product) => product.id === item.product_id),
  }));
}

function cartTotal() {
  return cartDetails().reduce((sum, item) => sum + (item.product?.price_minor ?? 0) * item.quantity, 0);
}

function renderCatalogLoading() {
  elements.productGrid.innerHTML = Array.from({ length: 5 }, () => `
    <article class="product-card skeleton-card" aria-hidden="true">
      <div class="skeleton visual"></div><div class="skeleton line wide"></div><div class="skeleton line"></div>
    </article>
  `).join("");
}

function renderCatalog() {
  if (!state.products.length) {
    elements.productGrid.innerHTML = `<div class="empty-state"><span>◌</span><h3>Каталог пока пуст</h3><p>Попробуйте обновить страницу через несколько секунд.</p></div>`;
    return;
  }
  elements.productGrid.innerHTML = state.products.map((product) => {
    const quantity = quantityFor(product.id);
    const [emoji, color] = productVisuals[product.sku] ?? ["🥬", "#65a96b"];
    const soldOut = product.available_quantity === 0;
    const busy = state.busyProducts.has(product.id);
    return `
      <article class="product-card">
        <div class="product-visual" style="--product-color:${color}">
          <span aria-hidden="true">${emoji}</span>
          <small>${soldOut ? "Нет в наличии" : `Осталось ${product.available_quantity}`}</small>
        </div>
        <div class="product-copy">
          <span class="sku">${escapeHTML(product.sku)}</span>
          <h3>${escapeHTML(product.name)}</h3>
          <p>${escapeHTML(product.description)}</p>
        </div>
        <div class="product-footer">
          <strong>${money(product.price_minor, product.currency)}</strong>
          ${quantity > 0 ? `
            <div class="stepper" aria-label="Количество ${escapeHTML(product.name)}">
              <button type="button" data-cart-action="decrement" data-product-id="${product.id}" aria-label="Уменьшить" ${busy ? "disabled" : ""}>−</button>
              <span>${quantity}</span>
              <button type="button" data-cart-action="increment" data-product-id="${product.id}" aria-label="Увеличить" ${busy || quantity >= product.available_quantity ? "disabled" : ""}>+</button>
            </div>` : `
            <button class="add-button" type="button" data-cart-action="increment" data-product-id="${product.id}" ${soldOut || busy ? "disabled" : ""}>
              ${busy ? "…" : "+ В корзину"}
            </button>`}
        </div>
      </article>`;
  }).join("");
}

function renderCart() {
  const details = cartDetails();
  const itemCount = details.reduce((sum, item) => sum + item.quantity, 0);
  elements.cartCount.textContent = String(itemCount);
  if (!details.length) {
    elements.cartContent.innerHTML = `
      <div class="cart-empty">
        <div class="empty-orbit" aria-hidden="true">↗</div>
        <h3>Корзина пуста</h3>
        <p>Добавьте продукты из каталога — остатки проверятся при оформлении.</p>
      </div>`;
    return;
  }
  elements.cartContent.innerHTML = `
    <div class="cart-items">
      ${details.map((item) => {
        const [emoji] = productVisuals[item.product?.sku] ?? ["🥬"];
        return `<div class="cart-item">
          <span class="cart-item-icon" aria-hidden="true">${emoji}</span>
          <div class="cart-item-copy">
            <strong>${escapeHTML(item.product?.name ?? "Товар")}</strong>
            <small>${money(item.product?.price_minor ?? 0, item.product?.currency)}</small>
          </div>
          <div class="stepper compact">
            <button type="button" data-cart-action="decrement" data-product-id="${item.product_id}" aria-label="Уменьшить">−</button>
            <span>${item.quantity}</span>
            <button type="button" data-cart-action="increment" data-product-id="${item.product_id}" aria-label="Увеличить">+</button>
          </div>
        </div>`;
      }).join("")}
    </div>
    <div class="cart-summary">
      <div><span>Товары</span><strong>${money(cartTotal())}</strong></div>
      <div><span>Доставка</span><strong class="accent-text">Бесплатно</strong></div>
      <div class="total-row"><span>Итого</span><strong>${money(cartTotal())}</strong></div>
      <button class="primary-button" id="checkout-button" type="button" ${state.checkoutBusy ? "disabled" : ""}>
        ${state.checkoutBusy ? "Резервируем товары…" : "Оформить заказ"}
      </button>
      <p class="cart-footnote">Товары резервируются транзакционно. Повторный клик безопасен.</p>
    </div>`;
  document.querySelector("#checkout-button")?.addEventListener("click", checkout);
}

async function loadShop() {
  renderCatalogLoading();
  setNotice(elements.catalogNotice);
  try {
    const [catalog, cart] = await Promise.all([api.catalog(), api.cart(DEMO_USER_ID)]);
    state.products = catalog.products ?? [];
    state.cart = cart;
    renderCatalog();
    renderCart();
  } catch (error) {
    setNotice(elements.catalogNotice, readableError(error));
    state.products = [];
    renderCatalog();
    renderCart();
  }
}

async function updateQuantity(productID, delta) {
  if (state.busyProducts.has(productID)) return;
  const next = quantityFor(productID) + delta;
  state.busyProducts.add(productID);
  renderCatalog();
  try {
    state.cart = next <= 0
      ? await api.removeCartItem(DEMO_USER_ID, productID)
      : await api.setCartItem(DEMO_USER_ID, productID, next);
    setNotice(elements.catalogNotice);
    renderCart();
  } catch (error) {
    toast(readableError(error), "error");
  } finally {
    state.busyProducts.delete(productID);
    renderCatalog();
  }
}

async function checkout() {
  if (state.checkoutBusy || !state.cart.items.length) return;
  state.checkoutBusy = true;
  renderCart();
  try {
    const idempotencyKey = `web-${crypto.randomUUID()}`;
    const order = await api.checkout(DEMO_USER_ID, state.cart.version, idempotencyKey);
    state.order = order;
    localStorage.setItem("freshflow.lastOrderID", order.id);
    closeCart();
    toast("Заказ создан — начинаем отслеживание");
    window.location.hash = `order/${order.id}`;
  } catch (error) {
    toast(readableError(error), "error");
    if (error.status === 409) await loadShop();
  } finally {
    state.checkoutBusy = false;
    renderCart();
  }
}

function orderEmptyMarkup() {
  return `<div class="empty-page">
    <div class="empty-orbit" aria-hidden="true">◎</div>
    <h2>Нет активного заказа</h2>
    <p>Оформите корзину, и здесь появятся этапы сборки, ETA и положение курьера.</p>
    <a class="primary-link" href="#catalog">Перейти в каталог</a>
  </div>`;
}

function renderOrder() {
  if (!state.order) {
    elements.orderContent.innerHTML = orderEmptyMarkup();
    return;
  }
  const currentIndex = ORDER_STEPS.indexOf(state.order.status);
  const position = courierPosition(state.delivery);
  const eta = state.delivery?.predicted_eta_seconds;
  const delivered = state.order.status === "delivered";
  const cancelled = state.order.status === "cancelled";
  const canCancel = state.order.status === "created" || state.order.status === "confirmed";
  elements.orderContent.innerHTML = `
    <div class="order-hero-card">
      <div>
        <span class="order-number">Заказ #${shortID(state.order.id)}</span>
        <h2>${delivered ? "Заказ доставлен" : cancelled ? "Заказ отменён" : STATUS_LABELS[state.order.status] ?? state.order.status}</h2>
        <p>${delivered ? "Спасибо! Фактическое время попадёт в оценку ETA-модели." : cancelled ? "Резерв товаров снят, а назначенный курьер снова доступен." : eta ? `Ожидаемое время — около ${duration(eta)}.` : "Скоро назначим курьера и рассчитаем ETA."}</p>
		${canCancel ? `<button class="cancel-button" type="button" data-order-action="cancel" ${state.cancelBusy ? "disabled" : ""}>${state.cancelBusy ? "Отменяем…" : "Отменить заказ"}</button>` : ""}
      </div>
      <div class="eta-badge"><small>${delivered ? "Статус" : "ETA"}</small><strong>${delivered ? "Готово" : duration(eta)}</strong></div>
    </div>
    <ol class="timeline">
      ${ORDER_STEPS.map((step, index) => `<li class="${index < currentIndex ? "done" : index === currentIndex ? "active" : ""}">
        <span>${index < currentIndex ? "✓" : index + 1}</span><strong>${STATUS_LABELS[step]}</strong>
      </li>`).join("")}
    </ol>
    <div class="tracking-grid">
      <section class="map-card" aria-label="Симуляция положения курьера">
        <div class="map-lines" aria-hidden="true"></div>
        <span class="map-point store" style="left:15%;top:72%"><i>F</i><small>Склад</small></span>
        <span class="map-point destination" style="left:84%;top:20%"><i>⌂</i><small>Адрес</small></span>
        ${state.delivery ? `<span class="courier-marker" style="left:${position.x}%;top:${position.y}%" aria-label="Текущее положение курьера"><i>➤</i></span>` : ""}
        <div class="map-caption"><span><i class="live-dot"></i> Координаты обновляются</span><small>Без внешнего map API</small></div>
      </section>
      <aside class="order-details">
        <span class="eyebrow dark">Детали</span>
        <dl>
          <div><dt>Сумма</dt><dd>${money(state.order.total_amount_minor, state.order.currency)}</dd></div>
          <div><dt>Позиций</dt><dd>${state.order.items.reduce((sum, item) => sum + item.quantity, 0)}</dd></div>
          <div><dt>Курьер</dt><dd>${state.delivery ? escapeHTML(state.delivery.courier_name || `#${shortID(state.delivery.courier_id)}`) : "Назначается"}</dd></div>
          <div><dt>Модель ETA</dt><dd>${escapeHTML(state.delivery?.eta_model_version ?? "Ожидается")}</dd></div>
        </dl>
        <div class="order-products">
          ${state.order.items.map((item) => `<div><span>${escapeHTML(item.product_name)} × ${item.quantity}</span><strong>${money(item.unit_price_minor * item.quantity, item.currency)}</strong></div>`).join("")}
        </div>
      </aside>
    </div>`;
}

async function loadOrder(orderID, { quiet = false } = {}) {
  const resolvedID = orderID || localStorage.getItem("freshflow.lastOrderID");
  if (!resolvedID) {
    state.order = null;
    state.delivery = null;
    renderOrder();
    return;
  }
  if (!quiet) setNotice(elements.orderNotice, "Обновляем состояние…", "info");
  try {
    state.order = await api.order(resolvedID);
    try {
      state.delivery = await api.delivery(resolvedID);
    } catch (error) {
      if (error.status !== 404) throw error;
      state.delivery = null;
    }
    setNotice(elements.orderNotice);
    renderOrder();
    if (state.order.status === "delivered" || state.order.status === "cancelled") closeLiveStreams();
  } catch (error) {
    setNotice(elements.orderNotice, readableError(error));
    if (!quiet) renderOrder();
  }
}

function closeLiveStreams() {
  state.orderStream?.close();
  state.deliveryStream?.close();
  state.orderStream = null;
  state.deliveryStream = null;
}

function startOrderLive(orderID) {
  closeLiveStreams();
  const resolvedID = orderID || localStorage.getItem("freshflow.lastOrderID");
  if (!resolvedID) {
    loadOrder();
    return;
  }
  loadOrder(resolvedID);
  const encodedID = encodeURIComponent(resolvedID);
  state.orderStream = new EventSource(`/api/v1/orders/${encodedID}/stream`);
  state.orderStream.addEventListener("order", async (event) => {
    state.order = JSON.parse(event.data);
    try {
      state.delivery = await api.delivery(resolvedID);
    } catch (error) {
      if (error.status !== 404) setNotice(elements.orderNotice, readableError(error));
    }
    renderOrder();
    if (state.order.status === "delivered" || state.order.status === "cancelled") closeLiveStreams();
  });
  state.orderStream.onerror = () => {
    if (state.order?.status !== "delivered" && state.order?.status !== "cancelled") {
      setNotice(elements.orderNotice, "Live-соединение переподключается…", "info");
    }
  };
  state.deliveryStream = new EventSource(`/api/v1/deliveries/order/${encodedID}/stream`);
  state.deliveryStream.addEventListener("delivery", (event) => {
    state.delivery = JSON.parse(event.data);
    renderOrder();
    if (state.delivery.status === "completed" || state.delivery.status === "cancelled") state.deliveryStream?.close();
  });
}

async function cancelCurrentOrder() {
  if (!state.order || state.cancelBusy) return;
  if (!window.confirm("Отменить заказ? Резерв товаров будет снят.")) return;
  state.cancelBusy = true;
  renderOrder();
  try {
    state.order = await api.cancelOrder(state.order.id);
    toast("Заказ отменён, резерв товаров освобождён");
    renderOrder();
  } catch (error) {
    toast(readableError(error), "error");
  } finally {
    state.cancelBusy = false;
    renderOrder();
  }
}

function metricCard(label, value, detail) {
  return `<article class="metric-card"><span>${label}</span><strong>${value}</strong><small>${detail}</small></article>`;
}

function renderAnalytics() {
  const value = state.analytics;
  if (!value) {
    elements.analyticsContent.innerHTML = `<div class="empty-page"><div class="empty-orbit">↗</div><h2>Нет аналитических данных</h2><p>Создайте и доставьте хотя бы один заказ.</p></div>`;
    return;
  }
  const hours = value.orders_by_hour ?? [];
  const maxOrders = Math.max(1, ...hours.map((item) => item.orders));
  const products = value.popular_products ?? [];
  const maxQuantity = Math.max(1, ...products.map((item) => item.quantity));
  elements.analyticsContent.innerHTML = `
    <div class="metric-grid">
      ${metricCard("Средний прогноз", duration(value.eta?.average_predicted_eta_seconds), "baseline ETA")}
      ${metricCard("Фактическое время", duration(value.eta?.average_actual_eta_seconds), "по завершённым доставкам")}
      ${metricCard("Доставлено вовремя", percentage(value.on_time_ratio), "actual ≤ predicted")}
      ${metricCard("Отмены", String(value.cancellations ?? 0), "за весь период")}
    </div>
    <div class="analytics-grid">
      <section class="chart-card wide-chart">
        <div class="card-heading"><div><span class="eyebrow dark">Поток заказов</span><h2>Заказы по часам</h2></div><span class="source-pill">ClickHouse</span></div>
        ${hours.length ? `<div class="bar-chart">${hours.slice(-12).map((item) => `<div class="bar-column"><span>${item.orders}</span><i style="height:${Math.max(8, (item.orders / maxOrders) * 100)}%"></i><small>${new Date(item.hour).toLocaleTimeString("ru-RU", { hour: "2-digit", minute: "2-digit" })}</small></div>`).join("")}</div>` : `<p class="chart-empty">Данные появятся после первого order.created.</p>`}
      </section>
      <section class="chart-card">
        <div class="card-heading"><div><span class="eyebrow dark">Спрос</span><h2>Популярные товары</h2></div></div>
        <div class="rank-list">${products.length ? products.map((item, index) => `<div class="rank-row"><span class="rank">${index + 1}</span><div><strong>${escapeHTML(item.product_name)}</strong><i><b style="width:${(item.quantity / maxQuantity) * 100}%"></b></i></div><em>${item.quantity}</em></div>`).join("") : `<p class="chart-empty">Пока нет заказанных товаров.</p>`}</div>
      </section>
      <section class="chart-card">
        <div class="card-heading"><div><span class="eyebrow dark">Процесс</span><h2>Время по статусам</h2></div></div>
        <div class="status-table">${(value.status_durations ?? []).length ? value.status_durations.map((item) => `<div><span><i class="status-dot"></i>${STATUS_LABELS[item.status] ?? escapeHTML(item.status)}</span><strong>${duration(item.average_duration_seconds)}</strong></div>`).join("") : `<p class="chart-empty">Нужен полный цикл доставки.</p>`}</div>
      </section>
    </div>`;
}

function renderOperations() {
  const couriers = state.operations?.couriers ?? [];
  if (!couriers.length) {
    elements.operationsContent.innerHTML = `<div class="empty-page"><div class="empty-orbit">⌁</div><h2>Курьеры не найдены</h2><p>Проверьте готовность delivery-service.</p></div>`;
    return;
  }
  const active = couriers.filter((courier) => courier.active_delivery_id).length;
  const available = couriers.filter((courier) => courier.status === "available").length;
  elements.operationsContent.innerHTML = `
    <div class="metric-grid operations-metrics">
      ${metricCard("Всего курьеров", String(couriers.length), "courier fleet")}
      ${metricCard("Свободны", String(available), "готовы к назначению")}
      ${metricCard("В работе", String(active), "активные доставки")}
      ${metricCard("Симуляция", "включена", "локальный dispatcher")}
    </div>
    <section class="operations-card">
      <div class="card-heading"><div><span class="eyebrow dark">Courier fleet</span><h2>Курьеры и назначения</h2></div><span class="source-pill">PostgreSQL + Redis GEO</span></div>
      <div class="courier-list">
        ${couriers.map((courier) => {
          const busy = state.operationBusy.has(courier.active_delivery_id);
          const activeDelivery = Boolean(courier.active_delivery_id);
          return `<article class="courier-row">
            <div class="courier-avatar">${escapeHTML(courier.name.slice(0, 1))}</div>
            <div class="courier-copy"><strong>${escapeHTML(courier.name)}</strong><span>${courier.status === "available" ? "Свободен" : `Заказ #${shortID(courier.active_order_id)}`}</span></div>
            <span class="courier-status ${escapeHTML(courier.status)}">${escapeHTML(STATUS_LABELS[courier.delivery_status] ?? (courier.status === "available" ? "Доступен" : "Назначен"))}</span>
            <div class="operation-actions">${activeDelivery ? `
              <button type="button" data-operation="delay" data-delivery-id="${courier.active_delivery_id}" ${busy ? "disabled" : ""}>+10 сек</button>
              <button type="button" class="complete-operation" data-operation="complete" data-delivery-id="${courier.active_delivery_id}" ${busy ? "disabled" : ""}>Завершить</button>` : `<span>Ожидает заказ</span>`}</div>
          </article>`;
        }).join("")}
      </div>
      <p class="operations-help">«+10 сек» удерживает курьера на текущей фазе симулятора и сдвигает прогноз. «Завершить» публикует обычное <code>delivery.completed</code> через outbox.</p>
    </section>`;
}

async function loadOperations() {
  setNotice(elements.operationsNotice, "Обновляем dispatcher…", "info");
  try {
    state.operations = await api.couriers();
    setNotice(elements.operationsNotice);
    renderOperations();
  } catch (error) {
    setNotice(elements.operationsNotice, readableError(error));
    state.operations = null;
    renderOperations();
  }
}

async function runDeliveryAction(deliveryID, action) {
  if (!deliveryID || state.operationBusy.has(deliveryID)) return;
  const confirmation = action === "complete" ? "Завершить доставку вручную? Это действие опубликует delivery.completed." : "Добавить 10 секунд задержки в симуляцию?";
  if (!window.confirm(confirmation)) return;
  state.operationBusy.add(deliveryID);
  renderOperations();
  try {
    await api.deliveryAction(deliveryID, action);
    toast(action === "complete" ? "Доставка завершена оператором" : "К симуляции добавлено 10 секунд");
    await loadOperations();
  } catch (error) {
    toast(readableError(error), "error");
  } finally {
    state.operationBusy.delete(deliveryID);
    renderOperations();
  }
}

async function loadAnalytics() {
  setNotice(elements.analyticsNotice, "Запрашиваем агрегаты…", "info");
  try {
    state.analytics = await api.analytics();
    setNotice(elements.analyticsNotice);
    renderAnalytics();
  } catch (error) {
    setNotice(elements.analyticsNotice, readableError(error));
    state.analytics = null;
    renderAnalytics();
  }
}

function openCart() {
  elements.cartPanel.classList.add("open");
  elements.mobileScrim.classList.add("visible");
  elements.cartToggle.setAttribute("aria-expanded", "true");
}

function closeCart() {
  elements.cartPanel.classList.remove("open");
  elements.mobileScrim.classList.remove("visible");
  elements.cartToggle.setAttribute("aria-expanded", "false");
}

function parseRoute() {
  const [view = "catalog", id] = window.location.hash.replace(/^#/, "").split("/");
  return { view: ["catalog", "order", "operations", "analytics"].includes(view) ? view : "catalog", id };
}

function navigate() {
  const route = parseRoute();
  elements.views.forEach((view) => view.classList.toggle("hidden", view.dataset.view !== route.view));
  elements.nav.forEach((item) => item.classList.toggle("active", item.dataset.nav === route.view));
  closeCart();
  closeLiveStreams();
  if (route.view === "order") startOrderLive(route.id);
	if (route.view === "operations") loadOperations();
  if (route.view === "analytics") loadAnalytics();
  window.scrollTo({ top: 0, behavior: "instant" });
}

async function checkReadiness() {
  try {
    await api.readiness();
    elements.serviceState.classList.add("online");
    elements.serviceState.querySelector("span").textContent = "Все сервисы готовы";
  } catch {
    elements.serviceState.classList.remove("online");
    elements.serviceState.querySelector("span").textContent = "API недоступен";
  }
}

elements.productGrid.addEventListener("click", (event) => {
  const button = event.target.closest("[data-cart-action]");
  if (!button) return;
  updateQuantity(button.dataset.productId, button.dataset.cartAction === "increment" ? 1 : -1);
});
elements.cartContent.addEventListener("click", (event) => {
  const button = event.target.closest("[data-cart-action]");
  if (!button) return;
  updateQuantity(button.dataset.productId, button.dataset.cartAction === "increment" ? 1 : -1);
});
elements.orderContent.addEventListener("click", (event) => {
  if (event.target.closest('[data-order-action="cancel"]')) cancelCurrentOrder();
});
elements.operationsContent.addEventListener("click", (event) => {
  const button = event.target.closest("[data-operation]");
  if (button) runDeliveryAction(button.dataset.deliveryId, button.dataset.operation);
});
elements.cartToggle.addEventListener("click", openCart);
elements.closeCart.addEventListener("click", closeCart);
elements.mobileScrim.addEventListener("click", closeCart);
document.querySelector("#refresh-catalog").addEventListener("click", loadShop);
document.querySelector("#refresh-order").addEventListener("click", () => loadOrder(parseRoute().id));
document.querySelector("#refresh-analytics").addEventListener("click", loadAnalytics);
document.querySelector("#refresh-operations").addEventListener("click", loadOperations);
window.addEventListener("hashchange", navigate);
window.addEventListener("beforeunload", closeLiveStreams);

renderCart();
renderOrder();
loadShop();
checkReadiness();
navigate();
