export const ORDER_STEPS = ["created", "confirmed", "assembling", "delivering", "delivered"];

export const STATUS_LABELS = {
  created: "Создан",
  confirmed: "Подтверждён",
  assembling: "Собираем",
  delivering: "В пути",
  delivered: "Доставлен",
  cancelled: "Отменён",
  assigned: "Курьер назначен",
  completed: "Доставка завершена",
};

export function money(minor, currency = "RUB", locale = "ru-RU") {
  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency,
    maximumFractionDigits: 0,
  }).format((minor ?? 0) / 100);
}

export function duration(seconds) {
  if (seconds == null || Number.isNaN(Number(seconds))) return "—";
  const value = Math.max(0, Math.round(Number(seconds)));
  if (value < 60) return `${value} сек`;
  const minutes = Math.round(value / 60);
  if (minutes < 60) return `${minutes} мин`;
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  return remainder ? `${hours} ч ${remainder} мин` : `${hours} ч`;
}

export function percentage(ratio) {
  if (ratio == null || Number.isNaN(Number(ratio))) return "—";
  return `${Math.round(Number(ratio) * 100)}%`;
}

export function shortID(id) {
  return id ? id.slice(0, 8).toUpperCase() : "—";
}

export function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

export function courierPosition(delivery) {
  if (!delivery) return { x: 18, y: 68 };
  const points = [
    [delivery.pickup_longitude, delivery.pickup_latitude],
    [delivery.destination_longitude, delivery.destination_latitude],
    [delivery.courier_longitude, delivery.courier_latitude],
  ];
  const xs = points.map(([x]) => Number(x));
  const ys = points.map(([, y]) => Number(y));
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const spanX = Math.max(maxX - minX, 0.001);
  const spanY = Math.max(maxY - minY, 0.001);
  return {
    x: clamp(12 + ((Number(delivery.courier_longitude) - minX) / spanX) * 76, 8, 92),
    y: clamp(88 - ((Number(delivery.courier_latitude) - minY) / spanY) * 76, 8, 92),
  };
}
