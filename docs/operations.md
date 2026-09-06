# FreshFlow: SLO и incident signals

Эта страница описывает локальный production-like baseline, а не реальные
гарантии сервиса. Все правила лежат в репозитории и воспроизводимы через
Docker Compose.

## SLO и пороги

| Signal | Цель / порог | Почему |
|---|---:|---|
| Gateway availability | 5xx ratio <= 2% за 5 минут | Измеряет технические сбои пользовательского API; 4xx исключены. |
| Gateway latency | p95 <= 750ms за 10 минут | Даёт ранний сигнал деградации синхронного пути. |
| Consumer lag | <= 100 records на service/topic | Показывает, что события не успевают обрабатываться. |
| Analytics worker lag | <= 100 records 2 минуты | Отдельно защищает freshness ClickHouse dashboard. |
| Analytics projection age | <= 60 секунд | Возраст события в момент успешной записи в ClickHouse. |
| On-time delivery | >= 90% за 24 часа, минимум 10 завершений | Защищает продуктовую цель, не реагируя на статистический шум. |

## Ожидаемый 404 при назначении доставки

После `order.created` доставка создаётся асинхронно через Kafka. Пока
`delivery-service` не обработал событие, запрос `GET
/internal/v1/deliveries/order/{orderID}` может корректно вернуть `404`.

Этот статус:

- остаётся на diagnostic panel **Expected 404: delivery not assigned**;
- может быть виден в Jaeger как error span;
- **не включается** в gateway 5xx availability SLO и не вызывает alert.

Если `404` остаётся после того, как заказ должен перейти в `delivering`, это
уже расследование: сравнить Kafka lag, логи `delivery-service`, trace и запись
в PostgreSQL.

## Как тренироваться на signals

1. Открой Grafana → **FreshFlow Overview** и Alerting → **Alert rules**.
2. Создай заказ и проверь trace в Jaeger: gateway → order → Kafka consumers.
3. Сверь Kafka lag и projection age после появления order analytics.
4. При alert сначала сформулируй гипотезу, затем используй trace, metrics и
   structured logs. Не рестартуй сервис до сбора доказательств.
