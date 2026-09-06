# Prometheus

Prometheus опрашивает `/metrics` восьми Go-процессов и ETA service каждые 5 секунд. Labels ограничены service/route/method/status/topic/partition; идентификаторы заказов и пользователей намеренно не используются.

`alerts.yml` содержит recording rules и operational alerts: gateway 5xx availability SLO, p95 latency, Kafka lag, отдельный backlog analytics-worker, задержку analytics projection, on-time delivery SLO и недоступность ETA target. 4xx, включая ожидаемый `404` ещё не назначенной доставки, исключены из gateway availability SLO.
