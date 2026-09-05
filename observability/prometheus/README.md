# Prometheus

Prometheus опрашивает `/metrics` восьми Go-процессов и ETA service каждые 5 секунд. Labels ограничены service/route/method/status/topic/partition; идентификаторы заказов и пользователей намеренно не используются.

`alerts.yml` содержит демонстрационные operational alerts для error ratio, Kafka lag и отсутствующего ETA target.
