# Локальная разработка

## Требования

Целевые версии инструментов:

- Go 1.23 или новее;
- Docker Engine с Docker Compose v2;
- `make` (необязательно; все команды будут иметь прямые эквиваленты);
- Python 3.12 для локального запуска ETA service вне контейнера;
- `kubectl`, Helm 3 и kind или k3d для локального Kubernetes stack.

Фактические образы и версии будут зафиксированы, а не оставлены на floating `latest` tags.

## Локальные endpoints

| Компонент | URL/port |
|---|---|
| API gateway | `http://localhost:8080` |
| Swagger UI | `http://localhost:8088` |
| ETA service | `http://localhost:8090` |
| Kafka bootstrap | `localhost:9092` |
| PostgreSQL | `localhost:5432` |
| Redis | `localhost:6379` |
| ClickHouse HTTP | `http://localhost:8123` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| Jaeger UI | `http://localhost:16686` |
| Web UI | `http://localhost:8089` |

Порты внутренних Go-сервисов будут доступны в compose network; наружу публикуется только то, что полезно для разработки и диагностики.

## Конфигурация

Все сервисы следуют 12-factor стилю конфигурации через environment variables. Планируемые общие переменные:

```text
FRESHFLOW_ENV
FRESHFLOW_LOG_LEVEL
FRESHFLOW_HTTP_ADDR
FRESHFLOW_POSTGRES_DSN
FRESHFLOW_REDIS_ADDR
FRESHFLOW_KAFKA_BROKERS
FRESHFLOW_OTEL_EXPORTER_OTLP_ENDPOINT
```

Сервисные переменные получают собственные префиксы. `.env.example` будет содержать только безопасные demo-значения.

## Запуск этапа 9

```powershell
docker compose up --build -d --wait
pwsh -NoProfile -File scripts/smoke.ps1
$env:FRESHFLOW_INTEGRATION = '1'
go test ./tests/integration -count=1
docker compose down
```

Unit-тесты ETA service вне Compose:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r ml/eta-service/requirements-dev.txt
$env:PYTHONPATH = (Resolve-Path ml/eta-service).Path
.\.venv\Scripts\python -m pytest ml/eta-service/tests
```

Проверка без запуска контейнеров:

```powershell
go test ./...
node --check web/app.js
node --test web/tests/*.test.js
go build ./services/api-gateway/cmd/api-gateway
docker compose config --quiet
```

`GET /healthz` проверяет только жизнеспособность процесса. `GET /readyz` gateway параллельно проверяет PostgreSQL, Redis, Kafka и readiness доменных сервисов с отдельным timeout и возвращает `503`, если хотя бы одна критичная зависимость недоступна.

Каждый Go-процесс и ETA service публикует `/metrics`. Prometheus targets доступны на `http://localhost:9090/targets`, dashboard — в Grafana folder `FreshFlow`, а checkout trace ищется в Jaeger по service `api-gateway` или `order-service` и correlation ID из structured logs.

Web UI обслуживается отдельным nginx-контейнером. Он проксирует `/api/*` в `api-gateway`, поэтому браузеру не нужен CORS-доступ к внутренним сервисам. Откройте `http://localhost:8089`, добавьте товары и оформите заказ; экран заказа опрашивает order/delivery API раз в 1,5 секунды, пока заказ не завершится.

### Проверка checkout вручную

```powershell
$user = '00000000-0000-4000-8000-000000000001'
$product = '10000000-0000-4000-8000-000000000001'
$cart = Invoke-RestMethod -Method Put -Uri "http://localhost:8080/api/v1/carts/$user/items/$product" -ContentType application/json -Body '{"quantity":2}'
$body = @{ user_id = $user; cart_version = $cart.version } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/v1/orders' -Headers @{ 'Idempotency-Key' = 'demo-checkout-1' } -ContentType application/json -Body $body
```

Повтор последнего запроса с тем же ключом и body вернёт тот же заказ и header `Idempotency-Replayed: true`. Тот же ключ с другим `cart_version` вернёт `409 idempotency_key_reused`.

Integration test дополнительно читает Kafka с уникальной consumer group, связывает `inventory.reserved` и `order.created` по correlation ID, повторно публикует тот же order envelope и проверяет в PostgreSQL, что notification-worker создал ровно одну запись.

Test ожидает не более 30 секунд, пока simulator и delivery/order consumers проведут заказ до `delivered`, затем проверяет public delivery snapshot, наличие оценённых ETA-прогнозов в PostgreSQL и проекции `order.created`/`delivery.completed` в ClickHouse. Повторное сообщение должно давать одну строку при чтении `FINAL`. Текущие координаты курьера и последний ETA доступны через `GET /api/v1/deliveries/order/{order_id}`.

Dashboard snapshot доступен через `GET /api/v1/analytics/summary`. Самостоятельно выполнить готовые аналитические запросы можно так:

```powershell
Get-Content db/clickhouse/queries/orders_by_hour.sql | docker compose exec -T clickhouse clickhouse-client --user freshflow --password freshflow --database freshflow
```

## Проверка структуры

Из корня репозитория:

```powershell
pwsh -NoProfile -File scripts/verify-structure.ps1
```

Скрипт проверяет наличие архитектурных документов и всех заявленных границ monorepo.

## Kubernetes local stack

Перед Kubernetes install соберите и загрузите образы FreshFlow в node runtime:

```powershell
kind create cluster --name freshflow
pwsh -NoProfile -File scripts/build-k8s-images.ps1 -Target kind -ClusterName freshflow
helm lint deploy/helm/freshflow
helm upgrade --install freshflow deploy/helm/freshflow --namespace freshflow --create-namespace --wait --wait-for-jobs --timeout 8m
kubectl -n freshflow port-forward service/freshflow-freshflow-web 8089:80
```

Chart создаёт migration Jobs и Kafka bootstrap Job, поэтому `--wait-for-jobs`
обязателен для чистого первого запуска. Подробный kind/k3d runbook, persistent
local storage и очистка описаны в `deploy/helm/freshflow/README.md`.
