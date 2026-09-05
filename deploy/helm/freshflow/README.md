# FreshFlow Helm chart

Chart запускает локальный FreshFlow stack в одном namespace: все application
services, PostgreSQL, Redis, single-broker Kafka KRaft, ClickHouse, Jaeger,
Kafka topic bootstrap и две migration Job. Он предназначен для kind/k3d и не
заменяет managed stateful-сервисы в реальном production.

## Что входит

- `Deployment` и `Service` для gateway, catalog, cart, order, delivery,
  notification, courier simulator, analytics, ETA и web;
- `StatefulSet` для PostgreSQL, Kafka и ClickHouse; Redis и Jaeger в singleton
  `Deployment`;
- `Secret` с явными локальными demo credentials и `ConfigMap` с endpoint-ами;
- `postgres-migrate`, `clickhouse-migrate` и `kafka-init` Jobs;
- `autoscaling/v2` HPA для `order-service`;
- readiness/liveness probes и Prometheus pod annotations для сервисов.

По умолчанию stateful data размещаются в `emptyDir`, чтобы `helm uninstall`
полностью очищал локальную демонстрацию. Для сохранения данных включите
`localDependencies.storage.enabled=true` и при необходимости задайте
`localDependencies.storage.storageClass`.

## kind

Нужны Docker Desktop, `kind`, `kubectl` и Helm 3. Из корня репозитория:

```powershell
kind create cluster --name freshflow
pwsh -NoProfile -File scripts/build-k8s-images.ps1 -Target kind -ClusterName freshflow
helm upgrade --install freshflow deploy/helm/freshflow --namespace freshflow --create-namespace --wait --wait-for-jobs --timeout 8m
kubectl -n freshflow get pods
kubectl -n freshflow port-forward service/freshflow-freshflow-web 8089:80
```

После port-forward откройте `http://localhost:8089`. Gateway можно проверить
отдельно: `kubectl -n freshflow port-forward service/freshflow-freshflow-api-gateway 8080:8080`.

## k3d

```powershell
k3d cluster create freshflow
pwsh -NoProfile -File scripts/build-k8s-images.ps1 -Target k3d -ClusterName freshflow
helm upgrade --install freshflow deploy/helm/freshflow --namespace freshflow --create-namespace --wait --wait-for-jobs --timeout 8m
kubectl -n freshflow port-forward service/freshflow-freshflow-web 8089:80
```

## Проверка и очистка

```powershell
helm lint deploy/helm/freshflow
helm template freshflow deploy/helm/freshflow | kubectl apply --dry-run=client -f -
kubectl -n freshflow get jobs,pods,svc,hpa
helm uninstall freshflow --namespace freshflow
kind delete cluster --name freshflow # или: k3d cluster delete freshflow
```

HPA требует работающий Kubernetes metrics-server. У kind/k3d он может не быть
установлен по умолчанию: манифест HPA всё равно безопасно создаётся, но статус
`<unknown>` исчезнет только после подключения metrics API. Для локального
стека все образы FreshFlow собираются и импортируются до Helm install, поэтому
chart не пытается скачать несуществующие образы из registry.

`localDependencies.enabled=false` оставлен для расширения chart, но базовый
values file намеренно ориентирован только на self-contained локальный stack.
Подключение к managed-инфраструктуре не является частью этого pet project:
demo Secret должен быть заменён внешним Secret manager в настоящем окружении.
