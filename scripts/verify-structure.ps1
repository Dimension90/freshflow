$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$requiredPaths = @(
    'README.md',
    'docs/architecture.md',
    'docs/events.md',
    'docs/development.md',
    'services/api-gateway',
    'services/catalog-service',
    'services/cart-service',
    'services/order-service',
    'services/delivery-service',
    'services/analytics-worker',
    'services/notification-worker',
    'services/courier-simulator',
    'ml/eta-service',
    'web',
    'web/index.html',
    'web/app.js',
    'web/api.js',
    'web/styles.css',
    'web/Dockerfile',
    'web/nginx.conf',
    'web/assets/fresh-groceries.webp',
    'contracts/openapi',
    'contracts/events',
    'db/postgres/migrations',
    'db/clickhouse/migrations',
    'observability/prometheus',
    'observability/prometheus/prometheus.yml',
    'observability/prometheus/alerts.yml',
    'observability/grafana',
    'observability/grafana/dashboards/freshflow-overview.json',
    'observability/otel',
    'pkg/platform/telemetry/telemetry.go',
    'ml/eta-service/app/main.py',
    'deploy/compose',
    'deploy/helm/freshflow',
    'deploy/helm/freshflow/Chart.yaml',
    'deploy/helm/freshflow/values.yaml',
    'deploy/helm/freshflow/templates/apps.yaml',
    'deploy/helm/freshflow/templates/dependencies.yaml',
    'deploy/helm/freshflow/templates/migrations.yaml',
    'deploy/helm/freshflow/templates/hpa.yaml',
    'scripts/build-k8s-images.ps1',
    'build/migrations/Dockerfile.postgres',
    'build/migrations/Dockerfile.clickhouse',
    'tests/integration'
)

$missing = foreach ($relativePath in $requiredPaths) {
    $absolutePath = Join-Path $repositoryRoot $relativePath
    if (-not (Test-Path -LiteralPath $absolutePath)) {
        $relativePath
    }
}

if ($missing) {
    Write-Error "FreshFlow structure is incomplete. Missing: $($missing -join ', ')"
}

Write-Output "FreshFlow stage 10 structure is valid ($($requiredPaths.Count) checks)."
