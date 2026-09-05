param(
    [ValidateSet('kind', 'k3d', 'none')]
    [string]$Target = 'kind',
    [string]$ClusterName = 'freshflow'
)

$ErrorActionPreference = 'Stop'
$repositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repositoryRoot
try {
    $goServices = @(
        'catalog-service', 'cart-service', 'order-service', 'delivery-service',
        'notification-worker', 'courier-simulator', 'analytics-worker'
    )
    foreach ($service in $goServices) {
        docker build --build-arg "SERVICE=$service" -f build/package/Dockerfile.go-service -t "freshflow/$service`:dev" .
    }
    docker build -f services/api-gateway/Dockerfile -t freshflow/api-gateway:dev .
    docker build -f ml/eta-service/Dockerfile -t freshflow/eta-service:dev .
    docker build -f web/Dockerfile -t freshflow/web:dev .
    docker build -f build/migrations/Dockerfile.postgres -t freshflow/postgres-migrations:dev .
    docker build -f build/migrations/Dockerfile.clickhouse -t freshflow/clickhouse-migrations:dev .

    $images = @(
        'freshflow/api-gateway:dev', 'freshflow/catalog-service:dev', 'freshflow/cart-service:dev',
        'freshflow/order-service:dev', 'freshflow/delivery-service:dev', 'freshflow/notification-worker:dev',
        'freshflow/courier-simulator:dev', 'freshflow/analytics-worker:dev', 'freshflow/eta-service:dev',
        'freshflow/web:dev', 'freshflow/postgres-migrations:dev', 'freshflow/clickhouse-migrations:dev'
    )
    if ($Target -eq 'kind') {
        kind load docker-image --name $ClusterName @images
    } elseif ($Target -eq 'k3d') {
        k3d image import --cluster $ClusterName @images
    }
} finally {
    Pop-Location
}
