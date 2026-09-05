$ErrorActionPreference = 'Stop'

$baseURL = if ($env:FRESHFLOW_BASE_URL) { $env:FRESHFLOW_BASE_URL.TrimEnd('/') } else { 'http://localhost:8080' }
$webURL = if ($env:FRESHFLOW_WEB_URL) { $env:FRESHFLOW_WEB_URL.TrimEnd('/') } else { 'http://localhost:8089' }
$correlationID = "smoke-$([guid]::NewGuid().ToString('N'))"

$health = Invoke-RestMethod -Uri "$baseURL/healthz" -Headers @{ 'X-Correlation-ID' = $correlationID }
if ($health.status -ne 'up' -or $health.service -ne 'api-gateway') {
    throw "Unexpected health response: $($health | ConvertTo-Json -Compress)"
}

$ready = Invoke-RestMethod -Uri "$baseURL/readyz" -Headers @{ 'X-Correlation-ID' = $correlationID }
if ($ready.status -ne 'ready') {
    throw "Unexpected readiness response: $($ready | ConvertTo-Json -Depth 4 -Compress)"
}

$root = Invoke-RestMethod -Uri "$baseURL/api/v1" -Headers @{ 'X-Correlation-ID' = $correlationID }
if ($root.name -ne 'FreshFlow API' -or $root.version -ne 'v1') {
    throw "Unexpected API response: $($root | ConvertTo-Json -Compress)"
}

$metrics = Invoke-WebRequest -UseBasicParsing -Uri "$baseURL/metrics"
if ($metrics.Content -notmatch 'http_server_request_duration_seconds' -or $metrics.Content -notmatch 'freshflow_orders_created_total') {
    throw 'Gateway Prometheus metrics are missing.'
}

$demoUserID = '00000000-0000-4000-8000-000000000001'
$demoProductID = '10000000-0000-4000-8000-000000000001'
$catalog = Invoke-RestMethod -Uri "$baseURL/api/v1/catalog/products" -Headers @{ 'X-Correlation-ID' = $correlationID }
if ($catalog.products.Count -lt 1) {
    throw 'Catalog is empty.'
}

$cart = Invoke-RestMethod -Method Put -Uri "$baseURL/api/v1/carts/$demoUserID/items/$demoProductID" `
    -ContentType 'application/json' -Body '{"quantity":1}' -Headers @{ 'X-Correlation-ID' = $correlationID }
$idempotencyKey = "smoke-$([guid]::NewGuid().ToString('N'))"
$checkoutBody = @{ user_id = $demoUserID; cart_version = $cart.version } | ConvertTo-Json -Compress
$order = Invoke-RestMethod -Method Post -Uri "$baseURL/api/v1/orders" -ContentType 'application/json' `
    -Body $checkoutBody -Headers @{ 'X-Correlation-ID' = $correlationID; 'Idempotency-Key' = $idempotencyKey }
$replayed = Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$baseURL/api/v1/orders" -ContentType 'application/json' `
    -Body $checkoutBody -Headers @{ 'X-Correlation-ID' = $correlationID; 'Idempotency-Key' = $idempotencyKey }
if ($order.status -ne 'created' -or $replayed.Headers['Idempotency-Replayed'] -ne 'true') {
    throw 'Checkout or idempotency replay failed.'
}

$analytics = Invoke-RestMethod -Uri "$baseURL/api/v1/analytics/summary" -Headers @{ 'X-Correlation-ID' = $correlationID }
if ($null -eq $analytics.orders_by_hour -or $null -eq $analytics.popular_products -or $null -eq $analytics.status_durations) {
    throw 'Analytics summary has an invalid shape.'
}

$webHealth = Invoke-RestMethod -Uri "$webURL/healthz"
$webPage = Invoke-WebRequest -UseBasicParsing -Uri $webURL
if ($webHealth.status -ne 'up' -or $webPage.Content -notmatch '<title>FreshFlow') {
    throw 'FreshFlow web UI is not ready.'
}

Write-Output "FreshFlow smoke test passed for API $baseURL and web $webURL."
