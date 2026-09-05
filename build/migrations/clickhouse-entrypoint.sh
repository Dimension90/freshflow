#!/bin/sh
set -eu

: "${FRESHFLOW_CLICKHOUSE_HOST:?FRESHFLOW_CLICKHOUSE_HOST must be set}"
: "${FRESHFLOW_CLICKHOUSE_USER:?FRESHFLOW_CLICKHOUSE_USER must be set}"
: "${FRESHFLOW_CLICKHOUSE_PASSWORD:?FRESHFLOW_CLICKHOUSE_PASSWORD must be set}"

until clickhouse-client --host "$FRESHFLOW_CLICKHOUSE_HOST" \
  --user "$FRESHFLOW_CLICKHOUSE_USER" --password "$FRESHFLOW_CLICKHOUSE_PASSWORD" \
  --query 'SELECT 1' >/dev/null 2>&1; do
  sleep 2
done

for file in /migrations/*.sql; do
  clickhouse-client --host "$FRESHFLOW_CLICKHOUSE_HOST" \
    --user "$FRESHFLOW_CLICKHOUSE_USER" --password "$FRESHFLOW_CLICKHOUSE_PASSWORD" \
    --database freshflow --multiquery < "$file"
done
