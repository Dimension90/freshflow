#!/bin/sh
set -eu

: "${FRESHFLOW_POSTGRES_DSN:?FRESHFLOW_POSTGRES_DSN must be set}"
exec migrate -path /migrations -database "$FRESHFLOW_POSTGRES_DSN" up
