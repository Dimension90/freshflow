.PHONY: help test py-test web-test helm-lint integration build up down logs ps config smoke load verify

help:
	@echo "FreshFlow commands: test py-test web-test helm-lint build up down logs ps config smoke load verify"

test:
	go test ./...

py-test:
	PYTHONPATH=ml/eta-service python -m pytest ml/eta-service/tests

web-test:
	node --check web/app.js
	node --test web/tests/*.test.js

helm-lint:
	helm lint deploy/helm/freshflow
	helm template freshflow deploy/helm/freshflow > /dev/null

integration:
	FRESHFLOW_INTEGRATION=1 go test ./tests/integration -count=1

build:
	go build ./services/...

up:
	docker compose up --build -d --wait

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

config:
	docker compose config --quiet

smoke:
	pwsh -NoProfile -File scripts/smoke.ps1

load:
	docker compose --profile load run --rm k6

verify:
	pwsh -NoProfile -File scripts/verify-structure.ps1
