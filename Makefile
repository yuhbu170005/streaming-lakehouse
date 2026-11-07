.DEFAULT_GOAL := help
COMPOSE = docker compose --profile core --profile query --profile obs
DEMO = docker compose -p streaming-lakehouse-demo -f docker-compose.yml -f compose.demo.yml --profile core --profile query --profile obs

.PHONY: help sync lint test
help:
	@printf '%s\n' 'make sync  Install locked development dependencies' \
		'make lint  Check Python lint and formatting' \
		'make test  Run the pytest suite' \
		'make up topics start-streams  Start the live stack' \
		'make up-lite  Start core plus observability' \
		'make demo  Start an isolated synthetic fixture stack' \
		'make dbt-build recon invariants  Run warehouse checks' \
		'make dashboards  Provision Metabase dashboards'

sync:
	uv sync --locked

lint:
	uv run --locked ruff check .
	uv run --locked ruff format --check .

test:
	uv run --locked pytest

.PHONY: build-core up up-lite topics produce start-streams stop-streams down dbt-build recon invariants dashboards compact docs reset demo demo-down
build-core:
	$(COMPOSE) build producer minio catalog bootstrap

up: build-core
	$(COMPOSE) build dagster
	$(COMPOSE) up -d --wait redpanda catalog trino dagster warehouse-metrics metabase prometheus grafana

up-lite: build-core
	$(COMPOSE) up -d --wait redpanda catalog prometheus grafana

topics:
	$(COMPOSE) up -d --wait redpanda
	$(COMPOSE) exec -T redpanda sh /opt/lakehouse/topics.sh

produce: topics
	$(COMPOSE) build producer
	$(COMPOSE) up -d --wait producer

start-streams: produce
	$(COMPOSE) build bootstrap
	$(COMPOSE) up -d --wait bronze silver gold reference

stop-streams:
	$(COMPOSE) stop producer bronze silver gold reference

down:
	$(COMPOSE) down

dbt-build:
	$(COMPOSE) exec -T dagster python -m orchestration.operations dbt

recon:
	$(COMPOSE) exec -T dagster python -m orchestration.operations recon

invariants:
	$(COMPOSE) exec -T dagster python -m orchestration.operations invariants

compact:
	$(COMPOSE) exec -T dagster python -m orchestration.operations compact

dashboards: dbt-build
	uv run --locked --group operations python -m orchestration.metabase

docs:
	uv run --locked --group operations dbt docs generate --project-dir dbt_project --profiles-dir dbt_project

reset:
	@test "$(CONFIRM)" = "delete-local-data" || (echo 'Pass CONFIRM=delete-local-data to delete this Compose project’s volumes.'; exit 1)
	$(COMPOSE) down --volumes

demo: build-core
	$(DEMO) up -d --wait redpanda catalog trino fixture
	$(DEMO) exec -T redpanda sh /opt/lakehouse/topics.sh
	$(DEMO) up -d --wait producer bronze silver gold reference

demo-down:
	$(DEMO) down

.PHONY: chaos-spark-kill chaos-broker-restart chaos-producer-cut chaos-minio-restart chaos-poison chaos-late-data chaos-schema-v2
chaos-spark-kill chaos-broker-restart chaos-producer-cut chaos-minio-restart chaos-poison chaos-late-data chaos-schema-v2:
	@mkdir -p docs/chaos_runs
	@log="docs/chaos_runs/$$(TZ=Asia/Ho_Chi_Minh date +%F)-$(@:chaos-%=%).txt"; \
	if uv run --locked --group operations python -m chaos.run "$(@:chaos-%=%)" > "$$log" 2>&1; then cat "$$log"; else cat "$$log"; exit 1; fi

LOAD_SECONDS ?= 1800
WARMUP_SECONDS ?= 60
BENCHMARK_SECONDS ?= 1800
BENCHMARK_OUTPUT ?= data/benchmark-live.json
SETTLE_SECONDS ?= 130
.PHONY: loadtest benchmark-live
loadtest:
	TRINO_PORT=28080 uv run --locked --group operations python -m chaos.benchmark run \
		--mode synthetic --project streaming-lakehouse-demo --duration "$(LOAD_SECONDS)" \
		--warmup "$(WARMUP_SECONDS)" --settle "$(SETTLE_SECONDS)" --output data/benchmark-synthetic.json

benchmark-live:
	uv run --locked --group operations python -m chaos.benchmark run \
		--mode live --project streaming-lakehouse --duration "$(BENCHMARK_SECONDS)" \
		--warmup "$(WARMUP_SECONDS)" --settle "$(SETTLE_SECONDS)" --output "$(BENCHMARK_OUTPUT)"

.PHONY: benchmark-compaction
benchmark-compaction:
	TRINO_PORT=28080 uv run --locked --group operations python -m chaos.benchmark compact --output data/benchmark-compaction.json

.PHONY: benchmark-silver
benchmark-silver:
	$(DEMO) build bootstrap
	$(DEMO) run --rm --no-deps bootstrap benchmark-silver

SCALE_SYMBOLS ?= 100
.PHONY: benchmark-scale
benchmark-scale: build-core
	sh scripts/benchmark_scale.sh "$(SCALE_SYMBOLS)" "$(BENCHMARK_SECONDS)"

.PHONY: acceptance-live
acceptance-live:
	uv run --locked --group operations python -m chaos.benchmark run \
		--mode live --project streaming-lakehouse --duration 21600 --warmup 120 \
		--expected-symbols 20 --acceptance --output data/benchmark-binance-6h.json
