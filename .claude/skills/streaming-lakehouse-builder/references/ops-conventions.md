# Ops conventions (Docker, observability, chaos, benchmarks, CI)

## Docker Compose
- Pin image tags (no `latest`); healthchecks; `depends_on: condition: service_healthy`; named volumes; `mem_limit` on every service.
- Profiles: `core` (Redpanda, MinIO, catalog, Spark jobs, producer), `query` (Trino, Metabase, Dagster), `obs` (Prometheus, Grafana). `make up-lite` = core + obs.
- Makefile targets: `up up-lite down topics produce start-streams stop-streams dbt-build recon invariants chaos-<name> loadtest lint test docs reset help`.

## Observability
- Prometheus scrape configs and Grafana dashboards provisioned as code (JSON in `config/`), no manual clicking required.
- Required panels: producer msgs/s, reconnects, gaps; Redpanda consumer lag; Spark input vs processed rate and batch duration; end-to-end latency p50/p95/p99; late events, DLQ rate; recon match rate; file counts per table.
- Alerts: lag, no-data, un-backfilled gap, recon mismatch. Webhook to Telegram optional; log-only if unset.

## Chaos tests
- Each `make chaos-<name>` is scripted, repeatable, and ends by running the invariant checks; output saved under `docs/chaos_runs/<date>-<name>.txt` and summarized in `docs/CHAOS_RESULTS.md` (what was done, expected, observed, pass/fail, recovery time).
- Tests: spark-kill (`kill -9` mid-batch), broker-restart, producer-network-cut, minio-restart, poison-events, late-data, schema-v2.
- If a test reveals a bug, fix it, re-run, and record both runs.

## Benchmarks
- Record: date, commit SHA, hardware (CPU/RAM), symbols count, duration, command. One table per metric. Warm-up excluded and stated.
- Percentile latency from `processed_at - trade_time` over ≥ 30 min windows. Synthetic load tests are reported separately.

## CI (GitHub Actions)
- Jobs: ruff → pytest unit → integration (fake WS server + Redpanda service container if feasible) → `dbt parse`/`compile` → build docs artifact.
- CI must not call Binance (flaky, rate-limited); use the fake WS server and recorded sample fixtures.

## Docs deliverables
`README.md`, `docs/ARCHITECTURE.md` (diagram as Mermaid + exported PNG), `FAILURE_MODES.md`, `CHAOS_RESULTS.md`, `BENCHMARKS.md`, `VERSIONS.md`, `LEARNING_LOG.md`, `INTERVIEW_NOTES.md` (owner-written).
