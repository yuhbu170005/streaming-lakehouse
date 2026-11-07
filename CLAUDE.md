# Real-time Crypto Streaming Lakehouse — Agent Context

Portfolio project #2 for a Data Engineer internship applicant (project #1 was a batch ELT lakehouse with dlt/dbt/Airflow). This one targets streaming, table formats, correctness under failure, and observability. The owner must be able to defend every design decision in an interview.

## Read first (in order)
1. `docs/SPEC.md` — what to build. Source of truth.
2. `docs/PLAN.md` — phases, tasks, acceptance criteria. One phase at a time.
3. `.claude/skills/streaming-lakehouse-builder/SKILL.md` — how to work.

## Non-negotiables
- **Correctness before features.** The headline claims are: no loss, no duplicates after crashes, and candles that match Binance's official klines. Everything else is secondary.
- Every claim in README/CV must come from a real measurement stored in `docs/BENCHMARKS.md` or a reproducible `make` target. Never invent numbers or screenshots.
- Public market data only. No API keys, no trading logic, no user-data streams. Respect Binance limits and terms; README includes a "not financial advice / public data only" note.
- Runs locally via Docker Compose on a 24 GB laptop; every service has a memory limit. No paid cloud.
- No secrets in git. `.env.example` documents all variables.
- Stop at the end of each phase, report evidence (commands + output summary), wait for the owner's go-ahead.
- The owner is learning: explain decisions in Vietnamese in chat; code, comments, docs, commits in English. Log them in `docs/LEARNING_LOG.md`.

## Stack (pin versions; verify the current stable release and compatibility matrix first)
Python 3.12 · uv · Redpanda (+ built-in Schema Registry) · Avro · Spark Structured Streaming (PySpark) · Apache Iceberg (REST catalog) on MinIO · Trino · dbt-trino · Dagster · Prometheus + Grafana · Metabase · Docker Compose · GitHub Actions · ruff · pytest
