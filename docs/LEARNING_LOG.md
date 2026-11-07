# Learning log

## Phase 0 — Foundation (2026-11-08)

- Established the repository layout, a locked Python environment, and shared lint and test commands for local development and CI.
- Selected Spark 4.1.3 against the Iceberg support matrix. Spark, Scala, and the connector artifacts must agree before deployment.
- Kept the streaming watermark at 30 seconds and documented a separate repair for closed candles affected by backfill.
- Explain in an interview how the scope of a recovery guarantee is defined by the tested ID range and measured recovery clock.
- Verified `make lint`, `make test` (1 passed), `docker compose config -q`, and both pre-commit hooks locally. [GitHub Actions](https://github.com/yuhbu170005/streaming-lakehouse/actions/runs/42401390782) passed sync, lint, and test after upgrading uv from 0.13.5 to 0.13.6 for the Linux x86_64 Python build.

## Ingest and streaming

- Persist the last acknowledged trade ID and missing ranges together. A crash between Kafka acknowledgement and the SQLite commit can replay input.
- Page backfill with the expected ID and stop at the first discontinuity. Fetch official klines for the recovered minutes so outage windows can be independently reconciled.
- Retain raw decimal strings and Kafka lineage in Bronze. Silver validates and performs an insert-only merge against the trade key.
- Explain why Bronze's checkpoint and Silver's business key protect different retry boundaries.

## Warehouse and operations

- Recompute a closed, affected candle only after the recovered range is complete in Silver. Protect repaired rows from live Gold upserts.
- Switched the REST catalog from SQLite to PostgreSQL after concurrent Spark commits produced SQLite lock errors.
- Keep Spark jobs long-lived and schedule bounded repair, quality, dbt, and maintenance tasks in Dagster.
- Explain why an Iceberg rewrite can reduce file counts while preserving the logical rows available to a streaming reader.
