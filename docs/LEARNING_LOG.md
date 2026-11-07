# Learning log

## Phase 0 — Foundation (2026-11-08)

- Established the repository layout, a locked Python environment, and shared lint and test commands for local development and CI.
- Selected Spark 4.1.3 against the Iceberg support matrix. Spark, Scala, and the connector artifacts must agree before deployment.
- Kept the streaming watermark at 30 seconds and documented a separate repair for closed candles affected by backfill.
- Explain in an interview how the scope of a recovery guarantee is defined by the tested ID range and measured recovery clock.
- Verified `make lint`, `make test` (1 passed), `docker compose config -q`, and both pre-commit hooks locally. [GitHub Actions](https://github.com/yuhbu170005/streaming-lakehouse/actions/runs/42401390782) passed sync, lint, and test after upgrading uv from 0.13.5 to 0.13.6 for the Linux x86_64 Python build.
