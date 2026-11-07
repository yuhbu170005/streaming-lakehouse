# Learning log

## Phase 0 — Foundation (2026-11-08)

- Established the repository layout, a locked Python environment, and shared lint and test commands for local development and CI.
- Selected Spark 4.1.3 against the Iceberg support matrix. Spark, Scala, and the connector artifacts must agree before deployment.
- Kept the streaming watermark at 30 seconds and documented a separate repair for closed candles affected by backfill.
- Explain in an interview how the scope of a recovery guarantee is defined by the tested ID range and measured recovery clock.
