# Streaming conventions

## Producer (Python asyncio)
- One combined-stream connection per ≤ N symbols (config); parse the `{"stream","data"}` envelope. Keep parsing separate from I/O so it's unit-testable.
- Money/quantity fields arrive as strings: keep as strings through Avro (or decimal logical type); never use float.
- Reconnect: exponential backoff with full jitter, cap, reset on stable connection; reconnect proactively before the documented connection lifetime; on `serverShutdown` reconnect immediately (respecting rate limits). Never exceed control-message limits.
- Bounded queue between WS reader and Kafka sender; when full, apply backpressure (pause reading) and increment a metric. Never drop silently.
- Kafka: idempotent producer, `acks=all`, retries with delivery callbacks that count and log failures; flush on shutdown.
- Gap detector per symbol: `expected = last_a + 1`; on `a > expected` record `(symbol, from=expected, to=a-1)` in `ops.gap_events` and trigger backfill; on `a <= last_a` treat as duplicate (count, still publish or drop per spec — document which).
- Backfill: REST aggTrades with `fromId` and paging; respect REST rate limits; publish with `is_backfill=true`; mark `backfilled_at`.

## Avro / Schema Registry
- Subject naming `<topic>-value`; compatibility BACKWARD; new fields must have defaults.
- Confluent wire format: 5-byte header (magic + schema id). When reading in Spark with `from_avro`, strip the header (`substring(value, 6, …)`) or use a registry-aware approach; test it explicitly.

## Spark Structured Streaming
- One checkpoint location per query on MinIO; never share or delete casually (deleting = reprocess/duplicate risk).
- Bronze is append-only; keep raw strings and Kafka metadata for audit/replay.
- Silver/gold writes use `foreachBatch` + Iceberg `MERGE INTO` keyed by the business key, so replayed batches are idempotent. Document the at-least-once → exactly-once-effect reasoning.
- Event time = `trade_time`; watermark 30 s on gold windows; late events are not applied to gold, stay in Silver, and increment `late_events_total`. After gap backfill, a batch recomputes affected closed candles from Silver and runs reconciliation again.
- Set `maxOffsetsPerTrigger`, trigger interval 5–10 s, small `spark.sql.shuffle.partitions` (local); explain each non-default setting in the learning log.
- Expose progress through a `StreamingQueryListener` exporter (input/processed rate, batch duration, rows, late/dlq counts).
- Avoid unbounded state: no `dropDuplicates` without a watermark; prefer MERGE for dedupe.

## Iceberg
- Atomic commits are what make crashes safe: be able to explain snapshot isolation and what a failed commit leaves behind (orphan files).
- Partition by time (`days(...)`) plus `bucket(n, symbol)` only where it helps; don't over-partition.
- Streaming creates many small files → scheduled `rewrite_data_files`; `expire_snapshots` (retention ~3 days); `remove_orphan_files` with a safe age; `rewrite_manifests` when needed. Log before/after file counts.
- Use time travel in at least one debugging example for the README.

## Correctness checks (must be automated)
- No duplicates: `count(*) = count(distinct symbol, agg_trade_id)` in silver.
- No gaps: per symbol within a range, `count(distinct id) = max(id) - min(id) + 1` (excluding ranges listed as open gaps in `ops.gap_events`).
- Lineage containment: valid bronze keys ⊆ silver keys; silver keys ⊆ bronze keys.
- Candle consistency: candle volume/trade_count equal aggregates over Silver in eligible closed windows after any required repair. Reconcile against official klines and report open gaps or pending repairs separately.
