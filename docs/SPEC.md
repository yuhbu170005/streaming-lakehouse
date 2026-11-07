# SPEC — Real-time Crypto Streaming Lakehouse

## 1. Objectives
Build a streaming platform that ingests live Binance trades, processes them with exactly-once *effect* into an Iceberg lakehouse (medallion: bronze/silver/gold), serves analytics via Trino/dbt, and is observable and chaos-tested.

Headline, verifiable claims:
1. **No loss, no duplicates** after killing any component mid-run (verified by invariant checks).
2. **Gap detection + backfill** from the source's trade IDs.
3. **1-minute OHLCV candles reconcile to Binance official klines** (target: 0 mismatches on closed candles).
4. Measured throughput, end-to-end latency (p50/p95/p99), recovery time, small-file compaction impact.

Non-goals: trading signals/bots, ML, paid cloud, Kubernetes (optional stretch).

## 2. Source
Binance Spot public market data over WebSocket, base `wss://data-stream.binance.vision` (market data only; verify current docs at start).
- Combined stream: `/stream?streams=<sym>@aggTrade/...` → payload `{"stream":..., "data":{...}}`. One connection ≤ 1024 streams.
- aggTrade fields: `e` event type, `E` event time (ms), `s` symbol, `a` aggregate trade ID, `p` price (string), `q` quantity (string), `f` first trade ID, `l` last trade ID, `T` trade time (ms), `m` buyer-is-maker, `M` ignore.
- Reference stream for reconciliation: `<sym>@kline_1m` (consume only closed candles, `k.x == true`).
- Limits to respect: ≤ 5 control messages/s per connection (ping/pong/subscribe), connection attempts rate-limited per IP, connections expire periodically (reconnect proactively), `serverShutdown` events require reconnecting. Use exponential backoff with jitter.
- Symbols: start with 20 liquid USDT pairs (config file `config/symbols.yaml`), scale to 100–200 for benchmarks.
- Gap logic: per symbol, `a` should increase by exactly 1; `trade_count = l - f + 1` per event. A jump in `a` = missing events → backfill via the market-data REST aggTrades endpoint (`fromId`), republished with `is_backfill=true`.

## 3. Architecture
```
Binance WS ─► Producer (asyncio) ─► Redpanda ──────────────┐
   │              │ metrics             topics:             │
   │              ▼                     aggtrade.raw.v1      │
   │         Prometheus                 aggtrade.dlq.v1      │
   └─ REST backfill (gap filler)        kline.ref.v1         │
                                                            ▼
                         Spark job A: Kafka ─► Iceberg bronze.aggtrade_raw (append)
                         Spark job B: bronze ─► silver.aggtrade (MERGE dedupe) + DLQ
                         Spark job C: silver ─► gold.candles_1m (windowed, MERGE upsert)
                         Spark job D: kline.ref ─► ref.klines_1m (closed candles)
                                                            │
        MinIO (S3) + Iceberg REST catalog ◄─────────────────┘
                 │                    │
               Trino ─► dbt-trino ─► marts ─► Metabase        Dagster: maintenance, recon, DQ
                                                              Prometheus + Grafana: ops
```
Streaming jobs run as long-lived Compose services. Dagster handles scheduled/batch work (table maintenance, reconciliation, dbt, quality reports) — not the streaming jobs themselves (explain this decision in docs).

## 4. Kafka / Redpanda
| Topic | Key | Partitions | Retention | Notes |
|---|---|---|---|---|
| `binance.aggtrade.raw.v1` | symbol | 6 | 3 days | Avro, Schema Registry subject `…-value` |
| `binance.aggtrade.dlq.v1` | symbol | 1 | 14 days | invalid/poison events + error reason |
| `binance.kline.ref.v1` | symbol | 3 | 3 days | closed 1m klines for reconciliation |
- Producer: `acks=all`, idempotence enabled, `lz4`, linger ~20–50 ms, bounded in-memory queue with backpressure (never drop silently).
- Envelope fields added by producer: `ingest_ts`, `producer_id`, `conn_id`, `is_backfill`, original payload fields.
- Schema compatibility: BACKWARD. Include a planned v2 change (add optional field) to demonstrate evolution.
- Avro + Schema Registry wire format has a 5-byte header (magic byte + schema id); Spark `from_avro` needs it stripped — document this gotcha.

## 5. Lakehouse (Iceberg on MinIO)
Catalog: Iceberg REST catalog (use a maintained image, e.g. the official Apache fixture, verify at start). Warehouse bucket `s3://lakehouse/`.

**bronze.aggtrade_raw** — append-only mirror of Kafka: payload fields (strings as received), `kafka_topic, kafka_partition, kafka_offset, kafka_ts, ingest_ts, bronze_ts`. Partition: `days(ingest_ts)`.

**silver.aggtrade** — typed, deduped, validated. Columns: `symbol, agg_trade_id (bigint), price (decimal(38,18)), quantity (decimal(38,18)), first_trade_id, last_trade_id, trade_count, trade_time (timestamp), event_time, is_buyer_maker, is_backfill, ingest_ts, processed_at`. Unique key `(symbol, agg_trade_id)`. Partition: `days(trade_time), bucket(8, symbol)`. Written with `foreachBatch` + `MERGE INTO … WHEN NOT MATCHED THEN INSERT` so replays/restarts are idempotent.
- Validation (invalid → DLQ with `error_reason`): price > 0, quantity > 0, symbol in allowlist, `trade_time` ≤ now + 5 s, `last_trade_id >= first_trade_id`, required fields non-null.

**gold.candles_1m** — one row per `(symbol, window_start)`: open, high, low, close, volume, quote_volume, vwap, trade_count, buy_volume, sell_volume, first_agg_id, last_agg_id, updated_at. Event-time tumbling 1-min window on `trade_time`; watermark 30 s; update mode via `foreachBatch` MERGE upsert (candles refine live). Open/close via `min_by/max_by` on `(trade_time, agg_trade_id)`. The streaming job keeps late events beyond the watermark in Silver, leaves Gold unchanged, and counts them in a `late_events` metric. After gap backfill completes and the recovered trades are visible in Silver, a replay-safe batch recomputes the affected closed windows from Silver, upserts them into Gold, and triggers reconciliation again. Coordinate Gold writers so streaming updates cannot overwrite repaired candles.

**ref.klines_1m** — closed official candles for reconciliation.

**recon.candle_vs_kline** — per closed candle: diffs on OHLC, volume, trade_count; `status` match/mismatch. Trade count compare: `sum(last_trade_id - first_trade_id + 1)`.

**ops.* tables** — `ops.gap_events (symbol, from_id, to_id, detected_at, backfilled_at)`, `ops.dq_results`, `ops.maintenance_log`.

Spark settings: checkpoints on MinIO per job; `trigger(processingTime=…)` 5–10 s; `maxOffsetsPerTrigger` set; shuffle partitions tuned small (local); explain every non-default setting.

## 6. Trino + dbt-trino
- Trino with Iceberg catalog pointing to the REST catalog.
- dbt models over silver/gold: `stg_candles`, `mart_symbol_hourly` (OHLCV, volatility proxy, volume), `mart_market_overview` (top volume/movers by hour), `mart_data_health` (gaps, late events, recon match rate). Tests: not_null/unique on keys, accepted ranges (high ≥ low, volume ≥ 0), relationship checks, freshness.
- Metabase dashboards on Trino: Market overview; Symbol drill-down; Data health.

## 7. Observability
Prometheus metrics (producer exporter + Spark `StreamingQueryListener` exporter + Redpanda metrics):
- producer: messages_total, reconnects_total, gaps_detected_total, queue_depth, send_errors_total
- spark: input_rows_per_sec, processed_rows_per_sec, batch_duration_ms, state rows, late_events_total, dlq_events_total
- end-to-end latency: `processed_at - trade_time` percentiles (also computed in SQL for benchmarks)
- consumer lag per group
Grafana dashboards: Pipeline health, Latency & throughput, Data quality. Alerts (Grafana alerting or simple Telegram webhook): lag > threshold, no data for N min, gap not backfilled, recon mismatch > 0.

## 8. Orchestration (Dagster)
Assets/jobs: `iceberg_maintenance` (hourly: `rewrite_data_files` compaction, daily: `expire_snapshots` retaining ~3 days, `remove_orphan_files`, `rewrite_manifests`), `reconciliation` (every 5 min), `gap_backfill` sensor/schedule, `dbt_marts` (hourly), asset checks for DQ. Log every maintenance action to `ops.maintenance_log` with before/after file counts.

## 9. Failure modes & chaos tests (`make chaos-*`, results in `docs/CHAOS_RESULTS.md`)
| Test | Action | Pass criteria |
|---|---|---|
| spark-kill | `kill -9` silver job mid-batch, restart | silver has no duplicates and no missing IDs vs bronze |
| broker-restart | restart Redpanda | producer recovers, no loss beyond documented bounds |
| producer-cut | drop network for 2 min | gap detected and backfilled; all IDs in the affected range present in Silver within 5 min of network restoration. Measure candle repair and re-reconciliation separately |
| minio-restart | restart MinIO | jobs retry/recover; no corruption |
| poison | inject malformed events | land in DLQ; pipeline keeps running |
| late-data | inject events with old `trade_time` | Silver keeps them; the streaming job leaves Gold unchanged beyond the watermark and increments the metric. Test the backfill repair separately |
| schema-v2 | deploy producer v2 with new optional field | consumers keep working |
Invariants (run as SQL/pytest, also by Dagster): per symbol and ID range: `count(*) = count(distinct agg_trade_id)` and `count(distinct id) = max - min + 1`; `silver ⊆ bronze` and `bronze (valid) ⊆ silver`; candle volume = sum of Silver volume in eligible closed windows after any required backfill repair. Report windows with open gaps or pending repairs separately.

## 10. Benchmarks (`docs/BENCHMARKS.md`, measured only)
Sustained events/s at 20/100/200 symbols; p50/p95/p99 end-to-end latency; CPU/RAM per service; recovery time after each chaos test; small-file count and Trino query time before/after compaction; bronze→silver MERGE cost vs batch size. Clearly label any replay/synthetic load runs (`make loadtest`) separately from live data.

## 11. Resource budget (24 GB host)
Redpanda 1.5–2 GB · Spark jobs 2 × ~2–3 GB · MinIO 0.5 GB · Iceberg REST 0.5 GB · Trino 3–4 GB · Dagster 1.5 GB · Prometheus+Grafana 1–1.5 GB · Metabase 1 GB · Postgres (catalog/Dagster/Metabase) 0.5 GB. Set `mem_limit` for each; document a "lite" profile (`make up-lite`) without Trino/Metabase.

## 12. Repo layout
```
streaming-lakehouse/
├── CLAUDE.md · Makefile · docker-compose.yml · .env.example · pyproject.toml
├── config/            # symbols.yaml, prometheus, grafana provisioning
├── producer/          # ws client, gap detection, kafka producer, backfill
├── schemas/           # Avro schemas (v1, v2)
├── streaming/         # Spark jobs: bronze, silver, gold, klines + shared lib
├── orchestration/     # Dagster project
├── dbt_project/       # dbt-trino
├── chaos/             # chaos scripts + invariant checks
├── tests/             # unit + integration (fake WS server, embedded Kafka/Redpanda)
├── docs/              # SPEC, PLAN, LEARNING_LOG, FAILURE_MODES, BENCHMARKS, CHAOS_RESULTS, ARCHITECTURE, img/
└── .github/workflows/
```

## 13. Definition of done
- `make up && make topics && make start-streams` brings the system up; data visible in Trino within 2 minutes.
- All chaos tests pass with recorded evidence; recon shows 0 mismatches on a ≥ 6 h run (record the actual number of candles compared).
- CI green: lint, unit tests, integration tests with a fake WS server, dbt parse/compile.
- README: architecture diagram, decisions + trade-offs, failure-modes table, measured benchmarks, how to run, limitations, disclaimer.
- `docs/INTERVIEW_NOTES.md` written by the owner (agent reviews only).
