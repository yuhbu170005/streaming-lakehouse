# PLAN — Real-time Crypto Streaming Lakehouse

Phases define implementation order. Run the **Accept** commands and update `docs/LEARNING_LOG.md` and `docs/FAILURE_MODES.md` as evidence becomes available. The owner requested continuous implementation through all phases.
Estimated 5–6 weeks part-time. Tasks marked **(owner)** are core-logic modules the owner implements in *learning mode* (see SKILL.md): the agent provides a skeleton, tests and review.

## Phase 0 — Foundation (0.5–1 day)
- [x] Repo layout (SPEC §12), `pyproject.toml` (uv), pre-commit, `.env.example`, Makefile skeleton, CI skeleton (ruff + pytest)
- [x] `docs/LEARNING_LOG.md`, `docs/FAILURE_MODES.md` (table: component → failure → effect → mitigation)
- [x] Verify current stable versions and the Spark ↔ Iceberg ↔ Trino compatibility matrix; write it to `docs/VERSIONS.md`
**Accept:** `make lint` green; CI green on push. [Verified GitHub Actions run](https://github.com/yuhbu170005/streaming-lakehouse/actions/runs/42401390782).

## Phase 1 — Ingest to Kafka (5–6 days)
- [x] Compose: Redpanda (+ console optional), Schema Registry check; `make topics` creates topics (SPEC §4)
- [x] Avro schema v1; register via script
- [x] **(owner)** Producer: combined-stream WS client, parsing → Avro, Kafka producer with idempotence/acks=all, backpressure
- [x] **(owner)** Reconnect logic: exponential backoff + jitter, proactive reconnect before connection expiry, handle `serverShutdown`, respect rate limits
- [x] **(owner)** Gap detection per symbol (`a` continuity) + REST backfill job (`is_backfill=true`)
- [x] Prometheus metrics endpoint in producer
- [x] Tests: fake WS server (drops, duplicates, gaps, out-of-order) → assertions on emitted messages/gaps
**Accept:** `make up topics produce` shows steady messages in `binance.aggtrade.raw.v1`; fake-WS tests green; manual 30-min live run with 0 unexplained gaps (record count).

## Phase 2 — Bronze & Silver (6–7 days)
- [x] Compose: MinIO, Iceberg REST catalog; Spark image with pinned Kafka/Iceberg/S3 jars
- [x] Job A: Kafka → `bronze.aggtrade_raw` (append, checkpointed)
- [x] **(owner)** Job B: bronze → `silver.aggtrade` via `foreachBatch` + `MERGE INTO` (typed, validated, deduped), invalid → `aggtrade.dlq.v1`
- [x] Silver validation rules (SPEC §5) + `ops.dq_results`
- [x] Tests: dedupe/idempotency (same batch applied twice → same table), validation unit tests, restart-from-checkpoint test
**Accept:** after 1 h live: `count(*) = count(distinct (symbol, agg_trade_id))` in silver; restart job B → still true; DLQ receives injected bad events.

## Phase 3 — Gold candles & reconciliation (5–6 days)
- [x] **(owner)** Job C: silver → `gold.candles_1m` (event-time tumbling window, watermark 30 s, MERGE upsert, open/close via `min_by/max_by`)
- [x] Job D: kline reference stream → `binance.kline.ref.v1` → `ref.klines_1m` (closed candles only)
- [x] `recon.candle_vs_kline` + Dagster/SQL job; report match rate
- [x] Replay-safe batch repair for closed Gold candles affected by backfill; reconcile after repair
- [x] Invariant checks (SPEC §9) as pytest + SQL
**Accept:** ≥ 2 h run with recon mismatches = 0 on closed candles (or every mismatch explained and documented); invariants pass. The measured run covered all 2,400 symbol-minute slots. See [benchmarks](BENCHMARKS.md).

## Phase 4 — Query layer & BI (4–5 days)
- [x] Trino + Iceberg catalog; sample queries file `docs/queries.sql`
- [x] dbt-trino project: staging + 3 marts + tests (SPEC §6)
- [x] Metabase dashboards (3), screenshots → `docs/img/`
**Accept:** `make dbt-build` green; dashboards load from marts.

## Phase 5 — Orchestration & observability (5–6 days)
- [x] Dagster project: maintenance (compaction, expire snapshots, orphan files, manifests), reconciliation, gap backfill, dbt marts, asset checks
- [x] `ops.maintenance_log` with before/after file counts
- [x] Prometheus + Grafana: 3 dashboards (SPEC §7); Spark `StreamingQueryListener` exporter; alert rules (+ Telegram webhook optional)
**Accept:** dashboards show live lag/throughput/latency; stopping the producer triggers the "no data" alert; compaction visibly reduces file count (numbers recorded).

## Phase 6 — Chaos & benchmarks (5–6 days)
- [x] `make chaos-*` scripts for every row in SPEC §9; results with evidence in `docs/CHAOS_RESULTS.md`
- [x] Verify Silver ID continuity within five minutes after network restoration in the two-minute producer-cut test; time candle repair and re-reconciliation separately
- [x] Load tests (`make loadtest`, clearly labelled synthetic) + live runs at 20/100/200 symbols
- [x] `docs/BENCHMARKS.md` filled with measured numbers only (SPEC §10)
- [x] Fix whatever the chaos tests break; update FAILURE_MODES.md
**Accept:** all chaos tests pass or have documented, justified known limitations; benchmark tables complete. The six-hour run covered all 7,200 expected windows with zero mismatches. See [chaos results](CHAOS_RESULTS.md) and [benchmarks](BENCHMARKS.md).

## Phase 7 — Polish & optional (4–6 days)
- [x] README (SPEC §13): architecture diagram, trade-offs, failure-modes table, benchmarks, run instructions, limitations, disclaimer
- [x] CI: unit + integration (fake WS) + dbt parse/compile
- [x] Schema evolution demo (producer v2 with optional field)
- [ ] Optional (pick at most one): Flink job reimplementing the gold candles for comparison · second source (Wikimedia EventStreams) · Kubernetes (kind + Helm) · Terraform
**Accept:** fresh-clone run works from the README in < 30 min (excluding image pulls). Verified in 223 seconds through dashboard provisioning. See [benchmarks](BENCHMARKS.md).

## Interview prep (owner writes answers in `docs/INTERVIEW_NOTES.md`; agent only critiques)
1. At-least-once vs exactly-once: what exactly do you guarantee, and where (producer, Spark, Iceberg)?
2. Why `MERGE` on `(symbol, agg_trade_id)` instead of `dropDuplicates`? State growth trade-offs?
3. Event time vs processing time; what does your watermark do; what happens to late events?
4. A job crashes between writing data and committing its checkpoint — what state is the table in? (Iceberg atomic commits)
5. Small-files problem: why does streaming cause it, how do you fix it, what's the cost?
6. How do you know your candles are correct? How would you detect a silent data-loss bug?
7. Kafka partitioning by symbol: what ordering guarantee do you get, what hot-spot risk?
8. 100× more symbols: what breaks first and what do you change?
9. Schema evolution: what's a backward-compatible change; what happens on a breaking one?
10. Why Dagster for maintenance but not for the streaming jobs?
