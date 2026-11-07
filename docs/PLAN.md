# PLAN — Real-time Crypto Streaming Lakehouse

One phase at a time. At the end of each phase: run the **Accept** commands, update `docs/LEARNING_LOG.md` and `docs/FAILURE_MODES.md`, commit, report, wait for the owner's go-ahead.
Estimated 5–6 weeks part-time. Tasks marked **(owner)** are core-logic modules the owner implements in *learning mode* (see SKILL.md): the agent provides a skeleton, tests and review.

## Phase 0 — Foundation (0.5–1 day)
- [x] Repo layout (SPEC §12), `pyproject.toml` (uv), pre-commit, `.env.example`, Makefile skeleton, CI skeleton (ruff + pytest)
- [x] `docs/LEARNING_LOG.md`, `docs/FAILURE_MODES.md` (table: component → failure → effect → mitigation)
- [x] Verify current stable versions and the Spark ↔ Iceberg ↔ Trino compatibility matrix; write it to `docs/VERSIONS.md`
**Accept:** `make lint` green; CI green on push. [Verified GitHub Actions run](https://github.com/yuhbu170005/streaming-lakehouse/actions/runs/42401390782).

## Phase 1 — Ingest to Kafka (5–6 days)
- [ ] Compose: Redpanda (+ console optional), Schema Registry check; `make topics` creates topics (SPEC §4)
- [ ] Avro schema v1; register via script
- [ ] **(owner)** Producer: combined-stream WS client, parsing → Avro, Kafka producer with idempotence/acks=all, backpressure
- [ ] **(owner)** Reconnect logic: exponential backoff + jitter, proactive reconnect before connection expiry, handle `serverShutdown`, respect rate limits
- [ ] **(owner)** Gap detection per symbol (`a` continuity) + REST backfill job (`is_backfill=true`)
- [ ] Prometheus metrics endpoint in producer
- [ ] Tests: fake WS server (drops, duplicates, gaps, out-of-order) → assertions on emitted messages/gaps
**Accept:** `make up topics produce` shows steady messages in `binance.aggtrade.raw.v1`; fake-WS tests green; manual 30-min live run with 0 unexplained gaps (record count).

## Phase 2 — Bronze & Silver (6–7 days)
- [ ] Compose: MinIO, Iceberg REST catalog; Spark image with pinned Kafka/Iceberg/S3 jars
- [ ] Job A: Kafka → `bronze.aggtrade_raw` (append, checkpointed)
- [ ] **(owner)** Job B: bronze → `silver.aggtrade` via `foreachBatch` + `MERGE INTO` (typed, validated, deduped), invalid → `aggtrade.dlq.v1`
- [ ] Silver validation rules (SPEC §5) + `ops.dq_results`
- [ ] Tests: dedupe/idempotency (same batch applied twice → same table), validation unit tests, restart-from-checkpoint test
**Accept:** after 1 h live: `count(*) = count(distinct (symbol, agg_trade_id))` in silver; restart job B → still true; DLQ receives injected bad events.

## Phase 3 — Gold candles & reconciliation (5–6 days)
- [ ] **(owner)** Job C: silver → `gold.candles_1m` (event-time tumbling window, watermark 30 s, MERGE upsert, open/close via `min_by/max_by`)
- [ ] Job D: kline reference stream → `binance.kline.ref.v1` → `ref.klines_1m` (closed candles only)
- [ ] `recon.candle_vs_kline` + Dagster/SQL job; report match rate
- [ ] Replay-safe batch repair for closed Gold candles affected by backfill; reconcile after repair
- [ ] Invariant checks (SPEC §9) as pytest + SQL
**Accept:** ≥ 2 h run with recon mismatches = 0 on closed candles (or every mismatch explained and documented); invariants pass.

## Phase 4 — Query layer & BI (4–5 days)
- [ ] Trino + Iceberg catalog; sample queries file `docs/queries.sql`
- [ ] dbt-trino project: staging + 3 marts + tests (SPEC §6)
- [ ] Metabase dashboards (3), screenshots → `docs/img/`
**Accept:** `make dbt-build` green; dashboards load from marts.

## Phase 5 — Orchestration & observability (5–6 days)
- [ ] Dagster project: maintenance (compaction, expire snapshots, orphan files, manifests), reconciliation, gap backfill, dbt marts, asset checks
- [ ] `ops.maintenance_log` with before/after file counts
- [ ] Prometheus + Grafana: 3 dashboards (SPEC §7); Spark `StreamingQueryListener` exporter; alert rules (+ Telegram webhook optional)
**Accept:** dashboards show live lag/throughput/latency; stopping the producer triggers the "no data" alert; compaction visibly reduces file count (numbers recorded).

## Phase 6 — Chaos & benchmarks (5–6 days)
- [ ] `make chaos-*` scripts for every row in SPEC §9; results with evidence in `docs/CHAOS_RESULTS.md`
- [ ] Verify Silver ID continuity within five minutes after network restoration in the two-minute producer-cut test; time candle repair and re-reconciliation separately
- [ ] Load tests (`make loadtest`, clearly labelled synthetic) + live runs at 20/100/200 symbols
- [ ] `docs/BENCHMARKS.md` filled with measured numbers only (SPEC §10)
- [ ] Fix whatever the chaos tests break; update FAILURE_MODES.md
**Accept:** all chaos tests pass or have documented, justified known limitations; benchmark tables complete.

## Phase 7 — Polish & optional (4–6 days)
- [ ] README (SPEC §13): architecture diagram, trade-offs, failure-modes table, benchmarks, run instructions, limitations, disclaimer
- [ ] CI: unit + integration (fake WS) + dbt parse/compile
- [ ] Schema evolution demo (producer v2 with optional field)
- [ ] Optional (pick at most one): Flink job reimplementing the gold candles for comparison · second source (Wikimedia EventStreams) · Kubernetes (kind + Helm) · Terraform
**Accept:** fresh-clone run works from the README in < 30 min (excluding image pulls).

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
