# Benchmarks

This file records observed runs. Benchmark JSON is written under the ignored `data/` directory and includes the commit, actual duration, Docker hardware, per-service CPU and RAM samples, Silver throughput and p50/p95/p99 event-time latency, and reconciliation counts.

## Reproduce

```sh
make benchmark-live
make loadtest
```

Each target defaults to a 60-second warm-up and a 30-minute measurement. Run `make acceptance-live` for the six-hour live reconciliation check over the 20-symbol default configuration. The benchmark distinguishes compared candles, mismatches, missing inputs, and no-trade periods.

The scale targets use a separate project with the same producer, Kafka, Bronze, and Silver path. They leave the main live subscription in place.

```sh
make benchmark-scale SCALE_SYMBOLS=100
make benchmark-scale SCALE_SYMBOLS=200
make benchmark-compaction
make benchmark-silver
```

Run the synthetic compaction measurement on the demo with its producer stopped and upstream batches drained. It compares five executions of the same Trino query before and after optimization and verifies row counts. The Spark merge benchmark writes isolated test tables and drops them after measuring the batches.

For synthetic load at a different symbol count, generate a fixture allowlist using `python -m chaos.benchmark symbols --count 100 --synthetic` and set `DEMO_SYMBOLS_FILE` when starting the demo project.

## Evidence

See [chaos results](CHAOS_RESULTS.md) for recovery durations. The six-hour Binance acceptance window and scale benchmarks are separate from the short startup verification.

### Live startup and BI check

A short development run used a 24 GiB Apple Silicon host. Trino observed 13,353 Bronze records, 13,259 Silver rows, 445 Gold candles, and 456 reference rows at one query snapshot. The first short reconciliation sample found 52 matches. A later sample found 408 matches, five official no-trade candles, and zero missing inputs or mismatches. All six invariant counts were zero. These samples precede the sustained run and do not measure sustained throughput.

Metabase queries ran over populated marts, and dbt reported 14 passes across four models and ten tests. [Market overview](img/market-overview.png) · [Symbol drill-down](img/symbol-drill-down.png) · [Data health](img/data-health.png)

### Synthetic smoke and compaction

| Command | Actual result |
|---|---|
| `make loadtest LOAD_SECONDS=90 WARMUP_SECONDS=15 SETTLE_SECONDS=20` | 901 Silver trades in 91.12 s, 9.89 rows/s, and p50/p95/p99 latency of 16,097 / 27,288 / 30,843 ms. One fixture symbol. This short run is separate from the 30-minute live benchmarks. |
| `make benchmark-compaction` | Bronze shrank from 117 to 1 current data file with 15,855 rows before and after. The median of five identical Trino count-and-distinct queries was 190.35 ms before and 63.06 ms after. |

The synthetic run used commit `8f0611d`, a 10-vCPU ARM64 Docker VM, and approximately 13.6 GiB of VM memory.

The isolated Spark `MERGE` benchmark used four inserts per batch size and excluded the first sample. The medians were 324.88 ms at 100 rows, 843.29 ms at 1,000 rows, and 325.46 ms at 10,000 rows. These measurements came from a small target table and shared local compute, so they describe the observed runs rather than a scaling curve.

The first sustained measurements were interrupted when the host entered idle sleep at 02:27 on 2026-11-08. Those runs produced no accepted measurements. The harness now writes progress files and invalidates sampling gaps longer than two minutes. On macOS, `caffeinate -is make acceptance-live` holds power assertions for the duration of the acceptance command.

The isolated demo also observed the no-data alert firing 235.51 seconds after its producer stopped. The daily maintenance procedures completed against the demo warehouse. Silver continued to consume new appends after Bronze compaction.

### Sustained Binance run

The 20-symbol baseline ran from 03:35:42 to 04:05:44 Asia/Ho_Chi_Minh on 2026-11-08. The 120-second warm-up was excluded. The 100-symbol scale test ran concurrently for part of this baseline, and the pipeline recovered 87 trades during the measurement.

| Symbols observed | Duration | Silver trades | Trades/s | p50 / p95 / p99 latency | Comparable candles |
|---|---:|---:|---:|---:|---|
| 20 | 1,801.76 s | 34,928 | 19.39 | 16,595 / 67,951 / 128,169 ms | 598 matches, 0 mismatches |

Two official no-trade candles were recorded separately. There were zero missing inputs in this window. The benchmark started at `85fed7e` and includes a checkpointed deployment of `73110ce`. Raw samples are in the local `data/benchmark-live-20.json` report.

Peak sampled resource use across the 29 one-minute samples is shown below. CPU is Docker's per-container CPU percentage and can exceed 100% on multiple cores.

| Service | CPU | RAM (MiB) |
|---|---:|---:|
| bronze | 152.0% | 1246 |
| catalog | 19.0% | 214 |
| dagster | 96.8% | 736 |
| gold | 172.0% | 1594 |
| grafana | 43.9% | 202 |
| metabase | 12.9% | 785 |
| minio | 70.7% | 394 |
| postgres | 28.1% | 67 |
| producer | 23.1% | 40 |
| prometheus | 13.0% | 81 |
| redpanda | 16.6% | 112 |
| reference | 116.7% | 1474 |
| scale-100-bronze | 129.5% | 1321 |
| scale-100-catalog | 24.1% | 177 |
| scale-100-minio | 58.6% | 324 |
| scale-100-postgres | 8.9% | 39 |
| scale-100-producer | 28.5% | 40 |
| scale-100-redpanda | 20.6% | 212 |
| scale-100-silver | 192.0% | 1563 |
| scale-100-trino | 22.1% | 1010 |
| silver | 309.4% | 1695 |
| trino | 509.5% | 2189 |
| warehouse-metrics | 17.1% | 35 |

### Backlog recovery sample

An earlier live 100-symbol run started measuring just after the host woke. It wrote 142,789 Silver rows in 1,801.91 seconds (79.24 rows/s), including 56,262 backfilled rows. The p50/p95/p99 event-time latency was 29,055 / 2,312,826 / 3,013,062 ms. This run measured catch-up from accumulated source history. The report is retained locally as `data/benchmark-scale-100-recovery.json`.

### Live scale and one-hour checks

The scale projects measured producer-to-Silver throughput and latency independently of Gold, BI, and scheduling. The 120-second warm-up was excluded. These were different market windows with different trade activity.

| Configured / observed symbols | Duration | Silver trades | Trades/s | p50 / p95 / p99 latency | Backfilled |
|---|---:|---:|---:|---:|---:|
| 100 / 100 | 1,800.74 s | 79,021 | 43.88 | 18,450 / 54,664 / 79,910 ms | 0 |
| 200 / 199 | 1,800.72 s | 76,835 | 42.67 | 18,542 / 74,041 / 152,181 ms | 0 |

Both runs used `81ba5a8`. The second run completed its measurement but failed the expected-symbol count. The scale command now checks recent trade activity before selecting pairs. The failed coverage report remains in `data/benchmark-scale-200-observed-199.json`.

In the first hour of the primary run, Silver held 64,812 rows and 64,812 distinct trade keys. Reconciliation recorded 1,198 exact matches and two official no-trade candles. Eight scheduled optimization actions recorded 3,379 input files and 19 output files in total across the affected tables.

After recent-trade screening, all 200 configured symbols appeared in Silver. The run used `84037c6`, started at 05:25:29, and ended at 05:55:30 Asia/Ho_Chi_Minh on 2026-11-08.

| Configured / observed symbols | Duration | Silver trades | Trades/s | p50 / p95 / p99 latency | Backfilled |
|---|---:|---:|---:|---:|---:|
| 200 / 200 | 1,800.82 s | 60,430 | 33.56 | 18,341 / 83,799 / 100,000 ms | 0 |

The first two hours of the primary run accounted for all 2,400 expected symbol-minute slots. There were 2,376 exact matches, 24 official no-trade candles, and zero missing inputs or mismatches.
