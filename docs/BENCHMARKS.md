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
