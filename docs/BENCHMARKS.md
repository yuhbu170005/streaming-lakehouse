# Benchmarks

This file records observed runs. Benchmark JSON is written under the ignored `data/` directory and includes the commit, actual duration, Docker hardware, per-service CPU and RAM samples, Silver throughput and p50/p95/p99 event-time latency, and reconciliation counts.

## Reproduce

```sh
make benchmark-live
make loadtest
```

Each target defaults to a 60-second warm-up and a 30-minute measurement. Set `BENCHMARK_SECONDS=21600` for the six-hour reconciliation run. The benchmark distinguishes compared candles, mismatches, missing inputs, and no-trade periods.

For scale runs, generate a live subscription file ordered by Binance's public quote-volume ticker and pass it to both producer and Silver through the Compose environment. Restart the affected services with the same file, then run the benchmark.

```sh
uv run --locked --group operations python -m chaos.benchmark symbols --count 100 > data/symbols-100.yaml
SYMBOLS_FILE=./data/symbols-100.yaml make start-streams
make benchmark-live
```

Use `--count 200` for the 200-symbol case. For synthetic runs, use `symbols --synthetic` and set `DEMO_SYMBOLS_FILE` when starting the isolated demo project.

## Evidence

See [chaos results](CHAOS_RESULTS.md) for recovery durations. The six-hour Binance acceptance window and scale benchmarks are separate from the short startup verification.
