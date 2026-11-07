# Chaos test results

Chaos tests run against the isolated synthetic Compose project. Each command writes its observed output under `docs/chaos_runs/`. See the README for the full command sequence. The producer-cut test times recovery from restoration of network access until the entire detected ID range appears exactly once in Silver.

## 2026-11-08 run

| Case | Observed result | Recovery after service or network restoration |
|---|---|---:|
| Silver SIGKILL | Killed during an active trigger. Silver contained every ID in 1001229–1001529. | 0.46 s after the restarted service passed its health check |
| Redpanda restart | Silver contained every ID in 1001529–1001630. | 9.80 s |
| MinIO restart | Silver contained every ID in 1001630–1001829. | 20.24 s |
| Poison | A malformed Avro record reached both the Kafka DLQ and Iceberg DQ table. | — |
| Late event | Silver retained the record, the Gold table stayed unchanged for its window, and the watermark-drop counter advanced. | — |
| Avro v2 | Bronze saw a second schema ID and Silver remained contiguous. | — |
| Two-minute producer network cut | Silver contained each ID in 1009829–1011190 exactly once. One gap contained 1,294 recovered IDs. The repair then reconciled two eligible closed candles in 4.59 s. | 28.46 s |

Each passing run ended with zero invariant violations. Logs in [chaos_runs](chaos_runs/) retain earlier failures. The first poison verification used an invalid offset flag. The initial full-range producer cut verified Silver but found the test exchange had no REST kline route. The route was added and exercised in the passing run.

The first late-data attempt ran during a warehouse restart. A later repeat reused a previously injected key and did not exercise a fresh late input. The final test uses a new ID and derives the target minute from Gold progress. Local file paths and trailing spaces in the logs were normalized.
