# Interview preparation worksheet

Use the questions in [PLAN](PLAN.md) to draft your own explanations. These project references provide examples and verification results to cite.

| Topic | Project evidence |
|---|---|
| Delivery and exactly-once effects | The architecture shows the separate Kafka acknowledgement, SQLite cursor, Bronze checkpoint, and Silver merge boundaries. |
| Why a merge for Silver | The Spark-kill log shows an interrupted micro-batch and a contiguous Silver range after recovery. The merge benchmark records observed cost by batch size. |
| Event time and watermark | The late-data log shows a record retained by Silver with the Gold window unchanged. The network-cut test shows the separate repair path. |
| Atomic commits and orphan files | The failure-modes table explains the Iceberg snapshot commit and the daily cleanup policy. |
| Small-file compaction | The demo reduced Bronze from 117 files to one while preserving the row count. The benchmark records the query timings. |
| Candle verification | The six-hour run accounted for all 7,200 expected symbol-minute slots. Explain how closed reference candles, empty minutes, and missing inputs are counted. |
| Partitioning and ordering | The Kafka topic uses symbol keys. Explain what happens when activity concentrates on a single symbol. |
| Scaling | The benchmark distinguishes current market activity, observed latency, and the separate recovery measurement. |
| Schema evolution | The schema-v2 run shows a new optional field while existing consumers continue reading. |
| Scheduled work | Dagster schedules bounded repairs, reconciliation, and maintenance. Spark owns the long-lived queries and checkpoints. |
