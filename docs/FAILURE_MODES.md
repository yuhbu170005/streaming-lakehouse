# Failure modes

These are design expectations. Each phase adds verification evidence for the components it introduces.

| Component | Failure | Effect | Mitigation and verification |
|---|---|---|---|
| Development and CI | Wrong Python or stale lock | Local and CI checks can run with different dependencies | Pin Python, use locked uv commands, and assert the active interpreter in the foundation smoke test. |
| Producer / Redpanda | Disconnect or delivery interrupted mid-batch, including broker unavailability | Gap or replay in the source IDs | Apply backpressure and retry. Detect missing ranges and backfill. In the two-minute producer-cut test, verify every ID in the affected range in Silver within five minutes of network restoration. Report unresolved gaps. |
| Spark / Iceberg | Crash mid-batch, duplicate or out-of-order delivery, or MinIO unavailable | A batch can retry and replay rows | Use separate durable checkpoints and replay-safe writes. Verify key uniqueness and Bronze/Silver containment after restart. |
| Gold / reconciliation | A recovered trade arrives after the watermark, or repair races with an upsert | A closed candle can diverge from Silver or the official kline | Retain the trade in Silver and count the late event. Recompute affected closed windows from Silver using a replay-safe batch with coordinated Gold writes. Reconcile again and time the repair separately. |

## Runtime recovery

| Component | Failure | Effect | Mitigation and verification |
|---|---|---|---|
| WebSocket | Disconnect, backpressure, or shutdown | Unread trades or reference closes during the outage | Retry with jitter and resume from the last acknowledged cursor. Fetch missing trade ranges and affected closed klines over REST. Honor Retry-After for rate limits. |
| Source history | A page does not begin at the expected ID | Gap cannot be safely closed | Leave the range open and retry. The gap age and open-gap metrics expose the condition. |
| Broker | Down during a batch | Producer cannot advance its cursor; partial delivery can replay | Bounded queues, broker retries, and durable cursor. Silver deduplicates after recovery. |
| Bronze | Poison bytes or unsupported format | A raw event cannot be parsed | Preserve raw bytes and Kafka lineage. Silver writes the rejection to Kafka DLQ and the Iceberg DQ table. |
| Silver | Termination after merge before checkpoint | Input batch can replay | Insert-only Iceberg merge chooses one valid row per business key. |
| Catalog | Concurrent commits | SQLite file lock caused 500 responses in early testing | PostgreSQL replaces SQLite for the REST catalog. Iceberg resolves optimistic concurrency and leaves failed files for later cleanup. |
| MinIO | Process unavailable | Iceberg I/O and checkpoint attempts can fail | Container restarts preserve the warehouse and checkpoints. Spark retries or restarts from its checkpoint. |
| Maintenance | Checkpoint points to an expired source snapshot | Incremental reader requires an explicit replay plan | Keep three days of snapshots, monitor lag, and verify checkpoint recovery before reducing retention. |
| Gold | Data older than the watermark | Closed Gold and full Silver can differ | Preserve Silver, count the late data, and rebuild eligible windows affected by a completed gap. |
| Metabase or Trino | Temporary restart | Dashboards or scheduled SQL unavailable | Query services recover independently. Kafka, Spark, and Iceberg continue ingestion and materialization. |
| Dagster | A run outlives a daemon restart in its run store | A serial queue can remain blocked | Store runs in PostgreSQL and monitor startup and execution timeouts. Streaming queries continue independently. |
