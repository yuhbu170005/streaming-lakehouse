# Failure modes

These are design expectations. Each phase adds verification evidence for the components it introduces.

| Component | Failure | Effect | Mitigation and verification |
|---|---|---|---|
| Development and CI | Wrong Python or stale lock | Local and CI checks can run with different dependencies | Pin Python, use locked uv commands, and assert the active interpreter in the foundation smoke test. |
| Producer / Redpanda | Disconnect or delivery interrupted mid-batch, including broker unavailability | Gap or replay in the source IDs | Apply backpressure and retry. Detect missing ranges and backfill. In the two-minute producer-cut test, verify every ID in the affected range in Silver within five minutes of network restoration. Report unresolved gaps. |
| Spark / Iceberg | Crash mid-batch, duplicate or out-of-order delivery, or MinIO unavailable | A batch can retry and replay rows | Use separate durable checkpoints and replay-safe writes. Verify key uniqueness and Bronze/Silver containment after restart. |
| Gold / reconciliation | A recovered trade arrives after the watermark, or repair races with an upsert | A closed candle can diverge from Silver or the official kline | Retain the trade in Silver and count the late event. Recompute affected closed windows from Silver using a replay-safe batch with coordinated Gold writes. Reconcile again and time the repair separately. |
