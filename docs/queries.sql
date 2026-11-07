-- Query from Trino. Each statement is independent.
SELECT * FROM lake.marts.mart_market_overview
ORDER BY hour_start DESC, volume_rank
LIMIT 20;

SELECT symbol, window_start, open, high, low, close, volume, trade_count
FROM lake.gold.candles_1m
WHERE symbol = 'BTCUSDT'
ORDER BY window_start DESC
LIMIT 30;

SELECT status, count(*) AS candles
FROM lake.recon.candle_vs_kline
GROUP BY 1 ORDER BY 1;

SELECT *
FROM lake.ops.gap_events
WHERE backfilled_at IS NULL
ORDER BY detected_at;

SELECT snapshot_id, committed_at, operation
FROM lake.silver."aggtrade$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- Replace the example snapshot ID with one returned by the preceding query.
-- SELECT count(*) FROM lake.silver.aggtrade FOR VERSION AS OF 1234567890;
