MERGE INTO lake.recon.candle_vs_kline AS target
USING (
    WITH coverage AS (
        SELECT symbol, date_trunc('minute', min(trade_time)) AS first_window
        FROM lake.silver.aggtrade
        GROUP BY 1
    ),
    comparison AS (
        SELECT coalesce(g.symbol, r.symbol) AS symbol,
            coalesce(g.window_start, r.window_start) AS window_start,
            g.open - r.open AS open_diff, g.high - r.high AS high_diff,
            g.low - r.low AS low_diff, g.close - r.close AS close_diff,
            g.volume - r.volume AS volume_diff, g.trade_count - r.trade_count AS trade_count_diff,
            CASE
                WHEN r.trade_count = 0 AND g.symbol IS NULL THEN 'no_trades'
                WHEN g.symbol IS NULL THEN 'missing_gold'
                WHEN r.symbol IS NULL THEN 'missing_reference'
                WHEN g.open = r.open AND g.high = r.high AND g.low = r.low AND g.close = r.close
                    AND g.volume = r.volume AND g.trade_count = r.trade_count THEN 'match'
                ELSE 'mismatch'
            END AS status
        FROM lake.gold.candles_1m g
        FULL JOIN lake.ref.klines_1m r ON g.symbol = r.symbol AND g.window_start = r.window_start
    )
    SELECT c.*, current_timestamp(6) AS checked_at
    FROM comparison c
    JOIN coverage s ON c.symbol = s.symbol AND c.window_start > s.first_window
    WHERE c.window_start < current_timestamp - INTERVAL '2' MINUTE
      AND c.window_start >= current_timestamp - INTERVAL '3' DAY
) AS source
ON target.symbol = source.symbol AND target.window_start = source.window_start
WHEN MATCHED AND (
    target.status IS DISTINCT FROM source.status
    OR target.open_diff IS DISTINCT FROM source.open_diff
    OR target.high_diff IS DISTINCT FROM source.high_diff
    OR target.low_diff IS DISTINCT FROM source.low_diff
    OR target.close_diff IS DISTINCT FROM source.close_diff
    OR target.volume_diff IS DISTINCT FROM source.volume_diff
    OR target.trade_count_diff IS DISTINCT FROM source.trade_count_diff
) THEN UPDATE SET
    open_diff = source.open_diff, high_diff = source.high_diff, low_diff = source.low_diff,
    close_diff = source.close_diff, volume_diff = source.volume_diff,
    trade_count_diff = source.trade_count_diff, status = source.status, checked_at = source.checked_at
WHEN NOT MATCHED THEN INSERT VALUES (
    source.symbol, source.window_start, source.open_diff, source.high_diff, source.low_diff,
    source.close_diff, source.volume_diff, source.trade_count_diff, source.status, source.checked_at
)
