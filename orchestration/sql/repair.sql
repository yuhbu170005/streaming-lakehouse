MERGE INTO lake.gold.candles_1m AS target
USING (
    WITH recovered AS (
        SELECT g.symbol, g.from_id, g.to_id
        FROM lake.ops.gap_events g
        JOIN lake.silver.aggtrade s
          ON s.symbol = g.symbol AND s.agg_trade_id BETWEEN g.from_id AND g.to_id
        WHERE g.backfilled_at IS NOT NULL
          AND g.backfilled_at >= current_timestamp - INTERVAL '3' DAY
        GROUP BY 1, 2, 3
        HAVING count(*) = g.to_id - g.from_id + 1
    ),
    affected AS (
        SELECT DISTINCT s.symbol, date_trunc('minute', s.trade_time) AS window_start
        FROM recovered g
        JOIN lake.silver.aggtrade s
          ON s.symbol = g.symbol AND s.agg_trade_id BETWEEN g.from_id AND g.to_id
        JOIN lake.ref.klines_1m r
          ON r.symbol = s.symbol AND r.window_start = date_trunc('minute', s.trade_time)
        WHERE r.window_start <= current_timestamp - INTERVAL '2' MINUTE
    ),
    stats AS (
        SELECT a.symbol, a.window_start,
            min_by(s.price, ROW(s.trade_time, s.agg_trade_id)) AS open,
            max(s.price) AS high, min(s.price) AS low,
            max_by(s.price, ROW(s.trade_time, s.agg_trade_id)) AS close,
            sum(s.quantity) AS volume,
            sum(CAST(s.price AS DECIMAL(17,8)) * CAST(s.quantity AS DECIMAL(20,8))) AS quote_volume,
            sum(s.trade_count) AS trade_count,
            sum(if(NOT s.is_buyer_maker, s.quantity, DECIMAL '0')) AS buy_volume,
            sum(if(s.is_buyer_maker, s.quantity, DECIMAL '0')) AS sell_volume,
            min(s.agg_trade_id) AS first_agg_id,
            max(s.agg_trade_id) AS last_agg_id
        FROM affected a
        JOIN lake.silver.aggtrade s
          ON s.symbol = a.symbol AND s.trade_time >= a.window_start
             AND s.trade_time < a.window_start + INTERVAL '1' MINUTE
        GROUP BY 1, 2
    )
    SELECT *, window_start + INTERVAL '1' MINUTE AS window_end,
        CAST(quote_volume AS DECIMAL(28,8)) / nullif(CAST(volume AS DECIMAL(28,8)), DECIMAL '0') AS vwap, current_timestamp(6) AS updated_at
    FROM stats
) AS source
ON target.symbol = source.symbol AND target.window_start = source.window_start
WHEN MATCHED AND (
    NOT target.repaired OR target.trade_count <> source.trade_count
    OR target.volume <> source.volume OR target.open <> source.open
    OR target.high <> source.high OR target.low <> source.low OR target.close <> source.close
) THEN UPDATE SET
    open = source.open, high = source.high, low = source.low, close = source.close,
    volume = source.volume, quote_volume = source.quote_volume, vwap = source.vwap,
    trade_count = source.trade_count, buy_volume = source.buy_volume, sell_volume = source.sell_volume,
    first_agg_id = source.first_agg_id, last_agg_id = source.last_agg_id,
    updated_at = source.updated_at, repaired = true
WHEN NOT MATCHED THEN INSERT (
    symbol, window_start, window_end, open, high, low, close, volume, quote_volume, vwap,
    trade_count, buy_volume, sell_volume, first_agg_id, last_agg_id, updated_at, repaired
) VALUES (
    source.symbol, source.window_start, source.window_end, source.open, source.high, source.low,
    source.close, source.volume, source.quote_volume, source.vwap, source.trade_count,
    source.buy_volume, source.sell_volume, source.first_agg_id, source.last_agg_id, source.updated_at, true
)
