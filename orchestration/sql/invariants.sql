WITH keys AS (
    SELECT symbol, agg_trade_id, count(*) AS copies
    FROM lake.silver.aggtrade GROUP BY 1, 2
),
ids AS (
    SELECT symbol, agg_trade_id,
        lag(agg_trade_id) OVER (PARTITION BY symbol ORDER BY agg_trade_id) AS previous_id
    FROM keys
),
unexplained AS (
    SELECT i.symbol, i.agg_trade_id
    FROM ids i
    LEFT JOIN lake.ops.gap_events g ON i.symbol = g.symbol
        AND i.previous_id + 1 >= g.from_id AND i.agg_trade_id - 1 <= g.to_id
        AND g.backfilled_at IS NULL
    WHERE i.agg_trade_id > i.previous_id + 1 AND g.symbol IS NULL
),
valid_bronze AS (
    SELECT b.symbol, b.agg_trade_id, min(b.bronze_ts) AS bronze_ts
    FROM lake.bronze.aggtrade_raw b
    LEFT JOIN lake.ops.dq_results d ON b.kafka_topic = d.kafka_topic
        AND b.kafka_partition = d.kafka_partition AND b.kafka_offset = d.kafka_offset
    WHERE d.kafka_offset IS NULL
    GROUP BY 1, 2
),
silver_candles AS (
    SELECT symbol, date_trunc('minute', trade_time) AS window_start,
        sum(quantity) AS volume, sum(trade_count) AS trade_count
    FROM lake.silver.aggtrade
    GROUP BY 1, 2
)
SELECT 'duplicate_bronze_offsets' AS check_name, count(*) AS violations
FROM (
    SELECT kafka_topic, kafka_partition, kafka_offset
    FROM lake.bronze.aggtrade_raw GROUP BY 1, 2, 3 HAVING count(*) > 1
)
UNION ALL
SELECT 'duplicate_silver_keys' AS check_name, count_if(copies > 1) AS violations FROM keys
UNION ALL
SELECT 'unexplained_id_gaps', count(*) FROM unexplained
UNION ALL
SELECT 'silver_keys_missing_bronze', count(*)
FROM keys s
LEFT JOIN valid_bronze b ON s.symbol = b.symbol AND s.agg_trade_id = b.agg_trade_id
WHERE b.symbol IS NULL
UNION ALL
SELECT 'valid_bronze_keys_missing_silver', count(*)
FROM valid_bronze b
LEFT JOIN keys s ON b.symbol = s.symbol AND b.agg_trade_id = s.agg_trade_id
WHERE s.symbol IS NULL AND b.bronze_ts < current_timestamp - INTERVAL '1' MINUTE
UNION ALL
SELECT 'closed_gold_silver_differences', count(*)
FROM lake.gold.candles_1m g
JOIN silver_candles s ON g.symbol = s.symbol AND g.window_start = s.window_start
WHERE g.window_end < current_timestamp - INTERVAL '1' MINUTE
    AND (g.volume <> s.volume OR g.trade_count <> s.trade_count)
