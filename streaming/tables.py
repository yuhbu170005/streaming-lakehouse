TABLES = {
    "bronze.aggtrade_raw": """
        e STRING, E_ms BIGINT, symbol STRING, agg_trade_id BIGINT, price STRING,
        quantity STRING, first_trade_id BIGINT, last_trade_id BIGINT, T_ms BIGINT,
        is_buyer_maker BOOLEAN, is_backfill BOOLEAN, producer_id STRING, conn_id STRING,
        kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_ts TIMESTAMP,
        ingest_ts TIMESTAMP, bronze_ts TIMESTAMP, payload BINARY, schema_id BIGINT
    ) PARTITIONED BY (days(ingest_ts))""",
    "silver.aggtrade": """
        symbol STRING, agg_trade_id BIGINT, price DECIMAL(38,18), quantity DECIMAL(38,18),
        first_trade_id BIGINT, last_trade_id BIGINT, trade_count BIGINT, trade_time TIMESTAMP,
        event_time TIMESTAMP, is_buyer_maker BOOLEAN, is_backfill BOOLEAN,
        ingest_ts TIMESTAMP, processed_at TIMESTAMP
    ) PARTITIONED BY (days(trade_time), bucket(8, symbol))""",
    "gold.candles_1m": """
        symbol STRING, window_start TIMESTAMP, window_end TIMESTAMP, open DECIMAL(38,18),
        high DECIMAL(38,18), low DECIMAL(38,18), close DECIMAL(38,18), volume DECIMAL(38,18),
        quote_volume DECIMAL(38,18), vwap DECIMAL(38,18), trade_count BIGINT,
        buy_volume DECIMAL(38,18), sell_volume DECIMAL(38,18), first_agg_id BIGINT,
        last_agg_id BIGINT, updated_at TIMESTAMP, repaired BOOLEAN
    ) PARTITIONED BY (days(window_start))""",
    "ref.klines_1m": """
        symbol STRING, window_start TIMESTAMP, open DECIMAL(38,18), high DECIMAL(38,18),
        low DECIMAL(38,18), close DECIMAL(38,18), volume DECIMAL(38,18), trade_count BIGINT
    ) PARTITIONED BY (days(window_start))""",
    "ops.gap_events": """
        symbol STRING, from_id BIGINT, to_id BIGINT, detected_at TIMESTAMP,
        backfilled_at TIMESTAMP)""",
    "ops.dq_results": """
        kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, symbol STRING,
        error_reason STRING, checked_at TIMESTAMP)""",
    "ops.maintenance_log": """
        table_name STRING, action STRING, files_before BIGINT, files_after BIGINT,
        executed_at TIMESTAMP)""",
    "recon.candle_vs_kline": """
        symbol STRING, window_start TIMESTAMP, open_diff DECIMAL(38,18),
        high_diff DECIMAL(38,18), low_diff DECIMAL(38,18), close_diff DECIMAL(38,18),
        volume_diff DECIMAL(38,18), trade_count_diff BIGINT, status STRING, checked_at TIMESTAMP
    ) PARTITIONED BY (days(window_start))""",
}


def create(spark):
    for namespace in sorted({table.split(".")[0] for table in TABLES}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{namespace}")
    for table, ddl in TABLES.items():
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS lake.{table} ({ddl} "
            "TBLPROPERTIES ('format-version'='2', 'write.merge.mode'='copy-on-write')"
        )
