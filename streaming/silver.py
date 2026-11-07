import os
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from streaming.common import DLQ, KAFKA, iceberg, merge, writer

KEY = ("symbol", "agg_trade_id")
OFFSET = ("kafka_topic", "kafka_partition", "kafka_offset")
DECIMAL = "decimal(38,18)"


def validate(batch, allowlist):
    return (
        batch.withColumn("price", F.expr(f"try_cast(price AS {DECIMAL})"))
        .withColumn("quantity", F.expr(f"try_cast(quantity AS {DECIMAL})"))
        .withColumn(
            "trade_time",
            F.timestamp_millis(
                F.when(F.col("T_ms").between(0, 253402300799999), F.col("T_ms"))
            ),
        )
        .withColumn(
            "event_time",
            F.timestamp_millis(
                F.when(F.col("E_ms").between(0, 253402300799999), F.col("E_ms"))
            ),
        )
        .withColumn(
            "trade_count",
            F.expr("try_add(try_subtract(last_trade_id, first_trade_id), 1L)"),
        )
        .withColumn(
            "error_reason",
            F.when(
                F.col("e").isNull()
                | F.col("agg_trade_id").isNull()
                | F.col("trade_time").isNull()
                | F.col("event_time").isNull()
                | F.col("trade_count").isNull()
                | F.col("is_buyer_maker").isNull()
                | F.col("is_backfill").isNull(),
                "missing_fields_or_bad_avro",
            )
            .when(F.col("e") != "aggTrade", "unexpected_event_type")
            .when(
                F.col("symbol").isNull() | ~F.col("symbol").isin(*allowlist),
                "unknown_symbol",
            )
            .when(F.col("price").isNull() | (F.col("price") <= 0), "invalid_price")
            .when(
                F.col("quantity").isNull() | (F.col("quantity") <= 0),
                "invalid_quantity",
            )
            .when(
                (F.col("trade_count") <= 0)
                | (F.col("first_trade_id") < 0)
                | (F.col("agg_trade_id") < 0),
                "invalid_trade_ids",
            )
            .when(
                F.col("trade_time") > F.col("bronze_ts") + F.expr("INTERVAL 5 SECONDS"),
                "future_trade",
            ),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def process(batch, _, allowlist):
    checked = validate(batch, allowlist).persist()
    try:
        bad = checked.filter(F.col("error_reason").isNotNull())
        if not bad.isEmpty():
            events = bad.select(
                F.coalesce("symbol", F.lit("unknown")).alias("key"),
                F.to_json(F.struct(*OFFSET, "symbol", "error_reason", "payload")).alias(
                    "value"
                ),
            )
            events.write.format("kafka").option(
                "kafka.bootstrap.servers", KAFKA
            ).option("topic", DLQ).save()
            dq = bad.select(
                *OFFSET,
                "symbol",
                "error_reason",
                F.col("processed_at").alias("checked_at"),
            )
            merge(dq, "ops.dq_results", OFFSET)
        order = Window.partitionBy(*KEY).orderBy("kafka_partition", "kafka_offset")
        columns = batch.sparkSession.table("lake.silver.aggtrade").columns
        good = (
            checked.filter(F.col("error_reason").isNull())
            .withColumn("rank", F.row_number().over(order))
            .filter("rank = 1")
            .select(*columns)
        )
        merge(good, "silver.aggtrade", KEY)
    finally:
        checked.unpersist()


def start(spark):
    # The checked-in symbols config uses a YAML list without nested values.
    allowlist = [
        line.strip()[2:]
        for line in Path(os.getenv("SYMBOLS_FILE", "/app/config/symbols.yaml"))
        .read_text()
        .splitlines()
        if line.strip().startswith("- ")
    ]
    return (
        writer(iceberg(spark, "bronze.aggtrade_raw"), "silver")
        .foreachBatch(lambda batch, epoch: process(batch, epoch, allowlist))
        .start()
    )
