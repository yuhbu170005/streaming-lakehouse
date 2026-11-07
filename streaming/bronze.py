from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from streaming.common import RAW, kafka, writer


def start(spark):
    schema = Path("schemas/aggtrade-v1.avsc").read_text()
    # The compatible v2 appends an optional field. The v1 prefix is unchanged.
    raw = kafka(spark, RAW)
    decoded = raw.withColumn(
        "record",
        F.when(
            (F.length("value") > 5) & (F.hex(F.substring("value", 1, 1)) == "00"),
            from_avro(
                F.substring("value", 6, 2147483647), schema, {"mode": "PERMISSIVE"}
            ),
        ),
    )
    frame = decoded.select(
        F.col("record.e").alias("e"),
        F.col("record.E").alias("E_ms"),
        F.col("record.s").alias("symbol"),
        F.col("record.a").alias("agg_trade_id"),
        F.col("record.p").alias("price"),
        F.col("record.q").alias("quantity"),
        F.col("record.f").alias("first_trade_id"),
        F.col("record.l").alias("last_trade_id"),
        F.col("record.T").alias("T_ms"),
        F.col("record.m").alias("is_buyer_maker"),
        F.col("record.is_backfill").alias("is_backfill"),
        F.col("record.producer_id").alias("producer_id"),
        F.col("record.conn_id").alias("conn_id"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.coalesce(F.timestamp_millis("record.ingest_ts"), F.col("timestamp")).alias(
            "ingest_ts"
        ),
        F.current_timestamp().alias("bronze_ts"),
        F.col("value").alias("payload"),
        F.conv(F.hex(F.substring("value", 2, 4)), 16, 10)
        .cast("long")
        .alias("schema_id"),
    )
    return (
        writer(frame, "bronze")
        .format("iceberg")
        .option("fanout-enabled", "true")
        .outputMode("append")
        .toTable("lake.bronze.aggtrade_raw")
    )
