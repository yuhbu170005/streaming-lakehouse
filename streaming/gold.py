from pyspark.sql import functions as F

from streaming.common import iceberg, merge, writer

KEYS = ("symbol", "window_start")


def candles(trades, *, repaired=False):
    order = F.struct("trade_time", "agg_trade_id")
    volume = F.col("quantity")
    zero = F.lit(0).cast("decimal(38,18)")
    group = trades.groupBy("symbol", F.window("trade_time", "1 minute")).agg(
        F.min_by("price", order).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", order).alias("close"),
        F.sum(volume).alias("volume"),
        F.sum(
            F.col("price").cast("decimal(18,8)") * volume.cast("decimal(18,8)")
        ).alias("quote_volume"),
        F.sum("trade_count").alias("trade_count"),
        F.sum(F.when(~F.col("is_buyer_maker"), volume).otherwise(zero)).alias(
            "buy_volume"
        ),
        F.sum(F.when(F.col("is_buyer_maker"), volume).otherwise(zero)).alias(
            "sell_volume"
        ),
        F.min("agg_trade_id").alias("first_agg_id"),
        F.max("agg_trade_id").alias("last_agg_id"),
    )
    return (
        group.withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn(
            "vwap",
            (
                F.col("quote_volume").cast("decimal(28,8)")
                / F.col("volume").cast("decimal(28,8)")
            ).cast("decimal(38,8)"),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("repaired", F.lit(repaired))
    )


def start(spark):
    source = iceberg(spark, "silver.aggtrade").withWatermark("trade_time", "30 seconds")
    return (
        writer(candles(source), "gold", checkpoint="gold-v2")
        .outputMode("update")
        .foreachBatch(
            lambda batch, _: merge(
                batch,
                "gold.candles_1m",
                KEYS,
                update=True,
                update_when="AND NOT t.repaired",
            )
        )
        .start()
    )
