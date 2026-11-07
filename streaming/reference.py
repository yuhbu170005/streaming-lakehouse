from pyspark.sql import functions as F

from streaming.common import GAP, REF, kafka, merge, writer


def start_klines(spark):
    schema = (
        "symbol STRING, kline STRUCT<t:BIGINT,x:BOOLEAN,o:DECIMAL(38,18),"
        "h:DECIMAL(38,18),l:DECIMAL(38,18),c:DECIMAL(38,18),v:DECIMAL(38,18),n:BIGINT>"
    )
    frame = kafka(spark, REF).select(
        F.from_json(F.col("value").cast("string"), schema).alias("r")
    )
    candles = frame.filter("r.kline.x = true").select(
        F.col("r.symbol").alias("symbol"),
        F.timestamp_millis("r.kline.t").alias("window_start"),
        F.col("r.kline.o").alias("open"),
        F.col("r.kline.h").alias("high"),
        F.col("r.kline.l").alias("low"),
        F.col("r.kline.c").alias("close"),
        F.col("r.kline.v").alias("volume"),
        F.col("r.kline.n").alias("trade_count"),
    )
    keys = ("symbol", "window_start")
    return (
        writer(candles, "reference")
        .foreachBatch(
            lambda batch, _: merge(batch.dropDuplicates(keys), "ref.klines_1m", keys)
        )
        .start()
    )


def start_gaps(spark):
    schema = "symbol STRING, from_id BIGINT, to_id BIGINT, detected_at BIGINT, backfilled_at BIGINT"
    frame = kafka(spark, GAP).select(
        F.from_json(F.col("value").cast("string"), schema).alias("g")
    )
    keys = ("symbol", "from_id")
    return (
        writer(frame.select("g.*"), "gaps")
        .foreachBatch(lambda batch, _: store_gaps(batch, keys))
        .start()
    )


def store_gaps(batch, keys):
    aggregated = batch.groupBy(*keys, "to_id").agg(
        F.min("detected_at").alias("detected_at"),
        F.max("backfilled_at").alias("backfilled_at"),
    )
    converted = aggregated.select(
        "symbol",
        "from_id",
        "to_id",
        F.timestamp_millis("detected_at").alias("detected_at"),
        F.timestamp_millis("backfilled_at").alias("backfilled_at"),
    )
    merge(
        converted,
        "ops.gap_events",
        keys,
        update=True,
        update_when="AND t.backfilled_at IS NULL AND s.backfilled_at IS NOT NULL",
    )
