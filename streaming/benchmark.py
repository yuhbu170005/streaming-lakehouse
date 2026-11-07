"""Measure the isolated Spark insert-only Silver merge on synthetic batches."""

import json
from statistics import median
from time import monotonic
from uuid import uuid4

from pyspark.sql import functions as F

from streaming.common import merge
from streaming.silver import KEY
from streaming.tables import TABLES

SIZES = (100, 1000, 10000)


def run(spark):
    reports = []
    for size in SIZES:
        table = f"ops.silver_benchmark_{uuid4().hex}"
        ddl = TABLES["silver.aggtrade"]
        spark.sql(
            f"CREATE TABLE lake.{table} ({ddl} TBLPROPERTIES ('format-version'='2')"
        )
        try:
            timings = []
            for repeat in range(4):
                offset = repeat * size
                incoming = (
                    spark.range(offset, offset + size)
                    .select(
                        F.concat(
                            F.lit("LOAD"), (F.col("id") % 20).cast("string")
                        ).alias("symbol"),
                        F.col("id").alias("agg_trade_id"),
                        F.lit("123.45").cast("decimal(38,18)").alias("price"),
                        F.lit("0.01").cast("decimal(38,18)").alias("quantity"),
                        F.col("id").alias("first_trade_id"),
                        F.col("id").alias("last_trade_id"),
                        F.lit(1).cast("long").alias("trade_count"),
                        F.current_timestamp().alias("trade_time"),
                        F.current_timestamp().alias("event_time"),
                        F.lit(False).alias("is_buyer_maker"),
                        F.lit(False).alias("is_backfill"),
                        F.current_timestamp().alias("ingest_ts"),
                        F.current_timestamp().alias("processed_at"),
                    )
                    .cache()
                )
                try:
                    incoming.count()
                    start = monotonic()
                    merge(incoming, table, KEY)
                    timings.append(round((monotonic() - start) * 1000, 2))
                finally:
                    incoming.unpersist()
            assert spark.table(f"lake.{table}").count() == size * 4
            reports.append(
                {
                    "batch_rows": size,
                    "warmup_merge_ms": timings[0],
                    "measured_merge_ms": timings[1:],
                    "median_ms": median(timings[1:]),
                }
            )
        finally:
            spark.sql(f"DROP TABLE IF EXISTS lake.{table} PURGE")
    print(json.dumps({"mode": "synthetic", "silver_merge": reports}), flush=True)
