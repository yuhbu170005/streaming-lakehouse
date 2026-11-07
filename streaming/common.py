import os
import uuid

from pyspark.sql import SparkSession

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
RAW = "binance.aggtrade.raw.v1"
DLQ = "binance.aggtrade.dlq.v1"
REF = "binance.kline.ref.v1"
GAP = "binance.gap.events.v1"
CHECKPOINTS = "s3a://lakehouse/checkpoints"


def session(name):
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    settings = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lake": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lake.type": "rest",
        "spark.sql.catalog.lake.uri": os.getenv("ICEBERG_URI", "http://catalog:8181"),
        "spark.sql.catalog.lake.warehouse": "s3://lakehouse/warehouse",
        "spark.sql.catalog.lake.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.lake.s3.endpoint": endpoint,
        "spark.sql.catalog.lake.s3.path-style-access": "true",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.connection.maximum": "24",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.caseSensitive": "true",
        "spark.sql.shuffle.partitions": "4",
        "spark.ui.enabled": "false",
        "spark.sql.streaming.stopTimeout": "15000",
    }
    builder = SparkSession.builder.appName(f"lakehouse-{name}")
    for key, value in settings.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka(spark, topic):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "30000"))
        .load()
    )


def iceberg(spark, table):
    return (
        spark.readStream.format("iceberg")
        .option("streaming-max-files-per-micro-batch", "128")
        .option("streaming-max-rows-per-micro-batch", "25000")
        .load(f"lake.{table}")
    )


def writer(frame, name, *, checkpoint=None):
    return (
        frame.writeStream.queryName(name)
        .option("checkpointLocation", f"{CHECKPOINTS}/{checkpoint or name}")
        .trigger(processingTime=os.getenv("SPARK_TRIGGER", "10 seconds"))
    )


def merge(frame, table, keys, *, update=False, update_when=""):
    if frame.isEmpty():
        return
    spark = frame.sparkSession
    view = f"incoming_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    predicate = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    matched = f"WHEN MATCHED {update_when} THEN UPDATE SET *" if update else ""
    try:
        spark.sql(
            f"MERGE INTO lake.{table} t USING {view} s ON {predicate} "
            f"{matched} WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)
