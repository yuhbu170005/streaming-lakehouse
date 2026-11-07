import sys

from streaming import bronze, gold, reference, silver, tables
from streaming.common import session
from streaming.metrics import Metrics


def main(job):
    spark = session(job)
    if job == "bootstrap":
        tables.create(spark)
        spark.stop()
        return
    starters = {
        "bronze": [bronze.start],
        "silver": [silver.start],
        "gold": [gold.start],
        "reference": [reference.start_klines, reference.start_gaps],
    }
    metrics = Metrics()
    spark.streams.addListener(metrics)
    for start in starters[job]:
        start(spark)
    metrics.serve(spark)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main(sys.argv[1])
