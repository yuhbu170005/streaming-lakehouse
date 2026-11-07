import logging
import time

from prometheus_client import Gauge, start_http_server

from orchestration.operations import TABLES, execute

LATENCY = Gauge(
    "lakehouse_e2e_latency_ms", "Rolling 30 minute ingestion latency", ["quantile"]
)
GAP_AGE = Gauge("lakehouse_oldest_open_gap_seconds", "Age of the oldest open gap")
STATUS = Gauge(
    "lakehouse_reconciliation_candles",
    "Closed candles by status over 30 minutes",
    ["status"],
)
MATCH_RATE = Gauge(
    "lakehouse_reconciliation_match_rate", "Exact matches over comparable candles"
)
FILES = Gauge("lakehouse_iceberg_files", "Current data files", ["table"])
REJECTIONS = Gauge(
    "lakehouse_silver_rejections_total", "Durably logged Silver rejections"
)
READY = Gauge("lakehouse_warehouse_exporter_ready", "Last warehouse scrape succeeded")


def collect():
    (quantiles,) = execute(
        "SELECT approx_percentile(date_diff('millisecond', trade_time, processed_at), "
        "ARRAY[0.5, 0.95, 0.99]) FROM lake.silver.aggtrade "
        "WHERE processed_at > current_timestamp - INTERVAL '30' MINUTE"
    )[0]
    if quantiles is not None:
        for quantile, value in zip(("0.5", "0.95", "0.99"), quantiles, strict=True):
            LATENCY.labels(quantile).set(value)
    gap_age = execute(
        "SELECT coalesce(date_diff('second', min(detected_at), current_timestamp), 0) "
        "FROM lake.ops.gap_events WHERE backfilled_at IS NULL"
    )[0][0]
    GAP_AGE.set(gap_age)
    counts = dict(
        execute(
            "SELECT status, count(*) FROM lake.recon.candle_vs_kline "
            "WHERE window_start > current_timestamp - INTERVAL '30' MINUTE GROUP BY 1"
        )
    )
    for status in (
        "match",
        "mismatch",
        "missing_gold",
        "missing_reference",
        "no_trades",
    ):
        STATUS.labels(status).set(counts.get(status, 0))
    compared = counts.get("match", 0) + counts.get("mismatch", 0)
    MATCH_RATE.set(counts.get("match", 0) / compared if compared else float("nan"))
    REJECTIONS.set(execute("SELECT count(*) FROM lake.ops.dq_results")[0][0])
    for schema, table in TABLES:
        count = execute(f'SELECT count(*) FROM lake.{schema}."{table}$files"')[0][0]
        FILES.labels(f"{schema}.{table}").set(count)


if __name__ == "__main__":
    start_http_server(9111)
    while True:
        try:
            collect()
        except Exception:
            READY.set(0)
            logging.exception("Warehouse metrics query failed")
        else:
            READY.set(1)
        time.sleep(60)
