"""Exercise recoveries only in the isolated Compose demo project."""

import argparse
import asyncio
import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import aiohttp

from orchestration.operations import execute, repair, sql_file
from producer.contracts import RAW, now_ms
from producer.publish import create_publisher

COMPOSE = [
    "docker",
    "compose",
    "-p",
    "streaming-lakehouse-demo",
    "-f",
    "docker-compose.yml",
    "-f",
    "compose.demo.yml",
    "--profile",
    "core",
    "--profile",
    "query",
]
SYMBOL = "DEMOUSDT"


def command(*args, capture=False, env=None):
    result = subprocess.run(
        args, check=True, text=True, capture_output=capture, env=env, timeout=180
    )
    return result.stdout.strip() if capture else None


def compose(*args, **kwargs):
    return command(*COMPOSE, *args, **kwargs)


def eventually(query, *, timeout=300):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            value = query()
            if value:
                return value
        except Exception as error:
            print(f"Retry: {type(error).__name__}: {error}", flush=True)
        time.sleep(3)
    raise TimeoutError("expected warehouse condition did not hold")


def head():
    return execute(
        f"SELECT max(agg_trade_id) FROM lake.silver.aggtrade WHERE symbol = '{SYMBOL}'"
    )[0][0]


def check_range(first, last):
    rows, unique = execute(
        "SELECT count(*), count(DISTINCT agg_trade_id) FROM lake.silver.aggtrade "
        f"WHERE symbol = '{SYMBOL}' AND agg_trade_id BETWEEN {first} AND {last}"
    )[0]
    return rows == unique == last - first + 1


def assert_invariants():
    results = dict(sql_file("invariants"))
    print(f"Warehouse invariants: {results}", flush=True)
    return not any(results.values())


async def publish_raw(event=None):
    os.environ["SCHEMA_REGISTRY_URL"] = "http://127.0.0.1:28081"
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "127.0.0.1:29092"
    async with aiohttp.ClientSession() as http:
        publisher = await create_publisher(http)
        if event is None:
            await asyncio.to_thread(
                publisher.client.produce, RAW, key=SYMBOL, value=b"invalid-avro"
            )
            await asyncio.to_thread(publisher.client.flush, 15)
        else:
            await publisher.send([(RAW, event["s"], event)])


def recovery(action):
    first = eventually(head)
    started = time.monotonic()
    action()
    restored = time.monotonic()
    last = eventually(
        lambda: (current := head()) and current >= first + 100 and current
    )
    eventually(lambda: check_range(first, last))
    print(
        f"Silver range [{first}, {last}] complete after {time.monotonic() - restored:.2f}s "
        f"from service restore; action took {restored - started:.2f}s",
        flush=True,
    )


def silver_kill():
    def batch_active():
        status = compose(
            "exec",
            "-T",
            "silver",
            "python3",
            "-c",
            "import urllib.request;print(urllib.request.urlopen('http://localhost:9110/status').read().decode())",
            capture=True,
        )
        return json.loads(status).get("silver", {}).get("isTriggerActive")

    eventually(batch_active, timeout=60)
    print("Silver reported an active trigger immediately before SIGKILL", flush=True)
    compose("kill", "-s", "SIGKILL", "silver")
    compose("up", "-d", "--wait", "silver")


def producer_cut():
    producer = compose("ps", "-q", "producer", capture=True)
    network = "streaming-lakehouse-demo_default"
    before = eventually(head)
    command("docker", "network", "disconnect", network, producer)
    try:
        time.sleep(120)
    finally:
        command(
            "docker", "network", "connect", "--alias", "producer", network, producer
        )
    restored = time.monotonic()
    upper = int(
        compose(
            "exec",
            "-T",
            "fixture",
            "python",
            "-c",
            "from producer.fixture_server import Market; print(Market().head())",
            capture=True,
        )
    )
    assert upper - before >= 1100, (before, upper)
    eventually(lambda: check_range(before, upper), timeout=300)
    elapsed = time.monotonic() - restored
    assert elapsed <= 300
    gap_count, recovered_ids = execute(
        "SELECT count(*), coalesce(sum(to_id - from_id + 1), 0) FROM lake.ops.gap_events "
        f"WHERE symbol = '{SYMBOL}' AND from_id <= {upper} AND to_id >= {before} "
        "AND backfilled_at IS NOT NULL"
    )[0]
    assert gap_count and recovered_ids >= 1000, (gap_count, recovered_ids)
    print(
        f"Silver range [{before}, {upper}] complete in {elapsed:.2f}s after network restore; "
        f"{gap_count} recovered gaps covered {recovered_ids} IDs",
        flush=True,
    )
    repair_started = time.monotonic()

    def candles_repaired():
        repair()
        sql_file("reconciliation")
        count = execute(
            "WITH affected AS ("
            "SELECT DISTINCT symbol, date_trunc('minute', trade_time) AS window_start "
            "FROM lake.silver.aggtrade "
            f"WHERE symbol = '{SYMBOL}' AND agg_trade_id BETWEEN {before} AND {upper} "
            "AND is_backfill) "
            "SELECT count(*) FROM affected a "
            "JOIN lake.gold.candles_1m g ON a.symbol=g.symbol AND a.window_start=g.window_start "
            "JOIN lake.recon.candle_vs_kline r ON a.symbol=r.symbol AND a.window_start=r.window_start "
            "WHERE g.repaired AND r.status = 'match'"
        )[0][0]
        return count

    matched = eventually(candles_repaired, timeout=180)
    print(
        f"Repaired and reconciled {matched} closed outage candles in "
        f"{time.monotonic() - repair_started:.2f}s after Silver recovery",
        flush=True,
    )


def poison():
    count_sql = "SELECT count(*) FROM lake.ops.dq_results WHERE error_reason = 'missing_fields_or_bad_avro'"
    before = execute(count_sql)[0][0]
    asyncio.run(publish_raw())
    eventually(lambda: execute(count_sql)[0][0] > before)
    result = compose(
        "exec",
        "-T",
        "redpanda",
        "rpk",
        "topic",
        "consume",
        "binance.aggtrade.dlq.v1",
        "-X",
        "brokers=redpanda:9092",
        "--offset=-1",
        "-n",
        "1",
        capture=True,
    )
    assert "missing_fields_or_bad_avro" in result
    print("Malformed Avro reached Kafka DLQ and Iceberg DQ table", flush=True)


def gold_late_count():
    metrics = compose(
        "exec",
        "-T",
        "gold",
        "python3",
        "-c",
        "import urllib.request;print(urllib.request.urlopen('http://localhost:9110/metrics').read().decode())",
        capture=True,
    )
    return sum(
        float(line.split()[-1])
        for line in metrics.splitlines()
        if line.startswith("spark_late_events_total")
    )


def late_data():
    # This symbol has no fixture trades, so synthetic late records form their own ID sequence.
    late_symbol = "LATEUSDT"
    minute_ms = (now_ms() // 60_000 - 3) * 60_000
    target = (
        f"symbol = '{late_symbol}' AND to_unixtime(window_start) = {minute_ms // 1000}"
    )
    before = execute(
        f"SELECT volume, trade_count FROM lake.gold.candles_1m WHERE {target}"
    )
    watermark_drops_before = gold_late_count()
    trade_id = execute(
        f"SELECT coalesce(max(agg_trade_id), 0) + 1 FROM lake.silver.aggtrade WHERE symbol='{late_symbol}'"
    )[0][0]
    event = {
        "e": "aggTrade",
        "E": minute_ms + 1000,
        "s": late_symbol,
        "a": trade_id,
        "p": "100",
        "q": "0.125",
        "f": 1,
        "l": 1,
        "T": minute_ms + 1000,
        "m": False,
        "ingest_ts": now_ms(),
        "producer_id": "chaos-late",
        "conn_id": "fixture",
        "is_backfill": False,
    }
    asyncio.run(publish_raw(event))
    eventually(
        lambda: execute(
            f"SELECT count(*) FROM lake.silver.aggtrade WHERE symbol = '{late_symbol}' AND agg_trade_id = {trade_id}"
        )[0][0]
    )
    eventually(lambda: gold_late_count() > watermark_drops_before, timeout=120)
    assert (
        execute(f"SELECT volume, trade_count FROM lake.gold.candles_1m WHERE {target}")
        == before
    )
    print(
        "Late record persisted in Silver, Gold stayed unchanged, watermark metric incremented"
    )


def schema_evolution():
    first = eventually(head)
    before = execute("SELECT count(DISTINCT schema_id) FROM lake.bronze.aggtrade_raw")[
        0
    ][0]
    env = {**os.environ, "AGGTRADE_SCHEMA_VERSION": "2"}
    compose("up", "-d", "--no-deps", "--force-recreate", "--wait", "producer", env=env)
    try:
        eventually(
            lambda: (
                execute(
                    "SELECT count(DISTINCT schema_id) FROM lake.bronze.aggtrade_raw"
                )[0][0]
                > before
            )
        )
        last = eventually(
            lambda: (current := head()) and current > first + 100 and current
        )
        eventually(lambda: check_range(first, last))
    finally:
        compose("up", "-d", "--no-deps", "--force-recreate", "--wait", "producer")
    print(
        "Avro v2 read through Bronze and Silver with contiguous trade IDs", flush=True
    )


def main():
    parser = argparse.ArgumentParser()
    cases = {
        "spark-kill": lambda: recovery(silver_kill),
        "broker-restart": lambda: recovery(lambda: compose("restart", "redpanda")),
        "producer-cut": producer_cut,
        "minio-restart": lambda: recovery(lambda: compose("restart", "minio")),
        "poison": poison,
        "late-data": late_data,
        "schema-v2": schema_evolution,
    }
    parser.add_argument("case", choices=cases)
    args = parser.parse_args()
    os.environ["TRINO_HOST"] = "127.0.0.1"
    os.environ["TRINO_PORT"] = "28080"
    assert compose("ps", "-q", "fixture", capture=True), "Start make demo first"
    print(f"{datetime.now(UTC).isoformat()} synthetic {args.case}", flush=True)
    eventually(head)
    cases[args.case]()
    eventually(assert_invariants, timeout=120)


if __name__ == "__main__":
    assert Path("compose.demo.yml").is_file()
    main()
