"""Measure live and synthetic lakehouse runs using warehouse results and Docker stats."""

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from urllib.request import urlopen

from orchestration.operations import execute, repair, sql_file

SPOT = "https://data-api.binance.vision"


def select_symbols(count, *, synthetic):
    if synthetic:
        return [f"LOAD{index:03d}USDT" for index in range(count)]
    with urlopen(f"{SPOT}/api/v3/exchangeInfo", timeout=30) as response:
        exchange = json.load(response)
    active = {
        row["symbol"]
        for row in exchange["symbols"]
        if row["status"] == "TRADING"
        and row["quoteAsset"] == "USDT"
        and row.get("isSpotTradingAllowed")
    }
    with urlopen(f"{SPOT}/api/v3/ticker/24hr?type=MINI", timeout=30) as response:
        tickers = json.load(response)
    eligible = [row for row in tickers if row["symbol"] in active]
    eligible.sort(key=lambda row: Decimal(row["quoteVolume"]), reverse=True)
    if len(eligible) < count:
        raise ValueError(f"Only {len(eligible)} eligible symbols available")
    return [row["symbol"] for row in eligible[:count]]


def system_stats(project):
    rows = subprocess.check_output(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}"], text=True
    ).splitlines()
    return [
        {"name": row["Name"], "cpu": row["CPUPerc"], "memory": row["MemUsage"]}
        for line in rows
        if (row := json.loads(line))["Name"].startswith(f"{project}-")
    ]


def measure(args):
    if args.duration <= 0 or args.warmup < 0:
        raise ValueError("duration must be positive and warmup must be nonnegative")
    print(
        f"Waiting {args.warmup}s before {args.duration}s {args.mode} measurement",
        flush=True,
    )
    time.sleep(args.warmup)
    started = datetime.now(UTC)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    hardware = subprocess.check_output(
        [
            "docker",
            "info",
            "--format",
            "{{.Architecture}} {{.NCPU}} CPU {{.MemTotal}} bytes",
        ],
        text=True,
    ).strip()
    samples = []
    deadline = time.monotonic() + args.duration
    while time.monotonic() < deadline:
        samples.append(
            {
                "at": datetime.now(UTC).isoformat(),
                "containers": system_stats(args.project),
            }
        )
        time.sleep(max(0, min(60, deadline - time.monotonic())))
    ended = datetime.now(UTC)
    start_sql = f"from_iso8601_timestamp('{started.isoformat()}')"
    end_sql = f"from_iso8601_timestamp('{ended.isoformat()}')"
    elapsed = (ended - started).total_seconds()
    rows, symbols, latency, recovered = execute(
        "SELECT count(*), count(DISTINCT symbol), "
        "approx_percentile(date_diff('millisecond', trade_time, processed_at), "
        "ARRAY[0.5,0.95,0.99]), count_if(is_backfill) "
        f"FROM lake.silver.aggtrade WHERE processed_at >= {start_sql} "
        f"AND processed_at < {end_sql}"
    )[0]
    repair()
    sql_file("reconciliation")
    statuses = dict(
        execute(
            "SELECT status, count(*) FROM lake.recon.candle_vs_kline "
            f"WHERE window_start >= {start_sql} AND window_start < {end_sql} "
            "GROUP BY 1"
        )
    )
    report = {
        "mode": args.mode,
        "commit": commit,
        "docker_hardware": hardware,
        "started_utc": started.isoformat(),
        "ended_utc": ended.isoformat(),
        "elapsed_seconds": elapsed,
        "warmup_seconds": args.warmup,
        "silver_rows": rows,
        "symbols_seen": symbols,
        "silver_rows_per_second": rows / elapsed,
        "latency_ms_p50_p95_p99": latency,
        "backfilled_silver_rows": recovered,
        "reconciliation": statuses,
        "container_samples": samples,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "container_samples"}
        )
    )


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="action", required=True)
    symbols = commands.add_parser("symbols")
    symbols.add_argument("--count", type=int, required=True)
    symbols.add_argument("--synthetic", action="store_true")
    run = commands.add_parser("run")
    run.add_argument("--mode", choices=("live", "synthetic"), required=True)
    run.add_argument("--project", required=True)
    run.add_argument("--duration", type=int, default=1800)
    run.add_argument("--warmup", type=int, default=60)
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "symbols":
        if not 1 <= args.count <= 200:
            parser.error("count must be between 1 and 200")
        print("symbols:")
        for symbol in select_symbols(args.count, synthetic=args.synthetic):
            print(f"  - {symbol}")
    else:
        measure(args)


if __name__ == "__main__":
    main()
