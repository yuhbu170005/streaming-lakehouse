"""Measure live and synthetic lakehouse runs using warehouse results and Docker stats."""

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from statistics import median
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
        ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
        text=True,
        timeout=20,
    ).splitlines()
    return [
        {"name": row["Name"], "cpu": row["CPUPerc"], "memory": row["MemUsage"]}
        for line in rows
        if (row := json.loads(line))["Name"].startswith(f"{project}-")
    ]


def save_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(path)


def measure(args):
    if args.duration <= 0 or args.warmup < 0:
        raise ValueError("duration must be positive and warmup must be nonnegative")
    progress_path = args.output.with_suffix(".progress.json")
    progress = {
        "mode": args.mode,
        "project": args.project,
        "planned_duration_seconds": args.duration,
        "status": "warming_up",
    }
    save_report(progress_path, progress)
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
        timeout=20,
    ).strip()
    samples = []
    progress.update(status="running", started_utc=started.isoformat(), commit=commit)
    print(f"Measurement started at {started.isoformat()} ({commit[:7]})", flush=True)
    deadline = time.monotonic() + args.duration
    previous = started
    while True:
        sample_time = datetime.now(UTC)
        gap = (sample_time - previous).total_seconds()
        if gap < 0 or gap > 120:
            progress.update(
                status="interrupted",
                interrupted_at=sample_time.isoformat(),
                gap_seconds=gap,
                completed_samples=len(samples),
            )
            save_report(progress_path, progress)
            raise SystemExit(
                f"Measurement interrupted by a {gap:.1f}s clock or sampling gap"
            )
        if time.monotonic() >= deadline:
            break
        sample = {"at": sample_time.isoformat()}
        try:
            sample["containers"] = system_stats(args.project)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            sample["error"] = type(error).__name__
        samples.append(sample)
        progress.update(
            last_sample_utc=sample_time.isoformat(), completed_samples=len(samples)
        )
        save_report(progress_path, progress)
        previous = sample_time
        time.sleep(max(0, min(60, deadline - time.monotonic())))
    ended = datetime.now(UTC)
    if args.settle:
        print(f"Waiting {args.settle}s for the last closed candles", flush=True)
        time.sleep(args.settle)
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
    statuses = {}
    if not args.ingest_only:
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
        "ingest_only": args.ingest_only,
        "commit": commit,
        "docker_hardware": hardware,
        "started_utc": started.isoformat(),
        "ended_utc": ended.isoformat(),
        "elapsed_seconds": elapsed,
        "settle_seconds": args.settle,
        "warmup_seconds": args.warmup,
        "silver_rows": rows,
        "symbols_seen": symbols,
        "silver_rows_per_second": rows / elapsed,
        "latency_ms_p50_p95_p99": latency,
        "backfilled_silver_rows": recovered,
        "reconciliation": statuses,
        "container_samples": samples,
    }
    save_report(args.output, report)
    progress.update(status="measured", output=str(args.output))
    save_report(progress_path, progress)
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "container_samples"}
        )
    )

    if args.expected_symbols and symbols != args.expected_symbols:
        raise SystemExit(f"Expected {args.expected_symbols} symbols, saw {symbols}")
    if args.acceptance:
        if elapsed < 21600 or not statuses.get("match"):
            raise SystemExit(
                "Acceptance requires six hours and at least one comparable candle"
            )
        if any(
            statuses.get(status, 0)
            for status in ("mismatch", "missing_gold", "missing_reference")
        ):
            raise SystemExit(f"Reconciliation acceptance failed: {statuses}")


def compact(output):
    table = "lake.bronze.aggtrade_raw"
    files = 'SELECT count(*) FROM lake.bronze."aggtrade_raw$files"'
    count = f"SELECT count(*), count(DISTINCT symbol) FROM {table}"

    def timing():
        samples = []
        result = None
        for _ in range(5):
            started = time.monotonic()
            result = execute(count)[0]
            samples.append((time.monotonic() - started) * 1000)
        return result, median(samples)

    before = execute(files)[0][0]
    rows_before, ms_before = timing()
    execute(f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '64MB')")
    after = execute(files)[0][0]
    rows_after, ms_after = timing()
    assert rows_before == rows_after, (rows_before, rows_after)
    report = {
        "mode": "synthetic",
        "table": table,
        "data_files_before": before,
        "data_files_after": after,
        "count_query_median_ms_before": ms_before,
        "count_query_median_ms_after": ms_after,
        "rows": rows_after,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


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
    run.add_argument("--settle", type=int, default=130)
    run.add_argument("--expected-symbols", type=int)
    run.add_argument("--acceptance", action="store_true")
    run.add_argument("--ingest-only", action="store_true")
    compaction = commands.add_parser("compact")
    compaction.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "symbols":
        if not 1 <= args.count <= 200:
            parser.error("count must be between 1 and 200")
        print("symbols:")
        for symbol in select_symbols(args.count, synthetic=args.synthetic):
            print(f"  - {symbol}")
    elif args.action == "compact":
        compact(args.output)
    else:
        measure(args)


if __name__ == "__main__":
    main()
