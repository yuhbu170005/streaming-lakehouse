import argparse
import os
import subprocess
import time
from pathlib import Path

from trino.dbapi import connect
from trino.exceptions import TrinoConnectionError, TrinoQueryError

ROOT = Path(__file__).resolve().parent
TABLES = (
    ("bronze", "aggtrade_raw"),
    ("silver", "aggtrade"),
    ("gold", "candles_1m"),
    ("ref", "klines_1m"),
    ("ops", "gap_events"),
    ("ops", "dq_results"),
    ("recon", "candle_vs_kline"),
)


def execute(sql):
    connection = connect(
        host=os.getenv("TRINO_HOST", "127.0.0.1"),
        port=int(os.getenv("TRINO_PORT", "18080")),
        user="lakehouse",
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        connection.close()


def sql_file(name):
    # These queries are read-only or replay-safe MERGEs.
    sql = (ROOT / "sql" / f"{name}.sql").read_text()
    for delay in (2, 5, 10):
        try:
            return execute(sql)
        except TrinoConnectionError:
            time.sleep(delay)
        except TrinoQueryError as error:
            if error.error_name not in {"ICEBERG_COMMIT_ERROR", "SERVER_STARTING_UP"}:
                raise
            time.sleep(delay)
    return execute(sql)


def repair():
    return sql_file("repair")


def reconcile():
    sql_file("reconciliation")
    return execute(
        "SELECT status, count(*) FROM lake.recon.candle_vs_kline GROUP BY 1 ORDER BY 1"
    )


def invariants():
    violations = sql_file("invariants")
    for name, count in violations:
        print(f"{name}: {count}", flush=True)
    return violations


def maintenance(*, daily=False):
    procedures = (
        {
            "expire_snapshots": "retention_threshold => '3d'",
            "remove_orphan_files": "retention_threshold => '3d'",
            "optimize_manifests": "",
        }
        if daily
        else {"optimize": "file_size_threshold => '64MB'"}
    )
    for schema, table in TABLES:
        fq_name = f"lake.{schema}.{table}"
        files = f'SELECT count(*) FROM lake.{schema}."{table}$files"'
        for action, arguments in procedures.items():
            before = execute(files)[0][0]
            execute(f"ALTER TABLE {fq_name} EXECUTE {action}({arguments})")
            after = execute(files)[0][0]
            execute(
                f"INSERT INTO lake.ops.maintenance_log VALUES ('{schema}.{table}', "
                f"'{action}', {before}, {after}, current_timestamp(6))"
            )
            print(f"{fq_name} {action}: {before} -> {after}", flush=True)


def dbt(*arguments):
    subprocess.run(
        [
            "dbt",
            *arguments,
            "--project-dir",
            "dbt_project",
            "--profiles-dir",
            "dbt_project",
        ],
        check=True,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "operation", choices=("recon", "invariants", "compact", "daily", "dbt")
    )
    operation = parser.parse_args().operation
    if operation == "recon":
        repair()
        print(reconcile())
    elif operation == "invariants":
        if any(count for _, count in invariants()):
            raise SystemExit(1)
    elif operation == "dbt":
        dbt("build")
    else:
        maintenance(daily=operation == "daily")


if __name__ == "__main__":
    main()
