from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetSpec,
    DefaultScheduleStatus,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    asset_check,
    job,
    op,
)

from orchestration import operations


@op(retry_policy=RetryPolicy(max_retries=3, delay=15))
def repair_candles():
    operations.repair()


@op(retry_policy=RetryPolicy(max_retries=3, delay=15))
def reconcile_candles(context, _repair_completed):
    context.log.info(str(operations.reconcile()))


@job
def reconciliation():
    reconcile_candles(repair_candles())


@op(retry_policy=RetryPolicy(max_retries=2, delay=15))
def compact_tables():
    operations.maintenance()


@job
def iceberg_maintenance():
    compact_tables()


@op
def expire_and_clean():
    operations.maintenance(daily=True)


@job
def iceberg_daily():
    expire_and_clean()


@op
def build_marts():
    operations.dbt("build")
    operations.dbt("source", "freshness")


@job
def dbt_marts():
    build_marts()


@op
def report_gap_backfill(context):
    pending = operations.execute(
        "SELECT symbol, from_id, to_id, detected_at FROM lake.ops.gap_events "
        "WHERE backfilled_at IS NULL ORDER BY detected_at"
    )
    context.log.info(f"Pending durable producer backfill ranges: {pending}")


@job
def gap_backfill():
    # The producer owns the durable queue and recovers it continuously.
    report_gap_backfill()


@asset_check(asset=AssetKey("silver_aggtrade"))
def lineage_and_candles():
    results = dict(operations.invariants())
    return AssetCheckResult(passed=not any(results.values()), metadata=results)


defs = Definitions(
    assets=[AssetSpec("silver_aggtrade")],
    jobs=[reconciliation, iceberg_maintenance, iceberg_daily, dbt_marts, gap_backfill],
    asset_checks=[lineage_and_candles],
    schedules=[
        ScheduleDefinition(
            job=reconciliation,
            cron_schedule="*/5 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=iceberg_maintenance,
            cron_schedule="10 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=iceberg_daily,
            cron_schedule="30 2 * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=dbt_marts,
            cron_schedule="20 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
        ScheduleDefinition(
            job=gap_backfill,
            cron_schedule="*/2 * * * *",
            default_status=DefaultScheduleStatus.RUNNING,
        ),
    ],
)
