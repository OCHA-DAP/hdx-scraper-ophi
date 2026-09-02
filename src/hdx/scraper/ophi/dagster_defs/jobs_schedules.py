"""Jobs, schedule, and sensor tying the asset graph together.

The pipeline runs once a year, in late October, on the weekday nearest the 22nd (the
midpoint of the 20th-24th window this has historically run in) - not expressible as a plain
cron string, so the schedule fires daily across that window and skips every day but the
computed target.

The per-country partitions are only known once `standardised_mpi_data` has run (they come
from the parsed country list), so they can't be scheduled directly: the schedule triggers
the "core" job, and a run-status sensor fans out one run per registered country partition
once that job succeeds - the standard Dagster mechanism for a partition set discovered at
runtime.
"""

from datetime import date, timedelta

from dagster import (
    AssetSelection,
    DagsterRunStatus,
    RunRequest,
    ScheduleEvaluationContext,
    SkipReason,
    define_asset_job,
    run_status_sensor,
    schedule,
)

from hdx.scraper.ophi.dagster_defs.assets import country_mpi_dataset, country_partitions

core_job = define_asset_job(
    "ophi_core_job",
    selection=AssetSelection.all() - AssetSelection.assets(country_mpi_dataset),
)
country_job = define_asset_job(
    "ophi_country_job",
    selection=AssetSelection.assets(country_mpi_dataset),
    partitions_def=country_partitions,
)


def target_run_day(year: int) -> date:
    """The weekday nearest 22 October, shifted off a weekend the way an "observed"
    holiday would be (Saturday -> the preceding Friday, Sunday -> the following Monday).
    """
    target = date(year, 10, 22)
    if target.weekday() == 5:  # Saturday
        return target - timedelta(days=1)
    if target.weekday() == 6:  # Sunday
        return target + timedelta(days=1)
    return target


@schedule(cron_schedule="0 6 18-26 10 *", job=core_job, execution_timezone="UTC")
def ophi_annual_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.date()
    if scheduled_date != target_run_day(scheduled_date.year):
        return SkipReason(
            f"{scheduled_date} is not this year's scheduled OPHI run day "
            f"({target_run_day(scheduled_date.year)})"
        )
    return RunRequest()


@run_status_sensor(
    monitored_jobs=[core_job],
    run_status=DagsterRunStatus.SUCCESS,
    request_job=country_job,
)
def ophi_country_fanout_sensor(context):
    partition_keys = context.instance.get_dynamic_partitions(country_partitions.name)
    return [
        RunRequest(run_key=f"{context.dagster_run.run_id}-{iso3}", partition_key=iso3)
        for iso3 in partition_keys
    ]
