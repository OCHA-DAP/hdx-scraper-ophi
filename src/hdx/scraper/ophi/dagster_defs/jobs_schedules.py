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

from collections import Counter
from datetime import date, timedelta

from dagster import (
    AssetSelection,
    DagsterEventType,
    DagsterRunStatus,
    OpExecutionContext,
    RunRequest,
    RunsFilter,
    ScheduleEvaluationContext,
    SkipReason,
    define_asset_job,
    in_process_executor,
    job,
    op,
    run_status_sensor,
    schedule,
)

from hdx.scraper.ophi.dagster_defs.assets import country_mpi_dataset, country_partitions

core_job = define_asset_job(
    "ophi_core_job",
    selection=AssetSelection.all() - AssetSelection.assets(country_mpi_dataset),
    # RetrieverResource is one shared instance for the whole run holding one scratch
    # folder/batch id (see its docstring) - under the default multiprocess executor,
    # each asset gets its own subprocess and its own resource instance, so e.g. one
    # asset's teardown can delete the shared folder out from under another asset still
    # writing to it. in_process_executor keeps every asset in this job on the one
    # process/resource instance the design assumes, matching how materialize() in
    # tests/test_dagster_defs.py already runs it.
    executor_def=in_process_executor,
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


@op
def summarize_country_results(context: OpExecutionContext) -> None:
    """Airflow's equivalent DAG has one end-of-run task that reports a consolidated
    per-country outcome and inlines any failed instance's full log (see
    airflow_defs/dag.py's summarize_country_results). There's no direct Dagster
    equivalent to wire up automatically: each country_mpi_dataset partition is its own
    separate *run* (via ophi_country_fanout_sensor), not a step within one run, so there
    is no single run whose task graph this could sit downstream of. This op is instead
    a standalone job (country_results_summary_job below) meant to be run manually, or on
    a schedule, once the fan-out is believed complete - not chained automatically.

    Finds every ophi_country_job run tagged with each registered partition (looking only
    at the latest run per partition), counts outcomes, and for any FAILURE run fetches
    the failure message and the full step compute log via the instance's own
    ComputeLogManager - the same "everything visible in one place" result as Airflow's
    version, using Dagster's own APIs instead of reading log files directly (this works
    with any configured ComputeLogManager backend - local, or e.g. AzureBlobComputeLogManager
    in a real deployment - not just local disk).
    """
    instance = context.instance
    iso3_list = sorted(instance.get_dynamic_partitions(country_partitions.name))
    counts: Counter = Counter()
    missing = []
    failed_run_ids: dict[str, str] = {}

    for iso3 in iso3_list:
        runs = instance.get_runs(
            filters=RunsFilter(
                job_name=country_job.name, tags={"dagster/partition": iso3}
            ),
            limit=1,
        )
        if not runs:
            missing.append(iso3)
            counts["never_run"] += 1
            continue
        run = runs[0]
        if run.status == DagsterRunStatus.SUCCESS:
            counts["success"] += 1
        elif run.status == DagsterRunStatus.FAILURE:
            counts["failed"] += 1
            failed_run_ids[iso3] = run.run_id
        else:
            counts[run.status.value] += 1

    context.log.info(
        f"country_mpi_dataset summary: {len(iso3_list)} countries total, {dict(counts)}."
    )
    if missing:
        context.log.info(f"Countries never run: {', '.join(missing)}")

    for iso3, run_id in failed_run_ids.items():
        context.log.info(f"--- {iso3} (run {run_id}) failure ---")
        for record in instance.get_records_for_run(
            run_id, of_type=DagsterEventType.STEP_FAILURE
        ).records:
            error = record.event_log_entry.dagster_event.event_specific_data.error
            if error:
                context.log.info(error.to_string())

        for record in instance.get_records_for_run(
            run_id, of_type=DagsterEventType.LOGS_CAPTURED
        ).records:
            data = record.event_log_entry.dagster_event.event_specific_data
            if "country_mpi_dataset" not in data.step_keys:
                continue
            log_key = instance.compute_log_manager.build_log_key_for_run(
                run_id, data.file_key
            )
            log_data = instance.compute_log_manager.get_log_data(log_key)
            if log_data.stderr:
                context.log.info(
                    f"--- {iso3} (run {run_id}) full log ---\n"
                    + log_data.stderr.decode(errors="replace")
                )


@job
def country_results_summary_job():
    summarize_country_results()
