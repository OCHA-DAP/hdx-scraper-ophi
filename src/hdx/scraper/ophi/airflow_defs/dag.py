"""Airflow 3.x TaskFlow DAG mirroring the Dagster asset graph in
dagster_defs/assets.py 1:1 - same 7 orchestration nodes, same read_only gating, same
annual schedule. The business logic in pipeline.py/dataset_generator.py/hapi_output.py/
hapi_dataset_generator.py is untouched; only the orchestration glue differs from
dagster_defs.

Each node below is written as a plain, undecorated function first and wrapped with
@task only inside ophi_pipeline() - this keeps the functions importable and callable
directly from tests, the same way test_dagster_defs.py calls into Dagster ops without
going through the scheduler/webserver.

Data-passing note: Dagster's assets share one process for a run and can pass Python
objects (e.g. the AdminLevel built by admin1_boundaries) directly to downstream assets.
Airflow tasks are independent processes and can only exchange JSON-serializable data via
XCom, so admin1_boundaries is kept here purely for graph-shape parity with Dagster (it
populates the AdminLevel cache on disk) and downstream tasks that need the AdminLevel
object itself (standardised_mpi_data, hapi_poverty_rate_dataset) rebuild their own via
build_admin1_boundaries() - cheap, since it reads from the scratch folder every task in
the run shares (see resources.py).
"""

import sys
from datetime import date, datetime, timedelta
from logging import getLogger
from os.path import join
from typing import Any

from airflow.sdk import DAG, TriggerRule, dag, task
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file
from loguru import logger as loguru_logger

from hdx.scraper.ophi.airflow_defs.resources import (
    build_admin1_boundaries,
    check_organization_access,
    retriever_context,
    setup_hdx_configuration,
)
from hdx.scraper.ophi.dataset_generator import DatasetGenerator
from hdx.scraper.ophi.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.ophi.hapi_output import HAPIOutput
from hdx.scraper.ophi.pipeline import Pipeline

# hdx.scraper.ophi.__main__ calls this at import time; this module never imports
# __main__, so without this call every hdx-python-api/hdx-python-utilities log message
# (download URLs, "Creating dataset: ...", read-only status, etc.) has no configured
# handler and is silently dropped rather than showing up in each task's log.
setup_logging()

# setup_logging() hardcodes a stderr sink with colorize=True and no override param.
# Airflow 3 always wraps a task's captured output as one JSON record per line (this is
# how the Task SDK ships logs to the supervisor process - not configurable away, per
# https://github.com/apache/airflow/discussions/53006), tagging each record's "level"
# purely by which stream it came from: stderr -> "error", stdout -> "info", regardless
# of the line's actual content. So writing to stderr made every one of our plain INFO
# messages show up mislabelled "error", on top of embedding ANSI colour codes as literal
# garbage in that JSON string. Re-registering loguru's sink here (reaching into its
# internals, since easy_logging doesn't expose either of these) fixes both: stdout
# instead of stderr gets the level tagged correctly by Airflow's own wrapper, so
# {level:...} is dropped from our own format string too (kept, it would just repeat
# what the wrapper's own "level" field already says); dropping {time:...} for the same
# reason avoids duplicating the wrapper's own "timestamp" field.
loguru_logger.remove()
loguru_logger.add(
    sys.stdout,
    level="INFO",
    colorize=False,
    format="{name}:{function}:{line} - {message}",
)

logger = getLogger(__name__)

READ_ONLY = (
    True  # mirrors dagster_defs.definitions' HDXConfigResource(); flip for a real run
)
# Explicit rather than left to fall back to whatever ~/.hdx_configuration.yaml says on
# whichever machine runs this - this is a comparison/testing deployment and must not
# silently resolve to prod.
HDX_SITE = "stage"


def target_run_day(year: int) -> date:
    """The weekday nearest 22 October, same logic as ophi_annual_schedule in
    dagster_defs/jobs_schedules.py."""
    target = date(year, 10, 22)
    if target.weekday() == 5:  # Saturday
        return target - timedelta(days=1)
    if target.weekday() == 6:  # Sunday
        return target + timedelta(days=1)
    return target


def _config_path(filename: str) -> str:
    return str(script_dir_plus_file(join("config", filename), Pipeline))


def _maybe_create(dataset: Dataset, batch: str) -> None:
    if READ_ONLY:
        logger.info(f"read_only=True: not writing dataset '{dataset['name']}' to HDX.")
        return
    dataset.create_in_hdx(
        remove_additional_resources=True,
        updated_by_script="HDX Scraper: OPHI",
        batch=batch,
    )


def is_target_run_day(dag_run: Any) -> bool:
    # Only a genuine scheduled tick is subject to the day gate - any other trigger kind
    # (manual, backfill, API) always runs, the same way a manual Dagster launch bypasses
    # ophi_annual_schedule's SkipReason check entirely rather than going through it.
    # dag_run.run_type, not logical_date, is what actually distinguishes these: a bare
    # CLI `airflow dags trigger` leaves logical_date=None, but Airflow's web UI trigger
    # stamps a real logical_date (now) on an equally-manual run, so checking for None
    # would incorrectly subject a UI-triggered run to the day check.
    if dag_run.run_type != "scheduled":
        return True
    scheduled_date = dag_run.logical_date.date()
    return scheduled_date == target_run_day(scheduled_date.year)


def admin1_boundaries(run_id: str) -> None:
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    with retriever_context(run_id) as r:
        build_admin1_boundaries(r.retriever).setup_from_url()


def ophi_national_excel(run_id: str) -> str:
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    with retriever_context(run_id) as r:
        pipeline = Pipeline(Configuration.read(), r.retriever, None)
        return str(pipeline.download_mpi_national())


def ophi_subnational_excel(run_id: str) -> str:
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    with retriever_context(run_id) as r:
        pipeline = Pipeline(Configuration.read(), r.retriever, None)
        return str(pipeline.download_mpi_subnational())


def ophi_trends_excel(run_id: str) -> str:
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    with retriever_context(run_id) as r:
        pipeline = Pipeline(Configuration.read(), r.retriever, None)
        return str(pipeline.download_trends())


def showcase_links(run_id: str) -> dict:
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    with retriever_context(run_id) as r:
        generator = DatasetGenerator(Configuration.read(), "", "", "")
        generator.load_showcase_links(r.retriever)
        return generator.get_showcase_links()


def standardised_mpi_data(
    ophi_national_excel: str,
    ophi_subnational_excel: str,
    ophi_trends_excel: str,
    run_id: str,
) -> dict:
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    with retriever_context(run_id) as r:
        admin1 = build_admin1_boundaries(r.retriever)
        admin1.setup_from_url()
        pipeline = Pipeline(Configuration.read(), r.retriever, admin1)
        pipeline.parse(ophi_national_excel, ophi_subnational_excel, ophi_trends_excel)

        standardised_countries = pipeline.get_standardised_countries()
        date_ranges = pipeline.get_date_ranges()
        return {
            "standardised_global_data": {
                "standardised_global": pipeline.get_standardised_global(),
                "standardised_global_trend": pipeline.get_standardised_global_trend(),
                "date_ranges": date_ranges,
            },
            "standardised_country_data": {
                "standardised_countries": standardised_countries,
                "standardised_countries_trend": (
                    pipeline.get_standardised_countries_trend()
                ),
                "date_ranges": date_ranges,
            },
        }


def country_iso3_list(standardised_country_data: dict) -> list[str]:
    """Airflow's .expand() can only map over a task's plain return-value XCom, not a
    custom key pulled off a multiple_outputs task - so unlike Dagster (which registers
    the fan-out partition keys as a side effect inside standardised_mpi_data itself via
    instance.add_dynamic_partitions()), this needs its own small task."""
    return sorted(standardised_country_data["standardised_countries"])


def global_mpi_dataset(
    standardised_global_data: dict,
    ophi_national_excel: str,
    ophi_subnational_excel: str,
    ophi_trends_excel: str,
    run_id: str,
) -> dict:
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    check_organization_access(READ_ONLY)
    with retriever_context(run_id) as r:
        configuration = Configuration.read()
        dataset_generator = DatasetGenerator(
            configuration,
            ophi_national_excel,
            ophi_subnational_excel,
            ophi_trends_excel,
        )
        global_date_range = standardised_global_data["date_ranges"]["global"]
        dataset = dataset_generator.generate_global_dataset(
            r.folder,
            standardised_global_data["standardised_global"],
            standardised_global_data["standardised_global_trend"],
            global_date_range,
        )
        countries_with_data = sorted(standardised_global_data["date_ranges"])
        countries_with_data = [c for c in countries_with_data if c != "global"]
        dataset.add_country_locations(countries_with_data)
        dataset.update_from_yaml(_config_path("hdx_dataset_static.yaml"))
        _maybe_create(dataset, r.batch)
        return {
            "dataset_id": dataset.get("id", "test-global-mpi"),
            "resource_ids": [
                res.get("id", f"test-resource-{i}")
                for i, res in enumerate(dataset.get_resources())
            ],
            "time_period": dataset.get_time_period(),
            "countries_with_data": countries_with_data,
            "folder": r.folder,
        }


def hapi_poverty_rate_dataset(
    standardised_global_data: dict, global_mpi_dataset: dict, run_id: str
) -> None:
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    check_organization_access(READ_ONLY)
    with retriever_context(run_id) as r:
        admin1 = build_admin1_boundaries(r.retriever)
        admin1.setup_from_url()
        configuration = Configuration.read()
        hapi_output = HAPIOutput(
            configuration,
            admin1,
            standardised_global_data["standardised_global"],
            standardised_global_data["standardised_global_trend"],
        )
        rows = hapi_output.process(
            global_mpi_dataset["dataset_id"], global_mpi_dataset["resource_ids"]
        )
        hapi_dataset_generator = HAPIDatasetGenerator(configuration, rows)
        dataset = hapi_dataset_generator.generate_poverty_rate_dataset(r.folder)
        if dataset is None:
            return
        dataset.add_country_locations(global_mpi_dataset["countries_with_data"])
        time_period = global_mpi_dataset["time_period"]
        dataset.set_time_period(time_period["startdate"], time_period["enddate"])
        dataset.update_from_yaml(_config_path("hdx_hapi_dataset_static.yaml"))
        _maybe_create(dataset, r.batch)


def country_mpi_dataset(
    countryiso3: str,
    standardised_country_data: dict,
    showcase_links: dict,
    run_id: str,
) -> dict:
    """Returns a small result dict ({countryiso3, status, folder}) rather than just the
    folder - summarize_country_results() collects these across every mapped instance so
    a single downstream task can report per-country outcomes without anyone having to
    open all ~109 individual task logs."""
    setup_hdx_configuration(hdx_site=HDX_SITE, read_only=READ_ONLY)
    check_organization_access(READ_ONLY)
    with retriever_context(run_id) as r:
        standardised_country = standardised_country_data["standardised_countries"].get(
            countryiso3
        )
        if not standardised_country:
            return {"countryiso3": countryiso3, "status": "no_data", "folder": r.folder}
        standardised_country_trend = standardised_country_data[
            "standardised_countries_trend"
        ].get(countryiso3, {})
        date_range = standardised_country_data["date_ranges"][countryiso3]
        countryname = Country.get_country_name_from_iso3(countryiso3)

        configuration = Configuration.read()
        dataset_generator = DatasetGenerator(
            configuration, "", "", "", showcase_links=showcase_links
        )
        dataset = dataset_generator.generate_dataset(
            r.folder,
            standardised_country,
            standardised_country_trend,
            countryiso3,
            countryname,
            date_range,
        )
        if dataset is None:
            return {
                "countryiso3": countryiso3,
                "status": "no_dataset",
                "folder": r.folder,
            }
        dataset.add_country_location(countryiso3)
        dataset.set_expected_update_frequency("As needed")
        dataset.update_from_yaml(_config_path("hdx_dataset_static.yaml"))
        _maybe_create(dataset, r.batch)
        showcase = dataset_generator.generate_showcase(countryiso3, countryname)
        if showcase and not READ_ONLY:
            showcase.create_in_hdx()
            showcase.add_dataset(dataset)
        status = "read_only" if READ_ONLY else "created"
        return {"countryiso3": countryiso3, "status": status, "folder": r.folder}


def _render_task_log(log_file: Any) -> str:
    """Reformat an Airflow 3 JSON-lines task log file into conventional plain-text
    lines for inlining into another task's log - dropping the per-line context keys
    that are redundant once you already know which task/run this is (logger, ti_id,
    try_number, map_index, dag_id, task_id, run_id, filename, lineno), keeping each
    line's own original timestamp (when that line was actually logged, as opposed to
    the timestamp Airflow's own JSON wrapper stamps on the summarize_country_results
    line this ends up inlined into, which is just whenever that task happened to run),
    and rendering a "Task failed with exception" record's error_detail as a short
    exception chain (exc_type: exc_value per exception) instead of the full
    frame-by-frame traceback for every exception in the chain, which otherwise makes
    each failure a huge wall of JSON that buries the actual error under stack-frame
    noise.
    """
    import json

    lines = []
    for raw_line in log_file.read_text().splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            record = json.loads(raw_line)
        except json.JSONDecodeError:
            lines.append(raw_line)
            continue
        timestamp = record.get("timestamp", "")[:19].replace("T", " ")
        level = record.get("level", "info").upper()
        lines.append(f"{timestamp} {level:<8} {record.get('event', '')}")
        for exc in record.get("error_detail") or []:
            lines.append(
                f"{timestamp} {level:<8}   "
                f"{exc.get('exc_type', '?')}: {exc.get('exc_value', '')}"
            )
    return "\n".join(lines)


def summarize_country_results(iso3_list: list[str], ti: Any, dag_run: Any) -> None:
    """Single end-of-run task that pulls every country_mpi_dataset mapped instance's
    result over XCom and logs one consolidated summary - the built-in alternative to
    opening each of the ~109 per-country task logs individually. Runs even if some
    country tasks failed (trigger_rule=all_done, set where this is wired up below).

    For any country missing from the pulled results (failed, or still running), this
    also inlines that mapped instance's full log file content into this task's own log
    - reading it directly off disk (this only works for the default local-file log
    handler on a self-hosted, single-machine setup like this one; a remote logging
    backend, e.g. Elasticsearch, or a distributed executor with no shared filesystem,
    would need TaskLogReader/the API instead) - so every failure is visible in one
    place without hunting down each country's own task log individually.
    """
    from collections import Counter
    from pathlib import Path

    from airflow.configuration import conf

    raw_results = ti.xcom_pull(task_ids="country_mpi_dataset") or []
    results = [r for r in raw_results if r is not None]
    reported = {r["countryiso3"] for r in results}
    missing = sorted(set(iso3_list) - reported)

    counts = Counter(r["status"] for r in results)
    logger.info(
        f"country_mpi_dataset summary: {len(iso3_list)} countries total, "
        f"{dict(counts)}, {len(missing)} did not report (failed or still running)."
    )
    if not missing:
        return

    logger.info(f"Countries with no result: {', '.join(missing)}")
    base_log_folder = Path(conf.get("logging", "base_log_folder"))
    for countryiso3 in missing:
        map_index = iso3_list.index(countryiso3)
        task_log_dir = (
            base_log_folder
            / f"dag_id={dag_run.dag_id}"
            / f"run_id={dag_run.run_id}"
            / "task_id=country_mpi_dataset"
            / f"map_index={map_index}"
        )
        attempt_logs = sorted(task_log_dir.glob("attempt=*.log"))
        if not attempt_logs:
            logger.info(
                f"Log output for {countryiso3}: no log file found at {task_log_dir}"
            )
            continue
        log_file = attempt_logs[-1]
        logger.info(f"Log output for {countryiso3}:")
        logger.info(_render_task_log(log_file))


@dag(
    dag_id="ophi_pipeline",
    # Fires daily across the same 18-26 Oct window as Dagster's ophi_annual_schedule; a
    # plain cron string can't express "nearest weekday to the 22nd" on its own, and a
    # custom Timetable would need registering as an Airflow plugin just for this one
    # schedule - so the same fire-daily-and-skip approach is used here via the
    # short-circuit task below instead.
    schedule="0 6 18-26 10 *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ophi"],
)
def ophi_pipeline() -> None:
    gate = task.short_circuit(is_target_run_day)()

    admin1 = task(admin1_boundaries)()
    national = task(ophi_national_excel)()
    subnational = task(ophi_subnational_excel)()
    trends = task(ophi_trends_excel)()
    links = task(showcase_links)()
    gate >> [admin1, national, subnational, trends, links]

    standardised = task(standardised_mpi_data, multiple_outputs=True)(
        national, subnational, trends
    )
    admin1 >> standardised

    global_dataset = task(global_mpi_dataset)(
        standardised["standardised_global_data"], national, subnational, trends
    )
    task(hapi_poverty_rate_dataset)(
        standardised["standardised_global_data"], global_dataset
    )

    iso3_list = task(country_iso3_list)(standardised["standardised_country_data"])
    country_results = (
        task(country_mpi_dataset)
        .partial(
            standardised_country_data=standardised["standardised_country_data"],
            showcase_links=links,
        )
        .expand(countryiso3=iso3_list)
    )
    summary = task(summarize_country_results, trigger_rule=TriggerRule.ALL_DONE)(
        iso3_list
    )
    country_results >> summary


dag_object: DAG = ophi_pipeline()
