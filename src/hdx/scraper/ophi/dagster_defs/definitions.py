"""Dagster Definitions entry point for the OPHI pipeline.

Runs as its own gRPC-server code location (see dagster-azure/AZURE_SETUP_PLAN.md for how
this is wired into the shared webserver/daemon deployment), e.g.:

    dagster api grpc -m hdx.scraper.ophi.dagster_defs.definitions -a defs -h 0.0.0.0 -p 4000

Local ad hoc materialization:

    uv run dagster asset materialize -m hdx.scraper.ophi.dagster_defs.definitions --select '*'
"""

from dagster import Definitions, load_assets_from_modules
from hdx.utilities.easy_logging import setup_logging

from hdx.scraper.ophi.dagster_defs import assets
from hdx.scraper.ophi.dagster_defs.jobs_schedules import (
    core_job,
    country_job,
    country_results_summary_job,
    ophi_annual_schedule,
    ophi_country_fanout_sensor,
)
from hdx.scraper.ophi.dagster_defs.resources import (
    AdminOneResource,
    HDXConfigResource,
    RetrieverResource,
)

# hdx.scraper.ophi.__main__ calls this at import time; dagster_defs never imports
# __main__, so without this call every hdx-python-api/hdx-python-utilities log message
# (download URLs, "Creating dataset: ...", read-only status, etc.) has no configured
# handler and is silently dropped rather than showing up in the run's stdout/stderr.
setup_logging()

retriever_resource = RetrieverResource(save=False, use_saved=False)

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[core_job, country_job, country_results_summary_job],
    schedules=[ophi_annual_schedule],
    sensors=[ophi_country_fanout_sensor],
    resources={
        # hdx_site is explicit here rather than left to fall back to whatever
        # ~/.hdx_configuration.yaml says on whichever machine runs this - this is a
        # comparison/testing deployment and must not silently resolve to prod.
        "hdx_config": HDXConfigResource(hdx_site="stage"),
        "retriever_resource": retriever_resource,
        "adminone_resource": AdminOneResource(retriever_resource=retriever_resource),
    },
)
