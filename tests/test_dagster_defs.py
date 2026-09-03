import logging
from os.path import isfile, join

import pytest
from dagster import DagsterInstance, Definitions, load_assets_from_modules, materialize
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent

from hdx.scraper.ophi.dagster_defs import assets
from hdx.scraper.ophi.dagster_defs.jobs_schedules import (
    core_job,
    country_job,
    country_results_summary_job,
)
from hdx.scraper.ophi.dagster_defs.resources import (
    AdminOneResource,
    HDXConfigResource,
    RetrieverResource,
)
from hdx.scraper.ophi.pipeline import Pipeline

logger = logging.getLogger(__name__)

CORE_ASSETS = [
    assets.admin1_boundaries,
    assets.ophi_national_excel,
    assets.ophi_subnational_excel,
    assets.ophi_trends_excel,
    assets.standardised_mpi_data,
    assets.showcase_links,
    assets.global_mpi_dataset,
    assets.hapi_poverty_rate_dataset,
]
ALL_ASSETS = [*CORE_ASSETS, assets.country_mpi_dataset]


class TestOPHIDagsterDefs:
    @pytest.fixture(scope="function")
    def configuration(self):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=script_dir_plus_file(
                join("config", "project_configuration.yaml"), Pipeline
            ),
        )
        # Every known country is a valid location here (unlike test_main.py, which
        # never calls add_country_locations): global_mpi_dataset does, over every
        # country the parsed data mentions. Country.countriesdata(use_live=False)
        # pins Country's class-level _use_live/_countriesdata cache process-wide, which
        # would otherwise leak into test_main.py's test if it runs afterwards in the
        # same session - snapshot and restore it so this test stays isolated.
        original_use_live = Country._use_live
        original_countriesdata = Country._countriesdata
        countries = Country.countriesdata(use_live=False)["countries"]
        Locations.set_validlocations(
            [
                {"name": iso3.lower(), "title": info["Preferred Term"]}
                for iso3, info in countries.items()
            ]
        )
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": tag}
                for tag in (
                    "development",
                    "education",
                    "health",
                    "indicators",
                    "mortality",
                    "nutrition",
                    "poverty",
                    "socioeconomics",
                    "sustainable development goals-sdg",
                    "water sanitation and hygiene-wash",
                )
            ],
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
        yield Configuration.read()
        Country._use_live = original_use_live
        Country._countriesdata = original_countriesdata

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture()
    def resources(self, input_dir):
        retriever_resource = RetrieverResource(
            save=False,
            use_saved=True,
            saved_dir=input_dir,
            batch_seed="test-batch",
            delete_scratch_on_success=False,
        )
        return {
            "hdx_config": HDXConfigResource(read_only=True),
            "retriever_resource": retriever_resource,
            "adminone_resource": AdminOneResource(
                retriever_resource=retriever_resource
            ),
        }

    def test_core_graph_and_country_partition(
        self, configuration, fixtures_dir, resources
    ):
        instance = DagsterInstance.ephemeral()

        core_result = materialize(CORE_ASSETS, resources=resources, instance=instance)
        assert core_result.success

        # Read the scratch folder back from the asset outputs rather than the
        # RetrieverResource object held in this test: Dagster resolves/copies pythonic
        # resources internally, so private state set during the run isn't visible on
        # the instance this test still holds a reference to.
        folder = core_result.asset_value(assets.global_mpi_dataset.key).folder
        # global_mpi.csv/global_mpi_trends.csv have no dataset/resource id columns, so
        # they're byte-identical to the fixtures regardless of read-only mode.
        # hdx_hapi_poverty_rate_global.csv embeds dataset_hdx_id/resource_hdx_id - the
        # fixture bakes in the literal placeholder ids test_main.py passes by hand
        # ("12"/"3456"/"7890"), which read-only mode here can't reproduce since no HDX
        # write ever assigns real ids, so that file is checked for existence only.
        for filename in ("global_mpi.csv", "global_mpi_trends.csv"):
            assert_files_same(
                join(fixtures_dir, filename),
                join(folder, filename),
            )
        assert isfile(join(folder, "hdx_hapi_poverty_rate_global.csv"))

        partitions = instance.get_dynamic_partitions(assets.country_partitions.name)
        assert "AFG" in partitions
        assert len(partitions) > 100

        country_result = materialize(
            ALL_ASSETS,
            selection=[assets.country_mpi_dataset],
            resources=resources,
            instance=instance,
            partition_key="AFG",
        )
        assert country_result.success
        country_folder = country_result.asset_value(assets.country_mpi_dataset.key)
        for filename in ("AFG_mpi.csv", "AFG_mpi_trends.csv"):
            assert_files_same(
                join(fixtures_dir, filename),
                join(country_folder, filename),
            )

    def test_country_results_summary(self, configuration, resources, monkeypatch):
        """country_results_summary_job needs runs tagged with a real job name
        ("ophi_country_job") to find via RunsFilter, which materialize() doesn't
        produce (its ad hoc job has a different name) - so this resolves core_job/
        country_job/country_results_summary_job against the test resources the same
        way definitions.py resolves them against the real ones, then executes each
        job for real via execute_in_process()."""
        test_defs = Definitions(
            assets=load_assets_from_modules([assets]),
            jobs=[core_job, country_job, country_results_summary_job],
            resources=resources,
        )
        instance = DagsterInstance.ephemeral()

        core_result = test_defs.get_job_def("ophi_core_job").execute_in_process(
            instance=instance
        )
        assert core_result.success

        partitions = sorted(
            instance.get_dynamic_partitions(assets.country_partitions.name)
        )
        assert "AFG" in partitions
        succeeding_iso3 = next(iso3 for iso3 in partitions if iso3 != "AFG")

        country_job_def = test_defs.get_job_def("ophi_country_job")

        # Induce a real STEP_FAILURE for AFG specifically, so the summary job has an
        # actual failure to detect and report - without this, a read-only test run
        # never touches HDX and has no other way to fail.
        original_generate_dataset = assets.DatasetGenerator.generate_dataset

        def failing_generate_dataset(self, folder, standardised_country, *args):
            if (
                args[1] == "AFG"
            ):  # args = (standardised_country_trend, countryiso3, ...)
                raise RuntimeError("boom: induced failure for testing")
            return original_generate_dataset(self, folder, standardised_country, *args)

        monkeypatch.setattr(
            assets.DatasetGenerator, "generate_dataset", failing_generate_dataset
        )

        failed_result = country_job_def.execute_in_process(
            instance=instance, partition_key="AFG", raise_on_error=False
        )
        assert not failed_result.success

        success_result = country_job_def.execute_in_process(
            instance=instance, partition_key=succeeding_iso3
        )
        assert success_result.success

        summary_result = test_defs.get_job_def(
            "country_results_summary_job"
        ).execute_in_process(instance=instance)
        assert summary_result.success

        # context.log.info() calls surface as plain EventLogEntry log records, not as
        # DagsterEvents, so they aren't in .all_events (only structured lifecycle
        # events like STEP_SUCCESS are) - instance.all_logs() returns both.
        messages = "\n".join(
            e.user_message for e in instance.all_logs(summary_result.run_id)
        )
        assert "'failed': 1" in messages
        assert "boom: induced failure for testing" in messages
        assert f"--- AFG (run {failed_result.run_id})" in messages
