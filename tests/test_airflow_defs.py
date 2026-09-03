import logging
from os.path import isfile, join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent

from hdx.scraper.ophi.airflow_defs import dag
from hdx.scraper.ophi.pipeline import Pipeline

logger = logging.getLogger(__name__)


class TestOPHIAirflowDefs:
    """Calls the task functions in airflow_defs/dag.py directly, the same way
    test_dagster_defs.py calls into Dagster ops without going through Dagster's
    scheduler/webserver - here without Airflow's scheduler/metadata database. Each
    function is exercised exactly as the DAG wires it, just with Python calls standing
    in for XCom.
    """

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
        # See test_dagster_defs.py's identical fixture for why Country's live-data cache
        # is snapshotted and restored here.
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

    @pytest.fixture(autouse=True)
    def _use_fixture_input(self, monkeypatch, input_dir):
        # Every task in the DAG opens its own retriever_context() - point them all at
        # the shared test fixtures instead of downloading for real, and keep the
        # scratch folder around after materialization so the CSV assertions below can
        # read it back.
        import functools

        from hdx.scraper.ophi.airflow_defs import resources as airflow_resources

        original = airflow_resources.retriever_context
        patched = functools.partial(
            original,
            save=False,
            use_saved=True,
            saved_dir=input_dir,
            batch_seed="test-batch",
            delete_scratch_on_success=False,
        )
        monkeypatch.setattr(dag, "retriever_context", patched)

    def test_core_pipeline_and_country_fanout(self, configuration, fixtures_dir):
        run_id = "test-run"

        admin1_result = dag.admin1_boundaries(run_id)
        assert admin1_result is None

        national = dag.ophi_national_excel(run_id)
        subnational = dag.ophi_subnational_excel(run_id)
        trends = dag.ophi_trends_excel(run_id)
        links = dag.showcase_links(run_id)

        standardised = dag.standardised_mpi_data(national, subnational, trends, run_id)
        iso3_list = dag.country_iso3_list(standardised["standardised_country_data"])
        assert "AFG" in iso3_list
        assert len(iso3_list) > 100

        global_dataset = dag.global_mpi_dataset(
            standardised["standardised_global_data"],
            national,
            subnational,
            trends,
            run_id,
        )
        dag.hapi_poverty_rate_dataset(
            standardised["standardised_global_data"], global_dataset, run_id
        )

        folder = global_dataset["folder"]
        for filename in ("global_mpi.csv", "global_mpi_trends.csv"):
            assert_files_same(
                join(fixtures_dir, filename),
                join(folder, filename),
            )
        assert isfile(join(folder, "hdx_hapi_poverty_rate_global.csv"))

        country_folder = dag.country_mpi_dataset(
            "AFG", standardised["standardised_country_data"], links, run_id
        )
        for filename in ("AFG_mpi.csv", "AFG_mpi_trends.csv"):
            assert_files_same(
                join(fixtures_dir, filename),
                join(country_folder, filename),
            )

    def test_target_run_day(self):
        from datetime import date

        # 22 Oct 2028 is a Sunday -> observed the following Monday.
        assert dag.target_run_day(2028) == date(2028, 10, 23)
        # 22 Oct 2033 is a Saturday -> observed the preceding Friday.
        assert dag.target_run_day(2033) == date(2033, 10, 21)
        # 22 Oct 2026 is a Thursday -> observed on the day itself.
        assert dag.target_run_day(2026) == date(2026, 10, 22)

    def test_is_target_run_day(self):
        from datetime import UTC, datetime
        from types import SimpleNamespace

        def fake_dag_run(run_type: str, logical_date: datetime | None = None):
            return SimpleNamespace(run_type=run_type, logical_date=logical_date)

        # Any non-"scheduled" trigger always runs, regardless of logical_date - this
        # covers both a bare CLI trigger (logical_date=None) and a UI trigger (which
        # stamps a real logical_date on an equally-manual run).
        assert dag.is_target_run_day(fake_dag_run("manual")) is True
        assert (
            dag.is_target_run_day(
                fake_dag_run("manual", datetime(2026, 9, 3, tzinfo=UTC))
            )
            is True
        )
        # A genuine scheduled tick is subject to the day gate.
        assert (
            dag.is_target_run_day(
                fake_dag_run("scheduled", datetime(2026, 10, 22, tzinfo=UTC))
            )
            is True
        )
        assert (
            dag.is_target_run_day(
                fake_dag_run("scheduled", datetime(2026, 10, 23, tzinfo=UTC))
            )
            is False
        )

    def test_dag_structure(self):
        assert dag.dag_object.dag_id == "ophi_pipeline"
        mapped_task = dag.dag_object.get_task("country_mpi_dataset")
        assert mapped_task.is_mapped
        node_ids = {t.task_id for t in dag.dag_object.tasks}
        assert node_ids == {
            "is_target_run_day",
            "admin1_boundaries",
            "ophi_national_excel",
            "ophi_subnational_excel",
            "ophi_trends_excel",
            "showcase_links",
            "standardised_mpi_data",
            "country_iso3_list",
            "global_mpi_dataset",
            "hapi_poverty_rate_dataset",
            "country_mpi_dataset",
        }
