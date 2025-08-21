import logging
from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.ophi.dataset_generator import DatasetGenerator
from hdx.scraper.ophi.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.ophi.hapi_output import HAPIOutput
from hdx.scraper.ophi.pipeline import Pipeline
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)


class TestOPHI:
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
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
            ]
        )
        Vocabulary._approved_vocabulary = {
            "tags": [
                {"name": tag}
                for tag in (
                    "hxl",
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
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    def test_main(
        self,
        configuration,
        fixtures_dir,
        input_dir,
    ):
        with temp_dir(
            "TestOPHI",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader,
                    tempdir,
                    input_dir,
                    tempdir,
                    save=False,
                    use_saved=True,
                )
                adminone = AdminLevel(admin_level=1, retriever=retriever)
                adminone.setup_from_url()

                pipeline = Pipeline(configuration, retriever, adminone)
                mpi_national_path, mpi_subnational_path, trend_path = pipeline.process()
                dataset_generator = DatasetGenerator(
                    configuration,
                    mpi_national_path,
                    mpi_subnational_path,
                    trend_path,
                )

                standardised_global = pipeline.get_standardised_global()
                standardised_global_trend = pipeline.get_standardised_global_trend()
                standardised_countries = pipeline.get_standardised_countries()
                standardised_countries_trend = (
                    pipeline.get_standardised_countries_trend()
                )
                date_ranges = pipeline.get_date_ranges()
                dataset = dataset_generator.generate_global_dataset(
                    tempdir,
                    standardised_global,
                    standardised_global_trend,
                    date_ranges["global"],
                )
                countryiso3s = list(standardised_countries.keys())
                assert sorted(countryiso3s) == [
                    "AFG",
                    "AGO",
                    "ALB",
                    "ARG",
                    "ARM",
                    "BDI",
                    "BEN",
                    "BFA",
                    "BGD",
                    "BIH",
                    "BLZ",
                    "BOL",
                    "BRA",
                    "BRB",
                    "BTN",
                    "BWA",
                    "CAF",
                    "CHN",
                    "CIV",
                    "CMR",
                    "COD",
                    "COG",
                    "COL",
                    "COM",
                    "CRI",
                    "CUB",
                    "DOM",
                    "DZA",
                    "ECU",
                    "EGY",
                    "ETH",
                    "FJI",
                    "GAB",
                    "GEO",
                    "GHA",
                    "GIN",
                    "GMB",
                    "GNB",
                    "GTM",
                    "GUY",
                    "HND",
                    "HTI",
                    "IDN",
                    "IND",
                    "IRQ",
                    "JAM",
                    "JOR",
                    "KAZ",
                    "KEN",
                    "KGZ",
                    "KHM",
                    "KIR",
                    "LAO",
                    "LBR",
                    "LBY",
                    "LCA",
                    "LKA",
                    "LSO",
                    "MAR",
                    "MDA",
                    "MDG",
                    "MDV",
                    "MEX",
                    "MKD",
                    "MLI",
                    "MMR",
                    "MNE",
                    "MNG",
                    "MOZ",
                    "MRT",
                    "MWI",
                    "NAM",
                    "NER",
                    "NGA",
                    "NIC",
                    "NPL",
                    "PAK",
                    "PER",
                    "PHL",
                    "PNG",
                    "PRY",
                    "PSE",
                    "RWA",
                    "SDN",
                    "SEN",
                    "SLE",
                    "SLV",
                    "SRB",
                    "STP",
                    "SUR",
                    "SWZ",
                    "SYC",
                    "TCD",
                    "TGO",
                    "THA",
                    "TJK",
                    "TKM",
                    "TLS",
                    "TON",
                    "TTO",
                    "TUN",
                    "TUV",
                    "TZA",
                    "UGA",
                    "UKR",
                    "UZB",
                    "VNM",
                    "WSM",
                    "YEM",
                    "ZAF",
                    "ZMB",
                    "ZWE",
                ]
                assert dataset == {
                    "data_update_frequency": "-2",
                    "dataset_date": "[2001-01-01T00:00:00 TO 2023-12-31T23:59:59]",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "global-mpi",
                    "owner_org": "00547685-9ded-4d69-9ca5-47d5278ead7c",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "development",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "education",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "health",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "mortality",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "nutrition",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "poverty",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "socioeconomics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "sustainable development goals-sdg",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "water sanitation and hygiene-wash",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Global Multidimensional Poverty Index",
                }
                assert dataset.get_resources() == [
                    {
                        "description": "This resource contains standardised MPI estimates by "
                        "first-level administrative unit (e.g. state, province) and "
                        "also shows the proportion of people who are MPI poor and "
                        "experience deprivations in each of the indicators by admin "
                        "one unit.",
                        "format": "csv",
                        "name": "Global MPI and Partial Indices",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "This resource contains standardised MPI estimates and their "
                        "changes over time by first-level administrative unit (e.g. "
                        "state, province) and also shows the proportion of people who "
                        "are MPI poor and experience deprivations in each of the "
                        "indicators by admin one unit.",
                        "format": "csv",
                        "name": "Global MPI Trends Over Time",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "This table shows the MPI and its partial indices",
                        "format": "xlsx",
                        "name": "MPI and Partial Indices National Database",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "This table shows the MPI and its partial indices "
                        "disaggregated by subnational regions",
                        "format": "xlsx",
                        "name": "MPI and Partial Indices Subnational Database",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "This table shows global mpi harmonized level estimates and "
                        "their changes over time",
                        "format": "xlsx",
                        "name": "Trends Over Time MPI Database",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
                for filename in ("global_mpi.csv", "global_mpi_trends.csv"):
                    expected_file = join(fixtures_dir, filename)
                    actual_file = join(tempdir, filename)
                    assert_files_same(expected_file, actual_file)

                hapi_output = HAPIOutput(
                    configuration,
                    adminone,
                    standardised_global,
                    standardised_global_trend,
                )
                rows = hapi_output.process("12", ["3456", "7890"])
                hapi_dataset_generator = HAPIDatasetGenerator(configuration, rows)
                dataset = hapi_dataset_generator.generate_poverty_rate_dataset(tempdir)
                assert dataset == {
                    "data_update_frequency": "365",
                    "dataset_preview": "no_preview",
                    "dataset_source": "Oxford Poverty & Human Development Initiative",
                    "license_id": "other-pd-nr",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "hdx-hapi-poverty-rate",
                    "owner_org": "40d10ece-49de-4791-9aed-e164f1d16dd1",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "education",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "health",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "poverty",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "HDX HAPI - Food Security, Nutrition & Poverty: Poverty Rate",
                }
                assert dataset.get_resources()[0] == {
                    "name": "Global Food Security, Nutrition & Poverty: Poverty Rate",
                    "description": "Poverty Rate data from HDX HAPI, please see [the documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#poverty-rate) for more information",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                    "dataset_preview_enabled": "False",
                }

                filename = "hdx_hapi_poverty_rate_global.csv"
                expected_file = join(fixtures_dir, filename)
                actual_file = join(tempdir, filename)
                assert_files_same(expected_file, actual_file)

                countryiso3 = "AFG"
                countryname = Country.get_country_name_from_iso3(countryiso3)
                standardised_country = standardised_countries[countryiso3]
                standardised_country_trend = standardised_countries_trend.get(
                    countryiso3
                )
                dataset = dataset_generator.generate_dataset(
                    tempdir,
                    standardised_country,
                    standardised_country_trend,
                    countryiso3,
                    countryname,
                    date_ranges[countryiso3],
                )
                assert dataset == {
                    "dataset_date": "[2015-01-01T00:00:00 TO 2023-12-31T23:59:59]",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "afghanistan-mpi",
                    "owner_org": "00547685-9ded-4d69-9ca5-47d5278ead7c",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "development",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "education",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "health",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "mortality",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "nutrition",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "poverty",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "socioeconomics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "sustainable development goals-sdg",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "water sanitation and hygiene-wash",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan Multidimensional Poverty Index",
                }
                assert dataset.get_resources() == [
                    {
                        "description": "This resource contains standardised MPI estimates by "
                        "first-level administrative unit (e.g. state, province) and "
                        "also shows the proportion of people who are MPI poor and "
                        "experience deprivations in each of the indicators by admin "
                        "one unit.",
                        "format": "csv",
                        "name": "Afghanistan MPI and Partial Indices",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "This resource contains standardised MPI estimates and their "
                        "changes over time by first-level administrative unit (e.g. "
                        "state, province) and also shows the proportion of people who "
                        "are MPI poor and experience deprivations in each of the "
                        "indicators by admin one unit.",
                        "format": "csv",
                        "name": "Afghanistan MPI Trends Over Time",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
                for filename in ("AFG_mpi.csv", "AFG_mpi_trends.csv"):
                    expected_file = join(fixtures_dir, filename)
                    actual_file = join(tempdir, filename)
                    assert_files_same(expected_file, actual_file)
                dataset_generator.load_showcase_links(retriever)
                showcase = dataset_generator.generate_showcase(countryiso3, countryname)
                assert showcase == {
                    "image_url": "https://raw.githubusercontent.com/OCHA-DAP/hdx-scraper-ophi/main/ophi_mpi.jpg",
                    "name": "afghanistan-mpi-showcase",
                    "notes": "The visual contains sub-national multidimensional poverty data from "
                    "the country briefs published by the Oxford Poverty and Human "
                    "Development Initiative (OPHI), University of Oxford.",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "development",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "education",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "health",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "mortality",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "nutrition",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "poverty",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "socioeconomics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "sustainable development goals-sdg",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "water sanitation and hygiene-wash",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan Multidimensional Poverty Index",
                    "url": "https://ophi.org.uk/media/45972/download",
                }
