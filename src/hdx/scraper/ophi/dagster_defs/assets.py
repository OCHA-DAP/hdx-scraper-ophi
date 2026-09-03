"""Dagster asset graph wrapping hdx.scraper.ophi's existing pipeline/dataset-generator
classes. Each asset is a thin wrapper around an existing method; the business logic in
pipeline.py / dataset_generator.py / hapi_output.py / hapi_dataset_generator.py is
untouched.
"""

from dataclasses import dataclass
from logging import getLogger

from dagster import (
    AssetExecutionContext,
    AssetOut,
    DynamicPartitionsDefinition,
    asset,
    multi_asset,
)
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel

from hdx.scraper.ophi.dagster_defs.resources import (
    AdminOneResource,
    HDXConfigResource,
    RetrieverResource,
)
from hdx.scraper.ophi.dataset_generator import DatasetGenerator
from hdx.scraper.ophi.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.ophi.hapi_output import HAPIOutput
from hdx.scraper.ophi.pipeline import Pipeline

logger = getLogger(__name__)

country_partitions = DynamicPartitionsDefinition(name="ophi_countries")


@dataclass
class StandardisedGlobalData:
    standardised_global: dict
    standardised_global_trend: dict
    date_ranges: dict


@dataclass
class StandardisedCountryData:
    standardised_countries: dict
    standardised_countries_trend: dict
    date_ranges: dict


@dataclass
class GlobalDatasetRef:
    dataset_id: str
    resource_ids: list[str]
    time_period: dict
    countries_with_data: list[str]
    folder: str


@asset
def admin1_boundaries(adminone_resource: AdminOneResource) -> AdminLevel:
    adminone = adminone_resource.build()
    adminone.setup_from_url()
    return adminone


@asset
def ophi_national_excel(
    hdx_config: HDXConfigResource, retriever_resource: RetrieverResource
) -> str:
    pipeline = Pipeline(Configuration.read(), retriever_resource.retriever, None)
    return str(pipeline.download_mpi_national())


@asset
def ophi_subnational_excel(
    hdx_config: HDXConfigResource, retriever_resource: RetrieverResource
) -> str:
    pipeline = Pipeline(Configuration.read(), retriever_resource.retriever, None)
    return str(pipeline.download_mpi_subnational())


@asset
def ophi_trends_excel(
    hdx_config: HDXConfigResource, retriever_resource: RetrieverResource
) -> str:
    pipeline = Pipeline(Configuration.read(), retriever_resource.retriever, None)
    return str(pipeline.download_trends())


@asset
def showcase_links(
    hdx_config: HDXConfigResource, retriever_resource: RetrieverResource
) -> dict:
    generator = DatasetGenerator(Configuration.read(), "", "", "")
    generator.load_showcase_links(retriever_resource.retriever)
    return generator.get_showcase_links()


@multi_asset(
    outs={
        "standardised_global_data": AssetOut(),
        "standardised_country_data": AssetOut(),
    },
)
def standardised_mpi_data(
    context: AssetExecutionContext,
    hdx_config: HDXConfigResource,
    retriever_resource: RetrieverResource,
    admin1_boundaries: AdminLevel,
    ophi_national_excel: str,
    ophi_subnational_excel: str,
    ophi_trends_excel: str,
) -> tuple[StandardisedGlobalData, StandardisedCountryData]:
    # The 4 read_* methods share one Pipeline instance because they mutate shared state
    # (e.g. a row from the national file is written into both the global dict and the
    # per-country dict, and date_ranges is accumulated across all 4 reads) - splitting
    # this into two independent Pipeline instances would silently drop rows, so this is a
    # single multi_asset rather than two separate assets.
    pipeline = Pipeline(
        Configuration.read(), retriever_resource.retriever, admin1_boundaries
    )
    pipeline.parse(ophi_national_excel, ophi_subnational_excel, ophi_trends_excel)

    standardised_countries = pipeline.get_standardised_countries()
    new_keys = sorted(standardised_countries)
    instance = context.instance
    existing_keys = set(instance.get_dynamic_partitions(country_partitions.name))
    instance.add_dynamic_partitions(country_partitions.name, new_keys)
    for stale_key in existing_keys - set(new_keys):
        instance.delete_dynamic_partition(country_partitions.name, stale_key)

    date_ranges = pipeline.get_date_ranges()
    global_data = StandardisedGlobalData(
        standardised_global=pipeline.get_standardised_global(),
        standardised_global_trend=pipeline.get_standardised_global_trend(),
        date_ranges=date_ranges,
    )
    country_data = StandardisedCountryData(
        standardised_countries=standardised_countries,
        standardised_countries_trend=pipeline.get_standardised_countries_trend(),
        date_ranges=date_ranges,
    )
    return global_data, country_data


@asset
def global_mpi_dataset(
    hdx_config: HDXConfigResource,
    retriever_resource: RetrieverResource,
    standardised_global_data: StandardisedGlobalData,
    ophi_national_excel: str,
    ophi_subnational_excel: str,
    ophi_trends_excel: str,
) -> GlobalDatasetRef:
    hdx_config.check_organization_access()
    configuration = Configuration.read()
    dataset_generator = DatasetGenerator(
        configuration,
        ophi_national_excel,
        ophi_subnational_excel,
        ophi_trends_excel,
    )
    global_date_range = standardised_global_data.date_ranges["global"]
    dataset = dataset_generator.generate_global_dataset(
        retriever_resource.folder,
        standardised_global_data.standardised_global,
        standardised_global_data.standardised_global_trend,
        global_date_range,
    )
    countries_with_data = sorted(standardised_global_data.date_ranges)
    countries_with_data = [c for c in countries_with_data if c != "global"]
    dataset.add_country_locations(countries_with_data)
    dataset.update_from_yaml(_config_path(dataset, "hdx_dataset_static.yaml"))
    # hdx-python-api's hdx_read_only flag is advisory only - it does not itself stop
    # create_in_hdx() from making a real HDX write, so the actual write is gated here.
    if hdx_config.read_only:
        logger.info(f"read_only=True: not writing dataset '{dataset['name']}' to HDX.")
    else:
        dataset.create_in_hdx(
            remove_additional_resources=True,
            updated_by_script="HDX Scraper: OPHI",
            batch=retriever_resource.batch,
        )
    return GlobalDatasetRef(
        dataset_id=dataset.get("id", "test-global-mpi"),
        resource_ids=[
            r.get("id", f"test-resource-{i}")
            for i, r in enumerate(dataset.get_resources())
        ],
        time_period=dataset.get_time_period(),
        countries_with_data=countries_with_data,
        folder=retriever_resource.folder,
    )


@asset
def hapi_poverty_rate_dataset(
    hdx_config: HDXConfigResource,
    retriever_resource: RetrieverResource,
    admin1_boundaries: AdminLevel,
    standardised_global_data: StandardisedGlobalData,
    global_mpi_dataset: GlobalDatasetRef,
) -> None:
    hdx_config.check_organization_access()
    configuration = Configuration.read()
    hapi_output = HAPIOutput(
        configuration,
        admin1_boundaries,
        standardised_global_data.standardised_global,
        standardised_global_data.standardised_global_trend,
    )
    rows = hapi_output.process(
        global_mpi_dataset.dataset_id, global_mpi_dataset.resource_ids
    )
    hapi_dataset_generator = HAPIDatasetGenerator(configuration, rows)
    dataset = hapi_dataset_generator.generate_poverty_rate_dataset(
        retriever_resource.folder
    )
    if dataset is None:
        return
    dataset.add_country_locations(global_mpi_dataset.countries_with_data)
    time_period = global_mpi_dataset.time_period
    dataset.set_time_period(time_period["startdate"], time_period["enddate"])
    dataset.update_from_yaml(_config_path(dataset, "hdx_hapi_dataset_static.yaml"))
    if hdx_config.read_only:
        logger.info(f"read_only=True: not writing dataset '{dataset['name']}' to HDX.")
    else:
        dataset.create_in_hdx(
            remove_additional_resources=True,
            updated_by_script="HDX Scraper: OPHI",
            batch=retriever_resource.batch,
        )


@asset(partitions_def=country_partitions)
def country_mpi_dataset(
    context: AssetExecutionContext,
    hdx_config: HDXConfigResource,
    retriever_resource: RetrieverResource,
    standardised_country_data: StandardisedCountryData,
    showcase_links: dict,
) -> str:
    """Returns the run's scratch folder (where this country's CSVs were written) so
    tests/callers can locate the output without reaching into resource internals.
    """
    from hdx.location.country import Country

    hdx_config.check_organization_access()
    countryiso3 = context.partition_key
    standardised_country = standardised_country_data.standardised_countries.get(
        countryiso3
    )
    if not standardised_country:
        context.log.warning(f"No data for {countryiso3}, skipping.")
        return retriever_resource.folder
    standardised_country_trend = (
        standardised_country_data.standardised_countries_trend.get(countryiso3, {})
    )
    date_range = standardised_country_data.date_ranges[countryiso3]
    countryname = Country.get_country_name_from_iso3(countryiso3)

    configuration = Configuration.read()
    dataset_generator = DatasetGenerator(
        configuration, "", "", "", showcase_links=showcase_links
    )
    dataset = dataset_generator.generate_dataset(
        retriever_resource.folder,
        standardised_country,
        standardised_country_trend,
        countryiso3,
        countryname,
        date_range,
    )
    if dataset is None:
        return retriever_resource.folder
    dataset.add_country_location(countryiso3)
    dataset.set_expected_update_frequency("As needed")
    dataset.update_from_yaml(_config_path(dataset, "hdx_dataset_static.yaml"))
    if hdx_config.read_only:
        context.log.info(
            f"read_only=True: not writing dataset '{dataset['name']}' to HDX."
        )
    else:
        dataset.create_in_hdx(
            remove_additional_resources=True,
            updated_by_script="HDX Scraper: OPHI",
            batch=retriever_resource.batch,
        )
    showcase = dataset_generator.generate_showcase(countryiso3, countryname)
    if showcase and not hdx_config.read_only:
        showcase.create_in_hdx()
        showcase.add_dataset(dataset)
    return retriever_resource.folder


def _config_path(dataset: Dataset, filename: str) -> str:
    from os.path import join

    from hdx.utilities.path import script_dir_plus_file

    return str(script_dir_plus_file(join("config", filename), Pipeline))
