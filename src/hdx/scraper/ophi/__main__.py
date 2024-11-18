"""Entry point to start OPHI pipeline"""

import logging
from os.path import expanduser, join

from src.hdx.scraper.ophi.pipeline import Pipeline

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.location.country import Country
from hdx.scraper.ophi._version import __version__
from hdx.scraper.ophi.dataset_generator import DatasetGenerator
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-ophi"
updated_by_script = "HDX Scraper: OPHI"

create_country_datasets = False


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    if not User.check_current_user_organization_access(
        "00547685-9ded-4d69-9ca5-47d5278ead7c", "create_dataset"
    ):
        raise PermissionError(
            "API Token does not give access to OPHI organisation!"
        )
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        batch = info["batch"]

        def update_dataset(dataset):
            if dataset:
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    hxl_update=False,
                    updated_by_script=updated_by_script,
                    batch=batch,
                )

        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            pipeline = Pipeline(configuration, retriever)
            mpi_national_path, mpi_subnational_path, trend_path = (
                pipeline.process()
            )
            dataset_generator = DatasetGenerator(
                configuration,
                mpi_national_path,
                mpi_subnational_path,
                trend_path,
            )
            if create_country_datasets:
                dataset_generator.load_showcase_links(retriever)
            standardised_global = pipeline.get_standardised_global()
            standardised_global_trend = (
                pipeline.get_standardised_global_trend()
            )
            standardised_countries = pipeline.get_standardised_countries()
            standardised_countries_trend = (
                pipeline.get_standardised_countries_trend()
            )
            date_ranges = pipeline.get_date_ranges()
            dataset = dataset_generator.generate_global_dataset(
                folder,
                standardised_global,
                standardised_global_trend,
                date_ranges["global"],
            )
            dataset.add_country_locations(list(standardised_countries.keys()))
            update_dataset(dataset)

            if create_country_datasets:
                for (
                    countryiso3,
                    standardised_country,
                ) in standardised_countries.items():
                    countryname = Country.get_country_name_from_iso3(
                        countryiso3
                    )
                    standardised_country_trend = (
                        standardised_countries_trend.get(countryiso3)
                    )
                    dataset = dataset_generator.generate_dataset(
                        folder,
                        standardised_country,
                        standardised_country_trend,
                        countryiso3,
                        countryname,
                        date_ranges[countryiso3],
                    )
                    dataset.add_country_location(countryiso3)
                    update_dataset(dataset)

    logger.info("HDX Scraper OPHI pipeline completed!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
