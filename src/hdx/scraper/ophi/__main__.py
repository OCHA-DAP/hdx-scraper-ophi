"""Entry point to start OPHI pipeline"""

import logging
from os.path import expanduser, join

from src.hdx.scraper.ophi.pipeline import Pipeline

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.framework.utilities.reader import Read
from hdx.scraper.ophi._version import __version__
from hdx.scraper.ophi.dataset_generator import DatasetGenerator
from hdx.utilities.dateparse import now_utc
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import (
    script_dir_plus_file,
    temp_dir, wheretostart_tempdir_batch,
)

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-ophi"
updated_by_script = "HDX Scraper: OPHI"


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
        today = now_utc()
        year = today.year
        Read.create_readers(
            folder,
            "saved_data",
            folder,
            save,
            use_saved,
            hdx_auth=configuration.get_api_key(),
            today=today,
        )
        pipeline = Pipeline(configuration)
        pipeline.process()
        standardised_global = pipeline.get_standardised_globaldata()
        dataset_generator = DatasetGenerator(configuration, year)
        dataset = dataset_generator.add_global_resource(standardised_global, folder, year)
        if dataset:
            dataset.update_in_hdx(
                operation="patch",
                match_resource_order=True,
                remove_additional_resources=False,
                hxl_update=False,
                updated_by_script=updated_by_script,
                batch=batch,
            )
        countriesdata = pipeline.get_countriesdata()
        standardised_countries = pipeline.get_standardised_countriesdata()
        for countryiso3, countrydata in countriesdata.items():
            standardised_country = standardised_countries[countryiso3]
            dataset = dataset_generator.generate_country_dataset(
                folder, countrydata, standardised_country, countryiso3, year)
            if dataset:
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                dataset.create_in_hdx(
                    remove_additional_resources=False,
                    hxl_update=False,
                    updated_by_script=updated_by_script,
                    batch=batch,
                )

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
