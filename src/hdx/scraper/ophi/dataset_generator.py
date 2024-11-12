import logging
from copy import copy
from typing import Dict, List, Optional

from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(
        self, configuration: Configuration, trend_path: str, mpi_path: str
    ) -> None:
        self._configuration = configuration
        self._trend_path = trend_path
        self._mpi_path = mpi_path
        self._global_hxltags = configuration["hxltags"]
        self._country_hxltags = copy(self._global_hxltags)

    def generate_resource(
        self,
        dataset: Dataset,
        resource_name: str,
        resource_description: str,
        hxltags: Dict,
        rows: List[Dict],
        folder: str,
        filename: str,
    ) -> bool:
        resourcedata = {
            "name": resource_name,
            "description": resource_description,
        }

        headers = list(hxltags.keys())
        success, results = dataset.generate_resource_from_iterable(
            headers,
            rows,
            hxltags,
            folder,
            filename,
            resourcedata,
        )
        return success

    def generate_dataset_metadata(
        self,
        title: str,
        name: str,
    ) -> Optional[Dataset]:
        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(name).lower()
        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("00547685-9ded-4d69-9ca5-47d5278ead7c")
        dataset.set_expected_update_frequency("Every year")

        tags = [
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
        ]
        dataset.add_tags(tags)

        dataset.set_subnational(True)
        return dataset

    def generate_dataset(
        self,
        folder: str,
        standardised_rows: List[Dict],
        standardised_trend_rows: List[Dict],
        countryiso3: str,
        countryname: str,
        date_range: Dict,
    ) -> Optional[Dataset]:
        if not standardised_rows:
            return None
        title = f"{countryname} Multi Dimensional Poverty Index"
        name = f"{countryname} MPI"
        dataset = self.generate_dataset_metadata(title, name)
        dataset.set_time_period(date_range["start"], date_range["end"])
        resource_description = self._configuration["resource_description"]

        resource_name = f"{countryname} MPI and Partial Indices"
        filename = f"{countryiso3}_mpi.csv"
        success = self.generate_resource(
            dataset,
            resource_name,
            resource_description,
            self._country_hxltags,
            standardised_rows,
            folder,
            filename,
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None

        if not standardised_trend_rows:
            return dataset
        resource_name = f"{countryname} MPI Trends Over Time"
        filename = f"{countryiso3}_mpi_trends.csv"
        success = self.generate_resource(
            dataset,
            resource_name,
            resource_description,
            self._country_hxltags,
            standardised_trend_rows,
            folder,
            filename,
        )
        return dataset

    def generate_global_dataset(
        self,
        folder: str,
        standardised_rows: List[Dict],
        standardised_trend_rows: List[Dict],
        date_range: Dict,
    ) -> Optional[Dataset]:
        if not standardised_rows:
            return None
        dataset = self.generate_dataset(
            folder,
            standardised_rows,
            standardised_trend_rows,
            "global",
            "Global",
            date_range,
        )

        resourcedata = {
            "name": "Trends Over Time MPI database",
            "description": self._configuration["trends_resource_description"],
        }
        resource = Resource(resourcedata)
        resource.set_format("xlsx")
        resource.set_file_to_upload(self._trend_path)
        dataset.add_update_resource(resource)

        resourcedata = {
            "name": "MPI and Partial Indices database",
            "description": self._configuration["trends_resource_description"],
        }
        resource = Resource(resourcedata)
        resource.set_format("xlsx")
        resource.set_file_to_upload(self._mpi_path)
        dataset.add_update_resource(resource)
        return dataset
