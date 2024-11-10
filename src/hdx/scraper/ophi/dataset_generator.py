import logging
from copy import copy
from typing import Dict, Optional, List

from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.scraper.framework.utilities.reader import Read

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        year: int,
    ) -> None:
        self._reader = Read.get_reader("hdx")
        self._resource_description = configuration["resource_description"]
        self._global_hxltags = configuration["hxltags"]
        self._country_hxltags = copy(self._global_hxltags)
        self._year = year

    def generate_api_resource(
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

    def generate_dataset(
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

        dataset.set_time_period_year_range(self._year)
        dataset.set_subnational(True)
        return dataset

    @staticmethod
    def get_automated_resource_filename(year: int):
        return f"global_mpi_api_{year}.csv"

    @classmethod
    def move_resource(cls, resources: List[Resource], year: int):
        filename = cls.get_automated_resource_filename(year)
        insert_before = f"Trends Over Time MPI {year} database"
        from_index = None
        to_index = None
        for i, resource in enumerate(resources):
            resource_name = resource["name"]
            if resource_name == filename:
                from_index = i
            elif resource_name.startswith(insert_before):
                to_index = i
        if to_index is None:
            # insert at the start if a manual resource for year cannot be found
            to_index = 0
        resource = resources.pop(from_index)
        if from_index < to_index:
            # to index was calculated while element was in front
            to_index -= 1
        resources.insert(to_index, resource)
        return resource

    def add_global_resource(
        self,
        rows: List[Dict],
        folder: str,
        year: int,
    ) -> Optional[Resource]:
        dataset = self._reader.read_dataset("global-mpi")
        filename = self.get_automated_resource_filename(year)
        success = self.generate_api_resource(
            dataset,
            filename,
            self._resource_description,
            self._global_hxltags,
            rows,
            folder,
            filename,
        )
        if not success:
            return None
        resources = dataset.get_resources()
        self.move_resource(resources, year)
        return dataset

    def generate_country_dataset(
        self,
        folder: str,
        rows: List[Dict],
        standardised_rows: List[Dict],
        countryiso3: str,
        year: int,
    ) -> Optional[Dataset]:
        if not rows:
            return None
        countryname = Country.get_country_name_from_iso3(countryiso3)
        title = f"{countryname} Multi Dimensional Poverty Index"
        name = f"{countryname} MPI"
        dataset = self.generate_dataset(title, name)

        resource_name = f"{name} {year}"
        filename = f"{countryiso3}_mpi_{year}_api.csv"
        success = self.generate_api_resource(
            dataset,
            resource_name,
            self._resource_description.replace(" by admin one unit", ""),
            self._country_hxltags,
            standardised_rows,
            folder,
            filename,
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None
        name = f"Trends Over Time MPI {year}"
        filename = f"{countryiso3}-trends-over-time-mpi-{year}.csv"
        resourcedata = {"name": name, "description": title}
        success = dataset.generate_resource_from_rows(
            folder,
            filename,
            rows,
            resourcedata,
        )
        dataset.add_country_location(countryiso3)
        return dataset
