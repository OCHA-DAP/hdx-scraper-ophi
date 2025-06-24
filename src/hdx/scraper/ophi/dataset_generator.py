import logging
from copy import copy
from typing import Dict, Iterable, Optional

from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.scraper.ophi.dynamic_metadata import DynamicMetadata
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class DatasetGenerator:
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

    def __init__(
        self,
        configuration: Configuration,
        metadata: DynamicMetadata,
        mpi_national_path: str,
        mpi_subnational_path: str,
        trend_path: str,
    ) -> None:
        self._configuration = configuration
        self._metadata = metadata
        self._showcase_links = {}
        self._mpi_national_path = mpi_national_path
        self._mpi_subnational_path = mpi_subnational_path
        self._trend_path = trend_path
        self._global_hxltags = configuration["hxltags"]
        self._country_hxltags = copy(self._global_hxltags)

    def load_showcase_links(self, retriever: Retrieve) -> Dict:
        url = self._configuration["showcaseinfo"]["urls"]
        _, iterator = retriever.get_tabular_rows(url, dict_form=True, format="csv")
        for row in iterator:
            self._showcase_links[row["Country code"]] = row["URL"]

    def generate_resource(
        self,
        dataset: Dataset,
        resource_name: str,
        resource_description: str,
        hxltags: Dict,
        rows: Iterable,
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

    def _slugified_name(self, name: str) -> str:
        return slugify(name).lower()

    def generate_dataset_metadata(
        self,
        title: str,
        name: str,
    ) -> Optional[Dataset]:
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": self._slugified_name(name),
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("00547685-9ded-4d69-9ca5-47d5278ead7c")
        dataset.add_tags(self.tags)
        dataset.set_subnational(True)
        return dataset

    @staticmethod
    def get_title(countryname: str) -> str:
        return f"{countryname} Multi Dimensional Poverty Index"

    @staticmethod
    def get_name(countryname: str) -> str:
        return f"{countryname} MPI"

    def generate_showcase(
        self,
        countryiso3: str,
        countryname: str,
    ) -> Optional[Showcase]:
        url = self._showcase_links.get(countryiso3)
        if not url:
            return None
        name = self.get_name(countryname)
        title = self.get_title(countryname)
        showcase = Showcase(
            {
                "name": f"{self._slugified_name(name)}-showcase",
                "title": title,
                "notes": self._configuration["showcaseinfo"]["notes"],
                "url": self._showcase_links[countryiso3],
                "image_url": "https://raw.githubusercontent.com/OCHA-DAP/hdx-scraper-ophi/main/ophi_mpi.jpg",
            }
        )
        showcase.add_tags(self.tags)
        return showcase

    def generate_dataset(
        self,
        folder: str,
        standardised_rows: Dict,
        standardised_trend_rows: Dict,
        countryiso3: str,
        countryname: str,
        date_range: Dict,
    ) -> Optional[Dataset]:
        if not standardised_rows:
            return None
        title = self.get_title(countryname)
        name = self.get_name(countryname)
        dataset = self.generate_dataset_metadata(title, name)
        dataset["notes"] = self._metadata.get_description(countryiso3)
        dataset.set_time_period(date_range["start"], date_range["end"])
        methodology_info = self._configuration["methodology"]
        resource_descriptions = self._configuration["resource_descriptions"]

        resource_name = f"{countryname} MPI and Partial Indices"
        filename = f"{countryiso3}_mpi.csv"
        success = self.generate_resource(
            dataset,
            resource_name,
            resource_descriptions["standardised_mpi"],
            self._country_hxltags,
            (standardised_rows[key] for key in sorted(standardised_rows)),
            folder,
            filename,
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None

        methodology_text = methodology_info["text"]
        mpi_and_partial_indices_methodology = self._metadata.get_methodology_note(
            "mpi_and_partial_indices"
        )
        dataset["methodology_other"] = (
            f"{methodology_text} [here]({mpi_and_partial_indices_methodology})"
        )
        if not standardised_trend_rows:
            dataset["methodology_other"] += "."
            return dataset
        resource_name = f"{countryname} MPI Trends Over Time"
        filename = f"{countryiso3}_mpi_trends.csv"
        success = self.generate_resource(
            dataset,
            resource_name,
            resource_descriptions["standardised_trends"],
            self._country_hxltags,
            (standardised_trend_rows[key] for key in sorted(standardised_trend_rows)),
            folder,
            filename,
        )
        trend_over_time_methodology = self._metadata.get_methodology_note(
            "trend_over_time"
        )
        dataset["methodology_other"] += f" and [here]({trend_over_time_methodology})."
        return dataset

    def generate_global_dataset(
        self,
        folder: str,
        standardised_rows: Dict,
        standardised_trend_rows: Dict,
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
        dataset.set_expected_update_frequency("Every year")

        resource_descriptions = self._configuration["resource_descriptions"]
        resourcedata = {
            "name": "MPI and Partial Indices National Database",
            "description": resource_descriptions["mpi_national"],
        }
        resource = Resource(resourcedata)
        resource.set_format("xlsx")
        resource.set_file_to_upload(self._mpi_national_path)
        dataset.add_update_resource(resource)

        resourcedata = {
            "name": "MPI and Partial Indices Subnational Database",
            "description": resource_descriptions["mpi_subnational"],
        }
        resource = Resource(resourcedata)
        resource.set_format("xlsx")
        resource.set_file_to_upload(self._mpi_subnational_path)
        dataset.add_update_resource(resource)

        resourcedata = {
            "name": "Trends Over Time MPI Database",
            "description": resource_descriptions["trends"],
        }
        resource = Resource(resourcedata)
        resource.set_format("xlsx")
        resource.set_file_to_upload(self._trend_path)
        dataset.add_update_resource(resource)

        return dataset
