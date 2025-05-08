from logging import getLogger
from typing import Dict, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

logger = getLogger(__name__)


class HAPIDatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        rows: Dict,
    ) -> None:
        self._configuration = configuration["hapi_dataset"]
        self._rows = rows
        self.slugified_name = self._configuration["name"]

    def generate_dataset(self) -> Tuple[Dataset, Dict]:
        title = self._configuration["title"]
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": self.slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("40d10ece-49de-4791-9aed-e164f1d16dd1")
        dataset.set_expected_update_frequency("Every year")
        dataset.add_tags(self._configuration["tags"])
        dataset["dataset_source"] = self._configuration["dataset_source"]
        dataset["license_id"] = self._configuration["license_id"]
        dataset.set_subnational(True)

        resource_config = self._configuration["resource"]
        return dataset, resource_config

    def generate_poverty_rate_dataset(
        self,
        folder: str,
    ) -> Optional[Dataset]:
        dataset, resource_config = self.generate_dataset()

        resource_name = resource_config["name"]
        resourcedata = {
            "name": resource_name,
            "description": resource_config["description"],
            "p_coded": True,
        }
        hxltags = resource_config["hxltags"]
        filename = resource_config["filename"]

        if len(self._rows) == 0:
            logger.warning("Poverty rate has no data!")
            return None

        success, _ = dataset.generate_resource_from_iterable(
            list(hxltags.keys()),
            (self._rows[key] for key in sorted(self._rows)),
            hxltags,
            folder,
            f"{filename}.csv",
            resourcedata,
        )
        if success is False:
            logger.warning(f"{resource_name} has no data!")
            return None

        dataset.preview_off()
        return dataset
