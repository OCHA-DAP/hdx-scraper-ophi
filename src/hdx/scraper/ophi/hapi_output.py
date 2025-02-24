from logging import getLogger
from typing import Dict, List, Optional

from hdx.api.configuration import Configuration
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.utilities.dictandlist import invert_dictionary

logger = getLogger(__name__)


class HAPIOutput:
    def __init__(
        self,
        configuration: Configuration,
        adminone: AdminLevel,
        standardised_rows: Dict,
        standardised_trend_rows: Dict,
    ) -> None:
        self._configuration = configuration
        self._adminone = adminone
        self._standardised_rows = standardised_rows
        self._standardised_trend_rows = standardised_trend_rows
        self._rows = {}

    def create_rows(
        self, rows: Dict, dataset_id: str, resource_id: str
    ) -> None:
        hxltag_to_header = invert_dictionary(
            self._configuration["hapi_dataset"]["resource"]["hxltags"]
        )
        input_to_output = {}
        for header, hxltag in self._configuration["hxltags"].items():
            input_to_output[header] = hxltag_to_header.get(hxltag)

        for row in rows.values():
            output_row = {}
            for header in row:
                output_header = input_to_output[header]
                if output_header is not None:
                    output_row[output_header] = row[header]
            countryiso3 = output_row["location_code"]
            output_row["has_hrp"] = (
                "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
            )
            output_row["in_gho"] = (
                "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"
            )
            provider_admin1_name = output_row["admin1_name"]
            output_row["provider_admin1_name"] = provider_admin1_name
            admin1_code = output_row["admin1_code"]
            if admin1_code:
                output_row["admin1_name"] = self._adminone.pcode_to_name[
                    admin1_code
                ]
                output_row["admin_level"] = 1
            else:
                output_row["admin1_name"] = ""
                if provider_admin1_name:
                    output_row["admin_level"] = 1
                else:
                    output_row["admin_level"] = 0
            output_row["dataset_hdx_id"] = dataset_id
            output_row["resource_hdx_id"] = resource_id
            key = (
                countryiso3,
                output_row["provider_admin1_name"],
                output_row["admin1_code"],
                output_row["reference_period_end"],
            )
            self._rows[key] = output_row

    def process(
        self,
        dataset_id: str,
        resource_ids: List[str],
    ) -> Optional[Dict]:
        self.create_rows(self._standardised_rows, dataset_id, resource_ids[0])
        self.create_rows(
            self._standardised_trend_rows, dataset_id, resource_ids[1]
        )
        return self._rows
