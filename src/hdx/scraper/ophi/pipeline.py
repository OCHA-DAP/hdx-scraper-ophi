import logging
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

from hdx.api.configuration import Configuration
from hdx.location.adminlevel import AdminLevel
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import dict_of_dicts_add
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
    ) -> None:
        self._configuration = configuration
        self._retriever = retriever
        self._adminone = AdminLevel(admin_level=1, retriever=self._retriever)
        self._adminone.setup_from_url()
        self._standardised_global = {}
        self._standardised_global_trend = [{}, {}]
        self._standardised_countries = {}
        self._standardised_countries_trend = [{}, {}]
        self._date_ranges = {}

    def process_date(
        self, countryiso3: str, date_range: str, row: Dict
    ) -> Tuple[datetime, datetime]:
        date_range = date_range.split("-")
        if len(date_range) == 2:
            start_date, _ = parse_date_range(date_range[0])
            _, end_date = parse_date_range(date_range[1])
        else:
            start_date, end_date = parse_date_range(
                date_range[0], max_endtime=True
            )
        row["reference_period_start"] = start_date
        row["reference_period_end"] = end_date

        def update_date_range(countryiso3: str):
            current_date_range = self._date_ranges.get(countryiso3)
            if current_date_range is None:
                self._date_ranges[countryiso3] = {
                    "start": start_date,
                    "end": end_date,
                }
            else:
                if start_date < current_date_range["start"]:
                    current_date_range["start"] = start_date
                if end_date > current_date_range["end"]:
                    current_date_range["end"] = end_date

        update_date_range(countryiso3)
        update_date_range("global")
        return start_date, end_date

    def add_row(
        self,
        countryiso3: str,
        admin1_code: str,
        admin1_name: str,
        date_range: str,
        row: Dict,
        global_dict: Dict,
        country_dict: Dict,
        msg: str,
    ) -> None:
        start_date, end_date = self.process_date(countryiso3, date_range, row)
        key = (countryiso3, admin1_code, admin1_name, start_date, end_date)
        if key in global_dict:
            logger.error(f"Key {key} already exists in {msg}!")
            return
        global_dict[key] = row
        dict_of_dicts_add(country_dict, countryiso3, key, row)

    def read_mpi_national_data(
        self, path: str, format: str, sheet: str, headers: List[str]
    ) -> None:
        _, iterator = self._retriever.downloader.get_tabular_rows(
            path,
            format=format,
            sheet=sheet,
            headers=headers,
            dict_form=True,
        )
        for inrow in iterator:
            countryiso3 = inrow["ISO country code"]
            if not countryiso3:
                continue
            row = {
                "country_code": countryiso3,
                "admin1_code": "",
                "admin1_name": "",
            }
            row["mpi"] = inrow[
                "Multidimensional poverty Multidimensional Poverty Index (MPI = H*A) Range 0 to 1"
            ]
            row["headcount_ratio"] = inrow[
                "Multidimensional poverty Headcount ratio: Population in multidimensional poverty (H) % Population"
            ]
            row["intensity_of_deprivation"] = inrow[
                "Multidimensional poverty Intensity of deprivation among the poor (A) Average % of weighted deprivations"
            ]
            row["vulnerable_to_poverty"] = inrow[
                "Multidimensional poverty Vulnerable to poverty (who experience 20-33.32% intensity of deprivations) % Population"
            ]
            row["in_severe_poverty"] = inrow[
                "Multidimensional poverty In severe poverty (severity 50% or higher) % Population"
            ]
            date_range = inrow["MPI data source Year"]
            self.add_row(
                countryiso3,
                "",
                "",
                date_range,
                row,
                self._standardised_global,
                self._standardised_countries,
                "mpi_national",
            )

    def read_mpi_subnational_data(
        self, path: str, format: str, sheet: str, headers: List[str]
    ) -> None:
        _, iterator = self._retriever.downloader.get_tabular_rows(
            path,
            format=format,
            sheet=sheet,
            headers=headers,
            dict_form=True,
        )
        for inrow in iterator:
            countryiso3 = inrow["ISO country code"]
            if not countryiso3:
                continue
            admin1_name = inrow.get("Subnational  region")
            admin1_code, _ = self._adminone.get_pcode(countryiso3, admin1_name)
            row = {
                "country_code": countryiso3,
                "admin1_code": admin1_code,
                "admin1_name": admin1_name,
            }
            row["mpi"] = inrow[
                "Multidimensional poverty by region Multidimensional Poverty Index (MPI = H*A) Range 0 to 1"
            ]
            row["headcount_ratio"] = inrow[
                "Multidimensional poverty by region Headcount ratio: Population in multidimensional poverty (H) % Population"
            ]
            row["intensity_of_deprivation"] = inrow[
                "Multidimensional poverty by region Intensity of deprivation among the poor (A) Average % of weighted deprivations"
            ]
            row["vulnerable_to_poverty"] = inrow[
                "Multidimensional poverty by region Vulnerable to poverty % Population"
            ]
            row["in_severe_poverty"] = inrow[
                "Multidimensional poverty by region In severe poverty % Population"
            ]
            date_range = inrow["MPI data source Year"]
            self.add_row(
                countryiso3,
                admin1_code,
                admin1_name,
                date_range,
                row,
                self._standardised_global,
                self._standardised_countries,
                "mpi_subnational",
            )

    def read_trends_national_data(
        self, path: str, format: str, sheet: str, headers: List[str]
    ) -> None:
        _, iterator = self._retriever.downloader.get_tabular_rows(
            path,
            format=format,
            sheet=sheet,
            headers=headers,
            dict_form=True,
        )
        for inrow in iterator:
            countryiso3 = inrow["ISO country code"]
            if not countryiso3:
                continue
            for i, timepoint in enumerate(("t0", "t1")):
                row = {
                    "country_code": countryiso3,
                    "admin1_code": "",
                    "admin1_name": "",
                }
                row["mpi"] = inrow[
                    f"Multidimensional Poverty Index (MPIT) {timepoint} Range  0 to 1"
                ]
                row["headcount_ratio"] = inrow[
                    f"Multidimensional Headcount Ratio (HT) {timepoint} % pop."
                ]
                row["intensity_of_deprivation"] = inrow[
                    f"Intensity of Poverty (AT) {timepoint} Avg % of  weighted deprivations"
                ]
                row["vulnerable_to_poverty"] = inrow[
                    f"Vulnerable to poverty {timepoint} % pop."
                ]
                row["in_severe_poverty"] = inrow[
                    f"In severe poverty {timepoint} % pop."
                ]
                date_range = inrow[f"MPI data source {timepoint} Year"]
                self.add_row(
                    countryiso3,
                    "",
                    "",
                    date_range,
                    row,
                    self._standardised_global_trend[i],
                    self._standardised_countries_trend[i],
                    "trends_subnational",
                )

    def read_trends_subnational_data(
        self, path: str, format: str, sheet: str, headers: List[str]
    ) -> None:
        _, iterator = self._retriever.downloader.get_tabular_rows(
            path,
            format=format,
            sheet=sheet,
            headers=headers,
            dict_form=True,
        )
        for inrow in iterator:
            countryiso3 = inrow["ISO country code"]
            if not countryiso3:
                continue
            admin1_name = inrow["Region"]
            admin1_code, _ = self._adminone.get_pcode(countryiso3, admin1_name)
            for i, timepoint in enumerate(("t0", "t1")):
                row = {
                    "country_code": countryiso3,
                    "admin1_code": admin1_code,
                    "admin1_name": admin1_name,
                }
                row["mpi"] = inrow[
                    f"Multidimensional Poverty Index (MPIT) {timepoint} Range 0 to 1"
                ]
                row["headcount_ratio"] = inrow[
                    f"Multidimensional Headcount Ratio (HT) {timepoint} % pop."
                ]
                row["intensity_of_deprivation"] = inrow[
                    f"Intensity of Poverty (AT) {timepoint} Avg % of  weighted deprivations"
                ]
                row["vulnerable_to_poverty"] = inrow[
                    f"Vulnerable to poverty {timepoint} % pop."
                ]
                row["in_severe_poverty"] = inrow[
                    f"In severe poverty {timepoint} % pop."
                ]
                date_range = inrow[f"MPI data source {timepoint} Year"]
                self.add_row(
                    countryiso3,
                    admin1_code,
                    admin1_name,
                    date_range,
                    row,
                    self._standardised_global_trend[i],
                    self._standardised_countries_trend[i],
                    "trends_subnational",
                )

    def process(self) -> Tuple[str, str, str]:
        datasetinfo = self._configuration["datasetinfo"]
        format = datasetinfo["format"]
        headers = datasetinfo["headers"]

        mpi_and_partial_indices = datasetinfo["mpi_and_partial_indices"]
        mpi_national = mpi_and_partial_indices["national"]
        url = mpi_national["url"]
        mpi_national_path = self._retriever.download_file(
            url, "national-results-mpi.xlsx"
        )
        sheet = mpi_national["sheet"]
        self.read_mpi_national_data(mpi_national_path, format, sheet, headers)

        mpi_subnational = mpi_and_partial_indices["subnational"]
        url = mpi_subnational["url"]
        mpi_subnational_path = self._retriever.download_file(
            url, "subnational-results-mpi.xlsx"
        )
        sheet = mpi_subnational["sheet"]
        self.read_mpi_subnational_data(
            mpi_subnational_path, format, sheet, headers
        )

        trend_over_time = datasetinfo["trend_over_time"]
        url = trend_over_time["url"]
        trend_path = self._retriever.download_file(
            url, "trends-over-time-mpi.xlsx"
        )
        sheet = trend_over_time["national_sheet"]
        self.read_trends_national_data(trend_path, format, sheet, headers)
        sheet = trend_over_time["subnational_sheet"]
        self.read_trends_subnational_data(trend_path, format, sheet, headers)

        return mpi_national_path, mpi_subnational_path, trend_path

    def get_standardised_global(self) -> Iterable:
        return self._standardised_global.values()

    def get_standardised_countries(self) -> Dict:
        return self._standardised_countries

    def get_standardised_global_trend(self) -> Iterable:
        self._standardised_global_trend[0].update(
            self._standardised_global_trend[1]
        )
        return self._standardised_global_trend[0].values()

    def get_standardised_countries_trend(self) -> Dict:
        for key, value in self._standardised_countries_trend[0].items():
            value.update(self._standardised_countries_trend[1][key])
        return self._standardised_countries_trend[0]

    def get_date_ranges(self) -> Dict:
        return self._date_ranges
