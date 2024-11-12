from typing import Dict, List, Tuple

from hdx.api.configuration import Configuration
from hdx.location.adminlevel import AdminLevel
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.retriever import Retrieve


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
        self._standardised_global = []
        self._standardised_global_trend = []
        self._standardised_countries = {}
        self._standardised_countries_trend = {}
        self._date_ranges = {}

    def process_date(
        self, countryiso3: str, date_range: str, row: Dict
    ) -> None:
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

    def process(self) -> Tuple[str, str]:
        datasetinfo = self._configuration["datasetinfo"]
        format = datasetinfo["format"]
        headers = datasetinfo["headers"]
        url = datasetinfo["trend_over_time"]
        sheet = datasetinfo["trend_sheet"]
        trend_path = self._retriever.download_file(
            url, "trends-over-time-mpi.xlsx"
        )
        _, iterator = self._retriever.downloader.get_tabular_rows(
            trend_path,
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
                self.process_date(countryiso3, date_range, row)
                self._standardised_global_trend.append(row)
                dict_of_lists_add(
                    self._standardised_countries_trend, countryiso3, row
                )

        url = datasetinfo["mpi_and_partial_indices"]
        sheet = datasetinfo["mpi_sheet"]
        mpi_path = self._retriever.download_file(
            url, "subnational-results-mpi.xlsx"
        )
        _, iterator = self._retriever.downloader.get_tabular_rows(
            mpi_path,
            format=format,
            sheet=sheet,
            headers=headers,
            dict_form=True,
        )
        for inrow in iterator:
            countryiso3 = inrow["ISO country code"]
            if not countryiso3:
                continue
            admin1_name = inrow["Subnational  region"]
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
            self.process_date(countryiso3, date_range, row)
            self._standardised_global.append(row)
            dict_of_lists_add(self._standardised_countries, countryiso3, row)
        return trend_path, mpi_path

    def get_standardised_global(self) -> List:
        return self._standardised_global

    def get_standardised_countries(self) -> Dict:
        return self._standardised_countries

    def get_standardised_global_trend(self) -> List:
        return self._standardised_global_trend

    def get_standardised_countries_trend(self) -> Dict:
        return self._standardised_countries_trend

    def get_date_ranges(self) -> Dict:
        return self._date_ranges
