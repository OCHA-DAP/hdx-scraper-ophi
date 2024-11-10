from typing import Dict, List

from hdx.api.configuration import Configuration
from hdx.location.adminlevel import AdminLevel
from hdx.scraper.framework.utilities.reader import Read
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
    ) -> None:
        self._configuration = configuration
        self._reader = Read.get_reader("hdx")
        self._adminone = AdminLevel(admin_level=1, retriever=self._reader)
        self._adminone.setup_from_url()
        self._headers = None
        self._countriesdata = {}
        self._standardised_global = []
        self._standardised_countries = {}

    def process(self) -> None:
        datasetinfo = self._configuration["datasetinfo"]
        self._headers, iterator = self._reader.read(datasetinfo)
        for inrow in iterator:
            countryiso3 = inrow["ISO country code"]
            if not countryiso3:
                continue
            dict_of_lists_add(self._countriesdata, countryiso3, inrow)
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
                date_range = inrow[f"MPI data source {timepoint} Year"].split(
                    "-"
                )
                if len(date_range) == 2:
                    row["reference_period_start"], _ = parse_date_range(
                        date_range[0]
                    )
                    _, row["reference_period_end"] = parse_date_range(
                        date_range[1]
                    )
                else:
                    (
                        row["reference_period_start"],
                        row["reference_period_end"],
                    ) = parse_date_range(date_range[0])
                self._standardised_global.append(row)
                dict_of_lists_add(
                    self._standardised_countries, countryiso3, row
                )

    def get_countriesdata(self) -> Dict:
        return self._countriesdata

    def get_standardised_globaldata(self) -> List:
        return self._standardised_global

    def get_standardised_countriesdata(self) -> Dict:
        return self._standardised_countries
