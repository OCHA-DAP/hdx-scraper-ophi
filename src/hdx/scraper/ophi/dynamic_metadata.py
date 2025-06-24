from hdx.api.configuration import Configuration
from hdx.utilities.retriever import Retrieve


class DynamicMetadata:
    def __init__(self, configuration: Configuration, retriever: Retrieve):
        url = configuration["dynamic_metadata"]
        headers, iterator = retriever.get_tabular_rows(
            url, dict_form=True, format="csv"
        )
        self._description = {}
        self._methodology_note = {}
        for row in iterator:
            field = row["Field"]
            applies = row["Applies"]
            value = row["Value"]
            if field == "Description":
                self._description[applies] = value
            elif field == "Methodology Note":
                self._methodology_note[applies] = value

    def get_description(self, countryiso3: str) -> str:
        description = self._description["default"]
        additional_notes = self._description.get(countryiso3)
        if additional_notes:
            description += f"  \n{additional_notes}"
        return description

    def get_methodology_note(self, applies: str) -> str:
        return self._methodology_note[applies]
