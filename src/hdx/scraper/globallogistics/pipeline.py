#!/usr/bin/python
"""Global Logistics scraper"""

import datetime
import logging
from os.path import exists
from typing import Dict, List, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.loader import load_json
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        tempdir: str,
        today: datetime,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir
        self._today = today

    def get_countries(self) -> List[Dict]:
        countries = []
        countriesdata = Country.countriesdata()
        for countryiso, countryinfo in countriesdata["countries"].items():
            income_level = countryinfo["#indicator+incomelevel"]
            if income_level.lower() == "high":
                continue
            countries.append(
                {"iso3": countryiso, "name": countryinfo["#country+name+preferred"]}
            )
        return countries

    def generate_dataset(self, countryinfo: Dict, feature: str) -> Optional[Dataset]:
        countryiso = countryinfo["iso3"]
        countryname = countryinfo["name"]
        dataset_name = f"{countryiso.lower()}-global-logistics-{feature}"
        dataset_title = f"{countryname}: Global Logistics Geodata for {feature}"
        # Dataset info
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
                "notes": f"Locations of {feature} in {countryname}",
            }
        )
        try:
            dataset.add_country_location(countryiso)
        except HDXError:
            logger.error(f"Couldn't find country {countryiso}, skipping")
            return None
        dataset.set_time_period(self._today)
        dataset.set_expected_update_frequency("As needed")
        tags = self._configuration["tags"] + [feature]
        dataset.add_tags(tags)
        # Only if needed
        dataset.set_subnational(True)

        base_url = self._configuration["base_url"]
        url = f"{base_url}{feature}?iso3={countryiso}&f=geojson&pageSize=500"
        filename = f"{countryiso}_{feature}.geojson"
        try:
            path = self._retriever.download_file(url, filename=filename)
            if not exists(path):
                return None
            json = load_json(path)
            if len(json["features"]) == 0:
                logger.warning(
                    f"No features in returned geojson for {feature} in {countryiso}!"
                )
                return None
            resource = Resource({"name": filename, "description": dataset_title})
            resource.set_format("geojson")
            resource.set_file_to_upload(path)
            dataset.add_update_resource(resource)
        except DownloadError as ex:
            logger.exception(ex)
            return None
        return dataset

    def generate_datasets(self, countryinfo: Dict) -> List[Dataset]:
        datasets = []
        for feature in self._configuration["features"]:
            dataset = self.generate_dataset(countryinfo, feature)
            if dataset:
                datasets.append(dataset)
        return datasets
