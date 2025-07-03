from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.globallogistics.pipeline import Pipeline


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
        with temp_dir(
            "TestGlobalLogistics",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                today = parse_date("2025-07-03")
                pipeline = Pipeline(configuration, retriever, tempdir, today)
                datasets = pipeline.generate_datasets({"iso3": "AGO", "name": "Angola"})

                dataset = datasets[0]
                assert dataset == {
                    "data_update_frequency": "-2",
                    "dataset_date": "[2025-07-03T00:00:00 TO 2025-07-03T00:00:00]",
                    "groups": [{"name": "ago"}],
                    "name": "ago-global-logistics-aerodromes",
                    "notes": "Locations of aerodromes in Angola",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "aviation",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "facilities-infrastructure",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Angola: Global Logistics Geodata for aerodromes",
                }
                resources = dataset.get_resources()
                assert resources[0] == {
                    "description": "Angola: Global Logistics Geodata for aerodromes",
                    "format": "geojson",
                    "name": "AGO_aerodromes.geojson",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }

                dataset = datasets[1]
                assert dataset == {
                    "data_update_frequency": "-2",
                    "dataset_date": "[2025-07-03T00:00:00 TO 2025-07-03T00:00:00]",
                    "groups": [{"name": "ago"}],
                    "name": "ago-global-logistics-ports",
                    "notes": "Locations of ports in Angola",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "ports",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Angola: Global Logistics Geodata for ports",
                }
                resources = dataset.get_resources()
                assert resources[0] == {
                    "description": "Angola: Global Logistics Geodata for ports",
                    "format": "geojson",
                    "name": "AGO_ports.geojson",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }

                dataset = datasets[2]
                assert dataset == {
                    "data_update_frequency": "-2",
                    "dataset_date": "[2025-07-03T00:00:00 TO 2025-07-03T00:00:00]",
                    "groups": [{"name": "ago"}],
                    "name": "ago-global-logistics-crossings",
                    "notes": "Locations of crossings in Angola",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "border crossings",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Angola: Global Logistics Geodata for crossings",
                }
                resources = dataset.get_resources()
                assert resources[0] == {
                    "description": "Angola: Global Logistics Geodata for crossings",
                    "format": "geojson",
                    "name": "AGO_crossings.geojson",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }
