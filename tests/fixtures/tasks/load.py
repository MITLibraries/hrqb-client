import json

import luigi

from hrqb.base import QuickbaseUpsertTask
from tests.fixtures.tasks.transform import PrepareAnimals


class LoadAnimals(QuickbaseUpsertTask):
    stage = luigi.Parameter("Load")
    table_name = luigi.Parameter("Animals")

    def requires(self):
        return [PrepareAnimals(pipeline=self.pipeline)]


class LoadAnimalsDebug(QuickbaseUpsertTask):
    stage = luigi.Parameter("Load")
    table_name = luigi.Parameter("Animals")

    def requires(self):
        return [PrepareAnimals(pipeline=self.pipeline)]

    def run(self):
        """Override default method to print input data and simulate successful upsert."""
        print(self.single_input_dataframe)  # noqa: T201
        with open("tests/fixtures/qb_api_responses/upsert.json") as f:
            self.target.write(json.load(f))


class LoadTaskMultipleRequired(QuickbaseUpsertTask):
    stage = luigi.Parameter("Load")

    @property
    def input_task_to_load(self) -> str | None:
        return "ExtractAnimalColors"

    def requires(self):
        """Example where Load task has multiple required parent tasks."""
        from tests.fixtures.tasks.extract import ExtractAnimalColors, ExtractAnimalNames

        return [
            ExtractAnimalColors(pipeline=self.pipeline),
            ExtractAnimalNames(pipeline=self.pipeline),
        ]
