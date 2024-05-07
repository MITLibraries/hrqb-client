import luigi

from hrqb.base import QuickbaseUpsertTask
from tests.fixtures.tasks.transform import PrepareAnimals


class LoadAnimals(QuickbaseUpsertTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Load")
    table_name = luigi.Parameter("Animals")

    def requires(self):
        return [PrepareAnimals(pipeline=self.pipeline)]


class LoadAnimalsDebug(QuickbaseUpsertTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Load")
    table_name = luigi.Parameter("Animals")

    def requires(self):
        return [PrepareAnimals(pipeline=self.pipeline)]

    def run(self):
        """Override default method to print data instead of upsert to Quickbase."""
        print(self.single_input_dataframe)  # noqa: T201
        self.target().write({"note": "data printed to console"})
