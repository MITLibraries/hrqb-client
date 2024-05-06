import luigi

from hrqb.base import QuickbaseUpsertTask
from tests.fixtures.tasks.transform import PrepareAnimals


class LoadAnimals(QuickbaseUpsertTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Load")
    table_name = luigi.Parameter("Animals")

    def requires(self):
        return [PrepareAnimals(pipeline=self.pipeline)]
