from hrqb.base import HRQBPipelineTask
from tests.fixtures.tasks.load import LoadAnimals


class Animals(HRQBPipelineTask):
    def requires(self):
        yield LoadAnimals(pipeline=self.pipeline_name)
