from hrqb.base import HRQBPipelineTask
from tests.fixtures.tasks.load import LoadAnimals, LoadAnimalsDebug


class Animals(HRQBPipelineTask):
    def requires(self):
        yield LoadAnimals(pipeline=self.pipeline_name)


class AnimalsDebug(HRQBPipelineTask):
    def requires(self):
        yield LoadAnimalsDebug(pipeline=self.pipeline_name)
