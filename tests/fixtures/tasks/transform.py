import luigi

from hrqb.base import PandasPickleTask
from tests.fixtures.tasks.extract import ExtractAnimalColors, ExtractAnimalNames


class PrepareAnimals(PandasPickleTask):
    stage = luigi.Parameter("Transform")
    table_name = luigi.Parameter("Animals")

    def requires(self):
        return [
            ExtractAnimalColors(pipeline=self.pipeline),
            ExtractAnimalNames(pipeline=self.pipeline),
        ]

    def get_dataframe(self):
        colors_df = self.named_inputs["ExtractAnimalColors"].read()
        names_df = self.named_inputs["ExtractAnimalNames"].read()
        return names_df.merge(colors_df, how="left", on="animal_id")
