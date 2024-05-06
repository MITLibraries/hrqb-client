import luigi
import pandas as pd

from hrqb.base import PandasPickleTask


class ExtractAnimalColors(PandasPickleTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    def run(self):
        colors_df = pd.DataFrame(
            [
                (42, "green"),
                (101, "red"),
            ],
            columns=["animal_id", "color"],
        )
        self.target().write(colors_df)


class ExtractAnimalNames(PandasPickleTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    def run(self):
        names_df = pd.DataFrame(
            [
                (42, "parrot"),
                (101, "cardinal"),
            ],
            columns=["animal_id", "name"],
        )
        self.target().write(names_df)
