import luigi
import pandas as pd

from hrqb.base import PandasPickleTask


class ExtractAnimalColors(PandasPickleTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    def get_dataframe(self):
        return pd.DataFrame(
            [
                (42, "green"),
                (101, "red"),
            ],
            columns=["animal_id", "color"],
        )


class ExtractAnimalNames(PandasPickleTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    def get_dataframe(self):
        return pd.DataFrame(
            [
                (42, "parrot"),
                (101, "cardinal"),
            ],
            columns=["animal_id", "name"],
        )
