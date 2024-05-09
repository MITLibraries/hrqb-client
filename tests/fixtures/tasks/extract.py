import luigi
import pandas as pd

from hrqb.base import PandasPickleTask, SQLQueryExtractTask
from hrqb.utils.data_warehouse import DWClient


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


class SQLExtractAnimalColors(SQLQueryExtractTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    @property
    def dwclient(self) -> DWClient:
        return DWClient(
            connection_string="sqlite:///tests/fixtures/sql_extract_task_test_data.sqlite",
            engine_parameters={},
        )

    @property
    def sql_query(self) -> str:
        return """
        select animal_id, color from animal_color
        """


class SQLExtractAnimalNames(SQLQueryExtractTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    @property
    def dwclient(self) -> DWClient:
        return DWClient(
            connection_string="sqlite:///tests/fixtures/sql_extract_task_test_data.sqlite",
            engine_parameters={},
        )

    @property
    def sql_query(self) -> str:
        return """
        select animal_id, name from animal_name
        """
