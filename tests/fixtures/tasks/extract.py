import luigi
import pandas as pd

from hrqb.base import PandasPickleTask, SQLQueryExtractTask
from hrqb.utils.data_warehouse import DWClient

SQLITE_CONNECTION_STRING = "sqlite:///tests/fixtures/sql_extract_task_test_data.sqlite"


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
            connection_string=SQLITE_CONNECTION_STRING,
            engine_parameters={},
        )

    @property
    def sql_file(self) -> str:
        return "tests/fixtures/sql/animal_color_query.sql"


class SQLExtractAnimalNames(SQLQueryExtractTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    @property
    def dwclient(self) -> DWClient:
        return DWClient(
            connection_string=SQLITE_CONNECTION_STRING,
            engine_parameters={},
        )

    @property
    def sql_query(self) -> str:
        return """
        select animal_id, name from animal_name
        """


class SQLQueryWithParameters(SQLQueryExtractTask):
    stage = luigi.Parameter("Extract")

    @property
    def dwclient(self) -> DWClient:
        return DWClient(
            connection_string=SQLITE_CONNECTION_STRING,
            engine_parameters={},
        )

    @property
    def sql_query(self) -> str:
        return """
        select
            :foo_val as foo,
            :bar_val as bar
        """

    @property
    def sql_query_parameters(self) -> dict:
        return {"foo_val": 42, "bar_val": "apple"}
