"""hrqb.base.task"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base import PandasPickleTarget, QuickbaseTableTarget
from hrqb.utils import today_date
from hrqb.utils.quickbase import QBClient


class HRQBTask(luigi.Task):
    """Base Task class for all HRQB Tasks."""

    path = luigi.Parameter()
    table_name = luigi.Parameter()

    @property
    def single_input(self) -> PandasPickleTarget | QuickbaseTableTarget:
        input_count = len(self.input())
        if input_count != 1:
            message = f"Expected a single input to this Task but found: {input_count}"
            raise ValueError(message)
        return self.input()[0]

    @property
    def input_pandas_dataframe(self) -> pd.DataFrame:
        input_object = self.single_input
        data_object = input_object.read()
        if not isinstance(data_object, pd.DataFrame):
            message = f"Expected pandas Dataframe but got: {type(data_object)}"
            raise TypeError(message)
        return data_object

    @property
    def input_pandas_series(self) -> pd.Series:
        input_object = self.single_input
        data_object = input_object.read()
        if not isinstance(data_object, pd.Series):
            message = f"Expected pandas Series but got: {type(data_object)}"
            raise TypeError(message)
        return data_object


class PandasPickleTask(HRQBTask):
    """Base Task class for Tasks that write pickled pandas objects."""

    def target(self) -> PandasPickleTarget:
        return PandasPickleTarget(
            path=self.path,
            table_name=self.table_name,
        )

    def output(self) -> PandasPickleTarget:
        return self.target()


class QuickbaseUpsertTask(HRQBTask):
    """Base Task class for Tasks that upsert data to Quickbase tables."""

    def target(self) -> QuickbaseTableTarget:
        return QuickbaseTableTarget(
            path=self.path,
            table_name=self.table_name,
        )

    def output(self) -> QuickbaseTableTarget:
        return self.target()

    def get_records(self) -> list[dict]:
        """Get Records data that will be upserted to Quickbase.

        This method may be overridden if necessary if a load Task requires more complex
        behavior than a straight conversion of the parent's DataFrame to a dictionary.
        """
        return self.input_pandas_dataframe.to_dict(orient="records")

    def run(self) -> None:
        """Retrieve data from parent Task and upsert to Quickbase table.

        Because Load Tasks (upserting data to Quickbase) are so uniform, this run method
        can be defined on this base class.  All data required for this operation exists
        on the Task: data from parent Transform class and QB table name.
        """
        records = self.get_records()

        qbclient = QBClient()
        table_id = qbclient.get_table_id(self.table_name)
        upsert_payload = qbclient.prepare_upsert_payload(
            table_id,
            records,
            merge_field=None,
        )
        results = qbclient.upsert_records(upsert_payload)

        self.target().write(results)


class HRQBPipelineTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=today_date())
