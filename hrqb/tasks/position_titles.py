"""hrqb.tasks.position_titles"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask
from hrqb.tasks.employee_appointments import (
    ExtractDWEmployeeAppointments,
)


class TransformUniquePositionTitles(PandasPickleTask):
    """Get unique position titles from employee appointment data."""

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [ExtractDWEmployeeAppointments(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        fields = {"position_title_long": "Position Title"}
        return (
            self.single_input_dataframe[fields.keys()]
            .drop_duplicates()
            .rename(columns=fields)
        )


class LoadPositionTitles(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Position Titles")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformUniquePositionTitles(pipeline=self.pipeline)]
