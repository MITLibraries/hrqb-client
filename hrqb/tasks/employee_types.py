"""hrqb.tasks.employee_types"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask
from hrqb.tasks.employee_appointments import (
    ExtractDWEmployeeAppointments,
)


class TransformEmployeeTypes(PandasPickleTask):
    """Get unique employee titles from employee appointment data."""

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [ExtractDWEmployeeAppointments(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        fields = {"employee_type": "Employee Type"}
        return (
            self.single_input_dataframe[fields.keys()]
            .drop_duplicates()
            .rename(columns=fields)
        )


class LoadEmployeeTypes(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Employee Types")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformEmployeeTypes(pipeline=self.pipeline)]
