"""hrqb.tasks.salary_change_types"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask
from hrqb.tasks.employee_salary_history import ExtractDWEmployeeSalaryHistory


class TransformSalaryChangeTypes(PandasPickleTask):
    """Get unique salary change types from employee salary history data."""

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [ExtractDWEmployeeSalaryHistory(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        fields = {"hr_personnel_action": "Salary Change Type"}
        return (
            self.single_input_dataframe[fields.keys()]
            .drop_duplicates()
            .rename(columns=fields)
        )


class LoadSalaryChangeTypes(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Salary Change Types")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformSalaryChangeTypes(pipeline=self.pipeline)]
