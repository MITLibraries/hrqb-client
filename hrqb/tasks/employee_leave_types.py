"""hrqb.tasks.employee_leave_types"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask
from hrqb.tasks.employee_leave import TransformEmployeeLeave


class TransformEmployeeLeaveTypes(PandasPickleTask):
    """Get unique employee leave types from employee leave data.

    Required fields:
        - Leave Type: lookup table value
        - Paid Leave: [Yes, No]
        - Accrue Seniority: [Yes, No, N/A]
    """

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformEmployeeLeave(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        leaves_df = self.single_input_dataframe
        fields = {
            "Related Leave Type": "Leave Type",
            "Paid Leave": "Paid Leave",
            "Accrue Seniority": "Accrue Seniority",
        }
        return leaves_df[fields.keys()].drop_duplicates().rename(columns=fields)


class LoadEmployeeLeaveTypes(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Leave Types")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformEmployeeLeaveTypes(pipeline=self.pipeline)]
