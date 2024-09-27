"""hrqb.tasks.employee_leave_balances"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import (
    PandasPickleTask,
    QuickbaseUpsertTask,
    SQLQueryExtractTask,
)
from hrqb.utils import (
    md5_hash_from_values,
    normalize_dataframe_dates,
)


class ExtractDWEmployeeLeaveBalances(SQLQueryExtractTask):
    """Query Data Warehouse for employee leave balance data."""

    stage = luigi.Parameter("Extract")

    @property
    def sql_file(self) -> str:
        return "hrqb/tasks/sql/employee_leave_balances.sql"


class TransformEmployeeLeaveBalances(PandasPickleTask):

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:
        return [ExtractDWEmployeeLeaveBalances(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        dw_leave_balances_df = self.single_input_dataframe

        dw_leave_balances_df = normalize_dataframe_dates(
            dw_leave_balances_df,
            ["absence_balance_begin_date", "absence_balance_end_date"],
        )

        # mint a unique, deterministic value for the merge "Key" field
        dw_leave_balances_df["key"] = dw_leave_balances_df.apply(
            lambda row: md5_hash_from_values(
                [
                    row.mit_id,
                    row.balance_type,
                ]
            ),
            axis=1,
        )

        fields = {
            "key": "Key",
            "mit_id": "MIT ID",
            "balance_type": "Balance Type",
            "beginning_balance_hours": "Beginning Balance Hours",
            "deducted_hours": "Deducted Hours",
            "ending_balance_hours": "Ending Balance Hours",
            "beginning_balance_days": "Beginning Balance Days",
            "deducted_days": "Deducted Days",
            "ending_balance_days": "Ending Balance Days",
            "absence_balance_begin_date": "Absence Balance Begin Date",
            "absence_balance_end_date": "Absence Balance End Date",
        }
        return dw_leave_balances_df[fields.keys()].rename(columns=fields)


class LoadEmployeeLeaveBalances(QuickbaseUpsertTask):

    stage = luigi.Parameter("Load")
    table_name = "Employee Leave Balances"

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformEmployeeLeaveBalances(pipeline=self.pipeline)]

    @property
    def merge_field(self) -> str | None:
        return "Key"

    @property
    def input_task_to_load(self) -> str:
        return "TransformEmployeeLeaveBalances"
