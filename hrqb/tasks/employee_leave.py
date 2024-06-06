"""hrqb.tasks.employee_leaves"""

# ruff: noqa: S324

import hashlib

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import (
    PandasPickleTask,
    QuickbaseUpsertTask,
    SQLQueryExtractTask,
)
from hrqb.utils import convert_oracle_bools_to_qb_bools, normalize_dataframe_dates


class ExtractDWEmployeeLeave(SQLQueryExtractTask):
    """Query Data Warehouse for employee leave data."""

    stage = luigi.Parameter("Extract")

    @property
    def sql_file(self) -> str:
        return "hrqb/tasks/sql/employee_leave.sql"


class TransformEmployeeLeave(PandasPickleTask):

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:
        from hrqb.tasks.shared import ExtractQBEmployeeAppointments

        return [
            ExtractQBEmployeeAppointments(pipeline=self.pipeline),
            ExtractDWEmployeeLeave(pipeline=self.pipeline),
        ]

    def get_dataframe(self) -> pd.DataFrame:
        dw_leaves_df = self.named_inputs["ExtractDWEmployeeLeave"].read()
        qb_emp_appts_df = self.named_inputs["ExtractQBEmployeeAppointments"].read()

        qb_emp_appts_df = qb_emp_appts_df[["HR Appointment Key", "Record ID#"]].rename(
            columns={
                "HR Appointment Key": "hr_appt_key",
                "Record ID#": "related_employee_appointment_id",
            }
        )
        leaves_df = dw_leaves_df.merge(qb_emp_appts_df, how="left", on="hr_appt_key")

        leaves_df = normalize_dataframe_dates(
            leaves_df,
            ["appt_begin_date", "appt_end_date", "absence_date"],
        )

        # WIP TODO: determine what data points from combination employee leave and
        # employee appointments determine the field "Accrue Seniority".  For now,
        # placeholder of blanket "Y" (true) until this is determined.
        leaves_df["accrue_seniority"] = "Y"

        leaves_df = convert_oracle_bools_to_qb_bools(
            leaves_df, columns=["paid_leave", "accrue_seniority"]
        )

        # mint a unique MD5 hash based on leave data
        leaves_df["key"] = leaves_df.apply(
            lambda row: self._create_unique_key_from_leave_data(
                row.mit_id,
                row.absence_date,
                row.absence_type,
                row.actual_absence_hours,
            ),
            axis=1,
        )

        fields = {
            "key": "Key",
            "mit_id": "MIT ID",
            "absence_date": "Leave Date",
            "absence_type": "Related Leave Type",
            "actual_absence_hours": "Duration Hours",
            "actual_absence_days": "Duration Days",
            "paid_leave": "Paid Leave",
            "accrue_seniority": "Accrue Seniority",
            "related_employee_appointment_id": "Related Employee Appointment",
        }
        return leaves_df[fields.keys()].rename(columns=fields)

    @staticmethod
    def _create_unique_key_from_leave_data(
        mit_id: str,
        absence_date: str,
        absence_type: str,
        absence_hours: str | float,
    ) -> str:
        """Create MD5 hash based specific leave data."""
        data_string = "|".join(
            [
                mit_id,
                absence_date,
                absence_type,
                str(absence_hours),
            ]
        ).encode()
        return hashlib.md5(data_string).hexdigest()


class LoadEmployeeLeave(QuickbaseUpsertTask):

    stage = luigi.Parameter("Load")
    table_name = "Employee Leave"

    def requires(self) -> list[luigi.Task]:  # pragma: nocover

        from hrqb.tasks.employee_leave_types import LoadEmployeeLeaveTypes

        return [
            LoadEmployeeLeaveTypes(pipeline=self.pipeline),
            TransformEmployeeLeave(pipeline=self.pipeline),
        ]

    @property
    def merge_field(self) -> str | None:
        return "Key"

    @property
    def input_task_to_load(self) -> str:
        return "TransformEmployeeLeave"
