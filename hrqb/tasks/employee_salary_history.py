"""hrqb.tasks.employee_salary_history"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask, SQLQueryExtractTask
from hrqb.utils import normalize_dataframe_dates


class ExtractDWEmployeeSalaryHistory(SQLQueryExtractTask):
    """Query Data Warehouse for employee salary history data."""

    stage = luigi.Parameter("Extract")

    @property
    def sql_file(self) -> str:
        return "hrqb/tasks/sql/employee_salary_history.sql"


class TransformEmployeeSalaryHistory(PandasPickleTask):
    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        from hrqb.tasks.shared import ExtractQBEmployeeAppointments

        return [
            ExtractDWEmployeeSalaryHistory(pipeline=self.pipeline),
            ExtractQBEmployeeAppointments(pipeline=self.pipeline),
        ]

    def get_dataframe(self) -> pd.DataFrame:
        dw_salary_df = self.named_inputs["ExtractDWEmployeeSalaryHistory"].read()
        qb_emp_appts_df = self.named_inputs["ExtractQBEmployeeAppointments"].read()

        # merge with employee appointment data for QB appointment record identifier
        qb_emp_appts_df = qb_emp_appts_df[["Record ID#", "HR Appointment Key"]].rename(
            columns={"Record ID#": "related_employee_appointment_id"}
        )
        salary_df = dw_salary_df.merge(
            qb_emp_appts_df,
            how="left",
            left_on="hr_appt_key",
            right_on="HR Appointment Key",
        )

        salary_df = normalize_dataframe_dates(
            salary_df,
            ["start_date", "end_date"],
        )

        # convert efforts to percentages
        salary_df["original_effort"] = salary_df["original_effort"] / 100.0
        salary_df["temp_effort"] = salary_df["temp_effort"] / 100.0

        fields = {
            "hr_appt_tx_key": "HR Appointment Transaction Key",
            "mit_id": "MIT ID",
            "related_employee_appointment_id": "Related Employee Appointment",
            "hr_personnel_action": "Related Salary Change Type",
            "hr_action_reason": "Salary Change Reason",
            "start_date": "Start Date",
            "end_date": "End Date",
            "temp_base_change_percent": "Temp Salary Change %",
            "original_base_amount": "Base Salary",
            "original_hourly_rate": "Hourly",
            "original_effort": "Effort %",
            "temp_change_base_amount": "Temp Base Salary",
            "temp_change_hourly_rate": "Temp Hourly",
            "temp_effort": "Temp Effort %",
        }
        return salary_df[fields.keys()].rename(columns=fields)


class LoadEmployeeSalaryHistory(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Employee Salary History")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        from hrqb.tasks.salary_change_types import LoadSalaryChangeTypes

        return [
            LoadSalaryChangeTypes(pipeline=self.pipeline),
            TransformEmployeeSalaryHistory(pipeline=self.pipeline),
        ]

    @property
    def merge_field(self) -> str | None:
        return "HR Appointment Transaction Key"

    @property
    def input_task_to_load(self) -> str | None:
        return "TransformEmployeeSalaryHistory"
