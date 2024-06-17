"""hrqb.tasks.employee_salary_history"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask, SQLQueryExtractTask
from hrqb.utils import md5_hash_from_values, normalize_dataframe_dates


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

        # merge with employee appointment data for QB appointment record identifier
        qb_emp_appts_df = self._get_employee_appointments()
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

        # set base salary change percentage from previous record, for same position
        salary_df = self._set_base_salary_change_percent(salary_df)

        # mint a unique, deterministic value for the merge "Key" field
        salary_df["key"] = salary_df.apply(
            lambda row: md5_hash_from_values(
                [
                    row.mit_id,
                    row.position_id,
                    row.appointment_begin_date,
                    row.appointment_end_date,
                    row.start_date,
                    row.end_date,
                ]
            ),
            axis=1,
        )

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
            "base_change_percent": "Salary Change %",
            "temp_change_base_amount": "Temp Base Salary",
            "temp_change_hourly_rate": "Temp Hourly",
            "temp_effort": "Temp Effort %",
            "key": "Key",
        }
        return salary_df[fields.keys()].rename(columns=fields)

    def _get_employee_appointments(self) -> pd.DataFrame:
        qb_emp_appts_df = self.named_inputs["ExtractQBEmployeeAppointments"].read()
        return qb_emp_appts_df[
            [
                "Record ID#",
                "HR Appointment Key",
                "Begin Date",
                "End Date",
            ]
        ].rename(
            columns={
                "Record ID#": "related_employee_appointment_id",
                "Begin Date": "appointment_begin_date",
                "End Date": "appointment_end_date",
            }
        )

    def _set_base_salary_change_percent(self, salary_df: pd.DataFrame) -> pd.DataFrame:
        """Create column with percentage change between sequential salaries.

        This method:
            1. sorts by appointment MIT ID and appointment dates
            2. groups the salary dataframe by MIT ID and unique appointment identifier
            3. select the base salary from the PREVIOUS salary
            4. calculates percentage change
        """
        new_salary_df = salary_df.copy()
        new_salary_df["previous_base_amount"] = (
            new_salary_df.sort_values(
                [
                    "mit_id",
                    "appointment_begin_date",
                    "appointment_end_date",
                ]
            )
            .groupby(["mit_id", "hr_appt_key"])["original_base_amount"]
            .shift(1)
        )
        new_salary_df["base_change_percent"] = round(
            (
                new_salary_df["original_base_amount"]
                / new_salary_df["previous_base_amount"]
                - 1.0
            ),
            3,
        )
        new_salary_df["base_change_percent"] = new_salary_df["base_change_percent"].where(
            new_salary_df["previous_base_amount"].notna(), 0.0
        )
        return new_salary_df


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
        return "Key"

    @property
    def input_task_to_load(self) -> str | None:
        return "TransformEmployeeSalaryHistory"
