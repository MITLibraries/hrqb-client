"""hrqb.tasks.employee_salary_history"""

import luigi  # type: ignore[import-untyped]
import numpy as np
import pandas as pd

from hrqb.base.task import (
    HRQBTask,
    PandasPickleTask,
    QuickbaseUpsertTask,
    SQLQueryExtractTask,
)
from hrqb.exceptions import IntegrityCheckError
from hrqb.tasks.employee_appointments import TransformEmployeeAppointments
from hrqb.utils import md5_hash_from_values, normalize_dataframe_dates
from hrqb.utils.quickbase import QBClient

PERCENT_DECIMAL_ACCURACY = 5


class ExtractDWEmployeeSalaryHistory(SQLQueryExtractTask):
    """Query Data Warehouse for employee salary history data."""

    stage = luigi.Parameter("Extract")

    @property
    def sql_file(self) -> str:
        return "hrqb/tasks/sql/employee_salary_history.sql"


class TransformEmployeeSalaryHistory(PandasPickleTask):
    table_name = luigi.Parameter("Employee Salary History")
    stage = luigi.Parameter("Transform")

    @property
    def merge_field(self) -> str | None:
        return "Key"

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        from hrqb.tasks.shared import ExtractQBEmployeeAppointments

        return [
            ExtractDWEmployeeSalaryHistory(pipeline=self.pipeline),
            ExtractQBEmployeeAppointments(pipeline=self.pipeline),
        ]

    def get_dataframe(self) -> pd.DataFrame:
        dw_salary_df = self.named_inputs["ExtractDWEmployeeSalaryHistory"].read()

        # merge with employee appointment data for QB appointment record identifier
        dw_salary_df = normalize_dataframe_dates(
            dw_salary_df,
            ["appt_begin_date", "appt_end_date", "start_date", "end_date"],
        )
        dw_salary_df["emp_appt_merge_key"] = dw_salary_df.apply(
            lambda row: TransformEmployeeAppointments.generate_merge_key(
                row.mit_id,
                row.position_id,
                row.appt_begin_date,
            ),
            axis=1,
        )
        qb_emp_appts_df = self._get_employee_appointments()
        salary_df = dw_salary_df.merge(
            qb_emp_appts_df, how="left", on="emp_appt_merge_key"
        )

        # convert efforts to percentages
        salary_df["original_effort"] = salary_df["original_effort"] / 100.0
        salary_df["temp_effort"] = salary_df["temp_effort"] / 100.0

        # set base salary change percentage from previous record, for same position
        salary_df = self._set_base_salary_change_percent(salary_df)

        # calculate effective salary and determine effective change percent since previous
        salary_df = self._set_effective_salary_and_change_percent(salary_df)

        # mint a unique, deterministic value for the merge "Key" field
        salary_df["key"] = salary_df.apply(
            lambda row: md5_hash_from_values(
                [
                    row.mit_id,
                    row.position_id,
                    str(row.appointment_begin_date),
                    str(row.start_date),
                ]
            ),
            axis=1,
        )

        fields = {
            "mit_id": "MIT ID",
            "related_employee_appointment_id": "Related Employee Appointment",
            "hr_personnel_action": "Related Salary Change Type",
            "hr_action_reason": "Salary Change Reason",
            "start_date": "Start Date",
            "end_date": "End Date",
            "temp_base_change_percent": "Temp Change to Base %",
            "original_base_amount": "Base Salary",
            "original_hourly_rate": "Hourly",
            "original_effort": "Effort %",
            "base_change_percent": "Salary Change %",
            "temp_change_base_amount": "Temp Base Salary",
            "temp_change_hourly_rate": "Temp Hourly",
            "temp_effort": "Temp Effort %",
            "key": "Key",
            "effective_change_percent": "Effective Salary Change %",
        }
        return salary_df[fields.keys()].rename(columns=fields)

    def _get_employee_appointments(self) -> pd.DataFrame:
        qb_emp_appts_df = self.named_inputs["ExtractQBEmployeeAppointments"].read()
        return qb_emp_appts_df[
            [
                "Record ID#",
                "Key",
                "Begin Date",
                "End Date",
            ]
        ].rename(
            columns={
                "Record ID#": "related_employee_appointment_id",
                "Key": "emp_appt_merge_key",
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
            PERCENT_DECIMAL_ACCURACY,
        )
        new_salary_df["base_change_percent"] = new_salary_df["base_change_percent"].where(
            new_salary_df["previous_base_amount"].notna(), 0.0
        )
        return new_salary_df

    def _set_effective_salary_and_change_percent(
        self, salary_df: pd.DataFrame
    ) -> pd.DataFrame:
        new_salary_df = salary_df.copy()

        # set effective salary to a temp base, or use the original base amount
        new_salary_df["effective_salary"] = (
            new_salary_df["temp_change_base_amount"]
            .replace(0, np.nan)
            .fillna(new_salary_df["original_base_amount"])
        )

        # calculate effective change percent since last row
        new_salary_df["previous_effective_salary"] = (
            new_salary_df.sort_values(
                [
                    "mit_id",
                    "appointment_begin_date",
                    "appointment_end_date",
                ]
            )
            .groupby(["mit_id", "hr_appt_key"])["effective_salary"]
            .shift(1)
        )
        new_salary_df["effective_change_percent"] = round(
            (
                new_salary_df["effective_salary"]
                / new_salary_df["previous_effective_salary"]
                - 1.0
            ),
            PERCENT_DECIMAL_ACCURACY,
        )
        new_salary_df["effective_change_percent"] = new_salary_df[
            "effective_change_percent"
        ].where(new_salary_df["previous_effective_salary"].notna(), 0.0)

        return new_salary_df

    @HRQBTask.integrity_check
    def all_rows_have_employee_appointments(self, output_df: pd.DataFrame) -> None:
        missing_appointment_count = len(
            output_df[output_df["Related Employee Appointment"].isna()]
        )
        if missing_appointment_count > 0:
            message = (
                f"{missing_appointment_count} rows are missing an Employee "
                f"Appointment for task '{self.name}'"
            )
            raise IntegrityCheckError(message)

    @HRQBTask.integrity_check
    def merge_field_values_are_unique(self, output_df: pd.DataFrame) -> None:
        if not output_df[self.merge_field].is_unique:
            message = f"Values for merge field {self.merge_field} are not unique."
            raise IntegrityCheckError(message)

    @HRQBTask.integrity_check
    def qb_row_count_less_than_or_equal_transformed_row_count(
        self, output_df: pd.DataFrame
    ) -> None:
        """Ensure Quickbase row count is less than or equal to transformed records.

        Each run of this task retrieves ALL data from the data warehouse.  If Quickbase
        has more rows then the data warehouse transformed data, this suggests a problem.

        Args:
            - output_df: the dataframe prepared by self.get_dataframe()
        """
        qbclient = QBClient()
        qb_table_df = qbclient.get_table_as_df(
            qbclient.get_table_id(self.table_name),
            fields=["Record ID#"],
        )

        qb_count = len(qb_table_df)
        transformed_count = len(output_df)

        if qb_count > transformed_count:
            message = (
                f"For table '{self.table_name}', the Quickbase row count of {qb_count} "
                f"exceeds this run's transformed row count of {transformed_count}. "
                "This should not happen."
            )
            raise IntegrityCheckError(message)


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
