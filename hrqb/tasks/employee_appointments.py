"""hrqb.tasks.employee_appointments"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import (
    HRQBTask,
    PandasPickleTask,
    QuickbaseUpsertTask,
    SQLQueryExtractTask,
)
from hrqb.exceptions import IntegrityCheckError
from hrqb.utils import md5_hash_from_values, normalize_dataframe_dates
from hrqb.utils.quickbase import QBClient


class ExtractDWEmployeeAppointments(SQLQueryExtractTask):
    """Query Data Warehouse for employee appointment data."""

    stage = luigi.Parameter("Extract")

    @property
    def sql_file(self) -> str:
        return "hrqb/tasks/sql/employee_appointments.sql"


class TransformEmployeeAppointments(PandasPickleTask):
    """Transform Data Warehouse data for Employee Appointments QB table."""

    stage = luigi.Parameter("Transform")
    table_name = "Employee Appointments"

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [ExtractDWEmployeeAppointments(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        emp_appts_df = self.single_input_dataframe

        emp_appts_df = normalize_dataframe_dates(
            emp_appts_df,
            [
                "appt_begin_date",
                "appt_end_date",
            ],
        )

        # mint a unique, deterministic value for the merge "Key" field
        emp_appts_df["key"] = emp_appts_df.apply(
            lambda row: self.generate_merge_key(
                row.mit_id,
                row.position_id,
                row.appt_begin_date,
            ),
            axis=1,
        )

        fields = {
            "mit_id": "MIT ID",
            "employee_type": "Related Employee Type",
            "appt_begin_date": "Begin Date",
            "appt_end_date": "End Date",
            "job_title_long": "Related Job Title",
            "position_title_long": "Related Position Title",
            "job_family": "Job Family",
            "job_subfamily": "Job Subfamily",
            "job_track": "Job Track",
            "position_id": "Position ID",
            "exempt": "Exempt / NE",
            "union_name": "Union Name",
            "term_or_perm": "Term or Permanent",
            "benefits_group_type": "Benefits Group Type",
            "key": "Key",
        }

        return emp_appts_df[fields.keys()].rename(columns=fields)

    @staticmethod
    def generate_merge_key(
        mit_id: str,
        position_id: str,
        appt_begin_date: str,
    ) -> str:
        return md5_hash_from_values(
            [
                mit_id,
                position_id,
                appt_begin_date,
            ]
        )

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


class LoadEmployeeAppointments(QuickbaseUpsertTask):

    stage = luigi.Parameter("Load")
    table_name = "Employee Appointments"

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        from hrqb.tasks.employee_types import LoadEmployeeTypes
        from hrqb.tasks.job_titles import LoadJobTitles
        from hrqb.tasks.position_titles import LoadPositionTitles

        return [
            LoadEmployeeTypes(pipeline=self.pipeline),
            LoadJobTitles(pipeline=self.pipeline),
            LoadPositionTitles(pipeline=self.pipeline),
            TransformEmployeeAppointments(pipeline=self.pipeline),
        ]

    @property
    def merge_field(self) -> str | None:
        return "Key"

    @property
    def input_task_to_load(self) -> str:
        return "TransformEmployeeAppointments"
