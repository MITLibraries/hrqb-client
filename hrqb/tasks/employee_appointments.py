"""hrqb.tasks.employee_appointments"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask, SQLQueryExtractTask
from hrqb.utils import md5_hash_from_values, normalize_dataframe_dates
from hrqb.utils.quickbase import QBClient


class ExtractDWEmployeeAppointments(SQLQueryExtractTask):
    """Query Data Warehouse for employee appointment data."""

    stage = luigi.Parameter("Extract")

    @property
    def sql_file(self) -> str:
        return "hrqb/tasks/sql/employee_appointments.sql"


class ExtractQBLibHREmployeeAppointments(PandasPickleTask):
    """Query Quickbase for data provided by Library HR about employee appointments."""

    stage = luigi.Parameter("Extract")

    def get_dataframe(self) -> pd.DataFrame:  # pragma: nocover
        qbclient = QBClient()
        return qbclient.get_table_as_df(
            qbclient.get_table_id("LibHR Employee Appointments")
        )


class ExtractQBDepartments(PandasPickleTask):
    """Query Quickbase for Department data to merge with Library HR data."""

    stage = luigi.Parameter("Extract")

    def get_dataframe(self) -> pd.DataFrame:  # pragma: nocover
        qbclient = QBClient()
        return qbclient.get_table_as_df(qbclient.get_table_id("Departments"))


class TransformEmployeeAppointments(PandasPickleTask):
    """Combine Data Warehouse and Library HR data for Employee Appointments QB table."""

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [
            ExtractDWEmployeeAppointments(pipeline=self.pipeline),
            ExtractQBLibHREmployeeAppointments(pipeline=self.pipeline),
            ExtractQBDepartments(pipeline=self.pipeline),
        ]

    def get_dataframe(self) -> pd.DataFrame:
        dw_emp_appts_df = self.named_inputs["ExtractDWEmployeeAppointments"].read()
        libhr_df = self.named_inputs["ExtractQBLibHREmployeeAppointments"].read()
        depts_df = self.named_inputs["ExtractQBDepartments"].read()

        # filter libhr data to active appointments, with position IDs
        libhr_df = libhr_df[(libhr_df["Active"]) & ~(libhr_df["Position ID"].isna())]

        # normalize position id to string and pad zeros
        libhr_df["Position ID"] = libhr_df["Position ID"].apply(
            lambda x: str(int(x)).zfill(8)
        )

        # merge data warehouse data with libhr data to create new employee appointments df
        emp_appts_df = dw_emp_appts_df.merge(
            libhr_df[
                [
                    "Related Employee MIT ID",
                    "Position ID",
                    "Related Supervisor MIT ID",
                    "HC ID",
                    "Related Department ID",
                    "Cost Object",
                ]
            ],
            how="left",
            left_on=["position_id", "mit_id"],
            right_on=["Position ID", "Related Employee MIT ID"],
        )

        # merge on departments to get directorates
        emp_appts_df = emp_appts_df.merge(
            depts_df[["Record ID#", "Directorate"]],
            how="left",
            left_on="Related Department ID",
            right_on="Record ID#",
        )

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
                row.appt_end_date,
            ),
            axis=1,
        )

        fields = {
            "mit_id": "MIT ID",
            "HC ID": "HC ID",
            "employee_type": "Related Employee Type",
            "appt_begin_date": "Begin Date",
            "appt_end_date": "End Date",
            "Directorate": "Related Directorate",
            "Related Department ID": "Related Department ID",
            "Related Supervisor MIT ID": "Supervisor",
            "job_title_long": "Related Job Title",
            "position_title_long": "Related Position Title",
            "job_family": "Job Family",
            "job_subfamily": "Job Subfamily",
            "job_track": "Job Track",
            "position_id": "Position ID",
            "Cost Object": "Cost Object",
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
        appt_end_date: str,
    ) -> str:
        return md5_hash_from_values(
            [
                mit_id,
                position_id,
                appt_begin_date,
                appt_end_date,
            ]
        )


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
