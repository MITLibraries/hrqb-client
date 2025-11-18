"""hrqb.tasks.job_titles"""

import luigi  # type: ignore[import-untyped]
import numpy as np
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask
from hrqb.tasks.employee_appointments import (
    ExtractDWEmployeeAppointments,
)
from hrqb.utils.quickbase import QBClient


class TransformUniqueJobTitles(PandasPickleTask):
    """Get unique job titles from employee appointment data."""

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [ExtractDWEmployeeAppointments(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        # extract unique job titles and pay grades from employee appointments
        job_titles_df = self.single_input_dataframe[
            ["job_title_long", "pay_grade"]
        ].drop_duplicates()

        # fetch active pay grades from Quickbase
        pay_grade_df = self._get_active_pay_grades()

        # join job titles with pay grade records
        job_titles_df = job_titles_df.merge(
            pay_grade_df, how="left", on="pay_grade"
        ).replace({np.nan: None})

        # rename columns to match Quickbase schema
        return job_titles_df.rename(
            columns={
                "job_title_long": "Job Title",
                "pay_grade_record_id": "Related Pay Grade",
            }
        )[["Job Title", "Related Pay Grade"]]

    def _get_active_pay_grades(self) -> pd.DataFrame:
        """Retrieve active pay grades from Quickbase.

        Returns dataframe of [pay_grade, pay_grade_record_id].
        """
        qbclient = QBClient()
        pay_grade_df = qbclient.get_table_as_df(
            qbclient.get_table_id("Pay Grade"),
            fields=["Pay Grade", "Record ID#", "Active Pay Grade"],
        )

        return pay_grade_df[pay_grade_df["Active Pay Grade"] == "Active"].rename(
            columns={
                "Pay Grade": "pay_grade",
                "Record ID#": "pay_grade_record_id",
            }
        )[["pay_grade", "pay_grade_record_id"]]


class LoadJobTitles(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Job Titles")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformUniqueJobTitles(pipeline=self.pipeline)]
