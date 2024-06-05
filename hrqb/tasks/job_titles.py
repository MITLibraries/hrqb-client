"""hrqb.tasks.job_titles"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask
from hrqb.tasks.employee_appointments import (
    ExtractDWEmployeeAppointments,
)


class TransformUniqueJobTitles(PandasPickleTask):
    """Get unique job titles from employee appointment data."""

    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [ExtractDWEmployeeAppointments(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        fields = {"job_title_long": "Job Title", "pay_grade": "Pay Grade"}
        return (
            self.single_input_dataframe[fields.keys()]
            .drop_duplicates()
            .rename(columns=fields)
        )


class LoadJobTitles(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Job Titles")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformUniqueJobTitles(pipeline=self.pipeline)]
