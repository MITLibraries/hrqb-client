"""hrqb.tasks.shared"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import PandasPickleTask
from hrqb.utils.quickbase import QBClient


class ExtractQBEmployeeAppointments(PandasPickleTask):
    """Query Quickbase for Employee Appointments.

    This task requires Employee Appointments have been loaded into Quickbase via
    Employee Appointments ETL tasks.
    """

    stage = luigi.Parameter("Extract")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        from hrqb.tasks.employee_appointments import LoadEmployeeAppointments

        return [
            LoadEmployeeAppointments(pipeline=self.pipeline),
        ]

    def get_dataframe(self) -> pd.DataFrame:  # pragma: nocover
        qbclient = QBClient()
        return qbclient.get_table_as_df(qbclient.get_table_id("Employee Appointments"))
