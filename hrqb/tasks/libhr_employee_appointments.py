"""hrqb.tasks.libhr_employee_appointments"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import (
    PandasPickleTask,
    QuickbaseUpsertTask,
)
from hrqb.utils.quickbase import QBClient


class ExtractLibHREmployeeAppointments(PandasPickleTask):
    """Extract data from Library HR provided employee appointment data.

    This task is expecting the CSV to be a local filepath.  Unlike other pipelines in this
    client, this pipeline is rarely run, and is suitable for local, developer runs to load
    data.
    """

    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")
    csv_filepath = luigi.Parameter()

    def get_dataframe(self) -> pd.DataFrame:
        return pd.read_csv(self.csv_filepath)


class ExtractQBDepartments(PandasPickleTask):
    """Get Departments data from Quickbase."""

    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    def get_dataframe(self) -> pd.DataFrame:
        qbclient = QBClient()
        return qbclient.get_table_as_df(qbclient.get_table_id("Departments"))


class TransformLibHREmployeeAppointments(PandasPickleTask):
    """Enrich CSV data with data from other QB tables."""

    stage = luigi.Parameter("Transform")
    csv_filepath = luigi.Parameter()

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [
            ExtractLibHREmployeeAppointments(
                pipeline=self.pipeline, csv_filepath=self.csv_filepath
            ),
            ExtractQBDepartments(pipeline=self.pipeline),
        ]

    def get_dataframe(self) -> pd.DataFrame:
        libhr_df = self.named_inputs["ExtractLibHREmployeeAppointments"].read()
        departments_df = self.named_inputs["ExtractQBDepartments"].read()

        # normalize department acronym merge field
        libhr_df["Department"] = libhr_df["Department"].str.upper()
        departments_df["Department"] = departments_df["Acronym"].str.upper()

        # merge department data from quickbase with libhr data
        libhr_df = libhr_df.merge(  # type: ignore[union-attr]
            departments_df[["Department", "Record ID#"]].rename(
                columns={"Record ID#": "Related Department ID"}
            ),
            how="left",
        )

        fields = {
            "MIT ID": "Related Employee MIT ID",
            "Supervisor ID": "Related Supervisor MIT ID",
            "Cost Object": "Cost Object",
            "HC ID": "HC ID",
            "Position ID": "Position ID",
            "Related Department ID": "Related Department ID",
        }
        return libhr_df[fields.keys()].rename(columns=fields)


class LoadLibHREmployeeAppointments(QuickbaseUpsertTask):
    table_name = luigi.Parameter("LibHR Employee Appointments")
    stage = luigi.Parameter("Load")
    csv_filepath = luigi.Parameter()

    @property
    def merge_field(self) -> str | None:
        """Explicitly merge on unique Position ID field."""
        return "Position ID"

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [
            TransformLibHREmployeeAppointments(
                pipeline=self.pipeline, csv_filepath=self.csv_filepath
            )
        ]
