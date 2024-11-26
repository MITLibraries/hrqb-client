"""hrqb.tasks.libhr_employee_appointments"""

import datetime
import re

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import (
    PandasPickleTask,
    QuickbaseUpsertTask,
)
from hrqb.utils import (
    convert_dataframe_columns_to_dates,
    md5_hash_from_values,
    normalize_dataframe_dates,
)
from hrqb.utils.quickbase import QBClient


class ExtractLibHREmployeeAppointments(PandasPickleTask):
    """Extract data from Library HR provided employee appointment data.

    This task is expecting the CSV to be a local filepath.  Unlike other pipelines in this
    client, this pipeline is rarely run, and is suitable for local, developer runs to load
    data.

    Expected schema of CSV file:
        - MIT ID: str, MIT ID
        - HC ID: str, pattern of "L-###{a|b|x)}"
        - Full Name:  str (OPTIONAL; human eyes)
        - Internal Position Title: str, free-text that is stored in LibHR table
        - Position ID: str, position number used to join warehouse data
        - Employee Type: str (OPTIONAL; human eyes)
        - Supervisor ID: str, MIT ID
        - Supervisor Name: str, (OPTIONAL; human eyes)
        - Cost Object: str
        - Department: str, Department acronym
        - Begin Date: YYYY-MM-DD str, begin date when Headcount ID (HC ID) applied
        - End Date: YYYY-MM-DD str, end date when Headcount ID (HC ID) applied
        - Notes: str, free-text notes
    """

    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")
    csv_filepath = luigi.Parameter()

    def get_dataframe(self) -> pd.DataFrame:
        libhr_df = pd.read_csv(self.csv_filepath)

        # convert Begin and End dates and set "Active" column
        libhr_df = convert_dataframe_columns_to_dates(
            libhr_df, columns=["Begin Date", "End Date"]
        )

        # set Active column value of Yes/No
        def determine_active(end_date: datetime.datetime) -> str:
            if end_date is None:
                return "No"
            if end_date >= datetime.datetime.now(tz=datetime.UTC):
                return "Yes"
            return "No"

        libhr_df["Active"] = libhr_df["End Date"].apply(determine_active)

        # normalize Headcount ID, raising exceptions if not properly formed or absent
        def remove_headcount_id_suffixes(original_headcount_id: str | None) -> str | None:
            if original_headcount_id is None:
                message = "LibHR CSV data is missing a Headcount ID for one or more rows."
                raise ValueError(message)
            matched_object = re.match(
                r"([T,L]-\d\d\d).*",
                original_headcount_id.strip().upper(),
            )
            if not matched_object:
                message = f"Could not parse HC ID: {original_headcount_id}"
                raise ValueError(message)
            normalized_hc_id = matched_object.group(1)
            return normalized_hc_id.upper()

        libhr_df["HC ID (Original)"] = libhr_df["HC ID"]
        libhr_df["HC ID"] = libhr_df["HC ID"].apply(remove_headcount_id_suffixes)

        return libhr_df


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

        # mint a unique, deterministic value for the merge "Key" field
        libhr_df["Key"] = libhr_df.apply(
            lambda row: md5_hash_from_values(
                [
                    str(row["MIT ID"]),
                    str(row["HC ID"]),
                    str(row["HC ID (Original)"]),
                    str(row["Begin Date"]),
                    str(row["End Date"]),
                ]
            ),
            axis=1,
        )

        libhr_df = normalize_dataframe_dates(libhr_df, ["Begin Date", "End Date"])

        fields = {
            "MIT ID": "Related Employee MIT ID",
            "Supervisor ID": "Related Supervisor MIT ID",
            "Cost Object": "Cost Object",
            "HC ID": "HC ID",
            "HC ID (Original)": "HC ID (Original)",
            "Position ID": "Position ID",
            "Internal Position Title": "Internal Position Title",
            "Related Department ID": "Related Department ID",
            "Active": "Active",
            "Key": "Key",
            "Begin Date": "Begin Date",
            "End Date": "End Date",
            "Notes": "Notes",
        }
        return libhr_df[fields.keys()].rename(columns=fields)


class LoadLibHREmployeeAppointments(QuickbaseUpsertTask):
    table_name = luigi.Parameter("LibHR Employee Appointments")
    stage = luigi.Parameter("Load")
    csv_filepath = luigi.Parameter()

    @property
    def merge_field(self) -> str | None:
        """Explicitly merge on unique Key field."""
        return "Key"

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [
            TransformLibHREmployeeAppointments(
                pipeline=self.pipeline, csv_filepath=self.csv_filepath
            )
        ]
