"""hrqb.tasks.employees"""

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base.task import (
    PandasPickleTask,
    QuickbaseUpsertTask,
    SQLQueryExtractTask,
)
from hrqb.utils import normalize_dataframe_dates, us_state_abbreviation_to_name


class ExtractDWEmployees(SQLQueryExtractTask):
    pipeline = luigi.Parameter()
    stage = luigi.Parameter("Extract")

    @property
    def sql_file(self) -> str:
        return "hrqb/tasks/sql/employees.sql"


class TransformEmployees(PandasPickleTask):
    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [ExtractDWEmployees(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        employees_df = self.single_input_dataframe

        employees_df = normalize_dataframe_dates(
            employees_df,
            [
                "date_of_birth",
                "mit_hire_date",
                "mit_lib_hire_date",
                "appointment_end_date",
                "i9_form_expiration_date",
            ],
        )

        employees_df["home_addr_state"] = employees_df["home_addr_state"].apply(
            us_state_abbreviation_to_name
        )

        fields = {
            "mit_id": "MIT ID",
            "first_name": "First Name",
            "last_name": "Last Name",
            "preferred_name": "Preferred Name",
            "date_of_birth": "Date of Birth",
            "mit_hire_date": "Original Hire Date at MIT",
            "mit_lib_hire_date": "Original Hire Date at MIT Libraries",
            "appointment_end_date": "End Date",
            "home_addr_street1": "Street 1",
            "home_addr_street2": "Street 2",
            "home_addr_city": "City",
            "home_addr_state": "State/Region",
            "home_addr_zip": "Postal Code",
            "home_addr_country": "Country",
            "mit_email_address": "MIT Email Address",
            "office_address": "Work Address Office",
            "office_phone": "Work Phone",
            "home_phone": "Cell Phone Number",
            "emergency_contact_name": "Emergency Contact Name",
            "emergency_contact_relation": "Emergency Contact Relationship",
            "emergency_contact_email": "Emergency Contact Email",
            "emergency_home_phone": "Emergency Contact Home Phone",
            "emergency_work_phone": "Emergency Contact Work Phone",
            "emergency_cell_phone": "Emergency Contact Cell Phone",
            "highest_degree_type": "Highest Degree Type",
            "highest_degree_year": "Highest Degree Year",
            "residency_status": "Residency Status",
            "yrs_of_mit_serv": "MIT Years of Service",
            "yrs_of_prof_expr": "Years of Professional Experience",
            "i9_form_expiration_date": "I9 Expiration Date",
        }
        return employees_df[fields.keys()].rename(columns=fields)


class LoadEmployees(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Employees")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        return [TransformEmployees(pipeline=self.pipeline)]
