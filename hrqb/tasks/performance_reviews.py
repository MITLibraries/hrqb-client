"""hrqb.tasks.performance_reviews"""

import datetime

import luigi  # type: ignore[import-untyped]
import pandas as pd
from dateutil.relativedelta import relativedelta  # type: ignore[import-untyped]

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask
from hrqb.utils import (
    convert_dataframe_columns_to_dates,
    md5_hash_from_values,
    normalize_dataframe_dates,
    today_date,
)


class TransformPerformanceReviews(PandasPickleTask):
    stage = luigi.Parameter("Transform")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        from hrqb.tasks.shared import ExtractQBEmployeeAppointments

        return [ExtractQBEmployeeAppointments(pipeline=self.pipeline)]

    def get_dataframe(self) -> pd.DataFrame:
        """Build dataframe of performance reviews from employee appointments."""
        emp_appts_df = self._get_employee_appointments()

        # loop through all appointments and create dataframe of performance reviews
        reviews: list[dict] = []
        for _, emp_appt_row in emp_appts_df.iterrows():
            reviews.append(self._get_three_month_review(emp_appt_row))
            reviews.append(self._get_six_month_review(emp_appt_row))
            reviews.extend(self._get_annual_reviews(emp_appt_row))
        perf_revs_df = pd.DataFrame(reviews)

        perf_revs_df = normalize_dataframe_dates(
            perf_revs_df,
            [
                "review_date",
                "period_start_date",
                "period_end_date",
            ],
        )

        # mint a unique, deterministic value for the merge "Key" field
        perf_revs_df["key"] = perf_revs_df.apply(
            lambda row: md5_hash_from_values(
                [
                    row.mit_id,
                    row.review_type,
                    row.review_year,
                ]
            ),
            axis=1,
        )

        fields = {
            "mit_id": "MIT ID",
            "employee_appointment_id": "Related Employee Appointment",
            "review_type": "Review Type",
            "period_start_date": "Period Covered Start Date",
            "period_end_date": "Period Covered End Date",
            "review_date": "Date of Review",
            "review_year": "Related Year",
            "key": "Key",
        }
        return perf_revs_df[fields.keys()].rename(columns=fields)

    def _get_employee_appointments(self) -> pd.DataFrame:
        """Get employee appointments from Quickbase."""
        emp_appts_df = self.named_inputs["ExtractQBEmployeeAppointments"].read()
        emp_appt_fields = {
            "MIT ID": "mit_id",
            "Record ID#": "employee_appointment_id",
            "Begin Date": "appointment_begin_date",
            "End Date": "appointment_end_date",
            "Related Employee Type": "employee_type",
            "Union Name": "union_name",
            "Exempt / NE": "exempt",
        }
        emp_appts_df = emp_appts_df.rename(columns=emp_appt_fields)[
            emp_appt_fields.values()
        ]
        return convert_dataframe_columns_to_dates(
            emp_appts_df, ["appointment_begin_date", "appointment_end_date"]
        )

    def _get_three_month_review(self, emp_appt_row: pd.Series) -> dict:
        review_date = emp_appt_row.appointment_begin_date + relativedelta(months=+3)
        return {
            "mit_id": emp_appt_row.mit_id,
            "employee_appointment_id": emp_appt_row.employee_appointment_id,
            "review_type": "3 Month Review",
            "review_date": review_date,
            "period_start_date": emp_appt_row.appointment_begin_date,
            "period_end_date": review_date,
            "review_year": str(review_date.year),
        }

    def _get_six_month_review(self, emp_appt_row: pd.Series) -> dict:
        review_date = emp_appt_row.appointment_begin_date + relativedelta(months=+6)
        return {
            "mit_id": emp_appt_row.mit_id,
            "employee_appointment_id": emp_appt_row.employee_appointment_id,
            "review_type": "6 Month Review",
            "review_date": review_date,
            "period_start_date": emp_appt_row.appointment_begin_date,
            "period_end_date": review_date,
            "review_year": str(review_date.year),
        }

    def _get_annual_reviews(self, emp_appt_row: pd.Series) -> list[dict]:
        """Get annual performance reviews for an appointment.

        This method begins with the appointment start year, with a minimum of 2019, then
        adds performance reviews through current year + 1.

        If an annual performance review would fall inside of a 3 or 6 month review, it is
        not included.

        NOTE: as of 6/17/2024, HR is in the process of re-evaluating annual review
            timeframes.  The cadence and review dates set below are placeholders until
            that is finalized.
        """
        start_year = max([emp_appt_row.appointment_begin_date.year, 2019])
        end_year = today_date().year + 2

        review_month = 7 if emp_appt_row.exempt else 8

        reviews = []
        for year in range(start_year, end_year):
            review_end_date = datetime.datetime(
                year, review_month, 1, tzinfo=datetime.UTC
            )
            review_start_date = review_end_date - relativedelta(years=1)

            # if annual review is less than 6 month review, skip
            six_month_review_date = self._get_six_month_review(emp_appt_row)[
                "review_date"
            ]
            if review_end_date <= six_month_review_date:
                continue

            reviews.append(
                {
                    "mit_id": emp_appt_row.mit_id,
                    "employee_appointment_id": emp_appt_row.employee_appointment_id,
                    "review_type": "Annual",
                    "period_start_date": review_start_date,
                    "period_end_date": review_end_date,
                    "review_date": review_end_date,
                    "review_year": str(year),
                }
            )
        return reviews


class LoadPerformanceReviews(QuickbaseUpsertTask):
    table_name = luigi.Parameter("Performance Reviews")
    stage = luigi.Parameter("Load")

    def requires(self) -> list[luigi.Task]:  # pragma: nocover
        from hrqb.tasks.years import LoadYears

        return [
            LoadYears(pipeline=self.pipeline),
            TransformPerformanceReviews(pipeline=self.pipeline),
        ]

    @property
    def merge_field(self) -> str | None:
        return "Key"

    @property
    def input_task_to_load(self) -> str | None:
        return "TransformPerformanceReviews"
