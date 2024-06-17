# ruff: noqa: PLR2004, PD901, SLF001, D205, D212
import datetime

import pandas as pd

from hrqb.utils import md5_hash_from_values


def test_task_transform_performance_reviews_get_employee_appointments_required_fields(
    task_transform_performance_reviews_complete,
):
    emp_appts_df = (
        task_transform_performance_reviews_complete._get_employee_appointments()
    )
    assert {
        "mit_id",
        "employee_appointment_id",
        "appointment_begin_date",
        "appointment_end_date",
        "employee_type",
        "union_name",
        "exempt",
    } == set(emp_appts_df.columns)


def test_task_transform_performance_reviews_get_employee_appointments_datetime_converts(
    task_transform_performance_reviews_complete,
):
    row = task_transform_performance_reviews_complete._get_employee_appointments().iloc[0]
    for column in ["appointment_begin_date", "appointment_end_date"]:
        assert isinstance(
            row[column],
            datetime.datetime,
        )


def test_task_transform_performance_reviews_get_three_month_review(
    task_transform_performance_reviews_complete,
):
    row = task_transform_performance_reviews_complete._get_employee_appointments().iloc[0]
    review_date = datetime.datetime(2010, 4, 1, tzinfo=datetime.UTC)
    assert task_transform_performance_reviews_complete._get_three_month_review(row) == {
        "mit_id": "123456789",
        "employee_appointment_id": 12000,
        "review_type": "3 Month Review",
        "review_date": pd.Timestamp(review_date),
        "period_start_date": row.appointment_begin_date,
        "period_end_date": pd.Timestamp(review_date),
        "review_year": str(review_date.year),
    }


def test_task_transform_performance_reviews_get_six_month_review(
    task_transform_performance_reviews_complete,
):
    row = task_transform_performance_reviews_complete._get_employee_appointments().iloc[0]
    review_date = datetime.datetime(2010, 7, 1, tzinfo=datetime.UTC)
    assert task_transform_performance_reviews_complete._get_six_month_review(row) == {
        "mit_id": "123456789",
        "employee_appointment_id": 12000,
        "review_type": "6 Month Review",
        "review_date": pd.Timestamp(review_date),
        "period_start_date": row.appointment_begin_date,
        "period_end_date": pd.Timestamp(review_date),
        "review_year": str(review_date.year),
    }


def test_task_transform_performance_reviews_get_annual_reviews(
    task_transform_performance_reviews_complete,
):
    row = task_transform_performance_reviews_complete._get_employee_appointments().iloc[0]
    ann_revs_df = pd.DataFrame(
        task_transform_performance_reviews_complete._get_annual_reviews(row)
    )
    assert len(ann_revs_df) == 7
    assert list(ann_revs_df.review_year) == [
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
    ]
    assert list(ann_revs_df.period_start_date) == [
        pd.Timestamp("2018-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2019-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2020-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2021-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2022-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2023-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2024-07-01 00:00:00+0000", tz="UTC"),
    ]
    assert list(ann_revs_df.period_end_date) == [
        pd.Timestamp("2019-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2020-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2021-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2022-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2023-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2024-07-01 00:00:00+0000", tz="UTC"),
        pd.Timestamp("2025-07-01 00:00:00+0000", tz="UTC"),
    ]


def test_task_transform_performance_reviews_get_annual_reviews_skip_six_month_overlap(
    task_transform_performance_reviews_complete,
):
    """
    Note in the test above that the number of annual reviews is 7, and that there is a
    review from 2018-2019.  By setting this appointment begin date as 2019-03-01, this
    means there should not be a 2018-2019 review, as the 6-month review will be after the
    beginning of the 2019-2020 review cycle.
    """
    row = task_transform_performance_reviews_complete._get_employee_appointments().iloc[0]
    row.appointment_begin_date = datetime.datetime(2019, 3, 1, tzinfo=datetime.UTC)
    ann_revs_df = pd.DataFrame(
        task_transform_performance_reviews_complete._get_annual_reviews(row)
    )

    assert len(ann_revs_df) == 6
    assert (
        pd.Timestamp("2018-07-01 00:00:00+0000", tz="UTC")
        not in ann_revs_df.period_start_date
    )


def test_task_transform_performance_reviews_key_expected_from_input_data(
    task_transform_performance_reviews_complete,
):
    row = task_transform_performance_reviews_complete.get_dataframe().iloc[0]
    assert row["Key"] == md5_hash_from_values(
        [
            row["MIT ID"],
            row["Review Type"],
            row["Related Year"],
        ]
    )


def test_task_load_employee_salary_history_explicit_properties(
    task_load_performance_reviews_complete,
):
    assert task_load_performance_reviews_complete.merge_field == "Key"
    assert (
        task_load_performance_reviews_complete.input_task_to_load
        == "TransformPerformanceReviews"
    )
