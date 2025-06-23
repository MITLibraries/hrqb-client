# ruff: noqa: E501, PLR2004, SLF001

import numpy as np
import pandas as pd

from hrqb.utils import md5_hash_from_values


def test_extract_dw_employee_salary_history_load_sql_query(
    task_extract_dw_employee_salary_history_complete,
):
    assert (
        task_extract_dw_employee_salary_history_complete.sql_file
        == "hrqb/tasks/sql/employee_salary_history.sql"
    )
    assert task_extract_dw_employee_salary_history_complete.sql_query is not None


def test_task_transform_employee_salary_history_employee_appointments_merge_success(
    task_transform_employee_salary_history_complete,
):
    row = task_transform_employee_salary_history_complete.get_dataframe().iloc[0]
    assert row["Related Employee Appointment"] == 12000


def test_task_transform_employee_salary_history_efforts_are_percents(
    task_transform_employee_salary_history_complete,
):
    row = task_transform_employee_salary_history_complete.get_dataframe().iloc[0]
    assert 0 <= row["Effort %"] <= 1.0
    assert 0 <= row["Temp Effort %"] <= 1.0


def test_task_load_employee_salary_history_explicit_properties(
    task_load_employee_salary_history_complete,
):
    assert task_load_employee_salary_history_complete.merge_field == "Key"
    assert (
        task_load_employee_salary_history_complete.input_task_to_load
        == "TransformEmployeeSalaryHistory"
    )


def test_task_transform_employee_salary_history_key_expected_from_input_data(
    task_transform_employee_salary_history_complete,
    task_shared_extract_qb_employee_appointments_complete,
):
    # NOTE: for this test, need to manually get part of the employee appointment data
    #   that this transform is using when generating the unique MD5 Key field value
    qb_emp_appt_row = (
        task_shared_extract_qb_employee_appointments_complete.target.read().iloc[0]
    )
    emp_salary_row = task_transform_employee_salary_history_complete.get_dataframe().iloc[
        0
    ]
    assert emp_salary_row["Key"] == md5_hash_from_values(
        [
            emp_salary_row["MIT ID"],
            qb_emp_appt_row["Position ID"],
            qb_emp_appt_row["Begin Date"],
            qb_emp_appt_row["End Date"],
            emp_salary_row["Related Salary Change Type"],
            emp_salary_row["Salary Change Reason"],
            emp_salary_row["Start Date"],
            emp_salary_row["End Date"],
        ]
    )


def test_task_transform_employee_salary_history_set_base_change_percent(
    task_transform_employee_salary_history_complete,
):
    df = pd.DataFrame(
        [
            ("123456789", "123", "2020-01-01", "2021-06-30", 10_000),
            ("123456789", "123", "2020-07-01", "2021-12-31", 10_300),
            ("123456789", "123", "2021-01-01", "2021-06-30", 15_000),
            ("123456789", "456", "2021-07-01", "2022-06-30", 20_000),
            ("123456789", "456", "2022-07-01", "2022-12-31", 22_500),
            ("123456789", "456", "2023-01-01", "2999-12-31", 24_750),
        ],
        columns=[
            "mit_id",
            "hr_appt_key",
            "appointment_begin_date",
            "appointment_end_date",
            "original_base_amount",
        ],
    )
    new_df = (
        task_transform_employee_salary_history_complete._set_base_salary_change_percent(
            df
        )
    )
    np.testing.assert_array_equal(
        new_df.previous_base_amount.values,
        [
            np.nan,  # first position, so no previous salary
            10_000.0,
            10_300.0,
            np.nan,  # new position, so previous salary None
            20_000.0,
            22_500.0,
        ],
    )
    np.testing.assert_array_equal(
        new_df.base_change_percent.values,
        [
            0.0,  # first position, so no change
            0.03,
            0.45631,  # 5 digits of accuracy, but rounded to 2 digits in Quickbase
            0.0,  # new position, so no change
            0.125,
            0.1,
        ],
    )


def test_task_transform_employee_salary_history_set_effective_change_percent_no_temps(
    task_transform_employee_salary_history_complete,
):
    df = pd.DataFrame(
        [
            ("123456789", "123", "2020-01-01", "2021-06-30", 10_000, 0),
            ("123456789", "123", "2020-07-01", "2021-12-31", 10_300, 0),
            ("123456789", "123", "2021-01-01", "2021-06-30", 15_000, 0),
            ("123456789", "456", "2021-07-01", "2022-06-30", 20_000, 0),
            ("123456789", "456", "2022-07-01", "2022-12-31", 22_500, 0),
            ("123456789", "456", "2023-01-01", "2999-12-31", 24_750, 0),
        ],
        columns=[
            "mit_id",
            "hr_appt_key",
            "appointment_begin_date",
            "appointment_end_date",
            "original_base_amount",
            "temp_change_base_amount",
        ],
    )
    new_df = task_transform_employee_salary_history_complete._set_effective_salary_and_change_percent(
        df
    )
    np.testing.assert_array_equal(
        new_df.previous_effective_salary.values,
        [
            np.nan,  # first position, so no previous
            10_000.0,
            10_300.0,
            np.nan,  # new position, so reset
            20_000.0,
            22_500.0,
        ],
    )
    np.testing.assert_array_equal(
        new_df.effective_change_percent.values,
        [
            0.0,  # first position, so no change
            0.03,
            0.45631,
            0.0,
            0.125,
            0.1,
        ],
    )


def test_task_transform_employee_salary_history_set_effective_change_percent_with_temps(
    task_transform_employee_salary_history_complete,
):
    df = pd.DataFrame(
        [
            ("123456789", "123", "2020-01-01", "2021-06-30", 10_000, 0),
            ("123456789", "123", "2020-07-01", "2021-12-31", 10_300, 11_300),
            ("123456789", "123", "2021-01-01", "2021-06-30", 15_000, 0),
            ("123456789", "456", "2021-07-01", "2022-06-30", 20_000, 21_000),
            ("123456789", "456", "2022-07-01", "2022-12-31", 22_500, 23_500),
            ("123456789", "456", "2023-01-01", "2999-12-31", 24_750, 0),
        ],
        columns=[
            "mit_id",
            "hr_appt_key",
            "appointment_begin_date",
            "appointment_end_date",
            "original_base_amount",
            "temp_change_base_amount",
        ],
    )
    new_df = task_transform_employee_salary_history_complete._set_effective_salary_and_change_percent(
        df
    )
    np.testing.assert_array_equal(
        new_df.previous_effective_salary.values,
        [
            np.nan,  # first position, so no previous
            10_000.0,
            11_300.0,
            np.nan,  # new position, so reset
            21_000.0,
            23_500.0,
        ],
    )
    np.testing.assert_array_equal(
        new_df.effective_change_percent.values,
        [
            0.0,  # first position, so no change
            0.13,  # 13% increase for CURRENT temp $11.3k over PREVIOUS $10k
            0.32743,  # 32% increase for CURRENT base $15k over PREVIOUS temp $11.3k
            0.0,  # new position, no change
            0.11905,  # 11% increase from CURRENT temp $23.5 over PREVIOUS temp $21k
            0.05319,  # 5% increase of CURRENT base $24.7k over PREVIOUS temp $23.5
        ],
    )
