# ruff: noqa: PD901, PLR2004

import numpy as np
import pandas as pd


def test_extract_libhr_employee_appointments_read_csv(
    task_extract_libhr_employee_appointments,
):
    df = task_extract_libhr_employee_appointments.get_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert df.iloc[0]["MIT ID"] == 123456789
    assert df.iloc[0]["Supervisor ID"] == 444444444


def test_transform_libhr_employee_appointments_merge_departments(
    mocked_qbclient_departments_df,
    task_transform_libhr_employee_appointments,
    task_extract_dw_employees_target,
):
    new_df = task_transform_libhr_employee_appointments.get_dataframe()
    assert new_df.equals(
        pd.DataFrame(
            [
                {
                    "Related Employee MIT ID": 123456789,
                    "Related Supervisor MIT ID": 444444444,
                    "Cost Object": 555555555,
                    "HC ID": "L-001",
                    "Position ID": 888888888,
                    "Related Department ID": 35.0,
                },
                {
                    "Related Employee MIT ID": 987654321,
                    "Related Supervisor MIT ID": 444444444,
                    "Cost Object": 555555555,
                    "HC ID": "L-100",
                    "Position ID": 999999999,
                    "Related Department ID": 40.0,
                },
                {
                    "Related Employee MIT ID": 987654321,
                    "Related Supervisor MIT ID": 444444444,
                    "Cost Object": 555555555,
                    "HC ID": "L-100",
                    "Position ID": 999999991,
                    "Related Department ID": np.nan,
                },
            ]
        )
    )


def test_load_libhr_employee_appointments_merge_field_set(
    task_load_libhr_employee_appointments,
):
    assert task_load_libhr_employee_appointments.merge_field == "Position ID"
