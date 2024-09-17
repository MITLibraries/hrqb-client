# ruff: noqa: PD901, PLR2004

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
    task_transform_libhr_employee_appointments,
):
    new_df = task_transform_libhr_employee_appointments.get_dataframe()
    assert new_df.iloc[0]["Related Department ID"] == 35.0
    assert new_df.iloc[1]["Related Department ID"] == 40.0
    assert pd.isna(new_df.iloc[2]["Related Department ID"])  # no match in merge, so NULL


def test_load_libhr_employee_appointments_merge_field_set(
    task_load_libhr_employee_appointments,
):
    assert task_load_libhr_employee_appointments.merge_field == "Key"


def test_transform_libhr_employee_appointments_merge_field_key_values(
    task_transform_libhr_employee_appointments,
):
    new_df = task_transform_libhr_employee_appointments.get_dataframe()
    assert list(new_df["Key"]) == [
        "81cf06bfd65aa1f7019750c57a79be99",
        "6e07102ee39ec1f22c63231d090bd4dd",
        "744aefdd46c40523d60cf69490d81655",
    ]
