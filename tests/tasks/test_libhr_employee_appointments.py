# ruff: noqa: PD901, PLR2004
import datetime

import pandas as pd


def test_extract_libhr_employee_appointments_read_csv(
    task_extract_libhr_employee_appointments,
):
    df = task_extract_libhr_employee_appointments.get_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert df.iloc[0]["MIT ID"] == 123456789
    assert df.iloc[0]["Supervisor ID"] == 444444444
    assert df.iloc[0]["Begin Date"] == datetime.datetime(2022, 2, 2, tzinfo=datetime.UTC)
    assert df.iloc[0]["End Date"] == datetime.datetime(2999, 12, 31, tzinfo=datetime.UTC)
    assert df.iloc[0]["Active"]


def test_extract_libhr_employee_appointments_strips_headcount_id_suffix(
    task_extract_libhr_employee_appointments,
):
    df = task_extract_libhr_employee_appointments.get_dataframe()
    row = df.iloc[2]
    assert row["HC ID"] == "L-101"
    assert row["HC ID (Original)"] == "L-101x"


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
        "bf150338afc4af388b4be1ec33e8b7ec",
        "f71875f7a6ceb7fd41d8ac2dbb4b6aa7",
        "e4dd968ab59c898e8318c57ad322518e",
        "8833ed8dbb41f850377dd6cdf896c922",
        "47652ff11feb5f688adc48fb38839d0a",
    ]
