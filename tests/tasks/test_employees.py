import pandas as pd
import pytest


def test_extract_dw_employees_load_sql_query(task_extract_dw_employees):
    assert task_extract_dw_employees.sql_file == "hrqb/tasks/sql/employees.sql"
    assert task_extract_dw_employees.sql_query is not None


def test_transform_employees_normalize_dates(
    task_transform_employees, task_extract_dw_employees_target
):
    new_df = task_transform_employees.get_dataframe()
    row = new_df.iloc[0]
    assert row["Date of Birth"] == "1985-04-12"
    assert row["I9 Expiration Date"] is None


def test_transform_employees_normalize_state_names(
    task_transform_employees, task_extract_dw_employees_target
):
    new_df = task_transform_employees.get_dataframe()
    row = new_df.iloc[0]
    assert row["State/Region"] == "Massachusetts"


def test_task_load_employee_appointments_explicit_properties(task_load_employees):
    assert task_load_employees.merge_field == "MIT ID"


def test_transform_employees_integrity_check_duplicate_mit_ids(task_transform_employees):
    test_df = pd.DataFrame(
        ["55555", "55555", "12345", "67890"],
        columns=["MIT ID"],
    )
    with pytest.raises(ValueError, match="Found 2 duplicate 'MIT ID' column values"):
        task_transform_employees.check_unique_mit_ids(test_df)
