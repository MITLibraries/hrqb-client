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
