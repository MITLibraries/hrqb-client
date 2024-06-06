# ruff: noqa: PLR2004, PD901


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
    assert (
        task_load_employee_salary_history_complete.merge_field
        == "HR Appointment Transaction Key"
    )
    assert (
        task_load_employee_salary_history_complete.input_task_to_load
        == "TransformEmployeeSalaryHistory"
    )
