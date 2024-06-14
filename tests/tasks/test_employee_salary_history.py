# ruff: noqa: PLR2004, PD901

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
            emp_salary_row["Start Date"],
            emp_salary_row["End Date"],
        ]
    )
