# ruff: noqa: PLR2004, PD901, SLF001


def test_extract_dw_employee_leave_load_sql_query(
    task_extract_dw_employee_leave_complete,
):
    assert (
        task_extract_dw_employee_leave_complete.sql_file
        == "hrqb/tasks/sql/employee_leave.sql"
    )
    assert task_extract_dw_employee_leave_complete.sql_query is not None


def test_task_transform_employee_leave_employee_appointments_merge_success(
    task_transform_employee_leave_complete,
):
    row = task_transform_employee_leave_complete.get_dataframe().iloc[0]
    assert row["Related Employee Appointment"] == 12000


def test_task_transform_employee_leave_oracle_bools_converted(
    task_transform_employee_leave_complete,
):
    row = task_transform_employee_leave_complete.get_dataframe().iloc[0]
    assert row["Paid Leave"] == "Yes"
    assert row["Accrue Seniority"] == "Yes"


def test_task_transform_employee_leave_create_key_from_md5_of_leave_data(
    task_transform_employee_leave_complete,
):
    assert (
        task_transform_employee_leave_complete._create_unique_key_from_leave_data(
            "123456789", "2010-07-01", "Vacation", "8.0"
        )
        == "ee9af1a7908735241aeb5c228e2e00fa"
    )
    assert (
        task_transform_employee_leave_complete._create_unique_key_from_leave_data(
            "123456789", "2010-07-01", "Vacation", "None"
        )
        == "767d2672e2e6b38a7c65f0ea2f799067"
    )


def test_task_transform_employee_leave_key_expected_from_row_data(
    task_transform_employee_leave_complete,
):
    row = task_transform_employee_leave_complete.get_dataframe().iloc[0]
    assert row[
        "Key"
    ] == task_transform_employee_leave_complete._create_unique_key_from_leave_data(
        row["MIT ID"],
        row["Leave Date"],
        row["Related Leave Type"],
        str(row["Duration Hours"]),
    )


def test_task_load_employee_salary_history_explicit_properties(
    task_load_employee_leave,
):
    assert task_load_employee_leave.merge_field == "Key"
    assert task_load_employee_leave.input_task_to_load == "TransformEmployeeLeave"
