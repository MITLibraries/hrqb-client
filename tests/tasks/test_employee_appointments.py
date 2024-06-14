# ruff: noqa: PLR2004, PD901

from hrqb.utils import md5_hash_from_values


def test_extract_dw_employees_load_sql_query(
    task_extract_dw_employee_appointment_complete,
):
    assert (
        task_extract_dw_employee_appointment_complete.sql_file
        == "hrqb/tasks/sql/employee_appointments.sql"
    )
    assert task_extract_dw_employee_appointment_complete.sql_query is not None


def test_task_extract_qb_libhr_complete_has_required_fields(
    task_extract_qb_libhr_complete,
):
    required_columns = [
        "Related Employee MIT ID",
        "Related Supervisor MIT ID",
        "HC ID",
        "Position ID",
        "Cost Object",
        "Related Department ID",
    ]
    assert set(required_columns).issubset(
        set(task_extract_qb_libhr_complete.target.read().columns)
    )


def test_task_extract_qb_departments_complete_has_required_fields(
    task_extract_qb_departments_complete,
):
    required_columns = ["Directorate", "Record ID#"]
    assert set(required_columns).issubset(
        set(task_extract_qb_departments_complete.target.read().columns)
    )


def test_task_transform_employee_appointments_libhr_data_merge_success(
    task_transform_employee_appointments_complete,
):
    row = task_transform_employee_appointments_complete.get_dataframe().iloc[0]
    assert row["HC ID"] == "L-001"
    assert row["Cost Object"] == 7777777
    assert row["Supervisor"] == "111111111"
    assert row["Related Department ID"] == 42.0


def test_task_transform_employee_appointments_departments_data_merge_success(
    task_transform_employee_appointments_complete,
):
    row = task_transform_employee_appointments_complete.get_dataframe().iloc[0]
    assert row["Related Directorate"] == "Information Technology Services"


def test_task_transform_employee_appointments_returns_required_load_fields(
    task_transform_employee_appointments_complete,
):
    df = task_transform_employee_appointments_complete.get_dataframe()
    assert {
        "HR Appointment Key",
        "MIT ID",
        "HC ID",
        "Related Employee Type",
        "Begin Date",
        "End Date",
        "Related Directorate",
        "Related Department ID",
        "Supervisor",
        "Related Job Title",
        "Related Position Title",
        "Job Family",
        "Job Subfamily",
        "Job Track",
        "Position ID",
        "Cost Object",
        "Exempt / NE",
        "Union Name",
        "Term or Permanent",
        "Benefits Group Type",
        "Key",
    } == set(df.columns)


def test_task_load_employee_appointments_explicit_properties(
    task_load_employee_appointments,
):
    assert task_load_employee_appointments.merge_field == "Key"
    assert (
        task_load_employee_appointments.input_task_to_load
        == "TransformEmployeeAppointments"
    )


def test_task_transform_employee_appointments_key_expected_from_row_data(
    task_transform_employee_appointments_complete,
):
    row = task_transform_employee_appointments_complete.get_dataframe().iloc[0]
    assert row["Key"] == md5_hash_from_values(
        [
            row["MIT ID"],
            row["Position ID"],
            row["Begin Date"],
            row["End Date"],
        ]
    )
