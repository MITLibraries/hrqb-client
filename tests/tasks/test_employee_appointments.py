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


def test_task_transform_employee_appointments_returns_required_load_fields(
    task_transform_employee_appointments_complete,
):
    df = task_transform_employee_appointments_complete.get_dataframe()
    assert {
        "MIT ID",
        "Related Employee Type",
        "Begin Date",
        "End Date",
        "Related Job Title",
        "Related Position Title",
        "Job Family",
        "Job Subfamily",
        "Job Track",
        "Position ID",
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
        ]
    )
