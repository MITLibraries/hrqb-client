def test_task_extract_qb_employee_appointments_complete_has_required_fields(
    task_shared_extract_qb_employee_appointments_complete,
):
    required_columns = [
        "Record ID#",
        "Key",
    ]
    assert set(required_columns).issubset(
        set(task_shared_extract_qb_employee_appointments_complete.target.read().columns)
    )
