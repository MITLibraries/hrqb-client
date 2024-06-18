def test_task_transform_employee_types_required_fields(
    task_transform_employee_types_complete,
):
    assert {"Employee Type"} == set(
        task_transform_employee_types_complete.get_dataframe().columns
    )


def test_task_transform_job_titles_required_fields(task_transform_job_titles_complete):
    assert {"Job Title", "Pay Grade"} == set(
        task_transform_job_titles_complete.get_dataframe().columns
    )


def test_task_transform_position_titles_required_fields(
    task_transform_position_titles_complete,
):
    assert {"Position Title"} == set(
        task_transform_position_titles_complete.get_dataframe().columns
    )


def test_task_transform_employee_salary_change_types_required_fields(
    task_transform_employee_salary_change_types_complete,
):
    assert {"Salary Change Type"} == set(
        task_transform_employee_salary_change_types_complete.get_dataframe().columns
    )


def test_task_transform_employee_leave_types_required_fields(
    task_transform_employee_leave_types_complete,
):
    assert {"Leave Type", "Paid Leave", "Accrue Seniority"} == set(
        task_transform_employee_leave_types_complete.get_dataframe().columns
    )


def test_task_transform_years_required_fields(
    task_transform_years_complete,
):
    assert {"Year", "Active Year"} == set(
        task_transform_years_complete.get_dataframe().columns
    )
