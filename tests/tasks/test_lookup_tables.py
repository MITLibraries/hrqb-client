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