from hrqb.exceptions import ExcludedTaskRequiredError, TaskNotInPipelineScopeError

TASK_NAME = "TaskFoo"


def test_excluded_task_required_exception_message():
    exc = ExcludedTaskRequiredError("TaskFoo")
    assert str(exc) == (
        f"Task '{TASK_NAME}' was required by pipeline but is explicitly "
        "excluded and does not have pre-existing target data."
    )


def test_task_not_in_pipeline_scope_exception_message():
    exc = TaskNotInPipelineScopeError("TaskFoo")
    assert str(exc) == f"Task '{TASK_NAME}' not found in pipeline."
