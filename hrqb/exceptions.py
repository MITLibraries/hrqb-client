"""hrqb.exceptions"""


class QBFieldNotFoundError(ValueError):
    pass


class ExcludedTaskRequiredError(RuntimeError):
    """Raised when an excluded pipeline task is required but has no target data."""

    def __init__(self, task_name: str) -> None:
        self.message = (
            f"Task '{task_name}' was required by pipeline but is explicitly "
            "excluded and does not have pre-existing target data."
        )
        super().__init__(self.message)


class TaskNotInPipelineScopeError(ValueError):
    """Raised when a task is referenced in pipeline but not part of task dependencies."""

    def __init__(self, task_name: str) -> None:
        self.message = f"Task '{task_name}' not found in pipeline."
        super().__init__(self.message)
