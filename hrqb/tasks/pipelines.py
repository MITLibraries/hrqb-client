"""hrqb.tasks.pipelines"""

import luigi  # type: ignore[import-untyped]
from luigi.execution_summary import LuigiRunResult  # type: ignore[import-untyped]

from hrqb.base.task import HRQBPipelineTask
from hrqb.config import Config


def run_task(task: luigi.Task) -> LuigiRunResult:
    """Function to run any luigi Task type via luigi runner."""
    return luigi.build(
        [task],
        local_scheduler=True,
        detailed_summary=True,
        workers=Config().LUIGI_NUM_WORKERS or 1,
    )


def run_pipeline(pipeline_task: luigi.WrapperTask) -> LuigiRunResult:
    """Function to run a HRQBPipelineTask."""
    if not isinstance(pipeline_task, HRQBPipelineTask):
        message = (
            f"{pipeline_task.__class__.__name__} is not a HRQBPipelineTask type task"
        )
        raise TypeError(message)
    return run_task(pipeline_task)
