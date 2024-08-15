"""hrqb.utils.luigi"""

import json
import logging

import luigi  # type: ignore[import-untyped]
import sentry_sdk
from luigi.execution_summary import LuigiRunResult  # type: ignore[import-untyped]

from hrqb.base.task import HRQBPipelineTask
from hrqb.config import Config

logger = logging.getLogger(__name__)


def run_task(task: luigi.Task) -> LuigiRunResult:
    """Function to run any luigi Task type via luigi runner."""
    return luigi.build(
        [task],
        local_scheduler=True,
        detailed_summary=True,
        workers=Config().LUIGI_NUM_WORKERS or 1,
    )


def run_pipeline(pipeline_task: HRQBPipelineTask) -> LuigiRunResult:
    """Function to run a HRQBPipelineTask."""
    if not isinstance(pipeline_task, HRQBPipelineTask):
        message = f"{pipeline_task.name} is not a HRQBPipelineTask type task"
        raise TypeError(message)

    results = run_task(pipeline_task)

    if upsert_results := pipeline_task.aggregate_upsert_results():
        message = f"Upsert results: {json.dumps(upsert_results)}"
        logger.info(message)

        if upsert_results.get("qb_upsert_errors"):
            tasks_with_errors = [
                task_name
                for task_name, task_results in upsert_results.get("tasks", {}).items()
                if task_results["errors"] is not None
            ]
            sentry_sdk.capture_message(
                f"Quickbase upsert error(s) detected for tasks: {tasks_with_errors}. "
                "Please see logs for information on specific errors encountered.",
                level="warning",
            )

    return results
