"""hrqb.tasks.pipelines"""

import luigi  # type: ignore[import-untyped]
from luigi.execution_summary import LuigiRunResult  # type: ignore[import-untyped]

from hrqb.config import Config


def run_pipeline(pipeline_task: luigi.WrapperTask) -> LuigiRunResult:
    """Function to run a HRQBPipelineTask via luigi runner."""
    return luigi.build(
        [pipeline_task],
        local_scheduler=True,
        detailed_summary=True,
        workers=Config().LUIGI_NUM_WORKERS or 1,
    )
