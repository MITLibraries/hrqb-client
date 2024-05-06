"""hrqb.tasks.pipelines"""

import luigi  # type: ignore[import-untyped]
from luigi.execution_summary import LuigiRunResult  # type: ignore[import-untyped]


def run_pipeline(pipeline_task: luigi.WrapperTask) -> LuigiRunResult:
    return luigi.build(
        [pipeline_task],
        local_scheduler=True,
        detailed_summary=True,
    )
