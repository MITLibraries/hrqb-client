"""hrqb.tasks.pipelines"""

# WIP: HRQBPipelineTasks will be defined here
from collections.abc import Iterator

import luigi  # type: ignore[import-untyped]

from hrqb.base.task import HRQBPipelineTask


class FullUpdate(HRQBPipelineTask):
    """Pipeline to perform a full update of all Quickbase tables."""

    def requires(self) -> Iterator[luigi.Task]:  # pragma: no cover
        from hrqb.tasks.employees import LoadEmployees

        yield LoadEmployees(pipeline=self.pipeline_name)
