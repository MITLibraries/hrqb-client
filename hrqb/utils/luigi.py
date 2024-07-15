"""hrqb.utils.luigi"""

import logging

import luigi  # type: ignore[import-untyped]
from luigi.execution_summary import LuigiRunResult  # type: ignore[import-untyped]

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
