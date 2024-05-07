"""hrqb.base.task"""

import logging
import os
from abc import abstractmethod
from collections.abc import Iterator
from typing import Literal

import luigi  # type: ignore[import-untyped]
import pandas as pd

from hrqb.base import PandasPickleTarget, QuickbaseTableTarget
from hrqb.config import Config
from hrqb.utils.quickbase import QBClient

logger = logging.getLogger(__name__)


class HRQBTask(luigi.Task):
    """Base Task class for all HRQB Tasks."""

    pipeline = luigi.Parameter()
    stage: Literal["Extract", "Transform", "Load"] = luigi.Parameter()
    table_name = luigi.OptionalStrParameter(default=None)

    @property
    def path(self) -> str:
        """Dynamically generate path for HRQBTask Targets.

        The pattern is:
            Targets Dir + Pipeline Name + Pipeline Stage + Task Name + File Extension

        Examples:
            output/LookupTablesPipeline__extract__JobData.pickle
            output/LookupTablesPipeline__transform__UniqueJobTitles.pickle
            output/LookupTablesPipeline__load__UpsertJobTitles.pickle
        """
        filename = (
            "__".join(  # noqa: FLY002
                [self.pipeline, self.stage, self.__class__.__name__]
            )
            + self.filename_extension
        )
        return os.path.join(Config().targets_directory(), filename)

    @property
    @abstractmethod
    def filename_extension(self) -> str:
        pass  # pragma: nocover

    @property
    def single_input(self) -> PandasPickleTarget | QuickbaseTableTarget:
        """Return single parent Task Target, raise error if multiple parent Tasks."""
        input_count = len(self.input())
        if input_count != 1:
            message = f"Expected a single input to this Task but found: {input_count}"
            raise ValueError(message)
        return self.input()[0]

    @property
    def single_input_dataframe(self) -> pd.DataFrame:
        """Convenience method to read Dataframe from single PandasPickleTarget input."""
        target = self.single_input
        if not isinstance(target, PandasPickleTarget):  # pragma: nocover
            message = f"Expected PandasPickleTarget input but got {type(target)}."
            raise TypeError(message)
        return target.read()  # type: ignore[return-value]

    @property
    def named_inputs(self) -> dict[str, PandasPickleTarget | QuickbaseTableTarget]:
        """Dictionary of parent Tasks and their Targets.

        This is useful when a Task has multiple parent Tasks, to easily and precisely
        access a specific parent Task's output.
        """
        return {
            task.__class__.__name__: target
            for task, target in list(zip(self.deps(), self.input(), strict=True))
        }


class PandasPickleTask(HRQBTask):
    """Base Task class for Tasks that write pickled pandas objects."""

    @property
    def filename_extension(self) -> str:
        return ".pickle"

    def target(self) -> PandasPickleTarget:
        return PandasPickleTarget(
            path=self.path,
            table_name=self.table_name,
        )

    def output(self) -> PandasPickleTarget:  # pragma: no cover
        return self.target()

    @abstractmethod
    def get_dataframe(self) -> pd.DataFrame:
        """Get dataframe that will be the output of this PandasPickleTask.

        This method is required by any Tasks extending this class.
        """

    def run(self) -> None:
        """Write dataframe prepared by self.get_dataframe as Task Target output."""
        self.target().write(self.get_dataframe())


class QuickbaseUpsertTask(HRQBTask):
    """Base Task class for Tasks that upsert data to Quickbase tables."""

    @property
    def filename_extension(self) -> str:
        return ".json"

    def target(self) -> QuickbaseTableTarget:
        return QuickbaseTableTarget(
            path=self.path,
            table_name=self.table_name,
        )

    def output(self) -> QuickbaseTableTarget:  # pragma: no cover
        return self.target()

    def get_records(self) -> list[dict]:
        """Get Records data that will be upserted to Quickbase.

        This method may be overridden if necessary if a load Task requires more complex
        behavior than a straight conversion of the parent's DataFrame to a dictionary.
        """
        return self.single_input_dataframe.to_dict(orient="records")

    def run(self) -> None:
        """Retrieve data from parent Task and upsert to Quickbase table.

        Because Load Tasks (upserting data to Quickbase) are so uniform, this run method
        can be defined on this base class.  All data required for this operation exists
        on the Task: data from parent Transform class and QB table name.
        """
        records = self.get_records()

        qbclient = QBClient()
        table_id = qbclient.get_table_id(self.table_name)
        upsert_payload = qbclient.prepare_upsert_payload(
            table_id,
            records,
            merge_field=None,
        )
        results = qbclient.upsert_records(upsert_payload)

        self.target().write(results)


class HRQBPipelineTask(luigi.WrapperTask):
    """Base class for Pipeline Tasks.

    This extends the special luigi.WrapperTask which is not designed to perform work by
    itself, but instead yield other Tasks from its requires() method.

    Example:
        def requires(self):
            yield TaskA(...)
            yield TaskB(...)

    This effectively creates a "Pipeline" of these Tasks defined here as the root Tasks,
    where all required parent Tasks are also invoked.

    It is these HRQBPipelineTasks that are designed to be invoked by the application's
    CLI.
    """

    @property
    def pipeline_name(self) -> str:
        return self.__class__.__name__

    def pipeline_tasks_iter(
        self, task: luigi.Task | None = None, level: int = 0
    ) -> Iterator[tuple[int, luigi.Task]]:
        """Yield Tasks, for all Tasks, in this Pipeline Task."""
        if task is None:
            task = self
        yield level, task
        for parent_task in task.requires():
            yield from self.pipeline_tasks_iter(task=parent_task, level=level + 1)

    def pipeline_targets_iter(self) -> Iterator[tuple[int, luigi.Target]]:
        """Yield Task Targets, for all Tasks, in this Pipeline Task."""
        for level, task in self.pipeline_tasks_iter():
            if hasattr(task, "target"):
                yield level, task.target()

    def pipeline_as_ascii(self) -> str:
        """Return an ASCII representation of this Pipeline Task."""
        output = ""
        for level, task in self.pipeline_tasks_iter():
            green_complete = "\033[92mCOMPLETE\033[0m"
            red_incomplete = "\033[91mINCOMPLETE\033[0m"
            status = green_complete if task.complete() else red_incomplete
            indent = "   " * level + "├──"
            output += f"\n{indent} {status}: {task}"
        return output

    def remove_pipeline_targets(self) -> None:
        """Remove Targets for all Tasks in this Pipeline Task."""
        for _, target in self.pipeline_targets_iter():
            if target.exists() and hasattr(target, "remove"):
                target.remove()
                message = f"Target {target} successfully removed"
                logger.debug(message)
