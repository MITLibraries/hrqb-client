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
from hrqb.utils.data_warehouse import DWClient
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

        A double underscore '__' is used to join these components together to allow
        splitting on the filename if needed, where a single component may (though not
        likely) contain a single underscore.
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
    def target(self) -> luigi.Target:
        """Convenience property to provide instantiated Target as output for this Task."""

    def output(self) -> luigi.Target:
        """Satisfy required luigi.Task method by returning prepared Target."""
        return self.target

    @property
    @abstractmethod
    def filename_extension(self) -> str:
        pass  # pragma: nocover

    @property
    def single_input(self) -> PandasPickleTarget | QuickbaseTableTarget:
        """Return single parent Task Target, raise error if multiple parent Tasks.

        When used, this convenience method also helps reason about Tasks.  Quite often, a
        Task is only expecting a single parent Task that will feed it data.  In these
        scenarios, using self.single_input is not only convenient, but also codifies in
        code this assumption.  If this Task were to receive multiple inputs in the future
        this method would then throw an error.
        """
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
    """Base Task class for Tasks that write a pickled pandas DataFrame."""

    @property
    def filename_extension(self) -> str:
        return ".pickle"

    @property
    def target(self) -> PandasPickleTarget:
        return PandasPickleTarget(
            path=self.path,
            table_name=self.table_name,
        )

    @abstractmethod
    def get_dataframe(self) -> pd.DataFrame:
        """Get dataframe that will be the output of this PandasPickleTask.

        This method is required by any Tasks extending this class.
        """

    def run(self) -> None:
        """Write dataframe prepared by self.get_dataframe as Task Target output."""
        self.target.write(self.get_dataframe())


class SQLQueryExtractTask(PandasPickleTask):
    """Base class for Tasks that make SQL queries for data."""

    @property
    def dwclient(self) -> DWClient:
        """Optional property to provide a DWClient instance."""
        return DWClient()  # pragma: nocover

    @property
    def sql_query(self) -> str:
        """SQL query from string to execute.

        Default behavior is to read a SQL file defined by self.sql_file.  Or, this
        property can be overridden to provide a SQL query explicitly.
        """
        if not self.sql_file:
            message = (
                "Property 'sql_file' must be set or property 'sql_query' overridden to "
                "explicitly return a SQL string."
            )
            raise AttributeError(message)
        with open(self.sql_file) as f:
            return f.read()

    @property
    def sql_file(self) -> str | None:
        """SQL query loaded from file to execute."""
        return None

    @property
    def sql_query_parameters(self) -> dict:
        """Optional parameters to include with SQL query."""
        return {}

    def get_dataframe(self) -> pd.DataFrame:
        """Perform SQL query and return DataFrame for required get_dataframe method."""
        return self.dwclient.execute_query(
            self.sql_query,
            params=self.sql_query_parameters,
        )


class QuickbaseUpsertTask(HRQBTask):
    """Base Task class for Tasks that upsert data to Quickbase tables."""

    @property
    def filename_extension(self) -> str:
        return ".json"

    @property
    def target(self) -> QuickbaseTableTarget:
        return QuickbaseTableTarget(
            path=self.path,
            table_name=self.table_name,
        )

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

        self.target.write(results)


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

    @staticmethod
    def init_task_from_class_path(
        task_class_name: str,
        task_class_module: str = "hrqb.tasks.pipelines",
        pipeline_parameters: dict | None = None,
    ) -> "HRQBPipelineTask":
        """Factory method to import and instantiate an HRQBPipelineTask via class path."""
        module = __import__(task_class_module, fromlist=[task_class_name])
        task_class = getattr(module, task_class_name)
        return task_class(**pipeline_parameters or {})

    def pipeline_tasks_iter(
        self, task: luigi.Task | None = None, level: int = 0
    ) -> Iterator[tuple[int, luigi.Task]]:
        """Yield all Tasks that are part of the dependency chain for this Task.

        This method begins with the Pipeline Task itself, then recursively discovers and
        yields parent Tasks as they are required.
        """
        if task is None:
            task = self
        yield level, task
        for parent_task in task.requires():
            yield from self.pipeline_tasks_iter(task=parent_task, level=level + 1)

    def pipeline_targets_iter(self) -> Iterator[tuple[int, luigi.Target]]:
        """Yield all Targets that are part of the dependency chain for this Task.

        Using self.pipeline_Tasks_iter(), this yields the Task's Targets.
        """
        for level, task in self.pipeline_tasks_iter():
            if hasattr(task, "target"):
                yield level, task.target

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
