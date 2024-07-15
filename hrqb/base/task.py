"""hrqb.base.task"""

import json
import logging
import os
from abc import abstractmethod
from collections.abc import Iterator
from typing import Any, Literal

import luigi  # type: ignore[import-untyped]
import numpy as np
import pandas as pd
from luigi.execution_summary import LuigiRunResult  # type: ignore[import-untyped]

from hrqb.base import PandasPickleTarget, QuickbaseTableTarget
from hrqb.config import Config
from hrqb.exceptions import ExcludedTaskRequiredError, TaskNotInPipelineScopeError
from hrqb.utils.data_warehouse import DWClient
from hrqb.utils.luigi import run_task
from hrqb.utils.quickbase import QBClient

logger = logging.getLogger(__name__)

successful_tasks = []
successful_upsert_tasks = []


class HRQBTask(luigi.Task):
    """Base Task class for all HRQB Tasks."""

    pipeline = luigi.Parameter()
    stage: Literal["Extract", "Transform", "Load"] = luigi.Parameter()
    table_name = luigi.OptionalStrParameter(default=None)

    @property
    def name(self) -> str:
        return self.__class__.__name__

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
            "__".join([self.pipeline, self.stage, self.name])  # noqa: FLY002
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
    def named_inputs(self) -> dict[str, luigi.Target]:
        """Dictionary of parent Tasks and their Targets.

        This is useful when a Task has multiple parent Tasks, to easily and precisely
        access a specific parent Task's output.
        """
        return {
            task.name: target
            for task, target in list(zip(self.deps(), self.input(), strict=True))
        }


@HRQBTask.event_handler(luigi.Event.SUCCESS)
def task_success_handler(task: HRQBTask) -> None:
    successful_tasks.append(task)


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
    def merge_field(self) -> str | None:
        """Field to merge records on during upsert.

        Optional property that extending task classes can define to include a merge field.
        Defaults to None.
        """
        return None

    @property
    def input_task_to_load(self) -> str | None:
        """Task name of parent, required, input task to get data from and upsert.

        If a QuickbaseUpsertTask load task has multiple required tasks, we cannot use the
        convenience method 'single_input_dataframe' to get the dataframe to load.  This
        property can be set to explicitly define which single parent task to retrieve data
        from for upsert.
        """
        return None

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
        if self.input_task_to_load:
            records_df = self.named_inputs[self.input_task_to_load].read()
        else:
            records_df = self.single_input_dataframe
        records_df = self._normalize_records_for_upsert(records_df)
        return records_df.to_dict(orient="records")

    def _normalize_records_for_upsert(self, records_df: pd.DataFrame) -> pd.DataFrame:
        """Normalize dataframe before upsert to Quickbase.

        If and when data type edge cases arise, as Quickbase is fairly particular about
        formats, this is a good place to apply additional, centralized normalization.
        """
        return records_df.replace({np.nan: None})

    def run(self) -> None:
        """Retrieve data from parent Task and upsert to Quickbase table.

        Because Load Tasks (upserting data to Quickbase) are so uniform, this run method
        can be defined on this base class.  All data required for this operation exists
        on the Task: data from parent Transform class and QB table name.

        Partial successes are possible for Quickbase upserts.  This method will log
        warnings when detected in API response, but task will be considered complete and
        ultimately successful.
        """
        records = self.get_records()

        qbclient = QBClient()
        table_id = qbclient.get_table_id(self.table_name)
        upsert_payload = qbclient.prepare_upsert_payload(
            table_id,
            records,
            merge_field=self.merge_field,
        )
        results = qbclient.upsert_records(upsert_payload)

        self.target.write(results)


@QuickbaseUpsertTask.event_handler(luigi.Event.SUCCESS)
def upsert_task_success_handler(task: QuickbaseUpsertTask) -> None:
    successful_upsert_tasks.append(task)


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

    parent_pipeline_name = luigi.OptionalStrParameter(default=None, significant=False)
    include_tasks = luigi.OptionalListParameter(default=None, significant=False)
    exclude_tasks = luigi.OptionalListParameter(default=None, significant=False)

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        """Check that attempted included or excluded tasks exist on init."""
        super().__init__(*args, **kwargs)
        self.verify_include_and_exclude_tasks_exist()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def pipeline_name(self) -> str:
        output = self.name
        if self.parent_pipeline_name:
            output = f"{self.parent_pipeline_name}__{output}"
        return output

    @abstractmethod
    def default_requires(self) -> Iterator[luigi.Task]:
        """Default root tasks for this pipeline.

        Note: this is used in place of the more conventional requires() method, allowing
        self.include_tasks, if set, to dynamically set root tasks.
        """

    def dynamic_requires(self) -> Iterator[luigi.Task]:
        """Dynamic root tasks for pipeline from self.include_tasks if set."""
        pipeline_tasks = {
            task.name: task
            for _, task in self.pipeline_tasks_iter(use_default_requires=True)
        }
        for task_name in self.include_tasks:
            if task_name in pipeline_tasks:
                yield pipeline_tasks[task_name]

    def requires(self) -> Iterator[luigi.Task]:
        """Required tasks for pipeline from defaults or dynamically from include_tasks."""
        if self.include_tasks:
            yield from self.dynamic_requires()
        else:
            yield from self.default_requires()

    def verify_include_and_exclude_tasks_exist(self) -> None:
        """Verify that included and excluded tasks exist in the pipeline."""
        filter_tasks = (self.include_tasks or ()) + (self.exclude_tasks or ())
        for task_name in filter_tasks:
            if self.get_task(task_name) is None:
                raise TaskNotInPipelineScopeError(task_name)

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
        self,
        task: HRQBTask | None = None,
        level: int = 0,
        use_default_requires: bool = False,  # noqa: FBT001, FBT002
    ) -> Iterator[tuple[int, luigi.Task]]:
        """Yield all Tasks that are part of the dependency chain for pipeline.

        This method begins with this pipeline task, recursively discovering and yielding
        required Tasks.  If use_default_requires=True, this iterator will return all
        default pipeline tasks, regardless of task includes or excludes.  This is helpful
        for understanding if a Task is part a pipeline's dependency chain, even if not
        scoped for run.
        """
        task = task or self
        yield level, task

        if use_default_requires and isinstance(task, HRQBPipelineTask):
            required_tasks = task.default_requires()
        else:
            required_tasks = task.requires()

        for parent_task in required_tasks:
            yield from self.pipeline_tasks_iter(task=parent_task, level=level + 1)

    def pipeline_targets_iter(self) -> Iterator[tuple[int, luigi.Task, luigi.Target]]:
        """Yield all Targets that are part of the dependency chain for this Task.

        Using self.pipeline_Tasks_iter(), this yields the Task's Targets.
        """
        for level, task in self.pipeline_tasks_iter():
            if hasattr(task, "target"):
                yield level, task, task.target

    def get_task(self, task_class_or_name: str | type) -> luigi.Task | None:
        """Get an instantiated child Task, by name or class, from this Pipeline task."""
        task_class_name = (
            task_class_or_name.__name__
            if isinstance(task_class_or_name, type)
            else task_class_or_name
        )
        for _, task in self.pipeline_tasks_iter():
            if task.name == task_class_name:
                return task
        return None

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
        for _, task, target in self.pipeline_targets_iter():
            if self.exclude_tasks and task.name in self.exclude_tasks:
                message = f"Skipping removal of excluded task: {task}"
                logger.debug(message)
                continue
            if target.exists() and hasattr(target, "remove"):
                target.remove()
                message = f"Target {target} successfully removed"
                logger.debug(message)

    def aggregate_upsert_results(self) -> dict | None:
        """Aggregate upsert results for Load tasks from pipeline run."""
        if not successful_upsert_tasks:
            return None
        results = {"tasks": {}, "qb_upsert_errors": False}
        for task in successful_upsert_tasks:
            result = None
            if task.target.exists():
                result = QBClient.parse_upsert_results(task.target.read())
                if result and result.get("errors") is not None:
                    results["qb_upsert_errors"] = True
            results["tasks"][task.name] = result  # type: ignore[index]
        return results

    def run_pipeline(self) -> LuigiRunResult:
        """Function to run a HRQBPipelineTask by wrapping run_task with additional work.

        This wrapper function does some preparation and cleanup:
            1. clears global lists of successfully run tasks
            2. checks that any excluded tasks are not required
            3. aggregates and logs any Quickbase upsert results if performed
        """
        successful_tasks.clear()
        successful_upsert_tasks.clear()

        if self.exclude_tasks:
            for _, task in self.pipeline_tasks_iter():
                if task.name in self.exclude_tasks and not task.target.exists():
                    raise ExcludedTaskRequiredError(task.name)

        results = run_task(self)
        if upsert_results := self.aggregate_upsert_results():
            message = f"Upsert results: {json.dumps(upsert_results)}"
            logger.info(message)

        return results
