"""hrqb.base.task"""

import json
import logging
import os
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Iterator
from typing import Literal

import luigi  # type: ignore[import-untyped]
import numpy as np
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
    def named_inputs(self) -> dict[str, luigi.Target]:
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
    def merge_field(self) -> str | None:
        """Field to merge records on during upsert.

        Optional property that extending task classes can define to include a merge field.
        Defaults to None.
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

        self.parse_and_log_upsert_results(results)
        self.parse_and_log_upsert_errors(results)

        self.target.write(results)

    def parse_and_log_upsert_results(self, api_response: dict) -> None:
        """Parse Quickbase upsert response and log counts of records modified."""
        record_counts = QBClient.parse_upsert_results(api_response)
        if not record_counts:
            return
        for key in ["created", "updated", "unchanged"]:
            record_counts[key] = len(record_counts[key])
        message = f"Upsert results: {record_counts}"
        logger.info(message)

    def parse_and_log_upsert_errors(self, api_response: dict) -> None:
        """Parse Quickbase upsert response and log any errors.

        Errors are returned for each record upserted, for each field with issues.  This is
        an unnecessary level of grain for logging, so the field error types are counted
        across all records and logged.  This gives a high level overview of fields that
        are failing, and how often, where further debugging would involve looking at the
        response directly in the task output.
        """
        if api_errors := api_response.get("metadata", {}).get("lineErrors"):
            api_error_counts: dict[str, int] = defaultdict(int)
            for errors in api_errors.values():
                for error in errors:
                    api_error_counts[error] += 1
            message = "Quickbase API call completed but had errors: " + json.dumps(
                api_error_counts
            )
            logger.warning(message)


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

    @property
    def pipeline_name(self) -> str:
        output = self.__class__.__name__
        if self.parent_pipeline_name:
            output = f"{self.parent_pipeline_name}__{output}"
        return output

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

    def get_task(self, task_class_or_name: str | type) -> luigi.Task | None:
        """Get an instantiated child Task, by name, from this parent Pipeline task."""
        task_class_name = (
            task_class_or_name.__name__
            if isinstance(task_class_or_name, type)
            else task_class_or_name
        )
        for _, task in self.pipeline_tasks_iter():
            if task.__class__.__name__ == task_class_name:
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
        for _, target in self.pipeline_targets_iter():
            if target.exists() and hasattr(target, "remove"):
                target.remove()
                message = f"Target {target} successfully removed"
                logger.debug(message)
