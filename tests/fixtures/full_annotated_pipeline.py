# ruff: noqa: D205,D209

import luigi
import pandas as pd

from hrqb.base import HRQBPipelineTask, PandasPickleTask


class GenerateNumbers(PandasPickleTask):
    """An Extract-like Task that generates numbers."""

    pipeline = luigi.Parameter()  # parameter that is supplied by calling Task
    stage = luigi.Parameter("Extract")  # ETL stage

    def get_dataframe(self):
        return pd.DataFrame(
            [0, 1, 2, 3, 4],
            columns=["number"],
        )


class GenerateLetters(PandasPickleTask):
    """An Extract-like Task that generates letters."""

    pipeline = luigi.Parameter()  # parameter that is supplied by calling Task
    stage = luigi.Parameter("Extract")  # ETL stage

    def get_dataframe(self):
        return pd.DataFrame(
            ["a", "b", "c", "d", "e"],
            columns=["letter"],
        )


class MultiplyNumbers(PandasPickleTask):
    """A Transform-like Task that multiples the number x 10."""

    pipeline = luigi.Parameter()  # parameter that is supplied by calling Task
    stage = luigi.Parameter("Transform")  # ETL stage

    def requires(self):
        """This task only requires a single parent Task."""
        return [GenerateNumbers(pipeline=self.pipeline)]

    def get_dataframe(self):
        # utilizing convenience method for a single DataFrame input
        numbers_df = self.single_input_dataframe
        numbers_df["number"] = numbers_df["number"] * 10
        return numbers_df


class CombineLettersAndNumbers(PandasPickleTask):
    """A Transform-like Task that combines numbers and letters into new Dataframe.

    This is effectively the last Task in the pipeline.  Most often, this will be a
    QuickbaseTableUpsert Task that actually writes data, but that's not strictly required.
    """

    pipeline = luigi.Parameter()  # parameter that is supplied by calling Task
    stage = luigi.Parameter("Transform")  # ETL stage

    def requires(self):
        """This Task relies on TWO parent Tasks for input data.  This Task will only
        get invoked if and when both of them are complete."""
        return [
            MultiplyNumbers(pipeline=self.pipeline),  # Task with multiplied numbers
            GenerateLetters(pipeline=self.pipeline),  # Task with letters
        ]

    def get_dataframe(self):
        """Because we know all parent Tasks have the same number of rows in their
        Dataframes, we can just concatenate them together for our output.

        However, because multiple parent Tasks, we cannot use the convenience property
        self.single_input_dataframe. Instead, we use the self.named_inputs() to get
        the Targets from those parent Tasks by name, and then get the dataframe itself
        from the PandasPickleTarget.read() method.
        """
        letters_df = self.named_inputs["GenerateLetters"].read()
        numbers_df = self.named_inputs["MultiplyNumbers"].read()
        return pd.concat([letters_df, numbers_df], axis=1)


class AlphaNumeric(HRQBPipelineTask):
    """Pipeline Task that's only requirement is yielding required Tasks."""

    def default_requires(self):
        yield CombineLettersAndNumbers(pipeline=self.pipeline_name)
