import pandas as pd
import pytest

from hrqb.base import PandasPickleTarget, PandasPickleTask, QuickbaseTableTarget


def test_pandas_pickle_task_gives_pandas_pickle_target(pandas_pickle_task):
    target = pandas_pickle_task.target()
    assert isinstance(target, PandasPickleTarget)
    assert target.path == pandas_pickle_task.path
    assert target.table_name == pandas_pickle_task.table_name


def test_pandas_pickle_task_output_path_is_target_path(pandas_pickle_task):
    assert pandas_pickle_task.output().path == pandas_pickle_task.target().path


def test_quickbase_upsert_task_gives_quickbase_table_target(quickbase_upsert_task):
    target = quickbase_upsert_task.target()
    assert isinstance(target, QuickbaseTableTarget)
    assert target.path == quickbase_upsert_task.path
    assert target.table_name == quickbase_upsert_task.table_name


def test_quickbase_upsert_task_output_path_is_target_path(quickbase_upsert_task):
    assert quickbase_upsert_task.output().path == quickbase_upsert_task.target().path


def test_luigi_task_incomplete_when_target_not_exists(tmpdir, pandas_pickle_task):
    assert not pandas_pickle_task.complete()


def test_luigi_task_complete_when_target_exists(tmpdir, pandas_pickle_task):
    with open(pandas_pickle_task.path, "a"):
        assert pandas_pickle_task.complete()


def test_hrqb_task_get_single_input_from_complete_parent_task_success(
    tmpdir,
    complete_first_pandas_dataframe_task,
):
    class SecondTask(PandasPickleTask):
        def requires(self):
            return [complete_first_pandas_dataframe_task]

    task = SecondTask(path=f"{tmpdir}/bar.pickle", table_name="bar")
    assert task.single_input.exists()
    task_input_dataframe = task.single_input.read()
    parent_task_output_dataframe = complete_first_pandas_dataframe_task.target().read()
    assert task_input_dataframe.equals(parent_task_output_dataframe)


def test_hrqb_task_get_single_input_from_incomplete_parent_task_not_exists(
    tmpdir,
    incomplete_first_pandas_task,
):
    class SecondTask(PandasPickleTask):
        def requires(self):
            return [incomplete_first_pandas_task]

    task = SecondTask(path=f"{tmpdir}/bar.pickle", table_name="bar")
    assert not task.single_input.exists()


def test_hrqb_task_get_parent_task_target_dataframe(
    second_task_with_complete_parent_dataframe_task,
):
    assert isinstance(
        second_task_with_complete_parent_dataframe_task.input_pandas_dataframe,
        pd.DataFrame,
    )
    with pytest.raises(TypeError, match="Expected pandas Series but got"):
        _ = second_task_with_complete_parent_dataframe_task.input_pandas_series


def test_hrqb_task_get_parent_task_target_series(
    second_task_with_complete_parent_series_task,
):
    assert isinstance(
        second_task_with_complete_parent_series_task.input_pandas_series, pd.Series
    )
    with pytest.raises(TypeError, match="Expected pandas Dataframe but got"):
        _ = second_task_with_complete_parent_series_task.input_pandas_dataframe


def test_hrqb_task_multiple_parent_tasks_single_input_raise_error(
    tmpdir,
    second_task_with_complete_parent_dataframe_task,
    second_task_with_complete_parent_series_task,
):
    class SecondTask(PandasPickleTask):
        def requires(self):
            # note the multiple parent Tasks here
            return [
                second_task_with_complete_parent_dataframe_task,
                second_task_with_complete_parent_series_task,
            ]

    task = SecondTask(path=f"{tmpdir}/bar.pickle", table_name="bar")
    with pytest.raises(
        ValueError, match="Expected a single input to this Task but found: 2"
    ):
        _ = task.single_input
