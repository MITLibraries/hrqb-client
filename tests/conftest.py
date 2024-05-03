import pandas as pd
import pytest
from click.testing import CliRunner

from hrqb.base.task import PandasPickleTask, QuickbaseUpsertTask


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("LUIGI_CONFIG_PATH", "hrqb/luigi.cfg")


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture(scope="session")
def session_temp_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("shared_temp_dir")


@pytest.fixture
def simple_pandas_dataframe():
    return pd.DataFrame([(42, "horse"), (101, "zebra")], columns=["id", "data"])


@pytest.fixture
def simple_pandas_series():
    return pd.Series(["horse", "zebra"])


@pytest.fixture
def quickbase_api_write_receipt():
    # example data from https://developer.quickbase.com/operation/upsert
    return {
        "data": [
            {
                "3": {"value": 1},
                "6": {"value": "Updating this record"},
                "7": {"value": 10},
                "8": {"value": "2019-12-18T08:00:00.000Z"},
            },
            {
                "3": {"value": 11},
                "6": {"value": "This is my text"},
                "7": {"value": 15},
                "8": {"value": "2019-12-19T08:00:00.000Z"},
            },
            {
                "3": {"value": 12},
                "6": {"value": "This is my other text"},
                "7": {"value": 20},
                "8": {"value": "2019-12-20T08:00:00.000Z"},
            },
        ],
        "metadata": {
            "createdRecordIds": [11, 12],
            "totalNumberOfRecordsProcessed": 3,
            "unchangedRecordIds": [],
            "updatedRecordIds": [1],
        },
    }


@pytest.fixture
def pandas_pickle_task(tmpdir):
    filepath = f"{tmpdir}/foo.pickle"
    return PandasPickleTask(path=filepath, table_name="Foo")


@pytest.fixture
def quickbase_upsert_task(tmpdir):
    filepath = f"{tmpdir}/foo.json"
    return QuickbaseUpsertTask(path=filepath, table_name="Foo")


@pytest.fixture
def complete_first_pandas_dataframe_task(
    tmpdir, pandas_pickle_task, simple_pandas_dataframe
):
    pandas_pickle_task.target().write(simple_pandas_dataframe)
    assert pandas_pickle_task.complete()
    return pandas_pickle_task


@pytest.fixture
def complete_first_pandas_series_task(tmpdir, pandas_pickle_task, simple_pandas_series):
    pandas_pickle_task.target().write(simple_pandas_series)
    assert pandas_pickle_task.complete()
    return pandas_pickle_task


@pytest.fixture
def incomplete_first_pandas_task(tmpdir, pandas_pickle_task):
    return pandas_pickle_task


@pytest.fixture
def second_task_with_complete_parent_dataframe_task(
    tmpdir, complete_first_pandas_dataframe_task
):
    class SecondTask(PandasPickleTask):
        def requires(self):
            return [complete_first_pandas_dataframe_task]

    return SecondTask(path=f"{tmpdir}/bar.pickle", table_name="bar")


@pytest.fixture
def second_task_with_complete_parent_series_task(
    tmpdir, complete_first_pandas_series_task
):
    class SecondTask(PandasPickleTask):
        def requires(self):
            return [complete_first_pandas_series_task]

    return SecondTask(path=f"{tmpdir}/bar.pickle", table_name="bar")
