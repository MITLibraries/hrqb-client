import os

import luigi
import pandas as pd
import pytest

from hrqb.base import HRQBLocalTarget, PandasPickleTarget, QuickbaseTableTarget


def test_hrqb_local_target_require_path_and_table_name():
    with pytest.raises(
        TypeError,
        match="missing 2 required positional arguments: 'path' and 'table_name'",
    ):
        HRQBLocalTarget()


def test_hrqb_local_target_init_success(tmpdir):
    filepath = f"{tmpdir}/temp_text_file.txt"
    target = HRQBLocalTarget(path=filepath, table_name="Foo")
    assert target.path == filepath
    assert target.table_name == "Foo"
    assert isinstance(target, HRQBLocalTarget)
    assert isinstance(target, luigi.LocalTarget)


def test_hrqb_local_target_write_read_file(tmpdir):
    filepath = f"{tmpdir}/temp_text_file.txt"
    target = HRQBLocalTarget(path=filepath, table_name="Foo")
    message = b"Hello World!"
    with target.open("w") as f:
        f.write(message)
    assert os.path.exists(filepath)

    with target.open("r") as f:
        assert f.read() == message


def test_pandas_pickle_target_dataframe_write_read_success(
    tmpdir, simple_pandas_dataframe
):
    filepath = f"{tmpdir}/temp_file.pickle"
    target = PandasPickleTarget(path=filepath, table_name="Foo")
    target.write(simple_pandas_dataframe)
    assert os.path.exists(filepath)

    assert isinstance(target.read(), pd.DataFrame)
    assert target.read().equals(simple_pandas_dataframe)


def test_pandas_pickle_target_series_write_read_success(tmpdir, simple_pandas_series):
    filepath = f"{tmpdir}/temp_file.pickle"
    target = PandasPickleTarget(path=filepath, table_name="Foo")
    target.write(simple_pandas_series)
    assert os.path.exists(filepath)

    assert isinstance(target.read(), pd.Series)
    assert target.read().equals(simple_pandas_series)


def test_quickbase_table_target_write_read_success(tmpdir, quickbase_api_write_receipt):
    filepath = f"{tmpdir}/temp_file.json"
    target = QuickbaseTableTarget(path=filepath, table_name="Foo")
    target.write(quickbase_api_write_receipt)
    assert os.path.exists(filepath)

    assert isinstance(target.read(), dict)
    assert target.read() == quickbase_api_write_receipt
