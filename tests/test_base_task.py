# ruff: noqa: PLR2004, PD901

import os
from unittest import mock

import luigi
import pandas as pd
import pytest

from hrqb.base import (
    HRQBTask,
    PandasPickleTarget,
    QuickbaseTableTarget,
    SQLQueryExtractTask,
)
from hrqb.config import Config
from hrqb.utils.data_warehouse import DWClient


def test_base_task_required_parameter_pipeline(pipeline_name):
    with pytest.raises(
        luigi.parameter.MissingParameterException,
        match="requires the 'pipeline' parameter to be set",
    ):
        HRQBTask()


def test_base_task_required_parameter_stage(pipeline_name):
    with pytest.raises(
        luigi.parameter.MissingParameterException,
        match="requires the 'stage' parameter to be set",
    ):
        HRQBTask(pipeline=pipeline_name)


def test_base_task_generic_valid_init(generic_hrqb_task_class, pipeline_name):
    generic_hrqb_task_class(pipeline=pipeline_name, stage="Extract")


def test_base_task_output_returns_target_property(generic_hrqb_task_class, pipeline_name):
    task = generic_hrqb_task_class(pipeline=pipeline_name, stage="Extract")
    assert task.output().path == task.target.path


def test_task_dynamic_path_from_parameters(task_extract_animal_names):
    assert task_extract_animal_names.path == os.path.join(
        Config().targets_directory(),
        "Animals__Extract__ExtractAnimalNames.pickle",
    )


def test_pandas_task_outputs_pandas_pickle_target(task_extract_animal_names):
    assert isinstance(task_extract_animal_names.target, PandasPickleTarget)


def test_pandas_task_filename_extension(task_extract_animal_names):
    assert task_extract_animal_names.filename_extension == ".pickle"


def test_quickbase_task_outputs_quickbase_table_target(task_load_animals):
    assert isinstance(task_load_animals.target, QuickbaseTableTarget)


def test_quickbase_task_filename_extension(task_load_animals):
    assert task_load_animals.filename_extension == ".json"


def test_base_task_incomplete_when_target_does_not_exist(
    task_extract_animal_names,
):
    assert not task_extract_animal_names.target.exists()
    assert not task_extract_animal_names.complete()


def test_base_task_complete_when_target_exists(
    task_extract_animal_names, task_extract_animal_names_target
):
    assert task_extract_animal_names.target.exists()
    assert task_extract_animal_names.complete()


def test_base_task_single_input_error_with_multiple_parent_tasks(
    task_transform_animals,
):
    with pytest.raises(
        ValueError, match="Expected a single input to this Task but found: 2"
    ):
        task_transform_animals.single_input()


def test_base_task_single_input_success(
    task_load_animals,
):
    assert task_load_animals.single_input is not None
    assert isinstance(task_load_animals.single_input, PandasPickleTarget)


def test_base_task_single_input_dataframe_success(
    task_transform_animals, task_transform_animals_target, task_load_animals
):
    assert isinstance(task_load_animals.single_input_dataframe, pd.DataFrame)


def test_base_task_named_inputs(
    task_extract_animal_names,
    task_extract_animal_colors,
    task_transform_animals,
):
    assert isinstance(task_transform_animals.named_inputs, dict)
    assert len(task_transform_animals.named_inputs) == 2
    assert (
        task_transform_animals.named_inputs["ExtractAnimalNames"].path
        == task_extract_animal_names.target.path
    )
    assert (
        task_transform_animals.named_inputs["ExtractAnimalColors"].path
        == task_extract_animal_colors.target.path
    )


def test_quickbase_task_default_get_records_is_parent_task_single_dataframe(
    task_transform_animals_target, task_load_animals
):
    assert (
        task_load_animals.get_records()
        == task_transform_animals_target.read().to_dict(orient="records")
    )


def test_quickbase_task_run_upsert_and_json_receipt_output_target_success(
    task_transform_animals_target, task_load_animals
):
    """Mocks upsert to Quickbase, asserting mocked response is written as Target data"""
    mocked_qb_upsert_receipt = {"message": "upserted to Animals QB Table"}

    with mock.patch("hrqb.base.task.QBClient", autospec=True) as mock_qbclient_class:
        mock_qbclient = mock_qbclient_class()
        mock_qbclient.get_table_id.return_value = "abcdef123"
        mock_qbclient.prepare_upsert_payload.return_value = {}
        mock_qbclient.upsert_records.return_value = mocked_qb_upsert_receipt

        task_load_animals.run()

    mock_qbclient.get_table_id.assert_called()
    mock_qbclient.prepare_upsert_payload.assert_called()
    mock_qbclient.upsert_records.assert_called()
    assert task_load_animals.target.read() == mocked_qb_upsert_receipt


def test_base_pipeline_name(task_pipeline_animals):
    assert task_pipeline_animals.pipeline_name == "Animals"


def test_base_sql_task_missing_sql_query_property_error(pipeline_name):
    class MissingQueryTask(SQLQueryExtractTask):
        # missing required sql_query property
        pass

    with pytest.raises(TypeError, match="abstract method sql_query"):
        MissingQueryTask(pipeline=pipeline_name, stage="Extract")


def test_base_sql_task_custom_dwclient(task_sql_extract_animal_names):
    dwclient = task_sql_extract_animal_names.dwclient
    assert isinstance(dwclient, DWClient)
    dwclient.init_engine()
    assert dwclient.engine.name == "sqlite"


def test_base_sql_task_sql_query(task_sql_extract_animal_names):
    assert (
        task_sql_extract_animal_names.sql_query
        == """
        select animal_id, name from animal_name
        """
    )


def test_base_sql_task_get_dataframe_executes_sql_query_return_dataframe(
    task_sql_extract_animal_names,
):
    df = task_sql_extract_animal_names.get_dataframe()
    assert isinstance(df, pd.DataFrame)


def test_base_sql_task_sql_query_parameters_used(
    pipeline_name, sqlite_dwclient, task_sql_extract_animal_colors
):
    foo_val, bar_val = 42, "apple"

    class SQLQueryWithParameters(SQLQueryExtractTask):
        stage = luigi.Parameter("Extract")

        @property
        def dwclient(self) -> DWClient:
            return sqlite_dwclient

        @property
        def sql_query(self) -> str:
            return """
            select
                :foo_val as foo,
                :bar_val as bar
            """

        @property
        def sql_query_parameters(self) -> dict:
            return {"foo_val": foo_val, "bar_val": bar_val}

    task = SQLQueryWithParameters(pipeline=pipeline_name)
    df = task.get_dataframe()
    assert isinstance(df, pd.DataFrame)
    row = df.iloc[0]
    assert (row.foo, row.bar) == (foo_val, bar_val)


def test_base_sql_task_run_writes_pickled_dataframe(task_sql_extract_animal_names):
    task_sql_extract_animal_names.run()

    assert os.path.exists(task_sql_extract_animal_names.path)
    assert task_sql_extract_animal_names.get_dataframe().equals(
        task_sql_extract_animal_names.target.read()
    )
    assert task_sql_extract_animal_names.complete()
