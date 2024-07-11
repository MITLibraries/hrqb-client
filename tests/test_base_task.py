# ruff: noqa: PLR2004, PD901

import os
from unittest import mock

import luigi
import pandas as pd
import pytest

from hrqb.base import (
    HRQBTask,
    PandasPickleTarget,
    PandasPickleTask,
    QuickbaseTableTarget,
    SQLQueryExtractTask,
)
from hrqb.config import Config
from hrqb.utils.data_warehouse import DWClient
from hrqb.utils.luigi import run_pipeline
from tests.fixtures.tasks.extract import ExtractAnimalNames
from tests.fixtures.tasks.load import LoadTaskMultipleRequired


def test_base_task_name(task_extract_animal_names):
    assert task_extract_animal_names.name == "ExtractAnimalNames"


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
    mocked_qb_upsert_receipt = {
        "data": [],
        "metadata": {
            "createdRecordIds": [11, 12],
            "totalNumberOfRecordsProcessed": 2,
            "unchangedRecordIds": [],
            "updatedRecordIds": [],
        },
    }

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


def test_quickbase_task_run_upsert_and_json_receipt_output_target_api_errors_logged(
    caplog, task_transform_animals_target, task_load_animals
):
    """Mocks upsert to Quickbase, asserting mocked response is written as Target data"""
    mocked_qb_upsert_receipt = {
        "data": [],
        "metadata": {
            "createdRecordIds": [11, 12],
            "lineErrors": {"2": ['Incompatible value for field with ID "6".']},
            "totalNumberOfRecordsProcessed": 3,
            "unchangedRecordIds": [],
            "updatedRecordIds": [],
        },
    }
    with mock.patch("hrqb.base.task.QBClient", autospec=True) as mock_qbclient_class:
        mock_qbclient = mock_qbclient_class()
        mock_qbclient.get_table_id.return_value = "abcdef123"
        mock_qbclient.prepare_upsert_payload.return_value = {}
        mock_qbclient.upsert_records.return_value = mocked_qb_upsert_receipt

        task_load_animals.run()

    assert "errors" in task_load_animals.parse_upsert_counts


def test_base_pipeline_name(task_pipeline_animals):
    assert task_pipeline_animals.pipeline_name == "Animals"


def test_base_pipeline_name_with_parent_pipelines(task_pipeline_creatures):
    assert task_pipeline_creatures.pipeline_name == "Creatures"
    child_task = task_pipeline_creatures.deps()[0]
    assert child_task.pipeline_name == "Creatures__Animals"


def test_base_sql_task_missing_sql_query_or_sql_file_error(pipeline_name):
    class MissingRequiredPropertiesQueryTask(SQLQueryExtractTask):
        pass

    task = MissingRequiredPropertiesQueryTask(pipeline=pipeline_name, stage="Extract")
    with pytest.raises(
        AttributeError,
        match=(
            "Property 'sql_file' must be set or property 'sql_query' overridden to "
            "explicitly return a SQL string."
        ),
    ):
        task.get_dataframe()


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


def test_base_sql_task_sql_file(task_sql_extract_animal_colors):
    assert (
        task_sql_extract_animal_colors.sql_file
        == "tests/fixtures/sql/animal_color_query.sql"
    )


def test_base_sql_task_sql_query_get_dataframe_return_dataframe(
    task_sql_extract_animal_names,
):
    df = task_sql_extract_animal_names.get_dataframe()
    assert isinstance(df, pd.DataFrame)


def test_base_sql_task_sql_file_get_dataframe_return_dataframe(
    task_sql_extract_animal_colors,
):
    df = task_sql_extract_animal_colors.get_dataframe()
    assert isinstance(df, pd.DataFrame)


def test_base_sql_task_sql_query_parameters_used(task_extract_sql_query_with_parameters):
    df = task_extract_sql_query_with_parameters.get_dataframe()
    assert isinstance(df, pd.DataFrame)
    row = df.iloc[0]
    assert (row.foo, row.bar) == (42, "apple")


def test_base_sql_task_run_writes_pickled_dataframe(task_sql_extract_animal_names):
    task_sql_extract_animal_names.run()

    assert os.path.exists(task_sql_extract_animal_names.path)
    assert task_sql_extract_animal_names.get_dataframe().equals(
        task_sql_extract_animal_names.target.read()
    )
    assert task_sql_extract_animal_names.complete()


def test_base_pipeline_task_get_task_str_success(task_pipeline_animals):
    task = task_pipeline_animals.get_task("ExtractAnimalNames")
    assert isinstance(task, PandasPickleTask)


def test_base_pipeline_task_get_task_class_success(task_pipeline_animals):
    task = task_pipeline_animals.get_task(ExtractAnimalNames)
    assert isinstance(task, PandasPickleTask)


def test_base_pipeline_task_get_task_not_found_return_none(task_pipeline_animals):
    assert not task_pipeline_animals.get_task("BadTask")


def test_quickbase_task_input_task_to_load_property_used(
    pipeline_name, task_extract_animal_colors_target
):
    task = LoadTaskMultipleRequired(pipeline=pipeline_name)
    assert task.input_task_to_load == "ExtractAnimalColors"
    input_dict = task.get_records()
    assert pd.DataFrame(input_dict).equals(task_extract_animal_colors_target.read())


def test_base_pipeline_task_aggregate_upsert_results_one_success_returns_dict(
    task_pipeline_animals_debug,
):
    run_pipeline(task_pipeline_animals_debug)

    assert task_pipeline_animals_debug.aggregate_upsert_results() == {
        "tasks": {
            "LoadAnimalsDebug": {
                "processed": 3,
                "created": 2,
                "updated": 1,
                "unchanged": 0,
                "errors": None,
            }
        },
        "qb_upsert_errors": False,
    }


def test_base_pipeline_task_aggregate_upsert_results_failed_load_returns_none_value(
    task_pipeline_animals_debug,
):

    # mock run() method of LoadAnimalsDebug to throw exception
    load_task = task_pipeline_animals_debug.get_task("LoadAnimalsDebug")
    with mock.patch.object(load_task, "run") as mocked_run:
        mocked_run.side_effect = Exception("UPSERT FAILED!")
        run_pipeline(task_pipeline_animals_debug)

    assert task_pipeline_animals_debug.aggregate_upsert_results() == {
        "qb_upsert_errors": False,
        "tasks": {"LoadAnimalsDebug": None},
    }


def test_base_pipeline_task_aggregate_upsert_results_upsert_with_errors_noted(
    task_pipeline_animals_debug,
):
    run_pipeline(task_pipeline_animals_debug)

    # manually modify output of load task to simulate upsert errors
    load_task = task_pipeline_animals_debug.get_task("LoadAnimalsDebug")
    new_target = load_task.target.read()
    new_target["metadata"]["lineErrors"] = {
        "2": ['Incompatible value for field with ID "6".'],
        "4": ['Incompatible value for field with ID "6".'],
        "5": ["Weird Error", "Another Weird Error"],
    }
    load_task.target.write(new_target)

    assert task_pipeline_animals_debug.aggregate_upsert_results() == {
        "tasks": {
            "LoadAnimalsDebug": {
                "processed": 3,
                "created": 2,
                "updated": 1,
                "unchanged": 0,
                "errors": {
                    'Incompatible value for field with ID "6".': 2,
                    "Weird Error": 1,
                    "Another Weird Error": 1,
                },
            }
        },
        "qb_upsert_errors": True,
    }
