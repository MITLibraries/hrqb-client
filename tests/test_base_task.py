# ruff: noqa: PLR2004

import os
from unittest import mock

import luigi
import pandas as pd
import pytest

from hrqb.base import HRQBTask, PandasPickleTarget, QuickbaseTableTarget
from hrqb.config import Config


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


def test_base_task_requires_filename_extension_property(pipeline_name):
    with pytest.raises(
        TypeError,
        match="abstract method filename_extension",
    ):
        HRQBTask(pipeline=pipeline_name, stage="Extract")


def test_base_task_generic_valid_init(pipeline_name):
    class GenericTask(HRQBTask):
        def filename_extension(self):
            return ".csv"

    GenericTask(pipeline=pipeline_name, stage="Extract")


def test_task_dynamic_path_from_parameters(task_extract_animal_names):
    assert task_extract_animal_names.path == os.path.join(
        Config().targets_directory(),
        "Animals__Extract__ExtractAnimalNames.pickle",
    )


def test_pandas_task_outputs_pandas_pickle_target(task_extract_animal_names):
    assert isinstance(task_extract_animal_names.target(), PandasPickleTarget)


def test_pandas_task_filename_extension(task_extract_animal_names):
    assert task_extract_animal_names.filename_extension == ".pickle"


def test_quickbase_task_outputs_quickbase_table_target(task_load_animals):
    assert isinstance(task_load_animals.target(), QuickbaseTableTarget)


def test_quickbase_task_filename_extension(task_load_animals):
    assert task_load_animals.filename_extension == ".json"


def test_base_task_incomplete_when_target_does_not_exist(
    task_extract_animal_names,
):
    assert not task_extract_animal_names.target().exists()
    assert not task_extract_animal_names.complete()


def test_base_task_complete_when_target_exists(
    task_extract_animal_names, task_extract_animal_names_target
):
    assert task_extract_animal_names.target().exists()
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
        == task_extract_animal_names.target().path
    )
    assert (
        task_transform_animals.named_inputs["ExtractAnimalColors"].path
        == task_extract_animal_colors.target().path
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
    assert task_load_animals.target().read() == mocked_qb_upsert_receipt


def test_base_pipeline_name(task_pipeline_animals):
    assert task_pipeline_animals.pipeline_name == "Animals"
