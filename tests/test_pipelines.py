from unittest import mock

import luigi
import pytest
import sentry_sdk

from hrqb.utils.luigi import run_pipeline
from tests.fixtures.full_annotated_pipeline import (
    AlphaNumeric,
    CombineLettersAndNumbers,
)
from tests.fixtures.tasks.extract import ExtractAnimalColors, ExtractAnimalNames
from tests.fixtures.tasks.load import LoadAnimalsDebug
from tests.fixtures.tasks.pipelines import AnimalsDebug
from tests.fixtures.tasks.transform import PrepareAnimals


def test_pipeline_pipeline_tasks_iter_gives_all_parent_tasks(task_pipeline_animals_debug):
    pipeline_tasks = [
        task for _level, task in task_pipeline_animals_debug.pipeline_tasks_iter()
    ]
    assert pipeline_tasks[0].__class__ == AnimalsDebug  # Pipeline Task
    assert pipeline_tasks[1].__class__ == LoadAnimalsDebug  # Load Task
    assert pipeline_tasks[2].__class__ == PrepareAnimals  # Transform Task
    assert pipeline_tasks[3].__class__ == ExtractAnimalColors  # Extract Task #1
    assert pipeline_tasks[4].__class__ == ExtractAnimalNames  # Extract Task #2


def test_pipeline_complete_when_all_parent_tasks_complete(
    task_extract_animal_names,
    task_extract_animal_colors,
    task_transform_animals,
    task_load_animals,
    task_pipeline_animals,
    task_extract_animal_names_target,
    task_extract_animal_colors_target,
    task_transform_animals_target,
    task_load_animals_target,
):
    # because Targets exist via the fixtures, all individual Tasks are considered complete
    assert task_extract_animal_names.complete()
    assert task_extract_animal_colors.complete()
    assert task_transform_animals.complete()
    assert task_load_animals.complete()

    # without running, or an existing Target, an HRQBPipelineTask is already considered
    # complete when all Tasks in the pipeline are complete
    assert task_pipeline_animals.complete()


def test_full_annotated_simple_pipeline():

    # With all the worker Tasks defined, and the pipeline Task 'AlphaNumeric' defined,
    # we can run the pipeline directly using luigi to demonstrate the Task inputs/outputs
    # and dependencies are automatically handled.  run_pipeline(...) is a custom wrapper
    # function that runs luigi.build(...), and is what's called by CLI commands.
    results = run_pipeline(AlphaNumeric())

    # assert successful results
    assert results.status == luigi.LuigiStatusCode.SUCCESS

    # Initializing a Task class directly, as long as the pipeline name is the same, gives
    # us quick access to its outputs (Targets).
    task = CombineLettersAndNumbers(pipeline="AlphaNumeric")
    task_df = task.target.read()

    # assert the dataframe created by CombineLettersAndNumbers is what we expect
    assert task_df.to_dict() == {
        "number": {0: 0, 1: 10, 2: 20, 3: 30, 4: 40},
        "letter": {0: "a", 1: "b", 2: "c", 3: "d", 4: "e"},
    }


def test_run_pipeline_with_non_hrqbpipelinetask_type_raise_error(
    task_extract_animal_names,
):
    with pytest.raises(
        TypeError,
        match="ExtractAnimalNames is not a HRQBPipelineTask type task",
    ):
        run_pipeline(task_extract_animal_names)


def test_run_pipeline_no_sentry_message_with_no_upsert_errors(mocker):
    spy_capture_message = mocker.spy(sentry_sdk, "capture_message")
    run_pipeline(AlphaNumeric())
    spy_capture_message.assert_not_called()


def test_run_pipeline_send_sentry_message_on_upsert_error(mocker):
    spy_capture_message = mocker.spy(sentry_sdk, "capture_message")
    with mock.patch.object(
        AlphaNumeric, "aggregate_upsert_results"
    ) as mocked_agg_results:
        mocked_agg_results.return_value = {
            "tasks": {
                "FinickyTask": {"errors": ["Error1", "Error2"]},
                "SolidTask": {"errors": None},
            },
            "qb_upsert_errors": True,
        }
        run_pipeline(AlphaNumeric())

    spy_capture_message.assert_called()
    assert (
        spy_capture_message.call_args[0][0]
        == "Quickbase upsert error(s) detected for tasks: ['FinickyTask']. Please see "
        "logs for information on specific errors encountered."
    )
