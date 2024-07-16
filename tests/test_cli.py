# ruff: noqa: D202, D205, D209

import pytest
from _pytest.logging import LogCaptureFixture
from click.testing import Result

from hrqb import cli

OKAY_RESULT_CODE = 0
ERROR_RESULT_CODE = 1
MISSING_CLICK_ARG_RESULT_CODE = 2


def text_in_logs_or_stdout(text, caplog: LogCaptureFixture, result: Result):
    """Check for text in pytest caplog (logging) or CliRunner result (stdout).

    Workaround for pytest irregularities where logging and stdout are mixed for CLI output
    and luigi logging.
    """
    return text in caplog.text or text in result.output


def test_cli_no_subcommand(runner):
    result = runner.invoke(cli.main)
    assert result.exit_code == OKAY_RESULT_CODE


def test_cli_verbose_ping(caplog, runner):
    caplog.set_level("DEBUG")
    args = ["--verbose", "ping"]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    assert "pong" in caplog.text


def test_cli_pipeline_commands_require_pipeline_arg(runner):
    args = ["--verbose", "pipeline", "status"]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == MISSING_CLICK_ARG_RESULT_CODE
    assert "Error: Missing option '-p' / '--pipeline'" in result.output


def test_cli_pipeline_status_all_tasks_incomplete(caplog, runner):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=Animals",
        "status",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    with open("tests/fixtures/cli/status_all_tasks_incomplete.txt") as f:
        assert f.read() in caplog.text


def test_cli_pipeline_status_extract_tasks_complete(
    caplog, runner, task_extract_animal_names_target, task_extract_animal_colors_target
):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=Animals",
        "status",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    with open("tests/fixtures/cli/status_extract_tasks_complete.txt") as f:
        assert f.read() in caplog.text


def test_cli_pipeline_remove_data_completed_extract_tasks(
    caplog, runner, task_extract_animal_names_target, task_extract_animal_colors_target
):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=Animals",
        "remove-data",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    assert "Successfully removed target data(s)." in caplog.text
    assert f"{task_extract_animal_names_target.path} successfully removed" in caplog.text
    assert f"{task_extract_animal_colors_target.path} successfully removed" in caplog.text


def test_cli_pipeline_remove_data_task_not_found(
    caplog, runner, task_extract_animal_names_target, task_extract_animal_colors_target
):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=Animals",
        "--include=BadTask",
        "remove-data",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == ERROR_RESULT_CODE
    assert (
        "CLI options --include or --exclude are invalid: Task 'BadTask' not found in "
        "pipeline. Exiting." in result.output
    )


def test_cli_pipeline_run_success(caplog, runner):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=AnimalsDebug",
        "run",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    dataframe_print_text = """   animal_id      name  color
0         42    parrot  green
1        101  cardinal    red"""
    lines = [
        dataframe_print_text,
        "Pipeline run result: SUCCESS",
    ]
    for line in lines:
        assert text_in_logs_or_stdout(line, caplog, result)


def test_cli_pipeline_run_and_remove_data_success(caplog, runner):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=AnimalsDebug",
        "run",
        "--cleanup",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    lines = [
        "Pipeline run result: SUCCESS",
        "Successfully removed target data(s).",
        "AnimalsDebug__Load__LoadAnimalsDebug.json successfully removed",
        "AnimalsDebug__Transform__PrepareAnimals.pickle successfully removed",
        "AnimalsDebug__Extract__ExtractAnimalColors.pickle successfully removed",
        "AnimalsDebug__Extract__ExtractAnimalNames.pickle successfully removed",
    ]
    for line in lines:
        assert text_in_logs_or_stdout(line, caplog, result)


def test_cli_pipeline_run_start_task_success(caplog, runner):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=AnimalsDebug",
        "--include=ExtractAnimalNames",
        "run",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    lines = [
        "Successfully loaded pipeline: 'tests.fixtures.tasks.pipelines.AnimalsDebug'",
        "Pipeline run result: SUCCESS",
    ]
    for line in lines:
        assert text_in_logs_or_stdout(line, caplog, result)


def test_cli_pipeline_run_start_task_cleanup_success(caplog, runner):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=AnimalsDebug",
        "--include=ExtractAnimalNames",
        "run",
        "--cleanup",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    lines = [
        "Successfully loaded pipeline: 'tests.fixtures.tasks.pipelines.AnimalsDebug'",
        "ExtractAnimalNames.pickle successfully removed",
        "Pipeline run result: SUCCESS",
    ]
    for line in lines:
        assert text_in_logs_or_stdout(line, caplog, result)


def test_cli_pipeline_run_start_task_not_found_error(caplog, runner):
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=AnimalsDebug",
        "--include=BadTask",
        "run",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == ERROR_RESULT_CODE
    assert (
        "CLI options --include or --exclude are invalid: Task 'BadTask' not found in "
        "pipeline. Exiting." in result.output
    )


@pytest.mark.usefixtures(
    "_dwclient_connection_test_success", "_qbclient_connection_test_success"
)
def test_cli_test_connections_success(caplog, runner):
    caplog.set_level("DEBUG")
    args = ["--verbose", "test-connections"]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    lines = [
        "Data Warehouse connection successful",
        "Quickbase connection successful",
        "All connections OK",
    ]
    for line in lines:
        assert text_in_logs_or_stdout(line, caplog, result)


@pytest.mark.usefixtures(
    "_dwclient_connection_test_raise_exception", "_qbclient_connection_test_success"
)
def test_cli_test_connections_data_warehouse_connection_error(caplog, runner):
    caplog.set_level("DEBUG")
    args = ["--verbose", "test-connections"]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    lines = [
        "Data Warehouse connection failed: Intentional Error Here",
        "Quickbase connection successful",
        " One or more connections failed",
    ]
    for line in lines:
        assert text_in_logs_or_stdout(line, caplog, result)


@pytest.mark.usefixtures(
    "_dwclient_connection_test_success", "_qbclient_connection_test_raise_exception"
)
def test_cli_test_connections_quickbase_connection_error(caplog, runner):
    caplog.set_level("DEBUG")
    args = ["--verbose", "test-connections"]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    lines = [
        "Data Warehouse connection successful",
        "Quickbase connection failed: Intentional Error Here",
        "One or more connections failed",
    ]
    for line in lines:
        assert text_in_logs_or_stdout(line, caplog, result)


def test_cli_pipeline_load_include_tasks_success(caplog, runner):
    """This test sets a new root task for pipeline of PrepareAnimals, thereby removing
    the task LoadAnimals entirely from the pipeline run scope."""
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=Animals",
        "--include=PrepareAnimals",
        "run",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    assert not text_in_logs_or_stdout("LoadAnimals", caplog, result)
    assert text_in_logs_or_stdout("PrepareAnimals", caplog, result)


def test_cli_pipeline_load_exclude_tasks_success(
    caplog, runner, task_extract_animal_names_target
):
    """This test uses a pre-existing data for task ExtractAnimalNames via a fixture,
    then excludes it from the run.  Even though task is excluded, the run is successful
    because pre-existing data exists."""
    caplog.set_level("DEBUG")
    args = [
        "--verbose",
        "pipeline",
        "--pipeline-module=tests.fixtures.tasks.pipelines",
        "--pipeline=Animals",
        "--exclude=ExtractAnimalNames",
        "run",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
