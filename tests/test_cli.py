import pytest
from _pytest.logging import LogCaptureFixture
from click.testing import Result

from hrqb import cli

OKAY_RESULT_CODE = 0
MISSING_CLICK_ARG_RESULT_CODE = 2


def text_in_logs_or_stdout(text, caplog: LogCaptureFixture, result: Result):
    """Check for text in pytest caplog (logging) or CliRunner result (stdout).

    Workaround for pytest irregularities where logging and stdout are mixed for CLI output
    and luigi logging.
    """
    if text in caplog.text:
        return True
    if text in result.output:
        return True
    return False


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
    assert "Removing all Pipeline Tasks Targets (data)." in caplog.text
    assert f"{task_extract_animal_names_target.path} successfully removed" in caplog.text
    assert f"{task_extract_animal_colors_target.path} successfully removed" in caplog.text


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
        "Removing all Pipeline Tasks Targets (data).",
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
        "run",
        "--start-task=ExtractAnimalNames",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    lines = [
        "Successfully loaded pipeline: 'tests.fixtures.tasks.pipelines.AnimalsDebug'",
        "Start task loaded: ExtractAnimalNames",
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
        "run",
        "--start-task=BadTask",
    ]
    result = runner.invoke(cli.main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    lines = [
        "Successfully loaded pipeline: 'tests.fixtures.tasks.pipelines.AnimalsDebug'",
        "Could not find task: BadTask",
    ]
    for line in lines:
        assert text_in_logs_or_stdout(line, caplog, result)


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
