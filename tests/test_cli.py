from hrqb.cli import main

OKAY_RESULT_CODE = 0
MISSING_CLICK_ARG_RESULT_CODE = 2


def test_cli_no_subcommand(runner):
    result = runner.invoke(main)
    assert result.exit_code == OKAY_RESULT_CODE


def test_cli_verbose_ping(caplog, runner):
    caplog.set_level("DEBUG")
    args = ["--verbose", "ping"]
    result = runner.invoke(main, args)
    assert result.exit_code == OKAY_RESULT_CODE
    assert "pong" in caplog.text
