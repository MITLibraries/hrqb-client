# ruff: noqa: S105, TRY301, TRY002, BLE001

import json
import logging

import sentry_sdk

from hrqb import config
from hrqb.config import configure_logger, configure_sentry


def test_configure_logger_not_verbose():
    logger = logging.getLogger(__name__)
    result = configure_logger(logger, verbose=False)
    info_log_level = 20
    assert logger.getEffectiveLevel() == info_log_level
    assert result == "Logger 'tests.test_config' configured with level=INFO"


def test_configure_logger_verbose():
    logger = logging.getLogger(__name__)
    result = configure_logger(logger, verbose=True)
    debug_log_level = 10
    assert logger.getEffectiveLevel() == debug_log_level
    assert result == "Logger 'tests.test_config' configured with level=DEBUG"


def test_configure_sentry_no_env_variable(monkeypatch):
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    result = configure_sentry()
    assert result == "No Sentry DSN found, exceptions will not be sent to Sentry"


def test_configure_sentry_env_variable_is_none(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    result = configure_sentry()
    assert result == "No Sentry DSN found, exceptions will not be sent to Sentry"


def test_configure_sentry_env_variable_is_dsn(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "https://1234567890@00000.ingest.sentry.io/123456")
    result = configure_sentry()
    assert result == "Sentry DSN found, exceptions will be sent to Sentry with env=test"


def test_sentry_scope_variables_removed_from_sent_event(
    mocker,
    sensitive_scope_variable,
):
    not_secret_value = "Everyone can see this exception message."
    secret_value = "very-secret-abc123"

    spy_scrubber = mocker.spy(config, "_remove_sensitive_scope_variables")
    try:
        raise Exception(not_secret_value)
    except Exception as exc:
        sentry_sdk.capture_exception(exc)

    original_event = json.dumps(spy_scrubber.call_args)
    scrubbed_event = json.dumps(spy_scrubber.spy_return)

    # not secret value present before and after scrubbing
    assert not_secret_value in original_event
    assert not_secret_value in scrubbed_event

    # secret value present in original, but absent from scrubbed
    assert secret_value in original_event
    assert secret_value not in scrubbed_event
