import pytest
from click.testing import CliRunner


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("LUIGI_CONFIG_PATH", "hrqb/luigi.cfg")


@pytest.fixture
def runner():
    return CliRunner()
