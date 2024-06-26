import logging
import os
from typing import Any

import sentry_sdk


class Config:
    REQUIRED_ENV_VARS = (
        "WORKSPACE",
        "SENTRY_DSN",
        "LUIGI_CONFIG_PATH",
        "QUICKBASE_API_URL",
        "QUICKBASE_API_TOKEN",
        "QUICKBASE_APP_ID",
        "DATA_WAREHOUSE_CONNECTION_STRING",
    )
    OPTIONAL_ENV_VARS = (
        "DYLD_LIBRARY_PATH",
        "TARGETS_DIRECTORY",
        "LUIGI_NUM_WORKERS",
    )

    def check_required_env_vars(self) -> None:
        """Method to raise exception if required env vars not set."""
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.getenv(var)]
        if missing_vars:
            message = f"Missing required environment variables: {', '.join(missing_vars)}"
            raise OSError(message)

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        """Provide dot notation access to configurations and env vars on this class."""
        if name in self.REQUIRED_ENV_VARS or name in self.OPTIONAL_ENV_VARS:
            return os.getenv(name)
        message = f"'{name}' not a valid configuration variable"
        raise AttributeError(message)

    def targets_directory(self) -> str:
        directory = self.TARGETS_DIRECTORY or "output"
        return directory.removesuffix("/")


def configure_logger(logger: logging.Logger, *, verbose: bool) -> str:
    # configure app logger
    if verbose:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s.%(funcName)s() line %(lineno)d: "
            "%(message)s"
        )
        logger.setLevel(logging.DEBUG)
        for handler in logging.root.handlers:
            handler.addFilter(logging.Filter("hrqb"))
    else:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s.%(funcName)s(): %(message)s"
        )
        logger.setLevel(logging.INFO)

    # configure luigi loggers
    configure_luigi_loggers(verbose)

    return (
        f"Logger '{logger.name}' configured with level="
        f"{logging.getLevelName(logger.getEffectiveLevel())}"
    )


def configure_luigi_loggers(verbose: bool) -> None:  # noqa: FBT001
    level = logging.DEBUG if verbose else logging.INFO
    logging.getLogger("luigi-interface").setLevel(level)
    logging.getLogger("luigi.scheduler").setLevel(level)


def configure_sentry() -> str:
    env = os.getenv("WORKSPACE")
    sentry_dsn = os.getenv("SENTRY_DSN")
    if sentry_dsn and sentry_dsn.lower() != "none":
        sentry_sdk.init(sentry_dsn, environment=env)
        return f"Sentry DSN found, exceptions will be sent to Sentry with env={env}"
    return "No Sentry DSN found, exceptions will not be sent to Sentry"
