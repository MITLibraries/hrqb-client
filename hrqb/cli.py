import logging
from datetime import timedelta
from time import perf_counter

import click

from hrqb.config import Config, configure_logger, configure_sentry

logger = logging.getLogger(__name__)

CONFIG = Config()


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Pass to log at debug level instead of info.",
)
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:  # noqa: FBT001
    ctx.ensure_object(dict)
    ctx.obj["START_TIME"] = perf_counter()
    root_logger = logging.getLogger()
    logger.info(configure_logger(root_logger, verbose=verbose))
    logger.info(configure_sentry())
    CONFIG.check_required_env_vars()
    logger.info("Running process")


@main.command()
@click.pass_context
def ping(ctx: click.Context) -> None:
    logger.debug("pong")
    logger.info(
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )
