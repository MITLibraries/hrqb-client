import logging
from datetime import timedelta
from time import perf_counter

import click

from hrqb.base.task import HRQBPipelineTask
from hrqb.config import Config, configure_logger, configure_sentry
from hrqb.utils import click_argument_to_dict
from hrqb.utils.data_warehouse import DWClient
from hrqb.utils.luigi import run_pipeline, run_task
from hrqb.utils.quickbase import QBClient

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


@main.command()
@click.pass_context
def test_connections(ctx: click.Context) -> None:
    """Test connectivity with Data Warehouse and Quickbase."""
    all_success = True
    for name, client in [("Data Warehouse", DWClient), ("Quickbase", QBClient)]:
        try:
            client().test_connection()
            message = f"{name} connection successful"
            logger.debug(message)
        except Exception as exc:  # noqa: BLE001
            all_success = False
            message = f"{name} connection failed: {exc}"
            logger.error(message)  # noqa: TRY400

    message = "All connections OK" if all_success else "One or more connections failed"
    logger.info(message)

    logger.info(
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )


@main.group()
@click.option(
    "-p",
    "--pipeline",
    type=str,
    required=True,
    help="Pipeline Task class name to be imported from configured pipeline module, "
    "e.g. 'MyPipeline'",
)
@click.option(
    "-pm",
    "--pipeline-module",
    type=str,
    required=False,
    help="Module where Pipeline Task class is defined. Default: 'hrqb.tasks.pipelines'.",
    default="hrqb.tasks.pipelines",
)
@click.option(
    "-pp",
    "--pipeline-parameters",
    callback=click_argument_to_dict,
    help="Comma separated list of luigi Parameters to pass to HRQBPipelineTask, "
    "e.g. 'Param1=foo,Param2=bar'.",
)
@click.option(
    "-t",
    "--task",
    "target_task",
    type=str,
    required=False,
    help="Select a target task for pipeline sub-commands (e.g. remove-data, run, etc.)",
)
@click.pass_context
def pipeline(
    ctx: click.Context,
    pipeline: str,
    pipeline_module: str,
    pipeline_parameters: dict,
    target_task: str,
) -> None:
    # load pipeline task
    pipeline_task = HRQBPipelineTask.init_task_from_class_path(
        pipeline,
        task_class_module=pipeline_module,
        pipeline_parameters=pipeline_parameters,
    )
    ctx.obj["PIPELINE_TASK"] = pipeline_task
    message = f"Successfully loaded pipeline: '{pipeline_module}.{pipeline}'"
    logger.debug(message)

    # load target pipeline task if present
    pipeline_target_task = None
    if target_task:
        pipeline_target_task = pipeline_task.get_task(target_task)
        if not pipeline_target_task:
            message = f"Could not find target task: {target_task}"
            logger.error(message)
            ctx.exit(0)
        message = f"Successfully loaded target task: {pipeline_target_task}"
        logger.info(message)
    ctx.obj["PIPELINE_TARGET_TASK"] = pipeline_target_task


@pipeline.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Get status of a pipeline's tasks."""
    pipeline_task = ctx.obj["PIPELINE_TASK"]
    logger.info(pipeline_task.pipeline_as_ascii())


@pipeline.command()
@click.pass_context
def remove_data(ctx: click.Context) -> None:
    """Remove target data from pipeline tasks.

    If argument --task is passed to parent 'pipeline' command, only this task will have
    its target data removed.
    """
    pipeline_task = ctx.obj["PIPELINE_TASK"]
    pipeline_target_task = ctx.obj["PIPELINE_TARGET_TASK"]

    if pipeline_target_task:
        pipeline_target_task.target.remove()
        message = f"Target {pipeline_target_task.target} successfully removed"
        logger.debug(message)
    else:
        pipeline_task.remove_pipeline_targets()

    logger.info("Successfully removed target data(s).")


@pipeline.command()
@click.option(
    "--cleanup",
    is_flag=True,
    help="Remove target data for all tasks in pipeline after run.",
)
@click.pass_context
def run(
    ctx: click.Context,
    cleanup: bool,  # noqa: FBT001
) -> None:
    """Run a pipeline.

    If argument --task is passed to parent 'pipeline' command, only this task, and the
    tasks it requires, will run.
    """
    pipeline_task = ctx.obj["PIPELINE_TASK"]
    pipeline_target_task = ctx.obj["PIPELINE_TARGET_TASK"]

    if pipeline_target_task:
        run_results = run_task(pipeline_target_task)
    else:
        run_results = run_pipeline(pipeline_task)

    message = f"Pipeline run result: {run_results.status.name}"
    logger.info(message)
    logger.info(pipeline_task.pipeline_as_ascii())

    if cleanup:
        ctx.invoke(remove_data)
