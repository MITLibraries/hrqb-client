# HR Quickbase (HRQB) Client

A python CLI application for managing data in the Library HR [Quickbase](https://www.quickbase.com/) instance.

## Overview

Library HR uses [Quickbase](https://www.quickbase.com/) to manage and report on employee data.  This application is responsible for 
extracting data from the Data Warehouse, extracting data from data managed by Library HR, transforming this to a data model pre-established
in Quickbase, and loading that data.  This CLI application serves as a simple pipeline to perform this work.

This pipeline will be a Python CLI application, with commands applicable to the data loads and management tasks required, that run on a 
schedule as an ECS container in AWS via EventBridge rules.

See additional diagrams and documentation in the [docs](docs) folder:
- [Data Sources](docs/data_sources.md)
- [Pipelines](docs/pipelines.md)

## Development

- To preview a list of available Makefile commands: `make help`
- To install with dev dependencies: `make install`
  - _NOTE: if developing on an `arm64` machine (e.g. Apple Silicon M-chips), please see doc [installation for arm64 machines](docs/arm64_installation.md)_
- To update dependencies: `make update`
- To run unit tests: `make test`
- To lint the repo: `make lint`
- To run the app: `pipenv run hrqb --help`

## Environment variables

### Required

```shell
SENTRY_DSN=# If set to a valid Sentry DSN, enables Sentry exception monitoring. This is not needed for local development.
WORKSPACE=# Set to `dev` for local development, this will be set to `stage` and `prod` in those environments by Terraform.
LUIGI_CONFIG_PATH=hrqb/luigi.cfg # this env var must be set, pointing to config file in hrqb folder
QUICKBASE_API_TOKEN=# Quickbase API token
QUICKBASE_APP_ID=# Quickbase App ID
DATA_WAREHOUSE_CONNECTION_STRING=# Data Warehouse SQLAlchemy connection string, e.g. oracle+oracledb://user1:pass1@example.org:1521/ABCDE
```

### Optional

```shell
DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH # used when developing on arm64 architecture + Rosetta2 environment
TARGETS_DIRECTORY=#Location to store Task Targets, overriding application default of "output" 
```

## CLI Commands

### `ping`
```text
Usage: ping [OPTIONS]

  Debug command to test application initializes okay.

Options:
  --help  Show this message and exit.
```
<br>


### `pipeline` [Command Group]

```text
Usage: -c pipeline [OPTIONS] COMMAND [ARGS]...

Options:
  -p, --pipeline TEXT          Pipeline Task class name imported from
                               configured pipeline module, e.g. 'MyPipeline'
                               [required]
  -pm, --pipeline-module TEXT  Module where Pipeline Task class is defined.
                               Default: 'hrqb.tasks.pipelines'.
  --pipeline-parameters TEXT   Comma separated list luigi Parameters to pass
                               to HRQBPipelineTask, e.g.
                               'Param1=foo,Param2=bar'.
  -h, --help                   Show this message and exit.

Commands:
  remove-data
  run
  status
```
<br>


### `pipeline status`
```text
Usage: status [OPTIONS]

  Return the status of a Pipeline.

Options:
  --help  Show this message and exit.
```
<br>


### `pipeline remove-data`
```text
Usage: remove-data [OPTIONS]

  Remove all Task output data (Targets) for a pipeline.

Options:
  --help  Show this message and exit.
```
<br>


### `pipeline run`
```text
Usage: run [OPTIONS]

  Run a Pipeline.

Options:
  --remove-data  Pass to automatically removed Task artifacts after run.
  --help         Show this message and exit.
```

