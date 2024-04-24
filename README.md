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
```

### Optional

```shell
DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH # used when developing on arm64 architecture + Rosetta2 environment 
```




