# HR Quickbase Client

A python CLI application for managing data in the Library HR [Quickbase](https://www.quickbase.com/) instance.

## Overview

TODO...

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
DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH # potentially useful when developing on arm64 / rosetta installations
```




