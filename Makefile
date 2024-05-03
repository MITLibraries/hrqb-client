SHELL=/bin/bash
DATETIME:=$(shell date -u +%Y%m%dT%H%M%SZ)
export LUIGI_CONFIG_PATH=hrqb/luigi.cfg

help: # preview Makefile commands
	@awk 'BEGIN { FS = ":.*#"; print "Usage:  make <target>\n\nTargets:" } \
/^[-_[:alpha:]]+:.?*#/ { printf "  %-15s%s\n", $$1, $$2 }' $(MAKEFILE_LIST)

## ---- Dependency commands ---- ##

install: # install Python dependencies
	pipenv install --dev
	pipenv run pre-commit install

update: install # update Python dependencies
	pipenv clean
	pipenv update --dev

## ---- Unit test commands ---- ##

test: # run tests and print a coverage report
	pipenv run coverage run --source=hrqb -m pytest -vv -m "not integration"
	pipenv run coverage report -m

coveralls: test # write coverage data to an LCOV report
	pipenv run coverage lcov -o ./coverage/lcov.info

test-integration:
	pipenv run pytest -vv -s -m "integration"

## ---- Code quality and safety commands ---- ##

lint: black mypy ruff safety # run linters

black: # run 'black' linter and print a preview of suggested changes
	pipenv run black --check --diff .

mypy: # run 'mypy' linter
	pipenv run mypy .

ruff: # run 'ruff' linter and print a preview of errors
	pipenv run ruff check .

safety: # check for security vulnerabilities and verify Pipfile.lock is up-to-date
	pipenv check
	pipenv verify

lint-apply: black-apply ruff-apply  # apply changes with 'black' and resolve 'fixable errors' with 'ruff'

black-apply: # apply changes with 'black'
	pipenv run black .

ruff-apply: # resolve 'fixable errors' with 'ruff'
	pipenv run ruff check --fix .

## ---- Temporary Development Commands ---- ##

docker-build: # build Docker container
	docker build --platform linux/amd64 \
	    -t hrqb-client:latest .

docker-bash: # bash shell to docker container
	docker run --entrypoint /bin/bash -it hrqb-client