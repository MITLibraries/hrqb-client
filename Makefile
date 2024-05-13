### This is the Terraform-generated header for hrqb-client-dev. If  ###
###   this is a Lambda repo, uncomment the FUNCTION line below  ###
###   and review the other commented lines in the document.     ###
ECR_NAME_DEV:=hrqb-client-dev
ECR_URL_DEV:=222053980223.dkr.ecr.us-east-1.amazonaws.com/hrqb-client-dev
# FUNCTION_DEV:=
### End of Terraform-generated header                            ###

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

### Terraform-generated Developer Deploy Commands for Dev environment ###
dist-dev: ## Build docker container (intended for developer-based manual build)
	docker build --platform linux/amd64 \
	    -t $(ECR_URL_DEV):latest \
		-t $(ECR_URL_DEV):`git describe --always` \
		-t $(ECR_NAME_DEV):latest .

publish-dev: dist-dev ## Build, tag and push (intended for developer-based manual publish)
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_URL_DEV)
	docker push $(ECR_URL_DEV):latest
	docker push $(ECR_URL_DEV):`git describe --always`


### Terraform-generated manual shortcuts for deploying to Stage. This requires  ###
###   that ECR_NAME_STAGE, ECR_URL_STAGE, and FUNCTION_STAGE environment        ###
###   variables are set locally by the developer and that the developer has     ###
###   authenticated to the correct AWS Account. The values for the environment  ###
###   variables can be found in the stage_build.yml caller workflow.            ###
dist-stage: ## Only use in an emergency
	docker build --platform linux/amd64 \
	    -t $(ECR_URL_STAGE):latest \
		-t $(ECR_URL_STAGE):`git describe --always` \
		-t $(ECR_NAME_STAGE):latest .

publish-stage: ## Only use in an emergency
	docker login -u AWS -p $$(aws ecr get-login-password --region us-east-1) $(ECR_URL_STAGE)
	docker push $(ECR_URL_STAGE):latest
	docker push $(ECR_URL_STAGE):`git describe --always`


## ---- Temporary Development Commands ---- ##

docker-build: # build Docker container
	docker build --platform linux/amd64 \
	    -t hrqb-client:latest .

docker-bash: # bash shell to docker container
	docker run --entrypoint /bin/bash -it hrqb-client