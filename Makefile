py=python3
pip=pip3

.PHONY: help
help:  ## Print the help documentation
	@grep -E '^[\/a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

ifeq ($(USE_AWS_VAULT),true)
  AWS_VAULT_PREFIX:=aws-vault exec $(AWS_PROFILE) --region $(AWS_REGION) --
endif

#
# Commands
#

.PHONY: export
export: ## Export the data from CSV to Parquet
	$(AWS_VAULT_PREFIX) $(py) ./cmd/export.py

#
# Python
#

.PHONY: venv
venv: ## Create a virtual environment
	$(py) -m venv .venv
	source .venv/bin/activate

.PHONY: requirements
requirements:  ## Install Python requirements
	$(pip) install -r requirements.txt

.PHONY: editable
editable:  ## Install this repo as editable
	$(pip) install -e .

.PHONY: package
package:  ## Create the python package
	$(py) setup.py build sdist check

#
# Docker
#

.PHONY: docker_build
docker_build: package  ## Build the docker container
	docker build -f Dockerfile . -t s3-access-logs:latest

.PHONY: docker_up
docker_up: ## Run docker-compose up
	$(AWS_VAULT_PREFIX) docker-compose -f docker-compose.yml up

.PHONY: docker_push
docker_push: ## Push docker image
	$(AWS_VAULT_PREFIX) ./scripts/push_container.sh

#
# Clean Targets
#

clean:
	rm -fr build
	rm -fr dist
	rm -fr tmp
	rm -fr *.egg-info
	rm -fr MANIFEST
	rm -fr s3access/__pycache__/
