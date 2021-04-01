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


#
# Clean Targets
#

clean:
	rm -fr bin
	rm -fr data
	rm -fr temp
	rm -fr tmp
	rm -fr *.egg-info
