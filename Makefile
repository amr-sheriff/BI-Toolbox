.PHONY: all reference_docs_clean help reference_docs_build

## help: Show this help info.
help: Makefile
	@printf "\n\033[1mUsage: make <TARGETS> ...\033[0m\n\n\033[1mTargets:\033[0m\n\n"
	@sed -n 's/^## //p' $< | awk -F':' '{printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' | sort | sed -e 's/^/  /'

## all: Default target, shows help.
all: help

######################
# DOCUMENTATION
######################

## reference_docs_clean: Clean the Reference documentation build artifacts.
reference_docs_clean:
	find ./docs/bi_toolbox -name '*.rst' -delete
	git clean -fdX ./docs
	rm docs/index.md

## reference_docs_build: Build the Reference documentation.
reference_docs_build:
	poetry run python3 docs/create_rst.py
	cd docs && poetry run make html
	poetry run python docs/scripts/custom_formatter.py docs/_build/html/