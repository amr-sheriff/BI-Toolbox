

# reference_docs_clean: Clean the Reference documentation build artifacts.
reference_docs_clean:
	find ./docs/bi_toolbox -name '*.rst' -delete
	git clean -fdX ./docs
	rm docs/index.md

# api_docs_build: Build the Reference documentation.
reference_docs_build:
	poetry run python3 docs/create_rst.py
	cd docs && poetry run make html
	poetry run python docs/scripts/custom_formatter.py docs/_build/html/