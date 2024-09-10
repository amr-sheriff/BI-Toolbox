


reference_docs_clean:
	find ./docs/api_reference -name '*_api_reference.rst' -delete
	git clean -fdX ./docs/api_reference
	rm docs/api_reference/index.md

