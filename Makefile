.DEFAULT_GOAL := help

.PHONY: help sync lint test
help:
	@printf '%s\n' 'make sync  Install locked development dependencies' \
		'make lint  Check Python lint and formatting' \
		'make test  Run the current pytest suite'

sync:
	uv sync --locked

lint:
	uv run --locked ruff check .
	uv run --locked ruff format --check .

test:
	uv run --locked pytest
