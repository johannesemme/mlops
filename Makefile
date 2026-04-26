.PHONY: install test dev

install:
	uv sync

test:
	uv run pytest tests/

dev:
	uv run dagster dev
