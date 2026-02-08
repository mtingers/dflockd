.DEFAULT_GOAL := help

.PHONY: help lint format typecheck test test-cov clean docs-serve docs-build

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

lint: ## Run linter
	uv run ruff check src/ tests/

format: ## Format code
	uv run ruff format src/ tests/

typecheck: ## Run type checker
	uv run pyright src/

test: ## Run tests
	uv run pytest

test-cov: ## Run tests with coverage
	uv run pytest --cov=dflockd --cov-report=term-missing

clean: ## Remove build artifacts and caches
	rm -rf build/ dist/ *.egg-info .mypy_cache .pytest_cache .ruff_cache htmlcov/ site/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

docs-serve: ## Serve documentation locally
	uv run --group docs mkdocs serve

docs-build: ## Build documentation site
	uv run --group docs mkdocs build --strict

build-publish: ## Build new version and publish to pypi
	uv version --bump minor
	uv build
	uv publish
