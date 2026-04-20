.DEFAULT_GOAL := help

.PHONY: help build test lint clean run docs-serve docs-build openapi-sync

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

VERSION ?= dev

build: ## Build the dflockd binary
	go build -ldflags "-X main.version=$(VERSION)" -o dflockd ./cmd/dflockd

test: ## Run tests
	go test ./... -v

lint: ## Run linter
	go vet ./...

clean: ## Remove build artifacts
	rm -f dflockd

run: build ## Build and run the server
	./dflockd

docs-serve: ## Serve documentation locally
	uvx --with mkdocs-material mkdocs serve

docs-build: ## Build documentation site
	uvx --with mkdocs-material mkdocs build --strict

openapi-sync: ## Mirror the embedded OpenAPI spec into docs/
	cp internal/httpapi/openapi.json docs/openapi.json
