.DEFAULT_GOAL := help

.PHONY: help build test test-race lint clean run docs-serve docs-build complexity openapi-sync

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

VERSION ?= dev

build: ## Build the dflockd binary
	go build -ldflags "-X main.version=$(VERSION)" -o dflockd ./cmd/dflockd

test: ## Run tests
	go test ./... -v

test-race: ## Run tests with -race (parallelism capped to avoid raft timer flakes under whole-tree load)
	go test -race -p=2 -count=1 -timeout=240s ./...

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

complexity: ## Report per-function lines + cyclomatic complexity (production only)
	go run ./tools/complexity -prod -top 30

complexity-strict: ## Fail if any production function exceeds the targets
	go run ./tools/complexity -prod -max-lines 5 -max-cyclo 3 -summary

openapi-sync: ## Mirror the embedded OpenAPI spec into docs/
	cp internal/httpapi/openapi.json docs/openapi.json
