# Override e.g. `make DOCKER=docker build` if you're in the `docker` group, or
# `make DOCKER=podman build` to use rootless Podman -- both are drop-in compatible with the
# plain Dockerfile/compose.yaml in this repo.
DOCKER   ?= sudo docker
COMPOSE  ?= $(DOCKER) compose
RUN      := $(COMPOSE) run --rm dev

.PHONY: build-image up down shell build test lint fmt fmt-check bench schema clean

build-image: ## Build the dev container image.
	$(COMPOSE) build dev

up: ## Start the InfluxDB + Grafana test stack.
	$(COMPOSE) up -d influxdb grafana

down: ## Stop the test stack.
	$(COMPOSE) down

shell: build-image ## Drop into an interactive shell in the dev container.
	$(COMPOSE) run --rm dev bash

build: build-image ## cargo build --workspace
	$(RUN) cargo build --workspace

test: build-image ## cargo nextest run --workspace
	$(RUN) cargo nextest run --workspace

lint: build-image ## cargo clippy, deny warnings
	$(RUN) cargo clippy --workspace --all-targets -- -D warnings

fmt: build-image ## cargo fmt
	$(RUN) cargo fmt --all

fmt-check: build-image ## cargo fmt --check
	$(RUN) cargo fmt --all -- --check

bench: build-image ## cargo bench --workspace
	$(RUN) cargo bench --workspace

schema: build-image ## Regenerate schema/logit.schema.json from the config types.
	$(RUN) cargo run -p logit-cli -- schema > schema/logit.schema.json

clean: ## Remove build artifacts (host + container volumes).
	$(COMPOSE) down -v
