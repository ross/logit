# Thin wrapper around script/* for anyone who reaches for `make` out of habit. script/* is the
# canonical interface (the "Scripts to Rule Them All" pattern:
# https://github.blog/engineering/scripts-to-rule-them-all/) -- see AGENTS.md and README.md, and
# add new commands there, not here.

.PHONY: bootstrap setup update server test lint fmt fmt-check schema audit cibuild console up down clean

bootstrap:  ## Build the dev container image.
	./script/bootstrap

setup:      ## One-time setup for a fresh checkout.
	./script/setup

update:     ## Rebuild after pulling changes.
	./script/update

server:     ## Run logit against the local test stack.
	./script/server

test:       ## cargo nextest run --workspace
	./script/test

lint:       ## cargo clippy, warnings denied
	./script/lint

fmt:        ## cargo fmt
	./script/format

fmt-check:  ## cargo fmt --check
	./script/format --check

schema:     ## Regenerate schema/logit.schema.json
	./script/schema

audit:      ## Supply-chain checks (cargo-deny, cargo-audit)
	./script/audit

cibuild:    ## Run the full CI-equivalent check sequence
	./script/cibuild

console:    ## Interactive shell in the dev container
	./script/console

up:         ## Start the InfluxDB + Grafana test stack.
	$${DOCKER:-sudo docker} compose up -d influxdb grafana

down:       ## Stop the test stack.
	$${DOCKER:-sudo docker} compose down

clean:      ## Remove build artifacts (host + container volumes).
	$${DOCKER:-sudo docker} compose down -v
