# Makefile for soothe-client (Rust)

SHELL := /bin/bash
PKG_NAME := soothe-client
PKG_VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

.DEFAULT_GOAL := help

.PHONY: help fmt fmt-check clippy test test-unit test-integration test-examples \
	check verify build clean doc publish-dry

help: ## Show help
	@echo "$(PKG_NAME)@$(PKG_VERSION)"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format with rustfmt
	cargo fmt

fmt-check: ## Check formatting
	cargo fmt --check

clippy: ## Clippy with -D warnings
	cargo clippy --all-targets -- -D warnings

test-unit: ## Unit tests (offline)
	cargo test --lib --test unit_api

test: test-unit ## Alias for unit tests

test-integration: ## Live integration tests (needs daemon)
	SOOTHE_INTEGRATION=1 cargo test --test integration -- --nocapture

test-examples: ## Run examples 01–06 against live daemon
	@for ex in 01_hello 02_stream_turn 03_text_completion 04_multi_turn 05_pool_service 06_jobs; do \
		echo "=== $$ex ==="; \
		cargo run --example $$ex || exit 1; \
	done

check: fmt-check clippy test-unit ## CI check without live daemon

verify: check build ## Full offline verify

build: ## Build release
	cargo build --release

doc: ## Build docs
	cargo doc --no-deps

clean: ## Clean target/
	cargo clean

publish-dry: ## Dry-run crates.io publish
	cargo publish --dry-run
