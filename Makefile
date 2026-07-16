# Convenience wrapper around this repo's per-crate build/test commands (there is
# no root Cargo workspace — see CLAUDE.md — so each crate normally needs its own
# `cd`). Run `make help` for a list of targets.

RUST_CRATES := tpt-keystone tpt-cli tpt-sdk tpt-harbor tpt-operator
TS_PACKAGES := packages/sdk-web packages/sdk-server packages/sdk-edge packages/sdk-react-native

.PHONY: help build test build-all test-all fmt clippy canvas run clean

help:
	@echo "Targets:"
	@echo "  make build        - cargo build for tpt-keystone only (the core engine)"
	@echo "  make run          - cargo run for tpt-keystone (starts a local node)"
	@echo "  make test         - cargo test for tpt-keystone only"
	@echo "  make build-all    - cargo build for every Rust crate"
	@echo "  make test-all     - cargo test for every Rust crate + npm test for every TS package"
	@echo "  make canvas       - build tpt-canvas for wasm32-unknown-unknown"
	@echo "  make fmt          - cargo fmt across every Rust crate"
	@echo "  make clippy       - cargo clippy across every Rust crate"
	@echo "  make clean        - cargo clean across every Rust crate"

build:
	cd tpt-keystone && cargo build

run:
	cd tpt-keystone && cargo run

test:
	cd tpt-keystone && cargo test

build-all:
	@for c in $(RUST_CRATES); do \
		echo "== cargo build ($$c) =="; \
		(cd $$c && cargo build) || exit 1; \
	done

test-all:
	@for c in $(RUST_CRATES); do \
		echo "== cargo test ($$c) =="; \
		(cd $$c && cargo test) || exit 1; \
	done
	@for p in $(TS_PACKAGES); do \
		echo "== npm test ($$p) =="; \
		(cd $$p && npm test) || exit 1; \
	done

canvas:
	cd tpt-canvas && cargo build --target wasm32-unknown-unknown

fmt:
	@for c in $(RUST_CRATES) tpt-canvas; do (cd $$c && cargo fmt); done

clippy:
	@for c in $(RUST_CRATES) tpt-canvas; do (cd $$c && cargo clippy) || exit 1; done

clean:
	@for c in $(RUST_CRATES) tpt-canvas; do (cd $$c && cargo clean); done
