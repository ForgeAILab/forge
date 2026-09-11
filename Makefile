.PHONY: dev dev-demo dev-no-daemon frontend test build install-local install-ctl ci ci-rust ci-web clean-test

# Start the server using ./test as the data directory (safe for manual/agent testing)
dev:
	cargo run -p forge-cli -- --data-dir ./test

# Same as dev, but seeds demo data on first run
dev-demo:
	cargo run -p forge-cli -- --data-dir ./test --demo

# Server only, no embedded AI daemon
dev-no-daemon:
	cargo run -p forge-cli -- --data-dir ./test --no-embedded-daemon

# Vite dev server (proxies /api to the Forge backend bind)
frontend:
	cd web && pnpm run dev

# Run all Rust tests
test:
	FORGE_SKIP_WEB_BUILD=1 cargo test --workspace --all-targets

# Build everything
build:
	cargo build

# Install this checkout's binaries into ~/.cargo/bin
install-local:
	cargo install --path crates/forge-cli --locked
	cargo install --path crates/forge-client --locked
	cargo install --path crates/forge-solo --locked

# Install only the forge-ctl CLI client (no web build)
install-ctl:
	cargo install --path crates/forge-client --locked

# Run the same checks Forge review CI uses
ci:
	./scripts/ci-forge-review.sh

ci-rust:
	./scripts/ci-rust.sh

ci-web:
	./scripts/ci-web.sh

# Wipe local test data (./test directory)
clean-test:
	rm -rf ./test
