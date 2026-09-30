# Contributing to Forge

Thanks for your interest in contributing to Forge! This document covers how to get started.

## Prerequisites

- **Rust** (stable, latest)
- **Node.js** 24+
- **pnpm** (via corepack: `corepack enable`)
- **Git** 2.20+

## Local Development

```bash
# Clone and build
git clone https://github.com/ForgeAILab/forge.git
cd forge
cargo build

# Start the server (uses ./test as data dir, safe for dev)
make dev

# In another terminal, start the frontend
make frontend
# Open http://localhost:5173
```

## Running Tests

```bash
# All Rust tests
cargo test

# Specific crate
cargo test -p db
cargo test -p api --test happy_path

# Frontend
cd web
pnpm install
pnpm lint
pnpm typecheck
pnpm test
pnpm exec playwright install --with-deps chromium
pnpm run e2e
```

## Continuous Integration

See [GitHub Actions builds and caches](docs/ci.md) for CI and release cache
behavior, cache scope limitations, and commands to investigate slow runs.

## Code Standards

- **No unsafe code** — `#![forbid(unsafe_code)]` is enforced workspace-wide
- **Clippy clean** — `cargo clippy -- -D warnings` must pass
- **Formatted** — run `cargo fmt` before committing
- **Frontend** — `pnpm lint` with zero warnings

## Pull Request Process

1. Fork the repo and create a branch from `main`
2. Make your changes with tests where appropriate
3. Ensure CI passes: `./scripts/ci-rust.sh`
4. For frontend changes: `./scripts/ci-web.sh` and the Playwright smoke test
5. Open a PR with a clear description of what changed and why

## Architecture Overview

See [README.md](README.md) for the crate dependency graph. Key conventions:

- **Repository trait pattern** in the `db` crate — async traits implemented on `SqliteDb`
- **Error propagation** — `DbError` → `ServiceError` → `ApiError`
- **Optimistic concurrency** — tasks/agents use a `version` column
- **Event-driven** — services publish `ForgeEvent` on state changes

## Reporting Issues

- **Bugs**: Use the bug report issue template
- **Features**: Use the feature request issue template
- **Security**: See [SECURITY.md](.github/SECURITY.md)

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
