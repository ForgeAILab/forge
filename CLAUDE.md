# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

The user-facing entry point is [README.md](README.md). Deeper material is split across `docs/`:
- [docs/architecture.md](docs/architecture.md) — crate graph, state machine, workflow engine, DB.
- [docs/api.md](docs/api.md) — REST surface, pagination, MCP tools.
- [docs/getting-started.md](docs/getting-started.md) — install, configuration, end-to-end walkthrough.
- [docs/cli.md](docs/cli.md) — `forge-ctl` reference.

When you change behavior, update the relevant `docs/` file in the same change. Treat README as a launch landing page — keep it short and link out.

## Project status: Public Beta (pre-`1.0`)

Forge is in public beta. The local-first single-user product is shipping to real users, but we are still pre-`1.0` and pre-stable.

What that means for changes you make:

- **Breaking changes are allowed**, but they must be **visible**: update [CHANGELOG.md](CHANGELOG.md) under the `Unreleased` section with a `### Breaking` entry, and bump the workspace version when cutting a release. Silent breakage is no longer acceptable now that users exist.
- **No backward-compat shims, no `_v2` suffixes, no deprecated aliases.** When something changes, update all call sites and delete the old code. Express the break in the changelog, not in the source.
- **No feature flags for rollout safety.** Ship the new behavior directly.
- **Database migrations must preserve user data** unless the user explicitly opts in to a destructive migration (e.g. a hard schema reset for the dev `./test` data dir). Add a new numbered migration; do not edit historical ones.
- **Public surfaces** — REST endpoints, MCP tool names, `forge-ctl` flag names, event payload shapes, JSONL log schema, on-disk file layout — are the ones to be most deliberate about. Internal Rust APIs and crate boundaries can still move freely.
- When a spec/design changes, rewrite the affected code to match. Don't layer the new design on top of the old one.

If you find yourself writing `// kept for backward compatibility`, a `_v2` suffix, a re-export shim, or a deprecated alias — stop, just change the code, and note the break in the changelog.

A stable `1.0` will land once the workflow engine consolidation, multi-user story, and release artifact hardening (signing, SBOMs, Homebrew, Windows builds) are finalized. Until then, assume the public surface is still moving.

## Build & test commands

```bash
cargo build                          # Build entire workspace
cargo test                           # Full test suite (CI only)
cargo test -p db --features test-template --test main <module>::<test-filter> # Focused integration test
cargo test -p db --lib <test-filter> # Focused unit test
cargo test -p services --features test-support --test main <module>::<test-filter>
cargo test -p services-agents --features test-support --test main <module>::<test-filter>
cargo test -p api --test main happy_path::<test-name>  # Run one relevant end-to-end case
cargo run -p forge-client -- --help  # forge-ctl CLI client
cargo build -p forge-client          # Build forge-ctl binary
cargo clippy --workspace --all-targets --features db/test-template,services-agents/test-support,services/test-support -- -D warnings
cargo fmt --all
FORGE_SKIP_WEB_BUILD=1 cargo check -p forge-cli  # Skip pnpm web build during Rust-only work
```

During local agent work, run only the focused tests directly related to the files and behavior being changed. Do not run a full crate, workspace, or all-target test suite locally; full-suite coverage belongs in CI. Expand to an adjacent focused test only when the change's impact analysis identifies that path as affected.

DB, services and most API integration tests are modules under `tests/suite/`, selected
with `cargo test -p <crate> --test main <original-file-stem>::<test-name>`.
The `services`, `services-agents` and `db` integration targets need their test
feature (`--features test-support` / `--features test-template`): a named
target without it is a Cargo error, and a plain `cargo test -p services` or
`cargo test -p db` runs the unit tests only and skips them without a message.
The clippy command names the same features so that linting those targets does
not depend on another crate's dev-dependencies.
No crate may dev-depend on itself (it compiles the lib twice in series);
see [docs/ci.md](docs/ci.md#test-features-and-required-features).
Services keeps `project_provisioning_characterization` separate; services-agents
keeps `main_project_create_command` separate for process isolation.
API keeps seven process-isolated targets; see
[docs/ci.md](docs/ci.md#api-binary-split-and-isolation) for the list.
See [docs/ci.md](docs/ci.md#services-unit-split-and-integration-inventory) for
inventory comparison and the services lib filter/skip partition.

The happy-path test (`crates/api/tests/suite/happy_path.rs`) is the canonical end-to-end smoke and a forcing function for spec alignment. Run only the relevant named case locally; CI runs the complete test target. If you break it, you probably need to revisit the spec, not the test.

### Running the server

Use the Makefile targets for local dev and manual/agent testing — they all point data at `./test/` (gitignored) and load `./test/forge.yaml`, so test state and config are isolated from the default install:

```bash
make dev               # start server → ./test data dir (default for testing)
make dev-demo          # same + seed demo data (idempotent)
make dev-no-daemon     # server only, no embedded AI daemon
make frontend          # Vite dev server on :5173, proxies /api to :8080
make clean-test        # wipe ./test directory and start fresh
```

Direct invocations (when you need flags not covered by `make`):

```bash
cargo run -p forge-cli -- --data-dir ./test           # explicit local data dir
cargo run -p forge-cli -- --no-mcp                    # disable MCP endpoint
FORGE_DATA_DIR=./test cargo run -p forge-cli          # env-var equivalent
# Server always binds to 127.0.0.1:8080
```

### Frontend

```bash
cd web && pnpm install && pnpm dev    # Vite dev server on :5173, proxies /api to :8080
cd web && pnpm build                  # Production build to web/dist/
cd web && pnpm lint && pnpm typecheck && pnpm test
```

`pnpm` is the package manager (enforced by `pnpm-lock.yaml`). Don't introduce `npm` or `yarn` lockfiles.

## When working on this repo

Source files are capped at 2,000 lines for Rust and 1,000 for TS/TSX. Run `python3 scripts/check-file-sizes.py`; existing oversized files are ratcheted in `scripts/file-size-allowlist.txt`. Growth fails, and shrinking or deleting a listed file requires `python3 scripts/check-file-sizes.py --update` to lower or remove its entry. Split by responsibility: keep `foo.rs` as the Rust module root with `foo/<part>.rs` children, and move tests with the code they test into `foo/tests.rs`; use sibling modules for TS/TSX. Never use `_part2`-style files; never raise or add an allowlist entry; split instead. `--init` is for maintainers only to create the one-time baseline.

- **Read `docs/architecture.md` before changing service wiring, the workflow engine, or the task state machine.** That doc is the source of truth for the lifecycle, hook ordering, role assignment, and retry-budget rules.
- **Public-API changes touch four places**: the route handler in `crates/api/src/routes/`, the request/response type in `crates/api-types/`, the generated TS types under `web/src/types/generated/`, and `docs/api.md`. Run `make types` to regenerate bindings in `web/src/types/generated/bindings/` and commit them with the other updates in the same change.
- **State-machine changes** must keep `crates/api/tests/suite/happy_path.rs` green and the table in `docs/architecture.md#task-state-machine` accurate.
- **Migrations** after V149 are named `V{YYYYMMDDHHMM}__{name}.sql`, using the UTC time you create the file, so parallel branches never pick the same version. Add a new file; don't edit historical migrations even during beta — users have running databases. The runner refuses to start on two files with one version, or on a database that applied a different migration under a version the build uses.
- **Errors** flow `DbError` (db) → `ServiceError` (services) → `ApiError` (api). Map at the boundaries; don't leak lower-layer error types upward.
- **Concurrency**: tasks and agents use a `version` column. Updates require `WHERE version = ?` and increment on success. Version mismatch → `DbError::VersionConflict` → HTTP 409.
- **Pagination**: opaque cursors, response field is `items` (not `data`). Many repository lists use offset cursors; dedicated artifact lists use keysets. Filter visibility before pagination/counts and fetch `limit + 1` to compute `has_more`; clients must not decode cursors.

## Architecture summary (reference only)

The full version lives in [docs/architecture.md](docs/architecture.md). Quick orientation:

```
forge-cli → api / services-agents / services
api → mcp-server
api / mcp-server / forge-solo → services-agents → services → db
api / mcp-server / forge-solo → services
services-agents / services → events / agent-host / executors / workspace → git
forge-cli / forge-solo → config
api-types (shared, zero internal deps)
api / services-agents / services → pricing → db / api-types
forge-solo owns its Solo bootstrap and session services
```

- **Repository pattern** — `db` defines async traits in `repository.rs`; `SqliteDb` in `sqlite.rs` implements all of them.
- **AppState** — `forge-cli/main.rs` constructs `Arc<SqliteDb>` and `Arc<EventBus>`, builds the shared graph through `services_agents::ForgeRuntimeBuilder`, and hands it to `AppState::from_runtime_arc()`. `AppState` is `Clone` (shared services use `Arc`) and used as Axum state.
- **Agent boundary** — `services-agents` owns Agent Chat, embedded-agent, native-tool, wake-delivery, and Main/Project orchestration implementations. It depends on `services`; core never depends back. Core uses the narrow `WakeTurns`, task `ExecutionSupport`, and `AgentEventConsumerNames` contracts, wired by `ForgeRuntimeBuilder`.
- **Event bus** — `events` crate wraps `tokio::sync::broadcast`; SSE at `GET /api/v1/events` subscribes.
- **Workflow engine** — `TaskService.transition()` resolves the applicable workflow once and delegates to `crates/services/src/workflow/engine/mod.rs`. The wrapper, engine, and hooks share that definition. Default workflow lives in `default_workflow.rs`.
- **Claim auto-dispatches** — `api::routes::tasks::claim_task` spawns the executor via `tokio::spawn`. There is no separate "dispatch" endpoint.
- **Review** — workflow entry hooks run configured checks on the durable check runner and dispatch ordinary reviewers; an explicit review re-run re-enters that same entry (`TaskService::rerun_review`). There is no separate review runner.
- **MCP server** — `POST /mcp`, JSON-RPC, has its own `McpState`, does not depend on the `api` crate.
- **Workspace** — no lock files: keyed in-process locks serialize repository-cache/integration and Workspace execution operations; path validation prevents traversal escapes. The `workspace` crate owns Task-root reclamation (permission repair, exact worktree removal, delivered-branch deletion) for the server and the daemon.
- **Config precedence** — CLI flags > env vars > config file > defaults. Config lookup uses `<data dir>/forge.yaml`, with the directory chosen by CLI `--data-dir` > `FORGE_DATA_DIR` > `~/.forge`; a file's `forge.data_dir` does not relocate that lookup. Default bind `127.0.0.1:8080`.

## Database

SQLite with WAL mode. Schema in `crates/db/migrations/V001__initial_schema.sql`. Migrations are named `V{version}__{name}.sql` (sequential up to V149, UTC timestamps `VYYYYMMDDHHMM` after that) and tracked by version and name in the `_migration` table. All primary keys are app-generated UUID v4; all timestamps are app-generated RFC3339.

Connection pool sets `PRAGMA foreign_keys=ON`, `journal_mode=WAL`, `busy_timeout=30000` per connection.

Enum serialization uses `Display`/`FromStr` (in `db::models`) for SQLite TEXT columns. Row mapping is manual via `sqlx::Row::get()`, not compile-time checked macros — so a renamed column will fail at runtime, not at `cargo check`. Update both the migration and the row-mapper together.

For tests, use `create_sqlite_pool("sqlite::memory:")` for an in-memory database.

## Frontend

React + TypeScript + Vite + TanStack Query/Router. Source in `web/src/`. Uses `@` path alias → `web/src/`. API client at `web/src/api/client.ts` calls `/api/v1/*` endpoints. When you change a response shape in `api-types`, run `make types` and commit the regenerated bindings under `web/src/types/generated/bindings/` in the same change. Do not hand-format generated bindings; ESLint and Prettier exclude them.
