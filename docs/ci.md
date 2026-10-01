# GitHub Actions builds and caches

The [CI workflow](../.github/workflows/ci.yml) runs Rust checks and tests,
security auditing, frontend checks and browser smoke tests, and npm package
validation. It runs on pull requests and on pushes to `main`; other branch
pushes do not trigger it, so a PR runs the suite once. A newer run cancels an
unfinished pull-request run for the same PR. Every `main` commit keeps its own
run, because a release tag needs a successful run on its exact commit.

`.cargo/config.toml` sets `RUST_MIN_STACK` to 16 MiB for every `cargo`
invocation, local and CI, so test threads have the same stack as the server's
runtime threads. With the default 2 MiB a deep dispatch future aborts the whole
test binary on Linux, and the tests after it never run.

`.cargo/config.toml` also sets `FORGE_TEST_FORBID_DEFAULT_DATA_DIR=1` for local
and CI Cargo invocations. The default data-directory resolver panics when this
variable is set and the current executable's parent directory is `deps`, so
unit and integration tests (including dependency crates) cannot fall back to
the developer's real `~/.forge`. Test harnesses must own a `tempfile::TempDir`
or use their existing temporary workspace and inject it with
`ForgeConfig::with_data_dir`, `ForgeRuntimeBuilder::from_config`, an explicit
config file/`ConfigOverrides::data_dir`, or `FORGE_DATA_DIR`; media, Project Agent
workspaces, workflows and logs must stay within temporary roots. The API
convenience constructors retain their temporary data root across state clones.
The executable check leaves `cargo run` and `make dev` unchanged, and explicitly
configured data roots bypass the tripwire.

The [release workflow](../.github/workflows/release.yml) does not re-run the
Rust or web test suites. Its `verify-ci` gate looks up the CI run for the
tagged commit on `main` and waits for it to finish, so a tag can be pushed
right after merging. The release fails if that run fails, is cancelled, or
does not exist (for example, a tag on a commit that never landed on `main`).
The version check, `cargo audit`, and npm pack dry run still run on the tag.
The web job builds `web/dist` once for the native archives. The GHCR image
builds from the Dockerfile, so it runs alongside the native builds rather
than after them. If a native build fails after the image is pushed, the image
is published but the GitHub release, npm, and Homebrew steps are not; rerun
the failed jobs.

## Rust dependency cache

CI uses `Swatinem/rust-cache` with `shared-key: rust`. The action
automatically includes the runner platform, Rust toolchain, build environment,
and Cargo dependency manifests in its keys; a dependency change can restore a
previous cache and rebuild the affected dependencies.

Only default-branch CI runs save this shared cache, including runs whose
checks fail. Pull-request runs restore it without uploading large
branch-scoped copies. This reduces competition for GitHub Actions cache
storage.

Native release builds retain a separate cache key for each target triple.
GitHub caches are scoped to a branch or tag: a release can restore its own
tag's cache or a compatible default-branch cache, but cannot restore a cache
saved under another release tag. The current CI does not warm these native
release-target caches on the default branch, so a new tag can still build
native dependencies from cold. A rerun of the same tag can reuse caches saved
by its successful platform jobs.

The Rust cache stores dependency build artifacts, not workspace crates or
test results. Project code still needs to compile, and the full test suite
still executes on every run.

The Rust job also runs `make types`, which executes only the api-types
`export_bindings_*` tests, then checks `web/src/types/generated/` for changed
or untracked bindings. After changing Rust API types, run `make types` locally
and commit the generated output in the same change. The sole export directory
is `web/src/types/generated/bindings/`; ESLint and Prettier exclude those
generator-owned files.

## Rust test database setup

DB-backed tests keep calling `db::run_migrations(&pool)`. Test crates enable
the `db/test-template` feature through their dev-dependencies; normal builds
do not enable it. The first eligible call in each test process replays the
embedded migrations into an isolated memory pool, serializes the result, and
closes that pool. Later fresh databases copy the cached byte image. The cache
contains no pool or runtime handles, so it works across the separate Tokio
runtimes created by unit tests.

Single-connection memory pools use sqlx 0.8's safe serialize/deserialize API.
File pools use SQLite's backup API through optional rusqlite 0.32 (the same
libsqlite3-sys version as sqlx), preserving their open file and WAL handles.
Each disk restore writes a temporary snapshot under `std::env::temp_dir()`;
its name includes the process ID and a random suffix, and it is removed when
the restore finishes, including error paths. Set `TMPDIR` to your test scratch
directory before running tests. Complete migration setup before using a pool.

Production startup, `run_migrations_from` fixture directories, databases with
applied migrations or other existing schema, persistent database settings
that differ from what `create_sqlite_pool` gives a fresh database, attached
databases, and explicitly named or multi-connection memory pools stay on
replay. `file_backed_migrations_apply_cleanly` replays the bundled directory
through `run_migrations_from`, so every migration body still runs against a
real file in each CI run. Query-only pools retain replay errors, and disk
copies check writability through sqlx before using the backup connection.
Tests that stop at a historical migration to inspect its immediate effect must
apply the remaining migrations before calling current repository row mappers.
Duplicate-version and applied-name guards remain on that path, and the
cached template itself uses that validated replay runner.
Snapshot tests compare every `sqlite_master` field, complete `_migration`
rows (including `applied_at`), and all table contents against the replayed
source. They also check pool isolation, file persistence, connection pragmas,
read-only file access, and migration error guards. Copies retain the template's
migration timestamps.

The dev profile optimizes `libsqlite3-sys`, `sqlx-core`, and `sqlx-sqlite` at
level 3; the test profile inherits these overrides. Workspace code retains
its ordinary debug settings. Cache misses rebuild these dependencies once.

For a scoped timing investigation, separate build time from test execution
and compare the same filters, machine, environment, and test-thread settings:

```bash
export CARGO_TARGET_DIR=$PWD/target FORGE_SKIP_WEB_BUILD=1
/usr/bin/time -p cargo test --locked -p db --lib migration_creates_schema_and_enforces_foreign_keys -- --nocapture
/usr/bin/time -p cargo test --locked -p services --lib task_dispatcher::tests::
```

Local macOS measurements on 2026-10-01 used the default libtest thread count.
Execution times exclude compilation; milliseconds per test below describe
suite throughput, not individual test latency.

| Sample | Before | After | Milliseconds per test, before → after |
| --- | --- | --- | --- |
| Representative schema/FK test (1 test) | 1.58 s | 0.75 s | 1,580 → 750 |
| Original DB unit tests (137 tests) | 413.05 s | 5.26 s | 3,015 → 38.4 |
| Dispatcher module (70 tests) | 424.08 s | 4.84 s | 6,058 → 69.1 |

The original DB sample used `cargo test --locked -p db --lib`; the matching
after sample added `-- --skip migration::template::tests::` to exclude the
six new validation tests. The complete after suite passed all 143 tests in
11.00 s. Cargo wall times, including any rebuild, were 42.43 → 19.21 s for
the representative test, 413.40 → 5.41 s for the matching DB sample, and
579.00 → 116.08 s for the dispatcher sample.

Temporary instrumentation around migration-body `raw_sql` calls measured
fresh memory replay at 1,724.9 ms/call before: 1,629.0 ms (94.4%) executing
migration bodies and 95.9 ms elsewhere. These are awaited wall times,
including sqlx dispatch; the remainder includes transaction and history
queries, discovery, and reconciliation. With dependency optimization,
replay averaged 807.3 ms (744.7 ms in bodies, 62.6 ms elsewhere). The first
template call took 1,174.7 ms; subsequent memory copies averaged 0.425 ms
over five calls (0.347–0.499 ms). Disk copies averaged 74.65 ms over three
calls (35.24–144.09 ms). The disk access probe rolls back its header write
to avoid an extra fsync. First-call and disk timings varied across runs;
these are the final probe's results. All timing probes were removed.

To check build cost, both profile variants cleaned only `db`,
`libsqlite3-sys`, `sqlx-core`, and `sqlx-sqlite`, then built
`cargo test --locked -p db --lib --no-run`. Unoptimized/optimized wall time
was 212.14/67.60 s, with combined user/system CPU time of 59.92/82.00 s.
The large wall-time variation is not evidence of faster compilation: the
optimized build used about 22 additional CPU seconds. Other dependencies
remained cached, apart from the new optional backup dependencies in the
first build. Cache misses pay this cost once; test execution avoids replay
for subsequent fresh databases in each process.

Each integration test binary is a separate process and pays for its own first
replay. A shared testkit crate and per-crate integration binary consolidation
are follow-ups; neither is part of the template change. Full workspace test
execution remains a CI responsibility.

## Frontend dependency cache

Both CI and the release web build use `actions/setup-node` to cache the pnpm
store, keyed by `web/pnpm-lock.yaml`. `pnpm install --frozen-lockfile` still
runs so the installed dependencies match the lockfile. The release web job
uploads the freshly built web assets once; all native builds download that
artifact.

## Container build cache

The release container job imports and exports a BuildKit registry cache at
`ghcr.io/<owner>/<repository>:buildcache`, with the repository name converted
to lowercase. `mode=max` saves intermediate build stages as well as final
layers, allowing later release tags to reuse Rust and frontend build layers.
This cache uses GHCR storage rather than the GitHub Actions cache quota.

The existing GHCR login and package-write permission also cover the cache
tag. The first build populates it; a missing cache falls back to a normal
build, and a cache-export failure does not fail publication.

## Investigating slow runs

Inspect job timings, cache hits, and Rust stage completion timestamps before
changing cache settings. A release's `verify-ci` job waits for `main` CI, so
most of a slow release is usually the CI run itself:

```bash
gh run list --repo ForgeAILab/forge --workflow ci.yml --limit 5
gh run view <run-id> --repo ForgeAILab/forge
gh run view <run-id> --repo ForgeAILab/forge --job <rust-job-id> --log
gh cache list --repo ForgeAILab/forge
```

In the Rust log, compare `cargo clippy` and the `Finished ... test profile`
timestamp with the final `cargo test: ok` timestamp. Time after the test
profile finishes is spent executing tests; build caching cannot remove it.

GitHub evicts caches when storage fills or entries expire. Default-branch
cache hits are the useful baseline for pull requests.
See [GitHub's cache scope and eviction reference](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)
and [Docker's registry cache reference](https://docs.docker.com/build/cache/backends/registry/).
