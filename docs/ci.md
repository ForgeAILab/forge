# GitHub Actions builds and caches

The [CI workflow](../.github/workflows/ci.yml) runs Rust checks and tests,
security auditing, frontend checks and browser smoke tests, and npm package
validation. It runs on pull requests and on pushes to `main`; other branch
pushes do not trigger it, so a PR runs the suite once. A newer run cancels an
unfinished pull-request run for the same PR. Every `main` commit keeps its own
run, because a release tag needs a successful run on its exact commit.

## Rust jobs

The Rust gates run as eight parallel jobs, then one job that sums them up:

| Job | Runs |
| --- | --- |
| `Rust lint` | shard coverage check, `./scripts/ci-rust.sh lint` (`cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`), the `make types` drift check, the Python tests under `scripts/` |
| `Rust test (services-unit-a)` | `cargo test -p services --lib --no-fail-fast -- --test-threads=4 task_service::` |
| `Rust test (services-unit-b)` | `cargo test -p services --lib --no-fail-fast -- --test-threads=4 --skip task_service::` |
| `Rust test (services-integration)` | `cargo test -p services --test '*' --examples --no-fail-fast -- --test-threads=4` |
| `Rust test (api-a)` | API binary selection from `scripts/ci_rust_shards.py`, with four libtest threads |
| `Rust test (api-b)` | Remaining API binaries and all non-integration targets, with four libtest threads |
| `Rust test (db)` | `cargo test -p db --all-targets --no-fail-fast` |
| `Rust test (rest)` | `cargo test --workspace --exclude services --exclude api --exclude db --all-targets --no-fail-fast` |
| `Rust` | no work of its own; succeeds only if the eight jobs above succeeded |

`Rust` is the name to use in branch protection. It also runs when a job it
needs failed, was cancelled or was skipped, and fails in each of those cases.
The matrix sets `fail-fast: false` and every shard keeps `--no-fail-fast`, so
one red run still lists every failure.

Each test job runs `./scripts/ci-rust.sh test <shard>`. Its packages, targets
and libtest filters are written down once, in `scripts/ci_rust_shards.py`.
`rest` is defined by
exclusion, so a new crate is tested there without an edit. The lint job runs
`python3 scripts/ci_rust_shards.py check`, which reads `cargo metadata` and
fails unless every workspace package is covered, every package a shard names
exists (cargo only warns about an unknown `--exclude`), and the workflow matrix
lists exactly the shards the script defines. Services lib filters must be
exact complements and its integration shard
uses `--test '*' --examples`, because `--tests` also runs lib tests. This
retains the example targets previously built by `--all-targets`; a new services
bin or benchmark fails the check until it is assigned. API integration
targets must each have exactly one explicit binary assignment; unknown, missing
or repeated targets fail the check. Its lib, bins, examples and benches belong
to one API shard. Other packages remain in exactly one shard with `--all-targets`. It prints
the package-to-shard table and compiles nothing, so it also runs locally.

Run locally, `./scripts/ci-rust.sh` without arguments still runs fmt, clippy
and `cargo test --workspace --all-targets --no-fail-fast` in one pass. Like
the shards, its test stage now explicitly uses `--test-threads=4`.

A shard resolves Cargo features for its own packages, where the single
workspace run unified them across all members. The workspace's own test
features are unaffected: `services/test-support`, `cli-adapters/test-support`
and `db/test-template` are requested by the dev-dependencies of each crate
that needs them. Third-party crates can be built with fewer features in a shard
(for example `tokio` without `test-util` outside `services`, and without `full`
in `services` and `db`). A crate must therefore declare the features of each
dependency it uses in its own `Cargo.toml`; a missing one fails that crate's
shard at compile time, as it already does for a local `cargo test -p <crate>`.

While `CI_RUST_TIMINGS` is set in the workflow, each test job first builds its
test binaries with `--no-run --timings` as a separate stage and uploads
`target/cargo-timings/` as the `cargo-timings-<shard>` artifact. This is a
temporary measurement for balancing the shards.

`.cargo/config.toml` sets `RUST_MIN_STACK` to 16 MiB for every `cargo`
invocation, local and CI, so test threads have the same stack as the server's
runtime threads. With the default 2 MiB a deep dispatch future aborts the whole
test binary on Linux, and the tests after it never run.

CI builds with `CARGO_PROFILE_DEV_DEBUG=line-tables-only` (and the same for
the test profile). The workspace still links many integration-test binaries;
with full debug info they fill the hosted runner's disk and the linker fails
with `ld terminated with signal 7 [Bus error]`. Panics and backtraces still
show file and line. The two variables are set in the job-level `env:` of the
Rust jobs in `ci.yml`, so every cargo invocation of a job, `make types`
included, builds with the same fingerprints and reuses the same artifacts.
`scripts/ci-rust.sh` falls back to the same values when they are unset, so a
local run of the script behaves as before. Other local builds are unaffected;
export the same two variables to shrink a local `target/` directory.

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

CI uses `Swatinem/rust-cache` with one key per Rust job: `shared-key:
rust-lint` for the lint job and `rust-test-<shard>` for each test job. The
jobs build different artifacts (clippy metadata, and a different set of
dependencies and features per shard); a single key would be saved by whichever
job finished first and miss for the others. The action automatically includes
the runner platform, Rust toolchain, build environment (including the
`CARGO_PROFILE_*` variables above), and Cargo dependency manifests in its
keys; a dependency change can restore a previous cache and rebuild the
affected dependencies.

Only default-branch CI runs save these caches, including runs whose checks
fail. Pull-request runs restore them without uploading large branch-scoped
copies. This reduces competition for GitHub Actions cache storage. A new or
renamed key therefore has no entry until a `main` run has saved it: pull
requests build their dependencies from scratch until then.

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

The `Rust lint` job also runs `make types`, which executes only the api-types
export tests (the ts-rs `export_bindings_*` test of every `#[ts(export)]` type
and the ignored `export_typescript` list), then checks `web/src/types/generated/` for changed
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
replay. DB integration files now share `tests/main.rs`, with their original
file stems as modules under `tests/suite/`. Services uses the same layout,
except `project_provisioning_characterization` stays in a separate binary:
its scaffold tests mutate `FORGE_SCAFFOLD_COMMAND` under their existing lock.
`main_project_create_command` also stays separate to preserve its shared
turn-log root and optional capture fixtures. Other fixtures use isolated pools,
unique temporary paths, immutable statics or atomic filename counters. The
API integration targets are consolidated and isolated as described below. Full workspace test execution remains
a CI responsibility.

## Services unit split and integration inventory

The two services unit shards share one filter constant in
`scripts/ci_rust_shards.py`: `task_service::` and its exact `--skip` twin.
All test shards use four libtest threads. The green CI run
[38219902459](https://github.com/ForgeAILab/forge/actions/runs/38219902459)
recorded 508.55 seconds for services lib tests. Inferring start times from
libtest's alphabetical queue and the timestamped completions on four workers,
then replaying the two queues separately, estimates 366.5 / 391.6 seconds
(6.8% apart). These are estimates from observed durations, including contention
and log batching, not instrumented per-test timings. Test counts alone
(408 / 1808 including ignored tests in that run) would give a misleading split.
Local validation at base `0adbb146` measured 239.92 / 277.75 seconds with
four threads (15.8% apart), with both halves passing. The build-timings
artifacts remain enabled to measure the new jobs in CI.

For focused integration work, use the original file stem as a module filter:

```bash
cargo test -p db --test main file_backed_migration::test_name
cargo test -p services --test main attention_service::test_name
cargo test -p api --test main happy_path::test_name
cargo test -p services --test project_provisioning_characterization test_name
```

To prove a consolidation preserves tests, record combined stdout and stderr
before moving files, repeat after the change, and compare the names. The
checker preserves unit names and normalizes only the old binary name into the
new module prefix; it compares multisets, rejects duplicate identities and
incomplete target listings, and includes ignored tests.

```bash
cargo test -p db --tests -- --list > db-before.log 2>&1
# After moving files:
cargo test -p db --tests -- --list > db-after.log 2>&1
python3 scripts/check_integration_test_inventory.py db-before.log db-after.log
# Repeat with -p services or -p api and their before/after logs.
```

For changes to the consolidated harness, run each integration binary in full
with `-- --test-threads=4` twice to check for order and process interference.
Keep environment-mutating targets isolated unless every reader shares the
same lock, and keep fixture include paths relative to their moved source.

## API binary split and isolation

API has eight integration binaries (down from 95): `main` contains 88 suites
under `tests/suite/`, named after their original targets. The test bodies stay
unchanged; moved suites import the root `common` module and adjust relative
fixture paths. `common/` and `fixtures/` remain in place, and paths based on
`CARGO_MANIFEST_DIR` retain their original meaning. For example:

```bash
cargo test -p api --test main happy_path::test_name
cargo test -p api --test main daemon_connect::test_name
cargo test -p api --test model_workflow test_name
```

| Separate binary | Why it stays in its own process |
| --- | --- |
| `model_workflow` | Dominant model-sequence workload, task-local run caps and optional global tracing initialization |
| `daemon_workspace_roundtrip` | Heavy git/daemon ownership, transfer and teardown roundtrips |
| `daemon_plan_transport` | Installs a global tracing logger and exercises real owner/retry workers |
| `environment_provisioning` | Clone/probe/retry workers and delayed fixture gates |
| `environment_surface_lists` | Installs a global SQL tracing subscriber and measures exact query counts |
| `integration_queue_task_summary` | Installs a global SQL tracing subscriber and measures exact query counts |
| `task_list_read_path` | Its ignored benchmark installs a global SQL tracing subscriber |

`daemon_connect` is safe in `main`: every server binds `127.0.0.1:0`, its
pool-close gate is owned by the fixture, and it mutates no process environment,
working directory or tracing subscriber. The other socket targets also use
port zero. Neither the API suites nor their shared helpers mutate process env
or cwd. `'static` lifetime bounds are not global state; the function-local
`OnceLock` timestamps in `project_charters` and `product_genesis` remain
independent, immutable and module-scoped. The shared DB migration template is
immutable after initialization and copies into private per-test databases.
Fixtures own unique temporary roots; literal remote paths are protocol inputs,
not shared local directories.

The latest green CI run used for the split was
[38227423555](https://github.com/ForgeAILab/forge/actions/runs/38227423555),
whose `Rust test (api)` log reports 304.87 seconds for `model_workflow`,
44.53 for `daemon_workspace_roundtrip`, 14.43 for provisioning, 7.73 for plan
transport, and 302.96 for the 88 targets now in `main`. API lib took 3.16
seconds; all API targets totalled 689.54 seconds. These are measured per-binary
libtest durations with four threads, not per-test estimates. Target ownership
and the final measured balance are maintained in `scripts/ci_rust_shards.py`.
Local validation at base `4d7d91c9` measured 262.28 / 228.86 seconds for
`api-a` / `api-b` using each binary's mean successful duration (13.6% apart).
The shared `main` binary passed 398 tests twice, in 44.50 / 74.56 seconds.
The model and plan binaries each passed once and failed once in the required
two runs, so their estimates use one successful observation each; these
numbers do not certify the repeated-run gate. Both targets already have
separate processes and retain their original source. The full before/after
inventory is equal: 658 identities, including 99 unit and 559 integration
cases. The actual two-shard listings also cover that inventory once.
Keep the heavy model target indivisible: if it exceeds the rest after
consolidation, that is the lower bound on the two-shard execution time.

Record API inventories with `cargo test -p api --tests -- --list` before and
after moving files, and compare them with
`scripts/check_integration_test_inventory.py`. Run each whole API binary
with `-- --test-threads=4` twice after changing the grouping; isolate any
interfering suite instead of retrying a flaky shared process. Ignored tests
remain in the inventory and keep their existing ignore status.

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

CI wall time is the slowest Rust job, not their sum. In a test job's log,
compare the `Finished ... test profile` timestamp with the final
`cargo test: ok` timestamp. Time after the test profile finishes is spent
executing tests; build caching cannot remove it.

GitHub evicts caches when storage fills or entries expire. Default-branch
cache hits are the useful baseline for pull requests.
See [GitHub's cache scope and eviction reference](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)
and [Docker's registry cache reference](https://docs.docker.com/build/cache/backends/registry/).
