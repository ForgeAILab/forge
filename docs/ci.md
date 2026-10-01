# GitHub Actions builds and caches

The [CI workflow](../.github/workflows/ci.yml) runs Rust checks and tests,
security auditing, frontend checks and browser smoke tests, and npm package
validation. It runs on pull requests and on pushes to `main`; other branch
pushes do not trigger it, so a PR runs the suite once. A newer run cancels an
unfinished pull-request run for the same PR. Every `main` commit keeps its own
run, because a release tag needs a successful run on its exact commit.

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
