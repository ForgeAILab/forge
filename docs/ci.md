# GitHub Actions builds and caches

The [CI workflow](../.github/workflows/ci.yml) runs Rust checks and tests,
security auditing, frontend checks and browser smoke tests, and npm package
validation. A newer run cancels an unfinished CI run for the same event and
ref. Push and pull-request runs have separate concurrency groups.

The [release workflow](../.github/workflows/release.yml) independently runs
the verification gates before building native archives and publishing the
container and packages.

## Rust dependency cache

CI and release Rust verification use `Swatinem/rust-cache` with the same
`shared-key: rust`. This retains the existing CI cache key while allowing
release verification to restore the default branch's cache. The action
automatically includes the runner platform, Rust toolchain, build environment,
and Cargo dependency manifests in its keys; a dependency change can restore a
previous cache and rebuild the affected dependencies.

Only default-branch CI runs save this shared cache, including runs whose
checks fail. Branch and pull-request runs restore it without uploading large
branch-scoped copies. Release verification also restores without saving a
tag-scoped copy. This reduces competition for GitHub Actions cache storage.

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

## Frontend dependency cache

Both CI and release verification use `actions/setup-node` to cache the pnpm
store, keyed by `web/pnpm-lock.yaml`. `pnpm install --frozen-lockfile` still
runs so the installed dependencies match the lockfile. Release verification
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
changing cache settings:

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
cache hits are the useful baseline for pull requests and release verification.
See [GitHub's cache scope and eviction reference](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching)
and [Docker's registry cache reference](https://docs.docker.com/build/cache/backends/registry/).
