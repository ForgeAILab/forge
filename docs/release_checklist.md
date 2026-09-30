# Forge Public Beta Release Checklist

Use this checklist for public beta releases.

## Local Verification

- [ ] Run focused tests for the changed behavior; leave full Rust, web, browser, and security suites to CI.
- [ ] Check formatting and release-version consistency.
- [ ] Run a repository history secret scan before making release artifacts public.

## GitHub Repository Settings

- [ ] Keep `main` protected.
- [ ] Require the CI, Security Audit, CodeQL, and Scorecard checks before merge.
- [ ] Require at least one approving review.
- [ ] Require CODEOWNERS review for protected paths.
- [ ] Keep secret scanning and push protection enabled.
- [ ] Keep private vulnerability reporting enabled.

## Release Steps

- [ ] Update `CHANGELOG.md`.
- [ ] Confirm the workspace version in `Cargo.toml`, its package entries in `Cargo.lock`, `web/package.json`, and `npx-cli/package.json` match. Every workspace crate, `forge-client` included, inherits `version.workspace`.
- [ ] Merge the reviewed release PR.
- [ ] Tag the merge commit on `main` with `vX.Y.Z`. You do not need to wait for `main` CI first: the release workflow's `verify-ci` gate waits for the CI run on that exact commit and fails the release if it does not pass. The tag does not re-run the test suites; see [CI and release behavior](ci.md).
- [ ] Confirm `.github/workflows/release.yml` passes its version, CI, security-audit, npm-package, web-build, and platform-build gates.
- [ ] Check the container registry cache import/export in the release logs. A first container cache import or native build for a new tag may be cold.
- [ ] Wait for the release workflow to publish native archives, `SHA256SUMS`, the GHCR image, npm bootstrapper, and Homebrew update request.
- [ ] Download one archive, verify its checksum and the presence of `forge`, `forge-ctl`, `forge-solo`, and `web/dist/index.html`, install it, and smoke-test `forge --help` plus browser navigation outside the repo checkout using an isolated data directory.
- [ ] Confirm the published Docker image contains `/usr/local/share/forge/web/dist/index.html`.
- [ ] Publish release notes that call the release a public beta/developer preview.

## Post-Release

- [ ] Watch install failures, release downloads, Docker pulls, and issue response time.
- [ ] Move unresolved release blockers into the next milestone.
