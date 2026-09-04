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
- [ ] Confirm the workspace version in `Cargo.toml`, its package entries in `Cargo.lock`, and `web/package.json` match. `forge-client` retains its independent version; the npm bootstrapper receives the release tag version in the release workflow.
- [ ] Merge the reviewed release PR and confirm the full CI, security, and code-scanning checks pass on the exact release commit before tagging.
- [ ] Tag the release with `vX.Y.Z`.
- [ ] Wait for `.github/workflows/release.yml` to publish artifacts and `SHA256SUMS`.
- [ ] Download one archive, verify its checksum and the presence of `forge`, `forge-ctl`, and `web/dist/index.html`, install it, and smoke-test `forge --help` plus browser navigation outside the repo checkout using an isolated data directory.
- [ ] Confirm the published Docker image contains `/usr/local/share/forge/web/dist/index.html`.
- [ ] Publish release notes that call the release a public beta/developer preview.

## Post-Release

- [ ] Watch install failures, release downloads, Docker pulls, and issue response time.
- [ ] Move unresolved release blockers into the next milestone.
