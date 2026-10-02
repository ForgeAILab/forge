---
created_at: 2026-10-02T05:22:12Z
updated_at: 2026-10-02T06:49:52Z
completed_at:
---

## 1. Readiness record and placement filter
- [x] 1.1 Migration (timestamp version): `project_machine_readiness` (project, owner kind, daemon, runtime, status, checks digest, failing checks JSON, scope covered, checked/next-check times, version); carry over each environment-paused Project as a `not_ready` row
- [x] 1.2 `db`: repository trait + SQLite implementation with optimistic `version`; digest helper over `environment.env` and `environment.checks`
- [x] 1.3 `placement::selection`: `EnvironmentNotReady`, `EnvironmentProbePending` filter codes (`EnvironmentUnverified` is deferred to 3.5 for this build step); the candidate carries its machine's readiness; rejections carry failing check names; pure-function tests for every new scenario
- [x] 1.4 Probe job: single-flight per (Project, machine), outside admission; server machine runs checks in the primary or managed checkout without writing; versioned result write; wake dispatch on completion
- [x] 1.5 Dispatcher: `environment_probe_pending` is a transient refusal (Task stays queued, no version churn after the first deferral); Project PATCH that changes the digest marks rows `unknown` and starts probes
- [ ] 1.6 Agent response `runnable_on` (admin: identities; others: count) from the placement executor facts

## 2. Per-machine failure, pause as last resort, re-checks
- [x] 2.1 `task_service/execution/environment.rs` failure branch: mark the machine `not_ready`; pause the Project only when no machine remains eligible; record the machine in `environment_pause`
- [x] 2.2 Task-scoped Attention + durable wait for a placed Task on a `not_ready` machine; re-dispatch when the machine is `ready`
- [x] 2.3 `environment_pause_sync` → per-machine re-check of recorded failing checks on the failing machine; compare-and-clear the Project pause; unreachable machines keep their row
- [ ] 2.4 `POST /projects/{id}/environment/recheck`: optional machine, results grouped by machine (**breaking** response shape); Project response `environment_readiness`; `api-types`, generated TS, `docs/api.md`
- [ ] 2.5 `forge-ctl project env-status` and `project env-recheck --machine`; `docs/cli.md`
- [ ] 2.6 Web: readiness table with per-machine "Check now" in Project environment settings; Task placement panel shows environment rejections and "checking machine X"; Agent page shows `runnable_on`, the pin, and clear-pin
- [ ] 2.7 `happy_path` named case for a single machine: check fails → Project paused → re-check passes → Task re-dispatches (behaviour unchanged)

## 3. Check scope, daemon probe, provisioning
- [ ] 3.1 `EnvironmentCheck.scope` (`workspace` default, `machine`), validation, settings UI and `forge-ctl`
- [ ] 3.2 Daemon transport: `machine.probe` + capability fact `machine_probe.v1`; run-policy purposes `environment_probe` and `repo_provision`; handshake facts; `forge-daemon` implementation with scratch directory lifecycle and timeouts
- [ ] 3.3 Probe job uses `machine.probe` for daemon machines (with a location: all checks; without: `machine` checks)
- [ ] 3.4 Daemon transport: `repo_location.provision` + `repo_provision.v1`; idempotent clone under `workspace_root/repos/<repo id>`; no partial directory on failure
- [ ] 3.5 Provisioning candidates in admission (only when no ready-location candidate passes); provisioning job single-flight per (repository, runtime): machine checks → provision → verify → full checks → wake dispatch; `settings.placement.provision` (`when_verified` | `never`)
- [ ] 3.6 Two-machine integration test with the daemon test harness: executor only on the daemon, machine checks pass → clone, verify, place; machine check fails → no clone, `placement_unavailable` with the check name; no machine checks → `environment_unverified`

## 4. Docs and release notes
- [ ] 4.1 `docs/architecture.md`: Workspace placement (readiness filter, provisioning), Project environment (per-machine failure, re-check), daemon command transport (two new operations)
- [ ] 4.2 `CHANGELOG.md` `### Breaking`: recheck response shape, `environment_pause` gains the machine, Project pauses only when no machine is eligible; `### Added` for the rest

### Backend build-step verification
- Implemented 1.1–1.5 and 2.1–2.3, including the before-transition probe refusal that keeps initial Tasks queued without version churn. The public API/CLI/web and provisioning work remains unchecked.
- Machine key: Project + server owner, or Project + daemon/runtime owner. The temporary missing-record pass for an unprobeable daemon is the explicit arm in `placement::selection::filter_candidate`; remove it with `machine.probe`.
- Daemon failure/re-check/dispatch integration is verified with an `interactive` role on the daemon. Existing daemon coder prompt loading calls `EmbeddedWorkspaceBackend::recorded_server_path` in `workflow/dispatch/loader.rs`; coder/planner start also prepares an outbox on the server using the daemon-local path in `task_service/execution/runner.rs`. Those independent paths prevent proving coder/planner daemon launch under the refactor freeze and are follow-ups for the owning refactor; neither file was changed.
- Existing environment-pause test assertions are unchanged. The shared launch-test fixture seeds an already-ready server record so those tests still reach preflight: an unknown machine must now be refused before claim. Existing scheduled re-check and manual fallback tests passed unchanged.
- The unchanged interval validator and the full happy-path integration target were not run: there is no named environment happy-path case, and the focused-test restriction takes precedence. API response fields, generated bindings, web files, CLI files, daemon operations/capabilities, and CHANGELOG.md were not changed.
