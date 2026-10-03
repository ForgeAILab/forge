---
created_at: 2026-10-02T05:22:12Z
updated_at: 2026-10-03T00:32:38Z
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
- [x] 3.1 `EnvironmentCheck.scope` (`workspace` default, `machine`), validation, settings UI and `forge-ctl`
- [x] 3.2 Daemon transport: `machine.probe` + capability fact `machine_probe.v1`; run-policy purposes `environment_probe` and `repo_provision`; handshake facts; `forge-daemon` implementation with scratch directory lifecycle and timeouts
- [x] 3.3 Probe job uses `machine.probe` for daemon machines (with a location: all checks; without: `machine` checks)
- [x] 3.4 Daemon transport: `repo_location.provision` + `repo_provision.v1`; idempotent clone under `workspace_root/repos/<repo id>`; no partial directory on failure
- [x] 3.5 Provisioning candidates in admission (only when no ready-location candidate passes); provisioning job single-flight per (repository, runtime): machine checks → provision → verify → full checks → wake dispatch; `settings.placement.provision` (`when_verified` | `never`)
- [x] 3.6 Two-machine integration test with the daemon test harness: executor only on the daemon, machine checks pass → clone, verify, place; machine check fails → no clone, `placement_unavailable` with the check name; no machine checks → `environment_unverified`

## 4. Docs and release notes
- [x] 4.1 `docs/architecture.md`: Workspace placement (readiness filter, provisioning), Project environment (per-machine failure, re-check), daemon command transport (two new operations)
- [ ] 4.2 `CHANGELOG.md` `### Breaking`: recheck response shape, `environment_pause` gains the machine, Project pauses only when no machine is eligible; `### Added` for the rest

### Backend A build-step verification
- Backend tasks 1.1–1.5 and 2.1–2.3 include audit corrections D-A–D-F: host-only probes, role-specific admission pause, transactional resume reset, environmental waits, parked slot projection, and preference-preserving deferral.
- Machine key is Project/server owner or Project/daemon/runtime owner. The temporary daemon missing/unknown/stale pass is `placement::selection::environment_filter`, shared by reserve and claim; step 3 removes it.
- Audit reproductions are permanent repo tests in `task_dispatcher/tests/environment_placement.rs`; they have no dependency on audit scratch files. The shared launch fixture no longer seeds a ready row: it configures the environment before ordinary claim; direct claims use launch preflight and dispatcher tests exercise real probes.
- Daemon launch tests use `interactive`, because separate jobs own coder/planner plan I/O fixes in `workflow/dispatch/loader.rs` and `task_service/execution/runner.rs`; neither site is changed.
- API response types, CLI, generated types, daemon operations, provisioning and CHANGELOG.md remain untouched. The second audit adds the existing filter codes to docs/api.md and two label strings to the web error map. Broad suites and the full happy-path target remain CI work; no named environment happy-path case exists.
- Audit coverage also verifies parked waits advancing `list_revision`, exact machine-specific deferral clearing, automatic continuation after a colliding digest edit, and a settings probe clearing its matching environment pause without a Task. The integration branch's future batched slot projection is not present at this HEAD; the current aggregate and row walk are checked for identical parked counts.
- Guard regressions cover a harmless name edit during re-check and retaining the environmental wait on offline transport. Valid digest edits with checks remaining retire the obsolete named-check environment pause in the same transaction; otherwise an unknown daemon fact could never reach the launch that verifies it. Removing all checks preserves the existing manual-resume behavior.
- Migration contract clarification: legacy asset-only pauses are preserved without a readiness row, honoring the no-check/no-row rule; configured-check pauses carry not-ready rows, and malformed settings carry unknown rows.

### Second-round audit corrections
- [x] Asset-backed checks use launch preflight; direct/manual claims bypass probe-pending.
- [x] Environment gating and pause use only the launching role, independently of Agent identity.
- [x] Passing re-check re-reads the Project; bad jobs/rows are isolated and rescheduled.
- [x] Unnamed failures require resume or Check now; pauses resolve redundant Task environment Attention.
- [x] Offline alternatives never affect the pause decision; no-check initial dispatch returns before context assembly.
- [x] Deleted daemon workspace resets its readiness to unknown and clears the wait.
- [x] Ported audit parity cases, focused suites, strict validation and final crate/web checks.

### Build step C verification
- [x] 3.1–3.6 and 4.1 implemented on `feat/environment-placement-daemon-probe`, base b34fe4c8.
- Protocol stays at revision 3; machine probe/provision are optional capability facts. Legacy ready-location owners retain launch preflight.
- V202610021500 adds restartable retry deadlines and refreshes scope-default digests while retaining verdicts and due times.
- Whole touched modules/targets passed: 443 test executions across 16 commands, plus 603 binding-export tests and 7 web tests. Clippy, Rust formatting, web typecheck, daemon build check and strict spec validation passed.
- Browser QA could not start: installed Chrome exited with SIGABRT. No screenshots or Lighthouse result are claimed.
- Step B surfaces and CHANGELOG.md remain under their respective owners; those task boxes are unchanged.
