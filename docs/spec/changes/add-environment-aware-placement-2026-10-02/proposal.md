---
created_at: 2026-10-02T05:22:12Z
updated_at: 2026-10-02T05:22:12Z
---

## Why
An Agent is an executor, a model and credentials; which machine runs it should follow from where the work can actually be done. Today placement checks that a machine is reachable, has the Agent's executor and has capacity, but never whether it can build the Project. The Project's environment checks run only after a workspace exists on the chosen machine, and one failing machine pauses the whole Project. A machine that has Codex but no Rust toolchain is therefore a valid target right up to the first failed launch. The owner hit this on v0.13.12 with a Project registered on one machine and an Agent set up from another.

## What Changes
- **Environment fit is a placement filter.** Each machine (the server host or a daemon runtime) has a recorded readiness for each Project, produced by running the Project's environment checks on that machine. A machine that is not ready is rejected at admission with `environment_not_ready` and the failing check names, before any workspace is prepared.
- **Checks declare a scope.** `machine` checks need no checkout (toolchains, disk, services) and can run before any code is on the machine; `workspace` checks run in a checkout, as today. Existing checks default to `workspace`, so behaviour is unchanged until an owner marks a check as `machine`.
- **A failing machine no longer pauses the Project.** A launch-time check failure marks that machine not ready for the Project. **BREAKING**: the Project is paused with `environment_not_ready` only when no machine is left that could run its work; `environment_pause` detail gains the machine it refers to. With a single machine, behaviour is the same as today.
- **Re-checks are per machine.** The scheduled and on-demand re-checks run on the machine that failed, not always in the server's primary checkout. **BREAKING**: `POST /projects/{id}/environment/recheck` returns results grouped by machine and accepts an optional machine.
- **A machine without the code can be used when it is verified.** If no machine that already has a ready location is eligible, a daemon that has the Agent's executor and passes every `machine` check may receive a managed clone of the repository, after which the full checks run there. Without at least one passing `machine` check the daemon is rejected with `environment_unverified`: Forge never moves code to a machine it knows nothing about. Registering a location by hand stays the explicit override. Projects can turn provisioning off.
- **Agents stay unbound.** An Agent has no machine unless an admin pins it; the pin stays an explicit constraint. The Agent response lists the machines it can currently run on, and the UI shows and clears the pin.
- Daemon protocol: `machine.probe` (run `machine` checks in an empty scratch directory) and `repo_location.provision` (clone the repository's remote into the runtime's workspace root), each behind a capability fact and the daemon's run policy.

Not in this change: native (non-CLI) Agents on daemons, moving CLI credentials between machines (a CLI Agent runs where its CLI is installed and logged in, as today), moving a prepared workspace to another machine (placement stays sticky), syncing a repository that has no remote, and Agent- or genesis-authored checks.

## Impact
- Affected specs: `workspace-placement`, `project-environment-pause`, `daemon-workspace-protocol` (all from changes that are implemented on `next/v0.14`), new `agent-machine-binding`.
- Affected code: `crates/services/src/placement/` (selection, admission), `crates/services/src/task_service/execution/environment.rs`, `crates/services/src/task_dispatcher/environment_pause_sync.rs`, `crates/services/src/repo_location.rs`, `crates/forge-daemon`, `crates/api-types` (`EnvironmentCheck.scope`, readiness and placement filter types, daemon transport), `crates/api/src/routes/projects.rs` and `agents.rs`, `crates/forge-client`, `web/src` Project settings and Task placement panel, `docs/architecture.md`, `docs/api.md`, `docs/cli.md`, one migration.
- Freeze: `task_service/` and `task_dispatcher/` are frozen for features until refactor item 2.3 lands; the two files above need an owner-approved exception. The change does not touch the state machine, cascades or recovery verbs.
