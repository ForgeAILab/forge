## Context

State on `next/v0.14` (5db8ea0b), from the code:

- An Agent's machine binding is already optional. `agent.daemon_id` is set only by an admin through the create/update API (`routes/agents.rs:33`); nothing sets it automatically. Unpinned CLI Agents run on whichever owner placement selects.
- Placement (`services/src/placement/selection.rs`) chooses among `ready` repository locations. Filters: reachability, visibility, `workspace.v1`, executor installed/authenticated/enabled, adapter capability facts, run policy, pin, Agent and daemon capacity, native-backend and work-mode limits. Order: existing placement, inherited root placement, pin, default location, server-owned, `(created_at, id)`. No filter knows about the Project's environment.
- Project environment (`settings.environment`: `env`, `assets`, `checks`, `recheck_interval_seconds`) is applied at preflight, after the workspace is prepared and immediately before launch (`task_service/execution/environment.rs`). A failing check terminalizes that execution before any provider call and pauses the Project (`system_pause_reason = "environment_not_ready"`, detail in `project.environment_pause_json`). The scheduled re-check runs on the recorded ready daemon placement when there is one, otherwise in the server's primary checkout.
- A daemon gets a repository location only when someone registers an existing checkout on it. The server can create a managed clone for itself; a daemon cannot.

So a machine that cannot build the Project is discovered only after a workspace exists on it, the discovery stops the whole Project, and a capable machine that simply lacks a checkout is never considered.

## Goals / Non-Goals
- Goals:
  - Placement never selects a machine known to be unable to run the Project's work.
  - One unfit machine does not stop work that another machine can do.
  - A capable machine without the code can be used, but only after Forge has verified it.
  - Single-machine installs behave exactly as they do today.
- Non-Goals:
  - Native Agents on daemons (`native_backend_unsupported` stays).
  - Any change to how CLI credentials work: a CLI Agent runs where its CLI is installed and logged in.
  - Moving a prepared workspace between machines. Placement stays sticky.
  - Repositories without a remote on a second machine (there is no filesystem sync).
  - Deriving checks automatically (genesis, Project Agent). The owner declares them.

## Decisions

### D1. Readiness is a record per (Project, machine), not a property of the Project
A "machine" is a workspace owner: the server host, or one daemon runtime. New table `project_machine_readiness`: `project_id`, `owner_kind`, `daemon_id`, `runtime_id`, `status` (`ready`, `not_ready`, `unknown`), `checks_digest`, `failing_checks_json` (names plus bounded output tail), `scope_covered` (`machine` or `full`), `checked_at`, `next_check_at`, `version`. The digest covers `environment.env` and `environment.checks`; a settings edit that changes it makes every row for the Project `unknown`.

- Alternative considered: keep one Project-level pause and add a "skip this machine" list. Rejected: it cannot express "ready for machine checks, not yet fully checked", and the pause detail would still be about one arbitrary machine.

### D2. Checks get a scope; the default keeps today's behaviour
`EnvironmentCheck.scope`: `workspace` (default) or `machine`. A `machine` check runs in an empty scratch directory under the owner's workspace root with the Project `env` and no assets, so it cannot depend on the checkout. A `workspace` check runs in a checkout, as today.

- On a machine that has a ready location, the probe runs **all** checks in that location's checkout (read-only, the same way today's scheduled re-check uses the primary checkout), so existing Projects get the placement filter without relabelling anything.
- On a machine without a location, only `machine` checks can run. They gate provisioning (D5).
- Alternative considered: default `machine`. Rejected: an existing check that reads a repository file would fail in the scratch directory and wrongly mark every machine unfit.

### D3. The filter reads the record; probing happens outside the admission transaction
Admission runs under `BEGIN IMMEDIATE` and cannot wait for a command on another machine. Selection therefore only reads `project_machine_readiness`:

| Record for the candidate's machine | Result |
|---|---|
| `ready`, digest current | candidate passes |
| `not_ready`, digest current | rejected, `environment_not_ready`, failing check names in the rejection |
| missing, `unknown`, or stale digest | rejected for this attempt with retryable `environment_probe_pending`; a probe job is started |

The probe is a background job with single-flight ownership per (Project, machine), the same pattern as `environment_pause_sync`. On completion it writes the record with a version check and wakes dispatch. `environment_probe_pending` is a transient refusal: automatic dispatch keeps the Task queued (no Task version change after the first deferral, like the existing owner-unreachable wait). A Project with no checks has nothing to probe: every machine is `ready` by definition and no rows are written.

Preference order is unchanged. Because readiness is a hard filter, "prefer the machine that already has the code" falls out of the existing order (default location, then server-owned) applied to the machines that passed.

### D4. A launch-time failure marks the machine, and pauses the Project only as a last resort
Preflight stays the authority immediately before launch (a machine can break after it was probed). On failure:

1. The execution is terminalized through the existing pre-dispatch environment path: no provider call, no retry budget, the Task keeps its state.
2. The machine's readiness row becomes `not_ready` with the failing checks; `next_check_at` is set from `recheck_interval_seconds`.
3. The Project is paused with `environment_not_ready` only if **no machine remains eligible for the Project**: every owner that has a ready location, or could be provisioned, is `not_ready`. Otherwise the Project keeps running.

A Task whose workspace is already on the failed machine cannot move (sticky placement). It waits on that machine with a Task-scoped Attention item (`environment_not_ready`, naming the machine and checks), the same durable-wait shape as `runtime_offline`, and is re-dispatched when the machine's re-check passes. Tasks with no placement yet go to other eligible machines.

With one machine, step 3 always applies, so today's behaviour and UI are preserved exactly.

- Alternative considered: move the waiting Task to another machine. Rejected: uncommitted worktree state lives on the owner, and placement stickiness is what makes recovery and review tractable.

### D5. Provisioning a machine that lacks the code
A daemon runtime is a *provisioning candidate* for a repository when: it has no location for it; the repository has a remote URL; the daemon advertises `machine_probe.v1` and `repo_provision.v1`; its run policy allows the new purposes; it has the Agent's executor; and the Project allows provisioning (`settings.placement.provision`: `when_verified` (default) or `never`).

Provisioning candidates are considered **only when no candidate with a ready location passes the filters**. Then:

1. Run the Project's `machine` checks through `machine.probe`. If the Project declares no `machine` check, reject with `environment_unverified`: the error tells the owner to declare one or register a location by hand. All must pass.
2. `repo_location.provision`: the daemon clones the remote into `<workspace_root>/repos/<repo id>` using the machine's own Git credentials, and the server records a `managed_clone` location, `unverified`.
3. The existing `repo_location.verify` runs; then the full checks run in the clone and the readiness row becomes `ready` with `scope_covered = full`, or `not_ready`.
4. Dispatch is woken; normal selection now sees a ready location on a ready machine.

Steps run as a background job, single-flight per (repository, runtime), outside admission; the Task waits with `environment_probe_pending`. A clone failure leaves the location `unavailable` with the daemon's error and exponential retry backoff, as server managed clones do today.

- Alternative considered: provision whenever the executor is present. Rejected: this is exactly the reported failure (code lands on a machine that cannot build it).
- Alternative considered: provision eagerly on every capable daemon. Rejected: copies code to machines that may never be needed.

### D6. Re-checks follow the machine
`environment_pause_sync` becomes a per-machine re-check: for every `not_ready` row whose `next_check_at` has passed, run the recorded failing checks on that machine (in its location's checkout, or the scratch directory for a machine with none). Success sets the row `ready`, clears a matching Project pause through the existing compare-and-clear, and wakes dispatch. An unreachable machine keeps its row and reschedules; it is already excluded by `owner_unreachable`.

The on-demand endpoint runs every configured check on every machine that has a row or a ready location (or one named machine) and returns results grouped by machine.

### D7. Agents
No schema change. The pin stays admin-only and optional. The Agent response gains `runnable_on`: the machines where the Agent's executor is installed, authenticated and enabled, from the same facts placement uses, so the UI can show "runs on: server, Mac mini" and warn when the list is empty. The Agent settings page shows the pin and lets an admin clear it.

## Risks / Trade-offs
- **First dispatch after a settings edit waits for a probe.** One probe per machine per digest; bounded by the check timeouts (1–300 s). Mitigation: the Project PATCH starts probes immediately instead of waiting for the first Task.
- **A probe can be stale.** Preflight still runs before every launch, so a stale `ready` costs one pre-dispatch failure and no budget, as today.
- **`machine` checks are arbitrary commands on a daemon outside a workspace.** They run under a new run-policy purpose that the daemon's local configuration must allow, in an empty directory inside the workspace root, with the same timeout bounds; a daemon that does not allow the purpose is simply never a provisioning candidate.
- **Provisioning clones code onto another machine.** Only machines the Task owner can see, only when verified, and a Project can set `never`.
- **Frozen files.** `environment.rs` and `environment_pause_sync.rs` are under the refactor freeze. The edits are confined to the failure branch and the re-check target; they do not touch transitions, cascades or dispatch eligibility rules beyond one new retryable refusal.

## Migration Plan
One migration (timestamp version): create `project_machine_readiness`. For a Project that is currently environment-paused, insert a `not_ready` row for the machine recorded in `environment_pause_json` (the server when none is recorded), so the pause and its re-check carry over. No other data changes; existing checks read as `scope = workspace`.

Build order (each step ships on its own):
1. Readiness table, probe job, the placement filter, `runnable_on` (no behaviour change for Projects without checks).
2. Launch-time failure marks the machine; Project pause as last resort; per-machine re-check; API, CLI and UI for readiness.
3. `scope`, `machine.probe`, `repo_location.provision`, provisioning candidates.

## Open Questions
- Should `runnable_on` and the readiness list be visible to non-admin users? Daemon identities are admin-only in the Agent response today; this proposal keeps them admin-only and shows other users only a count.
