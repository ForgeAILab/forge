---
updated_at: 2026-10-02T10:47:57Z
---

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
A "machine" is a workspace owner: the server host, or one daemon runtime. New table `project_machine_readiness`: `project_id`, `owner_kind`, `daemon_id`, `runtime_id`, `status` (`ready`, `not_ready`, `unknown`), `checks_digest`, `failing_checks_json` (names plus bounded output tail), `check_results_json` (one result per configured check), `output_tail` (also preserves unnamed launch failures), `role`, `workspace_id`, `scope_covered` (`machine` or `full`), `checked_at`, `next_check_at`, `version`. The digest covers `environment.env` and `environment.checks`; a settings edit that changes it makes every row for the Project `unknown`.

- Alternative considered: keep one Project-level pause and add a "skip this machine" list. Rejected: it cannot express "ready for machine checks, not yet fully checked", and the pause detail would still be about one arbitrary machine.

### D2. Checks get a scope; the default keeps today's behaviour
`EnvironmentCheck.scope`: `workspace` (default) or `machine`. A `machine` check runs in an empty scratch directory under the owner's workspace root with the Project `env` and no assets, so it cannot depend on the checkout. A `workspace` check runs in a checkout, as today.

- On a machine that has a ready location, the probe runs **all** checks in that location's checkout (read-only, the same way today's scheduled re-check uses the primary checkout), so existing Projects get the placement filter without relabelling anything.
- On a machine without a location, only `machine` checks can run. They gate provisioning (D5).
- Alternative considered: default `machine`. Rejected: an existing check that reads a repository file would fail in the scratch directory and wrongly mark every machine unfit.

### D3. The filter reads the record; this build step probes only the host
Admission's pure selection reads readiness for the concrete Task's worktree
roles. A named failure rejects only when `EnvironmentCheck::applies_to` matches
one of those roles; an unnamed launch failure applies to its recorded role.
The probe records all per-check results rather than discarding roles.

| Current record | Server candidate | Daemon candidate in build step A |
|---|---|---|
| `ready` | passes | passes |
| `not_ready` with an applicable failure | `environment_not_ready` with check names | same |
| `not_ready` with no applicable failure | passes | passes |
| missing, `unknown`, or stale digest | transient `environment_probe_pending` | passes |

Only the server host is proactively probed until step 3 adds `machine.probe`.
Probing through another Task's live daemon workspace is unsound and is not
allowed. The temporary daemon policy lives in one `environment_filter`
function, used identically at reserve and claim. A just-prepared workspace does
not change it. Daemon readiness comes only from launch-time checks.

Host probes run all configured checks with Project env in the repository's
server checkout, outside admission and without asset staging. Checks must be
read-only; this retains the base's primary-checkout cwd and does not enforce a
filesystem write sandbox. Output collection is bounded before redaction and
storage. Single-flight ownership is per Project/machine; version/digest fences
discard stale results. Completion releases the guard and wakes the existing
in-process dispatcher Notify immediately. A digest edit with checks remaining retires only the old digest's environment
pause in the same transaction: unknown daemon facts must be able to reach the
next launch. Host admission still waits for its probe. Removing all checks keeps
the base's manual-resume rule. User/repository pauses are preserved.

Settings changes invalidate rows and
start host probes for recorded or ready host locations. If an edit collides
with an older flight, completion schedules the current digest even without a
queued Task; retained host rows can use the server checkout without a location
row. A passing host probe compare-and-clears a matching environment pause using
its pre-probe Project snapshot, so a digest edit cannot strand a ready host
behind its old pause. User/repository pause guards remain authoritative.

Initial dispatch reuses the real admission context builder through a read-only
transaction. Probe deferral leaves the Task queued with no repeated version
change. A Project with no checks has no probes or rows. Preference order is
unchanged: if an otherwise eligible probe-pending owner would outrank the best
passing candidate, defer rather than placing on the lower-preference owner.

### D4. Admission and launch failure share the last-resort pause decision
Preflight still terminalizes a failing execution before any provider call or
retry budget and records that machine `not_ready`, with named check results,
role, workspace, bounded output and next-check time. The Task keeps its state.

The same task-aware function handles launch failure and admission refusal. If
all ready-location candidates that would otherwise run this Task's roles are
rejected only for `environment_not_ready`, compare-and-set a Project environment
pause naming the machine and checks. It runs before initial state entry, so a
first failing probe leaves no `dispatch_failed` annotation or transition.
Readiness versions and the Project pause guard fence concurrent changes; user
and repository pauses are never overwritten. Another eligible owner permits
new work.

Pins and existing/inherited placements stay sticky. When another owner is
healthy for other Tasks, including another Agent or executor, a Task pinned or
placed on the failed machine waits there
with Task-linked environment Attention, no `task.execution_failed` event and
no `recover_task` recommendation. An offline transport preserves an existing
environment-owned wait while its current not-ready fact still applies. Its metadata marker counts as parked for
Project slots and advances `list_revision` through the existing metadata
trigger. A ready result or successful admission clears the wait and Attention.
When the Project pauses on one machine, no additional Task Attention is
created: the pause remains the base's sole signal.

Manual resume and existing compare-and-clear paths reset `not_ready` rows to
`unknown` in the same transaction. On one host the next admission probes, then
launches or pauses again immediately, without waiting for the old due time.
Rows with no named failure are retryable through resume and scheduled re-check.

- Alternative considered: move a prepared workspace. Rejected: uncommitted
  state lives on the owner; placement remains sticky.

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

### D6. Re-check the failed machine and reschedule every outcome
One indexed due-row query per dispatcher pass drives independent `not_ready`
jobs. Re-run recorded failing checks; when there are no named checks, run all
configured checks applicable to the recorded role. Host jobs use the server
checkout; daemon jobs use only the failure's recorded ready placement through
existing `workspace.run`. Never borrow another live Task's workspace.

Actual check results update readiness with a version/digest fence. Transport,
unreachable, missing-workspace, policy and command version-fence errors retain
the row's facts and advance `next_check_at`; they never fail another Task's run.
Every outcome reschedules. Success marks ready, clears matching Task waits,
wakes dispatch, and compare-and-clears a matching Project pause through the
existing guard. A Project-version CAS loss from a harmless name edit retries
against the fresh version only while the pause epoch, digest and result version
remain unchanged. A user/repository pause or newer result wins. Clears intended to let work retry reset remaining failed rows
as described in D4. No daemon protocol operation is added in this step.

### D7. Agents
No schema change. The pin stays admin-only and optional. The Agent response gains `runnable_on`: the machines where the Agent's executor is installed, authenticated and enabled, from the same facts placement uses, so the UI can show "runs on: server, Mac mini" and warn when the list is empty. The Agent settings page shows the pin and lets an admin clear it.

## Risks / Trade-offs
- **First dispatch after a settings edit waits for a probe.** One probe per machine per digest; bounded by the check timeouts (1–300 s). Mitigation: the Project PATCH starts probes immediately instead of waiting for the first Task.
- **A probe can be stale.** Preflight still runs before every launch, so a stale `ready` costs one pre-dispatch failure and no budget, as today.
- **`machine` checks are arbitrary commands on a daemon outside a workspace.** They run under a new run-policy purpose that the daemon's local configuration must allow, in an empty directory inside the workspace root, with the same timeout bounds; a daemon that does not allow the purpose is simply never a provisioning candidate.
- **Provisioning clones code onto another machine.** Only machines the Task owner can see, only when verified, and a Project can set `never`.
- **Frozen files.** `environment.rs` and `environment_pause_sync.rs` are under the refactor freeze. The edits are confined to the failure branch and the re-check target; they do not touch transitions, cascades or dispatch eligibility rules beyond one new retryable refusal.

## Migration Plan
One migration (timestamp version): create `project_machine_readiness`. For a Project that is currently environment-paused, insert a `not_ready` row for the machine recorded in `environment_pause_json`, or inferred from its `workspace_id` placement before falling back to the server, so the pause and its re-check carry over. Malformed settings leave readiness `unknown` and log once instead of failing startup. Asset-only Projects with no checks retain their pause without a readiness row;
this resolves the no-check/no-row rule against the literal all-pauses cache
carry-over wording. No existing Project or placement is deleted; existing checks read as `scope = workspace` in the later scope step.

Build order (each step ships on its own):
1. Readiness table, probe job, the placement filter, `runnable_on` (no behaviour change for Projects without checks).
2. Launch-time failure marks the machine; Project pause as last resort; per-machine re-check; API, CLI and UI for readiness.
3. `scope`, `machine.probe`, `repo_location.provision`, provisioning candidates.

## Open Questions
- Should `runnable_on` and the readiness list be visible to non-admin users? Daemon identities are admin-only in the Agent response today; this proposal keeps them admin-only and shows other users only a count.
