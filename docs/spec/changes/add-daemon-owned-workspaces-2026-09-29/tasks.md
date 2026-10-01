---
created_at: 2026-09-29T00:00:00Z
updated_at: 2026-09-30T12:00:00Z
completed_at:
---

## 0. Routing divergence fix (ships first, standalone bug fix)
- [x] 0.1 Route `execution_provider_for_agent` (start and cancel, `runner.rs`) on the snapshot's `resolved_daemon_id`, falling back to `agent.daemon_id`
- [x] 0.2 Focused test: an unpinned CLI Agent with a resolved remote daemon dispatches to the remote provider, and the ledger and routing agree
- [x] 0.3 CHANGELOG `Unreleased` → `### Fixed`

## 1. Schema and models
- [x] 1.1 New migration: `repo_location`, `workspace_placement` (including `reserved_until`, `disconnected_at`, `failure_cause`), with indexes (`workspace_id` unique, `(daemon_id, state)`)
- [x] 1.2 Backfill: server `primary_checkout` per `repo.local_path`, and a server placement per non-cleaned workspace (`selected_by = backfill`)
- [x] 1.3 Models plus `Display`/`FromStr` enums, repository traits, `SqliteDb` impls, and manual row mappers
- [x] 1.4 Focused db tests: backfill correctness, version conflict, unique workspace placement

## 2. WorkspaceBackend with the embedded backend (no behavior change)
- [x] 2.1 Define the `WorkspaceBackend` trait and types in `services` (D6)
- [x] 2.2 `EmbeddedWorkspaceBackend` wrapping `WorkspaceManager`, `merge_service`, the review runner's command execution, `diff.rs`, and `plan_artifact.rs` reads
- [x] 2.3 Characterization tests pinning reviewer prompt bytes and merge outcomes before rerouting
- [x] 2.4 Reroute every `worktree_path` consumer outside the backend (environment, hooks, lifecycle, review contract and runner, diff, plan, operator_status, native_tools, terminal, recovery, cleanup, workflow actions) through the backend
- [x] 2.5 Make `Workspace.worktree_path` unreachable outside the backend module
- [x] 2.6 Existing focused merge, workspace, and review tests plus the relevant `happy_path` case pass unedited

## 3. Placement admission
- [x] 3.1 Placement selection (hard filters, preference order, `selection_reason`) in `claim.rs`
- [x] 3.1a Split claim into reserve → prepare → start (D4): the reserve transaction commits the placement; prepare runs outside a transaction; the start transaction creates the claim, the `Running` Execution, and the lease with a placement version check
- [x] 3.1c Capacity accounting (design D4): Agent slots = running Executions + `reserved`/`preparing` placements; daemon sessions counted by placement execution daemon for pinned and unpinned Agents; checked in the reserve transaction and re-checked at start; rewrite `agent_capacity.rs` and the claim admission check accordingly
- [x] 3.1b Placement failure causes (D9), kept off the retry budget; sweep expiring `reserved`/`preparing` rows past `reserved_until`
- [x] 3.2 Subtask inheritance from the root workspace placement; reject an incompatible subtask Agent
- [x] 3.3 Snapshot stores `placement_id`; remove `resolved_daemon_id` from the snapshot; ledger, terminal, and recovery read the placement
- [x] 3.4 `placement_unavailable` structured error through ServiceError → ApiError
- [x] 3.5 Focused tests: only-daemon location, pinned Agent, no compatible owner (no fallback), native reviewer rejected, PR repo rejected, re-claim reuses placement, prepare failure creates no Execution and keeps the retry budget, `run_purpose_denied` filter, concurrent claims cannot exceed `max_concurrent_tasks` while a reservation is preparing, unpinned executions count against the daemon session cap

## 4. Repository locations surface
- [x] 4.1 REST routes, `api-types`, generated TS, `docs/api.md`
- [x] 4.2 `forge-ctl repo location list|add|verify|set-default|remove`, and `docs/cli.md`
- [x] 4.3 Shared-mount probe verification
- [x] 4.4 Focused route tests: register, verify failure, delete-in-use 409

## 5. Daemon protocol revision 3 (daemon side)
- [x] 5.1 `api-types`: revision 3, `workspace.v1` capability, params and results for the 9 RPCs; minimum accepted revision stays 2
- [x] 5.1a Server sends the revision-2 `execution.terminal.ack` wire shape to revision-2 connections and `journal.ack` to revision ≥ 3 (surfaced during 5.1)
- [x] 5.2 Daemon workspace backend in `forge-client` (reuse the `workspace` and `git` crates), handle map, and confinement checks
- [x] 5.3 Extend `DaemonTerminalStore` into the single daemon journal (terminal, operation, and cleanup entries; `journal.ack`), with idempotent replay, generation fencing, and wrong-owner rejection
- [x] 5.3a Handshake advertises per-executor adapter capability facts and the local `workspace.run` policy; the daemon enforces the policy (`purpose_denied`)
- [x] 5.3b Daemon reads the execution outbox after the CLI exits and embeds bounded entries in the terminal report
- [x] 5.4 Journal replay of terminal and cleanup results on reconnect, with ack handling
- [x] 5.5 Focused daemon tests: duplicate operation id, stale generation, path escape, dirty-target merge refusal, denied run purpose, outbox entries present in a replayed terminal report

## 6. Daemon backend (server side) and lifecycle wiring
- [x] 6.1 `DaemonWorkspaceBackend` over the command stream; router picks the backend from the placement
- [x] 6.2 `execution.start` sends the handle-resolved daemon path from `workspace.prepare`, never a server path
- [x] 6.2a Runner ingests outbox entries from the terminal report for every placement (the embedded backend fills the same field); remove the direct server-filesystem outbox read
- [x] 6.3 Review CI steps, hooks, and environment setup via `workspace.run`; reviewer prompt inputs via `workspace.read` and `workspace.diff`
- [x] 6.3a Daemon support for review operations still on the server-only path after 2.4: reviewer git evidence and exact diff, detached conformance checkouts, restore to the candidate commit, and unbounded CI output semantics (surfaced during 2.4)
- [x] 6.3b Daemon support for owner operations that still require `embedded_path`: execution assets, filesystem plugins (knowledge capture and inject), recovery and supervisor git checks (surfaced during 2.4)
- [x] 6.4 Direct merge via `workspace.merge`; merge evidence stored from the result (merged SHA, diffstat, conflict paths)
- [x] 6.5 Cleanup via backend with owner ack; `cleaning` persists while offline
- [x] 6.5a Server acknowledges daemon operation errors through `error.details.entry_id`, and reconciles interrupted `workspace.run`/`workspace.merge` intents that the daemon retains as `daemon_unavailable` (surfaced during 5.x)
- [x] 6.6 Remote round-trip tests with an in-process daemon on a separate temp root that the server cannot see

## 7. Disconnect and reconnect
- [x] 7.1 Daemon monitor moves owned placements to `disconnected`, with an attention item; the dispatcher skips them
- [x] 7.1a Freeze lease expiry for running executions on `disconnected` placements (lease monitor and `execution_events.rs` notification admission); add `max_disconnect` config and the `owner_disconnected_timeout` failure; the hard deadline still applies
- [x] 7.2 Reconnect reconciliation (`workspace.describe` with active and journaled execution ids, journal drain, settle each running execution, head comparison) before `ready`
- [x] 7.2a Periodic reconciliation sweep over `disconnected` placements with an online owner and over `cleaning` placements; a reconnect only wakes it early
- [x] 7.2b `resume_session` offered only when the owner is online and reports `resume`
- [x] 7.3 User actions: retry on the same owner, cancel
- [x] 7.4 Focused tests: mid-execution disconnect with no duplicate execution; outage longer than the 60s lease still accepts the replayed terminal and its outbox; `max_disconnect` timeout; interrupted reconciliation completes through the sweep; offline owner does not fall back

## 8. Docs, changelog, UI
- [x] 8.1 `docs/architecture.md`: replace the "same absolute path" caveat with the placement model, owner table, and failure policy
- [x] 8.2 `docs/getting-started.md`: registering a machine-local repository on a linked daemon
- [x] 8.2a Document the daemon's `daemon.yaml` (`workspace.run.allow`) and the one-time migration of the terminal store into the journal (surfaced during 5.x)
- [x] 8.3 Task and workspace responses expose `placement`; web shows owner and state on the Task detail
- [x] 8.3a `docs/architecture.md` and `docs/getting-started.md`: the daemon run policy, and the dispatch-versus-reach trust note (the daemon is not a sandbox)
- [x] 8.4 CHANGELOG `Unreleased` → `### Breaking` (unpinned routing, protocol revision 3, snapshot field, placement in responses)

## 9. Acceptance (FrameRill, manual E2E against 10.0.0.2)
- [ ] 9.1 The server has no access to `/Volumes`; the FrameRill location is registered and verified on the Mac daemon
- [ ] 9.2 The placement selects the Mac with a recorded reason; the worktree exists only under the Mac workspace root
- [ ] 9.3 A Mac-only build or test runs in the Agent turn and in review CI steps on the Mac
- [ ] 9.4 Direct merge updates the Mac primary checkout with no manual sync
- [ ] 9.5 Daemon disconnect and reconnect reconcile the same workspace without duplicates; cleanup completes only after the daemon's ack
- [ ] 9.6 Task, execution, lease, placement, logs, commit evidence, merge result, and cleanup state are inspectable from the server
