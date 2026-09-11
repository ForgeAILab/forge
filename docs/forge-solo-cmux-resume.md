# Resume: Forge Solo flow test in cmux

Paused at the user's request on 2026-09-10, at the end of the afternoon session (America/Toronto). Resume this work only when asked. This is a handoff, not a claim that the app or end-to-end flow is complete.

## Start here

1. **The source compiles again.** The TUI scrollback fix calls Ratatui's `Paragraph::line_count` at `crates/forge-solo/src/view.rs:435`, which is gated behind the crate's opt-in `unstable-rendered-line-info` feature (ratatui/ratatui#293); that feature is now enabled in `crates/forge-solo/Cargo.toml`. Revisit it if Ratatui stabilizes or renames the API.
2. **No application Task exists.** The live test reached approved Charter + configured review CI. The correct `forge_scope_propose` call for `task.propose` still failed with `Forge proposal operation is outside this scope`. Diagnose why the Project chat's generic proposal operation set omits `task.propose`.
3. **The latest Codex description-preservation fix has not been exercised in the live Solo process.** It passed adapter tests and a real provider probe; rebuild and restart after resolving the compile issue.
4. **Solo is stopped.** Its dedicated cmux workspace remains at a shell prompt. All four chat jobs are terminal; there are no Tasks to dispatch. Agents were interrupted and instructed to provide handoff only.

```text
Pocket Tasks demo
  [done] isolated repository and Solo state
  [done] scoped Codex chat callback bridge
  [done] Charter drafted, reviewed, and approved in Solo
  [done] blocking unittest review command configured
  [BLOCKED] task.propose excluded from generic proposal scope
  [pending] implementation -> checks -> review -> merge
  [pending] run app and capture actual output
```

## User's intended outcome

Use the newly developed `forge-solo` for a specific existing repository to build a simple app, test the real flow, and show screenshots or ASCII. The user authorized upgrading Forge's managed CLI if needed, explicitly requested Luna max agents to fix the issues, and selected **cmux** instead of tmux. Their latest instruction is to pause and provide resume documentation.

Demo brief: **Pocket Tasks**, a Python 3 standard-library CLI with `add TITLE`, `list`, `done ID`, `delete ID`, and `stats`; global `--file PATH` chooses JSON storage. Stable positive IDs, ID-ordered output, `[ ]`/`[x]` markers, persistence across invocations, friendly nonzero errors for empty titles and unknown IDs, meaningful unittest coverage with temporary storage, and README copy-paste examples. No dependencies, network services, or remote publishing. Use one ordinary implementation Task through Forge's check/review/merge flow. Compact mode may use the approved Charter directly; no milestone or extra document is needed for this demo.

Do not implement the demo by hand and then describe it as a Solo-created app. The purpose is to test actual scoped tools, task dispatch, implementation, checks, review, and merge.

## Workspace and preserved state

| Item | Exact value |
|---|---|
| Forge source | `/Volumes/Data/codes/ai/open-forge` |
| Source branch / HEAD at pause | `main` / `be95eff20bdf41368541f8dc3d9f60c8248c5280` |
| Demo root | `/Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux` |
| Demo Git repository | `/Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux/pocket-tasks` |
| Demo initial commit | `1bfbb7f` (README and gitignore only; no implementation) |
| Solo state | `/Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux/state` |
| Database | `state/forge.db` under the demo root |
| Evidence | `evidence/` under the demo root |
| cmux CLI | `/Volumes/Data/Applications/cmux.app/Contents/Resources/bin/cmux` |
| cmux version | `0.64.22 (102)` |
| Dedicated cmux target | `window:2`, `workspace:112`, `surface:150`, `pane:138`, tty `ttys031` |
| Dedicated window UUID | `CDC55629-581E-4392-9C04-45FEF802FEC1` |
| Workspace title | `Forge Solo · Pocket Tasks` |
| Last built binary | `target/debug/forge-solo`, timestamp Sep 10 16:43; older than final source edits |

There is substantial pre-existing dirty work in this checkout, including the untracked whole `crates/forge-solo/` crate and runtime extraction. Other user sessions are editing the same repository. **Do not reset, clean, overwrite, or attribute the whole diff to this task.** No commit was made by this task. The status inventory at pause is saved in `evidence/source-status-at-pause.txt`.

The original repo-local `open-forge/test` directory unexpectedly disappeared during the earlier attempt. Cause unknown; the three agents denied cleanup actions, and no destructive cleanup was requested. Its old evidence is unavailable. The replacement demo lives outside that shared directory. Do not use `make clean-test`, recreate live state by SQL, or replace this state with the backup during an ordinary resume.

## Durable project state

| Entity | Value |
|---|---|
| Project | `07cfb4a2-19d5-413d-9caf-c64a0000485f` |
| Project name / version | `Pocket Tasks` / `5` |
| Project status | `charter_backed`, `charter_setup_required = false` |
| Chat | `a18d0177-fa7c-4c9b-bf82-79300f7d6085` |
| Codex identity | `ab248a55-4766-477d-8865-8a166459ef05` |
| Codex profile | `21d00efe-c276-4d56-977a-3fea8da22286` |
| Charter | `39b626fb-d300-434f-8528-1f1443433c5a` |
| Approved revision | `64a8e6cd-98a6-4905-8451-4ec473eb8d7f` (revision 1) |
| Charter lifecycle / version | `attached` / `3` |
| Content digest | `24d4e0e9e6f38752bd39110d0ef4545137a4d8125a99908e92ba5e2584814195` |
| Render digest | `e7ccf2b88ad77bd75ea56b0e89f0c158cc7d06d726a98d11e9313378cefff588` |
| Review CI | `python3 -m unittest discover -s tests -v` |
| Review setup steps | empty; Python standard library only |
| Execution gate/setup | `active` / `ready`, no reported blocker |
| Repo mode | `direct_merge`, main branch, local repository |
| Task / milestone / document counts | all zero |

Charter approval was a real TUI action: narrow layout, Tab out of Composer, `3` for Approvals, Enter to open the exact saved revision card, inspect it, Enter to confirm. A read-only DB query verified the approved revision and Charter-backed state afterward. The approved content is in `evidence/approved-charter.json` and `.md`.

Chat jobs at pause:

| Job | State | Meaning |
|---|---|---|
| `45148186-1930-45a6-8d10-385b3d8a44f5` | succeeded, v15 | Charter drafting; many malformed calls before success |
| `6ed50ce8-192c-4df0-be4c-d37d9f63e357` | cancelled, v8 | Configured review CI, then got stuck probing milestone payloads |
| `40202c48-6d1e-45a2-9e93-2748571cfc61` | succeeded, v6 | Task attempted through wrong project-artifact tool; nothing created |
| `4b50b8bd-5dd6-4fdb-99f3-3383e0ac4745` | succeeded, v6 | Correct generic tool still rejected task.propose; task count verified zero |

Logs are `state/agent-chat-logs/<job-id>.jsonl`. Read these rather than trusting the current visual timeline, which has a known wrapping/scroll defect.

## Fixes already made

These are the changes from this testing effort, mixed with pre-existing work; inspect specific files rather than treating the entire working diff as ours.

- Managed CLI pins: Codex `0.147.0 -> 0.154.0`; Claude Code `2.1.226 -> 2.1.267`. The old managed Codex rejected `gpt-6-astra` despite a newer global binary. Managed Claude's npm 12 install needed the exact package's native install script allowed; the adapter now passes the specific `--allow-scripts=@anthropic-ai/claude-code@2.1.267` setting. No global npm configuration change.
- Chat Git finalization: force `auto_commit=false` for CLI Agent Chat and honor it across adapters; read-only/non-Git chat completion no longer tries Task Git probes/commits. Ordinary Task behavior retained.
- Scoped Codex tools: app-server stdio dynamic callbacks forward only the host-advertised tools, using fresh non-resumed chat threads, no workspace writes, no unrelated MCP servers or builtin tools, and normal host-side validation. Files: `cli-adapters/src/codex.rs`, `codex/client.rs`, `codex/protocol.rs`.
- Service bridge: protected persisted chat/profile/binding validation; narrow ProjectVerify or AccountScratch sessions to AgentChat/Deny for CLI chat; intersect permissions; dispatch through the existing coordination provider. Files include `agent-host/src/protected_store.rs`, `typed_tools.rs`, `services/src/embedded_agent_service.rs`, `agent_chat_turn_worker.rs`, and `embedded_agent_service_scoped_cli_chat_tests.rs`.
- Charter schemas: the singleton adoption tool advertises its nested payload structure. Pending adoption content is now returned as `project.current_state.adoption_charter` while setup is still required, with IDs, version, digests, and typed content; never treated as approved authority.
- Solo input/state fixes: fresh per-launch idempotency namespace; failed-turn retry state; preserve printable `R`, `?`, digits in Composer; F1 help; latest bounded turns; authoritative cancellation version; accept the operating skill's revision token; selected exact approval/review cards open through Enter and survive refresh.
- Noninteractive CLI children default stdin to null; protocol adapters opt into pipes. This is isolation hardening, not a proven explanation for every earlier terminal anomaly.
- **Latest Codex fix:** `promote_payload_guidance` in `cli-adapters/src/codex.rs` copies `payload.description` into the function description, leaving the schema unchanged. The agent traced Codex 0.154's schema compaction dropping nested descriptions. The live description-only probe passes after this fix. This change is not in the currently built Solo binary. Do not conflate successful structural nested-schema delivery with successful description delivery. This diagnosis is based on upstream source and a passing post-fix probe; the description-only probe was not run before the fix, so there is no controlled failing-before result.

Upstream source evidence reported by the Codex agent: `rust-v0.154.0/codex-rs/tools/src/dynamic_tool.rs` calls `parse_tool_input_schema`; `tools/src/json_schema.rs` invokes `compact_large_tool_schema`; `tools/src/json_schema/compaction.rs` applies a 5,000-byte budget, first removes schema descriptions, then drops definitions, replaces complex objects at depth >=3, and prunes compositions. The description-only 7.5 KB probe passed in 11.04 seconds after the adapter fix; a combined rerun of both chat probes passed in 10.56 seconds. No docs/changelog entry was added for this latest fix before pause.

Behavior documentation was updated in `docs/cli.md`, `docs/api.md`, `docs/getting-started.md`, and `CHANGELOG.md` for the earlier completed changes. Check/add documentation for the very latest description-preservation and unfinished TUI changes when resuming.

## Remaining blockers and interrupted edits

### 1. TUI compile failure — first resume action

The TUI agent was interrupted while fixing two proven display bugs:

- `render_timeline` counted logical lines instead of rows after wrapping, so a narrow pane showed old content while claiming live tail. Its offset also used a distance from the tail as a distance from the top.
- The approvals list showed the already-approved adoption because current draft and approved revision IDs are equal. A pending approval must refer to a distinct draft.

Current unfinished files: `crates/forge-solo/src/view.rs`, `app.rs`, and `runtime_backend.rs`.

`view.rs` calls `paragraph.line_count(inner_width)` at approximately line 435. Ratatui exposes that method only behind an unstable feature, so the last focused test failed at compile with **E0624: method line_count is private**. Replace this with a supported wrapping/measurement approach or revise the change deliberately. `app.rs` has a `usize::MAX` top-scroll sentinel. `runtime_backend.rs` has the distinct-draft filter and regression test, not yet compiled. Do not claim these fixes tested. No test process remained running when the agent handed off.

### 2. Task proposal scope

The latest real attempt used **`forge_scope_propose`**, `operation: "task.propose"`, and omitted `action`. It was still rejected before persistence. The model reported that tool's operation enum contained only `message.send`; independently confirm this from composed ToolSpecs/session setup before changing permissions.

Relevant path: persisted chat/profile/binding -> `cli_chat_scope_binding` -> permission narrowing -> `ScopeToolComposition` operation sets -> generic `forge_scope_propose` -> `CoordinationToolProvider::project_chat_task_target` -> `TaskService::execute_task_proposal_direct`.

The model's initial project-artifact tool mistake was caused by operator guidance, not proof of missing Task authority. The later correct generic call is the useful reproduction. `task.propose` is a GenericProposal on the Coordination surface, not a TypedProposal on ProjectOrchestration.

Also note a misleading internal schema branch: `orchestration_payload_schema(TASK_PROPOSE_OPERATION)` advertises `action=create`, but the real `services/src/task_service/proposal.rs::TaskProposalPayload` denies unknown fields and has no `action`. Do not copy that typed-artifact shape into the actual generic command. The generic schema/contract must be checked separately.

The runtime agent's source inspection found no independent dispatcher wiring blocker: Solo starts `RuntimeSupervisor` in Solo mode and `TaskDispatcher`, with `TaskService -> TaskExecutorRouter -> embedded/CLI providers`. Task proposal creates the primary-repository Task and default worker assignment. The workflow supports `working -> review -> merging -> done`, with human review required. These are source findings, not a completed live Task test.

After restoring the intended existing Project chat permission, prove it with a production-shaped scoped service test and a live turn. Do not grant unrelated account/workspace authority. Then verify Solo really dispatches and finishes its first Task; an execution gate reporting ready is insufficient evidence.

### 3. Actual app and proof

Nothing beyond repository seed files exists. After task creation works, wait for actual implementation, configured CI, review, and merge. Exercise persistence, invalid IDs, empty titles, completion, deletion, stats, and README examples from the delivered repository. Capture actual terminal output (ASCII is acceptable) or a screenshot of the correct dedicated cmux window. One native screenshot attempt captured another user window and is not valid demo proof.

## Validation status

All passes below precede the interrupted TUI edit unless stated otherwise. Do not call the current checkout green.

| Area | Recorded result |
|---|---|
| CLI adapters, latest description fix | 98 unit tests passed; strict Clippy passed |
| Real Codex chat probes | Nested structural schema probe and large description-only schema probe passed |
| Codex adapter tests earlier | 25 passed |
| Agent host | 91 library tests passed; strict Clippy passed |
| Scoped CLI chat service tests | 4 passed, including production-shaped scope binding |
| Agent Chat worker | 39 passed |
| Solo before interrupted display edit | 107 library tests passed; strict Clippy passed; 2 startup tests earlier |
| Latest Solo display test | **Compile failed: E0624, view.rs:435** |
| Canonical API happy path | 2 passed earlier in the session |
| Frontend earlier | lint/typecheck; 75 files, 444 tests passed |
| Real provider execution earlier | Codex Task file write; Claude Task file write; Claude non-Git chat; clean-cache managed Claude startup passed |
| Formatting/diff check | passed before the latest interrupted edits; recheck after finishing them |

The structural Codex probe was strengthened so its prompt supplied no argument fields or values; required nested enum values came from the advertised schema. It passed in 5.61 seconds. The description-only test is `codex_chat_uses_payload_description_after_large_schema_compaction`; it must keep its key field/value only in the description to remain meaningful.

## Resume commands

First read this repository's `AGENTS.md` guidance and `docs/architecture.md` before changing runtime wiring. CodeGraph tools were unavailable in this session; use them if available in the resumed environment. Do not initialize or replace user state without the relevant authorization.

After completing the interrupted code and focused tests:

```bash
cd /Volumes/Data/codes/ai/open-forge
cargo fmt --all -- --check
cargo test -p forge-solo --lib
cargo clippy -p forge-solo --all-targets -- -D warnings
cargo build -p forge-solo
git diff --check
```

Rerun only affected tests, plus the necessary live flow; do not repeat every broad check without a reason. Useful adapter probes if their code changes:

```bash
cargo test -p cli-adapters --test codex_e2e codex_chat_invokes -- --ignored --nocapture
cargo test -p cli-adapters --test codex_e2e codex_chat_uses_payload_description -- --ignored --nocapture
```

Inspect the preserved state without starting any workers:

```bash
python3 /Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux/evidence/status.py
```

Reopen the same project and state in the dedicated cmux terminal:

```bash
/Volumes/Data/Applications/cmux.app/Contents/Resources/bin/cmux tree --workspace workspace:112 --window window:2
/Volumes/Data/Applications/cmux.app/Contents/Resources/bin/cmux send --workspace workspace:112 --surface surface:150 --window window:2 'sh /Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux/evidence/run-solo.sh'
/Volumes/Data/Applications/cmux.app/Contents/Resources/bin/cmux send-key --workspace workspace:112 --surface surface:150 --window window:2 enter
```

If cmux refs no longer exist, inspect its window/workspace inventory and create a dedicated terminal; do not target one of the user's other active terminals. The launcher runs:

```bash
/Volumes/Data/codes/ai/open-forge/target/debug/forge-solo /Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux/pocket-tasks --data-dir /Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux/state --agent codex
```

Read/capture the actual terminal:

```bash
/Volumes/Data/Applications/cmux.app/Contents/Resources/bin/cmux read-screen --workspace workspace:112 --surface surface:150 --window window:2 --lines 140
python3 /Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux/evidence/capture-cmux.py resumed-flow.txt
```

Composer receives printable input. Send text, then `send-key enter`. When a turn is running, Ctrl-C opens an exact cancellation card; inspect it before Enter. When idle, Ctrl-C gracefully exits Solo. `send-key ctrl+]` was rejected by cmux; narrow-layout Tab/`3`/Enter was verified for approvals. Do not reset terminal settings globally.

## Evidence index

Under `/Volumes/Data/codes/ai/forge-solo-demo-20260910-cmux/evidence/`:

- `02-adoption-approval.txt`: real cmux capture of the exact approval modal before confirmation.
- `approved-charter.json`, `approved-charter.md`: persisted approved content.
- `approved-charter-backup.db`: backup taken after approval.
- `paused-state-backup.db`: backup at pause, after all chat jobs became terminal.
- `paused-state.json`: concise durable entity state at pause.
- `source-status-at-pause.txt`: dirty/untracked source inventory.
- `tool-results.json`: selected tool-result evidence; live JSONL logs remain authoritative and may include later results.
- `verification.md`: earlier in-progress test notes; this resume document supersedes its status section.
- `brief.txt`, `run-solo.sh`, `status.py`, `capture-cmux.py`: original brief and reusable local helpers.

## Suggested first resumed message to the Project Agent

After fixing the scope composition and rebuilding:

> Continue the existing approved Pocket Tasks Project. First inspect current state and work to avoid duplicates. Create exactly one implementation Task through forge_scope_propose, operation task.propose, using the current Charter's exact review_requirement_ids and no action field. Review CI is already python3 -m unittest discover -s tests -v. Use the Charter directly in compact mode, with no milestone or extra document. Confirm the real Task id and dispatch state, and continue through implementation, checks, review and merge. Do not claim the app exists until it is delivered.

## Agent handoff at pause

All three were Luna with max effort, as requested by the user, and are now stopped/completed. This is distinct from the live Solo Codex profile, which inherited the ambient global Luna/medium setting. Do not change global Codex configuration just to test this project.

| Agent | Last ownership / handoff |
|---|---|
| `codex_scoped_tools` (Peirce) | Codex scoped bridge and description-preservation fix complete; 98 adapter tests and live probes passed; no active sessions |
| `solo_cli_runtime` (Mendel) | Scope/service bridge and pending Charter projection complete; dispatcher inspection only in latest slice; no proven dispatcher blocker; a broader `services project_runtime --lib` test was aborted; no active sessions |
| `solo_tui_recovery` (Kepler) | Earlier 107 Solo tests passed; latest renderer/approval-list edits unfinished and currently fail compilation; no active sessions |

No further implementation or test runs were performed after the user requested pause. Root only stopped Solo, captured durable state, and wrote this handoff.
