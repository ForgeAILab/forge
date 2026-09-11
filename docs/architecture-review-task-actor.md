# Task Component Architecture Review — The Actor Model Problem

**Scope:** `crates/services/src/task_service/`, `crates/services/src/workflow/engine/`, and the two transition entry points (`crates/api/src/routes/tasks/`, `crates/mcp-server/src/tools/`).

**Goal:** Refactor the task component to serve both human users and AI agents well.

**Date:** 2026-06-28

**Status:** Historical pre-refactor analysis. Paths and behavior below describe the 2026-06-28 snapshot and are not current; use [`architecture.md`](architecture.md) for the implemented contract.

---

## 1. Executive summary

The task state machine is not split between two competing implementations — the architecture doc's "legacy vs engine" framing is **stale**. In reality there is one engine (`WorkflowEngine::transition_inner`) that both humans and agents funnel into, wrapped by a pre/post-cleanup layer in `TaskService::transition`.

The actual problem is upstream of the engine: **the system has no type-level concept of *who* is driving a transition.** "User", "agent", and "system" are distinguished by string-prefix matching on a `triggered_by: &str` argument, and the strings are largely wrong. A full audit of every `triggered_by` assignment in non-test code shows that **only one call site emits an `"agent:"` prefix**; every other agent-driven transition — including the MCP tool an agent calls to move tasks — runs as `"system"`.

This makes the workflow engine's `HookAudience::AgentOnly` a category that, in practice, means *"system or claim"*, and it makes several `"...starts_with(\"user:\")"` policy checks correct only by coincidence. Introducing a typed `Actor` is the single highest-leverage change, after which the long-standing cleanup work (collapsing the wrapper, killing the inline engine reconstruction, demoting `is_awaiting_human` from a heuristic pile to a state query) becomes straightforward and safe.

**Headline recommendation:** introduce `Actor { User, Agent, System }`, thread it through the transition pipeline replacing the `triggered_by: &str` string, fix the emitters to tell the truth, and then migrate the imperative pre/post cleanup in `TaskService` into engine hooks keyed on actor. The happy-path test is the guardrail throughout.

---

## 2. How the task component works today

### 2.1 The layers

```
HTTP / MCP entrypoint
  └─ TaskService::transition()                 [pre/post orchestration wrapper]
       └─ WorkflowEngine::transition_inner()   [graph + guards + hooks + dispatch + audit log]
            └─ hooks (before_enter / on_exit / on_enter / after_enter) filtered by "audience"
```

- **Entrypoints** set a `triggered_by` string and call `TaskService::transition`.
- **`TaskService::transition`** (`crates/services/src/task_service/transition.rs:11`) does three things: (1) run pre-checks, (2) delegate to the engine, (3) run post-cleanup on the returned task.
- **`WorkflowEngine::transition_inner`** (`crates/services/src/workflow/engine/mod.rs:523`) is the real state machine: optimistic-version check, `before_exit` guards, status update + `transition_log` write + event publish, then `on_exit` / `on_enter` / `after_enter` hooks, with cascading transitions (depth-limited to 3).

### 2.2 The one place actor matters

Across this entire pipeline, "who is acting" is consulted in exactly one function inside the engine:

```rust
// crates/services/src/workflow/engine/hooks.rs:95
pub(super) fn hook_audience_matches(audience: HookAudience, triggered_by: &str) -> bool {
    match audience {
        HookAudience::All => true,
        HookAudience::AgentOnly => triggered_by.starts_with("agent:") || triggered_by == "system",
        HookAudience::UserOnly => triggered_by.starts_with("user:"),
    }
}
```

And in two more places inside the wrapper:

- `transition.rs:108` — `cancel_active_execution_for_user_transition` runs only when `triggered_by.starts_with("user:")`.
- `transition.rs:768` — `should_clear_review_passed_at` branches on `triggered_by.starts_with("user:")`.

Every other behavioral fork between "user did this" and "agent did this" reduces to those three string checks.

---

## 3. Findings

Findings are rated **Critical** (latent incorrectness), **High** (structural blocker for the stated goal), or **Low** (cleanup).

### F1 — Critical: the actor signal is fictional

`HookAudience::AgentOnly` is defined to match `triggered_by.starts_with("agent:") || triggered_by == "system"`. An audit of every `triggered_by` assignment in production (non-test) code:

| Call site | File:line | Emitted value | Real actor |
|---|---|---|---|
| Task claim | `task_service/claim.rs:250` | `"agent:claim"` | Agent ✓ |
| API transition (default) | `task_service.rs:144`, `:156` | `"user:api"` | User ✓ |
| Board drag | `api/routes/tasks/transitions.rs:15` | `"user:board_drag"` | User ✓ |
| Gate approve/reject | `api/routes/tasks/gates.rs:73`, `:113` | `"user:api"` | User ✓ |
| User-driven recovery | `execution/recovery.rs:829`, `:1569` | `"user:recovery:…"` | User ✓ |
| `From<i64>` default | `task_service.rs:132` | `"system"` | *varies* |
| Execution launch/completion | `execution/launch.rs:190,365,573` | `"system"` (via `From<i64>`) | **Agent** |
| Cascade transitions | `execution/cascade.rs:320`, `:1292` | `"system"` | System |
| Follow-up dispatch | `execution/follow_up.rs:243` | `"system"` (via `From<i64>`) | **Agent** |
| Historical subtask transitions | Former subtask transition module (deleted; ordered-sequence coordination now lives in `task_service/subtask.rs` and `execution/cascade.rs`) | `"system"` (via `From<i64>`) | **Agent/System** |
| Initial scheduling | `task_dispatcher/initial_scheduling.rs:98` | `"system:task_dispatcher"` | System |
| **MCP `forge_transition_task`** | `mcp-server/tools/handlers.rs:479` | `"system"` (via `From<i64>`) | **Agent ✗** |

**The only `"agent:"` emitter in the whole codebase is `claim.rs:250`.** Every other agent-driven path — including the MCP tool an agent uses to transition a task — runs as `"system"`.

**Consequences:**

1. `AgentOnly` hooks are, in production, *"system-or-claim"* hooks. There is currently no way to author a hook that runs when the AI agent acts but **not** when Forge's own automation acts. The two are indistinguishable in the data.
2. The MCP `forge_transition_task` tool — the canonical agent integration path — is indistinguishable from the internal scheduler. Any future feature of the form "agent-initiated transitions require a human co-sign" or "log agent decisions to the agent's ledger" is impossible without first fixing this, because the signal does not exist.
3. `AgentOnly`/`UserOnly` audience semantics cannot be reasoned about from the type system; they can only be reverse-engineered by grepping three prefix matches across two crates.

This is the finding that most directly defeats the stated goal of "works well for both user and agents." Until actors are real, every actor-aware feature is built on sand.

### F2 — Critical: policy checks are correct only by coincidence

Two wrapper-side rules key on `triggered_by.starts_with("user:")`:

- `cancel_active_execution_for_user_transition` (`transition.rs:108`) — when a human moves a card out of an active/gate state, kill the running executor.
- `should_clear_review_passed_at` (`transition.rs:768`) — clear the review-passed timestamp on certain user moves.

These are **correct today** only because agent completions happen to be labeled `"system"` and therefore skip the `"user:"` branch. The correctness is a string-prefix accident, not an actor-type rule. The failure mode is forward-looking: the moment any `"system:admin_override"` or `"system:*"` path is added that *should* cancel a running executor, it silently won't. Encoding policy as "does the free-form reason string start with these four characters" is a class of bug waiting for a trigger.

### F3 — High: the "dual path" described in the docs does not exist as stated

`docs/architecture.md` states:

> `WorkflowEngine` in `crates/services/src/workflow/engine.rs` is the new data-driven path; `TaskService.transition()` still uses the legacy `TaskStatus`/`transition_allowed` path. Treat the engine as a parallel code path until that split is removed.

This is no longer accurate. The trace shows `TaskService::transition` (`transition.rs:11`) is a **thin pre/post wrapper that delegates entirely to `WorkflowEngine::transition_with_deferred_dispatch`** (`transition.rs:70`). There is no competing `transition_allowed`-based transition path in the wrapper. The doc is misleading new contributors (and was misleading during this review) about where the work is.

The real shape is:

- **Pre** (`transition.rs`): `ensure_planning_plan_ready_before_leaving`, `cancel_active_execution_for_user_transition`.
- **Engine** (`workflow/engine/mod.rs:523`): the entire state-machine lifecycle.
- **Post** (`transition.rs:73-100`): clear `blocked_json`, clear `review_passed_at`, clear planning-awaiting metadata, clear manual-review-awaiting metadata, clear transient `error_annotation`, clear execution-retry metadata.

The boundary between "engine concern" and "wrapper concern" is *temporal* (pre vs post) rather than *domain-based*, which is why domain-meaningful cleanup (clearing review metadata, clearing blocked state) lives as imperative code in the wrapper instead of as part of the transition's hook lifecycle.

**Recommendation:** the doc must be corrected regardless of whether the refactor proceeds. As part of the refactor, the post-cleanup block migrates into `after_enter`/`on_exit` hooks (see Phase 5).

### F4 — High: `WorkflowEngine` is reconstructed inline by hand-copying 12 `Arc` fields

`TaskService::transition` (`transition.rs:42`) and `TaskService::advance_to_next_state` (`transition.rs:423`) each contain this block:

```rust
let engine = WorkflowEngine {
    db: Arc::clone(&self.db),
    event_bus: Arc::clone(&self.event_bus),
    review_runner: self.review_runner.clone(),
    merge_service: self.merge_service.clone(),
    cleanup_scheduler: self.cleanup_scheduler.clone(),
    task_executor: self.task_executor.clone(),
    daemon_connections: self.daemon_connections.clone(),
    workspace_exec_locks: self.workspace_exec_locks.clone(),
    terminal_activity: self.terminal_activity.clone(),
    workspace_root: self.workspace_root.clone(),
    repo_cache_locks: self.repo_cache_locks.clone(),
};
```

Two sites, eleven fields each, copy-pasted. This is a construction smell that exists because `WorkflowEngine` is not a long-lived dependency — it is reassembled from `TaskService`'s fields on every transition. Any new engine dependency must be added to `TaskService`, to both builder paths, *and* to both inline-reconstruction blocks, or it silently won't reach the engine. (Note: `HookContext` in `engine/mod.rs` repeats the same eleven fields *again* per hook execution.)

**Recommendation:** make `WorkflowEngine` a single `Arc<WorkflowEngine>` constructed once in `forge-cli/main.rs` alongside `TaskService`, stored in `AppState`, and borrowed by both. The inline reconstruction and the duplicate field lists disappear.

### F5 — High: `TaskService` is a god object

```rust
// crates/services/src/task_service.rs:98
pub struct TaskService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    merge_service: Option<Arc<MergeService>>,
    cleanup_scheduler: Option<Arc<WorkspaceCleanupScheduler>>,
    review_runner: Option<Arc<ReviewRunner>>,
    task_executor: Option<Arc<dyn TaskExecutor>>,
    daemon_connections: Option<Arc<DaemonConnectionRegistry>>,
    workspace_exec_locks: Option<Arc<WorkspaceExecutionLockManager>>,
    terminal_activity: Option<Arc<TerminalActivityTracker>>,
    repo_cache_locks: Option<Arc<RepoCacheLockManager>>,
    workspace_root: PathBuf,
    memory_service: Arc<MemoryService>,
}
```

Twelve fields, eleven of them `Option<Arc<...>>` wired through nine `with_*` builders. The module spans 30+ files (`task_service/` tree: 43+ symbols in the root alone, plus `execution/`, `tests/`, `workflow/`). Every transition-relevant decision — dispatch, review, merge, cleanup, terminal locking, memory indexing — routes through this one object. For a product whose explicit goal is dual-audience (user + agent), a fattening central seam means every actor-aware feature touches the same place and the type only grows.

This is the downstream symptom of F3/F4: because the wrapper owns "everything around the transition," it accumulates every dependency the transition might need.

### F6 — High: `is_awaiting_human` is the human/agent boundary, and it is a heuristic pile

`TaskService::is_awaiting_human` (`transition.rs:~170`) decides "does this task need a human?" by combining, in sequence:

1. `blocked_json` is set → true.
2. `metadata.extra["awaiting_human"]` bool → true.
3. Status is `review` and latest review is `AwaitingHuman` → true.
4. Status is `planning` and the planner role is assigned to a user → true.
5. Status is a `Gate` state with `gate_config.requires_user_approval()` and no decision since entry → true.
6. Gate state whose role is assigned to a user, no decision since entry → true.

Step 2 in particular reads a convention-encoded boolean out of a free-form metadata JSON bag. This is exactly the concept a dual-audience product needs to be first-class — *"right now, is this task waiting on a human or an agent?"* — and it is currently reverse-engineered from scattered metadata rather than derived from `(workflow_state, role_assignment, gate_config)`.

The `awaiting_human` metadata bool is written imperatively in several places (`set_planning_awaiting_review_metadata`, `clear_manual_review_awaiting_metadata` in `transition.rs`), which means the *stored* truth and the *derived* truth can drift. A derived property cannot drift.

### F7 — Low: audience asymmetry between `System` and `Agent`

`hook_audience_matches` (`hooks.rs:96`) treats `"system"` as satisfying `AgentOnly`. There is no symmetric treatment: nothing makes `System` a first-class audience. Combined with F1, this means `AgentOnly` is the only audience that silently absorbs a second actor. After the `Actor` introduction this collapses to an exhaustive match with no special cases.

### F8 — Low: `transition_log.trigger_reason` overloaded as both audit text and control signal

`trigger_reason` is a human-readable string ("gate approved", "cancel task", "manual advance") that is also *parsed* in `is_awaiting_human` (`transition.rs`) via `entry.trigger_reason.starts_with("gate approved")`. Audit prose and control flow should not share a field. This resolves naturally once F6 makes `is_awaiting_human` derived.

---

## 4. End-to-end flow traces

Two canonical transitions, `in_progress → review`, one human-initiated and one agent-initiated. Both reach the same engine; they diverge only in the pre-check and the audience filtering.

### 4.1 Flow A — Human board drag

```
POST /api/v1/tasks/{id}/transition  { status:"review", source:"board_drag", version }
│
└─ api/routes/tasks/transitions.rs:8  transition_task()
   ├─ TransitionOptions::from((version, reason))           task_service.rs:139
   │    → triggered_by = "user:api"
   ├─ override: source==BoardDrag                          transitions.rs:14
   │    → triggered_by = "user:board_drag"
   │    → defer_dispatch_seconds = Some(10)
   └─ TaskService::transition(id, "review", opts)          transition.rs:11
      │
      ├─ PRE  ensure_planning_plan_ready_before_leaving    transition.rs:130   (no-op, not leaving planning)
      ├─ PRE  cancel_active_execution_for_user_transition  transition.rs:108   ← RUNS ("user:")
      │        kills the running executor execution for this task
      │
      ├─ let engine = WorkflowEngine { …11 Arcs… }         transition.rs:42    ← F4 smell
      ├─ engine.transition_with_deferred_dispatch(…"user:board_drag"…)
      │   └─ workflow/engine/mod.rs:523  transition_inner:
      │      ├─ optimistic version check
      │      ├─ before_exit guards → hook_audience_matches("user:board_drag")
      │      ├─ UPDATE task.status, version++, INSERT transition_log, publish task.status_changed
      │      ├─ on_exit  (in_progress) → audience filter
      │      ├─ on_enter (review)      → audience filter
      │      └─ after_enter            → review-runner hook → audience filter
      │
      └─ POST transition.rs:73-100:
         clear blocked_json / review_passed_at / awaiting-human metadata /
         transient error_annotation / execution_retry_metadata
```

### 4.2 Flow B — Agent finishes coding

The agent does not call the transition API. Its executor process exits and an internal hook advances the task.

```
executor.run_execution() returns ExecutionOutcome::Completed
│  task_service/execution/runner.rs
└─ ExecutionRepo::update → status = Completed
└─ (launch / cascade logic fires)
   execution/launch.rs:190  self.transition(task.id, target, task.version)
   │
   └─ TaskService::transition(id, "review", version)
      ├─ transition(id, status, version) uses From<i64>   task_service.rs:127
      │    → triggered_by = "system"                       ← NOT "agent:"  (F1)
      ├─ PRE  cancel_active_execution_for_user_transition  → SKIPPED (not "user:")   (F2 coincidence)
      ├─ engine.transition_with_deferred_dispatch(…"system"…)
      │   └─ transition_inner:
      │      ├─ before_exit / on_exit / on_enter / after_enter
      │      └─ hook_audience_matches("system") → matches AgentOnly AND All, never UserOnly
      └─ POST cleanup (identical to Flow A)
```

### 4.3 Flow C — Agent invokes the MCP transition tool (the integration path)

```
POST /mcp  { method:"forge_transition_task", params:{ task_id, status, version } }
│
└─ mcp-server/tools/handlers.rs:479  forge_transition_task()
   └─ state.task_service.transition(task_id, status.into(), version)
      └─ …identical to Flow B: triggered_by = "system" via From<i64>
```

Flows B and C are indistinguishable to the engine and to the audit log, yet B is "the AI finished its work" and C is "the AI explicitly asked to move the task." After the refactor these are `Actor::System` and `Actor::Agent` respectively, and `AgentOnly` hooks finally mean what they say.

---

## 5. Risk analysis

| Risk | Likelihood | Impact | Notes |
|---|---|---|---|
| Tightening `AgentOnly` to exclude `System` changes runtime behavior for hooks that silently relied on the conflation (F1/F7). | High | Medium | Every existing `AgentOnly` hook must be audited. Some will need to become `All`. This is the one intentionally-breaking step. |
| A `"system:*"` path is added later that should cancel a running executor but won't, because the check keys on `"user:"` (F2). | Medium | High | Latent until triggered; the refactor eliminates the class. |
| Inline engine reconstruction drifts between the two sites (F4). | Medium | Medium | Already duplicated; a new engine dep silently absent from one site would be a subtle bug. |
| `is_awaiting_human` returns wrong answer when stored metadata drifts from derived state (F6). | Medium | High | User-facing "needs human" indicator goes wrong; agents may auto-advance tasks meant for humans. |
| Doc inaccuracy (F3) misleads contributors. | Certain | Low | Already occurring. |

---

## 6. Refactor plan

Phases are ordered so that **1–3 are non-behavioral** (string → typed, same matches), **4 is the intentional break**, and **5–6 are the structural payoff** that the actor model unlocks. The happy-path test (`crates/api/tests/happy_path.rs`) guards phases 1–5 throughout.

### Phase 1 — Define `Actor` (non-behavioral)

In `crates/api-types/` (zero internal deps — the correct home):

```rust
pub enum Actor {
    User { handle: String },
    Agent { id: String },
    System { component: String },
}

impl Actor {
    /// Used ONLY at the transition_log persistence boundary.
    pub fn to_trigger_string(&self) -> String { … }
    pub fn audience_matches(&self, audience: HookAudience) -> bool {
        match (self, audience) {
            (_, HookAudience::All) => true,
            (Actor::Agent { .. }, HookAudience::AgentOnly) => true,
            (Actor::User { .. }, HookAudience::UserOnly) => true,
            _ => false,
        }
    }
}
```

Note `System` matches **only** `All`. Regenerate the TS bindings (`web/src/types/generated/`).

### Phase 2 — Thread `Actor` through the pipeline (non-behavioral)

Replace `triggered_by: &str` with `&Actor` in:

- `TransitionOptions` (`task_service.rs`) — replace the three `From` impls (`:127`, `:139`, `:151`) with explicit `Actor` fields. The `From<i64>` default-that-means-"system" is a footgun and should be removed.
- `WorkflowEngine::transition`, `transition_with_deferred_dispatch`, `manual_override_transition`, `reset_to_initial`, `transition_inner` (`workflow/engine/mod.rs`).
- `HookContext.triggered_by` (`engine/mod.rs` and `engine/context.rs`).

Keep the `transition_log.triggered_by` **column** as TEXT; convert via `Actor::to_trigger_string()` at the write boundary only. Read-side code that parses the string (e.g. `is_awaiting_human`'s `starts_with("gate approved")`) is addressed in Phase 6.

### Phase 3 — Fix the emitters to tell the truth (behavioral intent, same matches)

This is where F1 is actually fixed. Map each call site to its real actor:

- `claim.rs:250` → `Actor::Agent` (intent already correct).
- `execution/launch.rs` and `execution/follow_up.rs` (agent-driven) → `Actor::System` is honest for launch/cascade. Ordered-child sequencing now lives in `task_service/subtask.rs` and `execution/cascade.rs`; child executions are independent and serial in the root workspace, so the former subtask transition path is not a current behavior to preserve.
- **`mcp-server/tools/handlers.rs:479` `forge_transition_task`** → `Actor::Agent { id: <calling agent> }`. This requires the MCP layer to know the calling agent (it has `McpState`); if agent identity is not yet plumbed through, this is the place to add it. This single change is what makes `AgentOnly` meaningful.
- `gates.rs:73,113`, `transitions.rs:15` → `Actor::User`.
- `recovery.rs:829,1569` (user-driven) → `Actor::User`; system-driven recovery → `Actor::System`.
- `task_dispatcher/initial_scheduling.rs:98` → `Actor::System { component: "task_dispatcher" }`.

After this phase, `hook_audience_matches` still uses the old rules, so observed behavior is unchanged.

### Phase 4 — Tighten audience matching (intentional break)

Change `hook_audience_matches` (`hooks.rs:95`) to delegate to `Actor::audience_matches` from Phase 1. `System` no longer satisfies `AgentOnly`.

**Audit every existing `AgentOnly` hook** (search workflow definitions and `default_workflow.rs`). For each, decide:

- It genuinely means "the AI agent" → leave `AgentOnly`; it now correctly skips on system cascades.
- It was quietly relying on "system counts as agent" → change to `All`.

This is the one step that can change runtime behavior. Per `AGENTS.md` beta policy: record it under `### Breaking` in `CHANGELOG.md`, no compat shim, no flag. The happy-path test plus a targeted audit of `AgentOnly` hooks is the verification.

### Phase 5 — Collapse the wrapper (structural payoff)

Now that actor is typed and the post-cleanup is the only thing left in `TaskService::transition`:

1. Move the post-cleanup block (`transition.rs:73-100`) into engine hooks:
   - clear `blocked_json` → an `on_enter`/`after_enter` hook on entry to the destination state.
   - clear `review_passed_at`, awaiting-human metadata, transient `error_annotation` → `after_enter` hooks keyed on the relevant `Actor`/state-kind combinations.
2. Promote `WorkflowEngine` to a single `Arc<WorkflowEngine>` constructed once in `forge-cli/main.rs`, stored in `AppState`, borrowed by `TaskService`. Delete both inline reconstructions (`transition.rs:42` and `:423`) and the duplicated eleven-field list in `HookContext` (the engine owns them).
3. `TaskService::transition` becomes a thin delegate. `cancel_active_execution_for_user_transition` (`transition.rs:108`) becomes a `before_exit` hook gated on `Actor::User`.

After this, the F3 doc correction becomes literal truth: there is one transition path.

### Phase 6 — Derive `is_awaiting_human` (correctness payoff)

Replace the metadata-scraping implementation (`transition.rs:~170`) with a pure query over `(workflow_state.kind, role_assignment, gate_config, latest_review_status)`. Delete the imperative `set_planning_awaiting_review_metadata` / `clear_manual_review_awaiting_metadata` writers — the property is now derived, so it cannot drift. Remove the `trigger_reason.starts_with("gate approved")` parse (F8).

### Phase 7 — Docs

Update `docs/architecture.md` "Workflow engine (in progress)" section: remove the stale "parallel code path" claim (F3), document the `Actor` model, and update the `transition_inner` lifecycle description to reflect that pre/post cleanup now lives in hooks.

---

## 7. Testing & migration strategy

- **Guardrail:** `crates/api/tests/happy_path.rs` is the forcing function called out in `AGENTS.md`. It must stay green through every phase. Phases 1–3 are verified by it alone since they preserve observed behavior.
- **Phase 4 verification:** add a service-level test asserting `AgentOnly` hooks fire for `Actor::Agent` and `Actor::System` paths but **not** vice-versa; enumerate the audited `AgentOnly` hooks as cases.
- **No data migration:** `transition_log.triggered_by` stays TEXT; historical rows keep their existing strings. The column is append-only audit data. `Actor` is an in-memory type; only newly written rows get the corrected strings from Phase 3.
- **Public surface:** `TransitionTaskRequest.source` (`TransitionSource` enum) is unchanged; the actor is derived server-side from the auth principal / MCP caller. The MCP tool `forge_transition_task` gains agent identity plumbing but its JSON-RPC signature is stable.
- **Breaking change handling (per beta policy):** Phase 4 is the breaking step. Single `### Breaking` changelog entry, no `_v2`, no deprecation alias. State-machine changes must keep the `docs/architecture.md#task-state-machine` table accurate.

---

## 8. What this unlocks

Once `Actor` is real and the wrapper is collapsed:

- **Hooks can finally target the AI agent specifically** — enabling "log every agent decision to the agent's ledger," "agent-initiated transitions require human co-sign," "notify the user only when an *agent* requests review," etc. None of these are expressible today.
- **One transition entrypoint** for REST (user) and MCP (agent), differing only in the `Actor` passed in. This is the literal shape of "works well for both."
- **`is_awaiting_human`** becomes a trustworthy, derived signal — the foundation for any "hand off between human and agent" UX.
- **`TaskService`** shrinks to creation, claim, role assignment, and read queries; the engine owns the transition lifecycle end to end. The god object stops growing.

---

## 9. Open questions

1. **MCP caller identity:** does `McpState` currently carry the calling agent id? If not, Phase 3's `forge_transition_task → Actor::Agent` requires plumbing it through. Needs a check of `crates/mcp-server/src/state.rs` and the MCP auth path.
2. **Subtask transitions:** the former subtask transition module has been removed. Current ordered-child sequencing is coordinated by `task_service/subtask.rs` and `execution/cascade.rs`; each child owns its execution and the root only receives aggregate review after the serial sequence settles.
3. **Cascade actor:** engine cascades re-enter `transition_inner` with `"system"` (`engine/mod.rs` cascade recursion). Should a cascade preserve the originating actor, or always be `System`? Currently always `System`; defensible, but worth making explicit.

---

## Appendix A — Evidence: all `triggered_by` assignments (non-test)

```
crates/services/src/task_service.rs:132            triggered_by: "system"            (From<i64> default)
crates/services/src/task_service.rs:144            triggered_by: "user:api"          (From<(i64, Option<String>)>)
crates/services/src/task_service.rs:156            triggered_by: "user:api"          (From<(i64, Option<String>, bool)>)
crates/services/src/task_service/claim.rs:250      triggered_by: "agent:claim"       ← ONLY "agent:" emitter
crates/services/src/task_service/transition.rs:399 triggered_by: "system"            (cancel_task)
crates/services/src/task_service/execution/cascade.rs:320        triggered_by: "system"
crates/services/src/task_service/execution/cascade.rs:1292       triggered_by: "system"
crates/services/src/task_service/execution/recovery.rs:829       triggered_by: "user:recovery:proceed_once"
crates/services/src/task_service/execution/recovery.rs:1569      triggered_by: "user:recovery:retry_review"
crates/services/src/workflow/actions/subtasks.rs:289             triggered_by: "system:test"
crates/services/src/task_dispatcher/initial_scheduling.rs:98     triggered_by: "system:task_dispatcher"
crates/api/src/routes/tasks/gates.rs:73            triggered_by: "user:api"
crates/api/src/routes/tasks/gates.rs:113           triggered_by: "user:api"
crates/api/src/routes/tasks/transitions.rs:15      triggered_by = "user:board_drag"  (runtime override)
```

Plus the implicit `"system"` via `From<i64>` at:
```
crates/services/src/task_service/execution/launch.rs:190,365,573
crates/services/src/task_service/execution/follow_up.rs:243
crates/services/src/task_service/subtask.rs and execution/cascade.rs (ordered-child sequencing)
crates/mcp-server/src/tools/handlers.rs:479  (forge_transition_task)
```

## Appendix B — Key file references

| Symbol | Location |
|---|---|
| `TaskService` struct | `crates/services/src/task_service.rs:98` |
| `TransitionOptions` `From` impls | `crates/services/src/task_service.rs:127,139,151` |
| `TaskService::transition` (wrapper) | `crates/services/src/task_service/transition.rs:11` |
| Inline engine reconstruction (×2) | `crates/services/src/task_service/transition.rs:42,423` |
| `cancel_active_execution_for_user_transition` | `crates/services/src/task_service/transition.rs:108` |
| `ensure_planning_plan_ready_before_leaving` | `crates/services/src/task_service/transition.rs:130` |
| Post-transition cleanup block | `crates/services/src/task_service/transition.rs:73-100` |
| `is_awaiting_human` | `crates/services/src/task_service/transition.rs:~170` |
| `should_clear_review_passed_at` | `crates/services/src/task_service/transition.rs:768` |
| `is_user_reachable_transition` | `crates/services/src/task_service/transition.rs:740` |
| `WorkflowEngine::transition` family | `crates/services/src/workflow/engine/mod.rs:69-141` |
| `transition_inner` (lifecycle) | `crates/services/src/workflow/engine/mod.rs:523` |
| `hook_audience_matches` (the divergence) | `crates/services/src/workflow/engine/hooks.rs:95` |
| User entrypoint | `crates/api/src/routes/tasks/transitions.rs:8` |
| MCP entrypoint | `crates/mcp-server/src/tools/handlers.rs:479` |
| Execution completion | `crates/services/src/task_service/execution/runner.rs` (`run_execution`) |
| Sole `"agent:"` emitter | `crates/services/src/task_service/claim.rs:250` |
