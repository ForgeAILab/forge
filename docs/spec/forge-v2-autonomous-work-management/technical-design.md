# Technical Design: Forge V2 — Autonomous Work Management

**Status:** Draft for implementation  
**Date:** 2026-08-08  
**Repository:** `ForgeAILab/forge`

---

## 1. Design objective

Implement the Forge V2 product model without first rewriting the workflow engine or discarding existing execution, review, recovery, worktree, and audit infrastructure.

The technical strategy is:

> **Add a simpler product façade and a new default workflow preset over the existing engine, then incrementally introduce task contracts, canonical phases, policy evaluation, attention queries, and delivery reports.**

This avoids a high-risk “rewrite everything” project while still correcting the product model.

---

## 2. Current architectural assets

The current repository already provides most of the hard primitives needed by Forge V2:

- data-driven workflow definitions and hooks;
- durable tasks and transition logs;
- role assignments and agent claiming;
- execution records and observability;
- task workspaces and per-task worktrees;
- review, CI, merge, and merge-repair services;
- retry budgets and automatic recovery;
- blocked, failed, workflow-health, and workflow-exception metadata;
- task plan artifacts and progress;
- notifications;
- agent and daemon capacity summaries;
- project and global operational views;
- REST, MCP, CLI, and web surfaces;
- extensive backend and Playwright test coverage.

The implementation should reuse these assets. The largest early changes are semantic and presentational, not foundational.

---

## 3. Current constraints and debt relevant to this change

### 3.1 Default workflow is role- and gate-heavy

The current default defines separate planner, coder, and reviewer roles and visible planning, implementation, review, merging, and merge-failure states. Planning can require human approval, and review dispatches a separate agent.

Forge V2 adds a new default workflow rather than immediately mutating all existing project definitions.

### 3.2 Actor identity is not sufficiently typed

The current transition pipeline relies on `triggered_by` strings and prefix matching to distinguish user, agent, and system behavior. Forge V2 adds more actor-aware policy and attention behavior, so the typed actor refactor should be treated as an early safety dependency rather than deferred cleanup.

### 3.3 `awaiting_human` is already available but derived through multiple signals

The API already exposes `awaiting_human`, blocked and failed metadata, workflow exceptions, and execution observability. These are sufficient to build a first Home/Attention surface before the complete policy model is implemented.

### 3.4 Operations is implementation-oriented

The current global Operations page exposes active execution IDs, workspace paths, roles, daemon IDs, agent pressure, retry pressure, and other operator information. Forge V2 should preserve this data but build a user-oriented Home façade over it.

### 3.5 Navigation exposes infrastructure

The current global navigation includes Agents, Daemons, Operations, and System Settings as peer destinations. Forge V2 moves runtime and operator surfaces under Settings or an advanced section.

---

## 4. Architectural approach

### 4.1 Avoid a full engine rewrite

The workflow engine remains the authoritative state transition mechanism.

Initial work should introduce:

- a new `autonomous_v1` workflow preset;
- canonical phase mapping;
- façade service methods for user-intent actions;
- attention and work queries;
- simplified UI routes;
- terminology mapping from execution → run and daemon → runtime.

### 4.2 Separate product actions from raw transitions

The UI should call intent-oriented operations such as:

- start work;
- pause work;
- submit for review;
- request changes;
- approve delivery;
- transfer task;
- retry verification;
- accept exception.

Internally, these operations may continue to call workflow transitions and hooks.

This prevents the UI from needing to know exact state names or transition graph details.

### 4.3 Preserve raw APIs for advanced users

Existing transition, workflow, and daemon APIs remain supported. New façade endpoints supplement rather than immediately replace them.

---

## 5. Terminology mapping

| Existing backend term | Default user-facing term | Treatment |
|---|---|---|
| Execution | Run | Copy and route labels change first; backend rename is optional and deferred. |
| Daemon | Runtime / Compute | Backend type remains daemon initially. |
| Workflow state | Phase / activity | Exact state is advanced diagnostic metadata. |
| Role assignment | Assignee / reviewer | Advanced workflows may still expose roles. |
| Review runner | Forge verification / independent review | Distinguish deterministic checks from semantic review. |
| Workspace/worktree | Isolated workspace | Usually hidden; visible under Changes or Diagnostics. |
| Blocked JSON / interruption metadata | Attention reason | Normalized by attention service. |

---

## 6. New and extended domain types

The examples below describe target Rust API types. Names may be adjusted to existing crate conventions.

### 6.1 Canonical phase

Add to `crates/api-types/src/workflow.rs`:

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CanonicalPhase {
    Backlog,
    Ready,
    Working,
    Review,
    Done,
}
```

Extend `StateDefinition`:

```rust
#[serde(default)]
pub canonical_phase: Option<CanonicalPhase>,
```

Use `Option` during migration so legacy workflow JSON remains readable.

Provide a fallback derivation function:

```rust
pub fn canonical_phase_for_state(&self, status: &str) -> CanonicalPhase
```

Fallback order:

1. explicit `state.canonical_phase`;
2. normalized `state.column` name;
3. known legacy state-name mapping;
4. `StateKind` heuristic;
5. default `Working` with warning telemetry for unknown non-terminal states.

Do not persist canonical phase on every task initially. It is derived from project workflow + task status, preventing drift.

### 6.2 Risk level

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}
```

Default: `Medium` for code tasks, `Low` for documentation or explicitly non-code tasks where detectable.

### 6.3 Task contract

```rust
#[derive(Debug, Clone, Serialize, Deserialize, TS, Default, PartialEq)]
pub struct TaskContract {
    pub objective: Option<String>,
    pub acceptance_criteria: Vec<AcceptanceCriterion>,
    pub allowed_paths: Vec<String>,
    pub protected_paths: Vec<String>,
    pub validation_profile_id: Option<String>,
    pub risk_level: Option<RiskLevel>,
    pub budget: Option<TaskBudget>,
    pub permission_policy: Option<String>,
    pub merge_policy: Option<MergePolicy>,
    #[ts(type = "Record<string, unknown>")]
    pub extensions: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
pub struct AcceptanceCriterion {
    pub id: String,
    pub text: String,
    pub status: AcceptanceStatus,
    pub evidence: Vec<String>,
}
```

For the first release, store the contract as JSON on the task. Normalize only fields needed for high-volume queries.

### 6.4 Project autonomy policy

```rust
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
pub struct AutonomyPolicy {
    pub preset: AutonomyPreset,
    pub planning_gate: GatePolicy,
    pub independent_review: GatePolicy,
    pub human_review: GatePolicy,
    pub merge_policy: MergePolicy,
    pub validation_required: bool,
    pub same_agent_recovery_attempts: u32,
    pub transfer_after_exhaustion: bool,
    pub risk_rules: Vec<RiskRule>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyPreset {
    Standard,
    Trusted,
    Strict,
    Custom,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatePolicy {
    Never,
    RiskBased,
    Always,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MergePolicy {
    Manual,
    AutoLowRisk,
    AutoWhenVerified,
    NoMerge,
}
```

Store under project settings JSON initially. Keep a stable typed serializer so the field can later be normalized without API breakage.

### 6.5 Validation profile

```rust
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
pub struct ValidationProfile {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub description: Option<String>,
    pub checks: Vec<ValidationCheck>,
    pub is_default: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ValidationCheck {
    Command {
        id: String,
        label: String,
        command: String,
        timeout_seconds: u64,
        required: bool,
    },
    GitProviderCheck {
        id: String,
        label: String,
        context: String,
        required: bool,
    },
    ScopeCheck {
        id: String,
        required: bool,
    },
    DiffPolicyCheck {
        id: String,
        policy: String,
        required: bool,
    },
}
```

The existing `ci_steps` or review configuration may be adapted into the first validation profile instead of introducing duplicate command execution.

### 6.6 Attention item

Attention should be returned as a derived DTO rather than persisted as mutable task truth.

```rust
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
pub struct AttentionItem {
    pub id: String,
    pub task_id: String,
    pub project_id: String,
    pub kind: AttentionKind,
    pub severity: AttentionSeverity,
    pub title: String,
    pub summary: String,
    pub recommended_actions: Vec<AttentionAction>,
    pub created_at: String,
    pub updated_at: String,
}
```

Kinds:

- `question`;
- `human_review`;
- `blocked_dependency`;
- `validation_failed`;
- `retry_exhausted`;
- `runtime_unavailable`;
- `permission_required`;
- `scope_expansion`;
- `merge_conflict`;
- `workflow_exception`;
- `budget_exceeded`.

### 6.7 Delivery report

```rust
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
pub struct DeliveryReport {
    pub id: String,
    pub task_id: String,
    pub version: i64,
    pub primary_execution_id: Option<String>,
    pub agent_id: Option<String>,
    pub repository: DeliveryRepository,
    pub requested_scope: DeliveryScope,
    pub actual_scope: DeliveryScope,
    pub diff: DiffSummary,
    pub commits: Vec<CommitSummary>,
    pub agent_validation: Vec<ValidationEvidence>,
    pub forge_validation: Vec<ValidationEvidence>,
    pub independent_reviews: Vec<IndependentReviewSummary>,
    pub exceptions: Vec<DeliveryException>,
    pub risk_level: RiskLevel,
    pub recommended_disposition: DeliveryDisposition,
    pub outcome: Option<DeliveryOutcome>,
    pub created_at: String,
}
```

Delivery reports are versioned snapshots. Avoid reconstructing old review evidence from mutable current task state.
`DeliveryRepository` is populated from the execution Workspace/lease, not from
the Task or the Project's later current repository selection.

---

## 7. Persistence changes

The original V2 slices below assumed the repository ended at `V058`. Their
historical V059–V064 labels remain as the proposal sequence; the later
Project-owned repository-authority correction uses the actual current V138
number and does not edit an earlier migration.

### V059 — canonical workflow phase

No task table change required.

Actions:

- update serialized workflow type to support optional `canonical_phase`;
- backfill built-in default workflow definitions when read or migrated;
- add workflow validation requiring canonical phases for new custom workflow saves;
- maintain fallback derivation for legacy JSON.

### V060 — task contract

```sql
ALTER TABLE task ADD COLUMN contract_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE task ADD COLUMN risk_level TEXT;
ALTER TABLE task ADD COLUMN validation_profile_id TEXT;
```

Rationale:

- contract remains flexible JSON;
- risk and validation profile are normalized for filtering and policy queries;
- merge policy can remain inside contract until query requirements justify a column.

### V061 — validation profile

```sql
CREATE TABLE validation_profile (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  description TEXT,
  checks_json TEXT NOT NULL,
  is_default INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX validation_profile_project_name
ON validation_profile(project_id, name);
```

Ensure only one default profile per project through service-level transaction or partial index where supported.

### V062 — delivery reports

```sql
CREATE TABLE delivery_report (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES task(id) ON DELETE CASCADE,
  version INTEGER NOT NULL,
  execution_id TEXT,
  report_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(task_id, version)
);

CREATE INDEX delivery_report_task_created
ON delivery_report(task_id, created_at DESC);
```

### V063 — optional task owner and reviewer normalization

Only introduce if the current polymorphic assignee and role-assignment model cannot express the desired UI cleanly.

Prefer initially:

- `task.assignee_type` / `task.assignee_id` as primary assignee;
- role assignments retained for advanced workflow roles;
- review requests derived from gate state and policy.

Avoid unnecessary schema churn.

### V064 — decision/question records, if existing comments are insufficient

A first implementation may use structured task comments plus task interruption metadata. Introduce a dedicated decision record only if reliable open/resolved question queries cannot be implemented otherwise.

### V138 — Project-owned Task repository authority

Remove `task.repo_id` and its index in one forward-only, data-preserving
migration. A Task does not select or snapshot a repository. Before each new
execution, TaskService resolves `project.primary_repo_id`, requires the Repo to
belong to that Project, and exposes a typed setup blocker when it is missing or
invalid. Lease insertion and renewal require that Project selection to match
the execution Workspace and the lease's `repository_binding_id`.

Do not rewrite Task versions or migrate the discarded Task value into another
selector. Preserve Workspace, execution, lease, delivery/review, pull-request,
evidence, release, and unselected legacy Repo rows; those records remain
attempt-level or historical provenance. Removing repository setup no longer
cascades Task deletion.

---

## 8. Workflow design

### 8.1 New built-in preset

Add:

```text
crates/services/src/workflow/default_autonomous_workflow.rs
```

Preset identifier:

```text
autonomous_v1
```

Visible phase mapping:

| Internal state | Kind | Canonical phase | Role |
|---|---|---|---|
| `backlog` | Backlog | Backlog | — |
| `ready` | Initial | Ready | — |
| `working` | Active | Working | `worker` |
| `review` | Gate | Review | — by default |
| `merging` | Gate | Review | — |
| `merge_failed` | Active | Review | `worker` |
| `done` | Terminal | Done | — |
| `cancelled` | Terminal | Done | — |

### 8.2 Worker behavior

`working.on_enter` dispatches the primary `worker` agent.

The worker prompt builder instructs the agent to:

1. inspect the task contract and repository;
2. plan internally and keep the plan updated;
3. implement the work;
4. run relevant self-validation;
5. repair failures within the task context;
6. stop and ask a structured question when a safe decision cannot be inferred;
7. report completion evidence, uncertainty, and scope changes.

Planning artifacts remain available but no longer gate ordinary progress.

### 8.3 Validation behavior

Before entering `review`, Forge runs the resolved validation profile.

On failure:

- do not create a terminal task failure;
- record validation evidence;
- dispatch a follow-up to the same worker thread using `ResumeLatestTargetRoleThread`;
- decrement the verification recovery budget;
- remain in or return to Working;
- create attention only when budget or policy requires it.

### 8.4 Review behavior

The review state no longer automatically dispatches a reviewer in Standard mode.

The policy evaluator determines:

- whether independent review is required;
- whether human approval is required;
- whether the task can automatically advance to merge.

High-risk tasks may dispatch a reviewer agent on entering review. Standard medium-risk tasks wait for human approval after validation.

### 8.5 Merge behavior

Keep `merging` and `merge_failed` as internal states during the incremental implementation.

Both map to canonical Review, so users do not receive separate board columns.

Automatic merge repair uses the same worker thread where practical. If repair fails or exhausts budget, derive a `merge_conflict` attention item.

### 8.6 Strict preset

Preserve the current workflow as:

```text
strict_multi_agent_v1
```

Existing projects retain their current serialized workflow. New project templates can explicitly select Strict.

---

## 9. Workflow preset management

Add a built-in workflow registry entry with:

```rust
pub enum WorkflowPresetId {
    AutonomousV1,
    StrictMultiAgentV1,
    Custom,
}
```

Project metadata stores the selected preset for display and migration assistance. The serialized workflow remains authoritative so custom edits are preserved.

Project creation defaults:

- preview period: configurable server default;
- target state: `autonomous_v1` for new projects;
- existing projects: unchanged.

Migration action:

```text
POST /api/v1/projects/{project_id}/workflow/migrate/autonomous-v1
```

The migration must:

- refuse while active tasks are in states with no safe mapping unless `force` with reason;
- preview state mappings;
- preserve task history and transition logs;
- update project workflow atomically;
- map current task statuses;
- retain role assignments as archived/advanced metadata where possible;
- produce a migration report.

---

## 10. Typed actor dependency

Before implementing risk- or actor-specific hooks, replace free-form transition actor control signals with a typed actor.

Suggested type:

```rust
pub enum Actor {
    User { user_id: String, source: UserActionSource },
    Agent { agent_id: String, execution_id: Option<String> },
    System { component: SystemComponent },
}
```

Keep a separate human-readable `reason` field.

Forge's current single-user product represents the user variant as
`User { user_id: None, source }`. The user id remains intentionally unset
until the multi-user story is implemented; action sources and audit display
formats are still typed and preserved.

Benefits:

- correct hook audience semantics;
- reliable audit attribution;
- policy decisions based on actor type;
- accurate agent-initiated question and transition handling;
- removal of string-prefix checks.

This refactor should be completed before Trusted auto-merge and risk-based actor policy are enabled.

---

## 11. Attention derivation service

Add:

```text
crates/services/src/attention_service.rs
```

Inputs:

- task response/model;
- current workflow definition and state;
- latest execution and stop reason;
- interruption metadata;
- workflow health and exception;
- review status;
- runtime availability;
- dependency status;
- policy evaluation;
- validation retry budget;
- open structured questions.

Output:

- zero or more normalized `AttentionItem`s;
- one primary attention item for card display;
- recommended actions that map to existing or new APIs.

Priority order when multiple conditions exist:

1. critical permission/security decision;
2. explicit agent question;
3. human approval required;
4. blocked dependency;
5. exhausted validation/recovery;
6. unresolved merge conflict;
7. runtime unavailable;
8. budget exceeded;
9. workflow exception;
10. informational warning.

The service should be deterministic and covered by table-driven tests.

---

## 12. Policy evaluation service

Add:

```text
crates/services/src/policy_service.rs
```

Responsibilities:

- resolve project policy + task override;
- infer minimum risk from paths and change evidence;
- determine pre-execution approval requirement;
- determine independent review requirement;
- determine human final approval requirement;
- determine auto-merge eligibility;
- emit reasons suitable for UI and audit logs.

Suggested result:

```rust
pub struct EffectiveTaskPolicy {
    pub risk_level: RiskLevel,
    pub planning_gate_required: bool,
    pub independent_review_required: bool,
    pub human_review_required: bool,
    pub auto_merge_allowed: bool,
    pub validation_profile_id: Option<String>,
    pub reasons: Vec<PolicyReason>,
}
```

Policy evaluation must be pure where possible. Side effects occur in workflow actions, not during evaluation.

---

## 13. Validation service

Refactor or wrap existing CI/review command execution as:

```text
crates/services/src/validation_service.rs
```

Requirements:

- resolve effective validation profile;
- execute checks against a recorded workspace/commit;
- record command, environment summary, duration, exit status, logs, and required/optional status;
- compare actual changed paths with task contract scope;
- prevent the worker from silently redefining required checks;
- support retry with the same agent thread;
- expose immutable evidence to delivery reports.

Do not remove existing review runner functionality until the validation façade has parity.

---

## 14. Delivery report service

Add:

```text
crates/services/src/delivery_report_service.rs
```

Generation triggers:

- successful external validation before review;
- resubmission after requested changes;
- independent review completion when required;
- final merge or explicit acceptance updates outcome through a new report version or immutable outcome record.

Inputs:

- task and contract;
- execution Workspace and its attempt-pinned repository;
- Git diff and commits;
- execution metadata;
- validation results;
- review results;
- PR provider state;
- policy decision.

The service should fail review submission when required provenance cannot be established, unless an authorized exception is recorded.

---

## 15. API design

Existing endpoints remain. Add user-intent façades and global queries.

### 15.1 Global Home and Work

```text
GET /api/v1/home
GET /api/v1/work
GET /api/v1/attention
GET /api/v1/review-queue
```

`GET /api/v1/home` returns bounded sections:

```json
{
  "needs_attention": [],
  "awaiting_review": [],
  "running": [],
  "recent_deliveries": [],
  "system_health": {}
}
```

`GET /api/v1/work` supports pagination and filters:

```text
project_id
canonical_phase
attention_kind
assignee_type
assignee_id
risk_level
validation_status
updated_after
sort
cursor
limit
```

### 15.2 Task intent actions

```text
POST /api/v1/tasks/{task_id}/start
POST /api/v1/tasks/{task_id}/pause
POST /api/v1/tasks/{task_id}/resume
POST /api/v1/tasks/{task_id}/submit
POST /api/v1/tasks/{task_id}/request-changes
POST /api/v1/tasks/{task_id}/approve
POST /api/v1/tasks/{task_id}/transfer
POST /api/v1/tasks/{task_id}/accept-exception
```

These endpoints resolve the project workflow and issue the correct transitions, follow-ups, or recoveries.

### 15.3 Contract and policy

```text
GET  /api/v1/tasks/{task_id}/contract
PUT  /api/v1/tasks/{task_id}/contract
GET  /api/v1/tasks/{task_id}/effective-policy
GET  /api/v1/projects/{project_id}/autonomy-policy
PUT  /api/v1/projects/{project_id}/autonomy-policy
```

### 15.4 Validation profiles

```text
GET    /api/v1/projects/{project_id}/validation-profiles
POST   /api/v1/projects/{project_id}/validation-profiles
GET    /api/v1/validation-profiles/{profile_id}
PUT    /api/v1/validation-profiles/{profile_id}
DELETE /api/v1/validation-profiles/{profile_id}
POST   /api/v1/validation-profiles/{profile_id}/test
```

### 15.5 Delivery reports

```text
GET /api/v1/tasks/{task_id}/deliveries
GET /api/v1/deliveries/{delivery_id}
```

### 15.6 Runtime copy compatibility

Keep existing `/daemons` endpoints. A future alias `/runtimes` may be introduced, but renaming backend routes is not required for the first product release.

---

## 16. Response DTO changes

Extend `TaskResponse` with additive fields:

```rust
pub canonical_phase: CanonicalPhase,
pub primary_attention: Option<AttentionItem>,
pub risk_level: RiskLevel,
pub effective_policy: EffectiveTaskPolicySummary,
pub latest_delivery: Option<DeliveryReportSummary>,
pub current_run: Option<RunSummary>,
```

Keep existing fields during migration:

- `status`;
- `awaiting_human`;
- `blocked`;
- `failed`;
- `workflow_health`;
- `workflow_exception`;
- `execution_observability`;
- `plan_progress`;
- `plan_artifact`.

The separate Project-repository correction is a public-beta breaking change:
remove `repo_id` from `TaskResponse` and generated Task bindings. Do not retain
an alias. Repository context for a completed or running attempt comes from its
Workspace/lease and delivery provenance; pre-execution selection comes only
from `project.primary_repo_id`.

The web client can progressively switch to the new summary fields.

---

## 17. Backend code map

### API types

- `crates/api-types/src/workflow.rs`
  - canonical phase;
  - autonomy policy;
  - workflow preset metadata.
- `crates/api-types/src/requests.rs`
  - task contract and intent-action requests.
- `crates/api-types/src/core.rs` or new modules
  - attention, delivery, validation, risk DTOs.
- regenerate `crates/api-types/bindings/` and `web/src/types/generated/`.

### Database

- `crates/db/migrations/`
  - contract, risk, validation profile, delivery report.
- `crates/db/src/models.rs`
  - new models.
- `crates/db/src/repository.rs` and `crates/db/src/sqlite/`
  - CRUD and queries.

### Services

- `crates/services/src/workflow/default_autonomous_workflow.rs`
- `crates/services/src/workflow/registry.rs`
- `crates/services/src/workflow/template_service.rs`
- `crates/services/src/task_service/transition.rs`
  - intent façades and typed actor integration.
- `crates/services/src/task_service/execution/launch.rs`
  - worker-role default.
- `crates/services/src/task_service/execution/follow_up.rs`
  - same-thread validation and requested-change recovery.
- `crates/services/src/task_service/execution/recovery.rs`
  - transfer package and exhaustion behavior.
- `crates/services/src/task_diagnostics.rs`
  - support attention derivation or retire overlapping logic.
- `crates/services/src/operator_status.rs`
  - reuse for Home data; separate operator diagnostics from user work overview.
- new `attention_service.rs`.
- new `policy_service.rs`.
- new `validation_service.rs`.
- new `delivery_report_service.rs`.

### API routes

- add `home.rs`, `work.rs`, `attention.rs`, `delivery.rs`, `validation_profiles.rs`;
- add intent-oriented task action routes;
- preserve raw task transition routes.

### MCP and CLI

Expose equivalent intent tools over time:

```text
forge_start_task
forge_submit_task
forge_request_changes
forge_approve_delivery
forge_transfer_task
forge_list_attention
```

Keep raw transition tools for compatibility. Ensure typed actor attribution for MCP agents.

---

## 18. Frontend code map

### Navigation

Update `web/src/components/app-shell.tsx`:

- add Home and Work;
- add Projects landing route;
- retain Agents;
- move Daemons and Operations under Settings/Advanced;
- preserve project switcher;
- rename daemon copy to runtime in normal surfaces.

### New pages

```text
web/src/pages/HomePage.tsx
web/src/pages/WorkPage.tsx
web/src/pages/ReviewQueuePage.tsx   # optional separate route; may be a saved Work view
web/src/pages/ProjectsPage.tsx
```

### Reuse and refactor

- `OperationsPage.tsx`
  - extract data components;
  - create user-facing Home sections;
  - retain an Operator Diagnostics route under Settings.
- `TaskListPage.tsx`
  - reuse filtering/table primitives for Work.
- `features/board/`
  - support canonical-phase data and global board query.
- `TaskDetailPage.tsx`
  - restructure around Overview, Activity, Changes, Checks, Runs.
- `ExecutionDetailPage.tsx`
  - keep as Run Diagnostics and update copy.
- `AgentsPage.tsx`
  - roster-first layout.
- `DaemonsPage.tsx`
  - route under Settings → Compute; update user-facing copy.
- `task-create-dialog.tsx`
  - basic/advanced progressive disclosure.

### New components

```text
attention-card.tsx
attention-badge.tsx
run-summary.tsx
delivery-report.tsx
validation-summary.tsx
risk-badge.tsx
effective-policy-summary.tsx
task-contract-editor.tsx
work-filters.tsx
saved-view-picker.tsx
```

### State and query keys

Add API hooks and query keys for:

- home;
- work;
- attention;
- review queue;
- task contract;
- effective policy;
- deliveries;
- validation profiles.

SSE invalidation should update these aggregated views when task, execution, review, runtime, or delivery events occur.

---

## 19. Project creation and onboarding

### 19.1 Project creation request

Extend project creation to accept:

```json
{
  "name": "Forge",
  "workflow_preset": "autonomous_v1",
  "autonomy_policy": { "preset": "standard" }
}
```

For backward compatibility, fields are optional and server defaults apply.

### 19.2 Default agent behavior

Current bootstrap creates default agents for registered executors. Preserve this, but improve display and routing:

- present detected executor defaults as ready-to-use agents;
- allow a project to use `Automatic` agent recommendation;
- do not require binding every workflow role;
- choose runtime automatically when one eligible route exists.

### 19.3 Validation detection

A later onboarding enhancement can inspect repository files and suggest profiles:

- `Cargo.toml` → `cargo test`, `cargo clippy`, `cargo fmt --check`;
- `package.json` → existing scripts;
- Python config → test/lint commands;
- repository CI workflow hints.

The user must confirm commands before they become required external validation.

---

## 20. Same-thread recovery

The current execution policy already supports resuming the latest target-role thread. Forge V2 makes this the default for:

- external validation failures;
- requested changes;
- merge repair;
- user follow-up within the same task;
- recoverable permission or environment corrections.

Create a `TaskRecoveryContext` builder that assembles:

```text
Task objective and contract
Current plan summary
Worktree and branch
Current diff
Latest validation output
Review feedback
Prior stop reason
Open question resolution
Remaining budget
```

Do not rely on the agent reconstructing context from a raw activity stream.

---

## 21. Transfer between agents

Add a transfer service operation rather than creating a fresh unrelated execution.

Requirements:

- preserve task and workspace;
- record source and destination agents;
- produce a structured transfer summary;
- allow the destination to inspect prior raw logs but receive a concise primary context;
- validate executor compatibility with the workspace and project;
- prevent concurrent agents from mutating the same task workspace unless an explicit parallel-exploration mode is used;
- create an audit event.

Suggested endpoint request:

```json
{
  "agent_id": "new-agent-id",
  "reason": "Primary agent exhausted validation retries",
  "strategy": "continue_workspace"
}
```

---

## 22. Concurrency and conflict management

### 22.1 Task-level isolation

Keep one worktree per task.

### 22.2 Same-task execution lock

Only one mutating run should own the task workspace at a time by default.

Read-only reviewer runs may inspect a snapshot without mutating it.

### 22.3 Cross-task conflict prediction

A future enhancement may compare changed or intended paths across active tasks and surface likely merge conflicts. It is not required for the first release.

### 22.4 Merge ordering

Tasks with dependencies or overlapping changes should respect a merge queue or require rebasing and revalidation against the latest base.

Delivery reports must record the base commit used for validation. If the base changes and policy requires freshness, revalidation occurs before merge.

---

## 23. Security and policy boundaries

### 23.1 Worktree is not a security sandbox

Continue to treat worktree isolation as collision prevention, not a complete security boundary.

### 23.2 Permission policy

Task/agent effective permissions should control:

- filesystem scope;
- network access;
- secret access;
- shell commands where applicable;
- repository push and merge;
- external service mutation.

### 23.3 Human approval

Require explicit approval for:

- critical-risk execution;
- secret access outside established policy;
- production or destructive operations;
- lowering automatically elevated risk;
- accepting required validation exceptions;
- auto-merge policy changes.

### 23.4 Audit

Record:

- typed actor;
- policy decision and reasons;
- contract version;
- validation profile version;
- approval or exception reason;
- transfer events;
- delivery report version;
- merge outcome.

---

## 24. Observability

### 24.1 User observability

Expose:

- current activity;
- elapsed time;
- concise progress;
- changed files;
- validation state;
- attention reason;
- review readiness.

### 24.2 Operator observability

Retain:

- execution IDs;
- sessions;
- daemon/runtime pressure;
- workspaces;
- token and rate-limit details;
- cleanup queues;
- retry pressure;
- raw errors.

These belong under Operator Diagnostics.

### 24.3 Product metrics

Store local aggregate counters by default if telemetry remains disabled:

- tasks started/completed;
- one-agent completion;
- validation retries;
- human attention events;
- transfers;
- review dispositions;
- merge outcomes.

If external telemetry is ever added, it must remain explicit and privacy-preserving.

---

## 25. Backward compatibility

### 25.1 Existing projects

- workflow JSON remains authoritative;
- missing canonical phase is derived;
- existing statuses continue to render;
- old planner/coder/reviewer assignments remain valid;
- existing URLs remain functional;
- new UI may display legacy state under Advanced.

### 25.2 Existing API clients

- additive response fields only in the first V2 release, except for the
  coordinated removal of Task `repo_id` described above;
- existing task transition endpoints remain;
- execution and daemon route names remain;
- generated TypeScript Task bindings remove `repo_id` with no compatibility
  field;
- CLI output changes should support a compatibility mode if scripts parse exact columns.

### 25.3 Existing tasks during migration

Do not migrate active task status silently.

Provide preview mapping and block unsafe migration when:

- task is in planning awaiting a human decision;
- task has an active execution whose role does not map cleanly;
- review or merge operation is in flight;
- custom hooks depend on removed state names.

Allow project migration after active tasks complete as the default recommendation.

---

## 26. Feature flags and rollout controls

Suggested settings:

```text
ui.work_management_v2
workflow.autonomous_v1_enabled
workflow.new_project_default
policy.risk_based_gates_enabled
policy.trusted_auto_merge_enabled
runtime.user_facing_terminology_v2
```

Feature flags should be server-provided capabilities rather than hardcoded frontend environment checks where possible.

---

## 27. Testing strategy

### 27.1 Unit tests

- canonical phase fallback mapping;
- policy evaluator;
- attention derivation;
- task-contract validation;
- validation profile resolution;
- delivery report versioning;
- workflow preset validation;
- typed actor audience matching.

### 27.2 Backend integration tests

- autonomous happy path;
- same-thread recovery after check failure;
- request changes;
- transfer agent;
- risk-based independent review;
- human approval;
- auto-merge low risk;
- merge conflict attention;
- runtime failover;
- legacy workflow compatibility;
- project migration preview and execution.

### 27.3 Frontend component tests

- attention cards;
- delivery report;
- task contract editor;
- risk and policy controls;
- Home sections;
- Work filters;
- canonical board cards.

### 27.4 Playwright

Add end-to-end scenarios described in `acceptance-tests.md`.

### 27.5 Migration tests

- database migration from a representative V058 fixture;
- legacy workflow JSON without canonical phases;
- existing active and terminal tasks;
- custom workflow preservation;
- rollback behavior when migration preview detects unsafe states.

---

## 28. Implementation slices

The architecture supports incremental delivery:

### Slice A — no schema changes

- autonomous workflow preset;
- canonical phase derived in API;
- Home façade from existing operations data;
- Work query using existing tasks;
- navigation and terminology changes;
- simplified task UI.

### Slice B — task contract and policy

- contract JSON;
- risk;
- project autonomy policy;
- effective policy summary;
- basic scope validation.

### Slice C — validation profiles and delivery report

- normalized validation profiles;
- external evidence persistence;
- review redesign.

### Slice D — conditional review and trusted automation

- risk rules;
- independent reviewer dispatch;
- auto-merge low risk;
- agent transfer and adaptive escalation.

This sequencing allows UX validation before deep policy automation.

---

## 29. Technical acceptance criteria

1. The new autonomous workflow is registered, validated, and selectable without modifying existing project workflow JSON.
2. Every workflow state returned to the new UI resolves to one canonical phase.
3. A Standard task uses one worker agent from start through implementation and self-test.
4. External validation failure resumes the same worker thread by default.
5. A failed run does not force a terminal task state.
6. Home and Work APIs query existing durable tasks rather than copying them.
7. Attention items are deterministic and identify a recommended action.
8. Delivery reports are versioned and preserve the exact validation and commit evidence used for review.
9. Runtime/daemon failures can be translated into affected-agent/task messages.
10. Existing strict and custom workflows pass their current integration tests.
11. Typed actor attribution is in place before risk-based auto actions are enabled.
12. Project migration is previewable, atomic, auditable, and safe for existing history.
13. REST, MCP, CLI, and web clients continue to operate against legacy workflows.
14. All new API types regenerate clean TypeScript bindings.
15. The Playwright suite covers both autonomous and legacy project paths.
16. Tasks accepted before repository attachment become runnable after valid
    Project setup without Task mutation, while attempt Workspaces keep their
    original repository provenance.
