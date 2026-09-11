# Acceptance Test Plan: Forge V2 — Autonomous Work Management

**Status:** Draft  
**Date:** 2026-08-08

This test plan defines product-level acceptance, not only code-level correctness. The initiative is not complete if internal workflow tests pass but users still need to understand planner/coder/reviewer/daemon mechanics to delegate ordinary work.

---

## 1. Test environments

Maintain at least these fixtures:

### E1 — New autonomous local project

- one local repository;
- one eligible runtime;
- two detected coding-agent executors;
- Standard autonomy policy;
- default validation profile;
- no existing tasks.

### E2 — Multi-project workspace

- three projects;
- three selected primary repositories plus one preserved, unselected legacy
  Repo row;
- multiple agents and runtimes;
- active, review, blocked, failed-run, and done tasks;
- overlapping project names and labels to test contextual display.

### E3 — Legacy strict project

- current planner/coder/reviewer workflow;
- active planning task awaiting approval;
- active coder task;
- review task;
- merge-failed task;
- custom hooks;
- transition and execution history.

### E4 — Custom workflow project

- nonstandard state names;
- explicit canonical mappings for some states;
- legacy states without mappings for fallback testing;
- user and agent role assignments.

### E5 — Failure and recovery project

- validation command that fails once then passes;
- offline preferred runtime with eligible fallback;
- merge-conflict fixture;
- agent that stops unexpectedly;
- retry budgets configured at project and task level.

### E6 — High-risk project

- protected auth, payment, migration, and infrastructure paths;
- Trusted autonomy policy;
- independent reviewer configured;
- auto-merge enabled only for eligible low-risk work.

---

## 2. Product acceptance scenarios

## A-001 — First task without workflow knowledge

**Given** a fresh Forge installation with one detected coding agent  
**When** the user creates a project, connects a repository, enters a task title, accepts the recommended agent, and starts  
**Then**:

- no daemon selection is required;
- no planner/coder/reviewer assignment is required;
- no workflow editor is shown;
- a task is created in Ready and begins Working;
- one primary agent receives the task;
- the task page shows a concise activity state;
- the user can reach review without opening advanced settings.

**Failure conditions:** any mandatory raw workflow, daemon, or role configuration.

## A-002 — One agent owns the complete normal task

**Given** an autonomous Standard project and a medium-risk task  
**When** the primary agent analyzes, plans, edits, self-tests, and reports completion  
**Then**:

- the same primary agent owns all normal work;
- planning progress may be visible but is not a human gate;
- no reviewer agent is launched automatically;
- Forge independently runs required validation;
- successful validation produces a delivery report;
- the task enters Review awaiting human approval.

## A-003 — Five-phase board

**Given** autonomous, strict, and custom project tasks  
**When** they are displayed on project and global boards  
**Then** every task appears in exactly one of:

- Backlog;
- Ready;
- Working;
- Review;
- Done.

Planning, testing, retrying, merging, and merge failure may appear as activity or attention labels, but not mandatory additional global columns.

## A-004 — Needs attention is derived, not duplicated

**Given** a task whose agent asks a question  
**When** the question is open  
**Then** the task appears in Home → Needs attention with kind `question`.

**When** the user answers and the agent resumes  
**Then** the question attention disappears without manually moving the card or clearing a separate task state.

## A-005 — External validation catches false completion

**Given** an agent reports success but a required Forge validation command fails  
**When** the agent submits work  
**Then**:

- the task does not become Review-ready or Done;
- failed command, exit status, duration, commit/workspace, and logs are recorded;
- the same worker context receives the failure evidence;
- retry budget decreases;
- the activity timeline distinguishes agent self-validation from Forge validation.

## A-006 — Same-context repair

**Given** A-005 and remaining retry budget  
**When** the worker fixes the failure and resubmits  
**Then**:

- task ID and workspace remain unchanged;
- the worker resumes the existing task context;
- a new validation attempt is recorded;
- the successful submission creates delivery report version 1;
- prior failed evidence remains inspectable.

## A-007 — Exhausted validation recovery

**Given** repeated required validation failure and exhausted budget  
**When** automatic recovery stops  
**Then**:

- the durable task remains active;
- the latest run may be failed/stopped;
- Home shows one `retry_exhausted` or `validation_failed` attention item;
- available actions include continue same agent, transfer, change contract/profile, accept authorized exception, or cancel;
- the UI does not display a generic terminal “task failed” without recovery options.

## A-008 — Request changes resumes worker

**Given** a review-ready task with delivery report version 1  
**When** the human requests changes with feedback  
**Then**:

- the task returns to Working;
- the same worker thread resumes by default;
- feedback, prior diff, contract, and validation evidence are included;
- delivery report version 1 remains immutable;
- resubmission creates delivery report version 2.

## A-009 — Transfer task between agents

**Given** a task with a failed worker run, existing diff, decisions, and test output  
**When** the user transfers it to another compatible agent  
**Then**:

- task ID, project, contract, workspace, and history remain;
- destination receives a structured transfer summary;
- source and destination agents are audited;
- only one mutating agent owns the workspace;
- the new run is associated with the same task.

## A-010 — Low-risk Trusted auto-merge

**Given** Trusted policy, low effective risk, no protected paths, current base, all required validations passed, and no exception  
**When** work is submitted  
**Then**:

- independent review and human approval are skipped if policy says so;
- merge occurs automatically;
- delivery report records policy reasons and outcome;
- task becomes Done.

## A-011 — Auto-merge denied after risk elevation

**Given** Trusted policy and a task initially marked low risk  
**When** the diff modifies a protected authentication path  
**Then**:

- effective risk is elevated;
- auto-merge is denied;
- required independent and human review are applied;
- policy reasons identify the protected path rule;
- lowering risk requires an explicit authorized reason.

## A-012 — High-risk independent review

**Given** a high-risk task with successful Forge validation  
**When** it reaches Review  
**Then**:

- an independent reviewer is dispatched according to policy;
- the reviewer operates read-only by default;
- reviewer findings are structured and attached to the delivery report;
- rejection returns feedback to the worker;
- acceptance still requires human approval.

## A-013 — Runtime outage with fallback

**Given** an active task whose preferred runtime goes offline and another eligible runtime can access the repository safely  
**When** Forge detects the outage  
**Then**:

- Forge explains the affected agent and task;
- it may recommend or perform policy-allowed recovery;
- no user-facing primary message consists only of daemon ID or heartbeat count;
- runtime transfer is audited;
- workspace integrity is preserved.

## A-014 — Runtime outage without fallback

**Given** no eligible fallback runtime  
**When** the active runtime goes offline  
**Then**:

- the task receives `runtime_unavailable` attention;
- Home shows a reconnect or transfer action;
- technical details are expandable;
- the task remains durable and resumable.

## A-015 — Merge conflict automatic repair

**Given** a review-approved task with a merge conflict and available repair budget  
**When** merge begins  
**Then**:

- task remains in canonical Review;
- Forge attempts repair using the worker context when compatible;
- successful repair triggers required revalidation;
- only a verified repaired commit can merge.

## A-016 — Merge conflict exhaustion

**Given** merge repair fails or exhausts budget  
**Then**:

- task remains canonical Review;
- Home shows `merge_conflict` attention;
- user sees conflict evidence and available recovery actions;
- no separate mandatory Merge Failed board column appears.

## A-017 — Unified Work view consistency

**Given** tasks across three projects  
**When** the user switches between Work list and board views with the same filters  
**Then**:

- both views represent the same task IDs;
- tasks are not copied into global-board records;
- Project context is visible, with attempt-pinned repository provenance only
  where work has run;
- canonical phase grouping remains correct.

## A-018 — Home prioritization

**Given** simultaneous critical permission request, agent question, human review, runtime outage, and informational warning  
**When** Home is loaded  
**Then**:

- actionable items are ordered by configured severity and age;
- each item has one primary recommended action;
- routine successful progress is absent;
- system health is collapsed unless it affects work.

## A-019 — Agent roster basic configuration

**Given** a user creates an agent profile  
**When** they configure name, role, model, capabilities, project access, permissions, and concurrency  
**Then**:

- runtime selection is optional when automatic routing is possible;
- executor details are under Advanced;
- the agent appears in task assignment controls;
- unavailable configuration produces an actionable explanation.

## A-020 — Legacy strict workflow remains functional

**Given** E3  
**When** users approve planning, run coder, dispatch reviewer, repair merge, and complete tasks  
**Then** current strict behavior remains functional.

The new UI may group states into canonical phases but must preserve exact state and gate controls under task details.

## A-021 — Custom workflow canonical fallback

**Given** E4 with an unmapped non-terminal custom state  
**When** tasks are listed globally  
**Then** a deterministic fallback phase is returned and a diagnostic is recorded.

Saving a newly edited custom workflow requires explicit canonical phase mapping for every state.

## A-022 — Project migration preview

**Given** E3  
**When** the user previews migration to autonomous_v1  
**Then**:

- no data is changed;
- each state mapping is shown;
- active planning, review, merge, and custom-hook blockers are identified;
- recommendation explains whether to wait, resolve, or force.

## A-023 — Safe project migration

**Given** a legacy project with no unsafe active tasks  
**When** migration is confirmed  
**Then**:

- workflow and task states update atomically;
- task, execution, workspace, review, comment, transition, and audit history remains accessible;
- role assignment history is not silently discarded;
- board immediately uses autonomous phases;
- a migration report is stored.

## A-024 — Migration rollback

**Given** an injected failure during project migration  
**When** the transaction fails  
**Then** no workflow or task status change is committed and the project remains usable.

## A-025 — Delivery report immutability

**Given** delivery report version 1  
**When** task description, contract, branch, validation profile, or diff changes later  
**Then** version 1 remains unchanged and continues to identify the evidence used at that review point.

## A-026 — Validation freshness

**Given** a delivery validated against base commit A  
**When** the merge base changes to B and project policy requires fresh validation  
**Then** merge is blocked until rebase/revalidation succeeds and the report identifies the new commit evidence.

## A-027 — Exception acceptance

**Given** an optional or policy-overrideable validation failure  
**When** an authorized human accepts an exception with reason  
**Then**:

- the exception is attached to the delivery report;
- required permissions are checked;
- auto-merge is disabled unless policy explicitly permits that exception class;
- audit identifies actor and reason.

## A-028 — Critical-risk pre-execution approval

**Given** critical effective risk  
**When** the task enters Ready  
**Then** it does not begin mutating work until an authorized human approves the task contract and permissions.

## A-029 — Raw advanced controls remain available

**Given** an advanced user or custom integration  
**When** they access raw workflow, transition, daemon, execution, MCP, or CLI surfaces  
**Then** existing capabilities remain available unless explicitly deprecated with compatibility notes.

## A-030 — Accessibility and responsive behavior

Home, Work, project board, task detail, agent roster, runtime settings, contract editor, and delivery review must:

- operate by keyboard;
- provide visible focus;
- announce dynamic attention/status updates appropriately;
- preserve semantic headings and landmarks;
- function in full sidebar, rail, tablet, and mobile layouts;
- respect reduced motion;
- avoid document-level horizontal overflow.

## A-031 — Task predates Project repository setup

**Given** an eligible, governed Task was accepted while its Project had no
primary Repo

**When** an owner attaches a valid Repo owned by that Project and the Project
is not manually paused

**Then**:

- the existing Task becomes eligible on the next scheduler wake without a
  Task update or backfill;
- its Task representation has no `repo_id`;
- the execution Workspace and Workspace lease identify the Project's current
  primary Repo; and
- normal Charter, dependency, assignment, capability, retry, and version gates
  still apply.

## A-032 — Repository changes preserve attempt history

**Given** a Task has an execution Workspace, lease, delivery/review evidence,
or release reference for Repo A

**When** the Project later selects Repo B

**Then** Forge preserves Repo A on all attempt-derived provenance, does not
rewrite it from the Project pointer, does not silently reuse an old-Repo
Workspace for a new attempt, and admits future work only after the normal safe
reset/setup boundary against Repo B.

---

## 3. API contract tests

### API-001 — Task response repository cutover

Task REST/MCP/Solo responses and generated bindings omit `repo_id` with no
compatibility alias. Other additive V2 fields retain their documented enum and
serialization contracts.

### API-002 — Canonical phase filter

Filtering by each phase returns only tasks whose current project workflow resolves to that phase.

### API-003 — Attention determinism

Same database state always returns the same attention kind, severity, and recommended action ordering.

### API-004 — Intent action capability

For every task state, action endpoint either:

- performs a valid action; or
- returns a structured unavailable response with allowed actions and reason.

It must not expose a generic internal transition error as the primary response.

### API-005 — Contract optimistic concurrency

Updating task contract requires task version or contract version. Concurrent edits return conflict details.

### API-006 — Effective policy explanation

Effective policy response includes resolved risk and reasons for every required gate or denied auto-action.

### API-007 — Delivery pagination/versioning

Delivery list is ordered by version and supports stable pagination. Version IDs never change.

### API-008 — Authorization

Only authorized actors can:

- change project autonomy policy;
- lower inferred risk;
- accept validation exception;
- approve critical work;
- enable auto-merge;
- transfer to restricted agents/runtimes.

### API-009 — MCP actor attribution

Task changes initiated through MCP are audited as Agent with correct agent identity, not System.

### API-010 — Legacy raw transition

Existing transition API remains functional and records typed actor/source correctly.

---

## 4. Workflow tests

### WF-001 — Autonomous state graph validation

- all targets exist;
- one backlog and at least one terminal state;
- cancellation state valid;
- canonical phase assigned;
- working dispatch targets worker;
- review validation occurs;
- request changes returns to working;
- merge failure remains canonical review.

### WF-002 — Strict preset parity

Serialized strict preset behavior matches the current default fixture unless an intentional migration note says otherwise.

### WF-003 — Custom workflow save validation

Newly saved custom workflows without canonical phase fail with field-level errors.

### WF-004 — Hook audience typed actor

All/User/Agent/System behavior is exhaustive and correct.

### WF-005 — Retry budgets

Execution, validation/review, and merge repair budgets resolve task override → project setting → preset default consistently.

### WF-006 — Human gate decisions

Approval and rejection are tied to gate entry/version and cannot be replayed against a later delivery.

---

## 5. Persistence and migration tests

### DB-001 — V058 upgrade

Upgrade representative database fixture through all new migrations without data loss.

### DB-002 — Contract defaults

Existing tasks receive empty contract safely and derive default risk/profile.

### DB-003 — Delivery uniqueness

Concurrent report generation cannot create duplicate versions for a task.

### DB-004 — Cascade behavior

Deleting task/project cleans new contract/profile/report records according to intended retention policy.

### DB-005 — Migration atomicity

Project workflow migration and task-state mapping happen in one transaction.

### DB-006 — Legacy workflow JSON

Workflow without new fields remains readable and round-trips without destructive rewrite unless explicitly saved.

---

## 6. Performance acceptance

Use a generated fixture with at least:

- 100 projects;
- 10,000 tasks;
- 2,000 active/review tasks;
- 50 agents;
- 20 runtimes;
- multiple delivery versions.

Requirements:

- Home returns bounded sections without scanning unbounded raw execution logs;
- Work pagination uses indexed filters where required;
- global board grouping does not fetch every task;
- attention derivation avoids per-row N+1 queries;
- SSE updates invalidate affected aggregate queries without full application reload;
- delivery report generation does not block unrelated task transitions.

Exact latency budgets should be set after baseline measurement on supported hardware, but regressions must be tracked in CI or a repeatable benchmark script.

---

## 7. Security acceptance

- worktree isolation is not represented as a security sandbox;
- permission and secret access are enforced independently;
- protected-path elevation cannot be suppressed by the worker agent;
- validation commands come from confirmed project policy, not agent output alone;
- auto-merge cannot bypass required validation or human gate;
- read-only reviewer cannot mutate worker workspace;
- runtime transfer respects data locality and repository access;
- all policy overrides are audited;
- raw logs and delivery reports redact secrets according to existing security policy.

---

## 8. Release gate checklist

Before autonomous_v1 becomes the default for new projects:

- [ ] All M1 workflow acceptance scenarios pass.
- [ ] Home and Work E2E tests pass across projects.
- [ ] Task creation requires no advanced configuration in E1.
- [ ] Pre-repository Tasks dispatch after Project Repo attachment without Task
  mutation, and repository changes preserve attempt history.
- [ ] Delivery report versions and checks are correct.
- [ ] Legacy strict and custom workflow suites pass.
- [ ] Typed actor attribution is deployed.
- [ ] Auto-merge remains disabled unless full M5 gate passes.
- [ ] Project migration preview and rollback are tested.
- [ ] Runtime outage copy is user-centered.
- [ ] README and getting-started screenshots match released UI.
- [ ] Clean install/demo smoke test passes.
- [ ] Feature flag rollback is verified.
- [ ] No critical or high unresolved security finding remains.
