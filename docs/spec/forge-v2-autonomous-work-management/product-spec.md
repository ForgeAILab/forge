# Product Specification: Forge V2 — Autonomous Work Management

**Status:** Draft for implementation  
**Owner:** ForgeAILab  
**Date:** 2026-08-08  
**Target release:** New default experience for newly created projects; legacy projects remain supported during migration.

---

## 1. Executive summary

Forge should evolve from a visible multi-stage agent workflow engine into a reliable work-management system for autonomous coding agents.

Modern coding agents increasingly plan, implement, test, diagnose, and continue long-running work within one session. Requiring a separate planner, coder, reviewer, and multiple explicit gates for every task creates duplicated work and excessive human attention. It also makes Forge feel more complicated precisely as agents become more capable.

Forge V2 therefore uses this default execution model:

```text
Task
  → one primary agent owns plan + implementation + self-validation
  → Forge runs external verification and policy checks
  → human or automatic merge policy decides acceptance
```

Forge differentiates through the parts that an individual coding agent cannot reliably provide alone:

- safe concurrent execution across many tasks;
- isolated workspaces and conflict management;
- externally enforced validation;
- durable task context and cross-agent recovery;
- task-level provenance and delivery evidence;
- policy-aware human gates;
- a unified view of work across agents, repositories, and projects.

The product should feel like a lightweight issue tracker designed specifically for human–agent work, not like a workflow engine the user must configure before delegating anything.

---

## 2. Problem statement

### 2.1 Current user problem

The current Forge model exposes several internal concepts at once:

- project;
- repository;
- task;
- workflow state;
- planner role;
- coder role;
- reviewer role;
- agent;
- daemon;
- execution;
- worktree;
- plan gate;
- CI gate;
- review gate;
- merge state;
- recovery and retry states.

Each concept is individually defensible, but their combined visibility creates a high cognitive entry cost. Users must understand the architecture before they can confidently assign work.

### 2.2 Changed technical environment

The original planner → coder → reviewer decomposition assumed that a single coding agent required external decomposition and frequent handoffs. That assumption is no longer suitable as the default. A capable coding agent can now:

- inspect the repository;
- formulate and revise a plan;
- implement across many files;
- run tools and tests;
- inspect failures;
- retry within the same context;
- produce a review-ready branch or pull request.

The new bottleneck is not primarily whether one agent can complete one task. It is whether a user can safely supervise many delegated tasks without terminal babysitting, lost context, false completion claims, or unclear review evidence.

### 2.3 Product risk if Forge does not change

If Forge continues to lead with visible orchestration stages, it risks becoming:

- a duplicate planner around agents that already plan;
- a duplicate validation loop around agents that already self-test;
- a high-friction way to obtain functionality available in simpler tools;
- a platform whose strongest capabilities are hidden behind configuration burden.

The product needs to preserve its enforcement and recovery strengths while making the normal path substantially simpler.

---

## 3. Product thesis

> **Forge is the reliable work manager for autonomous coding agents. Assign work to any agent, let it operate in an isolated environment, and receive either a verified delivery or one clear request for attention.**

### 3.1 Core operating principle

> **One agent per task by default. Many tasks in parallel. More agents only when risk, specialization, exploration, or failure justifies them.**

### 3.2 Product promise

A user should be able to queue several meaningful tasks, leave Forge running, and later return to:

- verified changes ready for review;
- clearly stated blockers requiring decisions;
- failed validations with evidence and recovery options;
- no branch collisions or lost task context;
- no need to inspect every terminal session.

### 3.3 Product category

Forge is not intended to be:

- a general-purpose Jira replacement;
- a chat frontend for one model;
- a graphical prompt-chain builder;
- an IDE replacement;
- a cloud-only coding agent provider.

Forge is an **agent work-management and execution-supervision system** with a lightweight issue-tracker interface.

---

## 4. Target users

### 4.1 Primary user: multi-agent individual developer

A technical founder, senior engineer, or independent developer who:

- uses two or more coding-agent tools;
- works across several repositories or projects;
- delegates multiple tasks concurrently;
- wants local or self-hosted execution;
- needs evidence before accepting agent work;
- does not want to monitor terminal sessions continuously.

### 4.2 Secondary user: small engineering team

A small team experimenting with agent delegation that needs:

- shared task ownership;
- human and agent assignees;
- review queues;
- protected merge policies;
- project-specific validation rules;
- auditability and reproducibility.

### 4.3 Tertiary user: agent-platform builder

A builder who uses Forge as a local control plane through REST, MCP, CLI, or embedded workflows. This user needs the advanced workflow engine and runtime controls, but should not determine the complexity of the default graphical experience.

---

## 5. Jobs to be done

### Primary job

> When I have several pieces of engineering work, I want to delegate them to capable coding agents and return only for meaningful decisions and review-ready results, so I can increase throughput without losing control of the repository.

### Supporting jobs

1. **Prepare work:** turn a rough request into a task with enough context and constraints for an agent to begin safely.
2. **Assign work:** choose an agent manually or accept Forge’s recommendation.
3. **Supervise work:** know what is running, blocked, awaiting review, or failing without reading raw logs.
4. **Verify work:** ensure required checks ran against the actual delivered commit.
5. **Review work:** understand the objective, changes, evidence, risks, and unresolved issues in one place.
6. **Recover work:** retry, resume, or transfer a task without losing the accumulated context or worktree.
7. **Coordinate work:** manage dependencies, conflicts, merge order, and capacity across multiple projects.

---

## 6. Product principles

### P1. Familiar surface, specialized behavior

The interface uses familiar project, task, assignee, board, review, and activity concepts. Agent-specific complexity is introduced only when it affects a decision.

### P2. The task survives the agent

The durable task owns the objective, contract, conversation summary, worktree, changes, tests, decisions, and history. Agents and runs can change without resetting the task.

### P3. Verification is external

Agent self-reports are useful progress evidence but do not satisfy Forge validation. Required checks must be executed and recorded by Forge against a known commit or workspace state.

### P4. Human attention is scarce

Forge requests human input for ambiguity, risk acceptance, credentials, protected actions, or final review—not for routine state transitions.

### P5. Exceptions are first-class

Blocked work, failed checks, offline runtimes, exceeded budgets, and merge conflicts are surfaced as actionable attention items with a recommended next action.

### P6. Technical states are not board states

Starting, planning, editing, testing, retrying, merging, and recovering may exist internally, but the board displays only phases meaningful to a person.

### P7. Safe defaults, explicit power

The default experience requires little configuration. Custom workflows, hooks, raw policy, prompt configuration, daemon routing, and operator diagnostics remain available under advanced settings.

### P8. Evidence over ceremony

Forge should not require a separate reviewer merely to create the appearance of rigor. Rigor comes from independent checks, scope enforcement, provenance, and risk-based review.

---

## 7. User-facing domain model

### 7.1 Project

A workspace that groups its current execution repository, tasks, agents, policies, validation profiles, and project activity.

A project owns:

- zero or one selected current primary repository, with prior execution repository identities retained only as history;
- a default autonomy policy;
- a default validation profile;
- task views and board configuration;
- linked agents and humans;
- optional advanced workflow definition.

### 7.2 Task

The durable unit of work.

A task owns:

- title and objective;
- description and acceptance criteria;
- project context;
- parent, subtasks, and dependencies;
- primary assignee;
- risk and policy overrides;
- current canonical phase;
- current or latest run;
- attention state;
- delivery report;
- comments, decisions, and audit history.

A task can survive multiple runs and multiple agents.

Repository selection is Project setup, not Task state. A Task carries no
repository selector; each new run resolves the Project's current primary
repository, while the run's Workspace and lease preserve the exact repository
used for delivery evidence and history.

### 7.3 Agent

A named execution profile that can accept tasks.

An agent includes:

- name and role description;
- executor/provider configuration;
- default model and reasoning settings;
- capabilities and project access;
- permission policy;
- concurrency and budget limits;
- preferred or eligible runtimes.

The ordinary UI presents the agent as a teammate. Executor internals are advanced configuration.

### 7.4 Run

One execution attempt on a task. This is the user-facing name for the existing execution concept.

A run includes:

- agent and model;
- start and stop times;
- status and current activity;
- tool and command evidence;
- token and cost usage;
- workspace and commit information;
- stop reason;
- validation and recovery history.

A task can have many runs. A failed run does not imply a failed task.

### 7.5 Runtime

The compute location where an agent can execute. This is the user-facing name for a daemon.

A runtime includes:

- host identity;
- availability and capacity;
- detected executors;
- permissions and environment;
- current sessions.

Runtime information is shown only when it affects availability, routing, security, or troubleshooting.

### 7.6 Task contract

The enforceable description of what the task is allowed and required to deliver.

A task contract can include:

- objective;
- acceptance criteria;
- allowed paths;
- protected paths;
- required validation profile;
- risk level;
- cost and duration budget;
- network or secret permissions;
- merge policy override.

The user may define the contract explicitly, accept a Forge-generated draft, or rely on project defaults.

### 7.7 Delivery report

A structured, immutable review snapshot generated when work is ready for review.

It includes:

- responsible agent and run;
- repository, base commit, head commit, and branch;
- requested scope and actual changed scope;
- file and diff summary;
- commits and pull request state;
- required validations and results;
- self-reported agent validation;
- independent review result when applicable;
- exceptions, skipped checks, and unresolved concerns;
- final outcome: local only, branch pushed, PR opened, merged, or rejected.

### 7.8 Attention item

A derived condition indicating that work requires human or operator action.

Attention is not normally a workflow state. It is derived from conditions such as:

- agent asked a question;
- credentials or permission required;
- scope expansion requested;
- validation repeatedly failed;
- task is blocked by a dependency;
- runtime is unavailable with no eligible fallback;
- retry or budget exhausted;
- merge conflict could not be repaired;
- human review is required by policy.

---

## 8. Canonical task phases

Every project workflow state maps to one canonical phase.

| Canonical phase | Meaning | Examples of internal states |
|---|---|---|
| **Backlog** | Not yet committed for execution | backlog, proposed, icebox |
| **Ready** | Eligible to start when capacity and dependencies permit | todo, ready, queued |
| **Working** | Agent or human is actively producing the result | planning, implementing, testing, retrying |
| **Review** | Delivery is being verified, reviewed, or merged | review, QA, approval, merging, merge repair |
| **Done** | Work is terminal | done, cancelled, rejected, archived |

### 8.1 Default project board

```text
Backlog → Ready → Working → Review → Done
```

### 8.2 Needs attention treatment

Needs attention is a filter, badge, and Home section—not a sixth permanent board column by default.

A card may remain in Working or Review while displaying:

- `Question`;
- `Blocked`;
- `Checks failed`;
- `Runtime unavailable`;
- `Approval required`;
- `Merge conflict`.

### 8.3 Internal state visibility

Task details and diagnostics may show the exact internal state. Board cards and global work views show canonical phase plus the current activity or exception.

---

## 9. Default autonomy policies

Forge presents policy presets rather than workflow graphs.

### 9.1 Standard — default

```text
Primary agent → Forge validation → Human review → Merge
```

- no mandatory plan approval;
- no independent agent review by default;
- external validation required;
- human final approval required;
- same agent resumes after requested changes or validation failure.

### 9.2 Trusted automation

```text
Primary agent → Forge validation → Auto-merge eligible low-risk work
```

- low-risk verified changes may auto-merge;
- medium and higher risk stop for human review;
- protected paths always stop for review;
- ambiguous tasks stop before scope-expanding actions.

### 9.3 Strict multi-agent

```text
Plan → approval → implementation → independent review → validation → human approval
```

- preserves the current high-ceremony behavior;
- used for critical systems, regulated work, or explicit user preference;
- remains available as a project preset and migration fallback.

### 9.4 Custom

Advanced users can edit the underlying workflow definition and hooks.

---

## 10. Risk policy

Risk affects approval, review, permissions, and merge behavior.

| Risk | Typical work | Pre-execution gate | Independent review | Human final gate | Auto-merge |
|---|---|---:|---:|---:|---:|
| **Low** | docs, tests, formatting, isolated UI copy, low-impact refactors | No | No | Project policy | Eligible |
| **Medium** | normal feature work, bug fixes, API changes with test coverage | No | Optional | Yes by default | No |
| **High** | auth, payments, permissions, migrations, infrastructure, dependency updates | Only when scope is ambiguous | Yes | Yes | Never |
| **Critical** | destructive operations, secrets, production access, security boundary changes | Yes | Yes | Yes | Never |

### 10.1 Automatic risk signals

Forge may recommend or elevate risk based on:

- protected path matches;
- database migration files;
- authentication or authorization modules;
- payment or financial code;
- infrastructure and deployment files;
- dependency lockfile changes;
- secrets, credential, or network requests;
- unusually large diff or broad repository scope;
- tests removed or weakened;
- agent-reported uncertainty;
- repeated recovery attempts.

A user can raise risk freely. Lowering an automatically elevated high or critical risk requires an explicit reason.

---

## 11. Core user journeys

### 11.1 First-run onboarding

#### Goal

Get one real task running without teaching the full Forge architecture.

#### Flow

1. User starts Forge.
2. Forge detects available local executors and creates sensible default agent profiles.
3. User creates or selects a project.
4. User connects a repository.
5. Forge detects likely validation commands and asks the user to confirm a project validation profile.
6. User enters a task title and optional description.
7. Forge recommends an agent and starts the task after one confirmation.
8. User lands on the task page with a concise activity summary.

#### Requirements

- Daemon terminology is absent.
- Workflow editing is absent.
- Planner/coder/reviewer assignment is absent.
- Advanced configuration is available but not required.
- The empty state explains the single loop: create, assign, verify, review.

### 11.2 Create and start a task

#### Basic form

Required:

- title.

Optional:

- description;
- assignee.

Defaulted by project policy:

- risk;
- validation profile;
- merge policy;
- runtime routing;
- execution settings.

#### Advanced section

- acceptance criteria;
- allowed and protected paths;
- risk override;
- validation override;
- model and reasoning override;
- budget;
- permissions;
- merge policy.

#### Start behavior

A task entering Ready automatically starts when:

- dependencies are satisfied;
- project is not paused;
- an eligible agent and runtime are available;
- required pre-execution approval is not pending.

### 11.3 Observe running work

The task page displays:

- current agent;
- concise activity such as `Analyzing`, `Editing`, `Running tests`, `Retrying`;
- elapsed time and recent progress;
- current plan summary when available;
- changed files count;
- latest validation signal;
- pause, stop, and message actions.

Raw tool calls and terminal output remain under Runs → Diagnostics.

### 11.4 Agent requests a decision

When an agent cannot proceed safely:

1. It creates a structured question with context, options, recommendation, and impact.
2. The task receives an attention badge.
3. The task appears in Home → Needs attention.
4. The user answers in the task activity thread.
5. Forge resumes the same task context and run thread where possible.

Questions should not be represented as generic failure states.

### 11.5 Validation fails

1. The primary agent reports completion.
2. Forge runs the project’s external validation profile.
3. A required check fails.
4. The task remains in Working or internal verification state.
5. Forge sends the exact failure evidence to the same agent context.
6. The agent repairs and resubmits within the configured retry budget.
7. If the budget is exhausted, Forge creates an attention item with options:
   - continue with the same agent;
   - transfer to another agent;
   - modify the task contract;
   - accept an exception with reason;
   - cancel.

### 11.6 Review and request changes

The Review page centers on the delivery report.

The user can:

- approve and merge;
- approve without merge;
- request changes;
- ask a question;
- request independent review;
- accept a documented validation exception when policy allows;
- cancel or return to backlog.

Requesting changes resumes the same task thread by default and preserves the worktree and delivery history.

### 11.7 Transfer a task between agents

A transfer package includes:

- task contract;
- the current run's attempt-pinned repository and worktree;
- current diff and commits;
- concise conversation and decision summary;
- prior plans;
- validation history;
- failed attempts and stop reasons;
- current attention reason.

The user chooses a new agent or accepts Forge’s recommendation. The task stays in the same canonical phase unless policy requires review.

### 11.8 Cross-project supervision

The global Work surface supports:

- all active tasks;
- tasks awaiting the current user;
- tasks awaiting review;
- running tasks;
- blocked or failed tasks;
- filtering by project, agent, phase, risk, and label;
- list and optional Kanban views;
- saved views.

The default global view is an attention-oriented list rather than a giant board.

---

## 12. Information architecture

### 12.1 Global navigation

```text
Home
Work
Projects
Agents
Settings
```

### 12.2 Home

Purpose: answer “What needs me now?”

Sections:

1. **Needs attention**
   - questions;
   - blocked tasks;
   - exhausted retries;
   - runtime failures;
   - policy decisions.
2. **Awaiting review**
   - delivery summary;
   - risk;
   - checks;
   - age.
3. **Running now**
   - task;
   - agent;
   - activity;
   - elapsed time;
   - project.
4. **Recent deliveries**
   - merged, approved, rejected, or failed outcomes.
5. **System health**
   - collapsed by default;
   - only elevated when a runtime or integration issue affects work.

### 12.3 Work

Global task query surface.

Default saved views:

- Active;
- Needs attention;
- Awaiting review;
- Running;
- Assigned to me;
- Completed recently.

Views:

- list — default;
- board — canonical phases;
- compact table — optional advanced view.

### 12.4 Projects

Project switcher and project-specific surfaces:

```text
Overview
Board
Tasks
Deliveries
Settings
```

Project chat can remain available, but should be de-emphasized or embedded into Overview unless usage proves it is a primary navigation destination.

### 12.5 Agents

Team-roster presentation:

- status and availability;
- current assignment;
- capabilities;
- model/provider;
- concurrency;
- project access;
- recent success and failure signals;
- advanced execution configuration.

### 12.6 Settings

```text
General
Models and credentials
Integrations
Compute / runtimes
Validation profiles
Advanced workflows
Access and members
```

Daemons are renamed to runtimes and moved under Compute.

---

## 13. Project board requirements

### 13.1 Columns

The default board has five canonical columns.

### 13.2 Card contents

Every card should show only high-value information:

- task title;
- project-relative identifier;
- primary assignee avatar;
- risk indicator when medium or higher;
- current activity or attention badge;
- validation summary when in review;
- dependency indicator;
- age or elapsed time when relevant.

Do not show daemon IDs, executor adapters, raw workflow state names, or retry counters by default.

### 13.3 Drag behavior

Dragging changes the human-visible phase but must respect policy:

- Backlog → Ready starts work when eligible.
- Working → Review can request agent stop and force validation if manually initiated.
- Review → Done requires the appropriate approval or explicit override.
- A drag that conflicts with active execution must explain the consequence before committing.

### 13.4 Custom workflows

Custom internal states remain supported. Each state must map to a canonical phase so the global Work board remains coherent.

---

## 14. Task detail requirements

### 14.1 Header

- task identifier and title;
- canonical phase;
- attention status;
- primary assignee;
- current run activity;
- primary action appropriate to state.

### 14.2 Tabs

#### Overview

- objective and description;
- acceptance criteria;
- current plan summary;
- task contract;
- subtasks and dependencies;
- current attention item;
- latest delivery summary.

#### Activity

Unified human-readable timeline:

- comments;
- agent progress summaries;
- questions and answers;
- decisions;
- state changes;
- validation events;
- review actions;
- merge outcome.

Raw tool calls are collapsed.

#### Changes

- diff summary;
- files changed;
- commits;
- branch and pull request;
- scope comparison;
- ability to inspect the actual diff.

#### Checks

- required validation profile;
- each check command or integration;
- result, duration, commit, and logs;
- skipped or overridden checks;
- agent self-validation clearly separated from Forge validation.

#### Runs

- run attempts;
- agent/model;
- status and stop reason;
- token/cost/time;
- transfer and recovery history;
- Diagnostics subview for raw logs and terminal output.

### 14.3 Sidebar

- project and its current repository (read-only Project setup context);
- assignee;
- risk;
- validation profile;
- merge policy;
- labels and priority;
- parent and dependencies;
- advanced workflow state.

---

## 15. Delivery report requirements

A delivery report must answer:

1. What was requested?
2. Who performed the work?
3. Where did it run?
4. What changed?
5. What validation ran?
6. What remains uncertain?
7. What action is now available?

### 15.1 Required fields

```text
Task objective
Acceptance criteria status
Primary agent and run ID
Repository
Base commit
Head commit
Branch
Pull request URL/status
Requested scope
Changed scope
Diff statistics
Commit list
Agent self-validation
Forge-required validation results
Independent review result, when present
Exceptions and skipped checks
Risk level
Recommended disposition
Generated timestamp
```

### 15.2 Immutability

Each review submission creates a new delivery report version. Requesting changes does not overwrite the prior report.

### 15.3 Completion semantics

A task becomes Done only after a defined disposition:

- merged;
- approved without merge;
- explicitly accepted as non-code output;
- cancelled;
- rejected and closed.

An agent message stating “complete” is not sufficient.

---

## 16. Agent management requirements

### 16.1 Agent list card

```text
Frontend Builder
Available · 1/2 active
React, TypeScript, visual testing
Default model: Gemini 3.6 Flash
Runs automatically on eligible local compute
Current task: WEB-142
```

### 16.2 Basic configuration

- name;
- description/role;
- default executor and model;
- capabilities;
- project access;
- permissions;
- concurrency.

### 16.3 Advanced configuration

- preferred runtime;
- CLI arguments;
- environment;
- reasoning effort;
- prompt template;
- recovery policy;
- budget limits.

### 16.4 Human and agent assignment

Humans and agents appear in the same assignment control, with clear type markers. Tasks can have:

- one primary owner;
- one current actor;
- optional reviewers;
- watchers.

The backend may preserve role assignments for advanced workflows, but the default UI should not require role-by-role assignment.

---

## 17. Runtime experience requirements

### 17.1 Automatic routing

Forge selects an eligible runtime based on:

- agent compatibility;
- project/repository availability;
- permissions;
- executor detection;
- capacity;
- user routing constraints.

### 17.2 User-facing failure copy

Bad:

> Daemon `d91...` missed three heartbeats.

Good:

> Frontend Builder cannot continue because “Mac Studio” is offline. Reconnect it or move the task to another compatible runtime.

### 17.3 When runtime selection is exposed

- user has multiple machines with different repositories or credentials;
- security or data locality matters;
- no automatic route is available;
- user is troubleshooting;
- advanced project policy pins execution.

---

## 18. Notifications and attention

### 18.1 Notification philosophy

Notify on decisions and exceptions, not routine progress.

### 18.2 Default notification events

- agent asks a question;
- task becomes blocked;
- required validation exhausts retries;
- delivery is ready for review;
- high-risk scope expansion requested;
- runtime outage has no fallback;
- merge fails after automatic recovery;
- task exceeds configured budget.

### 18.3 Suppressed by default

- task started;
- every tool call;
- every state transition;
- individual successful test commands;
- routine retry within budget;
- automatic merge-repair attempt.

---

## 19. Search, filters, and saved views

Global filters:

- project;
- phase;
- attention type;
- assignee;
- risk;
- validation status;
- label;
- updated date;
- delivery disposition.

Saved views are query definitions, not copied task collections.

Search indexes:

- title and description;
- comments and decision summaries;
- agent progress summaries;
- delivery reports;
- external issue references.

Raw execution logs should not dominate ordinary search results.

---

## 20. Integrations

### 20.1 External issue trackers

Forge may synchronize with GitHub Issues, Linear, Jira, or other systems. The native Forge task remains the execution record.

Minimum requirements:

- external URL and identifier;
- status synchronization policy;
- comments or delivery summary backfill;
- no duplicate task creation;
- conflict visibility.

### 20.2 Git providers

Delivery reports should distinguish:

- local branch only;
- branch pushed;
- pull request opened;
- pull request checks pending;
- merged;
- closed without merge.

### 20.3 Chat integrations

Chat channels may create, assign, or monitor tasks, but task history and review evidence remain canonical in Forge.

---

## 21. Success metrics

### 21.1 Activation

- median steps from project creation to first real task start;
- percentage of new projects that start a task without opening advanced settings;
- percentage of first tasks reaching review.

### 21.2 Human attention efficiency

- human interventions per completed task;
- percentage of interventions caused by meaningful decisions versus workflow ceremony;
- median time tasks wait for human attention;
- number of notifications per completed task.

### 21.3 Execution reliability

- percentage of tasks completed by one primary agent;
- external validation pass rate on first submission;
- recovery success within the same task context;
- transfer success between agents;
- workspace or branch collision rate;
- merge conflict repair rate.

### 21.4 Review trust

- percentage of review submissions with complete delivery evidence;
- percentage of agent self-reported successes rejected by Forge validation;
- request-changes rate;
- post-merge rollback or immediate fix rate where measurable.

### 21.5 Product simplicity

- percentage of active users who never open runtime management;
- percentage of projects using Standard or Trusted presets versus Custom;
- support questions involving agent/daemon distinction;
- time to understand why a task is blocked in usability testing.

---

## 22. Non-goals for the first implementation

- replacing Jira, Linear, or GitHub Issues as a full organizational planning system;
- sprint planning, story points, time tracking, or resource forecasting;
- arbitrary visual workflow-builder redesign;
- model benchmarking marketplace;
- autonomous product management without human project ownership;
- automatic risk classification that cannot be overridden or audited;
- deleting the daemon or workflow engine backend concepts;
- full multi-tenant SaaS redesign;
- automatic merging of high- or critical-risk work;
- replacing repository-native code review.

---

## 23. Product rollout

### Stage 1 — opt-in preview

- add `Autonomous` workflow preset;
- add feature flag for Home/Work/Review surfaces;
- new projects can opt in;
- strict workflow remains default for existing projects.

### Stage 2 — new-project default

- Autonomous becomes default for newly created projects;
- Standard autonomy policy requires human review;
- migration assistant available for existing projects;
- runtime terminology updated throughout normal UI.

### Stage 3 — simplified default navigation

- Home, Work, Projects, Agents, Settings become default global navigation;
- Daemons and Operations move to advanced/system surfaces;
- legacy strict workflow remains supported.

### Stage 4 — policy automation

- risk-driven gates;
- trusted low-risk auto-merge;
- conditional independent review;
- adaptive agent transfer and escalation.

---

## 24. Product acceptance criteria

The product specification is satisfied when all of the following are true:

1. A new project can run a task with one primary agent and no mandatory planner assignment.
2. The primary agent can plan, implement, self-test, and repair within one persistent task context.
3. Forge independently runs required validation before review or merge.
4. The normal board displays no more than five canonical phases.
5. Planning, retrying, testing, merging, and merge repair are not required visible columns.
6. Home accurately displays tasks needing human attention across all projects.
7. Work displays tasks across projects without duplicating task records.
8. A failed run remains attached to an active task and can be retried or transferred.
9. Requesting changes resumes the same worker context unless the user transfers the task.
10. Review presents an immutable delivery report with scope, diff, validation, and outcome evidence.
11. Runtime outages are explained through affected agents and tasks rather than daemon IDs alone.
12. Existing custom and strict workflows continue to function.
13. Existing task, execution, transition, review, and audit history remain readable after migration.
14. Low-, medium-, high-, and critical-risk policies produce the expected approval and review behavior.
15. End-to-end tests cover normal work, questions, validation failure, recovery, transfer, review, merge failure, and legacy projects.
