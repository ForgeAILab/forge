# Architecture

Forge is a Rust workspace plus a React/TypeScript frontend. This doc explains
the crate layout, the agent/scope model, the task state machine, the database,
and durable events. For runtime configuration see [getting-started.md](getting-started.md);
for the HTTP surface see [api.md](api.md).

The product model is one global Main Agent and exactly one Project Agent for
each operational Project, each with one durable Agent Chat. Forward-only
`V071+` migrations preserve historical collaboration data while the public
runtime uses the singular binding, chat, and handoff model described below.

## Crate layout

```
crates/
├── forge-cli/     # Binary entrypoint, server startup, CLI commands
├── forge-solo/    # Repository-scoped chat-first TUI and local entrypoint
├── forge-client/  # forge-ctl CLI client
├── forge-daemon/  # Local daemon detection and reporting
├── api/           # Axum REST endpoints, SSE, middleware
├── api-types/     # Shared request/response types (zero internal deps)
├── agent-host/    # Forge-owned direct Agent Runtime composition and protected stores
├── db/            # SQLite schema, migrations, repository implementations
├── services/      # Business logic (task state machine, workflow engine)
├── executors/     # TaskExecutor trait, Shell executor, JSONL logging
├── cli-adapters/  # Codex, Claude, Cursor, Gemini, opencode, shell, null adapters
├── workspace/     # Git worktree lifecycle, locking, path guardrails
├── git/           # Low-level git operations
├── review/        # CI runner, auditor orchestration
├── events/        # In-memory event bus (tokio broadcast)
├── mcp-server/    # MCP JSON-RPC tools for agent integration
└── config/        # Configuration loading, defaults
```

### Dependency flow

```
forge-cli → api → services → db
                → events      ↑
                → agent-host → agent-runtime
          → mcp-server -------┘
          → executors (log schema, shell executor)
          → workspace → git
          → config
          → api-types (shared request/response types, zero internal deps)

forge-solo → services → db / events / agent-host / executors / workspace → git
           → config / api-types / review / cli-adapters
```

## Architectural patterns

### Repository trait pattern

The `db` crate defines async traits (`TaskRepo`, `AgentRepo`, …) in
`repository.rs` and implements them all on a single `SqliteDb` struct in
`sqlite.rs`. Services and routes call trait methods as
`TaskRepo::create(&*state.db, ...)`.

### Error propagation chain

`DbError` (db) → `ServiceError` (services) → `ApiError` (api). The api crate's
`errors.rs` maps domain errors to HTTP status codes. All errors render as
`ErrorResponse { code, message, details, request_id }`.

### AppState wiring

The server builds a multi-thread Tokio runtime with 16 MiB thread stacks to
provide headroom for deeply nested Agent tool and recovery calls in debug
builds. The recovery-to-re-execution future is boxed so it is stored on the
heap rather than inline in the coordination-tool recovery future.

`forge-cli/main.rs` resolves configuration, builds one transport-neutral
`services::ForgeRuntime` through `ForgeRuntimeBuilder`, starts its
`RuntimeSupervisor` in server mode, and then constructs `AppState` as the Axum
adapter over that graph. `AppState` retains `Arc` references to the Task,
identity/profile, Main/Project Agent Chat, embedded-session, memory,
commitment, and Attention-facing services; it does not own a second copy of
the background worker lifecycle. `AppState` is `Clone` (shared fields are
`Arc`) and used as Axum state.

### Shared runtime composition and Forge Solo

`services::ForgeRuntimeBuilder` is the composition boundary shared by the
server and `forge-solo`. It resolves the effective configuration and injects
one database, event bus, adapter registry, Agent Runtime host, credential and
log roots, workflow, review/merge, workspace, and cleanup graph. The builder
constructs each domain service once; `RuntimeSupervisor` owns the background
worker handles and one shutdown signal. The server's `AppState` and Solo's
session facade consume this graph rather than maintaining parallel persistence,
workflow, retry, or recovery implementations.

The graph shares one `WorkspaceBackendRouter` across Task execution, workflow
hooks, lifecycle hooks, merge, cleanup, terminals, and operator status. Consumers
resolve the persisted placement before workspace I/O; the legacy
`Workspace.worktree_path` field is private to `db` and accessible only through
its backend accessor. A directly inserted server Workspace without a placement
receives a server placement on first resolution, without changing an existing
owner. Embedded hooks and environment checks retain their original stdin,
Git environment, output collection, and Project-value redaction semantics.
Review I/O uses `review::ReviewWorkspace`: CI steps remain unbounded, while
conformance commands keep their timeout and output budgets. Workspace operations
follow the placement owner through the embedded or daemon backend; see
[Workspace placement](#workspace-placement).
Exact review Git evidence, detached conformance checkouts, candidate restoration,
and unbounded CI use owner-local operations on daemon placements, keeping opaque
handles separate from server paths.

Both modes run the correctness-critical Forge core: migrations, crash
recovery, Project and Task lifecycle projection, Agent Chat turns, Task
dispatch, execution heartbeats, workflow hooks, memory/coordination,
Attention, wake delivery, durable-event projection, validation, merge, and
workspace cleanup. `forge-solo` additionally starts the local Agent Runtime
and local adapter execution needed by its reachable Project Agent and Task
capabilities. It omits only server transports: the Axum listener, web assets,
MCP endpoint, OAuth callback listener, remote-daemon connection lifecycle,
external sync, and task-terminal transport. Solo therefore opens no TCP
listener and does not silently start a hidden `forge` server.

The heartbeat monitor isolates recoverable pass failures within a tick while
preserving two safety dependencies: Agent timeout runs only after owner
suspension succeeds, and WorkspaceLease expiry runs only after lease renewal
succeeds. Placement suspension, progress warning, execution expiry,
reservation, and placement maintenance remain independent. Notification and
Project-hook broadcast consumers treat receiver lag as a warned delivery gap
and continue receiving subsequent events; they exit only when their bus closes
or runtime shutdown is requested. Durable replay remains the responsibility of
the durable-event consumers rather than these broadcast loops.

Codex Agent Chat uses app-server stdio dynamic-tool callbacks to invoke the
same `ScopeToolComposition` and shared `CoordinationToolProvider` as native
turns. The host resolves the persisted CLI session, verifies the admitted
identity/chat, and intersects profile, identity, binding, and scope permissions
before advertising tools. Persisted verification or scratch workspace access
is narrowed to `Deny` for the CLI chat transport; the original native session
scope is unchanged. Each invocation is prepared and checked within that
composition; domain services reauthorize mutations. Setup chats receive the
adoption catalog with its full nested Charter payload schema, and a later
turn receives the ready-Project catalog after approval. Solo persists that
future-ready Project ceiling at Agent selection time, but the canonical
`charter_setup_required` scope gate withholds Task and operational proposals
until the approval transaction commits; this lets approval preserve the
binding without either pre-approval authority or a post-approval gap. Chat
sandboxes remain filesystem-denied and suppress Task Git finalization. CLI
transports other than Codex currently retain their text-only Agent Chat path.

Solo owns one runtime process for one repository-scoped data root. Before
migrations or worker startup it acquires an exclusive
`<data-root>/runtime.lock`; a second live process stops with the data-root
context and a safe retry instruction. The operating system releases that lock
on process exit or crash. Normal, signal, panic, and forced shutdown paths
release owned resources and restore the terminal, but never delete durable
Project state.

Repository identity is stable across moves of the checkout. Solo resolves the
primary Git worktree and Git common directory, then reads or atomically creates
the `<git-common-dir>/forge-solo-id` marker. On Unix, the marker is enforced as
owner-only; non-Unix builds do not apply POSIX mode-bit checks and must rely on
the platform's ACLs to protect it. The marker contains only the version and
UUID repository identifier:

```text
version = 1
repository_id = "<uuid>"
```

Unless `--data-dir` is supplied, the isolated data root is
`<Forge data root>/solo/<repository-id>/`. SQLite, protected state, JSONL
logs, media, and generated Task worktrees stay there; no Solo database or
runtime files are written into the tracked source checkout. On Unix, the data
root is enforced as owner-only; non-Unix builds do not apply POSIX mode-bit
checks, so equivalent platform ACLs are required. An explicit data directory
is used exactly as given only after its recorded repository binding matches the
marker. Malformed, replaced, symlinked, or insecure markers and mismatched data
roots fail closed before Project data is exposed or changed.

Bootstrap is an idempotent typed service operation, not a TUI database path.
It creates or resumes one isolated local owner, owner membership, Project,
repository binding, Project Agent Chat, setup binding, and eligible Task
execution selection. First-run discovery accepts only the supported local CLI
harnesses whose structured availability and authentication checks pass:
`codex`, `claude_code`, `cursor`, `opencode`, `gemini`, or `smith`. An exact
single candidate is still confirmed; multiple candidates are picked
explicitly; no candidate leaves an actionable setup state. Provider API-key,
OAuth, browser-login, and credential-import flows remain outside Solo v1.

The visible Agent is the canonical Project Agent Chat principal with
filesystem access denied. It can coordinate repository work only by admitting
a normal Task. Task Workers and reviewers receive the scheduler-issued
WorkspaceLease and the existing role-scoped permissions. Solo Projects use
the existing `autonomous_v1` workflow: blocking checks, explicit human review,
merge, and cleanup. A Project remains setup-required until the adoption
conversation drafts the compact Charter and the user confirms the exact
revision, render digest, Agent selection, and operating-skill revision in its
focused approval card. Prose such as “yes” never approves a Charter, and no
repository-mutating Task is admitted before that receipt commits.

The TUI is a projection over ordinary Forge records. Immutable chat messages,
finite turn states, Tasks, approvals, Attention items, checks, commit evidence,
and typed failures are authoritative in SQLite. EventBus notifications only
invalidate projections; incremental JSONL reads through `LogReader` show
bounded live activity and never determine completion from text. Every Solo
session is bound to one owner, Project, repository, and Project Chat, and the
facade rejects opaque IDs outside that scope.

On startup, ordinary crash recovery reconciles ownerless or expired executions
before the terminal UI is entered; there is no in-TUI recovery progress screen.
Queued and retry-wait turns resume through `AgentChatTurnWorker`. Awaiting-input
turns remain durable, but the current Solo startup does not attach the
protected runtime interaction broker, so those questions are not answerable in
the TUI and remain parked until a broker-backed surface handles them. Failed,
blocked, cancelled, approval, and recovery states are shown from durable
records with only their canonical permitted action. A lost response is retried
with the same idempotency identity, so bootstrap, message, cancellation, retry,
and approval commands do not create duplicates. A forced quit may leave work
for the next launch to reconcile, but it never claims that unfinished work
succeeded.

### Shared command/query boundary

For migrated Main/Project orchestration operations, REST, MCP (where a command
is exposed), native Agent tools, user approval execution, and recovery jobs all
use the same domain command/query services. The boundary is split by domain
command family rather than implemented as one monolithic service:

```text
REST / MCP / native tool / approval / recovery
                         |
                 command/query adapter
                         |
             domain command or query service
                         |
                    SQLite transaction
              /           |            \
       domain rows   durable event   command receipt
                                      + optional AgentAction outcome
```

Adapters authenticate or receive the principal, parse their transport
envelope, call one service, and serialize its typed result. They do not issue
domain SQL, perform lifecycle validation, invent Project scope from an input
ID, choose a separate idempotency/approval rule, or maintain a second
operation/permission catalog. The service derives the canonical principal and
scope and owns authorization, validation, lifecycle, persistence, idempotency,
and durable event creation. Task transitions remain a dedicated service and
workflow-engine path outside this orchestration command boundary;
`TaskService.transition()` delegates to `WorkflowEngine`.

Each mutating command carries a `CommandContext` containing the principal,
canonical scope, operation, idempotency key, canonical input digest, expected
versions/digests, authorization or action provenance, and correlation/
causation metadata. Under the SQLite writer transaction, Forge first looks up
the receipt: an identical key and digest return its frozen typed outcome, while
changed input returns `idempotency_conflict`. A receipt miss re-authorizes the
current references and expected state, applies the domain mutation, appends
the durable event, persists the receipt and any action execution/outcome, and
commits once. A failed commit exposes none of those writes. The in-process
event bus is published only after commit; it is a live projection, not the
replay or authority boundary. Consequently, response loss replays the same
domain identifiers, receipt, event, and outcome without creating a duplicate.

### Identity, profiles, and explicit authority

`agent_identity` is the stable account-owned product identity — the one
user-facing "agent" with a directly-editable definition: harness (CLI or
direct), default model, reasoning, permission policy, and system prompt. A
connected identity may remain unbound, serve as the account's Main Agent, or
serve as a Project Agent through explicit bindings. A binding names only the
identity; each turn resolves the identity's current settings, so edits apply
without rebinding, and the binding row's stored profile reference is a
bind-time snapshot reserved for future per-binding overrides. Internally,
settings are stored as immutable `agent_profile` revisions and every edit
publishes a new revision behind the identity's versioned pointer — an
implementation detail, not a user-facing concept. CLI profiles preserve the
existing executor path, while native profiles select the Forge-hosted Agent
Runtime backend. Credentials are write-only protected values referenced by
opaque handles, never profile fields.

`MainAgentBinding` is the account's single active global assistant binding.
`ProjectAgentBinding` is the single active manager binding for each operational
Project. Binding cardinality is unconditional: a Task Worker or reviewer
assignment cannot satisfy a Project Agent binding, and there is no
role/`is_primary` combination to resolve. Connection health never grants
Project, Task, or filesystem access; an identity appears in the chat switcher
only when it is explicitly bound as Main or Project Agent.

Product Genesis stores Project Agent choice as a structured identity reference,
separate from Charter prose. Any configured, enabled, healthy identity may be
selected, including the active Main Agent or an identity already used by
another Project. Charter approval freezes the exact identity/profile/skill/
policy set; Project creation remains a separate explicit user action that
consumes that approval receipt. Disabled or unhealthy selections fail visibly
instead of being silently replaced. Automatic selection never chooses a
credential-less bootstrap default, although a user may explicitly select a
locally authenticated CLI identity.

The singular role boundary is enforced by the authenticated binding and the
Task scheduler, not by model instructions:

| Principal | Canonical scope | Authority | Explicit boundary | Workspace |
| --- | --- | --- | --- | --- |
| Main Agent | Account / Main Chat | Genesis discovery, bounded research and inquiries, Charter proposal/readiness, exact Project create/handoff, bounded portfolio projection | No Project-local Documents, Tasks, assignments, milestones, validation, waiver, release, or repository lease | Account scratch, not a repository checkout |
| Project Agent | One bound Project / Project Chat | Project Documents, Decisions, Task and assignment coordination, integrated validation, milestone/evidence/readiness/release-candidate proposals | No other Project, private Main history, credentials, user-only manual attestation, waiver, or release | Self-owned Project verification checkout |
| Task Worker | One assigned Task / attempt | Work allowed by the scheduler-issued capability profile | No portfolio, Genesis, unrelated Project/Task, approval, or release authority | Task-scoped lease |
| Reviewer/validator | One assigned review/check | Review or attestation allowed by policy | Read-only by default; remediation requires Worker authority, while any explicitly required independent attestation remains a separate policy gate | Read-only by default |
| Interactive user | Authorized account/Project | Exact approvals, setup choices, manual attestations/waivers, and release | Only existing authenticated user controls | Existing user controls |
| System | Server policy | Readiness, scheduling, leases, recovery, and projections | Not a chat principal; cannot substitute for user approval | Internal only |

The same configured identity may fill several rows, but each scope, history,
assignment, lease, and attestation remains separate. Project execution-role
selections are optional defaults, not an execution-time identity lock: an
explicit Task assignment may use any enabled configured Agent, including the
active Main or Project Agent and the same identity for Worker and reviewer. A
policy that requires independent evidence evaluates the evidence itself rather
than forbidding identity reuse globally.

Every persistent agent session binds to one canonical scope:

| Scope | Authority source | Filesystem |
| --- | --- | --- |
| Main Agent Chat | active Main Agent binding and account policy | denied |
| Project Agent Chat | active Project Agent binding and Project policy | denied |
| Task | admitted assignment, workflow role, and Task Workspace | role-bounded Task Workspace only |

Effective permissions are a fail-closed intersection of the account, selected
profile, canonical scope, tool, approval, and applicable binding or
Task-assignment layers. Opaque record IDs are references, not capabilities.
The reusable Agent Chat context scope records only this stable canonical
authority. Each admitted turn separately freezes its binding, identity,
Profile, operating-skill, and policy revisions, so editing an identity can
rotate the next native session without either mutating an earlier turn's
provenance or falsely reporting that the same Chat scope changed authority.
Task context scopes retain the Task ID as their canonical `scope_id`, but V105
makes their persistence key role-specific. One identity can therefore hold a
write-capable Worker session and a read-only reviewer session for the same Task
without reusing capabilities or conflicting over stored scope linkage.

### Main and Project Agent Chats

An account has one global Main Agent Chat (visible in setup state even before a
binding is selected), and each operational Project has one Project Agent Chat.
Chat ownership is account/Project-scoped rather than tied
to a replaceable identity, so binding replacement preserves message, handoff,
memory, and session provenance. Connected but unbound identities do not create
additional chats. There are no participants, addressing rules, responder
policies, arbitrary threads, or bounded multi-agent rounds.

A running chat turn is observable, not a black box. `FederatedAgentChatTurnRunner`
hands the native runtime a `TurnLogSink` (`services::turn_log_sink`, shared
with embedded Task execution) that writes every runtime event to
`<data-dir>/agent-chat-logs/<turn_job_id>.jsonl` in the Forge log schema, and
`GET /agent-chats/{chat_id}/turns/{turn_id}/logs` serves it. `AppState`
carries one `AgentChatTurnLogRoot` for both the writer and the route, re-pointed
when the real `--data-dir` arrives with the effective config.

Creating an operational Project creates its Project Agent binding and Project
Agent Chat atomically. Direct REST and MCP Project creation also derives the
authenticated creator as the Project owner and inserts that user's `owner`
Project-member row in the same transaction. The creator can therefore use
Project-scoped reads and writes immediately; a follow-up operation cannot fail
with a membership-related 404 between Project creation and membership
materialization. A migrated Project with no single safe binding is explicitly
`agent_setup_required` and keeps its Project/Task data readable, but cannot
admit Project Agent turns until the user resolves the identity. When an
approved adoption Charter resolves that setup binding, the same transaction
installs the canonical Project-Agent permission ceiling before activating the
binding; setup's empty ceiling never survives into an admitted Project Agent.

V117 separates one-time Project admission from current Project-Agent
authority. Genesis creation validates the complete Main-to-Project packet and
freezes one immutable `project_admission_receipt`; explicit legacy adoption
freezes the consumed adoption approval instead. The active binding references
that stable receipt plus the exact current consumed Charter approval, current
Charter/revision pointers, current Project operating-skill revision, and policy
digest. REST, MCP, and native callers all use one transactional binding command
that derives those fields, replaces the old binding, updates Chat readiness,
and appends the binding event atomically. A binding without complete
Charter-backed authority cannot be written.

User-message or handoff admission atomically appends the guarded immutable
message, its durable domain event, and one queued turn job. A turn exposes the
finite states `queued`, `leased`, `awaiting_input`, `retry_wait`, `succeeded`, `failed`, or
`cancelled`. Database-backed leases, optimistic versions, finite retry budgets,
and idempotency keys prevent duplicate turns and silent success after a failed
response commit. When a turn yields a runtime questionnaire interaction, it parks in
`awaiting_input` without consuming retry attempts; answering the interaction re-leases
and resumes the turn. Authorized non-terminal turns may be cancelled with their
current optimistic version and an idempotency key; stale or terminal
cancellation attempts are rejected. Progressive output is transient; success or failure appends
one immutable canonical assistant outcome with provenance, usage, duration,
correlation, and causation metadata.

Every new user-message, Main-to-Project handoff, or autonomous wake turn
goes through `AgentTurnAdmissionService`. Admission resolves
the current owning binding, its identity, that identity's current Profile,
operating-skill revision, policy/tool digests, and canonical scope, then freezes
those values and the admission digest on the queued job. The shared
`AgentChatTurnRunner` abstraction (the configured implementation is
`FederatedAgentChatTurnRunner`) runs native or constrained CLI backends for
all admitted turns. An explicit manual retry admits a new turn for the same
triggering message, resolving current Profile, binding and policy through
`AgentTurnAdmissionService`. Automatic retries keep the same frozen admission.
A Profile edit or binding replacement therefore affects the next admitted turn
only, never an already queued/leased turn.

The typed `TurnFailure` crosses the Agent Runtime host boundary before any
human-readable error formatting. `ProviderAttemptFinished.error` retains the
provider kind, retryability, delay, and usage reset hint; `BudgetFailure(Input)`
identifies context overflow. The turn policy owns all automatic retry decisions.
Deterministic configuration, authority, and request rejection fail immediately;
context overflow also fails because the host exposes no forced compaction.
A provider's prompt-too-long HTTP 400 is reported as `provider_rejected` because
no typed evidence distinguishes it from other request rejections; a retry fails
the same way until the context shrinks.
Transient/retryable request rejection, empty response, turn limits, unclassified
and postcondition failures use the existing three-attempt budget.
Postcondition retries keep the existing instruction overlay. Usage limits refund
the claim's attempt and defer with a separately counted floor (1, 5, 15, 30, then
60 minutes),
a fifteen-minute missing/past reset fallback, and a six-hour per-wait ceiling.
The 24th deferral or 24 hours from the first deferral terminalizes the turn with
`usage_limit`; no scheduled time exceeds that deadline. Chat provider health
uses the typed failure and the turn's resume time, with no message classifier.
Task and connection-test health retain message parsing and their 24-hour ceiling.
Pre-provider admission failures refund the attempt and stop at a separate cap of
three. A monotonic invocation counter keeps accounting identities distinct when
a charged attempt is refunded. Leases continue to fence every failure settlement.

Migration `V202610010600__chat_turn_failure_class.sql` adds nullable typed failure
and retry decision columns plus admission-failure, usage-deferral and invocation
counters. Old error codes/messages stay readable without invented typed evidence.
Deterministic failures raise one cause-naming Attention item and offer a typed manual retry.
Manual retry remains available for every failed or cancelled turn, including
historical untyped failures. The versioned, idempotent command admits a new turn
with current authority and resolves the incident through Attention's resolver.
A later turn for the same triggering message or any later chat message (including
a topic divider) supersedes the old retry action and incident.
It does not run automatically on configuration changes. The composer remains
disabled during a usage-limit deferral because that turn still occupies the
chat's single live-turn slot; the timeline names the reason and resume time.

For a Charter-backed Project, fresh turn admission checks the active binding
against the Project's current Charter pointers and its stable admission
receipt. A Genesis receipt performs one lookup of its recorded immutable
handoff and compares the canonical fingerprint; an adoption receipt compares
its recorded consumed approval digest. It never re-walks historical Main
messages, Main turns, source Profiles, Genesis instructions, or Project-create
event provenance. Rebinding reuses the receipt and creates no handoff; a
Charter amendment rotates current binding/approval pointers while retaining the
same receipt. Already admitted retrying turns continue to use their frozen
binding/Profile/skill provenance.

The Project operating skill (`forge.project.orchestration/v1@20`) is a
doctrine index, not a doctrine dump: the resident prompt carries the mission,
authority boundaries, standing invariants, and autonomous-drive rules, plus an
index of server-owned doctrine sections (`research`, `documents`,
`scope_change`, `tasks`, `milestones`, `release`) that the Agent reads on
demand through the `skill.section` native read. The approved Charter likewise
stays out of resident context: the handoff packet delivers only the Charter's
identity and digests, and the Agent reads the full rendered text with the
`project.charter` native read whenever its details matter. This keeps a
Project Chat turn's fixed prompt cost small enough that LCM compaction has a
real conversation budget to work with on small provider profiles.

Merge-friendly doctrine is delivered where layout and scope are decided. The
shared compile-time rule recommends small modules with clear ownership and
disjoint files for parallel Tasks; avoids central registries, route tables,
export/barrel lists and large shared libraries; and favors per-feature files
that are discovered or registered without editing a shared list. If a shared
edit is unavoidable, one Task owns it and other Tasks depend on that Task.
Work splits follow module boundaries, with owned modules/files named in each
Task. Main applies this to Charter architecture constraints during Genesis
(`forge.main.project-discovery/v2@7`). Its account baseline remains at @4,
with no additional resident layout rule. Project
standing invariants apply it to `task.propose`
(`forge.project.orchestration/v1@20`); the on-demand document and Task sections
apply it to architecture/execution plans and adaptive splitting. Planner runs
receive it once in their system prompt for both native and outbox delivery;
plan-artifact delivery instructions still require owned repository-relative
paths and scope reporting. Scaffolded `AGENTS.md` carries it for implementation
and follow-up proposals, retaining the worker's existing scope discipline.
Native Task proposal/adaptive payload guidance carries one short reminder when
either operation is admitted. MCP Task creation and sub-task descriptors share
that same reminder: split along module boundaries, name each Task's owned files,
and avoid Tasks that all edit one shared file. The full doctrine stays at the
layout/planning surfaces above. Transport-only plan
artifact instructions and argument field help retain their existing ownership
requirements without another copy of the rule.

Migration `V202610031431__merge_friendly_layout_guidance.sql` inserts new
digest-pinned Main/Project revisions, advances current skill pointers, and
moves Project bindings from @19 to @20. Previous revision rows, Genesis session
prompts, and frozen turn admissions stay unchanged. The compiled baseline
stays at @4 and retains exact bodies/digests for @1–@3; unknown revisions fail
closed. On-demand
doctrine, planner constants and scaffold exports have no persisted body
revision mechanism; their text follows the server build. These instructions
guide agents; they do not change merge/rebase/review behavior or enforce file
ownership as a capability grant.

Migration `V202610010550__chat_session_denied_operations.sql` derives Project
revision @18 by replacing the attention-wake rule in the immutable @17 body.
The previous body stays resolvable as `LEGACY_V17_PROJECT_PROTOCOL`; frozen
admissions still render their admitted revision. Recovery, resumption, and
re-execution require an offered operation and an addressed cause. A denial
marked `retry: none` is final for the turn; the Agent uses offered alternatives
and escalates to the user only what its authority cannot cover.

An autonomous `delivery_followup` admission also freezes a typed postcondition
on its server-authored trigger: one Project-scoped event must commit at a
sequence newer than the wake event before the worker may append the assistant
response and mark the turn `succeeded`. Which event depends on the delivery
state resolved at admission. While the milestone still has required acceptance
checks the Agent can settle itself, the required event is
`project.milestone.check.recorded` — readiness evaluated before those results
exist can only re-report the same missing checks. Once they are settled, the
required event is `milestone.readiness.evaluated`. A Project with no open
milestone owes neither, so no postcondition is frozen at all. Prose
alone fails an attempt that owes a record, without committing an agent message.
The same frozen job moves through the ordinary finite `retry_wait` budget, and
its retry receives a server-owned corrective instruction naming the record it
actually owed. A canonical `blocked`, `failed`, or `stale` readiness result
satisfies the reconciliation boundary; it does not imply validation or
authorize the user-only release action.

Attention interprets Task transitions through the applicable workflow's state
kind and canonical phase. A renamed human review gate still requests review,
and a successful terminal state still schedules delivery reconciliation; the
workflow's cancellation target resolves existing incidents without scheduling
delivery. Projection resolves the workflow from the event's recorded source
state and actor, then interprets its recorded target, so later Task movement
does not change which subtask workflow applied. State names alone confer no
completion or review meaning; interruption records own blocked/failed
Attention. An unknown target has no inferred lifecycle meaning.

The chat worker selects the session backend from the bound identity's profile.
A native profile uses the embedded host and an Agent Chat-scoped continuity
timeline; a safely migrated CLI profile may use an explicit constrained chat
backend and must advertise its actual limitations. CLI execution is derived
from the admitted immutable Profile's top-level model, reasoning effort, and
permission policy plus its bounded `config_json`; automatic retry never re-reads
the current Profile. Adapter discovery also preserves model-to-provider identity,
so a Smith model such as `gpt-5.6-terra` remains bound to its discovered
`chatgpt` provider rather than an adapter default. Main and Project chats have
deny-all filesystem access. Project Agent Task actions go through the existing
`TaskService` and workflow; repository mutation remains limited to admitted
Task Worker/reviewer executions in their Task Workspaces.

The CLI permission policy includes an explicit high-risk `yolo` value. It maps
to an adapter's strongest local execution mode only where that cannot cross a
Forge-owned role boundary. Managed Codex Task executions override profile
configuration to `workspace-write` with network access, always with approvals
set to `never`, unless the agent's permission policy is `yolo`: then the role
runs with `danger-full-access` so real-browser QA, GUI toolchains, and local
servers probed across commands work. The post-run read-only gate still
discards a reviewer's authored changes in the Task worktree, but it does not
constrain or audit filesystem writes, processes, credentials, or network side
effects elsewhere on the host. `yolo` is full host authority.
Forge runs CLI harnesses headless, so nobody can answer a per-tool prompt:
`supervised` maps to the same unattended mode as `auto` (Claude Code
`bypassPermissions`, Gemini/Smith `--yolo`, Cursor `--force`, OpenCode
`--dangerously-skip-permissions`). Only `plan` keeps a harness read-only
(Claude Code `plan`, Smith `--approval ask`, Cursor without `--force`), which
suits a reviewer that only reads. This is an execution-harness setting, not a Forge
capability grant. Canonical scope, Task assignment, Workspace-lease admission,
chat filesystem denial, and user-only approval/waiver/release authority are
still enforced independently.

Codex Task execution starts from a Forge-owned per-Task Codex home beside the
execution logs. It links only the user's existing authentication, excludes
ambient Codex configuration, rules, hooks, skills, plugins, and connectors,
disables host skill instructions and account/plugin discovery for the thread,
enumerates effective local MCP configuration and explicitly disables every
discovered server, and installs Forge-owned `forbidden` rules for changing
checkout/branch context or integrating/publishing Git history. Every managed
role, including a read-only reviewer, runs in the workspace-write sandbox with
network access so it can install dependencies, build, and run tests. The
sandbox excludes both `/tmp` and the inherited `TMPDIR`; beyond the worktree
its only writable roots are one freshly reset Forge-owned `task-scratch`
directory, the execution outbox, and the host's package-manager caches
(`~/.npm`, Cargo `registry`/`git`, the Go module cache, the XDG cache, pnpm's
store, and `~/Library/Caches` on macOS, each honoring its relocation
variable). The Task directory that contains the worktree is not writable by
the sandbox. CLI planning and implementation executions (`planner`, `worker`,
`coder`, and `executor`) instead receive a role-scoped `FORGE_PLAN_PATH` inside
their execution outbox. Forge seeds that private file from the canonical Task
plan for implementation turns. This outbox file is the supported plan-delivery
contract for every CLI adapter.
Forge enforces the surrounding Task directory as non-writable for managed
Codex runs; a CLI adapter without an OS sandbox may still have ambient
filesystem access to sibling files, so Forge does not claim universal
filesystem confinement for those processes. Writing a sibling directly is
outside the supported contract.

Native planning and implementation Task sessions use the typed `task.plan`
operation with `{"action":"write","content":"<full Markdown checklist>"}`
instead of receiving a canonical plan path. The tool surface names those roles
Planner and Worker; the Worker session covers `worker`, `coder`, and `executor`
execution roles. The host takes the runtime session from the invocation context
and requires it to bind the Task, identity, role, and ready Workspace to exactly
one `Running` execution. It writes that candidate to the same
execution-private outbox; reviewers are not offered the operation. Each call
replaces the full private checklist candidate, so an implementation role can
keep completion marks current during its run.

Neither `$FORGE_PLAN_PATH` nor `task.plan` publishes the canonical plan
directly. Before terminal settlement, Forge validates the candidate and freezes
its exact bytes in a host-owned staging area outside the agent-writable outbox;
later outbox changes cannot affect publication. A durable compare-and-swap
claim binds publication to the exact execution, Task state entry, and Project
version. On Unix, the frozen file replaces the canonical sibling `plan.md` by
atomic same-directory rename, and Forge rejects files with additional hard
links. Other platforms use a portable replacement fallback and do not promise
those two Unix properties. Reviewer and stale execution output cannot publish a
plan. Non-regular files, symlinks, invalid UTF-8, oversized content, and plans
without a checklist are rejected; a rejected candidate is retained in its
execution outbox for diagnosis. After successful staging, Forge removes the
agent-writable outbox and settles from the frozen copy.
For a server-owned workspace (including verified shared mounts), this broker
uses the same local sibling and staging files. For a daemon-owned workspace,
the server reads canonical text through the owner router and sends it in
`execution.start.plan_text`; implementation roles use the canonical plan with
`task.plan` as fallback, and only valid checklists are seeded; planners are never seeded.
The daemon prepares its private outbox locally and returns the exact candidate in
`execution.terminal.plan_text`. The winning terminal CAS stores that candidate
in the private `execution_plan_transport` table, in the same transaction as the
terminal receipt. Plan bodies are redacted against Project environment values
and absent from executor snapshots, the Execution API, and operation receipt
bodies; receipts retain only the digest and byte length. The publication claim then authorizes a fenced owner-local
`publish_plan` operation. The owner preserves the prior canonical plan in a
private snapshot for idempotent publication, compare-safe rollback, and cleanup.
The private Task sibling `.forge-plan-staging/` holds frozen server candidates,
and prior canonical plan bytes (or an absent-plan marker). On a daemon it holds `<execution>.transport.json` with the candidate and
prior canonical text for idempotent publication and compare-safe rollback.
Settlement or abandoned-publication cleanup removes each execution's files;
workspace cleanup removes the entire Task directory. The private database table
retains the winning transported artifact until its Execution is deleted.
Empty, checklist-free and missing required candidates use the existing bounded
workflow guard rejection. Unchanged daemon seed content uses that same guard. Oversized remote
candidates terminalize as failed and are acknowledged with the actual byte size
and limit. When terminal status is omitted, capture uses the same outcome
inference as the server: exit code zero with no signal or error means completed.
An oversized plan then produces a failed capture. Explicit or inferred failed
or cancelled executor outcomes retain their original reason.
Owner settlement errors persist exponential retry backoff and a visible Task
wait annotation; unreachable owners also carry `runtime_offline`, `owner_wait`,
and `deferred_dispatch`. Successful settlement clears that wait. Plan RPCs wait
behind long owner `workspace.run` commands, and discarding an already cleaned
workspace succeeds.
These artifact operations preserve owner/generation/HEAD fences and can settle
while the next CLI turn runs; they do not mutate tracked repository files.
The server never opens the daemon's worktree or outbox path; there is no
workspace filesystem sync. A read-only role
still cannot deliver code: Forge skips
its finalization, fails any execution that changed a tracked file or moved HEAD,
and resets the worktree afterwards. An unexpected authority-bearing input in that managed home
fails execution closed. Codex-generated trust configuration and system-skill
cache are discarded before each attempt and replaced with canonical Forge
configuration, so a retry cannot reject its own runtime artifacts. The adapter
also declines every built-in command/file approval request because Forge has no
user-facing approval bridge for those callbacks. Commands inside the declared
worktree sandbox proceed normally, while linked Git metadata remains outside
that write scope; after a completed writable Codex Task turn, Forge performs
the Task-branch commit from the trusted adapter boundary even if the profile
set `auto_commit=false`. Every automatic host finalization disables repository
hooks, filesystem-monitor commands, maintenance, rerere, and partial-clone lazy
fetches. Status explicitly includes untracked files and ignores dirty submodule
worktrees while retaining changed-gitlink detection; a staged-delta guard also
prevents Git's no-op commit fallback from inspecting submodules. Because Git
clean/process filters are arbitrary subprocesses but may define the
repository's canonical stored content, Forge does not silently bypass them: it
detects their effective configuration before status/add and fails closed
without running the filter.
Embedded/native workers instead retain their agent-authored commit contract. A
pre-isolation Codex Task session that is not visible from the managed home
restarts as a fresh isolated thread without importing ambient history. A
request to operate on the source checkout cannot inherit a personal allow rule. The
generated workflow system contract and user request are both persisted into
the transport-neutral execution input for initial dispatch, follow-up, resume,
and crash recovery. Worker and Coder contracts explicitly prohibit entering
another checkout or integrating the target branch; only Forge's post-review
merge service owns that operation.

Every native Task scope admitted for `task_read` receives both bounded UTF-8
file reads and `forge_task_list`. The list tool enumerates at most 256 sorted
direct children and may filter entry names with `*` and `?`; returned paths are
workspace-relative and identify files, directories, symlinks, or other entries.
A missing read is classified as `not_found`. Reading a directory returns a
structured `is_directory` result with its bounded children, so path discovery
does not depend on guessing filenames or interpreting an opaque I/O failure.

Main Agent tools cover discovery, configured web search, Project lifecycle,
bounded portfolio summaries, and explicit handoff. Main Agent sessions cannot
create, edit, assign, transition, review, merge, or deliver Tasks. A Project
Agent may manage Tasks only in its bound Project. A handoff is an immutable,
bounded, provenance-linked publication from the Main Chat to the target Project
Chat and schedules at most one target turn; it never copies credentials,
private memory, hidden global history, or Main Agent authority.

### Project truth, authority, and release evidence

The singular chats are interaction surfaces, not a mutable source of truth.
Forge stores consequential Project state as immutable, addressable revisions and
derives read models from those records. Authority is scoped by domain:

| Domain | Authoritative record | Owner / final authority |
| --- | --- | --- |
| Project identity and scope | approved `ProjectCharterRevision` | User approves; Main Agent recommends before handoff; Project Agent proposes amendments afterward |
| Execution traceability | approved Project Documents | Project Agent proposes; user may approve the traceability snapshot |
| Current execution repository | `project.primary_repo_id` resolving to a same-Project `Repo` | Project owner/admin configures Project setup; Tasks cannot select or persist a repository |
| Consequential choices | effective `DecisionRecord` (`active`, `superseded`, or `invalidated`) | Authorized principal recorded on the decision; candidate/editor records are not effective decisions |
| Work state | Task, validation, review, and event records | Task/workflow services, assigned workers, reviewers, and authorized users under existing policy |
| Outcome and release | milestone definition, `ReadinessSnapshot`, and immutable `Mxxx-rN` release manifest | Project Agent proposes; Forge evaluates; user alone releases |
| Context and continuity | authorized `ContextManifest`, LCM timeline, and scoped memory references | Forge authorizes sources; Runtime stores continuity; neither chat nor memory can promote authority |

The Main Agent owns global discovery and portfolio routing only. It can draft a
Genesis Charter and publish one bounded handoff, but it cannot manage a Project
or revise its Charter after attachment. The Project Agent owns planning and
orchestration for exactly one Project and can exercise integrated software in
its self-owned verification checkout. Typed validation commands record those
observations; they do not replace user-only manual attestations, waivers, or
release approval. Implementation and Task review remain workflow-managed work
under scheduler-issued Task `WorkspaceLease` authority.
Model output, Agent Profile text, chat prose, web pages, repository text, and
memory are data; none can widen a permission ceiling or satisfy an approval.

`WorkspaceLease` is an internal scheduler record, not a public API or chat
capability. Before execution, `TaskService` resolves the Task's Project, loads
its current `primary_repo_id`, and verifies that the selected Repo belongs to
that Project. A Task has no repository selector or persisted repository copy.
The V076 `workspace_lease` table then persists the Project/Task plus exact Task
version and execution attempt, attempt-pinned repository binding, resolved base
ref, role, capability JSON, assigned principal, capability-profile
revision/digest, issuing principal, issue/expiry timestamps, status, and
optimistic version. Its database guards require the lease binding to equal the
Project's current same-Project primary Repo and the execution Workspace Repo,
plus the same Project, Task version, assigned principal, running execution,
current approved Charter traceability, and profile revision/digest. One active
lease is allowed per Task and identity fields are immutable. Main and Project Agent identities
are eligible when explicitly assigned to the Task; their chat turns and Task
executions remain separate scoped sessions.
Custom workflow execution-role names are retained for assignment matching and
canonicalized to `worker`; only the dedicated `reviewer` role receives the
reviewer lease class. Its operation idempotency key is the exact execution
attempt ID: claim inserts the execution and lease in one transaction, and each
retry/follow-up creates a fresh child execution with its own lease. A matching
Task role assignment is authoritative for that execution role. Project
`default_role_assignments` seed new Tasks and provisioning, but changing a
Project default neither invalidates nor rewrites an explicit Task assignment.
The same eligible identity may fill Worker and reviewer roles. On a
`legacy_unverified` Project only, an explicit manual execution selection is the
assignment boundary when neither the role nor Task has an assignee; an existing
applicable assignment still must match. Charter-backed Projects always require
the exact Task Worker/reviewer role or Task assignment, not equality with the
Project default for that role.
`WorkspaceLeaseRepo` provides CAS renewal/revoke and bounded expiry operations.
The heartbeat renews an active lease before its deadline only while the exact
execution is still running and its Task, assignment, governance, current
Project repository, execution Workspace repository, and capability bindings
remain valid. Renewal changes only `expires_at`,
`updated_at`, and the optimistic version; all authority fields stay immutable.

Once an attempt exists, its Workspace and lease are historical repository
provenance. Diff, review, merge, evidence, and release paths use that pinned
identity rather than reinterpreting completed work through a later Project
selection. A reusable Workspace whose Repo differs from the current Project
primary Repo is not reused or granted a new lease; it must pass the normal safe
reset/setup boundary first.

The scheduler delivers authority through the internal execution channel by
creating the running execution and lease together; the executor acknowledges
that delivery by verifying the exact active lease immediately before provider
start and before execution work. A missing, expired, revoked, reassigned, or
superseded lease fails closed. A disconnected daemon placement suspends heartbeat
expiry until reconciliation or `max_disconnect`; the hard deadline still applies.
Heartbeat/recovery expiry otherwise cancels and terminalizes
the running attempt only after a valid running lease can no longer be renewed,
and records reconciliation; all terminal, failed,
cancelled, and stalled paths revoke the grant. A disconnect suspends the attempt;
its grant is revoked only when the attempt terminalizes. A retry
gets a new execution identity and lease. The claim path canonicalizes executor,
worker, and task-worker aliases to persisted `worker`, while reviewers remain
`reviewer`. No route, MCP tool, chat context, filesystem path, handle, or bearer
token exposes the row.

### Project readiness and execution setup

Project coordination, repository setup, and Charter-backed execution are
independent projections. A successful Project/Chat creation or a configured
repository does not imply the other dimensions:

| Dimension | States | Authority and consequence |
| --- | --- | --- |
| `coordination_state` | `setup_required`, `ready`, `unavailable` | Active singular Project-Agent binding plus a ready Project Chat; only `ready` admits a Project-Agent turn. |
| `execution_setup_state` | `setup_required`, `provisioning`, `ready`, `failed`, `unavailable` | Durable provisioning operation and verified repository linkage/filesystem state; Project role selections are optional defaults. |
| `execution_gate` | `active`, `reconciliation_required`, `unavailable` | A current approved Charter reports `active`. Legacy `pre_baseline_read_only` and `baseline_approval_required` values remain readable for historical rows. |

The Genesis create/handoff transaction may commit a valid Project while
execution setup is still `provisioning` or `setup_required`. Provisioning is a
finite, leased, checkpointed, idempotent operation that reconciles the
deterministic filesystem target, repository row, and Project link;
interruption resumes that operation rather than creating a second directory or
repository. Worker and reviewer defaults are derived from the Task workflow
when useful, but missing defaults do not block setup. Any enabled configured
Agent—including Main, Project, or the same Agent in both Task roles—may be
assigned explicitly. Read-only planning/discovery and write-capable
implementation both derive authority from the current Charter and their Task
workflow.

#### Project environment

`ProjectSettings.environment` (`env`, `assets`, `checks`,
`recheck_interval_seconds`) is applied before local or daemon execution starts.
Each check declares `scope`: `workspace` (the default for existing checks), or
`machine` for toolchains, disk and services that need no checkout. Owners must
write read-only commands. Without a checkout, daemon machine checks run with
Project env and no assets in fresh empty directories under the owner's root.
With a ready location, probes run all checks in its verified checkout.
Local preflight runs in `TaskService::run_execution`, after the workspace lock
and the final WorkspaceLease verification and before ledger admission; remote
preflight runs before the provider RPC. Both use the placement's workspace
backend (`task_service/execution/environment.rs`). The env map is stamped onto the
in-memory executor config under `_forge_task_environment` — runtime authority
like the Task role, carried through fallback-candidate normalization
(`executors::adapter::apply_runtime_scope`) but never part of candidate
identity — and `cli_adapters::command::run_in_task_worktree` and the shell
executor set it on the child process before Forge's own variables. Assets are
copied through a sibling staging path and atomically renamed only when the
target is absent; symlink traversal and recursive/overlapping declarations are
refused. A failing check terminalizes the execution through the dedicated
pre-dispatch environment failure path. No provider call or retry budget is
spent, and the Task keeps its workflow state with no blocking annotation. The
workspace owner is recorded `not_ready` in `project_machine_readiness`, with
its current checks digest, failing check names, role, bounded redacted output,
and next-check time. The key is the Project plus the server host, or the daemon
and runtime IDs of a daemon-owned placement. An embedded execution daemon or a
shared-mount execution provider does not change a server workspace's owner key.

Admission and launch failure share a last-resort pause decision for the concrete
Task and the role being launched. A failed check filters only the launching role to which
`EnvironmentCheck::applies_to` applies. If every otherwise eligible connected ready
location is rejected only for `environment_not_ready`, Forge compare-and-sets
an environment pause before initial state entry. The Task retains its state
without a `dispatch_failed` annotation or transition. The same decision follows
a launch failure; readiness versions fence concurrent results and user or
repository pauses are never overwritten. Other eligible owners allow new work.
An Agent pin or existing/inherited placement on an unfit owner instead waits
there when another owner is healthy for other Tasks, even if those Tasks use
another Agent or executor.

Offline transport retains an existing environment-owned wait while its current
not-ready fact still applies, without creating a recovery incident.

Machine-specific waits live in `metadata_json.environment_wait` and
`deferred_dispatch`, with Task-linked Attention of kind `environment_not_ready`
naming the machine and checks. They use the existing runtime-offline category
and recommend waiting, emit no `task.execution_failed` event, and clear when the
machine becomes ready or admission moves on. Waiting Tasks count as parked for
Project slots. Changes to `metadata_json` already advance `list_revision`, so
Project slot caches observe both setting and clearing the marker. On a
single-machine paused Project there is no additional Task Attention: existing
environment Task Attention and wait markers are resolved in the pause transaction.
The Project pause remains the sole signal. An environment Project pause defers
dispatch rather than adding a `dispatch_failed` annotation.

Internal `project.environment_pause_json` records the machine, workspace,
checks, role, bounded output, and pause/last-check/next-check times. The public
`environment_pause` now includes a readable machine identity; Project responses
also expose the recorded `environment_readiness` rows to every Project reader.
Settings show a per-machine readiness table with Check now; Projects without
checks show an empty state. Migration V202610020600 infers an
older pause's machine from its recorded workspace placement before falling
back to the server; it preserves Projects and placements. The runner fills the
canonical digest. Malformed settings leave rows `unknown` and produce one
startup diagnostic instead of preventing startup. Asset-only Projects with no checks keep their
pause without a readiness row. Migration V202610010410
clears legacy Task environment annotations without deleting history.

`environment_pause_sync` starts independent jobs from one query for due
`not_ready` rows per dispatcher pass. Named failures re-run their recorded
checks. Rows with no named failing check
are never rechecked on a schedule: they require manual resume or Check now,
because passing checks do not verify asset staging or run-policy repair. Project env is applied without
asset staging. Host checks use the repository's primary or managed server
checkout. Probe-capable daemon re-checks use `machine.probe` in a ready repository
location, or scratch space for recorded machine checks before provisioning.
Daemons without `machine_probe.v1` (or without probe policy permission) retain
the recorded ready-workspace `workspace.run` path, never another Task's
workspace or the server as a substitute. These commands must be read-only: like the base's scheduled
re-check, host checks run in the primary checkout and are not filesystem
sandboxed against writes. Output is bounded while collecting both streams,
then redacted before storage.

Every completed attempt advances `next_check_at`, including missing workspaces,
unreachable owners, transport/policy errors and version conflicts in command
execution. Such errors preserve the machine's check facts and never fail
another Task's run. Actual failing check results update output and check times.
Success marks the machine ready, clears its waits, re-reads the Project and
compare-and-clears the matching current environment pause and publishes `project.resumed`. Readiness writes
compare both the row version and current digest. A harmless Project-version
change retries the pause clear only for the same pause epoch, digest and result
version; intervening user/repository pauses or newer facts win. The tick never waits for
command I/O. Jobs are single-flight per Project/machine; host scheduled checks
also share the manual-check guard. Completion releases the guard and wakes the
in-process dispatcher `Notify`, without waiting for its ten-second timer.
Finished job errors and undecodable rows are logged once and rescheduled
individually; they do not abort dispatch for healthy Projects. A daemon whose
recorded workspace was deleted becomes unknown and its wait clears, allowing
a new launch to decide. Check timeouts remain 1–300 seconds (default 120), and re-check intervals remain
60–86400 seconds (default 600).

Readiness success removes the machine wait and wakes dispatch; independent
execution blockers still apply. Coder, worker and planner placements require
`execution.plan_transport`: start plan text is read through the owner with
`task.plan` fallback, and the daemon prepares its own private plan outbox.
Provisioned locations use this same owner routing and plan transport.

`POST /projects/{id}/environment/recheck` accepts an optional machine selector
(`server` or daemon runtime ID), runs every configured check on the selected
machine or all known targets, and returns `{machines, project}`. Each machine
has its identity, check results, and nullable unavailable error. A manual check
returns HTTP 409 if a selected machine is already being checked. A passing
machine can clear a matching environment pause. An empty check list never resumes automatically: asset-copy
failures and removed probes stay paused with an explicit manual-resume message.
Unnamed failures stay paused until manual resume or Check now; timers never
relaunch an unfixed setup failure.
The manual endpoint uses the same host checkout or recorded daemon failure
workspace as readiness jobs. It never substitutes host results for a selected
daemon. Daemons without a recorded ready failure workspace return an unavailable
result until daemon probes are implemented. Transport and unavailable-target
errors preserve facts and reschedule failed rows. Manual resume and the existing compare-and-clear paths reset that
Project's `not_ready` rows to `unknown` in the same transaction. The next
dispatcher host admission without assets probes immediately, then launches or
pauses again without waiting for the old `next_check_at`. Direct/manual claims,
asset-backed Projects and daemon unknown rows proceed to launch preflight. Digest edits with checks remaining also retire an obsolete named-check
environment pause transactionally, so the old pause cannot veto the new unknown
facts. Removing all checks retains the existing manual-resume rule. Running executions are left to finish.

Review steps and conformance checks
(`review::contract::project_environment`) and lifecycle hooks
(`LifecycleHookContext.env`) receive the same env; the clean conformance
checkout also receives the assets. `execution_start_params` stamps only `env`
for remote daemons, whose worktrees are not on this host.

#### Canonical execution blocker and capability-aware review

`ExecutionBlockerProjection` (`api-types::execution_blocker`) is the one
server-owned explanation for why repository-mutating execution cannot proceed
right now: `code`, `stage`, `scope`, affected/governing record refs, canonical
attempt/execution/commit evidence, safe headline/explanation copy, the
required principal, and exactly one permitted `next_action`.
`ProjectExecutionSetupResponse.execution_blocker` carries the Project-wide
instance (built from the same `coordination_state`/`execution_setup_state`/
`execution_gate` computation as the table above); `TaskResponse.execution_blocker`
carries either that same Project-wide blocker or, when the Project itself is
clear, a reconciliation scoped to only that Task
(`services::load_task_execution_blocker`). Conflicts attach to the smallest
affected Task/plan-item/milestone or traceability record; an invalid optional
a reconciliation does not freeze unrelated Task work. Every surface that explains a blocker — Project execution setup,
Task detail/banner, chat context, phase controls, and activity history —
renders this projection's copy verbatim; a Task's own
`execution_evidence` (`ExecutionEvidenceSummary`) is separately derived from
its attempt/execution/commit history and can never regress to "not started"
once an attempt or commit exists, even while the Task is blocked.

Gate evaluation is capability-aware
(`task_service::governance::{ensure_task_runnable, ensure_task_reviewable}`).
Repository mutation (the `worker` WorkspaceLease role) requires the Project
gate to be `active` with no applicable conflict outstanding. Read-only review
of an already-committed result (the dedicated `reviewer` role) may continue
past an outstanding conflict as long as every currently
applicable one is acceptance/evidence/risk/reviewer/release neutral — it
never touches one of the envelope's fixed-boundary fields (fixed outcomes,
fixed acceptance, fixed risk classes, forbidden side effects, release policy,
or elevated operations). Remediation that writes to the repository remains
gated exactly like any other repository mutation; only the read-only review
lease is granted the wider allowance.

### Execution liveness and terminal concurrency

An execution attempt has its own owner-bound liveness contract. The
`execution` row is authoritative for the attempt and carries
`execution_version`, `lease_owner`, `lease_expires_at`, `hard_deadline_at`,
`last_heartbeat_at`, and `last_progress_at`. Migration `V089` adds these
fields without rewriting execution history: terminal rows are ownerless,
existing running rows are made deterministically recoverable, and the legacy
activity timestamp is preserved only as semantic progress. The migration never
fabricates a live owner or heartbeat.

Every running attempt is claimed by exactly one server-issued owner before
provider work begins. Embedded execution uses a server-generated owner and a
server-controlled heartbeat task. Remote execution binds the same lease model
to the authenticated daemon connection incarnation (`daemon:<id>:connection:<n>`);
the daemon id is only a routing identity. Reconnecting creates a new
incarnation, so a stale socket cannot renew, report progress for, or
terminalize the replacement's execution. The opaque `lease_owner` value is
diagnostic metadata, never a credential or client capability.

Lease claim, renewal, semantic progress, and terminalization are repository
compare-and-swap operations. Renewal runs on a fixed server cadence and does
not depend on `TurnEventSink`, JSONL output, text, reasoning, tool calls, or
provider callbacks. Those events advance `last_progress_at` at most once per
second per execution (the live log itself stays in the execution's JSONL
file), and they do not prove ownership. Progress appends a durable
`execution.progressed` event only when it ends a stall epoch that already has
an `execution.progress_warning`, which resolves that Attention item.
A quiet provider/tool call therefore remains live while its owner lease is
current. A stale-progress scan can append a distinct, atomically revalidated
`execution.progress_warning` Attention event; it does not fail a live lease or
turn a warning into owner death.

`hard_deadline_at` is optional execution policy. By default it is `NULL`, so
Forge imposes no wall-clock ceiling: the owner can keep the attempt live by
renewing its short lease. An agent's `config_json.hard_deadline_seconds`
bounds each of its Task executions, and a caller can opt one new execution
into a different limit with the positive `overrides.hard_deadline_seconds`
value, which replaces the agent's; Forge records the
resolved timestamp on that execution, and the first lease claim makes it
immutable. Heartbeats are then clamped to the deadline and cannot extend it.
Generic executor settings such as a shell command's `timeout_seconds` never
become execution deadlines. The monitor treats an expired owner lease and a
reached configured deadline as different recovery causes (`execution.stalled`
with an `execution_lease_expired` reconciliation reason versus
`execution.hard_deadline_exceeded`) and never treats semantic silence alone as
expiry for embedded execution. For a daemon-owned placement or a remote daemon
execution provider, an expired heartbeat lease first suspends the placement as
`disconnected`, even while its TCP socket is open. The execution remains Running;
`max_disconnect` bounds that suspension without extending the hard deadline.
See [Daemon lifecycle and execution recovery](#daemon-lifecycle-and-execution-recovery).

Runner completion, failure, cancellation, daemon-disconnect recovery, and
monitor expiry all call the same terminal CAS with execution ID, expected
`status = 'running'`, `execution_version`, and the current owner where
applicable. The winning transaction writes the terminal metadata, clears the
active execution owner/lease, disposes the matching `WorkspaceLease` (`revoked`
or `expired`), and appends exactly one terminal domain event
(`execution.completed`, `execution.failed`, or `execution.cancelled`). Only
that committed winner cancels an executor, publishes reconciliation, or
cascades Task state. A zero-row CAS is a concurrent winner, not a second
failure: a late runner/daemon result is retained only as a bounded,
deduplicated `execution.late_terminal_rejected` diagnostic (protected error
text is truncated) and cannot overwrite execution, Task, lease, or readiness
truth. The terminal event is a per-attempt audit record and a resolution input
for any `progress_warning`; it does not directly create action-required
Attention or wake an Agent.

### Charter, Documents, Decisions, and effective state

Every Main Agent Chat turn carries a server-owned operating instruction.
Outside an active Product Genesis session, the account baseline skill
`forge.main.baseline/v1` Revision `@4` is in force: it tells the model it is
Forge's Main Agent, hands it the bounded portfolio projection, and restates the
no-Task/no-repository/no-credential boundary. It also routes clear
natural-language new-Project intent through the Main-only typed
`genesis.start` operation, asks one concise question for ambiguous
new-versus-existing Project intent, and keeps non-Project or existing-Project
requests in baseline scope. The browser does not own semantic classification.
The baseline is compiled into the server (each revision's content digest is
pinned by a test, not a seeded row) and the exact revision/digest is frozen in
the turn's context manifest. Historical `@1`–`@3` turns remain reproducible from
their frozen bodies and digests.

`genesis.start` is implemented by one receipt-backed command shared with the
REST start route. Account, Main Chat, and native source-turn authority are
derived server-side. One transaction creates the Genesis session and immutable
instruction/source provenance, appends the durable event and command receipt,
and admits a causally linked discovery continuation. On native success the
source baseline turn terminalizes as a control transfer, its provider loop is
cancelled, and no assistant response is stored; the continuation reuses the
single visible user message and freezes `forge.main.project-discovery/v2` only
after commit. Exact replay returns the committed receipt, while setup, active
session, altered-key input, and internal failures stay structured for the
baseline turn to handle.
The server-created state card includes the active Genesis session ID and its
current optimistic version. Main never infers the active session from chat
history. The turn loader extracts discovery facts from the selected immutable
instruction snapshot, including historical snapshots, but uses the admitted
operating-skill revision's canonical body as the system protocol. Stored
instruction bodies and their manifest digests remain immutable.

Product Genesis uses the server-owned `forge.main.project-discovery/v2` skill
only while its Genesis session is `discovering` or `ready_for_project`. It asks
no more than two consequential questions per turn and keeps facts, explicit
user decisions, research findings, assumptions, hypotheses, and open decisions
distinct. A Charter is append-only: each revision records typed content,
rendered approval view, base revision, provenance, canonical content digest, and
rendered-view digest. An approval receipt is principal-bound, single-use, and
has only `active`, `consumed`, or `revoked` lifecycle. `CreateProjectFromCharterApproval`
consumes that exact receipt and atomically attaches the Charter to one Project.

Project Documents are Forge-owned, revisioned artifacts rather than arbitrary
repository files. Their kinds are exactly `research`, `delivery_brief`,
`product_spec`, `design`, `architecture`, and `execution_plan`. They can be
rendered, diffed, and exported; a repository copy is a derived Task deliverable
and never becomes implicit Project truth. The Project Agent operating contract
is `forge.project.orchestration/v1`; profile instructions may shape tone or
expertise but cannot override it.

The Decision Log is append-only. An effective `DecisionRecord` is only
`active`, `superseded`, or `invalidated`; draft, proposal, approval, and
rejection are editor workflow records outside that effective state set. Forge
does not use a global “latest record wins” hierarchy. It computes a typed
`EffectiveProjectState` by domain, names the governing Charter/
Documents/Decisions/Tasks/checks/milestones/releases, and records a visible
canonical conflict plus `reconciliation_required` reason when authoritative
records disagree. It blocks only the affected execution or readiness path.

#### Main Chat topic epochs

Forge has exactly one account Main Chat. A *topic* is a durable, user-owned
context epoch inside that chat, not a second chat: `agent_chat_topic`
(migration `V103`) records an immutable sequence, label/summary, principal,
timestamp, and the `starting_message_sequence` its epoch begins at. Topic
membership is derived from `sequence >= starting_message_sequence`, so no
message row is ever rewritten and every historical message/turn ID and its
provenance survive the backfill unchanged. Rotation inserts one topic row plus
one ordinary system message that the timeline renders as a divider.

`FederatedAgentChatTurnRunner` bounds a new Main turn's episodic history to the
current topic's floor. A chat with no topic — every Project Chat, and any Main
Chat before its first topic — has floor `0`, so the behavior is unchanged for
them. Canonical portfolio state and unresolved durable obligations are supplied
independently of the epoch, and earlier topics stay inspectable rather than
being injected wholesale. Starting a topic is denied while a Main turn is live
or while a Genesis session/approval needs an explicit finish-or-cancel
decision.

#### Adaptive authority is a closed vocabulary

`AdaptiveEnvelope.allowed_task_operations` is `Vec<AdaptiveTaskOperation>` — a
closed server-owned enum of exactly `split`, `sequence`, and `replace`. JSON is
the bare lowercase string and the generated TypeScript is a closed union, so
every transport shares one vocabulary. These are adaptive *verbs*, never
command names: `task.propose` and `task.adaptive` are commands and can never
appear here. The single source of that vocabulary is `AdaptiveTaskOperation::ALL`
— every diagnostic, input schema, and parser derives from it rather than
repeating a literal list, because a second copy is how an envelope granting
unrecognized verbs were stored in the first place.

Project Agent-authored envelopes receive that complete safe vocabulary by
default. Splitting work, changing its sequence, and replacing an in-scope Task
therefore need no extra user approval. A future settings surface may narrow
that authority explicitly; until it exists, model-authored content cannot
accidentally remove one of those three operations.

Validation runs whenever the field is present and again over the complete
envelope on persisted-receipt replay and governance load. This is defense in
depth through one shared validator, not a per-adapter validator: REST, native,
and any future MCP adapter return the same field path and the same allowed
values.

The user sees that server-prepared correction as one plain-language decision:
**Accept** or **Reject**. The card states that Accept replaces the named record
and keeps technical identifiers collapsed. One transaction approves and
activates the successor, supersedes
(without deleting) the invalid revision, consumes the approval, resolves only
the named reconciliation, and records one receipt/event. It revalidates the
manifest, digests, Charter, milestones, and optimistic versions first, so any
race rolls the whole correction back.

#### Denial and reconciliation are different outcomes

Adaptive admission parses the requested operation as the closed enum *before*
any governance lookup, and its outcomes are deliberately distinct:

| Condition | Outcome | Durable conflict | Execution effect |
|---|---|---|---|
| Operation is malformed or unknown | `validation_error` | none | none |
| Valid `split`, `sequence`, or `replace` | allowed by the current Charter | none | normal Task mutation |
| An authoritative Task/artifact changes an inherited fixed boundary | `reconciliation_required` | exact conflict and affected records | only that change is rejected |

A rejected command that commits no authoritative mutation can never create a
`project_reconciliation_record`. Creating one requires evidence of two
diverging authoritative records or an invalid traceability record, and the
conflict stores the exact affected paths rather than a generic list. Wanting
authority to reshape the Task is not granted by an envelope; the Charter
already permits the three closed verbs.

#### Reconciliation has one shared, scoped command

`services::ProjectReconciliationService` owns Project and principal
authorization before lookup, record/conflict/governing-reference consistency,
expected-version and idempotency checks, action validity for the affected
record type, exact replacement references for `revised`/`superseded`, and the
atomic commit of resolution, canonical-conflict disposition, affected-record
updates, command receipt, and durable event. It publishes after commit and
wakes the exact Task scope affected by the committed change. `GET`/`GET`/`POST` under
`/api/v1/projects/{id}/reconciliations[/{id}/resolve]` are its only adapters.
Resolution is interactive-user-only for every record type; no chat agent
receives a generic self-resolve tool. A registry parity test fails whenever a
Project next action names an operation with no registered handler and
authorized presentation target.

#### Dispatch dispositions and wakes

A deterministic dispatch refusal — a governance denial, an unresolved conflict,
missing setup — is recorded once as a dispatch disposition keyed by Task ID,
Task version, requested capability, and a blocker digest
(`services::deferred_dispatch`). The periodic scan skips an unchanged
disposition without re-attempting it, re-annotating it, or logging it again;
without this, an unchanged blocker was retried and logged every ten seconds
indefinitely and review never ran. Classification is by typed `ServiceError`
variant, never by matching refusal text, because that text belongs to the
blocker projection and keeps changing. Potentially transient failures record no
disposition and retry on the next scan.

Metadata writes do not bump a Task's `version`, so a disposition stays keyed to
the state it observed. Assignment, source availability, reconciliation
resolution, and authorized retry changes call `services::wake_task_dispatch`,
which clears the disposition so exactly one subsequent scan re-attempts that
Task. A wake that clears a disposition or deferred-dispatch marker also bumps
the Task version in the same transaction, fencing a stale dispatcher that
could otherwise restore the cleared marker. Clearing absent markers remains a
safe replay no-op without timestamp or version churn.

#### One SSE transport envelope

`GET /api/v1/events` sends one default-message JSON envelope per frame; the
payload's `event_type` is the sole routing discriminator, and the route sets no
per-frame SSE `event:` name. SSE remains a latency optimization only: durable
queries plus a bounded resync stay sufficient to converge after loss,
reconnect, or an unrecognized `event_type`. Every committed durable event
uses one shared completion identity across consumers: its stored `dedupe_key`
when present, otherwise the bare event ID. Consumers must not invent a
transport-specific fallback, because disagreement with the repository contract
would pin that consumer's cursor at the event forever. Events publish after
their transaction and invalidate the exact query keys they affect; the client
invalidates active authoritative queries once per stream
open, reconnect, or resync, and reconciles a live optimistic turn with a
bounded foreground watchdog. The watchdog follows both a locally optimistic
admission and an authoritative cached server turn while either is live, refetches
messages and turns every 15 seconds, pauses while the document is hidden, and
checks immediately when the document becomes visible. These explicit reads
reconcile stale live turns and can refresh an expired access token; global
refetch-on-focus is disabled. It stops at a terminal or control-transfer state.
Chat and handoff queries use SSE invalidation with 15-second foreground interval
fallbacks; reconnect/resync invalidates active queries. Minimal durable
notifications cover message appends without a same-transaction admission or
completion event (including system/Charter anchors), turn status writes without
a same-transaction completion,
failure or cancellation event (including worker claims/recovery), inquiry
creation/non-ledger terminal writes, and topic rotation. Existing outbox consumers
relay these committed ID/status records to SSE without message bodies. Rust
writers append these notifications transactionally with app-generated IDs and
timestamps. Activity JSONL has no event source: turn/inquiry activity polls every
1.5 seconds only while live, and pauses in the background.

When Task creation omits an explicit governance envelope, Forge derives one
from the Project's current approved Charter. The approved Charter makes a
repository-capable Task runnable once Project-level repository setup and the
ordinary Task gates pass; optional plan-item, milestone, and Document references add
immutable traceability only. `planning_task` and
`discovery` use a read-only repository capability, while implementation Tasks
use the write capability selected by their workflow and assignment. Prompt
selection uses that same execution classification: a discovery/planning Task
dispatched through a workflow role named coder or worker receives the read-only
investigation contract, which asks for findings and an unchanged worktree rather
than implementation and a commit. That classification also suppresses inherited
implementation `ci_steps` during review. A read-only Task can still carry an
explicit requirement-linked conformance check designed for its research output.

#### Milestones, readiness, and immutable releases

Milestone definition revisions use only `draft`, `proposed`, `approved`, and
`superseded`. The milestone instance lifecycle uses only `planned`, `active`,
`ready_for_release`, `released`, and `cancelled`; blockers, stale results, and
`reconciliation_required` remain typed projections while an unreleased
milestone is `active`. Multiple milestones may be active, and the Project keeps
an explicit `primary_milestone_id` whenever at least one milestone is `active`.
Once selected, that pointer remains on the emphasized outcome while it advances
through `ready_for_release` and `released`; delivery does not erase the
Project's milestone context. It is repaired only when the target is removed
from the intended outcome set, such as cancellation. The primary is never
inferred from recency or Task counts. Compact Project creation supplies
`M001` (shown as `M1 — Deliver outcome`) when no other definition is present.
Each required compact acceptance check is created with a required evidence
requirement carrying the same stable ID; a passing result never substitutes
for proof.

Forge persists one immutable `ReadinessSnapshot` per standalone evaluation.
Repository/build context permits an absent remote URL for local-only
repositories; blank remote URLs normalize to `NULL`/`None` at repository writes
and context reads. Repository identity, name, work mode, default branch, Task
version, and observed timestamp remain required immutable metadata.

Each snapshot records the exact input manifest, source versions, evidence attachment
IDs/digests, policy references, result (`ready`, `blocked`, `failed`, or
`stale`), and readiness digest. A ready snapshot moves an unreleased active
milestone to `ready_for_release`; non-ready results leave it active with typed
reasons. Readiness creates no release pins. A user release request must name
the exact snapshot ID and digest; Forge re-authorizes and recomputes that
digest inside one transaction before creating the immutable `Mxxx-rN` manifest,
release-scoped evidence pins, lifecycle transition, and events. `released` is
terminal; later corrections append the next release revision and never mutate
history. Forge release is an internal frozen evidence snapshot, not a merge,
tag, deployment, or external publication.

Before evaluating or recomputing readiness, Forge checks the milestone
definition's own acceptance contract: every required acceptance check must have
a required evidence requirement under the same stable id. This keeps a
milestone from demanding proof it never asks anyone to attach. A malformed or
drifted contract fails closed as
a persisted non-ready snapshot with typed `reconciliation_required` reasons;
it cannot produce a ready snapshot from an empty or unrelated definition. Live
validation summaries and Project Overview check counts likewise include only
rows belonging to each milestone's current definition revision, never checks
or results left behind by a superseded definition. `project.current_state`
includes those exact current check IDs and evidence requirements so a Project
Agent does not have to reconstruct them from validation errors.

A Project Agent readiness action invokes the same `MilestoneRuntime`
evaluation as the authenticated REST route and returns the committed snapshot;
it is not a request event awaiting an absent consumer. A Project Agent release
candidate remains non-authoritative: Forge admits it only for the exact current
`ready` snapshot and milestone version, records the candidate, and raises human
attention for the user-only release decision. Blocked, failed, and stale
snapshots return their canonical reasons and create no candidate; the operating
contract requires the agent to report those blockers rather than claim a
release or `Known Issues: None`. The candidate-request event is an attention
projection only; it does not change the governed readiness inputs or advance
the readiness source watermark. A candidate therefore cannot make its own
readiness snapshot stale, and it cannot be treated as a release approval.

#### Shared media and evidence lifecycle

Task media and Project evidence can share one Project-authorized binary asset.
The forward migration adds ownership, attachment, evidence, and release-pin
metadata around existing rows; it preserves every existing asset ID, Task
media ID, Task URL, storage key, metadata, and file byte in place. It neither
moves nor duplicates bytes and makes no on-disk layout-break claim. The existing
Task media routes continue to authorize through the active Task attachment.
Attaching the same asset to a milestone creates metadata only and does not add
it to another Task's list. Deleting a Task or Task attachment makes its Task
URL unavailable under the existing policy; a release pin keeps the same bytes
retained for the stable authorized Project evidence URL while the asset remains
available.

Evidence attachment metadata uses exactly `available`, `quarantined`,
`redacted`, or `purged`. The public remove operation marks an attachment
`purged`; readiness excludes unavailable evidence, and the Project media route
serves bytes only while the shared asset is `available` and authorized. A
cleanup worker re-checks active Task/Project attachments and immutable release
pins under a lease immediately before deleting bytes, so restart and Task-delete
races cannot remove still-referenced evidence. Release pins remain immutable.
For evidence that satisfies a release-gating requirement, the attachment is
also bound to the exact source context that produced it: the Task revision,
execution/run (when applicable), validation result and digest, acceptance-check
definition revision, and bounded build/commit context. A caption, an
acceptance-check link, or a currently available binary is not freshness proof
by itself. Missing or mismatched context is projected as stale/unusable and is
excluded from a ready result until a new authorized attachment is captured.
The ordered readiness input manifest stores these source identities and
digests, and the release transaction rechecks them before pinning evidence.
Cleanup isolates failures per asset and per phase, so one poisoned upload or
filesystem entry cannot stop unrelated reconciliation or garbage collection.
Successful recovery of a purged asset is checkpointed, allowing later rows to
advance through the bounded sweep instead of repeatedly occupying its first
page.
V076 and the internal shared-media repository persist an audited redaction or
purge tombstone, retain the permitted checksum/audit metadata, and project a
pinned release's evidence as `evidence_unavailable` without rewriting its
manifest. Authorized Project owners/admins invoke `POST
/api/v1/projects/{id}/media/{asset_id}/redact` or `POST
/api/v1/projects/{id}/media/{asset_id}/purge` with a
`ProjectMediaTombstoneRequest` carrying the asset version, idempotency key,
explicit user authorization (`project.media.redact` or `project.media.purge`),
and a bounded reason. Redaction blocks serving through the Project media route
while retaining bytes; the legacy Task media route keeps its existing behavior
while the Task attachment remains active. Purge records the same immutable
audit data, removes bytes, and both dispositions overlay every affected release
pin as `evidence_unavailable`; after purge neither former URL serves the bytes.
Neither route rewrites the immutable release manifest, and neither accepts a
storage key or raw bytes.

#### Project Overview projection

`GET /api/v1/projects/{id}/overview` is a read-time projection over the
authoritative Charter, Document, Decision, execution-setup, Task/validation,
milestone, evidence, readiness, and release records. It is never a second
editable truth store. A document entry reports both the approved revision
that governs execution and any newer working draft/proposed revision; a newer
working revision is `changes_pending`, not proof that the approved revision
has disappeared. The typed document status is one of `current`,
`changes_pending`, `stale`, `reconciliation_required`, or `unavailable`. A
draft-only Document is also `changes_pending`; an invalid approved pointer,
incompatible governing source, or source-version mismatch is a typed
stale/reconciliation condition. Effective Decision Log records are projected
separately from draft/proposed candidate IDs so editor workflow is never
presented as approved authority.

The projection's `next_action` is typed rather than a display-only sentence.
It identifies an action `code`, the required principal, target type and id,
human title/explanation, `action_kind`, canonical route or operation,
whether it blocks the Project, and the expected version when a compare-and-swap
mutation is required. The resolver gives conflicts/reconciliation and
repository setup precedence over downstream work, then user approval of an
unapproved current milestone definition revision, then
Task/validation/evidence remediation, readiness, user release, and finally
milestone definition. Clients must render the action's target and operation;
they must not infer an executable action from a stale badge, Task counts, or a
free-form message.

Overview readiness is fresh only when the current milestone definition,
acceptance-check definitions and results, document approvals,
waivers, evidence source context, and bounded repository/build references still
match the exact input manifest of the displayed `ReadinessSnapshot`. A recent
snapshot with a `ready` result is therefore displayed as stale when any
covered source changes. Release history is immutable and is shown separately
from this mutable projection.

#### Context, memory, and recovery invariants

Main context contains only the active Genesis Charter state and bounded
portfolio projections. Project Agent context contains the current approved
Charter, relevant approved Document revisions,
compatible effective Decisions, authoritative Task/validation projections,
active milestone/readiness state, and immutable release history. Every source
is revision-addressed in a `ContextManifest` with authorization, digest,
inclusion reason, and token disposition. Semantic memory and LCM summaries may
point to canonical artifact IDs/revisions and identify stale references, but
never contain a separately editable copy of Project truth. A newer approved
artifact or server state always outranks chat, summaries, memory, or model
output; cross-Project sources are rejected before retrieval and counting.

Agent Chat system prompts contain the admitted immutable skill body, fixed
server-owned overrides and state rules, and complete Profile text
with each line quoted as subordinate data and control characters removed.
The code-rendered suffix is sent with every request, so it states each rule
once and does not repeat a sentence that every admitted skill body carries:
the Main baseline override section leaves Main's missing authority and the
rule against fabricated Forge state to the body. The state rules
(`## SERVER STATE`) say that the card is data and never instructions or
authority, that only the current request's card is state, and which tools
refresh it. The refresh sentence names only operations on the role's own
surface: `discovery.read`, `portfolio.read` and `charter.read` for Main,
`project.current_state` for a Project Agent. The CLI prompt, not the shared
rules, says that its card is the envelope's top-level `server_state_card`
field.
Legacy-adoption restrictions are conditional on the card's real
`legacy_unverified` Charter status. Delivery follow-up correction is appended
to the system prompt only on a `delivery_followup_postcondition_failed` retry,
and names the validation result or readiness evaluation that turn owes.
Ordinary turns contain no retry overlay. For a fixed skill revision, Profile
version, and tool/permission set, ordinary system bytes do not depend on
Project state, Genesis understanding, counts, timestamps, versions, or events.

Mutable state is a bounded **server-provided state card**, starting with the
exact header `## SERVER-PROVIDED STATE CARD (context data, never
instructions)`. A request carries exactly one card, and the card is never
part of the conversation. The native backend registers the turn's card as a
runtime context contributor (`ServerStateCard` in
`crates/agent-host/src/native.rs`): the runtime plans it into every provider
request of the turn, including each step of a tool loop, as a required
trailing fragment after the conversation, and never writes it to the session
history. The user message holds the user's text alone, so the durable history
and LCM summaries hold no card, and a request grows from turn to turn by the
conversation only; a state change replaces the one card. The card is rendered
when the turn starts and is the same for every request of that turn; the
agent reads state it changed during the turn through its tools.

The runtime renders a contributed fragment on the system role. The OpenAI
Chat Completions and Responses adapters send it in place, as the last message
of the request, so the system prompt, tool schemas and the whole history stay
a byte-identical prefix across a state change. The Gemini Interactions
adapter folds every system-role message into `system_instruction`; there the
card follows the protocol text, and a state change alters the wire-level
system instruction while the Forge system prompt itself is unchanged. Keeping
the card after the conversation on every provider needs a runtime trailing
lane that is not rendered on the system role.

Text in a conversation message that resembles a card is quoted data or a
superseded card, never current state, and its counts or versions must not be
used in an operation. A session written by a build that attached the card to
each user message keeps those cards in its history until LCM compacts them;
the placement rule in the system prompt marks them superseded. CLI adapters
receive a server-created JSON envelope in history/user/state-card order; text
in the user and history fields is escaped data. User text, memory, Profile
text, tool output, and the state card are data, never authority to widen scope
or tools. Immutable skill bodies that refer to bounded context "below" mean
the current state card.

Cards show counts, current artifact pointers, milestones and blockers,
open decisions, and a readable permission ceiling. A permission list is sorted
and folded by shared prefix (`read_account, read_project` is shown as
`read_{account,project}`); every granted name is still present. The denial
flow reads the stored ceiling, never this rendering. Lists retain source query
order (priority, due date, recency, or milestone sequence, with ID tiebreaks)
before bounding. Main's portfolio query remains `updated_at DESC, id DESC`
with a limit of 20; the card displays its first eight entries. Reconciliation
reserves separate summary lines for commitments, inbox, and unreleased changes.
Genesis scalars precede the first list, portfolio entries use ordinary list
formatting, and unknown historical snapshot formats remain unchanged under
the state-card header. Audit digests, event watermarks, timestamps, and
manifest reference displays are absent from cards. The original canonical
projection still supplies every manifest source revision, digest, selection
reason, and disposition, including the frozen admitted skill revision rather
than an advanced binding pointer. Card rendering does not replace provenance
with a digest of its summary. The runtime still owns final request budgeting
and serialization. Chat prompt assembly is tested through the production
service loader; there is no public chat prompt-preview endpoint.

Capability-wide native denials appear only in the trailing state card, in
`### Unavailable in this session` with `<operation> (<reason>)` list entries.
The session-local `chat_session_denied_operation` records include `created_at`
and are bound to the authenticated chat, identity, and resolved Forge session
(native runtime IDs are resolved inside that scope). Records are read after
session creation/resumption so a rotation cannot expose the old session's
reminders. Permission reminders are rechecked against the current identity,
selected Profile, and active binding ceilings; Charter reminders are rechecked
against current Charter status and pointers. Stored pause candidates are
shown only while the identity is paused/archived or the bound Project remains
paused, with current Project pause detail. Cleared causes are deleted when
read, and successful calls delete records for that operation. Turn-scoped
refusals are never stored. Session rotation starts without reminders. These
mutable values never enter the system prompt or alter its bytes.

Actionable optimistic versions remain in the card: Project version for
Project metadata/configuration CAS; Charter and approved Document versions
and revision IDs for artifact edits; milestone version and definition revision
for milestone/check actions; reconciliation, commitment, and inbox versions
for their versioned updates; Genesis session version and current Charter
version/revision for discovery and approval operations. Main portfolio entries retain
Project IDs and versions. Decisions and releases are immutable references.
Agents refresh `project.current_state` for Project artifact revisions, versions
and digests. For Main, `discovery.read` returns Genesis session IDs, lifecycle
and session versions; `portfolio.read` returns Project IDs, names and lifecycle
metadata without versions. `charter.read` returns Charter revisions, versions
and digests for typed Charter operations. Genesis IDs elsewhere in Main Chat
history are historical and cannot change the current state-card binding.

Genesis Project creation, binding, Project Chat, Charter attachment, handoff
message/turn, immutable Project admission receipt, events, `handed_off`
transition, and Charter-approval receipt consumption are one database
transaction. A failure leaves Genesis `ready_for_project`, the exact approval
receipt `active`, and no partial Project or handoff; retry with the same
idempotency key returns the original committed result if one exists, even when
the original Project binding has since been replaced.

Native operations are classified by one host-owned catalog before descriptor
exposure and again at the service boundary. Queries use read services;
automatically allowed Main/Project/Task coordination writes call their shared
command service directly; approval-required or explicitly audited proposals
create an `AgentAction`; denied operations are omitted and rejected. Direct
commands commit their domain result, durable event, and frozen command receipt
without an Action or ActionExecution row. Fresh Project and Task commands
recheck the current identity, selected profile, active binding permission, and
applicable Charter/governance state inside the same `BEGIN IMMEDIATE`
transaction after receipt lookup and before mutation. Exact committed replays
return the frozen result even if mutable authorization later changes. Mission
Control projects these receipt-only commits separately from pending/approved
approval Actions and never exposes an Action payload body.

Each migrated operation has one canonical contract in the same catalog. The
contract declares its native surface and exposure, setup availability,
supported canonical scopes, command classification, scope-aware permission,
input-contract family, and the shared output envelope. Provider JSON schemas
and preparation-time structural checks are derived from that contract; service
adapters look up the same operation and permission metadata and retain only
domain/lifecycle validation in the command service. The current MCP registry
contains no migrated dotted orchestration operation IDs, so its existing
`forge_*` direct APIs remain explicitly separate. An invariant test prevents a
migrated operation from entering MCP through a second manual descriptor; a
future MCP projection must consume the canonical contract.

Native and MCP orchestration adapters expose those command results through one
typed `OrchestrationOutcome` envelope. It carries `code`, `status`,
`operation`, canonical `scope`, optional `result`, `approval_target`,
`setup_requirements`, `current_version_or_revision`, and `retry`, plus the
always-present `safe_message`, `correlation_id`, and boolean `replayed`; a
committed receipt/event may add `receipt_id` and `event_id`. The stable codes
are `ok`, `approval_required`, `setup_required`, `version_conflict`,
`digest_conflict`, `idempotency_conflict`, `policy_denied`, `not_found`,
`transient_failure`, `internal_failure`, and `validation_error`. Status is
`succeeded`, `approval_required`, `setup_required`, or `failed`; replay is
represented only by `replayed`, never by a synthetic status. Approval and
setup outcomes never claim a committed or executable domain success.

The native adapter keeps safe domain failures in-band as a model-visible tool
value with the runtime error marker, while MCP returns a known-tool failure as
a JSON-RPC success result containing `isError: true`, `structuredContent`, and
text `content`. JSON-RPC parse/invalid-request and method/protocol failures
remain top-level errors. Both adapters redact protected persistence/runtime
causes. A current version/revision or retry argument is emitted only after the
canonical principal and scope have been authorized and only for state the
caller may inspect; idempotency conflicts do not load current state. Models
must use the stable code and typed fields rather than infer corrections from
prose.

Native policy refusals carry a typed `DeniedBy` cause, serialized as a safe
string. Its `scope()` and `clears()` rules are shared by the native adapter,
host cache, and service reader. Only `permission_missing`,
`charter_not_adopted`, `identity_paused`, `project_paused`, and the catalog's
`operation_not_in_scope` withdraw an operation. These session-scoped causes
carry `retry: {action: "none", retryable: false, scope: "session", ...}` and
say that repeating the call will be refused while its cause holds. Other
specific causes apply to the request for the turn and are neither cached nor
recorded. A paused target Agent returns `target_agent_paused`; a Project
pause on `task.action` is turn-scoped because it does not block every recovery
action. Neither withdraws the operation. Unknown prose maps to `unspecified`:
a neutral turn-scoped refusal with `retry.action: none` and `retryable: false`.
Subsequent calls are evaluated normally; only invalid arguments suggest
`correct_input`. Input-shaped errors return validation outcomes, while
current-state load errors return internal failures. Known own-scope evaluator
reasons stay precise; protected detail stays redacted with existing
missing/inaccessible-scope equivalence. Server-side diagnostics retain the
original reason and mapped cause. A held, useful `message.send` alternative
is included when policy admits it; only then does the message suggest
escalation to the user.

`ScopeToolComposition` shares an allowlisted denial cache across native
operation tools, keyed by runtime session, turn, and operation. Calls check
the cache, release the lock before provider invocation, then lock to store
completed denials. Later calls return the exact original terminal denial;
already-running parallel calls evaluate independently. A failed reminder
write is logged and still returns the denial. The Agent Runtime seals and
caches tool schemas at registration, so the advertised operation enum cannot
be withdrawn between model calls without a runtime change. Forge retains
the catalog and fails repeated calls fast; a new turn evaluates normally and
receives only still-valid session reminders. The database loads reminder rows
without a transaction. Services recheck permission and Charter causes through
the real operation policy, then delete expired rows in separate statements.
Read/recheck failures are logged and produce an empty reminder list. Success
clears only a reminder found by the turn's state-card read; the host parses
structured outcomes only for failed calls.

After that transaction commits, `project_provisioning` reconciles execution
setup as one durable, leased, finite, idempotent operation (also resumed on
creation replay). Its checkpoints cover preflight, repository scaffolding, filesystem
initialization, repository registration, Project linkage, and role assignment.
When the approved Charter carries a `scaffold` block, `repository_scaffolded`
runs the configured create-spark command (`FORGE_SCAFFOLD_COMMAND`, exported
by `forge-cli` from `[scaffold] command`) into the deterministic repository
directory, exports the approved Charter revision to `docs/spark/project.md`,
and appends a Forge section to the scaffold's `AGENTS.md`; otherwise the
checkpoint is `skipped`. It then initializes
or verifies a local git repository (first commit on `main`, holding the
scaffold when one ran) under `<workspace_root>/repos/`, registers or reuses one matching logical repository,
links it with Project-version CAS, and resolves canonical Worker and (when
required) independent-reviewer assignments from current workflow
policy. Main/Project-bound identities are never eligible, and a credential-less
bootstrap default is never chosen automatically (an explicit assignment remains
possible for locally managed CLI authentication). A missing Worker or
reviewer is a typed `setup_required` blocker in the current setup projection;
it is not an operator-only log or an executable success. Replays reuse the
operation, checkpoint target path, directory, repository row, Project link,
and role set. Agent `task.propose` executions on a
Charter-backed Project without an explicit governance envelope are bound
server-side to the current approved Charter; the proposal payload carries only
`plan_item_id` (required for implementation Tasks), optional `milestone_id`
(defaults to the Project's primary milestone), and optional capability/risk
classes. A proposal that names a plan item or milestone retains that exact
traceability even when its capability is read-only (for example, an independent verification Task); those references
are never discarded merely because the Task does not mutate the repository.
Optional `depends_on_task_ids` are re-authorized as accepted Tasks in the same
Project and inserted in the same transaction, so implementation → verification
ordering is scheduler-enforced rather than narration-only. Task proposal
execution commits the Task, governance projection, prerequisite links, durable
`task.created` event, and command receipt in one transaction; an exact
response-loss replay returns the frozen Task snapshot.
A ReadyOnly Project Agent may likewise invoke the native `task.adaptive`
Coordination operation in the bound Project or its Project Agent Chat. The
adapter accepts only the closed `split`, `sequence`, and `replace` payloads
with explicit source-task/version and board-revision preconditions. Project,
scope, actor, permission, governance, and fixed execution boundaries are
server-derived; unknown fields and attempted overrides are rejected. The
adapter calls `TaskService` directly, so no `AgentAction` or
`AgentActionExecution` is created. Its bounded result carries the frozen
receipt/event identity, source and affected Task ids, board revision, and
`replayed`; receipt-first exact replay bypasses mutable governance checks,
while a new command is authorized and validated inside the shared transaction.
A ReadyOnly Project Agent uses caller-filtered `task.action` offers for the
Tasks in its bound Project. Each command carries the Task id, exact version,
and a closed action object. Cancellation stops active executions and enters
the workflow's cancellation state. Only the owner may override a gate. Missing
or unauthorized offers return `action_unavailable` with current offers; stale
versions remain version conflicts. Recovery commits its condition change and
wakes the existing dispatcher, which waits for execution capacity.
A Project's coordination/chat state, repository setup, and execution projection remain
independent projections. Each setup response carries per-dimension freshness;
an unavailable source is returned with a bounded retry action and never
converted into inferred readiness. V087 backfills only database-verifiable
repository linkage. It records
local repository initialization as skipped with `filesystem_verified=false`
until a reconciler verifies the filesystem, and never fabricates identities or
successful setup. A release or media-pin failure leaves the milestone
`ready_for_release` with no
partial manifest or pin. Migration failures leave legacy media references and
bytes usable; physical cleanup is a separate guarded operation. These recovery
rules make replay safe without inventing approval or silently substituting a
name, Charter revision, artifact, or evidence asset.

Project deletion is a transactionally guarded teardown. It removes the
Project-owned immutable graph in dependency order — including Project/Task/
Project-Chat LCM timelines, entries, operations, and nodes — and then the
Project itself; the database permits those deletes only while the exact Project
deletion guard is active. Before authoritative deletion, the API requires
Project owner/admin authorization and, for `?force=true`, provider-acknowledged
execution cancellation plus Workspace lease revocation. The final
`BEGIN IMMEDIATE` transaction captures the exact Task IDs, Workspace paths, and
Project repository paths present at its boundary; it does not rename live paths
before commit. After commit, the API reacquires `BEGIN IMMEDIATE` immediately
before each confined cleanup and skips any captured Task, Workspace, Project,
repository, or cache path that has a live replacement owner. Eligible direct
children are atomically moved to unique quarantine siblings while that lock is
held, then recursively removed after commit. It best-effort removes the
remaining captured paths, managed caches, and the Project Agent directory,
confined to Forge-controlled direct-child roots. Cleanup failures
are logged while the already-committed deletion still returns `204`; arbitrary linked repositories outside
Forge-controlled roots are never removed. Direct attempts to mutate or delete an
individual immutable Charter, milestone, readiness, release, decision,
lease, evidence, review-contract, or review-assessment record remain rejected.
V134 rebuilds the V132 conformance foreign keys and delete guards without losing
rows, allowing those review records to cascade only during parent teardown.

### Provider entry health

Migration V144 stores the outcome learned from direct provider calls, agent
creation probes, and live connection tests for each credentialed entry.
HTTP 429, provider 5xx, and network failures back off exponentially from 30
seconds to 15 minutes; usage exhaustion waits for the
reported reset when available. Auth failures have no timer and require a
successful manual connection test to clear. A successful provider call clears
a transient failure. The first call after a timed backoff is the trial: another
failure increases the delay.

`compute_effective_status` reports the entry's agents as degraded while the
entry is unavailable, so new Task dispatch skips them. The Agent Chat claim
query leaves queued and retry-wait jobs unclaimed during backoff without
incrementing their attempt count. The provider list projects the stored
redacted failure and retry time. CLI runtimes retain their separate account
cooldown behavior.

### Usage accounting and provider pricing

Usage accounting is an append-only ledger at the provider-call grain. A Task
execution remains one workflow attempt, but every fallback candidate that
actually reaches a provider owns a distinct `usage_invocation`; Project Chat,
ordinary Main Chat, Genesis Chat, and Main inquiries use the same lifecycle.
Candidates skipped before a provider call remain route provenance and create no
invocation. Historical attribution is frozen from admission-time identity and
pricing-subject revisions rather than reconstructed through mutable Agent or
provider bindings.

Every runtime selection, invocation, and usage row belongs to an account; only
migrated pre-V135 history may stay ownerless. Task admission resolves that
account from the Project owner, else the Agent owner. Projects and Agents are
expected to be owned, but a missing owner never skips admission: the execution
is admitted under the instance's first administrator (the account that claims
ownerless resources at bootstrap), else the earliest account, and resolves the
models.dev list price unless that account configured an adjustment for the
same provider entry or CLI runtime. Embedded and remote terminal usage reports
therefore always find an admitted candidate. Only an instance with no account
at all leaves a Task attempt unadmitted; it stays visible in domain-run
denominators as no-provider/no-usage coverage.

Admission freezes the eligible immutable rate revision for every Task route
candidate, or for the selected chat/inquiry provider call. Immediately before
external provider work, Forge durably moves the stable invocation to `started`;
if that write fails, it does not call the provider. Normal completion commits
the domain terminal compare-and-swap, invocation settlement, non-overlapping
usage events, and durable domain/outbox event in one SQLite transaction. A
cancellation that wins while provider work drains keeps the domain result
cancelled and moves the accounting obligation to `pending_settlement`; the
drain may settle usage later without changing the cancellation. Recovery marks
a started obligation `unsettled` only when no replayable provider result
survives, and any provider retry receives a new attempt identity.

Usage events carry disjoint nullable input, output, cache-read, and cache-write
counters. Explicit metered zero is distinct from absent telemetry. The native
runtime's provider adapters split one reported prompt total into those buckets
and drop a bucket only when it is zero, so the host reports every bucket of a
metered attempt (absent ones as `0`) and each attempt's prompt size (the three
input buckets) as `context_tokens` for context-tier selection. A bucket a
report omits blocks the estimate only when the frozen rate charges a positive
rate for it. A
reported-money-only call creates one event with null counters; a settled call
with neither counters nor reported money creates no event while its invocation
remains visible as unmetered. Event and invocation idempotency keys come from
immutable domain/request identities. Exact duplicate reports are no-ops after
payload equality validation; conflicting reuse is a version conflict, never an
additive update.

Each usage event is one provider call, and totals are plain sums of events, so
a call must be reported by exactly one turn. Agent Runtime keeps one
append-only usage ledger per session, and Agent Chat and Task worker/planner
sessions persist across turns. The native host therefore notes the ledger's
length when a turn starts and reports only the records appended after it;
earlier records were reported by the turn that made them. A Forge-level retry
of a turn gets a new invocation and records only its own calls, while the
failed attempt keeps the calls the provider metered for it.

Remote daemons transport the complete per-candidate usage vector and retain a
terminal notification until the server acknowledges the composite transaction.
Daemons that do not advertise the required protocol revision are rejected
before dispatch so Forge never silently falls back to flattened or missing
accounting. Late owner/CAS losers cannot append usage.

Provider rates are immutable estimate inputs, not billing authority. Forge
refreshes the fixed models.dev catalog endpoint only through an explicit
authorized operation, activates a candidate snapshot atomically after bounded
validation, and keeps the last known good snapshot on failure.

Rates are resolved at admission by `services::pricing_auto`, inside the
admission transaction. It provisions the pricing subject (provider entry or
CLI runtime) and its non-secret revision, picks the effective
`pricing_adjustment` (the agent's own, else the subject's, else list price),
matches the runtime model to one models.dev row (pin → `provider/model` id →
subject provider → model-family provider → sole provider), and makes exactly
one binding active in the scope admission resolves: `''` for the provider-wide
binding, `agent:<id>` for an agent with its own adjustment. Discounted and
fixed prices are materialized as manual rate revisions, so a frozen selection
still references one immutable rate revision through one active binding.
Missing identity, rate, or tier evidence stays unknown. Adjustment and catalog
changes affect future admissions only, while an explicit retrospective
operation may create a separately versioned estimate.

New money paths use integer nano-USD values and an `i128` intermediate. Forge
sums the four measurable token buckets, divides once by one million with
half-away-from-zero rounding per usage event, and aggregates stored event
amounts. Provider-reported money and Forge estimates remain separate, never
contribute twice, and are projected with explicit complete, partial,
unavailable, pending, or no-usage coverage. The ledger stores only bounded
identifiers, counters, timestamps, amount/rate provenance, and redacted reason
codes—never prompts, completions, reasoning, tool content, schemas, commands,
files, credentials, or raw provider streams.

### Direct Agent Runtime host and LCM

Agent Settings at `/agents` is the single account-owned surface, organized as
three tabs over one model: `Providers` (configured provider entries — multiple
entries per provider type — plus CLI runtimes discovered on daemons), `Agents`
(the roster of direct and harness agents, each referencing one authentication
source), and `Bindings` (the Main Agent binding, the optional Project Agent
binding via a `?project=` deep link, and the read-only chat-scope list; `?tab=`
deep-links any tab). Provider setup is driven by a server-owned capability
catalog that also declares runtime compatibility per credential method; agent
creation re-validates that matrix. Browser and device login create finite,
account-owned authorization operations; only bounded public state is returned,
while callback state, PKCE verifiers, device codes, token bundles, and client
secrets are encrypted beside the existing protected runtime state. A completed
authorization publishes a provider entry only. Connection, agent creation, and
binding are deliberately separate transactions.

Availability is an orthogonal, reversible source policy. A provider entry has
its own enabled bit, CLI policy keys by exact `(daemon_id, executor_type)`, and
the existing Agent `paused` state controls one identity. Effective health
intersects these layers, so disabling a source makes every dependent identity
ineligible for Main, Project, Worker, and reviewer selection without deleting
configuration, credentials, bindings, or history. Re-enabling recomputes health
normally. Task concurrency counts Running executions plus `reserved`/`preparing`
placements that have no Running execution yet. Assigned Tasks without a
reservation and Main/Project Agent chat turns do not consume the identity's
`max_concurrent_tasks` quota. Every execution machine also has a run cap:
`server.max_concurrent_runs` for the server host, or the daemon's typed
`max_concurrent_runs`, limited further by its positive `run_limit` when set.
Unset configuration resolves to `max(2, logical_cores / 2)` on that machine;
zero means unlimited. The embedded daemon and direct server execution share
the server setting and any admin limit on the embedded row. A daemon record with no reported cap and no admin limit
is unlimited until it reports a value; omitted registration/report fields retain
the last recorded cap. Labels no longer configure capacity. Migration
`V202610020900` carries the first positive integer label cap in
`max_concurrent_sessions`, `max_sessions`, `active_session_cap`,
`max_concurrent_tasks` order into the typed column.

Machine occupancy uses execution placement, including unpinned Agents:
Running executions plus `reserved`/`preparing` placements without a Running
execution for the same Workspace, plus leased/running Agent Chat turns.
Expired reservations are excluded directly by the occupancy SQL, including
Operations and execution-start reads; no sweep is needed to release their count.
Server host occupancy combines placements with no execution daemon and those
routed to this host's embedded daemon; chats of unpinned or embedded-daemon
Agents use that same slot pool. Workspace-less executions use their frozen
executor daemon id, falling back to the Agent pin. Ready placements use no slot. Capacity is rechecked at start; a refusal
leaves the workspace ready and parks the Task for a later tick. Review check runs and merges do not consume slots: running checks
currently have no durable record to count. This is a known limit pending the
workflow refactor.

The resolved server configuration initializes one shared `MachineRunCap`
plain handle defined by `db` during runtime composition. Bare `SqliteDb::new`
instances are unlimited. The runtime resolves the automatic default and cached
embedded identity; `db` has no configuration/hardware-discovery dependency.
The settings route owns its write mutex. Settings writes are serialized;
only after the YAML write succeeds does the route update that handle.
Placement reads the handle afresh in each reserve/start transaction rather than
reading the startup snapshot. This setting never requires restart; other
settings retain their existing restart behavior. Both Agent and machine caps
are checked in the `BEGIN IMMEDIATE` reserve transaction and rechecked at
execution start. No running work is stopped when a cap is lowered;
dispatcher/service prechecks are only early filters. The
transaction also rejects an identity paused or switched to a newer selected
profile after dispatch preflight. Profile replacement covers daemon, provider,
and executor configuration reassignment even when the numeric task cap is
unchanged.

Harness agents may reference a provider entry (`credential_ref` on the active
profile). At dispatch, `TaskService` asks `EmbeddedAgentService` to inject the
entry's API key into the in-memory executor snapshot as the provider's
environment variable; the stored snapshot, events, and logs never carry the
key, and OAuth bundles are refused for harness injection. Harness agents
without an entry keep their CLI-managed login and are surfaced from daemon CLI
discovery. Adapter health parsers treat explicit negative authentication
phrases as authoritative before considering positive phrases; executable
presence alone is never evidence that a harness can accept work.

Credential handles distinguish static `api_key` payloads from renewable
`oauth_bundle` payloads and carry optimistic versions. Native adapters acquire
short-lived leases through Agent Runtime's `ProviderCredentialSource` rather
than receiving plaintext configuration. Expiring bundles refresh under a
per-credential single-flight lock and rotate ciphertext plus the handle version
in one transaction. Exact-revision invalidation prevents an older rejected
request from invalidating a newer lease. Provider errors, events, public rows,
and Debug output remain redacted.

An immutable direct ChatGPT-login profile may carry one reasoning effort
advertised for its selected model by the Codex catalog, including `xhigh`,
`max`, and `ultra` where supported. Native Main Chat, Project Agent Chat, Task,
and inquiry turns copy that profile value into Agent Runtime's reasoning
config; an omitted value leaves provider behavior unchanged.

The Project Agent route is an Agent Workspace: one durable conversation beside
a typed Project-record rail on desktop and a Conversation/Project segmented
view on compact screens. The rail calls the existing Project, Task, artifact,
Decision, and milestone services and surfaces saved/conflict/error receipts.
It does not widen authority: neither Agent can cause repository work except by
creating or admitting a Task through the workflow.

Each does hold a workspace of its own, and neither is the repository. A Project
Agent Chat gets `WorkspaceAccess::ProjectVerify` — a disposable checkout it may
read, write beside, and run commands in, so it can exercise the delivered
software itself — with no commit or push route out of it. A Main Chat, and every
ephemeral inquiry sub-agent it dispatches, gets `WorkspaceAccess::AccountScratch`:
a plain directory holding no repository at all, where "cannot write to a
repository" holds structurally rather than by policy. A chat scope with neither
provisioned stays at `WorkspaceAccess::Deny`.

Commands in any of those three workspaces — and in a Task worktree — are bounded
by one allowlist of bare program names, resolved per turn from the built-in set,
the owner's `commands` config, and the owning Project's `command_allowlist`
settings, and carried on the turn request so model input cannot reach it. The
list bounds the blast radius rather than sandboxing it: `bash` is on it, and a
shell reaches whatever the workspace does. What it bounds is which tools exist
at all, as the owner's decision rather than whatever `PATH` offers.

Both chat scopes and a Task worker may also fetch a public page through the
runtime's built-in `fetch` tool, gated on the same permission as the search
tool (a Task worker on its read permission) and on the host supplying a
transport. Forge's transport pins each request to addresses it resolved and
accepted, refuses anything but `https` without credentials, follows redirects
only inside the origin the runtime authorized, and caps the body — so a public
hostname that resolves into this machine's network is refused before a socket
opens, and a cross-origin redirect costs another authorized tool call instead
of arriving under the first one's permission. The authorization gate refuses a
network resource that is not a public https `GET`.

`forge-agent-host` composes Agent Runtime directly at the revision pinned in
the workspace `Cargo.toml`. It owns provider construction,
protected credential/checkpoint/session stores, interaction handling, runtime
events, content guards, usage mapping, cancellation/steering capabilities,
typed tools, and scope-derived workspaces. Forge does not depend on the sibling
TUI and does not add a Smith-native backend; Smith remains an existing CLI Task
executor/profile. Accordingly, a Smith-managed login under `~/.smith` is used
only while Smith owns that CLI runtime. Forge neither reads nor imports that
credential store into the native host; native Main/Project typed tools require
an account-owned Forge provider entry and lease.

Protected session/checkpoint payloads use a versioned zstd envelope inside
authenticated encryption; earlier encrypted JSON rows remain readable and are
converted on their next state change. Snapshot digests use domain-separated
HMAC-SHA256 with the protected store key and exclude the save timestamp and
diagnostic manifests, so unchanged saves do no encryption or database write. Checkpoints
are idempotent by session, turn, revision, and operation fingerprint, and retain
the runtime's provider/tool recovery barriers. A checkpoint's exact snapshot
also serves as the session snapshot (NULL standalone snapshot columns reference
it); only independent canonical state changes need a separate snapshot. Writes
use the protected row's revision CAS. A competing snapshot save supersedes the
losing save; a checkpoint save reloads and retries once if it can still advance
the winning turn. Newer turns/revisions, conflicting fingerprints, and any
remaining CAS conflict supersede the losing save. The store handles these races
for every runtime caller without failing the turn. Snapshot/checkpoint LCM policy markers are tracked
separately so a standalone save cannot relabel an older checkpoint. A checkpoint
decode failure returns an error naming the runtime session; it cannot safely
fall back to fresh state because that would discard provider/tool recovery
barriers.

Runtime manifests are an ordered recent window (default 32 planned steps),
not a lifetime session archive. New protected checkpoints omit this diagnostic
window, while ordinary snapshots preserve any supplied manifests and the
runtime-owned `runtime.manifest_boundary` extension. The boundary remains part
of the snapshot digest. Loading the checkpoint-backed snapshot or saving the
live diagnostic window therefore preserves the same canonical state identity.
Forge links context provenance from the latest manifest in the live session
after the turn; it does not reconstruct historical replay from a checkpoint.

Native turn failures project runtime failure classes into Forge's existing
typed categories: policy denial, context overflow, authentication, quota,
provider-stage request rejection, and typed turn limits retain their evidence.
Retry delays and quota reset timestamps remain optional, including explicit
zero. Transient/rate-limit classification alone never authorizes a retry, and
local Config failures carrying only transience or request-rejection evidence
retain their unclassified projection. State conflicts,
host-component failures, non-provider request rejection, cancellation, internal
failures, and unknown classes retain the coarse kind/retryability projection;
nonretryable conflicts are not promoted into provider cooldowns. Credential
recovery remains runtime-owned and is not a Forge retry or authority grant.

Forge does not configure Agent Runtime's tool-step or turn-time limits; their
defaults are `None`. Task workflow `max_turns` is also optional and has no
default. CLI Task execution, including Smith, receives no max-turn value unless
the governing Task/workflow explicitly sets one. A per-execution wall-clock
deadline is likewise opt-in through `hard_deadline_seconds`, as described in
the liveness contract above.

Agent Runtime terminal limits retain their typed cause across the host boundary:
`provider_attempts`, `tool_steps`, `time`, or `output`. These are distinct from
the Task workflow's optional `max_turns` policy and optional execution hard
deadline. Exhausted provider attempts on a native Task are classified as
`ExecutorUnavailable`, use the ordinary provider cooldown when no retry hint
survives the runtime boundary, and defer every execution role (including
planner/reviewer) within the Task's execution retry budget. Model
context/output bounds and finite failed-provider retry policy remain intrinsic
runtime safety constraints, not productive-work turn or wall-clock budgets.
If an explicit runtime tool/time/output policy produces a limit, the failure
preserves its exact typed cause in the execution error.

Lossless Context Memory continuity is keyed by `(identity_id, scope_type,
scope_id)`, never by a replaceable runtime session. Main/Project Agent Chats
use their own canonical timelines. Native Task workers and planners keep their
protected runtime sessions for follow-up continuity but do not use LCM; the
runtime's deterministic structural compactor bounds their older optional
history and tool results in-place. Reviewer attempts likewise use structural
compaction while continuing to start without prior Task history or protected
session reuse. SQLite implements the Agent
Runtime LCM reader/writer contracts with host-minted view authority on every
operation, immutable admitted entries, transactional DAG compare-and-swap,
operation fingerprints, and restart recovery. Histories from different
canonical scopes cannot be opened or merged by possessing a timeline/node ID.

A timeline records the runtime session that writes it. A restart suspends
every native session, and the next turn opens a fresh runtime session (as does
a session rotation) whose canonical history is rebuilt from the chat
transcript. That history cannot continue another session's timeline: once the
old timeline holds summary nodes the runtime cannot truncate the diverged
tail, and every turn failed with "LCM source range overlaps an active node".
So when a different runtime session binds a timeline that already has
entries, the store retires it — the row keeps its entries and nodes, its
`scope_id` gains a `#retired:<timeline id>` suffix, and `canonical_scope_id`
keeps the original scope so Project deletion still removes it — and creates a
fresh timeline for the new session.

Pressure sizing is host-supplied. `ForgeLcmSizer` charges an entry for its
serialized canonical form, because the runtime's default sizer scans an
entry's plain text plus one token per tool part and a tool call's arguments
and a tool result's body sit inside their content part, invisible to that
scan. A Project Agent timeline is mostly `[assistant tool call, tool result]`
pairs, so the default read ~41% under
what the context planner charges: pressure stayed Soft, which never compacts,
while the planner refused the turn with `budget_exceeded` — and because
canonical history is durable, every retry replayed it.

The runtime folds Forge's sizer, pressure policy, and summary policy into one
LCM component revision and refuses to decode component state written under a
different one, so changing any of them would fail every turn of every live
session. `protected_agent_session_state.lcm_policy_revision` records the
`FORGE_LCM_POLICY_REVISION` that last wrote a session, and the protected store
drops superseded LCM component state on load — from the session snapshot and
from the checkpoint's copy, which the resume overlay would otherwise reinstate
— leaving the coordinator to rebuild it from `agent_lcm_entry` /
`agent_lcm_node`. Bump that constant with any change to those three policies.

The runtime's process-local history/LCM accounting cache still reads authorized
inclusive ranges in pages of at most 1,024 entries and checks the DAG revision
before and after pressure accounting. Forge's store and sizer satisfy those
contracts without changing `FORGE_LCM_POLICY_REVISION` or dropping session LCM
state.

Leaf compaction only cuts at user boundaries, and an agentic turn is not
bounded by one reply: a Project Agent tool loop can put tens of thousands of
tokens into a single turn with no user message inside it. The runtime takes
such an oldest turn whole as one oversized leaf, rather than returning no plan
and failing every admission with "LCM context cannot fit". A failed turn's
input and tool rounds stay in the persistent session history, so when a retry
would re-send a message that already sits unanswered at the end of that
history, the native host sends a short continuation instead of a second copy.

Forge selects and authorizes domain context; Agent Runtime alone budgets and
serializes final model context. `context_manifest` records the offered source
IDs/revisions and selection reasons, links the runtime run-manifest fingerprint,
and records included/summarized/omitted dispositions without duplicating token
planning. Protected bodies never enter either manifest. Authorized manifest
inspection compares pointer-backed Project references with the current
canonical Charter, Document, milestone, Project, and binding
revisions and reports stale references as a read-time overlay; it never rewrites
the immutable manifest or LCM history.

### Per-run build budget and CPU priority

Every CLI agent process (and its children), native tool command, review CI
step, Project hook and environment check/setup command uses the executing
machine's build budget. Unset `build_jobs_per_run` computes
`max(1, logical_cores / run_cap)`: use the configured positive machine cap, or
the automatic cap `max(2, logical_cores / 2)` when the cap is unset or `0`.
`build_jobs_per_run: 0` disables Forge's defaults; a positive value is exact.
Forge supplies `CARGO_BUILD_JOBS=k`, `RUST_TEST_THREADS=k`, `MAKEFLAGS=-j<k>`,
`CMAKE_BUILD_PARALLEL_LEVEL=k` and `GOFLAGS=-p=<k>`. Each Project's
`environment.env` takes precedence, then the Forge/daemon process environment,
then these defaults. These are cooperative tool limits, not a hard CPU quota.

On Unix, run children start with a niceness increment of `run_nice` (default
`10`, range `0`–`19`; `0` disables it, resulting niceness capped at `19`). Their
children inherit that priority; Forge's own priority stays unchanged. Failure
to lower priority logs once and does not fail work. On Windows niceness is a
no-op. Updates affect newly spawned processes; existing children keep their
launch environment and priority.

Server controls are `server.build_jobs_per_run` and `server.run_nice` in YAML,
`FORGE_SERVER_BUILD_JOBS_PER_RUN` and `FORGE_SERVER_RUN_NICE` in the operator
environment, and `forge --build-jobs-per-run N --run-nice N`. Precedence is file,
then environment, then flag. Forge Settings changes these values live, with
cores, effective cap and budget shown beside the machine cap; launch overrides
apply again after a restart. Operations includes the available server facts.

Daemons use top-level `build_jobs_per_run` and `run_nice` in `daemon.yaml` beside
their credentials. `forge-daemon`, `forge-ctl daemon link` and
`forge-ctl daemon start` accept `--build-jobs-per-run N` and `--run-nice N` to
override the file. This is daemon-local policy, with no transport override and
no daemon protocol change. Remote policy facts are not reported.

The runtime owns one shared machine process-policy handle. Settings updates
and Operations read that handle; the server entrypoint installs it for the
common process launch helpers. Commands take a policy snapshot at launch.
The Unix pre-exec callback uses only OS syscalls; a parent-side receiver logs
priority failure once so logging cannot lock in the forked child.


### One capability, one declaration

A native agent operation is declared once, as an `OperationContract` row in
`crates/agent-host/src/operation_catalog.rs`. Two decisions that used to be
re-listed by hand are now read off that row: whether the operation may execute
as a direct command (`is_coordination_direct_command`, replacing an allowlist
in the policy layer) and whether a Project target is derived for it before
dispatch. Both previously failed silently when a new operation was missed —
`policy_denied: the operation is not admitted for the current Forge scope`, or
`direct command has no canonical target derivation` — messages that name
neither the operation nor the list; `task.action` and `task.dependency` each
shipped with that bug. What the row still cannot supply is the payload schema
and the dispatch body, so those remain per-operation code, guarded by
`scope_composition_drives_every_migrated_main_project_and_task_operation`,
which asserts the set of operations actually driven end to end equals the
catalog.

Rules that more than one surface enforces live in `services`, not in each
surface. A Project's settings document is the worked example: REST and MCP
each had a copy and they drifted, so a document one accepted was one the other
would refuse on the same Project. `services::project_settings` is now the
single implementation, and each surface maps its error into its own shape.

### HTTP shell and web assets

The API router also serves the built React application with an SPA fallback.
Hashed JavaScript and CSS assets receive immutable one-year cache headers and
eligible responses are Brotli/gzip compressed; HTML navigation responses remain
uncached so deployments pick up the current asset graph. The production client
keeps route screens and editor-backed dialogs behind dynamic import boundaries.

HTTP request and response logs carry `client_addr` from the peer socket via
Axum `ConnectInfo<SocketAddr>`. When supplied, `X-Forwarded-For` is recorded
separately as `forwarded_for`; it is untrusted diagnostic metadata and does not
change authentication or the peer address.

### Durable events and the in-process event bus

Agent-critical mutations commit a monotonic `domain_event` row in the same
SQLite transaction as their authoritative state. Events carry canonical scope,
actor, correlation/causation, bounded reaction depth, and dedupe identity.
Durable consumers checkpoint only after their projection is safe to commit, so
lag and restart replay cannot duplicate chat turn jobs, Attention rows, actions,
memory indexing, notification rows, Project hook DB actions, or commitment reconciliation. Seven durable consumers use the
single-process worker runtime described below. SSE uses a read-only in-memory
tail and connection-time ledger replay.

`domain_event` rows are not pruned. The table also serves as an idempotency
ledger (including lookups by `dedupe_key`) and as history for lease, execution,
milestone, and project logic. Age and consumer progress alone cannot establish
that a row is safe to delete. Event retention is planned with the outbox redesign,
which will first separate the idempotency ledger and domain history from delivery
bookkeeping.

#### Worker runtime contract

`services::worker_runtime::WorkerRuntime` combines a source-independent
`WorkerSupervisor` with `db::WorkerHealth`, poison policy and an ordered
`DurableEventSource`. Health, retry and dead-letter persistence live in `db`;
services contains scheduling and worker hooks. Dead letters use
`(worker_name, source_key)` text identity and item type, so a future leased-step
source can supply its own `FailureState` to `dead_letter_in_tx` and use
`RetryPolicy::decision`. Leases, concurrency and per-task ordering are outside
this slice.

An event worker declares a stable name, `Subscription::Exact`, literal `Prefix`
or `All`, per-worker retry policy and handle timeout. Subscription changes are
compared with persisted health on initialization and refreshed before handling.
The existing `event_consumer_cursor` is the sole checkpoint authority; health
has no cursor copy. SQL filtering, indexed lookups, mapping, validation and
checkpoint writes live in `db`. Exact subscriptions seek each type's `(event_type, sequence)` index;
prefix subscriptions use disjoint binary type ranges on that index. Live lag
counts matching ranges. The oldest pending wanted event is the lowest matching
sequence, whose creation time supplies its age.

`Worker<C = ()>` uses a defaulted generic commit-result type because the stable
Rust compiler does not support associated type defaults. `handle(event)` returns
`Result<Outcome<Prepared>, WorkerError>` outside a transaction. `Done(prepared)`
opens one short `BEGIN IMMEDIATE`, validates the checkpoint and next candidate,
calls `commit(transaction, event, prepared) -> Result<C, WorkerError>`, and commits
effect, cursor and item health atomically. `commit` may await only database work.
`after_commit(event, prepared, committed)` receives the actual committed result,
so the worker can condition its post-step on successful admission. Its failure
is logged and reported but cannot undo or strike the acknowledged event.

`Skip` means this event is not for the worker. It writes no effect or per-event
transaction; its advance is buffered with ignored events, folded into the next
handled checkpoint or flushed at most once per five seconds. A restart can
reclassify that small buffered prefix. Wanted events remain strictly ordered.
`Defer { after, reason }` suspends the whole stream without a strike or an
attempt cap; individual waits are clamped to one second through one hour before
persistence. Deferral reason and first deferral time are retained. A malformed
stored wait is logged once per runtime and treated as due; the next state change
rewrites it. All deadline arithmetic is checked.

`WorkerErrorKind` has three deliberate meanings:

| Kind | Use | Event handling |
| --- | --- | --- |
| `Failure` (strike) | Unexpected item failures returned by `WorkerError::new` or non-transient `WorkerError::database`; caught handle/commit panics and handle timeouts also take this path | Add a strike, retry under the worker's finite policy, and quarantine when the cap is reached |
| `Transient` | Retryable availability/concurrency failures | Never add a strike or quarantine; retry independently with waits from one second to the five-second idle maximum |
| `Terminal` | A deterministic semantic rejection, such as Attention's `DbError::Check` | A terminal commit first rolls back and repeats handle/commit once with a fresh snapshot; a repeated terminal rejection quarantines and advances without a strike. Handle-time terminal outcomes quarantine directly. |

`Terminal` was added because `commit` returns a commit result or an error, not
`Outcome::DeadLetter`. Attention can discover a `Check` only after its first write;
it must roll back that write and quarantine if fresh preparation still rejects it. Expressing this with a
one-strike policy would also quarantine unrelated panics and timeouts. An explicit
terminal error preserves immediate Check quarantine without changing scheduling,
supervision or health persistence.

Default policy is eight attempts, waits exponential from one second capped at
five minutes, and five-minute handle timeout. Policy waits have a one-second
floor, respect their representable cap, and an unrepresentable cap falls back
to five minutes. A capped or explicit `DeadLetter` moves the cursor and inserts
the quarantine record together. Runtime infrastructure failures never strike.
Memory classifies SQLite busy/locked/IO, pool failures and version conflicts
as transient; other database failures strike.

`tick` defaults to no-op and has an independent `tick_timeout()` (30 seconds).
A failure or timeout is recorded and logged. Its independent retry schedule doubles
from one second to five minutes and resets on success; cycles during this backoff
skip tick and proceed directly to events. It runs at most once per due cycle,
never once per backlogged event. Event-loop sleeps remain capped at five seconds.
Tick has its own health error scope. Identical runtime
and tick errors are deduplicated. Successful cycles clear runtime errors even
when empty; successful ticks clear tick errors. An item error survives unrelated
recovery until that item completes or is quarantined. Health's generated
`last_error` exposes the latest of the independently retained causes.

Operator status computes lag/age/stalls live from the cursor table and current
subscription, without scanning ignored event rows through `json_each`. Existing
`recent_errors` entries carry bounded worker causes and deferral reason/since;
readiness alone is informational, while errors and stalls raise attention.
Dead letters degrade health for the same one-hour window as ordinary errors.
Each worker also exposes its open total and five most recent open quarantines
with stable IDs, item keys/sequence, event type, consumer, attempts, reason and time,
without expiry. Resolved rows and their action audit are retained; there is no
retention/pruning job for either. A future retention job must delete
`worker_dead_letter_action` rows before their parent `worker_dead_letter` rows
to satisfy the foreign key.

`DeadLetterService` checks admin authority and maps the stable consumer name to
that runtime's existing worker instance. Only canonical positive bare sequence
keys are replayable. Coordination's `event:N:commitment:C` and `event:N:inbox:…`
quarantines, and Wake's `wake-retry:…` rows, are dismiss-only; replay rejects them
with `DeadLetterNotReplayable`/409 before any write or handler call. Manual replay
shares `WorkerRuntime`'s
bounded preparation, panic handling, transactional commit and after-commit path.
Preparation runs before the writer transaction; a compare-and-swap on the open
row's version fences the commit. Effects, health success, resolution principal/time
and an action audit entry commit together under `BEGIN IMMEDIATE`. Replay reads
the consumer cursor for display context, leaves it unchanged, and delivers the
original event only to its owning consumer.
Consumer-emitted domain events retain normal downstream semantics. Skip resolves
without effects. Commit errors roll back all effects, then a separate fenced
transaction records a failed attempt/error. A repeat quarantine upsert reopens
the stable row, clears resolution, stores the new error/attempt, and increments
its version. Replay checks the row after the consumer commit; a version beyond
the fence means commit re-quarantined it, so effects roll back and the action
records `replay_failed` with the new error. A consumer that isolates one item of
the replayed event inside its own commit (coordination quarantining a single
commitment or inbox item) is not a failed replay: the other items' effects stay,
as in normal delivery, and the item becomes a new open, dismiss-only row. Errors and deferrals never schedule
a retry. A terminal commit gets the same one fresh preparation as ordinary delivery.
Dismiss uses the same version fence and audit transaction without invoking the
worker. Competing actions using the same open version have exactly one winner;
the loser receives `DbError::VersionConflict` through the service/API boundaries.
No durable in-progress lease can strand a row after a crash. Post-commit hooks
retain normal worker semantics: their errors report health without reopening the
resolved event. The SSE tail has its own `event_relay` object rather than an event-consumer entry.

Replay deliberately breaks original event ordering: it applies event N after
newer acknowledged events, using current consumer state. An old `task.blocked`
coordination outcome can block a commitment that returned to `in_progress` and
send an old “Task delivery blocked” inbox item. Consumer idempotency does not
restore the original state timeline. The API/status summary exposes replayability,
source `event_created_at`, and `events_since` (later retained subscribed events
through the current checkpoint, including handler skips), so operators can assess
that context. Subscription changes mean this is not a historical receipt count.

The REST route spawns and awaits the service operation. Dropping the requesting
client's future detaches that task, preserving `after_commit` work such as Project
hook external Agent launch after a committed replay. A process crash retains
normal worker recovery behavior.

Each standard SQLite pool connection has an insert hook, commit marker and
rollback handling. Pool-scoped `Notify` is delivered on connection release
after COMMIT is visible. A pending-marker atomic gives ordinary releases an
immediate fast path, with no extra ping or lock-handle round trip. Hook mutexes
recover poisoning. `max_lifetime(None)` remains necessary because SQLx's expiry
close path bypasses the release hook; idle connection recycling is retained.
Explicitly held connections deliver their commit hint when released. Waiters
register before polling; notification hints never replace durable reads.

The supervisor counts loop exits/runtime panics, including failed initialization,
resets restart back-off after a healthy period, and aborts/awaits its existing
child on shutdown. False watch changes cannot create another loop. Missing health
rows are recreated. The consumer placements are:

| Worker | Subscription | `handle` / `commit` | `tick` | `after_commit` |
| --- | --- | --- | --- | --- |
| `scoped-memory-agent-chat-indexer` | Exact `agent_chat.message.admitted`, `agent_chat.response.completed`, `agent_chat.message.completed` | Prepare semantic memory / insert source-idempotent memory | None | None |
| `agent-coordination-outcomes` | Exact `task.transitioned`, `task.done`, `task.completed`, `task.blocked`, `task.failed`, `task.cancelled` | Read Task, scope-validated commitments and action origins / acknowledge proposal inbox, reconcile commitments and deliver outcomes | None | None |
| `attention_projection` | All (case-insensitive and substring classification) | Prepare incident, resolution and wake policy / write incident, resolutions, wake decision and budget | Resolve superseded turn incidents | Publish zero configured-budget notification with the resolved budget scope |
| `agent-wake-turns` | Literal prefix `agent.wake.` | Plan admission/disposition / persist disposition and optional message/turn admission | Reconsider due deferred or changed setup dispositions, isolating and bounding failures per row | None; decision resolution shares admission/cursor commit |
| `project-hooks` | Exact `task.transitioned`, `task.status_changed`, `project_hook.task_created`, `project_hook.task_archived` | Evaluate existing completion/epoch rules and prepare action content / claim run and write notification, comment or Task (including dispatch automation Task) with cursor | Settle stale started runs from execution evidence | Publish live hints and comment-memory indexing; launch external Agent execution only for the admitted started run |
| `notifications` | Exact `task.transitioned`, `task.status_changed`, `review.status_changed`, `notification.requested` | Prepare the existing human notification / insert notification row with cursor | None | Publish `notification.created` live hint |
| `conflict-hotspots` | Exact `task.transitioned` | Validate workflow handoff and read its full transition reason / seek Project Tasks then merge-failed log rows, count every path once, and commit episode/detection/cursor atomically | None | None |


#### Periodic and event-triggered process workers

`worker_runtime::PeriodicWorkers` supplies source-free `PeriodicWorker` tick
contexts backed by the same `WorkerSupervisor` and `db::WorkerHealth` as the
durable workers. The runtime and server-only startup share one registry exposed
by `OperatorStatusService`; no event subscription, cursor, poison strike or
quarantine is created. Operator status adds `periodic_workers` with the worker
identity, live `running`, process-local `last_tick_at` (tick start, sampled at
most once per 30 seconds and never written to SQLite), independently
retained error/time and persisted restart count. Stopped workers remain visible;
persisted health from an unstarted worker never implies a live process. Periodic
faults also enter `recent_errors` and raise Operations severity to `attention`.

| Worker | Original schedule and wake signals | Tick budget / policy |
| --- | --- | --- |
| `task-dispatcher` | Immediate scan, then 10 seconds after each pass; existing dispatch completion/probe Notify and dispatch_wake; atomic stop | 1 hour / warn, continue awaiting |
| `agent-chat-turns` | Existing immediate 250 ms Skip poll, gated by active-turn capacity; active JoinSet completion and shutdown watch | 5 minutes / warn, continue admission |
| `heartbeat-monitor` | Immediate check, then 10 seconds after each pass; existing stop and daemon reconciliation Notify | 5 minutes / warn, continue awaiting |
| `operator-status-emitter` | EventBus hints, existing 500 ms Skip coalescer (initial tick consumed); activity refresh after 5 seconds | 5 minutes / cancel |
| `lifecycle-projection` | Existing EventBus receiver; only events the handler acts on count as ticks; other hints bypass health, lag warned | 1 hour / cancel |
| `workspace-cleanup` | Independent immediate 60-second cleanup and 10-minute terminal sweep Skip intervals | 1 hour / cancel |
| `storage-maintenance` | Immediate 5-second Skip interval, same 100-page incremental vacuum; Task step retention on the first tick and hourly after | 5 minutes / cancel |
| `daemon-monitor` | Immediate check, then 30 seconds after each pass; stop Notify | 5 minutes / cancel |
| `embedded-daemon` | Immediate CLI scan/report, then 60 seconds after each pass; stop Notify; server and Solo | 5 minutes / cancel |
| `shared-media-cleanup` | One startup pass, then create 60-second Burst interval and consume initial tick | 5 minutes / cancel |
| `external-sync` | Immediate due-integration pass, then 60 seconds after each pass; stop Notify | 1 hour / warn, cancel safety unverified |
| `environment-settings-observer` | Existing Project update/resume EventBus hints, lag ignored; weak service ownership; stops with dispatcher | 5 minutes / cancel |

Simple immediate-pass/wait-after-pass workers use the shared tick driver.
Specialized interval/select state stays with the domain worker to preserve
missed-tick policy, startup timing and in-process wake behavior. Wait/receive
futures are outside the tick budget. Cancel-safe workers cancel an over-budget
future and record `periodic worker tick timed out after {budget}`. Dispatcher,
heartbeat, Agent Chat admission and external sync use a non-cancelling watchdog:
after the
budget they report `tick running longer than {budget}` once and continue polling
the same future, including while the health write waits for a database lock.
This preserves inline workflow barriers, unbounded CI and committed claim
batches. External sync also warns because import cancellation safety is unverified.
Completion clears the warning; an actual failure replaces it with the
failure cause. Recoverable failures retain the original cadence.

Successful ticks check SQLite health only once after child startup (to clear
stale persisted causes) and on a reported error-to-success transition. Repeated
identical tick faults are deduplicated in memory; steady successful ticks perform
no health reads or writes. `last_tick_at` is sampled in memory at most once per
30 seconds, with no SQLite heartbeat flush. Loop-specific warning messages are
retained with the `worker` field; Agent Chat logs an admission failure once.

A panic escapes the tick to the supervisor; unexpected return,
error or cancellation also restarts after 250 ms, doubling to a 5-second cap,
reset after 60 healthy seconds. Atomic stop is an intentional exit, not a restart.
Workers with a shutdown watch let the in-flight pass finish under the existing
runtime's 10-second shutdown deadline, then abort and join if needed. Workers
with atomic stop/Notify keep those stop APIs and their wait-after-pass behavior.
Aborting an owning supervisor also aborts its child, preventing detached loops.

Agent Chat keeps its existing 250 ms capacity-gated poller and active-turn
JoinSet. Only admission polling has a warning budget; individual turn execution
is outside it. The set is outside the poller's unwind boundary: a panic cancels
and drains the old turns before it is rethrown to the supervisor for restart.
Panic cleanup waits 15 seconds (the CLI executor's 10-second SIGTERM grace
before SIGKILL, plus 5), so a CLI turn's process group is killed before any
abort, then aborts and joins non-cooperative turns before restarting. Cooperative shutdown uses the same
cancel-and-drain path under the existing runtime shutdown deadline. Turn-lifetime drop guards cancel both the provider
signal and lease-renewal token on every exit, including panic and hard abort,
so an unfinished lease can expire rather than being renewed indefinitely.
Per-execution/per-probe/per-terminal tasks, request-bound provider-health CAS retries and usage/log scans, daemon WebSocket transport and
the Axum listener retain their current owners. The read-only domain-event relay
already uses `WorkerSupervisor` and retains its separate `event_relay` status.


Project hooks and notifications cut over in migration `V202610030200`.
Each new cursor is seeded at `MAX(domain_event.sequence)` in the migration
transaction; existing cursors and all user history survive. Only events committed
after this upgrade boundary can trigger either consumer. Health and live lag use
the existing operator consumer shape and process-owned worker registry.

Task creation/archive and human interruption signals previously had only bus
hints. The migration adds writer-transaction triggers for `project_hook.task_created`,
`project_hook.task_archived`, and `notification.requested`. A request freezes the original
Project/Task, title, event kind and full body for `task.blocked`, `task.failed`,
manual `task.recovery_required` (crash/heartbeat, excluding shutdown), and
`merge.failed` (detected merge conflict/dirty target). Review completion uses
runner-origin `review.status_changed` (including workflow CI results); human
approval/rejection, manual passes and mechanical authority carry do not notify.
Task delivery triggers exclude owner Hold, review-CI infrastructure/reset
annotations and queued-action restoration, including restored hard failures.
Automatic and manual board transitions share
`task.transitioned`. Direct status mutations use `task.status_changed`.
Attention records a zero-budget Project autonomy stall's `notification.requested`
in the same wake-decision transaction, including the open-incident count.
Raw execution attempt events remain silent. Live UI bus publications remain,
but neither consumer subscribes to them.

Notification creation and Project hook notification/comment/Task actions are
atomic with their checkpoints. Existing Project/rule/epoch uniqueness, cooldown,
concurrency, action content and dispatch prompt apply. Hook dispatch commits the
automation Task and a `project_hook_run.status = running` started marker with the
cursor before invoking the existing launch path in `after_commit`. The external
launch is **at most once** per admitted run: a restart or duplicate evaluation
never launches that run again, even if it crashed after the launch or just before
it. Exactly-once external execution is impossible without cooperation from the
external runner. A crash between commit and launch may therefore leave a running
run with an automation Task but no execution; it is deliberately not retried.
The worker tick inspects at most 100 `running` runs older than ten minutes,
using an active-run index. It marks a run `failed` with a visible expiration
reason when no execution exists, or `dispatched` with the execution ID when
launch succeeded but its status write did not. Both settlements complete the
run and release its concurrency slot, publish the existing run-history hint,
and never launch again. Launch itself has a five-minute timeout; a timeout
retains the started marker for this reconciliation and reports worker health.
Per-rule trigger/preparation errors record a failed run and allow later rules
to continue. An unread trigger gets an event-specific diagnostic dedupe key,
so it cannot consume a future successfully matched work epoch.
Live SSE hints and derived comment-memory indexing are best effort after commit;
the authoritative rows remain queryable after a crash before publication.

The three migrated consumers use the standard eight-strike policy for unexpected
hook faults and a five-minute handle timeout. Preparation performs local database
and policy work, never a provider/model call. Busy/locked/IO database failures and resolvable version conflicts are transient.
Deterministic domain rejections are terminal; unexpected failures take strikes.
Cancelled commitments need no outcome and are skipped.
Independent commitment and inbox effects use savepoints. A rejected item is left
unchanged and recorded with its identity/reason, while siblings and inbox delivery
continue. Non-item failures roll back the event; terminal commit failures refresh
preparation once before quarantine. Attention resolvers are conditional/idempotent
updates without per-row semantic refusals; a wake's message/turn/disposition and
incident resolution are one dependent atomic effect. A rejected transition
(such as blocked to completed) does not change the commitment state machine. Attention's deterministic
`DbError::Check` rejection quarantines immediately, including a check discovered
after the first effect write. Memory retains its existing poison policy.

Attention's terminal-execution settlement wait is whole-stream `Defer`, preserving
its previous ordering barrier. A deferred wake is `Done` with a durable `Deferred`
disposition and an advanced cursor: one unavailable Agent cannot block another
Agent's wake. Admission fallback uses a savepoint so rejected admission writes
cannot leak into the fallback disposition. Semantic wake retry attempts keep their
immutable lineage and finite budget separately from runtime retries.

Migration `V202610020700` drops `event_processing_lease` and
`event_projection_receipt` and the unused `attention_consumer_health` table.
These contained only delivery metadata and operational counters, timestamps,
bounded error diagnostics and lease details, not user data. Runtime health
replaces the old attention-health storage and accessors.
All durable consumer cursors and all domain/projection records survive unchanged;
only the retired `sse-broadcast` cursor is removed. Live legacy claims do not block
restart: each worker resumes strictly after its retained checkpoint.

The SSE relay is an ordered tail outside `WorkerRuntime`, owned by `WorkerSupervisor`.
Task death or unexpected exit restarts with backoff and bounded shutdown; its shared
in-memory position survives child restart, preventing gaps or duplicate broadcast.
Read errors and restart state are visible in operator status.
Its optional in-memory position is initialized from the ledger head in its loop;
a failed read leaves it uninitialized and retries without replaying history; serial drains read ascending event sequences,
publish their committed envelopes, and advance only memory. It uses the same
committed-event `Notify`, registered before polling, and the same 250 ms to
five-second idle fallback. It writes no cursor, lease or receipt; supervisor/error health writes occur only on
faults and recovery, with no steady-state idle writes.

SSE plain connects remain live-only. A connection reads the ledger only when
`Last-Event-ID` has the durable form `domain-event:<sequence>`; entity IDs, missing
headers and malformed IDs do not request replay. Bus-only and ordinary resync frames omit
the SSE `id` field, preserving the client's last durable cursor across reconnects.
A beyond-head resync alone sets `domain-event:<head>` to reset an invalid cursor.
Keep-alive comments also have no ID. Only durable frames set an SSE ID.
The payload's `entity_id` remains unchanged. Neither the web client nor forge-ctl
uses frame IDs for routing; MCP uses a separate stream. The web client recreates
`EventSource` on reconnect and refreshes active queries after a stable open.

For durable resume, subscribe before capturing the ledger head, then check the
bounded sequence range for at most 1,001 keys. At most 1,000 missed rows replay in
ascending sequence order through that snapshot, using 100-row pages. A larger
backlog produces one `events.resync_required` frame and no replay. Snapshot-covered
durable live frames are filtered, so delayed relay frames cannot duplicate replay;
appends beyond the captured head remain live. Bus-only events remain live-only,
and bus overflow still requests resync. The relay is the only publisher of durable frames. Composite and standalone
commits are delivered in sequence order, so a resume cursor cannot jump past
undelivered events.

The shared `RuntimeSupervisor` owns `StorageMaintenanceWorker`. Every five
seconds it runs only a bounded `PRAGMA incremental_vacuum(100)`, consuming every
step of the statement. At 4 KiB per page this can drain about 580 MB of free pages
in two hours, while limiting each write-lock acquisition to 100 pages. It shares
the runtime shutdown channel and skips missed ticks. It does nothing on a
database outside incremental mode.

Operations and Agent lifetime reads share one process-local `UsageLedgerIndex`
per database in both the server and Solo runtime. Its bounded attempt/run
summaries are observations; fresh ledger projections remain the accounting
reference. Cold builds and warm deltas publish atomically, so request
cancellation cannot leave partial accounting totals. Estimated source citations
retain counts and winning order per distinct reference, rather than events.
The index charges up to 128 MiB and uses a separately bounded 32 MiB fallback
memo outside its mutex when a ledger does not fit. Trigger-maintained row counts
make shrink probes constant time. See
[performance testing](performance-testing.md#incremental-usage-reads) for the
fold, invalidation proof, bounds, differential tests and benchmark runner.

The ten `usage_read_*` triggers maintain rowid maxima, invocation/execution
change revisions, row counts and a deletion generation in one singleton row.
`usage_changed_invocation` and `usage_changed_execution` coalesce updates into
one latest marker per identity. Applied markers are deliberately not pruned:
a second server process may still need an older revision. Storage is bounded
by one marker per invocation or execution identity ever updated, with markers
for deleted invocations/executions removed by cascade/delete triggers (renamed
execution identities can leave a marker for their old identity). A process
advances its own watermarks in the same read snapshot as its delta.

New SQLite files enable `auto_vacuum=INCREMENTAL` before the WAL switch and their
first table. WAL pool connections use `synchronous=NORMAL`. Existing files retain
their mode until an operator runs the explicit offline full-VACUUM conversion.
Server and Solo data-root process locking excludes that conversion from a
running runtime. Operator status reports free pages, incremental mode, consumer
lag and oldest pending age. Its expected consumers are derived from the
workers the supervisor starts; persisted cursors for workers outside that set
are omitted. All four durable workers use the cursor table and `worker_health` subscriptions
with live event queries. The SSE tail is omitted from `event_consumers` because it
has no durable backlog or checkpoint. All four workers and the SSE tail start
unconditionally in both
Server and Solo, including when MCP or the embedded daemon is disabled. A cursor
is stalled only when unprocessed events exist and it has not advanced for longer
than the configured threshold; an idle consumer with zero lag is never stalled.
See [getting-started.md](getting-started.md#database-space-and-consumer-health)
for the stall threshold, durability tradeoffs, and offline conversion requirements.

The `agent-coordination-outcomes` consumer is started by `forge-cli`; it turns
terminal Task transition events into one task-outcome inbox item and, for a
scope-validated originating commitment, one delivery evidence/lifecycle
projection.  Its event-derived dedupe keys make a crash between projection
and cursor checkpoint safe to replay.

The `agent-wake-turns` consumer (also started by `forge-cli`) closes the wake
loop. Migration `V088` records an install-time cutover cursor, so events that
commit after installation are evaluated even when the process has not polled
yet; startup never derives a cursor from the runtime event maximum. Each
`agent.wake.*` candidate receives one durable current disposition:
`turn_admitted`, `deterministically_suppressed`, `deferred`, or
`setup_required`. The disposition, cursor advancement and optional Agent Chat message/turn
admission commit in one transaction. Deferred and setup-required incidents retain bounded retry or
authoritative-state reconsideration lineage instead of disappearing as
completed delivery. Checkpointed candidates are ordered by ascending subscribed
`domain_event.sequence`; the cursor cannot skip an undisposed wake sequence. A disposition replay is
idempotent and advances the cursor at most once, only after its disposition
and any admitted turn/message commit.

Immediately before an admitted wake commits, the consumer revalidates the
durable Attention version, status, digest, source, dedupe identity, and
canonical scope. It then uses the same `AgentTurnAdmissionService` as user
messages and handoffs. That service resolves the current owning binding,
identity, selected Profile, operating-skill revision, permission/tool policy,
and canonical scope, and freezes their exact versions/digests on the queued
turn. Explicit retries and the turn runner continue from that persisted
snapshot rather than substituting later binding or Profile state. A worker
retry of a `retry_wait` job is not a new admission: it reclaims that same turn
job and invokes the runner with the same frozen provenance, changing only
lease/attempt metadata.

Execution terminal outcomes are written by the winning execution terminal CAS:
the terminal row, active `WorkspaceLease` disposition, and one durable
`execution.completed`, `execution.failed`, or `execution.cancelled` event are
committed together. These terminal events remain per-attempt audit records and
inputs for resolving `progress_warning`; they do not directly create
action-required Attention or wake an Agent. The Task dispatcher applies the
attempt result to the Task's effective disposition and commits one atomic
`task.interruption_changed` event. That post-disposition event creates or
resolves action-required Attention and is the only ordinary source of a
Project-Agent recovery wake. Human notifications remain driven by the
corresponding committed Task outcome, never by the raw attempt event.
Automatic/deferred retries and expected cancellation or reassignment are
therefore silent. If a terminal
execution remains manually resumable but no Task disposition is committed by
the bounded settlement grace, the orphan safety net promotes it to an
actionable Attention item; the retained `execution_failed` category remains
available for that diagnostic path and historical projections.
User Pause/Stop retains its manual-stop annotation and recovery controls, but
records `requires_intervention: false`: an Agent must not undo an intentional
stop. A running, newer, or explicitly linked successor excludes the stopped
attempt from orphan recovery, even if that successor has already finished.
Wake admission rechecks the attempt, Task disposition, deferred recovery, and
terminal workflow state under the same transaction as wake budgeting, so
recovery, cancellation, or deletion after projection suppresses the stale wake.
Observation belongs to the run that made it. A Task session holds a worktree
and a process and captures what its own run did (`task.evidence`,
`task.worklog`; an authorized planning or implementation session also keeps an
execution-private plan candidate with `task.plan`). A CLI harness writes the
same records and plan candidate to its execution outbox, which Forge ingests
when the run ends — see
[api.md](api.md#commitments-inbox-and-typed-actions)). The Project Agent holds its own verification workspace — a
durable `forge/` plus a disposable `checkout/` of the repository — and every
Project Agent Chat turn composes against it (`WorkspaceAccess::ProjectVerify`).
The workspace root also carries two Forge-owned authority markers:
`.forge-project-agent-generation` identifies the immutable Project generation
(the durable provisioning-operation ID, with the original creation timestamp
as the legacy fallback), while `.forge-project-agent-repository` contains only
a SHA-256 digest of the selected repository snapshot. The repository marker
never stores a remote URL or local path. Project-version/settings changes keep
the generation and durable `forge/` notes; a repository change replaces only
the disposable checkout. A markerless legacy workspace is adopted so its
notes survive migration, while its unproven checkout is rebuilt. If a Project
ID is reused, generation markers quarantine the old workspace before the new
Project can write there.
An acceptance check that asserts the whole delivered outcome is settled by the
Project Agent itself: it exercises the software in `checkout/` (its command
tool runs there, never at the workspace root, so a build tool's upward
manifest search cannot escape into the host's own repository; every command
is recorded as a `project_command_observation` whose id the Agent must cite
in `observed_command_ids` for a `task_validation` pass or fail, newer than
the delivered Task, or the record is refused), records the
result with `project.validation` (naming `observed_task_id` only when a Task
run made the observation), and captures its proof with `project.evidence`
(`capture`), which stores the artifact as a project-scoped media asset and
attaches it in one call. Verification is never delegated to a Task: an
implementation Task's completion contract demands a commit on the Task branch,
so a read-only verification Task fails by construction — the operating skill
directs the Agent to cancel such a Task and settle its checks itself, and to
respond to a failed check by dispatching the implementation Task that fixes
the defect.

The Main Agent holds a workspace of a different kind: an account scratch
directory (`WorkspaceAccess::AccountScratch`). It is not a checkout — no
clone, no worktree, no remote — so nothing in it can reach a repository
because no repository is in it. It exists so Main, and the ephemeral inquiry
sub-agents it dispatches, keep findings and notes on disk instead of in a
conversation that has to survive all day.

An **inquiry** (`inquiry.run`) is deliberately not a Task: no worktree, no
workflow state, no review, no dispatch queue. One nested native turn runs with
the account read surface and a per-inquiry directory under that scratch root,
the calling turn blocks on it, and only a bounded abstract plus the path to a
findings file re-enter the caller's context — which is the whole reason the
operation exists. Three properties are enforced structurally rather than by
instruction:

- **Depth is capped at one.** An inquiry runs under the `Account` scope, and
  `inquiry.run` is composed only into an `AgentChat` scope, so a sub-agent
  never sees the operation that would start another.
- **An inquiry cannot act.** Its composition carries no orchestration proposal
  surface at all. A permission-only boundary would not do: `genesis.start` is
  gated on `propose_discovery`, the same permission a sub-agent needs for
  public research.
- **It is Main-only.** `main_account_id` rejects a Project Chat, and migration
  `V131` pins `account_scratch` to an `account` row or an `agent_chat` row
  with no Project.

Inquiries are serialized per account: the Account scope's id must be the
owner's id, so runs share durable authority and session metadata. Each inquiry
nevertheless starts with an empty, ephemeral runtime conversation: it neither
restores nor saves session snapshots, protected checkpoints, or an LCM timeline.
Previous inquiry messages and token usage cannot leak into the next run. Main
Chat remains persistent; only the Account-scope scratch inquiry is ephemeral. The
`agent_inquiry` row is a visible run log, not a work item; its only user verb
is cancel, and its four token counters stay disjoint so a cached prefix is
never double-counted.

A sub-agent writes the same Forge JSONL activity log an Agent Chat turn writes,
keyed by inquiry id under the shared `AgentChatTurnLogRoot`, so one log reader
and one renderer serve both and its work is watchable while it runs. Each run
is registered with a cancellation token and tied to the calling tool future's
lifetime by a drop guard, which makes cancellation real:
`POST /api/v1/inquiries/{id}/cancel` stops
the provider call rather than only marking the record, and a cancelled chat
turn stops the research it was blocked on. If a run completes in the same
instant it is cancelled, the durable `status = 'running'` guard on the
completing write means the user's cancellation wins and the caller is told the
inquiry was cancelled instead of receiving a tool error. Dropping the caller's
future also cancels its inquiry. The runner owns terminal persistence; timeout
first cancels and drains the backend within a bounded shutdown window. Native
session admission rejects another turn while shutdown cleanup remains active,
and completion-storage errors are surfaced rather than reported as invented
terminal outcomes. Existing inquiry sessions with obsolete persistence
capabilities rotate through the normal versioned path without deleting history.

Committed inquiry events invalidate the mounted inquiry list, including discovery
from an empty or all-terminal cache. A 15-second foreground poll remains a fallback.
An expanded log view refetches the final tail when its run becomes terminal;
if collapsed, it performs that final fetch when reopened.

A decision the user records on something the Project Agent proposed — a
milestone definition revision approved or rejected
(`milestone.definition.transitioned`), a Decision approved
(`project.decision.approved`) or a candidate rejected
(`project.decision.candidate_rejected`), a Document revision approved
(`project.document.approved`) — creates a `decision_recorded` Attention
incident for that Project Agent and wakes it. The classifier requires
`actor_type = user`, so an Agent approving its own proposal never wakes
itself. The wake message carries the decision (`details.decision`) and a
directive to continue from it rather than ask the user to confirm again; the
wake consumer resolves the incident right after the turn is admitted, because
the incident exists only to deliver the decision. The web Project Chat
surfaces the same decisions with one-click actions (the "Needs your decision"
card), so approving in the chat is the whole loop: record, wake, continue.

Every successful terminal Task transition (`done` in the default workflow)
also creates a `delivery_followup`
Attention incident for that Project Agent. The follow-up asks it to reconcile
the Task outcome into authoritative validation, evidence, and milestone
readiness; the Task transition itself supplies none of those facts and never
authorizes a release. The wake message is a work order resolved from server
state, not a generic prompt: Forge takes the milestones the completed Task is
governed by (falling back to the Project's open milestones when the Task is
bound to none) and names each one's `milestone_id`, version, current definition
revision, whether every Task bound to it is now done, and every required
acceptance check still missing an authoritative result — separated into the
checks the Agent settles through `project.validation` and the `manual` ones only
the user can attest. Open-Task counts mirror the readiness rule, so the wake
never reports a milestone as finished while readiness still sees governed work
open. The admitted follow-up cannot succeed from narration: it must commit the
record its postcondition names or retry/fail under the existing finite turn
budget. A committed readiness evaluation resolves the pending delivery
follow-ups even when the evaluation remains blocked.
When upgrading from an older Attention consumer, migration `V093` appends one
deduplicated follow-up event for each Project whose latest completed Task has no
later readiness evaluation, so already-checkpointed delivery is reconciled too.
Migration `V094` similarly replays one event per Project whose follow-up
Attention remained open after a pre-guard prose-only turn, allowing the normal
durable wake path to recover those existing incidents after restart.

The `events` crate still wraps `tokio::sync::broadcast`, and the SSE endpoint at
`/api/v1/events` still drives live clients. For durable events it is a
post-commit delivery/cache-invalidation projection, not authoritative history
and never sufficient by itself to wake an agent.

### Scoped semantic memory and context provenance

The append-only memory layer continues to index execution summaries, reviews,
comments, failure-bearing transitions, and finalized Agent Chat messages. Every row
now carries canonical scope, visibility, owner identity, authority, provenance,
publication/supersession links, validity, and source event. Publication creates
a new wider-visible record; retraction, dispute, expiry, and supersession are
append-only lifecycle assertions rather than body edits.

Search and get apply authorization inside the SQL candidate query before FTS
matching, snippets, counts, cursor construction, or ranking. Inaccessible rows
therefore cannot be inferred from response differences. MCP responses retain
the context-not-instructions guardrail; repository/memory text cannot grant
tools, permissions, approvals, or a broader scope.

### Commitments, Attention, and Mission Control

Inbox items, commitments, and typed action/proposal envelopes are durable
coordination records. Commitment completion requires authorized evidence;
profile or session replacement does not erase an obligation. Mutating agent
actions carry scope, payload hash, dedupe, correlation/causation, requested
permission, and an `allowed`, `approval_required`, or `denied` policy result.
Protected actions cannot be self-approved. Task proposals enter the existing
Task service/workflow and do not become authoritative work before persistence.

The provider-facing generic coordination tool accepts the canonical nested
`payload` and optional flat aliases for its declared operation fields, whether
they arrive at the root or inside the tolerated `parameters` wrapper.
Preparation merges those shapes, rejects conflicting duplicates, drops null
aliases, and sends only the canonical envelope to policy and service
validation. Required fields remain enforced there so a malformed model call
returns a correctable tool error instead of terminating at provider schema
validation.

Attention is a deterministic, rebuildable projection of human input,
validation/review state, stalls, health, budget thresholds, and overdue
commitments. Any model wake occurs only after deterministic admission with
budget, cooldown, batching, dedupe, incident lease, self-event suppression,
and reaction-depth limits.

Attention consumer health uses live subscribed backlog and checkpoint progress.
A caught-up or newly initialized consumer is not stale; both the oldest pending
event and the progress/initialization baseline must exceed 90 seconds to be stale.
No ledger-prefix count is performed per request; the processed-event field was
removed. Error kind (`failure`, `transient`, `terminal`) and bounded message come
from worker health or recent quarantine records. The same health result supplies
Mission Control's capacity status.

Wake semantic retry errors are isolated by disposition ID. A failing row cannot
abort later rows. Transient failures back off without strikes; waiting rows are
filtered before the SQL limit and waiting is not a tick error. Unexpected failures
have an independent eight-strike cap in `worker_item_failure`; a terminal storage
rejection or cap exhaustion commits a terminal `wake_retry_failed` disposition
and a dead letter together. Evaluation/admission rejections that have a typed wake
outcome retain `wake_evaluation_invalid` / `turn_admission_rejected`; commit-time
missing authority defers as before. Initial and retry
admission, disposition and decision-incident resolution commit together. Consumer
`after_commit` hooks hold no durable effect; Attention only emits its budget-stall
bus notification for the zero configured-budget branch, using the budget scope.

Mission Control and Agent detail are bounded read models over authoritative
Task/identity/session/commitment/event state. They show needs-attention,
review-ready and active work, embedded-agent health/current scope/focus,
commitments, recent outcomes, and capacity; they do not introduce a second
mutable Task or Agent truth.

### Workspace placement

A repository's logical identity is separate from its machine-local checkouts.
Each repository location records an owner, path, kind (`primary_checkout`,
`managed_clone`, or `shared_mount`), default flag, verification status, and
version. Only `ready` locations are eligible. A daemon verifies its own checkout:
it must be within the runtime's `workspace_root`, be a Git worktree, resolve the
repository's default branch, and have a matching remote when one is present.
Both owners compare normalized repository identities: absolute paths and
`file://` URLs match, as do SSH (including scp-like syntax) and HTTP URLs for
the same host and repository path. Host case, default ports, trailing slashes,
and a terminal `.git` suffix do not affect the comparison; different paths and
non-default ports still differ.
The server never opens a daemon-local path. See the
[repository location commands](cli.md#repository-locations).

Each Task workspace has one persisted placement binding its Agent, owner,
repository location, execution provider, opaque workspace handle, generation,
state, and selection reason. Every workspace consumer uses this placement:
execution start/cancel, terminals, setup, hooks, review and CI, diffs, artifact
reads, merge, reset, recovery, and cleanup. The executor snapshot records
`placement_id`; no consumer resolves a daemon again from the Agent after
admission.

| Owner | Workspace lifecycle | Execution provider |
| --- | --- | --- |
| `server` | Embedded backend on the Forge process host | Embedded provider, or a daemon through a verified `shared_mount` location |
| `daemon` | Daemon backend on one named daemon runtime | That placement's daemon |

A `shared_mount` location requires a server-written probe that the daemon reads
back at the same path. Matching path strings alone do not qualify. Separate
machines use daemon-owned locations and workspaces, with no filesystem sync.
An Agent pinned to this server's registered embedded machine can use a server
placement without a shared mount. Admission records that embedded daemon as
`execution_daemon_id`, including for existing server workspaces without a
recorded provider; the workspace owner and handle stay on the server.
Daemon placements require CLI Agents for every assigned worktree role (coder,
reviewer, planner); native Agents are rejected with
`native_backend_unsupported`. Plan-writing roles (`planner`, `coder`, `worker`,
`executor`) also require the owner's `execution.plan_transport` capability;
a missing capability is a structured `capability_missing` placement refusal.
Revision-3 and older owners receive `daemon_upgrade_required` at admission.
No plan-writing role is parked with `owner_unsupported`.

Agent claims, initial launches, ordinary re-execution, and resume refuse a
paused Project with `ProjectPaused` before placement selection, reservation,
or workspace preparation. Repeating an already queued resume returns the Task
without placement work, even while paused. Follow-ups check the pause before
placement work, after transitioning the Task to its role's active state or
clearing its annotation. Re-execute recovery for a disconnected or timed-out
owner may update the placement and reconcile the owner before reaching the
pause guard. The execution admission transaction also checks the pause to
fence concurrent Project changes. Recovery keeps the `project_paused(<detail>)`
refusal and still permits other actions, such as cancelling the Task.

Embedded executor availability comes from the runtime's adapter registry.
Service and API fixtures inject `cli_adapters::test_support::test_registry`:
availability is fixture-controlled without CLI lookup or home credential
discovery, while Shell retains its local execution behavior. The fixture registry
and `TaskService::new_for_test` are gated by the `test-support` Cargo feature;
unit tests also compile the constructor. Production builds omit the fixture registry.

Claim admission runs **reserve → prepare → start**:

1. Under `BEGIN IMMEDIATE`, select a compatible ready location and persist a
   `reserved` placement with `reserved_until`. It consumes Agent and machine
   capacity, but creates no Execution, lease, or Task status change.
2. Outside the transaction, the owner prepares the workspace idempotently.
   Versioned updates move `reserved → preparing → ready` and record the handle
   and base SHA. Failed or expired preparation releases capacity, records
   `prepare_failed`, and spends no Task retry budget.
3. A claim transaction checks the ready placement's version and capacity, then
   creates the Task claim, Running Execution, and lease together. A ready placement
   holds no slot. A capacity refusal leaves it ready and records an ordinary
   parked machine waiter, without an Execution or retry-budget charge. Ready
   reclaim, missing-worktree recreation and restart relaunch use the normal
   version fence.

Backfilled `preparing` reservations expire after ten minutes. The reservation
sweep also reclaims crash-orphaned `reserved` or `preparing` rows without an
explicit expiry after ten minutes from their last update.

Candidates must pass reachability and visibility, executor availability and
adapter capability facts, daemon `workspace.v1` support, run policy, Agent pin,
capacity, the daemon placement limits, and Project environment readiness.
Missing adapter facts mean unsupported. Readiness is a fact per Project and
workspace-owner machine, with a SHA-256 digest covering only `environment.env`
and `environment.checks`; asset and interval edits do not invalidate it. Pure
selection reads the candidate's readiness and per-check results for the Task's
launching role. Current `ready` passes; a current `not_ready` row rejects only
when a failing check applies to that launching role (an unnamed launch failure applies
to its recorded role). Host missing, unknown or stale records return transient
`environment_probe_pending`. A daemon advertising `machine_probe.v1` and
allowing `environment_probe` uses the same readiness gate. A daemon without
that capability or permission retains launch-time preflight: unknown facts
mean unverified, never offline. Protocol revision stays 3. This policy applies
identically at reserve and claim. Direct/manual claims and asset-backed
Projects retain launch preflight on an existing location.
Projects with no checks ignore readiness, never probe and write no new rows;
removing checks deletes existing rows and resolves machine waits.

Check-only Projects are proactively probed on the host and probe-capable daemon owners.
When assets are configured, a primary-checkout probe cannot see staged assets:
admission uses launch-time preflight and ignores primary-checkout probe facts.
Actual launch-time not-ready facts still filter the machine. Direct/manual
claims also bypass probe-pending and check at launch; dispatcher admissions
retain probe deferral. Probes run every
configured check with Project env in the owner's verified repository checkout, outside
admission's transaction, without assets or workspace preparation. Each result
retains its check name and pass/fail status; role applicability is evaluated by
pure selection rather than by collapsing the result into a Project-wide fact.
Probes are single-flight per Project/machine and write with a digest/version
fence, then wake dispatch through the in-process dispatcher `Notify`. Settings
edits invalidate rows transactionally; the Project event observer starts host
probes for existing host rows or ready host locations and probes visible,
probe-capable daemon locations without waiting for a Task. A digest edit colliding with an older flight is re-probed on completion,
even without a queued Task; retained host rows can use the server checkout
without a location row. A passing host probe compare-and-clears a matching
environment pause against the current Project snapshot after its result, so a settings edit
cannot leave a ready host behind an old pause. Daemon probes use `machine.probe`; there is no proactive `workspace.run` through a live Task. Initial dispatch returns early before assembly when there are no checks, and
otherwise uses the same shared, read-only admission context builder as reservation. An environment refusal
keeps the Task queued with at most one version change. A preferred candidate
that is only probe-pending defers selection rather than diverting work to a
lower-preference passing owner; existing placement order is preserved.
When no ready-location candidate passes the environment, capability, visibility
and pin filters, admission may consider a daemon runtime with no location and
both `machine_probe.v1` and `repo_provision.v1`. Its local policy must allow
`environment_probe` and `repo_provision`; the repository needs a remote and
Project `settings.placement.provision` must be `when_verified` (default).
`never` disables provisioning. A ready location blocked only by capacity or a
pending probe prevents provisioning elsewhere. Probe and clone jobs take no run
slots. A daemon with no applicable machine check is `environment_unverified`.
This deterministic refusal is recorded once on the Task with Attention naming
the machine and the action: mark a Project check `machine`, or add the repository
on that machine. Repeated scans do not rewrite the Task. Changes to checks or
`placement.provision`, machine connections, and location changes wake it.
Role/assignee changes also invalidate the refusal. State-entry bookkeeping does
not: its key tracks placement facts and role assignments, rather than Task
version changes from internal hooks. A rolled-back entry refusal is recorded
against the restored Task before the scan finishes.
A provisioning candidate with an incompatible executor or capability does not
turn another candidate's deterministic refusal into a pending environment wait.

A background job, single-flight per repository/runtime, runs machine checks
before `repo_location.provision`. Only passing applicable checks permit cloning
under `<workspace_root>/repos/<repo id>` with the daemon's own Git credentials.
The resulting managed location is unverified until `repo_location.verify` and
full checks finish. Check failures retain per-role facts; successful completion
wakes normal dispatch. Clone failures leave an unavailable location and bounded
error. Durable exponential retry deadlines (`repo_provision_retry`) survive
server restarts without a persistent running flag; daemon retries reuse an
existing matching clone and remove interrupted private staging. Transport and
version errors retain check facts, reschedule and do not fail another Task.
Pending, provisioning and unverified Tasks reuse `environment_wait`, remain queued, and
are parked for Project slots. An unfinished daemon location with provisioning
disabled waits for location verification with `location_not_ready`, rather than
for a full probe without a checkout. A failed machine check on a provisioning candidate
creates Task-scoped environment Attention naming the machine and checks; it
never pauses the Project. The last-resort Project pause considers ready-location
candidates only. Scheduled rechecks recover the failing machine.
All job writes fence the check digest and observed readiness version; stale
results are discarded and the current digest is probed. Clone and verification
failures appear in the Task's wait reason, with elapsed time and the bounded
`repo_location.last_error`. Provisioning stops after five attempts for unchanged
inputs and connection, records deterministic `provision_failed` Task Attention
with the machine name and redacted last error, and resumes after reconnection or
a relevant settings edit. Both claim and dispatch paths record this once; their
eligibility key includes attempts, job-input digest and connection ID. A failed
clone does not make a connected machine offline. Selection and wait diagnostics
share one exhaustion rule. Unrelated Project settings edits leave an in-flight
job and its attempt intact; result writes still fence the job inputs and
readiness version. Socket incarnation IDs are random numeric tokens that remain
unique across server restarts without a public shape change. Backoff is 60–600 seconds. No execution,
lease or worktree exists during these jobs.

Provisioning sends the repository's default branch, creates or fetches its local
ref before verification, and uses `settings.placement.provision_timeout_seconds`
(default 1800; allowed 1–86400). The daemon's returned canonical root and clone
path are authoritative; the server checks `<returned root>/repos/<repo id>` and
records that path, including when the advertised root uses a symlink. Daemon
locations do not suppress creation of the server's own lazy location.
Credentials in URL userinfo and token-like query parameters are redacted from
errors, receipts, logs, and wait reasons on both sides; the clone URL itself is
sent to the daemon, which also uses its local Git credentials.

Deleting a Project removes its readiness and provisioning retry rows through
foreign keys. Task workspace cleanup uses the existing owner-routed cleanup
path. The repository's managed daemon clone is retained as an owner-local cache:
there is currently no repository-clone cleanup operation. The owner may remove
`<workspace_root>/repos/<repo id>` after its Task workspaces are cleaned.

The embedded provider supplies the server host's adapter facts, including session
resume for its session-capable executors. Recovery uses those facts for embedded
execution and the current owner's handshake for daemon execution; no command
socket is required for the embedded provider.
Preference is existing placement → inherited root placement → Agent pin →
default location → server-owned → `(created_at, id)`. The selection reason records
the winning rule and rejected candidates with filter codes. If no owner is
eligible, claim returns structured `placement_unavailable`; there is no silent
fallback.
Agent `runnable_on` exposes executor-fit facts from the same inputs as placement:
server adapter/native connection health, daemon runtime/connection, detected CLI
authentication, enabled policy, Agent pause and explicit pin. Admins receive
machine identities; other users receive only a count, and the pin remains
admin-only. Agent list/detail warn on zero machines; admins can clear the pin.
This read does not assert repository/environment fit or capacity. Task detail's
separate placement diagnostics panel reads recorded environment waits, pending
host probes, capacity waits and selection rejections without running admission.
It names machines/checks where recorded and renders `machine_capacity` in the
same component. CLI `project env-status` reads these Project readiness facts;
`env-recheck --machine` selects one target.

Machine saturation does not downgrade Agent availability or Project execution
setup; the Agent availability precheck uses only its identity quota.

A Task with no eligible machine and at least one candidate rejected only for
`machine_capacity` records a `machine_capacity` queued dispatch disposition,
without an annotation, Attention, failure or retry-budget charge. Initial
scheduling checks fresh read-only machine counts for every candidate before the
expensive Task gates, including runs dispatched earlier in the tick. The check
examines all potentially usable locations, including unverified clones; unknown
facts, a possibly free machine or another placement refusal delegate to reserve.
It uses the placement filters and server executor availability, without clone
verification, persisted location writes, a sweep or a writer lock.
The durable reserve/start transactions still fence races. A machine or Project capacity waiter in an active/gate state
counts as parked until dispatch observes a different outcome; a plain edit does
not un-park it, and its metadata change moves the Project list revision used by
slot memos. Queued recovery checks machine capacity before claiming its marker,
so full-machine ticks do not rewrite Tasks or publish recovery events. Automatic
review recovery checks before its barrier claim as well. A handled capacity wait
stops the active scan quietly. Its explicit dispatch owns the state-entry hook
so an ordinary coder run cannot take its slot or lease first. Queued replay claims
metadata only and checks its original queued token at Running insertion; a lost
capacity race preserves the clear blocker and marker, emits no recovery event
and retries next tick.
Capacity waits are retried by the existing dispatcher tick
(ten seconds by default); this path has no completion-event kick. Capacity
checks count Agent Chat turns, but do not change chat admission: a new chat
turn may still be leased/run when its machine is full, making subsequent Task
admissions wait.

There is no fairness guarantee across Projects: Projects are scanned oldest first,
active work is scanned before `todo`, and follow-ups or chat turns can take a freed
slot before the next dispatcher tick. Waiters resume on that tick. Review check
runs and merges take no slot because there is no durable running-check record.

Automatic dispatch keeps transient owner-unreachable, capacity, and
`environment_probe_pending` refusals queued. Initial dispatch reads the same
selection context before its workflow transition and defers an otherwise
viable probe refusal without entering the target state. The queued marker
creates no Execution or Task version change. Repeated probe waits refresh
only their retry time. Probe completion clears the delay without another Task
version change, so the next tick can enter the target state and launch.
Before the first placement exists, an offline owner creates
Task-scoped `runtime_offline` Attention and a durable wait bounded by
`workspace.max_disconnect_seconds`; expiry blocks the Task visibly. Repeated
identical waits update only their retry time; events, Attention, and Task version
change on the first deferral or a reason change. Expired waits on failed,
cleaning, or cleaned placements clear their stale wait and fall through to the
normal reset-required path. Upgrade annotations replace any stale owner wait. Explicit
recovery preserves the offline-Agent refusal even at capacity. Capacity recovery
uses `queued_recovery`; permanent replay refusals restore its original blocker.
The active scan retries structural placement refusals, because location,
executor, handshake, and run-policy changes can fix them without editing a Task.
On state entry, a structural refusal still rolls the transition back with
`dispatch_failed`; retryable owner, capacity, and environment-probe refusals defer dispatch.
Stable governance refusals use `metadata.dispatch_disposition` with the safe
reason, and remain parked until their authority changes or dispatch is woken.

After preparation, placement is sticky through retries, re-review, and recovery.
A subtask sharing its root workspace inherits the same placement; an incompatible
Agent fails admission. Reclaim describes the existing ready workspace. If its
directory was deleted, claim, resume, recovery launch, and review-CI preparation
recreate the worktree from its surviving Task branch through the recorded owner;
daemon owners receive a fenced `workspace.prepare` request with the recorded base
SHA. Server-owned Task launch and mutating delivery paths use the same validity
accessor before Git I/O, including review entry and reviewer evaluation, merge,
target-moved rebase, and reset-to-initial. An existing directory is invalid only
when its `.git` entry is absent or Git reports that it is not a repository; Git
spawn and I/O failures remain transient errors and never trigger recovery.
Missing or damaged worktrees are repaired or recreated from the surviving Task
branch. Launch may discard a root Workspace row when both are gone, while
delivery, hook, rebase, and reassignment checks preserve the row and return the
typed reset-required error. Read-only review-carry and conflict-marker checks
only validate a server worktree and never repair it or clear cleanup state. A
child always preserves its root's shared Workspace, including reassignment with
`reset_worktree`. Daemon-owned delivery calls retain their prior owner-local
path; damaged daemon worktrees require a reset, and daemon describe failures
remain transient errors without triggering metadata repair.
States progress from `reserved` to `preparing` to `ready`,
which can become `disconnected`, then back to `ready` after reconciliation.
Cleanup moves through `cleaning` to `cleaned`; failures use `failed`.
Updates use optimistic `version`
checks (conflicts return HTTP 409); `generation` changes only when the physical
workspace is recreated on the same owner.
An explicit daemon workspace reset or Task reset sends `workspace.reset` with
the next generation, the observed HEAD as its precondition, and the recorded
base SHA as its target. The returned handle, base, and generation are committed
with a placement version check. If that check loses after the owner recreates
the workspace, retry applies the retained reset receipt before using the old
generation again.

`V202610010400__daemon_owned_workspaces.sql` preserves existing data: server primary
locations are backfilled from `Repo.local_path`, and non-cleaned workspaces gain
server placements with `selected_by = backfill` and their existing worktree path
as the handle. Remote-only repositories gain an `unverified` managed-clone
location. The first embedded admission clones or verifies it outside the
admission transaction and marks it ready before selecting it. Verification uses
the same physical-path lock as clone/worktree writers, checks that a managed
clone is a checkout with the repository's matching remote, retries location CAS
conflicts, and persists clone errors with exponential retry backoff. Server paths
remain embedded backend details, not remote workspace addresses.

### Daemon command transport

Linked daemons keep a WebSocket command stream open at
`/api/v1/daemons/{id}/connect`. The API server routes filesystem requests
(`fs.list`, `fs.branches`), placement-routed managed executions
(`execution.start`, `execution.cancel`), and workspace operations over that stream.
The daemon validates paths against its advertised workspace root, runs the local
CLI adapter, streams
execution logs back as `execution.log` notifications, and reports final status
through `execution.terminal`.

Protocol revision 3 independently negotiates `machine_probe.v1` for
`machine.probe` and `repo_provision.v1` for `repo_location.provision`.
A probe accepts named commands, 1–300 second timeouts, Project env, and an
optional verified location ID. It returns exit status, timeout and a redacted
4096-byte output tail per command. Without a location, each command gets a fresh
empty directory removed on completion or cancellation, with abandoned probe
directories swept at daemon startup. Checks must be read-only: scratch directories
are a working-directory choice, not a filesystem sandbox. Checks inherit the
daemon's environment with Project env overrides, as `workspace.run` does.
Timeout and cancellation kill the entire probe process group; a completed probe
also stops background descendants. Probes take no workspace
lock and write no journal. Provisioning is idempotent by repository identity and
normalized remote: a matching clone is reused; conflicting content returns
`path_conflict` unchanged. Failed clones publish no partial final directory. Provisioning serializes only
requests for the same repository, leaving workspace operations and other
repositories' jobs independent. Clone URLs may carry owner-configured credentials;
these are never included in diagnostics. These operations require their separate
local policy purposes, `environment_probe` and `repo_provision`, and refusal is
`run_purpose_denied`. Missing capabilities do not change connection health.
Upgrade the server first: a daemon that opts into these new purposes needs a
server from this release; an older server rejects the handshake because its
run-purpose enum does not recognize them. Protocol revision remains 3.

Protocol revision 3 negotiates `workspace.v1` for `repo_location.verify`,
`workspace.prepare`, `workspace.describe`, `workspace.run`, `workspace.diff`,
`workspace.read`, `workspace.merge`, `workspace.reset`, and `workspace.cleanup`.
Plan-writing roles on a daemon-owned workspace require `execution.plan_transport`.
A revision-3 daemon without it can still run reviewers, interactive executions,
server-owned shared-mount executions, filesystem requests and PTYs. Deterministic
placement refusals record a structured Task annotation naming the machine and
missing capability. Dispatch waits until eligibility facts change, then clears
the refusal and retries.
Upgrade the server first, then every daemon using `forge-ctl` from that server
release (protocol revision 3 or newer), restarting each with its existing
`--workspace-root`.
A connection below revision 3 receives `daemon_upgrade_required` and cannot use any
command RPC: execution, repository verification, filesystem browsing
(`fs.list`/`fs.branches`), workspace operations, or PTY terminals. Operator status
shows `upgrade_required`; pinned Agents and refused Task admissions carry
`daemon_upgrade_required` with instructions to install the daemon from the
server's release. Repository locations retain upgrade reasons after a verification
attempt, without changing their verification status. Task admission is an upgrade refusal only
when an otherwise eligible owner is blocked solely by the upgrade (disregarding
facts absent from the older handshake), and no owner is blocked solely by
capacity or a transient condition. It creates no Execution or retry-budget charge.
Upgrade refusals are cleared by the heartbeat sweep once a refused daemon
reconnects at revision 3, waking Task dispatch automatically. Upgrading the daemon
is the required human action. The old daemon logs the instruction through its
existing warning handler; a new binary also prints it to stderr on connect.
A socket awaiting its handshake is `daemon_not_ready`, not an upgrade refusal.
Existing ready placements become disconnected while an upgrade is needed, with
an attention item and frozen leases. They wait up to `max_disconnect` (24 hours
by default), then fail with `owner_disconnected_timeout`.
Mutations carry an operation ID, placement generation, expected
base SHA or version, and handle or placement ID. Duplicate operation IDs return
the recorded result until acknowledgement; stale generations and wrong owners are rejected. Handles
map only to workspaces created under the daemon root. Direct merge writes the
verified primary checkout and refuses a dirty target.

`workspace.read` also accepts fixed Git inspection, owner paths, and bounded
file-tree reads. Git evidence preserves the complete output, including NUL
path separators. Review's `workspace.diff` variant uses the same three-dot diff,
fallback, and UTF-8 truncation marker as server review. Same-generation owner
operations use `workspace.reset` for assets, candidate restore, and rebase.
Conformance checks run in the prepared Task checkout through its
owner, preserving ignored dependency caches. Candidate restoration resets tracked
files after each check and refuses changes to HEAD or tracked content.

Byte reads resolve relative to the handle's `repo` worktree and stay within its
daemon-created workspace directory. This includes sibling Forge artifacts such
as `../plan.md` and the execution outbox. Traversal or symlinks outside that
directory, including into the daemon journal, are rejected. Missing files return
`workspace_file_not_found`, mapped to the embedded backend's not-found result;
an absent plan therefore preserves the plan gate's existing no-plan behavior.

A reviewed merge carries `reviewed_commit_sha` and fences the exact candidate
and target SHAs before `--ff-only` integration. Without reviewed evidence,
integration retains normal merge-commit and conflict behavior. Results include
the before/after SHAs, diffstat, and conflict paths.

Remote integration rechecks review authority after retaining its intent and
releases the SQLite write lock before awaiting the owner. This lets the socket
reader persist heartbeats and deliver the queued merge response. Reconciliation
through `workspace.describe` settles interrupted intents in the journal before replying.
An interrupted run without a retained exit result remains an infrastructure
error. Merge success can be reconstructed only when the verified target proves
the exact frozen candidate was integrated, with a diffstat reconstructed from
the original target SHA. Interrupted errors carry `entry_id`, `operation_id`,
and `interrupted: true` in their details.

Terminal reports, bounded plan/worklog/evidence outbox content, operation results,
and cleanup acknowledgements share one daemon journal. Both owner harvesters
accept newline-delimited, concatenated, and pretty-printed JSON objects, resume
at the next physical line after malformed input, and retain unknown evidence
kinds as `other` with the original kind in the caption. Entry positions use
stable `line` or `line:column` strings for ingestion receipts. Terminal and cleanup
results replay after reconnect until `journal.ack { entry_id }`.
Daemon plan candidates have a 128 KiB UTF-8 byte limit, leaving room for JSON
escaping within the 1 MiB terminal journal bound. Oversized, unreadable,
non-regular, symlinked, multiply linked (on Unix), or checklist-free candidates produce an explicit failed
terminal report, rather than truncating or omitting a successful plan. Plans
are retained with the same report identity and digest as worklog and evidence;
replay cannot substitute a different candidate. Server-owned plans retain
their existing 1 MiB limit and file layout. The server must
persist a result, including a terminal operation error, before acknowledging it.
Exact terminal replays are coalesced while retained so a describe-triggered replay
cannot compete with the reconciliation drain. The daemon then deletes the receipt; repeated acks are
idempotent. Unacknowledged results survive a crash between completion and ack.
For a successful write-capable execution, the daemon records the worktree's
actual Git HEAD when the selected adapter does not return one, so reconnect
reconciliation retains commit evidence for Shell and other minimal adapters.
The journal caps receipts at 1,024 and all persisted journal files (including the
workspace registry) at 32 MiB, with 16 MiB per receipt. Usage is indexed once and
updated on writes. Mutation deduplication opens only its operation receipt with
a buffered reader. `workspace.describe` scans pending entries for terminal IDs,
then the command runtime scans them again to replay pending notifications.
CI steps may request `timeout_secs = 0` for unbounded execution time, but output
is capped at 1 MiB per stream, even with `max_output_bytes = u64::MAX`. Truncated
log tails start with `[Forge: CI log truncated]` and set the stream's truncation flag.
Logs keep their UTF-8-safe tail and may be trimmed further, or dropped entirely,
to fit the shared journal. Admission reserves completion headroom for every run;
exit code, identity, and flags survive journal pressure. An unbounded CI request
accepts a truncated tail and retains the actual exit verdict; bounded conformance
commands still reject output over their budget. If a descendant holds a pipe open
after shell exit, the two-second drain retains the bytes read and sets
`stdout_drain_incomplete`/`stderr_drain_incomplete`, independently of size truncation.
Cleanup retains the handle and its execution IDs until the cleanup receipt is
acknowledged, so describe-first retries can replay its result. Reset and review release similarly retain retired review handles until their
receipts are acknowledged, preserving generation fences. Each acknowledgement
prunes only the handles retired by that operation.
Corrupt or non-regular `entry-*.json` files are quarantined as
`corrupt-<original name>` when possible and logged with their paths. Quarantine
rename or directory-sync failures are logged and skipped; valid entries still replay. A failed command-stream task terminates the
daemon promptly instead of continuing REST-only reporting.
Other command purposes require positive time and output budgets. Environment
values are redacted before journal writes; requests retain variable names and
a digest of the redacted request for replay identity, never secret values or a
caller-supplied digest. Owner-operation environments also retain only variable
names, and their check commands are redacted. Redaction applies to request commands,
result stdout/stderr, error messages, and terminal output text; identities,
accounting fields, exit codes, and identity paths are preserved.
Managed Codex resolves cache symlink chains before granting writable roots. It
normalizes the macOS `/System/Volumes/Data` prefix before comparisons and rejects
roots that contain HOME (also comparing device/inode identities of its ancestors),
the worktree, its parent, or the managed home. It rejects roots inside credential
directories, `<daemon root>/.forge`, the managed home, or another Task directory.
The daemon root itself is not protected, so `--workspace-root "$HOME"` permits
ordinary caches. Default caches are allowed even when mounted. A default cache
redirected by a symlink is refused only when its resolved path is exactly a mount root. Linux compares `/proc/self/mountinfo` mount-point strings
without opening the mounts; other platforms infer the mount root from device
boundaries or the filesystem root. Explicit cache environment-variable relocations
are exempt from the mount-root rule. Defaults are compared through canonical HOME,
so a symlink in HOME alone does not count as a cache redirect. Forge's exact
scratch and execution-outbox directories remain designated grants, and ordinary
relocated caches are allowed.

Ownership remains fenced per location and workspace handle. There is no sticky
daemon-wide runtime pin to survive Task completion, cancellation, or placement
removal.
See [Daemon journal migration](getting-started.md#daemon-journal-migration) for
the on-disk upgrade.

#### Daemon run policy and trust

`workspace.run` accepts only `ci_step`, `hook`, and `environment_setup` from
server-side Task or Project configuration, using the embedded backend's shell
semantics and command budgets. The daemon reads `workspace.run.allow` from its
local `daemon.yaml` beside its credentials; the default is `[ci_step]`. Hooks
and environment setup require explicit local opt-in. Requests cannot override
the effective policy loaded at startup. Admission rejects a candidate with `run_purpose_denied` when required
purposes are disallowed; the daemon also rejects the command with `purpose_denied`,
which is never retried. See the [configuration example](getting-started.md#daemon-run-policy).

Anyone who can edit server-side review steps, Project hooks, or environment
checks can run their permitted shell commands on the daemon's machine.

The daemon run policy is not a security boundary against a compromised or
malicious server: the shell executor and owner operations are not gated by it.
The server can read anything under the daemon's workspace root. Choose a root
containing only files you intend to expose to that server. Processes also have
the daemon user's `HOME`, credentials, and network access.

A write-capable embedded worker turn checks its repository delivery boundary
before it reports completion. If the final HEAD is unchanged while the
worktree is dirty, the execution fails and enters the normal retry/block path;
the authored files remain uncommitted for inspection or retry. Forge does not
stage or commit those files on the worker's behalf. Provider-authored commits
remain valid, while clean no-op turns continue through the workflow-aware no-op
policy in the Task runner. Within the execution retry budget, Forge records a
system comment instructing the assigned worker to inspect the preserved diff,
finish or clean it up, validate it, and commit before completion, then schedules
a deferred redispatch. Exhausted retries block visibly instead of looping.
Read-only planner/reviewer and read-only task-type runs remain governed by the
separate restore-and-fail backstop. CLI adapters commit whatever a
write-capable run leaves in its worktree after the process exits, but never
for a read-only run: the untracked build output a reviewer's checks leave
behind is cleanup for that backstop, not authored work, and committing it
would move HEAD and fail the review. Every CLI adapter also exports
`PWD=<worktree>` next to the child's working directory, because tools that
trust `$PWD` over `getcwd()` (OpenCode does) would otherwise resolve the
Forge server's own checkout as their project.

### Daemon lifecycle and execution recovery

Remote daemons periodically report local CLI availability and, when connected
over the command stream, their currently running managed execution ids via
`POST /api/v1/daemons/{id}/report` (`active_execution_ids`). This report is a
reconciliation input, not proof that output was emitted. A managed execution
is claimed against the authenticated daemon connection incarnation before
`execution.start` is dispatched; transport heartbeats renew that same
owner-bound execution lease independently of log notifications.

When a workspace owner or remote execution daemon disconnects, its `ready`
placements become `disconnected` and their Tasks wait with visible Attention.
Heartbeat-lease expiry detects the same outage when the daemon is frozen but
its TCP socket stays open. Suspension rechecks the execution version and expired
lease together with the placement version in one database CAS; a heartbeat,
terminal result, or concurrent socket-disconnect handler cannot double-apply it.
Forge does not
requeue or rebuild them on another machine. Running leases are suspended:
heartbeat loss cannot expire them or discard a retained terminal report.
`max_disconnect` (default 24 hours) ends the wait with
`owner_disconnected_timeout`; the execution's hard deadline still applies.
This also applies to server-owned workspaces executed on a remote daemon.

On reconnect, `workspace.describe` supplies workspace state plus active and
journaled execution IDs. Forge drains retained results first, resumes leases
for active executions, applies finished reports once, and fails an unknown
execution with `owner_lost_execution`. It compares HEAD with recorded evidence
before returning the placement to `ready` and waking dispatch. Forge records
the HEAD produced by its own rebase as generation-bound evidence in
`workspace_expected_head` (`V202610010530`) for
remote owners/providers. Evidence uses the execution's immutable terminal time,
so later edits to an old execution cannot supersede a rebase head. A recording
failure is logged without failing the successful Git operation; reconciliation
may then require an explicit reset.
Terminal reports are acknowledged after reconciliation and the initiating Task
transition plus any discovered cascade enqueue commit. Queued follow-up hops
may still be pending at acknowledgement; SSE or a later GET reports their final
Task state. A report received during suspension stays unacknowledged until that
settlement finishes; retained reports can replay after a settlement failure or
server restart.
A periodic sweep
retries reconciliation for disconnected placements whose owner is online, so
an interrupted reconnect can finish after a server restart. The workspace cleanup
scheduler is the sole periodic retry owner for `cleaning` placements; it retains
the cleanup backoff and waits for an owner acknowledgement. A stale, still-open socket waits for a heartbeat or a new connection
before reconciliation RPCs resume. Server-owned shared mounts use the embedded
backend to describe HEAD after the remote provider's retained terminal report
settles. Late terminal CAS losers cannot replace an accepted outcome.
The heartbeat monitor completes its core liveness pass before placement
maintenance. Owner reconciliation runs detached from the tick, grouped by
daemon, with per-placement in-flight guards and at most two concurrent owner
workers (leaving capacity in the five-connection SQLite pool). Each RPC has its
own transport deadline. A completion cascade, including review CI, is no longer
bounded by reconcile deadlines: it runs detached from the owner worker and
releases that worker's permit and daemon key. Live terminal delivery waits for
per-Task cascade serialization so every completion is driven, even without a
dispatcher or monitor tick. Terminal reservations and pending RPC entries are released on drop.
Readiness, Attention resolution, and dispatch wakes commit in one transaction.
The ready-placement sweep considers only retained terminal reports and placements
reconciled in the last minute. It skips a busy Task cascade without waiting and
ends an owner's batch on its first transport timeout or unavailability. The
monitor aborts its owner workers on stop. Workspace-operation acknowledgement
receipts survive a restart; execution-report acknowledgement retries use retained
in-memory reports, so after a server restart they resume when the daemon replays.
Ignored execution reports are acknowledged after the execution is terminal,
carries an owner-failure cause, or has been deleted. A refused workspace receipt
acknowledgement is logged without starving later receipts; transport failure
ends that owner's batch. Cleaned placements treat an already retired daemon
handle as absent, allowing terminal sweeps to remove their managed homes.

Placement and transport failures do not spend the Task retry budget.
`placement_unavailable` and `prepare_failed` create no Execution;
`owner_disconnected` suspends work; timeout or a lost execution offers recovery
on the same owner. `stale_generation` and `wrong_owner` refuse the operation and
surface an Attention item. Users may wait, retry on the same owner, or cancel;
cross-machine migration is outside this change. `retry { fresh_session: false }` requires the
owner to be online and advertise `resume` for the snapshot's executor.

Cleanup goes through the placement backend. An offline owner's placement stays
`cleaning` and visible until the owner acknowledges removal; only then is it
`cleaned`. Immediate cleanup retries a placement version conflict within its
existing timeout, including when the owner's unsolicited cleanup notification
commits `cleaned` before the matching RPC response is applied, so concurrent
disconnect or reconciliation does not discard the cleanup request. A retry of an already cleaned daemon workspace also drains
retained journal acknowledgements. With a persisted `cleaning` intent, an owner's
`invalid_input: unknown workspace_handle` response means cleanup already finished;
other unknown-handle responses remain errors. Cleanup acknowledgements report
`removed: false` when the exact embedded worktree was already absent.

Remote output, reasoning, and tool notifications update semantic progress
when accepted, but a quiet remote execution remains healthy while its lease is
current. Stale semantic progress may create a separate
`execution.progress_warning` Attention item. Only owner-lease expiry or the
execution's explicitly configured hard deadline is an execution-liveness
terminal condition; the hard deadline is not extended by heartbeat renewal.

### Task terminal sessions

Task terminal sessions are a separate API and daemon path for interactive shell
access to an existing task worktree. They do not layer onto
`TaskService.transition()` or the workflow engine. Creating a terminal does not
claim, transition, reset, or launch the task.

The browser connects only to the API server in v1. REST calls create sessions
and issue short-lived attach tokens; the browser then upgrades to
`/api/v1/terminals/{id}/ws?attach_token=...`. There is no direct
browser-to-daemon connection. For daemon-owned workspaces, the API server
proxies terminal operations over the existing daemon transport:
`terminal.start`, `terminal.input`, `terminal.resize`, and
`terminal.terminate` requests flow to the daemon, while `terminal.output` and
`terminal.exited` notifications flow back to the server. Embedded server mode
uses the same service path and also runs a local PTY-backed process; it does
not use plain stdin/stdout pipes.

Process ownership lives on the daemon side for daemon-owned workspaces and on
the API server for embedded workspaces. The API routes terminals through the
workspace's persisted placement, including its selected execution provider,
rather than the Agent's current daemon pin. Both runtimes
allocate a PTY, start the shell in the server-authorized worktree, forward input
and output, apply resizes, and terminate the process. Daemon-side starts
additionally reject workspace paths that escape the daemon workspace root.

The API server persists lifecycle metadata in `task_terminal_session`, including
task, workspace, daemon, dimensions, status, timestamps, creator, and exit
metadata. Attach tokens are stored only in memory and are single-use. Reconnect
scrollback is an in-memory bounded ring buffer per running session, capped by
`terminal.reconnect_scrollback_bytes`, and is dropped once all browser clients
detach from that session; full terminal transcripts are not persisted in v1.

Terminal sessions and managed Forge executions cannot run concurrently in the
same workspace. Terminal creation is blocked while a managed execution is
active, and managed execution startup must reject or defer while a terminal is
active for that workspace.

Cleanup is time- and ownership-bound. The default idle timeout is 30 minutes
(`terminal.idle_timeout_secs = 1800`) and the default absolute lifetime is
8 hours (`terminal.max_lifetime_secs = 28800`). Workspace cleanup terminates
running sessions before removing the worktree. If a daemon disconnects beyond
the heartbeat cleanup threshold, the daemon kills the terminals it owns and the
server records the sessions as exited, timed out, orphaned, or cleanup
terminated when it observes the terminal lifecycle event.
Embedded terminal watcher cancellation or control-channel closure also kills
the shell, allowing the blocking PTY reader to exit during runtime shutdown.

## Task state machine

```
backlog ──► todo ──► planning ──► in_progress ──► review ──► merging ──► done
   ▲          │                           ▲           │          │
   └──────────┘                           └───────────┘          ▼
                                                       merge_failed

                         any non-terminal state ──► cancelled
```

REST/MCP transition responses describe the requested commit; automatic cascade
hops are durable queued steps. A response's `pending_steps` tells clients that
follow-up work remains; SSE or a later GET supplies its final state.

All non-terminal states can transition to `cancelled`. Terminal states: `done`,
`cancelled`. The default workflow lives in
`crates/services/src/workflow/default_workflow.rs` with sequence
`backlog → todo → planning → in_progress → review → merging → done` and
`merge_failed` and `cancelled` as failure/terminal states. A `merge_failed`
Task can return to `review` after repair or retry from `in_progress`; typed
blocking annotations park a Task in its current state and are not states
themselves.

Project environment pauses do not add Task failure states. Embedded checks and
review CI use the workspace backend command path; remote starts perform the same
preflight before their provider RPC. Environment pauses retain the failed
workspace ID: scheduled and manual re-checks use the primary checkout for embedded
work and the recorded ready daemon placement for remote work. Missing or unresolved
daemon workspaces, non-ready placements and unreachable owners retain that
machine's facts and report unavailability; they never use the host as a substitute.
Named check failures retry on their machine. An unnamed launch failure needs
manual resume or Check now, and an owner run-policy refusal during re-check
retains the failure facts and reschedules the attempt.

An admitted Task waiting for an owner keeps its active Project slot even before
an Execution exists. Owner timeout, workspace reset and review-CI infrastructure
blockers count as parked through the shared blocking-kind list, used by dispatch,
SQL slot aggregation and API projection. Per-Project and per-Task scan boundaries
isolate owner expiry, deferral, upgrade wake and environment errors so later work
is still scanned. Placement's Agent/capability facts resolve the same effective
coder as claim and execution, inheriting only the coordination root's coder.
The root's other workspace roles still constrain placement because their later
executions use the same shared workspace.

 A `review_needs_owner`
annotation parks a Task in `review`, freeing its active slot until the owner
acts; the failed Review remains in history. Retry with guidance re-enters normal
recovery, while a manual pass or deferred follow-up lets the Task proceed to
merge without spending review-remediation budget.

The built-in workflow choices are:

- `default` / **Agent review** — the assigned reviewer Agent accepts or rejects.
- `no-review` — required checks run and the Task continues without a reviewer execution.
- `human-required` — the user or bound Project Agent accepts or rejects.
- `autonomous_v1` — the single-worker compatibility preset with hard validation and human review.

In the default workflow, a completed planner may leave `planning` only after
Forge can read a valid checklist plan. The planning gate then advances to
`in_progress` automatically; it is not a human approval boundary. A custom
workflow that sets `requires_user_approval` on that gate remains in planning
with `awaiting_human` until the user approves it, and the dispatcher will not
start or relaunch another non-reviewer role while that marker is present. A
missing or invalid plan is handled by the normal bounded execution-guard retry
path and becomes a durable blocker when the budget is exhausted, rather than
causing an unbounded sequence of fresh planner runs.

The `autonomous_v1` preset lives in
`crates/services/src/workflow/default_autonomous_workflow.rs`. It is a
single-worker graph: `backlog → ready → working → review → merging → done`,
with `merge_failed` returning to worker execution and `cancelled` as the
terminal cancellation target. `working` and `merge_failed` explicitly use the
`worker` role; `review` has no reviewer role and requires human approval.
Entering review runs the before-work hooks and CI steps as blocking guards.
Implementation prompts (coder and worker) list the exact effective `ci_steps`
and say they run as `bash -lc` from the worktree root, so the agent validates
against the real gate rather than its own interpreter or command. A failing
step's transition reason names the step, exit code, command, and last output
line (`CI step 1 failed (exit 1): \`pytest\` -- 2 failed`); the full output
tail stays on the review and is quoted to the coder's fix attempt.
Review rejection and merge repair resume the latest worker thread. The worker
plans internally, implements, self-tests, repairs ordinary unfinished-worktree
failures, and reports verification evidence, so the preset has no planning
state or plan-checklist gate. A real branch conflict goes back to the
worker, but Forge does the Git part: managed agents cannot rebase or write
linked Git metadata, so when the rebase onto a moved target conflicts — or a
candidate without an agent review contract (human reviewer, or no reviewer)
hits a plain merge conflict, which Forge then rebases the same way unless the
`merge_fix` budget is 0 — Forge
commits each text-content conflict step with its conflict markers, finishes the
rebase, and sends the Task to `merge_failed` marked `[conflict-handoff]` with
the affected paths. The worker reconciles those files by editing and committing them, and the
result goes through fresh checks; it keeps the previous approval when the review
authority carry described under review conformance applies, and otherwise gets
a fresh review. A handoff
does not spend the merge-fix retry budget. A blocking `before_exit` guard on
`merge_failed` (`require_conflict_markers_resolved`) refuses the move to
`review` while the committed HEAD still adds marker lines in a handed-off path;
the worker is resumed through the workflow guard-retry budget with the file
names, and the Task blocks when that budget is spent. Integration repeats the
check as the final safety net (only in paths handed off in the current retry
window) and parks unresolved files for manual Task-worktree repair. Modify/delete and binary
conflicts also require manual repair, as do more than five handoffs in one
retry window and conflicts on coordination roots, whose aggregate branch stays
on the manual path. There, the recovery action creates a
marked review-refresh transition, clears the old approval, and runs the
repaired result through fresh checks and review before merge.

The durable `conflict-hotspots` consumer observes workflow-authored
`merging → merge_failed` handoffs after its installation timestamp. One query
seeks Project Tasks through the existing Project index, then merge-failed logs
through `idx_transition_log_merge_failed` with UTC RFC3339 time bounds computed
in Rust. The seven-day window ends at the source handoff's timestamp; full
transition reasons supply every path, excluding dependency lockfiles. Three
distinct Tasks emit one `project.conflict_hotspot.detected` event and open a
Project/path episode atomically with the cursor. An open episode skips counting
and emission even while Attention is unprojected, acknowledged, or snoozed.
Only a user resolution at or after `open_since` closes it and advances its
boundary; three qualifying handoffs newer than that boundary can open the next
episode. The summary remains at its first crossing. Episode rows cascade on
Project deletion; shared retries/dead letters and existing wake budgets apply.
The Project Agent proposes one splitting Task; the user resolves the incident
in Mission Control.

### Task condition actions

`services::available_actions(&TaskSnapshot)` is the sole pure Task action resolver. The one snapshot builder loads Task, bounded execution authority, latest Review, role assignments, transition history, existing interruption columns, entry-barrier/flow-control metadata, placement and Agent/Project availability, and caller authority. REST and MCP Task list projections carry no actions and obtain offers on demand. The admitted native `work.read` projection includes live offers for the bound Project Agent. The function performs no database or workspace I/O. REST, diagnostics, execution controls, MCP, native coordination, Attention, and Solo consume its offers.

The closed verbs are `start`, `hold`, `release`, `retry`, `send_back`, `approve`, `restart`, and `cancel`. Each offer carries meaningful parameters, allowed boolean values, authority, reason, label, cancellation propagation, and a pinned resumable execution. Required operator reasons and send-back guidance are supplied by the caller and retained in review records, comments, follow-up Tasks and transition logs. Commands check the version and select a current offer. A missing offer produces one `action_unavailable` error with current offers. Gate overrides remain owner-only.

Annotations record conditions and evidence, never an action allowlist. Old JSON `recovery_actions` keys are ignored, including unknown historical strings. Historical queued commands are translated at the stored-data boundary from current snapshot facts; their original payload is retained. A superseded or unrepresentable intent restores its condition for an explicit new command. No schema migration is required. Annotation, blocked, failed and entry-barrier columns remain separate.

Recovery commits a queued intent with the saved condition. The dispatcher consumes it through normal admission and waits quietly for capacity, a paused Project/Agent or a reachable workspace owner. Permanent refusals and malformed/stale intents remove the marker and atomically restore the condition with the error, fenced by Task version and marker identity. Fresh retries use the role prompt and review-bound admission; send-back continuations use the review-fix prompt. Restart applies its reset immediately and retains its restart/unblocked events. A shared in-process wake resumes the existing loop after command commits and terminal executions. Gate decisions still transition through the workflow engine; their worker dispatch is deferred and queued, preserving the worker thread on send-back. No new polling worker is introduced. Session launches remain separate Task-adjacent operations. `hold` parks workflow Task work. Stopping a specific execution or side session uses the execution `/stop` resource, including when both run together.

Task action conditions precede generic review decisions. In `review`, ordinary
approval requires an awaiting-human Review and no condition. Re-running review requires
a completed implementation candidate; entry-check retries and budget resets remain
independent controls. Dependency cancellation uses the writer's
`workflow_guard_rejected` annotation with `blocking_reason:"dependency_cancelled"`
and offers cancellation only. A queued action always offers Hold alongside cancel,
even when a newer condition appears. Restart follows the resolver's explicit set
of resettable condition kinds. Pause refusals preserve the typed wait cause and
turn retry scope across REST, MCP, and native tools.

### Root Tasks and ordered subtasks

A root Task is a Task with no `parent_task_id`. Setting `parent_task_id` makes a
Task a direct child of that root and selects the root's shared workspace; it is
the hierarchy/workspace relationship, not a prerequisite edge. A root Task
with direct children is a non-executing coordination container, not a worker
execution. The bound Project Agent can create, sequence, assign, and observe
those children, but Forge never sends the root's implementation prompt to one
Agent as a proxy for all child work. A `coder` assignment on the root is the
default worker for children that have no own `coder`; converting a Task into a
coordination root keeps that assignment and removes other non-review roles.
The root's aggregate review role also remains assignable and is the only role
that may execute on the root, only while the root is in its review state. The
root `coder` never executes there. The review role is whatever the workflow's
review gate declares, not the built-in `reviewer` name.
`services::task_hierarchy::RootRolePolicy` owns these assignment and execution
decisions.

Each subtask is an independent Task with its own status, execution record,
session, logs, retry state, comments, and completion event. Its effective coder
is its own `coder` row when present, otherwise the root's `coder`, otherwise
none. Resolution happens at scheduling, claim, and execution admission; Forge
does not copy the inherited row onto the child. Changing the root default wakes
parked children without replacing an already-running execution. Siblings reuse
the root-owned workspace so their commits accumulate on one delivery branch.
Workspace ownership does not imply Agent ownership: the workspace execution
lock serializes access, and the dispatcher admits only the first incomplete
sibling, so ordered children execute one at a time even when different Agents
are assigned. Execution admission repeats both assignment resolution and the
single-run check transactionally against the shared workspace, so remote daemon
workers cannot bypass serialization. Reordering is transactional as well: the
terminal prefix and current first incomplete child remain fixed, and only the
untouched `todo` suffix may be permuted.

The first runnable child creates the root workspace on demand. Completing a
child wakes the next ordered sibling. Once every child is `done` or
`cancelled`, Forge advances the coordination root to its aggregate review
gate; review and merge remain root-level operations, but they bind CI and
integration to the latest relevant child implementation execution and the
root-owned shared workspace. The root may receive a reviewer execution for
that aggregate gate, but it never receives a root implementation execution.
If the integration target moves after aggregate review and Forge can rebase the
shared worktree cleanly, a durable review-refresh transition returns the root
directly to aggregate review without notifying or dispatching a merge-fix Worker
and without spending merge-fix budget. Crash recovery recognizes the same marker
and completes an interrupted refresh. A conflicting rebase cannot dispatch work
on the non-executing root, so Forge leaves the root visibly blocked with a manual
workspace-repair action that returns the repaired branch through aggregate
review before retrying integration. A failed or
blocked child stops the sequence at that child so the Project Agent can inspect
evidence, reassign it, or otherwise coordinate recovery.

Forge owns Task workspace cleanup. Entering a terminal workflow state schedules
cleanup without waiting for filesystem deletion. The deadline worker removes
the Task's exact worktree through `git worktree remove --force` and removes its
build output. Built-in `done` Tasks are eligible promptly; `cancelled` Tasks
retain their worktrees and managed homes for 24 hours to preserve uncommitted
work. Broad Git worktree pruning runs only in Forge-owned `.repos/` caches;
user-owned repositories retain all unrelated worktree registrations.
Task branches and execution logs are retained; only
`.codex-managed-home` (including task scratch) is removed from the Task's logs
directory. Cleanup is deferred while any execution or WorkspaceLease is active.
A bounded sweep runs on startup and every ten minutes to backfill terminal Tasks,
including missing worktrees and managed homes left by older installs. The
deadline worker snapshots a bounded set of due rows before processing them, so
rescheduling one failed cleanup does not remove later due rows from that tick.
Ordinary failures persist an attempt count and bounded error tail, then retry
with exponential backoff from one minute up to one hour. An unreachable daemon
owner keeps the fixed one-minute retry without increasing the failure count.
The sweep honors cleanup deadlines and grace periods, and ineligible scheduled
rows have their deadlines and retry state cleared so they cannot starve later
cleanup. Successful cleanup and workspace reuse also clear retry state. Cleanup
failures are logged by the worker and never reported as transition effect
failures.

Operators must not delete worktrees based on execution status. A completed or
failed execution can belong to a Task still in `review` or `merge_failed`, whose
workspace remains live until the Task itself reaches a terminal workflow state.
Child terminal/reassignment cleanup never owns the shared root workspace; a
terminal child releases only its own managed Codex home. A root cancellation
terminalizes active child executions before root cleanup, and root cleanup also
waits for every child to become terminal. Reopening a terminal Task waits for
physical cleanup before its workspace can be rebuilt from the retained branch.

Dependency IDs are separate directed prerequisite-DAG edges. `depends_on_ids`
on the direct MCP task-creation API (and `depends_on_task_ids` on the native
Task proposal) names Tasks that must reach `done` before the dependent can be
dispatched; the REST dependency routes use one `depends_on_id` per edge.
Dependency edges never establish hierarchy or workspace sharing: two Tasks can
be prerequisites while keeping unrelated workspaces, and siblings can share a
root workspace without depending on one another. Edges must stay within the
Project and remain acyclic. A child cannot depend on its coordination parent,
because parentage already supplies the shared workspace and the parent only
advances after its ordered children settle. Dispatch checks all prerequisite
edges independently of subtask order; reverse-dependent queries expose the
other direction of the same graph.

Assignment and execution are deliberately separate. `forge_assign_agent`
only persists the effective implementation-role assignment and therefore
works while a Project is paused. Resuming the Project lets the scheduler
start eligible assigned Tasks; assignment itself never creates an Execution.
Changing an assignment through this assignment-only tool is rejected while the
implementation role has a running Execution, avoiding hidden cancellation or
Task-state reset side effects.

These review modes are Task workflow behavior, not Project approval layers.
Project defaults may select a mode, and an individual Task may override it.
An execution stop or cancellation first remains an attempt-level audit event.
Automatic/deferred retries and expected cancellation or reassignment do not
create Attention or a wake. Once Task disposition is settled, the atomic
`task.interruption_changed` event records the effective Task state, reason,
execution reference, and any advertised recovery actions; only an actionable
result admits a wake for the configured Project Agent. The corresponding
committed Task outcome remains the source of any human notification. A manually
resumable terminal attempt with no disposition past the bounded settlement
grace is handled by the orphan safety net as an actionable exception.

### Workflow engine

`WorkflowEngine` in `crates/services/src/workflow/engine/mod.rs` is the
data-driven Task transition path. `TaskService.transition()` supplies service
pre-checks and delegates the transition to the engine; there is no parallel
legacy `TaskStatus`/`transition_allowed` state machine.

Workflows are project-defined JSON in `project.workflow_definition`. Empty
string or `"{}"` resolves at runtime to the built-in `DefaultWorkflow`.
Transition entry points resolve the applicable definition directly; there is
no process-local workflow cache to invalidate.

The applicable `WorkflowDefinition` is resolved **exactly once** per transition
entry via `WorkflowEngine::resolve_workflow_for_task`, keyed on whether the task
is a root or subtask, whether its **current** state belongs to the inherited
subtask workflow, and the acting party (`triggered_by`). The result is passed
into wrapper pre-checks, `WorkflowEngine::transition` / `transition_inner`,
and `HookContext` so hook actions, cascades, and advance steps consume the same
definition — no downstream layer re-resolves for that transition. Each queued
transition entry (for example a system cascade step) calls the same function at
its own entry, which is correct because resolution keys on current-state
membership, not on who started the original user move.

| Task | Current state | Actor | Applicable workflow |
| --- | --- | --- | --- |
| Root | any | any | Project |
| Subtask | Not in inherited subtask workflow (e.g. `review`, `merging`) | any | Project |
| Subtask | In shared subtask-workflow state (e.g. `in_progress`) | User (`user:*`) | Project |
| Subtask | In shared subtask-workflow state | Agent or system | Inherited subtask workflow |

This aligns validation with the frontend, which presents target states from the
project workflow for all tasks. Automatic subtask lifecycle in subtask-workflow
states is unchanged. All undefined-state rejections — in the engine, hook
actions, cascades, recovery helpers, and prompt preview — flow through
`WorkflowEngine::undefined_state_message`, which enumerates the workflow's
defined states.

Both ordinary transitions and atomic board moves record the effective
workflow's digest and source/target state semantics in the durable
`task.transitioned.workflow_snapshot`. The snapshot also pins the source Task
version and parent, with the write protected by the Task version CAS. Attention
and outcome reconciliation replay that recorded meaning after workflow edits
or Task reparenting; current workflow state cannot supply missing history.
Historical events without a snapshot remain unchanged and have unknown
workflow semantics. Live authorization and recovery checks still use current
state.

`StateKind` classifies states:

- **`backlog`** — parking lot; agent claims rejected. Reserved for a
  deliberate later move by a user or the Agent (e.g. reprioritizing); a Task
  never starts here. A Task proposed while its Project has no primary
  repository still starts in the workflow's initial state — the Project
  itself is paused instead (see below), and initial scheduling gives a Task
  with no role assignments at all the Project's default assignees before
  dispatching it, since those defaults can be written to Project settings
  after such a Task was proposed.
- **`initial`** — exactly one per workflow; validation rejects zero or multiple.
- **`active`** — work state; may declare a role such as `coder`.
- **`gate`** — validation/processing state; `gate_config.max_rejections`
  enables retry-budget checks.
- **`terminal`** — absorbing state; outbound transitions and non-terminal
  cancellation targets are rejected.
- **`custom`** — no built-in behavior beyond graph validation.

A Project's Tasks never sit in `backlog` just because the Project has no
primary repository. Tasks persist no repository selector and remain in their
normal workflow state while repository readiness is a Project-level execution
setup dimension. The Task dispatcher's scan (`TaskDispatcher::sync_repository_pause`,
ahead of its per-project dispatch/recovery in `check_once`) resolves
`primary_repo_id` to a Repo owned by that same Project. Missing or invalid
selection records a visible setup blocker, issues no execution or lease, and
never substitutes an unselected legacy Repo row. A linked Repo whose checkout
fails the same readiness check execution admission applies (a local checkout
must be a git repository whose `main` branch has a commit) pauses the Project
with `system_pause_reason = "repository_not_ready"` instead of letting every
Task park on a setup refusal that an out-of-band first commit would never
wake. Positive checkout verification is memoized per Repo snapshot, so a
ready Project costs no git processes per scan. Attaching a valid, ready Repo
(or the first commit landing) wakes normal scheduling, so an already governed Task can launch without a Task
update or backfill. A matching system pause may clear, but a user's own pause
via `POST /projects/{id}/pause` (or any general Project update that sets
`paused_at`) always clears `system_pause_reason`, so a deliberate pause is
never auto-resumed or mistaken for automatic setup state. Repository readiness
does not fabricate Charter governance; a Task missing required current-Charter
traceability remains separately blocked before lease issuance. Project pause is
also checked inside the `BEGIN IMMEDIATE` transaction that creates every running
Execution and its first owner lease. This is the final admission boundary for
initial, follow-up, recovery, and reviewer runs, so pausing between a dispatcher's
preflight and its write cannot launch a new execution. Pause also fences local
integration. A passed review that reaches the pause boundary retains a durable
`paused_integration` marker instead of advancing or writing the default branch;
the review-integration authority lock rechecks `project.paused_at` inside its own
serialized write transaction immediately before Git integration. After resume,
the dispatcher consumes the marker and retries the exact `review` or `merging`
capability without creating another reviewer execution.

The per-Project `check_once` order is repository pause synchronization,
environment pause synchronization (if the repository pause did not change),
plan-publication reconciliation, the paused-Project skip, recovery of admitted
work, then slot-gated initial scheduling. A changed pause skips dispatch for
that tick so the next scan reads the updated Project. Repository and environment
synchronizers only clear the system pause reason they own. Invalid environment
pause/settings JSON or a failed slot projection is logged and skips that Project;
later Projects still get their dispatch scan.

Initial scheduling admits ready Tasks in the existing queue order only while
`active < settings.max_active_tasks` and `parked < 2 * limit`. The default limit
is 5 for new and existing Projects; 0 disables both gates. Slot counting follows
the effective workflow's `active` and `gate` kinds, excludes blocking annotations
and awaiting-human Reviews. One SQL aggregate reads the counts without loading
Task or Review bodies; unlimited Projects skip it and report zero counts, and
dispatch skips it when no initial Task waits for admission. Coordination roots
whose children hold active slots do not consume another slot; once no child is
active, a root's own running execution, review, or merge holds one slot.
Planning, implementation, review, merging, and conflict repair therefore hold
slots, even without a running execution. Agent concurrency remains a separate
limit. Ordinary already-admitted work retains its Project slot. A parked machine waiter
must obtain Project room before it un-parks; a full Project replaces the machine
reason with a parked `project_capacity` wait and retries on later ticks.

The Project response exposes `{limit, active, parked, queued}` as `slots`.
Capacity waits record `project_capacity` dispatch dispositions with
`project_at_capacity` or `project_waiting_on_owner`; `workflow_health` projects
the current message and reason to cards and Task detail. Unlike sticky role
refusals, these waits are reconsidered every tick as other Tasks free capacity.
Recording a changed capacity disposition or clearing one publishes `task.updated`
to refresh cards, Task detail, and Project slot usage; unchanged ticks emit nothing.
Each visible Task is counted once by its effective state: `queued` counts initial
states, while admitted recovery waiting for Agent capacity still counts as active
unless parked. A durable `queued_recovery` wait reports `Retry Queued`; a Project
admission wait reports `Waiting for a Slot` or `Waiting on Owner`. Both are carried
in the compact Task list's existing `workflow_health` projection.

`WorkflowEngine::transition` lifecycle for `A → B`:

1. Validate the Task version, applicable workflow authority and graph edge.
2. Run filtered `A.before_exit` guards. Blocking failures reject before commit.
3. In one writer transaction, update status/version/epoch, write transition_log
   and task.transitioned, and insert a `hooks` task_step keyed by the log ID.
   A leased cascade's done acknowledgment shares this same transaction.
4. Return the Task produced by that CAS, its current Review and pending_steps.
   The Review is the one current at commit, before any entry hook ran: a Review
   that entry CI creates appears on later reads. Post-commit hooks and their
   cascades run asynchronously. A transition with hooks immediately has at least
   one pending step; tests use drain(task_id).
5. The hook step executes on_exit → before_enter → on_enter → effective
   after_enter in workflow order, filtered by the committed actor's audience.
   Gate retry-budget hooks retain their ordering. Board moves also defer
   before_enter effects: a failing blocking before_enter no longer rejects the
   move; the move commits and its entry then blocks (entry barrier `blocked`,
   owner Retry Entry Checks). Blocked entry retry inserts a fresh hook phase.
6. Each hook checkpoints its index and full outcome before proceeding. Hook
   settlement and any discovered cascade enqueue share one transaction, so a
   crash cannot lose a completed effect's follow-up. A hook that returns
   `Failed` under `on_failure: log` settles its checkpoint and the step `failed`
   (with `last_error`), emits transition.effect_failed and logs; it writes no
   Task annotation and does not block, as before. `run_merge` is the exception,
   whatever its policy: a failure caused by a transient error (the step queue's
   retry classification: database contention, version conflicts, daemon
   unavailable/not ready/timeout, rate limits) leaves its checkpoint open and
   retries the step with the cascade back-off (1 s doubling to 64 s, eight
   attempts in all). Once that budget is spent, or on any other error, the step
   settles failed and the Task gets a blocking `workspace_error` merge-failure
   annotation, which surfaces in Attention; the owner's Retry
   (`merge_gate_retry`) re-enters the merge state and re-runs the merge. A
   `block`-policy failure keeps its blocking compensation and annotation, and a
   thrown hook error settles through the committed-hook failure path with
   cascade_failed. Owner Advance/Restart offers are unchanged.

The task_step outbox is a leased FIFO per Task. Only the earliest unfinished
row is eligible. Eight fast slots and four long slots preserve capacity across
Projects. A hook step is long only if its hooks contain run_merge or
run_ci_steps, including exit hooks. Dispatch, provisioning and before-work hooks
are fast. Repository/workspace integration locks remain; a per-target integration
queue is refactor 3.2. Unique owner tokens and renewable 60-second leases fence
checkpoint/effect writes. A 250 ms poll backs up commit notifications. Shutdown
drains for eight seconds within the supervisor's ten-second grace; otherwise
unfinished leases expire and restart resumes their checkpoints.

Hooks use a shared immutable definition of their committed order/config and
Project authority, rather than repeating complete workflow definitions in rows.
Completed hooks are skipped on resume. Dispatch records its execution association
in the same transaction as admission and reuses that execution even if it has
already finished. Provisioning retains its existing placement/location/input
checkpoints. Merge checks that the target contains the exact candidate before
integrating again; merge/rebase outcomes and the recorded rebase target are
checkpointed. A rebase still in progress in the worktree (a crash inside `git
rebase`) is resumed before any ancestry check, exactly as a fresh rebase ends:
with conflict handoff the stopped rebase is continued, committing markers, and
handed to the coder with every marker path; without handoff it is aborted. The
coder never receives a worktree mid-rebase. Committed rebase/conflict Git facts
reconstruct an interrupted handoff that had already finished the rebase. A merge
found already landed records the candidate commit (the exact object reviewed
integration fast-forwards to) as its result and in the merge comment, never the
target's later tip. Review-authority carry records its hook identity with the Review/carry
settlement so replay returns the same successful cascade. Conflict-handoff actor
and marker/path JSON remain unchanged for the hot-spot detector.

CI and user-defined before-work scripts run **at least once**. A started script
without a recorded result restarts from the beginning after interruption;
completed before-work sub-scripts are skipped. The attempt saves its commands and
environment before running them, so later Project settings cannot rebind script
checkpoint indexes. These scripts already run on
entries to planning, in_progress, review and merge_failed: retrying or sending a
Task back runs them again, so they already must be repeatable and crash resumption
adds no new requirement. A resumed script's hook log includes
rerun_after_interruption: true and its step_id; resumed CI command entries carry
the same evidence. CI may restart the entire check sequence after a crash.

All runtime writes to an existing Task's workflow state execute through a claimed
Task step. Owner commands, claims, execution settlement, dispatcher effects,
recovery, assignment and coordination use the same worker path. Content-only
updates and same-column board order retain their version CAS. Creation initializes
unpublished rows in its writer transaction; effects on an existing parent are
queued separately. The entry status/epoch fence runs at step claim. Checkpoints
and effects retain the lease-owner fence; the former per-hook epoch rereads and
Task-state CAS retry loops are removed.

Callers that need a result enqueue a command and drive the same worker inline.
A registered notification, checked before waiting, bounds predecessor wait to five
seconds without polling. Once that command claims the lease, its result describes
its own committed Task. A timeout returns HTTP 409 `task_busy` with `pending_steps`,
`retry_after_ms` and a retry hint; the accepted command remains durable. Hooks and
cascades remain asynchronous, and responses retain `pending_steps`. Claim and its
execution startup share one command; dispatch continues if the HTTP waiter expires.
Cross-Task wakes enqueue without waiting for another Task lane, and explicit initial
role assignments publish in the new Task's birth transaction.

Cancel/Hold preempts scripts, CI and startup at a safe point. Integration is
protected through its result and terminal cascade; a landed merge wins and Cancel
returns the done Task. Remote commands receive `workspace.cancel`; acknowledgment
wait is at most ten seconds. Without confirmation the hook is superseded with
`remote_operation_unconfirmed`, while `pending_remote_cancel` durably excludes
that workspace from new steps and executions. Owner reconnect retries cancellation
and clears the exclusion on `killed`, `already_finished` or `unknown`. Restart/Retry
uses the existing queued recovery and blocking annotation until cleanup confirms.
Late results cannot write through a superseded step's owner token. Transport
loss alone does not abort a daemon command. Operations reports the pending count,
and Task workspace detail names the exclusion.

The step table replaces the in-memory exclusions. While the current entry's
`hooks` row (matched on status and status_epoch) is pending or claimed, its entry
checks have not settled: `awaiting_human` stays false and owner offers are
Cancel only, even when the latest Review is a stale one from an earlier entry.
Workspace reset returns 409 while the Task has any pending or claimed step,
because those steps hold the worktree and Workspace they captured. Paused
integration resume does nothing while the Task has a pending or claimed step;
the queued attempt's success consumes the marker.

On every start, before the step worker and dispatcher, a recovery sweep enqueues
the hooks of a Task's current entry when the Task is in a state whose on_enter
runs `run_merge`, has no pending or claimed step, and its current entry has no
hooks row. That is the shape of a `merging` Task whose inline merge a pre-upgrade
binary lost. The row is keyed `<entry transition_log id>:recovered`, skips the
prior state's exit hooks, and the merge resumes safely because it first checks
whether the target already contains the candidate; repeated sweeps never
duplicate it. Tasks that are paused (manual stop), held for a human, blocked by an
annotation or entry barrier, or carrying a paused-integration marker are skipped.
Other states are not swept: they have no durable completion witness, and a
missing hooks row there is the normal shape of an upgraded Task whose inline
hooks finished, or of a settled row that storage maintenance pruned.

Only cascades consume automatic chain positions and edges. Repeated unchanged-
evidence edges or more than 64 hops park with workflow_loop; fresh Review/rebase
head evidence starts a new segment. Failed steps whose failure blocks retain
cascade_failed or the existing specific blocking annotation. Failed/parked history and operator status
keep the established queue retention and recovery contract.

Initial admission counts imminent available/claimed fast role-entry work until a
real execution holds its slot. A completed status step's remaining lease reserves
nothing; its hook row owns the dispatch reservation. Completed/deferred dispatch
checkpoints, superseded entries, backoff, rows behind another step and long CI/merge
hooks reserve no agent or server capacity. Paused integration retries enqueue and
keep their marker until durable hook success, rather than interpreting a CAS
response as completed integration. The service wrapper bookkeeping runs inside the command step lease. There is no separate producer reservation.

When `run_ci_steps` finalizes a failed Review during a non-user entry (including
an entry-barrier retry), the engine clears the barrier and settles the verdict
through `TaskService::review_failure_target` before any reviewer dispatch or
review-authority carry. This also applies to older workflows whose CI hook uses
log-only failure handling. The normal review budget applies: the remediation
transition records a rejection; exhaustion records `review_budget_exhausted`
instead of leaving an unannotated Review gate. A human approval requirement
parks a passing review, but does not suppress automatic remediation of failed
CI. User-entered reviews retain their existing entry-check and human-decision
behavior.

Active-task recovery also settles a reviewer state's latest Failed Review
after two minutes from its failure write, when the verdict belongs to the
current non-user state entry, the Task has no blocker or blocked entry barrier,
is not awaiting a human, and has no running execution. It claims
the Task version before using that same failure routing; stale snapshots lose
the CAS, and a remediation transition or budget blocker makes repeat scans inert.
Recovery rechecks the
latest Review and running executions after claiming the Task snapshot.
User routing overrides and historical verdicts from earlier entries are excluded.
If failed-review recovery cannot parse details or conformance, it logs a warning
and uses plain review-failure remediation without finding routing. Live reviewer
completion retains strict parsing.

Integration recognizes a candidate already ancestral to the target as successful
before comparing its reviewed base with the target tip, retaining the normal
review-authority and candidate checks. This completes a merge whose terminal
cascade was interrupted even if sibling merges subsequently moved the target.

Terminal execution settlement uses its existing durable claims and CAS authority;
its post-commit hook effects are serialized and resumed by the per-Task queue.
Workflow hook dispatch uses the originating TaskService's provider/outbox dependencies.
Recovery treats a terminal workflow-role result
whose immutable Project revision is missing or superseded as unsettled and
dispatches a replacement under current authority; it never converts the
cascade's intentional no-op into a reconciliation receipt.

**Dispatch failure entering an active state:** when a dispatch hook
(`dispatch_role_agent` / `dispatch_fix_agent` / `dispatch_executor`) fails
`on_enter` of an `active` state and the task has no running execution, the
engine does not leave the task there looking in-flight. It records the
dispatch error on `task.error_annotation` (type `dispatch_failed`) and
cascades the task back to the workflow's initial state, skipping the active
state's exit guards (the `restart` allowance). Both the failed hook
and the rollback transition land in `transition_log`. The task dispatcher
treats a `dispatch_failed` annotation as blocking, so a task whose dispatch
deterministically fails — e.g. governance rejects it because the Project has no
current approved Charter — is parked in the initial state with
a visible reason instead of being rescheduled every tick; the dispatcher also
writes this annotation itself when a scheduling/recovery attempt fails with a
"not runnable" governance error. A later successful dispatch (or a board drag
that defers dispatch) clears the annotation.

Board moves use the same engine through `TaskService::move_task` and its board
persistence seam. A project owns a monotonic `board_revision`, advanced by
its original Task insert/delete and status/position/delete/archive update
triggers. A separate `list_revision` tracks changes to persisted list projection
inputs through narrow null-safe triggers; execution heartbeats, leases, progress,
and log-path writes leave both revisions unchanged. The ETag includes that list
revision, a projection version and the complete request parameters. The task
list reads the revisions, page and batched decorations in one deferred WAL
transaction, so the old list `409 board_snapshot_changed` is unreachable and
removed. `TaskListRead` holds its pooled connection for the request; an in-memory
pool has only one connection. Matching conditional requests read an indexed
Project row and probe a partial index for non-deleted deferred-dispatch metadata,
which disables conditional reads because retry health also changes with the
clock. The index and probe both guard JSON access with `json_valid`, preserving
malformed legacy task metadata. The migration installs the list index/triggers
once; a bundle test checks that all defined list triggers survive later migrations.
The public move command compares both the
task version and board revision after acquiring the SQLite write lock, validates
the destination workflow column and adjacent neighbor IDs, and writes status
plus board position once in a single transaction. Tight numeric gaps are
renormalized inside that transaction, so revisions are monotonic but not
gapless.

Same-column moves use the repository transaction directly and skip status
hooks. Cross-column moves run `before_exit` guards before the write, then insert
and run the same durable `on_exit`, `before_enter`, `on_enter`, `after_enter`,
dispatch and cascade phase as ordinary transitions. The direct persistence step
increments the task version exactly once; a later cascade is a separate normal
transition and can increment it again. Rejected guards write no task, move
operation, or transition log.

`task_move_operation` stores normalized request identity, processing/direct
commit state, and the completed logical result. A same-ID/same-request retry
replays the result; different reuse conflicts. An incomplete record makes the
existing post-commit crash gap detectable, while board/task refetch remains the
recovery source of truth. Each newly committed direct move publishes exactly
one `task.moved` event after commit. Status-changing move events feed lifecycle,
project-hook, notification, and operation-status consumers in place of a second
direct `task.status_changed`; any queued cascade emits its own normal
transition event.

**User routing override:** When a user actor's move would be rejected solely
because (a) no trigger edge connects the states or (b) the matching trigger is
system-only (`Fail`/`Retry`), and `B` is a defined state in the applicable
workflow, the engine completes the transition via a user-routing-override arm
inside `transition_inner`: `before_exit`/`before_enter`
content guards still run and may block; `on_enter`/`after_enter` hooks run
normally; `task.status_changed` is published unconditionally; agent dispatch fires
only when a role/agent is assigned. Override transitions are audited as
`triggered_by = "user:override:<source>"` (e.g. `user:override:api`). This is
separate from `manual_override_transition_with_authority`, a system-triggered
primitive with `skip_before_exit=true` used by
`TaskService::advance_to_next_state`.
An authenticated Project Agent cancellation is narrower: `task.action` with the cancel offer may
cross a system-triggered edge only when the destination is the workflow's
declared cancellation state. It receives no general routing override.

Hook audience filtering is uniform across phases. `HookAudience::All` always
runs. `AgentOnly` runs when `triggered_by` starts with `"agent:"` or equals
`"system"`; `UserOnly` runs only when it starts with `"user:"`. Non-matching
hooks are skipped without a hook-result entry.

Human-triggered transitions are treated as project-management actions. The
dependency gate does not block `user:*` card moves, including board drag
transitions, so users can reorder and reclassify work like they would in Jira.
Users may route a task to any defined workflow state via the override path when
strict routing would reject (see resolution rule above). Any user-initiated
transition that changes the task's state cancels in-flight executions with
`StopReason::UserCancelled`; same-state moves leave running executions
untouched. Parking an agent-assigned task in an Initial- or Backlog-kind state
retains role assignments but does not launch an executor from the move itself
— the task re-enters agent flow only through the normal scheduling path when it
later reaches a dispatchable state. AI execution remains gated separately:
initial role dispatch and interactive launch both run dependency checks before
creating an execution.

Dependency cancellation is a durable state, not a transient guard result. When
a prerequisite reaches the workflow's cancellation state, Forge projects a
typed blocker onto every unfinished dependent. New links to cancelled Tasks are
rejected, and removing the last cancelled prerequisite clears the matching
blocker so scheduling can resume.

Cancellation is implicit from any non-terminal state to
`workflow.cancellation_state` (or terminal `"cancelled"` if unset), even
without an explicit edge. Project `before_exit` guards are bypassed for this
path; `on_exit` and cancellation-state `on_enter` hooks still run.

### Roles and assignments

Roles are declared by workflow (`roles[]`) and states can require a role
(`state.role`). Per-task assignments live in `task_role_assignment` keyed by
`(task_id, role_name)` with either a stable agent identity or user. Claiming
auto-assigns the claimed state's role to the claiming identity when no
assignment exists; a conflicting pre-assignment returns HTTP 409. Replacing or
selecting a new profile therefore does not rewrite Task ownership/history.
Project-level role selections are defaults used to seed those rows; they are
not a per-Task allowlist. An explicit Task role may name any enabled, available,
Project-usable Task execution identity, including the same identity for Worker
and reviewer. Assignment and dispatch both recheck effective availability,
account/Project scope, coordinator exclusion, and the exact Task-scoped lease.
On a coordination root, `coder` is a default rather than an executable root
role: a child without its own `coder` inherits that root row, while an own row
overrides it. Task responses expose the resolved row and whether its source is
`own` or `inherited_from_root`.
Changing or confirming a Task role clears any stale deferred-dispatch
disposition so a previously blocked Task is reconsidered. When the role choice
is newer than a stopped attempt for that role, it is also the explicit retry
signal; the dispatcher starts one fresh attempt without a separate Resume
action, and any newly stopped attempt blocks again.

CLI profiles continue through the existing executor/daemon path. A compatible
native profile enters work through the same claim, assignment, workflow,
Workspace, validation, review, and delivery services; it does not get an
alternate repository-mutation route. Only the admitted Task session derives
the role-bounded Workspace/tools and structurally compacted Task history.
Other simultaneous sessions for that identity retain their own denied
Main/Project Agent Chat workspaces.

Repository claims preflight the selected identity before creating a Task
branch or worktree: an active Main or Project Agent identity is rejected even
if it also has a Task assignment. Forge admits at most one running
repository-capable execution for a Task, including retries and interactive
follow-ups, and rejects a second attempt before changing Task state. If a
process stops after creating the deterministic Task branch but before the
worktree record is committed, the next valid claim recovers that branch into a
new worktree instead of failing or creating an alternate branch.

`assignee` is an engine-reserved role name. Active states without explicit
`state.role` implicitly bind `assignee`. This fallback applies only to Active
states; Gate, Initial, Backlog, Terminal, and Custom states without roles bind
no role. `state.role = Some("assignee")` on a non-Active state is rejected
during validation. `DefaultWorkflow` is unchanged and uses declared `planner`,
`coder`, and `reviewer` roles.

### Retry budgets

Audit-log derived. Gate states may set `gate_config.max_rejections`;
`check_retry_budget` counts `transition_log` rows with `from_state = gate` and
`rejection = true`, then cascades to `blocked` when exhausted. Generic
user-triggered gate-to-active bounces are logged with `rejection = false` and
do not consume budget. Both `retry { reset_budget: true }` and the stronger
`restart` recovery action establish a new audit boundary, so a full
Task reset restores every gate's retry window rather than carrying an exhausted
merge/review budget back to `todo`.

Review-CI infrastructure retry applies only to a typed unreachable owner or an
RPC timeout before a CI command was sent. It spends no review rejection budget
and creates no Review row if CI never ran. Barrier, annotation, persisted
attempt count, and deferral commit atomically under Task/Project authority.
Automatic retries back off from five seconds exponentially, stop after five
connected-owner failures, and park with a blocker and `execution_failed` Attention
whose details name the cause. Disconnected placements wait on the same owner
without spending that cap; reconnect re-arms an exhausted barrier atomically
with readiness and resolves its Attention. Successful
retry clears the barrier, annotation, and review-CI Attention; reset and cancel
also resolve that Attention, and re-opening clears acknowledgement and snooze
state. Command timeout, repository mismatch, and not-ready/failed/cleaned
workspaces are permanent runner refusals, not infrastructure retries. Unbounded
daemon CI accepts truncated output while preserving its exit-code verdict;
output pressure does not make a successful command fail. Reset-required workspaces offer
`restart`. Embedded authority-loss cancellation and user review-entry
behavior retain their existing routing; genuine CI failures charge a rejection.

When a reviewer execution fails, Forge first schedules the next bounded execution
retry and keeps the Review running. Once retries are exhausted or disabled it
records the same durable task blocker and recovery annotation used for worker
execution failures. Startup recovery, daemon disconnect, heartbeat expiry, and
workspace-lease reaping all enter this settlement path. The periodic dispatcher
also reconciles a terminal reviewer execution when its completion event was lost,
including the narrow crash window where the Review row was finalized before the
Task cascade committed. Reconciliation is exact-attempt-lineage-only: a
reviewer or auditor execution may settle only the Review row whose explicit
role-specific execution binding matches it, with the exact direct
`Review.execution_id` relation retained only for legacy rows. A shared
candidate parent is not an attempt identity, and wall-clock ordering is never
used to infer lineage. An unbound legacy execution cannot settle a newer
Review and remains available only for explicit retry/recovery. The dispatcher
admits a fresh reviewer when
the current attempt is not exactly bound. Forge allocates the next Review
attempt and inserts its Running reviewer execution plus owner lease in one
`BEGIN IMMEDIATE` transaction. The Review row points at the candidate
implementation execution, while the reviewer and auditor children point at
that same candidate. Task/Project/workflow, role assignment, selected Agent
identity/version, and the latest Review snapshot are rechecked at that insert
boundary; reviewer capacity counts running executions and workspace reservations
plus the machine's effective run cap. The candidate must be a
completed implementation execution at that insert boundary. Failed or cancelled
remediation executions do not displace the last completed candidate, but a
newer running implementation still fences review admission. If Task/Project/workflow
authority changes after reservation, Forge terminalizes the reviewer execution
and uses an exact Review status/timestamp CAS to cancel the still-running
Review, so no stale attempt or lease remains apparent. Saturation cannot leave
the previous terminal attempt looking current or an orphan Running execution
behind. A historical passed Review
row also cannot auto-cascade unless the Task still carries current
`review_passed_at` authority. The Task remains in review with explicit recovery
actions; an execution failure does not count as a reviewer verdict rejecting the
work.
`retry { fresh_session: false }` is exposed only when the stopped execution has a session/config
snapshot and its agent still owns the exact Task role. For a daemon placement,
the owner must also be online and advertise `resume` for that executor.

### Crash recovery

Explicit Task recovery whose only refusal is Agent capacity is accepted into
the durable dispatch queue. Clearing its interruption and recording its action,
reason, and context share a Task-version CAS and a no-running-execution check.
The dispatcher retries that intent before ordinary scheduling once capacity
is available, revalidating the current workflow and execution admission. The
replay carries its queue ID through execution admission, and only its own
Running execution INSERT consumes the queued intent in the same transaction,
so restart cannot replay an already admitted recovery. While queued, the Task
reports `Retry Queued` through the existing deferred-dispatch health projection.
Repeated recovery requests return that queued Task without replacing the
intent. Resume fallback launches share this queue and replay the Resume path.
Only capacity refusals remain queued: permanent pre-claim/replay failures,
including paused/offline Agents or an unrelated execution taking the slot,
atomically remove the intent and restore its saved interruption with the error
as its reason. Task-version and queue-ID checks protect newer decisions.

`CrashRecovery` runs at server and Solo startup and deterministically reconciles
ownerless or expired running executions left by an earlier process. Migration
`V089` does not invent ownership for pre-existing rows: a running row without
verifiable lease ownership is immediately eligible for this recovery pass.
Startup suspends placed remote executions before recovering local grants.
`HeartbeatMonitor` likewise suspends placed remote heartbeat expiry and uses
the terminal CAS for embedded owner-lease expiry or a reached hard deadline.
A separate semantic-progress scan emits warnings without terminalizing a live
owner. Hard deadlines are recovered distinctly from ordinary owner-lease expiry.

Startup crash recovery marks every interrupted implementation execution whose
terminal CAS it wins with `resume_policy = auto`, regardless of its active
workflow state or whether the attempt has a reusable runtime session. It leaves
the Task in its current state without a manual `recovery_required` annotation so
the normal dispatcher can resume or freshly re-execute it. Heartbeat/owner-timeout
recovery remains a live failure decision and may publish the manual recovery
annotation appropriate to that stop reason. In either path, the winning CAS also
closes the execution's active `WorkspaceLease`; a stale monitor or runner cannot
annotate, cancel, or cascade a second time. Tasks whose assignee is a user are
excluded from crash-recovery selection — agent-oriented recovery is not meaningful
for human-driven tasks.

After the orphan pass, startup runs a sweep that clears stale
`recovery_required` annotations when `blocked_execution_id` is missing, refers
to a nonexistent execution, or refers to an execution that is not in a stopped
state awaiting user recovery. The sweep is idempotent and only ever clears
annotations.

### Failure classification

Interruption kinds are a closed vocabulary: `FailureKind` in `api-types`
(serialized snake_case, TS-exported). It is the only classification signal —
`InterruptionMetadata.kind`, `TaskBlockingAnnotation.type`, and the
`task.blocked`/`task.failed` event payloads all carry it, producers
(`block_task`, `fail_task`, annotation writers) take the enum rather than
strings, and recovery/exception derivation branches exclusively on its
predicates (`is_retry_exhausted_metadata`, `is_budget_exhausted_annotation`,
`is_merge_recoverable`, …). Reason/message prose carries no classification
weight anywhere. Legacy database rows were normalized once by migration
`V056__normalize_failure_kinds`; kinds that migration could not map
deserialize to an `Unknown` variant. Legacy/unknown annotations retain reset and re-execution offers where a valid apply plan exists. `dispatch_failed` has its own typed kind. Producers must never construct `Unknown`. The web client
likewise derives no failure semantics from workflow state names — gate
reject/bounce targets come only from explicit `reject`/`fail` trigger edges or
`gate_config.reject_target`.

Human-facing notifications are task-outcome driven, not per-attempt:
`task.failed` when `fail_task` sets `failed_json` (which also clears any stale
blocking annotation), and `task.recovery_required` when crash recovery or an
agent heartbeat timeout annotates a task for manual recovery. A raw
`execution.failed` or `execution.cancelled` event is audit-only (and may
resolve a `progress_warning`); it does not notify or wake by itself.
Graceful-shutdown recoveries auto-resume at the next startup and are not
notified. In the derived `workflow_exception` summary, a hard failure
supersedes any blocking annotation — `recover_task` only accepts
`restart`/`cancel` once `failed_json` is set, so only those
actions are offered. The web UI renders one actionable recovery surface,
`WorkflowExceptionPanel`, on both the task page and the board modal;
`TaskBlockingBanner` is an informational fallback for interruption states
without recovery actions. Generic execution follow-ups remain non-propagating
side sessions: they can preserve an agent conversation, but they cannot settle
a Review or advance Task state and are not shown beside an active workflow
exception. Review guidance is submitted through the exception's offered `retry {guidance}`
action so the replacement reviewer execution is bound to the authoritative
Review attempt and may propagate its result.

`transition_log` is the audit source of truth for state changes. The API
exposes it via `GET /api/v1/tasks/{id}/transitions`.

### Files of interest

- `crates/services/src/task_hierarchy.rs` — root-Task and ordered-subtask policy
- `crates/services/src/workflow/engine/mod.rs` — lifecycle
- `crates/services/src/workflow/actions/` — curated hook actions
- `crates/services/src/workflow/default_workflow.rs` — built-in graph
- `crates/services/src/workflow/validation.rs` — workflow graph validation
- `crates/services/src/workflow/cache.rs` — per-project resolved definitions
- `crates/services/src/workflow/registry.rs` — action name resolution
- `crates/db/migrations/V009__workflow_engine.sql` — `project.workflow_definition`,
  `task_role_assignment`, `transition_log`

## Happy path

The canonical end-to-end flow is captured by `crates/api/tests/happy_path.rs`.
It boots the in-process Axum router with an embedded daemon and a real temp
git repo, drives a task through `todo → in_progress → review → merging → done`,
and asserts:

- The merge SHA lands on the default branch.
- The worktree is removed.
- One `review` row with `status=passed` is persisted.
- The expected event sequence appears on the bus.

Any refactor that breaks this test likely needs a spec realignment before
merging. Claiming a task auto-dispatches the executor via `tokio::spawn` in
`api::routes::tasks::claim_task` — there is no separate "dispatch" endpoint.

## Concurrency control

Tasks and agents use optimistic concurrency via a `version` column. Updates
require `WHERE version = ?` and increment on success. Version mismatch →
`DbError::VersionConflict` → HTTP 409.

## Database

SQLite with WAL mode. Schema in
`crates/db/migrations/V001__initial_schema.sql`. All primary keys are
app-generated UUID v4; all timestamps are app-generated RFC3339.

Migrations are files named `V{version}__{name}.sql`, embedded in the binary
and recorded by version and name in the `_migration` table. Versions up to
V149 are sequential. Later migrations use their UTC creation time
(`VYYYYMMDDHHMM`), so branches written in parallel cannot claim the same
version. The runner applies every missing version in ascending order, so a
branch that merges after a newer-stamped migration still runs. It refuses to
start, before applying anything, in two cases:

- two bundled files share a version;
- the database already applied a different migration under a version this
  build uses.

Without that check, the second migration would be skipped without an error and
its schema would never exist. The one known reconciled exception is a V053
`integration_credentials` row from early hosts, which V054 repairs.

To recover a refused host, compare `SELECT version, name FROM _migration` with
`crates/db/migrations/`. Apply the build's migration by hand if its schema is
missing, then update that row's `name` to the build's file name.

Connection pool sets `PRAGMA foreign_keys=ON`, `journal_mode=WAL`,
`busy_timeout=5000` per connection.

The revised schema adds `account_main_agent_binding`,
`project_agent_binding`, immutable `project_admission_receipt`, `agent_chat`,
immutable `agent_chat_message`, bounded `agent_chat_turn_job`, and immutable
`agent_handoff` records to the existing
identity/profile, session, LCM, memory, commitment, event, Attention, Task,
execution, review, and terminal tables. It enforces one active Main binding per
account, one active Project binding per operational Project, one global chat per
account, and one Project chat per Project. Historical collaboration tables are
migration inputs rather than public product concepts.

Migrations V059–V070 remain immutable history. V071 or later performs the
forward-only correction: it creates the singular binding/chat records and
migrates legacy Conversation and pre-release collaboration messages,
metadata, instruction provenance, sessions, LCM/memory references, protected
content audit links, and turn jobs without changing message IDs or bodies.
When multiple source threads map to one chat, ordering is deterministic by
original timestamp, source ID, and source sequence, with source provenance
preserved. A binding is inferred only from one safe eligible responder;
ambiguous or invalid cases become explicit `agent_setup_required` state, and a
primary Worker is never promoted. Expired or ambiguous leases become finite
retry/terminal states rather than remaining silently leased. Historical
migration files are never edited. V075 quarantines the retired Room and
Project-agent-membership tables under `legacy_*`, remaps Room-scoped semantic
memory to the owning Agent Chat, and adds database guards that reject new Room
context, LCM, memory-binding, or manifest authority. Historical source IDs and
sequences remain available only as provenance.

V117 backfills one Project-owned admission receipt from the immutable Genesis
handoff or consumed adoption approval, links safely inferable bindings to it,
and enforces immutable/same-Project/current-Charter guards. Startup
reconciliation replaces the known inferable incomplete binding shape while
preserving its selected identity/settings and frozen turns. Ambiguous records
are moved to `agent_setup_required` with a durable repair-required event rather
than fabricating authority.

The Charter, Project artifact, milestone, release, and shared-media metadata
for this change are added by the forward-only
`V076__project_charter_milestones_media.sql` migration. It leaves V001–V075
immutable, preserves existing media identifiers/storage keys/file bytes, and
does not move or duplicate files. Any later migration must be independently
numbered and is outside this change's contract.

The execution liveness contract is added by the forward-only
`V089__execution_liveness.sql` migration. It preserves execution rows and
copies legacy `last_activity_at` into semantic `last_progress_at` only;
terminal rows remain ownerless, and pre-existing running rows receive no
fabricated owner or heartbeat and are eligible for deterministic recovery.
Lease/version CAS, progress-warning dedupe, and terminal event/WorkspaceLease
disposition atomicity are repository guarantees layered on these fields.

Migration `V136__project_owner_memberships.sql` backfills the missing
`owner` membership for every owned Project whose owner account exists. It is
idempotent and preserves existing memberships, so legacy Projects converge on
the same owner authorization invariant as new direct REST, MCP, and Genesis
creation. The subsequent V137 operating-skill migration activates
`forge.project.orchestration/v1@16`; its task-coordination doctrine makes the
`parent_task_id` shared-root hierarchy/workspace relation explicit and keeps
`depends_on_ids` as prerequisite DAG edges that never imply workspace sharing.

Migration `V138__project_owned_task_repository.sql` removes the nullable
`task.repo_id` column and its index without rewriting Task rows or historical
execution records. Workspace-lease guards instead require the Project's current
primary Repo to exist in that Project and to match both the new lease binding
and execution Workspace. Workspace, lease, review, evidence, release, and extra
unselected Repo rows retain their repository identities;
removing Project repository setup no longer deletes Tasks through a Repo
foreign-key cascade.

For tests, use `create_sqlite_pool("sqlite::memory:")` for an in-memory
database.

## Frontend

React + TypeScript + Vite + TanStack Query/Router. Source in `web/src/`. Uses
`@` path alias → `web/src/`. API client at `web/src/api/client.ts` calls
`/api/v1/*` endpoints. Types in `web/src/types/generated/api.ts` must match
`api-types` crate responses.

## Crate notes

- **db** — Enum serialization uses `Display`/`FromStr` (in `models.rs`) for
  SQLite TEXT columns. Row mapping is manual via `sqlx::Row::get()`, not
  compile-time checked macros.
- **agent-host** — Direct Agent Runtime composition, protected credentials and
  checkpoints, capability-aware native/CLI Agent Chat backends, content guards,
  and scope-derived workspace adapters.
- **services** — `TaskService.transition()` handles side effects (event
  emission, counter increments, workflow entry hooks on `→ review`,
  `MergeService` on `review → merging`, `WorkspaceCleanupScheduler` on `→ done` /
  `→ cancelled`). Background tasks: `CrashRecovery` at startup (orphan
  execution recovery and stale-annotation sweep), `HeartbeatMonitor` (owner
  lease/deadline expiry plus separate semantic-progress warnings),
  `DaemonMonitor`, Agent Chat turn workers, durable event consumers, Attention
  projection, and `WorkspaceCleanupScheduler`.
- **review** — the workflow's `run_ci_steps` hook prepares the Workspace and
  runs configured checks before ordinary reviewer dispatch. `ReviewRunner`
  owns explicit reviewer/auditor reruns. Task configuration
  overrides the Project's `default_review_config`; otherwise the Project
  defaults are inherited. A ready Project Agent can replace both
  default lists through the versioned, receipt-atomic `project.review_config`
  command and reads them from `project.current_state`. Discovery, planning, and
  explicitly read-only Tasks suppress both implementation lists. Empty steps
  auto-pass; setup failure stops checks and is retained separately. Creates the
  Review attempt, reviewer execution, and owner lease atomically at the
  admission boundary; the Review points to the candidate implementation
  execution and reviewer/auditor children share that candidate lineage. The
  `reviewer`-role execution shares the executor's workspace, with the same owner heartbeat,
  semantic-progress, hard-deadline, and terminal-CAS contract as Task
  execution. Depends only on `db`, `events`, `executors` — not on `api` or
  `services`.
- **api** — Routes include projects, Tasks, Main/Project Agent bindings and
  chats, embedded agents/sessions, memory/context, commitments/actions, Mission Control,
  terminals, repos, executions, events, daemons, CLIs, and executor types.
  Error module is `errors.rs` (plural). Middleware adds request IDs and CORS.
  `claim_task` auto-dispatches the executor.
- **executors** — `LogWriter` appends JSONL with schema version + sequence
  numbers. `ShellExecutor` spawns child processes with heartbeat supervision.

### Executor fallback chains

Both execution paths (embedded `AppState.task_executor` and the remote
daemon runtime) dispatch through `FallbackExecutor`, which walks an ordered
candidate route instead of a single adapter:

- **Authoring** — an agent's `config_json` may carry
  `fallbacks: [{executor_type, config}]`. The snapshot builder extracts it
  *before* typed-config normalization (which drops unknown fields),
  normalizes each candidate under its own `ExecutorKind`, and writes a
  first-class `routing` block
  (`{policy: "ordered_fallback_v1", candidates, selected_candidate_key,
  attempts}`) on the execution snapshot. Snapshots without `routing` behave
  exactly as single-candidate executions.
- **Fallback trigger** — only structured availability errors advance the
  chain: `ExecutorError::UsageExhausted { retry_after, usage }` and
  `ExecutorError::Unavailable`, plus a failed per-candidate availability
  precheck (`check_candidate_availability`, defaulting to the family-level
  check). Real task failures terminate the chain immediately. Adapters
  classify only structured signals (Smith stream events / result statuses,
  Codex protocol errors, Claude Code stderr and `is_error` result events,
  and Gemini stderr/error documents). Usage/rate-limit and HTTP 429 forms
  become `UsageExhausted`, with reset hints parsed from structured fields,
  relative delays, epoch timestamps, RFC3339 timestamps, and CLI clock/date
  messages (clock-only messages use the executor host's local time unless
  they explicitly say UTC; Codex dates may precede or follow the clock).
  Numeric 429 signals require an HTTP/status context or an explicit error
  `status`/`code` field; usage counters and costs are excluded. Codex stderr
  diagnostics and errors marked `willRetry: true` do not trigger fallback.
  Gemini capacity classification applies only after a failed process exit;
  a clean exit remains successful even if the stream contained an error.
  Assistant output text is
  never an input, and unclassifiable failures stay generic (no fallback).
- **Cooldowns** — an in-memory, process-lifetime registry keyed by
  `AccountKey` (the quota pool: Smith's resolved provider, Codex's profile,
  else the executor family). Exhausted accounts are skipped until
  `retry_after` (default 15 min); all candidates cooling fails fast without
  spawning. Candidate identity (`CandidateKey`) is separate: kind +
  discriminators + a stable hash of the session-stripped config.
- **Terminal disposition** — the chain reports `Ok(ExecutionResult)` with
  `failure_class` (`TaskFailed` | `ExecutorUnavailable`), `retry_after`,
  `resolved_candidate`, and `route_attempts`. The daemon protocol carries
  the same fields additively on `ExecutionTerminalNotification`
  (`failure_class`, `retry_at`, `resolved_candidate`, `route_attempts`);
  notifications without them degrade to generic executor-failed handling.
  The service layer maps `ExecutorUnavailable` to
  `FailureKind::ExecutorUnavailable` from these fields only — never prose.
- **Availability recovery** — transient `executor_unavailable` failures use
  the existing finite execution retry budget. Capacity exhaustion schedules
  a deferred dispatch at the structured `retry_at` plus deterministic jitter,
  bounded by exponential backoff and a maximum six-hour wait. Without a reset
  hint, CLI account cooldown defaults to 15 minutes; absent or malformed
  terminal hints use execution backoff. Each deferred attempt consumes one
  retry, and duplicate terminal delivery does not consume another. Exhaustion
  blocks with explicit recovery actions. A zero execution retry budget blocks
  with a disabled-retries message; an exhausted budget is identified separately.
  Stale project versions cannot settle the Task or schedule retries.
  Workflow health shows `Retry Scheduled`
  or `Retry Queued` with the capacity/usage-limit reason while waiting;
  permanent unavailability (auth/install failure everywhere) blocks the task
  for manual reconfiguration with no automatic redispatch.
- **Sticky selection and resume** — the winner's resolved config is written
  back to the execution snapshot (top-level `executor_type`/`config`, plus
  `routing.selected_candidate_key` and per-candidate `attempts` for
  provenance). Follow-ups resume via candidate identity: the parent's
  winning candidate is promoted to the front of a fresh route only when the
  exact `CandidateKey` is still present; any candidate switch starts a fresh
  session (`resume_session_id` never crosses accounts — executor-family
  equality is not sufficient).
- **mcp-server** — JSON-RPC dispatch over `POST /mcp` with its own `McpState`.
  Does not depend on the `api` crate.
- **workspace** — `.forge.lock` records task-worktree lock state; keyed in-process
  locks serialize repository-cache/integration and Workspace execution operations.
  Path validation prevents traversal escapes.
- **config** — `ForgeConfig` with precedence: CLI flags > env vars > config
  file > defaults. Default bind uses loopback with an OS-selected port, then
  persists the selected port under the Forge data directory.

### Charter conformance at review

The shared `review::contract` resolver reads the exact approved Project-owned
Charter, its digest, Task governance reference, Task acceptance/plan, linked
Document revisions, workflow, review assignment, and effective check policy.
Missing, mismatched, or oversized governing context fails admission visibly.
Workers, merge-fix workers, previews, and reviewer dispatch receive this context
outside configurable prompt overrides. Native and CLI launches use the same final
assembly. A model reviewer receives the response instruction and frozen contract
exactly once, and admission rejects a final prompt above 192 KiB. Shell commands
receive JSON as safely quoted `FORGE_GOVERNING_CONTEXT` and
`FORGE_REVIEW_CONTRACT` exports, so context prose is never executed as shell.
Review admission reads candidate Git objects through the placement owner and
refuses unavailable or unverifiable evidence. The versioned source digest is
frozen with the contract and recomputed through the same backend at settlement.

Review reruns, the `run_ci_steps` entry hook, and reviewer-completion conformance
checks call `task_service::workspace::prepare_workspace` before using the Task
checkout. They share executor dispatch's repository-authority checks and
missing/unusable worktree recovery. A stale Ready workspace row with a surviving
Task branch is rebuilt in place with the same workspace identity; a missing
branch retains the existing explicit reset-required failure rather than
reviewing a new candidate silently. Subtasks continue to use the root-owned
shared workspace.

Before an agent review launches, Forge freezes an execution-specific contract
with Task-scoped requirement IDs, checks, completed pre-review CI results,
candidate commit, target commit, and the exact repository-relative path manifest
for `base_sha..commit_sha`. A result includes its `ci:N` check ID,
exact command, exit code, and bounded output, so the reviewer can use the
already-recorded outcome as check evidence instead of inferring success from a
configured command. Missing required pre-review CI results fail admission.
Migration V132 adds immutable contracts/assessments without rewriting historical
outcomes; V145 rewrites stored assessments into the `{result, reason, report}`
shape, keeping each original assessment JSON as a fenced block in `report`. Every Task review includes `task:acceptance`, acceptance material from
its linked Documents, and the Charter's explicit non-goals/non-claims as
universal Project boundaries. Other Charter requirements enter the Task scope
only when `ReviewConfig.requirement_ids`, a
requirement-linked conformance check, or an authoritative allocation selects them.
Every Project Agent/REST Task proposal must make that ownership decision
explicitly through `review_requirement_ids`, including `[]` for no owned
non-universal requirement. A versioned Task update may replace the list with the
same current-Charter validation and invalidates any prior review acceptance.
The contract records the count and digest of the other Project requirements as
deferred. Those requirements remain integrated milestone-readiness obligations;
an early Task is never failed merely because later Project work does not exist yet.

Universal Project boundaries cannot be allocated away. An empty candidate is
reviewed as the current Task outcome, never as evidence that pre-existing content
was introduced by that Task.

The reviewer answers in free Markdown and ends with one small result block,
`{"result": "pass|fail|blocked", "reason": "...", "fixable_by": "coder|owner", "repeat": false}`.
`fixable_by` and `repeat` are optional, defaulting to `coder` and `false`;
unknown values use those defaults. Owner-only findings need authority or
resources outside the coder's Task; repeats identify a finding left unaddressed
from the previous attempt. The response format is kept
this small on purpose so any model can review: an earlier contract demanded one
JSON object with per-requirement dispositions and exact file/commit citations,
and live reviewers lost whole reviews to an invented extra field or a truncated
commit SHA. `review::contract::parse_assessment` takes the last JSON object in
the reply that names a result, ignores unknown keys, accepts `verdict` for
`result`, and keeps everything else as the Markdown `report`. The hard gate is
Forge's own setup steps and required checks, not the reviewer's citations: a
failing check fails the review whatever the reviewer said, and a `pass` on a
Task with no configured checks rests on the reviewer's judgment alone.

The server contract tells the reviewer to verify by exercising the change —
build it, run its tests, and drive the changed behavior programmatically or
visually — rather than by reading code, and to stop once it can answer three
questions: does the change work, is the changed behavior tested, and does the
delta stay inside the Task (unrelated refactors, churn, or new dependencies are
a blocking finding). To save the reviewer's opening tool calls,
`review::contract::prepare_prompt` appends the candidate diff (`git diff --stat`
plus the full diff when it fits in 48 KiB) and, on a re-review, the latest
earlier verdict with its reason, a bounded slice of its report, and the diff
since the commit it judged when that commit is still an ancestor. These
sections are best effort and stay inside the prepared-prompt byte limit.

Results map to conformance and routing as follows. `pass` becomes `passed`
(merging, or `awaiting_human` behind a human gate). `fail` becomes `failed` and
follows the review-remediation budget, with the reason and Markdown review as the
coder's feedback. `blocked` — the environment, not the code, stopped the
reviewer — becomes `blocked`: the Review finishes failed and the Task is parked
with a `review_blocked` blocking annotation for its owner, because neither the
coder nor another reviewer can install a missing toolchain. The owner can retry
the authoritative reviewer with optional guidance (`retry` with `guidance`),
or manually pass with a required reason. A manual pass appends a new passed
Review attempt with user provenance; it never rewrites the failed attempt.
A reply with no readable result block, or a review whose context or commit
changed underneath it, is `unverified`: it uses the bounded reviewer
execution-retry path, eventually creates a durable execution blocker, and never
dispatches a coder.

Before coder remediation, a reviewer `fail` tagged `fixable_by: owner`, or a
`repeat: true` failure after the immediately preceding attempt recorded a
reviewer `fail` assessment, parks the Task
with `FailureKind::ReviewNeedsOwner`. The annotation message records
`fixable by owner` or `repeated finding` plus the reason. Forge does not dispatch
the coder or consume review retry budget. A first-attempt repeat does not park,
and failures from Forge's own checks retain their normal routing. Reviewer
crashes, unverified results, and CI-only failures are not prior findings. The dispatcher
uses this same routing for stranded failed Reviews after the base's recovery
grace and authority fences; failed non-user review-entry CI still consumes the
review retry budget and records exhaustion when appropriate. Owner recovery
offers include `retry` with guidance, `approve`, and `cancel`; side-session
launches remain separate. For an owner finding, `approve { override: false }`
creates a linked backlog Task carrying the finding and records a manual pass
naming it, allowing the original to merge. `approve { override: true }` passes
the review without creating that follow-up. The owner decision is recorded in
the manual pass; the closed approval command has no separate reason field.
The follow-up uses normal service insertion, including current-Charter governance
and a Project work-epoch increment, in that same transaction.

Before an embedded reviewer run completes, Forge parses its reply with the same
parser; a reply with no readable result block gets up to two short follow-up
turns in the same run asking for the block alone. Each correction is appended to
the reply, so the first turn's Markdown review survives, and the whole reply is
logged as the run's final assistant message. The corrections are part of the one
execution and do not spend the reviewer retry budget; CLI reviewers do not get
them yet. Each native reviewer attempt starts with an empty conversation and no
Task checkpoint persistence, so a retry cannot accumulate the previous full
contract and report. Worker and planner Task continuity remains persistent,
with deterministic structural compaction instead of an LCM timeline.

Configured `setup_steps` run first in the Task's own worktree, verified to sit
at the frozen candidate commit with no tracked change, followed by required
checks. Only tracked content is delivered, so the worktree's ignored dependency
and build output is reused instead of a fresh clone repeating every install and
cold build; after the checks the worktree is reset to the candidate commit and
untracked, non-ignored output is removed so integration sees a clean tree. Each
command runs with bounded output and a
timeout (`check_timeout_seconds` in the review config, 1–14,400 seconds, default
30 minutes; an unset value is not written into the frozen contract). A command
that outruns the limit is Forge's own verification failing, not the reviewer:
the result is not recorded, the reviewer completion re-runs only the checks (up
to three attempts in all), and if they still time out the Task is parked with a
`review_blocked` annotation instead of dispatching another reviewer. Setup prepares dependencies but never satisfies a requirement;
failure is recorded separately and stops the checks. Forge records actual exit
codes independently of model output and reruns required checks before accepting
the assessment, even when the reviewer cited a frozen pre-review result. If
setup or the checks modify tracked files (or move HEAD) at the candidate commit,
the candidate does not reproduce from its own commit — a stale lockfile is the
usual cause — so the review fails and the coder is told which files changed.
Tracked changes in the reviewer's own worktree and stale Charter/Task/check
inputs make conformance unverified while retaining the parsed review.
An owner-transport or placement-infrastructure failure leaves the assessment
unrecorded, so reconnect recovery can evaluate it again against the same frozen
contract instead of freezing an environmental failure as review authority.
Natural-language Charter text never becomes an executable command;
product-specific deterministic checks must be configured explicitly.

The frozen prompt context includes bounded worklog comments and active attached
media metadata, giving read-only discovery Tasks a reviewable deliverable sink.
Such Tasks own no implementation requirement IDs. Execution setup counts an
implementation commit only when a non-reviewer execution changes `before_sha` to
a different `after_sha`; the repository's unchanged base commit is not evidence
of implementation.

New contracts store `context.source_digest_version: 2` in their existing JSON.
The v2 source digest fingerprints only review authority: the Charter revision ID
and content digest plus the Task's Charter reference; Task title, description
(including acceptance criteria), and plan; effective requirement IDs and
allocations; linked Document revision IDs and content digests; effective
`setup_steps`, `ci_steps`, `check_timeout_seconds` (30 minutes when unset), and
requirement-linked `conformance_checks`; the review state's workflow config and
Task overrides; and the Task's read-only review mode. The plan is authority
because the resolver includes it in the universal `task:acceptance` requirement.
Project environment variables are injected into review commands, so their names
are fingerprinted, never their values. Whole Project settings/workflow blobs,
reviewer assignments, worklog/media evidence, and other audit metadata do not
enter the v2 digest. Changing unrelated settings or other workflow states does
not revoke a v2 approval. In particular, `max_active_tasks` and
`environment.recheck_interval_seconds` govern admission and pause scheduling,
so neither enters review authority. Finding routing adds assessment fields,
not a review configuration input.

A missing `source_digest_version` means v1. Such contracts still verify with
`legacy_v1_source_digest`, the exact whole-source algorithm used when they were
frozen, including its settings, workflow, assignment, and evidence inputs. The
missing version stays omitted when serializing v1, preserving contract digests and
immutable assessment equality. No migration or review is required merely by
upgrading; changing a v1 input still requires fresh review. Fingerprint versions
are independent of the conformance policy version.

Acceptance and reviewer completion recheck the source digest using the version
frozen in the contract; completion also validates current governing material.
Acceptance rechecks source provenance inside the SQLite write transaction.
Direct integration holds the authority write lock during the local git operation,
compares source and target commits, fast-forwards only the immutable reviewed
object, and checks the resulting head. Stale review authority and a clean target
rebase enter a marked review-refresh route; they do not consume merge-fix budget
or dispatch a coder. Actual conflicts enter bounded merge repair. Changed content
must receive a new semantic review, regardless of `review_passed_at`, except for
the mechanical carry below. Explicit human and no-agent-review workflows remain
separate and cannot manufacture an automated Charter assessment.

**Review authority carry.** A Task that loses merge races on shared hub files
would otherwise pay a full reviewer run per lost race. The `review` state's
`on_enter` hook `carry_review_authority` (ahead of `dispatch_role_agent`) keeps
the previous approval when the Task re-enters `review` only because (a) Forge
rebased it cleanly onto a moved target (the transition log ends with the
`[review-refresh] [target-moved-rebase]` bridge followed directly by the move
into `review`), or (b) its Worker completed the repair of a Forge-committed
rebase conflict (the bridge carries `[conflict-handoff]`). Every condition must
hold, and failing any one is a `Skipped` hook, so the reviewer is dispatched as
usual:

- the transition was not user-triggered, the review gate does not require user
  approval, the Task is not a coordination root or read-only, and the log
  matches exactly one bridge then the entry (a check-failure bounce, a user move
  or any other intervening transition disqualifies it);
- `ci_steps` are configured and this entry's blocking `run_ci_steps` recorded
  every one with exit code 0 (with no checks, nothing verified the new tree);
- the Review attempt immediately before this entry passed and is still current
  authority under the same checks integration makes: passed conformance, current
  policy, an unchanged source digest under the contract's frozen fingerprint
  version and a matching frozen assessment;
- every path `HEAD` changes relative to the target tip is in the approved
  contract's `candidate_changed_paths`, `HEAD` descends from the current target
  tip, the worktree is clean and no handed-off file adds conflict markers;
- fewer than five carries already exist for that contract.

On success one writer transaction settles the Review attempt that
`run_ci_steps` opened as `passed` (its details keep the previous `conformance`
and `auditor` verbatim and this entry's `ci_steps`; the attempt is not a new
verdict), sets `review_passed_at`, and inserts a `review_authority_carry` row
naming the contract execution, the rebased or repaired `commit_sha`, the target
tip as `base_sha`, the kind (`clean_rebase` or `conflict_repair`) and the changed
paths. The hook then posts a system comment and cascades to `merging`; the
engine stops running `on_enter` hooks after a cascade, so no reviewer is
dispatched. `lock_review_integration` keeps every existing check and exposes the
effective candidate: the newest carry row whose `contract_execution_id` is the
current passed contract's, else the contract's own `commit_sha`/`base_sha`.
Integration compares `HEAD` and the target tip with that candidate, so a newer
real review (a different contract execution) supersedes
all earlier carries automatically. A stored workflow change (such as migration
`V148`) forces a fresh v1 review; for v2, only a change to review authority
invalidates the source digest.

The manual review-rerun endpoint consumes the same result categories as automatic
review completion. Passed reruns cascade into merging, failed reruns use the
review-remediation budget and target, and human-required passes remain in review
as `awaiting_human`; the endpoint returns the Task after that workflow work has
settled.

Review outcome and conformance are separate projections. Old PASS/done records
are retained with `not_assessed`; CI-only and manual reviews also lack automated
Charter evidence. New in-flight executions without a contract cannot grant
conformance acceptance, and old unmerged agent-review PASS requires a fresh
review before integration. A stored pass under an obsolete conformance policy also
requires a fresh review. Task workflow states and historical release records are
unchanged.

Task-step execution is not cancelled by claim/renewal failures. Claim errors back off while retaining in-flight jobs; shutdown drains for eight seconds within the supervisor's ten-second grace. Process-local active execution witnesses exclude their Tasks from claims even when wall time advances during laptop sleep; renewal can restore an expired lease only while its owner token still matches. The status-CAS completion accepts that live server witness, but always fences the SQLite owner token.

Step payloads reference the current Project workflow or a deduplicated immutable definition for explicit engine workflows; they never repeat full definitions. Queue execution resolves and validates the current gate approval policy and propagates the producer's clear_review_passed_at_on_commit flag, matching base behavior. Rebase head evidence and rebase counts end a chain segment even without CI/review. Loop lookup reads only the current chain.

Step retention runs hourly inside the `storage-maintenance` worker (its vacuum
tick stays at five seconds). Each prune deletes `done`/`superseded` rows
completed more than seven days ago and `failed`/`parked` rows completed more than
thirty days ago, in statements of at most 100 rows per class and at most 20
rounds per run, using the `task_step_settled(status, completed_at)` index. It
keeps rows of chains that still have pending or claimed steps, rows holding a
lease, and Tasks running a step. Unreferenced workflow definitions older than
seven days are then removed; the reference probe uses the indexed generated
column `task_step.workflow_ref_id`, not a JSON scan.

Initial dispatcher admission only commits/enqueues and kicks the worker. A queued rollback to the initial state finalizes the existing placement-refusal bookkeeping; the dispatcher does not drain chains inline.

Board reorders and recovery markers remain audit rows only; neither changes the status epoch. expected_version remains a diagnostic stamp and is never rebound. Replay-marker cleanup is a queued Task mutation. Queued role-entry agent references reserve admission capacity only for the short window before the entry takes its slot: a Task's available fast-lane head step, a claimed fast cascade or hook step whose dispatch has no recorded result. Steps behind another step, in retry back-off, or waiting for or inside a long-lane merge/CI hook hold no agent or server capacity; if their later dispatch finds the agent full it is skipped and the dispatcher's active-task recovery re-drives it. The dispatcher kicks and continues rather than running the queue. Lane classification is checked again against the resolved workflow at execution and requeues a changed lane before any transition starts.
