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

`forge-cli/main.rs` passes
`Arc<SqliteDb>`, `Arc<EventBus>`, the Forge agent host/backends, and background
workers to `AppState`. The revised `AppState` constructs the Task,
identity/profile, Main/Project Agent Chat, embedded-session, memory,
commitment, and Attention-facing services. `AppState` is `Clone` (shared fields
are `Arc`) and used as Axum state.

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
and durable event creation. The pre-existing workflow-engine/legacy Task
transition split remains outside this migrated orchestration boundary.

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

The singular role boundary is enforced by the authenticated binding and the
Task scheduler, not by model instructions:

| Principal | Canonical scope | Authority | Explicit boundary | Workspace |
| --- | --- | --- | --- | --- |
| Main Agent | Account / Main Chat | Genesis discovery, bounded research, Charter proposal/readiness, exact Project create/handoff, bounded portfolio projection | No Project-local Documents, Tasks, assignments, milestones, validation, waiver, release, or repository access | Denied |
| Project Agent | One bound Project / Project Chat | Project Documents, Decisions, baseline draft/proposal, Task and assignment coordination, milestone/evidence/readiness/release-candidate proposals | No other Project, private Main history, credentials, repository/filesystem, self-validation, baseline approval/activation, waiver, or release | Denied |
| Task Worker | One assigned Task / attempt | Work allowed by the scheduler-issued capability profile | No portfolio, Genesis, unrelated Project/Task, approval, or release authority | Task-scoped lease |
| Reviewer/validator | One assigned review/check | Independent review or attestation allowed by policy | Read-only by default; remediation requires a separate Worker assignment and may not review its own work | Read-only by default |
| Interactive user | Authorized account/Project | Exact approvals, setup choices, manual attestations/waivers, and release | Only existing authenticated user controls | Existing user controls |
| System | Server policy | Readiness, scheduling, leases, recovery, and projections | Not a chat principal; cannot substitute for user approval | Internal only |

Provider/model configuration may be reused across these rows, but principal,
scope, history, assignment, lease, and attestation provenance are not reused.
An active Main or Project Agent identity is never silently converted into a
Task Worker or reviewer, and an independent reviewer is never the same
identity as the Worker it reviews.

Every persistent agent session binds to one canonical scope:

| Scope | Authority source | Filesystem |
| --- | --- | --- |
| Main Agent Chat | active Main Agent binding and account policy | denied |
| Project Agent Chat | active Project Agent binding and Project policy | denied |
| Task | admitted assignment, workflow role, and Task Workspace | role-bounded Task Workspace only |

Effective permissions are a fail-closed intersection of the account, selected
profile, canonical scope, tool, approval, and applicable binding or
Task-assignment layers. Opaque record IDs are references, not capabilities.

### Main and Project Agent Chats

An account has one global Main Agent Chat (visible in setup state even before a
binding is selected), and each operational Project has one Project Agent Chat.
Chat ownership is account/Project-scoped rather than tied
to a replaceable identity, so binding replacement preserves message, handoff,
memory, and session provenance. Connected but unbound identities do not create
additional chats. There are no participants, addressing rules, responder
policies, arbitrary threads, or bounded multi-agent rounds.

Creating an operational Project creates its Project Agent binding and Project
Agent Chat atomically. A migrated Project with no single safe binding is
explicitly `agent_setup_required` and keeps its Project/Task data readable, but
cannot admit Project Agent turns until the user resolves the identity. When an
approved adoption Charter resolves that setup binding, the same transaction
installs the canonical Project-Agent permission ceiling before activating the
binding; setup's empty ceiling never survives into an admitted Project Agent.

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

Every new user-message, Main-to-Project handoff, autonomous wake, or baseline-
activation turn goes through `AgentTurnAdmissionService`. Admission resolves
the current owning binding, its identity, that identity's current Profile,
operating-skill revision, policy/tool digests, and canonical scope, then freezes
those values and the admission digest on the queued job. The shared
`AgentChatTurnRunner` abstraction (the configured implementation is
`FederatedAgentChatTurnRunner`) runs native or constrained CLI backends for
all admitted turns. An explicit retry reclaims the same turn job and frozen
provenance; it is not a second responder resolution or a new model admission.
A Profile edit or binding replacement therefore affects the next admitted turn
only, never an already queued/leased turn.

An autonomous `delivery_followup` admission also freezes a typed postcondition
on its server-authored trigger: a Project-scoped
`milestone.readiness.evaluated` event must commit at a sequence newer than the
wake event before the worker may append the assistant response and mark the
turn `succeeded`. Prose alone fails that attempt without committing an agent
message. The same frozen job moves through the ordinary finite `retry_wait`
budget, and its retry receives a server-owned corrective instruction to invoke
`project.readiness`. A canonical `blocked`, `failed`, or `stale` readiness
result satisfies the reconciliation boundary; it does not imply validation or
authorize the user-only release action.

The chat worker selects the session backend from the bound identity's profile.
A native profile uses the embedded host and an Agent Chat-scoped continuity
timeline; a safely migrated CLI profile may use an explicit constrained chat
backend and must advertise its actual limitations. Main and Project chats have
deny-all filesystem access. Project Agent Task actions go through the existing
`TaskService` and workflow; repository mutation remains limited to admitted
Task Worker/reviewer executions in their Task Workspaces.

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
| Execution intent | approved Project Documents and one active execution baseline | Project Agent proposes; user approves the baseline and material changes |
| Consequential choices | effective `DecisionRecord` (`active`, `superseded`, or `invalidated`) | Authorized principal recorded on the decision; candidate/editor records are not effective decisions |
| Work state | Task, validation, review, and event records | Task/workflow services, assigned workers, reviewers, and authorized users under existing policy |
| Outcome and release | milestone definition, `ReadinessSnapshot`, and immutable `Mxxx-rN` release manifest | Project Agent proposes; Forge evaluates; user alone releases |
| Context and continuity | authorized `ContextManifest`, LCM timeline, and scoped memory references | Forge authorizes sources; Runtime stores continuity; neither chat nor memory can promote authority |

The Main Agent owns global discovery and portfolio routing only. It can draft a
Genesis Charter and publish one bounded handoff, but it cannot manage a Project
or revise its Charter after attachment. The Project Agent owns planning and
orchestration for exactly one Project, but cannot edit a repository, self-review,
self-attest, self-waive, or release. Only a scheduler-issued Task
`WorkspaceLease` grants repository authority to an assigned worker or reviewer.
Model output, Agent Profile text, chat prose, web pages, repository text, and
memory are data; none can widen a permission ceiling or satisfy an approval.

`WorkspaceLease` is an internal scheduler record, not a public API or chat
capability. The V076 `workspace_lease` table persists the Project/Task plus
exact Task version and execution attempt, logical repository binding, resolved
base ref, role, capability JSON, assigned principal, capability-profile
revision/digest, issuing principal, issue/expiry timestamps, status, and
optimistic version. Its database guards require the same Project, Task version,
repository binding, assigned principal, running execution, approved-baseline
gate (or the explicit pre-baseline read-only discovery/planning predicate), and
profile revision/digest; one active lease is allowed per Task and identity
fields are immutable. Active Main Agent and Project Agent identities are
ineligible for leases even if a caller tries to assign them a Task role.
Custom workflow execution-role names are retained for assignment matching and
canonicalized to `worker`; only the dedicated `reviewer` role receives the
reviewer lease class. Its operation idempotency key is the exact execution
attempt ID: claim inserts the execution and lease in one transaction, and each
retry/follow-up creates a fresh child execution with its own lease. A matching
role assignment is authoritative for that execution role (for example, an
independent reviewer may differ from the Task's primary worker). On a
`legacy_unverified` Project only, an explicit manual execution selection is the
assignment boundary when neither the role nor Task has an assignee; an existing
applicable assignment still must match. Charter-backed Projects always require
the exact Task Worker/reviewer role or Task assignment.
`WorkspaceLeaseRepo` provides CAS renewal/revoke and bounded expiry operations.
The heartbeat renews an active lease before its deadline only while the exact
execution is still running and its Task, assignment, governance, repository,
and capability bindings remain valid. Renewal changes only `expires_at`,
`updated_at`, and the optimistic version; all authority fields stay immutable.

The scheduler delivers authority through the internal execution channel by
creating the running execution and lease together; the executor acknowledges
that delivery by verifying the exact active lease immediately before provider
start and before execution work. A missing, expired, revoked, reassigned, or
superseded lease fails closed. Heartbeat/recovery expiry cancels and terminalizes
the running attempt only after a valid running lease can no longer be renewed,
and records reconciliation; all terminal, failed,
cancelled, daemon-disconnected, and stalled paths revoke the grant. A retry
gets a new execution identity and lease. The claim path canonicalizes executor,
worker, and task-worker aliases to persisted `worker`, while reviewers remain
`reviewer`. No route, MCP tool, chat context, filesystem path, handle, or bearer
token exposes the row.

### Project readiness and execution setup

Project coordination, repository/role setup, and baseline execution are
independent projections. A successful Project/Chat creation or a configured
repository does not imply the other dimensions:

| Dimension | States | Authority and consequence |
| --- | --- | --- |
| `coordination_state` | `setup_required`, `ready`, `unavailable` | Active singular Project-Agent binding plus a ready Project Chat; only `ready` admits a Project-Agent turn. |
| `execution_setup_state` | `setup_required`, `provisioning`, `ready`, `failed`, `unavailable` | Durable provisioning operation, verified repository linkage/filesystem state, and required eligible Worker/reviewer assignments; implementation dispatch/write leases remain denied until `ready`. |
| `execution_gate` | `pre_baseline_read_only`, `baseline_approval_required`, `active`, `reconciliation_required`, `unavailable` | Current baseline and reconciliation records; only `active`, together with ready setup and other Task guards, permits repository-mutating implementation. |

The Genesis create/handoff transaction may commit a valid Project while
execution setup is still `provisioning` or `setup_required`. Provisioning is a
finite, leased, checkpointed, idempotent operation that reconciles the
deterministic filesystem target, repository row, Project link, and role set;
interruption resumes that operation rather than creating a second directory or
repository. Required Worker and (when policy requires it) independent-reviewer
roles are derived from the workflow/baseline, excluding active coordinator
identities. Missing roles are typed setup requirements with a user/system retry
or configuration action, not a best-effort executable success. Read-only
planning/discovery may remain available under its explicit capability before an
active baseline.

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
provider callbacks. Those events may advance `last_progress_at` and append a
deduplicated `execution.progressed` event, but they do not prove ownership.
A quiet provider/tool call therefore remains live while its owner lease is
current. A stale-progress scan can append a distinct, atomically revalidated
`execution.progress_warning` Attention event; it does not fail a live lease or
turn a warning into owner death.

`hard_deadline_at` is derived from the admitted profile/capability snapshot
and is a fixed upper bound. Heartbeats are clamped to it and cannot extend
the deadline. The monitor treats an expired owner lease and a reached hard
deadline as different recovery causes (`execution.stalled` with an
`execution_lease_expired` reconciliation reason versus
`execution.hard_deadline_exceeded`) and never treats semantic silence alone as
expiry.

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
truth.

### Charter, Documents, Decisions, and effective state

Every Main Agent Chat turn carries a server-owned operating instruction.
Outside an active Product Genesis session, the account baseline skill
`forge.main.baseline/v1` Revision `@2` is in force: it tells the model it is
Forge's Main Agent, hands it the bounded portfolio projection, and restates the
no-Task/no-repository/no-credential boundary. It also routes clear
natural-language new-Project intent through the Main-only typed
`genesis.start` operation, asks one concise question for ambiguous
new-versus-existing Project intent, and keeps non-Project or existing-Project
requests in baseline scope. The browser does not own semantic classification.
The baseline is compiled into the server (each revision's content digest is
pinned by a test, not a seeded row) and the exact revision/digest is frozen in
the turn's context manifest. Historical `@1` turns remain reproducible from
their frozen body and digest.

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
`EffectiveProjectState` by domain, names the governing Charter/baseline/
Documents/Decisions/Tasks/checks/milestones/releases, and records a visible
canonical conflict plus `reconciliation_required` reason when authoritative
records disagree. It blocks only the affected execution or readiness path.

When Task creation omits an explicit governance envelope, Forge derives one
from the Project's current Charter. Before baseline activation, ordinary
implementation Tasks are retained as non-runnable instead of being rejected;
baseline activation promotes matching Tasks. `planning_task` and `discovery`
Tasks additionally have an explicit pre-baseline lane: they may be claimed only
with the read-only repository capability and low risk, and both service and
transactional admission enforce that same predicate.

#### Milestones, readiness, and immutable releases

Milestone definition revisions use only `draft`, `proposed`, `approved`, and
`superseded`. The milestone instance lifecycle uses only `planned`, `active`,
`ready_for_release`, `released`, and `cancelled`; blockers, stale results, and
`reconciliation_required` remain typed projections while an unreleased
milestone is `active`. Multiple milestones may be active, and the Project keeps
an explicit `primary_milestone_id` whenever at least one milestone is `active`;
planned or `ready_for_release` milestones do not require that pointer, and it
is cleared when the last active milestone leaves that state. The primary is
never inferred from recency or Task counts. Compact Project creation supplies
`M001` (shown as `M1 — Deliver outcome`) when no other definition is present.

Forge persists one immutable `ReadinessSnapshot` per standalone evaluation.
It records the exact input manifest, source versions, evidence attachment
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

Before evaluating or recomputing readiness, Forge checks that the active
baseline milestone manifest pins the milestone's current definition. Required
stable requirement IDs in that definition must be represented by the
baseline's acceptance/evidence matrix, and each matrix row's
`check_definition_revision` must be one of the check-definition revisions
frozen by the release policy. A malformed or drifted contract fails closed as
a persisted non-ready snapshot with typed `reconciliation_required` reasons;
it cannot produce a ready snapshot from an empty or unrelated definition. Live
validation summaries and Project Overview check counts likewise include only
rows belonging to each milestone's current definition revision, never checks
or results left behind by a superseded definition.

A Project Agent readiness action invokes the same `MilestoneRuntime`
evaluation as the authenticated REST route and returns the committed snapshot;
it is not a request event awaiting an absent consumer. A Project Agent release
candidate remains non-authoritative: Forge validates the exact ready snapshot
and milestone version, records the candidate, and raises human attention for
the user-only release decision. The candidate-request event is an attention
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
execution setup precedence over downstream work, then baseline approval,
Task/validation/evidence remediation, readiness, user release, and finally
milestone definition. Clients must render the action's target and operation;
they must not infer an executable action from a stale badge, Task counts, or a
free-form message.

Overview readiness is fresh only when the current milestone definition,
baseline/policy, acceptance-check definitions and results, document approvals,
waivers, evidence source context, and bounded repository/build references still
match the exact input manifest of the displayed `ReadinessSnapshot`. A recent
snapshot with a `ready` result is therefore displayed as stale when any
covered source changes. Release history is immutable and is shown separately
from this mutable projection.

#### Context, memory, and recovery invariants

Main context contains only the active Genesis Charter state and bounded
portfolio projections. Project Agent context contains the current approved
Charter, the active approved baseline, relevant approved Document revisions,
compatible effective Decisions, authoritative Task/validation projections,
active milestone/readiness state, and immutable release history. Every source
is revision-addressed in a `ContextManifest` with authorization, digest,
inclusion reason, and token disposition. Semantic memory and LCM summaries may
point to canonical artifact IDs/revisions and identify stale references, but
never contain a separately editable copy of Project truth. A newer approved
artifact or server state always outranks chat, summaries, memory, or model
output; cross-Project sources are rejected before retrieval and counting.

Genesis Project creation, binding, Project Chat, Charter attachment, handoff
message/turn, events, `handed_off` transition, and receipt consumption are one
database transaction. A failure leaves Genesis `ready_for_project`, the exact
approval receipt `active`, and no partial Project or handoff; retry with the
same idempotency key returns the original committed result if one exists.

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

After that transaction commits, `project_provisioning` reconciles execution
setup as one durable, leased, finite, idempotent operation (also resumed on
creation replay). Its checkpoints cover preflight, filesystem initialization,
repository registration, Project linkage, and role assignment. It initializes
or verifies a local git repository (first commit on `main`) under
`<workspace_root>/repos/`, registers or reuses one matching logical repository,
links it with Project-version CAS, and resolves canonical Worker and (when
required) independent-reviewer assignments from current workflow/baseline
policy. Main/Project-bound identities are never eligible. A missing Worker or
reviewer is a typed `setup_required` blocker in the current setup projection;
it is not an operator-only log or an executable success. Replays reuse the
operation, checkpoint target path, directory, repository row, Project link,
and role set. Agent `task.propose` executions on a
Charter-backed Project without an explicit governance envelope are bound
server-side to the active user-approved execution baseline; the proposal
payload carries only `plan_item_id` (required for implementation Tasks),
optional `milestone_id` (defaults to the baseline's primary milestone), and
optional capability/risk classes. A proposal that names a plan item or
milestone retains that exact baseline traceability even when its capability is
read-only (for example, an independent verification Task); those references
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
A Project's coordination/chat state, execution setup, and baseline gate remain
independent projections. Each setup response carries per-dimension freshness;
an unavailable source is returned with a bounded retry action and never
converted into inferred readiness. V087 backfills only database-verifiable
repository linkage and current workflow/baseline role eligibility. It records
local repository initialization as skipped with `filesystem_verified=false`
until a reconciler verifies the filesystem, and never fabricates identities or
successful setup. A release or media-pin failure leaves the milestone
`ready_for_release` with no
partial manifest or pin. Migration failures leave legacy media references and
bytes usable; physical cleanup is a separate guarded operation. These recovery
rules make replay safe without inventing approval or silently substituting a
name, Charter revision, artifact, or evidence asset.

Project deletion is a transactionally guarded teardown. It removes the
Project-owned immutable graph in dependency order and then the Project itself;
the database permits those deletes only while the exact Project deletion guard
is active. Direct attempts to mutate or delete an individual immutable Charter,
milestone, readiness, release, decision, baseline, lease, or evidence record
remain rejected.

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

Harness agents may reference a provider entry (`credential_ref` on the active
profile). At dispatch, `TaskService` asks `EmbeddedAgentService` to inject the
entry's API key into the in-memory executor snapshot as the provider's
environment variable; the stored snapshot, events, and logs never carry the
key, and OAuth bundles are refused for harness injection. Harness agents
without an entry keep their CLI-managed login and are surfaced from daemon CLI
discovery.

Credential handles distinguish static `api_key` payloads from renewable
`oauth_bundle` payloads and carry optimistic versions. Native adapters acquire
short-lived leases through Agent Runtime's `ProviderCredentialSource` rather
than receiving plaintext configuration. Expiring bundles refresh under a
per-credential single-flight lock and rotate ciphertext plus the handle version
in one transaction. Exact-revision invalidation prevents an older rejected
request from invalidating a newer lease. Provider errors, events, public rows,
and Debug output remain redacted.

The Project Agent route is an Agent Workspace: one durable conversation beside
a typed Project-record rail on desktop and a Conversation/Project segmented
view on compact screens. The rail calls the existing Project, Task, artifact,
Decision, and milestone services and surfaces saved/conflict/error receipts.
It does not widen authority. Main and Project Agent sessions still use
`WorkspaceAccess::Deny`, receive no repository path or shell, and can cause
repository work only by creating/admitting a Task through the workflow.

`forge-agent-host` composes Agent Runtime directly at immutable revision
`a7075b1d2dd1cee05db63bc480ff46b0f97ec239`. It owns provider construction,
protected credential/checkpoint/session stores, interaction handling, runtime
events, content guards, usage mapping, cancellation/steering capabilities,
typed tools, and scope-derived workspaces. Forge does not depend on the sibling
TUI and does not add a Smith-native backend; Smith remains an existing CLI Task
executor/profile.

Lossless Context Memory continuity is keyed by `(identity_id, scope_type,
scope_id)`, never by a replaceable runtime session. Main/Project Agent Chats
use their own canonical timelines and native Task work uses a Task timeline.
SQLite implements the Agent
Runtime LCM reader/writer contracts with host-minted view authority on every
operation, immutable admitted entries, transactional DAG compare-and-swap,
operation fingerprints, and restart recovery. Session rotation follows the
same authorized timeline; histories from different canonical scopes cannot be
opened or merged by possessing a timeline/node ID.

Forge selects and authorizes domain context; Agent Runtime alone budgets and
serializes final model context. `context_manifest` records the offered source
IDs/revisions and selection reasons, links the runtime run-manifest fingerprint,
and records included/summarized/omitted dispositions without duplicating token
planning. Protected bodies never enter either manifest. Authorized manifest
inspection compares pointer-backed Project references with the current
canonical Charter, Document, baseline, milestone, Project, and binding
revisions and reports stale references as a read-time overlay; it never rewrites
the immutable manifest or LCM history.

### HTTP shell and web assets

The API router also serves the built React application with an SPA fallback.
Hashed JavaScript and CSS assets receive immutable one-year cache headers and
eligible responses are Brotli/gzip compressed; HTML navigation responses remain
uncached so deployments pick up the current asset graph. The production client
keeps route screens and editor-backed dialogs behind dynamic import boundaries.

### Durable events and the in-process event bus

Agent-critical mutations commit a monotonic `domain_event` row in the same
SQLite transaction as their authoritative state. Events carry canonical scope,
actor, correlation/causation, bounded reaction depth, and dedupe identity.
Consumers claim durable cursors/leases and checkpoint only after idempotent
projection, so lag and restart replay cannot duplicate chat turn jobs,
Attention rows, actions, memory indexing, or commitment reconciliation.
The `agent-coordination-outcomes` consumer is started by `forge-cli`; it turns
terminal Task transition events into one task-outcome inbox item and, for a
scope-validated originating commitment, one delivery evidence/lifecycle
projection.  Its event-derived dedupe keys make a crash between projection
and receipt checkpoint safe to replay.

The `agent-wake-turns` consumer (also started by `forge-cli`) closes the wake
loop. Migration `V088` records an install-time cutover cursor, so events that
commit after installation are evaluated even when the process has not polled
yet; startup never derives a cursor from the runtime event maximum. Each
claimed `agent.wake.*` candidate receives one durable current disposition:
`turn_admitted`, `deterministically_suppressed`, `deferred`, or
`setup_required`. The disposition, projection receipt, cursor advancement,
lease release, and optional Agent Chat message/turn admission commit in one
transaction. Deferred and setup-required incidents retain bounded retry or
authoritative-state reconsideration lineage instead of disappearing as
completed delivery. Claims and checkpoints are ordered by ascending
`domain_event.sequence`; a live lease at the head is an ordering barrier, and
the cursor cannot skip an undisposed sequence. A disposition replay is
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
lease/attempt metadata. The same admission path delivers
`project.execution_baseline.activated` to the Project Agent chat as a distinct
`baseline_activation` trigger; the user approval is its deterministic
admission policy and consumes no autonomous wake budget.

Execution terminal outcomes are written by the winning execution terminal CAS:
the terminal row, active `WorkspaceLease` disposition, and one durable
`execution.completed`, `execution.failed`, or `execution.cancelled` event are
committed together. The Attention projection can derive an
`execution_failed` incident from that event, while `execution.progressed` and
`execution.progress_warning` remain separate semantic-progress signals.
Project-scoped incidents with no more specific responder wake the active
Project Agent binding, whose default wake budget is 10 admissions per hour.
Every terminal `done` Task transition also creates a `delivery_followup`
Attention incident for that Project Agent. The follow-up asks it to reconcile
the Task outcome into authoritative validation, evidence, and milestone
readiness; the Task transition itself supplies none of those facts and never
authorizes a release. The admitted follow-up cannot succeed from narration: it
must commit a newer Project-scoped readiness evaluation or retry/fail under the
existing finite turn budget. A committed readiness evaluation resolves the
pending delivery follow-ups even when the evaluation remains blocked.
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

`ForgeMemorySource` is constructed with immutable identity and canonical-scope
bindings and returns already-authorized, ranked, bounded Agent Runtime memory
records. It suppresses raw chat-derived memories already represented in the
active LCM/recent history. LCM summaries remain derived episodic continuity,
not verified semantic facts.

### Commitments, Attention, and Mission Control

Inbox items, commitments, and typed action/proposal envelopes are durable
coordination records. Commitment completion requires authorized evidence;
profile or session replacement does not erase an obligation. Mutating agent
actions carry scope, payload hash, dedupe, correlation/causation, requested
permission, and an `allowed`, `approval_required`, or `denied` policy result.
Protected actions cannot be self-approved. Task proposals enter the existing
Task service/workflow and do not become authoritative work before persistence.

Attention is a deterministic, rebuildable projection of human input,
validation/review state, stalls, health, budget thresholds, and overdue
commitments. Any model wake occurs only after deterministic admission with
budget, cooldown, batching, dedupe, incident lease, self-event suppression,
and reaction-depth limits.

Mission Control and Agent detail are bounded read models over authoritative
Task/identity/session/commitment/event state. They show needs-attention,
review-ready and active work, embedded-agent health/current scope/focus,
commitments, recent outcomes, and capacity; they do not introduce a second
mutable Task or Agent truth.

### Daemon command transport

Linked daemons keep a WebSocket command stream open at
`/api/v1/daemons/{id}/connect`. The API server routes filesystem requests
(`fs.list`, `fs.branches`) and daemon-owned managed executions
(`execution.start`, `execution.cancel`) over that stream. The daemon validates
paths against its advertised workspace root, runs the local CLI adapter, streams
execution logs back as `execution.log` notifications, and reports final status
through `execution.terminal`.

Managed execution dispatch currently assumes the server-created task worktree
exists at the same absolute path on the daemon host. That covers local daemons
and containers or hosts with a shared workspace mount. A daemon on a separate
filesystem can still browse paths under its own `--workspace-root`, but
`execution.start` rejects server-only worktree paths until Forge has a remote
workspace sync or git handoff path.

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
separate restore-and-fail backstop.

### Daemon lifecycle and execution recovery

Remote daemons periodically report local CLI availability and, when connected
over the command stream, their currently running managed execution ids via
`POST /api/v1/daemons/{id}/report` (`active_execution_ids`). This report is a
reconciliation input, not proof that output was emitted. A managed execution
is claimed against the authenticated daemon connection incarnation before
`execution.start` is dispatched; transport heartbeats renew that same
owner-bound execution lease independently of log notifications.

When a WebSocket disconnects, its connection-incarnation owner can no longer
renew or terminalize the execution. The monitor reconciles the expired owner
lease through the same execution terminal CAS and may publish the
disconnect-specific `execution.daemon_disconnected`/`reconciliation.event`
diagnostics. A replacement socket receives a new owner token, and late
notifications from the old socket are rejected as concurrent outcomes. The
terminal event, WorkspaceLease disposition, and Task-block cascade happen only
if that CAS wins. Embedded-server executions use the equivalent in-process
owner and heartbeat path.

Remote output, reasoning, and tool notifications update semantic progress
when accepted, but a quiet remote execution remains healthy while its lease is
current. Stale semantic progress may create a separate
`execution.progress_warning` Attention item. Only owner-lease expiry or the
profile/capability hard deadline is an execution-liveness terminal condition;
the hard deadline is not extended by heartbeat renewal.

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
the API server for embedded workspaces. The API treats a task as daemon-owned
when the task is directly assigned to an agent with `daemon_id`, or when the
current workflow state's effective role assignment points to an agent with
`daemon_id`; otherwise it uses embedded server process handling. Both runtimes
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

## Task state machine

```
todo ──────────────► in_progress ──────► review ──────► merging ──────► done
 │                      │                  │              │
 └──► cancelled ◄───────┴──────────────────┴──────────────┘
                                           │
                                      merge_failed ──► blocked
```

All non-terminal states can transition to `cancelled`. Terminal states: `done`,
`cancelled`. The default workflow lives in
`crates/services/src/workflow/default_workflow.rs` with sequence
`backlog → todo → planning → in_progress → review → merging → done` and
`merge_failed`, `blocked`, `cancelled` as auxiliary/failure/terminal states.

The built-in `autonomous_v1` preset lives in
`crates/services/src/workflow/default_autonomous_workflow.rs`. It is a
single-worker graph: `backlog → ready → working → review → merging → done`,
with `merge_failed` returning to worker execution and `cancelled` as the
terminal cancellation target. `working` and `merge_failed` explicitly use the
`worker` role; `review` has no reviewer role and requires human approval.
Entering review runs the before-work hooks and CI steps as blocking guards.
Review rejection and merge repair resume the latest worker thread. The worker
plans internally, implements, self-tests, repairs failures, and reports
verification evidence, so the preset has no planning state or plan-checklist
gate.

### Workflow engine (in progress)

Flexible workflow work is partially implemented. `WorkflowEngine` in
`crates/services/src/workflow/engine/mod.rs` is the new data-driven path;
`TaskService.transition()` still uses the legacy `TaskStatus`/`transition_allowed`
path. Treat the engine as a parallel code path until the split is removed.

Workflows are project-defined JSON in `project.workflow_definition`. Empty
string or `"{}"` resolves at runtime to the built-in `DefaultWorkflow`.
`WorkflowCache` caches resolved definitions per project and invalidates on
workflow updates.

The applicable `WorkflowDefinition` is resolved **exactly once** per transition
entry via `WorkflowEngine::resolve_workflow_for_task`, keyed on whether the task
is a root or subtask, whether its **current** state belongs to the inherited
subtask workflow, and the acting party (`triggered_by`). The result is passed
into wrapper pre-checks, `WorkflowEngine::transition` / `transition_inner`,
and `HookContext` so hook actions, cascades, and advance steps consume the same
definition — no downstream layer re-resolves for that transition. Each nested
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

`StateKind` classifies states:

- **`backlog`** — parking lot; agent claims rejected.
- **`initial`** — exactly one per workflow; validation rejects zero or multiple.
- **`active`** — work state; may declare a role such as `coder`.
- **`gate`** — validation/processing state; `gate_config.max_rejections`
  enables retry-budget checks.
- **`terminal`** — absorbing state; outbound transitions and non-terminal
  cancellation targets are rejected.
- **`custom`** — no built-in behavior beyond graph validation.

`WorkflowEngine::transition` lifecycle for `A → B`:

1. Load task, check optimistic version, validate that `A` and `B` are defined
   states in the applicable workflow (undefined current or target rejects with
   an error enumerating defined state names), then validate the graph edge or
   implicit cancellation path.
2. Run filtered `A.before_exit` guards unless `B` is the cancellation target;
   `FailurePolicy::Block` failures return `GuardRejection` (HTTP 412).
3. Update `task.status`, increment `version`, write `transition_log`, publish
   `task.status_changed`.
4. Run filtered `A.on_exit`, filtered `B.on_enter`, then effective
   `B.after_enter` hooks. Gate states with `max_rejections` get
   `check_retry_budget` prepended unless already present.
5. Backfill `transition_log.hook_results_json`.
6. If an `after_enter` hook returns `HookResult::Cascade`, recursively
   transition with `triggered_by = "system"`; cascade depth is limited to 3.

**Dispatch failure entering an active state:** when a dispatch hook
(`dispatch_role_agent` / `dispatch_fix_agent` / `dispatch_executor`) fails
`on_enter` of an `active` state and the task has no running execution, the
engine does not leave the task there looking in-flight. It records the
dispatch error on `task.error_annotation` (type `dispatch_failed`) and
cascades the task back to the workflow's initial state, skipping the active
state's exit guards (the `reset_to_initial` allowance). Both the failed hook
and the rollback transition land in `transition_log`. The task dispatcher
treats a `dispatch_failed` annotation as blocking, so a task whose dispatch
deterministically fails — e.g. governance rejects it because no active
user-approved execution baseline exists — is parked in the initial state with
a visible reason instead of being rescheduled every tick; the dispatcher also
writes this annotation itself when a scheduling/recovery attempt fails with a
"not runnable" governance error. A later successful dispatch (or a board drag
that defers dispatch) clears the annotation.

Board moves use the same engine through `TaskService::move_task` and its board
persistence seam. A project owns a monotonic `board_revision`, advanced by
database triggers for board-affecting task inserts, deletes, status/position
updates, archives, and soft deletes. The public move command compares both the
task version and board revision after acquiring the SQLite write lock, validates
the destination workflow column and adjacent neighbor IDs, and writes status
plus board position once in a single transaction. Tight numeric gaps are
renormalized inside that transaction, so revisions are monotonic but not
gapless.

Same-column moves use the repository transaction directly and skip status
hooks. Cross-column moves run `before_exit`/`before_enter` guards before the
write, then reuse engine audit, `on_exit`, `on_enter`, `after_enter`, dispatch,
and cascade behavior from the committed task. The direct persistence step
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
direct `task.status_changed`; any synchronous cascade emits its own normal
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
separate from `manual_override_transition`, a system-triggered primitive with
`skip_before_exit=true` used by `TaskService::advance_to_next_state`.

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

CLI profiles continue through the existing executor/daemon path. A compatible
native profile enters work through the same claim, assignment, workflow,
Workspace, validation, review, and delivery services; it does not get an
alternate repository-mutation route. Only the admitted Task session derives
the role-bounded Workspace/tools and Task LCM timeline. Other simultaneous
sessions for that identity retain their own denied Main/Project Agent Chat
workspaces.

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
do not consume budget.

### Crash recovery

`CrashRecovery` runs at server startup and deterministically reconciles
ownerless or expired running executions left by an earlier process. Migration
`V089` does not invent ownership for pre-existing rows: a running row without
verifiable lease ownership is immediately eligible for this recovery pass.
`HeartbeatMonitor` applies the same owner-lease terminal CAS on its periodic
scan, while a separate semantic-progress scan emits warnings without
terminalizing a live owner. Hard deadlines are recovered distinctly from
ordinary owner-lease expiry.

Both recovery paths annotate a task with a `recovery_required`
`error_annotation` only when they actually win a terminal CAS for at least one
running execution, and publish `task.recovered` only in that case. The winning
CAS also closes the execution's active `WorkspaceLease`; a stale monitor or
runner cannot annotate, cancel, or cascade a second time. Tasks whose assignee
is a user are excluded from crash-recovery selection — agent-oriented recovery
is not meaningful for human-driven tasks.

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
deserialize to a read-only `Unknown` variant that renders info-only with no
recovery actions. Producers must never construct `Unknown`. The web client
likewise derives no failure semantics from workflow state names — gate
reject/bounce targets come only from explicit `reject`/`fail` trigger edges or
`gate_config.reject_target`.

Hard failures and recovery states surface to the user as notifications:
`task.failed` when `fail_task` sets `failed_json` (which also clears any stale
blocking annotation), and `task.recovery_required` when crash recovery or an
agent heartbeat timeout annotates a task for manual recovery.
Graceful-shutdown recoveries auto-resume at the next startup and are not
notified. In the derived `workflow_exception` summary, a hard failure
supersedes any blocking annotation — `recover_task` only accepts
`reset_to_initial`/`cancel_task` once `failed_json` is set, so only those
actions are offered. The web UI renders one actionable recovery surface,
`WorkflowExceptionPanel`, on both the task page and the board modal;
`TaskBlockingBanner` is an informational fallback for interruption states
without recovery actions.

`transition_log` is the audit source of truth for state changes. The API
exposes it via `GET /api/v1/tasks/{id}/transitions`.

### Files of interest

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
`crates/db/migrations/V001__initial_schema.sql`. Migrations are numbered
`V{NNN}__{name}.sql` and tracked in `_migration` table. All primary keys are
app-generated UUID v4; all timestamps are app-generated RFC3339.

Connection pool sets `PRAGMA foreign_keys=ON`, `journal_mode=WAL`,
`busy_timeout=5000` per connection.

The revised schema adds `account_main_agent_binding`,
`project_agent_binding`, `agent_chat`, immutable `agent_chat_message`, bounded
`agent_chat_turn_job`, and immutable `agent_handoff` records to the existing
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
  emission, counter increments, `ReviewRunner` on `→ review`, `MergeService`
  on `review → merging`, `WorkspaceCleanupScheduler` on `→ done` /
  `→ cancelled`). Background tasks: `CrashRecovery` at startup (orphan
  execution recovery and stale-annotation sweep), `HeartbeatMonitor` (owner
  lease/deadline expiry plus separate semantic-progress warnings),
  `DaemonMonitor`, Agent Chat turn workers, durable event consumers, Attention
  projection, and `WorkspaceCleanupScheduler`.
- **review** — `ReviewRunner` runs `task.review_config.ci_steps` as `bash -lc`
  commands in the worktree; empty steps auto-pass. Creates a `reviewer`-role
  execution sharing the executor's workspace, with the same owner heartbeat,
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
  Claude Code stderr and `is_error` result events); assistant output text is
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
- **Availability recovery** — `executor_unavailable` bypasses the execution
  retry budget entirely. Transient exhaustion (retry time known) schedules a
  deferred dispatch at the structured `retry_at` plus deterministic jitter;
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
- **workspace** — File-based locking via `.forge.lock`. Path validation
  prevents traversal escapes.
- **config** — `ForgeConfig` with precedence: CLI flags > env vars > config
  file > defaults. Default bind uses loopback with an OS-selected port, then
  persists the selected port under the Forge data directory.
