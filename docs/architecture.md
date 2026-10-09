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
├── operation-registry/ # Typed native operation specs and generated projections
├── db/            # SQLite schema, migrations, repository implementations
├── services/      # Business logic (task state machine, workflow engine)
├── executors/     # TaskExecutor trait, Shell executor, JSONL logging
├── cli-adapters/  # Codex, Claude, Cursor, Gemini, opencode, shell, null adapters
├── workspace/     # Git worktree lifecycle, locking, path guardrails
├── git/           # Low-level git operations
├── process-supervisor/ # Process-group ownership, kill sequence, drained bounded output
├── check-executor/ # Persistence-free owner execution of a CheckSpec, typed receipt
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

services / review / forge-client → check-executor → process-supervisor
                                                  → git → process-supervisor
                                                  → executors / api-types
```

`process-supervisor` is a leaf (tokio only): `git` cannot depend on `executors`
or `review`, and `forge-client` must not pull in `services`, so the one
process-group implementation that Git, CI steps and daemon helpers share lives
below all of them. `check-executor` sits between: it needs `git` (managed
checkout), `executors` (run budget, redaction) and `api-types`, and is called by
`services`, `review` and `forge-client`; it depends on neither `services` nor `db`.

## Architectural patterns

### Native operation registry

`operation-registry` is a leaf crate: Serde, schemars and async-trait, all of
which the workspace already resolved. `agent-host`, `services`, `db` and
`mcp-server` depend on it; it imports neither, so typed service handler registration does not
create a dependency cycle. Specifications live in domain modules of that crate
(`scope_reads`, `project_reads`, `main_reads`, `main_proposals`, `hand_proposals`), below `agent-host`, because `agent-host`
builds the advertised schema and cannot depend on `services`. Each module owns
its specs, its id list and the small context trait its handlers need;
`services` implements the traits. `lib.rs` only concatenates the modules,
checks that specs and ids agree with no collision, and iterates by id.

An `OperationSpec` owns its Serde input type and the JSON schema derived from
it, declared structural constraints, authority facts, effect class,
availability, native aggregate and field projection, short summary, bounded
guidance, and typed handler binding. A handler takes exactly the spec's input
type, so a mismatch does not compile.

**Contracts and authority.** The schema projection and full input contract
have different jobs; operation authority uses the same evaluator:

- *Advertised.* The aggregate tool schema sent to a provider keeps the flat
  shape `{operation: enum, arguments: object}` and uses only the JSON-Schema
  keywords the native schemas already used. It never expresses a per-operation
  contract with `allOf`, `anyOf`, `not`, `if`/`then`/`else`, `$ref`, `$defs`,
  `dependentSchemas` or `patternProperties`. The model learns a registered
  operation's contract from one generated line in the `arguments` description
  (`project.charter: no arguments`; `skill.section: {section: one of
  research|documents|...}`; `?` marks an optional field). The line is generated
  from the spec's schema, so it cannot drift from what is enforced.
- *Enforced.* After argument normalization, the registry validates and decodes
  the arguments against the spec: in tool preparation, which native execution
  and the CLI chat callback both run, and again at dispatch in `services`. A
  violation is an ordinary in-turn tool error naming the operation, the
  offending field and the expected contract. The check lives in one place,
  `input_check.rs`, and is driven by the canonical schema: closed object,
  required fields, scalar types, string lengths, numeric bounds, then the
  Serde decode. Before the type check an integer field sent as an
  integer-valued string (`"10"`) or float (`10.0`) is rewritten to the
  integer, because several providers emit numbers that way; the handler
  receives the integer. Nothing else is coerced (`"ten"`, `1.5`, `true` and a
  negative value for an unsigned field are refused), and neither the
  advertised line nor the canonical schema is widened. Denial comes before
  contract detail: the read tool rejects an operation outside the session's
  grant, then (on the named orchestration read tools; `forge_scope_read` has
  no such guard, its closed contracts refuse the field) a forged scope or
  authority field, and only then reports a contract violation; at dispatch, a caller the Main handler would deny gets
  that denial even when the arguments are malformed.
- *Canonical.* The full per-operation JSON Schema stays in the registry for
  validation, documentation and the read/proposal canonical fixtures. It is not
  what providers are sent.

Why: function-declaration schema dialects (OpenAI Chat Completions and
Responses, OpenAI-compatible routers, xAI, Gemini) do not reliably accept
conditional keywords, and Gemini rejects unknown ones; and a conditional per
operation would add thousands of tokens to every turn's tool prefix once all
operations move. Two tests in `agent-host` hold the rule: one walks every
advertised native definition for the forbidden keywords, one pins a byte
ceiling per surface (`SURFACE_BYTE_CEILINGS`, the unit
`scripts/measure-tool-definitions.py` prints). Raise a ceiling only on purpose.

Registered reads are `account.summary`, `agent_chat.summary`, `project.charter`,
`skill.section`, `project.current_state`, `project.observations`, `genesis.project_agents.read`, `charter.read`,
`charter.readiness`, `charter.diff`, `charter.approval_target`, `discovery.read`,
`portfolio.read`, and `inquiry.run`. Main query handlers accept the registry's
input types directly; their former dispatcher and handwritten decoders are
removed. Discovery/portfolio defaults and clamping remain in the handlers. Inquiry remains a query: its run log and
bounded findings do not grant proposal authority. Its `MainChatOnly`
availability records the existing exclusion from account inquiry sessions.
The scope-read aggregate adds no contract prose where the base had none.
Main proposal projection registers `genesis.project_agent.select` (direct
command) and `project.create` (approval-required). The named Main propose
aggregate and the generic scope-propose aggregate generate their payload
contract lines from those specs, retaining the portable envelope
`{operation, payload, dedupe_key, correlation_id, causation_id?, causation_depth?}`.
The envelope owns provenance; the spec owns only the payload. Selection's line
is `{expected_session_version, genesis_session_id?, project_agent_identity_id}`;
Project-create's is `{approval_id}`. The old `action` fields were discarded or
ignored by their handlers, so they are omitted from advertisement. Selection
declaratively strips its ignored discriminator before checking/decoding and
keeps its closed request; Project-create keeps an open payload because its
executor ignores extra fields, preserving their exact bytes for action dedupe.
`approval_id` names the user's Charter approval; the user executor reads it
from the stored action payload, so the agent is its only source. A new call
must carry it as a non-empty string: the base enqueuer queued a call without
it, and the owner then approved an action that could never execute. Supplying
an id grants nothing: execution still needs the owner's independent approval
of the action, the user executor, and a Charter approval that belongs to the
account and is not consumed. Selection uses the same integer
coercion as reads and the same receipt-backed command service.

Fresh proposals check the registry's current principal/scope/permission rule first, then forged authority
fields, then their payload contract, then the typed handler. The provider's
preparation denial check also runs before payload diagnostics. Dispatch of the two registered
proposals checks the entire envelope for authority/scope replacements; direct
provider calls previously ignored such root fields, while named preparation
already rejected them. Hand-path operations do not run this envelope check.
The guards share one name list (`operation_catalog::SERVER_DERIVED_FIELDS`):
dispatch adds `project_id`, preparation adds its prompt-injection names, and
preparation of a registered proposal runs both. Exact prepared
invocations use `propose_prepared` and `dispatch_prepared`: they reauthorize and
run existing domain checks, but never revalidate stored arguments against the
current contract. Runtime prepared, approval-pending and recorded edited calls
retain their serialized arguments and fingerprints. `project.create` only
queues its existing approval-required AgentAction; the unchanged user executor
parses the stored action payload, consumes the exact Charter approval and
atomically records the Project/handoff/receipt. It cannot execute by proposing
or by an agent executor. Dedupe keys, correlation/causation, immutable receipts,
protected checkpoint encryption and approval storage are unchanged.

`genesis.start` and `charter.draft` use typed specs in `hand_proposals` and
registered dispatch. Their existing command adapters and domain authorizers are
unchanged: Genesis still derives its creation intent from the leased user
message, and neither command grants a fresh effect to an owned unbound identity.
An owned former Main identity may retrieve its exact committed receipt as at
base. The native `main_account_id` gate remains on `project.create` and
`genesis.project_agent.select`; extending it to these direct commands would
narrow that receipt retrieval. Stored preparations retain their exact arguments;
the transport-only `action` is omitted from the advertised contracts and ignored
before decoding, as the domain adapters already did. `genesis.start` likewise
accepts and discards an unadvertised `initial_idea`: the hand path decoded it
and then replaced it with the leased user message. Both contracts accept every
field their hand path read. The Charter draft is the one registered operation
whose advertised line carries nested shapes (every Charter section and the
provenance object, generated from the registry schema): the tool description is
the only place a Main model is shown them, and a draft sent without them costs
one correction turn per missing field. All other operations keep
their hand paths; an operation never has both. Existing permission/binding checks enforce
authority; spec/check parity is tested pending EffectiveAuthority. Domain
semantic validation, transactions, workspace preparation, MCP projections and
doctrine remain hand-written.

To add an operation, declare a closed Serde/JsonSchema input and its
constraints in its domain module, add its id to that module's `IDS`, and bind
a typed context handler. Delete its former schema fragment, validator branch
and dispatch branch, implement the context method in `services`, and test
schema, contract line, dispatch, constraints and authority parity. Adding an
operation to an existing domain module needs no edit to `lib.rs`. A new domain module joins the composed `OperationContext` and contributes its
specs and id list to the appropriate effect catalog. Reads and Main proposals
use the same `OperationSpec` and typed decoding/dispatch machinery; registered
operation inventory includes both catalogs.

Project proposal specs now own `project.review_config`, `project.document`,
`project.decision`, `project.milestone`, `project.validation`,
`project.release.request` and `project.escalate`. Document and milestone actions
have separate closed field contracts, each of which keeps every field that was
advertised for the operation before the move; their provider projection is still one
generated line per operation, with no conditional schema keywords. Fresh calls
check authority before contract details. Commands receive the original admitted
payload, preserving receipt digests; exact preparations bypass the current field
contract while still running typed decoding and existing domain checks. Payload
ceilings use serialized UTF-8 bytes (65,536), review command lengths use characters,
and read limits use rows. Ready Project Agents also advertise the existing
Charter amendment draft capability; no approval authority is added.

`message.send`, `commitment.update`, `memory.publish`, `memory.supersede`,
`review.request` and `session.action` are pending-intent specs with an open,
size-capped payload that is stored as sent. Their
results remain pending proposals, never success for a message, memory, commitment,
review or session effect. No materializer was added. The operation schema produces the contract line and the canonical field names
used by normalization. Registered pending fields are no longer duplicated as
root-level aliases in `forge_scope_propose`. Flat calls still canonicalize from
the registry's field names, so handlers and prepared calls keep their inputs.
Hand Task operations retain their existing root and payload field declarations.
Named orchestration descriptions state the action once; the canonical scope and
authority rules are enforced by the same server checks. A `null` payload on a registered proposal is read as
`{}`.

`project.charter.adoption`, `project.evidence` and `project.readiness` retain their
hand paths. Literal fixtures from the real approved Charter/create and milestone
command setup show that adoption and evidence return committed receipts after
an identity pause or ceiling reduction. Registry admission would narrow those
replays, so no partial move is retained. Readiness's base capture reached its
bounded fixture-attempt limit; its dispatch and schema are unchanged. Moving
adoption and evidence needs a registry receipt-retrieval step that runs before
current authority is evaluated: an exact replay (same principal, scope,
operation, dedupe key and input digest) returns the stored receipt unchanged
whatever the caller's present pause state or ceiling, while any other call,
including the same key with different input, is evaluated as a fresh command.
The registered Main commands differ and must stay as they are: their replay is
refused while the identity is paused or restricted, and allowed after the Main
binding is replaced.

A queued proposal (the six pending ones and `project.release.request`) refused
for a missing permission still writes its `agent_action` row with status
`denied`, as the queueing step did before these operations were registered. A
call that could never have been queued (malformed payload, no Project target,
Charter not adopted) writes none. Project verification remains a disposable checkout with observed commands:
`project.validation` still requires the command observations for pass/fail and
refuses manual attestation through the Project-Agent path. Setup-only availability
now reports `charter_adoption_not_applicable` after setup completes, rather than
`charter_not_adopted`.

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

### Effective authority for registered operations (3.8 D)

`operation-registry::authority` owns permission normalization, the
`EffectiveAuthority::resolve` resolver, and one `evaluate` function. Native
registry specs and explicit MCP scope classifications both implement its
requirement interface. Principals distinguish interactive users, delegated
users, Main Agents, Project Agents, execution-bound Task Agents and system
components. MCP credentials remain delegated-user authority; a Project
constraint never infers an Agent identity.

The stored permission shapes are arrays of strings, `permissions` objects,
`allowed` objects, the empty default object, and JSON `null` (five stored
shapes). Empty objects and `null` normalize to no permissions. Both keys together must
normalize to the same set (order and duplicates do not matter). Conflicts
are refused at identity/profile/binding write boundaries with a typed error;
conflicting or malformed persisted entries grant nothing on reads. Shipped
seed/default and migration data use these supported forms and retain their
authority. The comma-separated legacy setup ceiling is display text only;
it is normalized through a string-array projection for rendering.

The database loads authenticated identity, admitted Profile, current selected
Profile, active Main/Project binding and Project setup facts. At native or CLI
turn admission the protected session fixes the Profile ID and effective
ceiling. Advertisement uses the registry evaluator against that ceiling and
principal/scope/setup facts. Before registered dispatch, the provider loads
fresh facts, intersects them with the admitted and remaining turn ceilings,
and runs the same evaluator before a fresh effect. Registered reads evaluate the admitted/remaining turn authority without resolving it again at preparation or execution; Project query targets are pinned from that authority. Narrowing is monotonic within the running provider;
restoring a permission later in that turn cannot revive its revoked grant. A widened Profile or binding never widens that turn; a missing or
replaced binding, inactive identity, or permission revoked since admission
returns typed `authority_revoked`. Current state can instead produce a typed
Charter denial or the command's existing version correction. Terminal-denial
caching suppresses repeated effect dispatch and records one reminder per
operation and turn. Preparation carries a server-generated typed denial to
invocation before exposing input contract details.

The scope ceiling table is literal and pinned by a test: a Project Agent
Chat carries `propose_task`, `propose_commitment`, `propose_memory` and
`propose_session` once the Charter is adopted, but not the Project scope's
`propose_review` or `propose_decision`. An owned identity without the Main
binding keeps its account ceiling for the unregistered account operations;
Main reads require the binding through their principal rule. Owned unbound identities retain `project.create` and `genesis.project_agent.select` proposal authority in the agent-action policy; the native boundary still requires the active Main binding for both (`OrchestrationAuthorizationService::main_account_id`); Charter adoption, amendment approval and final release remain user-only.
A stored document that fails to parse is logged with the identity and layer.

Registered Main reads, Project Charter/doctrine reads, identity summaries,
`genesis.project_agent.select` and `project.create` use this path. The
approval-envelope `AgentAction` policy (`evaluate_action_policy`) uses the same resolver/evaluator for account, Project, Chat and identity callers. Task assignment/terminal/reviewer checks remain on their existing path for slice F. The former `action_scope_access` check is removed. `main_account_id` remains as the native Main binding gate for Main proposals, `project.summary` in a Main Chat and Main public search, which the evaluator does not cover. A test-only frozen base policy compares each principal/scope/action cell, refuses any widening, and lists the one intended narrowing (a Project Agent queueing `project.create` from its Project scope or Chat). UI effective
permissions and CLI composition use the same resolver. Existing exact-object,
receipt-first replay, governing-policy and optimistic-version checks remain
in command transactions; fresh Genesis Agent selection also evaluates
transaction-loaded authority. The evaluator does not replace semantic input
validation, approval ownership or the workflow engine. Task caller authority
and Task offers remain on their execution path until slice F. Task read-only
summaries label workflow offers informational, and their ceilings carry no
review/workflow proposal permission.

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
Deterministic configuration, authority, provider schema rejection and authentication fail immediately; context overflow also fails because the host exposes no forced compaction. Provider schema/auth failures carry `provider_schema`/`provider_auth`; a provider rejection the provider marks retryable stays `provider_rejected` with its delay. Quota exhaustion carries `usage_limit`: an autonomous wake turn fails on attempt one (the sweep owns its re-admission), while a user-authored turn defers until the window resets (refunded attempt, escalating floor, six-hour per-wait and 24-hour total ceilings). Transient failures, empty responses, turn limits, unclassified and postcondition failures retain the three-attempt budget. Postcondition retries keep the existing instruction overlay.
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

#### Agent Chat topic epochs

A native Main or Project Chat topic owns one runtime session and one LCM timeline.
The first native turn records the initial topic; rotation records the successor
runtime session ID on a new immutable `agent_chat_topic` row. Transcript IDs,
message provenance and earlier topics remain inspectable.

For a chat whose responder (the active Main or Project binding's identity and
its selected Profile) is native, Genesis start, Project creation/handoff and the
first user message after eight hours idle mark `rotation_pending` in
`agent_chat_topic_rotation`, in the trigger's transaction. A CLI chat raises no
automatic intent and keeps its pre-topic-working-set behaviour: its REST topic
request rotates directly and is denied while a turn is live. A native REST request
uses the same intent: it is denied while a Genesis session needs a decision
(checked in the intent's own transaction), and a request that meets a pending
intent applies its label and summary to it. REST runs the rotation immediately
when no turn is live (bounded by the summary timeout) and otherwise returns a
pending result.

Rotations never run on the turn-claim path. The Agent Chat worker starts one
background rotation pass per poll (`run_once` runs it before claiming); it attempts
every due intent concurrently, fresh intents first, skipping chats with a leased or
originating live turn. Queued successor jobs, including wake turns, wait behind the
intent. Each attempt is leased and counted; a failure records `last_error_kind` and
backs off (`next_attempt_at`: 5 s, then 30 s). The third failure abandons the
intent in one transaction: `rotation_pending` clears, a visible system notice
(`outcome = topic_rotation_failed`) and an `agent_chat.topic.rotation_failed`
event (`error_kind`, `attempts`, `session_handed_over`) are recorded, and queued
turns are admitted again on the current topic. If the runtime fork never saved its
successor, the source session is kept (a pending fork intent is aborted) and the
reserved successor is marked `failed`. If the fork completed, the runtime has
superseded the source, so the successor takes over the chat and only the topic
record is missing.

The intent reserves its successor Forge/runtime IDs and a renewable lease.
A provider summary, bounded by a 30 s timeout, is sealed before calling runtime
`fork_session` with `ForkSeed::Summary` and `ForkLcm::NewTimeline`; a summary
timeout or failure falls back to the deterministic seed and never fails the
rotation. The runtime protects the seed and supersedes the parent. Forge then
commits the topic, divider, session replacement and intent removal together. A
crash leaves the same intent and IDs to replay; a crash after the fork reuses its
protected seed and completed successor. An expired lease can be reclaimed. A
successor is always bound to a fresh, empty timeline. Rotation never grants
additional authority.

The request layout is `[system][tools][topic summary][history][state card + input]`.
The summary is stable within its topic. State cards stay in a transient user-role
fragment, outside durable history. The runtime renders a contributed text fragment
on the system role, so the card carries a random per-backend marker line and a
provider adapter rewrites exactly that marked message to the user role (marker
removed) until the runtime admits user-role transient contributors. CLI chat
retains its existing transcript path.

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

The pinned runtime admits only text in its transient contributor lane. Forge
plans that required card there, then its provider serialization boundary narrows
the identified trailing card message from System to User. Vendor adapters retain
that role, so the stable system instruction,
tools, topic summary and prior history precede the changing card. The card is
planned and sized on every tool-loop step without becoming durable history.
`CachePlanChanged.first_changed_fragment` is recorded in native debug diagnostics;
changes in the history suffix or state card must not invalidate the stable
system/tools/topic-summary prefix.

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
domain/lifecycle validation in the command service. The MCP registry
contains no migrated dotted orchestration operation IDs. Its existing `forge_*`
direct APIs remain separate contracts with unchanged effects and result shapes.
`operation-registry::mcp` owns 18 account, identity, binding, Chat and handoff
contracts, their required roles and resource classifications. Schemas and typed
decoders derive from the same Serde/JsonSchema input structs. The MCP handlers
accept those concrete types; they no longer duplicate parameter declarations or
role predicates. Two effect-time re-checks stay in handlers, next to the write:
identity ownership in `forge_set_main_agent` and the administrator flag for
daemon pinning in `forge_register_agent`. Referenced-resource authorization
reads named fields of a JSON object, so the decoder refuses any other argument
shape. Task/Project MCP tools, including owner escalation, retain
their existing descriptor/handler paths pending their separate migration.

All 42 MCP names have one scope classification in the registry. A Project grant
omits/refuses the same eight account-wide tools. MCP always resolves a
`Principal::DelegatedUser`; Project binding never implies an Agent principal.
RPC generates the moved part of `tools/list` for each connection using the same
`EffectiveAuthority::evaluate` requirements as call admission. Project grants
resolve visibility and member/admin facts for the bound Project; account grants
retain all names and resolve role/ownership against each supplied reference.
Conditional daemon pinning is declared on the registration contract and passed
through that evaluator for both field advertisement and admission. Unmoved
Task calls retain the original database admission path.

Call order is known-name lookup, delegated scope/role admission, Project scope
binding, referenced-resource authorization, then strict typed contract decode
and the original handler effect. Denial precedes contract diagnostics and
owned-identity failures preserve missing/inaccessible equivalence. Each moved
input is closed; a contract violation maps to MCP `-32602` with
`mcp_contract_invalid`, operation and expected fields. The projection emits only
portable base schema keywords and one description line. Optional nulls,
opaque JSON policies, reserved ignored pagination, and strict integer spelling
preserve handler acceptance. Domain services retain their transaction-time
rechecks, CAS, content guards and permission-document validation. No native
orchestration, Task effect, Agent credential or workflow policy is introduced.

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

Runtime-to-timeline bindings are durable and authorized by canonical identity
and scope. U7 ownership claims atomically record owner/generation, increment the
DAG revision and fence every mutation, including replay and truncation. A session
created before U7 resumes once with `Adopt`; subsequent resumes are plain. A
pre-V149 timeline with no recorded runtime session is adopted only by the session
whose own persisted LCM state references it, and adoption stamps that session as
the timeline's runtime owner in the binding transaction, so no other session (in
particular a rotation successor) can resolve to it. Adopt's claim and the snapshot
save are separate writes; if the save fails after the claim, the same session
re-adopts idempotently on its next resume (the store reports the timeline as
unclaimed at the snapshot's generation to its own owner, and the repeated claim
is a no-op). Populated history is never silently adopted or retired. Explicit topic forks use
an independently authorized empty timeline. Historical V149 retired rows remain
readable and keep their canonical scope for deletion; new bindings do not rename
or retire old timelines.

U6 supplies one request sizer for the planner and LCM, including tool arguments,
results and measured fixed overhead. Main defaults to a 48,000-token target and
64,000-token hard cap; Project defaults to 96,000/128,000. Server config and env can
set each value. Smaller provider windows clamp both values. The planner enforces
the hard cap, and runtime-derived pressure/leaf/round defaults govern compaction.
Forge's serialized-entry sizer, chars/4 overhead calculation and pressure overrides
are removed.

The runtime measures fixed overhead only after a successful plan, so a cold
session (new, adopted, or with no successful plan yet) would budget history
against the whole target. On such a turn Forge sizes the system prompt, state card
and tool schemas with the runtime's default request sizer and lowers the target to
`min(target, hard - estimate)` (never below a quarter of the target). With the
default gaps this is a no-op; it matters when a small model window clamps target and
hard cap to the same value. Once the session persists a measured overhead, turns use
the configured target unchanged.

`ProviderLcmSummaryModel` uses the agent's configured provider/model with bounded
host instructions and a low reasoning setting (minimal for Gemini, low for other
controllable models; fixed/unsupported reasoning is left unconfigured). The runtime
capabilities expose no effort levels, so the value is fixed in Forge. The
deterministic model remains a fallback.
Every summary provider call is planner-admitted under the surface hard cap.
Leaf summary usage is reported from the runtime semantic-summary ledger with the
turn that made the call. A topic summary call is written to the
`agent_topic_summary_usage` outbox with its sealed seed and settled into the usage
ledger exactly once, keyed by the outbox row id, as its own invocation (candidate
`topic_summary`) on the chat's surface: right after the rotation commits, or by
the worker's rotation pass after a crash or an abandoned rotation. It is charged
even if the new topic never runs a turn, and never through later turns. Summary
seeds are sealed in the protected store.
U6 rebuilds tuning-derived state without deleting its LCM namespace or requiring
a Forge policy-revision bump. Strict binding and authorization mismatches still
fail closed.

Within a topic, a full `project.current_state` or `project.charter` result carries
a unique `read_ref`, and an unchanged repeat returns
`{ "unchanged_since_call": "<read_ref>" }` only while that full result is in the
model-visible history of the topic. References from the running turn are held in
memory and become durable, with the result's canonical history index, only when
the turn completes; a failed, cancelled or retried turn leaves none and clears the
session's references before its next turn. A durable reference is honoured only
while no LCM summary node covers its history index (the LCM entry sequence equals
the history index), and a same-turn reference only while no node reaches into the
running turn. A changed digest returns the full body; a new topic starts with an
empty read cache, and rotation deletes the predecessor's references. This changes
context representation only, never the authority of the underlying read.

The runtime's process-local history/LCM accounting cache still reads authorized
inclusive ranges in pages of at most 1,024 entries and checks the DAG revision
before and after pressure accounting. Forge's store and the runtime sizer satisfy those
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
| `attention_projection` | All (case-insensitive and substring classification) | Prepare incident and shared responder / write incident, resolutions, audit decision, disposition, message/turn and category budget | Every 60 seconds: reconcile open blocker batches, setup changes, and one-time owner escalations | Publish zero configured-budget notification with the resolved budget scope |
| `agent-wake-turns` | Literal prefix `agent.wake.` | Checkpoint audit records only | None | None |
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

Attention owns autonomous wake admission. It prepares the shared responder snapshot
outside the write transaction, then revalidates Attention and binding authority
inside the transaction that writes the audit event, disposition, message, turn job
and category budget charge. `agent-wake-turns` only checkpoints audit events; it
never treats them as work. Suppressed/setup-required incidents spend no budget.

The default Project budget remains 10/h, divided into blocker 4/h, delivery 4/h
and decision 2/h. Totals of 5 or more split 40/40/20 (blockers rounded up,
delivery down, decisions the remainder) with at least one wake per bucket.
Totals of 1–4 are one shared pool, so no category is starved; the sweep serves
blockers, then decisions, then delivery. A zero total stops autonomy and raises
the budget-stall notice once per binding. An owner's escalation answer is
owner-initiated and never charged. Migration keeps only charges still inside
their hour, in delivery, starting at the oldest live window. One five-minute
Project blocker window admits a single combined directive carrying all eligible
open blockers. The material-state digest excludes event delivery IDs,
sequences, projection versions, the Task title and the action-offer list, so
refreshing or renaming an unchanged incident cannot create new work.

The sweep runs at most every 60 seconds on Attention's WorkerRuntime tick
(superseded turn incidents still resolve every loop). It reads open Attention
with one keyed row per item in `agent_wake_attention_latest` (the latest
decision and its digest for every batch member) and isolates each scope: a
failing Project is logged and skipped. For a Project blocker it reads the turns
linked through `agent_wake_blocker`:

- any linked turn queued or running, whatever digest it carried: no wake and no
  escalation (no member gets a second turn or charge while the batch turn is
  pending);
- a succeeded turn for the current digest: the digest has had its turn; the
  owner is escalated once per completed turn, unless the turn recorded a
  recovery outcome and the blocker has not recurred since that turn ended;
- a failed or cancelled turn: infrastructure failures (transient, lease loss,
  cancellation) do not consume the digest, so the sweep re-admits after the
  Project cooldown; deterministic provider failures (`provider_auth`,
  `provider_schema`, `usage_limit`, `configuration_invalid`) never escalate,
  raise "Project Agent can't run" once per responder Profile version, and hold
  the blocker until the responder's Profile changes or, for a usage limit, the
  window resets (one hour when the provider gave no reset);
- otherwise the blocker is admitted unless its latest decision is a policy
  suppression (`ineligible_scope`, `self_event`, `reaction_depth_exceeded`,
  `resolved_incident`, `repeated_failure`, `retry_exhausted_same_chat`) for the
  same digest and responder.

A digest change, or a resolve followed by a reopen, re-arms the blocker.
Rows imported from before the upgrade never count as a completed turn: each
open imported blocker gets one normal, budgeted re-admission batched per
Project, and escalation applies only after a post-upgrade turn for the same
digest. Decision wakes suppressed by their bucket, cooldown or a duplicate are
also reconsidered: an unanswered decision is level state.

Recorded outcomes are `agent_wake_blocker.recorded_outcome`, set by the native
and MCP `task.action` verbs `retry`, `release`, `restart`, `cancel`,
`send_back` and `approve` during the responder's live blocker turn, and the
persisted `task.transitioned` events that responder authored on a blocker Task
after the turn started. The owner escalation `need` is one bounded line per
blocker: its summary plus the Task title, status, role, failure kind and reason.
Escalation records, owner Notification and owner Attention commit together. An
owner answer (the answer endpoint, `forge-ctl project escalations answer`, the
Mission Control Answer box, or a generic Resolve of the escalation item, which
records "Resolved by the owner.") resolves that Attention and commits the
decision continuation event; the continuation turn becomes the escalated
blockers' turn, so a blocker still unchanged after it escalates again rather
than going silent. These paths read Tasks but never mutate Task state.

Frozen responder, Profile, operating-skill and policy provenance are reused on
ordinary turn retries. Deterministic provider schema/auth failures, and usage
limits on autonomous wakes, stop on attempt one. A lease expiry refunds its
attempt charge at most three times per turn (`lease_refund_count`); later
expiries count against `max_attempts`, so a turn that keeps killing its process
ends. The invocation ordinal and usage coverage gap are kept either way.
Server-owned Project doctrine revision @21 adds two short recovery/escalation
sentences and retains @20 and older immutable bodies for already-admitted turns.

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
records `material_blocker.requires_intervention: false`: an Agent must not undo an intentional
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

Forge read/proposal tools normalize arguments through the runtime's
`Tool::normalize_arguments` hook before validation against the frozen schema.
The CLI Chat callback uses that same hook and runtime schema validator. All
result/denial wrappers forward the hook. A normalization error is a tool error;
there is no retry with raw arguments. Advertised schemas contain the real root
fields and required lists, with no duplicate `parameters` envelope schema.

Accepted forms (preserved from the previous preparation code) are:

- Canonical root fields, with or without the optional read `arguments` object.
- A complete `{"parameters": {...}}` wrapper.
- Canonical fields split between the root and `parameters`, including identical
  duplicates or an empty wrapper. Conflicting duplicates or a non-object
  wrapper are errors.
- Generic proposals with nested `payload`, flat payload aliases, or a mixture
  of matching/disjoint nested and flat fields. Flat aliases can accompany an
  absent or null payload; non-null aliases build the canonical payload.
- Null flat aliases, which mean omission. Nulls already inside a payload remain
  unchanged; conflicting flat/nested values are errors. Existing nullable
  generic `causation_id` and `causation_depth` fields are preserved.

Only Forge read/proposal tools interpret a top-level `parameters` envelope.
Properties literally named `parameters` inside read `arguments` or proposal
`payload`, and such properties on other tools, remain data. The provider-facing
flat aliases and nullable generic fields remain advertised for all providers,
including Gemini; schema-dialect selection is a later change. Required fields
are now checked by the runtime after normalization, followed by the existing
preparation and domain checks. These failures remain correctable tool results.

Already-prepared checkpoint calls and pending approvals retain their exact
arguments, fingerprints, effects, and authority: recovery reauthorizes the
stored preparation without normalizing or preparing it again. Approval edits
are new arguments and go through normalization, schema validation, preparation,
and authorization again. A pre-upgrade session retains its stored history,
admitted doctrine and receipts. On restart, Forge composes the current tools
for its existing server-bound scope; subsequent provider requests use the
smaller schemas. In-flight runtimes retain their frozen registry until they
stop. The changed tool prefix changes the next provider request's cache
fingerprint. No session migration, effect replay, or authority widening is
introduced.

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

Wake decisions have no semantic retry rows, strike counts or dead letters:
Attention admits the turn, its audit event, disposition and budget charge in one
transaction, and the level-triggered sweep reconsiders whatever is still open.
A failing scope is logged and retried on the next sweep. Initial admission,
disposition and decision-incident resolution commit together. Consumer
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
full checks finish. Handshake re-verification skips locations still referenced by
`repo_provision_retry`: provisioning owns their verification and full-check fence
until settlement, including after reconnect or server restart.
Check failures retain per-role facts; successful completion
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
**The workspace manager is the validity accessor.**
`services::workspace_manager::WorkspaceManager::ensure_valid(task, workspace,
purpose)` is the one place a `ready` row becomes a directory a caller may run
in. It never writes Task rows; it returns a `ValidWorkspace` (the row, its
resolved placement, the checked path for a server placement, and what was
repaired) or a typed `WorkspaceUnavailable`, which converts to the existing
service errors (`ResetRequired` to `WorkspaceResetRequired`, `OwnerUnreachable`
to `DaemonUnavailable`, `Busy` to a conflict, `Infrastructure` to the original
error unchanged). `Purpose` is `Execute`, `Review`, `Check`, `Integrate`,
`Hook`, `Reset` or `Inspect`. For a server-owned workspace it checks, with no
network and one Git process on the healthy path (a single `git rev-parse` that
resolves HEAD and names the repository, the Git directory and the checked-out
ref; the older HEAD probe runs only when that fails, to tell an unusable
directory from a transient Git failure):

| Observed | `Execute`, `Review`, `Check`, `Integrate`, `Hook`, `Reset` | `Inspect` |
|---|---|---|
| Linked worktree, HEAD on the Task branch (or mid-rebase) | valid; a stale cleanup deadline is cleared | valid |
| Directory missing, or present and unusable by Git | repaired or recreated from the Task branch (as above) | `Absent`; disk and the workspace row are untouched (a legacy row with no placement still gets its server placement recorded) |
| Linked worktree of a repository other than the recorded one | moved aside as `<name>.broken-<ms>` and recreated from the Task branch; never relinked. When no recorded repository has the Task branch (a worktree made before its Repo moved to another location), the worktree is the only home of the Task's work and is used as it is | `Absent`, or valid in the same exception |
| The worktree path, or its Task root, is a symbolic link | `ResetRequired`; nothing is used, moved or deleted | `Absent` |
| HEAD on another branch or detached, Task branch exists, no rebase in progress | Put back on the Task branch without losing a commit. HEAD is the Task branch's own commit: `git checkout <Task branch>`; no file changes and local changes are kept. HEAD is strictly ahead of the Task branch (commits made on a detached HEAD, or after an interrupted rebase finished): the Task branch is advanced to HEAD with a compare-and-swap `git update-ref` and checked out; no file changes, local changes are kept (`Repair::FastForwardedTaskBranch`). HEAD and the Task branch have diverged: HEAD's commit is kept under `refs/forge/rescued/<task id>/<UTC timestamp>` in the worktree's repository, one Forge comment on the Task names the ref and the commit, and the Task branch is checked out (`Repair::RescuedOffBranchCommits`); the checkout is never forced, so when it would overwrite uncommitted changes nothing moves and the result is `ResetRequired` naming the ref. One ref and one comment per rescued commit, however often the state is seen. HEAD is behind the Task branch: `Execute` checks the branch out when the tree is clean, else `ResetRequired`; the others return `ResetRequired`. `Reset` is the exception to all of these: it returns the worktree as it is, flagged off-branch, for the caller's `git reset --hard HEAD` | valid, flagged off-branch |
| Worktree and Task branch both gone | `ResetRequired`; the row is kept, except on the owning Task's launch path, which forgets it so the next launch starts from the default branch. A worktree of another repository that Git can still use is never forgotten | `Absent` |
| Row not `ready` | `workspace for task … is not ready` | reports the disk as above |

The repository check compares the worktree's Git common directory with the
placement's repository location, `Repo.local_path` and Forge's clone, whichever
are on disk; with none on disk it is skipped and recovery reports the missing
repository. A healthy worktree is valid even when its Repo row has been
deleted (a workspace outlives it); only a repair needs the row and reports
`repo not found`. A Repo row of another Project is refused for every state. A directory that is a repository of its own, rather than a linked
worktree, is not rejected yet. Daemon-owned placements keep the describe /
prepare contract described above; the manager never interprets their handle.
Callers: every claim on a `ready` server placement (`Execute`; the launch guard
all executor families share, CLI executors included), every `prepare_workspace`
path (`Execute`: workspace creation, the reviewer cascade, retry-entry
refresh), the execution runner at launch and the provider start parameters
(`Execute`), the native executor at the start of a turn (`Execute`, with the
Task service's workspace root and repository-cache locks), reassignment reset
(`Reset`), review entry CI (`Check`), review rerun (`Review`), blocking
`before_work` hooks (`Hook`), merge delivery and target-moved rebase
(`Integrate`), and three read-only users (`Inspect`): the lifecycle emitter,
the terminal before a shell opens, and evidence capture from a worktree file.
A claim on a daemon placement still asks its owner's `describe`. One Task run
with one coding execution and one review makes about seven to ten manager
calls, each one Git process on a healthy worktree: claim (1, replacing the
three Git processes of the backend `describe` it used before), runner launch
(1), native turn start (1 per turn, native executors only), a blocking
`before_work` hook (1), entry CI (1), review rerun or reviewer cascade (1),
delivery (1), a rebase when the target moved (1), and one `Inspect` per
lifecycle event that has script hooks.

Repairs happen inside the call, once: a step that finds its directory gone or
unusable gets it recreated from the Task branch and proceeds; a worktree off
the Task branch is put back as in the table. None of this writes a Task row,
so no review or retry budget is spent. What is left as `ResetRequired` cannot
be repaired without losing something or without an operator decision (the
Task branch is gone too, a symbolic link in the path, HEAD behind the Task
branch under a step that must not move the tree, a checkout that would
overwrite uncommitted changes). Callers do not retry it: each records its
existing typed result and stops, so there is no reset loop. A claim fails the
Task with `workspace_reset_required` / `workspace_failed`; a blocking
`before_work` hook records the `workspace_reset_required` annotation and
fails; review entry CI records the CI interruption with its entry barrier
without charging the CI infrastructure budget; delivery and rebase return a
merge failure of kind `workspace_error`. The explicit workspace reset, then
the Task's `retry`, clears it.

Files that sit beside the worktree in the Task root (the canonical plan,
staged plans, execution outboxes) are located through
`workspace_manager::task_root_anchor`. They outlive the worktree, so they are
read and discarded whether or not the worktree is usable; the readers confine
each file to the Task root themselves. A source-scan test,
`workspace_manager::tests::raw_workspace_path_getters_have_no_new_callers`,
records an upper bound per file for the remaining non-test uses of the raw
path getters (`ResolvedWorkspace::embedded_path`,
`Workspace::embedded_worktree_path_for_backend`;
`EmbeddedWorkspaceBackend::recorded_server_path` no longer exists). It fails when a file that
is not listed uses one, or a listed file uses more than recorded; fewer uses
pass. `raw_workspace_path_getter_uses_are_exactly_the_recorded_ones` pins the
exact set: the manager and the owner-local backends, plus `merge_service.rs`
(2), `task_actions.rs` (1) and the admission-failure cleanup in
`task_service.rs` (1).

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

Protocol revision 5 independently negotiates `machine_probe.v1` for
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
run-purpose enum does not recognize them. These capabilities did not change
the protocol revision; the current revision is 5.

Protocol revision 5 negotiates `workspace.v1` for `repo_location.verify`,
`workspace.prepare`, `workspace.describe`, `workspace.run`, `workspace.diff`,
`workspace.read`, `workspace.merge`, `workspace.reset`, and `workspace.cleanup`.
Plan-writing roles on a daemon-owned workspace require `execution.plan_transport`.
A revision-5 daemon without it can still run reviewers, interactive executions,
server-owned shared-mount executions, filesystem requests and PTYs. Deterministic
placement refusals record a structured Task annotation naming the machine and
missing capability. Dispatch waits until eligibility facts change, then clears
the refusal and retries.
Upgrade the server first, then every daemon using `forge-ctl` from that server
release (protocol revision 5 or newer), restarting each with its existing
`--workspace-root`.
A connection below revision 5 receives `daemon_upgrade_required` and cannot use any
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
reconnects at revision 5, waking Task dispatch automatically. Upgrading the daemon
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

### Owner-requested machine removal

Disconnected external daemon registrations can be removed by their owner or an
administrator. The removal transaction retains the
UUID/hostname as a tombstone, retires the unique registration key, revokes the
bearer credential, clears daemon cancellation/cleanup/provisioning/readiness
records, and makes locations/runtimes unavailable. Operational queries exclude
tombstones; historical joins retain names and execution/usage/transition evidence.
Conditional report, online, runtime and remote-operation writes fence late work.

Removal is permanent owner loss, so nothing waits for the machine. The
transaction releases every live placement the machine owns (`cleaned`, no handle,
workspace `cleaned` with a `machine_removed:` error, an earlier failure cause
kept): that is the existing "nothing exists, reselect" state, so the Task's next
admission rewrites the owner through ordinary selection and prepares a fresh
workspace elsewhere. A server-owned placement that only executed there keeps its
state and just drops `execution_daemon_id`. Agents pinned to the machine are
archived in the same transaction. The facts each Task needs (its live runs on the
machine, whether its workspace was lost, the removing user) travel in the
identity-fenced `settle_removed_machine` step, because the released rows can no
longer answer them.

That step runs under the Task lease. It fails the recorded runs through the
owner-loss terminal CAS and usage ledger as the removing user, clears the
pending-cancel marker when no other owner fences the Task, ends an owner or
environment wait on that machine, replaces an owner-recovery annotation with
`machine_removed`, and unassigns a retired Agent through the archive role sweep.
It then admits the Task again: an accepted action is replayed through the normal
recovery command, an accepted `owner_reconcile` retry is replaced by the ordinary
Retry the Task now offers, and lost work queues that same Retry. Placement then
selects another machine or parks with the ordinary placement wait. No Task
workflow field is written by the removal transaction. Cleanup after revocation
abandons owner-local files and retires server bookkeeping without an owner RPC.
A durable `machine.removed` event records the user actor and removal counts in the
same transaction. The immutable command-receipt delete guard permits only daemon
workspace cleanup receipts for an already tombstoned registration to be cleared.

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
rebase, and sends the Task to `merge_failed` classified as `bridge_kind = conflict_handoff` with
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
typed review-refresh transition, clears the old approval, and runs the
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

### Task condition dual-write foundation

`V202610060030__task_condition` adds one private, non-null `condition_json`
column and an expression index on its tagged enum kind. Stages one and two
established the shadow and dual-write adapters. Stage four makes its typed
presentation authoritative for public readers: health, human-readiness,
exceptions, offers/apply, slots, overview, operations, MCP/native/Solo,
Attention and environment surfaces. Legacy columns and condition metadata are
still written by the adapters and retained for import, rollback and invariant
repair until stage five. They are not public-reader inputs. The original
migration preserves Task versions/timestamps, retained queue intents,
publication claims and budget receipts, including archived/deleted Tasks and
malformed historical JSON.

The internal `db::TaskCondition` variants are `Clear`, `Entering`, `Running`,
`Deferred`, `Parked`, `Failed` and `Settled`. Parks carry one primary typed reason,
ordered secondary reasons, a continuation and bounded legacy evidence. The
legacy-only fallback derives `Clear`, `Deferred`, `Parked` and `Failed` from the
five legacy inputs. `Entering`, `Running` and `Settled` require durable
ownership/workflow witnesses. An entry barrier without a `blocked` status has
no owner, and legacy refuses to dispatch through it: it parks as a named
`UnknownCondition`. No custom workflow hooks are retried.

**One mapping, in Rust.** `db::map_legacy_condition` is a pure function of the
five legacy values and optional durable witness snapshot. The migration's backfill (a Rust post-step in the migration
transaction), every writer seam and the invariant check call it. There is no
mapping view and no trigger: an earlier draft compiled the mapping into every
Task write statement through triggers, which cost about 110 ms per statement
prepare and 0.6 to 1.2 ms per write. The column default is the stored form of the
empty mapping. It carries no witnesses, so the first producer to touch such a
row recomputes it in full.

**The mapping parks only what legacy blocks on.** A Task is `Parked`/`Failed`
only where today's dispatcher and readers treat it as blocked:

- `blocked_json` or `failed_json` present (any value, including malformed);
- an `error_annotation` whose `type` is one of
  `db::LEGACY_BLOCKING_ANNOTATION_KINDS`, the same list
  `task_dispatcher::helpers` uses, parsed the way the dispatcher parses it;
- an entry barrier of any shape, blocked or not, malformed or not: the active
  dispatcher, failed-review recovery, the merge-hook sweep and root advance
  all stop on a non-null barrier;
- `metadata_json` that is not a JSON object: both dispatcher scans fail
  `task.metadata()` on it and skip the Task every tick;
- a hold, or a correctly typed wait recorded in metadata (owner, environment,
  placement, upgrade, pause, approval, capacity, dispatch refusal, plan
  settlement).

Any other annotation (`merge_conflict`, `ci_failed`, `executor_failed`,
`retry_exhausted`, `{}`, untyped or unreadable text) is evidence on a
non-parking condition, as is a `deferred_dispatch` object that is not a usable
timer. `agent_timeout` maps to its own typed reason. Explicit failed-process
metadata keeps precedence; holds and exhausted budgets precede ordinary
failures. Same-cause annotation/block rows deduplicate. Generic
`retry_exhausted` keeps an unresolved budget kind until a producer supplies the
ledger witness. A BLOB or invalid UTF-8 in a legacy column and malformed or
non-object legacy values become named `UnknownCondition` reasons. Only two
shapes are non-parking observations, because legacy reads them as absent: a
wrongly typed wait key (every reader type-checks the value) and a non-text
annotation. None fails a write or backfill. Unrelated metadata keys stay with
their owner and invent no park.

**Evidence is bounded.** Each copied legacy value, and each of the 24 condition
metadata fragments, is capped at `db::EVIDENCE_VALUE_LIMIT` (4 KiB) with a
truncation marker; the legacy columns keep the full data. Metadata is scanned
one level deep with values left as raw text, so an unrelated large key is
skipped, never decoded or re-serialized, and large numbers, lone escapes and
deep nesting survive in their fragment. Mapping cost is linear in the size of
the five legacy values.

**Dual write at the writer seams.** Each writer of a legacy condition column,
or of a durable fact the condition witnesses, writes the shadow in the same
transaction as its own write. Shadow writes change only `condition_json`: no
version, timestamp, list/board revision, event, Attention incident or wake.
The seams, and what each one reads, are in the next section.

Queue intent/claim/receipt **bodies** stay out of condition evidence. A queued
recovery continuation retains only its intent id; the original command remains
its execution/rollback authority. Raw legacy annotations are kept only as
private import evidence, not advertised action lists or new public aliases. No
condition payload is fed to Attention in stage one, so incident digests and
consumed wake decisions are unchanged.

### Task condition producers and invariant repair

Stage two is still an internal dual write. The legacy fields stay
authoritative: no reader, dispatcher, recovery or Attention decision reads the
condition. The condition must say what legacy does today. A Task legacy holds
maps to a parking condition (`Parked`, `Failed`); a Task legacy runs maps to a
non-parking one.

**Witnesses are the facts.** A stored condition carries every durable fact it
was built from in `evidence.witnesses`, always starting with the entry:

| Witness | Fact | Changes when |
|---|---|---|
| `entry` (always) | status epoch, entry transition and its time | status write, entry receipt, reparent |
| `step` | the pending or claimed `hooks` step of this exact status and epoch | hooks step enqueued, superseded, finished |
| `execution` | the Task's newest running non-interactive execution, when this entry owns it | a running execution admitted or ended, its config snapshot changed, entry change |
| `budget` (exhausted conditions only) | ledger kind, window and spent | charge, window reset |
| `operation` | each unconfirmed remote cancellation fencing the Task | marker, acknowledgement, machine removal, workspace deletion, an execution admitted into a fenced workspace |
| `child` (while `coordination_review_pending`) | each visible child, its status and whether it is settled | child status, creation, deletion, reparent, reorder, Project workflow edit |

`Settled` records the terminal outcome. A condition without an `entry`
witness (the column default, an older encoding) carries nothing and is
recomputed in full.

`Entering` needs a `step` witness, `Running` an `execution` witness,
`Settled` a terminal state in the Task's effective workflow. Explicit blockers
win over live workflow work. Interactive sessions are never the witnessed
execution, and a settled execution is history, not a witness: recording one
writes no condition. The running execution is read from the running-only
partial index `idx_execution_running_task`, so no producer walks or
sorts a Task's execution history.

The entry is the first transition row stamped with the Task's status epoch
(the engine CAS and board moves stamp it). Without one it is the newest
unstamped row that changed state: a claim's receipt, or a pre-upgrade entry.
A same-state row (a recovery marker) is never the entry and carries no epoch,
so the stranded-hook recovery in `workflow/engine/durable.rs` reads exactly
what it read before the shadow existed. An execution names the receipt it was
admitted under (`state_entry_token`); the entry owns it when that receipt is
the entry or a later receipt of it, stamped with this epoch or unstamped and
not older than the entry. That is how a claim's and a retry marker's
executions bind without a stamp.

**Producers state a condition from what their write changed.** A producer
names the fact family its write can change (`db::ConditionChange`). It reads
the Task row and that family in one statement, carries every other family from
the stored condition's witnesses, re-applies the legacy mapping and writes the
result through the version-fenced `set_condition` write. A budget charge reads
no execution, a status write reads no ledger, and a Task with neither parent
nor children reads no other Task. The Project workflow's terminal states are
parsed once per distinct definition text, not per write.

| Writer | Change | Reads beside the Task row |
|---|---|---|
| The six metadata mutators (condition keys only) | `Legacy`, folded into the metadata `UPDATE` | nothing |
| Repository update, annotation, recovery metadata, entry barrier setters, Review entry budget, create-time metadata, lifecycle hook annotation, queue settlement and loop park, reconnect barrier reset | `Legacy` | nothing |
| `TaskQuery`, `BulkTaskQuery`, queued `TaskMutation::Sql` | `Legacy` or `Entry`, from the columns the statement assigns; none for a statement that assigns no mapped column | per change |
| Status CAS (repository, claim, board move, workflow engine), transition and recovery-marker insert, reparent | `Entry` | entry receipt, hooks step, the running execution, the workflow text, whether the parent waits on children |
| Hooks step enqueue, owner preempt that superseded a step, hooks step finish | `Hooks` | the entry's hooks step |
| Running execution admitted, execution terminal result (both paths), config snapshot update | `Execution` | the running execution |
| Execution recorded already settled | none | nothing |
| Ledger charge, window reset | `Budget` | the ledger, only when the condition is exhausted |
| Remote-cancel marker and acknowledgement, machine removal, workspace deletion | `Operations`, for every Task the workspace fences | the cancellations fencing the Task |
| Child creation, soft deletion, reparent, both subtask reorder writers | `Children` on the parent | the parent's children, only when its flag is set |
| Project workflow edit | `Entry` for Tasks whose status classifies differently under the new definition, `Children` for flagged roots | per change |
| Task insert, `sync_condition_in_tx`, backfill, invariant check | `Full` | everything |

A producer that finds a status epoch the stored condition never saw recomputes
lifecycle facts in full, carrying only independently owned integration lineage,
so a writer that under-claims cannot leave a stale entry behind a later write.

The workflow engine and the worker's step settlement have already fenced
their step lease, and state their condition through `SqliteDb::set_condition`
when the Task's step is current. That strict entry requires the claimed step
and live lease, rejects an expected-version mismatch, compares all five source
values and refuses a condition that disagrees with legacy-derived reasons or
its typed integration witness. It does not re-read those witnesses: verifying them is
the invariant check's job, so stating a condition costs no fact query. When
the step is no longer live (the transaction has already settled it), the
legacy write still stands and the condition is written under the version
fence alone; a condition the strict entry refuses outright is logged and not
written. Other writers hold no step (Task creation, bulk clears, admission)
and use the version fence alone, adding no rejection to a legacy write.

Four writers are authoritative on their own and must never fail for the
shadow: a ledger charge, an execution's terminal result, a step settlement
and a remote-cancel marker. There a failed shadow write is logged and left to
the invariant check.

A new direct SQL writer of a legacy column or of a witnessed fact must call
the seam; nothing in the schema enforces it. The test safety net is
`SqliteDb::task_condition_violations()`, the full recompute, which independently reloads lifecycle facts and carries
only the direct integration statement witness: the writer-family and producer tests and
the `happy_path`, `e2e` and `remove_machine` api cases assert it is empty.

**Mappings that follow legacy.**

- `coordination_review_pending` with unfinished children parks on them
  (`Children`); a complete sequence is a deadline-free `Deferred` advance. A
  child counts as finished when its status is terminal in the inherited
  subtask workflow or in the Project workflow, as `subtask_is_terminal`
  decides. A flagged root with no visible child is `Clear` with an
  observation: legacy dispatches it as an ordinary Task.
- A pending remote cancellation parks exactly the Tasks
  `task_has_pending_remote_cancel` fences: the workspace's owner and every
  Task with an execution in that workspace. `db::remote_cancel::TASK_FENCE` is
  the one predicate behind the dispatcher refusal, the owner-facing machine
  list and the condition. The cancelled step's own Task is not parked unless
  that predicate fences it; a sibling that ran in the shared root workspace is.
- Environment kind/reason waits without a deadline are `Deferred`
  observations rechecked by existing dispatch. Advisory merge/CI/dirty-worktree
  annotations stay non-parking where legacy proceeds.

**The invariant check.** The dispatcher's sweep runs it (see "Task condition
scheduler and owner matrix"): once per 120 seconds it visits every Task that
is not settled, in keyset pages behind the dispatch pass, reads each page on a
reader connection, recomputes each Task in full and compares.
It takes the writer only for a row that differs, with one statement fenced on
the Task version and the condition text it read, so a row written since is
left to its own producer. A row that cannot be recomputed is logged and
passed over. It writes only `condition_json`: never workflow state, version,
time, event or wake. Errors log and defer without stopping dispatch.

An entry barrier parks in every state but an initial one: the scheduler
admits a Task from an initial state without reading the barrier, so there it
is evidence and an observation, and the `entry` witness records `initial`.

`db::MAPPING_REVISION` versions the mapping and the stored encoding. The
revision last backfilled is recorded under the protected `system_setting` key
`task_condition_mapping_revision` (hidden from, and not writable through, the
admin settings API). The migration that creates the column records it after
its own backfill. When it differs at the first tick (a database migrated by an
earlier binary, or a later mapping change), the backfill is re-run over every
Task, settled ones included, in slices of at most 250 ms per tick, off the
startup path, and the revision is recorded when one pass completes. Bump the
constant with every mapping or encoding change.

Every completed pass logs its checked and repaired counts. Operator status
reports a `task_condition_invariant` entry in `recent_errors`, severity
`attention`, only while the last completed steady-state pass repaired at
least one row: that means a producer missed its write. A clean pass, and the
rewrite after a mapping change, add no entry.

**Cost.** Measured in a release build against `d6445863` with
`crates/db/tests/task_condition_write_cost.rs` and
`crates/db/tests/task_condition_producer_cost.rs`, interleaved on one machine:

| Path | `d6445863` | this change | change |
|---|---:|---:|---:|
| ordinary metadata (`mutate_metadata`, unrelated key) | 97 µs | 95 µs | −2% |
| `mutate_metadata`, condition key | 97 µs | 108 µs | +12% |
| `TaskQuery`, SQL-computed metadata | 105 µs | 105 µs | 0% |
| `update_status`, root | 234 µs | 259 µs | +11% |
| `update_status`, child | 256 µs | 281 µs | +10% |
| `budget::charge`, root | 108 µs | 120 µs | +11% |
| `budget::charge`, child | 109 µs | 119 µs | +10% |
| execution recorded settled, root | 129 µs | 130 µs | 0% |
| execution recorded settled, child | 146 µs | 144 µs | −2% |
| entry hooks step: enqueue, claim, finish (three transactions) | 422 µs | 478 µs | +13% |

Medians of eight interleaved runs of three rounds each. Tasks carry 40 logged
transitions and 10 executions, growing by 200 executions per round; the
Project workflow is the 16 KB definition API-created Projects store. With a
`{}` workflow every row is within one point of these except the hooks step,
which is +17% there; its minimum over the runs is +18% under both.

There are no mapping triggers.

`material_blocker(condition)` is the interruption and requires-intervention
flag the `task.interruption_changed` event states, captured before evidence
truncation and stripped by `db::strip_attention_delivery_metadata`, the rule
the incident digest applies (so the reporting execution is not part of a
blocker's identity). Witnesses, tags and bookkeeping never enter it. Tests
compare it with the event, and with the canonical digest of incidents the
Attention service really materializes for every blocker shape. Attention
still consumes its original events, so rollout re-arms no incident. Stage
three owns the total reconciler and dirty-set cutover.

### Task condition public readers (stage four)

The stored condition now carries a normalized typed presentation captured by its
producer before import-evidence truncation. Runtime public readers consume this
presentation and typed reasons, not private raw evidence or migrated metadata.
`ConditionChange::Human` refreshes Review and assignment changes in the same
transaction; entry, execution, hooks and workflow changes refresh their current
human-work facts. Generic human intervention remains distinct from approval
eligibility. A pending entry owns its checks and suppresses obsolete human waits.
List and detail project the same wait; health can overlay a live interactive run
without changing the Task condition or its blocker.

`V202610070507__task_condition_readers` rederives every existing condition inside
its migration transaction (mapping revision 4), replaces the list-revision trigger
and retry-time partial index, and preserves all Task/history/Attention/queue/budget
data. It clears only the retired dispatcher bridge, retaining its diagnosis in a
park when needed. Condition-only changes advance `project.list_revision`; ETag
projection version 2 prevents an old list validator from surviving the wire cutover.
The clock-sensitive probe uses `idx_task_condition_retry_project` and the condition
recorded retry/deferred-wait flag, including a timer co-occurring with another primary reason.

REST and forge-ctl Task JSON replace `error_annotation`, `blocked`, `failed` with
`api_types::TaskCondition`; MCP and native Task values use the same DTO. It excludes
raw import evidence, metadata and witnesses. Named health, exception, human wait
and offers stay. Entry owners show `Entering`; observer parks show `Needs Owner`.
Failed Review history after a newer entry or live continuation is not a current
exception. An undecodable stored condition returns a typed unknown diagnosis: corrupt or empty values are restated from the legacy columns, a recognisably newer encoding is quarantined (see "Undecodable stored condition"); private historical Task receipts import their condition at the archive boundary. Normalized diagnostic strings are bounded to 1,024 bytes and arrays to
16 entries; legacy storage retains full originals during this stage.

`task.interruption_changed` retains its type and carries `condition` plus the typed
`material_blocker`. The old payload decodes only in `DomainEvent::task_material_blocker`
at the immutable archive boundary. New condition/step identities, Task versions,
timestamps, reporting execution IDs and offer changes never enter incident identity.
Unchanged open incidents do not create a new incident or wake during upgrade.
Held, entry-owned, in-flight, capacity and retry-timer conditions are not autonomous
repair incidents. Admitted `OwnerOffline` retains an active slot; `ReviewNeedsOwner`
retains a parked slot. No scheduler decision or sweep policy changes.

Stage five must retain this typed presentation/material identity while deleting
legacy columns and migrated keys and replacing import/dual-write adapters. Full
legacy originals that exceed the presentation bounds must be preserved at an
archive boundary before storage removal. Drop/recreate the Task-related triggers
and indexes around any Task table rebuild; do not lose scheduler dirty generations,
parks, wait/deadline rows, queue receipts, pending cancellations or immutable events.

### Task condition statements (stage five A, in progress)

Stage five is split. **5a** has no schema change: a writer states the typed
condition its write causes and keeps writing the legacy field exactly as
before. **5b** drops the legacy columns and keys once nothing reads them.

A writer states through `db::ConditionStatement`, passed with its repository
call and applied in the same transaction by `task_condition::state`. The
statement edits the stored condition: it replaces the reasons its writer owns
and carries every other owner's reason (an entry block, a placement denial, a
human gate, a Project pause) as stored. The durable facts (entry, hooks step,
running execution, children, cancellations, ledger) are then laid over it by
the same `ConditionFacts::apply` every producer uses. The fields the writer
wrote are not mapped back. A stated condition has `evidence.stated = true`
and none of the six legacy copies (`error_annotation`, `blocked_json`,
`failed_json`, `entry_barrier_json`, `metadata`, `unparsed_metadata`).

Converted writers:

| Writer | Statement | Owns | Carries |
|---|---|---|---|
| `TaskService::hold_waiting_task` (owner Hold on a Task with no run) | `Hold { actor, reason, at }` | `Held`, the diagnostic, the interruption; drops failure, queued command, retry timer, dispatch refusal, environment wait, owner wait | entry block, placement denial, daemon-upgrade refusal, Project pause, human decision, plan settlement wait |
| `TaskService::release_to_dispatch_queue` (owner Release of a waiting hold) | `Release` | clears diagnostic, interruption, failure | everything else, including a recorded retry timer |

The hold's legacy annotation and the operator text of its condition are one
rendering of the same typed fields (`ConditionStatement::hold_operator_text`),
byte for byte what the annotation held before.

Every other writer still goes through the mapping (`map_legacy_condition`
under `ConditionFacts`), including the other hold paths: stopping a running
execution (`persist_manual_stop_annotation`) and releasing a stopped run. A
mapped write over a stated condition remaps legacy-derived reasons and carries
independent integration ownership. For legacy-only conditions the result agrees
with the mapping by construction (the per-writer
equivalence tests in `db/src/task_condition/statement_tests.rs` compare the
two after every converted write).

While legacy is still written the old mapping stays the oracle. The invariant
check, `check_task_condition_invariant` and `task_condition_violations` accept
a stated condition when it equals the mapping in everything but those six
copies and the `stated` mark (`TaskCondition::typed`); anything else is
repaired as before. The dispatcher does not treat a stated condition as a
stale copy of the legacy fields, and holds on its typed presentation
(`interruption_present`, `hard_failure`, `entry_recorded`) where it held on
the copies.

**Undecodable stored condition.** `task_condition::classify` puts a stored
value this build cannot decode into one of two classes. Row readers expose
either as an `UnknownCondition(ConditionJson)` park whose `problem` names the
class.

- *Corrupt or empty* (`malformed_json`, `non_object`, `invalid_shape`,
  `non_text`): invalid JSON, a scalar, `[]`, `{}`, a known tag with a broken
  body, and anything else that is not recognisably a newer encoding. It is
  **restated** from the legacy columns and durable facts, which every legacy
  writer still writes: by any producer, by a Hold, Release or integration
  statement (which falls back to the mapping, having nothing to edit), by the
  migration backfill, by the invariant check, and by the scheduler, which
  resolves such a Task from its legacy fields and asks the check to repair it.
- *Recognisably newer* (`unknown_kind`): a well-formed tagged value naming a
  variant this build does not know (the condition's own tag, or the tag of a
  reason, continuation, witness or integration reason inside it), or any
  undecodable value in a database whose recorded mapping revision is higher
  than this build's. It is **quarantined**: no producer, statement, backfill or
  check rewrites the bytes. A Hold, Release or integration statement over it
  returns `DbError::TaskConditionQuarantined` from inside the writer's
  transaction, so the legacy annotation and the version bump roll back with it;
  the error maps to `ServiceError::TaskConditionQuarantined` and HTTP 409
  `task_condition_quarantined`. A legacy writer that states nothing still
  lands, with its producer skipped. Each completed check pass counts the
  quarantined Tasks and keeps the first 20 ids; one warning per pass names
  them (not repeated while the set is unchanged), the backfill warns the same
  way, and operator status reports the count and ids. A database whose
  recorded revision is newer is not backfilled and its marker is not lowered.
  There is no repair path in this build: the exit is running a build that
  understands the encoding.

A readable but stale encoding remains repairable under the usual
version-and-stored-text fence.

**Size.** A stated condition holds typed fields and presentation text cut at
1,024 bytes: 8 KiB is the tested ceiling for a hold with a 200,000-byte
reason. A mapped condition still copies each legacy field into `evidence`
cut at 4,096 bytes; 24 KiB is its tested ceiling for the same reason.

### Integration-owned conditions (3.2 stage A)

Stage A extends the statement seam only. No production caller emits these reasons;
merge/rebase/CI/fast-forward, legacy writers, budgets and completion semantics are
unchanged. Stage B below adds passive queue/attempt storage; there is still no queue worker or path guard enforcement.
`IntegrationAttemptId` is an opaque typed string, never queue rank or worker token.

`ConditionStatement::Integration { reason }` replaces only integration's reason,
and is fenced on its attempt: an attempt restates its own reason, and a
different attempt is refused unless no attempt owns the row (none stated, the
previous one cleared, or handed off to a role).
`IntegrationCleared { attempt_id }` clears only the matching attempt.
`IntegrationHandedOff { attempt_id }` makes a repair/review continuation ready
for ordinary role admission and recovery while retaining its lineage. All go
through `SqliteDb::state_integration_condition_in_tx` and `set_condition` under a
live Task-step lease, with version, legacy-source and status-epoch fences. They
write only condition JSON, never the legacy pause/deferral/diagnostic/barrier fields.
No queue worker acquires Task-write authority through this seam. The lease
check is the generic step fence's: a claimed step with an expired stored lease
is refused unless this process still holds the step as active.

**A statement writes no event (stage D requirement).** A statement changes no
Task version and appends no `domain_event`, while incidents and agent wakes key
off `task.interruption_changed`, which only legacy column writes emit today. So
an intervention-grade `Deferred` (dirty target, exhausted budget, owner
required) stated on its own raises nothing. The stage D consumer that states
such a reason must emit `task.interruption_changed` in the same transaction,
under the Task step lease.

The seven typed `IntegrationReason` variants are Waiting (the attempt only),
Owned (typed phase), Repair (predecessor attempt, and a bounded sample of the
conflict paths and of the repair-touched paths), ReviewRequired (authority
reason), CandidateCheckFailed (check and message), Deferred
(infrastructure/owner/offline/dirty-target/budget/unresolved-result cause, with
owner and message) and Applied (result awaiting Task-step consumption).

What a reason deliberately does not carry, so stage B does not have to take it
back out:

- **No queue position in any form.** Waiting names no earlier attempts: every
  queue advance would otherwise rewrite every waiter's condition.
- **No clock.** Deferred says that it is deferred and why. Retry times and
  deadlines live on the attempt row.
- **Bounded lineage.** A path set is `IntegrationPaths { count, paths,
  truncated }`: the size of the whole set and at most 32 of its paths. The full
  set lives on the attempt row. Unknown conflict paths are an explicit `None`.
- **A settled Task keeps only the attempt id**, as an `IntegrationLineage`
  witness; the reason, its paths and its cause are dropped at settlement.

There is no guard expiry or lease metadata in a reason. Stage B owns operational
state and must bind these identities to its attempts; stage D owns effects and
Task-step acknowledgments.

A typed Integration witness survives every producer, even a full recompute or
status-epoch change. Legacy, Human, Hooks, Budget, Entry, Execution, Operations
and Children refresh their own fact families and carry integration. Hold/Release
carry integration independently of their diagnostic cleanup. Invariant repair
loads the supported stored witness and remaps only legacy and other durable
facts. Terminal settlement remains Settled, keeping only the attempt id. A live
execution always reads as Running, handed off or not: the Task is never
presented as parked on integration while it runs, the reason stays on the
evidence, and it is the park again when the run ends. An entry hooks step and a
retry timer are the Task step's own bookkeeping with nothing running, so an
integration wait parks over them (Entering and Deferred become
Parked-on-integration); the step keeps its witness and the timer its legacy
column, and each is the condition again once integration is cleared. Actual
coder/reviewer execution or hooks after a Repair, ReviewRequired or
CandidateCheckFailed handoff owns Running/Entering; those handoffs
retain the witness rather than pretending the integration worker runs an agent.
The explicit handoff-ready witness also keeps an idle role ready after its hooks
or execution finish: it does not revert to a queue-owned wait. Changing phase or attempt resets that handoff; updating repair paths keeps it.
Hold/Release and all producers carry it.
Other owners' parks remain primary, with integration secondary.

The material mapping revision is **5**. After upgrade the bounded background
backfill visits every row, settled ones included, once and records revision
5. Existing legacy rows acquire the current witnesses; existing integration
statements survive; corrupt rows are restated and rows a newer build wrote
remain quarantined rather than overwritten.
No migration is needed because condition JSON already stores typed statements.
Legacy equivalence is required for legacy-derived reasons, while integration is
validated from its statement witness, never inferred from legacy merge markers.

`next_step` returns a named IntegrationWorker wait when integration is the
primary reason, before ordinary recovery/admission. It never chooses
paused-integration's `Step::Integrate`, fake Running, fake Entering or a
capacity-unpark demand. The snapshot carries the condition witness without
stage-B reads. `next_step::integration_decides` is the one predicate for this,
shared with reconciliation's admission skip:

- integration primary: the IntegrationWorker park;
- a real owner blocker primary (a hold, a blocked entry, a failure, a human
  decision) with integration behind it: the blocker's own park and owner, e.g.
  User/ReleaseHold. Nothing the scheduler does clears those;
- a self-clearing primary (a capacity wait, a dispatch refusal, an offline
  owner, an environment wait, a pending placement refresh or owner-wait expiry)
  with integration behind it: the Task resolves exactly as it would without the
  integration reason, so the step that clears the primary runs and admission is
  observed. Integration stays as a retained secondary reason and becomes the
  primary, and the IntegrationWorker park, once the primary clears. A capacity
  park in this case is the capacity reason's own; an integration wait raises
  none.

The reconciliation sweep counts any integration wait as owned. Custom workflow
states, including an extra state and a merge state under another name, have the
same named park, preserving totality.

Ordinary queued/head/path/repair/review/result/infrastructure waits have
`slot_blocker=false`, keep an **active Project slot**, and consume no execution
capacity. OwnerOffline parks its Project slot quietly. TargetDirty,
BudgetExhausted and OwnerRequired are actual intervention blockers and count
parked. Their material inputs are cause, failure kind, owner and message only.
Attempt IDs, path sets, phase, rank, queue revision, lease deadline, worker token,
phase timestamps and retry deadlines/ticks never change incident identity.
Ordinary progress raises no incident or agent wake. Stage A emits no condition
events; later Task-step consumers must atomically emit a changed material blocker
when applying a real owner failure. Pushing remains explicit repository sync.

### Passive integration queues, import and observations (3.2 stage B)

Stage B adds `integration_queue` and `integration_attempt`. Their repository
methods are storage operations; no production queue worker calls them to run
Git, rebase, CI, authorize a fast-forward, send a Task back or mark it done.
`merging` still runs today's merge hooks. No condition statement is emitted,
no Task column or budget is migrated, and no legacy producer is retired.

A queue is unique by `(repo_id, target_branch)`. Branch validation strips one
`refs/heads/` prefix and preserves exact spelling. Location is never a queue
key. `resolve_integration_target_in_tx` is the single location-selection seam:
it reads the repo's explicitly default `repo_location` rows of kind
`primary_checkout`. Exactly one ready location selects the checkout, regardless
of server/daemon ownership or `repo.local_path`. The latter is a provisioning
hint, not a second target setting. Task placement and mere daemon presence never
choose a queue target. The witness stores location/owner/runtime/generation,
never a path or token.

Today's embedded Task merge uses the selected workspace location's checkout;
a daemon merge uses the verified primary checkout supplied to that owner. Those
paths do not veto a checkout because `local_path` disagrees. The table below
covers a Task using the configured default location. A Task already placed at a
non-default checkout still follows its existing backend path in this passive
stage; routing such delivery to the authoritative target remains activation work.

How the resolver decides (queue state, `target_location_id`, typed reason):

| Repo configuration | Result |
|---|---|
| Server only: `repo.local_path` set, one default `primary_checkout` server location at that path, `ready` | `open`, that location |
| Daemon only: no `local_path`, one default `primary_checkout` daemon location, `ready` | `open`, that location |
| Both, the server checkout is the default (daemon copies are not default) | `open`, the server location; a non-default copy never selects |
| Both, the default is a daemon location while `local_path` is set | `open`, the daemon location |
| `local_path` set, default server location at a different path | `open`, the default server location |
| No default `primary_checkout` location (including a pre-location legacy repo with no `local_path`) | `suspended`, none, `target_unconfigured` |
| Two or more default `primary_checkout` locations | `suspended`, none, `target_ambiguous` |
| The one configured location is not `ready` | `suspended`, that location, `target_unavailable` |

Creation records a snapshot. Claim refreshes the default location and its
owner/generation in the same queue-revision CAS transaction. A repaired
suspended queue can reopen at claim. An unavailable/unconfigured/ambiguous
resolution is committed as a suspension without advancing the fence, then the
claim returns a version conflict. A live lease or stale revision cannot refresh
a queue. An uncertain attempt retains its original intent even if the queue's
configured target changes; takeover transfers reconciliation ownership and
cannot repeat that effect.

**Deletion.** In this passive stage the queue tables are evidence, not
authority, and they restrict no deletion:

- deleting a Repo, or the Project that owns it, removes that repo's queues and
  their attempts by `ON DELETE CASCADE` (`repo` → `integration_queue` →
  `integration_attempt`), the same rule the other repo-owned tables in this
  schema use. Project deletion also removes import evidence that never resolved
  a queue (`queue_id IS NULL`), which has no parent to cascade from;
- deleting a repo location (`RepoLocationRepo::delete`) nulls
  `target_location_id` and `target_owner_json` and leaves the queue `suspended`
  with `target_unconfigured` in the same transaction; members and history stay.
  The foreign key itself is `ON DELETE SET NULL`;
- deleting a Task nulls `task_id` and keeps `task_ref`/`project_ref`;
  workspace, placement, execution, location, workflow-reference and
  predecessor foreign keys are `SET NULL` beside their opaque refs;
- removing a machine tombstones the daemon and deletes none of these rows.

**Stage D requirement (owner decision needed).** "Deletion is refused while an
integration effect is uncertain" is NOT implemented here. When the queue starts
driving Git, stage D must decide explicitly whether a Repo, repo location or
Project with an unresolved (`reconciling`, `ff_inflight`, quarantined-head or
running/uncertain-operation) attempt may be deleted, enforce it in the service
delete paths with a typed refusal the owner can act on, and record the public
break in the changelog. It must not come back as a silent foreign-key
`RESTRICT`.

The migration is additive SQL only: two tables and their indexes, no data DML,
no logic triggers, no changes to historical migrations. Delivery FKs are
nullable with separate opaque refs, so workspace/execution cleanup cannot erase
recorded identity. Columns that nothing in stage B reads or writes were left
out; a later stage adds each one it needs with `ALTER TABLE ... ADD COLUMN`
(all are nullable or constant-default, so no table rebuild). `daemon_id`/`runtime_id` and contract/Review/carry identifiers
are retained opaque witnesses rather than cascading foreign keys.

`integration_queue` columns (the SQL is authoritative):

| Column | SQL type / constraints |
|---|---|
| `id` | `TEXT PRIMARY KEY` |
| `repo_id` | `TEXT NOT NULL REFERENCES repo(id) ON DELETE CASCADE` |
| `target_branch` | `TEXT NOT NULL` |
| `target_location_id` | `TEXT REFERENCES repo_location(id) ON DELETE SET NULL` |
| `target_owner_json` | `TEXT CHECK(target_owner_json IS NULL OR json_valid(target_owner_json))` |
| `next_seq` | `INTEGER NOT NULL DEFAULT 1 CHECK(next_seq >= 1)` |
| `head_attempt_id` | `TEXT REFERENCES integration_attempt(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED` |
| `lease_owner` | `TEXT` |
| `lease_until` | `TEXT` |
| `fence_generation` | `INTEGER NOT NULL DEFAULT 0 CHECK(fence_generation >= 0)` |
| `state` | `TEXT NOT NULL CHECK(state IN ('open','suspended','quarantined','closed'))` |
| `available_at` | `TEXT` |
| `updated_at` | `TEXT NOT NULL` |
| `created_at` | `TEXT NOT NULL` |
| `revision` | `INTEGER NOT NULL DEFAULT 1 CHECK(revision >= 1)` |
| `last_error_kind` | `TEXT CHECK(last_error_kind IN ('infrastructure','target_unconfigured','target_ambiguous','target_unavailable','owner_required','unsupported_path','corrupt_import','contradictory_proof','needs_fact','timeout','workspace_lost','candidate_check_failed'))` |
| `last_error` | `TEXT CHECK(length(CAST(last_error AS BLOB)) <= 4096)` |

`integration_attempt` columns (the SQL is authoritative):

| Column | SQL type / constraints |
|---|---|
| `id` | `TEXT PRIMARY KEY` |
| `queue_id` | `TEXT REFERENCES integration_queue(id) ON DELETE CASCADE` |
| `task_id` | `TEXT REFERENCES task(id) ON DELETE SET NULL` |
| `task_ref` | `TEXT NOT NULL` |
| `project_ref` | `TEXT NOT NULL` |
| `queue_seq` | `INTEGER NOT NULL CHECK(queue_seq >= 1)` |
| `attempt_number` | `INTEGER NOT NULL DEFAULT 1 CHECK(attempt_number >= 1)` |
| `predecessor_attempt_id` | `TEXT REFERENCES integration_attempt(id) ON DELETE SET NULL` |
| `current` | `INTEGER NOT NULL CHECK(current IN (0,1))` |
| `admission_key` | `TEXT NOT NULL` |
| `expected_status` | `TEXT NOT NULL` |
| `expected_epoch` | `INTEGER NOT NULL CHECK(expected_epoch >= 0)` |
| `observed_task_version` | `INTEGER NOT NULL` |
| `workflow_ref_id` | `TEXT REFERENCES task_step_workflow(id) ON DELETE SET NULL` |
| `enqueued_at` | `TEXT NOT NULL` |
| `execution_id` | `TEXT REFERENCES execution(id) ON DELETE SET NULL` |
| `execution_ref` | `TEXT` |
| `workspace_id` | `TEXT REFERENCES workspace(id) ON DELETE SET NULL` |
| `workspace_ref` | `TEXT` |
| `placement_id` | `TEXT REFERENCES workspace_placement(id) ON DELETE SET NULL` |
| `placement_ref` | `TEXT` |
| `repo_location_id` | `TEXT REFERENCES repo_location(id) ON DELETE SET NULL` |
| `repo_location_ref` | `TEXT` |
| `owner_kind` | `TEXT CHECK(owner_kind IN ('server','daemon'))` |
| `daemon_id` | `TEXT` |
| `runtime_id` | `TEXT` |
| `placement_generation` | `INTEGER` |
| `original_candidate_sha` | `TEXT` |
| `candidate_sha` | `TEXT` |
| `target_tip_sha` | `TEXT` |
| `contract_execution_id` | `TEXT` |
| `review_id` | `TEXT` |
| `reviewed_paths_json` | `TEXT CHECK(reviewed_paths_json IS NULL OR json_valid(reviewed_paths_json))` |
| `changed_paths_json` | `TEXT CHECK(changed_paths_json IS NULL OR json_valid(changed_paths_json))` |
| `conflict_paths_json` | `TEXT CHECK(conflict_paths_json IS NULL OR json_valid(conflict_paths_json))` |
| `repair_paths_json` | `TEXT CHECK(repair_paths_json IS NULL OR json_valid(repair_paths_json))` |
| `guard_paths_json` | `TEXT CHECK(guard_paths_json IS NULL OR json_valid(guard_paths_json))` |
| `state` | `TEXT NOT NULL CHECK(state IN ('queued','path_wait','validating','rebasing','checking','awaiting_task_step','ready_ff','ff_inflight','reconciling','applied','ejected','needs_review','parked','quarantined','completed','cancelled','superseded'))` |
| `resume_state` | `TEXT CHECK(resume_state IN ('queued','path_wait','validating','rebasing','checking','awaiting_task_step','ready_ff','ff_inflight','reconciling','applied','ejected','needs_review','parked','quarantined','completed','cancelled','superseded'))` |
| `failure_kind` | `TEXT CHECK(failure_kind IN ('infrastructure','target_unconfigured','target_ambiguous','target_unavailable','owner_required','unsupported_path','corrupt_import','contradictory_proof','needs_fact','timeout','workspace_lost','candidate_check_failed'))` |
| `failure_message` | `TEXT CHECK(length(CAST(failure_message AS BLOB)) <= 4096)` |
| `slot_generation` | `INTEGER NOT NULL DEFAULT 0 CHECK(slot_generation >= 0)` |
| `permit_json` | `TEXT CHECK(permit_json IS NULL OR json_valid(permit_json))` |
| `operation_kind` | `TEXT CHECK(operation_kind IN ('merge','rebase','check','fast_forward','reconcile'))` |
| `operation_id` | `TEXT` |
| `owner_fence_json` (C2) | nullable owner/queue/attempt fence JSON, capped at 16 KiB |
| `effect_intent_json` (C2) | nullable frozen request + digest JSON, capped at 64 KiB |
| `effect_receipts_json` (C2) | non-null JSON array, default `[]`, capped at 1 MiB |
| `current_operation_state` | `TEXT CHECK(current_operation_state IN ('pending','running','succeeded','failed','uncertain','acknowledged'))` |
| `operation_receipts_json` | `TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(operation_receipts_json))` |
| `checks_json` | `TEXT CHECK(checks_json IS NULL OR json_valid(checks_json))` |
| `checks_commit_sha` | `TEXT` |
| `deadline` | `TEXT` |
| `effect_seq` | `INTEGER NOT NULL DEFAULT 0 CHECK(effect_seq >= 0)` |
| `effect_ack_json` | `TEXT CHECK(effect_ack_json IS NULL OR json_valid(effect_ack_json))` |
| `acknowledged_at` | `TEXT` |
| `integrated_before_sha` | `TEXT` |
| `integrated_sha` | `TEXT` |
| `available_at` | `TEXT` |
| `last_error_kind` | `TEXT CHECK(last_error_kind IN ('infrastructure','target_unconfigured','target_ambiguous','target_unavailable','owner_required','unsupported_path','corrupt_import','contradictory_proof','needs_fact','timeout','workspace_lost','candidate_check_failed'))` |
| `last_error` | `TEXT CHECK(length(CAST(last_error AS BLOB)) <= 4096)` |
| `started_at` | `TEXT` |
| `updated_at` | `TEXT NOT NULL` |
| `created_at` | `TEXT NOT NULL` |
| `completed_at` | `TEXT` |
| `import_source_json` | `TEXT CHECK(import_source_json IS NULL OR json_valid(import_source_json))` |
| `observations_json` | `TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(observations_json) AND length(CAST(observations_json AS BLOB)) <= 1048576)` |
| `observations_dropped` | `INTEGER NOT NULL DEFAULT 0 CHECK(observations_dropped >= 0)` |
| `revision` | `INTEGER NOT NULL DEFAULT 1 CHECK(revision >= 1)` |

Keys: queue `UNIQUE(repo_id,target_branch)`; attempts
`UNIQUE(queue_id,queue_seq,attempt_number)` and
`UNIQUE(queue_id,admission_key)`. The orphan admission key is separately unique.

Indexes:

- `CREATE INDEX integration_queue_ready ON integration_queue(state,available_at)`
- `CREATE INDEX integration_queue_expired_lease ON integration_queue(lease_until) WHERE lease_until IS NOT NULL`
- `CREATE INDEX integration_queue_location ON integration_queue(target_location_id)`
- `CREATE UNIQUE INDEX integration_attempt_current_task ON integration_attempt(task_ref) WHERE current=1`
- `CREATE UNIQUE INDEX integration_attempt_current_seq ON integration_attempt(queue_id,queue_seq) WHERE current=1`
- `CREATE UNIQUE INDEX integration_attempt_orphan_admission ON integration_attempt(admission_key) WHERE queue_id IS NULL`
- `CREATE INDEX integration_attempt_members ON integration_attempt(queue_id,state,available_at,queue_seq) WHERE current=1`
- `CREATE INDEX integration_attempt_history ON integration_attempt(task_ref,created_at DESC)`
- `CREATE INDEX integration_attempt_reconnect ON integration_attempt(daemon_id,current_operation_state)`
- `CREATE UNIQUE INDEX integration_attempt_operation ON integration_attempt(operation_id) WHERE operation_id IS NOT NULL`
- `CREATE INDEX integration_attempt_retention ON integration_attempt(state,completed_at)`
- `CREATE INDEX integration_attempt_import ON integration_attempt(task_ref,expected_epoch) WHERE import_source_json IS NOT NULL`
- `CREATE INDEX integration_attempt_import_disposition ON integration_attempt(json_extract(import_source_json,'$.disposition')) WHERE import_source_json IS NOT NULL`

Nullable JSON path sets distinguish an unknown set (`NULL`) from a known empty
set (`[]`). They keep exact repo-relative UTF-8 identity, including both rename
endpoints, case and Unicode spelling. Unsupported encodings and invalid path
shapes quarantine imports; no lossy normalization grants eligibility. Guards
have no expiry column. Repair-touched paths have their own full set; conflict
and guard sets are distinct. Stage E must implement their union, overlap and
explicit closure rules. Queue sequence is a reservation, not visible eligible
rank; successors atomically supersede their predecessor and keep its sequence.
Only one current attempt per Task and one current attempt per queue sequence can
exist. Terminal attempts are not current. Imported orphan evidence has no queue
and no current membership.

Claims reserve one head in a `BEGIN IMMEDIATE` transaction. An open queue without a live process lease can be claimed. A quarantined queue
can only transfer ownership of its existing reconciling/in-flight head, never
select another member. Takeover retains the reserved head,
advances `fence_generation`, stamps the head's `slot_generation` and advances its
revision. A ready permit is invalidated on takeover and returns to Task-step
authorization; in-flight work returns to reconciliation of its original
operation. Renewal requires queue revision, owner and fencing generation. Attempt
updates require revision and the transition graph; ReadyFf/FF-inflight data must bind candidate, target, Task epoch and slot generation in its permit, and configured check evidence must name the same candidate. Stale snapshots return
`DbError::VersionConflict`. These methods write no Task version. Expired process
leases never imply that an uncertain logical head is free. No production caller
claims these leases in Stage B.

The attempt transition table is data (`INTEGRATION_TRANSITIONS`), with an exit
for every non-terminal state and none for terminals. Self-state updates can
record facts on non-terminal rows; terminal machine state cannot transition.
Critical and uncertain states cannot be cancelled merely by a timeout.

| From | Allowed next states |
|---|---|
| `queued` | `path_wait`, `validating`, `parked`, `cancelled`, `superseded` |
| `path_wait` | `queued`, `validating`, `parked`, `cancelled`, `superseded` |
| `validating` | `applied`, `needs_review`, `parked`, `rebasing`, `awaiting_task_step`, `cancelled`, `superseded` |
| `rebasing` | `checking`, `ejected`, `parked`, `reconciling`, `cancelled` |
| `checking` | `awaiting_task_step`, `ejected`, `parked`, `cancelled` |
| `awaiting_task_step` | `ready_ff`, `needs_review`, `ejected`, `parked`, `cancelled` |
| `ready_ff` | `awaiting_task_step`, `ff_inflight`, `needs_review`, `parked`, `cancelled` |
| `ff_inflight` | `applied`, `rebasing`, `reconciling` |
| `reconciling` | `applied`, `rebasing`, `ready_ff`, `queued` (effect settled, nothing landed), `parked`, `quarantined` |
| `applied` | `completed` |
| `ejected` | `needs_review`, `parked`, `cancelled`, `superseded` |
| `needs_review` | `parked`, `cancelled`, `superseded` |
| `parked` | `queued`, `rebasing`, `checking`, `needs_review`, `ejected`, `cancelled`, `superseded` |
| `quarantined` | `reconciling`, `parked`, `queued`, `superseded` |
| `completed` | None (terminal) |
| `cancelled` | None (terminal) |
| `superseded` | None (terminal) |

`available_at` and `deadline` are optional application timestamps; there is no
SQL timeout default. The later head pipeline must read its timeout setting
(default 1800 seconds). The tables do not charge budgets: a normal stale-base
rebase must cost nothing, external target movement must be charged once by its
Task-step consumer, and ordinary waiters keep their active Project slot. Stage A
already owns the condition projection; queue positions, clocks, leases and full
path sets stay out of it.

**Bounded import.** `SqliteDb::import_integration_pass` imports at most 100 Tasks
per call in one transaction, with bounded history queries (64 rows per source,
plus one sentinel), and caps each source text at 65,536 characters before materialization. Oversized sources retain their original byte length and a marked prefix and require `complete_legacy_evidence`. Its unique `import:<Task>:<status_epoch>` admission records
are the durable progress register. Committed entries are excluded on the next
pass; a crash rolls back the whole unfinished slice. It orders by current entry
time, transition identity and Task ID and pins repository provenance from the
implementation execution's workspace, including a coordination root's relevant
child execution. It never substitutes the Project's current primary repo for
attempted work. It visits `merging`/`merge_failed` and relevant `review`, `done`
and `cancelled` history. This storage pass is callable but is not scheduled or
activated at startup in Stage B; cutover must drive it before queue consumption.

Each row freezes the priority, disposition, named missing facts and raw source
JSON in `import_source_json`. Malformed JSON, unknown typed bridges, unsupported
paths, missing execution/target and contradictory proof sources are retained as
quarantined rows. The importer writes only the two new tables: no Task, step,
condition, Review, carry, event, budget or Git effect. It does not redirect any
legacy hook. Actual Git/daemon facts unavailable to this pass are `needs_fact`,
never invented approvals, CI results or ancestry. An unresolved operation pins
a quarantined head and retains its original identity; a second uncertain head
is separately quarantined. Already current shadow membership is preserved and
its import records the need to resolve `existing_attempt_identity`.

| Priority | Storage disposition and evidence |
|---|---|
| 1 | Certified persisted Done → `applied` (`completed` for a done Task), preserving candidate and integrated result separately, including manual merge commits. Intent without success needs `candidate_ancestry`; incomplete owner receipts need `validated_owner_receipt`. Cancelled/deleted/archived success needs original-operation reconciliation. |
| 2 | Unresolved retained merge intent or running remote operation → `reconciling`, `needs_fact: original_remote_operation_result`; quarantine and pin the queue head. |
| 3 | Terminal/deleted/archived without possible effect → completed/cancelled/superseded history, not current. Subtasks acquire no independent queue. |
| 4 | Current pending/claimed hook and its frozen input/effects → `needs_fact: legacy_hook_continuation`; preserve its recorded phase and all blockers, with no redirect. |
| 5 | Rebase target without outcome → parked with rebasing resume and `needs_fact: git_rebase_in_progress_and_conflict_paths`; do not test ancestry before active-rebase recovery. |
| 6 | Rebased outcome or clean mechanical bridge → parked with checking resume and `needs_fact: rebased_head_and_bound_checks`; no invented coder or reused CI. |
| 7 | Typed conflict outcome/handoff in merge_failed → ejected (parked for held/exhausted repair), known conflict/guard paths retained; `needs_fact: current_retry_window_conflict_and_repair_paths`. Retry resets bound the typed bridge union. |
| 8 | ReviewRefresh or cleared authority without mechanical proof → needs_review; no carry grant. |
| 9 | Stored carry → parked with validating resume and `needs_fact: carry_head_base_and_check_evidence`; no approval is inferred from the row alone. |
| 10 | Matching paused-integration marker and paused Project → parked with the exact source/generation retained. |
| 11 | Matching marker, resumed Project → parked with queued resume and `needs_fact: candidate_head_and_review_authority`. |
| 12 | Marker names another entry → obsolete evidence with queued resume; current candidate/authority still needs a fact. |
| 13 | Existing blocked/failed/manual/human/entry blocker → parked; every source reason is preserved. |
| 14 | Future retry or CI infrastructure interruption → parked with deadline; an incompatible target is obsolete evidence. |
| 15 | Owner wait/disconnected placement/upgrade/pending remote cancellation → parked owner exclusion. Uncertain operations were handled first. |
| 16 | Removed owner/machine_removed workspace/lost daemon handle → parked with `needs_fact: candidate_object_availability`. |
| 17 | Accepted queued recovery or sticky disposition → parked, preserving owner command ordering. |
| 18 | Root merging with passed marker, pinned delivery, no hook or blocker → parked with queued resume and `needs_fact: candidate_head_and_review_authority`; the timestamp alone grants no authority. |
| 19 | Ordinary dirty/conflict repair → ejected, workspace/target repair → parked; no manufactured conflict paths or carry provenance. |
| 20 | Unknown/malformed/contradictory input → quarantined, never dropped. A history source exceeding the bound needs `complete_legacy_evidence`. |

Proof disagreement is checked before a lower-priority blocker can hide it. Owner
receipts must match the frozen operation, location, owner and generation; a
reviewed exact object must match its Done result, while an explicitly manual
merge may integrate a merge commit. A generation-only pause marker grants no
resume. Bridge-looking prose grants no mechanical lineage.

**Shadow observations.** The existing Task-step owner records typed, idempotent
observations on current membership. `record_hook_effect` stores admission,
merge and rebase observations in the same transaction as the legacy checkpoint.
MergeService buffers the exact target tip where it already reads it (embedded
under its authority lock, daemon from its existing target precondition); this
adds no Git command or database transaction during Git work. The buffer is
scoped to the Task step and is flushed with the existing hook result.
`update_status_inner` and `update_status_with_review_authority_inner` observe
actual CI exit codes, and carried authority's candidate/base/changed paths, in
their existing Review transaction. `finish_step_in_tx` records terminal done or
cancellation in the existing result transaction. Ordinary observations never
advance a runnable attempt phase; only resolved terminal Task results detach membership
into terminal history. A terminal legacy Task does not release an uncertain
operation or its quarantined head. Unobserved fields remain null; notably Done.before_sha is
not re-labelled as a target-tip fact.

An attempt created by the shadow stays `queued` and current while its Task
lives; it is closed straight to `completed`/`cancelled` when the Task settles.
That close is the one write that does not go through
`INTEGRATION_TRANSITIONS`: the table governs the future worker
(`transition_integration_attempt`), not this mirror of the legacy result.

Recording is one SQL statement per observation: a JSON append on the Task's
current attempt, found through the `integration_attempt_current_task` partial
unique index, with no read of the row into Rust and no rewrite of its other
columns. `observations_json` is bounded: an attempt keeps its first observation
and the 15 most recent, counts the rest in `observations_dropped`, keeps at
most 256 paths per list (`paths_truncated`) and at most 16 KiB per observation
(1 MiB per row, by CHECK). Replay of a retained identity is a no-op. Step
settlement, which every step of every Task passes, pays exactly one indexed
statement that matches nothing unless the Task is terminal and has a current
attempt. Only the first merge intent of a merge entry needs several statements
(create-or-get queue, admit attempt) and they run inside a savepoint.

A recording failure never fails or rolls back the real result. A failed single
statement undoes only itself (SQLite statement atomicity); a failed admission
rolls back to its savepoint. Each site has already written in its transaction,
so it holds the write lock and a recording statement cannot wait on another
writer. Failures are counted in one process-wide counter: the first and every
power of two is a warning, the rest are debug lines; no identity set is kept.
The shadow emits no event, states no condition, schedules no worker, changes
no ordering/retry timing/budget and drives no Git or CI. The target-read
buffer is one slot in the Task step's task-local scope: created empty by
`in_task_step`, keyed by step id and hook index, dropped with the step.

**Stage B shadow rows are disposable.** The importer has no production caller.
Before stage D's first import, it must discard stage B's unfenced shadow rows or
version its admission key. Once an owner gate has written an intent or receipt,
that attempt is retained authority and must survive this discard; a blanket
`DELETE FROM integration_attempt; DELETE FROM integration_queue;` is only valid
when no C2 owner effects have been admitted. Import progress
is the existence of an `import:<Task>:<status_epoch>` row, so a changed
classification is not re-applied over existing rows; a current shadow attempt
makes the importer retain its row non-current with
`existing_attempt_identity`; and mere import/read operations keep the queue target snapshot and any
quarantine pin. Claim refreshes that snapshot as described above.

Operator status adds only read-only counts by queue/current-attempt state and
quarantined imports; there are no queue REST/MCP/web/CLI reads yet.


### Integration effect primitives and Task-step recorders (3.2 stage C, part 1)

`services::integration_effects` contains the Git/command/socket effect adapters and
read-only facts used by today's Task-step consumer. It accepts an explicit
`EffectWorkspace` witness (owner, workspace, placement, generation and handle),
Git paths/branches and candidate/target objects, command purpose/environment,
and caller-supplied command/transport deadlines. Its functions receive no
repository, database, event publisher or Task service. `RpcExchange` receives
only its selected `DaemonConnection`; connection lookup
and protocol admission belong to the recorder. Replacement/unregistration marks
the old connection stale and fails its pending replies. `GitFacts` is
implemented by `ResolvedWorkspace`, whose daemon path sends the
owner inspection through the workspace client. The witness is input
provenance; the current consumers still perform placement and review authority
checks. The primitives do not verify the witness themselves, so today's
Task-step path behaves as before; the durable owner gate described below
verifies placement, HEAD and target before it calls them.

The split is phased to preserve the existing transaction and read order:

- `merge_cleanliness`, `expected_target`, `merge_heads`, `target_tip` and
  `validate_merge_candidate` collect/classify Git facts. The consumer records
  the candidate's execution `before_sha` before taking the local review guard,
  buffers stage B's target observation at the same target read, then calls
  `apply_merge`. Reviewed integration fast-forwards the exact approved object;
  manual integration still permits a merge commit. `merge_result` collects the
  resulting SHA or reads conflict paths and aborts after the guard is released,
  as before. An exact-object mismatch retains the original guard-drop path.
  `MergeService::record_merge_execution_evidence` owns execution writes,
  including their existing Project list-revision and usage-ledger triggers.
- `rebase` moves the existing dirty/stopped-rebase checks, text-marker
  materialization and unsupported-conflict abort behavior. `recover_rebase`
  checks for an interrupted rebase before ancestry;
  `finish_rebase_recovery` preserves the union and order of previously
  committed marker paths. Their `GitFacts` port exposes only a Git query. The hook
  consumer still reads/writes `rebase_target` and `rebase_outcome`, records
  Forge-established HEAD evidence, and applies comments, typed bridges and
  Task results. The existing owner HEAD probe produces `RebaseHeadFacts`;
  `record_rebase_head` consumes it and issues only the original SQLite write.
  Qualification and probe order are unchanged, with no added HEAD read.
  Fresh local rebase still uses the branch name, exactly as the
  legacy path does; local expected-object metadata is absent where today's
  caller has no such precondition. Daemon rebase retains its existing expected
  HEAD/generation checks and owner-operation messages.
- `CheckRunInput` supplies one command list, purpose, environment and placement
  witness, an explicit optional timeout and output capture bound. `CheckRun`
  yields one command at a time and returns `CheckRunOutcome` (command index,
  exit code, bounded redacted stderr/combined output tails and timestamps) or
  `CheckRunFailure` (infrastructure error and the completed command facts).
  `run_at` owns only local process execution and capture. The daemon uses the
  same `RunSpec`/`RunResult` and existing owner wire types. The consumer can
  retain/acknowledge each owner reply before issuing the next command; the
  `RunCiSteps` recorder then serializes the facts and performs the same Review
  settlement, Task authority projection and event publication. Entry CI still
  passes `None` (wire `timeout_secs=0`), with unbounded collection and the same
  4096-byte Review tails. A finite `Duration` is rounded upward to the existing wire's whole-second
  timeout (at least one second), so a sub-second or zero supplied duration
  cannot become the unbounded `timeout_secs=0` sentinel. No default timeout,
  CheckRunner cache or conformance consolidation is introduced.
- `RpcExchange::prepare` installs only the in-memory pending reply; the
  consumer's `record_exchange_admission` persists remote-operation admission
  before `execute` sends a frame. `execute` performs the socket exchange with
  the caller's existing timeout or `None`. The pending reply guard lives
  through `record_exchange_completion`, and `decode_reply` performs the
  existing typed/operation-identity validation. `record_merge_reply` retains
  validated execution evidence and the command receipt before finishing the
  Task operation and acknowledging the owner journal. The existing client
  owns persisted intents, retry/reconciliation and acknowledgement receipts.
  Authority check → persisted intent → authority recheck → RPC outside SQLite
  → typed validation → evidence commit → journal ACK is unchanged; reconnect
  still inspects outstanding intents. No daemon method or payload changes.

**Boundary rule.** Production files in `integration_effects/` must not import
or reference database repositories, SQL, the event bus, Task services or the
workflow engine. `effects_have_no_persistence_or_task_capability_imports`
checks every production source in this module for those capabilities: the
forbidden paths, the identifiers of the repository, service, backend and hook
types (so one cannot arrive through an allowed module), glob imports, and the
registry methods `rpc.rs` calls. It reads source text, so it cannot see a
capability reached through a handle the caller passes in. This is
an enforced module/source boundary, not a new crate boundary: existing service
error and transport types are retained. Direct server and real-daemon primitive
tests compare digests of every SQLite table before/after and assert no event;
owner journal retention is an effect fact and is deliberately not acknowledged
by a primitive. The same characterization assertions run before and after the
extraction, including execution trigger effects and remote receipt ordering.

All stage B shadow observation sites and content remain in their original
Task-step/Review/result transactions. Carry predicates, manual mode, Task
single-writer authority and cancellation protection are unchanged. There is
still no integration worker or activation. The owner gates and attempt sink below
now bind the existing Task-step merge/rebase recorders, without claiming a queue
or changing Task projections. The later head CI/rebase wall
timeout remains an explicit setting decision (default 1800 seconds), not a
default supplied by these primitives.


### Integration owners, fenced wire and reconciliation (3.2 stage C, part 2b)

A queue claim stamps `integration_attempt.owner_fence_json` with queue/attempt
IDs, monotonic generation, lease owner and the frozen target owner (location,
server/daemon identity, runtime and location version). `ServerIntegrationOwner`
and the daemon gate refuse foreign owners, stale fences and changed physical
placement before Git. Queue effects also verify expected HEAD and target refs
before Git writes. The persistence-free primitives never validate witnesses;
the owner gates own those checks.

Both owners use `git::integration` for rebase/conflict handoff and the reviewed
fast-forward. Server and daemon adapters retain their established outcome
classification and error rendering. The shared effects receive paths, objects,
mode and explicit limits; the daemon has no database dependency.

Order: owner checkout lock → receipt lookup → short fenced intent transaction →
last fence check/start checkpoint → Git outside SQLite → short receipt transaction.
`IntegrationEffectGuard` holds an in-process owner lock, not a write transaction.
The durable intent distinguishes `started:false` from a potentially performed
effect. A refusal after intent commit settles `not_performed`; an abandoned
unstarted intent is reconciled the same way. A queue claim's started intent never
authorizes another effect without owner receipt lookup, and a checkout with
another unresolved started queue intent refuses a new effect. A Task-step intent
never refuses anything (see "Task-step intents always settle" below). Only
intents that share an owner lock are compared: a rebase or check locks its
workspace alone, so one Task's rebase neither waits on nor is refused by another
Task's in-flight merge into the same checkout. Receipt identity remains
`(attempt_id, operation_kind, fence)` with the entire request checked against reuse.
An uncertain receipt is replaced on reconciliation, not appended as a second
receipt for the same key. An original owner can retain its receipt after lease
takeover; the frozen intent remains its identity.

The existing `V202610082317__integration_fencing.sql` columns retain their bounds:
16 KiB fence, 64 KiB intent, 128 KiB per receipt, 1 MiB receipt array. This stage
uses no additional SQL columns or stored enum values and needs no migration.
The daemon persists high-water fences and unresolved checkout operation identities
beside its workspace registry in `.forge/journal`; attempt effects use stable
attempt/kind/generation operation IDs in that journal. The existing journal
count/byte limits apply before an effect. Queue attempt receipts survive ACK and
replay without Git, including after restarting the owner. Cancelling an attempt
stops its Git process group before retaining a receipt with observed HEAD and
rebase progress. Task-step merge cancellation keeps today's protected-completion
behavior. Ordinary Task-step journal ACKs retain their existing lifecycle.

Daemon protocol revision **5** retains the tagged `integration` binding on
mutation and reconciliation messages. `attempt` carries the queue fence and
frozen witness; `task_step_effect` carries the existing Task step's attempt
binding; `task_step` identifies other existing workspace operations. There is
one revision-5 format, and revisions below 5 are refused with
`daemon_upgrade_required` and an “upgrade the daemon” message. Upgrade the server
then each daemon, preserving `--workspace-root`. Startup forward-migrates retained
revision-3 journal intents/results in place; old wire decoding is never enabled.

Today's merge/rebase recorders bind to the current shadow attempt and Task-step
fence without claiming a queue. Their owner witness follows the Task's current
placement. Existing dirty/review/ancestry/target classification, local review
authority guard, execution writes, comments, events and hook result ordering
remain authoritative. The new receipt facts confer no Task or Review authority.
The local review guard still spans the bounded reviewed merge because it fences
Review and Project edits; a queue owner guard does not replace that contract.

On server recovery, local intents are reconciled before Task recovery. Before a
new local effect, the same owner lock waits for any live effect to retain its
receipt. A restarted server can prove an exact reviewed queue merge from the frozen
candidate/target refs. A queue claim's unknown effect retains an uncertain receipt
and its intent. On reconnect, the workspace client reconciles daemon attempt intents
before ordinary execution receipts and any new effect. The owner returns its
retained receipt. An absent owner intent is fenced against delayed delivery and
settles `not_performed`; an interrupted merge needs exact object proof. Receipt
validation and attempt persistence precede ACK. No Git effect is repeated to
reconstruct a result.

**Task-step intents always settle.** Today's merge and rebase are bound to the
Task step's lease (`lease_owner = task-step:<step id>`, `generation` = the step's
claim count), not to a queue claim, and the Task step already owns recovery of
its effect: the durable `merge_intent` / `merge_outcome` / `rebase_target` /
`rebase_outcome` hook effects, the idempotent already-merged check, and
interrupted-rebase recovery. The attempt receipt is therefore a record of the
effect and never its result or a precondition of the next one:

- A recorder error, a missing/oversized/contradictory owner receipt and a failed
  receipt write are logged; the merge or rebase result the Task step computed is
  returned unchanged. An error result is recorded `failed`, never `uncertain`.
- A started Task-step intent whose guard is gone (server crash, dropped future,
  daemon that never answered) is settled `failed` with an `infrastructure`
  result by whichever comes first: startup reconciliation, the reconnect pass,
  or the next admission that takes the same owner lock, which holds the lock the
  dead guard held and so knows the effect is not running here. It never returns
  `ReconciliationRequired`.
- Reconciliation before a Task-step effect, at startup and on daemon reconnect
  is best effort: a failure is logged and neither crash recovery, the reconnect
  pass for run/merge intents, nor the effect is stopped.
- When the shadow attempt no longer matches the Task's placement or target
  branch, the step lease is still verified and the effect proceeds unrecorded.
- On the daemon, a Task-step effect is not held to the location version (the
  server changes it without telling the owner; only a queue claim freezes it),
  and a Task-step journal intent left without an outcome by a daemon restart is
  settled as interrupted, with a receipt, by the next Task-step request on that
  checkout instead of refusing it.

| Durable state (Task-step binding) | What moves it | When | Ends as |
|---|---|---|---|
| intent written, not started | startup / reconnect reconciliation, or the next admission on the checkout | next server start, daemon reconnect, or next merge/rebase there | receipt `failed` / `not_performed`; Task step retried by its lease |
| intent started, guard alive | the effect itself; today's deadlines and protected completion | bounded by the step | receipt `succeeded` or `failed` |
| intent started, guard gone | same three passes as above | next server start, reconnect, or next admission (step lease expiry reclaims the step) | receipt `failed` / `infrastructure`; step recovery re-reads Git |
| owner receipt missing or invalid | settled at the reply, else by the lookup, else by the next admission | immediately, or as above | receipt `failed` / `infrastructure`; owner result still returned |
| daemon journal intent without outcome | server lookup, else the next Task-step request on that checkout | reconnect, or next request | journal outcome `interrupted` plus receipt |
| daemon never reconnects | today's owner-disconnected timeout fails the placement; machine removal settles the attempt | existing timeout / operator removal | receipt `failed` / `infrastructure`, Task handled by placement recovery |
| machine or location removed mid-effect | the remover's transaction | at removal | receipt `failed`, attempt `parked` (or still `quarantined`), queue `suspended` |
| queue `suspended` / attempt `quarantined` | nothing automatic: no worker runs before activation, and today's Task-step path does not read queue state | n/a | passive rows; stage D owns their exits |

Queue-claim (`attempt`) intents keep the strict rule (uncertain until the owner's
receipt). No production caller creates one before activation.

Machine removal and permitted location deletion settle in-flight attempts with
an infrastructure receipt/failure in the remover's transaction, suspend their
queues, clear leases and preserve quarantine. A late owner cannot overwrite
that settlement. Existing placement-in-use deletion refusals and Repo/Project
CASCADE behavior remain unchanged.

Every Git command in `git`, including read probes and reviewed fast-forward,
uses the group runner: closed stdin, a separate process group, SIGTERM, up to
500 ms grace, then SIGKILL. Review's bounded Git evidence reads use that runner
as well. Owner merge and rebase accept cancellation; an explicit daemon
`deadline_nanos` witness binds the owner wall limit, separately from RPC timeout.
Their deadlines include Git witness probes. Cancellation retains the state interrupted-rebase recovery
expects, including a stopped rebase without `index.lock`.

Known limits before activation:

- Queue activation, the integration worker and Task state/public shape changes
  remain stage D work.
- Placement off the default location still follows today's Task placement;
  stage D will place integrating Tasks according to the target.
- Unknown effects are kept uncertain when neither a retained receipt nor exact
  Git completion proof exists. Lease expiry alone never grants a repeat.
- Queue attempt receipts are bounded and retained after ACK; retention/cleanup
  policy must preserve replay fencing when the future worker starts using them.
- The existing local Review/Project authority guard remains separate from queue
  owner serialization.

### Integration activation storage (3.2 stage D, part 1a; not called until D2)

Migration `V202610091612__integration_activation.sql` and
`db::integration_queue::activation` add the storage the queue worker will use.
Nothing in production calls it until D2: `merging` still runs today's merge
hooks, and existing rows read both new columns as `NULL`.

New `integration_attempt` columns:

| Column | Type / constraint | Meaning |
|---|---|---|
| `cancel_requested_at` | `TEXT` | Set once by `request_integration_cancel`. An ordinary attempt transition can neither set nor clear it. |
| `phase_timings_json` | `TEXT`, JSON object, at most 16384 bytes | The head's timings (`IntegrationPhaseTimings`), written only by `record_integration_head_timings`. |

`IntegrationPhaseTimings` is cumulative over an attempt's rounds: `rounds`,
`queued_ms`, `validate_ms`, `transfer_ms`, `rebase_ms`, `check` (either
`ran {slot_wait_ms, run_ms}` or `skipped {reason}` with reason
`target_unchanged` or `no_checks_configured`), `step_wait_ms`, `ff_ms`,
`head_total_ms`, `lost_races` (the newest 32 entries of `{kind, round, at}`,
kind `queue_member` or `external`) and `lost_races_folded` (`{queue_member,
external}` counts of older entries). A write with more than 32 lost races is
not refused: the repository folds the oldest into the counts, so a timings
write can never stop a head and the document stays far below its bound. Only
an `external` lost race spends the "target moved" allowance;
`external_target_moves()` counts listed and folded ones. Timings are
measurements: a stored document this build cannot read reads as absent (the
attempt stays readable) and the next write replaces it.

New indexes, all partial: `integration_attempt_timed (state, id)` for current
attempts with an `available_at`; `integration_attempt_cancel_requested (id)`
for current attempts with a cancel request; `integration_attempt_prunable
(completed_at, id)` for finished attempts that still hold receipts or
observations. No trigger is added.

Operations (trait `IntegrationActivationRepo`; all not called until D2). Every
write compares the row's `revision` and bumps it, so a caller holding an older
copy gets `VersionConflict` and must re-read:

| Operation | Rule |
|---|---|
| `request_integration_cancel` | Legal exactly where the attempt graph can reach `cancelled`: `queued`, `path_wait`, `validating`, `rebasing`, `checking`, `awaiting_task_step`, `ready_ff`, `ejected`, `needs_review`, `parked`. Refused (`InvalidTransition`) in `ff_inflight`, `reconciling`, `applied`, `quarantined` and terminal states. It only sets the flag; the worker performs the transition. Repeating it keeps the first time. A queued member carrying the flag is never selected as head by a claim, so a cancel and a claim racing for the same member have exactly one winner. |
| `record_integration_head_timings` | Head of its queue only. Replaces the whole document. |
| `start_integration_round` | The live lease holder, at its current fence generation, takes a new generation and lease for the same head, with the claim's takeover rules. One generation admits one receipt per effect kind, so another rebase or fast-forward of the same head needs a new round. |
| `claimable_integration_queues` | Unleased queues a worker must act on: open or suspended with a due queued member (not asked to cancel) or a reserved head, and every quarantined queue. Keyset by queue id, `LIMIT` at most 500. |
| `expired_integration_heads` | Queues whose lease ended at or before the given time. Keyset by queue id. |
| `due_parked_integration_attempts` | Current `parked` attempts whose `available_at` is due. A parked attempt without `available_at` waits for its owner. Keyset by attempt id. |
| `cancel_requested_integration_attempts` | Current attempts carrying a cancel request. Keyset by attempt id. |
| `quarantine_integration_queue` | Lease holder only; needs a head that is `ff_inflight`, `reconciling`, `quarantined` or has an unsettled effect. |
| `reopen_integration_queue` | The only exit from `quarantined`. Needs a witness that is re-verified against stored rows: `settled_effect` names a succeeded or failed (never uncertain) receipt of this queue, and no current member may still have an intent or a running / uncertain operation. While the queue still has its head, the witness must be that head's own receipt and the head must have left `ff_inflight` / `reconciling` (the worker applies the receipt to the attempt first, then re-opens); once the head has finished and released the slot, its receipt remains a valid witness, and pruning never removes it while the queue is quarantined. The lease and the head are not touched. An owner's reconnect lookup and a machine or location removal settlement both produce such a receipt; a timeout does not. An attempt that never had an effect admitted can have no receipt (an imported uncertain merge that the worker resolved by lookup, or a head that stopped before its intent): the witness for it is `no_effect`, accepted only when that attempt's stored row has no intent, no receipt and no pending / running / uncertain operation, under the same head rules. Whatever the witness, a queue with any current member still in `ff_inflight` / `reconciling`, or with an intent or a running / uncertain operation, does not re-open. With a target that is not ready the queue becomes `suspended` instead of `open`. A `suspended` queue re-opens with `target_ready` naming the repo's one ready default checkout and its version (a claim also still re-opens it). |
| `prune_integration_evidence` | Empties `effect_receipts_json`, `operation_receipts_json` and `observations_json` of at most `limit` attempts that finished before the cutoff. Never touches a current attempt, an attempt with an intent, a pending / running / uncertain operation or an uncertain receipt, an attempt whose Task has a live successor, or any attempt of a quarantined queue. Rows, SHAs and outcomes stay. A settled intent needs no pruning: its receipt already cleared it. |
| `integration_queue_ages` | Count and oldest age for queued members, heads, expired heads, parked members, pending cancel requests and unsettled effects. |
| `integration_timing_samples` | Timings of recently completed attempts, newest first, for percentiles. |

Time comparisons in the sweeps and in pruning use the instant, not the text, so
a timestamp written with an offset is handled. Where an index range needs a
text comparison on `completed_at` (pruning, timing samples) the caller's bound
is first rewritten to the UTC form `completed_at` is stored in; pruning also
re-checks the instant.

Plans: the due-parked, cancel-requested, claimable and timing-sample reads
carry no index hint and are held to their index by `EXPLAIN QUERY PLAN` tests.
The expired-lease sweep and the prune statement keep `INDEXED BY`, because the
planner otherwise walks the queue primary key or every old terminal attempt. A
named index is refused when the statement is prepared, from the schema and the
statement text alone (never from data or statistics), and a test prepares both
statements on the migrated schema and pins both index definitions, so a later
migration that changes either index fails that test.

What moves each new durable mark forward (the worker is D1d / D2):

| Mark | Moved by |
|---|---|
| `cancel_requested_at` on a queued member | Never claimed as head; the cancel-requested sweep returns it and the worker writes `queued → cancelled`. |
| `cancel_requested_at` on a head | The head driver sees it (its next attempt write fails the revision check and it re-reads) or the sweep returns it; the worker stops its own effect, waits for the receipt, then writes `cancelled`. If the attempt has meanwhile entered `ff_inflight`, `reconciling`, `applied` or `quarantined`, the mark stays and is acted on only after that result is known; the sweep keeps returning the row until it is terminal. |
| `cancel_requested_at` on `ejected` / `needs_review` / `parked` | Not a head; only the cancel-requested sweep finds it. |
| Queue `quarantined` | `reopen_integration_queue` with a settled receipt, or with `no_effect` when the pinning attempt never had an effect admitted (every imported uncertain merge). A head that went `reconciling → queued` while its queue is still quarantined is not claimable until the worker re-opens the queue; it then continues with `start_integration_round`. With a `reconciling` / `ff_inflight` head it is still claimable, so the worker can keep asking the owner; every unleased quarantined queue is in the claimable sweep. A machine or location removal settles the receipt. No timer opens it. |
| Queue `suspended` (also after a re-open with no ready target) | The next claim re-resolves the target and opens it, or `reopen_integration_queue` with `target_ready`. The claimable sweep returns it while it has a due queued member or a head. |
| Head left in any state by a dead worker | Expired-lease sweep, then a claim (takeover rules). After a machine removal the head can be `parked` or `quarantined` with no lease; the claimable sweep returns every unleased queue that has a head. |

A cancel request is refused in `ff_inflight`, `reconciling`, `applied` and
`quarantined`: the Git result is in flight, unknown or already landed, and a
flag cannot undo it. The Task-level cancel is not the storage's decision: from
the permit on, the Task-step consumer (D1b) keeps the Cancel command waiting
behind the protected result step, and for a quarantined attempt it must leave
the attempt and its queue as they are until the result is settled.

Queue states are data too (`INTEGRATION_QUEUE_TRANSITIONS`): `open` →
`suspended`, `quarantined`, `closed`; `suspended` → `open`, `closed`;
`quarantined` → `open`, `suspended`, `closed`; `closed` is terminal. The
attempt graph gains two edges. `validating` → `awaiting_task_step`: with an
unchanged target there is nothing to rebase or re-check. `reconciling` →
`queued`: a result proven not landed goes back to validation in place (a head
keeps its slot and lease); going through `parked` would release the slot, wait
for `available_at` and spend one of the Task's infrastructure retries. Any
move to `queued` from `reconciling` or `quarantined` is refused while the
attempt has an effect intent or a running / uncertain operation, so `queued`
is never a way around an unknown result.

### Task condition actions

`services::available_actions(&TaskSnapshot)` is the sole pure Task action resolver. The one snapshot builder loads Task, bounded execution authority, latest Review, role assignments, transition history, the typed condition and its normalized read presentation, entry/queue ownership, placement and Agent/Project availability, and caller authority. REST and MCP Task list projections carry no actions and obtain offers on demand. The admitted native `work.read` projection includes live offers for the bound Project Agent. The function performs no database or workspace I/O. REST, diagnostics, execution controls, MCP, native coordination, Attention, and Solo consume its offers.

The closed verbs are `start`, `hold`, `release`, `retry`, `send_back`, `approve`, `restart`, and `cancel`. Each offer carries meaningful parameters, allowed boolean values, authority, reason, label, cancellation propagation, and a pinned resumable execution. Required operator reasons and send-back guidance are supplied by the caller and retained in review records, comments, follow-up Tasks and transition logs. Commands check the version and select a current offer. A missing offer produces one `action_unavailable` error with current offers. Gate overrides remain owner-only. A role-launching offer (`start`, role `retry`, `release`) is made for one role, resolved in one place (`workflow::action_role`): the role of the Task's state, else the role of the state a claim would enter (the first outgoing Active state, else the first Gate). The Agent the offer needs, the role stored with the queued action, its replay and the claim all use that role, so `start` on a `todo` Task runs its coder, waits for capacity when the coder is busy, and is not offered while that Agent is paused or offline.

Annotations record conditions and evidence, never an action allowlist. Old JSON `recovery_actions` keys are ignored, including unknown historical strings. Historical queued commands are translated at the stored-data boundary from current snapshot facts; their original payload is retained. A superseded or unrepresentable intent restores its condition for an explicit new command. No schema migration is required. Legacy annotation, blocked, failed and entry-barrier columns remain private dual-write storage until stage five.

Recovery commits a queued intent with the saved condition. The dispatcher consumes it through normal admission and waits quietly for capacity, a paused Project/Agent or a reachable workspace owner. Permanent refusals and malformed/stale intents remove the marker and atomically restore the condition with the error, fenced by Task version and marker identity. Fresh retries use the role prompt and review-bound admission; send-back continuations use the review-fix prompt. Restart applies its reset immediately and retains its restart/unblocked events. A shared in-process wake resumes the existing loop after command commits and terminal executions. Gate decisions still transition through the workflow engine; their worker dispatch is deferred and queued, preserving the worker thread on send-back. No new polling worker is introduced. Session launches remain separate Task-adjacent operations. `hold` parks workflow Task work. Stopping a specific execution or side session uses the execution `/stop` resource, including when both run together.

Task action conditions precede generic review decisions. In `review`, ordinary
approval requires an awaiting-human Review and no condition. Re-running review requires
a completed implementation candidate; entry-check retries and budget resets remain
independent controls. Dependency cancellation uses the writer's
`workflow_guard_rejected` annotation with `blocking_reason:"dependency_cancelled"`
and offers cancellation only. It replaces whatever condition the Task had (a
hold, a failure park, the condition a queued action saved) and keeps it in
`blocked.details.superseded`; a queued action refused by the dependency gate
for a cancelled dependency settles into the same blocker, not an untyped
`recovery_required` park, and one refused for an unfinished dependency stays
queued behind a `dispatch_refusal` wait until the dependency finishes or its
link is removed. The displaced condition records the state it was taken in and
is restored only while the Task is still in that state. A queued action always offers Hold alongside cancel,
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

A subtask can be created only while its parent can still coordinate it: in a
backlog, initial or working state (`merge_failed` included). A parent that is
terminal or in a review-phase gate (`review`, `merging`) never dispatches a
child (the state rule of `coordination_root_allows_child_dispatch`), so creating one is refused
with `SUBTASK_PARENT_CLOSED` (`ServiceError::SubtaskParentClosed`,
`task_hierarchy::ensure_parent_accepts_subtasks`) instead of leaving an
unscheduled Task behind.

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
cleanup without waiting for filesystem deletion. Built-in `done` Tasks are
eligible promptly; `cancelled` Tasks retain their worktrees and managed homes
for 24 hours to preserve uncommitted work. Cleanup is deferred while any
execution or WorkspaceLease is active, and a root shared by subtasks is kept
until every child is terminal.

The only directory workspace cleanup may delete is
`<workspace root>/<task id>`. The id must be one plain path component that
does not start with a dot (not empty, `.`, `..`, absolute or nested, and
not `.repos` / `.forge`); the worktree must be its direct child; neither
the Task root nor the worktree path may be a symbolic link; the repository
must not live inside the Task root. Anything else is refused with
`path escapes worktree root` before a file is touched. Only linked
worktrees count as registrations: the main working tree of a repository is
never matched. The workspace root itself may sit behind a link; Git's
resolved paths are matched against the resolved root.

Reclaiming one Task root is one sequence in the `workspace` crate
(`WorkspaceManager::cleanup_worktree`), used by the server backend and by the
daemon `cleanup` handler:

1. Give the owner full access (`u+rwx`) to every directory under the Task
   root. Files are not changed (a file's mode does not stop its removal on
   Unix). Symbolic links are never followed or changed, and a mode is changed
   through a handle that is first checked to be the directory just inspected,
   so nothing outside the root is touched. Toolchains leave read-only trees
   behind (a Go module cache, for one); without this step every retry failed
   the same way. A directory its owner cannot read is not repaired and fails
   the cleanup. The walk and `git worktree remove` go by path, so they rely on
   nothing writing into the tree: cleanup runs only when no execution or lease
   is active.
2. `git worktree remove --force` for the Task's exact worktree.
3. Remove what is left of the Task root: build output, execution outboxes,
   `plan.md`, and any `<name>.broken-<ms>` copy an earlier worktree recovery
   moved aside.
4. Prune. Registrations under this Task root that still point at a missing
   directory are removed one by one. A broad `git worktree prune` runs only in
   Forge-owned `.repos/` caches; a user-owned repository keeps every worktree
   registration Forge did not create.

Every step is idempotent: a missing directory, a missing registration and a
repository that no longer exists are all success. A directory at the recorded
path that still has Git metadata but no registration is removed when it is a
leftover of the workspace's own repository, and refused when it is a checkout
of some other repository.

After the Task root is gone, and only for a terminal Task on a server-owned
workspace, Forge deletes the Task branch when the change is delivered. Delivery
is decided by Git at that moment, never by a stored flag: the branch tip must
be an ancestor of the Task's target branch (`merge_config.target_branch`, else
the repository default branch; `refs/heads/<target>` or
`refs/remotes/origin/<target>`; the local target is what Forge merges into, so
a change merged locally and not yet pushed is delivered). A branch that Git
does not report as contained
is kept: an undelivered or cancelled Task with commits of its own, a squash or
rebase delivery, a target that was moved back. A branch is also kept when it
is checked out in another worktree, when its name is not the `task/<id8>` name
Forge gave that Task, or when another workspace of the repository that is not
yet cleaned uses the same name (the name carries only eight characters of the
Task id). A workspace reset on a live Task never deletes the branch. Branches
on daemon-owned workspaces are not deleted yet. Execution logs are retained;
only `.codex-managed-home` (including task scratch) is removed from the Task's
logs directory.

Project lifecycle script hooks run in the Task worktree. The emitter inspects a
server-owned workspace through the workspace manager (`Purpose::Inspect`, which
never repairs). When the worktree is missing, is not a Git worktree, belongs to
another repository or sits behind a symbolic link, the hook is not run, the
emitter reports `workspace reset required`, and Forge adds one system comment
to the Task saying which hook was not run and why (one per event, workspace and
execution; a hook that could not be started on its owner is recorded the same
way). It never runs in the user's own checkout instead. That comment is for
the Task record; agent prompt loading leaves it out.
The primary-checkout context is used only where no worktree is expected: before
the workspace is prepared, and when the directory is gone while or after Forge
reclaims it (workspace `cleaning` or `cleaned`). Hooks run from the event bus
after the transition, so a hook failure never blocks a state change.
Cleanup tells a repository that is away from one that is gone. When the Repo
records a checkout (`local_path`) that is not on disk at that moment and Forge
holds no clone, the cleanup attempt fails with `repository … is not reachable
right now` and nothing is removed: the worktree registration lives in that
repository and can only be removed once it is back. The attempt is retried with
the usual backoff and raises the cleanup attention item at the threshold.
"Away" is decided by what is on disk: the checkout's parent directory is
missing too, or is an empty directory (an unmounted mount point). When the
parent is there and holds other entries, the checkout itself was deleted or
moved, its registrations went with it, and the Task root is reclaimed at once.
The wait is bounded: seven days after the attention item was raised, the Task
root is removed without the repository, the reason is written to the item's
`details.settled`, and the success resolves it (`git worktree prune` in the
repository drops the stale registration if it ever returns). A Repo row that
is gone, or one with no checkout and no clone, is still reclaimed at once.
A bounded sweep runs on startup and every ten minutes to backfill terminal Tasks,
including missing worktrees and managed homes left by older installs. The
deadline worker snapshots a bounded set of due rows before processing them, so
rescheduling one failed cleanup does not remove later due rows from that tick.
Ordinary failures persist an attempt count and bounded error tail, then retry
with exponential backoff from one minute up to one hour, with no attempt limit.
A failed branch deletion counts as a failed cleanup: the workspace stays
`cleaning` and is retried. After five failed attempts (about half an hour)
Forge raises one Project-scoped attention item (`progress_warning`, dedupe key
`workspace-cleanup:<workspace id>`, with the path, the attempt count and the
last error) and appends one `workspace.cleanup_failed` domain event. Later
retries do not add or reopen items. A successful cleanup, or the Task leaving
cleanup eligibility, resolves the item. An unreachable daemon
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
legacy `TaskStatus`/`transition_allowed` state machine. Each service constructs
one `Arc<WorkflowEngine>` from its immutable database/event-bus dependencies;
all service clones, transition/recovery/board/role entry points and queued
workers share it. A borrowed `WorkflowExecution` binds that engine to the
fully configured originating service for an operation. Hooks receive that
service through `HookContext`; the engine never owns a service or snapshots
mutable provider/executor/workspace configuration. Configuring a service does
not construct another engine. The queued worker retains a service clone with
its worker-cache ownership detached, so neither graph has an ownership cycle.

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
queued separately. The entry status/epoch fence runs when a step executes.
Checkpoints and effects retain the lease-owner fence; the former per-hook epoch
rereads and Task-state CAS retry loops are removed. In-transaction helpers that
write status, blocking annotations or recovery metadata fence through
`fence_task_lease_in_tx`, which debug-asserts that the caller runs in that Task's
step.

In-lease version override: a Task SQL effect applied under the lease
(`apply_task_sql`) keeps every predicate but rebinds `AND version = ?` to the
version read in its own transaction, because the lease already makes the step the
only workflow writer and a content edit may have bumped the version. The version
guard is therefore off inside the lease: an in-lease read-modify-write that spans
an `.await` must not rely on it to detect another writer.

Queued effects carry a fence. `Entry` (the default) applies only while the Task is
still in the status entry it was queued in, and a preempting Cancel/Hold supersedes
it while pending; it is for effects that are moot once the Task has moved on
(status writes, entry barriers, deferrals for one entry, transition-class commands
and the startup stale-annotation sweep). `Identity`
effects are fenced by their own SQL predicate or re-derive their work when they
run; they apply after a status change and survive a preempting Cancel/Hold. They
are the must-not-lose effects: dispatch wakes (per Task, Project, Repository and
readiness), clears of a Task's own wait markers, the `pending_remote_cancel`
clear, plan-settlement and owner-recovery blocking annotations (the latter
re-derives its admission conditions in SQL), agent-deletion and
coordination-root role clears, `block_cancelled_dependencies`,
`advance_coordination_root`, the coordination-review flag and an execution's
completion cascade (`maybe_cascade_executor_completion`, which re-checks the
current state, role attempt and settlement receipt). A completion that runs while
the Task is held is a no-op; releasing that hold settles the completed attempt
instead of re-running the role.
`TaskQuery::execute_in_tx` returns `Applied(rows)` or `Queued { step_id }`; a
lease-holding writer requires `Applied`, and no caller treats `Queued` as applied.

Callers that need a result enqueue a command and drive the same worker inline.
A registered notification, checked before waiting, bounds the predecessor wait
without polling: one deadline of five seconds, or fifteen for Cancel/Hold (five
plus the ten-second remote acknowledgment). Once that command claims the lease,
its result describes its own committed Task; running its own step is not counted
as waiting (claim with environment checks and `rerun_review` are bounded only by
their checks' timeouts). A timeout returns HTTP 409 `task_busy` with
`pending_steps`, `retry_after_ms` and a retry hint; the accepted command remains
durable. Hooks and cascades remain asynchronous, and responses retain
`pending_steps`. Claim and its execution startup share one command; dispatch
continues if the HTTP waiter expires. Cross-Task wakes enqueue without waiting for
another Task lane, and explicit initial role assignments publish in the new Task's
birth transaction. A root Cancel commits under the root lease and enqueues each
child's cancel as the child's own preempting step; it never waits on a child lease.

Cancel/Hold preempts scripts, CI and startup at a safe point. Integration is
protected from its start (`integration_started_at`) through its result and
terminal cascade: a preempt request never interrupts it and no acknowledgment
timeout settles it. A Cancel/Hold waiting behind it returns `task_busy` at its
bound and stays queued; when it runs after a landed merge it returns the done Task
and records a moot-Cancel system comment. A daemon disconnect mid-merge settles
through workspace containment and reconnect. Remote non-integration commands
receive `workspace.cancel`; acknowledgment wait is at most ten seconds. Without
confirmation the hook is superseded with `remote_operation_unconfirmed`, while
`pending_remote_cancel` durably excludes that workspace from new steps and
executions. The record is daemon-scoped (no foreign key to the Task, step,
workspace or placement), so deleting any of them proceeds and reconnect still
cancels it. Owner reconnect retries cancellation and clears the exclusion on
`killed`, `already_finished` or `unknown`. Restart/Retry/Release uses the existing
queued recovery and a blocking annotation naming the machine until cleanup
confirms. Late results cannot write through a superseded step's owner token.
Transport loss alone does not abort a daemon command. Operations reports the
pending count, and Task workspace detail names the exclusion. Crash recovery
enqueues at most one live recovery command per Task and running-execution set,
and publishes `task.recovered` when that step settles dead executions.

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
Recovery treats a completed workflow-role result
whose immutable Project revision is missing or superseded as unsettled and
dispatches a replacement under current authority; it never converts the
cascade's intentional no-op into a reconciliation receipt. A failed coder or
planner run is not fenced this way: a failure advances nothing, so it is
retried (or blocks the Task) under the current Project revision whichever
revision dispatched it. A Project edit, pause or resume therefore never
leaves an active Task with no run, no pending retry and no park. A failure is
fenced by run currency instead: the Task must be in the state and state entry
the run was dispatched for (`execution_belongs_to_current_state_entry`), the
state must still belong to the run's role, and the write itself requires the
run to be the newest of its role with its Agent still assigned
(`latest_execution_authority_matches_in_tx`). A failure that loses any of
these changes nothing and spends no budget.

**Dispatch failure entering an active state:** when a dispatch hook
(`dispatch_role_agent` / `dispatch_fix_agent` / `dispatch_executor`) fails
`on_enter` of an `active` state and the task has no running execution, the
engine does not leave the task there looking in-flight. It records the
dispatch error as the Task's diagnostic (`condition.details.diagnostic`, type
`dispatch_failed`; stored in `error_annotation` until stage 5) and
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
typed blocker onto every unfinished dependent, whatever that dependent was
waiting on: the blocker names every cancelled prerequisite and carries the
condition it displaced. New links to cancelled Tasks are rejected. Removing a
cancelled prerequisite while another remains renames the blocker; removing the
last one restores the displaced condition (a held Task is held again, a parked
Task is parked for its original reason with its original offers) or, when there
was none, clears the blocker so scheduling resumes.

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

`db::budget` owns every Task budget limit, comparison, charge and remaining
projection. Persisted spending lives in `task_budget(task_id, kind, window_id,
spent)`; `task_budget_charge` receipts identify the consuming step/effect so
replay cannot spend twice. Ledger changes commit in the authority-fenced
transaction that consumes the attempt (verdict settlement, retry scheduling,
execution admission, carried review or transition). Counts never come from
transition-log scans, execution summaries, or JSON counters.

The resolver reads the Task-wide `task_state_config.retry_budgets` first, then
the state config the consuming step supplies, then gate `max_rejections`
(Review/MergeFix), then the kind default. Workflow hooks supply their merged
state config, which carries the Task's per-state overrides
(`task_state_config[state]`); reviewer completion, Execution retry and the
workflow guard supply the workflow's own state config, so per-state values
apply only on hook paths. The web editor writes Task-wide values, which
therefore win everywhere. Existing Project merging policy and review
configuration participate in the merged state config; Project top-level
review/execution retry fields retain their existing inert behavior. Invalid
nonnegative-i32 retry values fall through. A review entry hook that fails with
no failed Review uses the same order but falls back to "unlimited" when there
is neither an override nor a gate cap. WorkflowGuard
uses the execution limit but has independent spending. The merging gate cap
and MergeFix are distinct existing constraints: both previously counted the
same merging rejection rows, and now have independent ledger rows. This keeps
the gate's effective cap when a repair override differs from its gate limit.

`remaining = max(limit - spent, 0)`. Review charges each chargeable failed
verdict, including the one that parks: the standard limit two gives one routed
bounce, with remaining two → one → zero. Entry into review and a passing
verification remain legal even when the remediation budget is spent. Existing
hook gate cascades marked as rejections retain their budget debit; a failed
verdict and its bounce share one charge, rather than charging both, and a
failed review entry with no failed Review shares its `entry:<hook step>`
receipt with the bounce cascade. Exhausting that entry budget, or the
cancelled-review entry cap, parks the Task behind a `review retry budget
exhausted` entry barrier. Already
admitted outcomes can leave spending above a subsequently reduced limit;
remaining stays zero. Additional-retry kinds admit their last allowed retry
and block the following one. Limits bound each kind, not total Task Executions.
Owner retries, send-backs and escalation answers do not spend agent retry
budgets. Owner authority is typed (`Actor::is_owner`): REST, web and
`forge-ctl` sessions and owner escalation answers. MCP credentials (PATs,
sessions, OAuth tokens) all resolve to the user with no agent-scoped kind, so
MCP `forge_task_action` carries a delegated user actor: it keeps owner action
offers but spends budgets like an agent. Audit rejection flags remain evidence
and are not budget counters.

| Kind | Standard allowance | Natural window / charge |
|---|---|---|
| Review | 2 failures, 1 routed bounce (fallback/autonomous limit 3) | Review origin window; failed verdict and existing charged gate outcome |
| GateRejection(state) | Planning 2; merging 1; cancelled review entry `max_rejections - 1`; custom gate limit | Matching gate; rejected transition |
| MergeFix | 1 ordinary repair; 0 disables | Merging origin window; ordinary repair transition |
| Execution | 3 additional automatic failure retries | Explicit reset window; failure-driven retry scheduling |
| WorkflowGuard | 3 additional follow-ups, using execution overrides | Since successful completion; guard follow-up intent |
| TargetMovedRebase | 5 clean refreshes | Explicit reset window; successful typed clean-rebase cascade |
| ConflictHandoff | 5 Worker handoffs | Explicit reset window; typed repair handoff (physical conflict can already have occurred) |
| ReviewCarry | 5 carries, then real review | Authoritative review contract; carried Review settlement |
| AutomaticReviewRecovery | Disabled normally; configured default 1 | Review-budget exhaustion episode; execution admission |
| ReportCorrection | 2 native correction turns | Volatile counter per invocation; provider-turn intent |
| ReviewCheckRerun | 2 check re-runs after a timeout (3 runs) | Volatile counter per review evaluation |
| ReviewCiInfrastructure | 5 connected failures, 4 automatic retries | Interruption episode (each fresh review entry); connected retryable CI failure |

A typed `RetryWindowReset` additionally resets every persisted kind. Successful
completion resets WorkflowGuard; a fresh passed contract resets ReviewCarry;
a fresh state entry, CI success or owner reconnect starts a fresh
infrastructure episode, while an entry retry keeps it. Execution
spending does **not** reset merely because status changes, so review laps cannot
refund failure retries. Automatic recovery opens an episode when Review
exhausts (decided where the review limit is resolved, so a Project stored with
the `'{}'` workflow uses the default workflow's gate), closes it on reset or a
new exhaustion, and counts cancelled attempts within that episode.
Completed/cancelled execution evidence remains intact. ReportCorrection and
ReviewCheckRerun remain volatile; their limits/comparisons use the same module.

Owner-fixable failures and repeated coder findings after a previous matching
failed reviewer attempt spend nothing. The charge point verifies the assessment,
all-zero checks, reason provenance and previous verdict. Environment blocks also
spend nothing. Parking and recovery use that same classifier; first-attempt
repeat flags remain ordinary chargeable failures.

Migration `V202610051649__task_budgets` snapshots retained counters and each
kind's current derived window, preserving upgrade allowances without recovering
erased history. A Project whose stored workflow has no states array (the `'{}'`
column default) is read as the default workflow's gates, so its planning-gate
spending survives. Task configuration is never rewritten: resolution keeps its
pre-ledger order. It keeps audit/review/execution evidence, converts queued typed
metadata effects atomically with the ledger, and removes replaced JSON counters.
A pre-upgrade failed verdict awaiting disposition is reconciled once; already
routed or exhausted verdicts receive backfilled charge receipts. Budget writes
invalidate Task-list revisions in the same transaction.

Review-CI infrastructure retry applies only to a typed unreachable owner or an
RPC timeout before a CI command was sent. It spends no review rejection budget
and creates no Review row if CI never ran. Barrier, annotation, persisted
ledger spending, and deferral commit atomically under Task/Project authority.
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

### Typed transition bridges

Transition history separates human `trigger_reason` prose from nullable
`bridge_kind` and `bridge_payload`. The kinds are `review_refresh`,
`target_moved_rebase`, `conflict_handoff`, `retry_window_reset`, `recovery`,
`gate_skipped`, `gate_approved`, `gate_rejected`, `ci_only_review_passed` and
`review_carry`. A target-moved rebase also has refresh semantics. Conflict
handoffs store `{"paths":[...]}`; recovery/reset payloads store `{"verb":...}`.
An ordinary/unclassified row has null metadata. Kinds do not replace actor,
state, immediate-predecessor, rejection or current-window checks. The columns
carry no enumerated `CHECK` (the payload keeps `json_valid`), so a new kind
needs no table rebuild: Rust writes only typed values and decodes a stored kind
it does not know as a typed `TransitionBridgeCorrupt` error, never as an
unclassified row.

Every transition reader uses typed evidence: refresh dispatch, carry entry,
rebase/handoff limits, gate bypass/decisions, cascade admission/rejection,
conflict-marker checks, conflict hotspots and the web CI-only badge. Memory
failure indexing uses typed evidence only: rejection, failure-named states,
a hook that returned `failed`, a `FailureKind` interruption on the Task once
the transition's hooks settled (for example the dispatch-failure rollback),
or a review-verdict hook that acted on a failed Review. Reason words and hook
messages are not failure signals; history backfill has only the row's own
evidence. `gate_approved` marks exactly the plain approval (reason
`gate approved`), as the gate reader always treated it; an approval with owner
guidance, a send-back and a human-review approve or reject carry no gate kind.
Only `retry_window_reset` establishes a new retry window. `recovery` marks the
same-state `retry` marker of whoever applied the action; the gate-to-target move
it allows is an ordinary rejection with no kind.

Migration `V202610051343__typed_workflow_bridges` preserves all old reason text,
backfills history and saved command/cascade/hook/repository-mutation and checkpoint metadata once, and leaves incidental
marker words unclassified. Gate decisions are backfilled from the
case-sensitive `gate approved` / `gate rejected` prefix of any actor, exactly
the rows the old reader counted; Workflow-authored `[review-refresh]` rows
(including manual merge repair) become `review_refresh`. The intentional refresh/rebase pair becomes one
`target_moved_rebase`. A genuine conflict handoff takes priority over conflicting
refresh tags. The last paths-JSON suffix is decoded only if it is an array of
strings; malformed/missing paths retain handoff identity with null payload.
Runtime never falls back to parsing prose, including when replaying queued work.
Historical event prose may be bounded; the hotspot consumer reads the full
kind/payload from the authoritative transition-log source row instead.

Automatic review-recovery executions have typed `purpose =
automatic_review_recovery`, set atomically during execution admission. The
attempt count covers only running attempts, which is what the former
summary-prefix count effectively measured (terminalization replaced the prompt
summary). Attempts stay serialized and a Task may receive one per retry
exhaustion, as before; `automatic_recovery.max_attempts` is not a lifetime cap. Changing a summary cannot
change the count, and a normal follow-up does not inherit its parent's purpose.
Existing recovery summaries are preserved by the migration.

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
  A completion from a stale Project version cannot settle the Task. A
  failure is retried or blocks under the current Project version, so a run
  that fails after a Project edit, pause or resume still spends the budget
  and backs off.
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
- **workspace** — Task worktree mechanics and Task-root reclamation, shared by
  the server and the daemon. There are no lock files: keyed in-process locks
  serialize repository-cache/integration and Workspace execution operations.
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
rebase enter a typed review-refresh route; they do not consume merge-fix budget
or dispatch a coder. Actual conflicts enter bounded merge repair. Changed content
must receive a new semantic review, regardless of `review_passed_at`, except for
the mechanical carry below. Explicit human and no-agent-review workflows remain
separate and cannot manufacture an automated Charter assessment.

**Review authority carry.** A Task that loses merge races on shared hub files
would otherwise pay a full reviewer run per lost race. The `review` state's
`on_enter` hook `carry_review_authority` (ahead of `dispatch_role_agent`) keeps
the previous approval when the Task re-enters `review` only because (a) Forge
rebased it cleanly onto a moved target (the transition log ends with the
typed `target_moved_rebase` bridge followed directly by the move
into `review`), or (b) its Worker completed the repair of a Forge-committed
rebase conflict (the bridge has `bridge_kind = conflict_handoff`). Every condition must
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

### Task condition scheduler and owner matrix

Stage three switches the dispatcher from scanning every Task every 10 seconds
to reconciling the Tasks something changed. Its decisions are those of the
scanning dispatcher: every Task that was dispatched is dispatched, to the same
target, in the same order and within the same limits, no later than before;
every Task that was held is held; and every legacy field, event and annotation
that was written is written with the same value. Stage four switches public health, awaiting-human, exceptions, actions,
operator Task status, MCP, Attention and web to the condition; the dispatcher
keeps its dispatch decisions and adapter-level reconciliation unchanged. Three things differ, and
nothing else:

1. a Task that sat forever with no step, no owner and no annotation is parked
   explicitly, with an owner action;
2. a missed wake is repaired within 120 seconds;
3. the dispatcher does no work on an idle tick.

**Resolution.** `next_step(snapshot)` is pure and total over default and
custom workflow states: one batched read per page of Tasks loads condition,
entry epoch, queue ownership, role assignments, the latest execution per role,
Reviews, hierarchy and placement, and the resolver returns either a step for
the existing `task_step` command/hooks adapter or a typed park naming an owner
and a recovery action. Admission keeps its authority, capacity and
optimistic-concurrency checks, and the facts a pure function cannot read (the
admission gates, the machine precheck, Agent availability) are taken at each
Task's own turn in the pass, so a Task sees what earlier Tasks of the same
pass did, as it did in a scan. A refusal is written by the same writer as
before: the dependency gate records its disposition and blocks on a cancelled
dependency, the Project limit and a machine run slot record their capacity
wait, a failed provision records its refusal annotation. A recorded refusal
holds until the Task's version changes or `services::wake_task_dispatch`
clears it; nothing else re-opens it. A role dispatch is a queued Task command
that resolves the Task again under its lease; role commands of different
Tasks are claimed independently, across Projects, Agents and lanes, and the
command runs on the one dispatcher instance of the runtime, whose stop fence
it observes.

**Kicks.** `task_schedule_dirty` holds one row per Task with a monotonic
generation. Triggers write it in the transaction that commits the fact, so a
crash after commit loses nothing, and acknowledgement clears only the
generation a pass read. Only a change that can alter a dispatch decision
kicks:

| Commit | Tasks marked |
|---|---|
| Task create, status, version, condition, priority, plan, hold or release | the Task |
| Task status, order, parent or condition | its parent, siblings, children and dependants; waiters on its Project's limit |
| Dependency or role assignment added, changed or removed | the Task (a root's coder: its children too) |
| Step queued, claimed, finished or retried (not a queued Task write) | the Task |
| Execution ends, or changes Agent, workspace or snapshot | the Task; waiters on that Agent, on a machine run slot and on the Project's limit |
| Execution starts | the Task; waiters on the Project's limit (their wait names the active count) |
| Execution heartbeat | nothing |
| Review or transition row | the Task |
| Workspace or placement change | the Task and its children; waiters on that Agent or machine |
| Machine status, run limit, version or CLI report | waiters on that machine or a run slot; Tasks whose Agents it hosts |
| Readiness result | Tasks waiting on that machine in that Project |
| Remote cancellation marked or acknowledged | the Tasks it fences |
| Project version, workflow, settings, pause or primary repository; repository | every Task of the Project |
| Agent profile, status, pause or slot count | Tasks assigned to that Agent |
| Chat turn leased or released | waiters on that machine or a run slot |
| Queued admission released | waiters on that Agent or a run slot |

`wake_task_dispatch` and the Project-wide wake clear the stored refusal and
deferral and bump the Task version, as before, and also mark the Tasks.
Some holds turn on facts no commit announces. Credentials, provider health
and backoff, connection health and CLI policy all surface as Agent
availability; a placement refusal is re-evaluated against a machine's live
connection and due provisioning retries; a step that had nothing to do yet,
and a Task whose pass failed, are asked again. Those Tasks, and only those,
are kept in an in-memory set and re-read at the scan interval (10 seconds),
which is when the scanning dispatcher re-read them. Exact deadlines (retry,
owner grace, failed-Review grace, readiness, reservation and lease expiry)
wake the loop at their time. A direct `check_once`, as tests and fixtures
call it, re-reads that set at once, and the first one an instance receives
reads every Task that is not settled, as a first scan did; the operator
refresh reconciles every Task at once. `project_schedule_dirty` keeps
repository maintenance working for Projects with no Tasks.

**Sweep.** Every 120 seconds, and from the first tick after startup, the
dispatcher walks every Task that is not settled, in keyset pages of 100 read
from the partial index `idx_task_schedule_open`, outside the dispatch pass and
never holding the writer for a read. A settled Task is never visited. Each
page is condition-checked (the stage-two check, repaired with its fenced
statement), resolved without I/O, and handed to the next dispatch pass when it
has work nothing kicked, a park that no longer says why it waits, or no owner
at all. The last case is the invariant: every Task that is not settled has a
queued step, a live execution or a park. A violation is logged, repaired by
reconciling and counted in the existing invariant report, with every Task
whose pass failed. The sweep replays no recorded refusal. Its cursor is in
memory; a restart begins a fresh lap. A tick spends at most 100 ms on it and
then yields to dispatch, and startup never waits for it: dispatch starts from
the durable dirty set and the first lap runs behind it. After a mapping
revision change the backfill covers every Task once, settled ones included,
in its own bounded slices.

**Failures stay with their Task.** The legacy fields stay authoritative: when
a stored condition does not match the legacy fields read in the same snapshot
(a writer missed its sync), the pass resolves from the legacy fields and
repairs the stored copy. A Task whose stored condition cannot be decoded is quarantined as a named
unknown park; its raw condition is preserved for a server that understands it.
A Task whose reconciliation fails is logged, counted and retried at the scan
interval; the pass continues, and only a pass that completed records the
commit generation it saw.

**Parks.** Derived parks live in `task_schedule_park`, fenced by status epoch;
they are not a second queue. A Task waiting on capacity is parked once:
`task_schedule_wait` records the Agent it waits for, whether it waits for a
machine run slot (`daemon_id = '*'`) or a named machine, and whether it waits
on the Project limit, so that a capacity change kicks exactly the waiters it
can admit and the wait writes nothing while it lasts.

A `WorkflowInvalid` or `UnknownCondition` owner park is folded into the typed
condition from the epoch-fenced `task_schedule_park` record. It names the same
owner and recovery guidance in health, exceptions and Attention. Stage four
removes `legacy_park.rs` and its temporary `workflow_guard_rejected` annotation
bridge. The migration captures a bridge left without a park by a crash before
clearing only dispatcher-owned bridge annotations. No user annotation is removed.
These observer parks neither block dispatch nor change slot classification;
resolution clears their condition when the Task can continue.
These parks are reserved for Tasks nothing can continue: a state the workflow
does not define or gives no scheduling meaning, a merge entry whose hooks were
lost and cannot be replayed safely, a plan publication marker that cannot be
read. A Task whose role nobody holds, or a person holds, is human work and is
left exactly as before.

| Park reason | Responsible owner | Recovery action |
|---|---|---|
| Held | User who held the Task | Release the hold |
| Failure, AgentTimeout, BudgetExhausted | User / Project Agent | Repair the cause, authorize recovery or a new budget window |
| EntryBlocked | Workflow | Inspect the entry barrier and retry the entry |
| UnknownCondition (visible) | Project owner | Move the Task back and forward again, or cancel it |
| WorkflowInvalid (visible) | Project Agent | Edit the workflow or move the Task to a state it defines |
| HumanDecision, HumanWork | User / Project Agent | Approve, do the work, assign the role or move the Task |
| Capacity | Scheduler | Wait: freed capacity kicks the matching waiters |
| DispatchRefusal | User / Project Agent | Correct the Task, Project or execution setup; a wake or a Task change re-opens it |
| ProjectPaused | User / repository or readiness owner | Fix setup and resume the Project |
| OwnerOffline, DaemonUpgradeRequired, AgentUnavailable | Machine or Agent owner | Reconnect, upgrade, enable the Agent or select another |
| Environment, PlacementDenied | Machine / Project execution-setup owner | Check, provision or repair the environment or placement |
| RemoteCancelPending | The remote operation's machine | Acknowledge the cancellation |
| Dependencies | Project Agent / dependency owner | Complete or resolve the dependency |
| Children | Project Agent / child workers | Settle the ordered child sequence |
| PlanSettlementWait | Publication worker | Settle the publication or cleanup |
| QueueOwned, InFlight | Task-step / execution worker | Wait for the live owner; its lease timer owns recovery |
| RetryDeadline, ReviewGrace | Scheduler | Re-read at the deadline or the next scan interval |
| ExecutionStopped | User / assigned role owner | Confirm, repair or resume the stopped attempt |
| ReviewChecks | Workflow / check owner | Complete or retry the required checks |

Slot accounting is the existing legacy projection: an admitted `OwnerOffline`
wait keeps an active slot and `ReviewNeedsOwner` a parked one. Active recovery
precedes new admission; initial candidates keep priority, creation and ID
order; only an admission from an initial state takes a Project slot.

**Equivalence.** `task_dispatcher/tests/equivalence.rs` replays real rows
through the dispatcher and compares the outcome with
`fixtures/scheduler_equivalence_9d9b228f.txt`, recorded by running the same
file on the base commit: dispatched or held, the target, every legacy field
and every event, for a hundred Task shapes at startup and in steady state, a
load test with a machine cap and a Project limit, failed-Review recovery and
wakes. The only differences accepted are the fixture's `@`-lines, each naming
one of the three allowed changes.

**Stage five.** SQLite drops a table's triggers and indexes with the table,
and refuses to drop a column an index or trigger names. Any `task` column drop
or table rebuild must first drop the `task_schedule_*` triggers on `task` and
the partial index `idx_task_schedule_open` (which reads `condition_json`), and
recreate them afterwards. The same holds for the triggers on every other
table the scheduler watches.

Stage four must compose these owner results into the public condition DTO and
switch all projections together, replace the visible park annotation with the
condition's own park, preserve the material-blocker digest and slot
classification, regenerate bindings and document the single public beta break.

### Passive canonical checks (3.3 stage A)

**Nothing executes through this contract or these repositories yet.** Entry
hooks, `ReviewRunner`, conformance, lifecycle scripts and environment probes
retain their existing commands, scheduling, timeout, output and cleanup
behavior. No legacy Review or check evidence is imported into a trusted cache.
The new server timeout setting is defined but no execution reads it.

`api-types::check_spec` is the shared, dependency-free contract location.
`review::check_spec::build_check_spec` builds `CheckSpec` from the same review
source snapshot used for admission, the current hook state’s effective config
(for entry CI in custom workflows), the selected Project environment, selected
lifecycle event/hooks and role. `review` already depends on `api-types`, as do
`services` and `db`; this introduces no crate cycle. Conformance reuses
`contract::context_from_source`, including governing requirement selection and
read-only filtering. The existing persistence-free 3.2 `CheckRunInput` and
`CheckRunOutcome` remain the execution primitive's input and outcome.
`CheckCommandOutcome` moves verbatim to `api-types` so the effect and stored
step evidence share one type; no second CI outcome representation is introduced.

`CheckSpec` contains `schema_revision`, `scope`, ordered `commands`,
`declares_cleanup`, `execution_policy`, and the evidence-only pair
`configured_commands` / `blank_commands`. It has no purpose and no whole-run
timeout: `CheckPurpose` only selects which bundle the builder assembles and is
recorded on the consumer row, and the wall limit is recorded on the run row.
`declares_cleanup` says whether the bundle has a cleanup step the runner must
perform; no family configured today declares one. Each `CheckCommandSpec`
contains `id`, exact `shell_text`, explicit `shell` (`bash -lc` today),
`working_directory`, sorted `environment_keys`, `timeout_seconds` (`None` for
unbounded legacy CI), `failure_policy`, `cacheability` and sorted
`requirement_ids`. The policy revision distinguishes today's server and daemon
semantics, including inherited environment, Git-variable removals and machine
build policy. Keys describe explicit inputs passed to the command; inherited
host inputs must additionally be controlled by the owner attestation before
reuse is possible. The builder never copies Project environment values into a
spec. Every existing configured command defaults to `uncacheable`; a future
configuration declaration must explicitly establish controlled inputs before
using `declared_controlled_inputs`.

| Execution family | Characterized bundle, cwd, environment and timeout |
|---|---|
| Review-entry CI | Effective workflow/Project/Task CI in order; Task root; Project keys; no per-command limit; stop at first failure. |
| Manual `ReviewRunner` CI | Same effective CI resolution and command semantics, hence the same spec and digest as review-entry CI; only the consumer's purpose (`review_ci`) differs. |
| Conformance | Effective setup followed by CI-derived and requirement-linked checks, using the admitted governing context; Task root; Project keys; configured 1–14,400-second per-command limit, default 1800. Setup failure stops the bundle; required-check failure does not stop later checks. |
| Blocking before-work | Selected event's blocking scripts only, original hook indices; Task root or existing lifecycle fallback; Project keys plus the eleven `FORGE_*` context keys; configured limit or legacy 30-second zero fallback; stop at failure. |
| Other lifecycle / hook test | Selected script hooks; the asynchronous before-work path excludes blocking scripts; existing workspace/fallback cwd and context keys; same timeout fallback; continue between scripts. Plugins have no shell-check contract. Owner hook tests select an original hook index and bypass the asynchronous blocking-hook filter. |
| Environment preflight | Role-applicable environment checks in order; Task root; Project keys; limits clamped to 1–300 seconds; stop at failure. |
| Project/machine readiness | Already selected readiness probe set in order; repository or owner probe scratch; Project keys; 1–300 seconds; collect all checks. Selection and owner routing remain outside the builder. |
| Exported environment helpers | Role-applicable selected checks; supplied directory; Project keys; 1–300 seconds; stop at failure. The single-check helper supplies `single_environment_check` and ignores role filtering, as today. |
| Agent-selected commands | No configured authoritative bundle exists. The builder returns an empty `agent_selected` spec; it does not manufacture reusable CI from arbitrary agent tool calls. |
| Future queue-head CI | `queue_head_ci` can describe CI-only or conformance bundles. The caller must explicitly supply `QueueHeadCheckBundle`; there is no default and no execution. This leaves the later gating decision open. |

**Run scope.** `CheckSpec.scope` says who may share one run of the bundle, and
it is part of the digest. `check_family_scope` classifies every family:

| Family | Scope | Why |
|---|---|---|
| Review-entry CI, manual review CI, queue-head CI (CI only), agent-selected | `commit` | The commands only read the commit. Every Task at that commit shares the run; no Task or workspace is in the identity. |
| Conformance and queue-head conformance **without** setup steps | `commit` | Same: checks only. |
| Conformance and queue-head conformance **with** setup steps | worktree | Setup steps prepare the worktree the checks then run in. |
| Blocking before-work, other lifecycle scripts / hook test | worktree | The script acts on the Task's worktree (or is owed to the Task's event). |
| Environment preflight | worktree | It judges the state of the Task's worktree, not the commit. |
| Project/machine readiness, exported environment helpers | probe | Workspace-scoped when the probe runs in a workspace; otherwise `commit` (no Task exists; the machine is covered by the attestation). |

A worktree-scoped spec carries `workspace { workspace_id, generation }`: the
workspace ID and its `workspace_placement.generation`, the durable identity
already used to fence workspace operations. Two Tasks at the same commit with
the same before-work script therefore get two runs, and a re-placed worktree
(next generation) is a new target. A worktree-scoped family that has no
worktree (lifecycle fallback cwd) carries `task { task_id }` instead, so it
still runs once for its own Task; with neither a workspace nor a Task the
builder refuses. The repository refuses a request whose `workspace_id` or
`task_id` differs from the one its scope names; it does not yet compare the
generation with the placement row (stage C rechecks authority on every hit).

**Blank steps.** Production runs a blank or whitespace-only CI step and it
passes. The builder never refuses one: it drops the step, because the outcome
is identical and nothing runs. `configured_commands` keeps the configured step
count and `blank_commands` the zero-based configured positions that were
dropped, so evidence can still say "step 3 of 5 was blank". CI command IDs
number the steps that run (`ci:0`, `ci:1`, ...), and the blank-step record is
excluded from the digest, so a list with blanks has the identity of the same
list without them and an all-blank list is exactly the empty-steps auto-pass.
Blank hook and environment-check commands are dropped the same way.

Characterization tests pin each configured family's command order, IDs, cwd,
explicit environment keys and per-command limits. Existing CI primitive tests
also compare the built spec with the actual commands yielded by `CheckRun`.
The whole-run wall limit is not in the spec and does not change those
per-command limits in this stage.

**Execution digest.** `CheckDigestInput::encoding` validates the input, then
produces compact UTF-8 JSON in a `forge.check-execution/2` schema envelope
(spec revision 2; stage A's `/1` was never deployed, so no stored digest needs
converting).
Object keys are recursively sorted lexically, arrays preserve order, and no
Unicode normalization or command whitespace normalization is applied. SHA-256
of those bytes, lowercase hex, is the digest. The fixture under
`crates/api-types/src/check_spec/fixtures/` pins the input, exact encoding and
hash; a key-permutation test pins invariance, and a test that changes one typed
field at a time (every spec and command field, each scope kind and workspace
ID/generation, each environment value kind, the attestation and the revision
number) pins invalidation. That test destructures the spec, command and digest
input exhaustively, so a new field does not compile until it is listed as
semantic or as not semantic.

Included:

- the spec's semantic fields: command order, IDs, shell text and shell
  semantics, cwd rule, environment key names, per-command timeout and
  stop/continue policy, cacheability, requirement links, whether a cleanup step
  is declared, schema revision and execution-policy revision;
- the run scope: `commit`, or the workspace ID and generation, or the Task ID
  (see the classification above);
- each declared key's classified identity: a controlled non-secret value, an
  opaque owner-maintained secret revision, an explicit removal, or a `volatile`
  marker (never the per-run value);
- the audited execution revision **number** (nonzero requires an audit ref), so
  a forced rerun never reuses or joins the previous execution;
- attested execution-input digest, or the distinct `not_attested` value, so a
  result never crosses machines or environments.

Excluded:

- **purpose** (entry CI, review CI, queue-head CI, ...): same commands, same
  commit, same environment attestation and same execution revision are one
  run, whoever asks. Purpose is a column of `check_consumer`;
- **the whole-run wall timeout**: it is a server setting, recorded on the run
  row as `applied_timeout_seconds`. Changing the setting neither splits an
  identity nor invalidates a reusable result, and a run that ended by timeout
  is never reusable, so no stale timeout verdict outlives a raised setting;
- the blank-step record (`configured_commands`, `blank_commands`);
- secret bytes and public hashes of secret bytes; secret changes use opaque
  private revisions rather than exposing a low-entropy secret hash;
- volatile values such as run/operation IDs, timestamps, lease values, output,
  Task epoch, execution ID and per-run directory paths; volatile declared keys
  prevent reuse rather than making a misleading stable cache key;
- audit-reference location (it proves why the revision advanced, not what ran);
- consumer origin/delivery metadata and semantic review authority; a mechanical
  pass cannot grant semantic approval or bypass human review.

Only full lowercase 40- or 64-hex commit object IDs are accepted; mutable refs
and abbreviations are refused. The future owner must additionally verify that
the object exists. Project ID, repo ID and exact commit are outside the spec digest and inside the
separate `forge.check-scope/1` identity key. This prevents sharing evidence
across repository or Project security boundaries. The owner attestation binds
inputs outside the commit: `ServerCheckExecutionInputs::identity` hashes an
explicit toolchain revision, environment revision, asset target-to-revision map,
secret key-to-opaque-revision map, shell revision, runner revision, server OS and
architecture in a `forge.check-server-inputs/1` envelope. All revision strings
are non-secret owner identifiers. Runtime ID alone is never an attestation.
This function is pure; stage A does not probe tools or infer that today's
inherited environment is controlled. Daemons use `NotAttested` until the owner
protocol can attest the same inputs. Uncontrolled network, clock, ambient files
or mutable dependencies require `uncacheable`; attestation cannot turn such a
spec into a reusable check.

**Tables.** `V202610080851__check_runs.sql` is additive, has no triggers or
legacy DML, and preserves existing data. `V202610082320__check_run_identity.sql`
adds `check_run.applied_timeout_seconds` and `check_consumer.purpose`, and
rebuilds `check_result` to change its certification CHECK (SQLite cannot alter
one), carrying every row and every consumer-to-result link. Only fields read/written by the passive
repositories, or required by constraints/indexes, are included. All JSON limits
are UTF-8 bytes, enforced in code and with SQLite BLOB-length CHECKs.

| `check_run` column | Constraint / meaning |
|---|---|
| `id` | TEXT primary key, application UUID. |
| `project_id` | NOT NULL Project FK, ON DELETE CASCADE. |
| `repo_id` | NOT NULL repo FK, ON DELETE CASCADE; repository verifies Project ownership. |
| `commit_sha` | NOT NULL exact commit witness. |
| `spec_digest` | NOT NULL execution digest, computed from validated inputs. |
| `identity_key` | NOT NULL scoped identity digest. |
| `input_json` | NOT NULL complete typed digest inputs, including audit ref; valid JSON, maximum 131,072 bytes. |
| `cacheable` | NOT NULL boolean, computed from all command declarations, attestation and absence of volatile inputs. |
| `state` | NOT NULL enum: queued, running, cancelling, cleaning, uncertain, succeeded, failed, cancelled. |
| `operation_id` | NOT NULL UNIQUE application UUID, allocated before claim and preserved on takeover. |
| `workspace_id` | Optional workspace FK, ON DELETE SET NULL. |
| `machine_id` | Optional physical-owner daemon FK, ON DELETE SET NULL. No capacity admission occurs. |
| `applied_timeout_seconds` | Optional positive integer: the whole-run wall limit this run executes under, taken from the request that scheduled it. Later requests that join or reuse never change it. The runner must apply this value, not the setting current at execution. |
| `lease_owner`, `lease_until` | Optional text, both null or both populated; expiry comparisons use SQLite julianday. |
| `lease_generation` | INTEGER NOT NULL DEFAULT 0, nonnegative; advances on every claim/takeover. |
| `version` | INTEGER NOT NULL DEFAULT 1, positive; every mutation uses and increments CAS version. |
| `created_at`, `updated_at` | NOT NULL timestamps. |
| `finished_at` | Optional timestamp; uncertainty never records terminal completion. |

Run keys/indexes: UNIQUE `(id, identity_key)` supports result's composite FK;
partial UNIQUE `check_run_live_identity(identity_key)` covers queued, running,
cancelling, cleaning and uncertain; `check_run_state_lease(state, lease_until)`;
`check_run_machine_state(machine_id, state)`; UNIQUE operation ID.

| `check_result` column | Constraint / meaning |
|---|---|
| `id` | TEXT primary key, application UUID. |
| `run_id`, `identity_key` | NOT NULL; composite FK to check_run `(id, identity_key)`, ON DELETE CASCADE. |
| `outcome` | NOT NULL enum: pass, fail, timed_out, cancelled, infrastructure_failed. |
| `cleanup` | NOT NULL enum: success, failed, uncertain, not_performed. |
| `certified` | NOT NULL boolean. The repository sets it for a pass whose cleanup succeeded, or whose cleanup is `not_performed` when the spec declares no cleanup step; it requires all ordered commands to have passed. The SQL CHECK refuses it for any non-pass outcome and for failed or uncertain cleanup. |
| `cacheable` | NOT NULL boolean copied from validated run eligibility. |
| `steps_json` | NOT NULL ordered existing CheckCommandOutcome array, valid JSON, maximum 262,144 bytes. |
| `output_truncated` | NOT NULL boolean; supplied capture truncation OR storage tail truncation. |
| `created_at` | NOT NULL timestamp. |

Result indexes: `check_result_run(run_id, created_at)` and partial UNIQUE
`check_result_reusable_identity(identity_key)` for pass + certified +
cacheable. Results are insert-only through the repository. Uncertain
cleanup can retain an immutable receipt followed by a separate reconciled
receipt; the first is never overwritten or reused. Each stderr/combined output
is redacted using transient values, then retained as at most a 4096-byte UTF-8
tail. Actual redaction values are never serialized. The row-level JSON cap also
bounds IDs, command text and timing evidence. When a long bundle's tails exceed
that cap, every tail is halved until the row fits and `output_truncated` is set:
output is evidence, not the verdict, so a finished run can always settle. Only
command text that alone exceeds the cap is refused.

| `check_consumer` column | Constraint / meaning |
|---|---|
| `id` | TEXT primary key, application UUID. |
| `project_id`, `repo_id` | NOT NULL owning FKs, ON DELETE CASCADE. |
| `task_id` | Optional Task FK, ON DELETE SET NULL. |
| `status_epoch` | INTEGER NOT NULL, nonnegative consumer fence. |
| `origin` | NOT NULL enum: entry, manual_review, conformance, before_work, lifecycle, environment, integration. |
| `purpose` | Optional `CheckPurpose` enum (entry_ci, review_ci, conformance, before_work, lifecycle, environment_preflight, readiness_probe, environment_helper, agent_selected, queue_head_ci); the repository always writes it. It records why this consumer asked and never selects the run. |
| `request_key` | NOT NULL UNIQUE idempotency key; contradictory identity/Task/epoch/origin/purpose is refused. |
| `identity_key` | NOT NULL requested scoped identity witness. |
| `run_id` | Optional run FK, ON DELETE SET NULL. |
| `result_id` | Optional result FK, ON DELETE SET NULL; settlement attaches the latest receipt to joined consumers. |
| `created_at` | NOT NULL timestamp. |

Consumer indexes: `(task_id, status_epoch)`, `run_id`, `result_id`, plus the
unique request key. No application/delivery worker exists. Deleting Project or
repo cascades evidence; deleting Task, workspace or machine detaches optional
references. Machine removal's existing tombstone behavior still works. There
are no ON DELETE RESTRICT references. Tests cover each deletion and fresh and
upgrade migration replay without promoting old review rows.

Relative to the inventory: the spec/environment/schema/execution revision and
audit reference are stored once in the bounded input manifest; result identity
joins the scoped run instead of duplicating repo/commit/spec data. Attempt
counters, runtime/placement witnesses, start/deadline/cancel/cleanup timestamps,
run result pointer, delivery/application/contract/review mappings,
HEAD/tracked-change witnesses, drain flags and legacy provenance are deferred:
no stage-A caller reads/writes them. Cleanup and outcome enums can represent
success, failure and uncertainty without selecting later cleanup policy. Nullable
columns can be added when the owner effect, durable runner and Task-step delivery
actually use them. No legacy cache is seeded.

**State machine and repository fencing.** `CHECK_RUN_TRANSITIONS` is data, with
a totality test ensuring every nonterminal has an exit and terminals have none:

| From | Exits |
|---|---|
| queued | running (first claim only), cancelled |
| running | cancelling, cleaning, uncertain |
| cancelling | cleaning, uncertain |
| cleaning | succeeded, failed, cancelled, uncertain |
| uncertain | cleaning, cancelled (owner reconciliation required before either) |
| succeeded / failed / cancelled | None |

Every listed exit is performed by a repository method, and a test drives each
one and refuses every unlisted one. A queued run was never dispatched, so it has
no exit to uncertain; it has no lease either, so it is cancelled with a
version-only fence (`lease_owner: None`) that can do nothing else.
Claim changes queued to running: nobody started a queued run, so it is simply
claimed. Taking over an **expired lease on a dispatched run** (running,
cancelling or cleaning) moves it to `uncertain` in the same statement, keeping
the operation ID and advancing the generation: the lost owner may still be
executing, so the new owner never continues the run as running. Concurrent
takeovers race on the version; exactly one wins. From `uncertain` the only
exits are the ones in the table: reconcile to a settled result with evidence
(`cleaning`, then settlement) or cancel; a rerun needs a new execution
revision, which is a different identity. An expired uncertain run can acquire a
**reconciliation** lease, remaining uncertain; claim never changes uncertainty
back to running. Renew, settlement and every transition of a claimed run require
current version, generation, owner and an unexpired lease. Final settlement inserts
immutable evidence and changes the run atomically. Version/generation/lease
mismatch returns `DbError::VersionConflict`. A storage caller must supply owner
reconciliation proof before exiting uncertainty; stage C owns that proof and
must reconcile retained operations before issuing a new effect. Storage alone
never proves process termination.

Request uses a SQLite immediate transaction: an existing consumer key is
idempotent, a reusable pass attaches a result, otherwise a matching live run is
joined or a new queued run is inserted. Uncertain runs remain in the partial
unique index and block duplicate scheduling. Uncacheable runs can share the
same in-flight operation, but completed evidence is never reused: a later
request schedules a fresh run. The real concurrency test uses a file-backed WAL
pool and four runtime threads with simultaneous requests and claims.

**Reuse rule.** Exact Project/repo/commit/semantic digest identity; every command
explicitly declares controlled inputs; all declared identity values are present;
no volatile input; owner-computed environment attestation; complete ordered
passing commands; certified pass (cleanup succeeded, or the spec declares no
cleanup step and none was performed); and the source run is `succeeded`.
Purpose and the wall timeout play no part. Different execution revisions or environment identities miss.
Failed, timed-out, cancelled, uncertain, unattested, uncacheable and failed
cleanup evidence never yields a hit, nor does a declared cleanup step that was
not performed. The partial result index admits
at most one certified reusable pass per exact identity. A forced rerun must
advance the audited execution revision rather than duplicating that identity.
Consumer authority and current attestation must be revalidated by the future
Task-step consumer before applying any hit; these tables confer no authority.

Operator status adds `check_runs.by_state` (all eight states, zeros included)
and `check_runs.reusable_results`. These are read-only observations, separate
from machine occupancy and severity; no worker or capacity behavior is enabled.

Stage B implements the persistence-free owner execution/transport seam below.
Stage C supplies admission/capacity, reconciliation and Task-step-only
request/application. It consumes these identities/repositories, adds only the
fields it actually needs, and rechecks current authority on every hit/join.
It passes the run's recorded `applied_timeout_seconds` as an absolute owner
wall deadline (default setting 1800 seconds); the two legacy policies retain
unbounded CI until that runner's activation.

### Owner check execution (3.3 stage B)

`check-executor` is a persistence-free owner primitive. It receives a `CheckSpec`,
local checkout target, operation ID, owner identity, explicit owner input
revisions, deadline, cancellation token and a borrowed `CheckPermit`. It acquires
no capacity, writes no check/Task/Review rows and publishes no events. Stage C
must admit the physical machine, retain the stable operation intent, certify the
receipt against the frozen spec and owner inputs, and apply it through Task steps.
There is no cache, single-flight by check digest, recovery loop or timing cutover.

`CheckoutTarget::ExactCommit` verifies a full lowercase commit object ID and
clones an exact detached checkout into a unique `check-*/repo` directory under
the owner's build area. Commands run at that repository root. This never resets
or cleans the source checkout or Task worktree. HEAD/tracked-change witnesses
are captured before removal; a changed managed candidate cannot pass. The
managed directory is removed even after failure, timeout, cancellation or failed
preparation. Removal has a separate settlement bound; unconfirmed removal is
`uncertain` cleanup. Existing CI continues to run in its Task worktree; stage D
owns the managed-checkout cutover.

Each command runs through `process-supervisor`, the single process-group
implementation shared with Git. It continuously drains both pipes, retains
bounded redacted UTF-8 tails and sets truncation flags. Output size cannot fail a
check. The command limit is clamped to the remaining absolute run deadline;
wall exhaustion is `timed_out`. Cancellation, timeout and dropped futures stop
the entire group with SIGTERM, a 500 ms grace period, then SIGKILL; the leader is
reaped. The group is the command's own, never the owner's: ids 0 and 1 are
never signalled. A descendant that moved to another session or group (`setsid`)
is outside the group and is not signalled; that is the platform limit. It is
detached from the pipes instead: after the leader exits, output is read for at
most two more seconds and the receipt carries `stdout_drain_incomplete` /
`stderr_drain_incomplete`; a finished command keeps its exit status even when
its limit expires during that drain.

What outlives a normal exit depends on the policy. Canonical policies stop the
command's group after every command. The two frozen CI policies do not: neither
the server nor the daemon ever stopped what a CI step left running, and a step
may start a service that the next step uses. Those processes are not stopped at
the end of the run either, exactly as before. Git retains its normal-exit
behavior (a hook that outlives Git is not stopped); daemon helpers keep their
two-second post-exit drain bound. Declared cleanup always runs after settled run commands,
using a fresh token and a separate bounded phase, including after failure,
timeout and cancellation. Each cleanup command's own limit is also clamped.
Tree termination grace is additional to the execution/cleanup deadline.
An owner handler interrupted before returning has no certifiable completion:
its retained intent reports `interrupted`, never a pass or permission to rerun.

New execution policies clear ambient environment, disable login-profile loading
and pass only declared keys plus the receiving machine's existing five build
budget variables and niceness. The explicit frozen `legacy-server/1` and
`legacy-daemon/1` policies preserve existing login-shell/environment/Git-variable
behavior during caller migration: the child inherits the owner process
environment (PATH, HOME, toolchain variables), then the Project environment and
secrets, then the run budget; no variable is removed except `GIT_DIR`,
`GIT_WORK_TREE` and `GIT_INDEX_FILE` where the caller already removed them
(daemon steps and server steps with a deadline). They also run no witness Git
command beside a step and take no whole-run deadline. Project build overrides retain precedence.
Attestation is computed on the owner from `ServerCheckExecutionInputs` before
preparation; revision metadata must bind tools, assets, secrets and any declared
cleanup policy. Owners without that evidence, including today's daemon, return
`NotAttested`; a runtime ID is never an input attestation.

`api-types::CheckReceipt` records operation/owner/input identity, ordered
`CheckCommandReceipt` values (exit, typed outcome, duration, redacted tails,
truncation, tree-stop evidence and timestamps), overall outcome, cleanup receipt,
HEAD/tracked-change witnesses and started/finished times. It certifies execution
facts only. A passing process with failed/timed-out/uncertain cleanup is not an
overall pass. No declared cleanup plus `not_performed` cleanup may pass. Stage C
must verify completeness, digest/scope, owner placement and attestation before
certification; the receipt confers no semantic review authority.

Daemon protocol **5** adds `check.run`, `check.lookup`, and `check.cancel`.
`check.run` takes a fence-free stable operation ID, owner-resolved workspace
handle or repository-location/exact-commit target, purpose, spec, transient env,
pinned cleanup plan and absolute RFC3339 deadline. Local purpose authorization,
placement generation/runtime validation and per-checkout mutation locking remain
on the owner. The request is bounded to 128 KiB, 64 run commands and 32 cleanup
commands; cleanup is at most 30 seconds and each retained stream tail is 4096
bytes. The shared crash-safe daemon journal reserves receipt space before launch
and retains sanitized check intents/results. An exact duplicate returns the
same receipt or `running`; a different request under the key is refused. Lookup
after reconnect returns the retained result; an unfinished intent without a live
handler after restart returns `interrupted`. Cancellation uses the existing
persistent operation tombstones and waits for process/cleanup settlement.

Every check state has an end. A `running` operation ends with a receipt when its
commands and cleanup settle, at the latest at its deadline plus the cleanup
bound and the kill grace; waiting for a busy checkout is bounded by the same
deadline and by cancellation, and settles the key as `timed_out` / `cancelled`
with nothing spawned. A key that was admitted but could not start (workspace
gone) settles as `infrastructure`. An intent whose handler was dropped or whose
daemon restarted is `interrupted` and holds no journal headroom. A check key
outlives `journal.ack` only until its request deadline has passed: before it a
duplicate must find the receipt, after it a duplicate settles as `timed_out`
before any command starts, so the key is no longer needed. The deadline may be
at most 24 hours ahead. Acknowledged receipts are deleted once their deadline
has passed; unacknowledged receipts and interrupted intents 24 hours after it
(at admission of a later check, at most once a minute, and at daemon start).
The headroom reserved for a running check is sized from its own command count
(about 51 KiB per declared command), not a fixed worst case. On the server, a
`check.run` call returns `DaemonUnavailable` as soon as the connection goes
stale and `DaemonTimeout` after the remaining deadline plus the cleanup bound
plus the RPC timeout; the caller then asks `check.lookup`.
Upgrade the server first, then every daemon from that release and restart using
the same workspace root. Revision-4 and older daemons are refused before dispatch.

The legacy `integration_effects::check::CheckRun` is a thin phased adapter to the
shared sequence/projection policy; each owner command uses the primitive before
its existing recorder continues. Review-entry CI and manual `ReviewRunner` CI
retain command order, cwd, inherited env, verdicts, comments/events and per-command
receipt acknowledgment. No extra setup or managed checkout is enabled. Required
conformance/setup, lifecycle/before-work, readiness/preflight/environment helper
and agent-selected families retain their existing orchestration and check policy.

### Durable check runner and its Task-step consumer contract (3.3 stage C)

`check_runner::CheckRunner::request` returns a consumer and `Hit(result)`,
`Joined(run)` or `Scheduled(run)`. Selection and consumer insertion are one
`BEGIN IMMEDIATE` transaction using the stage-A partial unique identity index.
A repeated consumer key joins its original run, including a terminal failure;
it never silently launches another attempt. A certified cacheable pass returns
`Hit`. Uncacheable or failed idempotent evidence stays `Joined`, with the
consumer's own `result_id`. New consumers never hit an uncacheable, unattested,
failed, timed-out, uncertain or cleanup-failed result. Purpose belongs to the
consumer, whole-run timeout belongs to the run, and neither divides the key.
Forced execution still needs a new audited execution revision. The runner
accepts at most 64 commands, consumer keys up to 512 bytes and wall bounds from 1 through 86,400
seconds, matching the daemon's maximum retained operation deadline.

The shared server/Solo runtime starts the supervised `check-runs` worker. It
holds only check-repository and owner-effect ports; it writes no Task, Review,
budget, comment, condition or domain event. Framework supervision owns worker
health separately. At startup and on its one-second sweep it claims queued or
expired runs with a 60-second lease and renews every 15 seconds while awaiting
an owner. The stored operation UUID exists before admission. It persists a
placement/generation/runtime intent and absolute deadline before `check.run`.
Project environment values are resolved transiently, not copied to the intent.
Server execution shares the integration owner's physical checkout lock and
rechecks its dispatch lease, placement and exact candidate before starting.
Daemon-local paths stay opaque to the server.

Owner receipts are retained before result projection. Certification compares
operation, pinned owner/runtime and execution-input identity, command order and
completion, exit outcomes, timestamps, cleanup and (for reusable or canonical
passes) exact HEAD/tracked-change witnesses. A malformed terminal receipt is
infrastructure evidence, never a pass. Server receipts remain available in
memory if their durable write has a transient failure. The worker records the
immutable result before delivering it. Daemon `journal.ack` uses
`forge:operation:<operation_id>` only after result durability. A fresh result
is acknowledged on the next sweep; a refused or unanswered acknowledgement is
retried no sooner than every 60 seconds, each attempt bounded to 5 seconds, and
ends when the machine is removed, passes the disconnected-owner bound, or the
result is 25 hours old (the daemon keeps a check entry for 24).

An idle worker costs five indexed reads per second and no write: the sweep
reads `check_run_state_lease` and the partial indexes
`check_consumer_undelivered` and `check_run_unacknowledged`, which hold only
rows that still need work, and it opens a write transaction only when one of
them is non-empty. A run or consumer the sweep cannot process is logged and
skipped; it never stops the sweep for the others. The worker touches only rows
of `check_run`, `check_result` and `check_consumer` created through
`CheckRunner::request`, and enqueues Task steps.

Transport timeout/disconnect means `uncertain`. Recovery looks up the original
operation; it never repeats `check.run` under that key. An owner reporting
unknown/interrupted must first retain a cancellation tombstone, which fences a
delayed dispatch frame. A retained receipt settles the original run. Unknown or
interrupted after that stop fence, an owner past the existing configured
workspace-disconnect bound, or removal of its machine settles infrastructure
failure. Reconciliation releases its worker job between lookups while keeping
machine occupancy and the single-flight key: the run keeps a 15-second lease,
so it is looked up again every 15 seconds, not every sweep. An owner that is
reachable but still answers "running" (or refuses to answer) 600 seconds past
the run's settlement deadline is given up on: the run settles as an
infrastructure failure that is **not** retried automatically, because nothing
proved the process stopped. Every consumer is answered "no verdict", and a
second run of that identity is an explicit decision. Scans use
least-recently-attempted order. Infrastructure retry allowance is stored per consumer: two automatic
retries, then one terminal infrastructure notification. Recovery rechecks the
cache before retrying and reuses a pass certified by another consumer meanwhile.
This prevents a silent second execution of a certified key.

| Run state | Automatic exit | Bound/policy |
| --- | --- | --- |
| `queued` | Admit to `running`; all consumers stale: `cancelled`; owner removed or slot wait expired: infrastructure failure | Slot wait 1,800 seconds per attempt (with two automatic retries a consumer waits at most about 90 minutes before its Task is answered "no verdict"). Expiry settlement runs even when all 32 effect jobs are occupied. Queue time is excluded from wall time. |
| `running` | Receipt to `cleaning`; lost reply or lost lease to `uncertain`; no consumers left: `cancelling`. A run taken over before its intent was stored launched nothing and settles as an infrastructure failure | Recorded wall limit (1–86,400 seconds) plus 65 seconds of settlement grace. Lease 60 seconds, renewed every 15; takeover within 60 seconds plus sweep delay. |
| `cancelling` | Cancel reply with a receipt to `cleaning`; any other reply, or a lost lease, to `uncertain` | One cancel exchange, at most 61 seconds. |
| `cleaning` | Immutable result to a terminal state; a lost lease to `uncertain`, which resumes from the retained receipt | One fenced transaction; takeover within 60 seconds plus sweep delay. |
| `uncertain` | Receipt settles the original run; owner reports unknown or interrupted after a cancel tombstone: infrastructure failure (retried); machine removed or owner past the disconnected-owner bound: infrastructure failure (retried); owner still "running" or refusing: infrastructure failure, **not** retried | Lookup at most 30 seconds, cancel at most 61, repeated every 15 seconds. Hard bound: the smaller of the configured disconnected-owner bound and the run's wall limit + 65 + 600 seconds. Never a second `check.run` for the operation. |
| `succeeded`, `failed`, `cancelled` | One `apply_check_result` Task step per consumer; owner acknowledgement | Delivery marker and enqueue share a transaction, on the sweep after settlement. An infrastructure failure is retried at most twice per consumer, then delivered as "no verdict". |

| Consumer delivery state | Automatic exit | Bound |
| --- | --- | --- |
| Waiting for its run | The run's exits above; the consumer's Task leaving its status entry cancels the consumer | As the run. |
| Result recorded, not delivered | Infrastructure failure with retries left: moved to the retry run; otherwise the delivery step is enqueued | Next sweep (one second). |
| Delivered, not applied | The Task-step worker runs the step: applied, or stale (Task left the status entry, or another attempt is in charge) and the Task is untouched | The Task-step worker's own retry and dead-letter bounds. |

Capacity admission uses the same SQLite writer transaction and shared occupancy
query as executions, reservations and Chat turns. Checks use the physical
checkout owner (`machine_id`), including server-owned shared mounts, and consume
no Agent concurrency. Hits and joins acquire no slot. A check borrows a live
reservation or execution slot for the same consumer Task on that same counted
machine; Chat cannot lend. Borrowing is calculated on every occupancy read, so
when the held slot ends or expires the running check accounts for its own slot.
Running, cancelling, cleaning and uncertain admitted checks retain occupancy;
queued checks do not. Slot release is observed by the periodic scan without a
notification race. Operations includes admitted, borrowed and capacity-wait
counts; per-machine `active_runs` includes exclusive check slots.

Result delivery enqueues an entry-fenced `apply_check_result` Task command with
consumer/result/run identities, exact commit and spec digest. The consumer's
`delivery_step_id` and enqueue share a transaction. Its marker survives Task-step
retention, so restart and pruning cannot enqueue a second notification. Already
stale epochs are enqueued as superseded and leave the Task untouched.

#### Task-step consumer contract

`check_runner::consumer::TaskCheckConsumers` is the only way a Task step uses
the runner. A consumer family (one per `CheckConsumerOrigin`) registers a
`CheckConsumerFamily`. A step asks with `request(TaskCheckRequest)`, naming
its Task, the status entry it runs under and its *authority*: the step or
attempt that asks, as the family names it. The request is idempotent on
(origin, Task, status entry, authority) and is refused when the Task has left
that status entry or no family is registered. Its reply (`Hit`, `Joined`,
`Scheduled`) is information only. Hit, joined or scheduled, the verdict always
arrives as exactly one `apply_check_result` Task step, so there is one
application path.

`apply(step_id, delivery)` runs inside that step. It refuses (the step fails
visibly) an envelope that is not carried by the consumer's recorded delivery
step, or whose consumer, run, result, commit or spec digest differ from what
is stored. It returns `stale` and leaves the Task untouched when the consumer
was cancelled, the Task left the status entry, or the family's
`current_authority` is no longer the one that asked. Otherwise it hands the
family a `CheckVerdict`: `Result` (pass, fail, timed out or cancelled, for
exactly the requested commit and spec) or `InfrastructureExhausted` (no
verdict after the automatic retries: the family parks its Task with a
retryable reason and records no failure of the candidate). The consumer is
then marked applied; a redelivered step returns `already_applied`. A crash
between the family's write and that mark redelivers once more, so a family's
`apply` is idempotent on `consumer_id`.

**No family is registered yet, and no execution family requests the runner.**
Merge-path and review-entry CI, manual `ReviewRunner`, conformance,
before-work and other checks retain their current orchestration, limits and
verdicts. The 3.2 integration queue stays inactive. Still pending: the typed
Task conditions for a check wait, a check-slot wait and exhausted check
retries (with their `is_blocked`, offer and dispatcher rules), the
review-entry and merge-path cutovers, canonical PATH/HOME declaration and
managed-checkout/canonical-policy activation. Current owner dispatch uses the
frozen legacy workspace policy and supplies no new server input attestation;
configured checks remain uncacheable. A reusable result has no expiry: reuse
ends only when the commit, spec digest or audited execution revision differs.
No historical review or legacy CI row is promoted to cache.

The owner supervision guarantees tested here cover normal cancellation, dropped
futures, retained receipts and restart reconciliation. Abrupt process death
between OS spawn and durable process-tree evidence, and escaped descendants,
are not proven by the runner; the stage-B process-supervisor limits still apply.
