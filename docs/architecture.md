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
├── forge-solo/    # Repository-scoped chat-first TUI, its bootstrap and session services, local entrypoint
├── forge-client/  # forge-ctl CLI client
├── forge-daemon/  # Local daemon detection and reporting
├── api/           # Axum REST endpoints, SSE, middleware
├── api-types/     # Shared request/response types (zero internal deps)
├── agent-host/    # Forge-owned direct Agent Runtime composition and protected stores
├── operation-registry/ # Typed native operation specs and generated projections
├── db/            # SQLite schema, migrations, repository implementations
├── services/      # Task core, workflow engine, shared domain services
├── services-agents/ # Shared composition root, inquiry/topic services, provider authorization, embedded executor
├── pricing/       # models.dev catalog, price resolution at admission, cost estimation
├── executors/     # TaskExecutor trait, Shell executor, JSONL logging
├── cli-adapters/  # Codex, Claude, Cursor, Gemini, opencode, shell, null adapters
├── workspace/     # Git worktree lifecycle, locking, path guardrails
├── git/           # Low-level git operations
├── process-supervisor/ # Process-group ownership, kill sequence, drained bounded output
├── check-executor/ # Persistence-free owner execution of a CheckSpec, typed receipt
├── review/        # review contract, conformance checks, review workspace
├── events/        # In-memory event bus (tokio broadcast)
├── mcp-server/    # MCP JSON-RPC tools for agent integration
└── config/        # Configuration loading, defaults
```

### Dependency flow

```
forge-cli → api / services-agents / services
api → mcp-server
api / mcp-server / forge-solo → services-agents → services → db
api / mcp-server / forge-solo → services
services-agents / services → events / agent-host / executors / workspace → git
agent-host → agent-runtime / config
forge-cli / forge-solo → config
forge-solo → api-types / review / cli-adapters / operation-registry
api / review → db (finding ledger and round-policy projection)
api-types (shared request/response types, zero internal deps)

api / services-agents / services → pricing → db / api-types

services / review / forge-client → check-executor → process-supervisor
                                                  → git → process-supervisor
                                                  → executors / api-types
```

`process-supervisor` is a leaf (tokio and tempfile): `git` cannot depend on `executors`
or `review`, and `forge-client` must not pull in `services`, so the one
process-group implementation that Git, CI steps and daemon helpers share lives
below all of them. `check-executor` sits between: it needs `git` (managed
checkout), `executors` (run budget, redaction) and `api-types`, and is called by
`services`, `review` and `forge-client`; it depends on neither `services` nor `db`.

`executors` and `cli-adapters` also use `process-supervisor`'s executable helpers.
Script publication stages in a private sibling directory, writes from a separate
Unix process, waits for that writer to exit, fsyncs through a read-only descriptor,
and renames into place. The multithreaded parent never opens the executable inode
for writing, so another fork cannot inherit a writable descriptor and make even
an indirectly invoked Git hook fail with Linux `ETXTBSY`. The staging directory
can sit in a run-writable compiler-cache store, so the writer creates the file
exclusively (`set -C`), the parent opens it with `O_NOFOLLOW | O_NONBLOCK` and
requires a regular file with one link, and a writer still running after 10 s is
stopped: an entry a run plants there fails the publication instead of being
written through or chmodded. All fake executable
fixtures and the generated compiler-cache launcher use this publisher.
The writer program lives in the crate, since release images build from
`crates/` alone (`scripts/test_crate_includes.py` keeps non-test includes
inside it); Python CI fixtures run the same program through
`scripts/executable_fixture.py`. The shared
sync/async spawn helpers retry only `ExecutableFileBusy`, five retries with
10/20/40/80/160 ms delays; other spawn errors and process exit failures retain
their original behavior. Compiler-cache probes, supervised processes, and direct
opencode/Gemini/Smith launches use these bounded retries.

Provider probes, native requests, web fetch and public search share
`config::is_restricted_public_address`. `config` has no internal crate
dependencies, so `agent-host` can use this implementation without a cycle.
Well-known NAT64 `64:ff9b::/96` and IPv4-translated `::ffff:0:0:0/96`
(RFC 2765) addresses apply the IPv4 policy to the final 32 bits in every
consumer, as provider/fetch 6to4 and mapped/compatible addresses do to their
embedded IPv4 destination. Local-use NAT64
`64:ff9b:1::/48` and Teredo remain refused. Every consumer rejects IPv4 IETF
protocol assignments at `192.0.0.0/24`, TEST-NET-1 at `192.0.2.0/24`, and
former 6to4 relay anycast at `192.88.99.0/24`, including IPv6 forms embedding
those destinations. All consumers also refuse IPv6 documentation
`2001:db8::/32` and `3fff::/20`, benchmarking `2001:2::/48`,
ORCHID `2001:10::/28`, ORCHIDv2 `2001:20::/28`, and discard-only `100::/64`.

`config::is_blocked_public_search_address` adds public search's existing
conservative IPv6 exclusions to that common policy for endpoint validation,
the request resolver and result URLs. These are deliberate differences:
public search refuses all IPv4-mapped/compatible and 6to4 addresses even when
their embedded IPv4 is public, while explicitly configured providers and
fetch can reach public IPv4 destinations through these transition forms.
Search also retains its broader guards over `2001:1::/32` through
`2001:5::/32`, `3ff0::/12`, and the rest of `64:ff9b::/48` outside the
well-known `/96` (unassigned). These conservative search-only guards cover
more than the precise benchmarking/documentation prefixes: search accepts
an endpoint and untrusted result metadata, while provider/fetch deliberately
admits otherwise public destinations outside the shared special-use ranges.
Keeping these broader guards search-only preserves that provider/fetch
boundary without relaxing search's existing restrictions. The shared address table in
`services::native_tools::tests::public_search_and_provider_address_policies_agree_except_documented_exclusions`
checks agreement and each documented exclusion.
`config::network_policy::tests::shared_policy_changes_only_intended_address_verdicts`
compares both predicates with verbatim copies of their pre-sharing versions
over a generated address set: the only verdict changes allowed are
provider/fetch refusing `192.88.99.0/24` (and IPv6 forms carrying it),
provider/fetch refusing the six IPv6 special-use ranges above, both policies
refusing IPv4-translated addresses carrying restricted IPv4, and public
search admitting well-known NAT64 addresses whose embedded IPv4 is public.

`config::is_restricted_public_hostname` owns the common hostname policy for
provider-entry validation and probes, native provider requests, fetch
authorization and transport, and public-search endpoint validation, request
resolution and result URLs. All refuse `localhost`, `localhost.localdomain`,
`internal`, `home.arpa`, `local` (mDNS), and subdomains of each, ignoring ASCII
case and trailing dots. Matching uses whole DNS labels: `example.internal.com`,
`internal.example.com`, `home.arpa.example.com` and `local.example.com` remain
allowed. At request time, search rejects the entire
hostname if resolution is empty or any answer is blocked, matching providers
and fetch; it never filters a mixed answer down to its public members.
The request client receives only an all-public answer set. Deterministic
regressions cover hostname suffixes and mixed IPv4/IPv6 DNS answers without
contacting DNS or an external endpoint.

`pricing` owns the provider pricing catalog, the SQLite pricing repository and
the admission-time price resolution (`pricing::auto`). It sits below `services`
and has its own `PricingError`, which `services` maps into `ServiceError`. The
one thing it needs from above, a provider entry's endpoint, is passed in as a
function by the caller. The Solo bootstrap and session services
(`forge_solo::bootstrap`, `forge_solo::session`) live in `forge-solo`, their
only caller, and use `services` like any other client.

## Architectural patterns

### Operation registry and immutable doctrine

`operation-registry` is below the native host, services and MCP adapters. Domain
modules own input types, canonical schemas, structural constraints, authority,
effect class, availability, surface bindings and typed handler bindings. Native
aggregates and MCP descriptors are projections of those specs. Host metadata,
operation names, permissions and classification are generated; MCP input variants
and their handler interface/dispatch come from one declaration. Service adapters
bind concrete effects without a second operation dispatch table or parameter type.
Runtime workspace tools, fetch and configured public search retain their single
capability-bound Tool implementations rather than masquerading as Forge domain
operations. Catalog tests cover every composed aggregate operation and MCP tool,
require one spec each, and prove every spec is reachable.

Fresh calls authorize before decoding the selected closed contract. Native
integer-valued strings/floats normalize to integers; MCP retains strict JSON
integer spelling. Provider schemas use portable base keywords, while complete
canonical schemas govern decoding. Optional generic read arguments use defaults
without a repeated argument list in every tool prefix. The Project-summary
selector and the Task-run informational-offer warning remain because omitting
those constraints leads to an invalid query or an unauthorized workflow attempt.
Named orchestration reads retain their generated field contracts. The Main Charter
draft retains nested shapes needed to produce a valid first call.

`project.summary`, adoption/amendment drafting, milestone evidence and readiness
now have registry specs. Every fresh payload/envelope rejects undeclared fields;
Charter adoption also declares the previously accepted `expected_digest`, and
Project evidence declares `source_run_id`. Evidence requires the acceptance-check
list its materializer reads. The receipt-bearing Project specs retrieve an exact
principal/scope/operation/key/input-digest receipt before current authority or
contract checks. That read is forwarded through the native authority wrapper and
preparation boundary. A changed input/key receives fresh authorization. Prepared
calls retain their arguments, and committed replay never executes an effect again.
Domain semantic validation, transaction authority, CAS, receipts and workspace
preparation remain in their command services. The six legacy pending-intent
operations still have no materializer and never report a completed domain effect.

The same EffectiveAuthority evaluator governs native and MCP advertisement and
admission. MCP remains delegated-user authority. Project grants omit account-wide
tools and load current Project visibility and member/admin/owner facts. Lifecycle
hook replacement requires owner/admin authority; script hooks inside opaque
Project settings require that same role, with a shared REST/MCP effect-time
recheck. Agent references in assignment, subtask creation and execution follow-up
use REST's Project usability query, and private identities answer as missing.
Stored foreign dependency IDs are not disclosed by constrained dependency reads.
Transition execution remains `SystemComponent::Mcp`; its returned action offers
use the delegated caller, just as Task reads do.

Current doctrine is Main baseline `forge.main.baseline/v1@5`, Genesis
`forge.main.project-discovery/v2@8`, and Project
`forge.project.orchestration/v1@22`. New immutable revisions retain historical
rows, admissions and receipts. Their overrides are inside the canonical body and
content digest. Historical renderers restore the exact legacy override suffix;
new frozen bodies carry their own overrides through future cutovers. Admitted
Project verification uses only its disposable checkout, with observed command
provenance and user-only manual attestation/release. Project Agents build Task
calls from `work.read.action_schema`; Task worklog/evidence `action` is optional,
and each distinct worklog entry uses a new `correlation_id`.

To add an operation, declare its typed spec and handler in its domain module,
bind the service context, and extend the coverage/input fixtures. Do not add a
parallel host permission/schema/name list. Receipts and exact prepared calls must
remain valid under their admitted history, while fresh effects use current facts.

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
`services_agents::ForgeRuntime` through `ForgeRuntimeBuilder`, starts its
`RuntimeSupervisor` in server mode, and then constructs `AppState` as the Axum
adapter over that graph. `AppState` retains `Arc` references to the Task,
identity/profile, Main/Project Agent Chat, embedded-session, memory,
commitment, and Attention-facing services; it does not own a second copy of
the background worker lifecycle. `AppState` is `Clone` (shared fields are
`Arc`) and used as Axum state.

Server and Solo config lookup uses the launch data directory (`--data-dir` >
`FORGE_DATA_DIR` > `~/.forge`) plus `forge.yaml`. They retain that exact path
for runtime settings reads and writes. A file's `forge.data_dir` can change
the runtime data root, but cannot redirect config lookup.

### Shared runtime composition and Forge Solo

`services_agents::ForgeRuntimeBuilder` is the composition boundary shared by the
server and `forge-solo`, in `services-agents`. API and MCP adapters also depend
on that upper crate; `services` has no dependency back to it. It resolves the
effective configuration and injects one database, event bus, adapter registry, Agent Runtime host, credential and
log roots, workflow, review/merge, workspace, and cleanup graph. The builder
constructs each domain service once; `RuntimeSupervisor` owns the background
worker handles and one shutdown signal. The server's `AppState` and Solo's
session facade consume this graph rather than maintaining parallel persistence,
workflow, retry, or recovery implementations.

Inside `services`, shared execution outbox contracts and the embedded execution
lease registry belong to `task_service::execution`. Its `ExecutionSupport`
interface supplies provider credential injection and outbox ingestion; the
runtime builder injects `EmbeddedAgentService` through that interface. Provider
endpoint defaults and URL-shape validation live in `provider_credentials`, and
plain system chat notices in `chat_notices`. The core `wake_turns` module owns
`WakeTurns` and `PreparedWakeTurn`; the agents-side callback builds the admission
at the original transaction boundary using the current Attention row.
`AttentionService::new` takes the `WakeTurns` implementation as an
`Arc<dyn WakeTurns>`: the runtime builder passes the `WakeTurnConsumer` it
starts as a worker, and Mission Control routes pass one built on the same
database. Shared shutdown and worker identity contracts live in core
`worker_runtime::assembly`; graph-dependent dead-letter registration lives in
`ForgeRuntime::dead_letter_service` in `services-agents`.

`config::is_restricted_public_address` owns the shared IP policy used by
provider-entry validation, service probes, native provider transport, the web
fetch tool's authorization gate and transport, and public search.
`config::is_restricted_public_hostname` owns the shared hostname checks. Public
search (`services::native_tools` request resolver and result filter, and
`config` endpoint validation) adds the deliberate IPv6 exclusions listed
above. Provider connection tests and health probes share a client builder that
resolves and validates once, pins those addresses and disables redirects and
proxies.

The upper crate also owns `agent_inquiry_service`, `main_chat_topic`,
`prompt_preview`, `provider_authorization`, and `embedded_task_executor`, with
their tests. The remaining chat/admission/native-tool modules still live in
`services`, where core unit-test fixtures and worker monitoring reference them.
The core retains the agent registry, capacity, memory, operating skills, turn
log sink, provider credentials/health, OAuth, daemon and orchestration services.
Shared recovery fixtures are exposed only with core `test-support`.

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
An unbound root containing only a regular `forge.yaml` is admitted as a new
Solo store; any existing runtime files still require the matching binding.

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
(since `forge.main.project-discovery/v2@7`). Its account baseline carries no
additional resident layout rule. Project
standing invariants apply it to `task.propose`
(since `forge.project.orchestration/v1@20`); the on-demand document and Task sections
apply it to architecture/execution plans and adaptive splitting. Planner runs
receive it once in their system prompt for both native and outbox delivery;
plan-artifact delivery instructions still require owned repository-relative
paths and scope reporting. Scaffolded `AGENTS.md` carries it for implementation
and follow-up proposals, retaining the worker's existing scope discipline.
Native Task proposal/adaptive payload guidance carries one short reminder when
either operation is admitted. MCP Task creation descriptors carry compact hierarchy/workspace guidance;
Task creation callers follow the ownership and split rules documented here. The full doctrine stays at the
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
its only writable roots are the execution's own temp directory (see
[Task-root layout and run environment](#task-root-layout-and-run-environment);
a Task root Forge did not reserve keeps a freshly reset `task-scratch` inside
the managed home), the Task's build directory `.forge-task/build`, the
execution outbox, and the host's package-manager caches
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

Before local launch, a Task version change that preserves its state entry,
current execution, assignment and repository authority is recovered by replacing
the execution's stale WorkspaceLease atomically. Replacement retries at most 32
times, reloading the Task, execution and active grant between attempts, with
exponential backoff capped at 50 ms. A replacement that commits is the run's
grant: a Task write after it is not re-checked for version (as after a strict
verify), only for the run's preconditions. A changed precondition stops launch
with its specific reason. Exhausted version contention is an auto-resumable pre-dispatch
infrastructure refusal: no executor starts, no execution retry budget is
charged, and the dispatcher's next scan launches the role again under current
authority. Re-dispatch is bounded too: the third refusal in a row for lease
contention of the same Task role parks the Task behind the dispatch-failure
blocker (`executor_failed`, recovered by the owner's Retry), still without
charging the execution retry budget.
The start of every run, embedded or daemon-placed, checks the grant first
(`start_execution`). When the only change since the claim is that the Task's
version moved past the run's own grant (a manual Launch or a session follow-up
starts in a later Task step than its claim), the start takes the same bounded
reissue, fenced to the run's state entry. A daemon-placed run has no later
server-side lease check.
The runner fences status and state-entry epoch against the Task captured before
launch preparation. A Project pause does not stop a run already admitted (a
pause refuses new admissions; running runs finish). An Agent paused during the
reissue ends the run as the Agent wait. Explicit interactive recovery may
inspect a Task held at admission; a new hold during launch still stops the run.
The workspace placement is not re-checked: no write moves a placement to
another owner after admission, and a workspace admits one running repository
execution. Across the backoff the launch holds only the in-process workspace
execution lock (final check); each try is its own short transaction.

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

Execution-failure retry admission commits its deadline, failure receipt, budget
charge and automatic execution resume policy in one transaction. Startup
recognizes the older crash prefix (the receipt and deadline committed before
that policy write) and settles the failed execution to restore automatic
resume under the current execution, assignment and Project authority. The
scheduler records the deadline in its durable wait row; expiry and startup
reconciliation resume the Task without an in-memory timer owner.

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
direct APIs remain separate contracts with unchanged names and result shapes.
`operation-registry::mcp` owns all 43 direct MCP contracts (the 42 existing
names plus the previously registered integration-queue read), their scope,
required roles, resource-reference rules, schemas, and typed decoders. The Task
and Project inputs live in `mcp/task.rs`; the registry remains independent of
services, DB, and API types. Handlers take those concrete input types. Exhaustive
conversions bind the eight Task condition commands, workflow triggers, and
lifecycle hooks to the existing API/domain types. No handwritten descriptor or
RPC scope-name list remains. Catalog order is stable.

MCP resolves `Principal::DelegatedUser`; a Project binding does not imply an
Agent principal. The catalog and `EffectiveAuthority::evaluate` generate the
entire `tools/list` per connection and govern known-name call admission. Project
grants omit the eight account-wide tools, load current visibility/member/admin
facts, and allow escalation only for the exact `project.owner_id`. Memory reads
and escalation require a bound Project and are omitted/refused for account
grants. Other Task/Project APIs retain their existing Project-visibility rule,
including ownerless visible Projects. Lifecycle-hook replacement and script-command settings require Project owner/admin authority; other visibility-based operations keep their existing policy. Account grants resolve ownership and roles against supplied references.
Conditional daemon pinning uses the same evaluator for field advertisement and
admission.

Call order is known-name lookup, delegated scope/role admission, Project scope
binding, referenced-resource authorization, strict typed contract decode, and
handler effect. Reference rules in the catalog select Task, parent, dependency,
child, and Execution binding checks. Denial precedes contract diagnostics;
missing/inaccessible resources remain indistinguishable. All inputs are closed,
including child inputs, follow-up overrides, condition-command variants, and
lifecycle-hook variants. Contract errors use MCP `-32602` with the bounded
`mcp_contract_invalid` correction. Strict JSON integer spelling is preserved.

Each advertised descriptor has one generated summary line and its fields.
Provider schemas use the portable base keywords. Tagged command/hook contracts
remain exact in Serde; their advertised field union gives the discriminator
values and fields, while current `available_actions` supplies meaningful Task
parameters. Nullability, custom workflow state strings, comma-separated/list
status filters, opaque policy/settings JSON, numeric bounds, and the accepted
`board_position` sort now reflect handler acceptance. Handler/domain checks
retain state, CAS, graph, content, and transaction-time authority invariants.

`forge_transition_task` keeps `SystemComponent::Mcp` as its workflow actor:
`Actor::is_user` selects the interactive override policy throughout the engine
(undefined edges, dependency and conflict-marker gates, displaced runs), so a
delegated MCP credential is not given it. `forge_task_action` keeps its
delegated-user actor, which spends retry budgets. Both Project settings paths pass the authenticated user into shared validation, including script-hook authority and assigned-agent usability checks. Assignment, subtask assignees and follow-up agent overrides use the same usability query. Under a Project-constrained grant a Task or Execution
outside the bound Project is reported exactly like a missing one. Task reads continue consuming the
existing typed integration summary and `available_actions`; merge states,
queue-owned eligibility, integration commands, and Task-step writing are
unchanged. Effect-time identity ownership and daemon-admin rechecks remain
next to the corresponding writes; escalation's exact owner check remains in
its service transaction.

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
execution cancellation plus Workspace lease revocation. An unfinished check run
(`queued`, `running`, `cancelling`, `cleaning`, `uncertain`) is in-flight work
like a running execution: the guard counts it (`live_check_runs`) and refuses.
Force cancels the runs' consumers, settles queued runs as cancelled at once, and
waits up to three seconds for the check worker to stop running ones on their
owner; a run that has not settled (an unreachable daemon cannot confirm the
stop) keeps the refusal, so the `check_run` row that fences the remote
operation is never deleted from under it. The final
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

Rates are resolved at admission by `pricing::auto`, inside the
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
leaves the workspace ready and parks the Task for a later tick. Check runs on the
durable check runner count in the same occupancy and go ahead of new
executions when a slot frees; merges consume no slot.

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
and never sufficient by itself to wake an agent. The bus itself is
instance-wide; each SSE connection filters it for its user (`routes::events`
`Audience`): an event resolved to a Project the user cannot see is withheld,
with per-connection visibility answers re-read at most every 10 seconds (see
[api.md](api.md#authorization)).

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

Setup-required decisions retain each Attention item's canonical incident digest,
including when the startup sweep finds several blockers in the same Project.
Only a prepared turn uses a batch admission digest. A setup or suppression
decision is deduplicated by scope, incident, source event, reason, digest and,
when one is bound, the responder identity, so a changed binding records a new
decision rather than colliding with the earlier one. The latest-decision row is
updated only when its digest, disposition, reason or responder identity changes;
an unchanged startup sweep writes no wake audit, disposition, latest-decision
timestamp or consumer checkpoint. Event projection commits its decision and
disposition with the source checkpoint in the worker's one transaction. The
sweep reads open Attention level state, so its historical causation event does
not imply a replay of the event-consumer cursor.

Mission Control and Agent detail are bounded read models over authoritative
Task/identity/session/commitment/event state. They show needs-attention,
review-ready and active work, embedded-agent health/current scope/focus,
commitments, recent outcomes, and capacity; they do not introduce a second
mutable Task or Agent truth.

### Task-root layout and run environment

The directory that contains a Task's worktree is its **Task root**:
`<workspace root>/<task_id>/` on the server, `<root>/.forge/workspaces/<handle>/`
on a daemon. The worktree stays where it was; Forge keeps one reserved
directory beside it:

```
<task root>/
  <repo name>/              the Git worktree (daemon: repo/)
  .forge-outbox/<exec>/     execution outbox
  .forge-task/              reserved
    tmp/<run key>/          TMPDIR, TMP, TEMP of one run
    home/codex/             managed Codex home
    home/gemini/            Gemini API-key home
    build/cargo/            CARGO_TARGET_DIR of the Task
<task roots dir>/.forge-tmp/<run key>/   run temp dir when the path above is too long
<workspace root>/.forge/build/cache/<repository id>/   shared compiler cache (opt-in), one store per repository
```

`.forge-task` is outside the worktree, so `git status`, `git clean`, review
dirt checks and worktree removal never see it, and it is removed with the Task
root (read-only build output included). A repository named `.forge-task` is
refused at worktree creation (`WorkspaceError::ReservedName`).

A Task root is **reserved** when `.forge-task` exists in it as a real
directory. Only the workspace owner creates it: the `workspace` crate when it
creates or recovers a Task worktree, the services `WorkspaceManager` when it
validates a workspace of the managed shape `<root>/<task_id>/<name>` for any
purpose but inspection, and the daemon in `workspace.prepare`. A recorded
path of any other shape, an exact-commit check checkout, a Project
verification checkout and a chat sandbox are not reserved: runs there keep
the environment they had before, and nothing is created beside them.

`executors::sandbox::SandboxEnv` is the environment a run gets from its Task
root, and `executors::run_process::apply_sandboxed(command, project_env,
&sandbox)` is the one place it is applied, together with the build budget and
niceness. Every path is a pure function of the worktree path and the run id,
so the server and a daemon derive the same directories without a protocol
field.

| Run | Run id | Temp directory lives |
|---|---|---|
| CLI adapter execution (Codex, Claude, Cursor, Gemini, OpenCode, Smith), shell executor | execution id | until the adapter's `execute` returns (success, failure, cancel) |
| Native command tool | fresh per command | until the command returns |
| Lifecycle script hook | fresh per hook | until the hook returns or times out |
| Check / CI command in the Task worktree, daemon `workspace.run` | fresh per command | until the command returns |
| Environment check, daemon probe | fresh per command | until the command returns (only when it runs in a reserved Task root) |

- `TMPDIR`, `TMP` and `TEMP` are set to the per-run directory over anything
  inherited. A key the Project environment declares is left to the Project.
  `HOME`, `XDG_CACHE_HOME`, `GOCACHE` and `npm_config_cache` are never
  redirected: CLI logins and shared toolchain caches live there.
- A Unix socket path is limited to 104 bytes on macOS, and tools create
  sockets under `TMPDIR`. Forge never hands out a `TMPDIR` longer than 72
  bytes: when `<task root>/.forge-task/tmp/<key>` is longer, the run uses
  `<task roots dir>/.forge-tmp/<key>`; when that is too long as well, the run
  keeps the inherited temp directory. With UUID Task ids the short directory
  is the usual place.
- A run that died with its process leaves its directory behind. Crash
  recovery removes the directory of every execution it settles; at startup
  the server (in crash recovery, for every ready server placement of the
  managed shape) and the daemon sweep the rest
  (`executors::sandbox::sweep_dead_runs`). Hooks, checks, tool commands and
  probes are in no table, so the sweep is safe by construction, not by
  ordering: it removes a directory only when this process did not create it
  (an in-process registry of unsettled runs), it was last modified before
  this process started, and its key belongs to no execution still recorded
  as running. A sweep failure is logged and never stops recovery. Nothing
  Forge reads after a run (outbox, plan, evidence, logs) lives in the
  per-run directory.
- Links are never followed. `.forge-task` that exists as a link or a file
  makes worktree creation fail and is never a reserved root; a linked or
  non-directory `.forge-task/tmp`, `.forge-tmp`, `.forge-task/home` or
  `.forge-task/build` is not created through, removed through or handed to a
  run (the run keeps the inherited temp directory, the previous home
  location, and builds in the worktree); a linked Task-root entry is skipped
  by the sweep.
- A CLI that confines its own writes is handed only what it can write
  (`cli_adapters::command::run_in_task_worktree_with`). A managed Codex Task
  (workspace-write) gets the per-run directory and `.forge-task/build` as
  writable roots, each only when it resolves where the Task root says, and
  the build root only when the child really uses that `CARGO_TARGET_DIR`;
  a Codex execution under `yolo` has no sandbox and gets everything; a Codex
  execution that is not a managed Task uses the operator's own sandbox
  configuration and gets nothing. Gemini launched with `--sandbox` (or
  `GEMINI_SANDBOX`) gets nothing. Claude, Cursor, OpenCode, Smith, the shell
  executor, the native command tool, hooks and checks run unsandboxed.
- One build directory per Task root is shared by the coder, hooks and
  in-worktree checks. Cargo serialises concurrent users with its own
  build-directory lock, and its fingerprints (source content and mtime,
  profile, flags, package path) decide what is rebuilt, so output left by
  another commit can make a check slower or faster but cannot change its
  result.
- **Shared compiler cache (opt-in).** A per-Task build directory makes every
  Task a cold build. When the operator names an installed wrapper in
  `workspace.compiler_cache.wrapper` (per machine; a daemon reads its own
  `daemon.yaml` and never the server's), `SandboxEnv` also carries
  `executors::compiler_cache::CacheEnv`: `RUSTC_WRAPPER` plus the wrapper's
  cache-directory variables from a table keyed by program name (`kache`:
  `KACHE_CACHE_DIR`, `KACHE_MAX_SIZE`; `sccache`: `SCCACHE_DIR`,
  `SCCACHE_CACHE_SIZE`, `SCCACHE_SERVER_UDS`; any other name:
  `RUSTC_WRAPPER` only). It is built by the same constructors as the build
  directory, so every run family that gets `CARGO_TARGET_DIR` gets it
  (executions, hooks, checks in a Task worktree, the native command tool,
  daemon `workspace.run`; never a probe). The store is
  `<cache dir>/<repository id>`, and the repository id is read from the
  worktree itself: a linked worktree's `.git` file names
  `<repository>/worktrees/<name>`, and `<repository>` is
  `<root>/.repos/<repo id>` on the server and `<root>/repos/<repo id>` on a
  daemon. A directory that is not a linked worktree gets no cache, and
  neither does one whose repository does not name it back
  (`<repository>/worktrees/<name>/gitdir` must resolve to the worktree's
  `.git`): the `.git` file is the Task's to rewrite, the repository's
  directory is not, so a Task cannot claim another repository's store. Each
  store carries a marker file (`.forge-compiler-cache`, the wrapper kind),
  written by rename and never through a link a run left in its place.
  - Trust boundary: one store per repository directory per machine, shared
    by that repository's Tasks and checks. It is a write channel between
    them (an entry one Task stores is linked by the next), which is the
    trust Tasks of one repository already have through the commits they
    send to the same checks. Nothing crosses repositories or machines.
  - Precedence is the build directory's: Project environment (an empty
    `RUSTC_WRAPPER` turns it off for the Project), a value on the command,
    the operator's process environment, then Forge. All or nothing: when
    anyone else carries `RUSTC_WRAPPER`, `CARGO_BUILD_RUSTC_WRAPPER` or one
    of the wrapper's own variables, Forge sets none of the set and the store
    is not a writable root. A process whose own environment names a wrapper
    would hand out nothing, so `install_configured` installs nothing for it
    and warns once. "The operator's process environment" counts only for a
    child that inherits it. A command started with a cleared environment
    (the native command tool, an isolated check) inherits nothing, so for it
    the operator's `CARGO_TARGET_DIR` and cache variables do not outrank
    Forge's (`run_process::apply_sandboxed_cleared`): it gets the Task's
    build directory and the cache, never a build inside the worktree.
  - A cache that cannot be used never fails a run. A known wrapper that
    does not answer `--version` in five seconds when it is resolved: the
    feature is off for the process. Per run: wrapper not executable, store
    not creatable or writable, `sccache` socket path over 100 bytes or its
    server not starting: the run gets no wrapper, and one warning per cause
    is logged per process. After three failed server starts in a row for a
    store, runs skip the start for ten minutes. A start can take ten
    seconds and never runs on an async worker: a run's start awaits it on
    the blocking pool (`sandbox::start_compiler_cache`) while every other
    task keeps being polled, and the sandbox constructors themselves never
    wait: with the server not up they hand its start to a thread of its
    own and offer that one run no wrapper (owner-local review steps and
    hooks built without a run scope take this path). The `--version` probe
    (five seconds at most) runs once, at process start. Compilers are never
    given a timeout.
  - `sccache` keeps one server per socket, and the server keeps the cache
    directory and the environment it started with. Forge therefore starts
    it itself, once per store (`--start-server` with the store's socket, a
    temp directory inside the store, no idle exit and none of the Forge
    process's own environment beyond `PATH` and `HOME`, under a file lock
    shared by every Forge process on that store). A server started by a
    run's first compile would inherit that run's per-run `TMPDIR` and fail
    every compile after the run settled. Runs are handed
    `<store>/rustc-wrapper`, a launcher that unsets `CARGO_TARGET_DIR`:
    `sccache` makes every `CARGO_*` variable part of the cache key, so with
    one build directory per Task no Task would ever hit another's entries.
  - A sandboxed CLI gets the wrapper only when it cannot break the run. A
    managed Codex Task (workspace-write) gets it only for a wrapper that
    builds uncached when it cannot reach its store (`kache`), with the store
    as a writable root when the child really carries Forge's wrapper and
    variables. `sccache` (its client fails the compile when it cannot reach
    its server) and unknown wrappers are not passed into that sandbox. Codex
    under `yolo` gets everything; Codex with the operator's own sandbox and
    Gemini with `--sandbox` get nothing.
  - A check's reuse identity is computed from the owner's inherited
    environment and the Project environment (`check_runner/policy.rs`),
    before any run-level variable is applied, so the wrapper never changes
    it.
- `CARGO_TARGET_DIR` is set to `<task root>/.forge-task/build/cargo` from
  `executors::sandbox::BUILD_DIR_TABLE` (Rust only). Precedence: Project
  environment, then a value the command already carries, then the operator's
  process environment, then Forge. A Project environment value of `""`
  disables it. Subtasks share their root's worktree and therefore its build
  directory; review and CI commands in the Task worktree use the same one.
  Checks in exact-commit checkouts get none.
- The managed Codex home is `<task root>/.forge-task/home/codex`, so two
  executions of different Tasks never share or reset one home (before, a
  daemon derived one home for all executions from its flat log directory).
  On first use after an upgrade the server-side per-Task home is moved from
  `<root>/.forge/logs/<project>/<task>/.codex-managed-home` into the Task
  root, so a resumed Task keeps its Codex sessions; the old shared daemon
  home is left where it is and a fresh home is created. The move is one
  rename and deletes nothing: a stray Codex process still holding the old
  home keeps writing the files it has open, which are now in the new home.
- Lifecycle hooks that run outside an execution log to
  `<workspace root>/.forge/logs/<project_id>/<task_id>/hooks/`.

### Server workspace root

The server's workspace root holds the Task roots, the clones Forge makes
(`.repos/<repository id>`), execution logs (`.forge/logs`), build
directories (`.forge/build`) and garbage-collection state (`.forge/gc`). Its
default is `<data dir>/worktrees`; `workspace.root` and
`FORGE_WORKSPACE_ROOT` override it (`WorkspaceConfig::root_explicit` records
that the operator chose it). Solo's root is always `<Solo data root>/worktrees`.

The database stores absolute paths into the root, so the root in use is
recorded once, in `system_setting.workspace_root`, by
`services::workspace_root::settle`, which every running server (`forge`,
Solo) calls after migrations and before anything is told where the root is:

| Recorded | Configured | Result |
|---|---|---|
| none | default | The root this database's own server-owned rows name (`workspace_root::stored_roots`), else `<data dir>/worktrees`; recorded. A stored path has one shape per kind: a Task worktree is `<root>/<task id>/<repository>` (workspace rows that are not `cleaned`), a clone is `<root>/.repos/<repository id>` (`repo_location.path`), a log is `<root>/.forge/logs/...` (`execution.logs_path`). The root is derived from those, never from the temp directory of this launch (a service manager and a shell disagree about it, and so do `/tmp` and macOS's `/var/folders/...`), and never from directories on disk (every data directory on a machine used to share `<system temp>/forge/worktrees`). Rows under several roots: the one holding the most workspaces that are not `cleaned` (then clones, then logs) is used and the others are named in a warning. |
| none | chosen | the chosen root, recorded; stored paths under another root are named in a warning and never refuse |
| R | default | R, whatever today's default is |
| R | chosen, same directory (any spelling, links resolved) | R |
| R | chosen, another directory | allowed and recorded only when no workspace row that is not `cleaned`, no directory under `R/.repos` and no `running` execution is under R; otherwise the start is refused with a message naming the move command |
| any, and `<data dir>/workspace-root-migration.json` exists | any | refused with `migration in progress`: a move did not finish |

After the root is chosen: if it does not exist it is created (a temp
directory the system emptied; the stored workspaces that lost their files
are counted in a warning, and each worktree is recreated from its branch
when its Task next runs). If it cannot be created (an unmounted volume, a
data directory from another machine) the start is refused, naming the move
command, which then has nothing to move and only points the database at the
new root. A root whose `.forge/gc/owner` names another database is used as
before and reported. A refusal is the last resort, always names the exact
command (`RootChoice::migrate_command`: with `--data-dir` when the data
directory is not the default one), and never happens to an install that
started before roots were recorded.

A root inside a system temp directory is reported: one warning at start,
and a `workspace_root` entry under `recent_errors` in operator status
(`system_setting.workspace_root_status`, which also carries the warnings
above) until the root is moved. A root taken from the rows of an install
that never chose one is such a root wherever today's temp directory is, and
stays reported on later starts.

**One source of the root.** `settle` returns the root; the caller hands it
to `ForgeRuntimeBuilder::with_workspace_root`, which hands it to the Task
service, the lifecycle emitter, the cleanup scheduler and the workspace
backends. Code that holds only a database (Genesis repository provisioning)
reads the record through `workspace_root::root_of`. Nothing reads
`FORGE_WORKSPACE_ROOT` except configuration loading, and the server does not
export it. A service built without a server start (tests, fixtures) uses
`workspace_root::fixture_root()`, a per-process directory that is no
install's root.

`forge --migrate-workspace-root [<new root>]`
(`services::workspace_root::migrate`) is the only way a root with data in it
changes. It runs under the data directory's runtime lock, so never beside a
server, and in this order:

1. **Refusals, before anything changes**: work recorded in flight (an
   execution `running`; a check run `running` / `cancelling` / `cleaning`;
   a task step `claimed` or `suspended`; an integration attempt that is
   not at rest (at rest: `completed`, `cancelled`, `superseded`, `parked`,
   and `needs_review` / `ejected`, where the Task is back with a reviewer or
   coder; each only without an owner effect outstanding and without an
   operation whose result is running or unknown. `quarantined` and
   `reconciling` block, and the refusal names each Task with its attempt
   state); a workspace lease
   `active` and not expired); a target that is not empty (unless the old
   root no longer exists), not a directory, inside the old root or around
   it, or refused by the garbage collector's `refuse_root`; a `.git` file
   below a worktree's top level (a submodule, a nested worktree) that names
   the old root by absolute path; for a copy, less free space than the old
   root's size (each hard link counted in full) plus
   `workspace.min_free_bytes`. A refusal removes the directories it made.
   There is no daemon lock file to test: a daemon that shares the root is
   recognised by what it owns there, which stays (step 3).
2. **Journal**: `<data dir>/workspace-root-migration.json` lists the old
   root (in every spelling a stored path may use), the new root, whether
   entries are renamed or copied, the entries, what stays, and what is
   finished. It is written whole (temp file, `fsync`, rename) before the
   next step.
3. **Entries**: only what this database's Forge made. Every data
   directory on a machine used to share `<system temp>/forge/worktrees`,
   so the old root may hold another database's entries side by side with
   this one's. The root's top level is listed with `.forge`, `.forge/gc`,
   `.forge/logs`, `.repos`, `repos` and `main-agents` opened, and each
   entry moves on its own: a Task root when the Task is in this database, a
   clone under `.repos` when the repository is, a log directory when the
   Project is, or any entry a text column names (or names a path inside);
   garbage-collection state, `.forge/build` and `.forge-tmp` unless
   `.forge/gc/owner` names another database (then they, and the marker,
   are that database's and stay). What a daemon sharing the root owns
   (`.forge/workspaces`, `.forge/gc/daemon-owner`) always stays. Containers
   the move emptied are removed.
   Everything else stays and is listed in the summary. On one
   filesystem each entry is renamed. Across filesystems each is copied
   (links as links, never followed; permissions and modification times
   kept; two names of one file inside an entry stay one file; extended
   attributes are not kept), compared byte for byte with the original,
   journalled, and only then removed from the old root. Sockets, pipes and
   devices are not copied and not removed: they stay in the old root with
   the directories above them and are listed. A copy a crash left half-made
   is removed and made again from the untouched original.
4. **Git**: for each clone under `.repos` and each repository the database
   knows by a local path, every registered worktree that moved is linked
   again. Where both ends are Forge's and moved, both link files are
   rewritten (the worktree's `.git` file and
   `<clone>/worktrees/<name>/gitdir`). A user's own repository is never
   written by hand: `git worktree repair <moved worktrees>` is run in it,
   which rewrites its registration of exactly those worktrees, and the
   repository is named in the summary. Then both links are checked and
   `git status` must succeed in each worktree. Each clone is checked with
   `git fsck --connectivity-only`, and a worktree with `.gitmodules` with
   `git submodule status`; a problem there is reported and does not block,
   so a clone that was damaged before the move cannot make it unfinishable.
5. **Database**, one transaction: `workspace.worktree_path`,
   `repo_location.path` and `workspace_placement.workspace_handle` of
   server-owned rows (each with `version + 1`; a daemon's rows and its
   copies of them are not touched, and no `expected_version` holder
   survives the stopped server), `repo.local_path`, `repo.remote_url`,
   `execution.logs_path`, `agent_context_scope.workspace_path`,
   `agent_inquiry.workspace_path` and `findings_path`,
   `runtime.workspace_root` of the daemon inside the server process only
   (another daemon's row is its own statement and stays),
   `integration_attempt.repo_location_ref`, the paths inside
   `task_step.payload_json` and `result_json`, and the `workspace_root`
   record. A whole-value path is rewritten when it is the old root or
   starts with it and a `/`. Inside JSON the JSON-escaped old root is
   replaced only where a `/` or the closing `"` follows it, so a root that
   is a prefix of another path (`/data/wt`, `/data/wt2`) is never touched
   and the document stays valid. Before the commit the same columns are
   counted again: if any still names the old root, the transaction is
   rolled back and the command fails with `db pending` (files moved,
   journal kept, start refused until a rerun). Other text columns that
   still mention the old root (comments, event payloads) are history; the
   summary counts them.
6. **Finish**: the root is adopted for garbage collection (the marker in
   `.forge/gc` moved with it, so the old root is unclaimed and another
   Forge may adopt it), a `MOVED` file is written in the old root, and the
   journal is removed.

The move only rolls forward. There is no abort: entries already renamed and
Git links already rewritten cannot be put back safely by a tool that was
just interrupted, while finishing is always safe because every step is
idempotent. A stop names its cause (the entry and the error for a full disk
or a permission problem; an entry present in both roots or in neither).
When the old root no longer exists, nothing is moved: the Git links of what
is at the new root are repaired and the database is pointed at it, which is
also how a data directory that was moved with its worktrees is adopted.

The database is rewritten only after every entry is in place, and a server
refuses to start while the journal exists, so no running server ever reads a
path of a half-moved root. Running the command again continues after the last
finished step. Daemon-owned workspaces are not part of the move.

After a move to `~/.forge/worktrees` a per-run temp directory still does not
fit inside the Task root (`<root>/<task id>/.forge-task/tmp/<key>` is the
root plus 64 bytes), so runs use `<root>/.forge-tmp/<key>` (the root plus 22
bytes): that fits the 72-byte limit for any root up to 50 bytes, which
covers a home directory path of up to 33 bytes. The old macOS temp default
(`/var/folders/.../T/forge/worktrees`, about 64 bytes) fitted neither, so
those runs kept the inherited temp directory.

### Workspace garbage collection

Cleanup of a terminal Task (below) reclaims what Forge recorded. The
garbage-collection pass reclaims what it did not: directories left by a crash
between two steps, by a deleted Task, by a run that never settled. The server
runs it at the end of every terminal-Task sweep (every 10 minutes, inside the
same 60 second budget: the Task and repository backfill may use 40 seconds,
the rest is kept for this pass) and each daemon runs it on the same interval
over its own root. Both use one implementation (`executors::gc`) and differ
only in the table that says what a directory is: workspace rows and Tasks on
the server, the persisted handle table on a daemon. A pass resumes from a
cursor (64 Task roots per pass), stops at its deadline, and counts an error on
one entry without stopping.

The pass runs only on a root that carries Forge's marker naming this owner,
and a sweep never writes that marker. A running server (`forge`, Forge Solo)
adopts its workspace root once at start-up: it writes this database's identity
(the `workspace_gc_owner_id` system setting) to `<root>/.forge/gc/owner`. A
daemon does the same for its handle table when its workspace backend is built,
in `<root>/.forge/gc/daemon-owner` (a server and its embedded daemon share one
root and sweep different directories of it). A scheduler or backend that never
went through that step (every test, every tool) sweeps nothing and writes
nothing. No configuration key turns the pass on or off.

Adoption is refused, and nothing is ever swept, when the root is the
filesystem root or one of its top-level directories, the home directory or a
parent of it, a git repository (it has a `.git` entry), or a path that does
not resolve to itself (a symbolic link on the way). The server resolves its
configured root once per pass and looks for the marker inside the result. The
marker is written whole or not at all (a staged file, then a hard link), so of
two servers starting together on one root exactly one owns it.

A Task root is a real directory whose name is a Task id (a 36-character UUID)
directly under the server workspace root, or `workspace-<uuid>` under
`<root>/.forge/workspaces` on a daemon.

| What | When it is removed |
|---|---|
| Task root with no workspace row, no Task and no Project of that name (daemon: no handle), that Forge made: it holds `.forge-task`, `.forge-outbox`, or a linked git worktree | Renamed to `<root>/.forge/gc/<name>-<unix seconds>` once the directory is older than 24 hours and no create of this process is in flight for it; deleted 24 hours after that. Never deleted where it stands. The rename has no copy fallback: if it fails (another filesystem), the directory stays. A directory with a Task-shaped name and none of those marks is a user's and is never moved. |
| Quarantined directory whose record appears later | Moved back when its place is still empty, otherwise left in quarantine. Never deleted. |
| Task root whose workspace rows are all `cleaned` (daemon: handle marked cleaned) | Renamed into `<root>/.forge/gc/trash/` under the Task's lifecycle lock (daemon: the handle's owner lock), only while the Task is terminal and has no running execution, active lease, run or create of this process; deleted after the lock is released, in the same pass or the next. |
| `<name>.broken-<ms>` in a live Task root | 7 days after the time in its name; earlier with its Task root. |
| Per-run temp directory `.forge-task/tmp/<key>` or `<task roots dir>/.forge-tmp/<key>` | When this process does not hold it, its key belongs to no running execution, and it was last modified more than 25 hours ago. While the root's filesystem is under the free-space floor that age drops to the age at which a check checkout is taken as dead (twice `server.check_run_timeout_seconds` plus an hour, 2 hours by default and on a daemon; never more than 25 hours): the check-run limit is the one configured limit on a run that is in no table, so what crashed runs leave behind stops accumulating for a day exactly when the disk is short. Neither is a bound on a run: an execution deadline is set per agent and has no ceiling, and a longer run is kept by the first two conditions. The sweep at start-up uses the same rule: a run a previous process left detached (a protected push, a check still settling) can still be alive after the restart and is in nobody's table, so being older than this process condemns nothing. |
| `.forge/build/checks/check-*` | Last modified longer ago than twice `server.check_run_timeout_seconds` plus an hour (never under 2 hours; a daemon uses 2 hours), and either the owner's check table (the server check runner's live operations, the daemon's running commands) is empty or the checkout predates this process. |
| `.forge-task/build` of a live, non-terminal Task | Only while the root's filesystem is under the free-space floor (`workspace.min_free_bytes` or `workspace.min_free_percent`, whichever is larger, default 10 GiB or 5 %; or `workspace.min_free_inode_percent` of its inodes, default 5 %), least recently used first, until it is back above the floor. Never for a Task with a running execution, an active lease, or a hook, check or tool command of this process in its root; never while any check operation is live. The server decides each Task under its lifecycle lock after reading its state again (it waits at most one second for that lock and otherwise skips the Task until the next pass: a pass can run inside a claim, and must never wait for the claim that asked for it), and the eviction is one rename into `<root>/.forge/gc/trash/` made under the lock every run start takes (the sandbox's live-run registry): a run that starts meanwhile is either seen, and keeps its build output, or starts after the rename and gets the directory made again. Every run of a Task root is in that registry, with or without a per-run temp directory of its own (a Task root whose path is too long for a socket has none, which is the usual case on macOS); a daemon's eviction uses the same registry. The delete happens after both locks are released. A terminal Task's build output goes with its Task root. |
| Entries of the shared compiler cache (`<cache dir>/<repository id>/**`, default `<root>/.forge/build/cache`) | Nothing at all on a root with no cache installed and no default cache directory (no stat of a Task root, no measurement). Otherwise two rules. On every pass, wherever the cache directory is: all stores together are held to `workspace.compiler_cache.max_bytes`, least recently used first (the later of a file's access and modification time). While the root's filesystem is under the free-space floor, and before any Task's build output: least recently used first until the disk is back above the floor or the cache is down to half of `max_bytes` (down to nothing for a cache left in the default directory after the setting was removed, which is collected under the floor only). A cache directory on another filesystem than the root is not trimmed for the floor. Only regular files inside a store that carries Forge's marker file are deleted: never a directory, a link, a socket, a `*.lock` file, a file directly in the store (marker, launcher, a wrapper's index) or anything under its `tmp`. A run can write its store, so each delete reaches the file from the store by file descriptor (`openat` with `O_NOFOLLOW`, `unlinkat`): a directory swapped for a link between the walk and the delete is not opened and nothing outside the store is deleted. Every tree removal of the collector (trash, quarantine, run directories, check checkouts) walks the same way, and a Task's build output is moved to the trash by a rename anchored at the Task-root directory, so a link planted where `.forge-task` was is never followed. A `sccache` store is trimmed whatever runs (an entry deleted under its live server is a miss and is stored again; removing the store directory would make every write fail, so it is never removed). A store of any other wrapper is left alone while a run of this process was handed it (the live-run registry records each run's store), while an owner-local hook, CI step or bounded review step runs in a worktree of its repository (those have no run scope and hold the store through `sandbox::hold_compiler_cache`), and while the owner has a live check or command. A cache store or cache directory is never a Task root: it is never quarantined and never removed as a cleaned Task's leftover, wherever the operator put it. The pass also measures the cache for the machine's disk facts (`compiler_cache_bytes`). The one case where a whole store goes: the store of a repository the owner no longer has, 24 hours after it was last handed to a run (its marker is rewritten at every run start), never while a run of this process holds it or the owner has a live check or command. A store is named after the repository's Git directory, so the server knows the store of every `repo` row's clone (`<root>/.repos/<id>`), of every `local_path` and of every worktree still on record; a daemon those of its repository locations and live handles. An unreadable table, or an owner that records no repository at all, removes nothing. Only a cache directory inside the workspace root is swept this way (the root's ownership marker keeps other databases out; a directory configured elsewhere may be shared and is bounded by its size cap only). The store is renamed aside under the lock every run start takes; its `sccache` server is then stopped (`--stop-server`, through the socket in the moved directory) and the directory deleted, both outside the lock and without following a link. Every removal goes down from the managed root by file descriptor, holds at most 32 directory levels open (a deeper directory is first moved to the top of the tree being removed), and never enters a directory on another filesystem. |
| `.forge/logs/<project>/<task>` (server only) | `workspace.log_retention_days` (default 30, `0` keeps logs forever) after a terminal Task last changed and after the last write to the directory, whichever is later, by the terminal-Task sweep, on a root this database owns. A reopened Task is not terminal and keeps its logs; a Task whose record carries an old or future timestamp keeps logs written inside the retention; a Task with an open review (running or waiting for a person) or an unresolved attention item about it keeps its logs until that is decided or resolved. |
| Legacy: `<root>/.forge/logs/<project>/<task>/.codex-managed-home` | With its terminal Task (unchanged). |
| Legacy: the daemon's shared `<root>/.forge-daemon/execution-logs/.codex-managed-home` | When the daemon runs no execution and no command. |
| Legacy: `<system temp>/forge/logs/<task id>/hooks` and `<system temp>/forge-gemini-api-key-home` | By the running server only, when it runs no execution and nothing touched the location for 7 days; a hook-log directory only when `<task id>` is a Task of this database. The system temp directory is shared by every Forge on the machine, and one server cannot know what another runs. These are exact paths; nothing else in the temp directory is removed. |

What the pass never touches:

- `.forge` (except its own `gc/`, `build/checks/check-*` and, under log
  retention, `logs/<project>/<task>`), `.forge-tmp`
  itself, `.repos`, `main-agents`, a directory named like an existing
  Project, and every name that is not Task-root shaped. Such names are not
  even listed.
- Anything outside the workspace root, apart from the exact legacy paths
  above. Every removal checks that the entry's parent resolves inside the
  root.
- Anything through a link. A link or a file found where a directory is
  expected is removed as the entry it is, or skipped; a Task root that is a
  link is not a Task root.
- A user's `local_path` repository: only exact worktree paths, as before.
- A directory that is being created. On the server a create has its Task
  before its directory; on a daemon a handle is recorded before its
  directory; a worktree create or recovery of this process registers its Task
  root before the directory exists and until it returns
  (`workspace::creating`); and no directory younger than 24 hours is
  quarantined.
- A live run. Hooks, checks, tool commands, probes and executions of this
  process are in the sandbox's registry, which also records the Task root of
  each run, whether or not the run has a temp directory.
- A workspace root owned by another database. A server whose identity is not
  the one in `<root>/.forge/gc/owner` sweeps nothing there, because its table
  would call the other server's live Task roots unknown. Two servers must not
  share a workspace root. This also happens after a database reset: the root
  still names the old database. It is never taken over automatically. The
  server says so at start-up and on every pass in its log, and operator
  status (`GET /api/v1/operations/status`, the Operations page) carries a
  `workspace_gc` entry under `recent_errors` for as long as it lasts. To take
  the root over, stop Forge and start it once with `--reclaim-workspace-gc`:
  directories the old database knew and the new one does not are then
  quarantined after a day and deleted a day later.
- A daemon root owned by another handle table. The daemon's identity is kept
  in its persisted workspace state, so a daemon whose state was lost is a
  different owner and sweeps nothing (it logs a warning; to hand the root
  over, stop it and delete `<root>/.forge/gc/daemon-owner`). One daemon
  process per root sweeps: a backend holds an exclusive lock on
  `<root>/.forge/gc/daemon.lock` for its life, and a second process (or
  backend) on the same root neither sweeps nor removes run directories at
  start-up, because the first one's runs are invisible to it.

A pass always finishes at least one Task root and moves its cursor past it,
so an entry that takes the whole budget cannot starve the ones after it.
Removing a tree is itself bounded by the pass: the removal looks at the
deadline every 256 entries and stops between entries when the pass is out of
time. What is left stays where it was (in the trash, in quarantine, in its
Task root), the pass reports `out_of_time`, not an error, and the next pass
carries on. A tree smaller than that always goes whole. Free space and free
inodes are read with `statvfs`; when they cannot be read nothing is evicted.

A deleted Task is reclaimed like a terminal one, whatever state it was
deleted in: deleting schedules its cleanup, and the cleanup waits until no
execution of it runs, no lease on it is active, no check run of it is
dispatched (`running`, `cancelling`, `cleaning` or `uncertain`), and no hook,
check or tool command of this server is live in its Task root. It comes due
again every minute until then. Deleting stops none of these by itself (a Task
can only be deleted from an inactive state). What ends them: an execution
ends by its own deadline or stall recovery; a lease expires; deleting cancels
the Task's check consumers, so a queued check run is cancelled and a
dispatched one runs to its wall limit (at most a day); a suspended review-CI
wait is a row, not a process, and holds nothing. There is no case in which
the directory is kept for ever, but an execution with a long per-agent
deadline keeps it for that long.

### Disk-pressure admission

A full disk must never silently kill work. The filesystem holding each
machine's workspace root is read for free bytes and free inodes: the server
reads its own root at admission (one reading answers for five seconds), and a
daemon reads its root for every report and sends the reading with it. All
machines are held to the server's floor, the same one the collector evicts
to, so eviction and refusal cannot disagree: `workspace.min_free_bytes` or
`workspace.min_free_percent` of the filesystem, whichever is larger, and
`workspace.min_free_inode_percent` of its inodes. A daemon receives the floor
in the reply to its report and uses it for its own collector; it has no floor
of its own. The floor is kept per workspace root, so two backends in one
process never see each other's.

The byte floor counts for at most half the filesystem: a volume smaller than
`min_free_bytes` (8 GiB under the 10 GiB default) has a 4 GiB floor instead
of one it can never meet. A filesystem that reports no size at all is not a
reading and refuses nothing; one that reports no inode count (APFS, btrfs)
has no inode floor.

A daemon's reading is trusted for five minutes, counted on the server's
clock from the report that carried it (the stored `measured_at` is the
server's time of receipt). A daemon that has been silent for longer has a
disk nobody knows: its old reading no longer refuses anything, and is shown
without `pressure`. A Task that could only run there waits for the owner
(`owner_unreachable`), which is a different and visible wait; another machine
takes it if one can. The other direction, an old reading over the floor on a
disk that has since filled, is closed on the daemon (below).

Under the floor a machine starts no new disk-consuming work:

- **Placement.** A candidate whose owner is under the floor is rejected with
  the filter code `disk_pressure`, beside `machine_capacity`. Other machines
  stay eligible and are chosen. A Task that already has a ready worktree
  there (its own, or the root's a subtask shares) is never refused: finishing
  work is how space comes back. A ready placement whose directory is gone
  would be made again at claim, which is new disk: that recreate is refused
  with the same code. The same holds for the other callers that turn the
  refusal into the Task's disk wait (a before-work hook, the review entry's
  check request, a launch after its claim and a reassignment reset): they
  ask with `WorkspaceManager::waiting_for_disk()`, and
  `ensure_valid` then returns the typed outcome
  `WorkspaceUnavailable::DiskPressure` when the worktree would have to be
  made again under the floor (directory gone, or there and unusable so that
  it would be moved aside and checked out again), and changes nothing. It
  converts to the `disk_pressure` placement refusal a claim gets
  (`placement::capacity_wait_of` reads it as the disk wait). What each
  caller does with it:
  - *A hook of a Task step* (`run_before_work_hooks`, `run_ci_steps`): the
    hook waits (`workflow::actions::common::wait_for_disk`). The Task takes
    the disk wait and the hooks step suspends on it (`awaited_consumer_id =
    'disk'`, uncharged), to run the hook again every 30 s until the disk
    recovers. Nothing is recorded as a hook result. A hook that reported the
    refusal as a failure instead sent the entry down its failure path: a
    blocking `before_enter` failure settles the step `failed` and annotates
    the Task "Transition committed; hook failed", which parks it for its
    owner, and the annotation moves the Task version, so the disk wait
    recorded a moment earlier was stale at once.
  - *A launch after its claim* (`run_execution`, the provider's
    `owner_execution_path`, the embedded executor's turn start):
    `TaskService::defer_launch_for_disk` ends the execution before dispatch
    (error prefix `waiting for free disk before launch:`, `resume_policy =
    auto`, the Task is not blocked) and records the Task's disk wait; the
    next claim waits for disk like any other.
  - *A reassignment reset*: records the wait and returns the typed refusal.

  Callers that do not ask (merge and rebase) are not refused: they would
  report a failed step, so a missing worktree is made again under the floor
  for them until their owner maps the typed outcome. A worktree that is
  there and valid is never refused,
  and neither is the in-place relink of one whose Git link broke (`git
  worktree repair`: two pointer files). A running execution is never stopped
  for disk.
- **The wait.** When every machine that could take the Task is short of disk,
  the Task takes the machine-capacity path: a recorded `machine_capacity`
  dispatch disposition carrying `capacity_scope: "disk"`, no annotation, no
  Attention, no failure, no retry-budget charge and no Project pause. The
  Task's condition is `capacity` with scope `disk`;
  `workflow_health.stale_reason` is `disk_pressure`. Both, and the
  `disk_pressure` filter code of the Task's placement diagnostics, are
  derived from the typed `capacity_scope`, never from the wait's message.
  When a machine with room on its disk exists but has no free run slot, the
  wait is the ordinary one for a slot: one reason at a time, the nearer exit.
- **The wake.** A run slot comes back with a commit (an execution ends), and
  that commit kicks the Tasks waiting for a machine. Free disk comes back
  with no commit at all, so a disk wait is a timer: while any machine is
  under its floor, every Task waiting for a machine carries a 30 second
  deadline on its scheduler wait row, and the dispatcher loop's own tick
  reads it again when the deadline passes (and arms the next one if the disk
  is still short). The deadline is durable, so a restarted server reads the
  waiters again at once. A freed run slot re-evaluates a disk waiter like
  any machine waiter and leaves the disk wait in place while the disk is
  short. A daemon that disconnects or is removed while Tasks wait for its
  disk stops counting within five minutes (its reading goes stale) or at
  once (removal), and the next timer tick moves those Tasks to another
  machine or to the owner wait.
- **Checks.** A check run is never held for disk. Every run the check worker
  dispatches (entry CI, review CI, the merge queue's checks) executes in its
  Task's existing worktree on the server or the daemon and makes no checkout,
  and a run in an existing worktree is never refused: finishing the review or
  the merge is how the worktree is given back. (A wait there would also end
  in the 30 minute queued-run expiry, which settles a run as an
  infrastructure failure.) Only the daemon's exact-commit check, which makes
  its own checkout and which nothing dispatches yet, is refused under the
  floor (below).
- **Reclaim first.** Before the server is counted as short, its garbage is
  collected once (trash, quarantine past its age, cleaned leftovers, build
  eviction) and its disk is read again. An admission asks for this only when
  the server is under the floor itself; at most one such pass a minute, with
  a 20 second budget. One caller runs the pass; every other admission
  returns at once, whether a pass is running or just ran, so a burst of
  claims neither runs a pass each nor queues behind one. The pass holds the
  collector's own cursor and, for at most a second each and one at a time,
  the lifecycle lock of a Task it evicts from; nothing a claim of another
  Task or another machine takes. The cleanup tick does the same whenever free
  space is under the collector mark (`workspace.gc_free_bytes` or
  `workspace.gc_free_percent`, default twice the floor) instead of waiting
  for the 10 minute timer. A daemon looks at its disk every 30 seconds and
  sweeps at once under the mark (at most once a minute), so its next report
  already carries what it reclaimed. When nothing collects on a root (the
  server's `workspace_gc` state, or a daemon's reported `gc_state`, is not
  `owned`), the wait says so: no automatic reclaim will end it.
- **The daemon's own check.** The server decides from the daemon's last
  report. Before it makes a worktree (`workspace.prepare` of a new one, or of
  one whose directory is gone) or an exact-commit check checkout, the daemon
  reads its disk itself and refuses with the error code `disk_pressure` when
  that reading is under the floor the server sent. Nothing is journaled for a
  refused request, so the same request is admitted once there is room. The
  server maps the refusal to the same `disk_pressure` placement refusal and
  disk wait; no retry budget is spent. A daemon that has no floor from its
  server yet refuses nothing. `workspace.prepare` of a worktree that exists,
  and every run in one, is never refused.
- **Unreadable disk.** A reading that cannot be taken refuses nothing, on the
  server and on a daemon (each logs one warning until the disk can be read
  again, not one per admission or report), and for a daemon that sent none. A scheduler nobody enabled admission for (every test
  and tool) knows no floor and never reports pressure; only a running server
  (`forge`, Solo) turns it on, beside adopting its workspace root.

Machine reads show the reading with the floor applied: `disk` on a daemon
(`GET /api/v1/daemons`), and on every `daemon_pressure` entry of operator
status, where `disk.pressure` (`bytes` or `inodes`) marks a machine that is
under its floor. `forge-ctl daemon` tables print it in the `Disk` column.

Each pass also measures live server-owned Task roots (32 per pass, least
recently measured first; disk blocks, links not followed) and writes
`workspace.disk_bytes` and `workspace.disk_measured_at`. A walk that passes
the deadline writes nothing; one that passes 500 000 entries writes only the
time, so `disk_bytes` stays `NULL` rather than hold a partial number. A daemon
does not report sizes.

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

A candidate rejected only for `disk_pressure` (its workspace filesystem is
under the free-space floor, see Disk-pressure admission above) is the same
kind of wait: where this section says `machine_capacity` for a rejection, a
rejection for `disk_pressure`, or for both, takes the same path, and the
recorded disposition names the disk.

A Task with no eligible machine and at least one candidate rejected only for
`machine_capacity` records a `machine_capacity` queued dispatch disposition,
without an annotation, Attention, failure or retry-budget charge. Initial
scheduling checks fresh read-only machine counts for every candidate before the
expensive Task gates, including runs dispatched earlier in the tick. The check
examines all potentially usable locations, including unverified clones; unknown
facts, a possibly free machine or another placement refusal delegate to reserve.
It uses the placement filters and server executor availability, without clone
verification, persisted location writes, a sweep or a writer lock.
A Task never waits for a slot it holds: when one of the Task's own executions
is running the precheck answers "not waiting" before it counts the machine, so
a machine capped at one run (`max_concurrent_runs: 1`, a supported setting)
does not show its only running Task as a capacity waiter, and a wait recorded
earlier is retired at that same observation. The run's follow-up (retry, review
entry check, reviewer) asks for a slot only after that execution has ended.
A paused Agent asks for no slot. A Task that has not started is not dispatched
while its Agent is unavailable, so reconciliation asks the dispatch's capacity
question for it when it carries a machine/Project capacity wait: a wait nobody
holds is retired and the Task parks on its Agent, a wait that is still real
(an offline Agent on a full machine) is kept. Startup therefore cannot leave a
capacity wait behind when recovery cancelled the last holder and the Agent is
paused. A started Task is asked by its role admission check, as before.
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
runs take a slot through the durable check runner, and queued checks are
admitted before new executions (see "Who gets a freed slot" under the durable
check runner); merges take no slot.

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
| Linked worktree of a repository other than the recorded one | moved aside as `<name>.broken-<ms>` and recreated from the Task branch; never relinked. When no recorded repository has the Task branch and the worktree is checked out on exactly that branch (a worktree made before its Repo moved to another location), the worktree is the only home of the Task's work and is used as it is; on any other branch it is never used | `Absent`, or valid in the same exception |
| The worktree path, or its Task root, is a symbolic link | `ResetRequired`; nothing is used, moved or deleted | `Absent` |
| HEAD on another branch or detached, Task branch exists, no rebase in progress | Put back on the Task branch without losing a commit. HEAD is the Task branch's own commit: `git checkout <Task branch>`; no file changes and local changes are kept. HEAD is strictly ahead of the Task branch (commits made on a detached HEAD, or after an interrupted rebase finished): the Task branch is advanced to HEAD with a compare-and-swap `git update-ref` and checked out; no file changes, local changes are kept (`Repair::FastForwardedTaskBranch`). The exception: when HEAD is already contained in the Repo's default branch (local or `origin`), its extra commits are the target's, not the Task's, and the Task branch is never advanced to them; that state is handled like HEAD behind the Task branch. HEAD and the Task branch have diverged: HEAD's commit is kept under `refs/forge/rescued/<task id>/<UTC timestamp>` in the worktree's repository, one Forge comment on the Task names the ref and the commit, and the Task branch is checked out (`Repair::RescuedOffBranchCommits`); the checkout is never forced, so when it would overwrite uncommitted changes nothing moves and the result is `ResetRequired` naming the ref. One ref and one comment per rescued commit, however often the state is seen; at most the five newest refs per Task are kept, all of a Task's are deleted when its workspace is cleaned up, and the comment is not loaded into agent prompts. HEAD is behind the Task branch: `Execute` checks the branch out when the tree is clean, else `ResetRequired`; the others return `ResetRequired`. `Reset` is the exception to all of these: it returns the worktree as it is, flagged off-branch, for the caller's `git reset --hard HEAD` | valid, flagged off-branch |
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
refresh), the provider start parameters (`Execute`), the execution runner at
launch (`claimed_path`: a filesystem-only confirmation that the worktree the
claim just checked is still there and confined, no Git process, falling back
to `Execute` when it is not), the native executor at the start of a turn (`Execute`, with the
Task service's workspace root and repository-cache locks), reassignment reset
(`Reset`), review entry CI (`Check`, which a review re-run also is), blocking
`before_work` hooks (`Hook`), merge delivery and target-moved rebase
(`Integrate`), and two read-only users (`Inspect`): the lifecycle emitter and evidence
capture from a worktree file. The terminal does not go through the manager: it
opens in the recorded directory after its own path guardrail, so a broken
workspace can still be looked into.
A claim on a daemon placement still asks its owner's `describe`. One Task run
with one coding execution and one review makes about seven to ten manager
calls, each one Git process on a healthy worktree: claim (1, replacing the
three Git processes of the backend `describe` it used before), runner launch
(0), native turn start (1 per turn, native executors only), a blocking
`before_work` hook (1), entry CI (1), the reviewer cascade (1),
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
each file to the Task root themselves. The anchor refuses a recorded path
that is relative, contains `..`, has no Task root above it, or is (or sits
under a Task root that is) a symbolic link. It does not compare the path with
the configured workspace root, because rows outlive a changed root. The
anchors are counted by the same source-scan gate as the raw getters. A source-scan test,
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

The command protocol independently negotiates `machine_probe.v1` for
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
the protocol revision; the current revision is 7. Revision 7 is one
unreleased revision with two changes: `integration.release_objects` deletes
the refs an attempt imported, and every daemon report carries the disk facts
of the workspace root, its reply the free-space floor, and
`workspace.prepare` can be refused `disk_pressure` (Disk-pressure admission,
above).

Protocol revision 7 negotiates `workspace.v1` for `repo_location.verify`,
`workspace.prepare`, `workspace.describe`, `workspace.run`, `workspace.diff`,
`workspace.read`, `workspace.merge`, `workspace.reset`, and `workspace.cleanup`.
Plan-writing roles on a daemon-owned workspace require `execution.plan_transport`.
A daemon without it can still run reviewers, interactive executions,
server-owned shared-mount executions, filesystem requests and PTYs. Deterministic
placement refusals record a structured Task annotation naming the machine and
missing capability. Dispatch waits until eligibility facts change, then clears
the refusal and retries.
Upgrade the server first, then every daemon using `forge-ctl` from that server
release (protocol revision 7 or newer), restarting each with its existing
`--workspace-root`.
A connection below revision 7 receives `daemon_upgrade_required` and cannot use any
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
reconnects at revision 7, waking Task dispatch automatically. Upgrading the daemon
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

A root Task with subtasks (a coordination root) uses the same states with its
own rules: it moves `todo → in_progress` when it has a subtask to run
(`todo → planning → in_progress` when it has a lead), rests in `in_progress`
while its subtasks run, and enters `review` when all of them are terminal.
See [Root Tasks and ordered subtasks](#root-tasks-and-ordered-subtasks).

### `merging` is the integration queue (3.2 stage D2b)

There is exactly one merge path. An eligible root Task entering `merging` is admitted to the
integration queue of its repository's target branch and **stays in `merging`**
while the queue worker brings it to the head, rebases it only if the target
moved, has the head check run through the durable check runner, carries or
re-requires review, and fast-forwards the default checkout. No flag selects
this and no other code merges.

| What happens to the Task | Written by | Task status | Condition (`parked`, reason `integration`) |
|---|---|---|---|
| Entry: the `run_merge` hook admits the Task (`integration_steps::admission`): one attempt, bound to the placement it merges from, or a successor of the attempt of an earlier entry (same place in line) | the entry's hooks step | `merging` | `waiting` |
| Head: validate, bring the target tip to the Task's checkout if it lives in another clone, rebase onto the tip if the target moved | queue worker (attempt and queue rows only) | `merging` | `waiting` |
| Head check on a rebased commit | `integration/request_check` step, then the check's delivery | `merging` | `owned` / `checking` |
| Review authority for the exact commit: unchanged commit, or carry by attempt lineage; the permit and the protected `result` step | `integration/settle` step (or the check's delivery) | `merging` | `owned` / `fast_forwarding` |
| Fast-forward of the exact commit in the default checkout | queue worker, through the owner of that checkout | `merging` | `owned` / `fast_forwarding` |
| Result: execution evidence, "Changes merged" comment | protected `integration/result` step | `merging` → `done` | `applied`, then settled |
| Rebase conflict (committed with markers), or a red head check | `integration/send_back` step | `merging` → `merge_failed` (coder), typed `conflict_handoff` bridge for a conflict | `repair` / `candidate_check_failed`, handed off |
| The rebased commit is not covered by the passed review | `integration/send_back` step | `merging` → `merge_failed` → `review` (`review_refresh` bridge) | `review_required`, handed off |
| Infrastructure, a dirty default checkout, a paused Project, a suspended queue | `integration/park` step | `merging` | `deferred` with its cause (retried by the worker's sweep, or the owner's) |
| The machine that holds the Task's checkout is unreachable, or its placement is not `ready` again | `integration/park` step | `merging` | `deferred` / `owner_offline`, `owner_id` the machine; no retry is counted, the sweep asks again (back-off to `soon_retry_max`), and the reconnect makes the attempt due at once |
| The Task's worktree would have to be recreated under the disk floor at admission | the entry's hooks step (suspended on the disk wait), or the reconciler | `merging` | the Task's disk capacity wait (`machine_capacity` / `disk`), 30 s recheck; no attempt exists yet |
| The Task leaves `merging` by Cancel or a board move | that command; then `integration/clear` | the command's target | cleared |
| The owner holds the Task in `merging` | the Hold command; the worker parks the attempt at its next gate | `merging` | Hold stays primary; integration is retained behind it |

`merge_failed` therefore only means "a coder is repairing a conflict, a red
check or a dirty worktree", or the one-step hop to `review` after a
`review_required` send-back. A clean rebase no longer forces the old `merging → merge_failed → review`
bounce (bridge `target_moved_rebase` followed by `review_refresh`) by itself.
If review authority cannot carry to the rebased commit, a `needs_review`
send-back still goes through repair to fresh review. Rows of that shape written
before the upgrade are still read (history, budgets, the review-entry carry).

An interrupted review-refresh hop is recovered by one durable cascade keyed
to the Task's status entry and reviewer target. Its retries use the Task-step
worker's eight-attempt bound; a refused guard or exhausted recovery records
the existing `cascade_failed` owner park and raises Attention. The dispatcher
honors that park (and any other owner blocker or current dispatch refusal)
before asking for a refresh again. Unchanged passes append no commands or
scheduler generations. Restart begins again from the coder with fresh
execution provenance; Cancel ends the Task. A missing historical Review row
alone does not prevent the normal fresh-review hop. While the Task's machine
is away the dispatcher issues nothing and the Task waits for the machine,
with no failure recorded. The cascade's identity also counts the attempts of
that entry that ended without moving the Task: if the park is lifted while the
Task stays in the state, exactly one new attempt is issued, which advances to
review when the cause is gone and parks again when it is not.

The paused-integration resume hop uses the same entry-keyed cascade and honors
a current refusal. Merge admission also honors an unchanged admission refusal,
while still repairing a missing or stale integration statement. A server
composed without a merge service (no production runtime is; some test
harnesses are) admits nothing, so the sweep does not ask for admission there
and writes nothing on the Task. An accepted
role Retry probes machine capacity before issuing its replay command: while
the slot is full it keeps the accepted intent and its existing capacity park.
The machine's release or capacity change lets the normal replay proceed.

What the entry hook still decides itself, before the queue: a subtask has no
merge of its own (its commits land with the root's merge), so it leaves the
merge state by that state's landed-merge target (`done`), and a dispatcher
pass makes the same move for a subtask it finds resting there
(`Step::AdmitIntegration`), so a subtask never rests in a merge state with no
step and no park; a Task
without a worktree or a server without a merge service is skipped as before; a
handed-off conflict file that still adds conflict markers blocks the Task for
manual repair; a candidate that cannot be named (no implementation run, lost
workspace or repository) fails as a `workspace_error` merge failure under the
merge-fix budget; a transient error retries the hooks step with back-off.

Start order (server and Solo, `RuntimeSupervisor::start`): crash recovery,
which reconciles the server owner's outstanding effects first; the importer
(`integration_steps::import::import_at_start`); the dispatcher's start
reconciliation; the Task-step worker and the check worker; then the queue
worker, whose first sweep claims every queue with work and every head whose
lease ran out. The heartbeat monitor runs in both modes; it turns a
reconnected daemon's placements back to `ready`, which a head whose rebase is
done waits for.

`TaskService::compose_integration` is the one place the worker is built
(over `integration_ports`), and `TaskService::compose_checks` takes the same
`IntegrationWorkerConfig`, so the merge-path check family and the worker share
one set of timers. A Task step that writes an answer on an attempt wakes that
head's driver (`IntegrationQueueWorker::notify_answered`); a driver that
misses the wake reads the answer at its poll.

**Settle-time proof: `done` means the target contains the commit.** The
attempt's `integrated_sha` is a claim the worker wrote (from a fast-forward
receipt, or from a landed witness it read earlier). The `integration/result`
step does not report on it. Before it writes the evidence, the comment or the
`done` cascade it asks the owner of the queue's default checkout whether the
target branch contains the commit (`integration_steps::proof`): one
`git merge-base --is-ancestor` in a server-owned checkout, the read-only
`integration.target_facts` of the daemon that owns a daemon-owned one. The
read runs outside of any database transaction. A receipt never replaces it;
a fast-forward receipt on the attempt that is another attempt's, of a later
claim generation, of another owner, or that names another commit refuses the
report before Git is read. When the integrated commit is not the Task's own
commit (a merge commit of before the queue) both are asked for.

| Proof | Task | `result` step | Attempt |
|---|---|---|---|
| The target contains the commit | evidence, comment, `merging` → `done` | answers `done` | `applied` → `completed` |
| The owner answers "not contained" (the target was rewound after the fast-forward, or the row was wrong) | stays in its merge state, `deferred` / `unresolved_result` with an interruption | answers `not_landed` and ends | the worker drops the recorded result and takes the attempt off the slot (`applied` → `parked`, `target_missing_commit`, kind `needs_fact`); the counted retry timer queues it again and validation merges it against the target as it is (after the counted retries: `owner_required` with the Retry offer) |
| No answer (the owner is not connected, the checkout cannot be read) | stays, `deferred` / `unresolved_result` | stays pending, parked for `result_park` (600 s); the worker re-arms it on every sweep (`sweep_interval`, 30 s), so it asks about every 30 s | stays `applied` and keeps the slot |
| An imported landed row (no worker holds a slot for it) is not proven | stays, `deferred` / `unresolved_result` while it asks; `deferred` / `owner_required` with an interruption (an Attention item) at the second "not contained" (`IMPORT_ABSENT_ASKS`, about 10 minutes) or after `IMPORT_PROOF_ASKS` (5, about 40 minutes) asks in a row that nobody answered and nobody will (the target's machine was removed, its default checkout is gone, a connected machine keeps failing the read). While the machine that holds the target's default checkout is known and only not connected, the row waits for it and is never parked (`proof::target_owner_away`) | the import continuation asks again at `result_park` (600 s; no worker re-arms it). Only asks are counted, on the step's retry reason (`[asked N, answered]` / `[asked N, no answer]`): a run of the step that asked nothing (it waited for the entry's hooks, it failed) or that waited for an offline machine starts the count again. At the bound the step ends `done` (`park_unproven_import`) | stays `applied` with its claim while it asks, then `parked` (`owner_required`, no deadline, `integrated_sha` cleared). The owner's Retry admits the Task again and the queue reads the target as it is: a Task whose own commit is on the target is reported once and nothing is merged again; Cancel ends it |

**One writer per merge during an upgrade.** Three things could act on a merge
that was in flight the old way: the old merge's own hooks step, the import
continuation step, and the queue worker. The two Task steps exclude each
other (one step per Task). The worker is kept out by the claim predicate
(`integration_queue`, the head pick and the claimable-queues listing): an
attempt admission made is claimable; an attempt from before the queue (an
imported row or a `shadow:` row) only once its `integration-import:<id>`
continuation step has settled and no live hooks step of the Task's present
entry carries an effect of the old merge path (`merge_intent`,
`merge_outcome`, `rebase_target`). A reserved imported head (an unknown
result) is held while that hooks step is live and while its own continuation
step is live (pending, claimed or suspended): the continuation reports a row
it finds `applied`, so a driver that reconciled the head to `applied` under it
would be a second writer. A live step is asked for, not a settled one, so a
head whose settled continuation was pruned is not held. The hand-over is
admission, in both steps: the `run_merge` hook finishes the rebase the old
step started and then admits; a merge the old step had finished is admitted
too, the head finds its commit in the target and the proven `result` step
reports it (only if the candidate can no longer be named does the hook report
it itself, after the same proof). The continuation waits for an earlier,
suspended hooks step of the entry instead of admitting beside it. A `shadow:`
row of a deleted Task gets no continuation and is never claimed; the start
pass deletes it (`delete_orphan_shadow_attempts`) when it is the Task's
*current* row, is at rest (`queued`, `path_wait`, `parked`, `quarantined`,
`ejected`, `needs_review`), is not a queue's head and records no effect (no
intent, no running or unknown operation). A history row (`current = 0`), a row
that records a landed merge (`applied`) and a row in any state in which an
owner may have been asked for an effect are kept. "Deleted" is
`task.deleted_at` set or the Task row gone; nothing undeletes a Task. The
delete walks the partial index of current rows and the index on
`predecessor_attempt_id` (`V202610122352`), not the attempt history.

**Unknown results from before the queue.** An imported merge whose old
operation never reported is recorded `uncertain` against the machine that ran
it, without a fenced intent, so no owner receipt settles it. It is settled by
removing that machine (`settle_removed_integration_owner`) or, explicitly, by
its owner's Retry (`settle_imported_integration_operation`): the operation
becomes `failed`, a `reconciling` attempt becomes `quarantined`, and Git
decides from there. Until then it refuses the deletions below.

**Importer continuation.** At every start the storage importer runs at most 100,000 passes of up to
100 Tasks, stopping earlier when a pass has nothing left or examines no Task.
Then each *current* imported attempt (and each attempt the passive shadow
recording left) without a continuation gets one identity-fenced
`integration/import_continue` step, keyed `integration-import:<attempt id>`. The step applies the continuation of the
imported state under the Task's lease; running the start twice enqueues
nothing new. A `queued` pre-queue attempt whose settled continuation step was
pruned (Task-step retention, 7 days) would not be claimable: the queue
worker's sweep (`sweep_once`, `continue_queued_imports`) enqueues the
continuation of every `queued` pre-queue attempt that has none, so it does not
wait for the next start. The sweep's read seeks the current `queued` rows
(`integration_attempt_retention`), not every pre-queue row; the step's key is
unique per Task, so two sweeps, or a sweep beside the start pass, enqueue one
step and count one.

| Imported state | Continuation |
|---|---|
| `applied` (the merge landed and was recorded), Task still in `merging` | the step reports it as `result` does (evidence, comment, `done`) and completes the attempt |
| `reconciling` (unknown result) | the worker's reconciliation of the head. The old operation has no fenced intent, so Git decides once the checkouts can be read (every owner was restarted for the upgrade): the target contains the candidate → `applied` and reported once, never merged again; it does not → a normal round. While the checkouts cannot be read the head is `quarantined` (`deferred` / `unresolved_result`) and asked again on the reconcile timer; it is never moved to `queued` or `parked` with the result unknown. The step states `owned` / `reconciling` |
| `parked` waiting only for a fact of the candidate (missing-hooks entry, resumed or obsolete pause marker, stored carry, a rebase the old path started or finished) | the step admits the Task again: the imported row is superseded by one bound to the Task's placement and present candidate; the legacy `paused_integration` marker of a merging Task is cleared. A candidate the old path had already rebased is admitted against the commit its review passed, so the head checks the rebased commit and `settle` carries the review (one check, no new review). A stored carry merges as it is. A rebase stopped in the checkout is finished by the owner's rebase under the interrupted-rebase rule (continued; a conflict it stopped on is handed to the Worker). It is read before the landed witness: a rebase stopped on the target tip is not a landed merge |
| `parked` under a Project pause | the same; the worker then holds the Task while the Project is paused and re-queues it on resume |
| a clean-rebase bridge with the Task in `merge_failed` / `review` | nothing is written: the Task's own flow continues as before the upgrade (the dispatcher's one mechanical hop from `merge_failed` to `review`, whose entry checks the rebased commit and carries the review), and the entry of `merging` then supersedes the imported row |
| `parked` with a stored deadline | the worker's sweep re-queues it at the deadline; the step states `deferred` / `infrastructure` |
| `parked` otherwise (blocked, failed, human wait, owner wait, sticky disposition, lost workspace), `quarantined` | nothing automatic; the step states `deferred` with its cause and raises the interruption for an intervention; the owner's existing action re-enters `merging`, which admits a successor |
| `ejected`, `needs_review` | the repair or review already in progress continues; on its return to `merging` admission supersedes the row |
| Task no longer in the imported entry | the continuation writes nothing; a queued row is cancelled by worker validation, while another current row is superseded by the Task's next admission rather than made historical by this step |

A hooks step of a `merging` entry that was pending or running at the upgrade
executes the admitting `run_merge`. It first reads the effects that step
recorded: a recorded `merge_outcome` of `Done`, or a `merge_intent` whose
candidate the target already contains, is reported once (comment, `done`); a
recorded `rebase_target` without an outcome is finished by the
interrupted-rebase rule it was started under (a conflict is handed to the
Worker); anything else is admitted.

**Head check identity.** The check of a rebased head is asked for under the
identity review entry asks under (`workflow/actions/review.rs`
`request_entry_check`): the spec built by `build_check_spec` with the canonical
policy, the review state's effective configuration, the Task's workspace id
and placement generation, digested by `check_runner::policy::digest_input`.
The purpose (`queue_head_ci`) is a property of the consumer, not of the
result. A commit that passed these commands in this worktree at review entry
is a hit and runs nothing; a commit the queue rebased is another commit and
runs once; a daemon-owned checkout keeps its owner's frozen policy and always
runs, as at entry. A head is only checked on a clean worktree (validation
parks a dirty one), which is the entry's dirty-tree rule. Measured by
`head_check_runs_are_zero_on_an_unchanged_target_and_one_per_rebased_task`:
Tasks that reach the head with the target where their review left it run no
head check; Tasks that had to be rebased run exactly one each.

**Owner reconnect.** When a reconnected daemon's placement turns `ready`
(`recovery::reconcile_workspace_placement`) the Task's merge is woken
(`TaskService::integration_owner_ready`): a parked attempt with a retry time
is made due and the worker notified; a Task whose merge was waiting for that
machine before the queue existed (importer row P15, an attempt that names no
candidate) is admitted there. The worker's sweep never re-queues an imported
row that names no candidate: only admission (its continuation step, the
reconciler, the reconnect, the owner's Retry) continues it, by superseding it.

**Roles, not names.** The importer selects and classifies by the role a state
plays in the Project's workflow (`integration_queue::importer::merge_roles`:
merge, repair, done = the merge state's `accept` target, cancellation = any
other terminal state, review = the `run_ci_steps` entry), and the Task-side
targets of a merge outcome are resolved by `integration_steps::reconcile::
role_target`, including the legacy effects the admitting `run_merge` reports.

A workflow with two merge states gives each its own outcome targets: a landed
merge goes to the `accept` target of the merge state the Task was in, a
repair to that state's own `reject` target when it carries the repair role's
exit guard. A workflow in which no state has the `run_merge` entry hook has
no merge role and nothing of it is imported.

**Deletion guards.** `integration_queue::refuse_unresolved_integration` runs
inside the transaction that deletes a repository, a repo location or a
Project, and before a location's `is_default` changes. It refuses with
`DbError::IntegrationUnresolved` (HTTP `409 integration_unresolved`, naming
the Task and the machine that performs the outstanding effect: the owner of
the Task's checkout for a rebase run on another machine than the queue
target's, else the target's owner) while an attempt of a queue in that scope has an
effect intent, a running or uncertain owner operation, or is `reconciling` /
`ff_inflight`. Removing the machine is the operation that settles an effect
nobody can answer for (`settle_removed_integration_owner`).

The default-location change is guarded in `RepoLocationService::update`, in
the transaction of the write the `PATCH` route makes
(`refuse_default_change_while_integration_unresolved`). A forced Project
delete checks the guard before it cancels anything. The owner's override
(`approve`) on a Task in its merge state is offered as "Mark done without
merging": it ends the Task as done, merges nothing and cancels the attempt.

**Write-transaction rule.** No database write transaction is held across a
Git subprocess, an owner RPC or a write on another pooled connection anywhere
under `integration_steps`, `integration_worker`, `integration_ports` or
`db::integration_queue`. The obsolete direct merge service and workspace-backend
merge operations have been deleted, including their three review-authority
write locks around Git/RPC. The queue fast-forwards through the integration
owners (`ServerIntegrationOwner::merge` and the daemon's fenced
`workspace.merge`), after a short Task-step authorization transaction.

Queue/member reads, web labels and `integration.updated` are live. Retention
(3.2 D3c): `StorageMaintenanceWorker::prune_integration_evidence` runs in the
hourly slot of check retention, under the same `server.check_retention_days`
(30; `0` disables), at most 20 batches of 100 attempts. It calls
`IntegrationActivationRepo::prune_integration_evidence`, which empties
`effect_receipts_json`, `operation_receipts_json` and `observations_json` of
an attempt that finished before the cutoff. Deny-by-default: the attempt is
terminal and not current, has no effect intent, no pending / running /
uncertain operation and no uncertain receipt, its Task has no current attempt,
and its queue is neither quarantined nor names it as head. **No attempt row is
deleted, by decision**: the row is the queue's history (states, commits,
timings, lineage), and a Review's `execution.integration_checks` and a settled
Task's `integration_lineage` witness name it by id; no reader of those ids was
audited for a missing row, and deletion is not built without that audit. The
table therefore grows by one row per merge attempt: one per entry of a merge
state, plus one for each re-admission (a re-entry after a repair or a
re-review, an owner's Retry that replaces the attempt, a rebind). An ordinary admitted row whose receipts and observations were emptied keeps
its scalar columns and small path/check/timing JSON, typically a few kilobytes.
An imported row also retains `import_source_json`; receipt pruning does not
shrink that frozen bounded legacy snapshot, which can be much larger. The only rows ever deleted are those of a deleted
queue (`ON DELETE CASCADE` from `integration_queue`), the rows of a deleted
Project that have no queue, and the current, resting `shadow:` row of a
deleted Task (above). What a Review quotes of a head
check is stored in the Review itself. `integration_queues.finished_with_receipts`
in operations status counts finished attempts that still hold receipts.
Transfer cleanup and owner-journal acknowledgment run as before.

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

### Merge queue reads and `integration.updated` (3.2 stage D3a)

Everything a person or an agent reads of the queue is a projection of rows
other components own. Nothing in this section writes a queue, an attempt or a
Task.

| Read | Built from | Where |
|---|---|---|
| `GET /api/v1/repos/{id}/integration-queues`, `forge-ctl repo queue`, native `repo.integration_queue`, MCP `forge_get_integration_queue` | a keyset page of `integration_queue` by `(repo_id, target_branch)`, the members in progress of those queues in one statement (pinned to `integration_attempt_current_seq`: never the queue's attempt history), their Task rows in one | `db::integration_queue::reads`, `services::integration_reads::repo_integration_queues` |
| `integration` on Task detail, Task list items, MCP `forge_get_task` / `forge_list_tasks`, `forge-ctl task show` | the Task's current attempt joined to its queue (`integration_attempt_current_task`), with the members ahead counted on `integration_attempt_current_seq` | `db::integration_members_of_tasks`, `services::integration_reads::task_integration_summary` |

One function derives the summary for every surface. The attempt's state gives
`state`, `phase` and `position`; the Task's condition (reason `integration`,
same `attempt_id`) refines what only a Task step knows: which decision a head
in `awaiting_task_step` waits for, a failed candidate check, and the cause of
a deferral. A queued member, or a member parked under a Task or Project gate, reads
`waiting` with the current cause, from the same `task_row_gate` the worker
and the `settle` step apply (a member parked under a stop that was lifted
names none). An active head retains its owned phase until the worker parks it. The member row carries whether its own
Project is paused, so no reader asks for the Project again. `action_hint` is
never a second rule: `retry` is `task_actions::integration_retry_offered`,
the function `available_actions` builds the `integration_retry` offer from,
so an attempt the worker parked before its park step told the Task hints at
nothing; `release` is the owner's Hold; `resume_project` is a Project action.

Who is listed: admission refuses a Task whose Project is not the queue's
repository's, so a queue's members are one Project's. The reads do not rest on
that. A queue read fetches at most 100 current members per queue, head first
then queue order, and lists only those of the reader's Project (the
repository's Project on REST, the bound Project for an agent, whose
`primary_repo_id` can name another Project's repository). `member_count`
and positions count all current members; another Project's member is never
listed, not even as the head. `members_truncated` reports whether the queue
has more current members than the fetch retained, before the visibility filter. A deleted Task keeps its place and loses its title. An attempt is "in progress" while it is `current`;
the table's CHECK makes a finished attempt non-current, so no read filters on
state.

The Task list reads the summaries of a page inside its existing read snapshot,
in one statement. Its `ETag` could not see the queue: the list-revision
triggers cover the tables a Task write touches, and the queue worker writes
none of them. Adding a trigger on `integration_attempt` would make the worker
write the Project row and break "the worker writes nothing outside queue,
attempt and Task step". The validator instead hashes an `integration_revision`
read with the snapshot: count, revision sum, queue-sequence sum and newest
write of the Project's members in progress (index
`integration_attempt_project_live`, migration `V202610111630`) plus each of
their queues' state and head, and the Project's pause (a wait cause of every
member that writes no Task). It also carries the exact set of those members,
each with its own `revision` (which only grows): the sums alone cancel out
when one member leaves and another joins with equal counters while the newest
write does not move (a wall clock set back). A lease renewal changes none of
these.

`integration.updated {repo_id, queue_id, task_id, project_ids}` is published
by `services::queue_invalidation::IntegrationUpdateProjector`, a periodic
worker (`integration-read-projection`) of the runtime supervisor. Each tick is
one SELECT on the pool (no transaction, no write) of every queue with its
members in progress, on `integration_attempt_current_seq`. The projector keeps
the previous read in memory and compares what a reader sees: per queue its
state, head and reason, per member the attempt columns the summary is
projected from and the number of members ahead (counted as the summary counts
it: a waiter carrying a cancel request is ahead of nobody). The row `revision`
is not compared, so a receipt, a fence or a lease renewal publishes nothing.
A queue in which anything visible changed gets exactly one event per tick:
`task_id` names the member when one member alone changed, and is `null` when
several did or the queue itself did; `project_ids` are the Projects of the
members concerned, so a client refetches those Projects' lists only. The first
read after a start publishes nothing: a restart replays no history. It waits
at least 250 ms and at most 1 s between reads and is woken by the domain-event
notifier, which every integration Task step already signals when it settles:
a step's change is published promptly, a change with no Task step (the line
moved, the head changed phase) within a second, and a burst inside one
interval is one event. The event is bus-only and carries ids only: no ledger
row, no Attention category, no notification, no wake. `GET /api/v1/events`
has no per-Project filter for any bus event; this one names nothing but ids
that read as `404` to a caller who may not see them. The integration steps
were not edited for it.

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
`crates/db/tests/suite/task_condition_write_cost.rs` and
`crates/db/tests/suite/task_condition_producer_cost.rs`, interleaved on one machine:

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

### Integration ownership and Task conditions

The `run_merge` entry hook admits an eligible root Task to the integration
queue; a Task without a worktree or a configured merge service is skipped,
and an unavailable candidate requires facts or an owner action instead. State names are not authority: custom workflows may rename
or define more than one merge state. Success follows that state's own `accept`
target; repair and review follow its workflow roles. Subtasks deliver through
their coordination root rather than creating an independent merge attempt.

The Task-step worker is the only Task writer. The queue worker owns queue and
attempt state and enqueues short Task steps; integration owners execute fenced
Git operations. No production database write transaction spans Git, an owner
RPC, a check run, or another pooled connection's write.

`ConditionStatement::Integration` states a typed reason under a live Task-step
lease, Task version, status entry and legacy-source fences. The seven reasons
are `waiting`, `owned` (phase), `repair`, `review_required`,
`candidate_check_failed`, `deferred` (cause/owner/message), and `applied`.
They contain an opaque attempt identity, never queue rank, lease tokens,
clocks or physical paths. Full path sets and operational timestamps remain on
the attempt; a condition samples at most 32 paths per path set. A settled Task retains only
its integration lineage.

A new attempt cannot overwrite another attempt's still-owned statement; a handed-off integration reason no longer excludes a successor. Superseding a
released predecessor preserves queue sequence; the Task-step writer clears
that predecessor's statement and states its successor in one transaction.
`IntegrationHandedOff` retains lineage while making coder/reviewer admission
ready. A real execution remains `running`; a real user Hold/block stays primary
with integration behind it. Self-clearing capacity, placement and environment
waits still reach their normal clearing action.

Ordinary queue and head ownership consumes no Agent execution concurrency
and retains an active Project slot; a requested CI run participates in shared
physical-machine capacity through the check runner. Owner-offline waits quietly park the slot. Dirty target, exhausted
budget and owner-required conditions are intervention blockers. Their material
identity uses cause, failure kind, owner and message; queue revisions, attempt
IDs, paths and timer ticks do not create incidents. A Task step emits a changed
`task.interruption_changed` with an intervention statement, in the same
transaction. Statements alone change no Task version or event.

Admission is level-triggered. A Task in a merge state has a live attempt or a
typed condition with an owner action. The dispatcher admits a missing/released
attempt without replaying custom entry hooks. Its compact attempt snapshot also
repairs missing/wrong condition ownership and stale parked/suspended causes
under the Task lease, preserving real Holds and blocks. A ready placement whose
binding changed makes a released attempt eligible for ordinary readmission;
this also recovers a missed reconnect notification. Corrupt/newer condition
formats retain the existing quarantine fence rather than being overwritten.

Three writers can state `deferred` for one attempt row: the `park` step, the
import continuation and the reconciler's restatement (from the scheduler's
compact snapshot). `integration_steps::deferral::deferral` is the one rule for
the cause and the machine waited for, from facts of the row: a quarantined
attempt is `unresolved_result` unless the importer made it and it names no
operation (`owner_required`: no result can still arrive); an imported row the
worker has not parked itself reads the importer's classification (P15
`owner_offline` with the machine the old wait named, a stored deadline
`infrastructure`, otherwise `owner_required`); any other row reads its park
reason code and failure kind. The reconciler restates a stated deferral only
when cause or machine differ, never for its wording.

### Queues, attempts and the authoritative target

A queue is unique by `(repo_id, target_branch)`, not by checkout location.
Branch validation removes one `refs/heads/` prefix and preserves spelling.
`resolve_integration_target_in_tx` selects the repository's single explicitly
default `primary_checkout`. `repo.local_path` is a provisioning hint. It does
not select a second target, and the Task's placement cannot replace the default.

| Default checkout configuration | Queue resolution |
|---|---|
| Exactly one ready default primary checkout, server or daemon | Open on that location |
| No default primary checkout | Suspended, `target_unconfigured` |
| More than one default primary checkout | Suspended, `target_ambiguous` |
| The configured default is not ready | Suspended, `target_unavailable` |

Admission pins the implementation execution's repository and workspace, even
if the Project later selects another primary repository. It records the
placement, owner kind, daemon, runtime, placement generation and reviewed commit.
A reconnect/verified placement recovery or the owner's Retry compares that
binding with the current ready placement. A released attempt with no unsettled effect is superseded with its queue
sequence retained. Admission preserves the reviewed original when it matches
the present HEAD or applies through recorded rebase lineage; otherwise it
pins the present HEAD and `settle` assesses review authority for that candidate.
Reserved heads and running/uncertain effects cannot be superseded; their frozen
owner must settle them first.

Claims refresh the target in the queue-revision CAS transaction. A live lease
or stale revision cannot refresh it. One head holds a target slot, including
while it waits for CI or Task-step authorization. Expired process leases permit
observation takeover; they never free an uncertain logical head. A takeover
advances `fence_generation`, stamps the head, drops an old ready permit, and
reconciles in-flight effects. Each new round receives another generation.

The additive queue/attempt and activation migrations retain user data. Their
SQL is authoritative for fields and indexes in `crates/db/migrations/`;
repository methods live in `db::integration_queue`. Updates use revisions and
the transition graph. There is one current attempt per Task and one current
reservation per queue sequence. Successors retain sequence, while eligible
position is computed by the read surface. Nullable foreign keys preserve
opaque Task/project/workspace/execution/location refs after cleanup. Task
removal keeps history, except the Task's current, resting `shadow:` row, which
the next start deletes (`delete_orphan_shadow_attempts`); repository/Project
deletion cascades its owned rows.

Attempts store candidate/original/target/integrated SHAs, predecessor lineage,
full conflict/repair/guard paths, owner binding, effect intent/receipts,
operation state, permit/acknowledgment, deadlines, failure, import evidence and
phase timings. Guard paths are retained data: path-guard enforcement is not
implemented. `path_wait` is reserved storage, not a production path guard.

### Startup import and legacy continuations

Startup attempts the bounded import before the Task-step and queue workers.
If import fails, the other workers still start; the queue worker retries import
every 30 seconds before its first sweep, and the admission reconciler admits
nothing while `integration_import_pending` remains set.
`import_integration_pass` examines at most 100 Tasks per transaction, retaining
at most 64 rows per history source plus a sentinel and at most 65,536 characters
per retained text column before materialization. A source whose UTF-8 byte length
exceeds 65,536 is marked incomplete, with its original byte length and character
prefix retained, and requires complete evidence. A per-Task savepoint retains
an unreadable row as quarantine and lets the pass continue.

Each `import:<task>:<epoch>` identity freezes the classification, missing facts
and source snapshot in `import_source_json`. The pass writes queue/attempt rows
only: no Task, Review, condition, budget, event or Git mutation. Classification
uses workflow roles and actual typed bridges, not bridge-looking prose or a
review-pass timestamp alone.

| Priority | Retained legacy shape and continuation |
|---|---|
| 1 | Recorded exact Done / merge intent needing ancestry or owner proof |
| 2 | Original remote operation still unresolved |
| 3 | Terminal history with no possible legacy effect |
| 4 | Pending/claimed merge hooks, needing candidate facts |
| 5 | Rebase started without an outcome |
| 6 | Clean rebase not yet checked/carried |
| 7 | Conflict already handed to its coder |
| 8 | Review-refresh bridge, or a missing review-pass marker with a retained review contract, without clean-rebase proof |
| 9 | Persisted review carry |
| 10–12 | Current/resumed/obsolete paused-integration markers |
| 13 | Hold, failure, human or entry blocker |
| 14 | Future retry or infrastructure interruption |
| 15 | Offline owner, upgrade or remote-cancellation exclusion |
| 16 | Removed/lost workspace owner, needing candidate availability |
| 17 | Accepted owner command or sticky disposition |
| 18 | Passed marker and pinned delivery with no runnable hook |
| 19 | Dirty/conflict repair already in progress |
| 20 | Unknown, malformed, contradictory or incomplete evidence |

Already-reported done Tasks and queue-owned entries are not scanned repeatedly.
A cancelled Task's terminal shadow attempt suppresses a redundant import only
when no legacy merge-intent checkpoint remains. A surviving intent is retained
conservatively even if storage cannot prove whether it landed. Its frozen
snapshot survives hook-step pruning; cancellation does not erase the only
possible-landed record.

Current imported/shadow attempts receive an identity-fenced `import_continue`
step at start; a `queued` one that has lost its settled step to retention
receives it again from the queue worker's sweep. Classification is committed
before that step can execute. A queued
pre-queue member is claimable only after continuation is acknowledged and no
live legacy merge hook owns it. A suspended older hook completes its recorded
rebase/read-only outcome and hands over through the same admission function.
There are no shadow observation writers in the active merge path.

An import missing its execution or pinned workspace (`corrupt_import`,
"missing execution or pinned workspace") is non-current because it has no
resolvable queue. Wholly unreadable or contradictory legacy evidence keeps
its existing continuation policy.
While it is the Task's only attempt and the Task still occupies that merge
entry, startup continuation and the dispatcher's sweep state
`deferred/owner_required` with its cause and raise the normal Attention
interruption. This includes quarantines committed by a previous start.
The Task integration summary exposes the quarantine, and operations status
counts it in `integration_queues.quarantined_imports`. Nothing readmits it
automatically. Retry uses normal admission to bind the Task's current branch,
workspace and ready placement; if the legacy execution or its workspace pin
is absent, only this explicit retry may use the Task's present workspace.
No execution or review evidence is fabricated. The new attempt retains the
quarantine as predecessor, requires the full head check even on an unchanged
target, and must pass normal review authority before fast-forwarding. If no
current workspace can be recovered, Retry reports the missing source and
keeps the park; Cancel ends the Task.

Once the owner has retried the entry, every later admission of that same
entry (a placement verified again, an attempt sent back or parked unbound)
uses the same source, and every attempt of it owes the head check, not only
the one the Retry admitted.

The park is level-triggered and idempotent. The dispatcher's sweep asks for
its statement only while the Task's condition does not carry it (never
stated, or lost); a stated park is not a missing merge entry, so later passes
and later starts write no Task step and no scheduler generation for it. A
later admitted attempt, a Task that left the merge entry and a deleted Task
are not the quarantine's: the predicate matches none of them. While the
Project is paused the sweep leaves the Task to the pause, Retry is withheld
like every other retry and the summary hints `resume_project`. A candidate
that is already contained in the target is reported as merged after the head
check; no commit is added to the target. Retry is refused, and the park kept,
when the Task's worktree and branch are both gone or the Task has no
workspace at all. When the legacy execution exists but names
no workspace, the head's review authority cannot be proven: the attempt is
sent back to the repair state and nothing merges. Every other `corrupt_import`
cause keeps the earlier policy: the row is history and the sweep admits the
Task through normal admission from its present rows, or blocks it with
`integration_admission_failed` when no candidate can be named.

The continuation binds a runnable candidate, states a wait/owner action, or
reports a landed result after the target-owner proof. A held/offline/lost-owner
row is not readmitted merely because its candidate was unnamed. Offline P15
resumes on verified reconnect; Retry asks again, and removal of a permanently
lost machine settles its old operation. An imported unknown operation with no
fenced intent can be explicitly settled by Retry: Git then decides whether the
candidate is already contained or must queue again. Completed reporting is
idempotent across restart; a pruned continuation is enqueued again at startup.

A queued or parked attempt names the workspace, location, owner, runtime id
and placement generation it was admitted with. When recovery has verified the
Task's placement `ready` under another of these (a daemon back under a new
runtime id, a re-placed Task), that binding is provably gone:
`INTEGRATION_OWNED` stops counting the attempt, and the reconnect wake, the
reconciler and the owner's Retry all replace it through the one admission
function (`admission::binding_stale`). The successor keeps the place in line;
the supersede transaction refuses a head or an attempt with an unsettled
effect. A placement that is not `ready` proves nothing: the attempt keeps its
binding and its park, and Retry puts that same attempt back in line.

### Fenced owners and object transfer

`services::integration_ports` assembles real owner, facts, location,
object-transfer and Task-step ports. `TaskService::compose_integration` gives
the server and Solo runtime their shared supervised worker. The former
`MergeService::merge`, `merge_workspace`/`merge_inner`, `WorkspaceBackend::merge`
and `MergeSpec` interfaces are deleted. `drain` and its synchronous queue
pumping compile only with test support. The remaining review-authority guard is
a test fixture; production authorizes in a short transaction.

An owner checks queue/attempt/generation/lease fences and the performing owner
before admitting an effect. The intent is committed before Git, and a durable
receipt records the result afterwards. A rebase off the target machine records
that performing owner in `witness.effect_owner`; it cannot impersonate the
queue's default-checkout owner. A repeated identical request replays its
receipt. An unknown started request is reconciled, never repeated blindly.

Server effects use keyed checkout/workspace locks and bounded process groups.
Daemon effects use the authenticated runtime, verified location, announced queue
fence, workspace generation and owner journal. `workspace.merge` remains the
queue's reviewed fast-forward message, so deleting the old Rust API removed no
wire message and requires no daemon revision bump. An owner may reconcile its
frozen request after observation ownership changes, but this grants no new
effect authority.

| Placement relative to the default checkout | Delivery |
|---|---|
| Same checkout/object store | Read/rebase/fast-forward locally on its owner |
| Another clone on the same owner | Import the target tip into an inbound claim ref; rebase there; export the exact candidate into the default checkout's outbound ref |
| Different owners | Transfer the same bounded bundles through both owner ports; rebase on the Task owner and fast-forward on the default-checkout owner |

Transfers are keyed by attempt/generation/direction, capped at 256 MiB, and
bind exact requested objects. Missing/wrong objects refuse the effect. The
Task's non-default target branch is not written. Claim refs are released after the slot; staging files are removed when a
transfer returns or is dropped. Startup sweeps server-checkout leftovers and
retains up to 4,096 daemon-end releases from attempts updated within 30 days;
connected-owner failures are tried at most three times, while disconnected
owners remain on that release list until they return.
The owner journals are acknowledged only after the server has retained their
receipt. Retention never discards an unsettled intent/receipt or evidence
pinning a quarantined queue.

### The active head pipeline

The supervised sweep runs on admission/answer notifications and every 30 seconds.
A Task that re-enters its merge state while the attempt of its previous entry
is still the head is admitted as soon as that head is released: the admission
flags the old attempt, wakes the worker and waits on the driver's release
signal (`IntegrationQueueWorker::notify_released`, sent by `HeadDriver::pass`
on `Pass::Released`), with a 100 ms poll for a worker of another process, for
at most `RELEASE_WAIT` (20 s) before it returns the conflict to its step. The
worker that signals is the one `TaskService::compose_integration` registers on
the service when the runtime is built, before any worker starts; every
admission is handed `TaskService::integration_worker()`. The admission
subscribes before the admit that sets the cancel flag, and only that flag
makes the driver release, so the signal cannot be missed.
At most 16 queues have drivers; claims rotate for fairness. It cancels released
members, makes due parks runnable, takes expired heads over, refreshes suspended
targets and revisits quarantine. One slow head/check does not hold another
queue's slot or a database writer. Members behind a head keep their reservation.

| Head state | Action and exit |
|---|---|
| `queued` / `validating` | Count/start a bounded round; read gate, candidate and target facts. Already contained → applied after proof; unchanged target → authorization; changed target → rebase; changed candidate → review |
| `rebasing` | Fenced owner rebase, or adoption of its existing receipt. Clean → checking; conflict → coder handoff; unknown → reconciling |
| `checking` | Short `request_check` step asks the durable runner. Passed result → authorization/permit; red → merge fix; infrastructure → bounded retry/owner wait |
| `awaiting_task_step` | `settle` re-reads review authority, carry eligibility, Task gate and owner-command ordering, then writes an exact permit or refusal |
| `ready_ff` | Reconfirm candidate and outbound objects; permit/cancel CAS; begin fast-forward |
| `ff_inflight` | Owner fast-forwards the exact candidate by ref CAS. Success → applied; external target movement → new round; unknown → reconciling |
| `reconciling` / `quarantined` | Ask the frozen effect owner for receipt/facts. Proven applied → result; proven not performed → queue/rebase; unknown → typed unresolved-result wait with timer and owner action |
| `applied` | Ready the protected result step; wait for its proof/report acknowledgment, then completed and release |
| `ejected` / `needs_review` | Released; Task step hands to coder/reviewer or an owner action |
| `parked` | Released; resume at its timer or verified gate/owner recovery, or wait for the offered owner action |
| `completed` / `cancelled` / `superseded` | Terminal history |

The first rebase for contention with an earlier queue member is a free
`queue_member` lost race. Movement while the slot is held is `external`; its
sixth occurrence parks the attempt. The head is bounded to nine rounds. The
first-rebase observation cannot distinguish an external push made before claim
from a previous queue member, so it remains a queue-member race.

Defaults: validation 30 s, owner rebase 120 s, CI wall bound 1800 s plus 300 s
waiting slack, deciding-step wait 300 s, permit lifetime 120 s, reviewed
fast-forward owner bound 30 s. Lease is 60 s, renewed after 15 s; waiting drivers
poll every second. Reconciliation asks every 60 s. Infrastructure retries are
30 s, 2 min and 10 min, then a typed owner wait. Quiet gate/offline/target-ready
waits double to a ten-minute maximum, with no counted Task retry. A red check
and a dirty Task checkout use the pre-queue merge-fix/rejection budget; a dirty
default checkout is an owner intervention with no Task retry charge.

Cancel and fast-forward admission are CAS writes on the same attempt. Before
`ff_inflight`, Cancel can win even after a permit is written. From the permit
transaction onward a pre-enqueued protected result step prevents a later owner
command overtaking the result; an expired/revoked permit releases that step.
An already-admitted fast-forward is reconciled rather than guessed cancelled.
Shutdown awaits an admitted effect's receipt; takeover retains uncertainty.

Timings accumulate per attempt: queue, validation, transfer, rebase, check
(slot/run or skip), Task-step wait, fast-forward and total head time. Lost races
retain their newest 32 entries and fold older counts. Worker park history keeps
its newest 64 entries. Neither is a Task condition or budget ledger.

### Task steps, CI identity and Review evidence

Integration requests are keyed by `(attempt, effect_seq, action)` and carry the
Task entry/generation fences. Their acknowledgment and Task effect commit
atomically. Deciding steps are enqueued before the corresponding attempt
transition; a step that arrives early retries, and stale generation/sequence
or Task entry writes nothing.

| Step | Task effect / answer |
|---|---|
| `request_check` | Ask the durable runner for the exact head; state checking. Its result-delivery step supplies the settle verdict |
| `settle` | Check review authority/carry and Task gates; pre-enqueue protected result; state fast-forwarding; write candidate/target/entry/generation permit or typed refusal |
| `result` | Obtain target-owner ancestry proof, write execution evidence and one merge comment, state applied, enqueue the workflow's success cascade, acknowledge |
| `send_back` | Record failure, typed repair/review reason and handoff; enqueue one workflow cascade/budget charge |
| `park` | State current deferred/waiting cause and emit an intervention when required |
| `clear` | Clear only this cancelled attempt's statement |
| `import_continue` | Apply the committed legacy classification or hand over through admission |

Review entry and queue head call the same `review_ci_identity` builder. Its
identity includes repository, exact candidate, normalized target branch,
commands, effective Review configuration, workspace generation and
owner/environment policy. Consumer purpose/authority do not change the result
identity. An unchanged reviewed commit tries transactional reuse-only admission:
a certified hit creates a consumer and no run. A matching passed Review under
a frozen/unattested policy retains its own result as review authority without
launching it again. A rebased commit is another identity and runs its check.
A missing matching entry result preserves the existing unchanged-target skip;
manual/agent review authority is still checked by settle. Only the `settle`
step that is due for its `effect_seq` records a reuse; a stale or answered
step writes no consumer. The reuse states no check wait on the Task (the
answer exists and the step applies it itself), and the head consumer is
created answered (`applied_at` set, no delivery step): the check worker's
delivery sweep leaves it out, so a merge of an unchanged commit spends no
Task step on a delivery. The Review's entry consumer is found by the request key
review entry asks with (`TaskCheckRequest::request_key`).

The consuming Task step appends the newest 64 command-shaped results to
`Review.details.execution.integration_checks`, with attempt, commit, consumer,
run, result and identity references and source (`executed`, `cache_hit`, or
`review_authority`). Original entry CI and immutable reviewer conformance are
preserved; head evidence grants no new reviewer authority. The write is in the
acknowledgment/permit transaction and is idempotent per attempt/result. It is
an annotation of the Task's newest Review: a Task with no Review gets none (a
check result never creates a Review row, which would read as review authority
or as a spent attempt), and details that cannot take it are left as they are;
neither fails the settle. `source` is read from the rows: the entry consumer
itself is `review_authority`, the first consumer of its run is `executed`,
any later consumer of a run is `cache_hit`. The
existing execution envelope is accepted by strict REST/MCP readers; generated
TypeScript exposes the new evidence. Raw Review workspace commands without
explicit limits now use 1800 s / 1 MiB; verdict/tail handling belongs to the
shared check executor and durable runner.

A result is never reported merged merely from `integrated_sha`. The target's
current owner proves it contains the exact candidate before `done`, including
imported landed rows and sibling target advances. Wrong-owner/generation receipts,
a forged SHA, an unavailable owner or a rewound target produce a typed
`unresolved_result`; a proven missing commit queues again when a worker holds
the row (`not_landed`), and parks an imported row for its owner at the second
such answer (no worker holds a slot for it; see the bounds table of
"`merging` is the integration queue"). Git/RPC proof is
outside the write transaction; the transaction rechecks attempt and Task entry.

### Deletion, recovery and operational limits

The unresolved-integration guard refuses repository/location/Project deletion
and default-location change with `409 integration_unresolved` while an effect
is outstanding or its result is unknown. Other resource guards can refuse
first: location deletion returns `409 version_conflict` while a placement still
names it and is not `cleaned`, before it reaches the integration guard. The guard names Task and machine and is checked in the mutation's
transaction. `force` cannot bypass it. Location deletion does not settle an
in-flight effect as failed. Removal of a permanently lost machine is the
explicit permanent-loss operation; it settles that owner's intents/imported
operations and lets the queue recover without silently dropping uncertainty.

Owner Retry readmits released/unbound/stale-bound attempts or makes a valid
park due. It cannot repeat a fenced unknown effect; reconnect/removal supplies
its receipt. Imported operations with no fenced intent can be explicitly closed
by Retry. Approve on a held Task in its merge state is labelled **Mark done
without merging**: it ends the Task and cancels its attempt without changing Git.

Retry of a quarantined import whose checkout owner is offline is accepted,
as Retry of an ordinary bound offline attempt is. The import retains a durable
retry-owner marker and the Task states `owner_offline` with that machine's ID.
No candidate read or attempt is charged while the machine is away. Reconnect,
or the admission sweep after readiness, enqueues the accepted continuation;
transient read failures then use the Task-step attempt bound and deterministic
or exhausted failures park typed. Hold, Cancel and Project pause still gate it.

Public queue/member reads, Task integration summaries, operations status and
`integration.updated` are live; see [api.md](api.md) and [cli.md](cli.md).
`pending_steps` counts `pending` or `claimed` Task-step rows, excluding the
caller's current step when read under that Task's lease. It does not count a
`suspended` check wait or an integration head waiting for CI or its slot until
that head enqueues a Task step. Queue position, phases, timing and actions come from the dedicated reads.

Path guards, speculative/batched merging and an automatic decision that an
unknown effect failed are not implemented. Transfer cleanup and bounded
receipt/journal storage are separate from evidence retention: current attempts,
unsettled effects and quarantine pins are never pruned just for age. Before a
running-install cutover, test a copy of its database/workspace root and inspect
the import report and operations status. Synthetic importer fixtures do not
replace validation of that install's retained owner evidence.


### Task condition actions

**Review without an Agent delivery.** This applies to a review gate whose
reviewer role is assigned to an Agent, or that has checks configured on a Task
with a Workspace that is not read-only (without a candidate the reviewer is
refused dispatch and the checks are skipped); human
reviewers and owner-approval gates without checks behave as before. A Task (not a coordination root) whose branch
has commits of its own that the target does not have (merges of the target do
not count) is a human submission and runs the ordinary configured review checks
and reviewer. Its completed delivery-evidence row records the exact base/head
and a human-submission summary, with no Agent, process, lease, session or logs.
It grants no review authority by itself. Review attempts retain their execution
foreign key and the ordinary candidate/admission fences. While the
Workspace's machine is away the branch cannot be read: nothing is decided, no
"nothing to review" park is stated, and the Task waits for the machine.

With no delivery candidate the review parks as
`failure { failure_kind: review_blocked }` with `blocking_reason`
`review_candidate_missing`, "nothing to review", and the existing
`execution_failed` Attention category. It offers Approve (the owner's decision,
exactly as before the park existed), Send Back to add work, and Cancel; no
reviewer retry is offered because there is nothing to retry on. Sending a
coordination root back holds it in implementation so the owner can add
corrective subtasks before Release; it cannot immediately advance to the same
empty review. An ordinary Task takes its configured rejection transition. The
park is stated once; unchanged dispatcher passes add no Task write, event,
step, execution or Attention incident.

The candidate of a coordination root is the latest implementation run of any
subtask, cancelled ones included (their commits are on the root's branch). A
root with no such run and a lead triggers planning once per review entry and
charges only the existing `replan` budget; exhaustion leaves the root in the
existing `budget_exhausted { replan }` park. Without a lead it uses the owner
park above, naming "every subtask was cancelled" when that is the case. An
unassigned, check-less gate that requires no human approval still skips into
`merging` by configuration. The merge entry finishes a root whose subtasks were
all cancelled as `done` without a merge when it arrives from the review gate
(skipped by configuration or approved by the owner) and it has no Workspace or
its branch is proven unchanged and clean. A changed, dirty or unreadable branch
keeps the ordinary integration handling.

`services::available_actions(&TaskSnapshot)` is the sole pure Task action resolver. The one snapshot builder loads Task, bounded execution authority, latest Review, role assignments, transition history, the typed condition and its normalized read presentation, entry/queue ownership, placement and Agent/Project availability, and caller authority. REST and MCP Task list projections carry no actions and obtain offers on demand. The admitted native `work.read` projection includes live offers for the bound Project Agent. The function performs no database or workspace I/O. REST, diagnostics, execution controls, MCP, native coordination, Attention, and Solo consume its offers.

The closed verbs are `start`, `hold`, `release`, `retry`, `send_back`, `approve`, `restart`, and `cancel`. Each offer carries meaningful parameters, allowed boolean values, authority, reason, label, cancellation propagation, and a pinned resumable execution. Required operator reasons and send-back guidance are supplied by the caller and retained in review records, comments, follow-up Tasks and transition logs. Commands check the version and select a current offer. A missing offer produces one `action_unavailable` error with current offers. Gate overrides remain owner-only. A role-launching offer (`start`, role `retry`, `release`) is made for one role, resolved in one place (`workflow::action_role`): the role of the Task's state, else the role of the state a claim would enter (the first outgoing Active state, else the first Gate). The Agent the offer needs, the role stored with the queued action, its replay and the claim all use that role, so `start` on a `todo` Task runs its coder, waits for capacity when the coder is busy, and is not offered while that Agent is paused or offline.

The stop fence uses the same effective assignment as execution admission:
a child's own coder, else its parent's coder; the step-check lead is the
root's assignment. Hold retains the offered Task-version check and the exact
assignment/state-entry fences; an inherited assignment does not manufacture
a version conflict after the execution has already been stopped.

A role retry, including merge repair, requires its current implementation Agent
assignment. A previous execution's Agent or an available default cannot stand
in for a removed assignment. A parked implementation failure names the missing
coder/worker assignment; assignment remains the ordinary role API and Cancel
remains available. Retry is offered again when an assigned Agent can take it.

Annotations record conditions and evidence, never an action allowlist. Old JSON `recovery_actions` keys are ignored, including unknown historical strings. Historical queued commands are translated at the stored-data boundary from current snapshot facts; their original payload is retained. A superseded or unrepresentable intent restores its condition for an explicit new command. No schema migration is required. Legacy annotation, blocked, failed and entry-barrier columns remain private dual-write storage until stage five.

Recovery commits a queued intent with the saved condition. The dispatcher consumes it through normal admission and waits quietly for capacity, a paused Project/Agent or a reachable workspace owner. Permanent refusals and malformed/stale intents remove the marker and atomically restore the condition with the error, fenced by Task version and marker identity. Fresh retries use the role prompt and review-bound admission; send-back continuations use the review-fix prompt. Restart applies its reset immediately and retains its restart/unblocked events. A shared in-process wake resumes the existing loop after command commits and terminal executions. Gate decisions still transition through the workflow engine; their worker dispatch is deferred and queued, preserving the worker thread on send-back. No new polling worker is introduced. Session launches remain separate Task-adjacent operations. `hold` parks workflow Task work. `release` is the owner's decision on the run the hold stopped: when no Agent can take the Task at that moment (the Agent or the Project is paused) the release clears the hold and marks that stopped run resumable (`resume_policy = auto`), so the dispatcher starts the role again as soon as the Agent is available instead of treating the stopped run as one that still needs a decision. Only that run is marked: the Task's latest non-interactive run, and only when the user stopped it (`cancelled` with `stop_reason = user_cancelled`). A run that failed or was stopped for any other reason (executor failure, timeout, stall, lost machine) keeps `resume_policy = manual` and its own recovery, and the mark is undone when the release loses its version race. Stopping a specific execution or side session uses the execution `/stop` resource, including when both run together.

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
queued behind a typed `dependencies` wait (a `dispatch_disposition` with the
capability `dependency_wait` naming the unfinished dependencies) until the
dependency finishes or its link is removed: while that disposition is current
the scheduler parks on it instead of replaying the action
(`Facts::dependency_wait`), and the dependency's completion or the link's
removal wakes the Task (`wake_task_dispatch`). A hold placed on a Task that
carries the `dependency_cancelled` blocker goes under it
(`hold_under_dependency_block`): the queued action is dropped as for any
hold, the blocker stays, and the hold replaces what the blocker carried in
`superseded`, so removing the dependency leaves the Task held. The displaced condition records the state it was taken in and
is restored only while the Task is still in that state. A queued action always offers Hold alongside cancel,
even when a newer condition appears. Restart follows the resolver's explicit set
of resettable condition kinds. Cancel wins over a settling plan
artifact: when the Task (or a subtask the root's cancel cascades to) holds a
`plan_publication_claim`, the cancel abandons it first (prior plan restored,
staged private files removed, claim released; when the restore fails it is
logged, the staged files are still removed and the claim is released) and
continues at the Task version that produced. A claim left by a transition that
already committed (its state is no longer the Task's) is cleared on the same
path. Both the cancel and the settle run through the Task's single writer, so
exactly one of them wins,
so an offered `cancel` is never refused with "settling a completed execution's
plan artifact". A coordination root's `retry` on an exhausted
review budget is review-only: it resets the budget and re-runs the aggregate
review where the root is, instead of resuming the root in a working state
where the aggregate-review guard refuses every role; a gate decision that
leaves a root in a working state queues no role for it (the scheduler advances
the root when its subtasks are complete). The same park offers `send_back`
(`root_review_reopen`): the budget is reset, the root takes its reject
transition to the working state and is held there, because an unheld root
whose subtasks are all finished is advanced straight back into the review
that failed. In the working state `ensure_parent_accepts_subtasks` admits a
corrective subtask, which waits on `parent` / `held` until the root's
`release`. The owner's manual pass of a failed review is review authority
for integration, for a root and for any other Task:
`review_integration_authority_in_tx` returns no contract (as for
a Task with no reviewer) when the latest passed review carries the
`manual_override` a user wrote, instead of demanding a passed reviewer
conformance the override by definition does not have. Pause refusals preserve the typed wait cause and
turn retry scope across REST, MCP, and native tools.

### Root Tasks and ordered subtasks

A subtask resting in a merge state leaves through that state's landed-merge
target. If that transition remains refused, reconciliation counts the durable
failed admission-command attempts in the same status epoch, including the
current attempt, and stops at the Task-step limit (eight). It uses the existing
`integration_admission_failed` / workspace-error park and owner recovery actions,
as root admission does. A restart preserves that count; a successful later
attempt finishes normally without a park. No merge attempt is created for a
subtask.

A root Task is a Task with no `parent_task_id`. Setting `parent_task_id` makes a
Task a direct child of that root and selects the root's shared workspace; it is
the hierarchy/workspace relationship, not a prerequisite edge. A root Task
with direct children is a coordination container, not a worker
execution. The bound Project Agent can create, sequence, assign, and observe
those children, but Forge never sends the root's implementation prompt to one
Agent as a proxy for all child work. A `coder` assignment on the root is the
default worker for children that have no own `coder`; converting a Task into a
coordination root keeps that assignment, keeps a planner that may lead the
root (below) and removes other non-review roles.
Two roles may execute on the root, each only in its own state: the lead in the
planning state, and the aggregate review role while the root is in its review
state. A
subtask whose root does not let it run
(`task_hierarchy::coordination_root_allows_child_dispatch` is false: the root
is held, parked on a failure or a blocked entry, or not in an implementation
(`Active`) state) says so: the dispatcher resolves `Reason::ParentWait` for it
(`task_dispatcher/next_step.rs`), which is one of the visible schedule parks
(`db::task_condition::readers::owner_park`), so the subtask's stored condition
is `parked` with the typed reason `parent { parent_id, cause }` (`held`,
`blocked`, `planning`, `not_coordinating`, `sequence`). `sequence` names an
unstarted child's wait for an earlier incomplete sibling, including one that
is parked with no live execution. The root's state and execution policy are
unchanged; the child's already-refused Start is no longer offered meanwhile.
Project pause has scheduler priority:
while the Project is paused, a child waiting on its parent shows
`project_paused` instead. Resume restores the parent wait if it still applies.
Every non-terminal child retains a typed park through either change. A
`project_paused` scheduler park has `state: null` and its exit is Resume Project,
only while that Project is actually paused: the condition reader ignores a
pause record whose Project has no `paused_at` (`Families::sql`,
`set_condition`), so a record a resumed Project left behind states nothing
and the invariant check repairs a condition that still shows it. A resume
removes the pause records of the Project's Tasks and restates their
conditions in its own commit (`wake_dispatch_for_project_in_tx`), one
condition write per Task that showed the pause, so the stored condition never
disagrees with the Project row. A Task whose
run or state entry is live keeps `running` / `entering` under a pause
(`ConditionFacts::apply`); the pause becomes its park when the run ends. It is restated from the scheduler
record, never carried by a check or integration statement. The condition write
compares against that record under the current Task epoch in the same
transaction; observer waits never invalidate an owned result. Thus: a check wait still owns its result
and a Project resume cannot retain the old pause park. It is a wait without a diagnostic or failure
kind, `start` is not offered on the subtask meanwhile, and the park is cleared
on the pass that finds the root coordinating again (a change of the root's
condition or status marks its children for the scheduler). A Task whose Agent cannot
take work is shown the same way: `Reason::AgentWait` is a visible schedule
park and the stored condition is `parked` on `agent { agent_id, status }`
(the Agent's effective status), cleared on the pass that finds the Agent
available. A retry (queued action or deadline) keeps its continuation while
it shows the Project or Agent park, and a queued action while it shows the
parent park; removing that park never loses the accepted retry. A retry
deadline under a parent or capacity wait stays `deferred`. A Task past its initial state whose Agent is at its run limit
resolves `Reason::AgentCapacity`, also a visible schedule park: its stored
condition is `parked` on `capacity { scope: agent }` and it is woken by the
Agent's wait record when a run ends. A Task still waiting for its first run
keeps the unstored `Reason::Capacity`. None of these visible waits sets an
interruption, failure or refusal flag, so `max_active_tasks` slot accounting
(`TaskRepo::count_project_slots`) counts the Task exactly as before. Releasing a held
root that already has subtasks launches no role for it (`release` takes the
`held_waiting` path): the hold is cleared and its subtasks run. The
root `coder` never executes there. The review role is whatever the workflow's
review gate declares, not the built-in `reviewer` name.
`services::task_hierarchy::RootRolePolicy` owns these assignment and execution
decisions.

**Root lifecycle (kickoff and the lead).** One rule for every coordination
root: its subtasks run only while it is in an implementation (`Active`) state
(`in_progress`, or `merge_failed` for a corrective subtask). Kickoff is a
level-triggered fact, not an event: `RootRolePolicy::root_advance_due` is true
for an unblocked root that has not reached implementation and has a subtask
left to run, `next_step` resolves `Step::AdvanceRoot` for it, and
`TaskService::advance_coordination_root` walks it forward. The same walk
serves completion (every subtask terminal: on to aggregate review).

| Root state | Condition | Result | Mechanism |
|---|---|---|---|
| initial or a gate before implementation | a subtask is left to run; no lead | `in_progress` | `Step::AdvanceRoot`, kickoff |
| initial | a subtask is left to run, or all are terminal; lead assigned | `planning` (the lead is dispatched by the state's entry) | `Step::AdvanceRoot`; the walk stops where the lead holds the state |
| `planning` | the lead's execution succeeded | `in_progress` | the planning gate's Accept; then `Step::AdvanceRoot` goes on to review when every subtask is already terminal |
| `planning`, lead assigned | planner assigned, nothing running | planner execution | `Step::Role` (never `AdvanceRoot`: the fact is false while the lead holds the state, whatever completion flag is left) |
| `in_progress` | subtasks running | parked on `children` | unchanged |
| `in_progress` | every subtask terminal | `review` | `Step::AdvanceRoot`, completion |
| `in_progress`, lead assigned | the first incomplete subtask is parked needing a decision, and the root has not re-planned for that park | `planning`, bridge `replan`; charges `replan` | `Step::ReplanRoot` → `TaskService::replan_coordination_root` |
| `in_progress`, lead assigned | the same, with the `replan` budget spent | stays; parked `budget_exhausted { budget_kind: replan }` | the same command writes the block |
| `review`, lead assigned | the aggregate review was rejected and the Review budget allows a retry | `planning`, bridge `replan`, `rejection`; charges `review` (as before) and `replan` | `review_failure_target` through `task_hierarchy::effective_reject_target` |
| `review`, lead assigned | the same, with the `replan` budget spent | stays in `review`; parked `budget_exhausted { budget_kind: replan }` | `review_failure_target` |
| `review`, no lead | the aggregate review was rejected | the gate's `reject_target`, as before | unchanged |
| `planning`, lead assigned | the lead asked the owner a question (`lead.request_input`) and its run ended | stays; parked `human_decision { boundary: lead_input }` until the owner answers, then the lead runs again; no `replan` charge | `next_step` `human_wait`; the answer clears the wait |

**Re-plan.** A coordination root with a lead goes back to its planning state
for two triggers, and its lead runs again there with the trigger's evidence in
its input (`planner.coordination.v1`: the review findings and failed checks,
or which subtask parked and why). Both are read from stored state on every
pass; no wake is needed.

- *Rejected aggregate review.* `task_hierarchy::effective_reject_target` is
  the reader of a gate's `reject_target` for an automatic rejection: the
  configured target, except that a coordination root with a lead never goes
  from a review-phase gate back to its implementation state (no role runs for
  it there) and goes to its planning state instead. The Review budget is
  resolved first and is untouched: a spent Review budget blocks the root in
  `review` with `review_budget_exhausted` exactly as without a lead. If the
  lead adds no fix work the root goes `planning → in_progress → review` again
  and the next rejection spends the Review budget, which bounds the loop.
  Owner actions (`root_review_reopen`, an owner's own send-back or retry)
  keep the workflow's configured target.
- *Parked subtask.* `RootRolePolicy::replan_due` is the fact (`Facts::
  root_replan`), evaluated beside `root_advance` in `next_step`, after the
  Project pause, the integration wait, the root's own blocker and a human
  wait: an unblocked root with a lead, in its working implementation state
  (never `merge_failed`), whose first incomplete subtask is parked needing a
  decision. "Needing a decision" (`task_hierarchy::subtask_decision_park`) is
  an allow-list, judged on the park's own reason: a failure of the work
  (`retry_exhausted`, `executor_failed`, `max_turns_exceeded`, `ci_failed`,
  `review_gate_failed`, `review_budget_exhausted`, `workflow_guard_rejected`,
  `dependency_cancelled`), an Agent timeout, a spent attempt or gate budget, a
  blocked entry, or a cancelled dependency. Everything else is not the
  lead's and spends no re-plan: the owner's hold or manual stop; a failure of
  the host, the repository, the workspace or the platform
  (`environment_not_ready`, `workspace_*`, `before_work_hook_*`,
  `executor_unavailable`, `dispatch_failed`, `merge_conflict`,
  `target_repo_dirty`, `dirty_worktree`, `internal_command_failed`,
  `recovery_required`, `review_blocked`, `review_needs_owner`, a kind this
  build does not know); a queued owner action; a human decision; any wait
  that ends on its own (capacity, an Agent, a machine, placement, the
  environment, a dependency, checks, the parent, a retry deadline, a run in
  flight); and a park that records no time (it has no identity to dedupe on;
  the blocker writers in `services` all record one, so in practice this is a
  row written by an older build or by hand). Those stay the Project
  Agent's and the owner's, and the root waits on its subtasks as a root
  without a lead does. The subtasks are sequential, so nothing runs beside
  the parked one, and the subtasks that have not started are held by the
  planning root (`parent { cause: planning }`). A re-plan never starts beside
  running work: while the parked subtask itself still has a live run (a stop
  that has not landed), neither the dispatcher's step nor the command acts
  (`task_hierarchy::replan_due_now`), and the root re-plans once that run has
  ended.

The transition into planning has no workflow trigger. The engine and the
cascade step admit it only under the `replan` bridge, only for a system actor
and only into the workflow's planning state (`task_hierarchy::replan_edge`);
the source state's `before_exit` hooks do not run (they are the conditions
of going forward: `subtask_sequence_complete` and
`require_plan_checklist_complete` on the built-in `in_progress`, none on
`review`), while its `on_exit` hooks and the planning state's entry hooks run
as on any transition. Nothing else is skipped: the rejected Review is already
settled, the Review budget is charged by the same rejection transition as
without a lead, and a root in `in_progress` or `review` owns no merge-queue
entry. A custom workflow that puts a side effect in a `before_exit` hook of
its implementation or review state does not get it on a re-plan.

Bounds. One re-plan per trigger: the receipt (`replan:review:<review id>`,
`replan:<subtask id>:<time the park was recorded>`) is the budget charge
identity and is carried by the planning entry's bridge payload. The fact reads
the entry, the charge reads the receipt, so a crash between the two re-issues
the step, charges nothing and enters planning once. A park the lead left in
place is never re-planned again: the root waits on its subtasks (`children`)
and the Project Agent is woken for that subtask; a later park of the same
subtask is a new trigger. Across all triggers the `replan` budget
(`db::budget::Kind::Replan`, 5 per budget window) is the machine bound. The
limit is fixed: `retry_budgets` does not configure it, in Project settings,
workflow state config or Task config. When it is spent the root parks where it is with
`budget_exhausted { budget_kind: replan }` (blocked `retry_exhausted`, source
`replan`), which raises Attention and wakes the Project Agent like any root
blocker; the owner's Retry with a new budget clears the park and resets the
window, and the root's own next step follows (re-plan, or the aggregate review
again). The root's own block holds its subtasks (`parent { cause: blocked }`),
so acting on the subtask alone does not release it; the Retry on the root
does. A re-plan refused deterministically is recorded as a dispatch
disposition (`coordination_root_replan`): the root shows the typed
`dispatch_refusal` park and the sweep reads it again whenever the root's row
changes.

Composed worst case for one root in one budget window, with nobody acting on
it (no owner, no Project Agent), at the default limits: the lead plans at most
6 times (kickoff plus 5 re-plans; up to 3 more runs if its own runs fail, the
root's Execution budget), and creates at most 24 subtasks beside the `n` it
was given. One subtask in one of its own budget windows costs at most 9 runs
(3 submissions, i.e. the first and 2 step-check returns, 3 failed worker runs
under its Execution budget, 3 step checks). A lead recovery is a Retry with a
new budget for that subtask, at most 2 per subtask, so the hard bound is 27
runs per subtask and `27 × (n + 24) + 9 + 3` runs for the root (3 = the
aggregate review's entries); for `n = 5` that is 795. Subtasks run in order
and a re-plan is triggered by one parked subtask, so at most 5 recoveries can
happen in the window and the reachable bound is `9 × (n + 24) + 5 × 9 + 12`:
318 runs for `n = 5`. Every term ends in a typed park with an Attention item
and an action: `budget_exhausted { replan }` on the root (Retry with a new
budget), `retry_exhausted` on the subtask whose recoveries are spent (the root
waits on `children`; the Project Agent is woken for that subtask),
`review_budget_exhausted` on the root. A question to the owner
(`lead.request_input`) adds one lead run per answer and needs a person each
time. A Retry with a new budget on the root (owner or Project Agent) opens the
next window; the Project Agent's own wake budgets bound how often it does.

**Lead operations.** The lead changes the breakdown with five registered
operations (`operation-registry::task_lead`, on the Task propose aggregate
`forge_scope_propose`). They are native Task-scope operations; MCP and REST
have none of them.

| Operation | What it does | Target | Refused when |
|---|---|---|---|
| `lead.read` | the root's subtasks (id, version, order, state, condition, started, hand-off notes); with `task_id`, one subtask's description, last runs, comments and the recovery actions offered to the lead | the root or a subtask | — |
| `lead.subtask` `create` | appends one subtask (the server fills the parent) | the root | the root no longer accepts subtasks (`SUBTASK_PARENT_CLOSED`); a creation bound is spent |
| `lead.subtask` `update` / `cancel` | title or description; cancel through the Task action | a subtask that never had an implementation run | the subtask started (`subtask_started`); stale `version` |
| `lead.subtask` `reorder` | the untouched `todo` suffix, through the sequence command | all subtasks of the root | the order moves the terminal prefix or the current subtask |
| `lead.subtask` `assign` | the worker (`coder`) of a subtask, or with no `task_id` the root's default worker | the root or a subtask | the Agent is not usable in the Project; the Agent is the lead itself (`lead_self_assignment`, also for `create` with an assignee) |
| `lead.recover` | one Task action the action system offers the lead (`retry`, `restart`, `cancel`) at the subtask's version | a subtask | the action is not offered to the lead now (`action_not_offered`); the recovery bound is spent |
| `lead.handoff` | a `handoff` comment; the subtask's next implementation prompt starts with the newest five | a subtask | more than 20 notes on the subtask |
| `lead.request_input` | asks the owner one question (below) | the root | the root already waits (`lead_input_pending`) |

Not granted: repository writes, merge, a review decision, cancelling or
moving the root, milestones, the Charter, releases, dependencies, and making
itself a worker. A subtask the lead creates has a title, a description and
optionally a worker: no workflow, review configuration, check command or hook
is an input of any lead operation. A step check has none of the operations:
its run returns a verdict (its result block) and changes nothing, and a step
it cannot pass reaches the lead's planning run through the re-plan.

Authority is decided in two places and nowhere else.

1. *Who is a lead* is a fact of the principal. The authority loader
   (`db::effective_authority`) derives `Principal::TaskAgent::lead_root_id`
   from the session's one running execution: its role is the step-check role,
   it runs on a root that has subtasks, and the root's own row of that role
   names this identity. A step check (the same Agent and role on a subtask in
   `lead_check`) and a planner on a plain Task lead nothing.
   `PrincipalRule::TaskLead` admits the five operations on that fact plus the
   Task scope's `task_read`; no permission name was added. Advertisement uses
   the same evaluator, so an ordinary planner, worker or reviewer session is
   never offered a `lead.*` operation and pays nothing for them in its tool
   prefix. Within a turn the fact can be lost and never gained
   (`EffectiveAuthority::narrowed`): a lead removed or replaced mid-run loses its execution and all Task-run
   operations at its next call.
2. *Is the target in the subtree* is one predicate at one boundary.
   `TaskService::bind_lead` is called first by every lead command: it repeats
   the workflow's side of the lead rule (`RootRolePolicy::lead`, the run on
   the root, the root in its planning state, the execution still running
   under the lead's role) and resolves the target through
   `task_hierarchy::ensure_in_lead_subtree` (the root or one of its direct
   subtasks, same Project). A handler obtains its target only from that call.
   A Task of another root, of another Project, a deleted or unknown id all
   answer the same refusal (`outside_lead_subtree`) and nothing is written.

Refusals are typed (`ServiceError::LeadOperationRefused`, reason codes
`outside_lead_subtree`, `not_leading`, `wrong_target`, `subtask_started`,
`lead_limit_reached`, `action_not_offered`, `lead_input_pending`,
`lead_self_assignment`) and reach
the lead as an ordinary tool result with the reason in `details.refusal`.

The effects are the existing commands. Create and reorder are the adaptive
Task command (`split`, `sequence`) with the lead as its principal, recorded as
principal type `task_lead` on the command receipt and its `task.adaptive`
event; the database repeats the lead check under the writer transaction for
that principal (`reauthorize_task_lead_command_in_tx`: the receipt's
correlation is the lead's running execution on the command's source root),
because the Project Agent's binding check does not apply to it. The receipt's
idempotency key is the call's `dedupe_key` scoped to the root and the lead, so
a repeated call returns the first subtask. Update is `update_task` plus one
`decision` comment that names the lead's execution. Cancel and recover are
Task actions performed as the lead's Agent: `ActionAuthority::Lead` is in the
audience of the offers the lead may take (`task_hierarchy::lead_verbs`:
`retry`, `restart`, `cancel` while the subtask is parked on something the lead
decides, `subtask_decision_park`, the allow-list the re-plan uses; `cancel`
while it never started), and a caller that is only the lead is offered nothing
else. "Started" is any run on the subtask other than a step check, whatever
the Project's workflow calls its implementation role. Assign is
`reassign_role`, the command REST's role assignment uses, after the same
Project usability check: the lead's assignment passes exactly the admission
an owner's assignment passes, and the lead may name any Agent usable in the
Project except itself.

Every operation carries the envelope's `dedupe_key`. Version checks are the
subtask's `version` (update, cancel, recover) and the root's version and the
board revision inside the adaptive command (create, reorder).

Bounds on what a lead can repeat are rows on the ROOT in the budget ledger
(`db::budget::LEAD_*`), in the root's budget window, so nothing the lead does
to a subtask (a retry with a new budget included) resets them; the owner's or
the Project Agent's Retry with a new budget on the root opens the next window.

| What | Bound | When it is spent |
|---|---|---|
| subtasks created in one lead run | 8 (`lead_subtasks_run:<execution>`) | `lead_limit_reached`; the run can do everything else |
| subtasks created under one root | 24 per budget window (`lead_subtasks`) | the same |
| recoveries of one subtask | 2 per budget window (`lead_recover:<subtask>`) | the same; the subtask stays parked, the root returns to `in_progress`, waits on its subtasks and the Project Agent is woken for the park (one re-plan per park) |
| hand-off notes on one subtask | 20 | the same |
| open questions per root | 1 | `lead_input_pending` |
| lead runs | one at kickoff, one per re-plan (5), one per answered question; a step check is one more run of the lead's Agent per submitted step (at most 3 per subtask and Execution window: `gate:lead_check` is 2 rejections), without lead operations | the existing budgets |

A refused call spends nothing. A charge and the command it guards are not one
transaction: a crash between them spends a unit and creates nothing, and the
retry of the same key is not charged again.

A lead's Retry opens a fresh budget window on the subtask, including
`gate:lead_check`; the root's `lead_recover:<subtask>` charge stays in the
root's window. A queued role Retry that still carries gate spending records
one `retry_window_reset` before validation, identified by its queue ID, and
only when the actor that accepted it is the root's lead (the offer names the
lead for every caller, so the offer alone does not say who accepted it).
Redelivery sees that reset and never opens another window.

A recovery the lead accepts while its root plans is not replayed against the
order check: the subtask keeps the typed wait `parent { cause: planning }`
until the root is back in its implementation state, and the accepted action
runs then (`next_step`: a queued recovery behind a planning root). So after a
parked subtask the lead retries it, or cancels it and adds what replaces it,
its run ends, the root returns to `in_progress` and the sequence goes on; after
a rejected review the lead adds fix subtasks, and the root returns to
`in_progress` and to review only when they are done. A lead that changes
nothing leaves the bounded no-op of the re-plan: the root waits on its
subtasks and the Project Agent is woken for the park, or the root returns to
review and the next rejection spends the Review budget.

**The lead's question (`lead.request_input`).** The question is a
`request_input` comment on the root with the lead's execution as provenance,
one Attention item for the owner (`human_input_required`, recommended action
`answer_lead_input`, dedupe key `lead-input:<root>:<comment>`; it never wakes
an Agent), and a wait on the root: `awaiting_human` with reason `lead_input`,
which the condition reads as `human_decision { boundary: lead_input }`. The
three are one transaction (`SqliteDb::open_lead_input_wait_in_tx` inside the
transaction of the comment and the item): a question is never shown without
the root waiting for its answer, the root never waits on a question nobody is
shown, and of two questions sent at once one is asked and the other is
refused without leaving a comment. A repeated call with the same key writes
nothing. An item whose root was deleted is closed by resolving it.

| Root | Condition | Result | Mechanism |
|---|---|---|---|
| `planning`, the lead's run asked | the run ends, successfully, failed or cancelled | stays in `planning`; the run is not the gate's Accept and is not settled again or treated as a manual stop after the answer | `maybe_cascade_executor_completion` (`lead_asked_input`); the snapshot leaves that run out of `terminal_execution` |
| `planning`, waiting | every pass, a restart | parked `human_decision { lead_input }`, owner the user; no lead run; subtasks `parent { cause: planning }` | `next_step` `human_wait` |
| `planning`, waiting | the owner resolves the item with an answer (`POST …/attention/{id}/resolve` with `answer`) | the answer is a user comment on the root, the wait is cleared, the item resolved, the root woken; the lead runs again with its questions and the owner's answers in its input | `AttentionService::resolve_with_answer` → `lead::answer_lead_input`, then `Step::Role` |
| `planning`, waiting, asking run live or ended | the owner holds the root, then releases it | a live asking run stops; release clears the hold and keeps the question wait. The lead runs once after the answer, not at release | the Release action uses `release_to_dispatch_queue` for a pending lead-input wait |
| any | the root leaves `planning` another way (Approve, move, cancel) | the wait is cleared and the item resolved | `lead::clear_lead_input_wait` in the transition |

An answer is no transition, so it charges no re-plan. The loop "ask, answer,
ask again" needs a person for every round and one question is open at a time.
Resolving the item without an answer is refused; an answer on any other item
is refused.

Project Agent wakes. Project-scope blocker wakes are admitted in one place,
the blocker sweep (the event path defers them to it), and the sweep has one
rule (`task_hierarchy::subtask_park_is_the_leads`): an open blocker of a subtask
wakes nobody while its root's lead handles it, that is while the root is about
to re-plan for that park or its lead is planning. It is level state, not a
suppression record: once the lead left the park in place, the root itself
parked (its `replan` budget included) or lost its lead, the next sweep admits
the wake. The rule never guesses: a subtask whose root is gone is the
Project Agent's, and a read that fails for any other reason holds the wake
for that sweep instead of waking the Project Agent beside a lead that may be
handling the park. Blockers of the root itself always wake the Project Agent.
The Attention item of the subtask is raised either way; only the wake is
filtered.

The **lead** is the Agent assigned to the workflow's planning role on the
root's own row. The planning state is found by position
(`task_hierarchy::planning_state`: the first gate with a role, outside the
review phase, on the path from an initial state to the first `Active` state),
and `RootRolePolicy::lead` / `task_hierarchy::root_lead` is the one resolver
for "does this root have a lead": the kickoff fact, the walk and the Task
response read it. A lead must be a native Agent and, in this version, the
planning role must be the built-in `planner` (the native host maps Task roles
by name); assignment is refused otherwise (`LEAD_REQUIRES_NATIVE_PROFILE`,
`LEAD_ROLE_NOT_SUPPORTED`), and conversion into a coordination root strips a
planner that may not lead. A root on a daemon-owned placement accepts the
assignment, but its planner cannot be placed there (daemon placement runs CLI
Agents only): the dispatch parks on the placement refusal. While the root
plans, its subtasks are parked `parent { cause: planning }` (owner: the
running lead; no action is offered). The lead plans through the breakdown:
its prompt is `planner.coordination.v1` (doctrine in the stable part, the
breakdown with states as user-role input), and the checklist plan artifact a
planner otherwise owes is not required of it. A root without a lead never
enters `planning` to stay: nothing changes for it except that it moves to
`in_progress` when it has a subtask to run instead of resting in `todo`.
A subtask started by hand (claim, move, manual transition) while its root's
kickoff is still due starts the root first
(`TaskService::start_coordination_root_of`; every actor but Forge's own
components). The start is the dispatcher's own next move made early, under the
system actor: it gives the caller no authority over the root, a root that
holds its subtasks (blocked, paused Project, planning for its lead, an
unfinished dependency) still refuses the subtask through the caller's order
check, and no subtask ever runs under a root that is not implementing.

Edges of the root lifecycle, each a typed wait with an exit:

| Situation | Root | Its subtasks | Exit |
|---|---|---|---|
| The root has an unfinished dependency | stays where it is, parked `dispatch_refusal { capability: coordination_root_advance }` | `parent { cause: not_coordinating }` | the dependency's completion wakes the root (`wake_dependents_of_completed_task`), which starts on the next pass; a cancelled dependency blocks it as it blocks any Task |
| The lead is removed or replaced while its planner runs | the old planning run is cancelled (`role_reassigned`); the removed lead's later calls lose authority | `parent { cause: planning }` until planning ends | removal takes the no-lead advance to `in_progress`; replacement starts the new lead once in the same planning state |
| The planner fails, is stopped, or its Agent is paused | `planning`, with the retry deadline, the failure, the hold or the Agent wait any Task shows | `parent { cause: planning }`, or `blocked` / `held` once the root records it | failed planners use the same Execution retry budget as coders; a paused Agent holds the retry until resume; exhausted retries require the owner's Retry; a manual stop requires Release |
| The planning gate requires a person's approval and the root has a lead | `planning`, `human_decision { boundary: plan_review }` after the planner succeeded | `parent { cause: planning }` | Approve (or Send back) on the root |
| The same gate, no lead | never stops there: the walk passes the gate under the system actor | run | — |

`RootRolePolicy::runs_role` is the reader for "does this role run on this root
now": the workflow's rule (the planning role in the planning state, the
aggregate review role in review), and in the planning state only while a lead
is assigned. Every site uses it (`task_hierarchy::root_runs_role` is its
database form): the action offers, the release continuation, the dispatcher
snapshot, the dispatch hook, execution admission and the active recovery, so a
planning root without a lead offers, launches and admits no planner. The plan-candidate exemption applies to exactly the lead's run (the
planning role, on a root with subtasks, in the planning state).

**Step check (`lead_check`).** The inherited subtask workflow is
`todo → in_progress → lead_check → done` (`cancelled` from any of them).
`lead_check` is an optional gate in the working phase, never a review: no
`Review` row, no check run, no Review budget and no integration evidence exist
for it, and it changes nothing about subtask creation, the aggregate review
role or merge admission. Its role (`planner`) is held by the root's lead and
by nothing else: a subtask in an inherited workflow state never reads that
role from its own rows, and no row is copied onto it.

| Subtask state | Condition | Result | Mechanism | Budget |
|---|---|---|---|---|
| `in_progress` | the implementation run succeeded | `lead_check` | the state's Accept | — |
| `lead_check` | the root has no lead | `done`, bridge `gate_skipped`; no execution, no person, no park, no Attention item | `auto_cascade_on_unassigned_role`, an on-enter hook of the gate: the entry's hook step enqueues the skip as a `cascade` step of the same chain, so it runs as soon as the entry's hooks end and waits for no dispatcher pass and no sweep interval | — |
| `lead_check` | the root has a lead | a `planner` execution on the subtask, under the lead's Agent, in the shared root workspace (prompt `planner.step_check.v1`) | `dispatch_role_agent` on the entry | — |
| `lead_check` | the lead's result block says `pass` | `done` | the gate's Accept (`TaskService::settle_step_check`) | — |
| `lead_check` | the result block says `fail` | `in_progress`, bridge `gate_rejected`, rejection; the same worker thread is resumed (`ResumeLatestTargetRoleThread`) with the lead's findings | the gate's Reject | charges `gate:lead_check` (limit: the gate's `max_rejections`, 2) |
| `lead_check` | entry with `gate:lead_check` spent | stays; `parked` with failure `retry_exhausted` and an Attention item; no check is started | `check_retry_budget`, before the dispatch on the same entry | — |
| `lead_check` | the run ended without a usable verdict (no result block, or `blocked`) | the same session is asked again for the block; when that budget is spent or there is no session, `parked` with failure `workflow_guard_rejected` (guard `step_check_result`) | the completion-guard path every completed run shares | `workflow_guard` (limit: the execution limit, 3) |
| `lead_check` | the run failed or timed out | the retry, deadline or failure any execution shows | execution retry | `execution` |
| `lead_check`, nothing running | the root has no lead any more (removed after the entry; a restart between the entry and its skip) | `done`, bridge `gate_skipped` | `Step::SkipGate`, resolved from stored state by `next_step` (fact `gate_skip`); removing or changing the lead marks the subtasks that rest in the gate | — |
| `lead_check`, nothing running | the root has a lead (a restart between the entry and the run) | the step check | `Step::Role` | — |
| `lead_check`, a check running | the lead is removed, or the Agent that is the lead is archived | the run is stopped; then `done`, bridge `gate_skipped`, in the next dispatcher pass | `TaskService::wake_step_checks_of` (from role removal and, for an archived Agent, `TaskService::lead_removed_from` after the archive sweep), then `Step::SkipGate` | — |
| `lead_check`, nothing running | the lead's Agent is paused (or cannot be reached) | stays; `parked` with `agent { agent_id, status }` naming the lead's Agent; the check starts when the Agent is resumed | `next_step` (`Reason::AgentWait`), as for a planning root whose planner is paused | — |
| `lead_check`, admitted check has not launched | the lead's Agent is paused between admission and launch | the unlaunched attempt ends auto-resumable; the subtask parks on the Agent and resume starts a fresh check; while the Agent stays paused nothing is launched again | `TaskService::defer_launch_for_paused_agent`; no persistent setup refusal | — |
| `lead_check`, a check running | the lead's Agent is paused | the run goes on and its verdict settles the step (a pause starts nothing and stops nothing) | — | — |

The admission-to-launch pause is handled the same way for every role of
every Task (a coder on a plain Task as well): the run ends `failed` with
`resume_policy = auto` and an error that starts with `Agent paused before
launch: `. That prefix keeps it out of execution-failure handling: no retry is
spent, no retry delay is taken and the Task is not blocked. The dispatcher's
next pass parks the Task on the Agent (`parked`, `agent { agent_id, status:
paused }`), and the resume starts a fresh run. An Agent that was archived
instead is not deferred: the run fails as a setup refusal and the archive
sweep settles its roles.

"No dispatcher decision" in the no-lead row means the skip does not depend on
one, not that the dispatcher never looks: the subtask is in `lead_check` for
the time between two queue steps (the entry and its cascade), and a dispatcher
pass that reads it in that window resolves the same skip from stored state
(`Step::SkipGate`, the `gate_skip` fact, which is also the recovery when the
process dies in that window). Both carry the Task version they read, so
exactly one of them makes the transition (`gate_skipped`, once in the
transition log) and the other is dropped as a version conflict; the subtask
needs no further pass either way. What a client sees per finished subtask is
two transitions and their events (`in_progress -> lead_check`,
`lead_check -> done`), never a rest in `lead_check`.
`without_a_lead_the_skip_rides_the_entry_s_own_chain` pins the hook path (the
`cascade` step shares the chain of the entry's `hooks` step): removing the
hook and keeping only the dispatcher's `SkipGate` fails it.

The one loop this adds is `lead_check ↔ in_progress`. Its machine bound is the
gate's `max_rejections`: two returns, then the third entry parks the subtask
(the owner's `retry` opens a new budget window; the parked subtask also stops
the ordered sequence, as any blocked subtask does). The next sibling starts
only when the subtask is `done`, so no subtask ever builds on a step the lead
has not accepted. A verdict is the run's result block, read by the reviewer's
result-block parser (`review::contract::parse_assessment`: the last JSON
object that names a result wins; `fixable_by` and `repeat` mean nothing here).
The verdict is recorded once per run as a comment on the subtask and as the
reason of the transition; the worker's next prompt carries the findings from
that rejection.

Retry on the spent-budget park opens a new budget window and re-enters the
gate, so the lead checks the rework that is already in the workspace; the
worker is not run again before anybody looked (validate first). The budget is
checked once per entry (`check_retry_budget` on entry, before the dispatch; the
auto-attached after-enter check is not added to a gate that already checks on
entry). A subtask never enters a merge state (the inherited workflow has
none), so merge-queue returns of the root never spend this budget.

A subtask resting in `lead_check` with the gate's ledger spent is offered
gate-budget recovery even though the gate's park is encoded as
`retry_exhausted` (execution-budget recovery does not reset that ledger; no
other gate's offer changes). Retry resets the budget and creates a new gate
entry; the role launch that continues it is an accepted action like any
other.

One rule covers every accepted action on a subtask whose hierarchy does not
admit it, whatever the cause (the root plans, is held, is blocked or has
not started; an earlier sibling is unfinished) and whoever accepted it (the
owner's Retry, the lead's `lead.recover`, the Project Agent's recovery, a
start by hand). An action that needs the hierarchy's admission is not
replayed while the subtask is not next in its root's order: `next_step`
parks the subtask on its parent (`parent { cause }`, or the children park
behind an earlier sibling) and the intent stays queued. A role launch
always needs admission. Any other action needs it once the hierarchy has
refused its replay (a move to a working state): the refusal is recorded on
the intent (`queued_recovery.awaits_hierarchy`) instead of restoring the old
park, so the next pass shows the parent wait. An action that needs no
admission (a retry of entry checks, a decision that ends the subtask) is
replayed whatever the root does. Because the root and child have separate
writers, admission may lose after an eligibility read: the hierarchy's
refusal, and a role launch's database authority conflict, keep the accepted
intent. The hierarchy's refusals remain `invalid operation` to every
caller. The root's status change marks its children for scheduling, so the
action runs when the root releases them.

An owner's manual move out of `lead_check`, a send-back to implementation
included, charges no `gate:lead_check` return (`db::budget::transition`:
an owner's move spends nothing).

Removing the root's lead stops a step check that is still running
(`TaskService::wake_step_checks_of`) and the subtask passes the gate in the
next dispatcher pass. Archiving the Agent that is the lead is the same lead
change: the archive sweep (`on_agent_deleted_in_tx`) clears the row as a
queued step of the root, and after its commit every caller (the archive
route, `TaskService::on_agent_deleted`, the removed-machine settlement) runs
`TaskService::lead_removed_from`, which waits for that clear and then calls
the same hook. A paused lead is not a missing lead: the step is not passed
unchecked. With no check running the subtask is parked on the Agent by name
until it is resumed; a check that is already running finishes and settles. A verdict of a run that no longer owns the role attempt
(lead removed or replaced, subtask held, cancelled or moved) is dropped
without a transition (`execution_owns_current_role_attempt`). A replaced
lead's running check is not stopped: the subtask waits for that run to end
(bounded by the execution timeout), then the new lead checks it. Hold on a
subtask in the gate stops the check; Release has the lead check it again.
Cancelling the subtask or its root ends the check. A subtask in the gate holds
the one Project slot it held during implementation (the check is that Task's
run); a parked one holds none. The lead's Agent is admitted by the same Agent
and machine limits as any run.

The step-check prompt carries the step's diff as per-run input (the user
part of `planner.step_check.v1`; the system part stays the stable doctrine and
result contract). `workflow::dispatch::loader::load_step_diff` reads it where
`AgentDispatchContext` is built, through the workspace backend's `diff`
(embedded and daemon-owned workspaces alike; no protocol change): the shared
root workspace, uncommitted leftovers included, against the commit the step
started from. That commit is the head recorded when the last run of an
earlier sibling ended (every step check records it, because the host runs it
read-only; a native worker records it too) and the root workspace's branch
point when none is recorded (the first step; or earlier steps run by a CLI
worker under a root that had no lead then: the prompt says that changes of
earlier steps are included). The diff is cut at 32 KiB (`STEP_DIFF_LIMIT`)
with the cut marked; a diff that cannot be read is reported in the prompt in
one line and never fails the dispatch.

The step check is read-only by the host, not by its prompt: the runner marks
every `planner` run read-only (`read_only_execution_role`), a native run gets
the `TaskRead` scope (no write and no command tools), and for any executor the
runner restores the worktree to the head it started from and fails the run
(`read-only execution produced worktree changes`) when it wrote.

The step-check role cannot be assigned on a subtask
(`422 STEP_CHECK_ROLE_HELD_BY_ROOT`), and Project default role assignments
do not copy it onto one (`TaskService::project_default_role_assignments`
leaves out every role `db::inherited_subtask::step_check_holder` gives to the
root; the only path that gives a subtask default roles is the dispatcher's
`Step::ApplyDefaults`). Rows that predate the rule hold nothing and are left
out of the action snapshot; they are not migrated away (they can be removed
by hand, and nothing reads them).

The names the database needs of this workflow (its states, the step-check
state and role) live in `db::inherited_subtask`; the services read them from
there and `the_database_names_are_this_workflow` fails when the workflow
definition and those names diverge.

One resolver answers "who holds this role on this Task"
(`task_hierarchy::effective_role_assignment`): `coder` falls back from the
subtask's row to its root's, a subtask's step-check role is its root's lead
(`RootRolePolicy::lead` under the Project workflow, which requires an Agent),
and every other role is the Task's own row. The dispatcher snapshot
(`snapshot::assignment`, over the rows `db::schedule_reads` loads: the Task's
own and its root's `coder` and `planner`), the workflow hooks, the action
snapshot and the completion ownership check call it; the database repeats the
same rule where it re-checks an admission in the writer's transaction
(`effective_execution_assignment_in_tx`, `assignment_holder_in_tx`). An
optional gate nobody holds is skipped to its Accept target when that is an
implementation state or a terminal state other than cancellation
(`workflow::optional_gate_skip_target`); a gate whose Reject returns to
implementation is passed, never bounced.

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
workers cannot bypass serialization. Dependency cycle admission includes the implicit waits of the hierarchy:
later siblings wait for earlier incomplete siblings, children need their parent
to start coordinating, and a parent completes only after its children. The
validator distinguishes starting from completing so the hierarchy itself is
acyclic while a parent prerequisite on its own unfinished child is refused. A prerequisite that closes that combined graph is
refused with the existing cycle error, including a path through an unrelated
Task. Reordering validates the resulting graph in the same writer transaction
and rolls the entire order back if it creates a cycle. Terminal Tasks contribute
no waits. For a cyclic link stored by an older build, removing the prerequisite
through the dependency command preserves the Tasks and wakes the accepted retry;
the parent order remains authoritative.

Reordering is transactional as well: the
terminal prefix and current first incomplete child remain fixed, and only the
untouched `todo` suffix may be permuted.

The first execution in the tree creates the root workspace on demand: the
lead's kickoff run when the root has one, otherwise the first runnable child.
Completing a
child wakes the next ordered sibling. Once every child is `done` or
`cancelled`, Forge advances the coordination root to its aggregate review
gate; review and merge remain root-level operations, but they bind CI and
integration to the latest relevant child implementation execution and the
root-owned shared workspace. The root may receive a planner execution in its
planning state and a reviewer execution for that aggregate gate, but it never
receives a root implementation execution.
If the integration target moves after aggregate review and Forge can rebase the
shared worktree cleanly, a durable review-refresh transition returns the root
directly to aggregate review without notifying or dispatching a merge-fix Worker
and without spending merge-fix budget. Crash recovery recognizes the same marker
and completes an interrupted refresh. A conflicting rebase cannot dispatch work
on the non-executing root, so Forge leaves the root visibly blocked with a manual
workspace-repair action that returns the repaired branch through aggregate
review before retrying integration. That action explicitly hands the attempt's
repair statement back to the workflow, retaining its lineage. The owner can
also `send_back` (`root_review_reopen`) from either the merge gate or a root
already parked on `integration/repair` in review: the root takes its reject
transition and is held in implementation so a corrective subtask can be
added. Release runs the child and then fresh aggregate review. No root coder
is launched, no new park kind is used, and this owner decision spends no
automatic re-plan budget. Execution admission follows the same Active-state
rule as scheduling: `merge_failed` permits corrective children even though
it belongs to the review phase; review-phase Gates do not. A failed or
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
| Subtask | In an inherited state the Project workflow does not have (`lead_check` for the built-in workflow; also `todo`/`in_progress` under a workflow that names its states differently, such as the autonomous one) | any | Inherited subtask workflow |
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
the dispatcher consumes the marker and retries the exact `review`
capability without creating another reviewer execution. A Task in `merging`
carries no marker: the queue worker parks it while the Project is paused and
re-queues it on resume.

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
2. Run filtered `A.before_exit` guards. Deterministic blocking failures reject before commit. Transient database,
   version-race and checkout I/O failures stay typed and return to the step
   queue, which retries at most eight attempts before an owner park.
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
startup clears claims and releases leases left by the previous process before
any recovery or worker starts. A claim held by the starting process itself (its
process-local step activity) is kept; a second process on the same database is
excluded by the data root's `runtime.lock`. Pending work resumes from its retained checkpoints
and attempt count; settled replies are released without repeating their effects.

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
rerun_after_interruption: true and its step_id. Review-entry CI is not run by
the hook itself (see "Review-entry CI on the check runner"): after a crash the
check run is reconciled by the check worker and the woken hook reads its
result, so CI command entries carry their `step_id` but never
`rerun_after_interruption`.

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
plus the ten-second remote acknowledgment). A pending command also wakes when a
predecessor's lease lapses, so it can reclaim work whose waiter and claimer both
stopped. Once that command claims the lease, its result describes its own
committed Task; its claimed wait has the separate ceiling described below
(`rerun_review` only re-enters `review` and runs no check itself). A timeout returns HTTP 409 `task_busy` with
`pending_steps`, `retry_after_ms` and a retry hint; the accepted command remains
durable. Hooks and cascades remain asynchronous, and responses retain
`pending_steps`. Claim and its execution startup share one command; dispatch
continues if the HTTP waiter expires. Cross-Task wakes enqueue without waiting for
another Task lane, and explicit initial role assignments publish in the new Task's
birth transaction. A root Cancel commits under the root lease and enqueues each
child's cancel as the child's own preempting step; it never waits on a child lease.

Repository mutation waiters use the same predecessor-only deadline. SQLite
write-lock acquisition and a claimed mutation's persistence and release are
not cancelled by the five-second queue response ceiling; SQLite's busy timeout
bounds database lock waits. A mutation still pending behind another Task step
returns `task_busy` at the deadline and remains queued.

A claimed command or mutation's wait is bounded too. Both waiters wake at
the step lease's expiry even when no notification or reply arrives. When
the lease of a claimed step lapses without a
settlement (its claimer stopped between claim and release), the waiter takes
the step over through the normal claim, which excludes live local owners.
A settled step whose release or reply was lost answers from its durable
result when its lease lapses, without repeating the command's effect.
The durable command reply stores the service error variant and its fields.
Reading it after a lost channel preserves the same service/HTTP category,
resource identity, policy refusal, version details and task-busy retry details
as the inline reply. Historical string-only errors cannot recover a missing
kind: `V202610140530` preserves their prior generic check error and every reply
row while normalizing the encoding.

A command or mutation that stays claimed under a live
owner for 90 seconds (one 60-second lease plus SQLite's 30-second lock
timeout) returns `task_busy` and remains queued: no caller waits without a
bound. The db handle carries that shared claimed-wait ceiling; tests shorten
it through a test-only setter. Dropping a command waiter removes its reply
registration while leaving its durable step and worker intact. The inline
claim of a head, its liveness witness and its runner are one spawned job, never
part of the waiter: a request dropped while the claim's commit is in flight
cannot leave the step claimed with nothing running it. A command attempt that
fails before it ran and is re-queued does not answer its caller: the command
is still accepted, so the waiter drives the retry when its back-off is due and
returns that answer, or `task_busy` at its deadline; only a settled failure
answers with the error. A lost reply sender for a live step is re-registered
for a later attempt. After the command ran, a busy database at its settlement
is waited out and never re-queues it (the step is failed, and its caller still
gets the command's own answer, if the lock never clears); a failed lease
release is repeated and never replaces the committed answer. A waiter that
reads a released step before its reply arrives waits up to 250 ms for the
typed reply before it uses the stored result.

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

A Task in a merge state (the state whose entry runs `run_merge`, whatever a
Project's workflow calls it) that has no integration attempt anything owns is
admitted by the dispatcher's sweep: `Step::AdmitIntegration`, executed by
`TaskService::reconcile_integration_admission` under the Task's step lease,
through the admission function the entry hook calls. No entry is replayed.
"Owned" is read from the attempt row (`ScheduleRead::integration_owned`): a
current attempt of the Task's present status entry that the queue worker
drives, or that waits for its owner parked or quarantined with a typed
reason. Not owned: no attempt, an ended one, one that was sent back while the
Task stayed in the entry, and an imported row that carries no admission
binding or that the importer parked for the Task's owner. The step admits
nothing under the Task gate, behind a pending step or an accepted owner
command, for a subtask, or while the start-time import is still pending. An
admission that cannot name a candidate blocks the Task
(`integration_admission_failed`) instead of leaving it unowned. The rule this
keeps: a Task in a merge state has a live attempt, or a typed owner-visible
condition with an offered action (`deferred` offers Retry,
`integration_retry`). Other states were never swept: they
have no durable completion witness, and a missing hooks row there is the
normal shape of an upgraded Task whose inline hooks finished.

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
`own` or `inherited_from_root`. The role of a subtask's step check (`planner`
in `lead_check`) is the root's lead and never a row of the subtask; see "Step
check" under Root Tasks.
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
| `review_progress` (ReviewProgress) | 2 extra repair rounds; 0 disables | Review origin window; `progress:<review id>` receipt |
| `review_verify_rerun` (ReviewVerifyRerun) | 1 inconclusive verify re-run; 0 disables | Ledger window keyed by Review id; `verify:<review id>:<evaluation>` receipt, checkpointed on the suspended hook; Task/Project overrides apply |
| RepairNoChange | 1 reminder, then owner park | Review origin window; unchanged clean candidate at the end of a coder repair |
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
ReviewVerifyRerun is persisted per Review invocation, independently of the Review
failure allowance; its first verify request initializes a fresh Review window,
including when that Review passes without a rerun. Replaying initialization
never refunds an existing charge. Task-wide overrides win
over supplied Review state config, which includes Project defaults for both
`review_progress` and `review_verify_rerun`.
Review lifecycle and budgets share `db::budget::is_review_state`: the state named
`review`, or a gate with the `reviewer` role whose `canonical_phase` is `review`
(a renamed review gate). Reviewer completion, failed/cancelled entry handling,
human decisions and re-runs, recovery, review carry, integration checks, and
review-ready projections all resolve that meaning from the workflow rather than
requiring the built-in state name. Such a gate shares the
Task's `review` and `gate:review` budgets with every other review gate. A
reviewer-role gate in another phase runs no Review and keeps its own
`gate:<state>` budget and entry cap. Verification
initialization and rerun charges require that review state and fence the Task's
actual status and version; renamed review gates receive the same Project defaults
and remaining-budget projections.

Owner-fixable failures and derived repeated reviewer findings spend nothing.
With structured findings, an open blocking reviewer finding checked for a third
time parks; its second appearance parks only without a smaller open blocking
count. Forge-owned findings count toward progress but never trigger repeat parks.
Once a reviewer fail has no blocking findings, derived rules stay off through
that Review window and the legacy self-reported repeat policy applies. The charge point verifies the assessment,
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

Startup crash recovery starts from the running executions (one query, then
one read per Task that has one; `recovery::tasks_with_running_executions`),
so its cost follows the number of interrupted runs, not the number of Tasks,
and a start with nothing to recover writes nothing. It covers every
non-terminal state of the Task's effective workflow, including the planning
gate, review gates and the inherited `lead_check` gate (which need not be in
the Project workflow). An Agent heartbeat timeout is narrower: it stops the
runs of that Agent's Tasks in implementation states only. Startup recovery marks every
interrupted execution whose terminal CAS it wins with `resume_policy = auto`,
regardless of its workflow state or whether the attempt has a reusable runtime
session. It leaves
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

Review failures distinguish candidate defects from owner and infrastructure work.
CI, required checks and verify failures become `CI-*`, `CHK-*` and `V-*`
findings and use the shared repair disposition. Reviewer `blocked` and exhausted
inconclusive verify park as `review_blocked` (verify has `source: verify`),
without spending the Review failure budget. Owner-fixable, repeated blocking
reviewer findings and non-blocking-only assessments park as `review_needs_owner`.
Ordinary exhausted repairs use `review_budget_exhausted`; progress may admit an
extra repair as described above. Typed check-owner infrastructure failures retain
`review_ci_infrastructure` retry/parking rather than becoming code findings.

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

The canonical end-to-end flow is captured by `crates/api/tests/suite/happy_path.rs`.
It boots the in-process Axum router with an embedded daemon and a real temp
git repo, drives a task through `todo → in_progress → review → merging → done`,
and asserts:

- The merge SHA lands on the default branch.
- The worktree is removed.
- One `review` row with `status=passed` is persisted.
- The expected event sequence appears on the bus.

The named cases `structured_review_findings_are_repaired_resolved_and_merged`
and `pre_existing_base_test_failure_is_triaged_reviewed_and_merged` additionally
exercise coder repair and finding resolution, and review plus queue-head triage
on a red base, through the REST router and real shell/check/integration owners.

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

After the whole bundle and its reconciliation finish, outside every migration
transaction and before workers start, the runner requests
`PRAGMA wal_checkpoint(TRUNCATE)`. A successful checkpoint syncs committed
pages and truncates the WAL to zero bytes. An external reader can keep the
checkpoint busy; SQLite then retains the WAL safely. This does not compact
the main database: pages freed by a table rebuild remain on its freelist for
reuse. No automatic VACUUM is performed.

To recover a refused host, compare `SELECT version, name FROM _migration` with
`crates/db/migrations/`. Apply the build's migration by hand if its schema is
missing, then update that row's `name` to the build's file name.

Connection pool sets `PRAGMA foreign_keys=ON`, `journal_mode=WAL`,
`busy_timeout=30000` per connection.

SQLite has one writer. Every transaction that writes begins with
`db::begin_immediate`, which records where it began until it commits or rolls
back. A write transaction held for more than five seconds, and one that waited
that long for a connection and the lock, is logged at `warn` with its call
site; a statement refused as busy (`database is locked`) is logged with the
write transactions open at that moment (`db::open_write_transactions`). Code
that waits for a future which may be inside a write transaction must keep
polling it: a store call awaited in the body of a `select!` branch stops the
other branches, and a stopped transaction keeps the write lock that call then
waits for until the busy timeout. `FORGE_DB_BUSY_TIMEOUT_MS` shortens the
timeout for diagnosis.

Connection release rolls back any remaining SQLx transaction depth before the
event-notification fast path or pool reuse. SQLx's custom `BEGIN IMMEDIATE`
acknowledges BEGIN before awaiting its handle check and constructing the
`Transaction` rollback guard. A Cancel/Hold that drops the future in that gap
must not return an open write transaction to the pool. The release hook covers
every integration action and owner command using the standard pool, including
their decision-to-commit windows, while preserving the existing permit/result
protection and owner-command ordering. The depth check is an atomic read; an
ordinary connection release adds no SQLite worker round trip.

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

Operator status, milestone readiness/release persistence, workspace cleanup,
Attention/wake projections, Task workspace admission, recovery persistence,
Agent status/coordination, Project runtime/escalation, placement environment waits,
first-user bootstrap, Task governance, Agent Chat workers/native reads, workspace
garbage collection, topic rotation, repository provisioning/location queries,
Genesis and Project artifact commands, and chat/usage-index persistence use
typed DB functions in the corresponding aggregate modules.
Task lifecycle, role and lead reads, plan transport, workspace-root startup and
migration, integration adapters, memory backfills, and environment, authorization
and notification projections also use typed DB query helpers.
Task-step queue, conflict-hotspot episodes, periodic worker health and workflow
transition/hook statements likewise live in their DB aggregate modules; queue
and workflow services retain each statement's original connection and await order.
The remaining dispatcher admission/reconciliation reads, workflow action and
prompt-loader queries, and fenced Task creation, review and recovery writes
also live in the DB aggregates. Task services keep their `TaskQuery` identity
fences, applied-result checks and caller-owned transactions through typed DB
functions; production services contain no raw SQLx query calls.
Workspace-root path statements live in `sqlite/workspace_root.rs`; their rewrite transaction
remains owned by the migration service, including its original deferred begin.
The services retain policy and transaction ownership; queries participating in
those transactions accept the caller's `SqliteConnection`. Read projections
return named row structs, while SQL execution and row decoding produce `DbError`
for conversion to `ServiceError` at the service boundary. Projections whose
callers conditionally decode fields or ignore decoding failures retain typed
field results or query-specific typed accessors so the service handles those
errors at the original point; nested wake-budget savepoints and fenced Task writes retain caller transaction ownership.

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
  projection, and `WorkspaceCleanupScheduler`. Shared worker identity and
  shutdown contracts remain here, alongside the chat/admission/native-tool
  modules still used by core unit-test fixtures and monitoring.
- **services-agents** — The transport-neutral `ForgeRuntime` /
  `ForgeRuntimeBuilder` composition graph and `RuntimeSupervisor`; inquiry and
  Main Chat topic services, prompt previews, provider authorization, and the
  embedded Task executor. Depends on `services`; the core does not depend back.
  The Agent Chat turn-worker and Main Chat topic acceptance suites and the
  runtime-lifetime recovery test live here; the other acceptance suites stay in
  `services`.
- **review** — the workflow's `run_ci_steps` hook prepares the Workspace and
  asks the durable check runner for the configured checks before ordinary
  reviewer dispatch; its hooks step waits `suspended` for the result. An
  explicit review re-run (`TaskService::rerun_review`, reached from the Task
  action's review `retry`) is that same entry again: it re-enters `review` for
  the current candidate under the workflow's authority, closing the attempt it
  replaces, and is refused `task_busy` while an entry or reviewer still runs.
  The entry records who asked; one the owner asked for spends no review budget.
  There is no second review pipeline. Task configuration
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
commit SHA. `review::contract::parse_assessment` takes the last complete JSON
object in the reply that names a result, ignores unknown keys, accepts `verdict` for
`result`, and keeps everything else as the Markdown `report`. An object listed
right after `[` or `,` (no blank line between) is a list item, never the
verdict, so a finding inside a malformed outer result block cannot become the
verdict through its `result` or `verdict` key. A correction turn is appended
after a blank line and is read even when the reply before it was cut off. An
unreadable reply goes through result correction.
Re-recording an immutable assessment compares parsed `ReviewConformance`
values, so explicit and omitted defaults are equivalent without rewriting the
stored JSON; any content change is still refused. The hard gate is
Forge's own setup steps and required checks, not the reviewer's citations: a
failing check fails the review whatever the reviewer said, and a `pass` on a
Task with no configured checks rests on the reviewer's judgment alone.

Strict persisted Review-detail readers accept Forge's manual-pass object
`manual_override` and durable carry checkpoint fields `carry_hook_step_id`
(string) and `carry_hook_index` (nonnegative integer), alongside the CI,
conformance, auditor, approval and execution fields. They preserve that metadata
for workflow writes, omit it from public detail responses, and continue to
refuse unknown top-level keys or malformed values.

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

Reviewer result blocks also accept optional structured `findings` and
`resolved` ids. The frozen assessment keeps only the reviewer's bounded input;
Forge assigns ids and derives open/reopened/resolved/unconfirmed status and blocking streaks with
`db::review_findings::finding_ledger`. `SqliteDb::review_window` reads the persisted Review
budget window and its Reviews together. A retry-window reset starts a fresh
ledger; a source that did not run leaves its findings unchanged. A reviewer
that omits an open finding makes it unconfirmed, while passing Forge checks
resolve their own findings. Review CI uses one `CI-<zero-based step>#<first 8
hex digits of SHA-256(normalised signature)>` finding per failing test. The
same identity carries across owners and server restarts. A failing step with
no signatures uses `CI-<step>`. A failing step's omitted signatures never resolve findings or advance their
streaks. `failures_truncated: false` means every extracted failure was retained;
it cannot prove that a fail-fast runner executed later test binaries. With the
current outcome fields, only a passing step provides complete pass evidence and
resolves its per-test findings: a first-run pass, or a CI-triage re-run of the
whole step that passed (step class `flaky`). A step-level finding replaced by per-test
findings becomes `superseded`, never a resolution or progress credit. The re-review prompt carries the prior ledger's
open/reopened ids and retains the 8 KiB report slice. Both the response instruction
and re-review section list frozen requirement IDs and explain `requirement_ids`,
`kind` (`regression`, `missed`, `out_of_scope`) and `same_cause_as`. Parsing stays
lenient; unknown requirement IDs are dropped before freezing, and empty optional
fields are omitted. After the reviewer's first assessment in the window (a
Review whose CI failed before the reviewer ran, or that was cancelled, does not
count), a new blocking finding without a regression,
known requirement citation, or candidate-diff file becomes minor with the note
“outside requirements and Task files”. Continued IDs keep their identity; a
continued downgraded finding stays minor until it qualifies.
`out_of_scope` never blocks and is listed as a follow-up candidate. Anchoring
changes only the ledger, never the assessment.

A resolved finding that fails again becomes `reopened`, with its last actual
`resolved_attempt` retained; briefs and comments say “re-broken: resolved in round
N”. It counts as open and starts a fresh blocking streak. Minor appearances
contribute zero: raising a minor to blocking starts at one. A structured reviewer
fail left with no blocking findings parks `review_needs_owner` with
`only non-blocking findings remain`, without spending a Review allowance. The Task findings API and CLI expose
the same projection. Merge failures stay outside it. The ledger also drives round-policy routing
and budget charging through `db::review_findings::round_policy`. The complete
projection is naturally ordered by id (`F2` before `F10`) and reserves all ids.
Before its 200-entry cap, the API prioritizes open blocking verify findings,
then other open blockers, then other severities/statuses; each group keeps
natural ordering.

Task review comments now use this same projection after the settled attempt:
headline, GFM checklist, optional recorded verify summary, then a Review-tab
pointer. Open findings are unchecked, findings resolved by this review are
checked (earlier resolutions are omitted), and unconfirmed
findings say "not re-checked" without a checkbox, listed in that order. Each line
is one line of text (summary clipped to 200 characters); the lines stop at 8 KiB
and the remaining ids follow on one "Also in the ledger" line, because comments
are read back into agent prompts. A window Review the ledger cannot read (an
older details shape) leaves the list out of the comment; rendering a comment
never stops the cascade that posts it. The full report stays with the
Review; only a findings-less review carries a UTF-8-safe 2 KiB report excerpt.
Review-entry CI and verify failures post the same result comment even without a reviewer.
Repair-round system comments name the charged budget (`review` or
`review_progress`) and open/resolved ids; owner parks name the rule and open ids.
Verify comments summarize the recorded verdict, failing check names and evidence
count; diagnostic notes remain in Review details and the reviewer prompt. Each result, repair and park comment has a
separate Review-derived `idempotency_key`, using the existing comment repository
transaction to suppress replay duplicates. The web Review tab reads the ledger
above the latest attempt and keeps per-attempt findings and the full report in
history. Verify rendering is conditional on recorded data; verify execution and
progress rounds use the same ledger. Verify runs on the durable check runner as described below.

Coder and autonomous-worker repair requests use one `RepairBrief` in
`workflow/dispatch`; automatic review recovery uses the same builder. It reads
the persisted Review-budget window, the complete finding ledger, all failing
Review CI steps with their failure signatures and tails, optional recorded
verify results/evidence, the reviewer report, and the reviewed coder's completion
summary. The ordered sections are Task (description and canonical plan), Round
(charged budget and remaining allowance), Requirement status, Pattern, Open
findings (including survived repair rounds), Resolved/unconfirmed changes,
Failing check output, Verify, Reviewer report, Attempts, and Logs. Empty optional
sections are omitted. Pattern precedes the open list so even an exceptional
id-only truncation retains the shared-cause instruction.
At most 20 signature findings per step are displayed, followed by `+N more`;
the ledger itself retains every finding. Failing check output lists blocking
steps first; steps CI triage waived under `full` follow, labelled non-blocking,
so their tails never use the shared allowance first. A no-change reminder still leads the
request and names its `repair_no_change` allowance.

The brief body is capped at 24 KiB. It trims the report to 2 KiB, then drops
it; cuts Attempts oldest round first (keeping the latest longest); reduces each check tail to 1 KiB; and reduces
the resolved/unconfirmed section to ids. Next, all check tails share 4 KiB
(later steps keep their command and signatures) and the verify section keeps
2 KiB. Only then is finding text clipped: summary 200, where 120, expected and
observed 300 characters; past a line budget, the remaining open findings are
listed by id. Task description and plan each keep at most 4 KiB, including a
UTF-8-safe truncation note. Waived and other update lines share a 1 KiB allowance
once compacted; check output shares 8 KiB and logs keep 1 KiB. The final byte
bound includes its truncation notice even if an exceptional window has more ids
than the entire brief can contain; the complete ledger remains readable.
Attempt diffs are loaded only for coder/worker repair dispatches in active states. Attempts retains the last three settled review rounds: their reviewed commit
range, up to 20 changed files with diff statistics through the workspace owner,
the coder completion summary (1 KiB), and resolved/left-open/opened finding IDs.
Missing historical Git evidence is explicitly unavailable. The closing instruction
asks the coder not to repeat an approach that left a finding open, or to explain
why it failed before. Requirement status shows at most 40 frozen requirements, open blockers first, then `+N more`:
`unmet` for an open blocking citation, `addressed` when only resolved findings cite
it, otherwise `no open finding`. Pattern joins transitive `same_cause_as` families,
including resolved ancestors and locations, and asks for every call site to be
fixed. Requirement status is omitted when dropping it avoids clipping open finding text; a small status list is retained when the other protected content already requires compact reductions. Pattern remains alongside findings under the hard
24 KiB cap. Because Pattern is never cut,
Pattern has display limits instead: open members first, at most 20 sites per
family and 30 in all (locations clipped), then "+N more"; the ledger keeps every
member.

A carried blocking reviewer finding (streak at least two in the latest review)
starts a fresh repair execution instead of resuming the prior thread. The brief
opens “Before editing, write a short root-cause note for each carried finding”,
requesting a `decision` worklog entry. Project setting
`repair_switch_model_when_stuck` defaults false; when opted in, an available CLI
fallback is preferred for that execution only. Agent configuration and the full
route stay unchanged. The next ordinary round returns to the primary candidate;
it starts fresh if the previous execution switched, so sticky thread continuity
cannot extend the model change. Other ordinary repair rounds retain thread reuse.
Native Agents have no CLI fallback chain and keep their configured model.

Actionable merge send-backs use brief-only `MERGE-CI` or `MERGE-CONFLICT`
findings. `MERGE-CI` quotes the send-back's target, failing command, up to 20
failing test signatures (`+N more`), and the last 2 KiB of output;
`MERGE-CONFLICT` gives the committed-marker paths and reconciliation instructions.
Neither enters the Review ledger. Dirty-worktree and owner-only repair messages
retain their existing behavior and prompt builder ids.

Results map to conformance and routing as follows. `pass` becomes `passed`
(merging, or `awaiting_human` behind a human gate). `fail` becomes `failed` and
follows the review-remediation budget, with the structured repair brief as the
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

Before coder remediation, `blocked` and `fixable_by: owner` keep their
existing owner parks. Otherwise, structured findings drive D5 in both reviewer
completion and review-entry hook disposition. Progress means strictly fewer
`status in {open, reopened} && severity == blocking` findings than after the preceding
Review in the same persisted window, including unchecked sources. Triage-waived
CI findings do not count. Only reviewer findings (`F*`) checked in this attempt
trigger repeats: streak three always parks, and streak two parks without
progress, with `review_needs_owner` and `repeated finding: <id> <summary>`,
also when a required conformance check failed in the same Review (owner
routing and the self-reported `repeat` still need the reviewer's verdict to be
the cause). Those parks spend no retry budget. While derived rules are on, the reviewer's
`repeat` flag is ignored. A raw reviewer fail without blocking input findings
(no list or a minor-only list) disables derived rules for the rest of the window.
Structured non-blocking-only failures still take the owner park above; legacy
findings-less failures use the previous matching-failed-review/self-reported-repeat
behavior.

A chargeable failure first consumes the ordinary Review failure allowance.
If that allowance has no retry left but the open blocking count dropped,
`ReviewProgress` grants up to two additional rounds, debited exactly once per
`progress:<review id>`. Task-wide `retry_budgets.review_progress` overrides the
supplied Review state config; the Project setting supplies a default when the
state has none. Gate `max_rejections` never sets this budget. Its remaining
projection uses the Review state even while the Task is working. Owner actions
spend nothing, but an owner's review re-run gets a progress round only while
the limit has one left (`0` stays disabled). Without progress, or with progress disabled/exhausted, ordinary
automatic recovery and `review_budget_exhausted` apply. Coordination roots with
a lead retain `charge_replan` and remain outside derived repeat/progress rules.
Explicit owner findings and the non-blocking-only anchoring park still apply,
including when recovery routes a previously settled Review.
Services and transactional charging share `WorkflowDefinition` planning-state and
coordination-lead-role resolution, including non-default workflows. A review-entry
infrastructure interruption during an already-granted progress round keeps the
existing infrastructure retry/park episode and does not spend another progress
allowance. Configuration and other deterministic hook errors retain ordinary
entry parking; the progress receipt never classifies an error as infrastructure.
The workflow's reject target is used, including autonomous `working`.

The dispatcher
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
The queue's settle step checks authority in a short write transaction. Its
owner then compares the frozen source/target commits and fast-forwards the exact
object outside that transaction; the result step proves target ancestry before
done. Stale review authority and a clean target
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
dispatched. `review_integration_authority_in_tx` keeps every existing check and exposes
the effective candidate to settle: the newest carry row whose `contract_execution_id` is the
current passed contract's, else the contract's own `commit_sha`/`base_sha`.
Integration compares `HEAD` and the target tip with that candidate, so a newer
real review (a different contract execution) supersedes
all earlier carries automatically. A stored workflow change (such as migration
`V148`) forces a fresh v1 review; for v2, only a change to review authority
invalidates the source digest.

The manual review re-run is a review entry and settles as one: a pass cascades
into merging, a failure goes to the review-remediation target without spending
the review budget (the owner asked for it), and a human-required pass remains
in review as `awaiting_human`. The action returns the re-entered Task; the
entry's work settles afterwards in the Task's own steps.

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

### Checks: the final state (3.3)

There is one path for review CI. This overview is the map; the sections
after it hold the detail and keep their stage names because the code comments
cite them.

| What | Where it is decided | Section |
|---|---|---|
| What runs | `review::check_spec::build_check_spec` builds a `CheckSpec` from the entry's effective configuration. Review entry asks for `canonical-ci/1` in a server-owned Task worktree; a daemon placement and a bundle with no worktree get the frozen `legacy-daemon/1` / `legacy-server/1` policies, which always run and are never reused | "Check contract", "Reusable results" |
| Who runs it | `check-executor`, a persistence-free owner primitive, on the machine that owns the checkout (server or daemon) | "Owner check execution" |
| When it runs | The `check-runs` worker: single-flight per identity, machine run-cap admission in the shared writer transaction, slot fairness with aging, recovery by operation id | "Durable check runner" |
| Who asks | A Task step, through `TaskCheckConsumers`. Two families: review entry (`run_ci_steps`, in production) and the integration queue head (production) | "Task-step consumer contract" |
| How the Task waits | Its hooks step suspends on the consumer (or on the disk wait) and the Task states a typed condition (`check`, or `capacity` with scope `disk`); every wait has a deadline or a typed park | "Review-entry CI on the check runner", "Typed check conditions" |
| What a re-run is | The same review entry again (`rerun_review`); there is no second review pipeline | "Manual review re-run through the review entry" |
| What changes under a wait | Configuration: the entry asks again by itself (at most 3 times). A withdrawn check (refused forced delete): the same. Workflow: typed park | "Review-entry CI on the check runner" |
| How long history lives | `server.check_retention_days` (30); deny-by-default predicate | "Retention of check history" |
| What the operator sees | `GET /api/v1/operations/status`: `checks`, `check_runs`, per-machine `check_runs` / `borrowed_check_runs`, `recent_errors` (`task_check_exhausted`, `retired_review_auditor_config`) | docs/api.md |

Review verify uses that same runner, after CI passes and before reviewer
dispatch, as a separate `CheckPurpose::ReviewVerify` consumer with origin
`entry`. `review::verify::build_bundle` generates the background start,
readiness and ordered check commands, all `Uncacheable`, under unchanged
`canonical-ci/1`. Verify's whole-run limit defaults to 1200 seconds (maximum
3600) independently of CI's 3600-second entry limit. The server owner picks
a loopback port under its existing admitted checkout lock and supplies
`FORGE_VERIFY_*` runtime inputs. Files live under
`<task root>/.forge-task/verify/<review id>/`, so coordination children sharing
a Task root cannot overwrite another Review’s request or reports. The Task-root request describes Task text,
requirements, merge-base/candidate, changed files and open ledger findings.
There is no new process-spawn path: check-executor and process-supervisor
execute and settle every group, including the background server, on pass,
fail, timeout, cancellation and dropped execution. The start step records its
group in `start.pgid`; before any verify starts, the owner stops a recorded
group whose leader is gone but whose members still run (left by a server
crash or restart), in every verify directory of the Task root. Runs this
server is executing are registered in memory and skipped, since a live run's
group looks the same once its start step has exited; a Review whose own
record cannot be confirmed stopped is refused rather than started beside it. Ports handed
to verify runs are held by the server for the longest wall limit. Once a run’s
receipt confirms its groups stopped, the owner deletes `start.pgid` to avoid
signalling a reused group id later. A process that leaves its group (`setsid`
or daemonizing) is outside `canonical-ci/1`'s stop. After settlement Forge probes
the verify port on both loopbacks. If it is still in use, `details.verify.notes`
and the reviewer prompt carry a non-blocking warning, retained across
inconclusive reruns even when the later port is free; Forge does not kill an
unowned process by port. This detects an escaped listener, not every escaped
process; group settlement is not OS-level containment.

The review hook keeps its asking-time authority through both CI and verify
suspensions, and writes combined Review details at settlement. A verify fail
writes `V-*` observations before reviewer dispatch and uses the existing
review failure disposition. An app exit during readiness produces only
`V-ready`, without an additional synthetic `V-start`. An inconclusive result
gets a ledger-charged rerun (default one, configurable with
`retry_budgets.review_verify_rerun`), then a `review_blocked` park with source
`verify` without charging the Review failure budget; owner offers
are Retry Verify, Mark Reviewed (manual approval with reason), and Cancel.
Infrastructure failures retain review-CI interruption handling. A daemon
placement records `not_run` / `placement_unsupported` without launching
verify. The integration queue head never requests verify.

`details.verify` records each check's result, summary, media ids, Task-root
evidence paths, app-log tail and skipped-evidence notes. The same Task evidence
store captures confined regular files under the 25 MiB per-file and kind limits,
with a shared 64 MiB byte allowance across all checks in one verify evaluation.
Further files that exceed the remaining allowance are refused with a note.
The app appends to `app.log` directly, so short startup lines are visible at
once; the owner keeps the first 16 MiB by cutting the file back every 200 ms
during the run and once after settlement, before the receipt is recorded.
A symlinked, hard-linked or replaced `app.log` is never truncated. A process
that escaped its group can still grow the file until the next prepare of that
Review deletes it. Reports cannot
escape the verify directory. Project values are redacted from output, report text and UTF-8
evidence; public `FORGE_VERIFY_*` port, URL and directory values are excluded
from redaction, including matching digits. Reviewer prompts include the
recorded Verify results, notes and evidence file paths. Optional verify config follows
the existing workflow/Project/Task merge order; Projects without it keep the
existing review path. See [API verify configuration](api.md#behavioural-verify-before-reviewer-dispatch).

Not on the runner, and unchanged: conformance setup and required checks,
before-work and other lifecycle scripts, environment preflight and readiness
probes. They keep their inline orchestration, limits and verdicts; the
contract below characterizes them so that a later move changes no behaviour.
Deleted since the stages below were written: the manual `ReviewRunner` and
its synthetic reviewer/auditor executions (stage E), `legacy_ci_bundle` and
`review::auditor::render_auditor_prompt` (stage F; both had only test
callers).

### Check contract (3.3 stage A)

`CheckSpec` and its builder describe every execution family. Review-entry CI
(and so the manual review re-run) executes through this contract and the
check tables; conformance, lifecycle scripts and environment probes are
characterized here and still run inline, with their existing commands,
scheduling, timeout, output and cleanup behavior. No legacy Review or check
evidence is imported into a trusted cache. `server.check_run_timeout_seconds`
is read by workspace garbage collection (how long a `check-*` build directory
may be in use); a review-entry run executes under its own 3,600-second wall
limit.

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
spec. The frozen policies mark configured commands `uncacheable`. The canonical
server-worktree CI builder marks its commands `declared_controlled_inputs`,
and the current review-entry and queue-head identity builder selects that
policy; reusable receipt validation still requires the owner attestation,
unchanged tracked candidate and confirmed process-tree settlement.

| Execution family | Characterized bundle, cwd, environment and timeout |
|---|---|
| Review-entry CI | Effective workflow/Project/Task CI in order; Task root; Project keys; no per-command limit; collect all under full triage, otherwise stop at first failure. |
| Manual review re-run CI | Characterized as the same effective CI resolution and command semantics as review-entry CI under the `review_ci` purpose. Since the re-run re-enters the review entry (3.3 stage E), it is review-entry CI and no producer asks with `review_ci`. |
| Conformance | Effective setup followed by CI-derived and requirement-linked checks, using the admitted governing context; Task root; Project keys; configured 1–14,400-second per-command limit, default 1800. Setup failure stops the bundle; required-check failure does not stop later checks. |
| Blocking before-work | Selected event's blocking scripts only, original hook indices; Task root or existing lifecycle fallback; Project keys plus the eleven `FORGE_*` context keys; configured limit or legacy 30-second zero fallback; stop at failure. |
| Other lifecycle / hook test | Selected script hooks; the asynchronous before-work path excludes blocking scripts; existing workspace/fallback cwd and context keys; same timeout fallback; continue between scripts. Plugins have no shell-check contract. Owner hook tests select an original hook index and bypass the asynchronous blocking-hook filter. |
| Environment preflight | Role-applicable environment checks in order; Task root; Project keys; limits clamped to 1–300 seconds; stop at failure. |
| Project/machine readiness | Already selected readiness probe set in order; repository or owner probe scratch; Project keys; 1–300 seconds; collect all checks. Selection and owner routing remain outside the builder. |
| Exported environment helpers | Role-applicable selected checks; supplied directory; Project keys; 1–300 seconds; stop at failure. The single-check helper supplies `single_environment_check` and ignores role filtering, as today. |
| Agent-selected commands | No configured authoritative bundle exists. The builder returns an empty `agent_selected` spec; it does not manufacture reusable CI from arbitrary agent tool calls. |
| Queue-head CI | The generic builder requires an explicit `QueueHeadCheckBundle` (CI-only or conformance), with no default. The active integration head uses the shared review-entry CI identity builder and requests the durable runner under the consumer purpose `queue_head_ci`. |

**Run scope.** `CheckSpec.scope` says who may share one run of the bundle, and
it is part of the digest. `check_family_scope` gives each family's default scope; `build_check_spec`
overrides commit scope with the workspace ID/generation for canonical server CI:

| Family | Scope | Why |
|---|---|---|
| Frozen review-entry/manual review CI, frozen queue-head CI (CI only), agent-selected | `commit` | The builder's family default; Project/repo/commit/spec identity scopes a live run. Frozen CI is not reusable after completion. |
| Canonical server review-entry CI and queue-head CI | workspace | The builder pins the Task's workspace ID and placement generation; another worktree or generation has another identity. |
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
one), carrying every row and every consumer-to-result link. The base contract columns are listed here; the additive runner migration
also persists the dispatch/owner intent, admission and capacity wait, runtime,
deadline/cleanup, result and consumer-delivery fields used by stage C. All JSON limits
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
| `machine_id` | Optional physical-owner daemon FK, ON DELETE SET NULL. The durable runner performs physical-machine capacity admission before dispatch. |
| `applied_timeout_seconds` | Optional positive integer: the whole-run wall limit this run executes under, taken from the request that scheduled it. Later requests that join or reuse never change it. The runner must apply this value, not the setting current at execution. |
| `lease_owner`, `lease_until` | Optional text, both null or both populated; expiry comparisons use SQLite julianday. |
| `lease_generation` | INTEGER NOT NULL DEFAULT 0, nonnegative; advances on every claim/takeover. |
| `version` | INTEGER NOT NULL DEFAULT 1, positive; individual claim/lease/state/settlement writes use and increment version CAS. Bulk withdrawal cancels unleased queued runs under state/consumer predicates and increments each affected run's version. |
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
| `cacheable` | NOT NULL boolean requiring validated run input eligibility, a certified pass and reusable owner evidence; it can be false even when the run's input flag was true. |
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
unique request key. The active runner enqueues consumer delivery as Task steps; result
application and current-authority revalidation happen only in those steps. Deleting Project or
repo cascades evidence; deleting Task, workspace or machine detaches optional
references. Machine removal's existing tombstone behavior still works. There
are no ON DELETE RESTRICT references. Tests cover each deletion and fresh and
upgrade migration replay without promoting old review rows.

Relative to the inventory: the spec/environment/schema/execution revision and
audit reference are stored once in the bounded input manifest; result identity
joins the scoped run instead of duplicating repo/commit/spec data. Stage C persists the operational run and consumer-delivery fields it uses;
owner receipts additionally carry HEAD/tracked-change and process-drain
witnesses, which receipt validation checks before storing mechanical evidence. Cleanup and outcome enums can represent
success, failure and uncertainty without selecting later cleanup policy. The runner and Task-step delivery use the additive stage-C columns described
below. No legacy cache is seeded.

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
The active Task-step consumer revalidates current authority and input identity
before applying a hit or joined result; these tables confer no authority.

Operator status adds `checks.by_state` (all eight states, zeros included)
and `checks.reusable_results`. These counters are read-only observations. The active worker separately
performs admission, and status also reports admitted, capacity-waiting and
borrowed runs from the same durable run/occupancy facts.

Stage B implements the persistence-free owner execution/transport seam below.
Stage C supplies admission/capacity, reconciliation and Task-step-only
request/application. It consumes these identities/repositories, adds only the
fields it actually needs, and rechecks current authority on every hit/join.
The active runner passes the run's recorded `applied_timeout_seconds` as an
absolute owner wall deadline for all policies. Producers choose the request's
whole-run limit; the legacy policies retain no per-command limit. A direct
primitive call without a whole-run deadline is unbounded under a legacy policy
and refused under any other (`a canonical check requires a whole-run wall
deadline`).

### Owner check execution (3.3 stage B)

`check-executor` is a persistence-free owner primitive. It receives a `CheckSpec`,
local checkout target, operation ID, owner identity, explicit owner input
revisions, deadline, cancellation token and a borrowed `CheckPermit`. It acquires
no capacity, writes no check/Task/Review rows and publishes no events. The active stage-C runner admits the physical machine, retains the stable
operation intent, certifies the receipt against the frozen spec and owner inputs,
and applies it through Task steps. The owner primitive itself owns no cache,
digest single-flight or recovery loop.

`CheckoutTarget::ExactCommit` verifies a full lowercase commit object ID and
clones an exact detached checkout into a unique `check-*/repo` directory under
the owner's build area. Commands run at that repository root. This never resets
or cleans the source checkout or Task worktree. HEAD/tracked-change witnesses
are captured before removal; a changed managed candidate cannot pass. The
managed directory is removed even after failure, timeout, cancellation or failed
preparation. Removal has a separate settlement bound; unconfirmed removal is
`uncertain` cleanup. Review-entry and queue-head CI run in the Task worktree under the canonical
or frozen policy. Their base-comparison subchecks use exact detached checkouts with commit scope;
the candidate and rerun continue in their original workspace.

Each command runs through `process-supervisor`, the single process-group
implementation shared with Git. It continuously drains both pipes, retains
bounded redacted UTF-8 tails and sets truncation flags. Synchronous byte observers
feed line extractors before tail truncation; streaming ANSI removal precedes the
text budget. Cargo test and nextest, vitest/jest, pytest and Go failures become
normalised identities (up to 200 per command, 300 Unicode characters each, deduplicated).
Vitest/jest failure glyphs (`×`, `✗`, `●`) count only within a recognised
runner block; a summary closes that block. Recursive pnpm (`pkg test: `) and turbo
(`pkg:test: `) test-task decorators are stripped before extraction, while tails
retain them; the identity is qualified with the package (`pkg > <identity>`), since
paths a package's runner reports are often package-relative. Parallel package/task producers keep independent runner/file state;
producer-state overflow marks evidence incomplete rather than waiving failures. A jest `FAIL <file>` line opens one
for any JS/TS file (the test-name pattern is configurable), without jest's
slow-suite run time. Nextest identities combine the binary and test name,
omitting durations, progress counters and repeated stdout/stderr; failed,
timed-out and signalled tests count, and under retries only the final attempt.
They differ from plain `cargo test` identities (no binary), which is safe
because triage compares runs of the same step command.
Each command gets an isolated `FORGE_CI_REPORT_DIR` under the reserved Task root
(or an owner temporary directory when no Task root exists); failed and errored
JUnit cases in XML reports there contribute identities before the directory is
removed. At the next check command in that worktree, abandoned
`.forge-task/ci-reports-*` directories matching Forge's generated names are
removed too; unrelated entries and symbolic-link targets are left alone.
Reports never change exit verdicts. Optional `failures` and
`failures_truncated` flow through owner receipts, durable results and Review
steps; default-empty/default-false values are omitted, preserving historical
JSON and older-daemon receipts. If all step signatures would exceed the durable
256 KiB row budget after tails are cut, signature lists are reduced with
`failures_truncated` set, so evidence volume cannot prevent settlement. Output size cannot fail a
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

What outlives a normal exit depends on the policy. New non-CI policies stop a command's group after each command. `canonical-ci/1`
keeps groups alive for later steps, then stops every recorded step group at run
end before its final candidate witness. The two frozen CI policies do not: neither
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
budget variables and niceness. The canonical CI policy is the exception: it
clears nothing and runs a step exactly as the frozen server policy does (see
"Reusable results" below). The explicit frozen `legacy-server/1` and
`legacy-daemon/1` policies preserve existing login-shell/environment/Git-variable
behavior during caller migration: the child inherits the owner process
environment (PATH, HOME, toolchain variables), then the Project environment and
secrets, then the run budget; no variable is removed except `GIT_DIR`,
`GIT_WORK_TREE` and `GIT_INDEX_FILE` where the caller already removed them
(daemon steps and server steps with a deadline). They add no witness Git command beside a workspace step and impose no implicit
whole-run deadline; the active durable runner supplies a recorded absolute
deadline, which the primitive applies to frozen CI too. Project build overrides retain precedence.
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

The check methods `check.run`, `check.lookup`, and `check.cancel` were
introduced at daemon protocol **5**; the current handshake requires protocol **7 or newer**.
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
commands and cleanup settle, after its deadline, cleanup/removal bounds and termination/drain phases;
canonical CI additionally verifies every recorded step group at run end
(each `stop_group` can take about three seconds); waiting for a busy checkout is bounded by the same
deadline and by cancellation, and settles the key as `timed_out` / `cancelled`
with nothing spawned. A key that was admitted but could not start (workspace
gone) settles as `infrastructure`. An intent whose handler was dropped or whose
daemon restarted is `interrupted` and holds no journal headroom. A check key
outlives `journal.ack` only until its request deadline has passed: before it a
duplicate must find the receipt, after an expired acknowledged receipt has been pruned, a duplicate with the
old deadline settles as `timed_out` before any command starts, so the key is no
longer needed. A receipt still retained before that sweep is replayed. The deadline may be
at most 24 hours ahead. Acknowledged receipts are deleted once their deadline
has passed; unacknowledged receipts and interrupted intents 24 hours after it
(at admission of a later check, at most once a minute, and at daemon start).
The headroom reserved for a running check is sized from its own command count
(50 KiB per declared run or cleanup command, plus the request bytes and 8 KiB fixed overhead), not a fixed worst case. On the server, a
`check.run` call returns `DaemonUnavailable` as soon as the connection goes
stale and `DaemonTimeout` after the remaining deadline plus the cleanup bound
plus the RPC timeout; the caller then asks `check.lookup`.
Upgrade the server first, then every daemon from that release and restart using
the same workspace root. A daemon below the minimum protocol revision (7), or missing the required
capabilities, is refused before command dispatch. Newer revisions remain
compatible when they retain those capabilities.

The inline `integration_effects::check::CheckRun` adapter is deleted with the
review-entry cutover. Review-entry CI (now one check run per entry, which a
manual review re-run also is) retains command order, cwd, inherited env,
verdicts and comments/events. With CI triage enabled, its separate base subcheck
runs setup in an exact managed checkout, as described below. Required
conformance/setup, lifecycle/before-work, readiness/preflight/environment helper
and agent-selected families retain their existing orchestration and check policy.

### Durable check runner and its Task-step consumer contract (3.3 stage C)

`check_runner::CheckRunner::request` returns a consumer and `Hit(result)`,
`Joined(run)` or `Scheduled(run)`. Selection and consumer insertion are one
`BEGIN IMMEDIATE` transaction using the stage-A partial unique identity index.
A repeated consumer key joins its original run, including a terminal failure;
it never silently launches another attempt. One exception: an integration
head's consumer that was withdrawn under it (a forced Project deletion that
was then refused withdraws every consumer) waits again when the head asks
again, on a certified result, a queued or running run or a fresh run
(`revive_withdrawn_consumer_in_tx`); not on a run that is being stopped. A certified cacheable pass returns
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
completion, exit outcomes, timestamps, cleanup and the exact HEAD/tracked-change witnesses required for reuse.
Canonical CI can still report its Task's verdict when a witness is dirty,
missing or its attestation became unavailable; that result is not reusable. A malformed terminal receipt is
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
them is non-empty. Per-run admission and effect-job failures are logged and skipped. A batch
selection/decoding, delivery enqueue or acknowledgement-batch failure can fail
the sweep; framework supervision reports it and restarts the worker. The worker touches only rows
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
| `queued` | Admit to `running`; last consumer withdrawn: `cancelled` in the withdrawal's own transaction (`cancel_abandoned_check_consumer`, `cancel_project_check_consumers`: no slot has to free, no result row is written); all consumers stale by Task epoch (the Task was deleted or left the status entry that asked, and no writer withdrew them): `cancelled` at the worker's next admission look, before any slot (`admit_check_run` withdraws them and cancels the run in its transaction; no result row); owner removed or slot wait expired: infrastructure failure | Slot wait 1,800 seconds per attempt (with two automatic retries a consumer waits at most about 90 minutes before its Task is answered "no verdict"). Expiry settlement runs even when all 32 effect jobs are occupied. Queue time is excluded from wall time. |
| `running` | Receipt to `cleaning`; lost reply or lost lease to `uncertain`; no consumers left: `cancelling`. A run taken over before its intent was stored launched nothing and settles as an infrastructure failure | Recorded wall limit (1–86,400 seconds) plus 65 seconds of settlement grace. Lease 60 seconds, renewed every 15; takeover within 60 seconds plus sweep delay. |
| `cancelling` | Cancel reply with a receipt to `cleaning`; any other reply, or a lost lease, to `uncertain` | One cancel exchange, at most 61 seconds. |
| `cleaning` | Immutable result to a terminal state; a lost lease to `uncertain`, which resumes from the retained receipt | One fenced transaction; takeover within 60 seconds plus sweep delay. |
| `uncertain` | Receipt settles the original run; owner reports unknown or interrupted after a cancel tombstone: infrastructure failure (retried); machine removed or owner past the disconnected-owner bound: infrastructure failure (retried); owner still "running" or refusing: infrastructure failure, **not** retried | Lookup at most 30 seconds, cancel at most 61, repeated every 15 seconds. Hard bound: the run's wall limit + 65 + 600 seconds; while the owner is disconnected, its configured disconnected-owner bound can end the wait earlier. Never a second `check.run` for the operation. |
| `succeeded`, `failed`, `cancelled` | One `apply_check_result` Task step per `(consumer, result)`; owner acknowledgement | Delivery marker and enqueue share a transaction, on the sweep after settlement. An infrastructure failure is retried at most twice per consumer, then delivered as "no verdict". |

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

**Who gets a freed slot.** Work closer to done goes first. On a machine with a
run cap `C`, let `k` be the checks that hold a slot of their own, `q` the
queued checks a consumer still waits for, and `S = max(C - 1, 1)` the checks'
share. Four rules, all evaluated in the admission transactions on the shared
occupancy query (`db::machine_capacity`):

1. **An execution leaves room for queued checks.** A reservation, an execution
   start and the dispatcher's precheck are refused (`machine_capacity`) while
   `active + min(q, max(S - k, 0)) >= C`: free slots are kept for the queue
   until checks hold their share. A queued check reserves nothing durable; the
   count is read each time.
2. **Checks are admitted in queue order**: review-entry checks, then
   integration head checks, then any other origin, oldest first within each. A
   check takes a slot of its own only when the checks ahead of it in that
   machine's queue still leave it one (`active + ahead < C`), so a sweep that
   reaches a later check first does not let it jump the queue. A check that
   borrows its own Task's slot is not subject to the order.
3. **Checks stay within their share while a Task waits for a run slot** this
   machine could give: no check is admitted to its own slot once `k >= S`.
   With no Task waiting, checks may use every slot. A waiter is a Task with a
   run-slot wait (`task_schedule_wait.daemon_id = '*'`) that holds no slot
   itself (no running execution) and whose worktree, if it has one, is on
   this machine. A Task that waits for a machine's readiness or reconnect
   asks for no slot and does not count.

`q` counts a queued check only while a consumer can still use its result: the
consumer is not cancelled and its Task is still in the status entry it asked
from. A check whose Task was cancelled, deleted or left `review` reserves
nothing from that commit on (the worker cancels the run when it next looks).
A check queued for machine A is counted on A only. A check for a daemon that
is offline reserves on that daemon only, where no execution can start either,
and a queued run expires after 30 minutes.

**A Task and its own slot.** The dispatcher's precheck answers "not waiting"
for a Task whose own run of the role in question is running: that Task is in
flight, nothing more is dispatched for it until the run ends, and a capacity
wait left on it is retired. Any other running execution of the Task (an
interactive session, a run of another role) is not that: a dispatch then needs
a second slot, the reserve and start transactions count the first one against
it, and the Task waits for a run slot. Fairness is per slot, not per Task:
such a Task already fills one slot, so under rule 1 it is refused at a
release where a queued check is kept a slot (with the machine otherwise full
of checks, `active = C - 1` and one kept slot refuse it). Rule 4 below ends
that wait. A review-entry check never needs a
second slot while its Task's run or reservation holds one: it borrows that
slot and takes it over when the run ends, so the machine's count does not
change.

4. **An aged waiter goes ahead of checks queued since it began to wait.** A
   wait for a run slot has an age (`task_slot_wait_age`, kept by triggers
   since `V202610110650`): when it began, and how many capacity releases the
   Task was woken for and still refused. The wait is *aged* after 3 releases
   or 120 seconds, whichever comes first (`SLOT_WAIT_AGED_RELEASES`,
   `SLOT_WAIT_AGED_SECONDS`). Both sides order an aged waiter and a queued
   check by time:
   - *Execution side* (the precheck, the placement selection and the
     reserve/start transaction, through `machine_capacity::capacity_for_task`
     and `machine_precheck::snapshot_for_task`): for an aged waiter, rule 1
     keeps slots only for the checks queued **no later than** the start of
     its wait. Checks queued since reserve nothing against it.
   - *Check side* (`admit_check_run`): a queued check counts the aged waiters
     whose wait began **before** the check was queued as ahead of it in the
     queue (`aged_waiters_ahead`), so a sweep that reaches the freed slot
     first does not take it. Counted are Tasks that wait for nothing but a
     run slot (`daemon_id = '*'`, no Project-limit wait), have their worktree
     on this machine, and are not the check's own Task. A waiter without a
     worktree can run on any machine and holds no machine's checks back.

   The age belongs to the wait, not to the row: a dispatch attempt drops the
   wait row before its start transaction decides, so the age survives a gap
   of up to 30 seconds (`lapsed_at`) and a wait that ended longer ago starts
   afresh. It ends when the Task starts a run. The release count is of
   slots really given back while the Task waited: a run of another Task
   that left `running`, and an admitted check that ended and was not asked
   for by this Task (triggers on `execution` and `check_run`). The dirty
   marks that wake a waiter are not counted: a Task receives those for its
   own placement, comments and Project edits too, so it cannot age its own
   wait.

   A check stands back for aged waiters for at most 90 seconds in total
   (`check_run.yielded_since`, `CHECK_YIELD_SECONDS`): one dispatcher wake
   plus the 60 s recheck backstop. A waiter that cannot use the slot after
   all (it also waits for disk, its Agent is busy elsewhere) therefore never
   starves a check.

One approximation, on the cautious side. Rule 1 counts a queued check that
will borrow its own Task's slot when the worker next looks at it, so a slot
can be kept for one sweep (about a second) that nobody needed.

**A run-slot waiter cannot miss its wake.** A Task that waits for a run slot
has a `task_schedule_wait` row with `daemon_id = '*'`; every commit that frees
a slot marks those rows' Tasks dirty (the end of an execution, a placement, a
chat turn or an admission step, and since `V202610101906` a check run leaving
`queued`/`running`/`cancelling`/`cleaning`/`uncertain`, a check run deleted in
one of those states, and a check consumer being cancelled). Two things make
that level-triggered:

- *Registration.* Whoever decided the Task waits read the machines before the
  row existed, so a release between that read and the row marked nobody.
  `SqliteDb::schedule_wait` marks the Task dirty in the same commit that first
  writes `'*'` for it. The pass this causes reads the machines with the row in
  place: a release committed before that read is seen by it, one committed
  after it finds the row. A row that already says `'*'` marks nothing, so a
  Task that keeps waiting is not read in a loop, and an unchanged row costs no
  write.
- *Backstop.* Every wait for a machine carries a durable recheck deadline on
  that row: 60 s for a run slot (`SLOT_WAIT_RECHECK`), 30 s while the Task
  waits for disk or any machine is under its free-space floor
  (`DISK_WAIT_RECHECK`; free disk comes back with no commit at all). The deadline survives a restart; startup's immediate discovery/sweep
  re-evaluates waits without requiring that stored timer to expire. A Task
  read again before it fires keeps the armed deadline. No ordering of a release and a wait, and no
  release that marks nobody, parks a Task on an idle machine for longer than
  one period.

Neither side starves. A queued check at the head of its queue is admitted at
the first slot release after `k < S` (rule 1 keeps the slot from executions);
one at position `p` after at most `p` further check completions, each bounded
by the run's wall limit. For `C >= 2` executions always keep one slot turning
over (rule 3), so a waiting Task is dispatched at the next release of that
slot. For `C = 1` the single slot goes to the queued check first; an execution
waits for at most the checks queued at that moment plus the one a running job
can still produce, because a check is only ever requested by a Task entering
review or its integration attempt, and no new run means no new check. Among
executions the dispatcher's existing order is unchanged.

Aging (rule 4) keeps both bounds and closes the one case they did not cover.
*The cap*: rule 4 changes how many free slots are kept for whom, never the
test `active < C` that every admission still passes in its own writer
transaction. *The waiter*: a Task that needs a second slot was refused for as
long as checks kept queuing. Once aged it waits only for the checks queued
before its wait began, a set that is fixed at that moment and shrinks by one
at each release it loses; every later check stands back for it. It is
admitted at the first release after those checks ran, unless its other run
ended first. *The checks*: a check is held back only by aged waiters older
than itself, again a set fixed when the check is queued, each of which takes
one slot once (starting its run ends its age); and for no more than 90
seconds of standing back in total, after which the existing argument applies
to it unchanged. Two parties never wait for each other: the order between an
aged waiter and a check is their timestamps (a tie goes to the check).

Result delivery enqueues an entry-fenced `apply_check_result` Task command with
consumer/result/run identities, exact commit and spec digest. The consumer's
`delivery_step_id` and enqueue share a transaction. Its marker survives Task-step retention, so restart and pruning cannot
enqueue a second notification for that `(consumer, result)`. An owner rearm or
revival may reset the marker and deliver the consumer's next result. Already
stale epochs are enqueued as superseded and leave the Task untouched.

#### Retention of check history

`check_run`, `check_result` (with the output tails in `steps_json` and the
owner receipt on the run) and `check_consumer` are history once a run is
terminal and its consumers are answered. The `storage-maintenance` periodic
worker deletes them hourly, after its Task-step retention pass:
`StorageMaintenanceWorker::prune_check_history` calls
`SqliteDb::prune_check_history(cutoff, 100)` at most 20 times per hour. Each
call is two statements, each its own short write (at most 100 consumers, then
at most 100 runs); no write transaction spans a batch. The cutoff is
`server.check_retention_days` ago (default 30; `0` disables the job;
`FORGE_SERVER_CHECK_RETENTION_DAYS` overrides the file).

The predicate is deny-by-default (`crates/db/src/check_run/retention.rs`):

| Row | Deleted only when |
|---|---|
| consumer | older than the cutoff; **and** finished (cancelled, applied, its Task gone, or its Task no longer in the status entry it asked from); **and** it holds no undelivered result; **and** no Task step outside `done`/`superseded` awaits it (`task_step.awaited_consumer_id`) or names it through a hook checkpoint; **and** its delivery step is `done`/`superseded` or gone (failed and parked steps still pin it); **and** the Review attempt named in its request key is not `running` or `awaiting_human`; **and** its run is terminal; **and** no unfinished integration attempt still owns the integration consumer |
| run (its results go with it, `ON DELETE CASCADE`) | terminal (`succeeded`, `failed`, `cancelled`); finished before the cutoff and more than 26 hours ago (the owner acknowledgement window is 25); **and no consumer row at all** still names the run or one of its results; **and** no unfinished integration attempt names its run/result/operation through stored check/effect/receipt/observation evidence |

The last condition is what makes it deny-by-default: a consumer this job did
not delete, for whatever reason, keeps its run and its result. A reusable
green result therefore lives as long as some request used it within the
retention period (a `Hit` writes a consumer that names the result), and for
the retention period after its last use. "A reusable result has no expiry"
now reads: none but retention. After it is deleted the same request is
`Scheduled` and runs again.

Delivery steps (`apply_check_result`, `apply_check_progress`) are Task steps
and are pruned by Task-step retention (7 days settled, 30 unresolved); a
consumer's `delivery_step_id` marker outlives its step and goes with the
consumer. A Review keeps its own copy of the verdict and tails
(`review.step_results_json`), which no retention touches.

Every column that points at one of the three tables is listed in
`db::CHECK_REFERENCES`: the foreign keys `check_result(run_id, identity_key)`,
`check_consumer.run_id` and `check_consumer.result_id`, and the id column
`task_step.awaited_consumer_id`. The test
`every_reference_to_a_check_row_is_known_to_the_retention_predicate`
enumerates them from the migrated schema (`pragma_foreign_key_list` over
every table, plus every column whose name says it holds a check run, result
or consumer id, and every `run_id`/`result_id` column of an `integration_*`
table). A reference added later fails that test until the predicate accounts for its
reader; the current inventory already covers integration-attempt evidence.

#### Task-step consumer contract

`check_runner::consumer::TaskCheckConsumers` is the only way a Task step uses
the runner. A consumer family (one per `CheckConsumerOrigin`) registers a
`CheckConsumerFamily`. A step asks with `request(TaskCheckRequest)`, naming
its Task, the status entry it runs under and its *authority*: the step or
attempt that asks, as the family names it. The request is idempotent on
(origin, Task, status entry, authority) and is refused when the Task has left
that status entry or no family is registered. Its reply (`Hit`, `Joined`,
`Scheduled`) is information only. A normal request delivers each `(consumer, result)` once through an
`apply_check_result` Task step. The reuse-only `try_reuse` path applies a hit in
the asking Task step and creates an already-applied consumer without a delivery
step. Rearming a consumer for a new result can produce a new delivery. An integration head's
`request_check` re-ask can also consume an already-recorded verdict in its
requesting step to repair a missing earlier acknowledgement or failed delivery;
it does not enqueue a second delivery for the existing result.

`apply(step_id, delivery)` runs inside that step. It refuses (the step fails
visibly) an envelope that is not carried by the consumer's recorded delivery
step, or whose consumer, run, result, commit or spec digest differ from what
is stored. It returns `stale` when the consumer was cancelled, the Task left its status
entry, or the family's `current_authority` changed. A stale epoch leaves the
Task untouched; cancellation or changed authority clears only that consumer's
own wait when the delivery holds the Task lease. Otherwise it hands the
family a `CheckVerdict`: `Result` (pass, fail, timed out or cancelled, for
exactly the requested commit and spec) or `InfrastructureExhausted` (no
verdict after the automatic retries: the family parks its Task with a
retryable reason and records no failure of the candidate). The consumer is
then marked applied; a redelivered step returns `already_applied`. A crash
between the family's write and that mark redelivers once more, so a family's
`apply` is idempotent on `consumer_id`.

#### Typed check conditions (3.3 stage D part 1)

A Task that asked for a check has a direct typed condition, stated with the
3.2 A mechanism: a `check` witness on the stored condition, written under the
Task's claimed step through `state_integration_condition_in_tx`
(`ConditionStatement::Check` / `CheckCleared`). No legacy column or metadata
marker is written, no version is bumped and no event is emitted. The reason is
`ParkReason::Check { wait: { phase, consumer_id, origin } }`, public as
`ConditionReason::Check`.

| Phase | Entered by | `is_blocked` | Owner / recovery | Offers | Exit |
|---|---|---|---|---|---|
| `result` | `TaskCheckConsumers::request` (inside the asking step) | no | `check_runner` / `wait_for_check` | `cancel` | the delivery step applies the result (`CheckCleared`), or parks it as exhausted |
| `slot` | the `apply_check_progress` step for the worker's slot-wait note; also `request` when it joins a run already queued behind a full machine | no | `check_runner` / `wait_for_check` | `cancel` | the slot frees: admission enqueues the `admitted` note and its step restates `result`; or the result itself arrives |
| `infrastructure_exhausted` | the delivery step, when the answer is "no verdict after the automatic retries" | yes | `user` / `retry_check` | `retry` (reason `check_infrastructure_exhausted`), `cancel` | `retry`: the same consumer is re-armed with a fresh infrastructure budget (`rearm_exhausted_check_consumer`), the sweep's infrastructure retry schedules its run and the condition returns to `result`; `cancel` ends the status entry |

Rules:

- **The worker writes no condition.** A full machine is stated through a
  progress step: `admit_check_run` enqueues one `apply_check_progress` Task
  step per live consumer, in the admission transaction, the first time a run
  is refused a slot (`capacity_wait_since` goes from NULL to set) and once more
  when that run is admitted. The causation key is
  `check-slot-wait:<consumer>:<run>` / `check-slot-freed:<consumer>:<run>`, so
  a run is announced once per wait, not once per sweep. The step
  (`TaskCheckConsumers::progress`) restates the wait only for the consumer and
  authority still in charge; a note the answer overtook is `stale`.
- **A wait belongs to the status entry that asked.** The witness records that
  entry's epoch. Once the Task leaves it (sent back, cancelled, advanced) the
  consumer is stale, and the next write of the Task drops the witness; nothing
  has to clear it. A settled Task never carries one.
- **Preservation.** Every other producer (legacy fields, hooks, executions,
  budgets, remote cancellations, children, the full recompute and the
  invariant repair) carries the witness. Another owner's park (a hold, a
  blocked entry) stays primary with the check wait as an additional reason. A
  consumer clears only its own wait: `CheckCleared` names the consumer and is
  a no-op for any other.
- **Dispatcher.** `next_step::check_decides` follows the integration rule: as
  the primary reason, or behind a real owner blocker, the check wait decides
  the Task's next step and parks it (`Worker` / `WaitForOwner`, or `User` /
  `RetryChecks` when exhausted). It is never a dispatch, an entry-hooks replay,
  a review-CI retry or a capacity-unpark demand, and reconciliation counts the
  Task as owned. Under a self-clearing primary (capacity, dispatch refusal,
  offline owner, environment) that primary keeps its own step.
- **Requests come from the Task's step.** `TaskCheckConsumers::request` refuses
  a caller outside the Task's claimed step: its wait could not be stated.
- **One delivery per answer.** The delivery step's causation key is
  `check-result:<consumer>:<result>`, so a consumer re-armed by `retry` is
  answered again by its next result and never twice for one.
- **Retry names the identity still in charge.** `retry_exhausted` re-arms the
  consumer only while the family's `current_authority` is the one that asked.
  When the candidate moved on inside the same status entry, the retry lifts
  the stale park (`CheckCleared`) and runs nothing: the old commit is never
  checked again, and the family in charge asks for its own identity. A
  repeated `retry` (double click, redelivered step) re-arms nothing twice.
- **One wait per status entry.** The condition holds a single check witness.
  A later request in the same entry replaces it (that is how a newer attempt
  takes over from a stale one), so a family must not keep two live consumers
  for one Task entry at once: the second would hide the first, and its answer
  would end the visible wait while the first is still running.
- **Operator visibility.** `GET /api/v1/operations/status` lists every Task
  parked as `infrastructure_exhausted` in `recent_errors` (entity type
  `task_check_exhausted`, severity `attention`) until it is retried, cancelled
  or leaves the status entry.

**Two families are registered, composed by one constructor
(`TaskService::compose_checks`, used by the production runtime and by every
test): review entry (`entry`, requested in production, see "Review-entry CI on
the check runner") and the live integration queue head (`integration`).** A manual review re-run is a review entry
and asks through the `entry` family. Conformance, before-work and other checks
retain their current orchestration, limits and verdicts. The integration queue
is active; its head requests the same CI bundle through this constructor. The
owners execute the frozen legacy policies and the canonical CI policy below;
only the canonical one attests its inputs and may be reused. A reusable result
has no expiry of its own: reuse ends when the commit, spec digest, owner
environment identity or audited execution revision differs, or when retention
deletes a result nobody used for `server.check_retention_days` (see "Retention
of check history"). No historical review or legacy
CI row is promoted to cache.

#### Reusable results: the canonical CI policy (3.3 stage D part 2, scope 1)

`canonical-ci/1` (`api_types::CANONICAL_CI_POLICY`) is the execution policy
under which a CI bundle's result may stand for a later request. Review-entry
CI of a server-placed Task requests it (a daemon-placed Task keeps
`legacy-daemon/1`: it always runs and is never reused).

**A canonical step runs exactly as an inline CI step runs today.** `bash -lc`
in the Task worktree; the owner process's environment, inherited, after the
login profile ran; the Project environment values on top; the machine build
budget and niceness; `GIT_DIR`, `GIT_WORK_TREE` and `GIT_INDEX_FILE` removed.
Nothing is cleared and nothing has to be declared, so a step that relies on a
variable from the server's environment or from a profile keeps working. A
service one step starts is still there for the next step. The policy does not
restrict what a step sees; it records it, and a result is reused only when
everything recorded is the same.

`build_check_spec` builds it when the caller sets `canonical_policy`, for the
entry-CI bundle in a server-owned Task worktree. A daemon-owned checkout, or a
bundle with no worktree, gets its owner's frozen policy instead (a clean
fallback at spec-build time: the check always runs and is never reused).
Asking for the canonical policy for any other family is a caller error.

What two runs with the same identity can still differ in:

| Input | How it is bound |
|---|---|
| Commit | In the identity (`commit_sha`). Witnessed: the owner re-reads HEAD before it starts (a moved HEAD is an infrastructure failure, as before), the executor records `prepared_head` before the first command and `finished_head` after the last. |
| Tracked changes | Witnessed. `git status --porcelain` must be empty before the first command and `git diff --quiet HEAD` clean after the last. Otherwise the result is the asking Task's verdict and is not reusable. |
| Untracked files Git does not ignore | Witnessed by the same `git status` before the first command: a run that starts with one is not reusable. A file a step leaves behind makes the next run on that worktree not reusable. |
| Ignored files (build output, installed dependencies) | In the identity, by worktree: a canonical spec is scoped to its workspace and placement generation (`CheckScope::Workspace`), so a result only ever answers for the worktree that produced it. Accepted limit: within that worktree, ignored files that changed since the run are not seen. The run is deliberately in the Task worktree and not in a clean checkout, because a cold build on every review is the cost this work removes. |
| Commands, order, working directory, per-command limit, stop rule, policy revision | In the identity (the spec digest). |
| Project environment values | In the identity twice: each declared key as an opaque salted revision, and the whole Project environment inside the environment revision below. |
| The owner's inherited environment | In the identity: `environment_revision` is a salted digest over every variable a login shell on the owner exports, by name and value, minus the denylist below. Read once per owner environment generation and retained as a durable salted revision; identity construction and owner execution reuse it. |
| Shell version, OS, architecture | In the identity (`shell_revision`; `ServerCheckExecutionInputs::identity`). |
| Forge version | In the identity: `runner_revision` is the policy name and the workspace version, so every release starts with no reusable result. |
| Machine | Not named. Only the server owner runs the policy, and every digest is keyed with a salt that lives in this installation's database, so a result cannot be found from another installation. A database moved to another machine reuses a result only if the whole inherited environment is identical there. |
| Tool versions behind an unchanged `PATH` | Accepted limit: not probed. A compiler upgraded in place is not seen until the Forge version, the environment or the commit changes. |
| The login profile's own non-determinism | The owner generation assumes stable exported profile inputs. Steps still run the login profile; a nondeterministic profile is outside the controlled-input reuse contract. Restart the server after changing its profile to renew the inherited answer. |
| Time and network (a step that downloads `latest`, a test that depends on the date) | Accepted limit: not seen. There is no per-Project switch to turn reuse off yet; one would be an optional field of the review config and is left to the cutover, where it first has an effect. |
| Machine build budget (five variables Forge adds when neither the operator nor the Project sets them) and niceness | Not in the identity: they bound how fast a step runs. If the operator, the profile or the Project sets one of the variables, that value is in the identity like any other. |
| Whole-run wall limit | Not in the identity (fixed per run when scheduled). A run stopped by it is never reusable. |

Left out of the environment digest (`check_executor::
ENVIRONMENT_IDENTITY_DENYLIST`), because a shell or the session that started
the server sets them per process and no build takes an input from them:

| Variables | Why |
|---|---|
| `PWD`, `OLDPWD` | the shell's own directory bookkeeping; the step runs in the checkout, the probe does not |
| `SHLVL`, `_` | set by every shell for itself |
| `COLUMNS`, `LINES` | the size of the terminal the server was started from |
| `TERM_SESSION_ID`, `ITERM_SESSION_ID`, `SECURITYSESSIONID`, `XDG_SESSION_ID`, `WINDOWID`, `TMUX`, `TMUX_PANE`, `STY` | the id of the terminal, multiplexer or login session the server was started from |
| `SSH_CLIENT`, `SSH_CONNECTION`, `SSH_TTY` | the address and port of the SSH connection the server was started over |
| `INVOCATION_ID`, `JOURNAL_STREAM`, `SYSTEMD_EXEC_PID` | per-start ids a service manager gives the server |

Everything else is in the digest, including `TMPDIR`, `SSH_AUTH_SOCK` and
anything else that changes when the server is restarted from a different
session: a restart into a different environment loses the stored results (a
miss) and nothing else.

**Values never leave the machine.** The digests are keyed with a random salt
the server creates on first use and keeps in `system_setting`
(`check_value_revision_salt`). Creation is one `INSERT OR IGNORE` followed by
a read, so concurrent first users agree on one salt. The key is not listed by
and cannot be written or deleted through the admin settings API, which is the
only surface that reads `system_setting` rows generically; it is in no event,
export, log line or MCP result. It survives a restart. Losing it changes
every identity: stored results are not found again and nothing else happens.
The probe's output is parsed in memory and never logged.

**The login-shell probe** (`check_executor::inherited_environment`) costs one
login shell per server/installation/environment generation, at first use. A
server start creates a new generation; restarting also picks up changed exported login
profile inputs. Generation ids choose the durable attestation
row, not the check digest: equal observed inputs still give equal identities
across restarts. Tool versions and profile-sourced mutable dependencies are not
probed; an in-place tool upgrade behind unchanged exported values remains
unseen, as before. **A profile change is not seen until the server restarts**:
before this, every identity build read the profile again. Until the restart
checks keep the identity read at start and an earlier pass is still reused.

The probe is bounded at ten seconds and its process group is stopped when the
bound passes. It returns a typed reason for a timeout, incomplete output,
excess output or failure to start. A missing answer returns
`ReviewError::CheckEnvironmentUnavailable` (carried by `ServiceError::Review`)
before an identity or consumer is admitted. Every caller is a Task step or the
check worker, so no REST request returns it: the Task states the cause. No `NotAttested` identity is substituted, no check executes before
the answer exists, and no passing Review can lose head evidence through a
probe timeout. What the Task does without an answer:

| Where | Bounded by | Landing |
|---|---|---|
| Review entry (`run_ci_steps`: the workflow's own entry, the owner's re-run, the owner's Retry of the entry) | `ENVIRONMENT_ATTEMPTS` (4) attempts of the hooks step, 1 s doubling back-off; the opened attempt and the hook checkpoint stay | The unstarted Review attempt is discarded (no Review, no review budget) and the Task states `before_work_hook_failed` / `review_ci_infrastructure` with the cause, whoever caused the entry (`interrupt_ci` annotates a user-triggered entry too). It re-enters review on the `review_ci_infrastructure` budget's back-off (5 entries); then `review_ci_infrastructure_exhausted` with `retry_hook` and an attention item. |
| Merge head (`request_check`, `settle` reuse) | `ENVIRONMENT_ATTEMPTS` (4) attempts of the Task step, 1 s doubling back-off | The step answers "no verdict" with the cause; nothing is asked of the check runner. The worker parks the attempt `infrastructure: check environment unavailable: ...` and the Task states `deferred` with that message. Counted retries (30 s, 2 min, 10 min), then `retries_exhausted: ...`, `owner_required` and the Retry offer; the owner's Retry starts the counted retries again. It never parks as `task_step_failed`. |

The first complete answer is stored in `check_environment_attestation`, keyed
by owner, environment generation and salt revision. Installation/generation
single-flight also covers independently composed DB handles: concurrent identity
builders do one successful probe and re-read its stored answer. The row holds
only salted digests (the inherited environment and the shell version), never a
name or a value; rows of generations that ended more than 30 days ago are
removed when a new one is stored. Identity construction and owner execution reuse that answer and do no
shell spawn. An unchanged Task's review entry plus merge previously used three
probes (entry identity, owner execution, head identity); a rebased head used
four. Both now use one on a cold generation and zero on a warm generation.
Project values are combined with the stored inherited revision under the
`forge.check-execution-environment/2` digest schema. Old digests simply miss.
The probe reads its own start/end marks and ignores the shell exit status,
which a logout script may replace.

**A changed Project environment is a miss.** Right before a run the owner
combines the stored inherited revision with the current Project values again.
If it is not the request's environment revision, the run still executes with
the Project environment in force and its receipt attests nothing.
`validate_receipt` accepts that receipt as a verdict and marks it not reusable;
it still refuses a receipt that attests something other than the request named.
A lost salt invalidates the stored answer's salt key and renews the attestation.
Frozen policies and a dirty checkout remain explicitly uncacheable.

**The process tree is stopped when the run ends**, not after each step. Each
step runs in its own process group and its descendants are left running when
it exits. After the last step (pass, fail, timeout or cancellation) and
before the final Git witness, the executor stops every step's group (TERM,
500 ms, KILL) and checks that no member is left
(`process_supervisor::stop_group`). `process_tree_stopped` on every command
of a canonical receipt is that verified answer for the whole run. A run whose
tree was not verified stopped is a verdict and is not reusable. Limits: a
descendant that left its group (`setsid`) is neither seen nor stopped, and a
server that dies mid-run leaves the tree to the operating system, as before.

**Verdict and reuse are separate.** `CheckResultEvidence.reusable` is set by
`validate_receipt` only for a pass that is attested, fully witnessed and
verified stopped; `finish_check_run` stores `check_result.cacheable` as "run
cacheable, certified and reusable". Everything else is delivered once to the
consumers of that run and never found by a later request:

| Request (same worktree, commit, spec, environment) | Outcome |
|---|---|
| earlier reusable pass | `Hit`: no run row, no slot, no process |
| a run is in flight | `Joined`: one run, one result per consumer |
| the same request key again, after its run ended | `Joined` to its own run and answered by that run's result, whatever it was |
| earlier pass was not reusable (tracked change, untracked file at the start, moved HEAD, unattested, tree not stopped) | `Scheduled` |
| earlier run failed, timed out, was cancelled or failed infrastructure | `Scheduled`: a red result is never reused, so a re-review of a red commit runs again and a flaky failure does not stick |
| another worktree, commit, command, Project value, inherited variable, shell, Forge version or policy | `Scheduled`: its own identity |

A consumer that joins a run in flight gets that run's verdict even when the
run turns out not to be reusable. Because the identity names the worktree,
only requests for the same worktree can join.

**Daemon placements** are never canonical. The builder falls back to
`legacy-daemon/1` and `CheckOwnerPort::prepare` refuses a canonical run on a
daemon placement as a second guard. For parity a daemon would have to probe
and attest its own inherited environment with a salt it keeps, return that
attestation with the receipt, and run the run-end tree stop; the attestation
is a change of the wire shape and therefore a daemon protocol bump.

Neither the spec revision nor the digest schema changes: the policy string is
already a digest input, so no stored result of a legacy policy can satisfy a
canonical request. No result was ever stored under `canonical-ci/1` before
this definition, so the policy revision stays `1`.

#### Review-entry CI on the check runner (3.3 stage D part 2)

Before configurable entry hooks, a persistent no-change guard compares the clean
workspace HEAD on its placement owner with the latest settled Review's judged commit
in the current Review budget window (passed/failed only; cancelled, running and awaiting-human rows are ignored), and acts only when that Review failed. A
later passing Review of the same commit suppresses an earlier failure. It applies
after a completed coder repair, and exempts owner-started moves, explicit owner Retry of a blocked entry, and owner-requested re-reviews. The first
unchanged repair spends `RepairNoChange` (fixed limit 1) and returns to the gate's
reject target with a brief headed "No change since review N". A second parks
`review_needs_owner` with `no change after repair round`. Neither creates a
Review, starts checks or a reviewer, nor charges Review or gate-rejection
budgets. Both post a Forge comment tied to the failed Review and keyed by the
review entry that decided it: a replayed entry posts nothing new, and a later
park of the same Review (after an owner send-back) posts its own. The
send-back states that HEAD still matches the failed candidate and names the
`repair_no_change` budget spent and remaining; the park names the open findings
and the owner's re-run review, mark reviewed with a reason, and cancel actions.
Dirty worktrees and changed commits continue through ordinary review,
as does an entry whose workspace owner cannot describe the checkout (an away
daemon): that entry meets the outage with the usual typed review-entry wait.
An entry retry carries the original source state and actor in its hooks payload,
so a same-state retry after reconnect still evaluates the repair gate and retains
owner exemptions. An explicit owner Retry retains that owner intent through a
subsequent offline interruption and automatic reconnect. The old step's continue
decision does not decide the new step.
A reminder after reconnect clears the resolved entry barrier and infrastructure
blocker before returning to the coder, just as a successful normal retry does.
The reminder is a `recovery` bridge (verb `repair_no_change`), which the cascade
worker lets out of a gate that requires owner approval.
The decision and reminder debit share an authority-fenced checkpoint at reserved
index -1, before Project hook ordinals, so crash resumption cannot debit twice or
reconsider an entry whose checks already started. A suspended entry admitted
before the guard shipped has no reserved decision: existing entry checkpoints
(a suspended hook opened its own first) make it record `Continue` without
inspecting Git. Exit checkpoints, and a wait recorded by an exit hook, do not
skip the guard. Explicit retry-window resets
reset this allowance with Review; ordinary review laps do not.

`run_ci_steps` no longer runs commands. In its hooks step it

1. opens the review attempt and records the authority it asks under (hook
   effect `ci_review`: the attempt id, the Task version and the Review row's
   status and `updated_at` at that moment; a woken or redelivered step
   continues that attempt and never opens a second),
2. builds the `EntryCi` check spec with `canonical_policy: true` and the
   worktree identity (a daemon placement gets its legacy policy from the
   builder and is never reused; blank `ci_steps` entries are dropped, and a
   spec with no command passes with no request), and asks
   `TaskCheckConsumers::request` once (hook effect `ci_consumer`) with
   origin `entry`, authority = the review attempt id and a 3600 s wall limit.
   A worktree that is dirty when the entry asks (uncommitted or untracked
   files) is not what the commit names: its identity is stated unattested,
   so the entry runs and its result is never reused,
3. reads `TaskCheckConsumers::wait_state`. A `Hit` already carries its result
   and is settled in the same invocation. A pending run (or exhausted
   infrastructure, see below) makes the hook ask the engine to suspend the
   step (`durable::suspend_hook`). A consumer nothing will answer any more
   (cancelled under a step nobody superseded, or its run or result row is
   gone) is asked for again: the attempt is closed
   (`superseded_by_withdrawn_check`) and the hook asks once more, by the
   mechanism and under the bound of a changed configuration (below). The
   same holds for a result whose outcome is `cancelled` (the entry joined a
   run that was being stopped for someone else). Past the bound the review
   attempt is cancelled and the hook fails with `review check was lost
   before it produced a result` instead of suspending again.

**A worktree that cannot be made again is a wait.** The entry check runs in
the Task worktree. When that directory is gone and the disk is under its
free-space floor, `prepare_workspace_for(.., Purpose::Check)` returns the
typed disk refusal (`WorkspaceUnavailable::DiskPressure`,
`placement::capacity_wait_of(..) == Some(CapacityWait::Disk)`) instead of
recreating it. `run_ci_steps` then discards the attempt it had opened (no
command ran), records the Task's disk capacity wait
(`Capacity{disk}`, the wait a claim takes) and suspends its hooks step on the
disk wait (`durable::suspend_hook_for_disk`: `awaited_consumer_id = 'disk'`,
`suspended_until` 30 s ahead, uncharged). No check consumer has that id, so
nothing is delivered to it: `wake_due_suspended` wakes the step at its
deadline, the hook runs again, and it either waits another 30 s or proceeds
(clearing the Task's disk wait) and asks for its check. It is never a failed
review. The readers below treat this step like any suspended step; the
queries that join `check_consumer` on the awaited id simply do not match it.
`run_before_work_hooks` waits the same way (see "Disk-pressure admission").

**A suspended hooks step is its own durable state.** `task_step.status =
'suspended'` (V202610100820) with `suspended_until` (the latest time it looks
again: wall limit + 120 s) and `awaited_consumer_id`. The step gives up its
lease, keeps its open hook checkpoint and is not charged the attempt. Readers:

| Reader | Suspended step |
|---|---|
| claim (`claim_step_lane`), predecessor ordering, `queued_admissions` | never a candidate, never blocks a later step: the delivery step, owner commands and queued effects run while CI runs |
| `pending_steps`, `TaskStepWorker::drain`, `TaskBusy` counts, operator queue depth | not counted: nothing in the step queue can advance it |
| `entry_hooks_pending`, the condition producer's hooks owner, `schedule_reads.queue_owned`, `schedule_has_owner`, stranded-hooks sweep, a preempting command's `preempting_hooks` | counted: the entry is still owned and its checks have not settled |
| `due_schedule_tasks` | due at `suspended_until` |
| preempting Cancel/Hold (`enqueue_step_in_tx`) | superseded like a pending entry-fenced step |
| retention (`prune_steps`) | never pruned |

Wakes move it back to `pending`: the awaited consumer's delivery
(`ReviewEntryChecks::apply` → `wake_suspended_hooks`, by consumer id), or,
ahead of every claim, `wake_due_suspended` for a passed deadline or a Task
that left the step's status entry (the ordinary entry fence then supersedes
it). The woken step runs `run_ci_steps` again; it reads the verdict itself,
because a delivery step cannot write into a step it has not claimed. The
delivery step no longer needs a priority to pass the waiting step, so check
deliveries are ordinary priority-0 steps again.

**Authority across the suspension.** The woken step settles under the
authority recorded when it asked, not under what it re-reads: the Review
write is fenced on the asking-time Task version and Review status /
`updated_at`. A Task version bump or a Review row change while the check ran
refuses the write exactly where the inline run refused it, and the attempt
is cancelled (`cancel_review_after_authority_loss`).

Settlement is otherwise unchanged: the result's commands become the Review's
`ci_steps` (same fields, 4096-byte tails; a result row that would exceed its
262144-byte cap gets shorter tails), then the same Review/Task projection,
comment and events as before. `TimedOut` and `Cancelled` cancel the review
attempt and fail the hook. Exhausted infrastructure is no verdict: the Task
is parked on the typed check condition and the step re-suspends at each
deadline until the owner's `retry` produces a result.

A step superseded while it waits never reads its result. The wait is recorded
on the step (`awaited_consumer_id`) in the transaction that stores the entry's
request (`check_run::record_entry_wait_in_tx`), not only when the step
suspends: a Hold or Cancel that stops the step after it asked and before it
suspended finds the same request to withdraw. (Before, that step had no
recorded wait: the consumer stayed live, its delivery found no step to wake
and the review attempt stayed `running` under the hold until the release.)
`review_entry::abandon_superseded_waits` cleans the wait up, in the preempting
Cancel/Hold command's own step (before the command acts) and when a woken step
finds its Task has left the entry:

- **Remote run.** When the run still executes on a daemon and this consumer is
  the only one waiting, its operation is registered as the step's running
  remote operation and put under the ordinary remote-cancellation fence
  (`remote_cancel::cancel_operations`): `workspace.cancel` is sent when the
  daemon is reachable, and otherwise a `pending_remote_cancel` marker keeps
  the workspace excluded (the Task's follow-up action parks on
  `pending_remote_cancel`, naming the machine) until the daemon reconnects and
  confirms. A still-running remote command can therefore not write into a
  workspace the Task has moved on from.
- **Consumer.** It is cancelled, so a late delivery is `Stale` and is applied
  to nothing, whether or not the Task's status epoch moved (a Hold keeps it).
  The consumer is cancelled *before* the run's dispatch record is read for the
  remote fence above, and the check worker looks for a waiting consumer once
  more after it has written that record and before it sends anything. A
  cancel that lands while the owner prepares (no record yet, nothing to
  fence) is therefore seen by the worker, which ends the run as cancelled
  without starting an operation; one that lands later finds the record and
  fences it. No ordering leaves a started remote operation unfenced.
- **Task condition.** The check wait is cleared (in the Task's own step).
- **Review attempt.** It is cancelled (`execution_retry.status =
  cancelled_authority_lost`).

The check worker looks every second whether a running dispatch still has a
live consumer (`CONSUMER_LIVENESS_INTERVAL`; the lease is renewed every 15 s)
and cancels a run nobody waits for, so its machine slot is freed promptly.
A `queued` run needs no worker for this: the write that withdraws its last
consumer sets it `cancelled` in the same transaction, so a run waiting behind
a full machine leaves `checks.by_state.queued` and `waiting_for_slot` at the
withdrawal, not when a slot frees.
An unreachable daemon keeps the run `uncertain` (slot and single-flight
identity retained) until it confirms the stop, is removed, or passes the
disconnected-owner bound. A Project is not deleted from under a live check
run: the delete guard counts unfinished runs (`project_in_use`,
`live_check_runs`). A forced delete has two phases. First, without changing
anything, it looks for a run dispatched to a daemon that is not connected and
not removed (`TaskService::project_deletion_unreachable_machines`): such a
run cannot be stopped now, so the request is refused naming the machine and
nothing is cancelled. The same look covers the running executions: one whose
provider is a machine that cannot be reached would refuse the delete in phase
two after earlier executions had already been cancelled, so it refuses here.
Only then does it cancel the consumers, settle queued
runs and wait (four seconds for the whole request) for the worker to stop
running ones. Phase two is all-or-nothing for the Tasks: when the delete is
refused after it cancelled (a run did not settle within the bound, a provider
refused, new work was admitted), `TaskService::resume_project_check_waits`
wakes every review entry whose consumer was cancelled, and each asks again
for its check instead of failing as lost. A machine that is gone for good is removed by its owner: the
worker settles the runs of a removed machine and the delete goes through. Should a run row disappear under its worker anyway,
the worker finds no consumer at its next look, tells the owner to stop (best
effort) and drops the run.

**A pause while the step is suspended.** The check is machine work already
asked for, so no pause stops it:

| Pause | The pending check | Afterwards |
|---|---|---|
| Project pause | runs to its end; the result is delivered and applied, the review attempt settles | the reviewer run is held by the pause; resume dispatches it. The check is not run again |
| Agent pause (coder, reviewer) | as above | the Task waits in `review` for its paused reviewer (`agent`); resume dispatches it |
| Task `hold` | not offered while the Task waits for its entry check, whether the check is queued behind a full machine or runs: the Task offers `cancel` only, which abandons the wait as described above. (The check wait is what parks the Task, also under a Project or Agent pause, so no `hold` is on offer beside it. A hold command that reaches such a Task below the offer list is accepted, because a hold may stop hooks in flight: it supersedes the hooks step, cancels its consumer and its review attempt, and a run nobody else waits for is stopped and gives its slot back. Its release runs the review entry again by itself (`resume_review_entry_after_release`), so a held Task never sits in `review` with its checks unrun. The same holds for an entry that waits for free disk.) | - |

A Project pause or resume (like any Project edit) moves the Project version,
which is the fence a hooks step was admitted under. A step that waited for a
check therefore re-reads the Project when it continues: while the Project
workflow and the entry's two merged state configurations are what the step
froze, it continues under the current Project version. An edit of the Project
workflow that leaves the entered state as it was (its hooks, gate, role and
configuration) counts as unchanged; an edit of the entered state ends the
step with a version conflict. Without this the
woken step failed, the attempt was cancelled, and recovery dispatched a
reviewer that had no CI result to read.

What changed while the step waited decides what happens to the result:

| Changed during the wait | The finished check |
|---|---|
| Nothing the entry was admitted under (a pause, a resume, a rename, a limit) | applied; the attempt settles |
| The Project environment | applied: the run that ran is the run that was asked for. Its result answers the old environment only (the environment is part of the check identity), so the next entry runs again |
| The entry's state configuration: the review configuration of the Project (`default_review_config`, with `ci_steps`) where the Task does not override it, or the Task's own (`task_state_config`; no Project version moves for this) | not applied. The woken step asks again for the current configuration, in the same review entry (below) |
| The Project workflow, in a way that leaves the entered state as it was (its hooks, gate, role and configuration) | applied; the step continues under the current workflow |
| The entered state in the Project workflow | not applied. The hook list the step froze is no longer that state's: the attempt is cancelled (`cancelled_authority_lost`), the Task parks `cascade_failed` in `review` and offers Restart, Request changes, Advance (override) and Cancel |

**A changed configuration asks again by itself.** `run_ci_steps` records the
merged configuration of the entered state it asked under (hook effect
`ci_config`). A woken step runs under the Project's current version and the
state's CURRENT merged configuration (`execute_hook_step`), and the hook
compares the two. When they differ and the attempt has no verdict yet
(`supersede_stale_ask`):

1. the stale consumer is cancelled (`review_entry::withdraw_consumer`): a late
   delivery is `Stale`, the worker stops a run nobody else waits for, and a
   run still executing on a daemon is put under the remote-cancellation fence
   as for a superseded step;
2. the attempt that asked is closed `cancelled` with
   `execution_retry.status = "superseded_by_config_change"`; the verdict of
   commands the entry no longer has is never written to it;
3. the re-request is counted (hook effect `ci_rerequests`) and the hook opens
   a new review attempt and asks once for the current configuration: one new
   consumer (request key `status epoch | new review id`), at most one new run
   (a certified pass of the same identity is a `Hit`, a run in flight is
   joined). The Task's check wait is replaced by the new consumer's.

The Task does not leave `review`, its status epoch and its hooks step are
unchanged, and no review budget is charged (no verdict was produced). The
change is noticed when the step is next woken: by the stale check's delivery,
or at the step's deadline. An entry asks again at most three times
(`ENTRY_CHECK_REREQUEST_LIMIT`); the next change ends the step as a workflow
change does (the typed `cascade_failed` park with its actions), so a
configuration that keeps changing cannot keep a Task asking forever.

**Liveness of the wait.** Every way the answer can fail to arrive ends in a
resume or a typed condition, never a silent wait:

| What happens | Outcome |
|---|---|
| check worker not running | the Task shows the typed check wait (`check`, phase `result` or `slot`) stated when it asked; the step looks again at `suspended_until` and re-suspends while the run is still pending |
| run or result row lost, consumer cancelled under a live step | the woken step asks again (at most three re-requests per entry); past that it fails the hook (`review check was lost...`) and cancels the attempt |
| delivery step dead-lettered or never enqueued | the step wakes at `suspended_until` and reads the verdict itself |
| daemon unreachable | the run stays `uncertain`; on reconnect the daemon's journaled result is looked up (the command is not run again) and the step resumes. Past the disconnected-owner bound the run is an infrastructure failure: retried, then the Task parks on `check` / `infrastructure_exhausted` (offers `retry`, `cancel`) |
| restart after the request, before the suspend | the redelivered step asks again with the same request key and gets the same consumer |
| restart while suspended, or after delivery before the wake | the run's lease expires and any worker takes it over; delivery or the deadline wakes the step |
| restart after the wake, before the settle | the step's lease expires, it is redelivered and settles from the stored result (or returns the result it already wrote) |

`wake_due_suspended` runs ahead of every claim as one `EXISTS` over the
suspended rows (an index search on `status = 'suspended'`, never a table
scan; asserted by `task_step_suspended_migration_preserves_...`) and updates
only when a row is due, so it cannot starve claims.

Review entry and the integration queue share the one check-witness slot of a
Task. They never collide: a consumer row carries its origin and is delivered
only to that origin's family, each family's `current_authority` names a
different object (the running review attempt with no CI evidence; the
`checking` integration attempt of that epoch), and the two occur in
different Task statuses, so a late delivery of one is `Stale` (epoch or
authority) before either family's `apply` runs.

`TaskStepWorker::drain` (tests and service utilities) drives the check
worker's sweep itself while the drained Task awaits an undelivered check, so
a drain still ends with the entry settled. A `TaskService` built without the
runtime composes the same check runtime (both families) through
`compose_checks` on first drain.

The owner supervision guarantees tested here cover normal cancellation, dropped
futures, retained receipts and restart reconciliation. Abrupt process death
between OS spawn and durable process-tree evidence, and escaped descendants,
are not proven by the runner; the stage-B process-supervisor limits still apply.

#### CI failure triage

Project `settings.review_ci_triage` defaults to `full`. `report` records the same
triage but preserves blocking routing; `off` runs neither extra check and preserves
the original stop-at-first-failure gate. Under `full`, candidate bundles collect
all CI steps, so a non-blocking early failure cannot hide a later failure. Each
failed command is rerun once in its candidate workspace; the rerun bundle is
uncacheable. A command passing on rerun is wholly flaky. When the rerun fails
again and shares a signature with the first run, signatures absent from either
complete run are `flaky`. A rerun failing with only other signatures (a fail-fast
runner stopping at another test) proves no flake: the command is consistent and
every signature goes to the base. Incomplete signature lists never prove a pass.

Consistent failures are compared with the candidate/target merge base, or the
exact target tip at the queue head. The base bundle has commit scope, distinct
`triage-base:*` command identities and `canonical-ci/1` policy. The same server
owner executes an `ExactCommit` checkout, copies Project assets, and uses the
candidate's Project and per-run build environment. It runs effective `setup_steps`
then CI through the last candidate failure. Commit, setup/CI commands, Project,
repository and attested environment determine sharing across Tasks. A completed
red base can be shared as comparison evidence only when its owner witnesses and
process settlement agree and its setup steps all passed (a run whose install
failed is executed again by the next request); it is never a certified or
reusable candidate pass.
Daemon placements and missing setup yield `base_unavailable`. Failed setup,
incomplete base CI, mismatched owner input receipts, or missing base signatures
for a signature-bearing red candidate yield `base_run_invalid`; consistent
failures remain unclassified and still block. A base step passing
makes consistent candidate signatures `introduced`; matching red-base signatures
are `pre_existing`. A signatureless candidate failure against a red base with
test signatures is `introduced` and blocks: the base reached test failures
while the candidate failed without comparable signatures. When the rerun
reports no signatures, a candidate signature the red base does not fail makes
the step `introduced` (the reason names it); candidate signatures that all fail
on the base, and a red base without comparable signatures, give
`pre_existing_unverified`, which still blocks.

Review details keep `ci_steps[].triage`, original nonzero exits and rerun/base run
references. Under `full`, only blocking steps route to repair; verify and reviewer
entry continue when every failure is flaky or pre-existing. Frozen conformance
keeps and honors non-blocking triage for the same CI id and command, rather than
running that command again without triage. Reviewer prose summarizes classification
counts, and the frozen check-input view shares a 20-signature display budget;
full classifications remain in the original Review and ledger. The ledger retains per-signature
`CI-<step>#<sha256-prefix>` findings with `ci_triage`; `flaky`/`pre_existing`
findings are minor with zero streak under `full`. Under `report`, they remain open
and blocking, retaining their classification in the ledger and repair brief. The
brief labels waived findings as non-blocking under `full`. Original check results are immutable failures.
An unchanged queue head whose Review passed by triage runs its own CI; the ordinary
certified-pass reuse path cannot consume a triaged failure. Queue settlement
triages against the target tip before granting a permit or authorizing send-back;
only a red result whose failing steps triage classified non-blocking becomes a
permit, so a timed-out or untriaged red result stays a check failure. A triaged
send-back message names the failures that block (under `report`, flaky and
pre-existing signatures too) and keeps the end of the first blocking step's
output (2 KiB), which its `MERGE-CI` brief carries alongside up to 20 failing
test signatures and a `+N more` count.

Both subcheck stages retain the original consumer family, status epoch and Review
or integration authority, with `ci-triage:rerun:` / `ci-triage:base:` authority
prefixes giving distinct idempotency keys. Delivery wakes the same suspended entry
or settles the same checking head. Cancellation and configuration replacement
withdraw the current subcheck; original failed evidence is never overwritten.
Durable consumer keys bound the rerun and base requests; there is no volatile
triage budget. A pending/running triage subcheck extends the head's 35-minute
check wait, bounded by the subcheck's own deadline. Exhausted triage infrastructure
retries settle as infrastructure and use the existing infrastructure park and
Attention, rather than waiting for a head timeout. Exact base checkouts copy the
Project assets; only their untracked paths are excluded from the initial
cleanliness witness. Tracked edits and unrelated files still prevent sharing.

Project Attention exposes `flaky_check` (command/signature, occurrence count and
Task ids) and one `base_check_failing` item per Project and command (latest base
commit, signatures or reason and run id). A newer red base updates that item. Replayed observations count once. Only a newer passing queue-head base observation of the target tip resolves
older incidents for that command; a cached older pass cannot resolve a newer
incident. Review-entry merge-base observations may open/update an incident but
never resolve it, even when an older merge-base finishes green later. Mission Control renders both as warnings. Neither incident is a
Task blocker or a certification of CI success.

#### Manual review re-run through the review entry (3.3 stage E)

There is one review path. The owner's review `retry` (offer reason
`review_checks_retry`, labels "Re-run Review" and "Recheck Review") and the
internal `rerun_review` command call `TaskService::rerun_review`, which does
not run anything itself:

1. It refuses unless the Task is in a review state (resolved with
   `db::budget::is_review_state`, including renamed review gates) and has a completed
   implementation candidate. It refuses `task_busy` when a review entry of the
   Task is still running (a hooks step is pending, claimed or suspended on its
   check) or a `reviewer`/`auditor` execution is running. The action system
   refuses earlier for the same states: it offers no `retry`, so the public
   request gets `action_unavailable`.
2. It closes the attempt it replaces. The newest Review, when still `running`
   or `awaiting_human`, is cancelled with
   `execution_retry.status = "superseded_by_rerun"`, so a human gate or a late
   reviewer cannot settle an attempt the owner asked to run again.
3. It re-enters that same review state with a same-state transition
   (`review_rerun_entry_with_authority`, exit hooks skipped) under
   `Actor::system(Workflow)` and the current Project workflow snapshot. This is
   a new status entry: a new hooks step, so a new review attempt and a new
   check request key (`status epoch | review id`). The entry's transition row
   and hooks payload carry `TransitionBridge::review_rerun(requested_by)`
   (`bridge_kind = recovery`, `bridge_payload = {"verb": "review_rerun",
   "requested_by": <Actor>}`): the workflow routes the entry, the bridge says
   who asked.

A Task whose `task_state_config` still names an auditor
(`review.auditor_agent_id`, read only by the retired pipeline) and has no
reviewer Agent assigned is refused with a conflict before step 2: the re-run
would otherwise pass on its checks where the old one refused. The key is
refused at Task create and update, and operator status reports open Tasks
that still carry it (`retired_review_auditor_config`). The Task's owner is
told on the Task as well: at start, `state_retired_auditor_notices` opens one
Attention item per such Task (category `review_risk`, dedupe key
`retired-auditor:<task id>`, the Task named in its details), which blocks
nothing and changes no Task. The item is a notice, not an incident: it is
written `acknowledged`, so it is listed to the owner and is no wake source
(an `open` item of this category is a blocker that the wake sweep hands to
the Project Agent, at the cost of a blocker wake per Task). It is stated once and resolved when the owner
removes the key (Task update) or the Task closes.

**A held Task is not re-entered.** `rerun_review` refuses a Task on hold
(`ParkReason::Held`) with a conflict, whoever asks: the owner is not offered
the re-run while the hold stands, and a system re-entry (recovery settling a
retired inline re-run, a queued `rerun_review` command, a budget reset) does
nothing: no attempt is superseded and no status entry, hooks step, check or
review attempt is created. `settle_retired_review_rerun` leaves a held Task
as it is (the stopped execution says why it ended and that the entry runs at
the release; the hold, not a failure, is what the Task shows).

**The release gives a review its entry back.** Two things can take the entry
of a held Task in `review` away, and `resume_review_entry_after_release` runs
it again at the release, as an automatic entry: (1) a hold command that
reached the Task while its entry hooks waited (for the entry check or for
free disk) superseded the hooks step, which `entry_hooks_preempted` reads
from the steps of the current status entry; (2) recovery stopped a retired
inline review re-run and did not re-enter the held Task. Without it the
released Task had no check and no attempt: with a reviewer it parked on a
refused dispatch, without one it showed the gate's approval. A hold in
`review` that stopped nothing (the reviewer was waiting or running) needs no
re-entry: the dispatcher gives the review its reviewer run again.

**Where a failed entry goes.** `review_failure_target` sends a red entry with
budget left to the review gate's `reject_target`, and to `in_progress` when
the gate names none. That fallback is why a workflow whose gate has no
`reject_target` still sends the Task back to work: "red CI and no remediation
target" cannot be built by nulling the field. A workflow has no remediation
target only when the state the failed review would go to does not exist in it
(the gate names a state the workflow lacks, or a custom workflow has no
`in_progress`). That transition cannot be made, so the failed entry parks
typed in `review`: `blocked` with kind `review_gate_failed`, reason `review
failed and the workflow has no remediation state`, an annotation naming the
missing state, and the blocked Task's owner actions (never a failed cascade).

**Budget.** `rerun_review` takes the requesting actor. The Task action passes
the owner (only the owner is offered the action); the budget-reset retry of a
coordination root passes its action actor; recovery passes
`system(CrashRecovery)`; a `rerun_review` command queued before v0.14 names no
requester and is the owner's. An entry the owner asked for spends no review
retry budget: `db::budget::review_rerun_entry` reads the latest entry into
`review` (the transition row with a status epoch; a recovery marker has none)
and the three charge points ask it. `failed_review_entry` (a failed entry
hook) charges nothing and decides the bounce from the budget as it stands;
`budget::transition` (the move out of `review` with `rejection`) admits neither
the review debit nor the `gate:review` debit, excluding its own row;
`review_verdict` (a failing reviewer) skips a Review that the entry opened
(created at or after the entry). An entry retry
(`retry_entry_barrier_with_authority`) is the same entry run again and keeps
the bridge. Every other requester's entry, and every later automatic entry,
spends as before.

**Stale verdicts.** A re-run reviews the same candidate again, so the failed
attempt it was asked to replace still names the current candidate.
`auto_cascade_on_review_pass` does not take a failed Review created before a
re-run entry for that entry's verdict. When the entry has nothing to run (no
checks, no reviewer, no user-approval gate), `auto_cascade_on_unconfigured_review`
first records the re-run as an empty passed attempt (`{"ci_steps": []}`), once
per entry, so the Task's latest Review is the re-run's.

From there the entry is the ordinary one: `run_before_work_hooks`,
`run_ci_steps` through the check runner (reuse of a certified green result for
an unchanged commit, worktree and environment; never of a red one; the
3,600-second wall limit and per-command timeouts; machine run-cap admission),
`carry_review_authority` (always skipped: a same-state entry is not a
merge-contention bridge), `dispatch_role_agent` for an assigned reviewer, and
the `after_enter` cascades. Because the entry runs under the workflow's
authority and not the user's, it is routed as an automatic entry: failed CI
settles through `review_failure_target` (remediation, or the exhausted-budget
blocker when the budget was already spent) instead of parking a user-managed
entry barrier, a Task without a reviewer passes on its checks instead of
waiting for a manual review, and a gate that requires user approval still
waits. Cancel, hold, a paused Project and a paused Agent act on it as on any
entry.

The retired pipeline (`review::ReviewRunner`) created a `reviewer` execution
with no Agent and a `review-owner:<uuid>` lease, ran CI under it without a
time limit, optionally ran an `auditor` child and wrote Review and Task
authority itself. Nothing creates such executions now. One left `running` by
an upgrade is recognised by that lease owner, which the reviewer execution and
its `auditor` child both carry (or, once the lease is released, by the
reviewer's `review-<candidate>.jsonl` log name and missing executor snapshot).
They are settled at start: `CrashRecovery::run_recovery` queues one
`settle_retired_review_rerun` command per Task that has any (identity-fenced,
not stacked by a second start), whatever the leases say, and the command stops
them (`stop_retired_review_reruns`) and settles the Task. One found later is
terminalized by `HeartbeatMonitor` when its lease expires. Either way, instead
of the reviewer failure cascade, `TaskService::settle_retired_review_rerun`
writes an error on the execution that says what it was and, once no other
leftover of the Task is still running, calls `rerun_review` (requested by
recovery, so it spends as an automatic entry). A Task whose entry is already
running needs nothing more (`task_busy` is not a failure here). If the entry
cannot run at all (no candidate), the Task is blocked on the stopped execution
(`internal_command_failed`, with the reason) and offers its recovery actions.

The reservation that pipeline made, `ReviewRepo::create_attempt_with_execution_and_lease`
(Task mutation `ReviewCreateAttemptWithExecution`), and the admission
exception for a `reviewer` execution without an Agent are deleted. A queued
mutation step of that kind left by an upgrade is closed as `superseded` by
`V202610110112`. The `review_runner` actor type on historical review events is
kept for reading.

### Native Task registry contracts (3.8 F)

Task scope reads (`task.summary`, `work.read`, `decisions.read`, `events.read`,
`memory.read`, `inbox.read`, `commitments.read`, `delivery.read`) use typed
`task_reads` specifications. Shared coordination reads use those same specs in
Main and Project scopes; defaults, null optional arguments, truncation and row
limit clamping stay in their handlers. Unknown argument fields are refused.

`task_records` owns `task.plan`, `task.worklog` and `task.evidence`.
The shared authority resolver derives a Task principal from the native session's
identity, Task, profile, workspace access and role. A Task session is admitted,
as it always was, while a running execution of that identity serves its role
(`worker` ↔ `worker`/`coder`/`executor`, `reviewer` ↔ `reviewer`/`auditor`,
`planner` ↔ `planner`). The principal carries an execution only when exactly one
does; with several, reads still work and records are refused. The execution's
recorded session id is not part of the match: a resumed execution carries its
parent's id from creation, and the Task session may have been rotated or
recreated since. The admitted execution ID and role cannot change within a turn. Selected-profile changes can revoke authority, and cannot widen
an admitted turn. Plan/worklog/evidence also recheck that execution at the write
boundary. Worklog never chooses the latest of several executions or writes a
comment with missing execution/role provenance, and a `correlation_id` reused
for a different entry is refused instead of answered with the first entry.
File evidence is read from that execution's workspace (the Task's when the
execution recorded none). Planner and implementation roles can stage a plan;
reviewers can record observations and cannot author a plan.

`task_lead` owns the five operations of a coordination root's lead
(`lead.read`, `lead.subtask`, `lead.recover`, `lead.handoff`,
`lead.request_input`): closed inputs with no field that names a root, a
parent or an actor, principal rule `TaskLead`, permission `task_read`. The
Task principal carries `lead_root_id` when its one running execution leads a
root (see "Root Tasks and ordered subtasks", Lead operations). The measured
cost in a lead session's tool prefix is 2,634 bytes (the propose tool goes
from 1,835 to 4,469 bytes); a test fails above 10,000 bytes and proves that
no other Task session is offered them.

`task_proposals` owns the Project Agent's eight-verb `task.action`,
`task.dependency`, `task.adaptive` and `task.propose` contracts. It reuses the
public typed Task action, assignment and governance inputs from `api-types`;
that crate still has no internal dependencies. `operation-registry` depends on
those shared value types, and never on services, persistence or a transport.
Typed bindings convert the Task creation/adaptive inputs directly to existing
commands. Command receipts, their canonical input bytes, reviewed objects,
retry budgets, queue eligibility, Task-step writes and settlement are unchanged.
Transaction-time current-policy/governance checks stay in the command boundary.

`available_actions` remains the only source of verb/state eligibility. The
parameter predicate used by Task action execution lives in the registry beside
its per-offer schema projection. Native Project `work.read` returns that schema
as `action_schema` beside each Task's current offers and version. It projects
those offers, including boolean choices and presets, rather than reconstructing
workflow or queue state. Task-run reads mark offers
`available_actions_informational: true` and return a null `action_schema`:
workers, reviewers and planners acquire no workflow mutation capability.

Native Task schemas and flat aliases are derived from only the admitted specs.
Aliases preserve nested fields for providers that strip undeclared properties;
the payload contract is described once and enforced from the canonical schema.
Native dispatch performs one typed Serde decode. Plan size is UTF-8 bytes,
worklog summary size is characters, and evidence requires one nonblank source.
Existing custom evidence kinds remain accepted and retain their label as
`other`. Names, Task action parameter names and provider wrapper normalization
are unchanged. Historical prepared calls keep their stored arguments and
fingerprints and still run current authority and domain checks on invocation.
