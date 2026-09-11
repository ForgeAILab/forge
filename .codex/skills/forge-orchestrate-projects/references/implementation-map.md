# Forge implementation map: Charter, Project planning, milestones, and evidence

This is the implementation map for the proposed Project Charter / durable
Documents / evidence-backed Milestones change. Use it for review and, only
after the user approves the Stage 1 change, implementation. It turns the Forge
authority references and change design into repository work, ordering,
invariants, and review gates. The Charter, active execution baseline, Task
workflow, and immutable release snapshot remain the authorities; this file is
not a second product specification or approval.

## Baseline inspected

The implementation must preserve the current singular-scope architecture:

- `crates/db/migrations/V071__singular_agent_chats.sql` through
  `V075__retire_legacy_room_runtime.sql` provide one account Main Agent Chat,
  one Project Agent binding/Chat per operational Project, durable Genesis, and
  retired Room quarantine. No new Room, participant, arbitrary thread, or
  recursive chat model is allowed.
- This change's forward-only schema is
  `V076__project_charter_milestones_media.sql`, immediately after
  `V075__retire_legacy_room_runtime.sql`. Keep V001–V075 immutable and do not
  allocate another migration for the same Charter/Project-artifact/milestone/
  media contract. Any later migration is a separate, independently reviewed
  change.
- Product Genesis is currently split across
  `crates/services/src/product_genesis.rs`,
  `crates/api/src/routes/product_genesis.rs`,
  `crates/api-types/src/product_genesis.rs`, and
  `web/src/features/product-genesis/`. It stores a V1 prompt/session and
  admits the first turn through the existing Main Chat. `ready_for_project`
  currently precedes ordinary `POST /api/v1/projects` creation and a later
  handoff; this change replaces that Genesis creation boundary with an exact
  user approval receipt and one create/bind/handoff transaction.
- Project creation is currently in `crates/api/src/routes/projects.rs` and
  `ProjectRepo::create_with_agent_binding` in `crates/db/src/sqlite/project.rs`.
  The Genesis link is currently recorded after the Project transaction and
  Main-to-Project handoff is handled separately by
  `crates/services/src/agent_chat_service.rs`; both boundaries must be
  consolidated for Genesis approval consumption.
- Current database access follows `crates/db/src/repository.rs` traits,
  `crates/db/src/models.rs` records, `crates/db/src/sqlite.rs` module wiring,
  and per-domain adapters under `crates/db/src/sqlite/`. Row mapping is
  manual; every schema change must update the migration, model, repository,
  adapter, and mapper together.
- Durable event history is `domain_event` (V060/V074) and the live delivery
  surface is `events::EventBus` plus `GET /api/v1/events`. Durable rows are
  authoritative and committed with mutations; SSE is a post-commit live
  projection/cache invalidation channel.
- Task media is currently V047 `task_media`, with routes in
  `crates/api/src/routes/tasks/media.rs`, persistence in
  `crates/db/src/sqlite/task_media.rs`, and files under
  `<data_dir>/media/<task_id>/<uuid>__<safe_filename>`. Existing Task media
  IDs, URLs, authorization, validation, and comments are public behavior.
- Current Rust API types live in `crates/api-types/src/`; generated bindings
  are under `web/src/types/generated/` and `web/src/types/generated/api.ts`.
  The frontend is React/TypeScript/Vite/TanStack Router + Query, with chat in
  `web/src/pages/ChatPage.tsx`, Genesis controls in
  `web/src/features/product-genesis/`, and project routes in
  `web/src/router.tsx`.
- `DESIGN.md` is the UI source of truth: semantic HSL tokens, Inter/system
  typography, 4px spacing, bounded media, responsive shell rules, visible
  focus/keyboard states, reduced-motion behavior, and truthful loading,
  stale, conflict, empty, error, setup-required, and permission-denied
  states. New milestone/evidence primitives must be added there before
  component code.

The internal WorkspaceLease path is concrete. V076 creates the
`workspace_lease` table with Project/Task plus exact Task version and execution
attempt, attempt-pinned repository binding, base ref, role, capability JSON, assigned
principal, capability-profile revision/digest, issuing principal, issue/expiry,
status, and version. `WorkspaceLeaseRepo` persists issue/read/revoke/expire
operations; `TaskService` creates the execution/lease pair atomically for
claims, resolves the Project's current primary Repo, checks same-Project
ownership plus Charter/baseline governance and Workspace-binding equality, and
verifies the active lease against the execution/principal/profile/capability
before launch and recovery. SQLite enforces one active lease per Task,
Project/Task, current Project primary Repo, execution Workspace, and
running-execution scope, exact Task-version/assignment/profile
predicates, and either the active user-approved baseline or the narrowly
read-only pre-baseline discovery/planning branch. The exact execution identity
is persisted as the lease operation idempotency key: replay returns the same
exact active grant and cannot allocate a second authority. The scheduler delivers it only through
the internal execution channel; runner verification is the delivery
acknowledgement, and no REST/MCP/chat surface receives the row.

Expiry/revocation is consumed by the heartbeat/recovery path: an expired grant
cancels and terminalizes a running attempt, records reconciliation, and the
normal retry path receives a new execution identity and lease. Terminal,
failed, cancelled, daemon-disconnected, and stalled paths revoke their grant.
The claim path canonicalizes executor/task-worker aliases to persisted
`worker`; reviewers remain `reviewer`. Keep the implementation/verification
items tied to tests and live proof, but do not describe the lease contract as
an unimplemented architecture gap.

Read before implementation: `AGENTS.md`, the Forge orchestration references
(`authority-and-effective-state.md`, `charter-and-handoff.md`,
`main-agent.md`, `project-agent.md`, `acceptance-scenarios.md`),
`docs/architecture.md`, and the approved change's `proposal.md`, `design.md`,
`tasks.md`, and delta specs.

## Non-negotiable implementation rules

1. Keep one global Main Agent and one Project Agent Chat per Project. Main
   performs discovery and bounded portfolio work; Project Agent plans and
   orchestrates only its bound Project; only assigned Task Workers/reviewers
   receive scheduler-issued Workspace leases.
2. Derive account, Project, Chat, binding, artifact, Task, media, and release
   scope from authenticated state. IDs supplied by a model are references to
   authorize, never authority. Deny cross-Project access before counts,
   snippets, filenames, checksums, URLs, or cursor construction.
3. User approval is explicit and principal-bound. Silence, “looks good”, a
   model action, Task completion, a green projection, or readiness alone is
   never Charter, baseline, waiver, or release approval.
4. Canonical payloads and rendered views have separate deterministic digests.
   Revisions, approvals, Decisions, readiness evaluations, release snapshots,
   evidence pins, and provenance are immutable/append-only. Mutable pointers
   use optimistic compare-and-swap with an expected version.
5. Every replayable mutation carries an idempotency/deduplication key and
   returns the original committed result on an identical replay. A failed
   transaction exposes no partial success.
6. Keep `TaskService`, the workflow engine, role assignment, Workspace,
   review, merge, validation, and retry-budget rules authoritative. Project
   Documents and Milestones may reference Tasks; they must not become a
   Markdown checklist, replacement state machine, or manually editable
   percentage.
7. Treat Profile text, user text, web/repository content, memory, handoffs,
   Task output, and media captions as untrusted data. Content guards run before
   persistence, cross-scope publication, context manifests, events, or logs.
8. Do not introduce Rooms, aliases, deprecated names, compatibility shims,
   `_v2` endpoints, hidden migration branches, or feature flags. Product
   Genesis V2 is a server-owned operating skill name, not a compatibility API.
   Ship the new semantics directly in dependency order; record the beta breaks
   in `CHANGELOG.md` under `Unreleased > Breaking`.

## Dependency-ordered delivery slices

The public change is one coordinated implementation. These slices are review
and dependency boundaries, not runtime rollout flags.

### Slice 1 — Charter authority boundary

Implement first:

- built-in operating skills and immutable instruction provenance;
- Genesis Charter drafts, readiness, diff, exact approval receipt, and
  supersession;
- existing-Project `charter_setup_required` adoption path;
- atomic `CreateProjectFromCharterApproval` with exact handoff and one target
  message/turn;
- Charter/Decision/Document references in context manifests and memory;
- Main Chat approval UI and explicit “no Project/handoff yet” truth.

The slice is complete only when a Main Agent cannot create a Project without a
valid receipt and the Project Agent cannot mutate after a mismatched handoff.
It must not expose an incomplete half-model to users.

### Slice 2 — Project planning and execution baseline

After Slice 1 is stable, implement:

- conditional typed Project Documents and immutable revisions/approvals;
- append-only Decision Log and scope-change/reconciliation records;
- bounded direct public research versus discovery Task routing;
- Project Agent actions and context assembly;
- execution baseline, adaptive envelope, Task provenance, and compact Project
  status.

Before a user-approved baseline, discovery/planning Tasks may be non-mutating;
implementation Tasks may be drafted but must not run or receive a write lease.
Within the approved adaptive envelope, safe Task splitting/sequencing is
allowed only when outcome, acceptance, risk, side effects, release policy, and
elevated operations are unchanged.

### Slice 3 — Milestone truth, shared media, and release proof

After Charter and Task references are authoritative, implement:

- outcome Milestones, acceptance checks, server readiness, user waivers, and
  immutable releases;
- in-place shared Project media metadata, evidence attachments, release pins,
  race-safe garbage collection, and audited redaction tombstones;
- rebuildable Project Overview, event consumers, evidence/release history;
- responsive browser acceptance and screenshot/video proof.

No slice may be hidden behind a feature flag. If the product is to ship these
as separate changes, revise and re-approve the governing proposal first.

## Crate and layer map

| Layer | Existing anchors | Implementation work |
| --- | --- | --- |
| `db` | `migrations/`, `models.rs`, `repository.rs`, `sqlite.rs`, `sqlite/project.rs`, `sqlite/agent_chat.rs`, `sqlite/domain_event.rs`, `sqlite/task_media.rs` | Add the forward-only schema, immutable row models, repository traits, transaction seams, exact CAS/idempotency queries, project-scope guards, media reference/pin locks, and row mappers. Add migration/file-backed fixtures. |
| `services` | `product_genesis.rs`, `agent_chat_service.rs`, `agent_chat_turn_worker.rs`, `context_manifest.rs`, `coordination_service.rs`, `embedded_agent_service.rs`, `task_service.rs`, `workflow/` | Add operating-skill rendering, Charter/Document/Decision/Baseline/Milestone/Media/Release services, atomic create command, readiness/release recheck, adoption, artifact context sources, scope policy, and rebuildable Overview projection. Keep `TaskService`/workflow as the only Task authority. |
| `api` | `routes/product_genesis.rs`, `projects.rs`, `agent_chats.rs`, `tasks/media.rs`, `events.rs`, `routes/mod.rs`, `state.rs` | Add authenticated, binding-scoped handlers and router entries for Charters, Documents, Decisions, Milestones/readiness/releases, Project media/evidence, and Overview. Remove the superseded Genesis field/action shape rather than adding an alias. Map `DbError → ServiceError → ApiError`. |
| `api-types` | `product_genesis.rs`, `agent_chats.rs`, `repo_review.rs`, `core.rs`, `requests.rs`, `lib.rs` | Define closed enums and typed request/response/projection envelopes, version/digest/idempotency fields, exact snapshot types, and cross-field validation. Export every public type. |
| `events` | `crates/events/src/lib.rs` | Add redaction-safe Charter/Document/Decision/Milestone/Readiness/Release/Evidence event contexts and stable event names. Publish only after the durable transaction commits; include IDs, versions, digests, and bounded outcomes, never bodies, secrets, or media bytes. |
| `mcp-server` | `tools/descriptors.rs`, `tools/handlers.rs`, `params.rs`, `state.rs` | Add only Project-bound typed artifact/Decision/Milestone/readiness proposals and exact user-approval surfaces where policy permits. Keep Main Task/repository tools absent and enforce scope again in services. |
| `forge-client` / CLI | `src/project.rs`, `src/task.rs`, `src/media.rs`, `src/client.rs` | Update callers if Charter/Milestone/media commands are exposed. Do not retain the removed Genesis creation payload or invent a compatibility command. |
| `web` | `pages/ChatPage.tsx`, `features/product-genesis/`, `features/agent-chat/`, `router.tsx`, `api/`, `types/generated/`, `pages/ProjectSettingsPage.tsx` | Add exact Charter review/approval, Project Overview, documents/decisions/milestones/evidence/release views, Query invalidation/SSE handling, and truthful navigation to the singular Project Chat. |

## Persistence map and migration contract

Add one new numbered, data-preserving migration after the current highest
migration (re-check the number immediately before creating it). A single
forward migration is easier to audit for the cross-table Genesis/Project/media
invariants; if implementation must split it, each file still gets a newly
verified number and no historical file is edited.

### Charter and operating-skill records

Add stable, Project-reparenting-safe records with constraints and immutable
update/delete triggers:

- `operating_skill` and `operating_skill_revision`: stable key, revision,
  schema/render version, canonical body, policy digest, content digest, and
  created/provenance metadata. Seed the server-owned built-ins exactly as:
  `forge.main.project-discovery/v2` and `forge.project.orchestration/v1`.
  Do not overload the existing Project `skill` table, which is a legacy
  Task/executor content surface.
- `project_charter`: stable Charter ID, nullable Genesis owner before handoff,
  nullable Project ID before attachment, current draft/current approved
  revision pointers, mode/maturity, lifecycle, and optimistic version. Enforce
  one Project and one Genesis owner per Charter and never allow re-parenting.
- `project_charter_revision`: monotonic per-Charter revision, base revision,
  lifecycle (`draft`, `proposed`, `approved`, `rejected`, `withdrawn`,
  `superseded` as applicable), schema-versioned typed content, exact rendered
  Markdown/view, render version, change summary, author/source provenance,
  canonical content digest, rendered-view digest, and timestamp. Revision
  bodies and digests are immutable.
- `project_charter_approval`: an immutable exact target/receipt containing
  approval type (`project_creation`, `charter_amendment`, or `adoption`),
  Charter/revision IDs, both digests, expected Charter version, approved name
  and slug, selected identity/profile/operating-skill/policy revisions,
  approving user/event/time, `active|consumed|revoked`, and idempotency key.
  Receipt lifecycle changes must be auditable and single-use.

Extend `product_genesis_session` with Charter/current-revision/approval
references and keep its public lifecycle `discovering → ready_for_project →
handed_off` or `cancelled`. A failed create attempt leaves the same ready
session and active receipt. Do not fabricate a Genesis approval for an old
handed-off session.

Extend Project/binding state with an explicit `charter_setup_required` status
and current approved Charter pointer/version as needed for CAS. Add the
operating-skill revision/policy digest selected by a Project binding. Compact
creation may create one `M1 — Deliver outcome` milestone in the same atomic
transaction; the Project ID must be shared by Project, binding, Chat, Charter,
milestone, and handoff.

### Project Documents, Decisions, baselines, and traceability

- `project_document` stores stable Project ownership, one typed kind, title,
  lifecycle, approval policy, current draft/current approved pointers, and
  optimistic version. `project_document_revision` stores immutable base,
  schema/render versions, canonical/rendered content and digests,
  change/provenance, and timestamps. Approvals/supersession retain historic
  revisions.
- The public DocumentKind set is frozen as exactly `research`,
  `delivery_brief`, `product_spec`, `design`, `architecture`, and
  `execution_plan`. The ordered milestone section of an `execution_plan` is
  the roadmap; `roadmap` is not a public kind or compatibility alias. Never
  emit both spellings.
- `project_decision` is append-only with effective lifecycle
  `active|superseded|invalidated`; keep unaccepted editor/proposal state outside
  that effective set. Store class `user_scope|project_implementation|policy|waiver`, question/context,
  options, selected outcome, rationale, principal/source, governing
  Charter/baseline, affected artifact/Task/Milestone IDs, optional supersedes
  ID, and timestamps.
- Add an execution-baseline bundle/revision and approval record if not already
  present. It must pin Charter and exact Document revisions, stable plan-item
  IDs, primary milestone, release policy, acceptance/evidence matrix,
  capability/risk classes, adaptive envelope, elevated operations,
  exclusions, rollback/recovery, and a digest. Only the interactive user may
  activate it.
- Add immutable Task governance links (or equivalent metadata rows) for exact
  Charter revision, baseline revision, plan item, applicable Document
  revisions, and Milestone. Link through existing `TaskService`; do not copy a
  mutable checklist into `task` or permit the Project Agent to claim delivery.

Use Project-local foreign keys/checks and indexes for every reference. A
reference to a Task, artifact, media asset, or milestone in another Project
must fail before the referenced row's metadata is returned.

### Milestones, checks, readiness, and releases

Add:

- `project_milestone`: Project-local monotonic sequence, optional unique
  display label, current definition revision, lifecycle
  `planned|active|ready_for_release|released|cancelled`, and optimistic version.
  Store one explicit versioned `primary_milestone_id` on the Project/projection;
  do not infer it from a per-row flag, sort order, or progress. Canonical identity is
  `(project_id, milestone_sequence)`, not the label.
- `project_milestone_revision`: immutable outcome, included/excluded scope,
  linked Charter/Document revisions, Task selection/dependencies, risks,
  acceptance checks, evidence requirements, known issues, change summary,
  content/render digests, and provenance.
- `project_milestone_check` and immutable check-result/readiness rows: stable
  check ID, required/optional, source kind (`task_validation`,
  `document_approval`, `manual`, `policy_waiver`, `media_evidence`, `git_ref`),
  expected result, exact source versions/digests, principal, timestamp, and
  pass/fail/missing/stale/waived outcome. A readiness evaluation must digest
  every input it observed and grants no authority.
- `project_release` plus immutable snapshot/reference rows: Project-local
  release sequence, milestone revision/digest, readiness ID/digest, approved
  Charter/Document/Decision revisions, Task IDs/versions/states, validation /
  review / bounded git refs, media evidence IDs/checksums/captions, waivers,
  known issues, actor/time, schema version, snapshot digest, and idempotency
  key. Each release record is immutable; corrections append the next milestone
  release revision (`Mxxx-rN`). Later work is live state or a later release and
  never mutates an earlier manifest.
- `project_release_media_pin` and redaction/tombstone rows. Pins are committed
  with the release and survive Task attachment deletion. Security/privacy/legal
  purge may remove bytes only through an audited action that preserves the
  release asset identity, checksum/digest, actor, time, reason, and
  unavailable/tombstone state.

Readiness must be recomputed from authoritative current records. A terminal
Task count alone never advances a Milestone. Release re-authorizes the user
and rechecks every version/digest under the same transaction before snapshot
and pin writes.

### In-place shared media metadata migration

Preserve bytes, storage keys, existing Task media IDs, and pre-deletion Task
URLs. Do not move files to a release directory and do not duplicate a binary.
Use metadata and attachment rows:

- Add `media_asset` keyed by a new internal asset ID with owning Project,
  original filename/content type/byte size/storage key, checksum, creation and
  deletion/GC state. For every existing `task_media` row, create exactly one
  Project asset pointing to the same storage key and bytes.
- Add `asset_id` to existing `task_media` (or an equivalent immutable mapping)
  while retaining each old `task_media.id` as the Task attachment identity.
  Keep Task list/retrieve/delete routes and response shape. A Task URL
  authorizes only through an active Task attachment; Project pins do not make
  a deleted Task URL valid.
- Add a Project-scoped evidence/attachment table keyed by asset, with optional
  Task/Milestone/source Task/run/validation/release links, caption, evidence
  kind (`screenshot`, `walkthrough_video`, `log`, `report`, `other`), supported
  acceptance-check IDs, author, checksum, created/deleted timestamps, and
  authorization metadata. A same-Project Milestone attach adds metadata only.
- Serve Project assets at an authenticated stable Project URL (for example
  `/api/v1/projects/{project_id}/media/{asset_id}`) without exposing storage
  keys. Existing `/api/v1/media/{media_id}` remains the Task-attachment URL.
- Task/workspace cleanup leaves active Task media unchanged. Removing a Task
  attachment makes its Task URL unavailable and marks the asset for GC only
  when no active attachment or release pin remains. A release pin keeps bytes
  and Project evidence available.
- Serialize attachment removal, Milestone attach, and release pinning in the
  database. A leased cleanup worker must re-check active references immediately
  before physical deletion and be restart/idempotency safe. A concurrent
  Task-delete versus Milestone-attach either commits the attach first or
  returns a typed conflict/not-found; never leave a live attachment to deleted
  bytes.
- Migration fixtures must cover empty/new/existing Projects, active and
  handed-off Genesis, duplicate filenames, archived/deleted Tasks, image/video
  media, missing files, checksums, and interruption/restart. Prove row counts,
  storage keys, bytes, and checksums remain intact.

## Canonical service flows

### Built-in role/operating skills

Implement pure renderers and server-owned revisions, not mutable prompt text:

- `forge.main.project-discovery/v2` activates only while Genesis is
  `discovering` or `ready_for_project`. It enforces the two-question policy,
  maturity-sensitive readiness, epistemic labels, bounded public research,
  name recommendation, typed Charter draft/diff, exact approval, explicit
  handoff outcome, and refusal of Main Task/repository/filesystem authority.
  Outside Genesis, the normal Main instruction is used; after cancel/handoff,
  V2 is not sticky.
- `forge.project.orchestration/v1` activates for the authenticated singular
  Project Agent binding. It verifies the handoff/Charter, resolves truth by
  authority domain, selects compact versus standard artifacts, routes research
  to discovery Tasks when files/code/experiments/private state/independent
  evidence are needed, records Decisions, manages traceable Tasks/Milestones,
  proposes readiness, and refuses repository access, cross-Project reads,
  self-validation, waivers, and release.
- Profile instructions may shape tone/expertise only below these contracts.
  Store the exact operating-skill revision, profile revision, policy digest,
  and rendered instruction/context-manifest provenance for every relevant turn.
  An operating skill revision change affects future turns only.

### Exact Charter approval and atomic Project creation

Implement a typed service command equivalent to:

```text
CreateProjectFromCharterApproval(approval_id, idempotency_key)
  -> project_id, project_agent_binding_id, project_chat_id,
     charter_id, handoff_id, target_message_id, target_turn_id
```

The SQLite transaction must:

1. Derive/authenticate the account and Main binding; lock Genesis, Charter,
   selected identity/Profile/operating-skill/policy, and approval receipt.
2. Verify the receipt is active/current, exact expected versions match, the
   canonical and rendered digests match, the maturity/mode is ready, the
   selected identity/Profile/skill/policy revisions are still eligible, and
   the exact approved name/slug is valid. On conflict, return a typed error;
   never silently substitute a name or revision.
3. Create exactly one Project, Project Agent binding, Project Agent Chat, and
   Charter attachment using one Project ID. Create the compact default
   `M1 — Deliver outcome` only when compact policy requires it.
4. Transfer Charter mutation authority to Project scope; create the bounded,
   server-signed handoff payload containing exact Charter revision/digests,
   approval provenance, settled Decision IDs, typed non-blocking unresolved
   items, safe research references, redaction summary, and target skill/profile
   revisions.
5. Append the immutable handoff, target handoff message, one queued target
   turn job, and matching durable domain events. Never copy Main transcript,
   hidden memory bodies, credentials, browser state, Workspace/path/token
   data, other Projects, or authority-bearing instructions.
6. Advance Genesis `ready_for_project → handed_off` and consume the active
   receipt in the same transaction. Commit only when all authoritative rows
   and events succeed.

Any failure rolls back Project, binding, Chat, Charter attachment, milestone,
handoff, message, turn, events, Genesis state, and receipt consumption. A
retry with the identical receipt/idempotency key returns the original IDs and
creates no duplicate. A later target model failure follows the existing
durable turn retry/failure lifecycle and does not roll back the committed
handoff.

Remove the old Genesis-only `product_genesis_session_id` creation bypass from
the generic Project request. Keep a separate authorized human/API path for
creating a Project explicitly in `charter_setup_required`; Main Agent creation
must consume the exact receipt through the typed command/action.

### Existing Project adoption and ownership transfer

Migration marks Projects without an approved Charter as
`charter_setup_required`; it does not synthesize approval from old chat,
Tasks, memory, or media. Such Projects retain readable/usable Tasks, Project
Chat, Documents, and evidence capture. Only release is blocked until a user
approves an exact adoption Charter.

The bound Project Agent may draft a `legacy_unverified` Charter from
authorized current facts and explicit unknowns, with provenance. The user
approves the exact revision/digests via an `adoption` receipt. Approval moves
the Project pointer with CAS and leaves source records as provenance, not
retroactive user decisions. Main cannot draft or approve a Project Charter
after attachment; later material changes are Project-local amendment proposals
requiring user approval and explicit `reconciliation_required` handling.

### Project planning, research, and traceable Tasks

- Charter readiness uses compact versus standard mode. Compact needs name,
  beneficiary/outcome, success boundary, explicit non-goals, constraints (or
  “none known”), and visible non-blocking assumptions/research. Standard mode
  resolves or visibly queues product, data/integration, security/compliance,
  accessibility, architecture/migration, operations/recovery, launch, and
  acceptance concerns.
- Direct Project research is limited to bounded public facts that fit the
  interaction; store source title/URL, retrieval time, claim, confidence,
  inference, limitation, stopping condition, and affected Decision/Document.
  Repository inspection, execution, experiments, deep/resumable synthesis,
  authenticated/private browser work, or independent evidence requires a
  discovery Task with a non-mutating capability profile (and a separate
  explicit user-authorized path for private state).
- Classify changes as clarification, in-envelope implementation Decision, or
  material Charter amendment. Append Decisions; do not rewrite history. After
  approved amendments or incompatible baseline changes, mark affected records
  `reconciliation_required` and explicitly retain/revise/cancel/invalidate/
  supersede each record.
- Every Project Agent-created Task goes through `TaskService` and carries
  immutable Charter/Document/baseline/plan-item/Milestone references, outcome,
  acceptance, dependencies, type, capability/risk class, and idempotency key.
  Task Workers/reviewers alone receive Workspace leases; Project Agent receives
  sanitized outcomes, validation, evidence, and bounded git references only.

### Milestone readiness and user-only release

- The Project Agent may create/update a Milestone definition and attach
  same-Project evidence. Multiple Milestones may be active, but Project stores
  one valid `primary_milestone_id` for Overview emphasis. The Project Agent may
  request readiness; it cannot attest, waive, or release.
- Forge evaluates every required check against exact current Task validation,
  Document approval, manual user checks, waivers, media relevance/freshness,
  known issues, and bounded git refs. Persist readiness ID/digest plus the
  complete source version/digest set. Failed or stale inputs keep the Milestone
  active with explicit reasons.
- A user-only release transaction re-authorizes the user, rechecks the
  readiness digest and every covered source, snapshots exact Charter/
  Document/Decision/Task/validation/git/evidence/waiver/issue inputs, pins
  media, transitions `ready_for_release → released`, and emits events. Release
  performs no merge, tag, deploy, external publish, or repository mutation.
- If snapshot/pin/event persistence fails, no release is visible and the
  Milestone remains ready. Replay with the same key returns the original
  release. Later Task/Document/caption changes do not rewrite the snapshot.

## Events and status projection

Use the existing durable `domain_event` transaction and post-commit EventBus;
do not create a second mutable status source.

Add redaction-safe event types/context for at least:

- Charter revision created, readiness changed, approval created/consumed/
  revoked, adoption required, and Charter superseded;
- Document revision/approval/supersession and Decision proposed/approved/
  rejected/superseded;
- baseline activated/reconciled and Task governance linked;
- Milestone created/updated/cancelled/readiness evaluated/reverted;
- evidence attached/removed, media asset retained/GC candidate/redacted;
- release snapshot created and released evidence tombstoned.

Each event includes authorized Project/entity IDs, versions, sequence/digest,
bounded result/status, correlation/causation, and dedupe identity. It never
contains artifact bodies, hidden prompts, credentials, cookies, Workspace
handles, filesystem paths, or media bytes. Event consumers must be idempotent
under cursor replay and restart.

Implement `ProjectOverviewService` (and, if a cached read model is needed, a
rebuildable projection keyed by the latest event watermark) from canonical
Charter, Document, Decision, Task/workflow, validation, Milestone, evidence,
and Release rows. Derive live Task counts and check states; never persist an
editable completion percentage. Keep live progress and immutable released
truth in separate response sections. A projection/cache error is shown as
stale/loading/error with retry and cannot alter readiness or release truth.

SSE may accelerate web invalidation, but mounted views still use bounded
polling/refetch like current chat and Genesis controls. Never rely on a lost
SSE event to make a completed response or release appear authoritative.

## Public API, types, and documentation synchronization

Every public change must land in four places in one implementation change:

1. Axum handler under `crates/api/src/routes/` (with authenticated
   scope/ownership and typed errors);
2. Rust request/response/domain type under `crates/api-types/src/`;
3. generated TypeScript under `web/src/types/generated/` (run the existing
   ignored `api-types` export test or update generated files deterministically);
4. `docs/api.md` endpoint/request/response/event/authorization documentation.

Use opaque keyset pagination and response field `items`; authorize before
query/count/snippet/cursor work. All side-effecting requests carry expected
version/digest and idempotency where replay is possible. Suggested public
resources from the proposed design (after user approval):

- Genesis Charter draft/history/readiness/approval under
  `/api/v1/account/main-agent/product-genesis/{session_id}/charter...`;
- Project Charter under `/api/v1/projects/{project_id}/charter...`;
- Project Documents/revisions/approvals and Decisions under their Project
  collections;
- Milestones/revisions/readiness and user release under the Project Milestone
  collection; immutable Release detail under the Project Release collection;
- Project media upload/list/retrieve plus the authorized owner/admin
  disposition routes `POST /api/v1/projects/{project_id}/media/{asset_id}/redact`
  and `POST /api/v1/projects/{project_id}/media/{asset_id}/purge`, and Milestone
  evidence association under Project-scoped media/evidence routes; retain Task
  media routes unchanged;
- `GET /api/v1/projects/{project_id}/overview` for the derived read model.

Freeze exact endpoint/action names in the user-approved design before implementation
and update all call sites together. The domain command is always
`CreateProjectFromCharterApproval(approval_id, idempotency_key)`; do not expose
a generic Main-Agent Project-create bypass or a compatibility alias.

Update MCP descriptors/handlers and `forge-ctl` only where those public
surfaces expose the new capability. Keep denied Main tools absent and enforce
again at service boundaries. Update `docs/architecture.md` for authority,
domain-specific Effective Project State resolution, migrations, media
retention, events, readiness, and release;
`docs/getting-started.md` for the new Genesis/adoption/release flow;
`docs/cli.md` if CLI commands change; and `CHANGELOG.md` with `### Breaking`
entries for (a) exact Genesis Charter approval/action and (b) Project-owned
media metadata/retention semantics for release-pinned bytes. The migration
does not move bytes or introduce a new on-disk layout. README should remain a
short landing page linking to the deeper docs.

## Frontend and DESIGN.md constraints

Update `DESIGN.md` before component implementation with primitives and states
for:

- Charter knowledge labels (`fact`, `user decision`, `research finding`,
  `assumption`, `hypothesis`, `open decision`), revision diff, exact digest/
  approval target, readiness gaps, conflict, and `charter_setup_required`;
- Milestone outcome/check/status surfaces, stale/failed/missing/waived/readiness
  states, one next action, and immutable release snapshot/history;
- bounded evidence gallery/video poster, caption/source/check linkage, media
  retention/redaction, and permission/error states.

Frontend work should follow these anchors:

- Extend `ProductGenesisControls.tsx`, its API/hooks/types, and `ChatPage.tsx`
  to display the exact Charter revision/rendered view/digests, material diff,
  readiness gaps, selected Project Agent identity/profile/skill revisions, and
  a separate explicit approval action. Never infer approval from chat text or a
  ready status. Preserve one Main Chat and “Continue with Project Agent”.
- Add Project Overview API/hooks and a lazy route/page (for example under
  `web/src/pages/` and `/projects/$projectId/overview`) that shows current
  Charter/vision, active primary Milestone, real Task/workflow counts,
  validation/check status, blockers, unresolved Decisions/risks, Document
  freshness, evidence gallery, and immutable release history. Keep a deep link
  to the singular Project Chat; do not create a local artifact/status store.
- Reuse existing `Card`, `Button`, `Dialog`, `Sheet`, `Badge`, focus, skeleton,
  error, and Markdown primitives. Use semantic tokens only: ember for current
  work/focus/action, warning for stale/conflict, destructive for failure,
  success for verified/released. Use Inter/system and 4px spacing; no new raw
  colors, generic shadows, or serif typography.
- At 1280px use an outcome/status column with a bounded evidence/decision rail;
  collapse to one ordered column at 768px; at 375px contain evidence in a
  bounded horizontal gallery, wrap identifiers/media titles, and create no
  page-level horizontal overflow. Images are bounded previews; videos show a
  poster/duration and explicit play/open control with no autoplay.
- Every interactive state has hover, active, visible focus, disabled/busy,
  loading, empty, error, stale/conflict, and permission-denied behavior.
  Announce status changes without stealing focus, support keyboard/screen
  readers, and respect `prefers-reduced-motion`. Cached/stale Overview data
  must say so and offer a safe retry.

## Verification, browser, and proof gates

Before claiming completion, run the repository gates in `AGENTS.md`:

- `cargo fmt --all`;
- `cargo build` and `cargo test` (including migration/file-backed tests);
- `cargo test -p api --test happy_path`;
- `cargo clippy --workspace --all-targets -- -D warnings`;
- `cd web && pnpm lint && pnpm typecheck && pnpm test && pnpm build`.

Add focused tests for:

- deterministic canonical/render digests, immutable revisions, draft/approval
  races, supersession, stale versions, name/slug conflict without substitution,
  adoption, Genesis re-parent denial, profile/skill/policy mismatch, and
  approval replay;
- atomic rollback/replay for Project/binding/Chat/Charter/handoff/message/
  turn/events/Genesis/receipt, exact bounded handoff redaction, no recursive
  Main response, no Main Task/repository action, and no cross-Project lookup;
- compact versus standard readiness, two-question protocol markers,
  epistemic separation, prompt-injection handling, direct-research limits,
  discovery Task routing, baseline/adaptive-envelope gates, Task traceability,
  Decision supersession, and reconciliation;
- every Milestone lifecycle edge, primary pointer CAS, required check
  pass/fail/missing/stale/waived, no auto-ready from terminal Tasks, user-only
  waiver/release, stale-readiness release rejection, snapshot/pin rollback,
  release idempotency, immutable post-release reads, and redaction tombstones;
- media migration row/file/checksum preservation, existing Task IDs/URLs,
  same-Project reuse without duplicate bytes, cross-Project denial before
  metadata leakage, Task-delete versus evidence attach races, release-pinned
  retention, unreferenced GC, restart reconciliation, and unsupported/oversized
  upload validation.

Run the black-box acceptance IDs in
`.codex/skills/forge-orchestrate-projects/references/acceptance-scenarios.md`,
at minimum AUTH-01–04, HAND-01–04, APPR-01–03, PLAN-01–04, CHANGE-01–05,
TASK-01–04, RES-01–02, MILE-01–07, MEDIA-01–05, and STATE-01–04.

The live browser gate is a real embedded/local flow, not a mocked dashboard:

1. Start with `make dev` (or the documented frontend/server targets) against
   `./test`; create a rough idea in the existing Main Chat.
2. Exercise bounded discovery/research, inspect the Charter diff and exact
   approval target, explicitly approve it, and verify one atomic Project/
   binding/Chat/handoff/turn result. Exercise failure and idempotent replay.
3. Verify Project Agent startup checks the exact handoff and cannot receive a
   Workspace; create conditional Documents/Decisions/Tasks and approve a
   baseline before any implementation dispatch.
4. Have an assigned Task Worker/reviewer produce the required test/build and
   browser validation records. Upload screenshot/video proof through Forge,
   reuse one Task asset as Milestone evidence, evaluate readiness, and release
   only through an interactive user action.
5. Inspect the immutable release digest/evidence/waiver/known-issue snapshot,
   mutate live Task/Document/media state, and prove the released view stays
   unchanged. Verify Task URL removal versus surviving authorized Project URL
   for pinned media.
6. Repeat key Overview states at 1280, 768, and 375 CSS pixels and keyboard /
   screen-reader navigation. Capture screenshot/video proof with the
   `forge-proof-media` workflow, attach stable Forge media references, and
   record IDs/revisions/digests, commands, expected/actual results, failures,
   stale states, waivers, and limitations in the acceptance report.

No release claim is complete until the Rust, web, API/type/docs, browser, and
proof gates agree with canonical Forge records.
