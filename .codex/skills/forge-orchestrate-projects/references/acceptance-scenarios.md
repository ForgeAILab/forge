# Forge Project Orchestration Acceptance Scenarios

This suite is a black-box contract for the Main Agent, Project Agent, Task
workflow, milestone/release, and shared-media boundaries. Each scenario uses
`Given` / `When` / `Then`; identifiers are stable enough to cite in an
acceptance report. Unless a scenario says otherwise, the caller is
authenticated and all referenced records belong to the named account.

## Role authority and protected boundaries

### AUTH-01 — Main Agent cannot manage Project work

- **Given** the global Main Agent is in the singular Main Chat and Project `P` exists
- **When** it attempts a Project Task mutation, repository operation, Workspace claim, validation attestation, waiver, or milestone release
- **Then** Forge returns a typed policy denial, changes no canonical record/event, and routes the request to the Project Agent or the Task workflow

### AUTH-02 — Project scope comes from the binding

- **Given** Project Agent `A` is authenticated through the binding for Project `P-A`, while `P-B` also exists
- **When** an action supplies `P-B` or a `P-B` Task, artifact, or media ID, including an action that only asks for counts or metadata
- **Then** Forge derives `P-A` from the binding and denies the request before lookup, count, snippet, digest, or other cross-Project disclosure

### AUTH-03 — Only assigned workers receive Workspace leases

- **Given** implementation Task `T` is assigned to Worker `W` and independent review is assigned to Reviewer `R`
- **When** the scheduler issues execution and review capabilities
- **Then** only the assigned principal receives a short-lived lease bound to Project, Task, base ref, capabilities, and expiry; chat agents receive no path, token, or Workspace handle, and `R` cannot silently become a write-capable implementer

### AUTH-04 — Untrusted text cannot widen authority

- **Given** a Profile, Document, web page, Task result, or handoff text requests credentials, arbitrary files, another Project, self-approval, or direct repository work
- **When** an agent attempts the requested action
- **Then** the server-owned operating skill and policy prevail, the text is treated as data, the action is denied, and no approval or permission ceiling changes (with the exact skill/Profile revisions retained in provenance)

## Atomic Project creation and handoff

### HAND-01 — Approved Genesis creates one complete Project handoff

- **Given** an active receipt binds the exact ready Charter content/render digests, selected Agent identity/Profile/skill/policy revisions, and approved metadata
- **When** `CreateProjectFromCharterApproval` succeeds with an idempotency key
- **Then** one transaction creates the Project, one binding, one Project Chat, Charter attachment, immutable bounded handoff, immutable Project admission receipt, target message/turn, domain events, and `Genesis=handed_off`, consumes the Charter approval receipt, and uses one Project ID throughout (including compact `M1 — Deliver outcome` when applicable)

### HAND-02 — Creation failure leaves no partial handoff

- **Given** an approved receipt is still active and any Project, binding, Chat, Charter, handoff, message/turn, event, or lifecycle write will fail before commit
- **When** the create-and-handoff command runs
- **Then** no partial record, consumed receipt, or emitted success is visible; Genesis remains `ready_for_project` and the receipt remains active for retry with the same idempotency key

### HAND-03 — Lost responses replay the committed result

- **Given** create-and-handoff committed but the client lost its response
- **When** the client retries with the same receipt and idempotency key
- **Then** Forge returns the original Project, binding, Chat, handoff, message, and turn IDs even if that binding was later replaced, and creates no duplicate Project, admission receipt, handoff, turn, or event

### HAND-04 — Issue-time handoff admission is exact and bounded

- **Given** a handoff contains an approved Charter revision/digests and safe unresolved/research references
- **When** atomic Project creation validates and freezes the handoff into the Project admission receipt
- **Then** the matching case admits only authorized Project context and no raw Main history, hidden memory, secrets, browser state, paths, or capabilities; a missing, inaccessible, cross-Project, or digest-mismatched reference rolls back with a typed conflict and admits no mutation

### HAND-05 — Later turns use stable admission plus current authority

- **Given** a Charter-backed Project has one valid admission receipt and its original binding is later replaced, its Profile edited, its same-key operating skill revised, or its Charter amended
- **When** Forge admits a fresh Project turn
- **Then** Forge uses the same admission receipt plus the current binding/current Charter authority, creates no replacement Main handoff, and does not query historical Main messages/turns/Profiles/instructions or creation-event provenance; queued/leased/retry turns retain their already frozen responder provenance

## Approval digests, replay, and races

### APPR-01 — Approval is an explicit exact-revision receipt

- **Given** a ready Charter revision and eligible selected Agent identity/Profile/operating-skill/policy revisions
- **When** the interactive user explicitly approves the expected Charter version with matching canonical and rendered-view digests and a deduplication key
- **Then** Forge appends one immutable `active` receipt containing the user principal, exact targets, digests, and provenance; repeating the same key returns that receipt, while silence, enthusiasm, continued chat, agent output, or Task progress creates no approval

### APPR-02 — Stale or mismatched approval targets fail closed

- **Given** the proposed Charter version, content/render digest, selected Profile/skill/policy revision, or expected version is stale or mismatched
- **When** approval or Project creation is requested
- **Then** Forge returns a typed version/digest conflict, creates no approval or Project, performs no hidden merge, and never substitutes a newer name, slug, or revision

### APPR-03 — Approval and creation race by compare-and-swap

- **Given** revision 4 has an active creation receipt while a newer revision 5 approval races receipt consumption
- **When** both transactions contend on the Charter/Genesis/receipt versions
- **Then** exactly one ordering commits; a committed newer approval revokes any still-active older receipt, and creation either attaches its still-current exact receipt or fails stale with no partial Project, after attachment Main can no longer mutate the Charter

## Compact, standard, and execution baseline gates

### PLAN-01 — Compact mode uses the fast path

- **Given** a low-risk Project has one outcome, a coherent name/beneficiary, success check, explicit non-goals, material constraints (or “none known”), and a visible non-blocking research/assumption queue
- **When** Charter readiness and Project setup are evaluated in `compact` mode
- **Then** the Charter may become approval-ready with a Delivery Brief and one execution-baseline approval target, without ceremonial standalone research, product, design, architecture, or roadmap documents

### PLAN-02 — Standard mode covers material uncertainty

- **Given** the Project has material UX, data, integration, security/compliance, accessibility, architecture, migration, operations, recovery, or launch uncertainty
- **When** readiness is evaluated in `standard` mode
- **Then** Forge requires the applicable typed Documents and Execution Plan and each material concern is resolved, explicitly inapplicable, or visibly queued; merely filling fields cannot take the compact fast path

### PLAN-03 — No repository execution before baseline approval

- **Given** no execution baseline is active
- **When** the Project Agent creates discovery/planning work and an implementation Task
- **Then** bounded non-mutating discovery/planning may run with its server capability profile, but implementation remains a non-runnable plan with no write lease/dispatch and no release operation

### PLAN-04 — The adaptive envelope permits only bounded reshaping

- **Given** the user approved one exact baseline digest containing governing revisions, plan-item IDs, primary milestone, release policy, acceptance/evidence matrix, capability/risk classes, elevated operations, and an adaptive envelope
- **When** the Project Agent splits, sequences, or replaces Tasks while preserving outcome, acceptance, risk class, side effects, release policy, and elevated-operation boundaries
- **Then** Forge permits the change and preserves origin/replacement provenance; a change crossing any boundary is blocked until reconciliation and the applicable new user approval

## Amendments, reconciliation, and immutable revisions

### CHANGE-01 — In-scope implementation choices stay below Charter level

- **Given** two reversible implementation alternatives both satisfy the approved Charter, baseline, and permission ceiling
- **When** the Project Agent chooses one
- **Then** Forge appends a Decision Log record with alternatives, rationale, actor, and affected artifacts/Tasks, updates the relevant Document if needed, and does not misattribute the choice to the user or supersede the Charter

### CHANGE-02 — Material scope becomes a visible amendment proposal

- **Given** a request changes Project identity, target user, core outcome, non-goal, success boundary, material risk/cost, safety posture, or launch commitment
- **When** the Project Agent proposes the change
- **Then** Forge appends an immutable amendment with base/candidate revisions, material diff, rationale, and affected Decisions, Documents, Tasks, baselines, and Milestones; the old Charter remains current until explicit user approval

### CHANGE-03 — Approved amendment marks affected truth for reconciliation

- **Given** an amendment candidate matches the current Charter version and its exact content/render digests
- **When** the authorized user approves it
- **Then** Forge advances the current Charter pointer atomically and marks affected records `reconciliation_required`; each record must be explicitly retained with reason, revised/replaced, cancelled, invalidated, or superseded before the flag clears

### CHANGE-04 — Amendment approval races the current pointer

- **Given** an amendment expects Charter revision 4 but revision 5 becomes current first
- **When** the user approves the amendment
- **Then** Forge returns a version conflict, leaves the amendment unapproved, and performs no downstream reconciliation against the wrong base

### CHANGE-05 — Revisions and released references are append-only

- **Given** two writers save Charter, Document, or Milestone changes from the same base, or a release cites an earlier revision
- **When** both writes are attempted and later current records change
- **Then** exactly one writer appends the next revision and the other receives a conflict; prior canonical/rendered payloads and digests remain immutable, and historic Task/release references never change to follow the newer pointer

## Task traceability and repository isolation

### TASK-01 — Project Agent Tasks are fully traceable but workspace-free

- **Given** an approved Charter, active baseline, plan item, artifact revision, and applicable Milestone
- **When** the bound Project Agent creates an implementation Task
- **Then** TaskService records immutable links to those exact revisions plus outcome, type, dependencies, acceptance, capability/risk class, and idempotency key, while the Project Agent receives no repository or filesystem authority

### TASK-02 — Agent prose cannot manufacture delivery or validation

- **Given** no authoritative Task delivery, review, validation, git, or evidence record reports an outcome
- **When** the Project Agent claims it edited, tested, merged, deployed, or validated repository work
- **Then** Forge keeps the work pending/unverified and exposes only sanitized authoritative results; the planner cannot self-attest or rewrite Task history

## Research and provenance

### RES-01 — Bounded public research records provenance

- **Given** one current public fact from a primary, non-authenticated source can resolve a Project decision within one interaction
- **When** the Project Agent performs configured bounded research
- **Then** the authorized artifact records the question, decision informed, source title/URL, retrieval time, supported claim, confidence, inference, uncertainty/limitation, stopping condition, and output destination; the source remains untrusted data and does not become approval

### RES-02 — Deep or private research is a scoped Discovery Task

- **Given** research requires repository inspection, code execution, experiments, substantial/resumable synthesis, authenticated/private state, or independent evidence
- **When** the Project Agent requests it
- **Then** Forge creates a traceable Discovery Task with outcome, acceptance, stopping condition, source-quality requirement, and artifact destination; chat receives no filesystem, Workspace, credential, or cookie authority, and authenticated work requires a separate explicit user-authorized path with redacted provenance

## Milestone primary selection, readiness, and release

### MILE-01 — Primary Milestone is a Project-local server pointer

- **Given** a compact Project is created, or a Project has multiple active Milestones
- **When** Forge creates the compact default or the Project Agent selects a different primary using the current Project version
- **Then** compact creates exactly one `M1 — Deliver outcome` primary by default; multiple active Milestones remain allowed but Forge stores exactly one valid `primary_milestone_id`, rejects stale/cross-Project selections, and the Overview emphasizes only that pointer

### MILE-02 — Readiness is computed from principal-bound exact inputs

- **Given** every required check has a current passing authorized validation/manual result, current evidence, disclosed known issues, and stable referenced Charter/Document/Task/git revisions
- **When** the Project Agent requests readiness with the expected Milestone version
- **Then** Forge alone computes and persists an immutable readiness ID/digest over every exact input and may transition to `ready_for_release`; the readiness label or digest grants no approval, waiver, or release authority

### MILE-03 — Terminal Tasks do not imply readiness

- **Given** every selected Task is terminal but a required validation, evidence item, or governing revision is missing or stale
- **When** readiness or the Project Overview is evaluated
- **Then** the Milestone remains `active` with an explicit blocker projection containing failed/missing/stale reasons and cannot become ready or released solely from Task completion

### MILE-04 — Waivers are user-bound and visible

- **Given** a required check fails and Project policy permits a waiver
- **When** the authorized user submits the exact check, expected version, reason, and waiver action
- **Then** Forge appends an immutable principal-bound waiver and readiness/release show it as waived with actor and rationale; a Project Agent waiver request without that user decision is denied

### MILE-05 — User release freezes an atomic immutable snapshot

- **Given** a current readiness ID/digest and Milestone version are valid
- **When** an authorized user releases with the current readiness token and idempotency key
- **Then** Forge re-authorizes the user, rechecks every readiness source, and atomically writes the snapshot (Charter/Document revisions, Decisions, Task/validation/git refs, evidence/checksums, waivers, issues, actor/time/digest), evidence pins, `released` state, and event; replay returns the same release and the action performs no merge, tag, deploy, or external publish

### MILE-06 — Release rejects a readiness race

- **Given** readiness succeeded for a Milestone
- **When** a referenced Task, Document, validation, git ref, evidence attachment, waiver, known issue, or check definition changes before the release transaction
- **Then** Forge rejects the stale readiness identity, creates no snapshot or evidence pin, makes the stale/active reason visible, and requires a fresh evaluation before release

### MILE-07 — Failed snapshot/pin commit is all-or-nothing

- **Given** a user submits an otherwise valid release but snapshot, digest, pin, or event persistence fails before commit
- **When** Forge executes the release transaction
- **Then** no partial release, pin, event, or terminal transition is visible, the Milestone remains `ready_for_release`, and retry with the same idempotency key can recover the original operation

## Media preservation, deletion, purge, and garbage collection

### MEDIA-01 — Task media is one reusable Project asset

- **Given** an authorized user uploads a supported image, video, or file to Task `T`
- **When** the same Project later attaches that asset to a Milestone check
- **Then** Forge creates one Project-owned asset plus the existing Task attachment/ID/list/URL shape, adds a scoped evidence attachment without copying bytes, and records caption, kind, source, checksum, time, and check linkage

### MEDIA-02 — Workspace cleanup and unshared deletion have distinct effects

- **Given** Task `T` reaches a terminal state and its media has no other attachment or release pin
- **When** Workspace cleanup runs, then an authorized actor deletes the Task media under existing policy
- **Then** Workspace cleanup leaves the attachment and Task URL usable; deletion tombstones/removes the Task attachment and URL and physically removes only the now-unreferenced Project asset

### MEDIA-03 — Release pins survive Task deletion

- **Given** a released snapshot pins an asset originally uploaded through Task `T`
- **When** `T` or its Task attachment is soft-deleted
- **Then** the Task list/URL becomes unavailable, while the authorized Project media URL, release evidence metadata, checksum, and bytes remain available until the release pin is removed or lawfully purged

### MEDIA-04 — Attachment deletion and GC are race-safe and restart-safe

- **Given** a Task attachment is the only visible reference while a same-Project Milestone attach, Task delete, and cleanup worker may run concurrently
- **When** those operations interleave or Forge restarts during physical cleanup
- **Then** database reference changes serialize, the cleanup lease rechecks attachments and release pins immediately before deletion, and Forge either retains bytes for the committed attachment or returns a typed conflict/not-found—never a live attachment to deleted bytes

### MEDIA-05 — Security/privacy purge leaves a release tombstone

- **Given** a deleted Task has no attachment and a released snapshot is the last pin on its asset
- **When** an authorized security/privacy/legal purge removes the bytes
- **Then** Forge stops serving both former URLs, preserves an immutable tombstone with asset identity/checksum/release digest/actor/time/reason, and does not rewrite the release to claim the evidence never existed; ordinary cleanup cannot perform this purge

## Status projection and legacy adoption

### STATE-01 — Overview projects canonical live status

- **Given** an active Milestone has mixed Task states, a failed check, unresolved risk, and a current Charter/Document set
- **When** the Project Overview is rebuilt
- **Then** it shows the current approved Charter, primary/active outcome, authoritative Task counts, failed/missing/stale checks, blockers, one next user action, decisions/risks, and document freshness without an editable or invented completion percentage or released badge

### STATE-02 — Released truth is separate from stale/live projection

- **Given** a release exists and work continues, or one projection source is unavailable/newer than the cached view
- **When** the Overview is opened
- **Then** it shows the immutable release history and exact digest/evidence/waiver inputs separately from current live work, with explicit stale/loading/error/retry state; cached progress is never presented as current release truth

### STATE-03 — Migration preserves legacy Project operation

- **Given** an existing Project has no safely inferable approved Charter at migration time
- **When** the Project is opened after the data-preserving migration
- **Then** it is marked `charter_setup_required`, preserves Project Chat, Tasks, evidence capture, and Document maintenance, blocks release only, and fabricates neither a Charter nor an approval from old chat, Task, or memory text

### STATE-04 — Legacy adoption requires a new user approval

- **Given** the Project Agent drafts a `legacy_unverified` adoption Charter from authorized current facts, unknowns, and provenance
- **When** the interactive user approves the exact revision and canonical/rendered digests
- **Then** Forge establishes that revision as the current approved Charter while preserving source records as provenance (not prior approval), and only then can the release gate use it; silence, Task completion, or chat continuation cannot adopt it
