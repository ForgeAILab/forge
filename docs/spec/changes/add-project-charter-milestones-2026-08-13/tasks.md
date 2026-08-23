---
created_at: 2026-08-13T19:04:05Z
updated_at: 2026-08-23T05:41:03Z
---

## 1. Approval and dependency gate

- [x] 1.1 Obtain explicit approval of this proposal, including user-only approval for exact Charter revisions/material Charter supersession and final milestone release.
- [x] 1.2 Confirm completed `add-project-agent-federation-2026-08-12` and `add-product-genesis-chat-2026-08-08` behavior remains the baseline: one Main Chat, one Project Chat per Project, no Rooms, no Main Task management, no chat-agent repository Workspace, and no `handoff_pending` lifecycle state.
- [x] 1.3 Freeze public domain names and additive REST/event/action shapes from `design.md`; record any approved deviation before implementation.
- [x] 1.4 Separate the repo-owned cross-role engineering playbook from self-contained `forge-main-agent` and `forge-project-agent` operating skills; validate and forward-test all role boundaries before runtime implementation.

Frozen on 2026-08-13 with no deviations from `design.md`: document kinds are `research|delivery_brief|product_spec|design|architecture|execution_plan` (no `roadmap` alias); runtime skill keys are `forge.main.project-discovery/v2` and `forge.project.orchestration/v1`; milestone definition and instance lifecycles remain distinct; `ReadinessSnapshot` is the release candidate; REST resources use the exact paths listed in `design.md` without compatibility aliases.

## 2. API types and canonical serialization

- [x] 2.1 Add Rust API/domain enums and structs for Charter content/knowledge items/readiness, immutable revisions, content/render digests, single-use approval receipt lifecycle, supersession, and Project `charter_setup_required`; generate matching TypeScript types and update exhaustive clients.
- [x] 2.2 Add typed Project Document kinds/lifecycle/revisions/approvals (`research`, `delivery_brief`, `product_spec`, `design`, `architecture`, `execution_plan`) and effective `DecisionRecord` states (`active|superseded|invalidated`), principal, and decision classes; keep draft/proposal/rejection editor workflow separate; generate matching TypeScript types.
- [x] 2.3 Add Milestone lifecycle/definition/check/readiness, multiple-active state with explicit `primary_milestone_id`, immutable `Mxxx-rN` Release revisions, media asset/attachment/pin, evidence availability (`available|quarantined|redacted|purged`), and Project Overview projection types; generate matching TypeScript types.
- [x] 2.4 Implement deterministic schema-versioned canonical serialization/digests for Charter revisions, Document revisions, Milestone revisions, validation results, readiness evaluations, media metadata, and immutable `Mxxx-rN` Release snapshots.
- [x] 2.5 Add request envelopes with expected version/digest, principal/authorization provenance, and deduplication keys for replayable approval, creation, baseline activation, readiness, evidence, and release actions.

## 3. Data-preserving migration after V075

- [x] 3.1 Add a new numbered migration for Project Charters, revisions, approvals, current pointers, Genesis ownership, and one-time attachment to a Project; do not edit historical migrations.
- [x] 3.2 Add Project Document/revision/approval and append-only Decision Log tables with Project-scoped foreign keys, indexes, uniqueness, and optimistic versions.
- [x] 3.3 Add Milestone/revision/check/result and immutable Release snapshot/reference tables with Project-local sequence uniqueness, lifecycle constraints, and idempotency indexes.
- [x] 3.4 Add Project ownership/attachment/pin metadata keyed to existing media assets while preserving every existing asset ID, Task media ID, stable URL, metadata, authorization, storage key, and file byte in place; do not move/duplicate bytes or claim an on-disk layout break.
- [x] 3.5 Mark existing Projects without an approved Charter as `legacy_unverified`/`charter_setup_required` without fabricating a Charter/approval or blocking existing Project Chat, Task, evidence, or Document APIs; keep adoption unapproved until an explicit user approval.
- [x] 3.6 Add migration fixtures for empty/new/existing Projects, active/handed-off Genesis, duplicate filenames, deleted/archived Tasks, existing image/video media, and interrupted migration recovery; prove row/file counts and checksums are preserved.

## 4. Charter repository and service

- [x] 4.1 Add repositories/services for append-only Charter revision creation, maturity-aware readiness evaluation, revision diff, optimistic conflict, deterministic digest, and immutable provenance.
- [x] 4.2 Implement exact-revision, principal-bound user approval receipts bound to content/render digests and selected Project Agent identity/profile/operating-skill/policy revisions; add active/consumed/revoked state, single-use semantics, current-approved-pointer supersession, and rejection of silence/model actions, stale versions, digest mismatch, or non-ready revisions.
- [x] 4.3 Implement existing-Project adoption Charter proposal/approval without inferring user decisions from chat, Tasks, or memory.
- [x] 4.4 Implement one `CreateProjectFromCharterApproval(approval_id, idempotency_key)` transaction that locks/verifies and consumes exactly one active receipt, creates Project/binding/Project Chat, attaches/transfers the Charter, appends the immutable handoff/message/turn job and events, transitions Genesis directly `ready_for_project` → `handed_off`, and consumes the receipt; expose no `handoff_pending`, roll back all state on failure, and return the original result on replay.
- [ ] 4.5 Add unit/repository/concurrency tests for draft races, approval races, Project-create-versus-approval ordering, post-attachment Main denial, name/slug conflict without silent substitution, supersession, immutable historic reads, cross-Project denial, Genesis re-parent denial, digest determinism, and restart continuity.

## 5. Main Agent Product Genesis instruction and actions

- [x] 5.1 Add server-owned `forge.main.project-discovery/v2` as a deterministic pure renderer matching `design.md`: activation lifecycle, mission, epistemic labels, two-question policy, readiness/fast path, naming, research, Charter output, approval, handoff, refusal, and non-responsibilities.
- [x] 5.2 Render only bounded server context into the instruction and persist the exact immutable instruction revision/model/profile/context-manifest provenance used by each turn.
- [x] 5.3 Add typed Main Agent actions/tools for reading/drafting the active Genesis Charter, checking readiness/diff, proposing the exact receipt target, and creating the Project only by consuming that receipt; deny generic Main-Agent creation bypass while preserving separate authorized human/API `charter_setup_required` creation, and expose no Task or repository actions.
- [x] 5.4 Update Main Chat/Product Genesis responses so Charter changes, readiness gaps, approval candidates, creation outcome, and “no Project/handoff yet” remain explicit without dumping the full artifact every turn.
- [ ] 5.5 Test protocol markers, every maturity, maximum-two-question wording, fact/decision/research/assumption/hypothesis separation, naming authority, prompt-injection denial, stale approval, Main Task denial, and no alternate chat/Room creation.

## 6. Handoff and Project Agent startup

- [x] 6.1 Extend the immutable handoff payload with schema version, exact Charter revision/digest/approval, bounded settled decisions, typed unresolved items, safe research references, redaction manifest, and complete source/target provenance.
- [x] 6.2 Re-authorize and redact every source at publication; exclude full Main history, hidden memory, credentials, protected runtime/browser state, unrelated Projects, arbitrary file paths, and authority-bearing text.
- [x] 6.3 Make Project Agent startup fail closed on missing/mismatched/unapproved/unattached Charter references and admit no mutating action until canonical verification succeeds.
- [x] 6.4 Make Project creation and idempotent one-message/one-turn handoff admission one transaction; treat later Project Agent turn execution failure through the normal durable turn retry/failure UI without rolling back the already delivered handoff.
- [ ] 6.5 Add handoff tests for exact revision/digest, superseded draft, protected-content redaction, cross-Project reference, no authority propagation, failure/retry, and no recursive Main response.

## 7. Project Agent instruction, Documents, Decisions, and research

- [x] 7.1 Add server-owned `forge.project.orchestration/v1` as a deterministic pure renderer matching `design.md`: startup, domain-specific `EffectiveProjectState` (no global truth hierarchy), compact/standard fast path, research, Documents, scope change/`CharterAmendment`, Tasks, decisions/memory, milestones/evidence, communication, refusal, and non-responsibilities; prove conflicting Profile text cannot override it.
- [x] 7.2 Implement typed Project Document repositories/services for append-only revision/diff/approval/supersession, approval-policy enforcement, Project scope, optimistic concurrency, immutable Task/Release references, and safe Markdown/JSON render/export views.
- [x] 7.3 Implement append-only Decision Log candidate/editor proposal/approval/rejection workflow plus effective `DecisionRecord` state transitions (`active|superseded|invalidated`) with principal, decision class, source, and affected-record links; never expose candidate workflow states as effective DecisionRecord states.
- [x] 7.4 Add Project Agent actions/tools for authorized Document/Decision operations and current Project state; descriptors SHALL state their exact scope and SHALL not accept caller text as authority.
- [x] 7.5 Add configured Project-scoped public web research with source metadata and untrusted-content handling, plus hybrid-policy routing that requires discovery Tasks for files/code/experiments/authenticated or long-running/evidence-bearing work.
- [x] 7.6 Enforce artifact/baseline/milestone traceability on Task creation, require the user-approved execution baseline before repository-capable Tasks become runnable, preserve adaptive-envelope provenance, and preserve all existing `TaskService`, assignment, scheduler `WorkspaceLease`, validation, review, merge, and concurrency boundaries.
- [ ] 7.7 Test fast-path Projects, approval-required Documents, scope-change classification, decision supersession, direct-search limits, discovery Task routing, authenticated-browser denial, Project Agent Workspace denial, and Main/cross-Project denial.

## 8. Context manifests, LCM, memory, and commitments

- [x] 8.1 Add Charter, Document, Decision, Milestone, and Release revisions as revision-addressed `ContextManifest` sources with authorization, digest, inclusion reason, and included/summarized/omitted disposition.
- [x] 8.2 Assemble Main context from only active Genesis Charter state and bounded portfolio projections; assemble Project context from only the bound Project's current approved artifacts and relevant authorized drafts/open records.
- [x] 8.3 Make semantic memory and LCM summaries reference canonical artifact IDs/revisions instead of creating separately editable copies; identify stale references when the current pointer changed.
- [x] 8.4 Extend Project Agent commitments/inbox reconciliation for research, document decisions, Task outcomes, readiness, and release evidence without allowing projected status to close a commitment without authoritative evidence.
- [ ] 8.5 Test cross-scope ACL filtering before retrieval/count/ranking, stale-memory precedence, prompt-injection content, token-budget projection, binding/profile/session rotation, and restart/replay continuity.

## 9. Milestone, readiness, and immutable release services

- [x] 9.1 Implement append-only milestone definition revisions (`draft|proposed|approved|superseded`) and Project-local `Mxxx` sequences, optional labels, multiple active milestones, explicit `primary_milestone_id`, and milestone lifecycle exactly (`planned|active|ready_for_release|released|cancelled`). Model blockers, stale results, and `reconciliation_required` as typed projections/reasons while active, not lifecycle aliases; enforce cancellation, optimistic concurrency, and cross-Project reference denial.
- [x] 9.2 Implement principal-bound acceptance checks and immutable `ReadinessSnapshot` candidate records with ordered exact input manifests, event watermarks, policy/context refs, exact evidence attachment/digest references, and `ready|blocked|failed|stale` results over exact Task/validation, Document approval, manual check, waiver, media evidence, known-issue, and bounded git-reference versions; standalone readiness creates no pins; deny self-review/self-attestation.
- [x] 9.3 On standalone readiness, move an unreleased `active` milestone to `ready_for_release` only for a ready result; leave non-ready milestones `active` with typed blocker/stale/reconciliation reasons, and leave `released` unchanged for correction readiness. Invalidate stale snapshots after referenced Task/artifact/git/evidence changes; expose exact failed/missing/stale reasons and never auto-ready/release from Task completion alone.
- [x] 9.4 Permit only authorized, principal-bound users to record manual checks/waivers and final release; keep the Project Agent limited to proposing readiness/release with visible known issues and exact snapshot inputs and deny self-review/self-attestation/self-waiver/self-release.
- [x] 9.5 Require the release request to name the exact candidate `ReadinessSnapshot` ID/digest; re-authorize the user, recompute the exact same readiness digest, and re-check every readiness-covered source version inside the release transaction. On a match, atomically create the immutable release manifest `Mxxx-rN`, release-scoped evidence pins, lifecycle transition, and events; do not create another readiness snapshot. Keep `released` terminal and implement audited privacy/security evidence tombstones plus `evidence_unavailable` after purge.
- [ ] 9.6 Test definition-revision and milestone lifecycle edges, readiness freshness, active/non-ready/released-correction projections, failure between manifest/pin/event operations, duplicate release replay, self-release denial, immutable historic reads, waiver visibility, and source mutation after release.
- [x] 9.7 Fail closed when an active baseline's acceptance/evidence matrix does not match its pinned current milestone definition; exclude superseded-definition checks from live projections; and wake the bound Project Agent after current or already-checkpointed Task delivery to reconcile validation, evidence, and readiness without auto-release.
- [x] 9.8 Require an autonomous delivery-followup turn to commit a newer Project-scoped readiness evaluation before it may succeed; retry the same frozen turn with a server-owned corrective instruction when it returns prose only, exhaust the existing finite turn budget visibly, and preserve blocked/stale readiness plus user-only release semantics.

## 10. Shared media and evidence lifecycle

- [x] 10.1 Add Project-owned metadata/attachments/pins around existing media assets while preserving every legacy asset ID, Task media ID, `/tasks/{task_id}/media` and `/media/{media_id}` request/response/pre-deletion URL, authorization semantic, storage key, and file byte in place; do not move/duplicate bytes or claim an on-disk layout break; expose an authorized Project evidence URL as a projection.
- [x] 10.2 Add Project media upload/list/retrieve and milestone evidence attach/reuse/remove actions with content validation, size limits, safe filenames, checksums, captions, evidence kinds, source provenance, and acceptance-check linkage.
- [x] 10.3 Serialize attachment changes and release pins in database transactions; mark last-reference assets as garbage-collection candidates and add an idempotent scheduler-leased cleanup/reconciliation path that re-checks references before deleting bytes, including Task-delete-versus-milestone-attach and restart races; never expose the lease to chat agents.
- [x] 10.4 Serve image/video inline and other files as attachments through stable authenticated Project URLs; prevent cross-Project filename/checksum/URL/count leakage.
- [x] 10.5 Publish replayable media/evidence events with bounded identifiers and test duplicate filenames, misleading extensions, unsupported/oversized files, reuse without byte duplication, Task cleanup, release pinning, redaction, and restart stability.

## 11. REST, events, and public clients

- [x] 11.1 Replace the superseded Genesis Project-creation approval/action request with the exact approved Charter revision/digest contract across route handler, api-types, generated TypeScript, and all callers; add route handlers/types for Genesis/Project Charters, Documents/revisions/approvals, Decisions, Milestones/revisions/readiness/release, Project media/evidence, Releases, and Project Overview without a compatibility alias.
- [x] 11.2 Use opaque keyset cursors and `items` for lists, authorization-before-query, typed 409 version/digest conflicts, idempotent mutation outcomes, and redaction-safe errors.
- [x] 11.3 Add replayable domain events for Charter/Document/Decision/Milestone/Release/Evidence changes in the same transaction as authoritative mutations; keep consumers idempotent.
- [x] 11.4 Synchronize generated TypeScript, MCP/action descriptors where exposed, `forge-ctl` behavior if added, and public API documentation in the same implementation change.
- [ ] 11.5 Extend the canonical API happy path with vague idea → Charter approval → atomic Project/handoff → Document/Task → evidence → readiness → user release, plus failure/idempotency branches.

## 12. Main Chat and Project Overview web experience

- [x] 12.1 Update `DESIGN.md` before component code with Charter diff/approval, knowledge labels, Milestone outcome/check, Evidence gallery, and Release snapshot primitives and all default/hover/active/focus/disabled/loading/empty/error/stale states.
- [x] 12.2 Add Product Genesis Charter status/diff/readiness/approval controls to the existing Main Chat, retaining the two-question conversation flow and exact “Continue with Project Agent” navigation.
- [x] 12.3 Add Project Overview header/current-outcome rail, authoritative Task/validation counts, blockers, document freshness, open decisions/risks, one next action, evidence gallery, and immutable release history.
- [x] 12.4 Add Project Agent Chat deep links/actions for Document, Decision, Task, Milestone, evidence, readiness, and release proposals without creating a second chat or local truth store.
- [x] 12.5 Render images as bounded previews and videos with poster/duration and explicit play/open controls; never autoplay, and show caption/source/check linkage and evidence freshness.
- [ ] 12.6 Implement truthful loading/empty/stale/error/conflict/setup-required/active/ready/released/cancelled/permission-denied states and accessible keyboard/screen-reader behavior.
- [x] 12.7 Verify responsive layouts at 1280, 768, and 375 CSS pixels with no page-level overflow, reachable composer/actions, contained media, wrapped identifiers, and preserved global/project singular-chat navigation.

## 13. Documentation, quality, and live proof

- [x] 13.1 Update `docs/architecture.md` with authority ownership, Charter/Document/Decision/Milestone/Release state, shared media lifecycle, context/memory precedence, and failure/recovery invariants.
- [x] 13.2 Update `docs/api.md`, `docs/getting-started.md`, README links if needed, and `CHANGELOG.md` under `Unreleased > Breaking` for both the Genesis approval/action change and release-pinned media retention semantics; explicitly document legacy-unverified adoption, preserved Task route/URL/storage-key/file-byte behavior, no on-disk layout break, and release evidence availability/purge semantics.
- [x] 13.3 Run `cargo fmt --all`, strict workspace Clippy, focused/full Rust tests including canonical happy path, and web lint/typecheck/tests/build.
- [ ] 13.4 Run production browser accessibility/performance/design QA and exercise hover/focus/disabled/loading/empty/error/stale/conflict states at mobile/tablet/desktop.
- [ ] 13.5 Run a live embedded-agent acceptance: rough idea, bounded Main discovery/research, exact Charter approval and Project creation, handoff verification, Project Agent research/Documents/Tasks, Task Worker delivery/review, image/video evidence reuse, readiness, user release, and immutable post-release inspection.
- [x] 13.6 Capture and attach screenshot/video proof with stable Forge media URLs and write an acceptance report containing IDs/revisions/digests, actions, expected/actual outcomes, commands, and known limitations before review.
