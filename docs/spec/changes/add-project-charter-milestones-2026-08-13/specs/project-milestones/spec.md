## ADDED Requirements

### Requirement: Outcome-Based Project Milestones

Forge SHALL support Project Milestones as outcome/release contracts with stable Project-local sequence identity (`M001`, `M002`, …), append-only definition revisions whose lifecycle is exactly `draft`, `proposed`, `approved`, or `superseded`, optional human-facing version labels, optimistic concurrency, multiple concurrently `active` milestones, an explicit `primary_milestone_id` pointer, and milestone lifecycle exactly `planned`, `active`, `ready_for_release`, `released`, or `cancelled`. Blockers, stale results, and `reconciliation_required` are typed projections/reasons while an unreleased milestone is `active`, not lifecycle aliases. A milestone SHALL define outcome, included/excluded scope, linked Charter/Document revisions, Task selection, dependencies, risks, acceptance checks, and evidence requirements. It SHALL NOT replace the Task workflow or use a manually editable percentage as truth.

#### Scenario: Project Agent plans a milestone

- **WHEN** the Project Agent proposes a valid milestone inside its bound Project
- **THEN** Forge assigns a monotonic Project-local sequence and appends its first immutable definition revision
- **AND** its optional display label may be software-like or domain-specific without becoming canonical identity

#### Scenario: Compact Project creation creates its primary milestone

- **WHEN** `CreateProjectFromCharterApproval` commits a compact Project
- **THEN** the same transaction creates canonical milestone `M001` with display label `M1 — Deliver outcome` and sets `primary_milestone_id` to it
- **AND** retries create no duplicate milestone or pointer; standard mode creates only definitions explicitly present in the approved Charter

#### Scenario: Milestone definition changes

- **WHEN** an authorized actor changes outcome, scope, acceptance, evidence, or Task selection using the current version
- **THEN** Forge appends a new definition revision and preserves prior revisions
- **AND** the revision uses `draft`, `proposed`, `approved`, or `superseded` independently of the milestone instance lifecycle
- **AND** released snapshots that cite an older revision remain unchanged

#### Scenario: Cross-Project Task is linked

- **WHEN** a milestone definition references a Task or artifact from another Project
- **THEN** Forge rejects the mutation before exposing metadata about that record

#### Scenario: Task completion reaches one hundred percent

- **WHEN** every selected Task is terminal
- **THEN** the overview updates its derived Task counts
- **AND** the milestone does not automatically become ready or released unless all acceptance/evidence rules independently pass

#### Scenario: Multiple milestones are active

- **WHEN** two or more milestones have independent outcomes and valid dependencies
- **THEN** Forge permits them to be `active` concurrently
- **AND** the Project stores an explicit `primary_milestone_id` identifying the one emphasized in the Overview without changing the others' lifecycle

#### Scenario: Primary milestone changes

- **WHEN** an authorized principal sets `primary_milestone_id` using the expected Project version
- **THEN** Forge verifies that the target milestone belongs to the Project and is `active`
- **AND** it changes only the presentation pointer, never milestone identity or release history

### Requirement: Server-Evaluated Milestone Readiness

Forge SHALL evaluate required milestone checks from authoritative Task validation, Document approval, manual verification, policy waiver, media evidence, and bounded git-reference sources. Every validation, manual check, waiver, and readiness result SHALL be bound to an authenticated principal, authorization basis, exact input/target digest, governing Charter/baseline/policy/check-definition revisions, expected version, explicit event, timestamp, and idempotency key. Workers SHALL submit work/evidence but SHALL NOT validate their own work; reviewers SHALL be independently assigned and SHALL NOT review their own authored work; the Project Agent SHALL propose but SHALL NOT self-review, self-attest, self-waive, or self-release. A standalone readiness evaluation SHALL persist one immutable `ReadinessSnapshot` containing the milestone definition revision, active baseline/release-policy references, ordered exact input manifest and event watermark, `ready|blocked|failed|stale` result, blocking reasons/check results/waivers, exact evidence attachment IDs/digests (not release pins), evidence availability, commit/build/check context, computing-policy revision, and readiness digest over every exact source version it observed. A ready result SHALL move an unreleased `active` milestone to `ready_for_release`; a non-ready result SHALL leave it `active` with typed blocker/stale/reconciliation reasons. Readiness for a correction SHALL leave a `released` milestone `released`. Standalone readiness SHALL create no release-scoped pins. Readiness state or identity SHALL NOT grant authority.

#### Scenario: Required validation is current and passing

- **GIVEN** every required check has a current passing outcome, required evidence is attached, and known issues are declared
- **WHEN** the Project Agent requests readiness evaluation with the expected milestone version
- **THEN** Forge persists an immutable `ReadinessSnapshot` and may transition the unreleased milestone from `active` to `ready_for_release`
- **AND** it records an immutable readiness identity/digest plus evaluated source IDs, versions, timestamps, and result digests

#### Scenario: Readiness is not ready

- **GIVEN** one or more required checks, evidence attachments, or referenced revisions are missing, stale, blocked, or otherwise non-ready
- **WHEN** the Project Agent requests standalone readiness evaluation
- **THEN** Forge persists the immutable non-ready `ReadinessSnapshot` and leaves the unreleased milestone `active`
- **AND** it exposes typed blocker, stale, or `reconciliation_required` reasons rather than changing the lifecycle to a diagnostic alias

#### Scenario: Delivery follow-up returns narration without readiness

- **GIVEN** Task delivery admitted an autonomous `delivery_followup` Project Agent turn
- **WHEN** the turn returns prose without committing a newer Project-scoped `milestone.readiness.evaluated` event
- **THEN** Forge commits no agent response and does not mark that turn successful
- **AND** it retries the same frozen turn with a server-owned corrective instruction under the existing finite retry budget
- **AND** a committed `blocked`, `failed`, or `stale` readiness evaluation satisfies reconciliation truthfully, while release remains an explicit user-only action

#### Scenario: Readiness evaluates a released correction

- **GIVEN** a milestone lifecycle is `released` and corrected inputs are proposed
- **WHEN** Forge evaluates standalone readiness for the correction
- **THEN** Forge may persist a new immutable `ReadinessSnapshot` for the candidate
- **AND** the milestone lifecycle remains `released` until a later user-approved correction release appends the next `Mxxx-rN`

#### Scenario: Readiness is principal-bound

- **WHEN** Forge records a validation, manual check, waiver, or readiness evaluation
- **THEN** it stores the principal, authorization basis, exact input/target digest, governing revisions, expected version, explicit event, timestamp, and idempotency key
- **AND** an unassigned reviewer, a worker validating its own work, or the Project Agent attempting self-attestation is denied

#### Scenario: Validation becomes stale before release

- **GIVEN** a milestone is `ready_for_release`
- **WHEN** a referenced Task, artifact, commit, or required validation changes so the readiness result is stale
- **THEN** Forge returns the unreleased milestone to `active` with explicit typed stale/blocker/reconciliation reasons
- **AND** the UI does not continue to present readiness as current

#### Scenario: User waives a check

- **WHEN** Project policy permits a waiver and an authorized user supplies the check, reason, and expected version
- **THEN** Forge records an immutable waiver decision linked to the check
- **AND** readiness and the eventual release prominently include the waiver rather than displaying an ordinary pass

#### Scenario: Project Agent tries to waive its own failed check

- **WHEN** the Project Agent requests a waiver without an authorized user decision
- **THEN** Forge denies the mutation and keeps the failed/missing check visible

### Requirement: Explicit Immutable Milestone Release

Only an authorized user SHALL release a `ready_for_release` milestone through the frozen release policy in the active user-approved baseline. The release request SHALL name the exact candidate `ReadinessSnapshot` ID and readiness digest. Forge SHALL recompute readiness inside the release transaction from freshly re-authorized exact inputs, require the recomputed digest to equal both the named candidate and request digest, and only then atomically create one immutable release manifest/revision (`Mxxx-rN`) plus release-scoped evidence pins. The release transaction SHALL NOT create another `ReadinessSnapshot`. The manifest SHALL freeze milestone revision/digest, label, summary/changelog, known issues, exact approved Charter/Document revisions, active baseline and release-policy revisions, included decisions, Task IDs/versions/states, validation/review outcomes, bounded repository/git references, evidence asset/attachment IDs and checksums, availability, release pin IDs/digests, waivers, releasing principal/authorization/event/time, schema version, and whole-snapshot digest. `released` SHALL be terminal; correction readiness leaves it `released`, and corrections append the next `Mxxx-rN` revision without mutating an earlier one. This action SHALL NOT merge, tag, deploy, publish externally, or grant repository authority.

#### Scenario: User releases ready milestone

- **WHEN** an authorized user submits the exact candidate `readiness_snapshot_id`, matching `readiness_digest`, milestone version, and deduplication key
- **THEN** Forge re-authorizes the user and verifies every source version covered by that candidate before atomically writing the release manifest, release-scoped evidence pins, milestone transition, and domain events
- **AND** replay returns the same release without another sequence, manifest, pin, or readiness snapshot

#### Scenario: Release recomputes readiness in its transaction

- **WHEN** an authorized user submits the exact candidate readiness snapshot ID/digest and release idempotency key
- **THEN** Forge reloads and re-authorizes every candidate-covered source and recomputes the exact same readiness digest before writing any release manifest or evidence pin
- **AND** a mismatch creates no manifest, release pin, or new readiness snapshot and leaves the unreleased milestone `active` with a typed stale/blocker/reconciliation reason

#### Scenario: Release races a source change

- **GIVEN** a milestone has a successful readiness evaluation
- **WHEN** a referenced Task, Document, validation result, git ref, evidence attachment, waiver, or known issue changes before the release transaction
- **THEN** Forge rejects the stale readiness identity and creates no release manifest, readiness snapshot, or evidence pin
- **AND** readiness is recomputed before another release attempt

#### Scenario: Project Agent attempts self-release

- **WHEN** the Project Agent invokes the final release action without an authorized user approval
- **THEN** Forge denies the transition and leaves the milestone `ready_for_release`

#### Scenario: Release records an already delivered outcome

- **GIVEN** authorized Task workflows have produced a commit, tag, deployment, or other delivery reference
- **WHEN** the user releases the ready milestone
- **THEN** Forge snapshots the bounded immutable identity/digest of that result
- **AND** the release action itself performs no repository or external publishing mutation

#### Scenario: Snapshot creation fails

- **WHEN** any snapshot reference, digest, or evidence pin cannot commit
- **THEN** no partial release is visible and the milestone remains `ready_for_release`
- **AND** a retry can use the same idempotency key after the fault is corrected

#### Scenario: Source records change after release

- **WHEN** a Task transitions, a Document is superseded, a branch moves, or a media caption changes after release
- **THEN** the released snapshot continues to report the exact recorded versions, digests, references, and captions from release time
- **AND** current Project progress is shown separately

#### Scenario: Release receives a correction

- **WHEN** the same milestone is later ready with corrected immutable inputs
- **THEN** Forge creates the next release revision (for example `M001-r2` after `M001-r1`)
- **AND** correction readiness leaves the milestone lifecycle `released` until that user-approved release commits
- **AND** the earlier release revision remains byte-for-byte immutable and addressable

#### Scenario: Released evidence needs security redaction

- **WHEN** an authorized security/privacy action removes access to released media bytes
- **THEN** Forge preserves an immutable tombstone containing asset identity, checksum, actor, time, and reason
- **AND** it does not rewrite the release to imply the evidence never existed

#### Scenario: Evidence is purged

- **WHEN** a mandatory privacy/security/legal purge removes bytes for a release-pinned evidence asset
- **THEN** Forge changes its availability to `purged`, retains only permitted tombstone/digest/audit metadata, and marks the release evidence `evidence_unavailable`
- **AND** readiness and Overview projections do not count that evidence as available proof

### Requirement: Shared Project Media Assets and Evidence Attachments

Forge SHALL reuse one Project-authorized media/blob layer for Task and milestone evidence. A binary asset SHALL be stored once and MAY have multiple scoped attachments. Migration MAY add a Project `MediaAsset` identity/mapping around a legacy row, but SHALL preserve every existing asset ID, Task media ID, URL, storage key, metadata, and file byte in place, without moving or duplicating bytes and without claiming an on-disk layout break. Existing Task media API identities, URLs, validation, authorization, and unpinned cleanup behavior SHALL remain valid. Milestone evidence SHALL use a stable Project-authorized URL, caption, evidence kind, source Task/run/validation when present, supported acceptance-check IDs, uploader, checksum, timestamp, and availability (`available`, `quarantined`, `redacted`, or `purged`). Standalone readiness SHALL reference exact evidence attachment IDs/digests and SHALL create no release pins; only a successful user-approved release transaction SHALL create release-scoped pins independently of Task attachment lifecycle.

#### Scenario: Standalone readiness does not pin evidence

- **GIVEN** a milestone has an evidence attachment with an exact content digest
- **WHEN** Forge evaluates standalone readiness and persists a `ReadinessSnapshot`
- **THEN** the snapshot references the attachment ID/digest and its availability
- **AND** no release-scoped evidence pin is created until the user-approved release transaction succeeds

#### Scenario: Reuse Task screenshot for milestone

- **GIVEN** an authorized Task in the same Project has an uploaded screenshot asset
- **WHEN** the Project Agent attaches it to a milestone acceptance check
- **THEN** Forge adds a milestone evidence attachment without copying the binary
- **AND** both Task and Project media views authorize through the same Project ownership

#### Scenario: Task with released evidence is deleted

- **GIVEN** a released snapshot pins an asset originally uploaded through a Task
- **WHEN** the Task is soft-deleted and its Task attachment is removed according to existing policy
- **THEN** the release's stable Project asset URL and evidence metadata remain available to authorized Project users
- **AND** the binary is not physically deleted while the release pin exists

#### Scenario: Unreleased unreferenced Task media is deleted

- **GIVEN** a Task media asset has no other attachment or release pin
- **WHEN** existing Task deletion policy removes its Task media
- **THEN** Forge removes the unreferenced stored binary as before
- **AND** it does not leave an orphan Project asset

#### Scenario: Evidence lacks relevance metadata

- **WHEN** a milestone requires walkthrough proof and an image/video has no caption or acceptance-check linkage
- **THEN** Forge may store it as a Project asset but does not count it as satisfying the required evidence check

#### Scenario: Cross-Project asset is attached

- **WHEN** a milestone references a media asset owned by another Project
- **THEN** Forge denies the attachment before revealing the asset's filename, checksum, URL, or attachment count

### Requirement: Truthful Project Overview and Release History

The web application SHALL expose a Project Overview derived from canonical Charter, Document, Decision, Task, validation, Milestone, Release, and Evidence records. It SHALL show Project identity/current approved Charter, all active milestone outcomes/states, explicit `primary_milestone_id`, one next user action, Task counts by authoritative workflow state, passed/failed/missing/stale acceptance checks, blockers, unresolved decisions/risks, document freshness, evidence gallery, and immutable release history. It SHALL distinguish live progress from released truth and SHALL preserve singular Project/Main chat navigation.

#### Scenario: Active milestone is incomplete

- **WHEN** the Project has an active milestone with unfinished Tasks and a failed check
- **THEN** the overview shows real Task counts, the failed check, blocker/reason, and next action
- **AND** it does not display an invented completion percentage or released badge

#### Scenario: Project has no Charter after migration

- **WHEN** an existing Project opens with `charter_setup_required`
- **THEN** the overview explains the adoption step and links to its singular Project Agent Chat
- **AND** existing Tasks remain visible and usable

#### Scenario: Released and current state differ

- **WHEN** work continues after a release
- **THEN** the overview displays the immutable released snapshot separately from current documents, Tasks, validation, and next milestone
- **AND** users can inspect each release's exact digest, evidence, waivers, and known issues

#### Scenario: Evidence is displayed

- **WHEN** an authorized user opens a milestone or release containing images and videos
- **THEN** images render as bounded thumbnails and videos expose a poster/duration and explicit play/open action without autoplay
- **AND** captions, source, acceptance linkage, and accessible names are available

#### Scenario: Responsive and accessible status view

- **WHEN** the overview is used at 1280, 768, or 375 CSS pixels or through keyboard/screen-reader navigation
- **THEN** header, next action, outcome, checks, decisions, evidence, and release history remain reachable and named
- **AND** long identifiers, labels, and media titles create no page-level horizontal overflow

#### Scenario: Overview data is stale or unavailable

- **WHEN** one or more projection sources cannot be refreshed or are newer than the displayed projection
- **THEN** the overview shows an explicit stale/loading/error state and safe retry behavior
- **AND** it does not present cached progress as current release truth

### Requirement: Milestone and Evidence Domain Events

Forge SHALL publish replayable redaction-safe domain events for milestone definition/lifecycle changes, readiness evaluation, release creation, evidence attachment/removal, and released-evidence redaction. Events SHALL include only authorized identifiers, versions, and bounded outcome metadata needed for projections; event consumers SHALL remain idempotent.

#### Scenario: Milestone becomes ready

- **WHEN** standalone readiness evaluation commits a `ReadinessSnapshot` and transitions an unreleased `active` milestone to `ready_for_release`
- **THEN** Forge publishes one event containing Project/milestone identity, version, and readiness outcome
- **AND** replay cannot duplicate a readiness transition or user notification

#### Scenario: Release commits

- **WHEN** the release snapshot transaction succeeds
- **THEN** Forge publishes a release-created event referencing the release and snapshot digest
- **AND** open Project Overview clients can refresh without receiving protected artifact or media bodies in the event
