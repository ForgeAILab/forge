# Milestones, Evidence, Readiness, and Immutable Releases

## Contents

- Milestone identity, lifecycle, and dependencies
- Baseline-frozen release policy
- Live progress versus verified truth
- Evidence and media records
- Readiness snapshots and atomic recomputation
- Release revisions and no-deploy semantics
- Availability, purge, and evidence loss
- Task deletion and race-safe media garbage collection
- Commit, build, check, and freshness context
- Required invariants and failure rules

## Contract and Authority

A milestone is a typed outcome and release contract. It is not a Task board, an editable completion percentage, or a claim that an agent believes is done. Every milestone, check, validation, evidence link, readiness snapshot, and release is Project-scoped and is authorized from the authenticated Project binding.

Use a stable milestone identity such as `M001` (the display form may be `M1`) for the outcome. Keep definition revisions immutable and reference the exact definition revision in the active execution baseline, readiness snapshot, and release manifest. A release revision is distinct from a milestone definition revision: `M001-r1`, `M001-r2`, and so on are immutable release snapshots of the same milestone identity.

The authority split is mandatory:

| Action | Allowed principal |
|---|---|
| Propose a milestone, dependency, check, evidence link, readiness request, or release candidate | Project Agent or user, within Project scope |
| Submit work, run output, or media | Assigned Task Worker, reviewer, user, or system through an authorized workflow |
| Attest a release-gating validation | Authorized independent reviewer, automated system check, or policy-permitted interactive user |
| Approve or activate the baseline and its release policy | Interactive user |
| Waive a policy-permitted gate | Interactive user, with a principal-bound waiver |
| Compute readiness/check freshness; pin evidence and allocate revisions during release; enforce retention | Forge system |
| Release a milestone | Interactive user through the typed release action |

The Project Agent may coordinate and recommend but may not approve a baseline, attest its own work, edit a check to make it pass, waive a gate, or release. A Worker may submit evidence but cannot validate work it performed. A reviewer cannot review its own assignment. Natural-language approval, Task completion, dashboard state, or a Project Agent's confidence is never a release authority.

## Milestone Identity, Lifecycle, and Dependencies

### Multiple active milestones

Forge may have several active milestones at once. Each active milestone has its own outcome, scope, dependency set, governing artifact and baseline revisions, Task selection, acceptance/evidence matrix, risks, and release policy reference. Do not merge active milestones into one synthetic status or choose one by sort order.

Persist `primary_milestone_id` as an explicit, versioned Project pointer. It is the one outcome emphasized in Overview and summaries; it is not inferred from creation time, highest progress, latest activity, or the first row returned by a query. When active milestones exist, the pointer must name exactly one of them. An explicit `null` is permitted only in a state where no milestone is active and must be displayed as “no primary milestone”; it never authorizes a release. Changing the pointer requires the expected Project version and an audit event. The pointer does not change the scope or readiness of any milestone.

### Lifecycle

Use the canonical Forge milestone lifecycle:

```text
planned -> active -> ready_for_release -> released
    |         |              |
    +---------+--------------+-> cancelled
```

- `planned`: a definition exists but is not active in the approved baseline;
- `active`: approved work may proceed; blockers, failures, staleness, and
  reconciliation are typed projections/reasons while the milestone remains
  active, not extra lifecycle aliases;
- `ready_for_release`: Forge currently has a passing readiness evaluation for
  the exact candidate inputs; the state grants no release authority;
- `released`: at least one immutable `Mxxx-rN` release exists; and
- `cancelled`: no new work/release may use the cancelled definition unless a
  later approved baseline explicitly adopts a successor.

Keep draft/proposed/superseded as milestone-definition revision states, not
public milestone lifecycle aliases. Keep a release candidate as an immutable
readiness/candidate record, not a `release_candidate` milestone state.

A release does not rewrite the milestone definition or turn every Task into a
completed Task. A correction appends a later readiness/candidate and immutable
release revision while the milestone remains historically `released`; it does
not need a second lifecycle spelling. A changed outcome, acceptance boundary,
dependency, or release policy requires a new definition/baseline revision and
reconciliation of affected records. Do not reinterpret a prior approval.

### Dependency graph

Dependencies are canonical records, not prose in a Task or chat message. Each edge names the source milestone ID and exact definition revision, target milestone ID and exact definition revision, dependency kind, required condition, owner, and creation/supersession event. Enforce:

- Project-local scope and authorization for both endpoints;
- no self-edge or cycle; dependency validation must reject a cycle before activation;
- explicit condition (`released`, a named check, an external commitment, or another typed condition), not “mostly done”;
- immutable dependency revisions with supersession/reconciliation when a governing milestone or baseline changes;
- dependency status derived from its authoritative source, never from a copied percentage;
- a release gate that fails or remains blocked when a required dependency is unresolved, stale, unavailable, cancelled, or outside the approved baseline.

External or cross-Project work may be represented only by a sanitized, immutable commitment/reference with an owner, source authority, expected revision/digest, and freshness rule. A URL or chat assertion is not a dependency proof. The effective dependency graph and its digest are inputs to readiness.

## Release Policy Frozen in the Approved Baseline

The active execution baseline is the only authority for release policy. It must include an immutable `release_policy_revision`, canonical content digest, and rendered digest where a user reviewed a rendered policy. The baseline freezes, at minimum:

- milestones, exact definition revisions, `primary_milestone_id`, and dependency rules;
- required acceptance checks and check-definition revisions;
- required validation principals and independent-review rules;
- evidence kinds, required contexts, freshness windows, and availability rules;
- allowed manual attestations and waiver conditions;
- stale-input and equivalence rules;
- elevated or irreversible actions, rollback/recovery expectations, and non-claims;
- the readiness aggregation policy and release revision allocation rule.

Do not read a mutable project default, current UI setting, Task description, or model recommendation during release. A change to any release-gating policy, required check, waiver rule, evidence requirement, dependency condition, or freshness rule is a baseline change. It requires a new user-approved baseline; affected Tasks, validations, evidence, milestones, readiness candidates, and release candidates become `reconciliation_required` or stale as applicable. The old baseline remains the governing authority for releases that already used it.

Every readiness snapshot and release manifest records the exact baseline ID/revision/digest and policy revision/digest. A policy mismatch is a hard stale/conflict condition, not an opportunity to merge the two policies.

## Live Progress Versus Verified Truth

Expose two separate projections and label them clearly:

**Live progress** is a rebuildable projection from the latest server-accepted Task revisions/events, dependency states, queued/running work, and known blockers. It may report counts, phases, or a coarse estimate, but no editable completion percentage is canonical. A Task marked done means the Task workflow accepted a terminal state; it does not mean its acceptance check passed.

**Verified release truth** is the system aggregation of principal-bound validation attestations and automated checks against the exact milestone, baseline, check-definition, commit/build, and evidence inputs. It must distinguish at least `pass`, `fail`, `pending`, `blocked`, `stale`, `unavailable`, and `waived` (where the frozen policy permits a user waiver). A missing or unavailable required check is not a pass. A waiver is shown as a waiver with its user, reason, policy basis, and digest; it is never summarized as an ordinary pass.

Status, Overview, chat summaries, and memory are navigation projections. They must be rebuildable from Tasks/events, validations, evidence, milestones, baselines, and releases. Never derive readiness from the progress projection or claim an immutable release fact from “100% complete.”

## Evidence and Media Records

### Three-layer model

Keep bytes, contextual links, and historical retention separate:

1. **`MediaAsset`** is the immutable content identity. It has a stable Project asset ID, original byte/content digest, byte size, content type, display metadata, the existing stable storage key, and Project ownership. The bytes and original digest are never edited in place. A migration may add this Project identity around an existing Task-media row; it does not repurpose or replace the existing Task-media ID.
2. **`EvidenceAttachment`** is a versioned Project-scoped link from an asset to a milestone, acceptance check, validation/run, Task, build, commit, or artifact. The originating Task attachment retains the existing Task-media ID and route identity. An attachment records its caption and evidence kind, supported requirement/check, source Task/run/build/git references, capture time, author principal, attachment digest, and expected context. Attachment and detach operations use optimistic versions and do not copy bytes.
3. **`EvidencePin`** is an immutable release-scoped reference to the exact attachment and asset digest, with the release's captured context and availability at pin time. A pin is a retention root. It preserves what the release relied on even if the live attachment, Task, or current milestone projection later changes.

One same-Project asset may have many attachments and pins across milestones, checks, Tasks, and releases. Reuse the asset and add a contextual link; do not upload, re-encode, move, or duplicate the bytes merely to make release evidence. A screenshot or video without a supported check and exact provenance is media, not proof.

### Context required for proof

An evidence attachment used by a gate must capture the exact source context, including as applicable:

- `source_task_id`, immutable Task revision/event or result ID, assignment/reviewer scope, and run/attempt ID;
- immutable attempt-pinned Workspace/lease repository binding plus commit/tree digest (never only a branch name), and the relevant artifact/document revision;
- `build_id`, build/input artifact digest, build-definition or pipeline revision, runner/toolchain/platform context, and result;
- `check_definition_id` and revision, release-policy revision, validation/run ID, runner, principal, start/finish times, result, and input-manifest digest;
- `asset_id`, original content digest/checksum, content type/size, capture time, evidence kind, caption, and supported acceptance requirement;
- milestone ID/definition revision, baseline ID/revision, and release-candidate/readiness identity when captured for a candidate.

The system may redact protected context in a model projection, but the canonical record must retain safe IDs/digests and authorization provenance. Do not put credentials, Workspace handles, raw browser state, or protected runtime traces in evidence metadata.

### Preserve legacy media identity during migrations

Existing Task-media IDs, pre-deletion API URLs, and storage keys are durable public-surface references. A numbered data-preserving migration may add a separate Project `MediaAsset` identity plus attachment, pin, digest, availability, or ownership metadata around existing rows, but must preserve each old Task-media ID, URL behavior, storage key, and physical byte location. Never move or duplicate bytes, rewrite old URLs, create a copied “release” file, or re-upload legacy media as part of migration. The old ID identifies its Task attachment; the new asset record references the existing storage key in place and records adoption provenance. Under existing deletion semantics, removing that Task attachment makes its Task-scoped URL unavailable, while an independently pinned asset remains reachable through its authorized Project asset URL.

Migration failure must leave the old media references and bytes usable. Physical storage cleanup is a separate guarded lifecycle operation, never an implicit migration side effect.

## Readiness and the Immutable `ReadinessSnapshot`

Forge alone computes readiness. The Project Agent may request an evaluation and present the result; it cannot approve, edit, or manufacture one.

`ReadinessSnapshot` is immutable and has, at minimum:

```text
readiness_snapshot_id
project_id / milestone_id / milestone_definition_revision
baseline_id / baseline_revision / baseline_digest
release_policy_revision / release_policy_digest
input_manifest (exact IDs, revisions, versions, and digests)
source_event_watermark
result = ready | blocked | failed | stale
blocking_reasons / check_results / waiver_refs
evidence_attachment_refs/digests and availability summary
commit/build/check context refs
computed_at / computing_policy_revision
readiness_digest
```

The input manifest covers the approved Charter and applicable artifact revisions, active baseline and frozen policy, milestone/dependency graph, selected Tasks/events, validation attestations and check definitions, manual attestations/waivers, evidence attachments/pins, and exact repository/commit/build/run contexts. The digest is over canonical ordered inputs, not a dashboard rendering.

The snapshot is the canonical release-candidate record; a separate mutable
“release candidate” authority is unnecessary. A standalone evaluation whose
result is `ready` compare-and-swaps an unreleased milestone from `active` to
`ready_for_release`. A non-ready evaluation leaves/returns it `active` with
typed blocker projections. For a milestone that already has a release, a new
`ready` snapshot is a correction-release candidate while the lifecycle remains
historically `released`; it does not regress the milestone state. The release
request must name the exact candidate snapshot ID and digest.

### Atomic recomputation at release

The release command must not trust a previously displayed “ready” result. In one Forge transaction:

1. Authenticate the interactive user, lock the Project/milestone/release pointer, and compare expected versions/digests and the idempotency key.
2. Resolve the current approved baseline, frozen release policy, milestone definition/dependencies, Task/event watermark, validations, waivers, evidence, and commit/build/check contexts.
3. Re-evaluate freshness, availability, dependency conditions, principal eligibility, and every required gate; recompute the canonical input manifest and `readiness_digest` from those exact rows. Require them to equal the named candidate snapshot and still evaluate `ready`.
4. Reference that immutable candidate snapshot from the release manifest, record the transaction's matching recheck digest/watermark, create release-scoped evidence pins, and allocate the next release revision with a uniqueness/compare-and-swap check. Do not create or refresh a snapshot merely to make it match.
5. Commit the release/pin records, state transitions, and outbox/domain events together. On any conflict or failed gate, roll back the release and pins and return the exact blocker; do not expose a partial success. A separate later readiness request may persist a new snapshot describing the changed state.

No previously computed snapshot can be silently refreshed. If any relevant record changes, mark the old snapshot stale through a derived relation; a subsequent explicit readiness request may create a replacement snapshot. A stale digest never produces a release, and a release manifest references the exact candidate snapshot whose digest the transaction recomputed successfully.

## Release Revisions and No Deploy Side Effect

Only the interactive user may invoke the typed release action after inspecting the candidate, exact readiness identity/digest, included scope, checks, waivers, known issues, evidence availability, and release diff. The system must re-authorize the user and recompute readiness in the release transaction; a Project Agent cannot call this action on the user's behalf.

For milestone identity `M001`, allocate monotonic immutable release revisions `M001-r1`, `M001-r2`, and so on. Never reuse a number or mutate a prior manifest. A release manifest includes:

- release ID/revision, milestone and definition revision, Project, releasing user, timestamp, and idempotency key;
- Charter, baseline, frozen release-policy, dependency, and readiness snapshot IDs/revisions/digests;
- immutable Task/event and artifact references included/excluded, with non-claims and known issues;
- principal-bound validation results, waivers, check-definition revisions, commit/build/run context, and evidence pin IDs/digests/availability;
- correction/supersession links to prior releases, if any, and the release event/outbox identity.

Forge release is an internal immutable snapshot of Project truth. It does not merge branches, create a git tag, deploy, publish, distribute, notify an external service, or mutate a repository. Those are separate explicitly authorized workflows and must not be inferred from `released`. A lost response is retried with the same idempotency key and returns the original release; it must not allocate a second revision.

A correction, new evidence, changed context, or newly approved baseline creates a later readiness snapshot and release revision. It never rewrites `M001-r1`, changes its manifest, or makes its historical claim disappear. The later release may state that it supersedes or corrects the earlier one while the earlier record remains inspectable.

## Availability, Purge, and Evidence Loss

Track media availability explicitly. These are content-retention states, not check results:

- `available`: bytes are retrievable through an authorized reference and match the recorded digest;
- `quarantined`: access is blocked pending security, abuse, rights, or integrity review; it cannot satisfy a release gate while quarantined;
- `redacted`: the original representation is restricted and a permitted redacted derivative or metadata may be served. The original digest remains recorded. A gate is satisfied only if the frozen policy explicitly accepts the exact redacted representation and its digest;
- `purged`: bytes have been irreversibly removed under an authorized mandatory security, privacy, or legal operation. Retain only permitted metadata/tombstone and digest records.

Soft deletion, detaching a Task attachment, or hiding a URL is not physical purge. A released evidence pin remains a retention root. Quarantine/redaction/purge changes are append-only availability events and do not rewrite the original asset, pin, readiness snapshot, or release manifest.

### Mandatory purge tombstone

Physical purge is allowed only for a documented mandatory security/privacy/legal basis and an authorized system workflow. It must atomically or durably record a purge tombstone containing, as policy permits:

- stable `asset_id`, prior public URL/reference, storage key, original content digest, byte size/content type, and prior availability;
- purge reason/category, legal/security basis reference, requesting and authorizing principal, time, operation ID, and storage result;
- affected attachment, evidence-pin, readiness, and release IDs;
- redaction/deletion audit digest and any retention/legal-hold decision.

The tombstone/digest is not a substitute for the bytes and must not expose protected content. For every affected release pin, append an availability projection/event of `evidence_unavailable` that points to the tombstone/digest. `evidence_unavailable` is neither `pass`, `fail`, nor a waiver. A current or future readiness evaluation requiring that evidence is blocked or unavailable according to the frozen policy. The original release remains an immutable historical manifest with its original asset digest; its evidence availability may be displayed as unavailable without rewriting the manifest.

The public disposition surface is explicit and Project-scoped: an authenticated
Project owner or admin member may call `POST
/api/v1/projects/{project_id}/media/{asset_id}/redact` or `POST
/api/v1/projects/{project_id}/media/{asset_id}/purge` with a
`ProjectMediaTombstoneRequest` carrying the current asset version, matching
authorization action, idempotency key, and bounded reason. The Project Agent
may propose the user action but cannot invoke or authorize it. Redaction blocks
serving the original bytes through the Project media route; purge also removes
the stored bytes; both persist the audited tombstone and project affected
release pins as `evidence_unavailable` without rewriting the manifest. The
legacy Task route keeps its existing behavior while its Task attachment remains
active; after purge neither former URL serves the bytes. Ordinary attachment
removal and garbage collection are not substitutes for this audited mutation.

If evidence is quarantined or redacted, show the state and reason and recompute affected candidate readiness. If it later becomes available, create a new availability event and, where required, a new readiness snapshot; do not edit the old snapshot.

## Task Deletion and Race-Safe Garbage Collection

Task deletion must not break a released release's proof. Ordinary Task deletion is a logical/soft deletion or a detachment operation. Before deleting or cascading any Task-scoped rows, Forge must preserve every release-pinned asset, pin, original digest, and source Task/event context needed to interpret the manifest. A pin retains the source Task ID and immutable result/event digest even if the live Task is tombstoned. If the product permits physical Task-row removal, denormalize the permitted immutable source context into the pin/tombstone first; never cascade from `task_id` to a pinned asset or release pin.

Only an explicit authorized redaction or mandatory purge may make a pinned
release asset unavailable, and it follows the tombstone/evidence-unavailable
process above. Ordinary Task cleanup must never delete or move pinned bytes.

Media garbage collection is reachability-based and race-safe. Treat active attachments, release evidence pins, legal holds, quarantine/redaction holds, migration holds, and any other policy retention roots as references. Attachment, pin, deletion, hold, and GC-candidate writes use the same database transaction and optimistic asset version.

The GC worker must:

1. Select only an explicitly eligible unpinned asset and record `asset_id`, version, digest, storage key, and eligibility time in a candidate.
2. Acquire a compare-and-swap deletion claim/lease for that exact asset version. A concurrent attach/pin either commits before the claim and prevents eligibility or is rejected/retried while the claim is held; it cannot race into a dangling pin.
3. Recheck all reachability roots, legal holds, availability/quarantine state, and current asset version while holding the claim. If any root exists or the version changed, release the claim and do not delete bytes.
4. Delete the physical bytes only under the guarded claim, then finalize the tombstone/purged state with a compare-and-swap. Recovery must resolve an interrupted claim deterministically; it must not assume a missing file means an unrecorded purge.

Never decide GC from a cached count, a stale UI list, or a Task's deleted flag. A pin created concurrently with cleanup must either be visible to the recheck or force cleanup to abort. If physical storage is eventually consistent, retain the claim/tombstone and reconciliation record until the storage result is verified.

## Commit, Build, Check, and Staleness Rules

Validation and evidence are fresh only for the exact contexts they record. At capture and at readiness recomputation, bind the evidence to:

- the approved Charter/baseline/milestone and release-policy revisions;
- the exact attempt-pinned Workspace/lease repository binding, commit/tree digest, and artifact/document revisions;
- build/run ID, input/output artifact digests, build-definition/toolchain/platform context, and result;
- check-definition revision, validation runner and principal, input-manifest digest, and attestation time;
- evidence asset/attachment digest, capture time, supported acceptance check, and release-candidate/readiness identity.

Mark a validation, evidence attachment, readiness candidate, or release candidate stale when any governing Charter, artifact, baseline, milestone/dependency, release policy, check definition, Task input/result, commit/tree, build/run, evidence digest, or required availability state changes. A new commit or build is a new context even if the source diff looks small. A changed source event, principal assignment, reviewer scope, or waiver also invalidates the affected result.

Do not mark evidence fresh merely because its URL still resolves. Do not mark it stale merely because an unrelated Project record changed. The system may retain freshness across a change only by recording an explicit, policy-permitted equivalence relation whose canonical input/context digest proves the required semantics are unchanged; chat explanation or human assertion alone is insufficient.

An unavailable, quarantined, purged, or disallowed redacted asset is reported as unavailable in the gate result, not as a stale pass. Recompute readiness after remediation, new validation, restored availability, a new build/commit, or an approved baseline/policy change. Releases preserve the exact context and availability captured at release time.

## Required Invariants and Failure Rules

- Every release references one Project, one milestone definition revision, one approved baseline, one frozen release policy, one immutable readiness snapshot, and exact evidence pins.
- `primary_milestone_id` is explicit and versioned; multiple active milestones remain independently queryable and releasable.
- No release proceeds with a stale readiness digest, unresolved required dependency, missing principal-bound validation, unapproved waiver, or required `evidence_unavailable` input.
- Readiness aggregation and release revision allocation are system operations; Project Agent proposals cannot alter their result.
- Release, readiness, evidence pins, events, and state transitions commit atomically or none are visible as successful.
- Old releases, snapshots, asset IDs, URLs, storage keys, digests, and tombstones are immutable. Corrections append a later revision.
- Ordinary Task deletion and GC never remove a release pin or reachable bytes. Mandatory purge is audited and leaves the required tombstone/digest plus `evidence_unavailable`.
- Projection/cache failure shows stale or error state and never changes canonical readiness or release truth.
- Version/digest conflicts fail closed and require refresh/re-proposal. Never merge two approvals or silently choose the newest mutable text.
