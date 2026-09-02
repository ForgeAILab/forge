# Project Charter, Approval, Creation, and Handoff

## Contents

- Charter purpose and lifecycle
- Charter schema
- Readiness
- Approval receipt
- Atomic Project creation
- Project admission receipt
- Handoff projections
- Handoff invariants and exclusions
- Post-handoff amendments

## Charter Purpose and Lifecycle

The Charter is the authority for Project identity, users/beneficiaries, outcome, scope, non-goals, success boundary, and fixed constraints. It is not a Task plan or implementation specification.

Use a stable `project_charter_id`. During Genesis it belongs to one Genesis session and has no Project ID. `CreateProjectFromCharterApproval` attaches it to exactly one Project and permanently transfers mutation authority from Main/Genesis to Project scope.

Use lifecycle:

```text
working draft revision
  -> immutable proposed revision
      -> user approval receipt
          -> current approved revision
              -> later project-local amendment proposal
                  -> user-approved superseding revision
```

Keep every accepted server revision immutable. Client-local keystrokes need not create revisions. A save/propose action uses an expected version/base revision and records source provenance.

Each revision stores:

- Charter ID, revision ID/number, base/parent revision;
- lifecycle (`draft`, `proposed`, `approved`, `rejected`, `withdrawn`, `superseded` as applicable);
- typed canonical payload and schema version;
- exact rendered Markdown/view and render version;
- canonical content digest and rendered-view digest;
- change summary/material diff;
- author principal/identity/Profile/operating-skill revision;
- source message/turn/research references in server-private provenance;
- creation time and immutable approval/supersession references.

## Charter Schema

### Required in compact mode

- `project_mode: compact`
- approved display name and optional system slug proposal;
- target user/beneficiary when applicable;
- one-sentence outcome or identity;
- one success check / acceptance boundary;
- explicit non-goals;
- material constraints or explicit “none known”;
- assumptions/research queue with impact and revisit trigger;
- provenance/change summary.

Compact mode is allowed only when one outcome is low-risk and no material architecture, data, integration, security/compliance, migration, operations, or irreversible uncertainty exists.

### Standard Charter sections

1. **Revision metadata** — IDs, versions, state, parent/supersession, content/render digests, dates.
2. **Identity** — approved display name, one-sentence identity, Project type, mode/maturity, value proposition.
3. **Problem / opportunity** — current condition, why it matters, why the Project exists.
4. **People** — primary user/beneficiary, stakeholders, explicitly excluded audiences when material.
5. **Core experience / outcome** — core loop, primary outcome, principal journeys.
6. **Scope and deliverables** — included outcomes/capabilities, required deliverables, later possibilities.
7. **Non-goals** — explicit exclusions and adjacent ideas not authorized.
8. **Success and acceptance boundary** — signals/measures, measurement boundary, required evidence, non-claims.
9. **Constraints** — product, technical, privacy/security/compliance, accessibility, time/resource, compatibility/migration, operations/launch, agent-authority constraints.
10. **Dependencies and risks** — impact, current treatment, trigger, owner.
11. **Knowledge ledger** — observed facts, user decisions, research findings, assumptions, hypotheses, open decisions, research queue.
12. **Handoff note** — recommended first planning action, mode recommendation, important constraints/evidence references; no new scope.
13. **Provenance and delta** — safe source references, base revision, change summary/material diff.

### Knowledge item fields

Use stable statement IDs. Common fields:

- statement/value;
- epistemic status;
- normative (`true` only for approved decisions/requirements);
- provenance/source kind and safe reference;
- confidence when non-normative;
- observed/retrieved time and freshness/expiry when relevant;
- impact;
- owner/decision owner;
- default/revisit trigger/falsification evidence;
- transfer approval for cross-scope handoff.

`transfer_approved` means the user permits the item to cross into the Project. It does not turn an assumption, hypothesis, or research finding into accepted Project truth.

## Readiness

Readiness is a server-validated property of an exact revision, not the Main Agent's confidence.

Compact readiness requires all compact fields to be coherent and no blocking unknown.

Standard readiness additionally requires material concerns to be resolved, explicitly inapplicable, or queued as visible non-blocking work:

- product/user journey;
- data/integrations;
- privacy/security/compliance;
- accessibility;
- architecture/compatibility/migration;
- operations/observability/recovery;
- launch/time/resource constraints;
- acceptance/evidence boundary.

Creation may hand off top unresolved questions only if they are non-blocking. Keep at most two in the concise handoff summary; retain the complete approved assumptions/research queue in the Charter.

## Approval Receipt

Approval must be an explicit UI/command action, never inferred from natural language.

Minimum receipt fields:

```text
approval_id
approval_type = project_creation | charter_amendment | adoption
charter_id
charter_revision_id
charter_content_digest
charter_render_digest
expected_charter_version
approved_project_name
approved_project_mode
selected_project_agent_identity_id
selected_project_agent_profile_revision_id
selected_project_agent_operating_skill_revision
selected_project_agent_policy_digest
approved_by_user_id
approval_event_id
approved_at
state = active | consumed | revoked
consumed_by_project_id?
idempotency_key
```

The Agent identity/Profile exist before binding; the Project binding ID is created later. Bind approval to the selected revisions, not to a nonexistent binding ID.

Rules:

- A new approved Charter revision revokes any still-active older Project-creation receipt.
- Profile/operating-skill/policy revision mismatch makes the receipt stale unless the user re-approves.
- Creation consumes the receipt exactly once.
- Revocation/consumption never rewrites approval content.
- Replay with the same idempotency key returns the original approval/outcome.

## Atomic Project Creation

Use one command:

```text
CreateProjectFromCharterApproval(
  approval_id,
  idempotency_key
) -> {
  project_id,
  project_agent_binding_id,
  project_chat_id,
  handoff_id,
  target_message_id,
  target_turn_id
}
```

In one SQLite transaction:

1. Authorize Main/account and lock Genesis, Charter, selected identity/Profile, and approval receipt.
2. Verify receipt is active/current and every content/render/Profile/skill/policy digest matches.
3. Validate exact approved name/slug; do not substitute on conflict.
4. Create Project, Project Agent binding, and singular Project Chat.
5. Attach the Charter to the Project and transfer mutation authority.
6. Create compact default primary milestone `M1 — Deliver outcome` when `project_mode=compact`; standard mode may create planned milestone definitions from the approved Charter only when present.
7. Create server-signed handoff record, project-visible payload, target handoff message, and one queued Project Agent turn.
8. Freeze one immutable Project admission receipt that names the handoff, consumed approval, initial Charter/revision, and canonical packet digest.
9. Link the active binding to that admission receipt and to the exact current consumed Charter approval.
10. Append domain events/outbox records.
11. Transition Genesis to `handed_off` and mark the Charter approval receipt `consumed` with Project ID.
12. Commit.

Any error rolls back every item. Runtime execution of the queued Project Agent turn happens after commit and follows the normal finite retry/failure lifecycle; a failed model response does not undo the delivered handoff.

Enforce uniqueness on approval ID and idempotency key. A retry after a lost response returns the same IDs.

## Project Admission Receipt

Admission is established once, at the transaction that first makes the
Project Charter-backed:

- Genesis creation validates the complete bounded handoff and freezes its ID
  and canonical request fingerprint.
- Legacy adoption has no Main handoff; it freezes the exact consumed adoption
  approval and content digest.
- The receipt is immutable, unique per Project, and never supplied by an
  adapter or model.

An active Charter-backed binding must reference that stable receipt, the exact
current consumed Charter approval and Charter/revision pointers, current
Project operating-skill revision, policy digest, Profile snapshot, and
permission ceiling. Rebinding reuses the admission receipt and creates no
handoff. A Charter amendment rotates the binding's current approval/Charter
authority and also reuses the receipt.

Fresh turns check receipt integrity plus current binding/Charter authority.
They must not re-derive admission by scanning Main messages/turns, source
Profiles, Genesis instruction revisions, or Project-creation events. Queued,
leased, and retrying turns retain the responder provenance frozen when they
were admitted.

## Handoff Projections

Keep one immutable handoff with two projections.

### Project-visible payload

The Project Agent and Project UI may receive:

```text
schema / schema_version
handoff_id / supersedes_handoff_id?
created_at
payload_digest
project_id / approved_name / system_slug / project_mode
creation_receipt_id
target_binding_policy_digest
target_identity/profile/operating-skill revisions
charter_id / revision_id / revision_number
charter_schema_version
charter_content_digest / charter_render_digest
charter_approval_id / approved_by / approved_at
bounded_summary
settled_decision_ids[]
top_unresolved_items[]
included_research_record_ids[]
redaction_summary
server_private_audit_digest
```

The exact Charter content/render is admitted as an authorized artifact/context-manifest source rather than a paraphrase. Do not make the visible packet depend on dereferencing Main Chat IDs.

Top unresolved item fields:

```text
statement_id
epistemic_status = open_question | assumption | hypothesis
normative = false
blocking = false
transfer_approved = true
impact
default?
owner
revisit_trigger
```

### Server-private audit provenance

Retain for audit but exclude from Project Agent context:

```text
handoff_id
genesis_session_id
source_main_chat_id
source_message_ids/digests
source_turn_ids
main_identity/profile/instruction revisions
portfolio projection refs
web research run/tool refs
approval/create event IDs
redaction decisions
runtime build/run manifest
created_at
```

The same interactive user may inspect authorized provenance through a dedicated UI/API, but model context receives only the visible packet and audit digest.

## Handoff Invariants

- Project, binding, chat, handoff, and Charter use the same Project ID.
- Approved name exactly matches the receipt and Charter.
- Charter revision is current, approved, immutable, and matches both digests.
- Target identity/Profile/skill/policy match the receipt and created binding.
- `payload_digest` covers the canonical visible payload except its own field.
- Every visible statement exists inside the approved Charter or an explicitly included immutable research record.
- Unresolved summary has at most two non-blocking/non-normative items; the full queue remains in the Charter.
- Publication is idempotent on the consumed approval.
- A supplemental/correction publication is an explicit new handoff; ordinary rebinding and Charter amendment are not handoff publications.
- Issue-time admission fails closed on any packet/hash/approval mismatch. Later turns fail closed on a missing/cross-Project receipt or stale current binding/Charter authority without re-walking Main provenance.

## Never Cross the Model Boundary

Exclude:

- raw Main transcript or dereferenceable Main message IDs;
- hidden reasoning, scratchpads, prompts, evaluator traces;
- rejected/withdrawn drafts and unapproved research;
- other-Project IDs, summaries, artifacts, Tasks, memories, or counts;
- global memory not explicitly approved for this Project;
- credentials, tokens, cookies, API keys, environment secrets;
- protected runtime/checkpoint/interaction state;
- capability or Workspace tokens;
- filesystem paths, repository credentials/handles;
- raw portfolio/search/tool traces.

## Post-Handoff Amendments

Main loses Charter-writing authority after atomic attachment. Project Agent or user may create:

```text
CharterAmendmentDraft {
  amendment_id
  base_charter_revision_id
  candidate_charter_revision_id
  rationale
  affected_decision_ids[]
  affected_document_ids[]
  affected_task_ids[]
  affected_execution_baseline_ids[]
  affected_milestone_ids[]
}
```

User approval must compare expected current Charter revision plus candidate content/render digests. A mismatch fails. Successful approval advances the pointer and marks affected records `reconciliation_required` until explicitly resolved.

The same amendment transaction updates the active binding to the new consumed
approval and current Charter revision while retaining the Project admission
receipt. It never requires or creates a replacement Main handoff.
