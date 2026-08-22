# Changelog

All notable changes to Forge are documented in this file.

Forge follows Semantic Versioning. During the `0.x` public beta period, APIs and workflows may change between minor versions.

## [Unreleased]

### Added

- Main Chat now starts Product Genesis from clear natural-language new-Project
  intent through the Main-only typed `genesis.start` command. The existing
  `/start-product <idea>` UI command remains an optional shortcut. Both paths
  share one atomic, receipt-backed start boundary; native success transfers the
  leased baseline turn into exactly one discovery continuation without a second
  visible user message or duplicate assistant response. Ambiguous intent asks a
  concise question, and ordinary portfolio/existing-Project chat is unchanged.

- Added the native ReadyOnly `task.adaptive` Project Agent operation for
  bounded split, sequence, and replace Task commands. The operation is
  Project/Project-chat scoped, requires `propose_task`, uses direct command
  receipts without an `AgentAction`, and returns receipt-first replay metadata.

### Fixed

- The Main and Project Agents can see the shape of the payload they must send.
  The generic orchestration proposal tool declares `payload` as a plain object
  (provider function-calling APIs only reliably deliver flat schemas), and its
  description named only the top-level required keys. For an operation whose
  payload is deeply nested — `charter.draft` carries the whole Charter
  `content` tree — the field names below the top level were invisible, so the
  model had to rediscover them one rejected call at a time. A Genesis Charter
  draft cost 30 rejected `charter.draft` calls before landing, and the
  knowledge-ledger provenance it could never guess left readiness `blocked`.
  The payload description now carries a compact recursive signature of the
  exact contract, including nested objects, arrays, and enum variants.

- Orchestration validation failures tell the model what was wrong. Contract
  and command-boundary rejections were collapsed into a bare "the operation or
  arguments are not valid for this Forge surface", discarding a
  server-authored reason that named the offending field — so the model
  retried the same rejected shape indefinitely with nothing to correct. The
  reason now travels in the outcome (for example
  `(expected_document_version must be positive)`). Policy denials stay generic:
  their reason can describe authority the caller may not observe.

- A Project can be deleted after its agents have run. `DELETE /api/v1/projects/{id}`
  failed with "context manifests are immutable" for any Project whose agent had
  produced a single context manifest. Project teardown already installs a
  `project_deletion_guard` so append-only Project tables release their rows
  once, but `context_manifest` and `context_manifest_source` never joined that
  contract and aborted unconditionally — and they are reached by cascade, so
  the whole transaction rolled back. `V090` relaxes both delete triggers to
  abort only while their parent row still exists, which is exactly the
  direct-delete case; update immutability is unchanged.

- An agent can dispatch the execution it was just assigned. Role eligibility
  required `EffectiveStatus::Active`, which requires spare concurrency — but
  dispatch re-checks eligibility *after* the execution is running, so the
  execution consumed its own agent's capacity. Any agent with
  `max_concurrent_tasks = 1` failed every repository dispatch with "repository
  execution identity is not active and Project-eligible". `Busy` is a
  scheduling fact enforced when a Task is claimed, so it no longer strips an
  identity of its Project role.

- An embedded agent can serve as reviewer. The review path built its auditor
  executor from the CLI adapter registry alone, so an embedded reviewer failed
  with "No adapter registered for executor type: embedded". Reviews now
  dispatch the auditor through the same routed executor the Task path uses.

- A shell auditor's verdict is read. `===REVIEW: PASS===` was only looked for
  in assistant-channel log entries, so a shell auditor — which writes ordinary
  stdout — always failed as "verdict marker missing" even when its own log
  contained the marker. Auditor stdout is now part of the verdict text, with
  lines kept separate so two partial lines cannot be glued into a marker
  neither printed.

- A Worker can no longer become its own reviewer. Claiming into a state seeded
  the claiming agent into that state's role, including the reviewer role, so a
  Worker claiming its way into the review gate silently self-assigned the
  review it was supposed to receive independently. A claim for the Project's
  independent-reviewer role is now refused when the claimer is that Task's
  Worker.

- Two concurrent submissions of the same Project-creation command both return
  the committed receipt. The loser of that race read the Charter approval
  while it was still `active`, then validated Genesis state the winner had
  already advanced, and failed with "Product Genesis does not point to this
  exact active Charter approval" instead of replaying. A lost response is
  normally retried, and the retry can overlap the original, so this turned an
  ordinary retry into a spurious conflict. The loser now re-resolves exactly
  once against the now-consumed approval; the retry is admitted only when that
  approval is consumed *and* the committed handoff carries the caller's own
  idempotency key, so a genuine conflict is never retried into success.

- Approving a Project adoption Charter now installs the canonical Project-Agent
  permission ceiling while activating an `agent_setup_required` binding. The
  binding previously became active with setup's empty ceiling, so the newly
  admitted Project Agent could read its Project but every direct command was
  rejected at the transactional authorization boundary.

- Conflict, authorization, and task-availability errors from the Forge
  coordination tools reach the model instead of collapsing into "Forge
  coordination operation failed". A conflict's entire value is the value it
  names — which version to send, which revision to base on — and the model
  never saw any of it: a live run retried an adoption draft eight times, then
  told the user the platform had locked the Charter pending approval, an
  explanation it invented to fit an error it could not read. Genuinely
  internal failures (database, git, review) stay generic.

### Fixed

- `project.charter.adoption` version conflicts name the value that would
  satisfy them. Nothing on the Project surface reads a Charter, so a model
  told only that its `expected_charter_version` is wrong has nowhere to look
  the right one up: a live run alternated between "a Project Charter adoption
  draft already exists" and "the Project Charter changed before adoption was
  materialized" eight times and gave up without writing a revision. Both
  conflicts now carry the current Charter version (and the draft revision id
  to base on), the immutability conflict names the Charter's actual mode and
  maturity, and the tool schema documents where `expected_charter_version`
  comes from.

### Fixed

- A Project Agent no longer reports an unapprovable adoption Charter as done.
  `project.charter.adoption` committed the revision and returned success
  without the readiness verdict the user's approval is judged against, so a
  Charter with unfilled required sections looked finished to the model and
  was rejected only when the user tried to approve it. The action result now
  carries `readiness` (status plus blocking gaps), matching what the Main
  Charter draft already returned.
- The Charter approval surfaces say why an approval cannot proceed. Blocking
  readiness gaps are shown — and the button disabled — before the click, and
  a rejection reports the server's own reason instead of a blanket "the
  Charter or Project changed", which was wrong for every rejection that was
  not an optimistic-concurrency conflict.

### Added

- A Project Agent's adoption Charter can be approved from the UI. The agent
  can draft one but never approve it, so until now the draft dead-ended: the
  Genesis approval card renders only in Main Chat, and Project Overview only
  said to ask the agent for a Charter it had already written. The exact
  revision now surfaces on both surfaces the user actually works in — pinned
  in Project Agent Chat next to the conversation that produced it, and as a
  real review-and-approve control in the Overview banner — sharing one hook so
  neither can offer a different revision than the other. Approval goes through
  the existing `POST /projects/{id}/charter/revisions/{revision_id}/approve`
  contract, pinning the Charter and Project versions observed during review.

### Breaking

- `StartProductGenesisRequest` now requires a non-empty `idempotency_key`.
  Product Genesis start is atomic across the session, immutable instruction and
  source provenance, durable event/receipt, and discovery admission. Concurrent
  active starts now return the typed `active_session_conflict` outcome; missing
  Main setup returns `setup_required`.

- Project Overview clients must consume a typed `next_action` projection and
  explicit document/evidence freshness state. The Overview now distinguishes
  an approved Document revision from a newer working draft, binds release-gating
  evidence to its exact Task/run/validation/check/build context, and marks
  missing or mismatched context stale instead of treating an available file or
  caption as proof. Project Agent release recommendations remain
  non-authoritative attention records; only the user release operation may
  consume the exact readiness snapshot and create an immutable release.

- Execution responses now expose the owner-bound liveness projection
  (`execution_version`, opaque `lease_owner`, lease expiry, hard deadline,
  heartbeat, semantic progress, owner health, and warnings). Output, reasoning,
  and tool events are progress signals, not ownership heartbeats; clients must
  not treat a quiet but leased execution as stalled or use `lease_owner` as a
  credential. Terminal events are winner-only compare-and-swap outcomes, with
  the matching `WorkspaceLease` disposition committed atomically. New
  `execution.progressed`, `execution.progress_warning`, and
  `execution.hard_deadline_exceeded` events distinguish progress, attention,
  owner expiry, and hard deadline recovery.

- Agent wake delivery now exposes durable typed outcomes for every claimed
  wake candidate: `turn_admitted`, `deterministically_suppressed`, `deferred`,
  or `setup_required`. The wake consumer resumes from a migration-recorded
  cutover cursor and no longer fast-forwards to the runtime event maximum.
  Event consumers and diagnostics must not assume every `agent.wake.*` event
  immediately creates a turn or parse absence of a turn as successful
  delivery.

- Main/Project Agent user, handoff, retry, and wake turns now resolve the
  identity's current Profile at admission and freeze that Profile/policy
  provenance for the admitted turn. Binding-time Profile snapshots no longer
  select later turns; a Profile edit affects the next admitted turn only.

- REST `ServiceError::InvalidOperation` responses now use the stable
  `validation_error` code, matching native orchestration failures. Clients
  should branch on this code rather than the removed `invalid_operation` code.

- Native and MCP orchestration tools now share a typed `OrchestrationOutcome`
  envelope with stable `code`/`status`, canonical scope, correlation id,
  explicit boolean `replayed`, and typed approval/setup/current-state/retry
  fields. Native domain failures are model-facing in-band tool values; known
  MCP tool failures are JSON-RPC success results with `isError`,
  `structuredContent`, and `content`, while parse/method/protocol errors remain
  top-level JSON-RPC errors. Callers must stop parsing free-form error prose;
  protected causes remain redacted.

- Project creation no longer implies executable readiness. Creation responses
  include independent `coordination_state`, `execution_setup_state`, and
  `execution_gate` truth through `execution_setup`; missing repository,
  Worker, or independent-reviewer prerequisites remain visible as typed
  `setup_required`/provisioning/failed states and block implementation
  dispatch. Clients must use `GET /api/v1/projects/{id}/execution-setup` and
  its canonical setup actions (select a Worker or reviewer, attach a
  repository, or retry provisioning) rather than treating a committed
  Project or repository row as ready; the old same-principal Worker/reviewer
  fallback is gone.

- `POST /api/v1/agents/{id}/task-proposals` now executes the admitted Task
  command in one step and returns a durable command receipt with the Task. It
  no longer creates an `AgentAction`/`AgentActionExecution`, and the obsolete
  `/api/v1/actions/{id}/execute-task` endpoint and its request/response types
  have been removed. Historical action rows remain readable.

- Native Main Charter drafts and catalog-admitted safe Project coordination
  commands now return committed command-receipt outcomes directly. They no
  longer manufacture an automatically executed `AgentAction` or
  `AgentActionExecution`, so callers must use the returned receipt/event fields
  instead of Action ids, versions, or execution ids. Approval-required
  operations, including Project creation and release requests, remain Actions.

- Project Agent `project.evidence` proposals now require the positive
  `expected_milestone_version` from the current Project state. Omitting it or
  sending zero no longer defaults to the live milestone version; stale native
  proposals fail the same compare-and-swap check as REST requests.

- `project.charter.adoption` treats `charter_id` as a reference, not as the id
  to store under. The field is now optional: omit it to start the Project's
  adoption Charter and read the server-minted id back from the action result
  (which also gained `charter_version` for the follow-up `expected_charter_version`),
  or pass the id the server already returned to revise it. Naming another
  Project's Charter is still a scope error. A live run had the model coining
  `charter-notejot-001` and that slug became the row's primary key.

### Fixed

- A Project Agent can revise its adoption Charter draft. Every revision after
  the first failed with a bare `VersionConflict`: the handler routed all
  drafts on a setup Project through the shell-plus-first-revision atomic
  path, which rejects any `expected_charter_version` but 1. Whether the
  Charter shell exists now decides the path, so review feedback can be
  applied before approval instead of only after it.
- A server restart no longer signs the web client out. The token-refresh call
  cleared the session on *any* non-2xx and on transport failure, so every
  restart — routine on a local-first app — discarded a still-valid refresh
  token and bounced to `/login`. Only the server actually rejecting the
  refresh token (400/401/403/422) ends the session; anything else surfaces as
  a retryable `503` with the session intact.
- The SSE event stream no longer goes silent 15 minutes into a sitting.
  `EventSource` cannot send an `Authorization` header, so the access token
  rides in the query string and is fixed for the life of a connection — past
  its expiry every reconnect re-sent the same dead token and the stream stayed
  down, which is what left "Thinking…" spinners pinned after a turn had
  already finished. Reconnects now re-read the auth store, so a token
  refreshed by REST traffic is picked up. The stream deliberately does not
  refresh tokens itself: refresh tokens are single-use and deleted on
  redemption, so a second caller is another chance to burn one and end the
  session.

### Fixed

- A Project Agent created through the UI can actually orchestrate a Project.
  Four independent gates each produced a silently toolless agent, which then
  narrated fake success ("the baseline and tasks are ready") while the
  database held zero charters and zero tasks:
  - The Agent Chat content guard rejected any message containing `sk-` as a
    bare substring, so ordinary Project prose — `Task-1`, `task-by-task`,
    `risk-free`, `disk-based` — failed the turn with "protected values cannot
    be stored in Agent Chat content". The `sk-` marker now has to open a word
    and be followed by a key-length token, and `api key` only marks a secret
    when an assignment (`=`/`:`) carries a value after it, so `REST API. Key
    endpoints` passes.
  - The new-agent wizard's `DEFAULT_CEILING` omitted `propose_project` and six
    other scopes that seeded agents already had, so no agent created through
    the UI could ever be granted Project orchestration — the ceiling is the
    outermost bound, so an omission there cannot be restored by any binding.
  - Saving a Project Agent binding wrote an empty permission ceiling.
    Placeholder bindings report `permission_ceiling: {}`, which is present but
    grants nothing, so `?? DEFAULT_PROJECT_PERMISSION_CEILING` never fired.
    The panel falls back on an empty allowed list, and the turn worker now
    rejects a nothing-granting ceiling loudly instead of running a turn with
    no tools bound.
  - `project.charter.adoption` required the agent to reproduce
    `rendered_view`/`render_version` byte-for-byte from the Rust renderer,
    with no draft operation on the Project surface to read the canonical value
    from. Adoption now matches the Main Charter contract at all three
    enforcement points: a supplied render is verified, an omitted one is
    rendered server-side.

### Breaking

- Execution-baseline REST writes now use the shared command boundary and an
  explicit `operation`: the collection `POST` saves a first `draft` candidate
  instead of creating a shell, revision writes choose `save_draft` or
  `propose_for_approval`, and activation must include the exact
  `expected_baseline_version`. Proposal responses include the frozen
  `approval_target` and `requires_user_authorization`; old shell/proposed
  request bodies are rejected.
- Project Agent `project.execution_baseline` actions now require the canonical
  rendered view/version and content/render digests. `draft_revision` and
  `revise` persist lifecycle `draft`; only `propose_approval` persists
  `proposed` and returns an exact approval target requiring user authorization.
  The former weaker native payload and its implicit promotion are removed.
- `task.propose` execution now reports the real executing principal (`user` for
  REST, `agent` for native Project-Agent execution) instead of the internal
  Task service label. Its Task, governance, roles, durable event, command
  receipt, and Action outcome commit atomically, and exact response-loss
  retries return the original frozen Task rather than materializing another.

- Project Agent typed actions no longer trust an agent-supplied id when
  creating a Project Document (`project.document` with a `draft_revision`
  targeting an unrecognized `document_id`). The id is now minted
  server-side with `new_uuid_v4()`, matching the existing Charter
  precedent, and the real `document_id` is returned in the action result
  JSON for the model to use on follow-up actions. Anything that assumed
  its proposed `document_id` became the persisted primary key must read
  the id back from the result instead.
- Execution-baseline ids are server-minted everywhere. The REST propose
  route `POST /projects/{id}/execution-baselines` no longer accepts a
  `baseline_id` field (`CreateExecutionBaselineRequest` rejects it with
  HTTP 400), and the Project Agent `project.execution_baseline` action
  treats `baseline_id` as a reference to an existing baseline only: omit
  it on `draft_revision` to create a new baseline (the minted id comes
  back in the result), and `revise`/`propose_approval` require an id that
  already exists. A live run had the model inventing a mutated project id
  as the baseline primary key.
- `task.propose` no longer silently creates unrunnable implementation
  Tasks. When a project is repository-backed and has an active execution
  baseline, proposing a `task`/`sub_task` without a `plan_item_id` — or
  with one the active baseline doesn't contain — is rejected with an
  error listing the valid plan item ids. Proposals naming a plan item
  that already has a non-cancelled Task are rejected with the existing
  task id and status (duplicate work is proposed against the existing
  Task instead). Planning/discovery proposals are unaffected.
- A failed role dispatch entering an active workflow state no longer
  strands the Task there looking in-flight. The engine rolls the Task
  back to the workflow's initial state, records a `dispatch_failed`
  error annotation with the dispatch error, and the task dispatcher
  parks annotated Tasks instead of rescheduling them every tick. A
  successful dispatch clears the annotation, so approving the missing
  baseline un-parks the work naturally.

### Added

- Embedded (native) Task executions now stream turn events into the standard
  JSONL log: tool calls (`tool_call`/`tool_result`), reasoning (`thinking`,
  a new `LogKind`), and assistant deltas. Execution history for embedded
  agents shows the full turn instead of only the final message, and the
  execution-detail filter/raw viewers understand the new `thinking` kind.
- The Project Agent Chat pins an execution-baseline approval card when the
  agent proposes a revision: review the exact rendered baseline in a dialog
  and approve + activate it with one action, writing the normal durable
  approval receipt through the existing REST contract.

### Fixed

- Activating an execution baseline now back-fills governance for
  preplanned Tasks: non-terminal Tasks whose governance names a plan item
  in the activated revision are re-bound to it (charter revision,
  baseline, milestone, provenance) and become runnable, matching what
  `task.propose` derives for Tasks proposed after activation. Previously
  only rows already bound to the exact activated revision were flipped,
  leaving anything proposed pre-baseline permanently unrunnable.
- Activating an execution baseline sets the project's primary-milestone
  pointer when it is missing, using the manifest's primary (or only)
  milestone with the same validity checks as
  `project.milestone.primary.set`. A missing pointer hard-failed every
  agent turn admission ("Project with active milestones has no explicit
  primary milestone").
- `forge_scope_propose` declares its `task.propose` payload fields in the
  tool schema (`title`, `task_type`, `plan_item_id`, `milestone_id`, …).
  The payload was previously an opaque object, so schema-driven models
  never discovered `plan_item_id` and every proposal landed ungoverned.
- Server restarts reap stale embedded-agent sessions: startup recovery
  suspends native-backend sessions left in a non-terminal status
  (`starting`/`ready`/`running`/`degraded`) so they stop claiming a
  healthy runtime that did not survive the process. Conversation/LCM
  continuity is unaffected — a fresh session in the same scope resumes
  the same timeline; CLI-backend sessions are untouched.
- Milestone readiness evaluation no longer 500s: its Task/repository
  context queries referenced columns that don't exist (`task.type`,
  `repo.kind`, review CI columns from an older schema draft), failing
  every `POST /projects/{id}/milestones/{milestone_id}/readiness` on a
  repository-backed project.
- Execution liveness no longer infers ownership from output. Embedded and
  remote attempts renew a server-owned lease independently of turn/log events;
  text, reasoning, and tool boundaries update semantic `last_progress_at` only.
  A quiet but leased provider remains live until its fixed profile/capability
  hard deadline, while stale semantic progress produces a distinct Attention
  warning instead of a false owner-death failure.
- The Attention projection no longer wedges permanently on a dedupe-key
  conflict. `agent.wake.admitted` dedupe keys now include the triggering
  event id (re-admitting the same incident after lease expiry is a new
  admission, not a conflicting replay), and the projection loop quarantines
  semantic-conflict poison events instead of retrying them forever with the
  cursor stuck — a live run had every wake silently blocked for this.
- Dispatcher-initiated embedded worker executions no longer fail session
  authorization with "no Running worker execution for this identity". The
  durable task-role assignment now admits the write-capable session
  alongside the live execution check; requiring the Task-level assignee
  alone rejected every auto-dispatch (only interactive claims set it).
- Planning/discovery Tasks can actually execute on the embedded backend.
  Their workflow dispatches the write-capable coder role but the execution
  snapshot is server-marked read-only, which previously failed twice: the
  session binding granted TaskWrite from the role alone ("native turn scope
  does not match the server-issued session binding"), and the typed-tool
  composition had no mapping for a read-only worker ("Task workspace access
  is not valid for role `worker`"). Session authorization now derives
  read-only access from the Task type, a read-only worker composes the
  planner toolset, and migration `V085` downgrades context scopes the old
  bug persisted with write access (read-only is strictly narrower).
- Project Document creation is atomic: the Document shell, its first
  revision, and the `current_draft_revision_id` pointer are now written
  in one transaction (`ProjectOrchestrationRepo::create_project_document_atomically`).
  Previously the shell was inserted immediately and the first revision was
  created in a separate step after content parsing, so a mid-flow failure
  (or a retry with a fresh placeholder id) could leave an orphan Document
  shell with no revision and a `NULL current_draft_revision_id` — observed
  in a live run as four rows for one logical document, two of them
  orphans.
- Project Milestone creation is atomic: the Milestone shell and its first
  definition revision are now written in one transaction
  (`ProjectOrchestrationRepo::create_project_milestone_atomically`).
  Previously the two inserts were separate calls, so a failure between
  them could leave an orphan Milestone shell with no revision. A first
  ("define") revision still lands in `draft` lifecycle and intentionally
  leaves `current_definition_revision_id` unset until a later action
  promotes it — the `project_milestone_pointer_guard_update` trigger only
  accepts a `proposed`/`approved` target — so this is expected, not a bug.
- `GET /api/v1/projects/{id}/milestones` no longer hard-fails the entire
  list when one Milestone has a `NULL current_definition_revision_id`
  (the normal mid-flow state described above). The projection now falls
  back to that Milestone's latest definition revision, of any lifecycle;
  a Milestone with no definition revision at all (which atomic creation
  should make impossible) is skipped with a `tracing::warn!` instead of
  failing every other Milestone in the response.
- Genesis Charter creation no longer trusts the agent-supplied
  `charter_id`: the first Charter shell's primary key is minted
  server-side and returned in the action result (a live Gemini run
  persisted the placeholder `11111111-1111-1111-1111-111111111111` as a
  primary key, which would collide with the next Genesis session).
- SQLite write transactions now open with `BEGIN IMMEDIATE`
  (`db::begin_immediate`, all 125 former `pool().begin()` sites). Deferred
  transactions that upgraded to writes failed instantly with
  `SQLITE_BUSY_SNAPSHOT` regardless of `busy_timeout`, surfacing as
  `LCM store backend failed` turn failures under concurrent load. The LCM
  and protected stores also log the underlying database error before
  mapping it to their opaque store-failure variants.
- Typed tool errors reaching the model now carry the server's actual
  rejection message (`AgentHostError::Runtime` passthrough) instead of a
  generic "Forge tool provider failed", and the execution-baseline
  release-policy schema error names the expected schema string. Models
  can now self-correct instead of retrying blind.
- A merge attempt against a dirty worktree now routes the task to
  `merge_failed` (the merging gate's defined reject edge) instead of
  cascading toward `review`, an edge the default workflow does not
  define — tasks used to wedge in `merging` forever.
- `git::status_porcelain` no longer chops the first character of the
  first path (`run_git` trims stdout, which ate the leading space of the
  two-character status code; errors reported files like `EADME.md`).
- Worker prompts now state explicitly that the worktree must be committed
  (`git add -A && git commit`) before finishing, because the merge gate
  rejects uncommitted worktrees.
- Mission Control no longer returns a blank feed: `execution_failed`
  attention rows (written by the projector) were unmappable on the read
  side and one such row failed the entire `GET /api/v1/mission-control`
  response. `AttentionCategory` gains `execution_failed`, and the list
  endpoint skips (with a warning) any projection row it cannot map
  instead of failing the page.

### Fixed

- Native (embedded) agents can execute the `planner` role. The embedded
  Task executor only admitted worker/coder/reviewer, so every genesis
  project whose workflow has a planning state hard-failed with "embedded
  Task execution is not admitted for role `planner`". Planner is now a
  read-only native Task role: the session composes read-only tools (no
  write/command surface) and the runner forces the read-only worktree
  path, like reviewer.
- Dispatch no longer hard-fails when the Task row moves between
  WorkspaceLease issuance and launch. A role handoff, retry-metadata
  clear, or concurrent transition bumps `task.version` and broke the
  lease's exact-match verification ("active WorkspaceLease does not
  exactly match Task execution authority"). The runner now revokes this
  execution's stale lease and reissues once through the normal issuance
  path (which re-validates assignment authority fresh) before failing
  closed; authority held by another execution is never revoked.
- The read-only discard error names the actual trigger. A reviewer run on
  a normal `task` used to be told to "recreate it as task_type 'task'" —
  which it already was. Role-triggered read-only (reviewer/planner) now
  says the role never receives write access and implementation belongs to
  the worker/coder role; the recreate-as-'task' remedy remains only for
  `planning_task`/`discovery` task types.
- A `capability_class` outside the server-approved set is rejected when
  the Task is created, with an error enumerating the allowed values
  (`repository_read`, `repository_write`, `read_only`, `discovery_read`,
  `planning_read`). Previously a baseline authoring its own class names
  (e.g. "implementation") admitted the Task and then every dispatch
  failed with "not server-approved"; the chat `task.propose` contract now
  documents the allowed values.
- Workflow-dispatched worker/coder executions that "complete" without
  touching the repository or advancing the workflow now fail instead of
  leaving the task stuck `in_progress` forever. The failure consumes the
  normal execution retry budget (deferred auto-redispatch, block on
  exhaustion) and the reason is posted as a task comment so the next
  attempt's prompt carries the feedback. User-claimed runs keep the old
  completion semantics.
- Milestone readiness now gates on every Task bound to the milestone
  through governance, not only the Tasks its definition happens to list.
  A milestone definition authored before (or without) an explicit Task
  selection could compute `ready_for_release` while all of the
  milestone's governed implementation Tasks were still open — staging an
  empty release. Cancelled/archived/deleted governed Tasks do not gate.
- Native Gemini agents no longer fail large Task turns with
  `budget_exceeded`. Profiles were created with the generic conservative
  limits (128k context / 96k input) although every current Gemini API
  model serves a 1M-token context window; provider-aware defaults now
  apply (1M context / 800k input / 64k output). Because profiles are
  immutable, existing rows that carry the old generic triple baked into
  their config resolve to the provider defaults at load time — that exact
  triple can only be a baked default, never a deliberate choice of all
  three values.
- Gemini executions can now self-heal through workflow-guard rejections.
  The gemini adapter captures the CLI's `session_id` from the final
  `--output-format=json` document and honors `resume_session_id` via
  `--resume`, so the guard bounce-back loop (e.g. "plan checklist
  incomplete") sends the agent back to finish instead of hard-blocking the
  task on the first rejection, and the `resume_session` recovery action
  works for gemini-backed executions.
- Read-only executions that write anyway now fail loudly. A
  `planning_task`/`discovery` execution (or reviewer run) whose worktree
  changed used to have its work silently discarded while the execution
  reported `completed` — implementation work authored under the wrong task
  type vanished without a trace. The discard still happens (the policy
  stands), but the execution now fails with an error naming the task type
  and the fix.
- Auto-dispatch and Genesis role seeding no longer route work to a
  credential-less gemini agent (whose CLI exits immediately) when any
  other eligible agent exists.
- Tasks blocked by a workflow guard now show a "Send Back to Agent" action
  in the task detail sidebar, wired to the `resume_session` recovery.
  Previously the task showed no action at all and looked permanently stuck.
- Native (embedded) gemini agents are dispatchable again. The connection
  health probe sent the AI Studio API key as a Bearer token, which Gemini
  always rejects — every native gemini agent reported
  `provider_authentication_failed` and the task dispatcher refused to
  dispatch to it. The probe now authenticates with `x-goog-api-key`.
- The context-manifest secret guard no longer rejects the literal policy
  revision `forge-task-context-policy-1`: the `sk-` OpenAI-key marker only
  matches at a word boundary, so ordinary words like "task-context" pass.
  Previously every embedded Task execution failed with "protected values
  cannot be stored in context manifest policy_revision".

- Product Genesis project setup is now a durable, truthful operation. Project
  creation commits the Project and its handoff even when repository or role
  setup is incomplete, while a leased finite operation/checkpoint projection
  records the current state and typed retry/setup action. Replays reuse the
  same operation, target directory, repository row, primary link, and role
  assignments; a missing Worker or independent reviewer remains
  `setup_required` instead of becoming a log-only or self-reviewing success.
  When eligible principals exist, provisioning initializes or verifies one
  local git repository under `<workspace_root>/repos/` (first commit on
  `main`), registers/reuses its logical row, links it with Project-version
  CAS, and resolves canonical Worker/reviewer defaults while excluding
  Main/Project-bound identities.
  - `task.propose` binds the created Task to the project's active
    user-approved execution baseline server-side when the payload names a
    plan item but does not carry a full governance envelope; the payload
    gains flat `plan_item_id`, `milestone_id`, `capability_class`, and
    `risk_class` fields, and the chat tool schema documents the contract
    (milestone defaults to the baseline's primary milestone). A proposal
    that names no plan item still materializes as a pre-baseline,
    non-runnable planning record.
  - A baseline that declares no capability/risk classes no longer rejects
    every repository-capable Task; an empty class list now means
    "unconstrained" and the lease issuer's server-selected capability
    profile applies.
- Admitted agent `task.propose` actions now execute inline through the
  normal TaskService path. Previously nothing in the product ever executed
  them — the proposals stayed `proposed` forever and no Task was created.
  Validation failures surface to the agent as the tool call's error and can
  be corrected in-turn.
- The generic chat proposal tool schema is null-tolerant for optional
  fields (Gemini emits explicit nulls, which failed the runtime's argument
  validation and killed the whole turn) and now documents the
  `task.propose` payload contract in the tool description.
- Provider API-key injection (`auth_source: forge_provider`) actually
  reaches CLI executors now. `CommandOverrides` is `#[serde(flatten)]`ed
  into executor configs, so the key must be written to `config.env`; the
  injector wrote a nested `command_overrides` object that deserialization
  silently ignored, leaving every harness run on the CLI's own login.
- Gemini CLI executor: pass the prompt as `-p`'s argument (current CLIs
  reject a bare `-p` with the prompt on stdin), trust the task worktree via
  `GEMINI_CLI_TRUST_WORKSPACE`, and when a provider API key is injected,
  point the CLI at a Forge-owned `GEMINI_CLI_HOME` that selects API-key
  auth so a user-level OAuth `selectedAuthType` cannot override it.
- Pre-dispatch execution failures caused by the in-transition
  WorkspaceLease/version race are auto-resumable: the dispatcher's recovery
  pass re-dispatches outside the transition. Previously they were pinned
  `resume_policy: manual` and the role (typically the reviewer) stalled
  forever.
- Agent-attached milestone evidence stores a typed
  `AuthorizationProvenance` receipt. The old `{operation, action_id}` shape
  made every subsequent readiness evaluation fail with "corrupt immutable
  evidence authorization JSON".
- Genesis Charter approval is no longer blocked on "No eligible Project
  Agent is selected yet". When the Genesis session has no (eligible)
  preferred Project Agent, the server auto-selects a deterministic eligible
  one — account-owned, unpaused, current profile, not the active Main
  Agent, preferring identities without an active Project binding — so both
  the Charter card and the Main Agent's `charter.approval_target` action
  always carry a complete revision set. The card's empty state now only
  appears when the account has no eligible agent at all and links to Agents
  setup.
- The Genesis Charter card is pinned above the Main Chat timeline instead
  of trailing the newest message. It no longer scrolls away (or drags the
  view away from the composer) while the conversation streams, and its
  approval actions stay reachable; the expanded review scrolls internally.
- Typing `approve` (or `/approve`) in the Main Chat now performs the exact
  Charter approval and creates the Project. The composer routes the typed
  confirmation through the same authenticated exact-revision flow as the
  card's buttons (approval receipt, then atomic create-and-handoff), so it
  remains an interface confirmation — previously typed approval was a dead
  end ("awaiting interface confirmation") with no interface path from the
  keyboard. A bare `approve` is only intercepted while a proposal is
  actually awaiting confirmation; otherwise it is sent to the agent as a
  normal message.

### Added

- Project Agents now drive work autonomously between user messages. The
  wake pipeline is closed end to end:
  - Terminal execution outcomes land in the durable event ledger
    (`execution.failed` / `execution.completed`, Project-scoped), so a
    failed dispatch finally has a durable trace. Attention projects an
    `execution_failed` incident from it (auto-resolved by a later
    successful run or a terminal Task transition).
  - Project-scoped incidents with no more specific responder now wake the
    Project Agent binding identity (previously most wakes resolved to an
    executor identity that failed Project-scope eligibility, so nothing
    was ever admitted).
  - A new durable `agent-wake-turns` consumer delivers each admitted wake
    as a system-authored message plus queued turn on the woken agent's
    chat — the missing half of the wake design: leases were written but
    nothing ever consumed them. Delivery is idempotent across crash
    replay; on first startup the consumer fast-forwards past the existing
    ledger instead of replaying stale wakes; and a `retry_exhausted`
    incident about a chat's own turns is never delivered back to that
    chat (feedback loop).
  - Activating an execution baseline queues a "begin execution" turn for
    the Project Agent directly (a user approval is its own deterministic
    admission; no wake budget consumed), so approving the baseline is the
    single moment work starts.
  - Project Agent bindings start with a wake budget of 10/hour instead
    of 0 (which disabled autonomous wakes entirely); existing bindings
    are migrated.
  - The Project operating skill (revision
    `forge.project.orchestration/v1@2`) adds an AUTONOMOUS DRIVE
    contract: system-authored turns are work orders, setup runs to a
    single activation approval, claimed steps must exist as server
    records, and missing prerequisites with eligible reversible defaults
    are decided and recorded instead of asked. Turn admission now accepts
    a newer server-owned revision of the approved skill key (the
    immutable approval receipt keeps recording the revision selected at
    approval time; the binding must still carry the exact current server
    contract), and the immutable Charter handoff packet's recorded skill
    revision is validated as a revision of the same server-owned skill
    key rather than an exact match — so server-side skill upgrades no
    longer brick existing Project chats.
- The Charter lifecycle is anchored in the Main Chat history with durable
  system messages: one per proposed revision (name, vision, change
  summary), one for the exact approval receipt, and one when the Project
  is created and handed off. The proposal survives refresh and handoff in
  the transcript instead of living only in the transient card. Ids are
  deterministic, so replays and retries never duplicate an anchor.
  (`AgentChatMessageRepo::append_agent_chat_message` now allocates the
  sequence server-side and treats the message id as its idempotency key.)

### Changed

- The Main Agent Product Genesis operating skill (revision
  `forge.main.project-discovery/v2@3`) now mandates conversational
  discovery: normal turns are short prose without structured scaffolds or
  Charter dumps, Charter revisions are saved silently and acknowledged in
  one line, and the single full structured recap happens at the
  readiness-gate settle point where explicit approval is requested.
- The Genesis Charter card always shows the current revision (approved,
  else draft). The user-facing revision dropdown is gone — revision history
  stays internal — replaced by a "Compare with previous" toggle that renders
  the previous and current revisions side by side with changed fields
  highlighted and unchanged fields checked.

### Added

- Per-agent token usage, estimated cost, total runs, success rate, and duration analytics across the system:
  - `AgentResponse` includes `total_input_tokens`, `total_output_tokens`, `total_cache_read_tokens`, `total_cache_write_tokens`, `total_tokens`, and `total_cost_usd`.
  - `ProjectAnalyticsResponse` (`GET /api/v1/projects/{id}/analytics`) includes `by_agent` breakdown with token counts, estimated price, runs, success rate, and average duration.
  - Agent Settings detail panel displays token consumption and estimated cost cards.
  - Project Settings Analytics tab displays a "By Agent" breakdown table.

### Breaking

- The Agent Chat page no longer carries the Project workbench side rail
  (inline create forms for tasks, documents, decisions, and milestones), the
  "Project records" pill navigation, or the scope-affordance chips. The chat
  is a clean conversation surface; the agent performs those actions directly
  from chat (`task.propose` and the other chat-scope tools), and project
  configuration lives in Project Settings, linked from the chat header.

### Changed

- Agent Chat timelines (Main chat, Project chat, and the floating launcher)
  were redesigned: user messages render as right-aligned bubbles, agent
  replies render as markdown with a visible copy/meta/provenance row, and a
  turn's live state (queued/thinking/retrying) renders as its own expandable
  activity entry in the timeline — no longer inside the user message card —
  with cancel and retry actions always visible. Provenance inspection is a
  compact fingerprint icon in the message meta row, and messages more than
  two hours apart are separated by a timestamped session divider.
- The chat composer supports slash commands: typing `/` opens a command menu
  (↑↓ + Enter to pick). Product Genesis starts with `/start-product <idea>`
  instead of the removed "Start a product" button.

### Fixed

- The typed orchestration tool schemas are now flat `type: object`
  envelopes: several provider function-calling APIs (notably Gemini) accept
  only an OpenAPI-style object at the parameters root and silently drop
  `oneOf`, leaving models blind to the envelope. Per-operation payload
  shapes are summarized in a generated description, and the exact contracts
  stay enforced by the host and server validators, whose errors return to
  the model in-turn.
- `project.document draft_revision` creates the Document row on the first
  draft of an unknown id: a Charter-created Project starts with no Documents
  and the Agent surface has no separate create operation, so the Delivery
  Brief could never be drafted.
- Execution-baseline proposals no longer demand byte-exact server-derived
  values from the caller: `release_policy_digest`, `rendered_view`,
  `content_digest`/`render_digest`, and the Charter/Document ArtifactRef
  digests are resolved or derived server-side from the referenced ids (and
  verified only when a caller round-trips exact server values). Previously a
  model could never produce a valid baseline.
- `expected_baseline_version` and the render/digest fields are optional in
  the baseline payload validator; a fresh draft defaults to the created
  baseline's version.
- New `services` examples `baseline_probe` and `task_probe` replay the exact
  Project Agent tool calls against a database named by `FORGE_PROBE_DB`,
  so orchestration payload changes can be verified deterministically without
  driving a model.
- Charter-created Projects no longer fail every Project Agent turn with
  "Project primary milestone points at a Project with no active milestones":
  the primary-milestone pointer validator now accepts the freshly seeded
  `planned` milestone before activation.
- Project Agent turns after the first no longer fail with "Project Agent
  turn has no exact consumed Charter handoff": the worker re-verifies the
  handoff packet against its recorded consumption delivery instead of
  requiring the handoff to point at the current turn's own ids.
- `charter.draft` no longer requires `rendered_view`/`render_version` at any
  validation layer (tool schema, host validator, provider validator) — the
  server renders the canonical view itself and verifies those fields only
  when a caller round-trips exact server values; the typed tool also strips
  model-supplied values, which can never match the server renderer
  byte-for-byte.
- Failed coordination operations now log their underlying service error at
  warn level instead of collapsing silently into "Forge tool provider
  failed".
- The `db` crate has a `build.rs` with `rerun-if-changed=migrations`, so a
  newly added migration can no longer be silently missing from an
  incremental build (`include_dir` pitfall).
- The server-side orchestration authority guard no longer rejects payload
  keys named `scope`: the canonical Charter content schema requires a domain
  `scope` section, so every well-formed Main Agent Charter draft was refused
  with "scope and authority are server-derived". Scope-override injection
  stays blocked through the `scope_type`/`scope_id` keys.
- Typed orchestration tool schemas now emit `type` and a one-value `enum`
  beside every string `const` (and a `type` beside bare string enums).
  Gemini's function-calling schema subset drops bare `const`, so Gemini
  models emitted `{}` for `action` fields and every Charter/orchestration
  proposal failed schema validation.
- Migration `V081` repoints `operating_skill.current_revision_id` at the
  Main operating-skill revision `@2` that `V080` seeded but never activated,
  which made every Main Agent Genesis turn fail with "operating skill is not
  the canonical server contract". The error message now also says to restart
  the server so pending migrations reseed the skill.

- Agent management no longer surfaces profiles anywhere. An agent is one
  directly-editable definition — harness (CLI or direct), default model,
  reasoning, permission policy, system prompt, description — edited inline in
  the `/agents` detail panel: the settings render as live form fields and a
  Save/Discard bar appears once something changes, for both CLI-harness and
  direct agents (`ChangeModelDialog` and the profile list/selector UI are
  deleted). Profile revisions still exist internally as the immutable history
  of an agent's settings, but they are no longer a user-facing concept.
- Main and Project Agent bindings now name only the agent.
  `PUT /api/v1/account/main-agent` and `PUT /api/v1/projects/{id}/project-agent`
  no longer accept `profile_id`; every turn resolves the bound agent's
  *current* settings, so editing an agent applies to its next turn in every
  bound scope without rebinding. The binding row's stored profile reference
  is now a bind-time snapshot reserved for future per-binding overrides.
- The New agent wizard is harness-first: step 1 picks how the agent runs
  (a direct provider entry, or a CLI harness listed with its version), step 2
  sets the defaults — model, reasoning, policy, description, system prompt,
  and, for CLI harnesses, an optional powering provider entry.
- `forge-ctl embedded main set` and `forge-ctl embedded project set` drop
  their `--profile-id` flag to match the binding API.

- `AgentChatTurnStatus` gains `awaiting_input` to represent turns parked for
  runtime questionnaire interactions. API consumers and SSE event listeners
  must handle the new state. A parked turn references its pending protected
  interaction and releases its execution lease without consuming retry attempts.
- The pre-chat Product Genesis configuration form and permanent header button
  have been removed from the web UI. Discovery now starts directly from the Main
  Chat composer or empty state (composer text seeds the initial idea; omitted
  maturity defaults to `mvp`). Setup may be collected in-conversation and
  persisted once via the typed guided-setup action
  `POST /api/v1/account/main-agent/product-genesis/{session_id}/guided-setup`.
- Launching a task no longer offers a "Save changes to agent" checkbox on
  the launch dialog's model/reasoning/policy overrides. Those overrides
  remain execution-scoped (they only affect the run being launched); the
  side effect that silently PATCHed the agent's persistent defaults from the
  Task Detail launch flow is gone. Persistent model changes now live entirely
  in Agent Settings (`/agents`), edited inline in the agent's detail panel.

### Added

- New endpoint `GET /api/v1/providers/{id}/usage` reports a provider entry's
  account usage (e.g. ChatGPT's 5h/weekly rate-limit windows) so the UI can
  show it on provider entries. Only ChatGPT-OAuth (Codex backend) entries are
  probeable today, via the Codex CLI's `GET /wham/usage` endpoint; every other
  entry — and any probe failure — reports `source: "unknown"` with a redacted
  `detail` message and no windows. Usage is never fabricated as 0%.
- Main Agent Chat turns outside an active Product Genesis session now carry a
  server-owned baseline operating skill (`forge.main.baseline/v1`). Previously
  a plain Main Agent chat could reach the model with no system prompt at all,
  so the agent had no idea it was running inside Forge. The baseline states the
  agent's identity, scope, and boundaries, includes the bounded portfolio
  projection, and is recorded in the turn's context manifest.

### Changed

- Redesigned the Providers and Bindings tabs on `/agents`. Provider cards now
  lead with the entry's own name (provider name as subtitle), a clean
  Connected/Revoked/Error badge, and a small inline refresh button beside the
  header that refreshes the entry's status and usage; the per-card
  "test connection" box/buttons and the technical endpoint URL row are gone
  (the wizard's final step keeps only a quiet auto-run connectivity line), and
  the card's Disconnect button is styled destructive. CLI runtime cards show
  the runtime display name over the daemon host/id with an
  Authenticated/Not Logged In/Unavailable badge. The Bindings tab is now a
  master/detail matching the Agents tab: a searchable scope roster (Global
  Main Agent plus every project, each badged with its bound agent or
  "Not configured") on the left and the selected scope's binding configuration
  on the right, with `?project={id}` auto-selecting that project; the
  read-only "Bound agent scopes" projection stays below.
- Redesigned the `/agents` page. The Agents tab is now a Runtimes-style
  master/detail: a searchable/filterable roster on the left, one agent's
  model, profiles, and bound scopes on the right — the inline session list
  and capability strips are gone (profiles are the unit of continuity users
  act on; session internals stay reachable from the chat context inspector).
  Changing a model is now one `ChangeModelDialog` flow (pick an
  already-published profile, or publish a new model on a provider entry) that
  replaces the old two-step "publish profile" + "select profile" dance, and
  can update a Main/Project Agent binding to the result in the same submit.
  The Bindings tab now always lists every project's Agent binding — including
  "Not configured" ones — instead of only rendering a Project Agent section
  when the page was opened with `?project=`; that param now just
  scrolls/highlights the matching project's card. Provider entries on the
  Providers tab show their usage window(s) from the new usage endpoint. The
  dead legacy `web/src/pages/agents/` UI (unreferenced since the federated
  agents rewrite) is deleted.
- The agent dialogs on `/agents` use the launch-dialog selectors again
  instead of bare model/effort text fields. For a CLI-harness agent,
  `ChangeModelDialog`'s "Update settings" tab (renamed from "Update model")
  now offers the discovered-model combo box, the per-model reasoning
  selector, and the permission-policy selector (Auto / Supervised / Plan),
  persisting all three through `PATCH /api/v1/agents/{id}`. The New Agent
  wizard's configure step gains the same three selectors for CLI-harness
  runtimes (discovery keyed by executor type), sending `reasoning_effort`
  and `permission_policy` on creation.

### Fixed

- Native Agent Chat turns against a ChatGPT-OAuth (Codex backend) provider no
  longer fail with "Provider: Responses reasoning signature changed". The
  backend re-encrypts a reasoning item's `encrypted_content` on every
  serialization, so the terminal `response.completed` payload never matches
  the streamed item byte-for-byte; the embedded runtime's Responses adapter
  treated that as a malformed stream. Forge now uses the runtime's dedicated
  ChatGPT/Codex preset (agent-runtime `04a772d`), which carries the Codex CLI
  identifying headers itself and reads only usage from the terminal payload —
  the same contract Codex CLI applies.
- Starting a browser provider login no longer fails with "redirect_origin is
  not a configured trusted origin" when the UI is served from an origin the
  operator never configured (a LAN address, a hostname, or a non-default dev
  port). `POST /api/v1/provider-authorizations` now also trusts the origin
  the authenticated request itself arrived from — the `Origin` header from a
  browser, or the dialed `Host` (honoring `X-Forwarded-Proto`) from
  `forge-ctl` — since bouncing the browser back to where it already is cannot
  redirect it anywhere new. Explicitly configured `cors_origins` /
  `public_base_url` origins keep working as before.
- A native Agent Chat session no longer wedges permanently after a failed
  turn ("Conflict: cannot accept a new turn over a non-terminal checkpoint").
  The embedded runtime synchronized its lossless context memory (LCM) before
  checking whether the turn completed, so a mid-stream provider failure
  immortalized disowned history in the immutable LCM store; every later turn
  then hit "LCM immutable sequence has a gap" during finalization and the
  checkpoint could never reach terminal. The runtime (bumped to agent-runtime
  `b3f966b`) now skips LCM sync on non-completed turns and self-heals an
  already-diverged timeline by truncating the orphaned provisional tail;
  migration `V079` narrows the LCM entry delete guard so that truncation is
  possible while entries covered by summary nodes stay immutable. Existing
  wedged chats recover automatically on their next turn.
- A spent provider usage window (e.g. ChatGPT's `usage_limit_reached` 429) is
  no longer retried until the turn deadline and reported as the misleading
  "turn limit reached". The transport maps it to a non-retryable
  limit-exhausted error with the reset horizon, and Agent Chat turn jobs
  record the structured error code `usage_limit`.
- Agents built on a ChatGPT OAuth login (the Codex backend) now work. Native
  turns send the backend's required request headers (`chatgpt-account-id`,
  `OpenAI-Beta: responses=experimental`, `originator`), and the native
  transport now surfaces non-2xx provider responses as typed errors — a 401
  triggers the provider's credential-refresh path instead of being parsed as
  an empty event stream.
- OAuth-backed OpenAI agents are no longer permanently reported as degraded.
  The connection health probe (`GET /models`) has no such route on the
  ChatGPT Codex backend; any authenticated non-401/403 answer from that
  backend now counts as a healthy credential.
- Failed native Agent Chat turns now record the underlying runtime error
  (e.g. "turn limit reached", an auth rejection) in the turn job and the
  server log instead of the opaque "native Agent Chat turn failed".
- Browser OAuth login no longer fails with `redirect_origin is not a
  configured trusted origin` when the UI is served by Forge itself. The
  trusted-origin list now includes the server's own serving origin (both
  `localhost` and `127.0.0.1` spellings for a loopback bind, and the
  `public_base_url` origin when configured) in addition to the configured
  CORS origins.
- After a device-code (or browser) OAuth login succeeds, the Providers tab now
  refreshes immediately. Previously the new provider entry was stored
  server-side but the UI kept showing "No providers connected" until a full
  page reload, which made the login look like it had failed.

## [0.8.0] - 2026-08-15

### Breaking

- Agent navigation is consolidated around one canonical `/agents` settings
  surface. `/agents/federated`, the legacy `/agents/new` and per-agent UI
  routes, and the Project-local `project-agent` settings tab are removed
  without aliases. Main Chat now sits directly below the Project switcher, and
  each Project's former Chat entry is now an Agent Workspace with a scoped
  Project-record editing rail.
- `PATCH /api/v1/projects/{id}` now requires the current Project `version` and
  increments it on success; stale edits return HTTP 409. This prevents the
  Project Agent Workspace and Project Settings from silently overwriting each
  other.
- Credential handle responses now include `credential_method` and `version`,
  and disconnect requires that version and returns a redacted provider
  revocation outcome. Migration V078 classifies every existing protected
  credential as `api_key` while adding encrypted renewable OAuth bundles and
  finite provider authorization operations.
- Provider entries are now separate from agents. The single-shot
  `POST /api/v1/embedded-agents/connect` contract (credential + model + agent
  in one call) is removed. Connecting stores a provider entry only:
  `POST /api/v1/providers` for API keys, or a provider authorization operation
  for OAuth (its response no longer contains `profile_id`, and
  `StartProviderAuthorizationRequest` loses its identity/model fields). Agents
  are created afterwards referencing the entry: `POST /api/v1/embedded-agents`
  (`credential_id` + `model`) for the direct runtime, or `POST /api/v1/agents`
  with the new optional `credential_id` for a CLI harness with dispatch-time
  key injection. `GET /api/v1/agent-providers` moved to
  `GET /api/v1/providers/catalog` and each credential method now declares a
  `runtimes` compatibility matrix; `GET/DELETE /api/v1/credentials*` moved to
  `GET/PATCH/DELETE /api/v1/providers*` with usage data and dependent-agent
  reporting. `forge-ctl embedded connect` became `forge-ctl embedded create`
  (`--credential-id`), and `forge-ctl embedded credential` became
  `forge-ctl embedded provider` with `add`/`rename` support. The web Agent
  Settings surface is reorganized into `Providers` and `Agents` tabs with a
  three-step agent-creation wizard.

### Added

- `forge-ctl embedded provider login` signs in to a provider with OAuth from the
  machine the browser runs on. It binds the provider's localhost callback
  locally and relays only the authorization code to Forge, so browser login
  works against a remote server; the PKCE verifier and the tokens never leave
  the server. `--method device` prints a code instead.

### Fixed

- Browser OAuth used the requesting web origin as the OAuth `redirect_uri`,
  which OpenAI's Codex client never accepts — it whitelists only
  `http://localhost:1455/auth/callback` (or `:1457`). Forge now issues that
  loopback callback and, when the browser is on the server's machine, binds the
  port itself for the length of the ceremony. `StartProviderAuthorizationRequest`
  gained `loopback_owner` (`server`/`client`, default `server`) and
  `loopback_port` so a client that already owns the socket can say so. Browser
  login from a non-loopback origin is now rejected up front with guidance
  instead of failing at the provider. Gemini keeps its operator-registered
  Forge callback route.
- Device-code logins (OpenAI, xAI) no longer require the caller's origin to be
  in `server.cors_origins`. They never redirect a browser, so the trusted-origin
  check applied only to browser OAuth; previously any origin outside
  `cors_origins` failed with `redirect_origin is not a configured trusted
  origin`.
- The ChatGPT authorization URL now sends `id_token_add_organizations=true` and
  `originator`, and no longer sends Google's `access_type`/`prompt` parameters.

- Server-owned provider capability discovery and guided authorization through
  `GET /api/v1/agent-providers` and short-lived
  `/api/v1/provider-authorizations` operations. Forge supports stable API keys,
  experimental ChatGPT browser/device login, experimental xAI device login,
  and configured Google OAuth for the documented Gemini API without importing
  provider CLI credential caches.
- Native OpenAI Responses, xAI Responses, and Gemini Interactions adapters now
  acquire renewable credentials through Agent Runtime's host-injected lease
  contract. Refresh is single-flight, encrypted token rotation is atomic, and
  Main/Project Agent sessions remain deny-all for filesystem access.
- `POST /api/v1/providers/{id}/test` runs a live connection test against a
  provider entry's API (one minimal authenticated request; refresh-aware for
  OAuth bundles) and returns `status`, `latency_ms`, a redacted `message`, and
  `checked_at`. Secrets and provider response bodies are never echoed.
- Adding a provider is now a four-step wizard (choose provider → choose
  authentication method → connect → verify). The verify step auto-runs the
  connection test, and every provider entry card gained a `Test connection`
  action.

### Changed

- Agent Settings is now three tabs: `Providers`, `Agents` (roster only), and
  `Bindings` (Main Agent binding, optional Project Agent binding, and the
  chat-scope list). Project deep links (`?project=`) open the Bindings tab and
  `?tab=` deep-links any tab. When the server is unreachable, each tab shows a
  single retryable error panel instead of one per section.

## [0.7.4] - 2026-08-14

### Breaking

- Coordination mutation and typed execution envelopes now reject unknown JSON
  fields instead of silently discarding them; callers must use the exact closed
  request shape.
- Product Genesis Project creation now requires an explicit user approval
  receipt bound to the exact Charter revision, canonical content/render
  digests, selected Project Agent revisions, and idempotency key. The typed
  `CreateProjectFromCharterApproval` operation consumes that single-use receipt
  atomically with Project/binding/Chat/Charter/handoff creation; a ready Genesis
  brief or the removed `product_genesis_session_id` field cannot bypass it.
  Existing Projects adopt through an explicit `legacy_unverified` Charter
  approval, and no `handoff_pending` state or compatibility alias is provided.
- Release-pinned evidence now has explicit shared-media retention semantics.
  Task media IDs, URLs, storage keys, metadata, and file bytes remain in place
  without moving or duplicating bytes or claiming an on-disk layout break.
  Deleting a Task removes its Task attachment/URL under existing policy, while
  a successful user-approved `Mxxx-rN` release pins the same asset through an
  authorized Project evidence URL. Evidence attachment availability is
  `available`/`quarantined`/`redacted`/`purged`; removing an attachment marks it
  purged, while ordinary garbage collection preserves assets referenced by
  active attachments or immutable release pins. V076 and the internal
  shared-media repository persist audited redaction/purge tombstones and the
  `evidence_unavailable` release overlay without rewriting an immutable release
  manifest. Project owners/admins may now use the explicit, audited
  `POST /api/v1/projects/{id}/media/{asset_id}/redact` or
  `POST /api/v1/projects/{id}/media/{asset_id}/purge` mutation with the current
  asset version, idempotency key, authorization action, and bounded reason;
  redaction blocks serving through the Project media URL and marks pinned
  release evidence unavailable, while the legacy Task media URL keeps its
  existing behavior while its Task attachment remains active. Purge also
  removes bytes, so neither former URL serves them; neither disposition rewrites
  the release manifest.
- Agents are stable account-owned identities with immutable, selectable
  profiles. The legacy profile-shaped Agent representation and
  `/api/v1/projects/{id}/agent-links` surface are removed. The approved
  replacement has one active Main Agent binding per account and exactly one
  active Project Agent binding per operational Project; Task Worker/reviewer
  assignments remain separate and cannot satisfy a chat binding.
- The general-purpose collaboration surface is replaced by one global Main
  Agent Chat and one Project Agent Chat per Project. Participant lists,
  addressing, responder policies, bounded rounds, arbitrary threads, and the
  corresponding REST/MCP/CLI/types/event surfaces are removed without aliases.
  The intended REST resources are `/api/v1/account/main-agent`,
  `/api/v1/projects/{id}/project-agent`, `/api/v1/agent-chats`, and
  `/api/v1/projects/{id}/agent-handoffs`.
- New migrations begin at V071; V059–V070 are never edited. The forward-only
  migration preserves legacy conversation/collaboration message IDs, ordering,
  ordinary bodies, provenance, runtime metadata, sessions, LCM/memory links,
  protected-content audit links, Task history, and turn-job state. Ambiguous
  binding inference becomes explicit `agent_setup_required`; a primary Worker
  is never promoted, and expired leases become finite retry/terminal states.
  V075 quarantines the retired Room/membership tables under `legacy_*`, remaps
  Room-scoped memory to Agent Chat scope, and rejects new Room authority rows.
  The Charter, Project artifact, milestone, release, and shared-media metadata
  for this change are in the forward-only `V076` migration; V001–V075 remain
  immutable and existing media bytes stay in place.
- Forge now builds its embedded host against Agent Runtime revision
  `a7075b1d2dd1cee05db63bc480ff46b0f97ec239` and requires Rust 1.86 or newer for
  that integration.

### Added

- Configurable, least-privilege `forge_public_web_search` support for Main and
  Project Agent Chat. The tool is omitted when unconfigured, performs only
  bounded unauthenticated HTTPS requests with redirect/proxy/DNS-private-host
  protections, labels result text as untrusted, and never materializes or
  persists search output as a user decision.
- Direct embedded-agent creation and protected provider connection in Forge,
  including immutable native profile revisions, safe health/capability output,
  explicit Main Agent Chat/Project Agent Chat/Task sessions, rotation
  continuity, and deny-all filesystem access outside admitted Task
  Worker/reviewer sessions.
- Approved Main/Project Agent Chat replacement contract with immutable
  messages, finite visible turn states, explicit provenance-linked handoffs,
  bounded retry/lease recovery, and atomic Project-Agent binding/setup
  behavior; implementation lands with the V071+ migration.
- Forge-owned Agent Runtime hosting with per-identity/per-scope Lossless Context
  Memory timelines, SQLite LCM persistence, protected checkpoints and
  credentials, context manifests, and authorized provenance inspection.
- Scoped semantic-memory ACLs and publication/supersession provenance,
  persistent inbox items and evidence-backed commitments, typed action policy
  envelopes, a durable domain-event ledger, Attention projections, and bounded
  Mission Control/Agent-detail read APIs and web views.
- REST, MCP, and `forge-ctl` operations for embedded connections, Main/Project
  Agent bindings and chats, handoffs, sessions, commitments, context, and
  Mission Control (the replacement surfaces land with the V071+ migration).

### Changed

- Context-manifest source projections now report whether pointer-backed Project
  references are stale and, when present, the current canonical revision. This
  is a read-time overlay; immutable manifest selection decisions and
  fingerprints are unchanged.

- Agent Chat and Task outcomes commit to the durable event ledger; the
  in-process event bus is a delivery and cache-invalidation projection, not
  wake-up authority.
- Existing CLI Task executors remain available, including Smith. Forge does not
  add a Smith-native embedded backend or depend on the sibling TUI; the direct
  backend composes Agent Runtime in the Forge-owned `forge-agent-host` crate.

### Fixed

- Successful Agent Chat retries now clear stale retry diagnostics, and typed
  Project Agent Task proposals inherit the Project's default review policy when
  they do not supply a per-Task override.
- Repository-capable claims reject Main and Project Agent identities before
  creating a worktree or branch, recover a Task branch left by an interrupted
  pre-worktree attempt, and reject a second running repository execution or
  follow-up before mutating Task state.
- HTTP request tracing records only the request path, preventing access tokens
  and other sensitive query parameters from being written to server logs.
- Smith-backed Agent Chat bounds admitted assistant output to 500 characters
  before chat-history, semantic-memory, and FTS persistence, preventing a
  verbose turn from amplifying every later CLI prompt.
- CLI-backed Agent Chat turns now pass the executor/config snapshot required by
  the shared adapter, Product Genesis closes when the handoff cites the Main
  Agent's response, and typed Task proposals reject unknown task types before
  persistence instead of surfacing a SQLite constraint failure.
- Running repository executions renew their scheduler-owned WorkspaceLease
  while the exact execution and authority bindings remain valid. Charter-backed
  Task creation derives omitted governance instead of failing mainstream
  clients, and the pre-baseline discovery/planning read-only lane now passes
  transactional execution admission.
- Charter-backed Projects can be deleted through a guarded transactional
  teardown without weakening immutable-row protections. Milestone projections
  accept typed check/evidence blockers, Project Agent definitions use canonical
  `Mxxx` keys, primary-pointer validation applies only to active milestones,
  and Project Agent readiness now computes a snapshot rather than emitting an
  unconsumed request.
- Nested artifact fields named `scope` no longer masquerade as authority
  overrides, while root authority fields remain denied. Approval and manual
  check idempotency keys are scoped to their operation, Project/account, and
  authenticated principal, with access checks before replay lookup.
- Approved Documents and canonical context-manifest pointers now project fresh
  state correctly. Closed Task proposal payloads are validated before action
  admission, and malformed payloads cannot become approved-but-unexecutable
  ledger entries.
- Shared-media cleanup isolates per-row/per-phase failures and checkpoints
  reconciled purges, so one poisoned asset cannot halt garbage collection or
  permanently pin the sweep to its first page.

## [0.7.3] - 2026-08-09

### Fixed

- Reviewer and auditor executions now restore the task worktree to its exact pre-review commit and remove untracked review artifacts on both embedded and remote runtimes, preventing accidental reviewer edits or auto-commits from entering the task diff.

## [0.7.2] - 2026-08-09

### Fixed

- Claude Code auditor verdicts are now read from Claude's nested assistant-message and successful-result log formats, preventing valid reviews from failing with `verdict marker missing`.

## [0.7.1] - 2026-08-09

### Fixed

- `forge-ctl login` now hides interactively entered passwords, restores terminal settings after success, failure, cancellation, or EOF, and directs non-interactive callers to `--password-stdin` instead of consuming piped input implicitly.

## [0.7.0] - 2026-08-08

### Breaking

- Saving a workflow (workflow template save or project workflow update) now requires an explicit `canonical_phase` on every state; definitions without phases are rejected with a field-level error naming the offending state. Existing stored workflows continue to load and run unchanged — only re-saves must add phases.

### Added

- `CanonicalPhase` (`backlog`/`ready`/`working`/`review`/`done`) as the product-wide grouping language: optional `canonical_phase` on workflow states with ordered fallback derivation for legacy definitions, an additive `canonical_phase` field on task responses (derived at read time, never persisted), and a `canonical_phase` filter on project task lists that composes with existing filters (`phase=done` includes cancelled tasks).
- `autonomous_v1` builtin workflow preset: one `worker` role owns plan → implement → self-test; no planning gate; Forge-run `ci_steps` validation gates review and a failure automatically resumes the same worker thread; review requires human approval with no auto-dispatched reviewer; merge states stay within the Review phase; worktrees are cleaned up on done/cancelled.
- Intent-oriented task action endpoints: `POST /api/v1/tasks/{id}/{start|pause|resume|submit|request-changes|approve|cancel}` resolve the project workflow to the correct transition without clients hardcoding state names; unavailable actions return a structured 409 with `available_actions` and `reason`. The raw `/transition` endpoint is unchanged.
- Typed transition actor (`Actor`: User/Agent/System) replaces string-prefix actor checks throughout the engine, services, API, and MCP server. Audit log strings are format-compatible.
- Product terminology adapter in the web UI: user-facing copy now says Run (execution), Runtime (daemon), and Phase; routes and API names unchanged.
- Legacy workflow/DB compatibility fixtures with schema-parity regression tests, seeding future migration tests.
- Smith agents forward `reasoning_effort` to the CLI as `--effort`. `SmithConfig` gains an `effort` field, populated from the agent record like `model` already is; when the agent sets no `reasoning_effort`, no flag is emitted and behavior is unchanged. Requires a Smith build that accepts `--effort` — older Smith releases select effort only through a named profile or `SMITH_REASONING_EFFORT`.

### Changed

- Attribution fixes from the typed-actor refactor: human review approvals/rejections and MCP-initiated transitions are no longer audit-logged as `system`; claim hooks attribute the actual assignee. `AgentOnly` workflow hooks now match all system components, so the dependency gate applies to dispatcher-initiated launches. Human review rejections now record `rejection: true` and count toward the review retry budget. Display values: `system:daemon` → `system:executor` (execution `stopped_by`), `system` → `system:workflow` (recovery `blocked_by`).
- The task list `status` filter accepts custom-workflow state names instead of only built-in statuses.

## [0.6.1] - 2026-08-08

### Changed

- Smith execution options are now discovered from the user's `~/.smith/config.toml` — configured models, main-enabled profiles with their provider/model pairings, and provider names — instead of a hardcoded model list. Hosts without a Smith config discover empty lists.
- Bumped managed CLI pins: `@anthropic-ai/claude-code` 2.1.220 → 2.1.226, `@openai/codex` 0.146.0 → 0.147.0. `@musistudio/claude-code-router` stays on 2.0.0: v3 replaced the `ccr code <args>` pass-through with profile-based invocation and needs its own adapter rework.

## [0.6.0] - 2026-08-07

### Added

- First-class support for `Smith` (`smith`) CLI agent executor across `executors`, `cli-adapters`, embedded daemons, MCP tool descriptors, database migrations, and the web UI.
- Executor fallback chains: an agent's `config_json` may declare ordered `fallbacks: [{executor_type, config}]` candidates (e.g. multiple Smith provider profiles, or a cross-CLI fallback). Both the embedded path and remote daemons dispatch through a `FallbackExecutor` that advances only on structured availability failures (quota exhaustion, missing CLI, failed auth), keeps in-memory per-account cooldowns, aggregates token usage across attempted candidates, and logs every hop to the execution's JSONL log. The Smith and Claude Code adapters classify quota/auth signatures from structured stream signals only.
- New `FailureKind::ExecutorUnavailable`: when every candidate is unavailable, the task defers dispatch to the structured retry time **without consuming the execution retry budget** (transient), or blocks for manual reconfiguration (permanent). The generated `FailureKind` TypeScript union gains `'executor_unavailable'`.
- Daemon protocol: `ExecutionTerminalNotification` gains optional `failure_class`, `retry_at`, `resolved_candidate`, and `route_attempts` fields (additive — older daemons degrade to generic executor-failed handling).
- Session resume is now candidate-identity-aware: follow-ups promote the parent execution's winning candidate when it is still routed, and a candidate switch (including a different Smith profile on the same executor) starts a fresh session instead of replaying another account's session id. Smith executions now inject `resume_session_id` on follow-up like Claude Code/Cursor.

### Fixed

- Cancelling a shell execution now always SIGKILLs the whole process group after the grace period. Previously, if the direct child died to SIGTERM while a TERM-ignoring descendant survived, the escalation was skipped and the execution stalled until the descendant exited on its own (deterministic on Linux).
- Playwright smoke tests in CI use the container's bundled Chromium instead of requiring a Google Chrome install; local runs still use the `chrome` channel.

## [0.5.0] - 2026-08-03

### Breaking

- Replaced `PUT /api/v1/tasks/{id}/position` and its `PositionRequest`/`PositionResponse` types with the atomic, versioned `POST /api/v1/tasks/{id}/move` command. Project task-list pages now include `board_revision`; board clients must submit both that revision and the moved task's version.
- Board status moves now publish the canonical `task.moved` SSE event instead of a `task.status_changed` event for the direct move. The payload includes operation identity, old/new status and position, resulting task and board versions, and requested neighbors.

### Added

- Persisted project board revisions and idempotent move-operation records, with transactional neighbor validation, position renormalization, actionable concurrency conflicts, and replay of completed operation IDs.

### Changed

- The production web client now lazy-loads route screens and editor-backed dialogs, and the server Brotli/gzip compresses eligible responses while retaining immutable cache headers for hashed assets.
- Updated the Forge-managed Codex CLI to 0.146.0 and Claude Code CLI to 2.1.220. Adapter discovery now advertises the current GPT-5.6 and Claude 5 model families plus model-specific reasoning choices, including Codex `max`/`ultra` and Claude Code `xhigh`/`max`/`ultracode`; Gemini discovery now includes its stable model aliases and current Gemini 3.x catalog, and the web selectors filter effort choices for the selected model.

## [0.4.0] - 2026-07-03

### Changed

- Task interruption kinds are now a closed, typed vocabulary (`FailureKind`): `Task.blocked.kind`, `Task.failed.kind`, the blocking annotation `type`, and the `task.blocked`/`task.failed` event payloads carry an enum value instead of a free string. Wire values are unchanged for all known kinds; the generated TypeScript types narrow from `string` to the union. Classification of recovery actions now depends only on the structured kind — rewording a reason/message no longer changes which actions are offered.
- Migration `V056__normalize_failure_kinds` renames legacy aliases in existing rows (`retry_budget_exhausted` → `retry_exhausted`, `crash` → `executor_failed`, `hook_failed` → `before_work_hook_failed`) and adds a structured kind to rows that were previously classified only by their reason phrasing. Unmappable kinds are preserved and render as info-only interruptions with no recovery actions.
- The web client no longer infers gate rejection semantics from workflow state names (`*_failed` suffixes). Reject buttons appear only when the workflow declares a `reject`/`fail` trigger edge or `gate_config.reject_target`; workflows relying on naming conventions must declare the edge.

### Added

- Notifications for hard task failures (`task.failed`) and for crash-recovery or agent-timeout states that need manual intervention (`task.recovery_required`). Graceful-shutdown recoveries auto-resume at startup and are not notified; user-initiated recovery actions are not echoed back as notifications.
- Failed lifecycle-hook details (command, exit code, stderr/stdout tails) now surface in the `workflow_exception.failing_step` summary, so the recovery panel shows them wherever it renders.

### Changed

- The task board modal now renders the same actionable recovery panel as the full task page, driven by `workflow_exception`. Failed tasks were previously a dead end in the modal (message with no actions); they now offer Restart Task / Cancel Task. `TaskBlockingBanner` is reduced to an informational fallback for interruption states without recovery actions.
- Failure severity colors are no longer inverted in the task UI: hard failures render red, recoverable blocked states amber.

### Fixed

- A hard-failed task with a leftover blocking annotation no longer offers retry/resume actions that the server rejects with 400: `failed_json` now supersedes the annotation in the derived `workflow_exception` (offering only Restart/Cancel), and `fail_task` clears the stale annotation at write time.
- The recovery panel no longer shows two "Cancel Task" buttons when the backend action list also contains `cancel_task`.

## [0.3.0] - 2026-07-02

### Added

- `DaemonReportRequest.active_execution_ids` — optional list of execution ids the reporting daemon is currently running. When present, the server reconciles stale server-side running executions owned by that daemon. Long-running daemon processes (`forge-daemon`, `forge-ctl daemon start`/`link`) claim their active set from startup onward; finished ids linger in reports for 120s so in-flight completions are never reconciled away.
- New execution stop reason `daemon_disconnected` and SSE event `execution.daemon_disconnected`, emitted when the server interrupts an execution whose remote daemon went away (120s disconnect grace via the heartbeat monitor, or immediately when a restarted daemon reports without the execution).

### Fixed

- Executions on a dead or disconnected remote daemon are now failed promptly with `stop_reason = daemon_disconnected` instead of waiting for the 300s activity stall timeout and being mislabeled `execution_stalled`. Failed executions follow the normal retry budget before blocking the task.
- The shell executor now honors `command`, `args`, and `env` from the agent config snapshot (previously silently ignored; empty configs keep the `sh -c <description>` default). Cancelling an execution whose process already finished is a no-op instead of an error.
- The heartbeat monitor no longer routes stall-cancellation of remote-daemon executions through the embedded executor.

## [0.2.0] - 2026-07-01

### Breaking

- Removed REST endpoints that had no consumers (web, CLI, or MCP): the legacy non-state-scoped gate decisions `POST /api/v1/tasks/{id}/gates/approve` and `/gates/reject` (use the state-scoped `/gates/{state_name}/approve|reject`), `GET /api/v1/tasks/{id}/conflicts`, `POST /api/v1/tasks/{id}/conflicts/abort`, `POST /api/v1/tasks/{id}/rebase`, `GET /api/v1/runtimes` and `/runtimes/{id}`, and the bare `GET /api/v1/workspaces/{id}` (`/workspaces/{id}/diff` remains).
- Removed the `override` field from `TransitionTaskRequest`; it was never read — user routing auto-escalation applies unconditionally, so observed behavior is unchanged.
- `forge-cli`'s build script now skips the frontend build only when `FORGE_SKIP_WEB_BUILD` is `1`/`true`/`yes` (previously any value, including `0`, skipped it).

### Added

- The JWT signing secret is now configurable via `server.jwt_secret` in the config file or `FORGE_JWT_SECRET`; when unset, Forge generates a random 32-byte secret on first start and persists it to `<data_dir>/jwt_secret.bin` (mode `0600`). Bcrypt cost is configurable via `server.bcrypt_cost` / `FORGE_BCRYPT_COST` (default 12).

### Changed

- User-initiated task transitions on subtasks now resolve against the project workflow, fixing rejections such as `state 'review' is not defined in workflow` when dragging a subtask to a state the board offered. Users may route a task to any defined workflow state, overriding missing-edge and system-only routing restrictions; content guards still apply. Override transitions are audited as `triggered_by = "user:override:<source>"`.

### Fixed

- Updated the Rust lockfile to pull patched `quinn-proto` and `anyhow` releases so `cargo audit` passes for the 0.2.0 release.

- User moves no longer fail with `state '<name>' is not defined in workflow` from downstream layers: the workflow is resolved once per transition and threaded through hooks and cascades; all undefined-state errors now enumerate the defined states. Any user move that changes state cancels in-flight executions, and parking a task to backlog keeps its agent assignment without relaunching.

- The false "Recovered after server restart" banner: crash recovery now annotates only tasks whose running execution it actually cancelled, skips user-assigned tasks, and clears stale recovery banners automatically at startup.

- Production servers previously signed session JWTs with a hardcoded development secret at bcrypt cost 4; they now use the configured or per-install generated secret at cost 12.

- Fixed memory search pagination so cursors follow the result ordering, escaped punctuated memory search input before passing it to SQLite FTS, and made review/execution/conversation memory indexing idempotent by source reference.

## [0.1.11] - 2026-06-08

### Added

- Memory layer: a new append-only `memory_item` store (FTS5-indexed) that automatically captures execution summaries, reviews, task comments, failure/hook-error transitions, and conversation messages as searchable, project-scoped, attributed memories.
- New REST endpoint: GET /api/v1/projects/{id}/memory/search — project-scoped layered memory search with pagination
- New REST endpoint: GET /api/v1/memory/{id} — memory item retrieval by id
- New MCP tool: forge_memory_search — project-scoped memory search with injection-guard wrapper
- New MCP tool: forge_memory_get — memory item retrieval by id
- New REST endpoint: POST /api/v1/memory/backfill (admin) — backfill memory index from existing data
- New CLI command: forge-ctl memory backfill
- Effective prompt preview: GET /api/v1/tasks/{id}/prompt-preview (read-only, no dispatch), MCP tool forge_preview_prompt, and CLI forge-ctl task prompt-preview

### Changed

- Prompt contracts v2: all default prompt builders updated with managed-execution contract, explicit role boundaries, structured handoffs (coder family), and structured reviewer findings. Builder ids bumped: coder_implementation_v1→v2, coder_review_fix_v1→v2, coder_merge_fix_v1→v2, reviewer_default_v1→v2, planner_default_v1→v2, generic_default_v1→v2.

### Fixed

- Task comments created through the REST API were not indexed into the memory layer because the handler bypassed the indexing service path; user comments now route through `TaskService::add_user_comment` and are indexed.
- Codex executor model list now advertises currently supported models (gpt-5.5, gpt-5.4, gpt-5.4-mini, gpt-5.3-codex-spark); removed stale entries (gpt-5.3-codex, gpt-5.2-codex, gpt-5.1-codex-max, gpt-5.4-fast) that the current Codex CLI rejects.

## [0.1.10] - 2026-06-06

### Fixed

- Daemon command-stream disconnects now mark the daemon offline immediately, server startup clears stale external daemon online state, and command-stream heartbeats refresh last-seen state while the daemon remains connected.
- Task and workspace diff endpoints now compare against the workspace branch's merge base instead of the moving default branch, so unrelated default-branch changes do not appear in task diffs.

## [0.1.9] - 2026-06-03

### Fixed

- Daemon link/start/report now create the configured workspace root before reporting it, so Add Local Repository can browse the launch directory instead of failing on `path=.` when the directory is missing.

## [0.1.8] - 2026-06-03

### Fixed

- Fixed existing databases that already recorded migration version 53 before the Cursor executor migration so daemon reports can create `cursor` agents.

## [0.1.7] - 2026-06-03

### Added

- Added `forge-ctl daemon start` to restart a previously linked daemon from saved credentials without repeating initial registration.

## [0.1.6] - 2026-06-02

### Fixed

- User-managed task and subtask status moves are no longer blocked by dependency or root-managed subtask guards; AI dispatch and execution launch still enforce dependency gates before starting work.
- Board status transitions now retry genuine task version conflicts once and show the real API error for other HTTP 409 responses.
- MCP initialize responses now report the crate package version instead of a hard-coded server version.

## [0.1.5] - 2026-05-30

### Added

- Added a first-class Cursor executor backed by `cursor-agent` headless stream JSON mode, including daemon detection, agent registration, web UI configuration, session resume, and execution log normalization.

### Changed

- Updated the Forge-managed Codex, Claude Code, and Claude Code Router package pins to their current npm `latest` versions.

### Fixed

- Linked `forge-ctl daemon link` sessions now keep the daemon command stream open so filesystem browsing and remote agent dispatch work from server-managed local daemons.
- Daemon reports with a full authenticated CLI set no longer fail while checking existing daemon-scoped agents.
- Remote daemon `execution.start` failures now fail and block the execution for recovery instead of leaving it stuck in `running`.
- `forge-ctl` now defaults to the stored login server before falling back to the last local server state.
- Project list responses from older servers without `project_hooks` fields deserialize correctly.
- Repo-less tasks no longer auto-dispatch agent work, and stopped executions now surface in workflow health.

## [0.1.4] - 2026-05-21

### Added

- Added the project-wide hook engine with committed task-event evaluation, all-work-completed trigger support, hook actions for dispatching agents, creating tasks, comments, and notifications, plus hook-run history access.
- Added project hook persistence and observability foundations: `project_hooks_json`, `task.is_automation`, `project_work_epoch`, the `project_hook_run` table, `project_hook.run_changed` events, and `ProjectHookRule`/trigger/action/run response API types.
- Added `project_hooks` to project API responses and `PATCH /api/v1/projects/{id}` so project-wide hook rules can be validated and persisted.
- Task terminal sessions (disabled by default; enable via `terminal.enabled`), including `POST/GET /api/v1/tasks/{id}/terminals`, `GET /api/v1/tasks/{id}/terminals/availability`, `GET /api/v1/terminals/{id}`, `POST /api/v1/terminals/{id}/attach-token`, `POST /api/v1/terminals/{id}/resize`, `POST /api/v1/terminals/{id}/terminate`, `GET /api/v1/terminals/{id}/ws`, and the `task.terminal.session_changed` SSE event.

### Fixed

- Terminal resize/start now rejects row or column counts below 2 with `invalid_input`, drops reconnect scrollback after all clients detach, validates terminal session limit config on load, and serializes web reattach attempts.
- Refreshed the Rust dependency lockfile and compatibility fixes so `cargo audit` and Rust CI pass on the current stable toolchain.

### Breaking

- Task media now requires access to the owning project, restricts media deletion to project owners/admins, and rejects SVG uploads instead of serving them as inline media.

## [0.1.3] - 2026-05-16

### Added

- Linux release artifacts now include musl builds for Alpine and other musl-based environments.

## [0.1.2] - 2026-05-16

### Changed

- npm bootstrapper no longer opens a browser by default; pass `--open` to opt in.
- Forge persists the selected local server port so `forge-ctl` can discover the server without a manual `--server` URL.

## [0.1.1] - 2026-05-16

### Added

- `forge-ctl login`, `logout`, and `whoami` commands for API-token based CLI auth.
- MCP install flows can create/login with API tokens before writing client config.
- npm bootstrapper package so users can start Forge with `npx @forgeailab/forge`.

## [0.1.0] - 2026-05-15

### Added

- Initial public beta of the local-first Forge workflow engine.
- Rust server, REST API, MCP endpoint, `forge` server binary, `forge-ctl` client binary, and web UI.
- Task lifecycle, isolated workspaces, agent registration, execution logs, review flow, and merge flow.
- CI coverage for Rust workspace tests, web unit tests, cargo audit, and a Playwright app-shell smoke test.
- Release archives for Linux and macOS containing `forge`, `forge-ctl`, and built web UI assets.
- GitHub release checksum generation through `SHA256SUMS`.
- Docker image publishing to GitHub Container Registry with provenance and SBOM metadata.
- Public repository metadata for generated release notes, code ownership, dependency updates, CodeQL, and OpenSSF Scorecard.
- Runtime support for installed web UI assets through `FORGE_WEB_DIST_DIR` and the standard `share/forge/web/dist` location.
