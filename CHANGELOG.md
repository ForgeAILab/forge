# Changelog

All notable changes to Forge are documented in this file.

Forge follows Semantic Versioning. During the `0.x` public beta period, APIs and workflows may change between minor versions.

## [Unreleased]

### Breaking

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
