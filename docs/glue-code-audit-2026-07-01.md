# Glue-Code & Dead-Path Audit

**Date:** 2026-07-01
**Scope:** whole workspace (16 crates + `web/`), focused on (a) glue code — duplicated wiring, parallel implementations, construct-then-patch patterns — and (b) features that exist but don't actually work end to end.
**Method:** codegraph structural queries, targeted greps, full `cargo test --workspace --no-fail-fast` (98 test binaries), two scoped exploration passes (daemon/executor layering; API-surface drift), and manual review of the `add-user-transition-override` change (Codex gate timed out).
**Companion doc:** [architecture-review-task-actor.md](architecture-review-task-actor.md) covers the task/actor problem in depth; this audit confirms its findings are still current and have grown worse (see G1).

---

## Part 1 — Things that don't really work

### B1 — Critical: JWT auth signs with a hardcoded dev secret; the `jwt_secret` config is dead code

`AppState::with_adapter_registry_services_and_shutdown` — the constructor `forge-cli/main.rs:204` uses in production — builds the auth service as:

```rust
// crates/api/src/state.rs:239
let auth_service = Arc::new(AuthService::new(
    Arc::clone(&db),
    b"test-jwt-secret-for-development".to_vec(),
    4, // low cost for tests
));
```

`with_effective_config` (state.rs:287) rebuilds the OAuth and terminal services from real config but **never replaces `auth_service`**. Meanwhile the config crate fully plumbs a secret that nothing reads:

- config file key `server.jwt_secret` (`config/src/file.rs:24`)
- env `FORGE_JWT_SECRET` (`config/src/loader.rs:144`)
- CLI override (`loader.rs:192`)
- `jwt_secret_path()` helper (`config/src/types.rs:136`)

`rg jwt_secret` outside `crates/config/` hits only `AuthService`'s own field. Every install signs session JWTs with the same compile-time constant at bcrypt cost 4, while `docs/api.md:7` states "Authentication is required on all non-exempt routes." Loopback-only binding mitigates this locally, but api.md explicitly contemplates reverse-proxy exposure.

**Fix:** consume `config.server.jwt_secret`, falling back to generating and persisting a per-install secret at `jwt_secret_path()`; take bcrypt cost from config with a sane default. Small change, closes the hole.

### B2 — Resolved: removed the unused knowledge lifecycle plugins

The knowledge inject/capture pair was removed after confirming that nothing
consumed `.forge/knowledge-context.md` or the capture commit. Lifecycle hooks
now contain only configured plugins supplied by an embedding runtime.

### B3 — High: `max_turns` is only enforced for embedded executions

The embedded runner counts assistant turns from the log stream and force-cancels on budget exhaustion (`services/src/task_service/execution/runner.rs:196-341`, `annotate_max_turns_exceeded_block` at 638). The remote path (`forge-client/src/daemon_runtime.rs:249-298`) forwards `max_turns` into `ExecutionContext` but implements no counting or cancellation, and nothing server-side in `daemon_transport/` compensates. A task with a turn budget dispatched to a linked/remote daemon runs unbounded — silent behavioral divergence between the two daemon modes.

### B4 — High: the memory layer is invisible to the agents it was built for

MCP tools `forge_memory_search` / `forge_memory_get` and the REST/CLI surfaces work, but no dispatch prompt, `default_tool_names()` (`workflow/dispatch/mod.rs:333-348`), or agent template ever mentions them. The memory-layer proposal explicitly deferred prompt wiring to "Phase 3 (context packs)", so this is a known gap, not a bug — but as shipped, an agent only discovers memory via raw `tools/list`. The design intent ("automatically get relevant context at claim/launch/follow-up", `docs/synapsis-memory-report.md`) is not yet implemented anywhere.

### B5 — Medium: flaky workspace test caused by a shared on-disk template store

`workflow_template_full_crud_flow` failed in a full-workspace run (404 vs 200 at `crates/api/tests/workflow_templates.rs:132`) and passes in isolation and in a second full run — a parallelism flake. Root cause candidate: every test `AppState` hardcodes the **same** directory:

```rust
// crates/api/src/state.rs:118
let workflows_dir = std::env::temp_dir().join("forge-test-workflows");
```

98 test binaries run in parallel under `cargo test --workspace`, all sharing one on-disk `WorkflowTemplateService` store; one binary's writes/cleanup race another's list. Use a unique temp dir per `AppState` (e.g. suffix a UUID) — production is unaffected since `main.rs` passes `config.workflows_dir()`.

### B6 — Medium: namespaced system actors are rejected on system-only edges

`transition_inner` gates system-only edges with `triggered_by != "system"` (exact compare). `initial_scheduling.rs:98` emits `"system:task_dispatcher"`, which fails that compare — if the dispatcher ever drives a system-only edge it is rejected as if it were a user. Same class of string-protocol bug as F1/F2 in the actor review; fold into that refactor. (Tracked in `docs/spec/changes/add-user-transition-override-2026-06-28/tasks.md` §9.3.)

### B7 — Medium: two decorative public surfaces from the override change

Manual review of `9077c43` (standing in for the timed-out Codex gate):

- `TransitionTaskRequest.override` (`api-types/src/requests.rs:127`) is deserialized and **never read** — auto-escalation applies unconditionally. A public REST field with no behavior.
- `WorkflowEngine::user_override_transition` (`workflow/engine/mod.rs:144`) has zero production callers; only its own tests use it. The live path is the escalation arm inside `transition_inner`.

Both tracked in the change's `tasks.md` §9. The shipped *behavior* (guards still enforced, audit trail correct, version locking intact) reviewed clean.

---

## Part 2 — Glue code

### G1 — The actor-string glue is growing (companion doc, updated numbers)

[architecture-review-task-actor.md](architecture-review-task-actor.md) remains accurate, but two of its counts are already stale in the bad direction:

- Inline `WorkflowEngine { ...11 Arcs... }` reconstruction: **6 sites** now, not 2 — `task_service/transition.rs:46,433` plus `task_service/execution/recovery.rs:965,1430,1488,1692`.
- `triggered_by` string-prefix checks: ~10 sites across two crates, including a new `"user:override:"` sub-protocol introduced by the override change (`workflow/engine/mod.rs:154,159,596,669,688`, `hooks.rs:96-97`, `actions/review.rs:373`, `actions/gates.rs:270`, `transition.rs:544`).

Every month of delay adds sites the Actor refactor must touch. This is the single highest-leverage cleanup in the codebase.

### G2 — `AppState` construction is construct-then-patch glue

`crates/api/src/state.rs` builds services with defaults, then partially rebuilds them:

- Telescoping constructors: `new` → `with_adapter_registry` → `..._and_shutdown` → `..._services_and_shutdown` (9 args, `#[allow(too_many_arguments)]`).
- `with_effective_config` (state.rs:287) **reconstructs** `OAuthService` and `TerminalService` and re-registers their handlers on `cleanup_scheduler`/`daemon_connections` — everything constructed before it with the placeholder config is thrown away, except `auth_service`, which is forgotten (→ B1). Any service added later must remember to join this second-pass rebuild.
- Background tasks spawn inside the constructor with dropped handles: `NotificationService::start()` (state.rs:225) and `OperatorStatusEmitter::start` (state.rs:234). Every `AppState` (including every API test) leaks unstoppable tasks; the shutdown path in `main.rs` never stops them.
- Two-phase `set_*` wiring for cycles: `execution_events.set_task_service(weak)`, `daemon_connections.set_embedded_execution_context(...)`, `set_terminal_event_handler(...)` ×2, `set_terminal_cleanup_handler(...)` ×2.
- `main.rs` pre-builds `merge_service`/`cleanup_scheduler`/`review_runner` to pass in, while the test constructor builds identical ones itself — duplicated recipes.

**Fix direction:** one builder that takes `ForgeConfig` up front (no placeholder-then-patch), constructs each service once, and returns join handles for everything it spawns.

### G3 — `forge-daemon` is an abandoned parallel binary

`crates/forge-daemon/` (~1,900 lines) duplicates what `forge-ctl daemon link/start` already does — it even depends on `forge-client` and calls the same `run_command_stream`, but re-wraps it in a second CLI. Docs only ever tell users to run `forge-ctl daemon ...`; `forge-daemon` is unmentioned and untouched since 0.1.9 while `forge-client/src/daemon.rs` kept evolving. Delete it, or make it the blessed deployment artifact and delete the `forge-ctl` subcommand.

### G4 — Copy-paste triplication across the daemon boundary (with real drift)

- **CLI detection** (`detect_clis` / `binary_version` / `availability_status`): near-verbatim ×3 — `forge-daemon/src/detect.rs:9-93`, `forge-client/src/daemon.rs:370-451`, `services/src/embedded_daemon.rs:186-273`. Even `DEFAULT_REPORT_INTERVAL_SECONDS = 60` is duplicated.
- **PTY terminal handling**: `forge-daemon/src/terminal.rs` vs `services/src/terminal_service.rs:884-996`; helpers `command_builder`, `default_shell`, `pty_size` are byte-identical (`terminal.rs:405-452` ≡ `terminal_service.rs:1483-1523`), and the two have already diverged on path-containment validation.
- **FS browsing**: `daemon_transport/fs_local.rs` vs `forge-client/src/daemon_fs.rs` — identical sort closures, drifted skip-lists: the remote path hides `.DS_Store` (`daemon_fs.rs:15`), the embedded path shows it. User-visible inconsistency caused purely by copy-paste.

**Fix direction:** a small shared crate (or a module in `executors`/`workspace`) for CLI detection, PTY plumbing, and fs listing; both daemons consume it.

### G5 — Three sources of truth for API types, synced by hand

Rust `api-types` → ts-rs bindings in `crates/api-types/bindings/` (197 files) → hand-copied snapshot in `web/src/types/generated/bindings/` (**42 files behind**; no copy script exists anywhere) → plus a 1,610-line hand-written `web/src/types/generated/api.ts` re-declaring shapes ts-rs already generates (`TerminalSessionResponse`, `MemorySearchResponse`, `CreateTerminalSessionRequest`, ...). `index.ts` re-exports both, so which declaration wins depends on import order of names.

Related: `MemoryBackfillResponse` is hand-rolled in `forge-client/src/memory.rs:20` because the canonical type lives in `crates/api/src/routes/admin.rs:35` instead of `api-types` (violating the repo's own four-places rule), so the CLI had no import path.

**Fix direction:** make ts-rs emit directly into `web/src/types/generated/bindings/` (single step, no copy), move the admin/memory response types into `api-types`, and start shrinking `api.ts` toward zero.

### G6 — Dead and orphaned API surface

- **Legacy gate routes**: `/api/v1/tasks/{id}/gates/approve|reject` (`api/src/lib.rs:353-360`, handlers `gates.rs:42-123`) — zero callers anywhere (web uses the state-scoped `/gates/{state_name}/...` variant; no CLI, no MCP, no test). Superseded code that should have been deleted per beta policy.
- **Conflicts/rebase surface**: `/tasks/{id}/conflicts`, `/conflicts/abort`, `/tasks/{id}/rebase` — no consumer, no route-level test; real conflict recovery goes through follow-up executions.
- `/api/v1/runtimes`, `/runtimes/{id}` — no consumer, no test.
- `/api/v1/workspaces/{id}` (bare) — orphaned; UI uses `/tasks/{id}/diff`.
- Admin endpoints (`/admin/users*`, `/admin/settings*`) — no web UI, no CLI, no MCP client at all.
- `CancelledExecution.executor_config_snapshot_json` / `.agent_id` (`services/src/recovery.rs:441-448`) — populated, `#[allow(dead_code)]`, never read: leftover scaffolding for an unbuilt auto-resume feature.
- `forge-daemon/src/commands.rs:14` `handle_request` — `#[allow(dead_code)]`, only its own tests call it.

### G7 — `TaskService` builder-chain duplication beyond the wrapper

The 9-call `TaskService::new(...).with_*(...)` chain appears twice **inside one function** (`workflow/actions/dispatch.rs:179-203` and `:285-308`) in addition to `AppState`. With the 6 inline engine reconstructions (G1), there are now three distinct hand-maintained recipes for assembling the same dependency set. A new dependency must be added in up to 9 places or it silently doesn't reach one path.

### G8 — Documentation drift

- `docs/architecture.md:168` and `CLAUDE.md:102` still describe the "legacy `TaskStatus`/`transition_allowed` path" as live. It isn't (actor review F3, still unfixed) — and it misdirected this audit too until verified.
- `docs/api.md` mentions ~37 of **131** registered `/api/v1` route paths (94 undocumented, including whole features: conversations, admin, notifications, workflow-templates, daemons). Reverse direction is clean: 0 documented-but-missing routes.
- MCP: 24 tools implemented, **10 documented** (14 missing, including `forge_transition_task` — the tool the actor review centers on).

### G9 — Dev-loop glue: `forge-cli/build.rs` builds the frontend on every cargo invocation

`build.rs` runs `pnpm install --frozen-lockfile` + `pnpm run build` even for `cargo clippy` and `cargo test`. *Correction (2026-07-01):* a `FORGE_SKIP_WEB_BUILD` escape hatch already existed in `build.rs` — the real gap was that it was documented nowhere (not in CLAUDE.md, docs, or Makefile). Resolved: the variable now requires a truthy value and is documented in CLAUDE.md's build section.

### Cleared (checked, not glue)

- `workflow/actions/*` vs `task_service/execution/*` — **not** duplicated: actions delegate into `task_service` / `merge_service` / `review_runner`; the business rules live once.
- All six CLI adapters in `default_registry()` are real implementations; `NullAdapter` is demo-only; `AdapterExecutor` is the sole production `TaskExecutor`.
- The three "recovery" subsystems (crash-recovery annotate, user-driven `RecoveryAction`, dispatcher catch-up) are coordinated, not racing — though two unrelated functions named `recover_task` (`services/src/recovery.rs:492` vs `task_service/execution/recovery.rs:4`) are a rename-worthy trap.
- `db::execution_transition_allowed` / `review_transition_allowed` are alive (used by `sqlite/execution.rs:168`, `sqlite/review.rs:34`).
- Frontend calls no phantom endpoints; MCP production mount shares the fully-wired `TaskService` (the bare `mcp_server::AppState::new` is test-only — but that means MCP tests can never exercise dispatch happy paths, and `claim.rs:265` swallows dispatch errors, so a future bare-state caller would silently no-op).
- Full test suite: 98 binaries green on a clean run (one flake, see B5).

---

## Suggested order of attack

1. ~~**B1** — wire the JWT secret~~ **Done 2026-07-01**: config/env/persisted-file resolution in `crates/config/src/jwt_secret.rs`, threaded through the production constructor; bcrypt cost configurable (default 12).
2. **G1/B6** — execute the already-planned Actor refactor (`architecture-review-task-actor.md`); it collapses the string protocols, the 6 engine reconstructions, and the wrapper. Do it before more `"user:override:"`-style growth.
3. ~~**Deletions**~~ **Done 2026-07-01** except `forge-daemon` (delete-or-bless still pending a product decision): legacy gate routes, conflicts/rebase + runtimes + bare-workspace orphans, `user_override_transition`, the `override` request field, `CancelledExecution` dead fields all removed (see CHANGELOG `### Breaking`).
4. ~~**B5** — unique per-instance `workflows_dir` in test constructor~~ **Done 2026-07-01**.
5. **G5** — collapse the TS type pipeline to one generated location. *Partially done 2026-07-01*: admin/memory response types moved into `api-types` and forge-client's hand-rolled copies deleted; the bindings-copy pipeline and hand-written `api.ts` remain.
6. **G4** — extract the shared daemon helpers (or let the `forge-daemon` deletion absorb most of it); then decide **B3** (`max_turns` on remote) inside that consolidation.
7. **B2/B4** — decide the memory story: either ship prompt wiring (Phase 3) or park the knowledge plugins; document whichever you keep.
8. **G8** — docs pass once the surface stops moving from the deletions above.
