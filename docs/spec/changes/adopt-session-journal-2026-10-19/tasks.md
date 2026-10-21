---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-21T22:04:36Z
completed_at:
---

Execute in order. Each numbered checkbox is one Codex job, sized ≤ about 3 h;
split an overrun rather than silently extending scope. Checkboxes record completion.
Run only the named focused tests locally; CI runs full suites. Test names below
are proposed exact names to create where absent. Do not run a whole crate,
workspace or all-target suite locally. Use task-specific TMPDIR/CARGO_TARGET_DIR
on `/Volumes/Data`; do not print the process environment.

Agent-host integration targets introduced here use `--features test-support` if
they need production test constructors. Declare required-features accordingly;
enable it on the command line, never with a self dev-dependency. DB and
services(-agents) integration commands below include their required test feature.
No runtime bench crate becomes a production dependency.

## Release gate and foundations

- [x] **1. Bump agent-runtime pin to the release containing the engine (gate; ≤3 h).**
  The baseline workspace Cargo.toml/Cargo.lock pinned agent-runtime
  **0.7.0 + U9 groups C/D** at `1de6d7d`
  (`1de6d7d34d2cfe4fc35791bca440932602539cf7` in the lockfile), shared by
  agent-host, services and services-agents. No separate pre-0.7 upgrade job is
  needed; this gate moves that pin to the engine release.
  Use the first agent-runtime release containing extract-journal-storage-engine;
  record its actual release/tag and immutable SHA, with one source/revision for all
  runtime packages and testkit. Verify D2 rules 1–7/receipt, N1 compact pins, N2/R4
  working memo, R5/R6, S5, JournalBodySharing::PerSession, the amended 25%
  deferred-removal/physical-copy receipt rules, retained_bytes, scope-aware
  conformance, D-1's reader-operation authority check and reviewer `a49e3e0`
  futures-mutex fix in that release. Released runtime D5 records the resolved D-1
  decision, including head-bound pin memo certificates; task 13 uses engine clones
  for memo/lease reuse regardless.
  Stop subsequent jobs if unavailable; do not implement runtime fixes in Forge or
  invent a version. Update Cargo.toml/lockfile together. Confirm the released S5
  floor/exception and pruning semantics match D4, or correct D4 first.
  Focused check: `cargo check -p forge-agent-host -p services -p services-agents`; add/run
  `cargo test -p forge-agent-host --test session_journal_dependency engine_release_contract`.
  Completed with **v0.8.0**, commit `fd5b14ca12f4a8dbc4d52030f3c095292f4eb4a7`
  (annotated tag object `d4dfe1952eeac542d9ec0d5c88ecce07f0c9e86b`). All eight
  runtime/testkit packages use that source; the seven existing runtime packages
  moved from 0.7.0 to 0.8.0 and 487 other packages kept their versions/sources.
  The released full JournalFence and required replacement record are reflected
  in D2/D4/D5, task 3/6/11 and the native-session-journal delta. S5's inclusive
  floor and single current-epoch exception match. Both named tests passed (2/2),
  as did the three-package check, fmt check, agent-host all-target Clippy with
  warnings denied, strict spec validation and whitespace checks. No production
  call-site adaptation was needed; native persistence remains materialized.
  Upstream LCM planning changes are documented in CHANGELOG and architecture.
  Tagged source checklist and command receipts: `/Volumes/Data/tmp/refactor/c3-t1/report.md`.

  Follow-up on 2026-10-20: all eight runtime/testkit packages now use
  **v0.9.0**, commit `8c19be5b226c2fdaeb6708ec5d39e91c235857a5`.
  Cargo.lock changes only those runtime packages; AGENT_RUNTIME_REVISION and
  engine_release_contract follow the same pin. The two seven-cut/both-domain
  Forge wedge regressions pass. A new unresolved-acceptance regression verifies
  that post-turn persist fencing preserves the original failure and the next
  turn reopens successfully; Forge introduces no new turn-failure class.

- [x] **2. File-backed test support and engine harness (≤2 h).**
  Use the testkit pinned in task 1; add file DB/temp-dir ownership, the test-only mapping
  decorator that inserts a stub agent_session for each unseen runtime id on its
  first apply (no pre-seeded id list), test-only legacy row seeding and real
  decoder helpers.
  Use explicit test-support without self dependencies. Start with engine factories
  over volatile storage for protected/ordinary/supported and unsupported collection;
  later jobs swap only the storage factory. Preserve the real decoder schema gate.
  Focused tests: `cargo test -p forge-agent-host --features test-support --test session_journal_conformance engine_harness_smoke`
  and the same command filtered to `forge_legacy_decoder_accepts_native_materialized_checkpoint`.
  Completed: feature-gated file-backed fixtures and on-apply mapping decorator;
  released engine/testkit factories use volatile PerSession storage. Both named
  tests pass, including pre-envelope/enveloped real legacy decode and the
  unchanged runtime schema gate. CI explicitly selects the required feature.

- [x] **3. Add six-table migration and cascade invariants (≤2 h).**
  Allocate one `V{YYYYMMDDHHMM}__session_journal.sql` per D4 (unique UTC version at creation).
  Add session, pin, catalog, pack, bitmap membership and operation_pack tables
  keyed by runtime session id, with session control-generation/revision/epoch columns
  and an indexed `forge_session_id` cascade FK on
  every session-metadata/membership table, a non-unique index on
  `agent_session(runtime_session_id)` (never UNIQUE), required indexes and
  in-row CHECKs. Packs have a strict domain/runtime-session owner, indexed Forge
  owner label and retained canonical byte count, without an owner cascade so
  physical deletion stays bounded (D4/D8). Keep legacy data byte-exact.
  Per-session metadata/memberships cascade. Test the migration on a
  database that already holds duplicate and NULL runtime_session_id values.
  Focused test: `cargo test -p db --features test-template --test main session_journal_schema::migration_preserves_legacy_and_scoped_packs`.
  Completed: `V202610200941__session_journal.sql` adds the six tables, ten
  explicit indexes and in-row guards without rewriting legacy data. The named
  file-backed upgrade test passes with duplicate and NULL runtime ids, byte-exact
  legacy rows, strict scoped memberships, cascade checks and surviving packs.
  Both migration-runner checks, fmt and two-package all-target Clippy pass.
  Task 2–3 command receipts: `/Volumes/Data/tmp/refactor/c3-t2-3/report.md`.

## JournalStorage implementation

- [x] **4. Pack codec and real AEAD (≤3 h).**
  Implement JRPACK v1 as a sealed index unit (sorted four-byte prefix/varint
  directory) plus a sealed zstd-3 body unit of kind-ordered canonical bodies,
  exact full-id/kind verification and bounded decode. XChaCha20-Poly1305 with
  fresh nonces, existing keys and AAD binding unit kind, pack id, generation and
  owning domain/runtime-session key.
  Derive the AEAD and lookup keys from the master key with HKDF-SHA256 (D3).
  Seal all metadata/operation units; use small catalog/pin HMAC lookup keys, no
  per-object index. Test collisions, malformed offsets, tampering/AAD swaps and
  absence of clear object ids/prefixes.
  Focused tests: `cargo test -p forge-agent-host --lib session_journal::codec::`
  and `cargo test -p forge-agent-host --lib session_journal::seal::`.
  Completed: JRPACK v1 separates bounded prefix/varint indexes from kind-ordered
  zstd-3 canonical bodies; complete ids and envelope kinds are verified. Real
  XChaCha20-Poly1305 uses fresh nonces and full row/scope/generation AAD, with
  separate HKDF-SHA256 encryption and HMAC lookup keys from the existing master
  key. Metadata DTOs preserve unknown engine versions and bytes. All seven
  focused codec, sealing and opaque-metadata tests pass; no runtime wiring changed.

- [x] **5. Lossless read methods and cache consistency (≤3 h).**
  Implement load/objects/members/operation/pins/catalog/dirty_sessions against
  file-backed sqlx. Preserve opaque version/bytes; scalar metadata must agree with
  authenticated payload. Explicitly return JournalBodySharing::PerSession;
  lazily rebuild only the requested domain/session's prefix directory from its
  sealed index units (D4). Storage open decodes no pack units. Batch pack reads,
  enforce membership on cache hits, use pack generations and bounded directory/
  body caches (64 MiB combined charge); refresh the scoped directory across
  independent connections.
  Implement retained_bytes with all retained canonical copies including deferred
  dead bytes, independently per domain. Dirty paging sorts runtime session ids. No one-query-per-object/
  full-closure reread per warm commit.
  Focused tests: `cargo test -p forge-agent-host --features test-support --test session_journal_storage read_membership_metadata_and_reopen`
  and the same target filtered separately to `session_scoped_lazy_admission_and_reopen`
  and `retained_bytes_include_deferred_bodies` (include unrelated-session packs,
  evicted directories, independent domain/session copies and same-scope dedup).
  Completed: file-backed sqlx reads preserve authenticated metadata and opaque
  engine bytes, check scalar agreement and scope every body by current membership.
  Scoped directories load sealed indexes lazily; generation checks refresh only
  changed packs, body reads batch, and one charged 64 MiB LRU bounds directories
  and decoded bodies together. Dirty paging uses runtime ids; retained bytes
  include deferred and orphan copies per domain. All three named tests pass with
  independent pools, reopen, unrelated corruption and bounded cache admission.

- [x] **6. Fenced atomic apply and seven cuts (≤3 h).**
  One sqlx begin_immediate transaction/connection; SQL expect guard for existing
  and absent records, including control writes retaining revision/epoch. Compare
  the full JournalFence (revision, writer_epoch, generation); require a replacement
  session record with generation exactly expect.generation + 1, rejecting overflow
  or a non-successor atomically. A failed fence returns
  StateConflict. Never use cache staleness or a changed data_version as a semantic
  conflict. Refresh only the written domain/session's pack directory inside the
  transaction per D5 without using a database-wide invalidation counter;
  preserve genuine immutable-body conflicts and persist exactly supplied
  revisions/epochs and the supplied successor generation. Stage non-control,
  checkpoint, batch and head in the specified cut order and publish the final new pack (split at the 1 MiB logical cap). Roll back
  every pre-commit cut; AfterCommit
  commits then errors. Invalidate caches on ambiguous outcomes. Emit the D8 apply
  tracing (kind, duration, lock hold, receipt; no contents or ids).
  Focused tests: same storage target filtered separately to
  `apply_atomic_cuts` and `independent_connection_expect_fences`.
  Completed: one BEGIN IMMEDIATE transaction enforces the full SQL fence,
  checked zero-fence absence and exact successor generation while preserving
  supplied revision/epoch values. Concrete ordered staging publishes capped S1
  packs; all six pre-commit cuts roll back and AfterCommit loses only its
  acknowledgement. Scoped cache refresh never conflicts for staleness; errors
  and cancellation invalidate the written scope. Count/duration-only D8 tracing
  includes receipts and lock hold. Both named tests pass, including all seven
  cuts for new/existing records, concurrent controls, full-width counters and
  warm admission. Pin/catalog changes and bounded sealed tier-zero operations
  round-trip; Collect writes, release, pruning, full operation containers and compact return
  typed Unsupported until their later tasks. Runtime wiring remains unchanged.
  Review amendment (c3-storage review, 2026-10-20): admissions still follow the
  cut order, but an apply's new bodies are now encoded, sealed and inserted once
  before the record; per-cut staging rows were unobservable (every pre-commit
  cut rolls back) and tripled warm-commit pack writes inside the write lock.

- [x] **7. Pin/catalog/staging persistence (≤2 h).**
  Execute Put/Remove records verbatim inside apply, with no private-engine parsing
  or host closure computation. Wire retained roots and staged memberships through
  engine async controls. Unknown engine metadata must round-trip losslessly.
  Focused test: same storage target filtered to `pins_catalog_stage_restart`.
  Completed: engine async retained-root, staged-leaf and release-pin controls
  persist through the existing verbatim Put/Remove apply path. The named test
  passes across independent-pool restart, historical retained-head opening and staged
  membership recovery; unknown engine versions/bytes and catalog removals
  round-trip without private-engine parsing or host closure computation.

- [x] **8. S4 operation packs and S5 directive execution (≤3 h).**
  Binary-carry 2^tier packs hold lossless complete original results, capped at
  1 MiB of serialized records before compression/sealing; bound overflow segments,
  import and prune/rebuild output, and reject an oversized record atomically.
  Implement
  replay lookup/conflicting-input behavior through the engine. Execute only its
  explicit collection pruning directive: keep revision >= floor (floor =
  current−64, inclusive) plus the one highest-revision record whose
  result.writer_epoch equals the post-write record's epoch; prune the rest of
  that domain/session only; rebuild tiers atomically. Test the edge at revision 70
  (floor 6 kept, 5 pruned) and an old current-epoch record below the floor being
  pruned. Do not prune on commits; rt-e7's reference now implements/measures S5.
  Focused tests: same storage target filtered separately to
  `operation_pack_replay_restart` and `engine_replay_window_edges`.
  Completed: sealed zstd operation packs use exact binary-count carry tiers and
  capped overflow segments. Explicit S5 directives atomically rebuild only
  changed packs; unchanged ciphertext survives. Both named tests pass through
  real engines, independent-pool restart, conflicting replay, revision 70
  (inclusive floor 6), the single current-epoch exception, rollback and scoped
  isolation. Zero-release collection exercises S5 before body purge support.

- [x] **9. Release, deferred purge and exact physical receipts (≤3 h).**
  Clear only releasing membership bits and deny reads immediately, including warm
  cache hits; deduplicate releases. Keep sealed dead bodies below 25% of canonical
  body bytes; the apply reaching 25%, scope emptying and retired-session collection
  purge dead bytes and atomically remap survivors even with S2 disabled. Scan all
  retired-scope packs, including earlier deferred slots with no new release id.
  Count physical copies/original canonical lengths only at removal, including
  previously deferred bodies, once per copy; moving live bodies counts zero.
  Same-scope re-admission restores a retained slot; independent domain/session
  copies do not delay reclamation. Reconcile retained_bytes and cumulative receipts.
  Focused tests: same storage target filtered separately to
  `deferred_purge_threshold_and_receipts`, `retired_and_empty_scope_force_purge`
  and `independent_session_domain_copy_receipts`.
  Completed: releases deduplicate ids and clear only the owning membership.
  Mandatory byte-threshold, empty-scope and retired-collection purges remap
  live survivors under a new pack generation and count original canonical
  lengths once per removed physical copy. All three named tests pass, covering
  warm/independent caches, same-scope re-admission, byte-vs-slot thresholds,
  old deferred slots without new release ids, independent domain/session
  copies, cumulative retained-byte reconciliation and engine-selected release.

- [x] **10. S2 maintenance and async physical compaction (≤3 h).**
  Add async post_terminal geometric merging (minimum 4 KiB logical tiers), the
  1 MiB logical pack cap on imports/admissions/all repacks, collection base repack
  limited to fully live packs below 64 KiB or fully live survivors just purged by
  that apply, and membership-preserving compact (one domain/session and at most
  8 MiB rewritten per transaction, no full VACUUM, then db::incremental_vacuum).
  Leave stable large and deferred-dead packs sealed; terminal equal-tier pairs
  must fit the cap. Compact never purges dead bytes outside apply receipt accounting.
  Move only the owning scope's bitmap transactionally; retries preserve acknowledged state.
  Test-only S2 off/on knobs are not product rollout flags.
  Focused tests: same storage target filtered separately to
  `terminal_geometric_repack`, `collection_repack_eligible_scoped_packs`,
  `purge_rewrites_only_capped_pack` and `import_and_operation_pack_caps`.
  Completed: async post-terminal maintenance geometrically merges fully live
  equal tiers from a 4 KiB minimum. Collection merges only small fully live
  packs or survivors purged in that apply; stable large/deferred packs stay
  sealed. Compact keysets one domain/session and at most 8 MiB of logical
  inputs per transaction, preserves every membership and session generation,
  then calls db::incremental_vacuum. All four named tests pass for bounded
  maintenance, overflow/import/rebuild caps and atomic oversized rejection;
  task 9's mandatory purge tests also pass with S2 off. No native runtime
  wiring or full VACUUM was introduced.

## Conformance gate

- [x] **11. Storage conformance in every configuration (≤3 h).**
  Point factories at ForgeJournalStorage. Run unedited
  assert_journal_storage_conformance with S2 off/on and storage reopened at every
  hydration, all declaring PerSession with independent domain copies, real AEAD
  and file-backed sqlx. Mandatory purges stay enabled with S2 off. This covers
  protected/ordinary, arbitrary redacting policies, collection/unsupported collection,
  fence, independent-copy receipts, deferred removal/retained-byte evidence,
  metadata, paging, the control_generation_fence case and restart cases. Use
  testkit's engine host impl; do not add a Forge collector.
  Focused commands: `cargo test -p forge-agent-host --features test-support --test session_journal_conformance storage_conformance_no_repack`,
  then that command filtered to `storage_conformance_repack`, then
  `storage_conformance_reopen`. Each named wrapper must pass.
  Completed: all three wrappers pass the unedited v0.8.0 suite's 22 named
  cases each (66 case executions), over real ForgeJournalStorage, AEAD and
  file-backed sqlx. S2 off retains mandatory purges; S2 on uses the production
  maintenance path. Reopen discards storage caches and opens an independent
  pool on every load over the same retained file. Testkit supplies engine
  collection semantics. No production storage fix or runtime wiring was needed
  at that gate. The v0.9.0 follow-up passes all 26 named cases in all three
  configurations (78 case executions), including the four new authority/isolation
  probes and the retryable lost-acknowledgement rule.

- [x] **12. Broken-storage negative controls (≤2 h).**
  Adapt upstream test-only fault shapes over real Forge storage: torn transaction,
  lost write, reordered n+1, ignored epoch, a remaining holder's copy unlinked
  early, released/deferred bytes served to a non-member, and ignored control
  generation (released v0.8.0's private StaleGeneration shape). Assert
  the named failures in D6; do not flag a valid independent-copy release as
  early shared deletion. Upstream private test adapters are not
  exported APIs. No defects ship in production.
  Focused test: `cargo test -p forge-agent-host --features test-support --test session_journal_conformance broken_storage_`.
  Completed: seven integration-target-only fault decorators adapt the private
  v0.8.0 shapes over real Forge storage. Each is rejected at its exact D6 case,
  including ignored control generation. Reordering preserves the current
  closure but loses the acknowledged predecessor; premature unlinking fails
  the surviving holder's fresh open. Valid independent-copy releases pass all
  three positive suites and the four-scope physical-receipt regression.
  D6, this inventory, the proposal and native delta now include the seventh
  released control (StaleGeneration), correcting the original six-control list.
  No fault adapter is part of a production module or runtime path. Evidence:
  `/Volumes/Data/tmp/refactor/c3-t11-12/report.md`.
  The v0.9.0 follow-up adds NonRetryableLostAck (atomic_apply_cuts), for eight
  negative controls. DeadBodyServed is rejected earlier by the new
  body_cache_membership case; its exact expected named rejection follows the
  released suite's order. All eight controls pass.

## Cut-over and recovery

- [x] **13. Shared engine wiring and ordinary projection (≤3 h).**
  SessionJournals owns one engine per storage/domain, handing clones to every
  Runtime and maintenance/collection path so memo and lease registry stay shared;
  never construct engines per turn or worker. Configure the approved 128 MiB
  estimated memo charge per engine (256 MiB combined, not a heap bound; review
  observed up to 1.24× retained heap/charge, roughly 159 MiB per engine / 317 MiB
  combined). Forge's local, single-user use has no evidence requiring the larger
  runtime default; memo misses cost only cold validation, affecting speed, never
  correctness. Advertise both capabilities from the existing store with D2 config
  and implement the conservative D3 policy decided on 2026-10-20: preserve all
  restore-critical data and runtime-mandated exact fields; omit sensitive optional
  diagnostics. Do not promise that runtime cache/manifests contain no content;
  protected exactness is unchanged. Ephemeral scopes remain empty.
  Prove memo reuse across per-turn writer changes, shared lease visibility to
  collection and cold behavior at restart; include zero/small budgets in tests.
  Focused tests: `cargo test -p forge-agent-host --features test-support --test session_journal_runtime native_delta_and_projection`
  and the same command filtered separately to `writer_change_memo_and_restart`
  and `runtime_and_collector_share_engine_leases`.

  Completed: the protected store owns SessionJournals with exactly one engine per
  domain over a shared production storage. Capabilities return clones; maintenance
  receives the same owner/clones. Both engines enable collection and use 128 MiB
  estimated memo charge. The conservative policy removes sensitive optional
  forge.diagnostics.* extensions and preserves all recovery data, including LCM,
  cache and manifests. All three named tests pass: native delta/projection and
  ephemeral behavior, per-writer memo reuse with zero/tiny budgets and cold
  restart, and shared writer/reader lease visibility to collection. The owner is
  outside session_journal/ to avoid the parallel storage-hardening work.

- [x] **14. Scalar DB continuity and hand-over probes (≤2 h).**
  Replace legacy-only DB helpers/callers; published native head or eligible legacy
  state counts, reservation does not; retired native state cannot fall back.
  checkpoint_present keeps its meaning. No public response change.
  Focused tests: `cargo test -p db --features test-template --test main session_journal_probe::publication_and_checkpoint_flags`
  and `cargo test -p services --features test-support --test main attention_continuity::native_checkpoint_presence`.

  Completed: one scalar DB probe now supplies publication, retirement, saved and
  checkpoint flags to Attention and topic hand-over. It uses indexed Forge owner
  labels and the current runtime mapping, without reading encrypted bodies.
  Reservations permit legacy reads; either native publication takes authority;
  retirement in either domain suppresses all stale fallback. The two named tests
  pass, including native checkpoint/no-checkpoint, partial retirement and legacy
  payload eligibility. The affected services-agents compile check also passes.
  No response type or public transport changed.

- [x] **15. Per-turn/fork probes and bounded extension read (≤2 h).**
  Replace native.rs full-snapshot probes with scalar metadata and a separate
  JournalReader::extension(LCM_COMPONENT_ID) path. Prefer protected extension if
  ordinary omitted it. Cover absent, reservation, legacy, native and tombstone;
  existence reads no pack and the extension path touches no history/checkpoint.
  Focused test: runtime target from task 13 filtered to `native_probe_and_extension`.

  Completed: per-turn and fork preparation uses scalar saved/retired flags after
  scope authorization; ephemeral scopes bypass persistence discovery. LCM overhead
  and timeline adoption read one extension, ordinary first with protected fallback,
  retaining legacy decoding before native authority. The named test passes for
  absent/reservation/legacy/native/tombstone, a genuine 1,000-message LCM session,
  protected fallback, corrupt-pack-independent existence, and warm/cold reader
  operations limited to Head/Batch/Map/Extension objects. Cold lease acquisition
  still performs runtime-required root validation, separate from the bounded
  extension operation; no host validation bypass was added.

- [x] **16. Delete replaced legacy writers; retain importer/baseline (≤2 h).**
  Both production saves become typed refusals; remove old CAS/digest-skip/save
  hooks and write-only policy paths. Keep enveloped/pre-envelope reads and
  test-only exact legacy save-order/digest baseline helpers for D10. Do not clear
  payloads or drop the table this release.
  Focused tests: `cargo test -p forge-agent-host --lib protected_store::tests::legacy_read_`
  and runtime target filtered to `legacy_writes_refuse_and_baseline_keeps_two_blobs`.

  Completed: both production save traits now return the specified typed read-only
  conflict without touching the legacy table. Production CAS, digest skipping,
  checkpoint retry/supersession, save barriers and write-only policy-marker paths
  are removed. Four named legacy_read_ tests pass for envelopes/pre-envelope JSON,
  policy-independent reads, corruption and session-named checkpoint failures. The
  named runtime refusal/baseline test passes and verifies no writes, exact old
  digest/save ordering and the real two-sealed-blob L2 baseline. Baseline helpers
  and their HMAC dependency compile only under tests/test-support. Legacy rows
  and schema are preserved. Architecture and Unreleased entries describe cut-over
  and backup-based downgrade; later matrix/lifecycle/measurement gates remain open.

- [x] **17. Lazy import state matrix (≤3 h).**
  Idle, snapshot-only, CallingModel, ModelResponseReady, AwaitingApproval,
  AwaitingInteraction, ExecutingTools, ToolOutcomeReady and cache-started rows
  import with no provider/tool I/O and byte-unchanged legacy row. Exact-first
  where a checkpoint exists; protected equality and projected ordinary equality.
  Second native open uses R5; no invented checkpoint for ordinary-only state.
  Focused test: runtime target filtered to `legacy_import_state_matrix`.

  Completed: `legacy_import_state_matrix` imports nine genuine runtime-generated
  v3 states through Forge's encrypted legacy decoder into file-backed SQL. SQL
  publication auditing proves protected-first ordering for checkpoint-bearing
  state; protected checkpoints equal their exact inputs and ordinary snapshots
  equal Forge's projection. All rows stay byte-unchanged, import and second open
  perform zero provider/tool/interaction I/O, second open performs zero legacy
  loads, and snapshot-only import creates no protected head/checkpoint.

- [x] **18. Import/turn crash cuts and async control regression (≤3 h).**
  Use JournalCommitFailurePoint::ALL for equivalence/crash/restart over real SQL;
  include before/after exact publication, ordinary repair and lost ack. No stale
  legacy overlay or replayed model/tool operation. Suspend SQL storage and race
  controls/open/close on current-thread and multi-thread Tokio; no sync polling.
  Focused tests: runtime target filtered separately to `import_and_commit_crash_cuts`
  and `async_controls_do_not_stall_worker`.

  Completed: `import_and_commit_crash_cuts` passes all seven released cuts across
  119 import combinations and all 23 live plain/tool barriers (161 turn cases),
  280 combinations total. Real SQL rollback/publication flags, fresh pools and
  engines, exact/projection equivalence at every live and recovered barrier,
  exact authority, ordinary repair, lost acknowledgements, unchanged legacy
  rows, stale-row exclusion and provider/tool replay counts are checked.
  `async_controls_do_not_stall_worker` passes both domains on current-thread
  and multi-thread Tokio with SQL writes genuinely suspended by an independent
  transaction, competing controls/writer open, a ready local close and a live
  timer. No synchronous storage polling or storage implementation change.

- [x] **19. Native integration fixtures and writer exclusion (≤3 h).**
  Replace direct legacy-table test writes in native_chat_lcm_compaction,
  native_chat_tool_exchanges and embedded_task_executor tests with journal failure
  injection/corruption. Cover turn/fork/abort/inquiry concurrent writers and drop
  releasing the slot before provider I/O. Run only the changed named cases.
  Focused tests: runtime target filtered to `writer_fence_and_drop`; select the
  affected `cargo test -p services-agents --features test-support --test main <module>::<changed_test>`
  cases and the one relevant `cargo test -p api --test main happy_path::<native_chat_case>`.
  Record the actual case names in the job receipt before running; no target-wide run.

  Completed (second approach, c3-t19). The first attempt (c3-t17-20) hand-encoded
  engine snapshots and published them into both domains; it failed twice with
  StateConflict and was discarded. Every fixture now comes from real runtime
  turns, and no test writes an engine object or a legacy row. The superseded
  LCM revision is produced by an earlier backend running turns under a smaller
  Main working set and input budget (both LCM component-revision inputs); a
  fresh backend with current tunables resumes, the published revision is
  checked to differ, both domains are republished under the current revision
  and the next turn keeps it. Adopt's failed save cuts the ordinary engine's
  next commit (AfterObjects): the claim is durable, neither domain advances
  and both retries resume. The failing rotation fork uses a storage fault
  scoped to the successor's native journal session rows; a one-shot cut is not
  session-scoped and would hit the parent's fork intent first. The executor
  open failure zeroes sealed pack bodies an earlier store's real turn wrote. The
  inquiry case also asserts no native rows in any journal table. The topic-commit
  rotation case shares that fixture and passes unchanged. `writer_fence_and_drop`
  covers concurrent turn/fork/abort writers losing before provider I/O,
  concurrent ephemeral inquiries, and drop releasing the slot. No API happy-path
  case runs a native turn (its lead uses `CodingExecutorAdapter` without a
  provider), so none was run.

  Original v0.8.0 finding (resolved by the v0.9.0 follow-up): on a native chat turn, one cut at the protected
  engine's next commit fails the turn before provider I/O. The ordinary domain
  still publishes once more, and protected stays at its prior head. Every later
  turn then fails with "canonical session history and terminal checkpoint are
  from different boundaries", on a fresh backend and a restarted store too.
  Reproducer: `/Volumes/Data/tmp/refactor/c3-t19/diag-protected-cut-plain.rs`.
  Runtime v0.9.0 now rolls back proven unaccepted input and fences unresolved
  acceptance until reopen. Both wedge regressions and Forge's new
  unresolved_acceptance_preserves_turn_failure_and_reopens case cover the fix.

- [x] **20. Credential exclusion with real packs (≤2 h).**
  Seed API key/OAuth credentials, run a tool turn and refresh; decrypt all pack,
  session, operation, pin/catalog payloads and assert credentials never entered
  either domain. Keep ordinary redaction assertions separate from exact recovery.
  Focused test: runtime target filtered to `journal_credential_exclusion`.

  Completed: `journal_credential_exclusion` runs API-key and expired-OAuth tool
  turns through the real OpenAI-compatible adapter, credential source and runtime
  over file-backed SQL. A loopback refresh rotates access/refresh tokens and the
  credential version; transport assertions prove acquisition and one tool call
  per turn. The exhaustive test-only AEAD audit opens every pack index/body,
  session record, operation pack, pin and catalog in both domains, including
  decompressed canonical bodies, each framed object and deferred/unheld copies,
  before and after reopening. JSON string escapes and nested byte vectors are
  inspected generically without decoding runtime-private engine state. Raw, whitespace/invisible/case-folded, percent-encoded, base64 and
  16-character-window canaries validate the exclusion check. Exact protected
  recovery is checked separately; ordinary diagnostic redaction remains the
  independent task-13 test. The named test passes.

## Lifecycle and measurement

- [x] **21. Retention worker and terminal scheduling (≤3 h).**
  Wire the worker and rotation/recovery nudges: 60 s tick, revision grant then
  protected/ordinary retire, bounded dirty collection (20,000 membership releases
  per call, 10 s scheduling budget), S2 terminal hook. Cover replaced sources, active-writer
  conflicts, successor safety, Task completion preservation and crash between
  domain retirements, forcing collection of all dead bytes in each retired
  domain's packs, including deferred bytes below threshold. All engine controls
  use the task-13 clones and are awaited. Emit the D8 worker
  tracing; an undecryptable session is logged once and skipped, never retired or
  released.
  Focused test: `cargo test -p services-agents --features test-support --test main session_journal_retention::rotation_and_retry`.

  Completed with runtime v0.9.0: the shared runtime supervisor starts one
  `session-journal-retention` periodic worker in server and Solo. It scans
  immediately, then on a 60-second Skip interval and post-commit
  terminal/rotation/recovery/deletion nudges. A resumable phase cursor checks the
  10-second scheduling budget per retirement candidate and between engine calls
  and cleanup batches; it never cancels an in-flight control. Retirement calls
  task 22's selection function, authenticates both domains and obtains both
  revision-bound grants before protected/ordinary retirement. Own unreadable
  records prevent retirement; unknown dependency metadata can defer another
  candidate without falsely labelling it unreadable. Partial domain progress is
  durable and retries after restart. Every engine control awaits task-13 clones.
  Each collect uses 20,000 membership releases; retired domains force all dead
  slots out of their packs, including earlier deferred bytes below threshold.
  Successful native terminal/fork/abort publication awaits S2 and nudges the
  worker; maintenance cannot undo the turn. Task 23's indexed drain runs bounded
  5,000-candidate / 32-pack / 8-MiB batches until drained or the scheduling budget
  expires. D8 tracing separates physical receipts, membership bounds, cleanup,
  readable retained/live/deferred bytes and collect-call duration (a gate-hold
  upper bound including wait). Unreadable session warnings are process-wide,
  capped at 4,096 non-evicting ids: once per tracked session, one overflow warning
  at capacity, then per-session suppression with aggregate counting intact.
  No ids, contents or keys are logged. Worker preservation counts and id
  deduplication are current-pass only; no lifetime id set survives in the cursor.
  `rotation_and_retry` passes for a real runtime fork, successor and completed
  Task preservation, live-writer retry, cold partial-retirement recovery,
  below-threshold deferred-byte purges with retained roots in both domains,
  unreadable isolation/dependency deferral, worker registry/nudge/shutdown,
  deletion drain and zero scheduling budget. The existing selection and deletion
  regressions, plus the new chat-delete and native/CLI archive tests, pass.
  Evidence: `/Volumes/Data/tmp/refactor/c3-t21/report.md`.

  Review follow-up: dormant selection and active reuse rotate tombstoned native
  rows through the existing versioned transaction. Native open reports typed
  retirement and Chat/Task/inquiry callers retry once on a fresh runtime identity.
  Ready-status writes guard tombstones atomically. Explicit resume refuses with
  `409 session_retired`, without changing the row. Failed/cancelled outcomes
  and their usage envelopes survive typed shutdown/persist fencing conflicts;
  cleanup closes leases and removes the active registration before return.
  The cancellation barriers cover committed/uncommitted cuts before provider I/O
  and after provider I/O with nonzero per-turn usage. Small-cap warning and
  repair/delete/restart regressions establish bounded tracking and per-pass counts. Regressions cover the older
  dormant/newer replaced profile scenario, a selection/open race and non-chat
  resume. Evidence: `/Volumes/Data/tmp/refactor/c3-t21-fix/report.md`.

  Re-review follow-up: Task manifest hashing and `agent_session_id` use the host
  row matching the final successful runtime output, including reviewer-correction
  rotation. Corrections carry successful request ids into later turns. Native
  unchanged-read guards begin only after a Chat open succeeds; repeated retired
  opens cannot accumulate obsolete runtime markers. Real Task executor and
  correction regressions check B's runtime, B's host manifest link and B's manifest
  hash; eight repeated Chat open races check an empty failure-marker set.
  Evidence: `/Volumes/Data/tmp/refactor/c3-t21-fix2/report.md`.

- [x] **22. Approved unresumable-session retirement (≤2 h).**
  Include non-Agent-Chat native sessions suspended at restart and Agent Chat rows
  other than the scope's dormant session, where dormant is decided by calling
  `dormant_native_chat_session_id` per (identity, scope, profile), never a
  re-derived rule. Keep resumable dormant chats (one per profile), reusable Task
  sessions and active writers. Test a scope with dormant chats for two profiles. Do not import legacy-only dead rows merely
  for retirement; leave them for next-release cleanup. Tombstones suppress fallback.
  Focused test: same retention module filtered to `unresumable_and_dormant_restart`.

  Completed: `SessionJournalRetention::retire_page` selects bounded native-only
  pages and calls the task-13 engine clones through `retire_published`. Replaced
  sources qualify; suspended non-chat rows qualify; chat rows qualify only when
  the exact identity/scope/profile resume query chooses another dormant row.
  Each domain is loaded authentically, granted its current revision and retired
  protected first; conflicts defer ordinary retirement and retry later. No legacy
  row is imported. `unresumable_and_dormant_restart` passes for two profiles in
  one scope, startup-suspended account/Task rows, reusable completed-Task state,
  replaced sources, shared live writers, partial retirement and cold retry.
  Native tombstones suppress stale legacy fallback. Completed task 21 now
  schedules these pages; it distinguishes own-record corruption from a safe
  engine refusal caused by another session's unknown dependency metadata.

- [x] **23. Cascades and bounded orphan-pack maintenance (≤2 h).**
  Project/Agent/chat deletion cascades session metadata/memberships; nudge cleanup
  of all deleted-owner packs, including deferred dead bytes regardless of the
  25% threshold: candidates from indexed owner/orphan columns in pages of 5,000
  pack ids, at most 32 packs or 8 MiB retained canonical bytes per transaction.
  Recheck owner/membership under the write lock; preserve every other session's
  independent copies. Test partial bounded progress and retry after restart,
  draining the deleted owner without decrypting packs or fabricating apply receipts.
  No global tracing collector.
  Focused test: same retention module filtered to `deletion_drains_owned_packs_preserves_other_copies`.

  Completed: the additive deletion migration preserves sealed/legacy bytes,
  maintains an indexed orphan marker, and uses Project/Task/physical-Agent FKs.
  Repository code now handles the two logic edges in source transactions:
  Agent archive (including machine removal) deletes native/CLI sessions, and
  direct/User/Project chat deletion removes polymorphic scopes. Only the orphan
  projection triggers remain in the unreleased deletion migration. Committed session deletions nudge the existing pool activity signal;
  rollback does not. `drain_deleted_packs` calls a DB-only bounded drain: one page
  of at most 5,000 indexed ids, owner/membership rechecks under BEGIN IMMEDIATE,
  at most 32 packs and 8 MiB retained canonical bytes per transaction, cursor
  continuation and restart from zero. It decrypts no packs, removes all orphan
  bytes regardless of the 25% threshold, and reports separate cleanup counts.
  `deletion_drains_owned_packs_preserves_other_copies` passes for real deletion
  paths, 12.5% deferred bytes, independent copies/cold resume, unreadable deleted
  packs, a 5,001-pack partial/restart drain and the exact 8 MiB bound. Runtime-id
  reuse fails with the old-owner mapping error before drain and works afterward
  on the same shared engines. The migration-preservation regression passes.
  No global tracing collector or synthetic apply receipt was added. Task 21
  schedules repeated drain batches; the direct function and automatic worker
  paths are both covered.

- [ ] **24. Forge measurement harness and smoke (≤3 h).**
  Implement production sqlx/real-AEAD engine fixtures A/B/C and actual legacy L2
  row baseline per D10. Include empty-layout subtraction, file/dbstat agreement,
  identity-policy diagnostic, real production projection, retained legacy overlap,
  before/after collection/retirement, the production cadence (S2 hook and
  collection after every turn plus final shutdown sweep), cumulative committed
  object/operation pack payload+nonce writes and largest single collection-call
  write (rt-e7 metric), separate WAL bytes and largest single-pack rewrite,
  lazy session hydration time/bounded scoped-directory cache charge with unrelated
  sessions present, longest collect gate hold, retained/live/deferred bytes and
  physical receipts, memo/cache charge/heap/hit/miss/eviction/oversize and timing/read
  counters, including memo hit/miss counts and cold-validation time so the 128 MiB
  per-engine budget can be raised on evidence. Include C/N=4000 as a report-only
  supplemental shape; label its collection cadence and do not claim the runtime
  measured it per turn.
  Preserve exact seeds and cache assertions; JSON output plus Markdown table.
  Focused test: `cargo test -p forge-agent-host --features test-support --test session_journal_bench packed_n100_smoke`.

  Measurement implementation and smoke completed (2026-10-21), with an explicit
  NOT MET telemetry remainder: production sqlx/real-AEAD A/B N=100/1000/4000
  plain/tool and C N=1000/4000 ran at the per-turn S2/collection cadence, with
  exact pinned seeds/cache assertions, actual two-blob L2, matched full-schema
  baselines and file/dbstat/pointer-map agreement. The debug packed_n100_smoke
  passes for four N=100 shapes; N=100 post-collection bytes are A 106,496/110,592
  and B 172,032/180,224 (plain/tool). The ignored report also measures the identity
  diagnostic, real import/retained overlap, retirement, 64-revision S5 edges,
  cumulative transactional TEMP pack/legacy counters, WAL frames, per-turn
  timings/amplification, physical receipts and canonical byte differences.
  With 100 unrelated sessions / 805 packs, target hydration decodes only three
  indexes, takes 3.753 ms and charges 452,109 bytes (24,839 directory bytes).
  The 16 KiB cache experiment retains 14,621 bytes, evicts 396 entries and records
  104 cumulative oversize rejections. The 100-dirty collect calls measure
  779.050/705.695 ms protected/ordinary. Explicit cold/warm probes report 28 cold
  misses and 28 warm hits; all 500/2,000 C runtime engine opens read no bodies.
  Peak whole-process RSS is 2,773.59 MiB, including diagnostics/working sets.
  Runtime v0.9.0 exposes no public RootMemo charge/heap/internal lookup,
  eviction or oversize statistics. Those mandatory fields remain null with an
  explanation; no private-state decoding, unsafe inspection or runtime patch
  was used. The checkbox remains open for this telemetry gap. The approved
  128 MiB per-engine budget and conservative D3 policy remain unchanged.
  Outputs: /Volumes/Data/tmp/refactor/c3-t24-25/; full report/table in
  report.md and docs/performance-testing.md.

- [x] **25. Run Forge disk/performance gate (≤3 h).**
  Run only the named ignored release report:
  `cargo test -p forge-agent-host --features test-support --release --test session_journal_bench forge_packed_report -- --ignored --exact --nocapture`.
  Record all A/B N=100/1000/4000 and C results. B/N=4000 plain and tool after
  collection must each be ≤2.0× actual Forge L2; aim 1.5×. Record D10 performance
  checks, memo hit/miss counts, cold-validation time, transaction duration and
  old-plus-new peak; use this evidence for any increase to the 128 MiB per-engine
  memo budget. If a gate fails, report it and schedule a bounded storage-only
  fix/recheck; no schema-5 work or baseline
  manipulation. Identify rt-e7's measured reference numbers separately; publish
  only actual Forge measurements as Forge outcomes, never reference projections.

  Completed gate execution (2026-10-21): NOT MET for adoption. The named ignored
  release report completed all 14 production rows, the B/100 identity diagnostic
  and the scoped/100-dirty experiment in 6,655.24 seconds including compilation.
  B/N=4000 post-collection physical bytes are 3,862,528 / 1,359,872 L2
  (2.84036x plain) and 4,501,504 / 1,392,640 L2 (3.23235x tool), both above 2.0x.
  C/N=1000 is 2,293,760 / 942,080 (2.43478x); supplemental C/N=4000 is
  8,908,800 / 3,981,312 (2.23765x), both measured with per-turn collection.
  One bounded storage-only candidate used one-shot zstd level 9 for object bodies;
  its unchanged B/4000 recheck measured 2.78614x / 3.16471x, still failing.
  The candidate was reverted exactly; its patch and JSON remain in the report.
  No second candidate, schema-5 change, seed/projection/baseline change or memo
  increase was attempted. B/4000 delta writes pass at 7.658% / 5.440% of legacy,
  but turn/import checks fail; all individual D10 checks and actual overlap peaks
  are recorded. The longest C/4000 collect call is 2,992.811 ms, above D8's 1 s
  review bound (the separate 100-small-dirty-session calls pass). Cold validation,
  write transactions, memo observations, cache/RSS and post-retirement metadata
  are recorded without presenting rt-e7's modeled/shared-domain numbers as Forge
  outcomes. Exact internal memo/heap telemetry remains the task-24 NOT MET item.
  Full results and verification: /Volumes/Data/tmp/refactor/c3-t24-25/report.md.
  Task 26 and the overall change completion stamp remain untouched.

- [ ] **26. Implementation docs, CHANGELOG and spec validation (≤2 h).**
  Update architecture, getting-started, performance table and Unreleased
  Breaking/Changed entries per D9, with actual pins/results. State next-release
  legacy cleanup, approved retirement and the 64-revision replay window
  (inclusive floor plus the current writer epoch's last operation).
  Validate this change's deltas and links; verify no per-object SQL requirement or
  stale identity/lifetime-replay/4× gate, global-sharing/index requirement,
  immediate-purge-per-release rule or outdated runtime pin remains outside the
  verbatim historical receipt. Verify the two 2026-10-20 Owner Decisions entries
  and their 128 MiB per-engine memo budget and conservative D3 redaction are
  reflected consistently; Open Questions is now None. Preserve every existing
  Owner Decisions entry verbatim.
  Focused checks: strict spec validation and whitespace/link review. No full code
  suite; reuse the recorded named-test receipts, rerunning only for new changes.
