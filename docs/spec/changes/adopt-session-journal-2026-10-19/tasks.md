---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-20T11:30:20Z
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
  collection semantics. No production storage fix or runtime wiring was needed.

- [x] **12. Seven broken-storage negative controls (≤2 h).**
  Adapt upstream test-only fault shapes over real Forge storage: torn transaction,
  lost write, reordered n+1, ignored epoch, a remaining holder's copy unlinked
  early, released/deferred bytes served to a non-member, and ignored control
  generation (released v0.8.0's private StaleGeneration shape). Assert
  the seven named failures in D6; do not flag a valid independent-copy release as
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

- [ ] **17. Lazy import state matrix (≤3 h).**
  Idle, snapshot-only, CallingModel, ModelResponseReady, AwaitingApproval,
  AwaitingInteraction, ExecutingTools, ToolOutcomeReady and cache-started rows
  import with no provider/tool I/O and byte-unchanged legacy row. Exact-first
  where a checkpoint exists; protected equality and projected ordinary equality.
  Second native open uses R5; no invented checkpoint for ordinary-only state.
  Focused test: runtime target filtered to `legacy_import_state_matrix`.

- [ ] **18. Import/turn crash cuts and async control regression (≤3 h).**
  Use JournalCommitFailurePoint::ALL for equivalence/crash/restart over real SQL;
  include before/after exact publication, ordinary repair and lost ack. No stale
  legacy overlay or replayed model/tool operation. Suspend SQL storage and race
  controls/open/close on current-thread and multi-thread Tokio; no sync polling.
  Focused tests: runtime target filtered separately to `import_and_commit_crash_cuts`
  and `async_controls_do_not_stall_worker`.

- [ ] **19. Native integration fixtures and writer exclusion (≤3 h).**
  Replace direct legacy-table test writes in native_chat_lcm_compaction,
  native_chat_tool_exchanges and embedded_task_executor tests with journal failure
  injection/corruption. Cover turn/fork/abort/inquiry concurrent writers and drop
  releasing the slot before provider I/O. Run only the changed named cases.
  Focused tests: runtime target filtered to `writer_fence_and_drop`; select the
  affected `cargo test -p services-agents --features test-support --test main <module>::<changed_test>`
  cases and the one relevant `cargo test -p api --test main happy_path::<native_chat_case>`.
  Record the actual case names in the job receipt before running; no target-wide run.

- [ ] **20. Credential exclusion with real packs (≤2 h).**
  Seed API key/OAuth credentials, run a tool turn and refresh; decrypt all pack,
  session, operation, pin/catalog payloads and assert credentials never entered
  either domain. Keep ordinary redaction assertions separate from exact recovery.
  Focused test: runtime target filtered to `journal_credential_exclusion`.

## Lifecycle and measurement

- [ ] **21. Retention worker and terminal scheduling (≤3 h).**
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

- [ ] **22. Approved unresumable-session retirement (≤2 h).**
  Include non-Agent-Chat native sessions suspended at restart and Agent Chat rows
  other than the scope's dormant session, where dormant is decided by calling
  `dormant_native_chat_session_id` per (identity, scope, profile), never a
  re-derived rule. Keep resumable dormant chats (one per profile), reusable Task
  sessions and active writers. Test a scope with dormant chats for two profiles. Do not import legacy-only dead rows merely
  for retirement; leave them for next-release cleanup. Tombstones suppress fallback.
  Focused test: same retention module filtered to `unresumable_and_dormant_restart`.

- [ ] **23. Cascades and bounded orphan-pack maintenance (≤2 h).**
  Project/Agent/chat deletion cascades session metadata/memberships; nudge cleanup
  of all deleted-owner packs, including deferred dead bytes regardless of the
  25% threshold: candidates from indexed owner/orphan columns in pages of 5,000
  pack ids, at most 32 packs or 8 MiB retained canonical bytes per transaction.
  Recheck owner/membership under the write lock; preserve every other session's
  independent copies. Test partial bounded progress and retry after restart,
  draining the deleted owner without decrypting packs or fabricating apply receipts.
  No global tracing collector.
  Focused test: same retention module filtered to `deletion_drains_owned_packs_preserves_other_copies`.

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

- [ ] **25. Run Forge disk/performance gate (≤3 h).**
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
