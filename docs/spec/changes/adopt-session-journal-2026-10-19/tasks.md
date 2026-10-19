---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-19T21:27:07Z
completed_at:
---

Execute in order. Each numbered checkbox is one Codex job, sized ≤ about 3 h;
split an overrun rather than silently extending scope. All remain unimplemented.
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

- [ ] **1. Bump agent-runtime pin to the release containing the engine (gate; ≤2 h).**
  Use the first agent-runtime release containing extract-journal-storage-engine;
  record its actual release/tag and immutable SHA, with one source/revision for all
  runtime packages and testkit. Verify D2 rules 1–7/receipt, N1 compact pins, N2/R4
  working memo, R5/R6, S5 and reviewer `a49e3e0` futures-mutex fix in that release.
  Stop subsequent jobs if unavailable; do not implement runtime fixes in Forge or
  invent a version. Update Cargo.toml/lockfile together.
  Focused check: `cargo check -p forge-agent-host`; add/run
  `cargo test -p forge-agent-host --test session_journal_dependency engine_release_contract`.

- [ ] **2. File-backed test support and engine harness (≤2 h).**
  Add testkit at the exact pin, file DB/temp-dir ownership, the test-only mapping
  decorator that inserts a stub agent_session for each unseen runtime id on its
  first apply (no pre-seeded id list), test-only legacy row seeding and real
  decoder helpers.
  Use explicit test-support without self dependencies. Start with engine factories
  over volatile storage for protected/ordinary/supported and unsupported collection;
  later jobs swap only the storage factory. Preserve the real decoder schema gate.
  Focused tests: `cargo test -p forge-agent-host --features test-support --test session_journal_conformance engine_harness_smoke`
  and the same command filtered to `forge_legacy_decoder_accepts_native_materialized_checkpoint`.

- [ ] **3. Add six-table migration and cascade invariants (≤2 h).**
  Allocate one `V{NNN}__session_journal.sql` per D4 (unique UTC version at creation).
  Add session, pin, catalog, pack, bitmap membership and operation_pack tables
  keyed by runtime session id, with an indexed `forge_session_id` cascade FK on
  every per-session table, a non-unique index on
  `agent_session(runtime_session_id)` (never UNIQUE), required indexes and
  in-row CHECKs. Keep legacy data byte-exact. Per-session rows cascade; an
  advisory pack owner never cascades shared bodies. Test the migration on a
  database that already holds duplicate and NULL runtime_session_id values.
  Focused test: `cargo test -p db --features test-template --test main session_journal_schema::migration_preserves_legacy_and_shared_packs`.

## JournalStorage implementation

- [ ] **4. Pack codec and real AEAD (≤3 h).**
  Implement JRPACK v1 as a sealed index unit (sorted four-byte prefix/varint
  directory) plus a sealed zstd-3 body unit of kind-ordered canonical bodies,
  exact full-id/kind verification and bounded decode. XChaCha20-Poly1305 with
  fresh nonces, existing keys and AAD binding unit kind, pack id and generation.
  Seal all metadata/operation units; use small catalog/pin HMAC lookup keys, no
  per-object index. Test collisions, malformed offsets, tampering/AAD swaps and
  absence of clear object ids/prefixes.
  Focused tests: `cargo test -p forge-agent-host --lib session_journal::codec::`
  and `cargo test -p forge-agent-host --lib session_journal::seal::`.

- [ ] **5. Lossless read methods and cache consistency (≤3 h).**
  Implement load/objects/members/operation/pins/catalog/dirty_sessions against
  file-backed sqlx. Preserve opaque version/bytes; scalar metadata must agree with
  authenticated payload. Build the complete global prefix index at open from
  index units only (D4); batch pack reads, enforce membership on cache hits, use
  pack generations and a bounded body LRU; refresh across independent
  connections. Dirty paging sorts runtime session ids. No one-query-per-object/
  full-closure reread per warm commit.
  Focused tests: `cargo test -p forge-agent-host --features test-support --test session_journal_storage read_membership_metadata_and_reopen`
  and the same target filtered to `global_index_dedups_evicted_cross_session_body`.

- [ ] **6. Fenced atomic apply and seven cuts (≤3 h).**
  One sqlx begin_immediate transaction/connection; SQL expect guard for existing
  and absent records, including record=None writes; persist exactly supplied
  revisions/epochs. Stage non-control/checkpoint/batch/head in the specified cut
  order and publish one final pack. Roll back every pre-commit cut; AfterCommit
  commits then errors. Invalidate caches on ambiguous outcomes.
  Focused tests: same storage target filtered separately to
  `apply_atomic_cuts` and `independent_connection_expect_fences`.

- [ ] **7. Pin/catalog/staging persistence (≤2 h).**
  Execute Put/Remove records verbatim inside apply, with no private-engine parsing
  or host closure computation. Wire retained roots and staged memberships through
  engine async controls. Unknown engine metadata must round-trip losslessly.
  Focused test: same storage target filtered to `pins_catalog_stage_restart`.

- [ ] **8. S4 operation packs and S5 directive execution (≤3 h).**
  Binary-carry 2^tier packs hold lossless complete original results. Implement
  replay lookup/conflicting-input behavior through the engine. Execute only its
  explicit collection pruning directive: keep revision >= floor (floor =
  current−64, inclusive) plus the one highest-revision record whose
  result.writer_epoch equals the post-write record's epoch; prune the rest of
  that domain/session only; rebuild tiers atomically. Test the edge at revision 70
  (floor 6 kept, 5 pruned) and an old current-epoch record below the floor being
  pruned. Do not prune on commits or copy the bench's unimplemented-S5 refusal.
  Focused tests: same storage target filtered separately to
  `operation_pack_replay_restart` and `engine_replay_window_edges`.

- [ ] **9. Release, physical purge and exact receipts (≤3 h).**
  Clear only releasing membership bits, deduplicate ids, examine every holder,
  purge last-membership bodies in that apply, atomically remap all bitmaps and
  return original canonical lengths/count once. Partial purges work with optional
  S2 disabled; shared survivors remain readable across domains/sessions/reopen.
  Focused test: same storage target filtered to `last_membership_receipt_and_partial_pack`.

- [ ] **10. S2 maintenance and async physical compaction (≤3 h).**
  Add async post_terminal geometric merging (minimum 4 KiB logical tiers),
  collection base-pack repack and membership-preserving compact/incremental vacuum.
  Move every holder bitmap transactionally; retries preserve acknowledged state.
  Test-only S2 off/on knobs are not product rollout flags.
  Focused tests: same storage target filtered separately to
  `terminal_geometric_repack` and `collection_repack_all_holders`.

## Conformance gate

- [ ] **11. Storage conformance in every configuration (≤3 h).**
  Point factories at ForgeJournalStorage. Run unedited
  assert_journal_storage_conformance with S2 off/on and storage reopened at every
  hydration, real AEAD and file-backed sqlx. This covers protected/ordinary,
  arbitrary redacting policies, collection/unsupported collection, all receipt,
  fence, sharing, metadata, paging and restart cases. Use testkit's engine host
  impl; do not add a Forge collector.
  Focused commands: `cargo test -p forge-agent-host --features test-support --test session_journal_conformance storage_conformance_no_repack`,
  then that command filtered to `storage_conformance_repack`, then
  `storage_conformance_reopen`. Each named wrapper must pass.

- [ ] **12. Five broken-storage negative controls (≤2 h).**
  Adapt upstream test-only fault shapes over real Forge storage: torn transaction,
  lost write, reordered n+1, ignored epoch and shared body unlinked early. Assert
  the five exact named failures in D6; upstream private test adapters are not
  exported APIs. No defects ship in production.
  Focused test: `cargo test -p forge-agent-host --features test-support --test session_journal_conformance broken_storage_`.

## Cut-over and recovery

- [ ] **13. Shared engine wiring and ordinary projection (≤3 h).**
  SessionJournals owns shared storage and long-lived engines; advertise both
  capabilities from the existing store with D2 config and D3 policy. Protect exact
  recovery, preserve runtime-mandated fields and ordinary-only restore-critical
  values, redact optional sensitive diagnostics. Ephemeral scopes remain empty.
  Prove memo reuse across per-turn writer changes and cold behavior at restart.
  Focused tests: `cargo test -p forge-agent-host --features test-support --test session_journal_runtime native_delta_and_projection`
  and the same command filtered to `writer_change_memo_and_restart`.

- [ ] **14. Scalar DB continuity and hand-over probes (≤2 h).**
  Replace legacy-only DB helpers/callers; published native head or eligible legacy
  state counts, reservation does not; retired native state cannot fall back.
  checkpoint_present keeps its meaning. No public response change.
  Focused tests: `cargo test -p db --features test-template --test main session_journal_probe::publication_and_checkpoint_flags`
  and `cargo test -p services --features test-support --test main attention_continuity::native_checkpoint_presence`.

- [ ] **15. Per-turn/fork probes and bounded extension read (≤2 h).**
  Replace native.rs full-snapshot probes with scalar metadata and a separate
  JournalReader::extension(LCM_COMPONENT_ID) path. Prefer protected extension if
  ordinary omitted it. Cover absent, reservation, legacy, native and tombstone;
  existence reads no pack and the extension path touches no history/checkpoint.
  Focused test: runtime target from task 13 filtered to `native_probe_and_extension`.

- [ ] **16. Delete replaced legacy writers; retain importer/baseline (≤2 h).**
  Both production saves become typed refusals; remove old CAS/digest-skip/save
  hooks and write-only policy paths. Keep enveloped/pre-envelope reads and
  test-only exact legacy save-order/digest baseline helpers for D10. Do not clear
  payloads or drop the table this release.
  Focused tests: `cargo test -p forge-agent-host --lib protected_store::tests::legacy_read_`
  and runtime target filtered to `legacy_writes_refuse_and_baseline_keeps_two_blobs`.

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
  protected/ordinary retire, bounded dirty collection (20,000 objects/call, 10 s
  scheduling budget), S2 terminal hook. Cover replaced sources, active-writer
  conflicts, successor safety, Task completion preservation and crash between
  domain retirements. All engine controls are awaited.
  Focused test: `cargo test -p services-agents --features test-support --test main session_journal_retention::rotation_and_retry`.

- [ ] **22. Approved unresumable-session retirement (≤2 h).**
  Include non-Agent-Chat native sessions suspended at restart and Agent Chat rows
  other than the scope's dormant session. Keep resumable dormant chats, reusable
  Task sessions and active writers. Do not import legacy-only dead rows merely
  for retirement; leave them for next-release cleanup. Tombstones suppress fallback.
  Focused test: same retention module filtered to `unresumable_and_dormant_restart`.

- [ ] **23. Cascades and bounded orphan-pack maintenance (≤2 h).**
  Project/Agent/chat deletion cascades session metadata/memberships; nudge cleanup
  of unheld slots/empty packs in bounded transactions. Preserve shared holders;
  test partial pack cleanup and retry after restart, with no synthetic apply receipt
  for a deletion that happened outside apply. No global tracing collector.
  Focused test: same retention module filtered to `deletion_preserves_other_pack_holders`.

- [ ] **24. Forge measurement harness and smoke (≤3 h).**
  Implement production sqlx/real-AEAD engine fixtures A/B/C and actual legacy L2
  row baseline per D10. Include empty-layout subtraction, file/dbstat agreement,
  identity-policy diagnostic, real production projection, retained legacy overlap,
  before/after collection/retirement, operation-window and timing/read counters.
  Preserve exact seeds and cache assertions; JSON output plus Markdown table.
  Focused test: `cargo test -p forge-agent-host --features test-support --test session_journal_bench packed_n100_smoke`.

- [ ] **25. Run Forge disk/performance gate (≤3 h).**
  Run only the named ignored release report:
  `cargo test -p forge-agent-host --features test-support --release --test session_journal_bench forge_packed_report -- --ignored --exact --nocapture`.
  Record all A/B N=100/1000/4000 and C results. B/N=4000 plain and tool after
  collection must each be ≤2.0× actual Forge L2; aim 1.5×. Record D10 performance
  checks, transaction duration and old-plus-new peak. If a gate fails, report it
  and schedule a bounded storage-only fix/recheck; no schema-5 work or baseline
  manipulation. Publish only actual Forge measurements, not rt-e4 projections.

- [ ] **26. Implementation docs, CHANGELOG and spec validation (≤2 h).**
  Update architecture, getting-started, performance table and Unreleased
  Breaking/Changed entries per D9, with actual pins/results. State next-release
  legacy cleanup, approved retirement and the 64-revision replay window
  (inclusive floor plus the current writer epoch's last operation).
  Validate this change's deltas and links; verify no per-object SQL requirement or
  stale identity/lifetime-replay/4× gate remains outside the verbatim receipt.
  Focused checks: strict spec validation and whitespace/link review. No full code
  suite; reuse the recorded named-test receipts, rerunning only for new changes.
