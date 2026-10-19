---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-19T00:00:00Z
completed_at:
---

Each task is sized for one Codex job of at most about 3 hours. Every task names the focused
tests to run. Run only those tests locally; CI runs the full suite. Briefs pass
`TMPDIR` and `CARGO_TARGET_DIR` on `/Volumes/Data`. Task numbers are referenced from `design.md`.

## 0. Prerequisites (not Forge jobs)
- [ ] 0.1 The runtime pin bump `82f5aac` → `1de6d7d` lands, with no adoption (separate job in flight).
- [ ] 0.2 Owner answers Q0. The agent-runtime change that extracts the journal engine behind a storage trait (design D2, R1–R6) lands, with its own spec and its in-memory suites unchanged and green. Forge pins that immutable revision across all runtime packages (`Cargo.toml:37`, `Cargo.lock`).

## 1. Conformance harness (first Forge job; needs only 0.1)
- [ ] 1.1 Add `agent-runtime-testkit` as a `[dev-dependencies]` entry of `crates/agent-host/Cargo.toml`, from the same git source and revision as `agent-runtime`. Create `crates/agent-host/tests/session_journal_conformance.rs` with three factories:
  - a protected factory returning `SessionJournalConformanceFixture`
  - an ordinary factory taking `Arc<dyn JournalProjectionPolicy>`
  - a collection factory returning `SessionJournalCollectionFixture`

  For now all three are backed by `InMemorySessionJournal`. Each factory is the single place the SQLite adapter swaps in later.

  Integration tests cannot reach `test_store()` / `session_store()` (`protected_store.rs:2013-2063`) or the fixtures at `:3547`/`:3882`, because they live in `#[cfg(test)] mod tests`. Expose a test-support constructor behind a `test-support` feature of `forge-agent-host`, enabled only in its own dev-dependencies. It creates the `agent_identity`, `agent_profile`, `agent_context_scope` and `agent_session` rows and returns the store, plus a helper that seals a legacy row. Tasks 4.1 and 6.1 reuse it.

  Add `forge_legacy_decoder_accepts_native_materialized_checkpoint`: it runs `assert_native_materialized_checkpoint_schema_guard` through Forge's real decode (seal with `SqliteProtectedRuntimeStore`, then `CheckpointStore::load_latest`).
  Tests: `cargo test -p forge-agent-host --test session_journal_conformance`.

## 2. Schema and storage
- [ ] 2.1 New migration `crates/db/migrations/V{YYYYMMDDHHMM}__session_journal.sql`, timestamped after the newest migration on the branch. It creates the five tables and two indexes in design D4, with all CHECKs, and touches no existing table. Add a db test that applies the migration to a file database, inserts a session with head, membership, operation and record rows, then deletes the `agent_session` row. Every per-session row must cascade; the body row must stay.
  Tests: `cargo test -p db session_journal_schema`.
- [ ] 2.2 `crates/agent-host/src/session_journal/seal.rs`:
  - HMAC key derivation for the object, operation and pin domains (`forge/session-journal/{object,operation,pin}\0`) from the existing master key
  - XChaCha20-Poly1305 sealing with `object_key` as associated data
  - the `FORGE-JOURNAL\0` envelope with a format byte
  - zstd for bodies of at least 1 KiB when compression saves at least 1/8

  Unit tests: round trip; associated-data swap refused; unknown format refused; no 32-byte object id appears in sealed output; threshold boundaries 1,023/1,024/1,025 bytes.
  Tests: `cargo test -p forge-agent-host session_journal::seal`.
- [ ] 2.3 (needs 0.2) `crates/agent-host/src/session_journal/storage.rs`: `SqliteJournalStorage` implementing the runtime storage trait (R1). It covers:
  - one `db::begin_immediate` transaction per engine operation
  - body `INSERT OR IGNORE`, then membership
  - the head CAS on `revision` and `writer_epoch`
  - writer-epoch bump on writer open
  - operation rows with a sealed result head
  - records for pins, dependencies and the retained catalog
  - staging memberships
  - deterministic failure injection at each `JournalCommitFailurePoint` cut

  Unit tests: CAS conflict rolls back the bodies; a stale epoch is refused; the same operation with the same input returns the original head after the head has advanced; a different input conflicts.
  Tests: `cargo test -p forge-agent-host session_journal::storage`.
- [ ] 2.4 Read path:
  - `open` prefetches the session-domain's published bodies in one query, up to 256 MiB of plaintext per lease
  - a process-wide decrypted LRU of 64 MiB, filled on commit and prefetch and evicted by deletion
  - `read` checks membership for the lease's domain

  Cross-domain test: a body shared by the ordinary and protected memberships survives deletion of one membership and is removed with the last. An ordinary lease cannot read a protected-only object.
  Tests: `cargo test -p forge-agent-host session_journal::read`.
- [ ] 2.5 Point the protected and ordinary conformance factories from 1.1 at the SQLite engine, using a fresh `file::memory:?cache=shared` pool per scenario. Fix the adapter until `assert_session_journal_conformance` and `assert_ordinary_session_journal_conformance` pass unedited.
  Tests: `cargo test -p forge-agent-host --test session_journal_conformance protected ordinary`.
- [ ] 2.6 `JournalCollectionHost` for the SQLite engine:
  - `root_epoch` and `authorize_retirement` (the grant columns)
  - `pin_transfer`, `retain_root` and `release_pin` (records)
  - `stage_leaves`
  - `compact` = `PRAGMA incremental_vacuum`
  - `retained_bytes` = `SUM(encoded_len)` over distinct members

  Add the batched orphan-body sweep. Point the collection factory at SQLite. The collection suites and the three ordinary GC suites must pass unedited.
  Tests: `cargo test -p forge-agent-host --test session_journal_conformance collection`.

## 3. Cut-over
- [ ] 3.1 (needs 2.1 only) Replace the db probes. `topic_rotation_successor_has_protected_state` (`crates/db/src/sqlite/embedded_agent.rs:974`) becomes "native head published in either domain (`head_ciphertext IS NOT NULL`; a `revision 0` reservation does not count), or legacy payload non-NULL". `attention_session_checkpoint_count` (`crates/db/src/sqlite/agent.rs:1053`) becomes "legacy checkpoint, or published protected head with `has_checkpoint = 1`". Test the reservation-only case for both. Rename both to describe the new meaning and update their callers (`topic_rotation.rs:520`, `attention_service.rs:2231`).
  Tests: `cargo test -p db session_journal_probe`, `cargo test -p services attention_continuity`.
- [ ] 3.2 Add `SessionJournals` to `crates/agent-host/src/session_journal/mod.rs`: two engines, storage ids `forge.session-journal.v1.{ordinary,protected}`, identity projection, and the shared master key. Have `SqliteProtectedRuntimeStore` implement `SessionStore::journal()` and `CheckpointStore::journal()`, constructed in `SqliteProtectedRuntimeStore::new` (`protected_store.rs:128`). Test: a native session created, run for two turns with the testkit fake provider, and resumed writes heads in both domains and no legacy row; the second turn's commits insert no history body that the first turn already stored.
  Tests: `cargo test -p forge-agent-host session_journal_native_turns`.
- [ ] 3.3 Add `SessionJournals::probe` (design D5.9) and replace the full-load probes at `crates/agent-host/src/native.rs:1150-1170` and `:1596-1608`. `overhead_measured` uses R6 `JournalReader::extension`. Test the decisions for native, legacy-only, absent and retired sessions (retired must surface the runtime conflict, not "fresh"). Assert that the existence probe reads no body.
  Tests: `cargo test -p forge-agent-host native_probe`.
- [ ] 3.4 Delete the legacy write paths:
  - in `protected_store.rs`: `save_checkpoint_once`, both `save` bodies, the `snapshot_digest` digest-skip and the `save_barrier` hook
  - each trait's `save` now returns `RuntimeError::conflict("legacy session store is read-only")`
  - the legacy write tests (`:2084-2716`); keep or move the decode, compression and legacy-read tests

  Tests: legacy `load`/`load_latest` still decode enveloped and pre-envelope rows; both `save`s refuse.
  Tests: `cargo test -p forge-agent-host protected_store`.
- [ ] 3.5 Credential exclusion (design D3.5). Seed an api-key credential and an OAuth bundle, run a tool turn, then force a refresh. Decrypt every body, head, operation result and record of the session, and assert no secret bytes appear.
  Tests: `cargo test -p forge-agent-host session_journal_credentials`.

## 4. Import, crashes, concurrency
- [ ] 4.1 v3 import through the real store:
  - seed legacy rows with Forge's own legacy sealing for idle, CallingModel, ModelResponseReady, AwaitingApproval, AwaitingInteraction, ExecutingTools, ToolOutcomeReady and a cache-started state; reuse the fixtures at `protected_store.rs:3547` and `:3882` where they fit
  - open each through the runtime
  - assert: both heads published; the materialized snapshot and checkpoint equal the legacy decode; zero provider requests; the legacy row unchanged byte for byte
  - assert that a second open performs no import (R5)

  Tests: `cargo test -p forge-agent-host session_journal_import`.
- [ ] 4.2 Import crash cuts. Inject storage failures before and after the protected publication and before the ordinary publication, then reopen. Before the protected commit, v3 must remain authoritative. After it, the ordinary head is repaired once and no turn is replayed.
  Tests: `cargo test -p forge-agent-host session_journal_import_crash`.
- [ ] 4.3 Move the tests that write `protected_agent_session_state` directly onto the journal:
  - `crates/services-agents/tests/suite/native_chat_lcm_compaction.rs:241`
  - `native_chat_tool_exchanges.rs:1262`, `:1288`, `:2594-2596`, `:2907` (triggers become storage failure injection or triggers on `agent_session_journal_head`)
  - `crates/services-agents/src/embedded_task_executor.rs:1448` (an unreadable state becomes a corrupt sealed head)

  Tests: the named test functions only, plus the one relevant `happy_path` case for native chat.
- [ ] 4.4 Second-writer behaviour. Within one process, a second writer open for a live session fails with `StateConflict` before any provider I/O. Cover the turn worker, `fork_topic`, `abort_topic_fork` and the inquiry runner, and confirm each maps to its existing error (`AgentHostError::VersionConflict` for forks, `native.rs:1568-1575`). A writer dropped without shutdown releases its slot synchronously, so the next turn opens immediately.
  Tests: `cargo test -p forge-agent-host session_journal_writer_fence`, `cargo test -p services-agents topic_rotation_concurrent_turn`.

## 5. Retention
- [ ] 5.1 Add the `session-journal-retention` worker in `crates/services-agents/src/` on the workers framework, with a 60 s tick, wired in `runtime/mod.rs`. Each tick:
  - finds `replaced` sessions with an unretired head
  - writes the revision grant and retires protected, then ordinary
  - runs bounded `collect` per domain (`max_objects = 20_000`, 10 s budget)
  - runs the orphan-body sweep in batches of 5,000

  `rotate_agent_chat_topic` and `rotate_agent_session` callers nudge the worker after commit. Tests:
  - rotation retires the source, and its latest opens conflict
  - collection reclaims bytes; the successor is untouched
  - Project deletion leaves no body after one tick
  - Task completion retires nothing
  - a crash between retiring the two domains completes on the next tick

  Tests: `cargo test -p services-agents session_journal_retention`.
- [ ] 5.2 (only if the owner approves Q1 beyond `replaced`) Extend the worker to suspended native sessions Forge cannot resume: non-Agent-Chat scopes, and Agent Chat rows that are not the scope's dormant session (`embedded_agent_service.rs:1528-1542`).
  Tests: `cargo test -p services-agents session_journal_retention_dormant`.

## 6. Measurement
- [ ] 6.1 Add the `#[ignore]` bench `crates/agent-host/tests/session_journal_bench.rs` (design D10), running legacy versus native at N=100/1000/4000 with plain and tool turns. It records per-turn bytes written, WAL growth, wall times, probe and import time, and retained bytes before and after collection and after retirement. It writes a JSON report under `$TMPDIR`.
  Run: `cargo test -p forge-agent-host --release --test session_journal_bench -- --ignored --nocapture`.
- [ ] 6.2 Run 6.1 on a release build and record the table and gate results (pass or fail per gate) in `docs/performance-testing.md`. Report any failed gate to the owner before release.

## 7. Docs and changelog
- [ ] 7.1 Make the documentation changes:
  - `CHANGELOG.md` `Unreleased`: apply the Breaking and Changed text from design D9
  - `docs/architecture.md`: rewrite `:2412-2449`, and add the "Session journal storage" subsection and the retention worker
  - `docs/getting-started.md:459`: import and downgrade consequences; disk note

  Validate the spec change folder.
