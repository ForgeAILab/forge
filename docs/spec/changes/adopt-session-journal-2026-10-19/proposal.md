---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-19T00:00:00Z
---

## Why

Every native Agent turn rewrites its whole session. `SqliteProtectedRuntimeStore` stores the
ordinary snapshot and the protected checkpoint as one encrypted row in
`protected_agent_session_state`, and each save serializes, compresses and re-encrypts
the complete history (`crates/agent-host/src/protected_store.rs:1713-1715`, `:1851-1852`).
At 4,000 messages the runtime measured this at 5.66 MB (protected) plus 1.77 MB (ordinary) of
logical rewrite per turn. The host also decrypts the full snapshot before every turn only to
learn whether the session exists (`crates/agent-host/src/native.rs:1150-1170`).

Agent Runtime `1de6d7d` ships the opt-in `SessionJournal` capability (U9). It provides
content-addressed immutable objects, exact protected and projected ordinary domains, fenced
writers, idempotent commits, a one-time v3 import, and explicit retirement and collection.
A one-message turn then submits about 43 KB protected and 12 KB ordinary at any history
length. The owner approved Forge adopting U9 before U10, with new migrations that keep user
data and import existing sessions on first use. Forge is the consumer the runtime's
`consumer_open_forge` model mirrors. Until Forge adopts, none of U9's write savings reach users.

## What Changes

- **Runtime prerequisite (blocking).** The journal engine (commit validation, ordinary
  projection and re-encoding, reachability, dependency indexing, collection) is private to the
  volatile `InMemorySessionJournal`. A runtime change extracts it behind a storage trait, so Forge
  supplies only durable SQLite storage. Forge needs six things from it (design D2):
  - R1: one serialized write transaction per operation
  - R2: process-local leases with synchronous close
  - R3: incremental per-session collection
  - R4: a memo of validated roots across writer changes
  - R5: native-first open
  - R6: a bounded single-extension read

  Forge's adapter work starts once that runtime revision is pinned.
- **Two journal domains over one encrypted store.** `SqliteProtectedRuntimeStore::journal()` returns
  an ordinary journal for `SessionStore` and a protected journal for `CheckpointStore`. The
  ordinary projection is identity, which is exactly today's behaviour: the ordinary store already
  keeps the exact snapshot, encrypted. Bodies are sealed with the existing key. They are keyed by
  an HMAC of the object id, so no plaintext content hash reaches disk, and they are stored once
  across both domains. Credentials still never enter either domain.
- **New tables** in one new timestamped migration. Nothing existing changes.
  - `agent_session_journal_head`: per session and domain, with `revision` as CAS, writer epoch,
    sealed head, `has_checkpoint`, retirement tombstone and grant, `gc_dirty`
  - `agent_session_journal_body`: shared sealed immutable bodies
  - `agent_session_journal_object`: domain and session membership, including staging
  - `agent_session_journal_operation`: idempotency and replay fences
  - `agent_session_journal_record`: durable pins, dependency index, retained-head catalog
- **Import on first writer open** through the runtime's v3 import, including mid-turn
  checkpoints, with no provider or tool I/O. The legacy row is never written again.
- **Legacy store becomes read-only.** The legacy `load`/`load_latest` methods stay, as the
  import source. Their `save` paths are deleted and refuse with a typed conflict.
- **Host probes move to the journal.** These now read the journal heads:
  - the per-turn `saved` / `overhead_measured` probe
  - the topic-fork probe
  - topic-rotation hand-over
  - Agent continuity `checkpoint_present`

  The legacy columns are still checked for sessions that have not been imported yet.
- **Retention.** A `session-journal-retention` worker retires sessions Forge will never resume
  (topic-rotation sources and profile rotations; more per owner decision Q1). Each tick it
  collects unreachable objects of changed sessions and removes orphaned bodies. There is no TTL,
  and Task completion retires nothing. Project, Agent and chat deletion keep cascading.
- **Conformance in Forge's suite.** `agent-runtime-testkit` becomes a dev-dependency. The protected,
  ordinary and collection conformance suites, plus Forge's legacy-decoder schema gate, run against
  the real SQLite adapter.
- **BREAKING (visible):**
  - Sessions import on their next turn. Older builds refuse the upgraded database, as for any
    migration, so downgrading means restoring a backup.
  - A second concurrent runtime writer on one session fails with a conflict instead of
    silently superseding.
  - Live long sessions retain more disk, up to about 4× the uncompressed conversation, while
    writing about 99% less per turn.
- Out of scope:
  - U10 (LCM stays a second copy)
  - a warm runtime per session
  - redaction of the ordinary domain
  - dropping the legacy table (a later cleanup change, design D7.6)

## Impact

- Affected specs: `native-session-journal` (new), `session-journal-retention` (new)
- Affected code:
  - agent-runtime (prerequisite, separate repository and change): journal engine and storage
    trait (R1–R6)
  - `Cargo.toml`: runtime pin moves to the prerequisite revision.
    `crates/agent-host/Cargo.toml`: `agent-runtime-testkit` dev-dependency.
  - `crates/db/migrations/V{YYYYMMDDHHMM}__session_journal.sql`, and db probes in
    `crates/db/src/sqlite/{embedded_agent.rs,agent.rs}`
  - `crates/agent-host/src/session_journal/` (new: storage, keys, sealing, cache, host controls),
    `crates/agent-host/src/protected_store.rs` (`journal()`, legacy save removal),
    `crates/agent-host/src/native.rs` (probes)
  - `crates/services-agents/src/` (retention worker, `topic_rotation.rs` and
    `embedded_agent_service.rs` nudges, `runtime/mod.rs` wiring)
  - `crates/services/src/attention_service.rs` (`checkpoint_present` source)
  - tests:
    - `crates/agent-host/tests/session_journal_{conformance,runtime,bench}.rs`
    - `crates/services-agents/tests/suite/native_chat_{lcm_compaction,tool_exchanges}.rs`, which
      move off the legacy table
  - docs: `docs/architecture.md`, `docs/getting-started.md`, `docs/performance-testing.md`,
    `CHANGELOG.md`
- Public surfaces:
  - No REST, MCP, CLI or event payload changes.
  - `AgentContinuityHealth.checkpoint_present` keeps its meaning.
  - The on-disk layout gains five tables.
- Overlap: the runtime pin bump job (`82f5aac` → `1de6d7d`) must land first. U10 (LCM canonical
  journal) follows this change.
