---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-19T00:00:00Z
---

# Design: Forge adopts the Agent Runtime session journal (U9)

All Forge references are to `next/v0.14` at `78702e7c`. Runtime references are
to agent-runtime `main` at `1de6d7d` (written `runtime:<path>:<line>`); the
runtime's own U9 design is
`runtime:docs/spec/changes/add-session-journal-2026-10-07/design.md`
(written `U9 design:<line>`). The runtime pin bump from `82f5aac` to `1de6d7d`
is a separate job with no adoption. This change starts after it lands.

## Context

### Current facts

| Fact | Evidence |
| --- | --- |
| One type implements both runtime store traits. `SqliteProtectedRuntimeStore` is the `SessionStore` (ordinary) and the `CheckpointStore` (protected). | `crates/agent-host/src/protected_store.rs:115`, `:1777`, `:1899` |
| Both traits write one row per Forge session in one table, `protected_agent_session_state`. Its `session_id` references `agent_session(id) ON DELETE CASCADE`. | `crates/db/migrations/V062__embedded_agent_sessions.sql:99-111`; columns added in `V142__lcm_policy_revision.sql:16` and `V202609302227__protected_session_snapshot_digest.sql:3-5` |
| The row is keyed by the Forge session id. The runtime id maps to it through `agent_session.runtime_session_id`. | `protected_store.rs:267-275` |
| Both payloads are sealed with XChaCha20-Poly1305 inside a `FORGE-SESSION\0` zstd envelope (format 1). Older unenveloped encrypted JSON still reads. | `protected_store.rs:150-160`, `:179-218` |
| The key is SHA-256 of the server's JWT secret, at `key_revision` 1. | `crates/services-agents/src/embedded_agent_service.rs:257-266`, `crates/services-agents/src/runtime/mod.rs:520-523` |
| The ordinary save stores the exact, unredacted snapshot. Its only filter is an HMAC digest that skips unchanged saves. The digest excludes the timestamp and manifests. | `protected_store.rs:220-236`, `:1815-1897` |
| A checkpoint carries the exact snapshot. A checkpoint save NULLs the standalone snapshot columns, and `load` falls back to the checkpoint's copy, so only one copy is stored. | `protected_store.rs:1735-1736`, `:1795-1811` |
| Checkpoint saves are idempotent by turn, revision and fingerprint, and use CAS on `state_revision`. A lost CAS reloads once and keeps the winner. | `protected_store.rs:1660-1775`, `:1964-2008` |
| Every save rewrites and re-encrypts the whole snapshot or checkpoint, so each transition costs O(history). | `protected_store.rs:1713-1715`, `:1851-1852` |
| `lcm_policy_revision` is selected on load but never consulted there. Only the save path's rewrite check uses it. | `protected_store.rs:1785`, `:1831-1836`, `:1911` |
| The runtime gets both stores only for `context_mode.persistent_session`. Other scopes are ephemeral. | `crates/agent-host/src/native.rs:885-888`, `:1187-1191` |
| Before every turn, the host decrypts and decodes the full snapshot (or checkpoint). It uses it only to compute `saved` and `lcm_overhead_measured`. | `native.rs:1150-1170`, `:622-629` |
| Topic fork repeats the same full-load probe. Forge only uses `ForkSeed::Summary` with a new LCM timeline. | `native.rs:1596-1608`, `:1725-1734` |
| Topic rotation decides hand-over with a SQL probe on the legacy payload columns. | `crates/db/src/sqlite/embedded_agent.rs:974-987`, called from `crates/services-agents/src/topic_rotation.rs:518-521` |
| Agent continuity reports `checkpoint_present` from `checkpoint_turn_id IS NOT NULL`. That field is a public API value. | `crates/db/src/sqlite/agent.rs:1053-1062`, `crates/services/src/attention_service.rs:2229-2247`, `crates/api-types/src/mission_control.rs:282`, `web/src/types/generated/bindings/AgentContinuityHealth.ts:3` |
| A rotation marks the source session `replaced`. A profile change replaces the session under a new runtime id. Neither path deletes protected state. | `crates/db/src/sqlite/agent_chat_topic.rs:328-357`, `crates/db/src/sqlite/embedded_agent.rs:614-650`, `crates/services-agents/src/embedded_agent_service.rs:1874-1915` |
| After a restart, native sessions are suspended. Only an Agent Chat scope resumes its dormant runtime identity. Other scopes get a fresh session. | `crates/services/src/recovery.rs:195-208`, `embedded_agent_service.rs:1528-1542` |
| Project deletion cascades Project → `agent_context_scope` → `agent_session` → protected state. It deletes LCM rows explicitly. | `crates/db/src/sqlite/project.rs:973-1120`, `V074__agent_chat_canonical_scope.sql:104`, `V062__embedded_agent_sessions.sql:68-72` |
| One process owns a data directory. The server, Solo and the vacuum conversion each hold an exclusive `runtime.lock`. Daemons never open the database. | `crates/forge-cli/src/main.rs:720-735`, `crates/forge-solo/src/runtime_lock.rs:5-16`, `docs/architecture.md:377`, `:7348-7349`, `docs/getting-started.md:497` |
| Native Agents run only in the server or Solo process. Daemon placements reject native Agents. | `docs/spec/changes/add-daemon-owned-workspaces-2026-09-29/specs/workspace-placement/spec.md` (First-slice limits), `docs/architecture.md:341` |
| SQLite runs WAL with `synchronous=NORMAL` and a 30 s busy timeout. Writers use `begin_immediate`. | `crates/db/src/connection.rs:238-260`, `:340` |
| A database with unknown applied migrations is refused with `DbError::DatabaseNewerThanBuild`. Downgrade means restoring a pre-upgrade backup. | `crates/db/src/migration.rs:392-405`, `docs/getting-started.md:459` |
| Credentials live in `credential_handle`, `protected_credential_secret` and `protected_provider_authorization_state`. They reach the runtime only as `ProviderCredentialSource` leases. | `protected_store.rs:837`, `:852`, `:1028`, `:1535-1570` |
| LCM content is stored separately, as plaintext JSON in `agent_lcm_*`. U9 does not change it; U10 does. | `crates/db/migrations/V063__agent_lcm_timelines.sql:24-34`, U9 design:92 |
| The docs record that Forge does not advertise the journal. | `docs/architecture.md:2440-2445` |
| `agent-runtime-testkit` is not a Forge dependency. | `crates/agent-host/Cargo.toml` `[dev-dependencies]` |

### What the runtime provides (and what it does not)

- `SessionJournal` has open/read/commit/renew/close/retire/collect, plus `journal()` discovery on both store traits (`runtime:crates/agent-runtime-core/src/journal/mod.rs:37-82`, `runtime:crates/agent-runtime-core/src/store.rs:243-262`). Native mode needs both capabilities when both stores are configured. Mixed pairs stay legacy (U9 design:261-268).
- The one implementation is `InMemorySessionJournal`. It is volatile (`runtime:crates/agent-runtime-core/src/journal/memory.rs:501-519`). The commit, projection, validation, reachability, dependency-index and collection engine is private to it: `ObjectAssembler` `:1711`, `DraftReader` `:2246`, `DependencyRoots` `:154`, `draft_reachable_closure` `:3100`, `reachable_closure` `:3130`.
- Hosts get a public `JournalProjectionPolicy` over a complete `SessionSnapshot` (`memory.rs:41`), `IdentityJournalProjection` (`:57`), the canonical object codec (`runtime:.../journal/encoding.rs:315-420`) and `JournalReader` (`runtime:.../journal/reader.rs:161`, `:709`, `:727`).
- Conformance suites live in `agent-runtime-testkit`:
  - `assert_session_journal_conformance` (`runtime:crates/agent-runtime-testkit/src/conformance/journal.rs:99`)
  - `assert_ordinary_session_journal_conformance`, with an arbitrary host policy (`:172`)
  - `assert_session_journal_collection_conformance` over a `JournalCollectionHost` (`.../journal_gc.rs:21-53`, `:434-457`)
  - the `consumer_open_forge` gates (`runtime:crates/agent-runtime-testkit/tests/consumer_open_forge.rs`)
- Writer leases never expire on their own (`memory.rs:513-517`). `SessionPersistence::drop` calls `close` through `now_or_never` and spawns it only if it suspends (`runtime:crates/agent-runtime/src/runtime/journal.rs:596-625`).
- `SessionPersistence::open` still loads the legacy pair once even when both native heads exist (U9 design:838-841).
- Measured U9 costs, as logical bytes before Forge encryption, compression or WAL (runtime `CHANGELOG.md` Unreleased; `tasks.md` 2.7):
  - A one-message turn submits about 43 KB protected and 12 KB ordinary at any N.
  - Bootstrap or import retains about 3.8× one legacy snapshot per domain.
  - The legacy path rewrites 5.66 MB protected and 1.77 MB ordinary per turn at N=4000.
  - CPU is still O(N): the complete-phase CPU target was not met.

## Goals / Non-Goals

- Goals:
  - Every persistent native session writes O(delta) per transition instead of rewriting its whole state.
  - Today's guarantees hold: the encryption, the exactness of both domains, credential exclusion, the recovery barriers, and the readers' meanings (`saved`, hand-over, `checkpoint_present`).
  - Existing sessions import once, on their first writer open, through the runtime's v3 import, with no provider or tool I/O. User data is preserved.
  - Explicit, event-driven retirement and garbage collection. No retention TTL.
  - The runtime's conformance suites run in Forge's test suite against the real SQLite adapter.
- Non-Goals:
  - U10: the LCM timeline stays a second copy (`agent_lcm_*`).
  - Changing the per-turn runtime construction in `prepare_runtime` (`native.rs:680-958`). A warm runtime per active session is a later change.
  - Redacting the ordinary domain.
  - Dropping the legacy table. That waits for a later cleanup change (D7.6).
  - Multi-process access to one database. `runtime.lock` remains the rule.

## Decisions

### D1. Where the stores live today and what changes

All persistent session state lives in `protected_agent_session_state`, written only by
`SqliteProtectedRuntimeStore` (`protected_store.rs:1660-2008`).

| Reader / writer | Today | After this change |
| --- | --- | --- |
| Runtime ordinary store | `SessionStore::{load,save}` on the legacy row (`protected_store.rs:1778-1897`) | `SessionStore::journal()` returns the Forge **ordinary** journal. `load` stays the v3 import reader. `save` is removed and becomes a typed refusal (D7.5). |
| Runtime protected store | `CheckpointStore::{load_latest,save}` (`:1900-2008`) | `CheckpointStore::journal()` returns the Forge **protected** journal. `load_latest` stays the v3 import reader. `save` becomes a refusal. |
| Per-turn `saved` / `overhead_measured` probe | full decrypt (`native.rs:1150-1170`) | `SessionJournals::probe` (D5.9): one SQL row for existence, plus a bounded extension read (R6) for `overhead_measured` |
| Fork `saved` probe | full decrypt (`native.rs:1596-1608`) | the same `probe` |
| Topic hand-over | `topic_rotation_successor_has_protected_state` (`embedded_agent.rs:974-987`) | true if a native head is published in either domain (`head_ciphertext IS NOT NULL`; a `revision 0` writer reservation does not count), or a legacy payload is non-NULL |
| `checkpoint_present` | `attention_session_checkpoint_count` (`agent.rs:1053-1062`) | true if a legacy checkpoint exists, or the protected head is published with `has_checkpoint = 1` (retirement resets it to 0) |
| Credentials | separate tables (`protected_store.rs:821-1336`) | unchanged; never in either journal (D3.4) |
| LCM | `agent_lcm_*` (`crates/agent-host/src/lcm.rs`) | unchanged (U10) |
| API, MCP, web, exports, analytics | read no session payload. Usage comes from the turn's in-memory snapshot (`native.rs:1208`, `:1363-1380`) and the usage ledger. | unchanged. `checkpoint_present` keeps its meaning. |
| Tests that write the legacy table directly | `crates/services-agents/tests/suite/native_chat_lcm_compaction.rs:241`; `native_chat_tool_exchanges.rs:1262`, `:1288`, `:2594-2596`, `:2907`; `crates/services-agents/src/embedded_task_executor.rs:1448` (test module) | rewritten against the journal tables, or against the journal's failure injection (task 4.3) |

A grep of `crates/api`, `crates/mcp-server`, `crates/api-types`, `web/src` and
`crates/forge-client` finds no read of the legacy table or of session
payloads. The only exposed value derived from it is
`AgentContinuityHealth.checkpoint_present`.

### D2. Runtime prerequisite: a storage-generic durable journal engine

**Decision.** Forge does not reimplement the journal engine. Before Forge's
adapter work starts, a runtime change extracts the engine inside
`InMemorySessionJournal` behind a storage trait. `InMemorySessionJournal`
then becomes the volatile storage instance of that engine. Forge implements
only the durable storage, over SQLite.

**Why.**

- `assert_ordinary_session_journal_conformance` hands the adapter arbitrary
  projection policies, including redacting ones. Under each policy the
  adapter must:
  - rebuild the candidate snapshot from the projected root and the delta
  - run the policy
  - re-encode the result with the same structured encoders the writer uses:
    persistent JSON spines, compact Z85 branch ids, and cached path
    encodings (U9 design:130-169)
  - remap references and recompute the roots
  - check them against a cold encoding
- Protected commits must verify candidates exactly, through a staged
  `JournalReader` materialization (`memory.rs:2550-2697`).
- Collection must index fork intent and child catalog dependency roots from
  runtime-owned extensions (`memory.rs:150-300`).

All of that is private (about 3,600 lines). A Forge copy would drift from the
writer on every runtime fix, and the cold-encoding oracle would reject it.
The runtime already states that durable adapters must supply their own
framing, transactions and fences (U9 design:464-466). This decision keeps
that split: storage, transactions and fences belong to Forge, and semantics
belong to the runtime.

**What Forge needs from the runtime change** (Forge's acceptance criteria for the prerequisite):

- **R1. Storage trait with one serialized write transaction.**
  - Each `commit`, `retire`, durable pin change, staging change and
    collection deletion runs entirely inside one storage write transaction.
    SQLite maps it to `BEGIN IMMEDIATE`.
  - Reads use a consistent snapshot.
  - The storage persists these records:
    - objects (`put`/`get`/`delete` membership; see D4)
    - the per-session-per-domain record: head, writer epoch, batch
      sequence, retirement tombstone and grant
    - operation (idempotency) records
    - durable pins, the dependency index and the retained-root catalog
    - staging
  - The engine never holds a storage transaction across an `.await` on
    anything but the storage itself.
- **R2. Process-local leases.**
  - Lease, reader-pin and writer-slot registries stay in engine memory.
  - `close` and lease drop finish synchronously, with no storage I/O. That
    way `SessionPersistence::drop`'s `now_or_never` close always completes
    (`runtime:.../runtime/journal.rs:605-625`).
  - Writer open bumps the durable writer epoch in a storage transaction.
  - `revoke_writer(session)` stays a host method.
- **R3. Incremental, session-scoped collection.**
  - A sweep costs time proportional to the sessions whose roots or pins
    changed since the last sweep, not to the whole domain.
  - It stays correct when an object is shared across sessions (identical
    content dedups, and FromIndex or Continue transfer pins reference
    another session's closure).
  - Forge's storage offers per-session object membership (D4) for this.
    The runtime owns the algorithm, and it must pass the full collection
    suite, including the FromIndex, Continue and pending-child rows.
- **R4. Validated-root memo across writer changes.**
  - Forge builds a new `Runtime` per turn (`native.rs:680-958`), so every
    turn is a new writer.
  - The engine keeps the set of roots it has already fully validated, per
    instance. A new writer opening an unchanged head skips the full
    validation that U9 design:157-164 charges per writer change.
- **R5. Native-first open.**
  - `SessionPersistence::open` checks for native heads before loading the
    legacy pair, and loads legacy only when import or repair needs it. The
    runtime's views already behave this way (U9 design:573-576).
  - Without R5, each turn of an imported session decrypts its whole v3 row
    again (U9 design:838-841).
- **R6. Bounded single-extension read.**
  - `JournalReader::extension(namespace)` reads one versioned extension
    through the map root in O(log n) objects.
  - Forge needs only `LCM_COMPONENT_ID.fixed_overhead_tokens`
    (`native.rs:622-629`).
  - Without R6, Forge keeps today's O(N) materialization for that probe. The
    probe is not a correctness dependency.

The runtime's existing in-memory suites must pass unchanged against the
extracted engine with volatile storage. That is how R1 to R6 prove they
preserve semantics.

### D3. Two logical domains over one encrypted physical store

D3.1 **Projection policy.**

- Today the ordinary store keeps the exact snapshot, sealed with the same
  key as the checkpoint (`protected_store.rs:1815-1897`). The two even share
  one physical copy (`:1795-1811`).
- Forge's ordinary policy is therefore `IdentityJournalProjection`
  (`memory.rs:57`): nothing is redacted or omitted.
- Forge's protection is encryption at rest, for both Sensitive and
  RedactionSafe extension state. This is the host policy the runtime asks
  for (`runtime:.../store.rs` `SessionSnapshot::extension_state` doc:
  "protect, redact, or reject").
- Ordinary heads still differ from protected ones, as they do today:
  - they keep the manifest window
  - the runtime decides what each barrier writes (terminal publication,
    idle accounting, hard-admission intents)

D3.2 **Storage ids.**

- `forge.session-journal.v1.ordinary` and `forge.session-journal.v1.protected`.
- Each domain has its own heads, batches, idempotency records, pins,
  staging and retirement state. An ordinary lease can never read a
  protected-only object: reads are authorized by the domain membership plus
  the engine's reachability from that lease's pinned roots.

D3.3 **Physical sharing.**

- The runtime allows identical exact and ordinary bodies to share storage
  when one domain's confidentiality and access rules satisfy both (U9
  design:389-392). Forge's two domains share one key, one table space and
  one process, so they qualify.
- Bodies are stored once, keyed by an HMAC of the object id. Domain
  membership is separate rows (D4).
- With identity projection, ordinary message and usage objects equal the
  protected ones, so history is stored once, close to today's single copy.

D3.4 **Confidentiality on disk.** Nothing the journal writes reveals content
or content hashes in the clear.

- Object ids are SHA-256 over canonical plaintext, so a stored id would let
  anyone holding the database file confirm a guessed message. Forge stores
  `object_key = HMAC-SHA256(k, "forge/session-journal/object\0" || id)`
  instead, reusing the domain-separation pattern of `snapshot_mac`
  (`protected_store.rs:128-133`). Operation and pin identities get their
  own HMAC domains (`.../operation\0`, `.../pin\0`).
- Each body is sealed with XChaCha20-Poly1305 under a fresh 24-byte nonce.
  Its associated data is `object_key`, so ciphertexts cannot be swapped
  between rows.
- Heads, the original result head of each operation record, pin roots and
  dependency records are sealed the same way.
- The plaintext envelope is `FORGE-JOURNAL\0`, a format byte (`0` raw,
  `1` zstd level 3), then the payload. Bodies of at least 1 KiB are
  compressed when compression saves at least 1/8. Smaller nodes stay raw.
- `JournalObject::decode` re-verifies the real object id after decryption
  (`runtime:.../encoding.rs:394-419`). AEAD covers integrity at rest, so the
  adapter adds no separate checksum.

D3.5 **Credentials never enter either domain.**

- Provider secrets reach the runtime only through `ProviderCredentialSource`
  leases (`protected_store.rs:1535-1570`), which the runtime keeps out of
  snapshots and checkpoints. This holds today (U9 design:404-406).
- The journal adds no new input path. The host projection runs identity, so
  it neither adds nor removes values.
- A focused test seeds a credential, runs a tool turn and an OAuth refresh,
  and asserts that the secret's bytes appear in no decrypted body, head or
  operation record (task 3.5).

### D4. Schema

One new migration, `V{YYYYMMDDHHMM}__session_journal.sql`. It is named when it
lands, after `V202610180820`, following the timestamp naming in
`crates/db/migrations`. It adds tables only: no existing row changes, and
`protected_agent_session_state` stays as it is. Every table has
`session_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE`,
except the body table, which deliberately has no session foreign key.

```sql
-- One row per (session, domain): the head record, its fences and tombstone.
CREATE TABLE agent_session_journal_head (
    session_id            TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    domain                TEXT NOT NULL CHECK (domain IN ('ordinary', 'protected')),
    revision              INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0), -- CAS; 0 = writer reservation, no head
    writer_epoch          INTEGER NOT NULL DEFAULT 0 CHECK (writer_epoch >= 0),
    batch_sequence        INTEGER NOT NULL DEFAULT 0 CHECK (batch_sequence >= 0),
    head_ciphertext       BLOB,            -- sealed JournalHead; NULL before first publish and after retirement
    head_nonce            BLOB,
    has_checkpoint        INTEGER NOT NULL DEFAULT 0 CHECK (has_checkpoint IN (0, 1)),
    retired_revision      INTEGER,         -- tombstone: survives collection
    retired_root_epoch    INTEGER,
    retired_at            TEXT,
    retirement_grant_revision INTEGER,     -- host authorization, bound to one revision
    retirement_granted_at TEXT,
    gc_dirty              INTEGER NOT NULL DEFAULT 0 CHECK (gc_dirty IN (0, 1)),
    key_revision          INTEGER NOT NULL,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    PRIMARY KEY (session_id, domain),
    CHECK ((head_ciphertext IS NULL) = (head_nonce IS NULL)),
    CHECK (retired_revision IS NULL OR head_ciphertext IS NULL)
);
CREATE INDEX idx_session_journal_head_dirty ON agent_session_journal_head(domain, gc_dirty)
    WHERE gc_dirty = 1;

-- Physical immutable bodies, shared by both domains and by every session
-- that references identical content. No session FK: lifetime = memberships.
CREATE TABLE agent_session_journal_body (
    object_key     BLOB PRIMARY KEY CHECK (length(object_key) = 32),
    ciphertext     BLOB NOT NULL,
    nonce          BLOB NOT NULL CHECK (length(nonce) = 24),
    encoded_len    INTEGER NOT NULL CHECK (encoded_len > 0), -- logical journal bytes
    key_revision   INTEGER NOT NULL,
    created_at     TEXT NOT NULL
);

-- Logical object store per domain and session (published or staged).
CREATE TABLE agent_session_journal_object (
    session_id   TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    domain       TEXT NOT NULL CHECK (domain IN ('ordinary', 'protected')),
    object_key   BLOB NOT NULL REFERENCES agent_session_journal_body(object_key),
    staged_pin   BLOB,                     -- NULL = published membership
    PRIMARY KEY (session_id, domain, object_key)
) WITHOUT ROWID;
CREATE INDEX idx_session_journal_object_key ON agent_session_journal_object(object_key);

-- Idempotency / replay fences, retained for the session's lifetime.
CREATE TABLE agent_session_journal_operation (
    session_id        TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    domain            TEXT NOT NULL CHECK (domain IN ('ordinary', 'protected')),
    operation_key     BLOB NOT NULL CHECK (length(operation_key) = 32),
    input_mac         BLOB NOT NULL CHECK (length(input_mac) = 32), -- HMAC of the private commit digest
    revision          INTEGER NOT NULL,
    batch_sequence    INTEGER NOT NULL,
    writer_epoch      INTEGER NOT NULL,
    result_ciphertext BLOB NOT NULL,       -- sealed original JournalHead
    result_nonce      BLOB NOT NULL,
    committed_at      TEXT NOT NULL,
    PRIMARY KEY (session_id, domain, operation_key)
) WITHOUT ROWID;

-- Durable pins (transfer, retained root, import, staging) and engine indexes
-- (dependency roots, retained-head catalog), as sealed engine records.
CREATE TABLE agent_session_journal_record (
    session_id   TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    domain       TEXT NOT NULL CHECK (domain IN ('ordinary', 'protected')),
    record_kind  TEXT NOT NULL CHECK (record_kind IN
                     ('transfer_pin', 'retained_pin', 'import_pin', 'staging_pin',
                      'dependency', 'retained_head')),
    record_key   BLOB NOT NULL,
    ciphertext   BLOB NOT NULL,
    nonce        BLOB NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (session_id, domain, record_kind, record_key)
) WITHOUT ROWID;
```

**Notes.**

- **Concurrency.** `revision` plays the role of Forge's `version` rule
  (`WHERE revision = ? … SET revision = revision + 1`). Every head
  publication and retirement checks it. A mismatch maps to the runtime's
  `StateConflict`, never to `DbError::VersionConflict`, because these
  errors flow to the runtime and not through `ServiceError` to `ApiError`.
- **Root/pin epoch.** It lives in engine memory (R2). Collection runs only
  inside the owning process, so no durable counter is needed. Retirement
  evidence records the epoch it saw.
- **Bodies.**
  - The adapter inserts a body with `INSERT OR IGNORE` and a membership
    with `INSERT OR IGNORE`.
  - Deleting a membership deletes its body in the same transaction when
    no membership references it any more (through
    `idx_session_journal_object_key`).
  - A cascade from `agent_session` removes memberships but not bodies. The
    orphan-body sweep (D8.3) removes those with one indexed anti-join, with
    no tracing.
- **Sizes.**
  - A membership row is about 80 bytes and the head row about 1.5 KB.
  - An operation row is about 0.6–1.2 KB: the sealed head is zstd-compressed
    with its 64-hex roots. A turn makes about 5–8 commits, so this adds
    about 3–10 KB per turn. The runtime requires these records for the
    session's lifetime (U9 design:241-244), so they cannot be pruned.
  - Each body adds 40 bytes of AEAD overhead plus 9 bytes of envelope.
- **WAL behaviour.**
  - A commit is one `BEGIN IMMEDIATE` transaction that inserts O(delta)
    bodies and memberships, writes one operation row, and updates one head
    row.
  - That replaces today's whole-blob rewrite and its WAL frames of
    O(compressed history) per save (`protected_store.rs:1711-1767`).
  - `synchronous=NORMAL` keeps today's durability: a process crash loses
    nothing committed; a power loss can lose the last commits atomically,
    exactly as for the legacy row.
- **Downgrade.** The migration adds tables, so an older build refuses the
  database (`migration.rs:392-405`).

### D5. Adapter semantics, contract by contract

The Forge side lives in a new `crates/agent-host/src/session_journal/`
module. It contains:

- `SqliteJournalStorage`: implements the runtime storage trait (R1)
- `SessionJournals`: owns the two engine instances, the key material and
  the host controls
- the wiring in `SqliteProtectedRuntimeStore::journal()` for each trait

Each item below says how a `SessionJournal` contract is met:

1. **Atomic commit.** The engine prepares and validates the candidate, then
   calls the storage's write transaction (R1). In one
   `db::begin_immediate` transaction the storage:
   - inserts bodies and memberships
   - writes the operation row
   - updates the head row:
     `UPDATE … SET revision = revision + 1, head_* = ?, has_checkpoint = ?, batch_sequence = ?, gc_dirty = 1 WHERE session_id = ? AND domain = ? AND revision = ? AND writer_epoch = ?`

   Zero rows affected rolls the whole transaction back. The engine's
   `JournalCommitFailurePoint` cuts map to storage-level injection points
   between these statements (task 2.3).
2. **Writer fencing across Forge processes.**
   - Only one process can use the database (`runtime.lock`, D1 facts).
   - Writer slots are process-local (R2). Writer open runs
     `UPDATE … SET writer_epoch = writer_epoch + 1` in a transaction, or
     inserts the reservation row at `revision 0`.
   - Every commit checks `writer_epoch` in its `WHERE` clause. A handle from
     an earlier writer in the same process fails the fence.
   - After a crash or restart there are no live slots. The next writer gets
     a higher durable epoch, so nothing from the dead process can publish.
   - Within one process, two runtimes for the same session refuse the
     second writer with `StateConflict` before any provider I/O. Today both
     write and one is superseded (`protected_store.rs:1888-1893`).

   Forge already runs one turn per chat at a time, and refuses a topic
   fork while a turn is active (`native.rs:1568-1575`). Task 4.4 adds the
   test.
3. **Daemons.** Daemons run CLI executors only and never open the database,
   so no fence crosses a daemon (D1 facts). No protocol change.
4. **Idempotency.**
   - `(session, domain, operation_key)` is the primary key.
   - The same operation with the same `input_mac` returns the sealed
     original head, even after the head has advanced.
   - The same operation with a different `input_mac` conflicts.
   - Rows live until the session row is deleted (cascade).
5. **Pinned reads.**
   - The engine authorizes each `read` by reachability from the lease's
     pins. The storage resolves `object_key` through the body table after a
     membership check for the lease's domain.
   - To avoid one `SELECT` and decrypt per object when the runtime
     materializes N messages, `open` prefetches the session-domain's
     published bodies with one query into the lease handle. The prefetch
     is bounded at 256 MiB of plaintext per lease; past that, reads go one
     object at a time.
   - Immutable bodies make a process-wide LRU cache of decrypted bytes
     coherent (64 MiB default). It is filled on commit and prefetch.
     Collection evicts what it deletes.
6. **Epoch checks.** These run in the engine (R2/R3). Collection rechecks
   the epoch inside the same storage transaction that deletes memberships.
7. **Retirement.**
   - `retire` needs:
     - a `retirement_grant_revision` equal to the current revision, written
       by the host (D8)
     - no live writer slot
     - no unresolved transfer or dependency record
   - It clears `head_*`, resets `has_checkpoint` to 0, sets the tombstone
     columns and marks the session dirty, in one transaction.
   - Afterwards both read-only and writer latest-root opens return
     `StateConflict` ("session journal is retired"), even after collection
     (`runtime:.../journal/mod.rs:42-47`).
8. **Collection.**
   - The engine (R3) deletes unreachable memberships of dirty sessions,
     bounded by `max_objects`.
   - The storage deletes bodies left with no membership in the same
     transaction, clears `gc_dirty` for every session it fully swept, and
     reports logical `reclaimed_bytes` from `encoded_len`.
   - `compact` is physical: `PRAGMA incremental_vacuum(N)`. Forge's files
     use incremental auto-vacuum (`docs/architecture.md:3064-3065`), and
     compaction never deletes unreachable objects.
9. **Host probes.** `SessionJournals::probe(runtime_session_id)` returns
   `{ native: Option<{published_domains, retired, has_checkpoint}>, legacy_payload: bool }`
   from `agent_session_journal_head` and the legacy columns, plus
   `overhead_measured` through R6. A domain counts as published only when
   `head_ciphertext IS NOT NULL`. A `revision 0` reservation left by a
   writer that opened and closed without publishing (an aborted fork or a
   failed first turn) is not "saved". The probe replaces:
   - the two `native.rs` probes
   - the db probes in `embedded_agent.rs:974` and `agent.rs:1053`, through
     two new db functions; the old ones are deleted
10. **Host controls** (`JournalCollectionHost`, `journal_gc.rs:21-53`): root
    epoch, `authorize_retirement` (the grant columns), `pin_transfer` and
    `retain_root` (records), `release_pin`, `stage_leaves`
    (`staged_pin` memberships), `compact`, and `retained_bytes`
    (`SUM(encoded_len)` over the domain's distinct members). Forge
    production code calls only `authorize_retirement`, `retire` and
    `collect`. The rest exist for conformance and for runtime-driven
    transfers.

### D6. Conformance suites in Forge's test suite

Task 1 adds `agent-runtime-testkit` as a dev-dependency of `forge-agent-host`,
at the same immutable revision as `agent-runtime`. Forge then runs:

| Target | Suites | Factory |
| --- | --- | --- |
| `crates/agent-host/tests/session_journal_conformance.rs` | `assert_session_journal_conformance` (protected; real injected cuts and `revoke_writer`) | a fresh `file::memory:?cache=shared` pool per scenario (shared cache, so the engine's several connections see one database) + `SqliteJournalStorage` + protected engine |
| same | `assert_ordinary_session_journal_conformance` (the factory receives arbitrary policies) | same, ordinary engine with the supplied policy |
| same | `assert_session_journal_collection_conformance`, plus `assert_ordinary_journal_collection_conformance`, `assert_ordinary_gc_shared_suffix_conformance`, `assert_journal_gc_projected_intent_conformance` | `SessionJournalCollectionFixture { backend, host }` built from the SQLite adapter |
| same | `assert_native_materialized_checkpoint_schema_guard` with Forge's real legacy decoder | Forge decode of `CheckpointStore::load_latest` bytes |
| `crates/agent-host/tests/session_journal_runtime.rs` | `assert_chunked_journal_recovery`, `assert_native_idle_compaction`, `assert_optional_durable_journal` where their fixtures accept an injected journal pair. Otherwise, the Forge-owned equivalents: v3 import of every legacy checkpoint state through `SqliteProtectedRuntimeStore` with real encryption | real `SqliteProtectedRuntimeStore` |

Before the SQLite storage exists (task 1.1), the same targets run against the
reference engine with volatile storage. That proves the wiring and gives
the later swap a one-line diff. Suites that need fresh stores take a
`file::memory:?cache=shared` pool per scenario.

### D7. Import and migration

D7.1 **First writer open.**

- With the adapter wired, every persistent native session is a capable
  pair: one struct provides both `journal()`s (`native.rs:885-888`).
- The runtime's `SessionPersistence::open` reserves both writer domains,
  loads the v3 pair once through Forge's legacy `load`/`load_latest`
  (decrypt and typed decode, unchanged), validates it, and publishes the
  exact head before the ordinary one. It uses stable `runtime-import-v3-*`
  operation ids and makes no provider or tool calls (U9 design:612-637,
  CHANGELOG "Session journal migration").
- Forge does nothing at migration time. The database migration only adds
  tables.

D7.2 **Mid-turn sessions.**

- At upgrade the server is stopped (`runtime.lock`), so no turn is live.
- A checkpoint left mid-turn by a crash imports as its exact state:
  - CallingModel requests are frozen
  - ModelResponseReady and ToolOutcomeReady results are reused
  - approvals and interactions are preserved
  - executing and cache-started states keep their no-replay handling
- Resume then follows today's rules: an abandoned non-terminal checkpoint
  is finalized as failed without replay (`docs/architecture.md:2556-2566`).
- Forge's startup suspension (`recovery.rs:195-208`) is unchanged.

D7.3 **Crash during import.**

- Before the protected commit, v3 stays authoritative and the next open
  retries.
- After it, the native head is authoritative. A missing ordinary head is
  repaired from the v3 record on the next open.
- Forge's legacy row is never written during import.

D7.4 **Process split.**

- The host and the runtime share one process, which holds the only writer
  (`runtime.lock`).
- Remote daemons hold no session state.
- The vacuum conversion takes the same lock (`docs/getting-started.md:497`),
  so it cannot interleave with an import.

D7.5 **Legacy store after adoption.**

- `SessionStore::load` and `CheckpointStore::load_latest` remain, unchanged,
  as the v3 import source.
- Both `save` methods lose their bodies: the CAS, digest-skip and seal code
  at `protected_store.rs:1660-1775` and `:1815-1897`. Each returns
  `RuntimeError::conflict("legacy session store is read-only")`. For a
  capable pair the runtime never calls them (U9 design:261-268), so a call
  is a wiring bug and must fail rather than write stale state.
- `snapshot_digest`, the `save_barrier` test hook and the
  `lcm_policy_revision` write paths go with them. The columns stay until
  the cleanup migration.

D7.6 **Legacy table end state.**

- This change keeps `protected_agent_session_state` unchanged and readable.
- With R5, an imported session never reads its legacy row again.
- Import is lazy, so rows of sessions that are never reopened stay legacy.
  That includes non-chat sessions, which a restart replaces
  (`recovery.rs:195-208`).
- A separate cleanup change follows, no earlier than the release after the
  first release that ships this one. Its migration and worker:
  - NULL the legacy payload of every session that has both native heads.
    The data is already in the journal, so this is data-preserving.
  - Import the legacy-only sessions Forge can still resume (dormant Agent
    Chat sessions), one bounded writer open and close per session, with no
    provider.
  - Remove legacy payloads of dead sessions per Q1.
  - Drop the table once no row holds a payload.

D7.7 **Downgrade.**

- The new migration makes older builds refuse the database
  (`DbError::DatabaseNewerThanBuild`, `migration.rs:404`). No old binary
  can resume a stale v3 row of an imported session, which is the runtime's
  downgrade hazard (runtime `CHANGELOG.md:81-86`).
- Downgrade means restoring a pre-upgrade backup (`docs/getting-started.md:459`).
  Every change made after the backup is lost, as for any Forge migration.
- No idle-export tool is built.

### D8. Lifecycle: retirement and collection

D8.1 **What retires a session.** Retirement is permanent: even read-only
latest opens fail afterwards. Forge retires only sessions it will never
resume:

- **Topic rotation source.** It is marked `replaced` in the same transaction
  that records the topic (`agent_chat_topic.rs:328-357`). Its successor seed
  is already published, because `child.persist()` runs before
  `rotate_agent_chat_topic` (`native.rs:1725-1736`,
  `topic_rotation.rs:267-290`). The fork intent is resolved, so retirement
  is allowed.
- **Profile rotation.** `rotate_agent_session` marks the previous session
  `replaced` and creates a new runtime id (`embedded_agent.rs:614-650`,
  `embedded_agent_service.rs:1911`).
- **Dead suspended sessions** (Q1). This means non-Agent-Chat native
  sessions suspended by a restart, which Forge never resumes
  (`embedded_agent_service.rs:1528-1542`), and Agent Chat suspended rows
  that are not their scope's dormant session. They are off by default
  until the owner answers Q1.
- **Not on task completion.** A Task session can be reused while the Task
  lives, through review fixes and repair, and recovery may rebind it. Parity
  with today: kept.
- **Project, Agent or chat deletion.** The `agent_session` cascade removes
  heads, memberships, operations and records in the deletion transaction.
  The orphan-body sweep removes the bodies afterwards (D8.3). Deletion does
  not go through `retire`/`collect`, because the session row itself is gone
  and no open can find it.

D8.2 **How.**

- A new `session-journal-retention` worker in `services-agents`, on the
  existing workers framework (as `workspace_cleanup.rs:218` uses), ticks
  every 60 s like workspace cleanup (`workspace_cleanup.rs:37`).
- Each tick:
  1. Finds `replaced` sessions with a published, unretired head.
  2. For each one with no live writer slot:
     - writes the grant (`authorize_retirement` at the current revision)
     - calls `retire` on both domains (protected first)
  3. Runs `collect` per domain with `max_objects = 20_000` and a 10 s
     budget, over dirty sessions.
  4. Runs the orphan-body sweep.
- Topic rotation and profile rotation also nudge the worker right after
  their transaction commits, so retirement usually happens within one tick.
- A crash at any step leaves a state the next tick repeats.

D8.3 **Collection cadence.**

- Collection deletes only objects no live root, retained root or pin can
  reach. That covers superseded checkpoint state, replaced cache records
  and manifest paths, and aborted staging.
- It never expires live data, so it is garbage collection, not a TTL.
- Periodic triggering is justified: without it, a live session keeps every
  superseded CallingModel request and checkpoint state. The runtime's N=1000
  case reclaims 165,592 of 570,121 bytes with the session live (runtime
  `CHANGELOG.md:131-136`).
- Orphan-body sweep:
  `DELETE FROM agent_session_journal_body WHERE NOT EXISTS (SELECT 1 FROM agent_session_journal_object o WHERE o.object_key = body.object_key) LIMIT ?`.
  It runs in batches of 5,000 per transaction, so Project deletion never
  holds the write lock for a long time.

D8.4 **Retained-bytes expectations** (logical; task 6 verifies them).

| Session state | Legacy today | Native after this change |
| --- | --- | --- |
| Bootstrap / import at N messages | one zstd + AEAD blob of the snapshot (or checkpoint) | about 3.8× the uncompressed snapshot (runtime U9), with history bodies shared across domains and ≥ 1 KiB bodies zstd-compressed. Target: ≤ 4× the legacy *uncompressed* snapshot across both domains. |
| Per one-message turn | 0 net (row overwrite); WAL churn about 5× compressed snapshot | about 76 KB logical new objects (plain) or 158 KB (tool) before collection (runtime table). After collection: new message, usage and manifest-window bytes plus O(log N) spine plus 3–10 KB of operation rows. |
| Dead (`replaced`) session | kept forever | 0 bytes after retirement and collection (runtime N=1000: 0 after retirement); tombstone row and operation rows remain until the `agent_session` row is deleted |
| Project deletion | cascade | cascade, plus bodies removed within one worker tick |

### D9. Rollout

- **No feature flag.** The release that contains this change wires
  `journal()` for both traits. Every persistent native session is a capable
  pair and imports on its next writer open. Ephemeral scopes stay
  store-less (`native.rs:1187-1191`).
- **Mixed pairs.** Production has none: the same struct implements both
  traits. A test-only store that implements only one trait stays legacy,
  exactly as the runtime specifies.
- **Visible effects.**
  - Per-turn database writes drop from O(history) to O(delta). At N=4000
    that is about 7.4 MB of logical rewrite per turn before compression,
    against about 55 KB.
  - Long sessions use more disk while they live (D8.4).
  - The first turn of an existing chat after upgrade pays a one-time
    import, about one legacy resume plus a bootstrap commit. At N=4000 that
    bootstrap is one `BEGIN IMMEDIATE` with roughly 10k+ body and
    membership inserts. Other Forge writers wait on the 30 s busy timeout
    (`connection.rs:254-258`) while it runs. D10 measures its duration. If
    the duration is in seconds, the runtime import must commit in bounded
    chunks (raised against R1) before the cleanup change's background
    import is specified.
  - A second concurrent writer on one session fails fast instead of being
    silently superseded.
  - Downgrade needs a backup.
- **CHANGELOG** (`Unreleased`). Proposed text, applied in task 7.1:
  - `### Breaking`
    - Native Agent sessions persist through the Agent Runtime session
      journal. On its next turn, each existing session is imported once
      from its stored snapshot and checkpoint, including unfinished turns,
      with no repeated model or tool calls. Older builds refuse the
      upgraded database; downgrade by restoring a pre-upgrade backup.
      Sessions advanced after the upgrade lose their runtime state in such
      a restore.
    - A second runtime that opens the same native session while a turn is
      running now fails with a conflict instead of overwriting the first.
  - `### Changed`
    - Each runtime transition writes only what changed (new messages,
      usage, changed state) instead of re-encrypting the whole
      conversation. Long sessions write about 99% less per turn. Their
      retained size grows to about four times the uncompressed
      conversation while they live.
    - Session state of replaced sessions (topic rotation sources and
      profile changes) is retired and collected in the background. Chat
      transcripts are unaffected.
- **Docs to update** in the same change as the code:
  - `docs/architecture.md`: rewrite the protected-store paragraphs at
    `:2412-2438`; replace the pin paragraph at `:2440-2449`; add a
    "Session journal storage" subsection covering D3, D4, D5 and D8; and
    add the retention worker to the worker inventory.
  - `docs/getting-started.md:459`: name the import and the downgrade
    consequence. Disk guidance goes near the data-directory section.
  - `docs/performance-testing.md`: add the session-journal bench and its
    results (D10).
  - No `docs/api.md` change: `checkpoint_present` keeps its meaning and no
    endpoint changes.

### D10. Measurement plan

- `scripts/perf_fixture.py` does not apply. It seeds tables over SQL and
  HTTP, with no native session state (`scripts/perf_fixture.py:30-39`), and
  it cannot produce encrypted runtime state without the server key path.
- The bench reuses the `test_store()` pattern instead
  (`protected_store.rs:2013-2063`).
- New `#[ignore]` target `crates/agent-host/tests/session_journal_bench.rs`,
  release profile, run by `scripts/perf_bench.py`-style invocation and
  documented in `docs/performance-testing.md`. It uses a file-backed
  database in a temp dir under `$TMPDIR`.
- For N ∈ {100, 1000, 4000} messages (4000 is the largest the runtime's
  4,096-entry cache identity admits; runtime `CHANGELOG.md:126-129`):
  - **Legacy baseline.** Build the session with the legacy store (seeded
    v3 history, as the runtime's fixtures do). Run 10 one-message turns
    and 10 tool turns with the testkit fake provider.
  - **Native.** The same starting v3 row; the first turn imports.
- Per turn, record:
  - bytes written: `SUM(length(...))` of inserted rows, and the WAL file
    size before the checkpoint
  - wall time of `start_session`, the turn and `shutdown`
  - for native, logical submitted bytes and new-object bytes from the
    engine's counters
- After the runs, record retained bytes: `page_count × page_size` after
  `wal_checkpoint(TRUNCATE)` and `incremental_vacuum`, and logical
  `retained_bytes`. Then collect again and retire, and record once more.
- Also record the per-turn probe time (`native.rs:1150-1170` legacy versus
  `probe`), the one-time import time, and the longest single write
  transaction during import (time the write lock is held) at each N.
- **Gates**, recorded as pass or fail in the report (failures go to the
  owner, they are not hidden):
  - at N ≥ 1000, native bytes written per turn ≤ 10% of legacy
  - at N = 100, native turn wall time ≤ legacy + 10%; at N ≥ 1000, below
    legacy
  - retained bytes after collection ≤ 4× the legacy uncompressed snapshot
  - import time ≤ 2× one legacy resume
- Output: a JSON report plus the table in `docs/performance-testing.md`.

## Risks / Trade-offs

- **Prerequisite slip.** Forge adoption waits for R1–R6, which is
  cross-repository work. Mitigation: tasks 1.1, 2.1, 2.2 and 3.1 (harness,
  schema, sealing and the db probes) do not depend on the engine. The
  conformance harness lands first against the reference engine.
- **CPU stays O(N) per turn.** Validation and policy decode stay O(N) when
  there is no warm runtime (U9 design:171-176). R4 removes the per-writer
  full validation. The rest is today's cost class. Gate D10 tracks it.
- **Disk growth for live long sessions** (D8.4, Q2). Mitigations: shared
  bodies across domains, compression, periodic collection, and retirement
  of dead sessions.
- **Shared bodies need correct deletion.** A body is deleted only when no
  membership remains, in the same transaction. Collection conformance plus
  a cross-domain test (task 2.4) cover it.
- **Second-writer refusals.** A latent concurrent-runtime path would now
  fail instead of racing. Task 4.4 searches for and tests the known paths:
  turn worker, topic fork, abort fork and the inquiry runner.
- **Key rotation** stays as unsupported as today (`key_revision` 1). The
  column is recorded on every body so a future rotation can find rows.

## Migration Plan

1. Runtime prerequisite R1–R6 lands; the runtime pin moves to that
   immutable revision.
2. Conformance harness (reference engine).
3. Schema migration and storage.
4. Conformance against SQLite.
5. Cut-over: `journal()`, probes, legacy save refusal.
6. Retention worker.
7. Measurement.
8. Docs and CHANGELOG.

Rollback before release: revert the change. After release: restore a backup
(D7.7).

## Owner Decisions (2026-10-19)

- **Q0. Runtime prerequisite: approved.** The runtime extracts its journal
  engine behind a storage trait (D2, R1–R6) before Forge adoption starts.
  Forge reimplements nothing.
- **Q1. Retirement: replaced and unresumable sessions.** Forge retires
  `replaced` sessions (topic-rotation sources and profile rotations). It also
  retires suspended native sessions it can no longer resume (non-Agent-Chat
  scopes after restart, and Agent Chat rows that are not the scope's dormant
  session). Task 5.2 is in scope. Chat transcripts in `agent_chat_message`
  are unaffected.
- **Q2. Disk: reduce first.** The owner did not accept "≤ 4× the
  uncompressed conversation". Adoption waits for a disk-reduction
  prerequisite (R7 below), and D8.4 and gate D10 change to that target.
  - **R7. Physical retained bytes close to today's.**
    - Measure physical bytes: SQLite pages after checkpoint and VACUUM,
      including encryption, compression, per-row and index overhead. Logical
      object bytes are not the measure.
    - Target: the physical retained bytes of a live session with N=4000
      messages, after collection, are at most 2× today's legacy on-disk size
      for the same session (one zstd+AEAD blob per store). Aim for 1.5×.
      Report the number at N=100, 1000 and 4000.
    - Candidate levers, to be decided by measurement:
      - pack each commit's new objects into one compressed and sealed pack
        row with an offset index, instead of one row and one AEAD envelope
        per object;
      - share identical bodies across domains (already in D3);
      - binary object ids at the storage boundary;
      - fewer, larger objects for small values in the runtime encoding
        (needs a runtime format version, because 0.6.x is released);
      - collect eagerly after terminal boundaries.
    - The runtime-side parts land in the same runtime prerequisite as
      R1–R6, with their own spec.
- **Q3. Legacy table: lazy import now, cleanup next release.** Sessions
  import on first writer open. The next release background-imports resumable
  legacy-only sessions, NULLs migrated payloads, applies Q1 to dead sessions,
  and drops the table once it is empty.
