## ADDED Requirements

### Requirement: Consume the released storage-generic runtime engine
Forge SHALL implement `JournalStorage` as `ForgeJournalStorage` and use the runtime's `JournalEngine<ForgeJournalStorage>` for protected and ordinary journal semantics. Adoption SHALL start by pinning all runtime packages and testkit to the same immutable revision of the first agent-runtime release containing extract-journal-storage-engine, (`v0.8.0`, `fd5b14ca12f4a8dbc4d52030f3c095292f4eb4a7`), including amended D2 rules 1–7, `JournalBodySharing::PerSession`, physical-copy `JournalApplyReceipt`, `retained_bytes`, the N1/N2 fixes, D-1's reader-operation authority check, the corrected futures mutex gate, native-first open, bounded extension reads and engine-owned replay pruning. Forge SHALL NOT copy the runtime's validation, closure, lease, dependency, replay or collection algorithms.

#### Scenario: The prerequisite release is unavailable
- **WHEN** the released runtime pin cannot satisfy the complete consumed contract
- **THEN** Forge adoption is blocked at task 1
- **AND** no speculative runtime version or Forge engine workaround is substituted

#### Scenario: Reuse across per-turn Runtime instances
- **WHEN** Forge constructs a new Runtime for another turn of the same session
- **THEN** it receives clones of exactly one long-lived engine per storage/domain, sharing the validated-root memo and lease registry with every runtime and collector
- **AND** a memo hit avoids full closure reads while restart or eviction requires cold validation

#### Scenario: Memo budget and collection leases
- **WHEN** production engines are constructed and a collector runs while another runtime holds a lease
- **THEN** each engine is configured with 128 MiB of estimated memo charge, 256 MiB combined, independently of the bounded storage cache and live writer working sets
- **AND** collection uses the same engine's shared lease registry
- **AND** memo misses cost only cold validation, affecting speed, never correctness; Forge's local, single-user use has no evidence requiring the larger runtime default
- **AND** measurements report memo hit/miss counts and cold-validation time so the budget can be raised on evidence, plus actual resident memory and the review's observed up-to-1.24× retained heap/charge caveat (roughly 159 MiB per engine / 317 MiB combined at full charge), without promising a heap cap

### Requirement: Native journal persistence for persistent native sessions
Every persistent native session SHALL advertise an ordinary journal through `SessionStore::journal()` and a protected journal through `CheckpointStore::journal()`. Both engines SHALL share one Forge storage instance with distinct storage ids/roles and collection enabled; each transition SHALL add only new canonical bodies and changed metadata. Ephemeral scopes SHALL remain store-less and no rollout flag SHALL select legacy writes.

#### Scenario: Second turn writes only the delta
- **WHEN** a persistent session completes a second one-message turn
- **THEN** it adds no duplicate physical body for a message already stored in the same domain/runtime-session scope
- **AND** the runtime advances the applicable domain heads through the engine
- **AND** the post-terminal hook schedules S2 maintenance after publication succeeds

#### Scenario: Ephemeral scope
- **WHEN** a native turn runs without persistent context
- **THEN** it writes no journal session record, pack, membership or operation pack

### Requirement: Protected exact and ordinary projected domains over encrypted packs
The protected domain SHALL preserve exact checkpoint/snapshot state. The ordinary engine SHALL apply Forge's conservative D3 redacting `JournalProjectionPolicy`, decided on 2026-10-20, before encoding/hashing, preserving runtime-required identity, ordering/cardinality, counters, timestamps, manifests and cache values, and all restore-critical data in ordinary-only sessions. Optional sensitive host diagnostic values SHALL be omitted or redacted without changing protected exactness; runtime-mandated exact cache/manifests may contain content. Both domains SHALL use real XChaCha20-Poly1305 with the existing protected-store key material, fresh nonces and authenticated row binding.

Full object ids SHALL remain inside sealed payloads and rebuildable RAM lookup maps; prefix/offset indexes SHALL remain inside sealed packs, with no per-object SQL hash/index. Catalog/pin lookup keys SHALL remain domain-separated HMACs; their real ids and all operation ids/digests/results SHALL be sealed. Membership and engine lease reachability SHALL both be required to serve bytes, including cache hits. Credentials SHALL be absent from both domains.

#### Scenario: Ordinary projection and exact recovery
- **WHEN** a candidate contains an optional sensitive diagnostic extension and exact recovery state
- **THEN** ordinary materialization equals the host projection, with the diagnostic value redacted or omitted
- **AND** protected materialization equals the exact submitted recovery state
- **AND** a policy rejection makes no raw ordinary draft visible

#### Scenario: Ordinary-only session retains recoverability
- **WHEN** a snapshot-only legacy session or a session persisted before its first checkpoint is opened
- **THEN** ordinary projection preserves the conversation and extension values needed for exact resume
- **AND** Forge does not invent a checkpoint or discard recovery data in order to redact optional diagnostics

#### Scenario: Membership isolation on a cached pack
- **WHEN** an ordinary lease requests a protected-only body already in the plaintext cache
- **THEN** no bytes are returned for that object
- **AND** another session or storage id without membership is refused in the same way

#### Scenario: Pack verification and ciphertext swap
- **WHEN** a pack prefix collides with another object's prefix or ciphertext is moved to a different authenticated row key
- **THEN** the reader verifies the complete runtime object id and refuses a missing/mismatched id
- **AND** a row-binding/authentication failure fails closed rather than opening a fresh session
- **AND** AAD binds unit kind, pack id, generation and owning domain/runtime-session key, using distinct HKDF-derived AEAD and lookup keys

#### Scenario: No credential reaches the journal
- **WHEN** a session runs a tool turn and refreshes an OAuth credential
- **THEN** no decrypted pack, session, operation, catalog or pin payload contains the API key, access token or refresh token

### Requirement: Six-table pack layout and lossless storage records
Forge SHALL create session-record, pin, catalog, pack, bitmap-membership and operation_pack tables as described in design D4, without per-object body/offset rows. Per-session metadata/membership rows SHALL be keyed by the runtime session id the trait receives and SHALL carry an indexed `forge_session_id` foreign key that cascades from `agent_session`; an apply whose runtime id maps to no or several `agent_session` rows SHALL fail with a typed host error and write nothing. Packs SHALL have strict domain/runtime-session ownership and an indexed Forge owner label without a cascading owner FK, permitting bounded deletion cleanup. JournalEngineState SHALL round-trip version and bytes opaquely; Forge SHALL implement every JournalStorage read method, ordered dirty paging and apply/compact. Each pack SHALL hold a separately sealed prefix/varint offset index unit and a sealed zstd body unit of kind-ordered canonical bodies, with full-id verification and no runtime schema-5 change. Storage SHALL declare `JournalBodySharing::PerSession`, deduplicate only within `(storage_id, role, runtime_session_id)`, and keep independent copies across sessions and across the two domains. Storage open SHALL decode no pack units; lookup directories SHALL load lazily for the requested scope only. Pack ids SHALL never be reused.

#### Scenario: Batch admission and lookup after restart
- **WHEN** an apply admits a batch of new bodies and storage is reopened
- **THEN** the final new bodies occupy a compressed/sealed pack with bitmap memberships
- **AND** only the requested domain/session's lookup directory is rebuilt lazily from its sealed index units, with candidate body units opened as needed for full-id verification
- **AND** unrelated sessions' packs and the other domain's packs are not decoded or searched for admission
- **AND** unknown opaque engine-state versions/bytes are returned unchanged

#### Scenario: Independent session and domain copies
- **WHEN** a session/domain admits a body stored only in another session or domain, including after cache eviction or storage reopen
- **THEN** it writes its own physical copy without looking up the other scope's body
- **AND** reclaiming that copy does not wait for the other scope's membership or affect its readability

#### Scenario: Same-scope re-admission
- **WHEN** an apply admits an identical body already retained in its domain/session pack, including an unserved deferred slot
- **THEN** it reuses that slot and establishes membership without a duplicate physical copy
- **AND** a different body for the same id conflicts atomically

#### Scenario: Consistent read during repack
- **WHEN** another connection commits a pack rewrite and bitmap remap while an object batch is being read
- **THEN** the read observes a consistent completed transaction or retries
- **AND** a stale cached generation/slot cannot return different bytes or bypass membership

#### Scenario: Dirty paging
- **WHEN** dirty_sessions is called with a domain, after-session cursor and limit
- **THEN** it returns only that domain's dirty runtime session ids in ascending order strictly after the cursor
- **AND** a zero limit returns no sessions

### Requirement: Atomic fenced apply with deletion receipts
Every JournalWrite SHALL run in one sqlx BEGIN IMMEDIATE transaction on one connection, preserving the runtime's ordered cut points, all memberships, pin/catalog changes, replacement record and operation packs. SQL SHALL enforce all three `JournalFence` fields (revision, writer_epoch, generation) inside that transaction for every write, including controls preserving revision/epoch; absence SHALL match only the all-zero fence. Every write SHALL carry a required replacement session record with generation exactly expect.generation + 1; overflow or a non-successor generation SHALL conflict atomically. The session generation SHALL be persisted independently of pack generations. A mismatch SHALL be a typed runtime state conflict with no effects; other ambiguous errors SHALL invalidate caches for reload.

Releases SHALL end membership read authority immediately. A sealed pack MAY retain dead bodies below 25% of its physically retained canonical body bytes, never serving them; the apply reaching 25% or more SHALL purge all its dead slots transactionally. An empty domain/session scope and collection of a retired session SHALL purge all that scope's dead bytes, including earlier deferred bytes without new release ids. Purges SHALL run even with optional S2 merging disabled. JournalApplyReceipt SHALL count physical body copies and their original canonical encoded lengths before compression/sealing exactly once at removal time, including previously deferred bodies. Clearing bits, moving live bodies or reclaiming SQLite pages SHALL count zero. Forge SHALL return `Some` from `retained_bytes(domain)` with canonical bytes physically retained in that domain's session packs, including deferred dead bodies; this diagnostic SHALL NOT control liveness or stand for file size.

#### Scenario: Failure before publication
- **WHEN** any pre-commit JournalCommitFailurePoint is injected
- **THEN** no pack, membership, pin/catalog, session record or operation change from that apply becomes visible

#### Scenario: Lost acknowledgement
- **WHEN** AfterCommit is injected and a fresh engine opens the file
- **THEN** the complete committed state is durable and visible
- **AND** retry within the retained replay window returns the original result without reapplying

#### Scenario: Stale SQL fence
- **WHEN** a writer or a collection/pin/authorization write presents a stale revision, writer epoch or control generation
- **THEN** SQL refuses it and all staged changes roll back
- **AND** the refusal does not depend solely on a process cache or mutex

#### Scenario: Concurrent controls at the same revision and writer epoch
- **WHEN** two controls prepare writes from the same session fence and one commits
- **THEN** the second control conflicts on generation even though revision and writer epoch still match
- **AND** pins, retirement grants and dependency candidates from the first control are preserved

#### Scenario: Invalid successor generation
- **WHEN** a write supplies a non-successor generation or expect.generation cannot be incremented
- **THEN** the apply conflicts without any effects

#### Scenario: Unrelated database writes
- **WHEN** other Forge subsystems commit to the same database file between a journal read and the next apply
- **THEN** the apply refreshes its caches inside its own write transaction and succeeds if its expect fence still matches
- **AND** no StateConflict is raised for cache staleness

#### Scenario: Below-threshold release and later threshold purge
- **WHEN** a release leaves a partially live sealed pack below 25% dead canonical body bytes
- **THEN** the released body is unreadable even on a cache hit, its bytes remain in retained_bytes and its receipt contribution is zero
- **AND** the later apply reaching 25% purges all dead slots, remaps survivors and counts all removed copies and canonical lengths once, even with S2 disabled
- **AND** retained-byte differences equal cumulative physical receipts across those applies

#### Scenario: Retired or empty scope forces removal
- **WHEN** a scope empties or collection runs for a retired session with below-threshold deferred dead bytes
- **THEN** all dead bytes in its owned packs are purged, including slots not named by a new release
- **AND** the removing apply returns their physical-copy receipt while independently held copies in other scopes stay readable

### Requirement: Engine replay window with packed original results
Forge SHALL store lossless operation records in binary-carry operation packs capped at 1 MiB of serialized records before compression/sealing, with bounded overflow segments and atomic rejection of an oversized record, and let the engine select pruning only during collection. Import and prune/rebuild output SHALL obey the same cap. When the engine supplies `prune_operations_before = floor` (current revision minus 64, saturating), storage SHALL keep, within that domain and session only, every record whose revision is at or above the floor plus the single record with the highest revision whose result writer epoch equals the session record's writer epoch after the write; it SHALL prune every other record below the floor and SHALL NOT prune without the directive. Identical retained replays SHALL return the original head, different inputs SHALL conflict, and pruned replays SHALL conflict without applying again.

#### Scenario: Replay after head advance and writer change
- **WHEN** a retained operation is retried with the same input after a later publication or writer change
- **THEN** the engine returns its complete original result head from the operation packs
- **AND** no new revision or duplicate body is written

#### Scenario: Window boundary and current epoch exception
- **WHEN** collection at current revision 70 supplies the pruning floor 6
- **THEN** records at revisions 6 through 70 survive and records below 6 are removed except the epoch exception below
- **AND** a current-writer-epoch record below the floor survives only if it is that epoch's highest-revision record
- **AND** a replay of a pruned operation conflicts rather than becoming a new commit

### Requirement: One-time lazy import with exact-first authority
The additive migration SHALL preserve existing data and leave protected_agent_session_state readable. First native writer open SHALL use the runtime's v3 importer with no provider/tool I/O; where a checkpoint exists, protected exact publication SHALL precede ordinary publication. Native-first open SHALL avoid unnecessary legacy decoding, both legacy saves SHALL refuse writes, and the legacy table SHALL be removed only by a separate next-release cleanup after resumable rows are imported and dead rows handled under the approved retirement rule.

#### Scenario: Mid-turn import
- **WHEN** a legacy checkpoint at CallingModel or a persisted model/tool result is imported
- **THEN** exact requests/results and existing approval/interaction/no-replay barriers survive
- **AND** the legacy row stays byte-for-byte unchanged with no model or tool call

#### Scenario: Partial import
- **WHEN** the process stops after protected import publication and before ordinary publication
- **THEN** the next open repairs ordinary publication from matching unchanged import evidence once
- **AND** it never overlays stale legacy data onto an advanced native head

#### Scenario: Data-preserving migration and downgrade
- **WHEN** the one new V{YYYYMMDDHHMM} migration is applied
- **THEN** it creates the pack schema without editing historical migration files or deleting legacy payloads
- **AND** an older build refuses the newer database; downgrade uses a pre-upgrade backup

### Requirement: Host probes and async engine controls
Host existence, hand-over and checkpoint probes SHALL use scalar native metadata and eligible legacy fallback, never full materialization. Reservations SHALL not count as publication, tombstones SHALL suppress stale fallback, and LCM overhead SHALL use JournalReader::extension independently. Durable controls SHALL be awaited; Forge SHALL not use volatile synchronous polling/blocking wrappers on a Tokio worker or replace the runtime's futures mutex gate with a Tokio handoff.

#### Scenario: Published native checkpoint
- **WHEN** a native-only session has a published protected head with has_checkpoint
- **THEN** Agent continuity reports checkpoint_present=true and hand-over recognizes saved state
- **AND** the existence probe decrypts no pack

#### Scenario: Reservation and retirement
- **WHEN** the only native record is a revision-0 writer reservation or a retirement tombstone
- **THEN** a reservation is not reported as saved state and retirement is surfaced as a conflict
- **AND** a stale legacy payload cannot resurrect the retired identity

#### Scenario: Suspended storage on Tokio
- **WHEN** storage suspends while retirement, collection, open and close contend
- **THEN** durable controls yield asynchronously and local close/revoke remain I/O-free
- **AND** current-thread and multi-thread workers remain able to make progress

### Requirement: Real-storage conformance and negative controls
Forge SHALL run assert_journal_storage_conformance unedited over production ForgeJournalStorage with repacking off, on, and storage reopened per hydration, real sealing and file-backed restart. Every configuration SHALL declare PerSession with independent domain copies and retain mandatory threshold/retirement purges. Scope-aware receipts, remaining-holder readability, non-member isolation and bounded deferred deletion/retained-byte evidence SHALL all pass. Forge SHALL also retain equivalence/import/crash coverage through JournalCommitFailurePoint::ALL and the real legacy decoder schema guard. All six upstream broken-storage fault shapes SHALL be tested over Forge-backed decorators and rejected at their named cases; private upstream test adapters SHALL NOT be assumed exported.

#### Scenario: All configurations and policies
- **WHEN** Forge's named storage conformance wrappers run
- **THEN** every primitive, protected, ordinary, arbitrary-projection, collection, unsupported-collection and restart assertion passes in all three storage configurations

#### Scenario: Broken storage is detected
- **WHEN** a Forge-backed decorator introduces a torn transaction, lost write, reordered publication, stale epoch, premature deletion of a surviving holder's copy, or serving released/deferred bytes
- **THEN** conformance rejects it at atomic_apply_cuts, restart_acknowledged_state, restart_commit_sequence, writer_epoch_fence, shared_body_last_membership, or membership_isolation_and_receipts/bounded_deferred_deletion respectively
- **AND** a passing broken backend is a test failure
