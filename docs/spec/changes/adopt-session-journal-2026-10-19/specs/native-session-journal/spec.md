## ADDED Requirements

### Requirement: Consume the released storage-generic runtime engine
Forge SHALL implement `JournalStorage` as `ForgeJournalStorage` and use the runtime's `JournalEngine<ForgeJournalStorage>` for protected and ordinary journal semantics. Adoption SHALL start by pinning all runtime packages and testkit to the same immutable revision of the first agent-runtime release containing extract-journal-storage-engine, including D2 rules 1–7, `JournalApplyReceipt`, the N1/N2 fixes, the corrected futures mutex gate, native-first open, bounded extension reads and engine-owned replay pruning. Forge SHALL NOT copy the runtime's validation, closure, lease, dependency, replay or collection algorithms.

#### Scenario: The prerequisite release is unavailable
- **WHEN** the released runtime pin cannot satisfy the complete consumed contract
- **THEN** Forge adoption is blocked at task 1
- **AND** no speculative runtime version or Forge engine workaround is substituted

#### Scenario: Reuse across per-turn Runtime instances
- **WHEN** Forge constructs a new Runtime for another turn of the same session
- **THEN** it reuses the long-lived domain engines and their bounded validated-root memo
- **AND** a memo hit avoids full closure reads while restart or eviction requires cold validation

### Requirement: Native journal persistence for persistent native sessions
Every persistent native session SHALL advertise an ordinary journal through `SessionStore::journal()` and a protected journal through `CheckpointStore::journal()`. Both engines SHALL share one Forge storage instance with distinct storage ids/roles and collection enabled; each transition SHALL add only new canonical bodies and changed metadata. Ephemeral scopes SHALL remain store-less and no rollout flag SHALL select legacy writes.

#### Scenario: Second turn writes only the delta
- **WHEN** a persistent session completes a second one-message turn
- **THEN** it adds no duplicate physical body for a message already stored
- **AND** the runtime advances the applicable domain heads through the engine
- **AND** the post-terminal hook schedules S2 maintenance after publication succeeds

#### Scenario: Ephemeral scope
- **WHEN** a native turn runs without persistent context
- **THEN** it writes no journal session record, pack, membership or operation pack

### Requirement: Protected exact and ordinary projected domains over encrypted packs
The protected domain SHALL preserve exact checkpoint/snapshot state. The ordinary engine SHALL apply Forge's redacting `JournalProjectionPolicy` before encoding/hashing, preserving runtime-required identity, ordering/cardinality, counters, timestamps, manifests and cache values, and all restore-critical data in ordinary-only sessions. Optional sensitive host diagnostic values SHALL be omitted or redacted; both domains SHALL use real XChaCha20-Poly1305 with the existing protected-store key material, fresh nonces and authenticated row binding.

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

#### Scenario: Membership isolation on a cached shared pack
- **WHEN** an ordinary lease requests a protected-only slot from a pack already in the plaintext cache
- **THEN** no bytes are returned for that object
- **AND** another session or storage id without membership is refused in the same way

#### Scenario: Pack verification and ciphertext swap
- **WHEN** a pack prefix collides with another object's prefix or ciphertext is moved to a different authenticated row key
- **THEN** the reader verifies the complete runtime object id and refuses a missing/mismatched id
- **AND** a row-binding/authentication failure fails closed rather than opening a fresh session

#### Scenario: No credential reaches the journal
- **WHEN** a session runs a tool turn and refreshes an OAuth credential
- **THEN** no decrypted pack, session, operation, catalog or pin payload contains the API key, access token or refresh token

### Requirement: Six-table pack layout and lossless storage records
Forge SHALL create session-record, pin, catalog, pack, bitmap-membership and operation_pack tables as described in design D4, without per-object body/offset rows. JournalEngineState SHALL round-trip version and bytes opaquely; Forge SHALL implement every JournalStorage read method, ordered dirty paging and apply/compact. Packs SHALL use whole-pack zstd, kind-ordered canonical bodies and an encrypted prefix/varint offset directory, with full-id verification and no runtime schema-5 change.

#### Scenario: Batch admission and lookup after restart
- **WHEN** an apply admits a batch of new bodies and storage is reopened
- **THEN** the final new bodies occupy a compressed/sealed pack with bitmap memberships
- **AND** full-id lookup is rebuilt from authenticated packs without any per-object SQL index
- **AND** unknown opaque engine-state versions/bytes are returned unchanged

#### Scenario: Consistent read during repack
- **WHEN** another connection commits a pack rewrite and bitmap remap while an object batch is being read
- **THEN** the read observes a consistent completed transaction or retries
- **AND** a stale cached generation/slot cannot return different bytes or bypass membership

#### Scenario: Dirty paging
- **WHEN** dirty_sessions is called with a domain, after-session cursor and limit
- **THEN** it returns only that domain's dirty runtime session ids in ascending order strictly after the cursor
- **AND** a zero limit returns no sessions

### Requirement: Atomic fenced apply with deletion receipts
Every JournalWrite SHALL run in one sqlx BEGIN IMMEDIATE transaction on one connection, preserving the runtime's ordered cut points, all memberships, pin/catalog changes, replacement record and operation packs. SQL SHALL enforce both expect fields inside that transaction, including writes without record replacement; absence SHALL match only revision/epoch zero. A mismatch SHALL be a typed runtime state conflict with no effects; other ambiguous errors SHALL invalidate caches for reload.

Last-membership releases SHALL remove the body bytes from packs in that same transaction and return JournalApplyReceipt with the exact number and sum of original canonical body lengths physically removed, counting duplicate releases once. Clearing one shared membership, repacking survivors or reclaiming SQLite pages SHALL NOT be reported as body deletion.

#### Scenario: Failure before publication
- **WHEN** any pre-commit JournalCommitFailurePoint is injected
- **THEN** no pack, membership, pin/catalog, session record or operation change from that apply becomes visible

#### Scenario: Lost acknowledgement
- **WHEN** AfterCommit is injected and a fresh engine opens the file
- **THEN** the complete committed state is durable and visible
- **AND** retry within the retained replay window returns the original result without reapplying

#### Scenario: Stale SQL fence
- **WHEN** a writer or a record=None collection/pin write presents a stale revision or writer epoch
- **THEN** SQL refuses it and all staged changes roll back
- **AND** the refusal does not depend solely on a process cache or mutex

#### Scenario: Last membership in a partially live pack
- **WHEN** an apply releases the final global membership of one slot while other slots remain held
- **THEN** that body's bytes are purged by a transactional pack rewrite and all surviving bitmaps are remapped
- **AND** the receipt counts that body and its canonical length exactly once, even with S2 merging disabled

### Requirement: Engine replay window with packed original results
Forge SHALL store lossless operation records in binary-carry operation packs and let the engine select pruning only during collection. Records SHALL remain for revisions greater than current revision minus 64 and for every result in the current writer epoch; storage SHALL execute only the explicit engine pruning directive. Identical retained replays SHALL return the original head, different inputs SHALL conflict, and pruned replays SHALL conflict without applying again.

#### Scenario: Replay after head advance and writer change
- **WHEN** a retained operation is retried with the same input after a later publication or writer change
- **THEN** the engine returns its complete original result head from the operation packs
- **AND** no new revision or duplicate body is written

#### Scenario: Window boundary and current epoch exception
- **WHEN** collection prunes records outside the latest 64 revisions
- **THEN** records exactly at or below the cutoff from older epochs may be removed
- **AND** newer records and all current-epoch results survive, even if the epoch has more than 64 results
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
- **WHEN** the one new V{NNN} migration is applied
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
Forge SHALL run assert_journal_storage_conformance unedited over production ForgeJournalStorage with repacking off, on, and storage reopened per hydration, real sealing and file-backed restart. Forge SHALL also retain equivalence/import/crash coverage through JournalCommitFailurePoint::ALL and the real legacy decoder schema guard. All five upstream broken-storage fault shapes SHALL be tested over Forge-backed decorators and rejected at their named cases; private upstream test adapters SHALL NOT be assumed exported.

#### Scenario: All configurations and policies
- **WHEN** Forge's named storage conformance wrappers run
- **THEN** every primitive, protected, ordinary, arbitrary-projection, collection, unsupported-collection and restart assertion passes in all three storage configurations

#### Scenario: Broken storage is detected
- **WHEN** a Forge-backed decorator introduces a torn transaction, lost write, reordered publication, stale epoch or early shared-body deletion
- **THEN** conformance rejects it at atomic_apply_cuts, restart_acknowledged_state, restart_commit_sequence, writer_epoch_fence or shared_body_last_membership respectively
- **AND** a passing broken backend is a test failure
