## ADDED Requirements

### Requirement: Consume the released storage-generic runtime engine
Forge SHALL implement `JournalStorage` as `ForgeJournalStorage` and use the runtime's `JournalEngine<ForgeJournalStorage>` for protected and ordinary journal semantics. Adoption SHALL start by pinning all runtime packages and testkit to the same immutable revision of the released agent-runtime engine (`v0.9.0`, `8c19be5b226c2fdaeb6708ec5d39e91c235857a5`), including amended D2 rules 1–7, `JournalBodySharing::PerSession`, physical-copy `JournalApplyReceipt`, `retained_bytes`, the N1/N2 fixes, D-1's reader-operation authority check, the corrected futures mutex gate, native-first open, bounded extension reads and engine-owned replay pruning. Forge SHALL NOT copy the runtime's validation, closure, lease, dependency, replay or collection algorithms.

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

#### Scenario: Terminal failure and cancellation cleanup
- **WHEN** failed acceptance is unresolved and shutdown or persist returns the runtime's typed fencing conflict for a failed or cancelled turn
- **THEN** Forge closes leases, removes its active registration and logs cleanup as secondary
- **AND** the original terminal classification/message and per-turn usage envelope survive; the next request opens a fresh handle
- **AND** genuine primary persistence failures on otherwise successful turns remain errors

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
- **AND** pack units bind unit kind, pack id, generation, runtime session and Forge owner label; metadata units bind their full domain/runtime-session key; distinct HKDF-derived AEAD and lookup keys are used

#### Scenario: No credential reaches the journal
- **WHEN** a session runs a tool turn and refreshes an OAuth credential
- **THEN** no decrypted pack, session, operation, catalog or pin payload contains the API key, access token or refresh token

### Requirement: Six-table pack layout and lossless storage records
Forge SHALL create session-record, pin, catalog, pack, bitmap-membership and operation_pack tables as described in design D4, without per-object body/offset rows. Per-session metadata/membership rows SHALL be keyed by the runtime session id the trait receives and SHALL carry an indexed `forge_session_id` foreign key that cascades from `agent_session`; an apply whose runtime id maps to no or several `agent_session` rows SHALL fail with a typed host error and write nothing. Packs SHALL have strict runtime-session ownership, per-domain membership bitmaps and an indexed Forge owner label without a cascading owner FK, permitting bounded deletion cleanup. JournalEngineState SHALL round-trip version and bytes opaquely; Forge SHALL implement every JournalStorage read method, ordered dirty paging and apply/compact. Each pack SHALL hold a separately sealed prefix/varint offset index unit and a sealed zstd body unit of kind-ordered canonical bodies, with full-id verification and no runtime schema-5 change. Storage SHALL declare `JournalBodySharing::PerSession`, deduplicate only within one runtime session, store an identical immutable body once for both domains of that session with independent memberships, and keep independent copies across sessions. A domain SHALL gain membership of a body only by admitting its complete, byte-identical canonical bytes. Storage open SHALL decode no pack units; lookup directories SHALL load lazily for the requested session only. Pack ids SHALL never be reused.

#### Scenario: Batch admission and lookup after restart
- **WHEN** an apply admits a batch of new bodies and storage is reopened
- **THEN** the final new bodies occupy a compressed/sealed pack with bitmap memberships
- **AND** only the requested session's lookup directory is rebuilt lazily from its sealed index units, with candidate body units opened as needed for full-id verification
- **AND** unrelated sessions' packs are not decoded or searched for admission
- **AND** unknown opaque engine-state versions/bytes are returned unchanged

#### Scenario: Independent session copies
- **WHEN** a session admits a body stored only in another session, including after cache eviction or storage reopen
- **THEN** it writes its own physical copy without looking up the other session's body
- **AND** reclaiming that copy does not wait for the other session's membership or affect its readability

#### Scenario: Shared copy between a session's domains
- **WHEN** the ordinary domain admits a body byte-identical to one the protected domain of the same session already stores
- **THEN** no second physical copy is written and the ordinary domain gains its own membership bit on the existing slot
- **AND** releasing it in one domain leaves it readable to the other domain with a zero receipt, and it is removed and counted once only after both domains release it
- **AND** a body only the protected domain admitted is never readable through the ordinary domain, including after purge rewrites and merges
- **AND** a corrupt shared pack fails closed for that session only

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
- **AND** discovery uses the clear dirty index without authenticating records, so a corrupt session remains discoverable for engine isolation

### Requirement: Atomic fenced apply with deletion receipts
Every JournalWrite SHALL run in one sqlx BEGIN IMMEDIATE transaction on one connection, preserving the runtime's ordered cut points, all memberships, pin/catalog changes, replacement record and operation packs. SQL SHALL enforce all three `JournalFence` fields (revision, writer_epoch, generation) inside that transaction for every write, including controls preserving revision/epoch; absence SHALL match only the all-zero fence. Every write SHALL carry a required replacement session record with generation exactly expect.generation + 1; overflow or a non-successor generation SHALL conflict atomically. The session generation SHALL be persisted independently of pack generations. A mismatch SHALL be a typed runtime state conflict with no effects; other ambiguous errors SHALL invalidate caches for reload. Every ambiguous COMMIT/lost acknowledgement SHALL be retryable, as SHALL SQLx SQLite BUSY/LOCKED (including extended codes) and infrastructure availability failures. A non-retryable apply error SHALL leave no durable effect.

Releases SHALL end membership read authority immediately. A sealed pack MAY retain dead bodies below 25% of its physically retained canonical body bytes, never serving them; the apply reaching 25% or more SHALL purge all its dead slots transactionally. A slot SHALL be dead only when no domain of its session holds it. A domain releasing its last member in a session and collection of a retired session SHALL purge all of that session's dead bytes, including earlier deferred bytes without new release ids. Purges SHALL run even with optional S2 merging disabled. JournalApplyReceipt SHALL count physical body copies and their original canonical encoded lengths before compression/sealing exactly once at removal time, including previously deferred bodies. Clearing bits, moving live bodies or reclaiming SQLite pages SHALL count zero. Forge SHALL return `Some` from `retained_bytes(domain)` with canonical bytes physically retained in the packs of the sessions that domain participates in, including deferred dead bodies and copies shared with the other domain; this diagnostic SHALL NOT control liveness or stand for file size. When a session is unreadable, diagnostics SHALL skip and log that session; byte_stats SHALL expose the skipped-session count and readable-only byte totals rather than pretending to report a complete domain size.

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
- **AND** the removing apply returns their physical-copy receipt while slots another domain still holds, and other sessions' copies, stay readable

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

### Requirement: Background conversion and fallback import with exact-first authority
The additive migration SHALL preserve existing data and leave protected_agent_session_state readable. Background conversion and fallback native writer open SHALL use the same runtime v3 importer, shared engines and validation with no provider/tool I/O; where a checkpoint exists, protected exact publication SHALL precede ordinary publication. Native-first open SHALL avoid unnecessary legacy decoding, both legacy saves SHALL refuse writes, and the legacy table SHALL be removed only by a separate next-release cleanup after resumable rows are imported and dead rows handled under the approved retirement rule.

#### Scenario: Background conversion precedes first open
- **WHEN** a dormant native host session still has a legacy row and lacks native publication
- **THEN** the supervised worker converts it without user open and the next open reads native state without importing
- **AND** the legacy row remains unchanged until next-release cleanup

#### Scenario: Mid-turn import
- **WHEN** a legacy checkpoint at CallingModel or a persisted model/tool result is imported
- **THEN** exact requests/results and existing approval/interaction/no-replay barriers survive
- **AND** the legacy row stays byte-for-byte unchanged with no model or tool call

#### Scenario: Partial import
- **WHEN** the process stops after protected import publication and before ordinary publication
- **THEN** the next open repairs ordinary publication from matching unchanged import evidence once
- **AND** it never overlays stale legacy data onto an advanced native head

#### Scenario: Pre-topic checkpoint-only LCM
- **WHEN** an older legacy snapshot omits LCM state and its checkpoint references an unretired owner-less timeline for the same identity and scope
- **THEN** background conversion and lazy resume resolve that checkpoint timeline and use the same runtime importer validation
- **AND** older tuning revisions and absent or stale Forge policy markers preserve the validated LCM state, canonical history and transcript
- **AND** live resume delegates tuning metadata rebuild to the runtime; malformed identity, binding or frontier evidence remains a conflict

#### Scenario: Retry after a failed LCM import
- **WHEN** a failed import left unpublished writer reservations and an untouched empty replacement LCM binding
- **THEN** retry may rebind to the session's own persisted timeline only while the replacement is unclaimed, revision zero, contains no entries, nodes or operations, and neither native domain has published or retired
- **AND** the binding repair checks those conditions in one write transaction, preserves the original timeline and legacy payloads, and leaves a populated or differently owned binding in conflict

#### Scenario: Shared pre-topic Chat timeline owned by a superseded sibling
- **WHEN** the Chat's resume target references a shared pre-topic timeline owned by an older, unlinked suspended native sibling of the same identity, context scope and Profile
- **AND** the timeline predates the target and was updated after its creation, neither runtime has published or retired native state, and no claim, lease, topic/rotation, competing resumable session or additional reference makes ownership ambiguous
- **THEN** conversion and lazy resume transfer the original timeline's runtime ownership and binding to the resume target in the same immediate transaction
- **AND** that transaction marks the older session replaced by the target with its version fence, preserving its legacy payload and making it retire-eligible even after the target activates
- **AND** no timeline, entry, node or operation is duplicated or discarded; retries are idempotent and a failed binding write rolls back the whole transfer
- **AND** an active owner, another resumable Profile or tied newest session, native publication/retirement, or additional ownership evidence keeps the target parked with a precise Conflict reason

#### Scenario: Transient LCM binding failure
- **WHEN** LCM binding encounters a retryable database failure, including SQLite BUSY/LOCKED primary or extended codes
- **THEN** conversion leaves the session eligible for a later sweep of the same worker and excludes it from parked conflicts

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
Forge SHALL run assert_journal_storage_conformance unedited over production ForgeJournalStorage with repacking off, on, and storage reopened per hydration, real sealing and file-backed restart. Every configuration SHALL declare PerSession with within-session domain sharing and retain mandatory threshold/retirement purges. Scope-aware receipts, remaining-holder readability, non-member isolation and bounded deferred deletion/retained-byte evidence SHALL all pass. Forge SHALL also retain equivalence/import/crash coverage through JournalCommitFailurePoint::ALL and the real legacy decoder schema guard. All seven upstream broken-storage fault shapes SHALL be tested over Forge-backed decorators and rejected at their named cases; private upstream test adapters SHALL NOT be assumed exported.

#### Scenario: All configurations and policies
- **WHEN** Forge's named storage conformance wrappers run
- **THEN** every primitive, protected, ordinary, arbitrary-projection, collection, unsupported-collection and restart assertion passes in all three storage configurations

#### Scenario: Broken storage is detected
- **WHEN** a Forge-backed decorator introduces a torn transaction, lost write, reordered publication, stale epoch, ignored control generation, premature deletion of a surviving holder's copy, or serving released/deferred bytes
- **THEN** conformance rejects it at atomic_apply_cuts, restart_acknowledged_state, restart_commit_sequence, writer_epoch_fence, control_generation_fence, shared_body_last_membership, or membership_isolation_and_receipts/bounded_deferred_deletion respectively
- **AND** a passing broken backend is a test failure

### Requirement: Runtime speed-work bounded storage protocol
Forge SHALL consume the owner-authorized interim runtime revision b0a8c90e96ccb85e4642a119117a8fd5d3a06993 (still versioned 0.9.0), with the final pin targeting the v0.10.0 tag. ForgeJournalStorage SHALL implement all four required collection methods with bounded SQL member/pin pages, full-fence and physical-layout continuation checks, read-only no-effect preflight, and complete atomic collection applies rechecked under the write lock. The additive paging migration SHALL preserve sealed user data, memberships and opaque runtime metadata. Forge SHALL NOT replace collection pages with whole-session result slicing or declare production collection unsupported.

#### Scenario: Physical repack between member pages
- **WHEN** S2 or compact changes a session's pack layout without changing its logical fence
- **THEN** a continuation from the old layout conflicts instead of silently skipping or duplicating a member
- **AND** a fresh page sequence enumerates exact membership, with at most one capped pack decoded per page

#### Scenario: Collection plan exceeds physical allowance
- **WHEN** an engine-selected release requires more physical units than the plan permits
- **THEN** preflight requests a smaller release without changing durable state
- **AND** apply independently rechecks the bound under BEGIN IMMEDIATE and applies the complete accepted batch atomically

#### Scenario: Old encrypted pin metadata
- **WHEN** a pre-upgrade scope first loads after the paging migration
- **THEN** authenticated pin ids populate only relative ordering ranks outside collection
- **AND** bounded pin pages preserve pin-id order without changing ciphertext or exposing clear pin ids

### Requirement: Incremental ordinary projection preserves the approved policy
ForgeJournalProjection SHALL opt into the runtime incremental projection protocol with a stable policy revision and SHALL return deltas equivalent to complete conservative D3 projection. It SHALL preserve previously projected positions, history, usage, manifests, restore-critical extensions and runtime-owned structured references. A Sensitive replacement in the optional forge.diagnostics. namespace SHALL become a removal, including when replacing a formerly visible value. Bootstrap, unknown revisions and opaque diagnostic references SHALL use complete projection rather than assert unproven equivalence.

#### Scenario: Sensitivity changes at an existing diagnostic
- **WHEN** an update replaces a RedactionSafe optional diagnostic with a Sensitive value
- **THEN** incremental projection removes that namespace rather than retaining the formerly visible value
- **AND** applying the projected delta equals applying the complete policy to the reconstructed candidate

#### Scenario: Warm ordinary persistence
- **WHEN** the current projected view and its encoding carry the same policy revision
- **THEN** Forge projects only changed optional diagnostic updates through project_delta
- **AND** it does not force the runtime's complete-projection fallback at every boundary


### Requirement: Selected-Profile ownership of shared pre-topic LCM
Arbitration SHALL follow Forge's selected-Profile Chat resume choice when that session's own saved state references the shared pre-topic timeline. It SHALL preserve every foreground resume that succeeded before this branch. Other-Profile siblings SHALL remain resumable and unreplaced; a durable version-fenced receipt SHALL authorize omitting only the recorded timeline's derived state from otherwise exact import.

#### Scenario: Shared timeline assignment and sibling receipts
- **WHEN** the selected resume target references unowned T or T owned by an eligible legacy sibling in the same identity/context scope
- **THEN** ownership, binding and version-fenced receipts commit in one immediate transaction before another sibling publishes
- **AND** every referencing sibling receives a receipt regardless of status, age or replacement links; only older, unlinked, suspended same-Profile native siblings become replaced
- **AND** a replaced predecessor in the same scope is accepted, topics/rotations do not block, and native-state refusal is limited to siblings owning, binding or referencing T
- **AND** conflicting claims/bindings, unsafe ownership moves and version races retain their fences; failed writes roll back every ownership effect

#### Scenario: Foreground resume wins over arbitration guards
- **WHEN** Forge resumes a session referencing unowned T before background conversion or while a sibling converts
- **THEN** arbitration retries contention briefly and otherwise falls back to the pre-branch adoption behavior without returning an arbitration Conflict
- **AND** partial arbitration effects roll back before adoption, and the full original transcript resumes with zero provider calls
- **AND** an existing owner can resume through its own binding while receipt work is deferred

#### Scenario: Own canonical timeline while another Profile is selected
- **WHEN** a session already owns and binds T and the selected Profile uses a topic timeline or has no session in this chat
- **THEN** background conversion preserves that session's binding and imports normally without routing it through the unrelated selected Profile

#### Scenario: Selected target already owns the timeline
- **WHEN** the selected target already owns and binds T and its unpublished or published saved state references T
- **THEN** the same admissions, writer checks and version fences authorize a receipt-only transaction
- **AND** siblings convert and resume after a Profile switch back without moving T or republishing the target's state

#### Scenario: Resume a Profile that relinquished shared pre-topic LCM
- **WHEN** a retained session resumes after relinquishing T
- **THEN** each legacy copy referencing receipted T omits its LCM and semantic-summary namespaces and preserves its transcript and other protected state exactly
- **AND** a copy without LCM state keeps its semantic summary; original ciphertext remains unchanged
- **AND** a fresh timeline rebuilds missing derived state, receipts survive deletion of T, and unrecorded binding mismatches still fail closed

#### Scenario: Transient and unreadable siblings
- **WHEN** the selected target is active, a sibling has foreground/conversion admission, or a journal writer is present
- **THEN** background conversion remains eligible for another sweep with the same worker cursor
- **AND** a corrupt sibling's decode error is attributed to that sibling, which can be parked as unreadable without blocking target conversion or resume

#### Scenario: Shared timeline conversion is independent of cursor order
- **WHEN** conversion reaches any eligible legacy sibling before or after the selected resume target
- **THEN** both orders and fresh-cursor retries preserve the same owner, exact sibling imports, unchanged ciphertext and original LCM history, without duplicate timelines, receipts or publications
