## ADDED Requirements

### Requirement: Native journal persistence for persistent native sessions
Every native Agent session that runs with persistent context SHALL persist through the Agent Runtime `SessionJournal` capability.
- The Forge session store SHALL advertise an ordinary journal from `SessionStore::journal()` and a protected journal from `CheckpointStore::journal()`, so that every persistent session is a capable pair.
- Each runtime transition SHALL write only new immutable objects and changed metadata. It SHALL NOT rewrite the complete snapshot or checkpoint.
- Ephemeral scopes SHALL remain store-less.
- There SHALL be no configuration flag that selects the legacy path.

#### Scenario: Second turn writes only the delta
- **WHEN** a persistent native session completes a second one-message turn
- **THEN** that turn's commits store no message body that the first turn already stored
- **AND** both the ordinary and protected heads advance their revision

#### Scenario: Ephemeral scope
- **WHEN** a native turn runs in a scope without persistent context
- **THEN** no journal head, object or operation row is written for it

### Requirement: Identity projection over encrypted storage
The ordinary domain SHALL apply an identity projection. It SHALL hold the same exact snapshot content that Forge's ordinary snapshot store holds before this change. The protected domain SHALL hold the exact checkpoint state.
- Both domains SHALL be encrypted at rest with the existing protected-store key.
- Object, operation and pin identities SHALL be stored only as keyed HMACs. No plaintext content hash SHALL be written to disk.
- Each sealed body SHALL be bound to its storage key, so it cannot be moved to another row.
- Identical bodies MAY be stored once across both domains. An ordinary lease SHALL NOT read an object that is not a member of the ordinary domain and reachable from that lease's pinned roots.
- Provider credentials SHALL NOT appear in any stored body, head, operation record or pin record.

#### Scenario: Ordinary content equals today's snapshot
- **WHEN** a native session's ordinary head is materialized after a turn
- **THEN** it equals the snapshot the runtime presented to the store, including Sensitive extension state

#### Scenario: Ordinary lease cannot read a protected-only object
- **WHEN** an ordinary lease requests an object that only the protected domain references
- **THEN** the read is refused and no bytes are returned

#### Scenario: No credential reaches the journal
- **WHEN** a session runs a tool turn and its OAuth credential is refreshed
- **THEN** no decrypted journal body, head, operation result or pin record contains the access or refresh token

### Requirement: Atomic, fenced and idempotent commits
Each journal commit SHALL publish its bodies, memberships, operation record and head in one SQLite write transaction.
- Head publication SHALL require the expected `revision` and the current `writer_epoch`, and SHALL increment `revision`.
- Writer open SHALL increment the durable `writer_epoch`.
- A second writer open for a session that already has a live writer in the process SHALL fail with a state conflict before any provider request.
- A dropped or closed writer SHALL release its slot without a database write.
- A repeated operation with the same input SHALL return its original head, even after the head has advanced. The same operation with a different input SHALL conflict.
- Operation records SHALL be retained for the lifetime of the session row.

#### Scenario: Failure inside the commit transaction
- **WHEN** a commit fails after its bodies are inserted but before the head update
- **THEN** no body, membership, operation row or head change from that commit is visible

#### Scenario: Concurrent writer on one session
- **WHEN** a topic fork tries to open the writer of a session whose turn is running
- **THEN** the fork fails with a conflict before any provider request
- **AND** the running turn's commits are unaffected

#### Scenario: Lost acknowledgement
- **WHEN** a commit lands but its acknowledgement is lost, and the runtime retries the same operation after a later commit
- **THEN** the store returns the original head and writes nothing

### Requirement: One-time import of existing sessions
A session with legacy state in `protected_agent_session_state` SHALL be imported through the runtime's v3 import on its first native writer open.
- Import SHALL publish the protected head before the ordinary head.
- Import SHALL make no provider or tool call. Unfinished turns SHALL keep their recovery barriers.
- Forge SHALL NOT write the legacy row during or after import. The legacy store's save methods SHALL refuse with a conflict.
- Once both native heads exist, opening the session SHALL NOT decrypt the legacy row again.
- Older builds SHALL refuse the migrated database through the existing newer-database guard.

#### Scenario: Mid-turn checkpoint imports without replay
- **WHEN** a session whose legacy checkpoint is at CallingModel is opened after the upgrade
- **THEN** its exact request is preserved in the protected head and resume follows the existing no-replay rules
- **AND** the provider receives no request during import

#### Scenario: Crash during import
- **WHEN** the process stops after the protected head is published but before the ordinary head
- **THEN** the next open repairs the ordinary head once and does not import again
- **AND** the legacy row is unchanged

#### Scenario: Older build after upgrade
- **WHEN** an older Forge build opens a database migrated by this change
- **THEN** startup fails with the newer-database error and no session is resumed from legacy rows

### Requirement: Host probes read the journal
Session existence, topic-rotation hand-over and Agent continuity `checkpoint_present` SHALL be determined from journal head metadata. The legacy payload columns SHALL be checked only for sessions not yet imported. The per-turn existence probe SHALL NOT decrypt or decode session content.

#### Scenario: Native-only session reports a checkpoint
- **WHEN** a session created after the upgrade has a protected head that carries a checkpoint
- **THEN** its Agent detail reports `checkpoint_present = true`

#### Scenario: Hand-over after a native successor save
- **WHEN** a topic rotation is abandoned after its successor's native head was published
- **THEN** the rotation hands over to the successor instead of aborting the fork

### Requirement: Runtime journal conformance in Forge's test suite
Forge's test suite SHALL run, against the production SQLite adapter, the Agent Runtime protected, ordinary and collection conformance suites, together with the materialized-checkpoint schema gate that uses Forge's legacy decoder. Each suite SHALL run unedited.

#### Scenario: Ordinary suite with a redacting policy
- **WHEN** the ordinary conformance suite supplies a redacting projection policy to the adapter factory
- **THEN** the adapter stores only projected objects, refuses rejected projections without writes, and passes every assertion
