## ADDED Requirements

### Requirement: Retirement of sessions Forge will never resume
Forge SHALL retire the journal of a native session only when the session has been replaced. A session is replaced in two cases: a successful topic rotation that recorded its successor, or a profile change that created a new runtime session.
- Retirement SHALL be preceded by a host grant bound to the head's current revision.
- Retirement SHALL be refused while the session has a live writer or an unresolved transfer.
- Retirement SHALL retire the protected domain before the ordinary domain.
- After retirement, read-only and writer latest-root opens SHALL fail with a state conflict, including after collection.
- Forge SHALL NOT retire a session when its Task completes.

#### Scenario: Topic rotation source is retired
- **WHEN** a topic rotation completes and marks its source session `replaced`
- **THEN** within one retention tick both domains of the source are retired
- **AND** a later open of the source session fails with a conflict instead of starting fresh

#### Scenario: Task completion keeps the session
- **WHEN** a Task whose native session has a journal head reaches `done`
- **THEN** the session's heads remain published and unretired

#### Scenario: Crash between domain retirements
- **WHEN** the process stops after retiring the protected domain and before retiring the ordinary domain
- **THEN** the next retention tick retires the ordinary domain

### Requirement: Explicit, bounded collection without expiry
Forge SHALL delete journal objects only through collection. Collection SHALL delete objects that no live root, retained root, reader pin, transfer pin or staging pin can reach.
- Collection SHALL run from the retention worker, for sessions whose roots or pins changed since their last sweep.
- Each run SHALL be bounded by an object count and a time budget.
- A body SHALL be deleted only when no domain membership references it, in the same transaction that removes its last membership.
- There SHALL be no retention TTL. Collection SHALL NOT expire live or retained roots.
- Physical compaction SHALL be separate from collection, and SHALL never remove unreachable objects.

#### Scenario: Superseded checkpoint state is reclaimed
- **WHEN** a live session has published several CallingModel checkpoints and the retention worker sweeps it
- **THEN** objects reachable only from superseded checkpoints are deleted
- **AND** the session still materializes its full history and latest checkpoint

#### Scenario: Shared body survives partial deletion
- **WHEN** a body is a member of both domains and only the ordinary membership becomes unreachable
- **THEN** collection removes the ordinary membership and keeps the body

### Requirement: Deletion cascades remove journal state
Deleting an `agent_session` row SHALL remove its journal heads, memberships, operation records and pin records in the same transaction, through foreign-key cascades. This covers Project deletion and any other cascade. The retention worker SHALL then delete bodies that no longer have any membership, in bounded batches.

#### Scenario: Project deletion
- **WHEN** a Project with native chat and Task sessions is deleted
- **THEN** no journal head, membership, operation or record row of those sessions remains when the deletion commits
- **AND** after one retention tick, no body that only those sessions referenced remains
