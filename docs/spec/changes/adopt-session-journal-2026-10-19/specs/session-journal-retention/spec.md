## ADDED Requirements

### Requirement: Retirement of replaced and unresumable sessions
Forge SHALL retire replaced topic/profile-rotation sources and suspended native sessions it can no longer resume: non-Agent-Chat scopes after restart and Agent Chat rows that are not their scope's dormant session. Forge SHALL preserve resumable dormant chats and reusable Task sessions; Task completion alone SHALL NOT retire a session. The dormant session SHALL be decided per identity, context scope and profile by the same query Forge uses to resume, so each profile keeps its own dormant chat. A session undecryptable after a key change SHALL NOT be retired, released or deleted because of that failure. Engine authorization SHALL bind the current revision and refuse live writers or unresolved transfer/dependency state; protected retirement SHALL precede ordinary retirement.

#### Scenario: Topic rotation source
- **WHEN** a completed rotation records its published successor and marks the source replaced
- **THEN** the retention worker retires both source domains when the engine accepts their grants
- **AND** the successor and chat transcript remain intact

#### Scenario: Restart distinguishes resumable and dead sessions
- **WHEN** recovery suspends native sessions
- **THEN** the worker retires non-Agent-Chat sessions and Agent Chat rows other than their scope's dormant session
- **AND** it preserves resumable dormant chats and does not retire an active writer
- **AND** it does not eagerly import legacy-only dead sessions solely for retirement; their payload cleanup stays in the next release

#### Scenario: Dormant chats for two profiles in one scope
- **WHEN** an Agent Chat scope has suspended native sessions for two profiles after a restart
- **THEN** the newest unreplaced suspended session of each profile is kept as that profile's dormant session
- **AND** only older suspended rows of each profile are retired

#### Scenario: Task completion
- **WHEN** a Task reaches done and its session remains reusable
- **THEN** the journal heads remain published and unretired

#### Scenario: Permanent tombstone and partial retirement
- **WHEN** the process stops between protected and ordinary retirement
- **THEN** the next tick retries the remaining domain without resurrecting the retired one
- **AND** latest-root opens of a retired domain conflict after collection/restart despite a surviving legacy row

### Requirement: Runtime-owned bounded collection without expiry
Forge SHALL schedule engine collect calls for dirty sessions in both domains, with an object bound and scheduling time budget, initially 20,000 objects per call and 10 seconds between budget checks. The engine SHALL own root epochs, closures, liveness, pin/dependency handling, catalog release and replay-pruning selection; storage SHALL execute atomic release/receipt writes. No TTL or age rule SHALL expire live or retained roots.

#### Scenario: Live superseded checkpoints
- **WHEN** a live session has superseded checkpoints/cache/manifest paths and the worker collects it
- **THEN** the engine releases only unreachable memberships
- **AND** full history, the latest exact checkpoint, retained roots and pinned objects remain readable

#### Scenario: Epoch conflict
- **WHEN** roots or pins change before a scheduled collection's expected epoch is accepted
- **THEN** the engine refuses the stale request without deletion and Forge retries later

#### Scenario: Shared body remains referenced
- **WHEN** ordinary collection releases a slot still held by protected or another session
- **THEN** the body remains readable by that holder and the release receipt counts zero for it

### Requirement: Physical pack maintenance preserves membership
Forge SHALL trigger S2 geometric-tier merging after successful terminal publication and base-pack repacking during collection. Physical maintenance SHALL atomically remap every holder's bitmap, including other domains/sessions, without changing runtime objects, heads, membership or replay policy; failure SHALL be retryable without undoing an acknowledged turn. Mandatory last-membership purges SHALL occur in apply even when optional geometric/base merging is disabled for conformance. Packs SHALL be capped in logical size so that a purge or base repack rewrites only the capped packs it touches, never a whole session; `compact` SHALL run in bounded per-session transactions and SHALL NOT run a full VACUUM.

#### Scenario: Purge in a long session
- **WHEN** collection after a turn releases one superseded checkpoint body of a 4000-message session
- **THEN** the capped pack holding that body is rewritten without it, and any small geometric tiers may be folded into capped packs
- **AND** every other capped pack with no released slot keeps its byte-identical ciphertext

#### Scenario: Terminal hook and collection base pack
- **WHEN** terminal publication succeeds and equal-size pack tiers exist
- **THEN** the async terminal hook merges them geometrically with fresh AEAD nonces
- **AND** subsequent collection can form a kind-ordered base pack while preserving every surviving holder

#### Scenario: Physical compaction
- **WHEN** compact repacks live bodies or reclaims free SQLite pages
- **THEN** it changes no logical membership, root epoch or operation-retention decision
- **AND** no physical page saving is misreported as JournalApplyReceipt canonical body bytes

### Requirement: Deletion cascades preserve shared holders
Deleting an agent_session SHALL cascade its journal session, pin, catalog, membership and operation-pack rows in the deletion transaction. Pack owner labels SHALL NOT cascade shared bodies; bounded post-deletion cleanup SHALL purge unheld slots and empty packs while preserving other holders. Retirement SHALL retain its tombstone and any still-required metadata/operation results until session deletion, without promising zero physical bytes.

#### Scenario: Project deletion with a shared pack
- **WHEN** a Project is deleted and one affected pack also has a surviving session holder
- **THEN** deleted sessions' metadata/membership rows disappear on commit
- **AND** bounded cleanup purges only unheld slots and the survivor still opens after restart

### Requirement: Forge physical disk gate against real legacy L2
Forge SHALL run fixture B at N=4000, plain and tool, through production ForgeJournalStorage with sqlx, real AEAD, S1–S5 and collection, and compare each physical retained size with the same fixture's actual Forge legacy idle row containing two sealed blobs (L2). Both post-collection ratios SHALL be at most 2.0×; 1.5× is an aim. Runtime reference S1–S4 measurements and S5 projections SHALL NOT substitute for this Forge gate, and runtime schema 5 SHALL remain out of scope.

The gate ratios SHALL be taken after the production cadence (S2 hook and collection after every turn), and the report SHALL include cumulative write bytes against legacy, storage open time, global prefix-index memory and the longest collect gate hold. For each layout physical bytes SHALL be page_count×page_size after truncated WAL checkpoints and VACUUM, minus its matching empty-schema/session-setup baseline, verified against file/dbstat totals. Reports SHALL include all metadata/index/crypto costs, A/B at N=100/1000/4000, grown C, before/after collection/retirement, L1 diagnostics and the temporary footprint of retained lazy-import legacy rows. L1, small-N and A/C SHALL not replace the settled B/N=4000 L2 gate.

#### Scenario: Forge gate passes
- **WHEN** B/N=4000 plain and tool both measure at or below 2.0× their real legacy L2 after collection
- **THEN** the disk gate passes and the report separately records whether each meets the 1.5× aim
- **AND** it identifies actual Forge results separately from the reference's 1.49096× plain/1.62353× tool S1–S4 results

#### Scenario: Gate miss or temporary import overlap
- **WHEN** a Forge ratio exceeds 2.0× or lazy import retains both old and new payloads
- **THEN** a gate miss blocks adoption pending a storage-only fix and the old-plus-new footprint is reported separately
- **AND** no seed, projection, denominator or threshold is silently changed to manufacture a pass

#### Scenario: S5 evidence
- **WHEN** grown fixture C is measured after engine-directed pruning
- **THEN** the report records actual retained operation tiers and replay-boundary/current-epoch behavior
- **AND** projected S5 numbers from the reference are not labeled measured Forge outcomes
