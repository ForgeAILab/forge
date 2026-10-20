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
Forge SHALL schedule engine collect calls for dirty sessions in both domains, using clones of the same long-lived engine per storage/domain as every runtime, with an object bound and scheduling time budget, initially 20,000 membership releases per call and 10 seconds between budget checks. The engine SHALL own root epochs, closures, liveness, pin/dependency handling, catalog release and replay-pruning selection; storage SHALL execute atomic release/physical-receipt writes and report retained canonical bytes including deferred bodies. max_objects SHALL bound membership releases, not promise an equal bound on previously deferred physical removals. No TTL or age rule SHALL expire live or retained roots.

#### Scenario: Live superseded checkpoints
- **WHEN** a live session has superseded checkpoints/cache/manifest paths and the worker collects it
- **THEN** the engine releases only unreachable memberships
- **AND** full history, the latest exact checkpoint, retained roots and pinned objects remain readable

#### Scenario: Epoch conflict
- **WHEN** roots or pins change before a scheduled collection's expected epoch is accepted
- **THEN** the engine refuses the stale request without deletion and Forge retries later

#### Scenario: Independent copy remains referenced
- **WHEN** ordinary collection releases its copy of an id also held by protected or another session
- **THEN** the other scope's independent copy remains readable
- **AND** the ordinary receipt counts only its own physically removed copies at removal time; below-threshold deferred bytes remain in retained_bytes and count zero until purged

### Requirement: Physical pack maintenance preserves membership
Forge SHALL trigger S2 geometric-tier merging after successful terminal publication and base-pack repacking during per-turn collection. Each pack SHALL belong to one domain/runtime-session scope; maintenance SHALL remap only that scope's bitmap atomically without changing runtime objects, heads, membership or replay policy, and failure SHALL be retryable without undoing an acknowledged turn. Dead bodies SHALL become unreadable on release; sealed packs MAY defer their removal below 25% dead canonical body bytes. The apply reaching 25%, an empty scope and retired-session collection SHALL force purges even when optional merging is disabled. Receipts SHALL count physical copies and canonical encoded bytes only when removed, including earlier deferred bodies. Imports, admissions, purge rewrites and all repacks SHALL obey the 1 MiB canonical body cap; an oversized object SHALL be rejected atomically. Serialized operation packs SHALL also be capped at 1 MiB. Base repacking SHALL select only fully live packs below 64 KiB or fully live survivor packs purged by that apply; terminal equal-tier pairs SHALL fit the 1 MiB cap. Stable large and deferred-dead packs SHALL stay sealed. `compact` SHALL use bounded per-session transactions, move only eligible fully live bodies so no dead removal escapes apply receipts, and SHALL NOT run a full VACUUM.

#### Scenario: Deferred release and bounded threshold purge in a long session
- **WHEN** collection after a turn releases superseded bodies in a 4000-message session
- **THEN** touched packs below 25% dead bytes remain byte-identical but released bodies cannot be read, while packs reaching 25% are purged in the apply
- **AND** fully live packs below 64 KiB or just-purged survivor packs may be folded into capped base output
- **AND** stable large packs with no purge and deferred-dead packs keep their byte-identical ciphertext

#### Scenario: Retirement drains deferred bytes
- **WHEN** a retired session's collection finds packs below 25% dead canonical bytes, including dead slots from earlier applies
- **THEN** it purges every dead slot without requiring a new release id
- **AND** receipts and retained-byte differences count the physical removal once, while held roots remain readable

#### Scenario: Terminal hook and collection base pack
- **WHEN** terminal publication succeeds and equal-size pack tiers exist
- **THEN** the async terminal hook merges them geometrically with fresh AEAD nonces
- **AND** subsequent collection can form capped kind-ordered output from eligible fully live inputs in that same domain/session only

#### Scenario: Physical compaction
- **WHEN** compact repacks live bodies or reclaims free SQLite pages
- **THEN** it changes no logical membership, root epoch or operation-retention decision
- **AND** no physical page saving is misreported as JournalApplyReceipt canonical body bytes

### Requirement: Deletion cascades and bounded owned-pack cleanup
Deleting an agent_session SHALL cascade its journal session, pin, catalog, membership and operation-pack rows in the deletion transaction. Pack rows SHALL retain indexed owner labels without a cascading owner FK so post-commit cleanup can delete all of the deleted session's packs, including below-threshold deferred bytes, in bounded transactions of at most 32 packs or 8 MiB retained canonical bytes, scanning candidates in pages of 5,000 pack ids. Cleanup SHALL recheck owner/membership under the write lock, resume after restart until drained, and preserve other sessions' independent copies. Removal outside apply SHALL be observed separately, not fabricated as a JournalApplyReceipt. Retirement SHALL retain its tombstone and any still-required metadata/operation results until session deletion, without promising zero physical bytes.

#### Scenario: Project deletion with identical bodies in another session
- **WHEN** a Project is deleted and another session holds identical ids in independent packs
- **THEN** deleted sessions' metadata/membership rows disappear on commit
- **AND** bounded cleanup drains all deleted-owner packs regardless of dead-byte share, without decrypting them, and the other session still opens after restart

### Requirement: Forge physical disk gate against real legacy L2
Forge SHALL run fixture B at N=4000, plain and tool, through production ForgeJournalStorage with sqlx, real AEAD, PerSession/independent-domain copies, S1–S5 and collection, and compare each physical retained size with the same fixture's actual Forge legacy idle row containing two sealed blobs (L2). Both post-collection ratios SHALL be at most 2.0×; 1.5× is an aim. Runtime rt-e7's measured PerSession S1–S5 numbers, which share bodies across a session's domains, SHALL NOT substitute for this Forge gate, and runtime schema 5 SHALL remain out of scope.

The gate ratios SHALL be taken after the production cadence (S2 hook and collection after every turn plus final shutdown sweep). The report SHALL include cumulative committed object/operation pack payload+nonce writes and largest single collection-call write, matching rt-e7's counter units, plus separate WAL write bytes against legacy, largest single-pack rewrite, lazy session hydration time and bounded scoped-directory/body-cache charge with unrelated sessions present, memo charge/resident-memory/hit/eviction/oversize evidence and the longest collect gate hold. Retained/live/deferred canonical bytes and cumulative physical receipts SHALL remain distinct from file savings. For each layout physical bytes SHALL be page_count×page_size after truncated WAL checkpoints and VACUUM, minus its matching empty-schema/session-setup baseline, verified against file/dbstat totals. Reports SHALL include all metadata/index/crypto costs, A/B at N=100/1000/4000, grown C including supplemental N=4000 with cadence labeled, before/after collection/retirement, L1 diagnostics and the temporary footprint of retained lazy-import legacy rows. L1, small-N and A/C SHALL not replace the settled B/N=4000 L2 gate.

#### Scenario: Forge gate passes
- **WHEN** B/N=4000 plain and tool both measure at or below 2.0× their real legacy L2 after collection
- **THEN** the disk gate passes and the report separately records whether each meets the 1.5× aim
- **AND** it identifies actual Forge results separately from rt-e7's measured 1.60542× plain/1.79118× tool S1–S5 results under both collect-at-end and per-turn schedules, each missing the optional 1.5× aim

#### Scenario: Gate miss or temporary import overlap
- **WHEN** a Forge ratio exceeds 2.0× or lazy import retains both old and new payloads
- **THEN** a gate miss blocks adoption pending a storage-only fix and the old-plus-new footprint is reported separately
- **AND** no seed, projection, denominator or threshold is silently changed to manufacture a pass

#### Scenario: S5 evidence
- **WHEN** grown fixture C is measured after engine-directed pruning
- **THEN** the report records actual retained operation tiers and replay-boundary/current-epoch behavior
- **AND** rt-e7's per-turn C/N=1000 1.27826× and collect-at-end C/N=4000 1.22863× L2 numbers remain reference evidence rather than measured Forge outcomes
