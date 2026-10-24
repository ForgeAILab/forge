---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-24T07:55:11Z
---

# Design: Forge adopts JournalEngine with an encrypted session pack store

Forge baseline: branch `spec/adopt-session-journal`, rebased onto `next/v0.14`
at `07937e73`. Revision 2 follows the runtime's per-session sharing contract and
measured bounded-purge layout. It specifies Forge adoption, not another runtime
implementation.

## Context and evidence

The runtime contract is authoritative in
[extract-journal-storage-engine](https://github.com/ForgeAILab/agent-runtime/blob/c036af7b4635cf7d76451aaa666a9df87405ca1a/docs/spec/changes/extract-journal-storage-engine-2026-10-19/design.md),
especially amended D2 rule 4, D5's per-engine memo and D8.4's measurements.
Implementation references below use `runtime:` for source paths in the
`v0.10.0` tag at immutable commit
`c036af7b4635cf7d76451aaa666a9df87405ca1a`. Moving branch heads are not release evidence. Runtime Owner Decision Q4 accepts independent
per-session copies; the earlier Forge review's global-index requirement is withdrawn.
The 2026-10-21 amendment (D3 "Within-session body sharing", tasks 27–29) makes
Forge share identical bodies between the two domains of one runtime session, as
the reference does; it still never shares across sessions.

| Evidence | Consequence for Forge |
| --- | --- |
| `runtime:crates/agent-runtime-core/src/journal/storage/mod.rs` and `journal/engine/` | Implement `JournalStorage`; use `JournalEngine<ForgeJournalStorage>` for semantics. |
| Reviewer commit `a49e3e0` in the runtime review | The engine gate is `futures_util::lock::Mutex`, never a Tokio mutex permit handoff. This correction overrides the older D3 wording in the u9b spec/source snapshot. |
| `historical receipts: rt-e2-review/` findings N1/N2 and F2 | The release gate includes compact pin metadata, the working R4 memo and the gate/control correction; a branch snapshot is not proof these fixes shipped. |
| `runtime:crates/agent-runtime-journal-bench/src/{packed.rs,schema.sql,codec.rs,metadata.rs}` | Reference for pack framing, bitmaps, transactions and operation tiers; not a production dependency. It uses rusqlite and modeled seal lengths, not a real cipher. |
| `historical receipts: rt-e7/{report.md,measurements.json}` and runtime D8.4 | Measured PerSession S1–S5 bytes and pack writes with capped packs and 25% dead-byte purges, at end and per turn. B/N=4000 is 1.60542× L2 plain / 1.79118× tool. |
| `historical receipts: rt-e6-review/report.md`, F2–F4 | Share one engine and its clones per storage/domain; memo charge can under-estimate retained heap by 1.24×. The owner chose 128 MiB per engine on 2026-10-20; no data shows Forge needs the larger runtime default. |
| `runtime:crates/agent-runtime-core/src/journal/engine/{mod.rs,memo.rs}` and `journal/reader.rs` | The supplied source now implements D-1 through `ReadOperation::finish`: check current-head or durable-pin authority once per reader operation and retry via membership-scoped storage on a stale memo view. Released runtime D5 records the D-1 decision and head-bound pin certificates; rt-e6-review F2 describes the earlier state. |
| `historical receipts: rt-e1/report.md` and Forge `protected_store.rs` | The idle legacy row contains two sealed blobs in all 13 measured cases (L2), despite checkpoint saves temporarily NULLing the standalone snapshot. |

Today `SqliteProtectedRuntimeStore` implements both `SessionStore` and
`CheckpointStore`, writing `protected_agent_session_state` by Forge session id.
Runtime ids map through `agent_session.runtime_session_id`. Each save encodes a
whole snapshot/checkpoint. The ordinary payload is currently exact and encrypted;
it is not already a redacted journal. XChaCha20-Poly1305 uses a 24-byte nonce and
16-byte tag; the existing master key is SHA-256 of Forge's protected key material
(the server JWT secret in the server wiring), with `key_revision = 1`.
`FORGE-SESSION\0` format 1 wraps zstd level 3 inside encryption. Old encrypted
JSON remains readable for import.

Forge runs SQLite through sqlx, WAL, `synchronous=NORMAL`, a 30 s busy timeout
and `db::begin_immediate`. One owning server or Solo process holds `runtime.lock`;
daemons do not open this database. Existing migration files are immutable.

## Goals / Non-Goals

- Adopt U9 before U10, with delta writes, exact protected recovery and a
  policy-projected ordinary domain, both encrypted at rest.
- Preserve user data, recovery barriers, credential exclusion and the meanings of
  `saved`, topic hand-over and `checkpoint_present`.
- Meet the physical disk gate: fixture B, N=4000, plain and tool, after collection,
  at most 2.0× Forge's measured real legacy L2; aim for 1.5×.
- Background conversion now with lazy import fallback; retire replaced and
  unresumable sessions; clean up the legacy table next release. Collection has no TTL.
- Forge builds storage, framing, SQL fences and lifecycle scheduling only.
- No U10/LCM-table consolidation, warm-runtime redesign, runtime schema 5,
  per-object SQL tables, feature flags, compatibility aliases or multi-process DB
  ownership. Storage packing does not change canonical runtime objects.

## Decisions

### D1. Ownership and host-facing changes

`SessionJournals`, owned by the long-lived protected store/runtime service, holds
one shared `Arc<ForgeJournalStorage>` and two long-lived
`JournalEngine<ForgeJournalStorage>` instances: exactly one per storage and
domain. Hand clones of those engines to every Runtime instance, reader, fork path
and retention/collection worker. Never construct another engine over the same
storage/domain during the process lifetime. Clones share the validated-root memo
and the lease registry; a new engine loses both. Writer changes therefore reuse
warm roots and collection sees the leases used by all runtime instances.

Released runtime v0.10.0 preserves D-1: warm reads finish each reader
operation by checking stored current-head authority or the selected historical
root's durable pin; a stale view retries through membership-scoped storage.
`JournalEngine::read`, reader materialization and extension reads use that path
(`engine/mod.rs`, `engine/memo.rs::ReadOperation`, `reader.rs`). This supersedes
rt-e6-review F2. Released runtime D5 records the resolved decision and requires
a head-bound memo certificate for a historical pin. Task 1 verifies this in the
published runtime release; it is release-source evidence, not a Forge storage test result.
That check does not make independent engines safe: collection sees only its
engine's leases, and cross-session dependency/retirement checks are not protected
by a same-session fence. Exactly one engine and its clones per storage/domain
is required for correctness as well as memo reuse, matching the pinned runtime's
development guide. The release gate verifies the warm-read check too.

| Forge use | Adoption |
| --- | --- |
| `SessionStore::journal()` | Ordinary engine, storage id `forge.session-journal.v1.ordinary`, explicit redacting host projection (D3). |
| `CheckpointStore::journal()` | Protected engine, storage id `forge.session-journal.v1.protected`, exact state. |
| Legacy `load` / `load_latest` | Read-only, authenticated v3 import source; native-first open skips it when not needed. |
| Legacy `save` methods | Delete replaced write machinery; return `RuntimeError::conflict("legacy session store is read-only")`. |
| Per-turn and fork probes in `native.rs` | Scalar published/retired metadata plus the bounded LCM extension read (D5). |
| Topic hand-over DB probe | A published native head in either domain, or an eligible legacy payload. A revision-0 writer reservation is not saved state. |
| Agent continuity | `checkpoint_present` means a published, unretired protected head with `has_checkpoint`, or an unimported legacy checkpoint. |
| Credentials and `agent_lcm_*` | Separate stores; never copy credentials into journal DTOs. LCM storage changes wait for U10. |
| REST, MCP, CLI, events | No payload changes. Update implementation docs during adoption; no generated-type changes unless an implementation changes an API. |

### D2. What Forge consumes from the runtime

**Release/pin gate.** Every runtime package and the testkit now consume
**agent-runtime v0.10.0** at immutable tag commit `c036af7b4635cf7d76451aaa666a9df87405ca1a`.
The original task-1 gate adopted the first engine release; this follow-up adds
per-session collection isolation, the retryable lost-acknowledgement contract,
and failed-acceptance rollback/fencing. Verify the receipt amendment, R1–R7,
N1/N2 fixes, native-first open, bounded extension reads, S5, the amended
PerSession/deferred-removal contract, reviewer gate fix and D-1 authority checks.
Forge uses shared engine clones regardless. Missing pieces block adoption;
they are not Forge engine jobs.

Task 33 also opts `ForgeJournalProjection` into `revision`/`project_delta` at
revision 1. The policy is pointwise and idempotent: previously projected positions
stay unchanged, and only updated Sensitive optional diagnostic values become
removals. A replacement must remove any formerly visible value. Complete
projection remains the authority for bootstrap, unknown revisions and opaque
diagnostic references. This preserves D3's policy; it does not substitute an
identity projector. Equivalence tests cover both sensitivity transitions,
removals, unknown namespaces and unchanged non-extension delta fields.

**Task 33 adoption (2026-10-24).** The v0.10.0 pin includes runtime P1/P2/P4
and the bounded collection/storage protocol. Forge has no direct `SessionState`
field consumers; it uses the still-public `SessionSnapshot`.

The public boundary is `JournalStorage: Send + Sync + Debug + 'static`.
`sharing()` is synchronous and Forge explicitly returns
`JournalBodySharing::PerSession` (the trait default is Global). The following
methods are async. Every result is `Result<…, RuntimeError>`; domain/session
arguments are borrowed `&JournalDomain` / `&SessionId`.

| Method | Additional arguments | Successful result |
| --- | --- | --- |
| `load(domain, session)` | — | `Option<JournalSessionRecord>` |
| `objects(domain, session, ids)` | `ids: &[JournalObjectId]` | `Vec<Option<Arc<[u8]>>>`, in request order; non-member = `None` |
| `members(domain, session)` | — | `Vec<JournalObjectId>`; non-collection inventory |
| `operation(domain, session, operation)` | `&JournalOperationId` | `Option<JournalOperationRecord>` |
| `pins(domain, session)` | — | `Vec<JournalPinRecord>` |
| `catalog(domain, session, head)` | `&JournalObjectId` | `Option<JournalCatalogEntry>` |
| `dirty_sessions(domain, after, limit)` | `Option<&SessionId>`, `usize` | `Vec<SessionId>`, session-id order strictly after cursor; zero limit = empty |
| `apply(write)` | owned `JournalWrite` | `JournalApplyReceipt` |
| `retained_bytes(domain)` | — | `Option<u64>`; Forge returns `Some` canonical body bytes physically retained, including deferred dead bytes (D4), not the trait's default `None` |
| `compact(domain)` | — | `()`; physical work only, no membership change |
| `collection_capability()` | synchronous | `Bounded`: 256 page items, 1 MiB canonical bytes per physical unit, 32 apply units |
| `collection_page(domain, session, request)` | full fence, kind, opaque continuation, item cap | bounded member/pin page; no whole-session slicing |
| `prepare_collection_apply(plan)` | exact write and unit allowance | read-only `Fits { units }` or no-effect `Split { max_release }` |
| `apply_collection(plan)` | same exact plan | atomic fenced receipt plus actual rewritten input-unit count |

DTOs are preserved losslessly, including unknown opaque engine-state versions;
Forge does not parse private engine metadata or recompute runtime roots.

| DTO | Persisted/transported fields |
| --- | --- |
| `JournalDomain` | `storage_id`, `role` |
| `JournalEngineState` | `version: u32`, `bytes: Vec<u8>`; opaque, sealed; Debug exposes version/length only |
| `JournalFence` | `revision: JournalRevision`, `writer_epoch: u64`, `generation: u64`; absent record = all zero |
| `JournalSessionRecord` | `generation`, `revision`, `writer_epoch`, optional head `JournalObject`, `has_checkpoint`, optional `retirement`, `collection_dirty`, `engine` |
| `JournalStoredObject` / `JournalObjectBatch` | `id`, kind hint, canonical bytes; batch `objects` in canonical id order |
| `JournalOperationRecord` | operation id, private digest, published revision, complete original result `JournalHead` |
| `JournalPinRecord` | pin id, `blocks_retirement`, roots, opaque engine state |
| `JournalCatalogEntry` | head id, opaque engine state |
| `JournalPinChange` / `JournalCatalogChange` | `Put(record)` or `Remove(id)` |
| `JournalWriteKind` | `OpenWriter`, `Commit`, `Retire`, `Authorize`, `Pin`, `ReleasePin`, `Stage`, `Collect` (non-exhaustive) |
| `JournalWrite` | domain, session, kind, `expect: JournalFence`, required replacement `record: JournalSessionRecord` (generation = expect.generation + 1), object batch, optional operation, pin/catalog changes, `release` ids, optional `prune_operations_before`, optional test `inject` |
| `JournalApplyReceipt` | `reclaimed_objects: u64`, `reclaimed_bytes: u64`; physical body copies removed by this acknowledged transaction, including previously deferred bodies, once per copy at removal time; bytes = canonical encoded lengths before compression/sealing, not pack or SQLite page bytes |

Forge honors the runtime's seven trait rules:

1. **Atomicity.** `apply` is all-or-nothing. A precondition mismatch is
   `StateConflict` at `PreProvider`/`Checkpoint`, with no effects. Other errors are
   ambiguous; the engine discards its cached record and reloads before retry.
2. **Order and cuts.** Non-control objects → Checkpoint → Batch → pins/catalog →
   session record (including Head object publication) → operation → COMMIT.
   All seven `JournalCommitFailurePoint` variants reach storage: `AfterObjects`,
   `AfterCheckpoint`, `AfterBatch`, `AfterPin`, `AfterHead`, `AfterIdempotency`,
   `AfterCommit`. Pre-commit cuts roll everything back; `AfterCommit` commits,
   then loses the acknowledgement. The final new pack(s) may be staged/replaced
   inside that transaction; packing must not erase the cut boundaries.
3. **SQL fences.** Compare `expect` inside the write transaction, in SQL, against
   revision, writer epoch and control generation (`JournalFence`). Absence matches
   only the all-zero fence. Every apply supplies a replacement session record and
   advances its generation exactly once, even when revision/epoch are unchanged.
   A non-successor generation or generation overflow conflicts with no effects;
   revision and writer epoch remain exactly as supplied by the engine.
4. **Membership/sharing and receipts.** Forge declares PerSession and shares
   identical immutable bodies between the domains of one runtime session, as
   the runtime permits (2026-10-21 amendment, D3). The physical deduplication
   scope is the runtime session id `S` across its domains; no admission lookup
   crosses sessions. Membership stays per `(storage_id, role, S)`: a domain gains
   a bit only by admitting the complete canonical bytes, verified byte-identical
   to the stored body, including re-admission of a deferred slot or a slot the
   other domain holds. An immutable id with different bytes in that session
   conflicts. Every held member stays readable; a body is never served to a
   domain without its own bit, even when the other domain holds it or it sits in
   a sealed pack/cache. A slot is dead only when no domain of the session holds
   it; another session never delays reclaiming. Below 25% dead canonical body
   bytes a sealed pack may defer removal; the apply reaching 25%, a domain
   releasing its last member in the session, or collection of a retired session
   purges dead bytes (D4). Deletion cleanup also purges the deleted session's
   bytes (D8). A receipt counts each physical copy at removal, including earlier
   deferred bytes; clearing bits, a release that leaves another domain holding
   the slot, and moving live bodies count zero.
5. **Consistent reads.** Each read observes a completed apply, never a mixture of
   pre-repack bitmaps and post-repack bodies. The engine gates reads feeding a
   write; immutable bodies do not require a transaction spanning trait calls.
6. **Transaction scope.** A storage transaction exists only inside `apply` (or
   independent physical maintenance), never in engine/host/provider code.
7. **Isolation.** Knowing a content digest never grants access. Storage checks
   `(storage_id, role, session)` membership; the engine additionally checks lease
   roots and reachability.

Construct engines with `JournalEngine::protected(storage, storage_id, config)`
and `::ordinary(storage, storage_id, policy, config)`. Production config enables
collection (`collection_enabled = true`) and sets `validated_root_memo_bytes`
explicitly to **128 MiB estimated memo charge per engine**, 256 MiB combined for
the protected and ordinary engines, per the 2026-10-20 owner decision. Forge is a
local, single-user app; no data shows the larger runtime default is needed. The
rt-e7 measurements used that default, not Forge's selected budget. A memo miss,
including after oversize rejection or eviction, costs only a cold validation:
it affects speed, never correctness. Keep one engine per storage and domain,
cloned into every runtime and the collector.
This is an estimated charge limit, not a heap/RSS cap: rt-e6-review F3 measured
retained heap at 1.007–1.240× charge, worst with many small messages. At 1.24×,
128 MiB charge corresponds to about 158.72 MiB per engine / 317.44 MiB for both
(roughly 159 MiB / 317 MiB);
this is observed headroom, not a guaranteed bound. Live writer working sets and
the separate 64 MiB storage cache add to that. Shared layers can instead be
over-charged, and F4 notes that historical heads from a busy session can evict
other sessions' current roots. D10 reports charge, actual resident memory,
oversize/eviction counts, memo hit/miss counts and cold-validation time so the
budget can be raised on evidence. Tests also exercise zero/small budgets and
collection disabled.

The engine owns commit validation, ordinary projection/re-encoding, checkpoint
floors, replay semantics, sequence/predecessor checks, leases, closures,
dependencies, retained-root selection, grants/tombstones, root epochs,
incremental collection and S5 selection. Forge does not copy them. R3 pages dirty
sessions and computes `members − live`; R4 reuses validated closures/snapshots
across writer changes, with one cold validation after restart/eviction. R5 avoids
reloading legacy; R6 provides `JournalReader::extension(namespace)`. R7 supplies
the storage-only measurement basis, not a guarantee of Forge's own file size.

The engine write gate is `futures_util::lock::Mutex<()>`, with the cooperative
budget behavior recorded by reviewer `a49e3e0`; never substitute a Tokio mutex
handoff. Its short registry mutex is not held across awaits. `close`/drop and
`revoke_writer` are I/O-free. Released N1 pin metadata stores roots, compact
dependency summaries, writer epochs and head-bound memo certificates rather than
a full closure. N2/R4 reuse validated roots across warm commits/writer changes.
Forge persists these opaque values verbatim and does not duplicate their algorithms.

### D3. Logical domains, encryption and ids at rest

**Protected is exact; ordinary is redacted by host policy.** The protected engine
retains exact snapshot/checkpoint values and all recovery barriers. The ordinary
engine invokes Forge's `JournalProjectionPolicy` before runtime encoding/hashing;
storage receives only the resulting admitted batch, never raw ordinary drafts.
The runtime validates/re-encodes the projection. Forge must not use
`IdentityJournalProjection` as its production policy merely because old ordinary
rows were encrypted.

The conservative Forge policy decided on 2026-10-20 removes sensitive, optional
host diagnostic extension values and redacts diagnostic message values wherever
doing so preserves recovery. It preserves session identity, history/usage/manifest
cardinality and ordering, execution counters and timestamps. Runtime-owned manifests and
`runtime.core.previous_cache` stay exact as required by the structured-delta
contract. Restore-critical conversation/extension data must not be discarded from
an ordinary-only session with no exact checkpoint: such values remain encrypted
and exact until an exact recovery source exists. Import/resume equivalence tests
cover that case. The 2026-10-20 owner decision settles this conservative policy:
omit sensitive optional diagnostics, preserve all restore-critical data and
runtime-mandated exact fields, and make no promise that runtime cache/manifests
contain no content. Broader free-text redaction needs explicit data examples and
resume-equivalence evidence. Protected exactness is unchanged. The implementation reserves
`forge.diagnostics.*` for optional host diagnostics and omits Sensitive values
there; no restore-critical state may use that namespace family. Unknown
namespaces, LCM, conversation and runtime cache/manifests remain exact because
there is no evidence that they can safely be discarded.

Both domains use the same derived encryption-key material and separate session
records, memberships, pins, catalogs and operation packs. Since the 2026-10-21
amendment, body packs belong to the runtime session rather than to one domain:
a pack is owned by exactly one `(runtime_session_id, forge_session_id)` and each
domain holds its own membership bitmap over it. Different sessions never share
bodies, indexes or admission lookup candidates (Q4 unchanged).

**Within-session body sharing (2026-10-21 amendment).** Task-25 profiling found
that 99.2% / 99.6% of Forge's B/4000 disk gap to the runtime reference was the
ordinary domain re-storing about 20,000 bodies the protected domain already held.
Forge therefore stores an identical immutable body once per session and lets both
domains reference it. The rules that keep this safe:

1. *Conservative projection is unchanged.* The ordinary engine still projects
   before encoding/hashing and storage still receives only the admitted ordinary
   batch. A domain gets a membership bit only by admitting the complete bytes;
   storage verifies the full id and byte equality against the stored body before
   setting the bit. Object ids are content digests, so a shared body is exactly
   one the ordinary projection produced. A body only the protected domain admitted
   (exact checkpoint state, redacted diagnostics) never gains an ordinary bit,
   and reads, members, purge remaps and merges carry each domain's bits
   separately: no rewrite grants a domain a slot it did not hold.
2. *Independent memberships, roots, pins and collection.* Each domain keeps its
   own session record, fence, pins, catalog, operation packs, dirty flag and
   membership bitmap. Its engine decides liveness alone. Releasing a slot clears
   only the writing domain's bit; the body is physically removed only when no
   domain of the session holds it (D4 purge rules, receipts once per copy).
3. *Session-scoped sealing.* Pack index and body units are sealed under a
   session-scoped AAD, with new unit codes and table label (`session_pack`):
   format, key revision, unit kind, the runtime session id, Forge owner label,
   pack id and pack generation. No domain field appears in a pack AAD, and no
   domain-bound unit uses the new codes, so a domain-bound binding can never
   open a pack unit or vice versa. Session, pin, catalog and operation units stay
   bound to their full domain/session key exactly as before. The AEAD key is
   unchanged: one HKDF-derived XChaCha20-Poly1305 key used only for
   XChaCha20-Poly1305, with the HMAC lookup key still derived under its own label.
   No key is used by two algorithms, and a random 24-byte nonce per seal keeps the
   existing nonce analysis.
4. *Rollback and swap analysis.* Moving pack ciphertext to another session,
   owner label, pack id or generation still fails authentication, as does moving
   a metadata unit across domains. A pack unit is no longer bound to a domain,
   but packs no longer have a domain to swap: domain authority over a slot is the
   clear membership bitmap, exactly as read authority was already the bitmap
   inside a domain-owned pack. Editing bitmaps or rows requires write access to
   the SQLite file, which stays out of scope (below). A file writer could set an
   ordinary bit on a protected-only slot of the same session; before the change
   the same writer could already resurrect released slots, roll back records and
   relabel membership. Bitmap overlap now also shows how much of a session's
   protected content also appears in ordinary. Both are accepted under the
   existing local-attacker scope; neither reveals plaintext or a digest.
5. *Isolation of failures.* A corrupt shared pack fails closed for every domain
   of that session (D-2 isolation is per session already); other sessions are
   untouched.

**Real sealing.** Keep **XChaCha20-Poly1305**, Forge's existing master-key path and
`key_revision`; do not copy the reference's zero-tag/nonce simulation. Compress a
pack's body unit with zstd level 3, then seal its index unit and body unit
separately (D4), each with a fresh random 24-byte nonce and a real 16-byte
authentication tag. Use a versioned `FORGE-JOURNAL\0`
envelope. Authenticate format, table kind, unit kind (pack index or pack body),
pack id plus pack generation (or metadata row key), format revision and key
revision as associated data. Pack ids are never reused (SQLite `AUTOINCREMENT`);
a purge rewrite keeps the id and increments `generation`, a merge writes a new
id. Binding the generation guards against bugs, stale caches and ciphertext
swaps under the current generation that could misalign the membership bitmap. Bind each pack
unit's AAD to its owning runtime session and Forge owner label (session-scoped
since the 2026-10-21 amendment); singleton metadata binds its full domain/session
key. Every rewrite/repack uses a
fresh nonce. Seal session, pin, catalog and operation payloads too.
Decode/authentication errors fail closed; never treat corruption as
an absent or fresh session. Diagnostics never print decrypted contents or keys.

**Id decision.** Runtime object ids remain content digests and are never rewritten.
Four-byte prefixes and offsets live in each pack's **sealed index unit**; full ids
are recomputed from verified bodies. Rebuildable RAM lookup directories (D4)
are scoped to one runtime session and populated lazily on its first object access
or admission; storage open decrypts no pack index or body. The old per-object
`HMAC(id)` key and SQL id index are unnecessary: there is no clear digest/prefix
in the database against which to confirm a guessed message. Encryption alone
would not protect a clear catalog digest, so the small catalog lookup key remains
`HMAC-SHA256(lookup_key, domain-separated catalog context || domain || session ||
head_id)`; pin lookup keys similarly use a separate pin context. The journal's
AEAD key and `lookup_key` are derived from the existing master key with
HKDF-SHA256 under distinct labels (`forge/session-journal/aead/v1`,
`forge/session-journal/lookup/v1`), so one key is never used for both
XChaCha20-Poly1305 and HMAC. `key_revision` stays 1. Real ids stay in
sealed metadata. Operation ids/digests/results stay inside operation packs, with
no per-operation clear hash index. Membership bits and scalar counts expose
structure/size, not plaintext digest confirmation. AEAD protects content and row
binding; full object-id verification protects lookup correctness.

A local attacker with write access to the SQLite file is out of scope: they can
already edit `agent_session`, credentials and the legacy table, or restore an
older copy of the file. Per-row freshness is not provided; authenticated rows
and clear membership bitmaps do not detect rollback of previously valid state.
Optional later hardening could add a keyed MAC per membership row through a new
migration. That is future work, not an adoption task.

**Key change.** The master key is SHA-256 of the server JWT secret. Changing or
losing that secret makes every sealed unit undecryptable, exactly as it does for
today's legacy rows and credentials. An authentication failure is a typed error
for that session: its open, import and collection fail closed, and the worker
logs it once per session up to D8's process cap and continues with other sessions. Nothing is retired,
released or deleted because of it. Deletion cleanup removes packs that have no
membership row without decrypting them; partially held undecryptable packs stay.
Key rotation remains unsupported.

Credentials remain exclusively in the existing credential tables and
`ProviderCredentialSource` leases. Decrypted-pack/metadata tests cover API keys,
OAuth access/refresh tokens and refresh flows; ordinary redaction is not a
substitute for excluding credentials from the protected domain too.

### D4. Forge table and pack layout

One additive migration: `V{YYYYMMDDHHMM}__session_journal.sql`. `{YYYYMMDDHHMM}` is allocated at
implementation time as a unique UTC `YYYYMMDDHHMM` version, following Forge's
post-V149 convention. Never edit a historical migration or hard-code a speculative
next number. Preserve all legacy rows; the legacy table drop is next release.

The six tables below replace the old body/object/operation-row design. `D` means
`(storage_id, role)` and `S` the trait's **runtime session id**, stored as the
opaque TEXT the runtime passes. Keys, reads and dirty paging use it directly, so
reads never consult a mapping: an unknown runtime id has no rows and reads as
absent, which the conformance `stranger` cases require. Dirty paging returns
runtime ids in runtime-id order, never Forge UUIDs.

Every session-metadata/membership table also has an indexed `forge_session_id`
column, `TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE`, used only
for cascades and host probes (SQLite otherwise scans
the child table on each `agent_session` delete). `agent_session.runtime_session_id`
is nullable, not unique and not indexed today. The migration adds a plain
**non-unique** index on it, never a UNIQUE index that would fail on an existing
database with duplicates. `apply` resolves the mapping inside its transaction;
zero or several matching `agent_session` rows is a typed host error (not
`StateConflict`) that writes nothing, and a row's stored mapping must match on
later applies. Conformance test support wraps `ForgeJournalStorage` in a
test-only decorator. Before it delegates an apply for an unseen runtime id, it
inserts a stub `agent_session` (seeded identity/profile/scope fixture) carrying
that id. Production storage has no such mode. Pre-seeding a list of testkit ids
is not acceptable: the suite uses dozens of literal and formatted ids (for
example `atomic-{point:?}`) that change upstream.

| Table | Key and clear query columns | Sealed payload / lifetime |
| --- | --- | --- |
| `agent_session_journal_session` | PK `(storage_id, role, runtime_session_id)`; indexed `forge_session_id` FK; generation, revision, writer_epoch, collection_dirty, published, has_checkpoint, retired; key_revision, format, nonce | Lossless `JournalSessionRecord` including head, retirement and opaque engine state. Revision 0 + no head is a reservation. Partial dirty index ordered by domain/runtime session id. |
| `agent_session_journal_pin` | PK `(D, S, pin_key)`; session FK; key/format/nonce | Complete `JournalPinRecord`, including roots and opaque state; Put/Remove only through apply. |
| `agent_session_journal_catalog` | PK `(D, S, head_key)`; session FK; key/format/nonce | Complete `JournalCatalogEntry`; `head_key` is keyed as in D3, not the raw digest. |
| `agent_session_journal_pack` | `INTEGER PRIMARY KEY AUTOINCREMENT` pack id (never reused); strict session owner `S` plus indexed `forge_session_id` label (no domain columns since the 2026-10-21 amendment); generation, key/format, object/retained-canonical-byte counts; index on `(S, pack_id)` | Two session-sealed units with their own nonces: the index unit (prefix/offset directory) and the zstd body unit. One runtime session owns all slots; each of its domains holds its own bitmap; no cross-session holder is valid. Pack owner labels have no cascading FK so deletion cleanup can be bounded. Retained bytes include deferred dead slots. |
| `agent_session_journal_membership` | PK `(D, S, pack_id)`; session FK and pack FK on `(pack_id, S, forge_session_id)`; bitmap; secondary indexes on pack_id and `(S, pack_id)` | One bit per prefix-directory slot, per domain. No object id, per-object row or offset index in SQL. Remove empty bitmap rows. |
| `agent_session_journal_operation_pack` | PK `(D, S, tier)`; session FK; key/format/nonce | S4 lossless original operation records in binary-carry tiers, including private digest, revision and complete result head. |

Session, pin, catalog, membership and operation-pack rows cascade from
`agent_session`; pack rows do not. Packs are exclusively owned, but intentionally
outlive the row deletion until bounded post-deletion cleanup removes them (D8).
Their runtime id and Forge owner labels remain available after the cascade;
membership rows cannot refer to a pack owned by a different session.
An apply checks that owner and mapping inside its transaction. In-row invariants
have CHECKs:
`has_checkpoint` is false without a published head and on retirement, and a
bitmap is non-empty. Bitmap length against the pack's slot count spans two
tables, which a SQLite CHECK cannot express, so storage validates it on every
read and write. Scalar query columns must agree with the authenticated record on load.

The task-3 schema uses eight-byte big-endian BLOBs for generation, revision and
writer epoch (and pack generation), preserving every runtime `u64` value in SQL
fences without narrowing to signed SQLite integers. Composite membership FKs
also enforce the pack's complete runtime-session/Forge owner key. The
allocated migration is `V202610200941__session_journal.sql`.
The additive `V202610201253__session_journal_deletion.sql` adds a clear
`orphaned` pack marker and partial pack-id index. Membership insert/delete/move
and session-deletion triggers maintain it; existing packs are backfilled from
owner/membership existence without rewriting sealed units. Chat deletion removes
its polymorphic context scopes; Agent archival (the user-facing Agent deletion)
removes its native and CLI sessions. These two logic edges live in repository
code in the same source transaction: `delete_agent_chat` and the User/Project
parent deletions call `delete_agent_chat_scopes_in_tx`; both Agent archive entry
points and machine-removal archival call `delete_saved_agent_sessions_in_tx`.
Only orphan-marker projection triggers remain in the journal migrations;
they must observe membership/session FK cascades. Pack layout invalidation
lives in rewrite/delete writers and orphan drain, with one indexed bump per
affected session per transaction. Monotonic pack inserts need no bump. Project/Task and physical Agent deletion
use existing FKs. Archive has no unarchive path; it removes saved CLI resume handles
without signalling an already running CLI process. Late row updates fail NotFound.

**S1/S3 framing.** Each apply's genuinely new bodies enter one final pack row
(no row if all bodies exist already), or several rows cut at the S2 logical-size
cap when a large apply such as an import exceeds it. Pack v1 keeps the reference's content but
splits it into two sealed units. The **index unit** is `JRPACK\0`, version byte,
count and the id-sorted four-byte prefixes with varint body offsets/lengths. The
**body unit** is the canonical bodies physically ordered by kind and full id,
compressed with zstd level 3. Bitmap slot order is directory order, not
body-offset order. The prefixes are random and do not compress, so moving them
out of the zstd frame costs about one nonce and tag per pack, not per object;
D10 measures it. All lengths, offsets, counts and schema/kind envelopes are
validated on decode. A prefix collision yields several
candidates; recompute/verify the **complete** runtime object id against the requested
id, never accept the prefix alone. Kind is checked from the canonical envelope,
not trusted from the batch's clustering hint. No canonical object, reference or
`JOURNAL_SCHEMA_VERSION` changes.

**Session-local admission and lazy lookup.** `sharing()` explicitly declares
`JournalBodySharing::PerSession`. Deduplication, prefix candidates, new packs and
maintenance are scoped to the runtime session `S`, shared by its domains;
memberships, reads and releases stay scoped to `(D, S)`. No cross-session
admission lookup occurs. Testkit branches `membership_isolation_and_receipts` on
that declaration and accepts either shared or separate domain copies within a
session; the old zero-until-fourth-holder global requirement does not apply.

Opening storage and loading scalar/session metadata decode no pack units.
The first object lookup, membership inventory or admission lazily reads only the
requested session's pack directory, using its indexed owner key, plus that
session's membership rows. Rebuild the session's bounded RAM prefix/offset
directory from its sealed index units; load candidate body units through the
bounded LRU and verify full ids/bytes. An identical admitted body already in the
session, including an unserved deferred slot or a slot only the other domain
holds, reuses that slot and sets the writing domain's bit; a verified miss adds
one new copy. Different bytes for an existing id conflict, and
`JournalObject::kind()` verifies the id digest and kind on admission. Other
sessions keep independent copies even for identical ids. Per-session directory
caches are
evictable and generation-checked; no database-wide body map is built. Update
caches only after COMMIT (D5). No clear id, prefix or keyed per-object tag is
stored in SQL. As in the reference's lazy `pack.owner` lookup, opening one
session does no work proportional to all other sessions' packs.

**Atomic apply and SQL fence.** Use one sqlx `db::begin_immediate` transaction for
all components of a `JournalWrite`. For an existing row, an SQL guard with
`WHERE storage_id = ? AND role = ? AND runtime_session_id = ? AND revision = ? AND
writer_epoch = ? AND generation = ?` must match for every write, including
pin/collection/authorization controls that preserve revision and epoch. For an
absent row, only the all-zero `JournalFence` plus a checked SQL absence guard
matches. Every write requires a replacement `JournalSessionRecord` with
`generation = expect.generation.checked_add(1)`; reject a non-successor or overflow
atomically. No unchecked upsert may overwrite a concurrent writer or control.
Persist the engine-supplied revision and epoch exactly; writer open advances the
epoch, while pin/collection writes can retain revision and epoch. Session control
generation is independent of pack generation (the latter fences cached slots).
Mismatch rolls back with the runtime's typed conflict, not a silent save supersession.

Follow D2's ordered cuts within the same transaction: stage new packs and all
membership additions, checkpoint/batch, pin/catalog changes, record/head and
operation pack. Stage `release` and any explicit operation pruning in that same
transaction. Commit once; update shared caches only after success. On rollback or
ambiguous commit acknowledgement, invalidate affected caches and reload. The
SQL fence is required even with an in-process gate and `runtime.lock`.

**Membership deletion, deferred purge and receipt.** Clear only the written
domain/session's bits, deduplicating repeated release ids. Read authority ends
immediately, including on cache hits. A slot is dead when no domain of the
session holds it. For each touched sealed pack, compute
`dead_bytes / retained_body_bytes` from canonical encoded lengths of dead slots
and all physically retained slots, before compression/sealing. Below **25%**,
leave the ciphertext sealed and return zero for the deferred copies. The apply
that reaches **25% or more** rewrites/reseals that pack without all dead slots,
increments its generation and atomically remaps every holder domain's bitmap
from its own previous bits; delete empty packs and empty bitmap rows. When the
written domain holds nothing left in the session, the apply purges all dead
bytes of the session's packs. Collection of a retired session scans all the
session's packs and forces the same purge even below 25%, including dead slots
from prior applies with no newly released id. Slots held by any domain remain
readable to that domain. These purges are mandatory with S2 off or on.

Return the exact physical copy count and sum of original canonical encoded
lengths removed in this acknowledged apply, including previously deferred slots,
once per copy. A shared copy counts once, in the apply that removes it; a
release while another domain still holds the slot counts zero. Separate session
copies are counted independently.
Merely clearing bits, re-admitting a retained slot, moving live bodies and
reclaiming SQLite pages count zero. Optional maintenance does not silently remove
deferred bodies outside this receipt path; deletion cleanup is separately
accounted because it is outside apply (D8).

**Retained-byte evidence.** Implement `retained_bytes(domain) -> Some(bytes)` as
the trait defines it for a sharing scope: the sum of canonical body lengths still
physically stored in the packs of every session in which that domain has a
session record or membership, including deferred dead slots, each copy once.
Shared copies therefore appear in both domains' diagnostics; the two values are
not additive. Maintain the per-pack retained-byte count atomically and verify it
on pack decode; use a consistent read snapshot. `byte_stats` reports live bytes
as slots held by any domain of those sessions and deferred bytes as slots held by
none, so live + deferred = retained. This diagnostic does not include metadata,
operation records, ciphertext overhead or SQLite pages, never grants read
authority and never decides liveness. GC evidence compares its before/after
difference with physical apply receipts, so deferred bytes cannot disappear from
the accounting simply because membership bits were cleared.

**S2 repacking and rewrite cost.** A Forge post-terminal hook runs async storage
maintenance, in its own transaction through the coordinator, after terminal
publication succeeds. Merge equal logical-byte tiers (minimum 4 KiB), carrying
merges geometrically. **Cap every pack at 1 MiB of logical body bytes.** This is
the storage cap verified by D10. Imports, admissions, purge
rewrites and all repacks obey the cap; reject an individual body above it
atomically. Merges and the base repack cut kind-ordered output at the cap, so
volatile kinds (heads, batches, checkpoints, cache plans, manifests) and stable
history land in different packs.
A purge rewrites only its touched capped packs, never a whole-session base pack.
As in the amended reference, collection's base repack selects **only fully live
packs below 64 KiB**, or fully live survivor packs already purged by that apply.
Stable large packs and packs containing deferred dead slots stay sealed; merging
the latter could concentrate dead bytes above 25% in an output pack. Terminal
equal-tier merges likewise select fully live inputs and only merge a pair whose
combined logical bytes fit the 1 MiB cap. "Fully live" means every slot is held
by at least one domain. Within a shared session both paths exclude a pack
another domain holds entirely when the acting domain does not. Fully live mixed
packs that no single domain holds entirely remain eligible. This retains the
protection against pairing another domain's complete stable history with the
acting domain's short-lived bodies, while reducing the fragmentation of mixed
packs. The earlier fully-held-only rule measured B/N=1000 delta writes at 10.3%
after the 35.9% regression, but left C/N=1000 at 33 packs. Task 33's bounded
others-pack evaluation records the current size/write tradeoff below. Every move remaps each holder domain's bitmap of the
owning session atomically from that domain's own previous bits, without touching
another session.

The rt-e7 measurements exercise collection per turn and show bounded pack writes
instead of the earlier whole-history rewrite behavior (D8). Retain the per-turn
collection nudge. Repack changes physical placement only; the engine alone
decides liveness. A failed terminal hook is retryable maintenance and cannot undo
an acknowledged turn.

**Compaction and vacuum.** The reference `compact(domain)` repacks every session
of the domain in one transaction and then runs a full `VACUUM`. Forge bounds it:
one domain/session and at most 8 MiB of rewritten logical bytes per transaction,
no full `VACUUM`, then the existing `db::incremental_vacuum`. Compact changes no
session control generation. It merges only eligible fully live packs; deferred bodies stay on the apply/retirement purge path so no
physical body removal escapes receipt accounting. That helper is a no-op
unless the database has `auto_vacuum = INCREMENTAL`, which only databases created
empty by a recent Forge have. On older databases, later writes reuse freed pages,
but the file does not shrink until the operator runs the existing offline
conversion. Full `VACUUM` stays an isolated measurement step, never a turn or
worker operation. D10 reports freelist pages and file length separately.

**S4/S5 operations.** Appending an operation merges equal-count `2^tier` packs
(binary carry) in the apply transaction, capped at **1 MiB of serialized operation
records before compression/sealing**, including import and prune/rebuild output.
If a carry would exceed the cap, leave the full segment sealed and allocate an
overflow segment outside the carry tiers (reference keys start at tier 64);
reject an individual oversized record atomically. Original operation/digest/result
fields round-trip exactly. Identical replay returns the original head within the retained
window; different input conflicts. The **engine** selects S5 pruning during
collection and passes an **inclusive** floor:
`prune_operations_before = current_revision.saturating_sub(64)` (runtime
`controls.rs`, D8.6: revision current−64 survives, current−65 may be pruned; at
revision 70 the floor is 6 and 65 records remain). Storage executes exactly the
trait rule (`storage/mod.rs`, `VolatileJournalStorage`): within the written
(domain, session) only, keep every record with `revision >= floor`, plus the
**single last** record (maximum `revision`) whose `result.writer_epoch` equals
the `writer_epoch` of this write's required replacement session record. Every other
current-epoch record below the floor is pruned; the exception is one record, not
the whole epoch. Rebuild operation tiers atomically in the same apply. Execute
pruning only when the directive is supplied; Forge invents no TTL or independent
replay floor and never prunes during commit. A pruned replay returns
`StateConflict` and cannot apply again. The rt-e7 reference implements S5 and
measures retained tiers after actual pruning; it is no longer a projection.
Task 1 verified this floor and single-operation exception in `v0.8.0`
(`engine/controls.rs:332`, `storage/volatile.rs:397`); no replay-rule correction
was needed.

Task 33 adds `V202610240425__journal_collection_pages.sql` without changing sealed rows or
membership: session `layout_generation` advances for physical pack changes,
and indexed pin `pin_order` stores only relative order. Pin mutations maintain
sparse ranks; missing ranks backfill in a writer transaction. `load()` never
backfills or decrypts pins. Insertions decrypt the new id and logarithmically
many ranked comparisons; only changed ranks are updated. Pin pages reject
duplicate ranks and nonascending decrypted ids, including the lookahead and
the preceding continuation's authenticated pin. Member continuations bind layout
generation as well as full fence,
rejecting repack/compact positions even when logical generation is unchanged.
Read-only preflight and the write-lock recheck count touched packs, deferred
purges, bounded base-repack inputs and changed operation segments. Merge
outputs transform their charged inputs. A bounded forced purge leaves
`collection_dirty` true while physical work remains. Receipts retain the
existing physical-copy and canonical-byte rules.

### D5. Forge adapter and engine-call mapping

Production storage module: `crates/agent-host/src/session_journal/`, with codec,
sealing and cache. The host `SessionJournals` owner lives alongside it in
`crates/agent-host/src/session_journals.rs`, keeping runtime wiring separate from
storage hardening. No Forge `SessionJournal` engine
implementation or private runtime-metadata decoder is needed.

| Forge action | Engine/runtime call and storage effect |
| --- | --- |
| Session open/resume | Runtime discovers both `journal()` capabilities and calls engine `open`. Writer open issues `apply(OpenWriter)` with the full old `JournalFence` and a required replacement record advancing epoch and generation. The second local writer conflicts before provider I/O. Read-only roots are engine-authorized. |
| Turn commit / idle publication | Runtime calls `commit`; engine validates exact/projected candidates, sequence and replay, then calls storage `apply(Commit)`. Forge adds no semantic CAS/retry algorithm. Run the post-terminal S2 hook after successful terminal publication. |
| Checkpoint barrier | Runtime checkpoint persistence calls protected `commit` with the exact checkpoint; ordinary publication follows runtime policy. Failures preserve existing no-replay/model/tool barriers. |
| Checkpoint and snapshot reads | `JournalReader` over an engine lease. The host's LCM probe calls `extension(LCM_COMPONENT_ID)` for `fixed_overhead_tokens`; it reads map paths and one opaque extension, not history or a structured cache tree. Use the protected view if the ordinary policy omitted that extension. |
| Shutdown / dropped turn | `close` / runtime lease drop release local slots/pins without storage I/O; no synchronous durable control adapter. |
| Retirement | Await `authorize_retirement(session, expected_revision, at)`, then `retire(session, expected_revision)`, protected first, then ordinary; engine checks writer/pin/dependency/grant conditions. |
| Collection scheduling | Observe `engine.root_epoch()` synchronously (I/O-free), then await `collect(JournalGcRequest { storage_id, expected_root_epoch, max_objects })`; retry an epoch conflict later. Engine pages `dirty_sessions`, computes release and S5 directives. |
| Restart | Recreate storage/caches and the two engines. Durable records, writer epochs, control generations, operation results, grants, pins/catalog and tombstones survive; leases, reader pins and validation memo do not. First open validates cold and issues a newer writer epoch. |
| Transfer/staging/conformance controls | Await engine `pin_transfer`, `retain_root`, `release_pin`, `stage_leaves`, `compact`; never serialize a host-computed closure. |

**sqlx, not rusqlite.** Reuse Forge's sqlx pool, transactions, busy policy and error
boundary; the runtime bench is a model, not a module to import. All SQL operations
inside apply use the same `Transaction<Sqlite>` connection. Do not issue `BEGIN`
on one pooled connection and follow-up statements on another. Precompute pack
encoding/sealing outside the write lock where possible; CPU-heavy zstd/crypto
runs through bounded `spawn_blocking` work as in the existing protected store.
Any work that depends on transaction-read data stays part of that storage call;
no provider, tool, projection callback or unrelated host await is in the SQL
transaction. Measure lock-hold time, especially import and base-pack creation.

**F2 async controls.** Use the durable engine's async controls with `.await`.
Never drive a possibly suspending sqlx/engine future using `now_or_never` loops,
`block_on`, `blocking_lock`, thread yielding or the volatile newtype's synchronous
wrappers on a Tokio worker. `root_epoch`, revoke, failure arming and close remain
safe because the engine promises they only touch local registry state. The
runtime owns the corrected futures mutex gate; Forge must neither replace it nor
recreate it in an adapter. Test current-thread and multi-thread Tokio runtimes
with suspended storage and competing maintenance/open operations.

**Read/cache consistency.** Batch pack reads, not one SQL/decrypt per object.
Reads go from the reading session's membership rows to those packs' index units
(cached by pack id and generation), then verify the full id. Keep a bounded
plaintext body/directory LRU (initially 64 MiB combined charged bytes) and reuse
the engine's separate R4 memo. Charge retained scoped-directory entries as well
as decoded bodies, evict idle scope directories, and do not retain an entry
larger than that budget; transient per-call decode memory is separate.
Check membership on every request, including cache hits. Each trait read runs
in **one** read transaction on one pooled connection, so bitmaps, generations
and units come from the same WAL snapshot. Cache keys include pack id and
generation, so a rewrite never serves old slots.

**Staleness is never a conflict.** Do not copy the reference's
`PRAGMA data_version` check. That counter is database-wide, and Forge's other
subsystems write the same file through the same pool all the time, so it would
invalidate on every unrelated write. The reference's response, returning
`conflict()` from `apply`, would surface as `StateConflict` and fail the turn.
Fence failures and genuine runtime contract conflicts remain typed conflicts;
cache staleness never is. Inside each write transaction,
after `BEGIN IMMEDIATE`, storage refreshes only the written `(D, S)` directory
from that owner's pack ids and generations. Remove missing rows, load index units
for new/changed rows, and re-read each admission candidate in that transaction
before deciding; an evicted scope rebuilds lazily from the same owner query.
The write lock is held throughout, so nothing changes during the refresh. This
keeps an independently opened storage correct without a database-wide counter,
global high-water mark or another session's index. Reads similarly validate their
scoped directory against their one WAL snapshot; cache staleness causes refresh
or retry, not a semantic conflict.
Production journal mutations go through one storage coordinator per process
(`runtime.lock` excludes other processes). Cascaded membership deletions need no
cache invalidation because membership is read from SQL on every request. No
full-closure prefetch on every commit.

**Storage failure contract (introduced in runtime v0.9.0, review D3).** A non-retryable
apply error guarantees no durable effect. Every ambiguous COMMIT, lost ack,
failed rollback acknowledgement or post-COMMIT cache failure is retryable.
SQLx BUSY/LOCKED classification uses numeric SQLite codes (including extended
codes through their low byte), alongside IO/CANTOPEN and pool/worker availability.
Validation, authentication and constraint errors before a successful commit stay
non-retryable and roll back. Cache/codec infrastructure failures are retryable
so collection aborts a domain instead of misidentifying a session. S2 commit
failures are retryable; compact errors are retryable because earlier bounded
batches may have committed. The DB-only deletion drain maps any COMMIT error to
transient `DbError::AmbiguousCommit`, then through ServiceError; no new public
HTTP or turn-failure class is introduced. Physical maintenance failures never
undo an acknowledged turn.

**No journal call inside a Forge DB transaction.** Never call the engine or
storage while holding a Forge `begin_immediate` transaction or a pooled
connection. The pool has five connections and storage needs its own write lock,
so such a call waits for itself until the 30 s busy or acquire timeout. Nudges
from deletion, rotation or recovery are sent after the caller's commit.

**Background importer mapping (2026-10-21).** The conversion worker calls
`SqliteProtectedRuntimeStore::convert_legacy_session`, which tries per-session
exclusive host admission shared with native turn/fork preparation. A fenced
engine writer check skips callers outside host admission. It then uses the real
legacy decoders to decide whether the runtime needs Forge's LCM binding and
starts `RuntimeBuilder` over the same stores/engine clones with
`StartSession::resume(...).with_checkpoint_recovery(Defer)`. The runtime's v3
importer owns validation and exact-first publication. No tools or credentialed
provider are configured; the conversion provider refuses calls. Drop the handle
without shutdown/persist so no synthetic boundary is written. Publication is
checked after start: a live-composition refusal after successful import does not
undo or misreport conversion. Non-retryable validation conflicts park; retryable
database errors stay eligible for a later sweep. Non-retryable serialization
failures use the capped unreadable tracker. Forge does not copy import encoding
or infer publication from a successful activation alone.

Pre-topic LCM discovery consults the legacy checkpoint when the ordinary
snapshot omits that namespace, matching the runtime's protected-state overlay.
It adopts only the referenced timeline for the same identity/scope and owner.
An earlier failed snapshot-only lookup may have persisted an empty replacement
binding. Before publication or retirement, the resolver may repair that binding
in one transaction only if its replacement is unclaimed, revision zero, and has
no entries, nodes or operations. A populated replacement stays in conflict.
For a shared pre-V149 Chat timeline owned by an older, unlinked suspended
sibling, conversion and lazy resume may transfer ownership to the session
the Chat actually resumes: active first, otherwise newest suspended native
session per identity/context scope/Profile. The owner must be an older
same-Profile legacy sibling, the timeline must predate and have been updated
after the target's creation, and neither runtime may have published or retired
native state. Claims, leases, topics/rotations, competing resumable sessions,
other bindings, additional legacy references, or another sibling's native state
leave a precise Conflict. The same immediate transaction moves runtime ownership
and binding and marks the older session replaced by the target with its version
fence, preserving its legacy row and making it retire-eligible after foreground
activation. The original timeline, entries, nodes and operations are retained.
V149 is unchanged; it backfills scope/retirement metadata, not runtime ownership.
Both conversion and lazy resume retain the old LCM extension for
runtime identity/binding/frontier validation and exact import; the live runtime
rebuilds changed tuning metadata. Stale/NULL Forge policy markers do not justify
discarding identity evidence or rewriting legacy payloads.

**Scalar probes.** Return native publication, tombstone and checkpoint flags
without decrypting a pack, selecting rows by the indexed `forge_session_id`. A reservation is absent publication; a tombstone is a
retired conflict, not fresh state. Consult legacy payloads only when native state
has not taken authority; a surviving stale legacy blob must never resurrect a
retired or advanced session. Hand-over and continuity DB helpers retain their
public meaning. Extension decoding is a separate bounded reader operation, never part of the
existence probe. Acquiring an engine-issued lease on a cold or evicted root
first performs the runtime's full-root validation (including history/checkpoint
closure); a warm owner reuses that validation. The extension operation itself
reads only head, batch, map path and the selected opaque value. No host adapter
bypasses that validation to promise a history-free cold open.

### D6. Conformance and failure evidence

Add `agent-runtime-testkit` at exactly the runtime pin. Run
`assert_journal_storage_conformance(factory)` unedited against the **real
ForgeJournalStorage**, using fresh file-backed sqlx databases and real keys.
The factory retains its temp directory and uses D4's test-only mapping decorator,
which creates a stub `agent_session` for each runtime id on its first apply. Use an explicitly enabled test-support feature without
self dev-dependencies, or a test-local support module.

Run all reference configurations: (1) S2 repacking disabled, (2) enabled,
(3) storage reopened on every engine hydration over the same file, plus fresh
engine recovery. Every production-storage configuration declares PerSession with
within-session domain sharing; disabling S2 never disables threshold/retirement
purges. Collection disabled/enabled and arbitrary ordinary policies are
covered by the suite; exercise cold/evicted/warm R4 budgets in Forge tests. Do not
replace file-backed restart evidence with only `sqlite::memory:` tests.

The suite includes primitive apply cuts, revision/epoch/control-generation fences,
opaque metadata/body conflicts, ordered dirty paging, membership isolation and scope-dependent
receipts, independently reclaimable session copies, bounded deferred
deletion and exact cumulative receipts on scope emptying; protected/ordinary
journal suites;
collection, ordinary shared suffix and projected intent; and unsupported
collection. Forge does not reimplement `JournalCollectionHost` semantics: testkit
already implements it for `JournalEngine<S>`.

Run all eight deliberately broken-storage cases using Forge-backed test-only
fault decorators, preserving the upstream named expected failures:

| Defect | Required named rejection |
| --- | --- |
| Torn transaction | `journal_storage::atomic_apply_cuts` |
| Non-retryable lost acknowledgement | `journal_storage::atomic_apply_cuts` |
| Lost acknowledged write | `journal_storage::restart_acknowledged_state` |
| Reordered n+1 publication | `journal_storage::restart_commit_sequence` |
| Ignored writer epoch | `journal_storage::writer_epoch_fence` |
| Ignored control generation | `journal_storage::control_generation_fence` |
| Shared body deleted with another holder | `journal_storage::shared_body_last_membership` |
| Released/deferred body served to a non-member | `journal_storage::body_cache_membership` (the new warm-cache probe rejects the decorator first) |

Task 12 corrects the original six-control inventory: released v0.8.0 also
contains the private `StaleGeneration` fault and the named
`control_generation_fence` case. This seventh control is required alongside
the writer-epoch control; revision and epoch may stay unchanged across controls.
The v0.9.0 follow-up introduced the eighth control, NonRetryableLostAck,
unreadable-session isolation, body-cache membership, full-id lookup and exact
membership enumeration. The pinned v0.10.0 suite has 27 named cases per
configuration, including `bounded_collection_protocol`.

For the premature-deletion decorator, deliberately damage the surviving scope's
copy; a correct PerSession release in A must never be labeled faulty just because
B holds the same id. Add Forge-specific assertions that two sessions retain
separate copies and can each reclaim them, that two domains of one session share
one copy that survives one domain's release and is removed only after both
release, and that every remaining holder opens successfully. GC byte evidence uses Forge's
`retained_bytes`, including unserved deferred bytes. Do not force Global receipt
expectations on this adapter or add a Global product mode.

The upstream broken adapters are test-only, not exported APIs. Adapt their fault
shapes over Forge storage in Forge tests; a suite passing the broken backend is
a test failure. Do not weaken assertions, mask errors or ship fault modes.

Also retain runtime equivalence/import/crash coverage through
`JournalCommitFailurePoint::ALL`, arming `engine.fail_next_commit_at`; test SQL
rollback at each pre-commit cut and fresh-engine recovery after `AfterCommit`.
Compare protected materialization exactly; compare ordinary materialization to
the host projection. Keep `assert_native_materialized_checkpoint_schema_guard`
with Forge's real legacy decoder. Cover real AEAD tamper/AAD swaps, prefix
collisions, independent-connection fences, credentials, snapshot-only legacy,
ordinary-only pre-checkpoint sessions, restart/drop and the relevant native-chat
happy-path case. Locally run only the named targets/filters in tasks.md; CI owns
full suites. This spec revision runs no code/build tests.

### D7. Background conversion, fallback import and data-preserving migration

The one `V{YYYYMMDDHHMM}` migration creates D4's tables/indexes only; it neither rewrites
legacy payloads nor edits historical SQL. Every persistent native session becomes
a capable pair on cut-over, without a rollout flag; ephemeral scopes stay
store-less.

The background worker and fallback first writer open use the same runtime v3 import. That import reads/decrypts/validates the legacy
pair, uses stable import operations and makes no model/tool/provider call.
Checkpoint-bearing imports publish **protected exact first**, the authority
point, then ordinary projection. A crash before exact publication leaves v3
authoritative; after it, the runtime repairs a missing ordinary head only from
matching unchanged import evidence. Never overlay stale v3 on an advanced native
head. Preserve the runtime's valid snapshot-only/ordinary-only cases; do not
invent an exact checkpoint or erase restore-critical data to satisfy a table
shape. Exact-first ordering applies whenever there is an exact checkpoint to
publish. Test all cases, not just an idle checkpoint.

Mid-turn CallingModel requests remain frozen, ModelResponseReady and
ToolOutcomeReady results are reused, approvals/interactions survive, and
executing/cache-started states keep their no-replay barriers. Startup suspension
and abandoned-turn finalization follow today's Forge recovery rules.

Keep legacy `load`/`load_latest`, including pre-envelope decoding, until cleanup.
Delete replaced save/CAS/digest-skip paths and make saves typed refusals. Never
write the legacy row during or after import. Native-first open skips legacy when
heads suffice, while legitimate import/repair still loads it. Tombstones suppress
fallback even if an old legacy blob remains.

**This release** converts legacy-only sessions in the background under D8, leaving
lazy import as the fallback when an open arrives before the pass. The measured
B/4000 first import (~3.4 s) is full legacy-to-native conversion, not ordinary
native hydration, and is no longer a latency gate. **Next release**, a separate
change adds a data-preserving migration to NULL migrated payloads after authority
verification, handle dead sessions under the approved retirement rule and drop
`protected_agent_session_state` once empty. This release never deletes legacy rows. Do not pre-create the drop migration.
Older builds refuse unknown applied migrations; downgrade requires a pre-upgrade
backup, never resuming stale legacy state from an upgraded database.

### D8. Lifecycle, collection and retained size

Retire `replaced` topic/profile-rotation sources **and** suspended native sessions
Forge cannot resume: non-Agent-Chat scopes after restart, and Agent Chat rows
that are not their scope's dormant session. Keep resumable dormant chats and Task
sessions that remain reusable; Task completion alone never retires. An open
topic-rotation intent protects its reserved successor's predecessor, including
after the startup sweep, until the intent closes. These are
approved decisions, not conditional tasks or new open questions.

"Dormant" means exactly what `SqliteDb::dormant_native_chat_session_id` would
resume: the newest suspended, unreplaced native row per **(identity, context
scope, profile)**. A scope has one dormant session per profile. The worker calls
that same query for every profile with suspended rows in the scope and never
re-derives the rule, so a chat for another profile stays resumable. "Task
sessions that remain reusable" are Task sessions not suspended by a restart;
after a restart only Agent Chat resumes. If a resume ever reaches a retired
journal, for example after the newer dormant row was replaced, Forge starts a
fresh session through the versioned host rotation transaction, retaining the
identity, scope and profile and allocating a new runtime id. Active reuse checks
retirement too. Ready-status writes check tombstones under the host write lock.
Engine calls stay outside host transactions. If retirement wins between selection
and native open, the backend returns typed `SessionRetired`; the caller rotates
and retries once. A second retirement is a normal turn failure. Tombstones stay;
there is no stale legacy fallback or reuse of a retired runtime id as fresh state.
Explicit `POST /api/v1/agent-sessions/{id}/resume` instead refuses retired history
with HTTP 409 `session_retired`, leaving status/version unchanged and directing
the client to start a new session.

Native terminal outcome/usage construction is independent of shutdown/persist
cleanup. Failed and cancelled turns keep their original outcome and usage envelope
when cleanup returns the runtime's typed unresolved-acceptance fencing conflict;
secondary cleanup failures do not replace a primary failure. Shutdown closes
leases and Forge removes its active registration. Genuine persistence failures
on otherwise successful turns remain errors; every later request opens anew.

Task 21 now schedules the task-22 selection function and task-23 deletion drain
through the existing PeriodicWorkers framework in both server and Solo. The
supervisor owns exactly one worker and its shared-engine clones. Successful
native terminal, fork and abort publications await S2; a maintenance error does
not replace the turn outcome. Post-terminal, rotation, recovery and committed
session-deletion nudges share the pool activity signal with domain-event commits.
After each wake a fixed one-second debounce drains further hints into one pass;
the 60-second tick and durable indexes recover missed wakes. Byte diagnostics
sample the ordinary domain at most once per pass, only with debug logging for
`forge_session_journal`. A retirement error does not prevent collection/orphan
cleanup in the same pass; each distinct cause warns once per cursor lifetime.

A `session-journal-retention` worker uses the existing workers framework, a 60 s
tick and post-commit rotation/recovery nudges. Retire protected then ordinary only
after authenticating both records and obtaining both revision-bound grants;
the engine still enforces no-live-writer and resolved-transfer/dependency
conditions. A race/conflict retries later. A crash
between domains resumes on the next tick. Tombstones persist after collection;
latest-root opens continue to conflict. Operate on published native domains;
legacy-only dead sessions are not eagerly imported merely to retire them. Their
legacy payloads remain subject to the approved next-release cleanup. A domain
with no published root has no native payload to reclaim.

Each tick also collects dirty sessions in both domains, initially
`max_objects = 20_000` per call and a 10 s scheduling budget. The worker checks its
budget between calls/batches rather than claiming to preempt synchronous CPU
inside one engine call. The engine holds its write gate for the whole `collect`
call (runtime `collect_inner`), so every commit and open in that domain waits.
The implemented cursor resumes retirement pages, protected collect, ordinary
collect and cleanup batches after the scheduling budget expires. The worker
records collect duration as a conservative gate-hold upper bound including gate
wait; the scheduling deadline never cancels an in-flight engine control. The
runtime speed-work call cooperatively yields under `JournalGcBudget` (250 ms
soft target, 256 items per unit). `has_more` continues the engine-owned pass;
Forge refreshes the root epoch for each call. `max_objects` still bounds only
membership releases, not previously deferred physical removals. The worker
checks its ten-second deadline and 32-call cap between calls, retaining its
phase when a continuation remains. D10 records each individual call; at or above
1 s remains a release-review blocker, targeting below 250 ms. Forge never
replaces the engine's session/mark cursor with host tracing. Engine collection owns
live-closure calculation, pin and
root-epoch checks, catalog release, dirty clearing and S5. Storage executes
release/receipt accounting and S2 pack work. Live sessions are collected too;
superseded checkpoint/cache/manifest paths otherwise accumulate. Post-terminal
hooks nudge collection and perform geometric S2 maintenance; collection performs
the base repack. No TTL, object age rule or new runtime schema is introduced.

**Background conversion (2026-10-21).** A sibling supervised
`session-journal-conversion` worker runs immediately and every 60 seconds (Skip).
It is separate from retention so a slow import cannot delay retirement/cleanup.
A keyset cursor pages only native host sessions with legacy rows and no ordinary
publication or retirement and a non-NULL legacy payload; exact-only partial
imports remain repair candidates. The worker shares the retirement predicate
and skips sessions it would retire instead of importing dead legacy history.
The durable publication flags are completion markers: restart scans from zero
without reconverting finished sessions. Each pass examines at most eight
candidates, holds one decoded session at a time, yields between sessions and
checks a two-second scheduling deadline before starting the next. Await an
in-flight import to completion; the deadline is not a cancellation or hard bound
on a single conversion (~3.4 s for B/4000 in current evidence).

Per-session admission shared with native turns/forks gives foreground work
priority: conversion uses try-write, skips active sessions, and retries them on
a later sweep. Foreground opens hold read admission; an open already racing an
in-flight conversion waits for it to finish rather than surfacing its writer
conflict. The runtime's own writer fences cover writers outside this host
admission. Active writer conflicts skip/retry, including a lazy importer winning
first. Retryable errors advance the cursor and remain eligible for the next
sweep, including SQLite BUSY/LOCKED during LCM binding. Other per-session errors
advance the cursor and park that owner with its error-kind reason until worker
restart, including validation conflicts.
Foreground lazy import remains available after repair; legacy rows stay intact.
Both paths use the existing v3 importer, shared engines and exact-first validation
with checkpoint recovery deferred, with no provider/tool I/O. Conversion never
persists a synthetic turn or deletes/updates the legacy row. An unreadable row
uses the existing capped warning tracker and stays present. Tracing reports
converted, skipped-active, skipped-unreadable, conflicts/deferred, remaining,
examined, parked-by-reason and scheduling-budget exhaustion. Remaining excludes
parked and retirement-policy skips and includes active skips, reaching zero
when eligible conversion work finishes.

**D-2 isolation.** Discovery pages the clear dirty index and returns every id,
including an unreadable record; it never authenticates an entire discovery page.
The released engine loads the session, reports non-retryable per-session failures
in `unreadable_sessions`, preserves its state and dirty hint, and continues.
Conflicts skip silently; retryable and domain-wide failures still abort. The
same shared engine reports a bad session again on the next call, reading its
record at most once per collect call. Storage diagnostics and compact skip and
log a bad session; `byte_stats` exposes omitted-session counts and readable-only
retained/live/deferred bytes. The trait's scalar retained_bytes is therefore a
partial diagnostic in that case, never a liveness or total-file-size authority.
A capped, non-evicting process tracker retains at most 4,096 runtime ids. It
logs each tracked session once, emits one overflow warning when the cap is
reached, then suppresses further per-session warnings. Aggregate observations
continue counting; per-session warnings include the opaque runtime id, never
contents or keys. The worker retains ids
only for deduplication within the current pass. Its `preserved_sessions` and
`unreadable_sessions` counts cover that pass, so repaired/deleted sessions and
worker restarts cannot grow lifetime cursor state.
An unreadable candidate's record prevents either retirement domain from changing.
An unreadable OTHER session may still protect a retirement candidate through
unknown dependency metadata: the engine refuses that retirement, and Forge
records a deferral while continuing collection, preserving partial domain progress.
It must not label that healthy candidate unreadable or bypass dependency safety.

Project/Agent/chat deletion cascades per-session metadata and memberships. After
commit, nudge bounded physical cleanup to remove **all** packs owned by the
deleted Forge session, including below-threshold deferred bytes; no other
session can hold those packs, and both domains' memberships cascade with the
`agent_session` row. Find owner/orphan candidates without
decryption in pages of up to 5,000 pack ids. A candidate has a deleted owner or no
membership row; recheck ownership and absence of membership under the write lock
before deleting. Delete at most 32 packs or 8 MiB of retained canonical bytes per
transaction and resume after restart until drained. Never reapply the 25%
threshold to deletion cleanup. Other sessions' independent copies remain intact.
The indexed orphan marker is the durable nudge. A pool-scoped activity signal
also wakes after committed session deletions, using the existing domain-event
commit/release hooks; unrelated activity wakes are harmless. Rollback does not
wake, and startup/ticks scan the durable index from zero. The drain reads one
candidate page, acquires its own write transaction, rechecks owner labels and
absence of membership, and reports a cursor for the next bounded batch. A runtime
id reused by a new `agent_session` is refused with `journal pack owner mapping
changed` while old-owner packs remain; the same shared engines can open it once
those packs have drained. No legacy fallback restores the deleted owner.

This path is explicit session deletion, not an alternate tracing collector.
It emits physical copy/byte totals separately, never a synthetic engine apply
receipt for removal outside apply. Tombstone records remain until their
`agent_session` is itself deleted. Retirement does not promise
zero disk bytes: tombstones, in-window replay results and any valid
pins can remain. Measure these separately.

**Observability.** Storage and the worker emit structured `tracing` events
(target `forge_session_journal`) with counts and durations only, never contents,
ids or keys. Events cover:

- each apply's kind, duration, write-lock hold and receipt;
- S2, compact and cleanup rewritten/deleted packs and bytes;
- collect reports and gate hold, canonical retained/live/deferred bytes, and
  cumulative physical receipt totals separate from membership releases;
- lazy directory/body loads and cache charge, hits, misses and eviction counts;
- retirement outcomes and conflicts;
- decrypt/authentication failures, logged once per session.

Long write holds also surface through `db::begin_immediate`'s existing
5-second warning. No API, event payload or metrics endpoint changes.

The actual **PerSession S1–S5** reference tables below replace the earlier S1–S4
results and S5 projections. Source: rt-e7 report (`refactor/rt-e7/report.md` historical receipt),
raw measurements (`refactor/rt-e7/measurements.json` historical receipt) and runtime
D8.4. Collection once at the end (selected rows):

| Reference fixture | N | Turn | Legacy L2 bytes | Pack bytes before / after collection | After / L2 |
| --- | ---: | --- | ---: | ---: | ---: |
| A | 1000 | plain | 53,248 | 344,064 / 311,296 | 5.84615× |
| A | 4000 | plain | 204,800 | 1,208,320 / 1,179,648 | 5.76000× |
| A | 4000 | tool | 208,896 | 1,224,704 / 1,179,648 | 5.64706× |
| B | 100 | plain | 40,960 | 135,168 / 102,400 | 2.50000× |
| B | 1000 | plain | 335,872 | 622,592 / 577,536 | 1.71951× |
| B | 4000 | plain | 1,359,872 | 2,387,968 / 2,183,168 | 1.60542× |
| B | 4000 | tool | 1,392,640 | 2,719,744 / 2,494,464 | 1.79118× |
| C | 1000 | tool | 942,080 | 11,993,088 / 1,228,800 | 1.30435× |

Collection **after every turn**, plus a final sweep after shutdown (all measured
per-turn rows):

| Fixture | N | Turn | L2 bytes | Final bytes | Final / L2 | Cumulative pack writes | Largest collection write |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| B | 1000 | plain | 335,872 | 577,536 | 1.71951× | 845,627 | 33,297 |
| B | 1000 | tool | 339,968 | 622,592 | 1.83133× | 1,491,335 | 288,771 |
| B | 4000 | plain | 1,359,872 | 2,183,168 | 1.60542× | 3,057,083 | 313,815 |
| B | 4000 | tool | 1,392,640 | 2,494,464 | 1.79118× | 3,540,392 | 323,123 |
| C | 1000 | tool | 942,080 | 1,204,224 | 1.27826× | 144,922,993 | 368,800 |

The supplemental **C/N=4000 collect-at-end** run measured **4,886,528 bytes /
1.22863× L2** (L2 = 3,977,216), cumulative pack writes **487,612,606 bytes** and
largest collection write **9,208,323 bytes**. This last number is a whole
collection call across multiple capped packs, not one oversized pack. No
C/N=4000 per-turn result is claimed. Actual S5 pruning reduces C/N=1000 operation
results from 4,002 to 130, and C/N=4000 from 16,002 to 130.

For B/N=4000, collect-at-end and per-turn both measure **1.60542× plain /
1.79118× tool** (about 1.61× / 1.79×). Both pass the reference 2.0× gate and
both miss the optional 1.5× aim. B's per-turn cumulative pack writes grow 3.615×
plain / 2.374× tool for a 4× increase in N; C/1000 has 250 measured turn samples.
The reference reports O(N) writes in admitted bytes for these bounded fixtures,
not whole-history rewrites per turn. Keep Forge's per-turn collection cadence.

Pack-write counters include committed object/operation payloads and nonces,
including intermediate rewrites; they exclude metadata, SQLite page/WAL overhead
and VACUUM. Largest collection write is the largest per-domain collect-call
counter delta. Final physical sizes include all persisted tables after
checkpoint/VACUUM and empty-schema subtraction. These are measured SQLite/zstd
S1–S5 results with modeled seal lengths, **not Forge integration or real AEAD**.
The reference shares equal bodies between a session's domains; Forge does too
since the 2026-10-21 amendment, but the reference's numbers still do not
substitute for Forge's own measurement. Forge must still pass D10 through
production storage. A's tiny-message overhead stays report-only; schema 5 stays
out of scope.

Task 33 uses explicit `JournalGcBudget` values (runtime defaults: 250 ms soft
target, 256 items per unit) and refreshes `expected_root_epoch` between bounded
calls. The retention phase continues while `has_more` within the existing
10-second scheduling deadline, at most 32 calls per pass; zero-work reports
back off. Conversion keeps eight candidates and two seconds, without cancelling
an in-flight import. The report's after-turn and final shutdown sweeps drain
continuations and durable dirty hints with a 10,000-call assertion bound.
Incomplete live membership in `unreadable_sessions` uses the same capped
warning/health path as authentication failures. A separate non-evicting tracker
caps over-capacity warnings at 4,096 sessions and includes each tracked runtime
session id. Reports count distinct over-capacity sessions per pass and preserve
the union with unreadable sessions. Byte diagnostics run once per finished
domain phase. The benchmark records each call rather than treating a whole drain
as one held gate.

### D9. Rollout and documentation

Ship both journal capabilities directly after the runtime release gate, storage
conformance and the D10 disk, turn and collect gates pass. No old/new adapter layering or selection
flag. Shared engines survive per-turn Runtime replacement. A newly created native
session writes no legacy row; existing sessions convert in the background with lazy import on open as fallback. Keep the existing
single-process `runtime.lock`, daemon boundary and credential store.

Implementation updates `docs/architecture.md` (storage/engine split, packs,
projection, worker, pin/replay semantics), `docs/getting-started.md` (import,
downgrade and key continuity), `docs/performance-testing.md` (fixture and actual
Forge table), and `CHANGELOG.md` under `Unreleased`:

- `### Breaking`: journal on-disk layout/newer-database guard and backup-based
  downgrade; concurrent writer conflicts; operation replay guaranteed only for
  revisions at or above current−64 (inclusive floor) plus the current writer
  epoch's last operation; older pruned replays conflict.
- `### Changed`: delta persistence through the runtime engine, encrypted packs,
  ordinary policy projection, retirement of replaced/unresumable sessions and
  bounded collection. Include measured Forge numbers after D10, not an invented
  99% saving or the rejected 4× target.

This document records proposed code/doc work only. The existing 2026-10-19 Owner
Decisions entries below are preserved verbatim as the historical receipt,
including then-candidate levers and older wording; new entries record later owner decisions, including the 2026-10-21 revision below. Settled constraints remain L2, storage-only S1–S5,
the runtime's 64-revision window (inclusive floor plus the
current writer epoch's last operation, D4) and no runtime schema 5.

### D10. Forge measurement and release gate

Add a Forge-owned ignored test/bench in
`crates/agent-host/tests/session_journal_bench.rs` using the **production
ForgeJournalStorage**, sqlx and real XChaCha20-Poly1305. The unpublished runtime
bench is a fixture/method reference, not a linked production dependency. Include a
small non-ignored N=100 smoke. Full measurement is one named ignored release
case, not a workspace test run.

Reuse the fixed seeded fixture definitions and cache assertions from the runtime
bench: A tiny seeded messages; B realistic 100–800 B user text, 200–2000 B assistant
text and 0.5–8 KiB tool JSON, N=100/1000/4000, plain and tool two-turn shapes;
C grows to 1000 messages over 250 tool turns with a new writer per turn and a
32-manifest window; report the supplemental N=4000/1000-writer-epoch shape too.
Do not tune the seed/content to the gate. Record the ordinary projection and
the within-session domain sharing policy; report an identity-policy diagnostic for
comparison with rt-e7 without making identity the production policy or treating
the reference's modeled sealing as Forge's real AEAD.

For each case, create isolated file DBs with the same Forge schema/session setup:

1. **L2:** generate the final idle legacy row with Forge's actual legacy
   encode/compress/seal and save ordering/digest rules, in test-only baseline
   support retained when production legacy writes are deleted. Measure both
   ciphertext columns and assert/report two sealed blobs, not 2× a guessed
   one-blob length. Also report L1 (checkpoint-only), without gating on it.
2. **Native:** run the identical fixture through the engines over
   ForgeJournalStorage, with S1–S5, terminal maintenance and collection to a
   stable dirty/replay state. Measure immediately before collection, after it,
   and after retirement; include operation tiers, pins/catalog and opaque engine
   metadata. Confirm the released window edge (current−64 kept, current−65
   pruned), the single current-epoch last-operation exception and restart.
3. For **each** layout, run `wal_checkpoint(TRUNCATE)`, `VACUUM`, and a final
   truncated checkpoint. Physical retained bytes = `page_count × page_size`
   minus that same layout's empty-schema/session-setup baseline. Verify file
   length/dbstat totals; include rows, indexes, real nonce/tag/framing overhead.
   Do not divide canonical bytes, compressed payload lengths or logical
   `retained_bytes` by L2. Account for baseline common rows identically.
4. Report the native-only storage ratio **and** total upgraded footprint with
   the retained lazy-import legacy row. The hard gate measures steady native
   persistence versus legacy; do not hide the temporary old-plus-new footprint
   until next-release cleanup or count stale legacy data as a pack saving.
5. **Production cadence:** run C and both B variants with the production
   schedule: the S2 hook after every terminal turn and a collection after every
   turn, as the worker nudge does, plus a final sweep after shutdown. Report
   cumulative committed object/operation pack payload + nonce writes and the
   largest single collection-call write by that same counter, matching rt-e7.
   Separately report cumulative WAL frame bytes (frames × page size), the largest
   single pack rewrite and the final physical size,
   against the legacy store's cumulative writes for the same run. The
   post-collection gate ratio is taken after this cadence, not only after one
   final collection.

**Hard disk gate:** both B/N=4000 variants after collection must have
`Forge_native_physical_bytes / Forge_legacy_L2_physical_bytes ≤ 2.0`.
Aim for 1.5×; do not promote it or A/C/small-N diagnostics into new gates. A zero
baseline yields an undefined diagnostic ratio, not artificial padding. Failing
the Forge gate blocks adoption even if the reference passed. Adjust only
storage-level packing/compression/collection; do not silently introduce schema 5
or change the denominator, projection, seed or acceptance threshold.

Also report lazy session hydration time and bounded session-directory/body-cache
charge with many unrelated sessions present, per-turn pack bytes/WAL,
turn/open/probe/import wall time, longest write
transaction, longest `collect` call (engine gate hold) with 100 dirty sessions,
geometric-merge write amplification, freelist pages, engine object counters,
physical-copy receipt totals and canonical retained/live/deferred byte differences
versus file savings, memo/cache charge/hit/miss/read/eviction/oversize counts,
cold-validation time (so the 128 MiB per-engine memo budget can be raised on
evidence), resident memory (including the observed 1.24× charge headroom), and
post-retirement metadata/operation bytes. Retain the existing focused
performance checks under the 2026-10-21 owner decision: native journal overhead
per turn ≤ approximately 250 ms over legacy at B/N=4000 and ≤ approximately
50 ms over legacy at N=100, plain and tool, release builds at per-turn cadence.
Every single `collect` call / engine gate hold must stay below 1 s (target below
250 ms). N≥1000 delta writes ≤10% of legacy remains a tracked check including S2
and purge rewrites. Import is no longer latency-gated: move conversion to D8's
background pass and retain lazy fallback. Record every pass/fail. Speed work
belongs to agent-runtime change `speed-up-journal-hot-path-2026-10-21`, phases
P1/P2/P4 (P3 optional); rerun Forge's gate once that runtime release is pinned.
Current sharing evidence: disk PASS at 1.60843× / 1.79118×; B/4000 native turns
1,685.62 / 2,547.75 ms versus legacy approximately 229 / 427 ms, so overhead
approximately 1,457 / 2,121 ms FAIL; C/4000 max collect 2,106.75 ms FAIL pending
runtime. Turn/collect budget misses remain release-review blockers. No
import-latency claim is implied by background scheduling. Output JSON plus a Markdown table in
`docs/performance-testing.md`. This revision adopts the measured method; it does
not claim Forge has run or passed it.

### Task 33 measurement receipt (2026-10-24)

Forge pins runtime **v0.10.0** at `c036af7b4635cf7d76451aaa666a9df87405ca1a`. The historical measurements below used `b0a8c90e96ccb85e4642a119117a8fd5d3a06993`, whose Rust source is identical to the release. Forge forwards preparation hints and opts into equivalent incremental projection. Full reports used unchanged fixtures, production projection, real AEAD/sqlx, same-run legacy L2, S2/per-turn collection and bounded final drains.

| Fixture | t24 disk/L2 | writes disk/L2 | Final disk/L2 | Delta writes | Native / legacy last turn ms | Overhead ms / check | Max collect ms incl. retirement |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| A/100 plain | 13.00000 | 10.50000 | 10.00000 | 434.868% | 43.99 / 3.07 (14.35x) | 40.93 / PASS | 6.78 |
| A/100 tool | 13.50000 | 9.50000 | 9.50000 | 413.172% | 77.70 / 5.67 (13.70x) | 72.03 / FAIL | 10.08 |
| A/1000 plain | 9.53846 | 6.46154 | 6.46154 | 44.206% | 56.92 / 13.91 (4.09x) | 43.02 / n/a | 17.32 |
| A/1000 tool | 9.46154 | 6.46154 | 6.38462 | 46.276% | 104.26 / 25.11 (4.15x) | 79.16 / n/a | 17.14 |
| A/4000 plain | 9.08000 | 5.78000 | 5.78000 | 11.258% | 120.11 / 50.08 (2.40x) | 70.03 / n/a | 64.18 |
| A/4000 tool | 8.90196 | 5.68627 | 5.66667 | 11.893% | 180.09 / 92.83 (1.94x) | 87.26 / n/a | 63.90 |
| B/100 plain | 4.20000 | 2.90000 | 2.90000 | 70.803% | 51.12 / 7.69 (6.65x) | 43.43 / PASS | 7.92 |
| B/100 tool | 4.00000 | 2.72727 | 2.54545 | 121.206% | 98.47 / 12.32 (7.99x) | 86.16 / FAIL | 13.90 |
| B/1000 plain | 2.96341 | 1.73171 | 1.73171 | 10.336% | 108.16 / 48.98 (2.21x) | 59.18 / n/a | 28.12 |
| B/1000 tool | 3.28916 | 1.87952 | 1.87952 | 10.173% | 194.69 / 89.04 (2.19x) | 105.64 / n/a | 31.90 |
| B/4000 plain | 2.84036 | 1.61145 | 1.61145 | 5.538% | 311.94 / 201.96 (1.54x) | 109.98 / PASS | 130.31 |
| B/4000 tool | 3.23235 | 1.79412 | 1.79412 | 4.009% | 511.41 / 377.76 (1.35x) | 133.65 / PASS | 124.01 |
| C/1000 tool | 2.43478 | 1.50000 | 1.40000 | 9.317% | 601.03 / 264.11 (2.28x) | 336.92 / n/a | 45.88 |
| C/4000 tool | 2.23765 | 1.34568 | NOT MET: unfinished | n/a | n/a | n/a | n/a |

Packing candidate: **kept**. C/1000 improves from 1.48261x to 1.40000x L2 (33 to 30 packs). Unit 14/14, storage 30/30, conformance 18/18 and the added mixed-domain S2/collection/reopen regression pass; no completed baseline check becomes a new failure. The C/4000 supplemental run remains unfinished and explicitly NOT MET; no pass is claimed for that shape.

100-dirty-session drains: protected 248.75 ms / 3 calls, ordinary 249.09 ms / 2 calls; both below one second. Target hydration 4.25 ms, 4 target packs out of 804; unrelated packs are excluded. Isolated B/100 plain identity diagnostic: 118,784 bytes, 2.90000x L2. Completed production rows have 26 cold misses and 26 warm hits in the explicit probes; cold validation spans 19.50–1636.28 ms. Internal scoped probe counters: {'rt.memo.hit': 26, 'rt.memo.miss': 26}. Estimated probe memo charge spans 1,047,126–65,150,820 bytes across both engines. Peak reported process RSS through completed cases 1767.94 MiB. These are probe counters/estimated charge and process RSS, not whole-run counters or allocator heap. Keep 128 MiB per engine unchanged.

**NOT MET:** Fixed steady-turn overhead: A/100 tool, B/100 tool. Mean across both turns (including cold first-turn work) fails its recorded fixed check for: A/100 plain, A/100 tool, B/100 plain, B/100 tool, B/4000 plain, B/4000 tool; keep it separate from the steady delta-turn metric. B/1000 delta-write <=10% remains tracked; see measured percentages above. The 1.5x disk aim is separate from the passing 2.0x B/4000 hard gate. Unfinished production shapes: C/4000 tool. A timed-out full report is not a completed gate; no missing C/4000 values are inferred. Exact allocator heap is unavailable through the runtime API; telemetry is estimated charge/scoped counters plus RSS. Task 24 remains open for that gap. All Owner Decisions below remain verbatim.

The v0.10.0 pin, revision constant and dependency contract are aligned; the historical benchmark receipt predates the tag and no benchmark rerun is claimed.


## Risks / Trade-offs

- The consumed prerequisite is `v0.10.0`. The release gate verifies the tagged contract,
  including upstream N1/N2, gate and S5 fixes; Forge must not work around them by
  owning engine semantics.
- Validation/policy CPU can still be O(N); R4 avoids repeated cold closure reads
  across writers, not all per-commit materialization. Warm-runtime work is out of
  scope. Pack/base-repack latency and the two memo budgets need measurement.
- Both logical domains remain encrypted; membership bits and scalar counts expose
  structure and sizes. No clear content digest is stored. Key rotation remains
  unsupported as today; key_revision is recorded on every sealed unit.
- Lazy import temporarily retains old and new bytes. Disk reports show that peak
  separately; next-release cleanup is required to remove the legacy table.
- Within-session domain sharing (2026-10-21) removes the ordinary domain's
  duplicate copies; separate sessions still keep independent copies and D10
  measures the result through Forge's actual storage. No global index is
  required by the amended runtime conformance. Shared packs are session-sealed:
  domain authority over a shared slot is the clear membership bitmap, and bitmap
  overlap reveals how much protected content also appears in ordinary. A local
  file writer could grant an ordinary bit on a protected-only slot. All three are
  accepted under the existing local-attacker scope (D3); admission still requires
  the ordinary engine to present the exact projected bytes.
- Deferred dead bytes must remain unreadable and counted in retained-byte
  diagnostics until a threshold/retirement/deletion purge. Atomic remaps, exact
  physical receipts and every conformance configuration gate this behavior.
- Per-engine memo charge is estimated, not a heap bound; 128 MiB per engine /
  256 MiB combined charge may retain about 158.72 MiB per engine / 317.44 MiB
  combined at the review's 1.24× observation, plus writer/cache memory. Memo misses
  cost cold validation, affecting speed, never correctness; D10 records hit/miss
  counts, cold-validation time and resident memory before raising the budget on
  evidence. A separate engine loses shared leases/memo even with D-1's warm-read
  correctness check; reuse clones.
- Ordinary redaction must preserve restore-critical data and runtime exact fields;
  protected equality and ordinary-policy equality are different test assertions.

## Migration Plan

The rebased workspace already pins agent-runtime **0.7.0 + U9 groups C/D** at
`1de6d7d` (`1de6d7d34d2cfe4fc35791bca440932602539cf7` in Cargo.lock), as recorded
by workspace Cargo.toml/Cargo.lock. Execute tasks 1–26 in order: bump that pin to
the first released runtime containing the engine (task 1); test support and
six-table schema;
codec/reads/apply/pins/operations/receipts/repack; conformance and negative cases;
engine wiring/probes/legacy refusals; import/crash/integration checks; approved
retirement and deletion maintenance; measurement; final docs/CHANGELOG. Each task
is one ≤3 h Codex job with named focused tests. No code is part of this revision.

## Open Questions

None.

## Owner Decisions (2026-10-19; additions 2026-10-20)

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
- **Q4. Ordinary diagnostic redaction (2026-10-20): conservative.**
  Use the conservative D3 policy: preserve all restore-critical data and
  runtime-mandated exact fields; omit sensitive optional diagnostics. Broader
  free-text redaction needs explicit data examples and resume-equivalence
  evidence. Do not promise that runtime cache/manifests contain no content.
  Protected exactness is unchanged.
- **Q5. Memo budget (2026-10-20): 128 MiB per engine.** Configure
  128 MiB estimated memo charge per engine, 256 MiB combined for protected
  and ordinary. Forge is a local, single-user app; no data shows the larger
  runtime default is needed. Memo misses cost only a cold validation: speed,
  never correctness. The performance task must record memo hit/miss counts
  and cold-validation time so the budget can be raised on evidence. The
  charge is an estimate: the review measured retained heap up to 1.24× charge,
  roughly 159 MiB per engine / 317 MiB for both at full charge, not a heap cap.
  Keep one engine per storage and domain, cloned into every runtime and the
  collector.

## Owner Decisions (2026-10-21)

- **Q6. Fixed latency budgets supersede D10's relative gates.** Native journal
  overhead per turn is ≤ approximately 250 ms over legacy at B/N=4000 and
  ≤ approximately 50 ms over legacy at N=100, plain and tool, release builds
  with per-turn cadence. Every single collect call / engine gate hold is <1 s
  (target <250 ms). Disk after collection remains ≤2.0× actual L2, aim 1.5×.
  Delta writes ≤10% of legacy for N≥1000 remains a tracked check.
- **Q7. Background conversion now; lazy fallback; legacy removal next release.**
  Import is no longer gated on latency. Forge converts legacy sessions to native
  in the background so the first open need not perform full conversion. This
  revises Q3's lazy-import schedule: lazy import remains the fallback when the
  session opens before the pass reaches it. Keep legacy rows and defer table
  removal to the next release.
- **Q8. Runtime owns speed work.** Use agent-runtime change
  `speed-up-journal-hot-path-2026-10-21`, phases P1/P2/P4; P3 is optional.
  Rerun Forge's gate after pinning the runtime release containing that work.

## Owner Decisions (2026-10-24)

Waive and ship: the fixed turn-latency misses (A/100 tool 72 ms and B/100 tool 86 ms vs ~50 ms), B/1000 delta writes 10.3% vs 10%, and the unfinished C/4000 run are accepted for this release and fixed in a follow-up after merge.
