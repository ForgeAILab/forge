---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-24T07:52:00Z
---

## Why

Forge rewrites, compresses and encrypts complete native session snapshots and
checkpoints at every persistence barrier. The runtime now has a storage-generic
`JournalEngine<S>` in release `v0.10.0`; Forge should consume that engine rather
than implement journal semantics. The reference packed SQLite adapter measured
fixture B/N=4000 at **1.60542× L2 plain and 1.79118× L2 tool**, both with
collect-at-end and per-turn collection.
L2 is today's actual idle legacy row with **two sealed blobs**, not the earlier
one-blob assumption. These are measured **PerSession S1–S5** results from
`historical-receipts/rt-e7/report.md`, with modeled seal lengths and
within-session domain sharing. Forge now shares bodies between a session's two
domains too (2026-10-21 amendment). Its real-AEAD/sqlx B/4000 disk gate now
passes at 1.60843× / 1.79118×; turn/collect gates await runtime speed work.
The reference per-turn C/N=1000 is **1.27826× L2**;
the supplemental C/N=4000 collect-at-end run is **1.22863× L2**.

## What Changes

- **Released runtime gate first.** Pin every runtime package and testkit to the
  same immutable revision of **the first agent-runtime release containing
  extract-journal-storage-engine**. Verify the complete trait/receipts, R1–R7,
  PerSession/deferred-removal contract, N1/N2 fixes, native-first open, bounded
  extension reads, engine-owned 64-revision replay pruning, D-1's reader-operation
  authority check and the reviewer's futures-mutex gate correction. No invented
  version, branch-only production pin or Forge semantic workaround. The rebased
  baseline pinned `1de6d7d` (0.7.0 + U9 groups C/D); task 1 adopted the first
  engine release. The retention follow-up now pins all eight runtime/testkit
  packages to `v0.10.0` at `c036af7b4635cf7d76451aaa666a9df87405ca1a`, including
  bounded resumable collection,
  per-session collection isolation, retryable ambiguous acknowledgements and
  failed-acceptance rollback/fencing.
- **Forge implements only `JournalStorage`.** Two long-lived
  `JournalEngine<ForgeJournalStorage>` instances share one sqlx-backed storage.
  Create exactly one engine per storage/domain and hand clones to every runtime
  instance and collector, sharing the memo and lease registry. Configure the
  approved **128 MiB estimated memo charge per engine**, 256 MiB combined.
  Forge is a local, single-user app; no data shows the larger runtime default is
  needed. Memo misses cost only cold validation, affecting speed, never
  correctness; record hit/miss counts and cold-validation time before raising the
  budget on evidence. The review saw retained heap up to 1.24× charge, roughly
  159 MiB per engine / 317 MiB combined, so this is not an RSS cap (design D2).
  The runtime owns validation, projection/re-encoding, leases, closures,
  dependencies, fences, retirement and incremental collection selection. Forge
  supplies SQL enforcement, framing, encryption and lifecycle scheduling.
- **Six tables, no per-object SQL layout.** One additive migration
  `V{YYYYMMDDHHMM}__session_journal.sql` creates session records, pin, catalog, pack,
  membership and operation_pack tables. Allocate the unique UTC migration version
  when implementing; never edit historical migrations or discard user rows.
- **Per-session pack storage.** S1 batches new objects into one zstd level-3 pack
  (cut at a 1 MiB logical cap); S2 merges geometric tiers after terminal
  publication and repacks only fully live packs below 64 KiB or survivor packs
  purged by that apply during collection, each excluding packs fully held only by another
  domain while permitting fully live mixed packs with no complete holder; stable large and deferred-dead packs stay sealed. Imports and every repack obey the cap. S3 keeps kind-ordered bodies
  and prefix/offset indexes inside
  sealed packs (separate index and body units), with full-id verification.
  Storage declares `JournalBodySharing::PerSession` and deduplicates only within
  one runtime session: its two domains share identical bodies through separate
  membership bitmaps, while sessions never share copies. Lazy scoped indexes
  decode only the requested session's packs; storage
  open decodes no pack units. S4 packs complete original operation
  results; the engine selects S5 pruning (inclusive floor current−64 plus the
  current writer epoch's last operation), which storage executes only on an
  explicit collection directive. Serialized operation packs also have a 1 MiB cap.
- **Keep Forge's cipher and keys.** Real XChaCha20-Poly1305, fresh 24-byte nonce,
  authenticated row binding and existing key material/key_revision. Full object ids
  and prefixes stay inside sealed packs; no per-object HMAC/index is needed.
  Small catalog/pin lookup keys remain domain-separated HMACs so they do not leak
  clear digests. The reference's simulated sealing is not production crypto.
- **Two logical domains.** Protected state stays exact. Ordinary state uses a
  conservative D3 redacting host policy decided on 2026-10-20: preserve all
  restore-critical values and runtime-mandated exact fields; omit sensitive
  optional diagnostics. Runtime cache/manifests may contain content; protected
  exactness is unchanged. Both domains are encrypted; ordinary leases can never
  read protected-only slots. Credentials
  remain excluded from both domains.
- **One transaction per apply.** Use `db::begin_immediate` through sqlx, check
  the full `JournalFence` revision, writer epoch and control generation in SQL,
  require a replacement session record advancing generation exactly once, stage
  all components and cuts, commit once. Release membership immediately; sealed packs may retain unserved dead
  bytes below 25% of canonical body bytes. The apply reaching 25%, an empty scope,
  or retired-session collection purges them; bounded deletion cleanup drains the
  deleted session's packs. Return `JournalApplyReceipt` physical copy counts and
  canonical bytes at removal time, including earlier deferred bytes; clearing
  bits and moving live bodies count zero. Implement `retained_bytes` including
  deferred bodies. Await durable engine controls; no volatile sync polling on Tokio.
- **Background, exact-first conversion with lazy fallback.** Keep legacy read/decode methods as the runtime's
  v3 source, including unfinished and snapshot-only sessions. Publish protected
  first when a checkpoint exists, repair partial imports without provider/tool
  I/O, and refuse legacy saves. Native-first open skips unnecessary legacy loads.
  Convert legacy-only sessions now through a bounded supervised background pass;
  skip active writers and preserve unreadable rows. Lazy import on open remains
  fallback. Keep legacy rows and remove the table next release. Import is no
  longer latency-gated.
- **Probes and lifecycle.** Read scalar native publication/checkpoint/tombstone
  metadata; read LCM overhead through `JournalReader::extension`. Retire replaced
  and unresumable suspended sessions, keep resumable dormant chats and reusable
  Task sessions. Schedule bounded dirty-session collection, terminal repacking and
  deletion cleanup without a TTL. An unreadable session is preserved and logged
  once per tracked id up to a capped, non-evicting 4,096-id process tracker,
  followed by one overflow warning and per-session suppression; aggregate counts
  and healthy collection continue. Unknown dependency metadata can
  conservatively defer other retirements; Forge never overrides that refusal.
- **Acceptance.** Run `assert_journal_storage_conformance` unedited with repacking
  off/on and reopened storage, all eight Forge-backed broken-storage cases, real
  import/equivalence/crash cuts and named integration checks. Forge's own bench
  runs B/N=4000 through production storage against L2 from the real legacy row;
  both plain/tool ratios must be ≤2.0× after per-turn collection (aim 1.5×).
  Report cumulative pack writes and the largest collection-call write alongside
  WAL/timing/memory evidence; the runtime's measured counters are in design D8.
- **BREAKING:** the new on-disk schema requires backup-based downgrade; a second
  concurrent writer conflicts; pruned operation replays outside the approved
  64-revision window (inclusive floor plus the current writer epoch's last
  operation) conflict. Record these in CHANGELOG during
  implementation. No feature flag or compatibility aliases.

Out of scope: U10/LCM-table consolidation, warm runtimes, runtime schema 5 and
legacy-table removal in this release. The existing Owner Decisions entries remain
verbatim; later entries record the 2026-10-20 memo/redaction decisions and
the 2026-10-21 background-conversion and fixed-budget revision.

## 2026-10-21 acceptance revision

D10 now uses fixed native per-turn overhead budgets: ≤~250 ms over legacy at
B/N=4000 and ≤~50 ms at N=100, plain/tool, release, per-turn cadence; each
collect/gate hold <1 s (target <250 ms). Disk stays ≤2.0× L2 (aim 1.5×), and
N≥1000 delta writes ≤10% remains tracked. Current sharing results pass disk
(1.60843× / 1.79118×) and fail turn/collect pending runtime work. Import moves
out of the latency gate into background scheduling. Speed work belongs to
agent-runtime `speed-up-journal-hot-path-2026-10-21` P1/P2/P4 (P3 optional);
rerun Forge's gate after pinning that release. The earlier Owner Decisions
remain an unmodified historical receipt; the new entry supersedes Q3's timing.

## Impact

- Affected specs: `native-session-journal`, `session-journal-retention`.
- Future implementation: workspace runtime pin/lockfile and agent-host testkit;
  `crates/agent-host/src/session_journal/`, `protected_store.rs`, `native.rs`;
  one new DB migration and scalar DB probes; services-agents retention/recovery
  wiring and nudges; continuity caller and focused native-chat tests.
- Future docs: `docs/architecture.md`, `docs/getting-started.md`,
  `docs/performance-testing.md`, `CHANGELOG.md`.
- Explicit Agent Session resume refuses retired history with `409 session_retired`;
  automatic selection/open races rotate once to a fresh runtime identity. Other
  public surfaces and `checkpoint_present` meaning stay stable. The disk
  layout gains six tables; no per-object body/index or operation-result rows.
- Task 1 implements only the released dependency pin, release contract test and
  associated documentation/spec alignment. Native storage adoption remains in
  subsequent tasks; the runtime repository remains read-only.
- Evidence and detailed contract: design D2–D10; reference measurements are inputs,
  not a claim that Forge's release gates already pass.
