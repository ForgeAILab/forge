---
created_at: 2026-10-19T00:00:00Z
updated_at: 2026-10-19T21:47:35Z
---

## Why

Forge rewrites, compresses and encrypts complete native session snapshots and
checkpoints at every persistence barrier. The runtime now has a storage-generic
`JournalEngine<S>` on unreleased branches; Forge should consume that engine rather
than implement journal semantics. The reference packed SQLite adapter measured
fixture B/N=4000 at **1.49096× L2 plain and 1.62353× L2 tool after collection**.
L2 is today's actual idle legacy row with **two sealed blobs**, not the earlier
one-blob assumption. These are S1–S4 reference results with modeled seal lengths;
S5 is projected there, and Forge must still prove its own real-AEAD/sqlx gate.

## What Changes

- **Released runtime gate first.** Pin every runtime package and testkit to the
  same immutable revision of **the first agent-runtime release containing
  extract-journal-storage-engine**. Verify the complete trait/receipts, R1–R7,
  N1/N2 fixes, native-first open, bounded extension reads, engine-owned 64-revision
  replay pruning and the reviewer's futures-mutex gate correction. No invented
  version, branch-only production pin or Forge semantic workaround.
- **Forge implements only `JournalStorage`.** Two long-lived
  `JournalEngine<ForgeJournalStorage>` instances share one sqlx-backed storage.
  The runtime owns validation, projection/re-encoding, leases, closures,
  dependencies, fences, retirement and incremental collection selection. Forge
  supplies SQL enforcement, framing, encryption and lifecycle scheduling.
- **Six tables, no per-object SQL layout.** One additive migration
  `V{NNN}__session_journal.sql` creates session records, pin, catalog, pack,
  membership and operation_pack tables. Allocate the unique UTC migration version
  when implementing; never edit historical migrations or discard user rows.
- **Per-session pack storage.** S1 batches new objects into one zstd level-3 pack
  (cut at a 1 MiB logical cap); S2 merges geometric tiers after terminal
  publication and repacks small tiers into capped kind-ordered packs during
  collection, so a purge rewrites one capped pack, not the session; S3 keeps kind-ordered bodies and prefix/offset indexes inside
  sealed packs (separate index and body units), with full-id verification.
  Membership bitmaps preserve domain and session isolation; one physical copy per
  id is shared by all holders, found through a RAM prefix index rebuilt at open
  from the sealed index units. S4 packs complete original operation
  results; the engine selects S5 pruning (inclusive floor current−64 plus the
  current writer epoch's last operation), which storage executes only on an
  explicit collection directive.
- **Keep Forge's cipher and keys.** Real XChaCha20-Poly1305, fresh 24-byte nonce,
  authenticated row binding and existing key material/key_revision. Full object ids
  and prefixes stay inside sealed packs; no per-object HMAC/index is needed.
  Small catalog/pin lookup keys remain domain-separated HMACs so they do not leak
  clear digests. The reference's simulated sealing is not production crypto.
- **Two logical domains.** Protected state stays exact. Ordinary state uses a
  redacting host policy under runtime constraints; restore-critical values and
  runtime-mandated cache/manifests remain exact where required. Both domains are
  encrypted; ordinary leases can never read protected-only slots. Credentials
  remain excluded from both domains.
- **One transaction per apply.** Use `db::begin_immediate` through sqlx, check
  `expect` revision and writer epoch in SQL, stage all components and cuts, commit
  once. Release the last membership and remove the physical body in that same
  transaction. Return `JournalApplyReceipt` counts/canonical bytes, not compressed
  pack savings. Await durable engine controls; no volatile sync polling on Tokio.
- **Lazy, exact-first import.** Keep legacy read/decode methods as the runtime's
  v3 source, including unfinished and snapshot-only sessions. Publish protected
  first when a checkpoint exists, repair partial imports without provider/tool
  I/O, and refuse legacy saves. Native-first open skips unnecessary legacy loads.
  Keep the legacy table this release; import remaining resumable rows and drop it
  next release through a separate data-preserving change.
- **Probes and lifecycle.** Read scalar native publication/checkpoint/tombstone
  metadata; read LCM overhead through `JournalReader::extension`. Retire replaced
  and unresumable suspended sessions, keep resumable dormant chats and reusable
  Task sessions. Schedule bounded dirty-session collection, terminal repacking and
  deletion cleanup without a TTL.
- **Acceptance.** Run `assert_journal_storage_conformance` unedited with repacking
  off/on and reopened storage, all five Forge-backed broken-storage cases, real
  import/equivalence/crash cuts and named integration checks. Forge's own bench
  runs B/N=4000 through production storage against L2 from the real legacy row;
  both plain/tool ratios must be ≤2.0× after collection (aim 1.5×).
- **BREAKING:** the new on-disk schema requires backup-based downgrade; a second
  concurrent writer conflicts; pruned operation replays outside the approved
  64-revision window (inclusive floor plus the current writer epoch's last
  operation) conflict. Record these in CHANGELOG during
  implementation. No feature flag or compatibility aliases.

Out of scope: U10/LCM-table consolidation, warm runtimes, runtime schema 5 and
legacy-table removal in this release. The Owner Decisions receipt is unchanged.

## Impact

- Affected specs: `native-session-journal`, `session-journal-retention`.
- Future implementation: workspace runtime pin/lockfile and agent-host testkit;
  `crates/agent-host/src/session_journal/`, `protected_store.rs`, `native.rs`;
  one new DB migration and scalar DB probes; services-agents retention/recovery
  wiring and nudges; continuity caller and focused native-chat tests.
- Future docs: `docs/architecture.md`, `docs/getting-started.md`,
  `docs/performance-testing.md`, `CHANGELOG.md`.
- Public API/MCP/CLI/events and `checkpoint_present` meaning stay stable. The disk
  layout gains six tables; no per-object body/index or operation-result rows.
- This revision changes only this spec folder and the requested local report.
  It performs no implementation, builds, release or runtime-repository edits.
- Evidence and detailed contract: design D2–D10; reference measurements are inputs,
  not a claim that Forge's release gates already pass.
