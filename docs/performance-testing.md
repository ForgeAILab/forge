# Performance stabilization: Operations and log diagnostics

This is a targeted 0.13.2 hardening patch, not a scheduler rewrite or a claim
that all performance findings are resolved. No database migration, permission,
Task lifecycle, API response, or execution-log schema changes are required.

## What changes

- Operations log summaries use one sequential scan, rather than repeatedly
  reading from the beginning for every 500-entry page. The scan reads at most
  the file size observed at open, so continuous appends cannot prolong one
  request indefinitely.
- Each `OperatorStatusService` keeps up to 64 recently used log summaries.
  Concurrent readers of the same log share the work; different logs do not
  share an I/O lock. Entries are validated using file size, modification and
  creation timestamps and, on Unix, device/inode/change time. A changed file,
  unreadable file, or entry older than 30 seconds is not reused. Files changed
  during a scan are not cached. Raw message contents are never cached here.
- Log-only activity requests an Operations refresh at most once per five
  seconds. Lifecycle changes retain the existing 500ms coalescing behavior.
  A lagged notification receiver requests reconciliation. This does not
  throttle the execution log stream itself.
- Native streaming still writes every visible delta to the durable log and
  live stream, preserving ordering and UI responsiveness. Its SQLite-backed
  semantic-progress callback is now bounded to at most once per second during
  text/reasoning streams. Tool-call boundaries and explicit progress requests
  force an immediate update.
- Operations recognizes `blocked_json` independently of workflow phase.
  Deleted, archived, `done`, and `cancelled` tasks are excluded from this
  blocker list. The existing display priority is preserved: `error_annotation`
  takes precedence, with the structured blocker reason used only when it is
  absent. The query never changes a Task's phase.
- The unused queued-task scan is removed from Operations status computation.

The cache is a rebuildable display optimization. It is not used to authorize
commands, renew leases, declare completion, calculate billing, or decide
milestone readiness. The existing `turn_count` interpretation (Assistant log
records, not provider calls) is unchanged. Progress throttling also does not
renew the execution owner lease; the server heartbeat remains independent.

**Remaining work:** a changed log still needs one full scan; this is not yet
an append-offset summary index. The general execution/chat `LogReader::read`
and `tail` endpoints, durable text-delta batching, broad frontend SSE
invalidations, outbox duplication, and dispatcher scans are not rewritten by
this patch. Diagnose those separately with measured workloads.

## Run safely

Use the existing development setup in [getting-started.md](getting-started.md).
Stop the other Forge process before launching the test instance. Use the
ignored `./test` data root, not a production database. Do not run `clean-test`
when comparing results: preserve the same test data and log sizes.

For meaningful timings compare release builds to release builds on the same
machine; a debug/release comparison does not isolate this change.

```bash
mkdir -p ./test/perf
FORGE_LOG_FORMAT=json RUST_LOG=warn,forge::perf=debug \
  cargo run --release --locked -p forge-cli -- --data-dir ./test \
  2>./test/perf/streaming.jsonl
```

The `forge::perf` target records allowlisted scalar samples:

| Operation | Recorded measurements |
| --- | --- |
| `operations_status` | Wall-clock duration including query/file wait, success |
| `operations_log_snapshot` | Duration including same-file wait, cache hit, scanned bytes/lines, parsed entries, success |

No prompt, tool arguments, raw error, local path, or record ID is emitted by
these samples. Other application logs can still contain private data. Keep
raw logs local. Cargo's non-JSON build messages are ignored by the collector.

## Test windows

Capture separate files for each window. Keep the Operations page open when
measuring its endpoint, and record whether any other tabs are open.

| Scenario | Exercise | What to check |
| --- | --- | --- |
| `idle` | No new activity for 60 seconds | An unchanged log can hit the cache; no log-only notification loop while idle |
| `streaming` | A Task producing output for 120 seconds | Live text still arrives; Operations activity refreshes and progress writes are bounded |
| `long-log` | Inspect a still-running Task with a large existing log | One rebuild parses each line once, rather than once per prefix/page |
| `multitask` | Several independent Tasks, within configured capacity | Endpoint latency and scan volume under concurrency |
| `reconnect` | Briefly disconnect/reconnect the browser | The existing resynchronization still converges |

Also exercise a review-phase task with a real blocker, clear that blocker,
and verify it enters/leaves the Operations blocker list without changing its
workflow phase. Do not manually edit a production SQLite database for this.

Optionally export the browser Network panel as HAR for the same test window.
HAR may contain tokens, cookies, chat messages, tool output, and private URLs.
**Never upload the raw HAR.** The collector processes it locally and exports
only allowlisted route categories, status counts, durations, and byte counts.

## Produce the report

Python 3.10 or newer; standard library only. This script makes no network
requests and does not inspect your database, credentials, or repository.

```bash
python3 scripts/collect_performance.py \
  --log ./test/perf/streaming.jsonl \
  --scenario streaming \
  --revision "$(git rev-parse HEAD)" \
  --out ./test/perf/streaming-report.json
```

Add `--har ./test/perf/streaming.har` to include browser measurements. `--har`
can also be used without `--log` for a baseline build without instrumentation.
Output files must be new: the collector refuses to overwrite an existing file.
Only the aggregate report should be shared. Inspect it before sharing it.
Include OS, release/debug mode, concurrent Task count, and your observed lag
separately; these details are intentionally not harvested automatically.

Check `samples`, `requests`, `sample_limit_reached`, and ignored/oversized
counts before interpreting results. Zero samples may mean the Operations
page was not opened or JSON tracing was not enabled; it does not mean zero
latency. Percentiles use nearest rank. The collector excludes SSE connection
lifetimes from normal request latency. Browser body sizes may be unavailable
or affected by caching. No CPU, RSS, SQL query count, or lock-wait profiler is
included; do not infer those metrics from these samples.

## Focused regression checks

```bash
GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 \
  FORGE_SKIP_WEB_BUILD=1 cargo test -p services operator_status
GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 \
  FORGE_SKIP_WEB_BUILD=1 cargo test -p services turn_log_sink
python3 -m unittest discover -s scripts -p 'test_*.py' -v
cargo fmt --all -- --check
```

The Rust filters cover summary scan counts, cache reuse/eviction/expiry,
concurrent reads, append/truncate/replacement, partial JSON, missing files,
blocker projection, notification throttling, and semantic-progress write
throttling. The repository's existing CI remains responsible for full
workspace coverage.

Do not call a branch fully verified until its Rust checks and the actual
interactive scenarios have completed. Python collector tests validate the
collector only; they do not validate the Rust application or demonstrate an
end-to-end speedup.

## Reproducible build comparison

Use Python 3.10+ (standard library only) and already-built release binaries.
These tools talk only to a loopback server and SQLite; no fixture data, logs,
credentials, or measurements leave the machine. No Rust build is needed.
Build once with the oldest supported baseline (currently v0.13.12), then
replay that fixture against each later build. The baseline binary initializes
its own schema and registers `perf@example.test`; after stopping it, the
builder inserts explicit, schema-checked SQL rows. An unknown required column
without a default produces a table/column error. Foreign-key and integrity
checks must succeed. Choose a new or empty fixture directory and new report
files: the tools refuse to overwrite existing data/results.

```bash
export TMPDIR=/Volumes/Data/tmp CARGO_TARGET_DIR=$PWD/target FORGE_SKIP_WEB_BUILD=1
# Optional explicit test HOME for child servers; otherwise each gets a scratch HOME.
export FORGE_PERF_HOME=/Volumes/Data/tmp/test-home
python3 scripts/perf_fixture.py build --baseline-binary /path/to/baseline/forge --out /Volumes/Data/tmp/perf/heavy/fixture --profile heavy --port 18101
python3 scripts/perf_bench.py run --binary /path/to/baseline/forge --fixture /Volumes/Data/tmp/perf/heavy/fixture --label baseline --out /Volumes/Data/tmp/perf/heavy/baseline.json --port 18101
python3 scripts/perf_bench.py run --binary /path/to/candidate/forge --fixture /Volumes/Data/tmp/perf/heavy/fixture --label candidate --out /Volumes/Data/tmp/perf/heavy/candidate.json --port 18102
python3 scripts/perf_bench.py compare /Volumes/Data/tmp/perf/heavy/baseline.json /Volumes/Data/tmp/perf/heavy/candidate.json
```

The `heavy` profile contains one owned heavy Project and repository, using the
baseline's default workflow; 60 Tasks (12 roots, four subtasks each) across
all nine baseline states; 600 terminal coder/reviewer executions; 180 review
rounds with three CI step results each; 900 transitions; 180 role assignments;
24 sibling dependencies; and 60 cleaned workspaces. Each execution has three
settled usage invocations, pricing selections, and ledger events (1,800 of
each) with substantial token counts and provider-reported costs. No external
pricing catalog or provider is required. There are 50,000 domain events,
200 Main-Agent chat messages and 100 successful turn jobs, plus the empty
Project chat and setup binding that baseline SQL triggers create.

No Task of the heavy Project carries deferred-dispatch metadata, so a build
with a task-list validator returns an `ETag` there and the conditional path
is measured. That metadata lives in a second, small Project (six root Tasks
with no history, two of them deferred until the year 2120; its own repository,
membership, chat and setup binding). A build switches the validator off for a
whole Project while any of its Tasks is deferred, and the benchmark reports
that for this Project. Fixtures built before this split (manifest schema
`forge.perf-fixture/1`) had the metadata on the heavy Project, so no build
returned an `ETag` for them; they still run, without the second Project's
scenario. Results from the two fixture generations have different digests and
are not comparable.

Named constants in `perf_fixture.py` control the sizes. UUID5 identifiers and
a fixed UTC epoch control inserted identities and timestamps. Baseline-created
default agents and profiles retain their configuration, with normalized IDs
and times; pristine immutable profiles are reinserted, with triggers enabled.
The trigger-created Project chat and binding IDs are normalized too. The
manifest records seeded table counts, version, byte size and a canonical row
digest. The registered user's identity and timestamps and salted password
hash are excluded from that digest; references to that identity are normalized.
The password is the public test constant `PASSWORD` in the script.

The fixture is deliberately idle: both Projects have a manual pause, all default
agents are paused and idle, no execution is running or holds a recovery lease,
workspaces are cleaned without cleanup deadlines, chat turn jobs have succeeded,
and consumer cursors point to the event head. A few Tasks carry real baseline
blocked/failed interruption shapes and blocked entry barriers; four reviews
await a human while their executions remain terminal.
There are no plan-publication claims. The repository paths intentionally do
not exist; a manual Project pause prevents checkout probing/dispatch. Main and
Project agent bindings require setup, so no autonomous work can start.

Each run copies the fixture into a temporary directory and starts the supplied
binary there, with a test HOME and `--no-embedded-daemon` when supported. It
stops and restarts the same copy, waits for `/healthz`, observes 60 idle seconds,
logs in, measures requests, and stops the child even on exceptions. It validates
the migrated DB, checks the original fixture hash, and removes the scratch copy.
No real `~/.forge` is used. Table digests before/after idle and from before the
second start through all GETs expose any unexpected task, execution, transition,
workspace, event or chat activity. Consumer cursors and the event head are
included in the result.

Requests use one persistent HTTP connection, sequentially. Every available
scenario has ten warmups and 100 measured requests (plus an untimed availability
probe); timings include reading the complete response. The list scenarios use
limits 20, 50 and 100, with separate conditional requests when an ETag is
returned (`tasks ETag limit=N`; expect 100% HTTP 304 and an empty body). The
`tasks deferred limit=20` scenario lists the small deferred-dispatch Project;
its `tasks deferred ETag limit=20` row is a measurement only if the build still
returns an ETag there. Otherwise it is skipped with the reason: `validator off`
when the same build returned an ETag for the heavy Project, or
`build did not return an ETag` when it has no validator at all (v0.13.12).
Ten fixed root/subtask IDs exercise the ordinary Task route. Additional
GETs mirror `web/src/api/hooks.ts`, `client.ts`, board workflow/agent reads and
the task modal: consolidated detail and relations when present, executions,
reviews, history, comments, media, workspace, dependencies and usage. Project
and account analytics use fixed fixture dates. Chat list/messages/turns read
the first page. Missing endpoints (404/405 or an HTML SPA fallback) and absent
ETags get explicit skip reasons. Other errors are counted by status, including
transport failures as status `0`; they do not abort a scenario. Error timings
must not be mistaken for speedups.

Interpret the nearest-rank p50/p95 and minimum as local request latency, and
response bytes as decoded HTTP body bytes. Status counts and `share_304` show
whether the comparison returned full responses or cache validations. `b/a`
ratios below one indicate lower latency; there are no thresholds or pass/fail
judgments. A conditional 304 saves payload transfer but can still require DB
work. First-start time includes forward migrations **and** normal startup;
second-start time uses the migrated copy. Their difference is not isolated
migration time. These are warm-machine starts, not cold disk-cache measurements.

Idle SQLite figures count observed `PRAGMA data_version` changes using one
read-only connection at 50 Hz. This is a **lower bound** on commits per second:
multiple transactions between polls count once, and neither checkpoints nor
unchanged table digests imply an absence of writes elsewhere. CPU is the server
process's `ps` cumulative CPU-time delta over idle; RSS is the largest sample
once per second during that window, not the OS lifetime high-water mark. If
process inspection is unavailable, these fields are null with a recorded reason
and other measurements continue. DB sizes are checkpointed main-file bytes
before and after; WAL churn is not disk growth in this figure.

This is one machine, one sequential synthetic workload, not browser rendering,
concurrent users, live model execution, long execution-log parsing, throughput,
or production billing validation. Report machine/build context and compare the
same canonical fixture digest. No scenario establishes general performance
by itself, and very small differences need repeated runs before interpretation.

To prove row determinism with the actual baseline, build a second fixture into
another empty directory, then run the optional read-only comparison test:

```bash
PERF_FIXTURE_A=/Volumes/Data/tmp/perf/heavy/fixture \
PERF_FIXTURE_B=/Volumes/Data/tmp/perf/heavy/fixture-repeat \
  python3 -m unittest discover -s scripts -p test_perf_fixture.py -v
```

Ordinary `python3 -m unittest discover -s scripts -p 'test_*.py' -v` needs no
server; it skips only this optional comparison of already-built fixtures.

## Incremental usage reads

Operations and Agent pages share one database-owned, process-local
`UsageLedgerIndex` through `OperatorStatusService` in the common server/Solo
graph. The fresh `usage_aggregate_for_operations` and
`usage_aggregate_for_agent` functions remain the references. Task/execution
usage and analytics retain their existing paths.

A warm read probes the singleton `usage_ledger_revision` row by primary key.
There are no history COUNT/SUM scans or execution-ID arrays. On change, one
SQLite read snapshot consumes bounded rowid slices for newly admitted
invocations, events, estimate revisions and executions, plus indexed latest
invocation/execution change markers. Watermarks advance only after successful
commit. Repeated in-place edits coalesce by ID; terminal execution ownership,
identity, duration and status edits are included. Heartbeat-only edits are
excluded. The data-preserving `V202610020420__usage_ledger_revision.sql`
migration adds invalidation metadata and indexes, never accounting totals.

V135's `usage_invocation_identity_immutable_update` freezes attribution and
run identity; its lifecycle/telemetry guards forbid changes after terminal
settlement. `usage_event_invocation_guard_insert` allows events only for
settled invocations and fixes their surface/source to the invocation.
`usage_event_immutable_update` and `cost_estimate_revision_immutable_update`
forbid edits. Pricing selections/rates/catalogs and estimate preview identity
fields are frozen. New events can therefore be accumulated without rereading
old events. A new estimate revision reads only its affected event at the old
and new revision watermarks, subtracts the old effective contribution, and
adds the new one. Direct execution deletion and Project/account cascades
increment an O(1) deletion generation; the next read rebuilds. Delete triggers
also reset affected rowid maxima with rightmost-row lookups, preserving
correctness when SQLite reuses rowids after teardown.

Counts, token buckets, optional nano-USD sums/presence, attempt coverage and
reason token/provider counts are additive. Compact per-attempt summaries are
necessary to deduplicate reason/provider counts and classify fully costed
attempts across many event appends. Per-run summaries classify pending,
no-provider, fully/partially/unavailable-cost and fully metered runs, and
collapse each reason's run count to one. Running scope totals replace only
changed run contributions; requests never fold all historical runs. Overall
coverage, cost kind, known subtotal and complete total are derived from those
totals, preserving optional zero versus unknown amounts.

`sources` is the non-additive field: the projection keeps the last source
reference for a key in source/attempt-ordinal/invocation/occurred-at/event-id
order. Each scope retains a count and winning order per distinct frozen
`CostSourceRef`, with an ordered map of variant winners. It retains no citation
per event. Removing a winner during repricing stages a database lookup for its
replacement in the same snapshot; ordinary appends do no historical lookup.
Removing a sole citation needs no replacement. Amount-only repricing preserves
the same event's provenance/order, so it retains that known winner without a
database search; newer events in the same delta can still supersede it.
Cross-Agent attribution keeps the original filter: an Agent-owned invocation
includes all its events; another Agent includes only matching events. Appends
invalidate only those Agent scopes and Operations.

A warm sync first reads its complete delta into private storage. A separate
synchronous function applies the delta and advances its watermarks together;
there is no await in that publication function. A cancelled request cannot
publish part of an event or reprice delta. Cold/reset builds use a private state
and publish it only when complete, skip historical changed-row markers, and
check the singleton's maxima against `MAX(rowid)` before building. A mismatch
logs an error and rebuilds from the actual rowids.

The summary index has a 128 MiB charged budget. Hash-table capacity, owned
strings/reason vectors, distinct provenance variants, ordered winners, and
public caches, a 50% allowance for allocator retention, and an 8 MiB reserve
for retained read batches/driver caches are charged. Capacity growth is checked before insertion, and
cold/delta payloads use 400-row slices and 150-event provenance joins. Oversized
staged deltas are discarded too; a build can exceed its budget only by its
current read batch. A reset releases the invalid state before building its
replacement. Discards log the charge, bound and four ledger row counts. Trigger-maintained
owned-execution counts also let cold reads reject a state whose minimum table
allocation cannot fit, without allocating a trial index.

Above the bound, a fresh computation folds 128-source batches instead of
retaining the whole ledger's rows. It runs outside the index and cache mutexes.
Separate Operations and Agent fill gates coalesce concurrent cache misses;
cache hits bypass those gates. A separate 32 MiB/256-Agent fallback memo is keyed by the snapshot's
revision header and deletion generation; unchanged polls read only constant
size metadata. Event/reprice deltas invalidate just their affected Agent
responses, while execution statistics survive unrelated ledger appends. The
index remembers a failed fit and retries only after the trigger-maintained row
counts shrink by at least 25%, or after process restart. A successful retry is
logged. Internal invariant failures log an error and use the fresh reference.
Public response size still determines the cost of copying distinct sources.

The release RSS probes on this machine charged approximately 99.2 MiB for
12,000 runs/36,000 attempts, against 109–112 MiB of server RSS growth. At
13,800 runs/41,400 attempts the charge was approximately 102.3 MiB against
111–113 MiB growth, for both reported and estimated cost. With three attempts
and events per run, both fixtures fit at 41,400 events and discard at 43,200
when the run map grows; this is a capacity boundary, not an event-count limit.
The charged cost averages roughly 7–8 KiB per run after subtracting the fixed
8 MiB reserve, and varies with map capacity/Agent attribution. Adding 500
events with the same invocation/provenance in the focused test adds under
4 KiB. The 24× trial is stopped before the expensive capacity growth; the
55× fixture is rejected by the minimum-allocation probe before building an
index. Discarded indexes retain zero summaries.

The active/scale/RSS runner lives in `scripts/perf_usage_reads.py`; its fixture
helpers are in `scripts/perf_usage_fixture.py`. The pinned `heavy` profile is
unchanged. `usage-estimated` is a separate deterministic profile with the same
run counts, applied cost-estimate revisions and usage spread over three Agents.
Preparation and measurement write only copies, with all production triggers
and foreign keys enabled. Active requests append one event to rotating
invocations; estimated appends also receive an applied estimate, and an old
event is repriced every 50 requests. Write time is excluded from read latency.
Each route starts a fresh server for its cold request, then measures idle and
active p50/p95 with either one or four persistent clients. Concurrent active
rounds commit one event per client before the clients read together, so raw-SQL
write-lock waits are excluded. RSS is sampled with
`proc_pidinfo` on macOS and `/proc` on Linux.

```sh
export TMPDIR=/Volumes/Data/tmp CARGO_TARGET_DIR=$PWD/target FORGE_SKIP_WEB_BUILD=1
python3 scripts/perf_usage_reads.py prepare --binary "$PWD/target/release/forge" \
  --fixture /Volumes/Data/tmp/perf/heavy/fixture-v2 --scale 20 --estimated \
  --out /Volumes/Data/tmp/perf/usage-estimated20
python3 scripts/perf_usage_reads.py run --binary "$PWD/target/release/forge" \
  --fixture /Volumes/Data/tmp/perf/usage-estimated20 --clients 4 --requests 100 \
  --out /Volumes/Data/tmp/perf/usage-estimated20-4clients.json
```

Agent page execution counts/success rates and running occupancy use the same
execution deltas; active task/turn assignments remain live and workflow states
are decoded once per relevant Project. Terminal durations use SQLite's own
per-row Julian-day expression and an exact dyadic accumulator. A conservative
floating-error interval certifies the final rounded millisecond; ambiguous
rounding boundaries use and cache the original SQL AVG until execution inputs
change. Legacy relative/non-RFC date expressions retain live SQL statistics.
Those exceptional statistics fallbacks may scan an Agent's history; ledger
aggregation never does so during ordinary deltas. Other Operations diagnostics
and `active_execution_count` remain live. Empty source breakdowns retain their
single-acquisition fast path.

Correctness validation includes the original three deterministic 360-step
repository mutation sequences and six expanded seeds read after random batches
of one to eight changes. Operations, individual/batched Agent aggregates, running
counts and execution statistics are compared with independent fresh references.
The generator includes all surfaces, NULL Agent/Project scopes, account deletion,
admission estimates, unmetered/unsettled outcomes, shared provenance, nonzero
durations, ownership/date edits and heartbeats, plus a cold rebuild at the end.
The test relaxes only the
Agent-attribution clause of the in-memory event admission guard to exercise
historical cross-Agent attribution; all other guards remain enforced. Execution
delete and ownership SQL match the existing workspace boundary because there
is no repository API for them. A pure-fold differential gate runs 3,000 seeds
split between guarded and legacy shapes, including tied order keys. File-backed
tests exercise concurrent readers/writers and cancellation at every poll boundary
for event/reprice deltas. Additional tests cover compact source winner recovery,
repair-buffer bounds, old-event repricing, rollback, deletion/rowid reuse, cold
header/schema checks and bounded fallback. A
read-count seam proves seven appends read fourteen event/provenance rows at
both 12- and 420-run histories; a single append after 420 events on one run
reads two rows.

`scripts/test_usage_mutations.py` ports the audit's 18 fault injections. It copies
source into a new scratch directory with its own Cargo target, uses dependencies
offline, and requires every mutation to compile and fail a repository test:

```sh
python3 scripts/test_usage_mutations.py \
  --scratch /Volumes/Data/tmp/usage-mutations --no-debug-info
```

The first-pass activity/scale results below were produced by the predecessor
of `scripts/perf_usage_reads.py`. First-pass binaries and outputs are preserved as
`usage2-before-forge`, `usage2-before-1.json` and `usage2-before-20.json`.

### Release validation, 2026-10-02

Darwin arm64, release binaries, the original fixture digest intact. The full
idle replay is `/Volumes/Data/tmp/perf/heavy/usage2.json`: 40 scenarios,
zero request errors, no seeded activity changes. P50/p95 ms are Operations
0.840/1.144, account Agents 0.423/0.459, Project Agents 0.456/0.508.

The controlled activity/scale runs use 100 requests after ten warmups per
scenario. The first-pass 20× baseline uses 30 requests because each active
request took almost a second. Scaling is exactly 20× for the measured tables:
1,800→36,000 invocations/events and 600→12,000 executions. Both sizes retain
identical task/Agent identities, so an append to one existing invocation
exercises increasingly long event history without changing the Agent roster.

| Route | Before idle 1× / 20× | After idle 1× / 20× | Before active 1× / 20× | After active 1× / 20× |
| --- | ---: | ---: | ---: | ---: |
| Operations | 1.387 / 23.782 | 0.725 / 0.729 | 34.248 / 770.458 | 0.892 / 1.012 |
| Account Agents | 1.101 / 15.985 | 0.519 / 0.489 | 39.026 / 892.286 | 0.626 / 0.630 |
| Project Agents | 1.130 / 16.011 | 0.541 / 0.519 | 38.667 / 874.645 | 0.624 / 0.633 |

Results are `usage2-before-{1,20}.json` and `usage2-after-{1,20}.json` in the
same scratch directory. Intermediates before the live-diagnostic indexes are
preserved separately. Profiling those intermediates identified two remaining
Operations history costs: latest-error sorts and recent-error filtering. The
new expression indexes are selected explicitly by those unchanged live
queries; query-plan tests require their use without a temporary sort. Those
indexes and the running-occupancy index are in the same migration.

A warm usage read executes one primary-key revision SELECT. An event-only
delta executes four SELECTs (two header probes, new-event rows and frozen
provenance), plus read BEGIN/COMMIT. Seven
appends transfer fourteen payload/provenance rows independent of history;
there is no source/invocation history reload. Agent page execution metadata
adds a second constant header probe; the whole idle collection routes execute
four/five SELECTs on the paused fixture (account/Project), including the live
assignment count and membership check. Idle Operations retains thirteen
statements including its live diagnostics/storage PRAGMAs.

Thirteen focused tests passed, including the three-seed differential gate
(360 random steps plus the mutation prelude per seed), delta-read bounds,
long-run append, source-order replacement, ownership/repricing/deletion,
budget fallback, transaction rollback, diagnostic index plans, and relevant
existing projection/Operations cases. Touched-crate check, warnings-denied
Clippy, formatting and final release build passed. Workspace/broad suites
remain CI work; no public response changes or generated-type edits were made.

### Audit follow-up release validation, 2026-10-02

Darwin arm64; release build of the uncommitted audit fixes. All source fixtures retain their original hashes. Timings are milliseconds. Each route uses a fresh server for its cold request. Idle/active phases have 50 samples per sequential client and 200 with four clients; the over-bound four-client comparison has 52 samples on both builds. Active writes rotate invocations, with an old-event reprice every 50 writes, through production SQL guards in copied fixtures. Four-client rounds commit their writes before reading together.

Reported fixtures contain 600/12,000/33,000 executions and 1,800/36,000/99,000 invocations/events (1×/20×/55×). Estimated fixtures have the same 1×/20× counts plus 1,800/36,000 applied revisions, spread over three Agents. The 55× fixture is above the summary budget.

RSS columns are MiB: before the cold request → after cold → after idle → after active. `R` means reported cost; `E` means estimated cost.

| Fixture | Clients | Route | Idle p50 / p95 | Active p50 / p95 | Cold | RSS before / cold / idle / active |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| R1× | 1 | operations status | 0.680 / 0.808 | 1.147 / 1.654 | 39.969 | 47.3 / 57.9 / 60.3 / 60.8 |
| R1× | 1 | agents | 0.450 / 0.488 | 0.724 / 1.207 | 37.463 | 47.0 / 58.1 / 58.4 / 58.8 |
| R1× | 1 | project agents | 0.510 / 0.598 | 0.626 / 1.080 | 40.656 | 46.5 / 58.5 / 58.7 / 59.0 |
| R1× | 4 | operations status | 1.866 / 2.814 | 3.126 / 4.008 | 37.326 | 47.4 / 59.7 / 62.5 / 63.0 |
| R1× | 4 | agents | 1.063 / 1.354 | 1.624 / 2.060 | 38.031 | 47.0 / 58.1 / 59.4 / 59.9 |
| R1× | 4 | project agents | 1.242 / 1.800 | 1.580 / 2.048 | 36.536 | 47.4 / 58.7 / 59.8 / 60.3 |
| R20× | 1 | operations status | 0.670 / 0.829 | 1.150 / 1.769 | 798.387 | 46.8 / 158.2 / 161.7 / 162.1 |
| R20× | 1 | agents | 0.457 / 0.524 | 0.588 / 0.868 | 790.263 | 47.4 / 160.3 / 161.2 / 161.5 |
| R20× | 1 | project agents | 0.476 / 0.515 | 0.594 / 1.002 | 745.346 | 47.4 / 155.9 / 156.0 / 156.5 |
| R20× | 4 | operations status | 1.563 / 2.434 | 2.737 / 3.112 | 727.269 | 47.0 / 157.8 / 161.6 / 162.1 |
| R20× | 4 | agents | 0.925 / 1.168 | 1.309 / 1.993 | 741.767 | 47.3 / 158.4 / 159.5 / 160.0 |
| R20× | 4 | project agents | 0.994 / 1.230 | 1.382 / 1.704 | 754.836 | 47.3 / 157.1 / 158.2 / 158.6 |
| E1× | 1 | operations status | 0.636 / 0.673 | 0.802 / 1.077 | 43.059 | 45.0 / 56.3 / 59.9 / 60.1 |
| E1× | 1 | agents | 0.458 / 0.500 | 0.544 / 0.733 | 41.956 | 45.2 / 58.1 / 59.0 / 59.0 |
| E1× | 1 | project agents | 0.484 / 0.518 | 0.627 / 0.941 | 42.772 | 45.3 / 56.6 / 56.7 / 57.1 |
| E1× | 4 | operations status | 1.561 / 2.192 | 3.361 / 15.024 | 41.925 | 45.8 / 57.0 / 60.4 / 61.4 |
| E1× | 4 | agents | 1.027 / 1.253 | 1.570 / 5.290 | 42.895 | 45.4 / 57.1 / 59.5 / 60.1 |
| E1× | 4 | project agents | 1.036 / 1.288 | 1.362 / 1.795 | 42.477 | 45.0 / 56.9 / 59.4 / 59.9 |
| E20× | 1 | operations status | 0.691 / 0.731 | 0.886 / 1.146 | 911.213 | 51.2 / 159.8 / 160.2 / 160.5 |
| E20× | 1 | agents | 0.452 / 0.486 | 0.561 / 0.751 | 901.879 | 49.6 / 159.2 / 159.5 / 159.6 |
| E20× | 1 | project agents | 0.476 / 0.511 | 0.609 / 0.978 | 901.610 | 51.0 / 159.6 / 159.7 / 160.2 |
| E20× | 4 | operations status | 2.135 / 3.122 | 3.870 / 9.348 | 1009.073 | 51.2 / 162.3 / 163.1 / 164.1 |
| E20× | 4 | agents | 1.115 / 1.437 | 2.218 / 27.649 | 1234.097 | 50.8 / 162.5 / 163.6 / 164.7 |
| E20× | 4 | project agents | 1.177 / 1.572 | 2.237 / 32.691 | 1000.820 | 49.8 / 159.3 / 160.4 / 121.4 |
| R55× | 1 | operations status | 0.922 / 1.025 | 2887.051 / 3403.240 | 2853.784 | 48.5 / 58.5 / 63.0 / 70.5 |
| R55× | 1 | agents | 0.468 / 0.643 | 2827.528 / 3129.087 | 2984.248 | 49.9 / 60.8 / 60.9 / 72.1 |
| R55× | 1 | project agents | 0.475 / 0.705 | 2753.402 / 2876.157 | 2924.962 | 48.2 / 60.0 / 61.0 / 68.5 |
| R55× | 4 | operations status | 1.718 / 2.762 | 2652.892 / 2734.335 | 2724.469 | 48.8 / 59.7 / 62.9 / 68.0 |
| R55× | 4 | agents | 1.074 / 1.511 | 2732.388 / 3034.957 | 2776.996 | 47.7 / 60.7 / 62.9 / 66.8 |
| R55× | 4 | project agents | 1.458 / 1.938 | 2852.462 / 2891.145 | 2960.271 | 47.5 / 59.9 / 61.9 / 67.2 |

Outputs are under `/Volumes/Data/tmp/usage-fixes/`: `bench6-after-{reported1,reported20,estimated1,estimated20}-{1,4}.json` and `bench5-after-reported55-{1,4}.json`.

The same over-bound fixture on base `5db8ea0b`:

| Clients | Route | Base idle p50 / p95 | Base active p50 / p95 | Base cold | Base RSS before / cold / idle / active |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | operations status | 2956.716 / 3893.183 | 3018.484 / 3969.922 | 2979.163 | 47.0 / 576.6 / 2032.5 / 1264.1 |
| 1 | agents | 420.633 / 443.280 | 12173.807 / 14110.676 | 11921.167 | 45.7 / 450.6 / 451.4 / 471.8 |
| 1 | project agents | 438.457 / 455.184 | 12091.295 / 13834.336 | 13751.571 | 48.3 / 451.3 / 452.2 / 598.4 |
| 4 | operations status | 16493.164 / 18461.239 | 17921.874 / 26625.839 | 3215.782 | 47.5 / 571.4 / 2980.8 / 3459.9 |
| 4 | agents | 1354.389 / 1594.151 | 29089.596 / 33049.282 | 18926.576 | 46.1 / 449.5 / 488.8 / 1793.2 |
| 4 | project agents | 1214.831 / 1521.026 | 26053.328 / 33692.692 | 13176.358 | 47.5 / 451.4 / 490.4 / 2020.2 |

Base outputs: `bench3-base-reported55-{1,4}.json`. The over-bound new build improves p50/p95 and cold latency on all three reads and both client counts. It retains zero index summaries: 232.4 MiB is the rejected minimum allocation, not allocated memory. The response memo keeps idle reads constant; source batches keep active RSS below 80 MiB in these runs.

All 28 projection tests pass. Pure fold uses 3,000 deterministic seeds; repository differential tests cover all surfaces, legacy shapes, batched changes, deletion, timestamps/ownership, running counts and cold/warm equality. Cancellation tests cover every poll boundary through completion for event/reprice deltas. All 18 final-source mutations compiled and were caught by repository tests; results are `/Volumes/Data/tmp/usage-fixes/mutations-final/results.json` and `mutations-ultimate.log`. Clippy (warnings denied), formatting for db/services/api, the release build, and named DB/pricing/Operations/API-Agent tests pass.

The standard runner also completed all 40 scenarios without errors on the final
release binary: `/Volumes/Data/tmp/perf/heavy/usage5.json` and the repeat
`/Volumes/Data/tmp/perf/heavy/usage-repeat.json`. Repeat warm p50/p95 was
0.873/1.294 ms for Operations, 0.499/0.636 ms for Agents, and 0.508/0.688 ms for
Project Agents, against 36.716/44.502, 11.052/11.862 and 10.709/12.950 ms in
`final-next.json`. A paired base run is `base-paired.json`; its comparison with
`usage5.json` is `/Volumes/Data/tmp/usage-fixes/paired-comparison.md`.
Unchanged Task list routes varied by +0.122/+0.187/+0.158 ms (limits 20/50/100,
4–8%) on the repeat versus that paired base. These small host/run differences
are recorded rather than treated as evidence of a change to those routes.

Sequential warm/active p50 stays flat between 1× and 20× for both pricing
shapes. Four-client estimated-cost active p95 is less stable: 9.348 ms for
Operations, 27.649 ms for Agents and 32.691 ms for Project Agents at 20×.
Those tails remain visible in the table; this measurement does not establish
flat concurrent p95. Over-bound active reads intentionally use the bounded
fresh fallback, while idle reads retain the constant-cost response memo.
