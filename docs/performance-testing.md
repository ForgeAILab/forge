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
- Operations recognizes a blocked Task from its condition, independently of
  workflow phase.
  Deleted, archived, `done`, and `cancelled` tasks are excluded from this
  blocker list. The existing display priority is preserved: the stored
  annotation takes precedence, with the structured blocker reason used only
  when it is absent. The query never changes a Task's phase.
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

### Project slot read projection

Project GET and list responses share an API-state memo capped at 256 Projects.
Each entry contains counts and `(list_revision, version)`, read with the existing
Project row. A hit runs no slot statement. Missing entries on a list page use
one grouped statement, regardless of the number of Projects. Unlimited Projects
(`max_active_tasks = 0`) need no slot statement and retain zero counts. The
dispatcher continues to call the original, uncached `load_project_slots`.
The revision column is selected only by the Project GET/list read projections;
general Project repository reads retain their original columns and model.
Mutation responses continue to read uncached counts.

The grouped statement returns its observed Project revisions with its counts.
If a write committed between the Project row read and that statement, the API
does not memoize the result under the earlier key. Hits describe the snapshot
observed by the Project row read; they do not promise to include writes committed
after that read. The mutex protects only in-memory lookup/insertion and is never
held across SQL. The fixed cap also bounds entries left by deleted Projects.

| Slot input | What invalidates the memo |
| --- | --- |
| Task existence, Project membership, status (roots and subtasks) | V202610011636 Task insert/delete/update triggers bump `list_revision` for the old/new Project |
| Parent/child links and child deletion, including settled or archived children in the child-existence probe | The same Task triggers cover same-Project links; V202610020410 also bumps an external parent's Project on child insert/delete/reparent/move/soft-delete |
| Task visibility: `archived_at`, `deleted_at` | Task update trigger bumps `list_revision` |
| Holds: non-null `blocked_json`, `failed_json`, blocking `error_annotation.type` (malformed/nonblocking annotations retain their existing behavior) | Task update trigger bumps `list_revision` |
| Latest Review's status and ordering (`attempt_number`, `created_at`, `id`), Review-to-Task relation | Review insert/delete/update triggers bump `list_revision`; IDs are immutable in application writes and `(task_id, attempt_number)` is unique, so the ID tie-break cannot select a different Review |
| Existence of a running Execution for a coordination root, Execution-to-Task relation | Execution insert/delete/update triggers bump `list_revision` on status or Task changes; running heartbeats do not change these inputs |
| Project workflow state names/kinds, canonical phase, effective reviewer/auditor role, `run_merge` entry hooks, custom states inherited by subtasks | Workflow-definition trigger bumps `list_revision`; versioned workflow updates also move `version` |
| Built-in default workflow, inherited subtask workflow, blocking annotation kind list and resolver rules | Constants in the binary; process restart creates an empty memo |
| `settings.max_active_tasks`, including unlimited capacity | All production settings mutation paths increment Project `version` (`update_at_version`, review-config command, execution-setup command); no new settings trigger is needed |
| Project pause/environment-pause flags; Task metadata, per-Task state config, assignments and priorities | Not read by the slot SQL (including `entry_barrier_json`); normal version/revision changes can conservatively invalidate, but these fields do not alter the counts |

Project/Task identities are immutable application-generated UUIDs. Execution
lease deadlines, deferred-dispatch times and Agent pause flags are not inputs:
this projection reads execution status and Task state/holds, not wall-clock time.
The production settings setters above increment `version` in the same guarded
transaction as the settings update. Task/Review/Execution revision triggers and
the parent-link triggers likewise commit atomically with their source changes.

For authenticated Project GETs, the response path has one Project SELECT plus
zero slot statements on a memo hit, or one slot statement on a bounded miss
(previously one on every bounded GET). For a list page without `include_total`,
there is one Project SELECT plus zero slot statements on all hits, or one grouped
slot statement for any bounded misses (previously N slot statements for N bounded
Projects). `include_total=true` adds the unchanged count statement.

The runner includes `project list limit=100`. Fixture generation 2 has two
Projects: the heavy Project and the small deferred-dispatch Project. Ten warmups
mean the measured GET/list requests exercise memo hits. The multi-Project slot
test covers different workflows, inherited/custom kinds, over 200 queued Tasks,
empty/unlimited Projects and cross-Project child links. Existing slot scenarios
also compare the batch to the original single-Project function. API memo tests
mutate each changing input and use a database with no schema to prove that hits
cannot issue SQL. The batch implementation has exactly one `fetch_all` outside
the Project loop; the Project list route calls it once for all page misses and
constructs responses without additional slot reads.

Local validation on 2026-10-02 used macOS 27 / ARM64 release binaries and the
unchanged generation-2 heavy fixture. Reports are under
`/Volumes/Data/tmp/perf/heavy/`:

| Build/report | Project GET p50 ms | Project list p50 ms |
| --- | ---: | ---: |
| v0.13.12, `final-v0.13.12.json` | 0.144 | Not measured |
| 5db8ea0b, `final-next.json` | 0.246 | Not measured |
| v0.13.12 with the list scenario, `slots-baseline-v0.13.12.json` | 0.147 | 0.149 |
| 5db8ea0b with the list scenario, `slots-baseline-next-repeat.json` | 0.247 | 0.329 |
| Final 5db8ea0b control, `slots-baseline-next-final.json` | 0.249 | 0.326 |
| Final implementation, `slots.json` | 0.144 | 0.132 |
| Final implementation repeat, `slots-scoped-repeat.json` | 0.139 | 0.153 |

Both final implementation runs returned no request errors, preserved the fixture
digests, and changed no tracked fixture tables during idle or through the GETs.
The focused Rust checks covered eight slot tests and four memo tests; the added
Python test checks the new Project list scenario. Full suites remain CI work.
Other scenario timings varied across runs, so the Project timing improvement
does not by itself establish a complete no-regression result. Process CPU/RSS
measurements were unavailable because the sandbox denied `ps`.
For example, Task relations measured 0.232 ms in the final implementation repeat
versus 0.200 ms in the final baseline control. The full no-regression acceptance
criterion remains unconfirmed; investigate that comparison in an isolated
benchmark window without widening this change into the unrelated routes.

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

The summary index defaults to a 128 MiB charged budget, configurable with
`server.usage_index_budget_mb`, `FORGE_SERVER_USAGE_INDEX_BUDGET_MB`,
`forge --usage-index-budget-mb N`, or Forge Settings → Server. Unset/null uses
128 MiB; `0` disables indexing and always takes the memoized full-read path.
Settings updates apply on the next read: a fitting index stays warm, a lowered
budget discards an oversized index, and changing the budget after discard
retries a cold build. Operations reports `usage_index.current_size_bytes`,
`budget_bytes`, and `fallback`; the current size is an estimated charge, not RSS.
The separate fallback memo retains its existing 32 MiB limit.

Hash-table capacity, owned
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
counts shrink by at least 25%, its effective budget changes, or after process
restart. A successful retry is logged. Internal invariant failures log an error and use the fresh reference.
Public response size still determines the cost of copying distinct sources.

The release RSS probes on this machine charged approximately 99.2 MiB for
12,000 runs/36,000 attempts, against 109–112 MiB of server RSS growth. At
13,800 runs/41,400 attempts the charge was approximately 102.3 MiB against
111–113 MiB growth, for both reported and estimated cost. With three attempts
and events per run, both fixtures fit at 41,400 events and discard at 43,200
when the run map grows; this is a capacity boundary, not an event-count limit.
For this fixture, a rough planning ratio is 41,400 events / 128 MiB ≈ 323
events per MiB. Budget scaling is approximate: subtract the fixed 8 MiB reserve,
allow for hash-table growth steps, and account for attribution/provenance width
and events per invocation. A budget below the fixed reserve may use fallback
even for a small non-empty ledger; repeated events sharing an invocation and
provenance can consume much less memory than distinct runs.
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

Both Agent collection routes use one shared response builder. It combines the
incremental usage/statistics reads and grouped assignments with the page-wide
`runnable_on_for_agents` projection. Effective status also batches credential
and connection health, CLI policies and live reservations; running counts come
from the index. Reservation expiry and the distinction between ready placements
and held slots match admission's scalar capacity calculation. The
`environment_surface_lists` regression compares complete warm requests at one
and twenty CLI/native Agents and checks the component statements, so adding a
per-Agent read to either list fails the test.

After merging `next/v0.14` at `7b913b25` into the usage branch, the complete
warm-request statement counts were identical at exactly one and twenty Agents:

| Backend | Agents list, 1 / 20 | Project Agents list, 1 / 20 |
| --- | ---: | ---: |
| CLI | 8 / 8 | 9 / 9 |
| Native | 9 / 9 | 10 / 10 |

Each includes two constant usage/statistics header probes, one grouped
assignment statement, three effective-status statements and page-wide runnable
facts. Project membership adds one statement. Native runnable health adds one
statement. Counts and scalar-response comparisons are recorded in
`/Volumes/Data/tmp/usage-merge/test-api-environment_surface_lists-final.log`.

The merge benchmark also exposed unused CLI availability probes for paused
Agents in the new runnable projection. The heavy fixture pauses all seven
default Agents; probing installed CLIs still cost about 0.6–0.7 seconds per
page although every runnable result was necessarily empty. Paused identities
now skip those probes. A counted-adapter regression verifies zero probes for a
paused page and one probe when an Agent of that executor family becomes active.

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

All 28 projection tests pass. Pure fold uses 3,000 deterministic seeds; repository differential tests cover all surfaces, legacy shapes, batched changes, deletion, timestamps/ownership, running counts and cold/warm equality. Cancellation tests cover every poll boundary through completion for event/reprice deltas. All 18 final-source mutations compiled and were caught by repository tests; results are `/Volumes/Data/tmp/usage-fixes/mutations-final-results.json` and `mutations-ultimate.log`. Clippy (warnings denied), formatting for db/services/api, the release build, and named DB/pricing/Operations/API-Agent tests pass.

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

### Merge confirmation, `7b913b25`, 2026-10-02

The final release tree includes the Project slot memo, runtime worker/relay diagnostics, machine caps and page-wide runnable facts. Sequential measurements use 50 idle and 50 active reads per route, rotating appended events with an old-event reprice every 50 writes. Fixtures are copied, and source hashes are unchanged. Timings are milliseconds; no concurrent matrix was repeated.

| History | Route | Previous idle p50 | Merged idle p50 / p95 | Previous active p50 | Merged active p50 / p95 | Merged cold |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1× | operations status | 0.680 | 1.603 / 2.261 | 1.147 | 1.615 / 2.275 | 46.211 |
| 1× | agents | 0.450 | 0.588 / 0.660 | 0.724 | 0.815 / 1.197 | 38.719 |
| 1× | project agents | 0.510 | 0.592 / 0.745 | 0.626 | 0.845 / 1.428 | 38.054 |
| 20× | operations status | 0.670 | 1.423 / 1.844 | 1.150 | 1.643 / 2.473 | 824.631 |
| 20× | agents | 0.457 | 0.570 / 0.661 | 0.588 | 0.741 / 1.050 | 839.236 |
| 20× | project agents | 0.476 | 0.792 / 1.211 | 0.594 | 1.013 / 1.514 | 897.176 |

The merge preserves constant statement counts and removes the paused-CLI probe regression. Absolute Operations latency is higher than the usage-only branch; the merged route also reports live worker, relay and machine-capacity sections. Both history sizes remain in low single digits, and both Agent routes remain below 2 ms. The measurements do not establish identical absolute timing to the earlier branch.

Final outputs: `/Volumes/Data/tmp/usage-merge/bench-reported1.json` and `bench-reported20.json`; commands are in `benchmark-commands.json`. Complete verification logs and exit/pass/failure records are in `verification-all.json`. All requested Rust modules/targets, type export, TypeScript checking, workspace clippy and formatting passed. The additional environment regression module passed three tests. No Rust test reported FAILED.

`pnpm typecheck` unexpectedly invoked automatic dependency installation, contrary to the requested no-install constraint; the first attempt was interrupted, but the later invocation completed installation and typecheck. This deviation is recorded in `typecheck-note.log` and `web-typecheck.log`. Type generation introduced no additional tracked web diff.

<!-- session-journal-d10:start -->
## Session journal D10 measurements (2026-10-21)

**Disk gate: MET after within-session sharing (task 29); turn/collect
checks: NOT MET pending runtime. Import is no longer latency-gated.** These are actual Forge measurements at agent-runtime v0.9.0 /
`8c19be5b226c2fdaeb6708ec5d39e91c235857a5`, from the production SQLx storage and
real AEAD. The tables after the task-29 section are the original task-25 run,
with independent domain copies, kept for comparison.

### Owner acceptance revision, 2026-10-21

D10 replaces relative turn/resume checks with fixed native journal overhead:
≤~250 ms per turn over legacy at B/N=4000 and ≤~50 ms at N=100, plain and tool,
release builds at per-turn cadence. Every single collect call / engine gate hold
must be <1 s (target <250 ms). Disk stays ≤2.0× actual L2 (aim 1.5×); delta
writes ≤10% of legacy for N≥1000 stays tracked. Older tables below retain their
historical verdict columns; no historical run was recomputed.

Current task-29 evidence gives disk PASS at 1.60843× / 1.79118× and turn FAIL:
B/4000 native 1,685.62 / 2,547.75 ms versus legacy 229.38 / 427.33 ms exceeds
the ~250 ms overhead budget by a wide margin. C/4000 collect 2,106.75 ms FAILS
the <1 s bound. Loaded A/N=100 timings do not establish the ~50 ms budget.
The B/4000 import measurements 3,349.73 / 3,925.23 ms are full legacy-to-native
conversion. Forge now schedules that work in the background, retaining lazy
fallback for an early open; it does not claim conversion got faster.

Runtime change `speed-up-journal-hot-path-2026-10-21` owns P1/P2/P4 speed work
(P3 optional). Pin that runtime release, update the report's gate fields to the
fixed budgets, then rerun Forge's gate. This background-conversion change does
not rerun the expensive release measurement or claim a latency pass.

### B/1000 delta-write fix: maintenance moves only fully held packs (2026-10-22)

Sharing raised the B/N=1000 delta-write ratio from 10.68% / 10.49% to 35.94% /
25.32%. Per-call traces of both builds showed why: protected's S2 hook paired
its turn-1/turn-2 request-settings/checkpoint pack (262,691 logical bytes,
protected-only) with ordinary's stable history pack (430,579 bytes, which
protected does not hold). Both sit in the 2^18 tier, so they merged (128,152
bytes sealed). The final sweep then purged the merged pack's dead checkpoints
(122,004-byte rewrite) and the base repack re-merged the survivor (137,138
bytes). Tool shows the same chain: 130,366 / 122,102 / 140,765. The metric counts
each physical pack write once, so this is not double counting.

Commit `79996e80` limits S2 tiers and the collection base repack to packs the
acting domain holds entirely. Same ignored release report, fixtures, seeds,
projection, baseline, cadence and thresholds. Raw output:
`/Volumes/Data/tmp/refactor/c3-writes/gate/`.

| Fixture | Delta writes / legacy, task 29 → fix | Forge/L2, task 29 → fix | Packs after collection | Max collect call, ms |
| --- | ---: | ---: | ---: | ---: |
| B/1000 plain | 35.937% → **10.311%** | 1.73171× → 1.73171× | 3 → 6 | 73.78 → 72.67 |
| B/1000 tool | 25.317% → **10.156%** | 1.86747× → 1.87952× | 3 → 6 | 83.58 → 83.46 |
| B/4000 plain | 7.753% → 5.348% (PASS) | 1.60843× → **1.61145×** (PASS) | 8 → 11 | 330.56 → 278.42 |
| B/4000 tool | 5.514% → 4.006% (PASS) | 1.79118× → **1.79412×** (PASS) | 9 → 12 | 326.24 → 318.88 |
| C/1000 tool | 10.770% → 8.805% (PASS) | 1.26522× → 1.50000× | 13 → 33 | 256.60 → 175.46 |
| C/4000 tool | 3.100% → 2.574% (PASS) | 1.13992× → 1.34568× | 41 → 127 | 2106.75 → 2955.71 |

B/1000 is now below the pre-sharing 10.68% / 10.49% but still above 10%. The
remainder is not specific to sharing. The largest single item is S2 merging two
request-settings/checkpoint admission packs (23,716 / 25,695 bytes) whose bodies
die at the next collection. The cost of the rule is fragmentation: a pack that no
domain holds entirely is left for `compact`. C packs triple, C disk ratios rise
and C/4000's longest collect call worsens (D8 <1 s was already failing). The
B/4000 hard disk gate moves by one 4 KiB page. Turn and import times are within
noise of task 29. B/1000 native turn is 377.09 / 654.80 ms versus legacy 51.69 /
97.84 ms; B/4000 is 1,466.28 / 2,392.78 versus 210.95 / 391.25 ms. Import is
809.14 / 931.08 and 3,233.74 / 3,820.18 ms. This run's A rows were not under the
heavy load of task 29's run; see the JSON for them.

### Task 29: within-session body sharing re-measurement

Same ignored release report, fixtures, seeds, projection, L2 baseline, cadence
and thresholds. The only changes are session-owned shared packs and the
storage-only fixes (task 28). Raw output: `/Volumes/Data/tmp/refactor/c3-share/gate/`.

| Fixture | Actual L2 | After collection, task 25 → 29 | Forge/L2, task 25 → 29 | ≤2.0× |
| --- | ---: | ---: | ---: | --- |
| A/100 plain | 8,192 | 106,496 → 73,728 | 13.00000× → 9.00000× | n/a |
| A/100 tool | 8,192 | 110,592 → 77,824 | 13.50000× → 9.50000× | n/a |
| A/1000 plain | 53,248 | 507,904 → 323,584 | 9.53846× → 6.07692× | n/a |
| A/1000 tool | 53,248 | 503,808 → 315,392 | 9.46154× → 5.92308× | n/a |
| A/4000 plain | 204,800 | 1,859,584 → 1,179,648 | 9.08000× → 5.76000× | n/a |
| A/4000 tool | 208,896 | 1,859,584 → 1,183,744 | 8.90196× → 5.66667× | n/a |
| B/100 plain | 40,960 | 172,032 → 110,592 | 4.20000× → 2.70000× | n/a |
| B/100 tool | 45,056 | 180,224 → 122,880 | 4.00000× → 2.72727× | n/a |
| B/1000 plain | 335,872 | 995,328 → 581,632 | 2.96341× → 1.73171× | n/a |
| B/1000 tool | 339,968 | 1,118,208 → 634,880 | 3.28916× → 1.86747× | n/a |
| **B/4000 plain** | 1,359,872 | 3,862,528 → **2,187,264** | 2.84036× → **1.60843×** | **PASS** (aim 1.5×: miss) |
| **B/4000 tool** | 1,392,640 | 4,501,504 → **2,494,464** | 3.23235× → **1.79118×** | **PASS** (aim 1.5×: miss) |
| C/1000 tool | 942,080 | 2,293,760 → 1,191,936 | 2.43478× → 1.26522× | n/a |
| C/4000 tool | 3,981,312 | 8,908,800 → 4,538,368 | 2.23765× → 1.13992× | n/a |

The B/4000 results match the profiling estimate (1.615×/1.797×) and the runtime
reference (1.605×/1.791×). Timings, ms, task 25 → 29 (last turn including
synchronous S2/collection; legacy in the same run for context):

| Fixture | Native turn | Legacy turn | Import | Legacy resume | Max collect call | Max write transaction | Delta writes / legacy |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| B/1000 plain | 428.82 → 400.60 | 59.60 | 824.67 → 825.34 | 7.43 | 90.49 → 73.78 | 36.68 → 14.87 | 10.675% → 35.94% |
| B/1000 tool | 673.55 → 680.24 | 115.51 | 971.16 → 1008.98 | 8.49 | 100.55 → 83.58 | 40.91 → 18.83 | 10.492% → 25.32% |
| B/4000 plain | 1585.06 → 1685.62 | 229.38 | 3361.03 → 3349.73 | 29.37 | 358.55 → 330.56 | 145.90 → 24.15 | 7.658% → 7.753% (PASS) |
| B/4000 tool | 2635.42 → 2547.75 | 427.33 | 3917.32 → 3925.23 | 34.16 | 423.68 → 326.24 | 157.21 → 56.46 | 5.440% → 5.514% (PASS) |
| C/1000 tool | 930.11 → 845.89 | 264.89 | 2941.98 → 2910.84 | 34.36 | 682.06 → 256.60 | 630.06 → 57.14 | 11.035% → 10.771% |
| C/4000 tool | 3937.09 → 3309.28 | 3938.27 | 12294.36 → 12235.53 | 185.84 | 2992.81 → 2106.75 | 2526.39 → 1580.27 | 3.214% → 3.100% (PASS) |

Turn and import times are within run-to-run noise of task 25. Turn overhead
fails the revised D10 budget; import is now a separately reported conversion
cost. The measured latency is dominated by runtime snapshot/proof and collection
tracing. Sharing shrinks the longest collection call and write
transactions, but C/4000's 2,106.75 ms collect call still exceeds D8's 1-second
review bound. The A rows ran while the machine was heavily loaded (their
legacy timings also rose 5–10×), so A timings in this run are not comparable;
their byte counts are unaffected. B/1000 delta ratios rise because the much
smaller retained layout makes S2/collection rewrites a larger share; the ≤10%
check applies to the B/N≥1000 rows and passes only at N=4000, as before.

The fixed upstream seeds are history `0xbadc0ffee1234567`, provider scripts
`0x12349876abcd5678`, and turn inputs `0x987612345678abcd`. A/B use two turns over
N seeded messages. C grows to N messages over N/4 tool turns, with a new writer
per turn and a 32-manifest window. Every Forge row uses the S2 terminal hook,
collection after every turn and a final shutdown sweep. C/N=4000 is supplemental;
its Forge per-turn result does not imply a per-turn rt-e7 reference run.

L2 comes from the frozen actual Forge legacy save/digest/order helpers, with two
separately measured ciphertexts and two nonces. Both layouts subtract the same
full-schema/session setup after checkpoint/VACUUM. File/page/dbstat agreement
includes verified pointer-map pages. Canonical byte/receipt counts remain
separate from physical file bytes. TEMP counters on all five connections count
committed pack writes, including intermediate rewrites, without main-WAL pages.

**Full Forge physical results, bytes.** The rt-e7 column is reference-only: modeled sealing and sharing between a session’s domains, unlike Forge. “Turn” and “end” describe that reference’s collection cadence; Forge rows all use per-turn collection.

| Fixture | Forge L1 | Actual Forge L2 | Forge before final sweep | Forge after collection | Forge/L2 | rt-e7 after / L2 ratio (cadence) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A/100 plain | 0 | 8,192 | 204,800 | 106,496 | 13.00000× | 57,344 / 7.00000× (end) |
| A/100 tool | 0 | 8,192 | 249,856 | 110,592 | 13.50000× | 61,440 / 7.50000× (end) |
| A/1000 plain | 20,480 | 53,248 | 598,016 | 507,904 | 9.53846× | 311,296 / 5.84615× (end) |
| A/1000 tool | 24,576 | 53,248 | 643,072 | 503,808 | 9.46154× | 307,200 / 5.76923× (end) |
| A/4000 plain | 90,112 | 204,800 | 1,953,792 | 1,859,584 | 9.08000× | 1,179,648 / 5.76000× (end) |
| A/4000 tool | 94,208 | 208,896 | 1,998,848 | 1,859,584 | 8.90196× | 1,179,648 / 5.64706× (end) |
| B/100 plain | 20,480 | 40,960 | 270,336 | 172,032 | 4.20000× | 102,400 / 2.50000× (end) |
| B/100 tool | 20,480 | 45,056 | 323,584 | 180,224 | 4.00000× | 94,208 / 2.09091× (end) |
| B/1000 plain | 159,744 | 335,872 | 1,110,016 | 995,328 | 2.96341× | 577,536 / 1.71951× (turn) |
| B/1000 tool | 163,840 | 339,968 | 1,277,952 | 1,118,208 | 3.28916× | 622,592 / 1.83133× (turn) |
| B/4000 plain | 655,360 | 1,359,872 | 4,132,864 | 3,862,528 | 2.84036× | 2,183,168 / 1.60542× (turn) |
| B/4000 tool | 655,360 | 1,392,640 | 4,812,800 | 4,501,504 | 3.23235× | 2,494,464 / 1.79118× (turn) |
| C/1000 tool | 454,656 | 942,080 | 2,482,176 | 2,293,760 | 2.43478× | 1,204,224 / 1.27826× (turn) |
| C/4000 tool | 1,896,448 | 3,981,312 | 9,117,696 | 8,908,800 | 2.23765× | 4,886,528 / 1.22863× (end) |

Identity-policy diagnostic B/100 plain measures 172,032 bytes versus 172,032 with production projection. The fixed fixtures contain no optional sensitive Forge diagnostic extension; identity and production projection are equal for these inputs. Identity is not the product policy.

**One bounded storage-only attempt.** The only functional candidate replaces streaming zstd level 3 object-body compression with one-shot zstd level 9; canonical objects, seeds, projection, domain copies, caps, baseline and acceptance thresholds stay unchanged. The recheck runs only unchanged B/N=4000 plain/tool under the same ignored release report. Candidate disposition is recorded in `storage-fix-disposition.json`.

| Candidate | Actual L2 | Native after collection | Ratio | ≤2.0× | ≤1.5× aim |
| --- | ---: | ---: | ---: | --- | --- |
| B/4000 plain | 1,359,872 | 3,788,800 | 2.78614× | FAIL | FAIL |
| B/4000 tool | 1,392,640 | 4,407,296 | 3.16471× | FAIL | FAIL |

The single candidate saved about 2% but both disk gates remained above 2.0× and turn/import checks still failed. It was reverted exactly to the fully measured level-3 baseline; no second storage candidate or schema-5 work was attempted. The candidate patch and its real-AEAD/sqlx recheck remain reviewable artifacts.

**D10 performance checks.** Delta bytes exclude the first terminal and include later S2/collection and the final shutdown sweep. The timing column compares the last actual turn plus synchronous maintenance with the matching legacy turn; raw run-only, all turn/open samples and means are also retained. Import compares real native import with a real legacy runtime resume, without provider/tool I/O. Autocheckpoint is disabled on all five connections so cumulative frames can be counted; each turn is sampled and the WAL is truncated, identically for both stores. Forced measurement checkpoints are outside turn timing. These are measurements from this machine, not confidence intervals.

| Fixture | Delta pack / legacy | ≤10% (N≥1000) | Native / legacy turn, ms | Turn check | Import / resume, ms | Import ≤2× | Max write transaction, ms | Max collect, ms |
| --- | ---: | --- | ---: | --- | ---: | --- | ---: | ---: |
| A/100 plain | 347.118% | n/a | 59.78 / 2.84 | FAIL | 59.82 / 0.82 | FAIL | 10.23 | 12.17 |
| A/100 tool | 380.137% | n/a | 116.36 / 4.91 | FAIL | 61.77 / 0.76 | FAIL | 18.04 | 18.67 |
| A/1000 plain | 50.582% | FAIL | 215.93 / 14.57 | FAIL | 420.50 / 2.29 | FAIL | 23.71 | 62.39 |
| A/1000 tool | 49.688% | FAIL | 298.38 / 25.40 | FAIL | 447.05 / 2.36 | FAIL | 22.92 | 59.22 |
| A/4000 plain | 12.437% | FAIL | 774.86 / 50.89 | FAIL | 1682.67 / 7.54 | FAIL | 88.58 | 236.69 |
| A/4000 tool | 12.505% | FAIL | 1117.76 / 100.80 | FAIL | 1765.87 / 8.11 | FAIL | 91.60 | 250.15 |
| B/100 plain | 57.301% | n/a | 83.96 / 6.99 | FAIL | 98.82 / 1.14 | FAIL | 10.22 | 13.98 |
| B/100 tool | 115.064% | n/a | 154.36 / 12.40 | FAIL | 117.89 / 1.35 | FAIL | 27.12 | 23.95 |
| B/1000 plain | 10.675% | FAIL | 428.82 / 47.84 | FAIL | 824.67 / 7.08 | FAIL | 36.68 | 90.49 |
| B/1000 tool | 10.492% | FAIL | 673.55 / 90.10 | FAIL | 971.16 / 8.05 | FAIL | 40.91 | 100.55 |
| B/4000 plain | 7.658% | PASS | 1585.06 / 201.71 | FAIL | 3361.03 / 27.53 | FAIL | 145.90 | 358.55 |
| B/4000 tool | 5.440% | PASS | 2635.42 / 377.46 | FAIL | 3917.32 / 32.04 | FAIL | 157.21 | 423.68 |
| C/1000 tool | 11.035% | FAIL | 930.11 / 260.08 | FAIL | 2941.98 / 32.10 | FAIL | 630.06 | 682.06 |
| C/4000 tool | 3.214% | PASS | 3937.09 / 1324.71 | FAIL | 12294.36 / 161.12 | FAIL | 2526.39 | 2992.81 |

**Writes, overlap and retirement.** WAL means cumulative WAL frames × page size, excluding WAL headers and VACUUM; TEMP measurement state contributes no main-WAL frames. Collection-call maxima exclude the separate retirement pass.

| Fixture | Native pack writes | Legacy payload writes | Largest collection call | Largest pack rewrite | Native / legacy WAL | Old+new peak | Native after retirement |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A/100 plain | 247,251 | 54,769 | 46,342 | 26,846 | 2,330,624 / 143,360 | 114,688 | 20,480 |
| A/100 tool | 363,118 | 83,364 | 47,494 | 40,010 | 3,133,440 / 241,664 | 118,784 | 20,480 |
| A/1000 plain | 592,525 | 358,995 | 14,642 | 17,906 | 2,818,048 / 626,688 | 561,152 | 20,480 |
| A/1000 tool | 681,638 | 531,994 | 20,595 | 20,515 | 3,620,864 / 917,504 | 557,056 | 20,480 |
| A/4000 plain | 1,931,484 | 1,388,627 | 14,528 | 14,448 | 4,132,864 / 1,896,448 | 2,064,384 | 20,480 |
| A/4000 tool | 2,020,054 | 2,049,358 | 17,293 | 17,213 | 4,956,160 / 2,695,168 | 2,068,480 | 20,480 |
| B/100 plain | 312,282 | 341,158 | 18,916 | 50,586 | 2,408,448 / 651,264 | 212,992 | 20,480 |
| B/100 tool | 623,788 | 500,517 | 116,575 | 76,098 | 3,317,760 / 933,888 | 225,280 | 20,480 |
| B/1000 plain | 1,139,906 | 2,734,701 | 20,549 | 23,716 | 3,321,856 / 3,461,120 | 1,331,200 | 20,480 |
| B/1000 tool | 1,376,724 | 3,896,485 | 12,610 | 31,385 | 4,112,384 / 4,747,264 | 1,458,176 | 20,480 |
| B/4000 plain | 4,600,402 | 11,386,086 | 298,705 | 226,990 | 6,823,936 / 13,606,912 | 5,222,400 | 20,480 |
| B/4000 tool | 5,357,652 | 16,325,355 | 303,507 | 228,840 | 8,126,464 / 18,653,184 | 5,914,624 | 20,480 |
| C/1000 tool | 110,326,108 | 998,506,302 | 273,213 | 151,425 | 566,349,824 / 1,175,695,360 | 3,235,840 | 57,344 |
| C/4000 tool | 546,548,246 | 16,999,152,382 | 403,616 | 201,983 | 2,409,234,432 / 19,170,725,888 | 13,062,144 | 40,960 |

S2 amplification uses committed non-maintenance pack payload+nonce bytes as its denominator; it is distinct from canonical-byte amplification. The JSON includes L1, both individual ciphertext lengths, row/index/table page totals, freelists, post-retirement session/operation/pin/catalog bytes, retained/held object counts, canonical retained/live/deferred bytes and physical receipts for every case. It also records geometric S2 and total maintenance write amplification and every per-turn pack/WAL sample. These remain distinct units; canonical reclaimed bytes are never substituted for physical page savings.

**Memo and cache evidence.**

Cold reopened validation/open time ranges from **16.99 to 6585.18 ms** across both domains. Warm reopened-root probes classify **28 hits / 0 misses**, using zero/nonzero storage object reads. They are observable open-level counts, not internal memo-lookup counters.
C/1000 tool: 500 runtime engine-open observations, 500 with no body reads and 0 with body reads; mean runtime resume/open 698.12 ms.
C/4000 tool: 2000 runtime engine-open observations, 2000 with no body reads and 0 with body reads; mean runtime resume/open 3192.28 ms.

With **100 unrelated sessions / 805 packs**, storage construction decodes zero units. Target hydration decodes only its **3 indexes**, takes **3.75 ms**, and charges **452,109 bytes**, including **24,839 directory bytes**, under the 64 MiB cache budget. The one-byte cache charges zero and rejects oversize entries. Under a 16 KiB budget, 100 scopes charge 14,621 bytes, with 396 evictions and 104 cumulative oversize rejections. Exact cache hit/miss/read/decode counters are in JSON.

The protected collect call with 100 dirty sessions holds the engine gate for at most **779.05 ms** including call overhead: **PASS** against D8’s 1-second release-review bound. Its release allowance remains 20,000, not a session/time bound.
The ordinary collect call with 100 dirty sessions holds the engine gate for at most **705.70 ms** including call overhead: **PASS** against D8’s 1-second release-review bound. Its release allowance remains 20,000, not a session/time bound.

Peak process resident high-water memory reaches **2773.59 MiB**, measured through safe `getrusage`, not a process listing. It includes runtime working sets, full-view/import diagnostics, measurement records, allocator retention and storage caches; it is not memo heap. Keep **128 MiB estimated charge per engine** unchanged: the observed warm-root evidence does not establish a need to raise it. The review’s 1.24× observation corresponds to 158.72 MiB per engine / 317.44 MiB combined at full charge; that is reference headroom, not an observed heap cap.

The longest collect call overall is C/4000 tool at **2992.81 ms**: **FAIL** against the one-second review bound, despite the passing 100-small-session calls.

**NOT MET telemetry:** the pinned runtime keeps RootMemo private and exposes no charge/heap or internal hit/miss/eviction/oversize API. Those JSON fields are null with a reason; storage cache charge is an estimate, not measured allocator heap. No unsafe layout inspection, runtime patch, invented counters or production budget increase was used. Task 24 remains unchecked for these missing mandatory metrics despite the working harness; task 25 records completed gate execution and its failed verdict.


The complete JSON, candidate patch and verification receipts are retained under
`/Volumes/Data/tmp/refactor/c3-t24-25/`. Native-only retirement sizes exclude the
legacy row only for measurement; production retirement retains legacy payloads.
Old-plus-new peak is the larger of the two measured overlap layouts, not an
in-flight WAL/allocator peak. Source and method live in
`crates/agent-host/tests/session_journal_bench.rs` and `tests/support/bench_*.rs`.

Run the small debug smoke or the full ignored report:

```bash
cargo test --offline -p forge-agent-host --features test-support --test session_journal_bench packed_n100_smoke
cargo test --offline -p forge-agent-host --features test-support --release --test session_journal_bench forge_packed_report -- --ignored --exact --nocapture
```

`FORGE_JOURNAL_MEASUREMENT_DIR` selects the output directory. The default is
`$TMPDIR/forge-session-journal-measurement`. For a bounded storage-candidate
recheck, `FORGE_JOURNAL_GATE_RECHECK=1` selects only unchanged B/N=4000 plain/tool
under the same ignored report. It is a test-harness option, not a product flag.
The report records numeric pass/fail values while validating all measurements;
a successful report process is not an adoption-pass signal.
<!-- session-journal-d10:end -->

## Task 33 runtime hot-path rerun (2026-10-24)


Measured receipt

Interim runtime `b0a8c90e96ccb85e4642a119117a8fd5d3a06993` (0.9.0); final pin is the **v0.10.0 tag**. Forge now forwards preparation hints and opts into equivalent incremental projection. Source/verification/every test edit: `/Volumes/Data/tmp/refactor/c3-t33/report.md`. Full reports used the unchanged fixtures, production projection, real AEAD/sqlx, same-run legacy L2, S2/per-turn collection and bounded final drains.

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

**NOT MET:** Fixed steady-turn overhead: A/100 tool, B/100 tool. Mean across both turns (including cold first-turn work) fails its recorded fixed check for: A/100 plain, A/100 tool, B/100 plain, B/100 tool, B/4000 plain, B/4000 tool; keep it separate from the steady delta-turn metric. B/1000 delta-write <=10% remains tracked; see measured percentages above. The 1.5x disk aim is separate from the passing 2.0x B/4000 hard gate. Unfinished production shapes: C/4000 tool. A timed-out full report is not a completed gate; no missing C/4000 values are inferred. Exact allocator heap is unavailable through the runtime API; telemetry is estimated charge/scoped counters plus RSS. Task 24 remains open for that gap. The final v0.10.0 tag pin remains pending; the owner-authorized interim SHA is still versioned 0.9.0. All Owner Decisions below remain verbatim.
