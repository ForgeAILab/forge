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
