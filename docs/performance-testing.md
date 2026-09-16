# Performance stabilization: Operations and log diagnostics

This is a targeted testable patch, not a scheduler rewrite or a claim that
all performance findings are resolved. No database migration, permission,
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
- Operations recognizes `blocked_json` independently of workflow phase.
  Deleted, archived, `done`, and `cancelled` tasks are excluded from this
  blocker list. The existing display priority is preserved: `error_annotation`
  takes precedence, with the structured blocker reason used only when it is
  absent. The query never changes a Task's phase.
- The unused queued-task scan is removed from Operations status computation.

The cache is a rebuildable display optimization. It is not used to authorize
commands, renew leases, declare completion, calculate billing, or decide
milestone readiness. The existing `turn_count` interpretation (Assistant log
records, not provider calls) is unchanged.

**Remaining work:** a changed log still needs one full scan; this is not yet
an append-offset summary index. The general execution/chat `LogReader::read`
and `tail` endpoints, broad frontend SSE invalidations, outbox duplication,
native per-delta progress writes, and dispatcher scans are not rewritten by
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
| `streaming` | A Task producing output for 120 seconds | Live text still arrives; Operations activity refreshes are bounded |
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
python3 -m unittest discover -s scripts -p test_collect_performance.py -v
cargo fmt --all -- --check
```

The Rust filter covers summary scan counts, cache reuse/eviction/expiry,
concurrent reads, append/truncate/replacement, partial JSON, missing files,
blocker projection, and notification throttling. The repository's existing
CI remains responsible for full workspace coverage.

Do not call a branch fully verified until its Rust checks and the actual
interactive scenarios have completed. Python collector tests validate the
collector only; they do not validate the Rust application or demonstrate an
end-to-end speedup.
