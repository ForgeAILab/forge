# Forge v0.11.0 request-storm snapshot

Captured on 2026-09-10 at 19:48 EDT before stopping the installed release.

## Runtime snapshot

- Listener: `127.0.0.1:18080`
- Executable: `/Users/mai1015/.forge/npx/releases/v0.11.0/forge-aarch64-macos/forge`
- Server PID: `67232`
- Launcher PID: `67181` (`node .../.bin/forge`)
- Server CPU at capture: about `245%`
- Server memory at capture: `0.4%`; sampled physical footprint `136.9 MB`
- Established browser connections: 7
- SQLite database: about `109.6 MB`; WAL: about `5.8 MB`
- Current log: about `25 MB`

Bounded unauthenticated curls showed that the listener itself was not wedged:

- `GET /`: HTTP 200 in 1.275 ms
- `GET /api/v1/projects`: HTTP 401 in 0.716 ms

The slowdown was path/load-specific. In the latest 2,000 logged authenticated GET
completions, the largest groups were:

| Count | Path |
| ---: | --- |
| 420 | `/api/v1/projects/5009dbfa-29cc-443f-95aa-0f7dfc6708f1/tasks` |
| 392 | `/api/v1/projects/5009dbfa-29cc-443f-95aa-0f7dfc6708f1/workflow` |
| 354 | `/api/v1/projects/5009dbfa-29cc-443f-95aa-0f7dfc6708f1/members` |
| 344 | `/api/v1/projects/5009dbfa-29cc-443f-95aa-0f7dfc6708f1` |
| 287 | `/api/v1/projects/5009dbfa-29cc-443f-95aa-0f7dfc6708f1/agents` |
| 185 | `/api/v1/projects` |

The end of the sample showed repeated agent-list requests reaching roughly
119-487 ms and repeated task-list requests reaching roughly 15-82 ms. An
earlier sample during the same incident observed overlapping task-list requests
at roughly 2.3-2.5 seconds.

This evidence points to a browser request/invalidation loop driving server CPU,
not a dead TCP listener or a generally slow unauthenticated HTTP stack.

## Source fixes present in the checkout

The source checkout now:

- disables automatic React Query retries and reconnect refetches;
- stops interval polling after a query enters an error state;
- gives SSE reconnects capped exponential backoff that resets only after a
  stable connection window;
- delays the broad SSE resync so an immediately flapping connection cannot
  refetch every active query;
- coalesces task-list invalidations across wildcard and project-specific events
  and does not cancel an in-flight refetch;
- forwards React Query abort signals to task-list fetches;
- leaves the command-palette task query disabled while the palette is closed;
- prevents the pending-chat watchdog from refetching queries already in error.

The same checkout also contains the assignment/subtask lifecycle correction:
MCP assignment is assignment-only, coordination roots do not run implementation,
children have independent assignments and Executions, ordered children share the
root workspace serially, and the root advances to aggregate review only after its
children are terminal.

These source changes have not been installed over the release captured above.
`pnpm typecheck` and compile-only checks for `db`, `services`, `mcp-server`, and
`api` passed. No unit, integration, browser, or live behavior tests were run.

## Shutdown

PID `67232` was sent `SIGTERM` after capture. It and launcher PID `67181` exited
cleanly, and a final listener check confirmed that port `18080` was free. No
database files or release artifacts were changed.
