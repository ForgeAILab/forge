---
created_at: 2026-10-02T08:55:00Z
updated_at: 2026-10-02T10:35:49Z
---

## Why
Nothing limits how much agent work one machine runs at once. The per-Agent cap (`max_concurrent_tasks`) and the per-Project cap (`max_active_tasks`) both add up across Agents and Projects, the per-daemon session cap exists only as an untyped label with no default, and the server host has no machine-level cap at all. A handful of Projects can therefore start more agent runs than the host can carry. The owner hit this while testing and asked for a cap in settings.

## What Changes
- **Every machine has a run cap.** A machine is the server host or one daemon. A run is a Running Task execution, a workspace reservation that has not started yet, or an in-flight Agent Chat turn, counted on the machine that executes it (the count the daemon session cap already uses).
- **Server host cap in settings.** New setting `server.max_concurrent_runs` (config file, environment, CLI flag, Settings API and the Forge Settings page). Unset means automatic: half the logical cores, at least 2. `0` means unlimited. A change applies without a restart.
- **Daemon cap is a typed field.** A daemon has the same setting in its own configuration with the same automatic default, computed on that machine, and reports it at registration and in status reports. **BREAKING**: the cap is no longer read from `labels_json` (`max_concurrent_sessions`, `max_sessions`, `active_session_cap`, `max_concurrent_tasks`); the migration carries an existing label value over.
- **Admin limit per machine.** An administrator can set a limit for any daemon from the Machines page. The effective cap is the lower of the daemon's own cap and the admin limit.
- **Default changes from unlimited to automatic.** **BREAKING**: a server host or daemon that had no cap now gets the automatic one.
- **Enforced at placement.** A machine at its cap is rejected at admission and again at execution start with `machine_capacity` (**BREAKING**: replaces the filter code `daemon_capacity`, and now applies to the server host too). A Task with no machine left waits in its state with a visible reason and starts when a run ends. Nothing running is ever stopped.
- **Visible.** Operations status lists every machine, including the server host, with occupied runs and the effective cap.

Not in this change: review check runs and merges do not take a slot (a running check has no durable record to count; follow-up after refactor item 2.3), no load-average guard, no pre-emption.

## Impact
- Affected specs: `machine-capacity` (new)
- Affected code: `crates/config`, `crates/api/src/routes/{settings,daemons,operations}.rs`, `crates/api-types` (settings, daemon registration/report, operations, placement filter codes), `crates/services/src/placement/{capacity,selection}.rs`, `crates/services/src/{agent_capacity,operator_status}.rs`, `crates/forge-daemon`, `crates/forge-client` (daemon), `crates/db` (daemon row + migration), `web/src/pages/{ForgeSettingsPage,DaemonsPage,OperationsPage}.tsx`, `docs/{api,architecture,cli,getting-started}.md`
