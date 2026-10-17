---
created_at: 2026-10-02T08:55:00Z
updated_at: 2026-10-02T14:47:10Z
completed_at: 2026-10-02T14:47:10Z
---

## 1. Server host cap
- [x] 1.1 Config key `server.max_concurrent_runs` (file, environment, CLI flag); unset = automatic (half the logical cores, at least 2), `0` = unlimited
- [x] 1.2 Settings API and Forge Settings page: read and update, showing the automatic value in effect; applies without a restart
- [x] 1.3 Placement counts runs on the server host and rejects it at the cap, at reserve and at execution start

## 2. Daemon cap
- [x] 2.1 Daemon configuration and flag with the same automatic default; typed field at registration and in reports; stored on the daemon row
- [x] 2.2 Migration: columns for the daemon's cap and the admin limit; carry over a positive label cap; remove label parsing
- [x] 2.3 Admin limit: `PATCH /api/v1/daemons/{id}` (admin only) and the Machines page; effective cap = lower of the two

## 3. Admission and visibility
- [x] 3.1 Filter code `machine_capacity` replaces `daemon_capacity`; Tasks wait with a visible reason and start when a run ends
- [x] 3.2 Operations status: server host and daemons with occupied runs and effective cap
- [x] 3.3 Docs (`api.md`, `architecture.md`, `cli.md`, `getting-started.md`) and CHANGELOG (`### Breaking`, `### Added`)

Changelog text is delivered in the final implementation report and reply, per the owner instruction to leave `CHANGELOG.md` unchanged.

## 4. Independent audit follow-up
- [x] 4.1 Quiet, read-only recovery capacity precheck and equivalent-marker reuse
- [x] 4.2 Current-version parked machine waits, Project memo invalidation and automatic review recovery
- [x] 4.3 Execution-start capacity recheck and ready-workspace waiting; exclude expired reservations
- [x] 4.4 Replace reserve probe with read-only routing/count prechecks; remove db/config coupling and cache identity
- [x] 4.5 Port file-backed race/reproductions, correct names/docs, regenerate types and focused validation

## 5. Second audit corrections
- [x] 5.1 Remove launch holds and restore base reservation, ready reclaim and recreation behavior
- [x] 5.2 Recheck every candidate before expensive gates; preserve todo state and quiet steady ticks
- [x] 5.3 Tie capacity reasons to actual outcomes; stable edit projection and Project cap on un-parking
- [x] 5.4 Quiet automatic-review recovery and lost-race queued replay, with next-tick resumption
- [x] 5.5 Conservative precheck, shared handle identity, expired Agent pressure and full requested module checks
