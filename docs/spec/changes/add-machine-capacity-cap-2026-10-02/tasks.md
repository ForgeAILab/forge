---
created_at: 2026-10-02T08:55:00Z
updated_at: 2026-10-02T08:55:00Z
completed_at:
---

## 1. Server host cap
- [ ] 1.1 Config key `server.max_concurrent_runs` (file, environment, CLI flag); unset = automatic (half the logical cores, at least 2), `0` = unlimited
- [ ] 1.2 Settings API and Forge Settings page: read and update, showing the automatic value in effect; applies without a restart
- [ ] 1.3 Placement counts runs on the server host and rejects it at the cap, at reserve and at execution start

## 2. Daemon cap
- [ ] 2.1 Daemon configuration and flag with the same automatic default; typed field at registration and in reports; stored on the daemon row
- [ ] 2.2 Migration: columns for the daemon's cap and the admin limit; carry over a positive label cap; remove label parsing
- [ ] 2.3 Admin limit: `PATCH /api/v1/daemons/{id}` (admin only) and the Machines page; effective cap = lower of the two

## 3. Admission and visibility
- [ ] 3.1 Filter code `machine_capacity` replaces `daemon_capacity`; Tasks wait with a visible reason and start when a run ends
- [ ] 3.2 Operations status: server host and daemons with occupied runs and effective cap
- [ ] 3.3 Docs (`api.md`, `architecture.md`, `cli.md`, `getting-started.md`) and CHANGELOG (`### Breaking`, `### Added`)
