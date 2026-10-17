---
created_at: 2026-10-03T00:00:00Z
updated_at: 2026-10-03T06:05:53Z
---

## Why
Concurrent runs each start build tools that use every core. The machine run cap alone cannot keep the Forge host responsive.

## What Changes
- Each machine supplies a build budget to CLI agent children, native commands, review CI, Project hooks and environment checks/setup. Project environment wins, then the operator process environment, then the budget.
- Automatic jobs are `max(1, logical_cores / cap)`. A configured positive machine run cap is the divisor; an unset or unlimited cap uses the automatic run cap. `build_jobs_per_run: 0` disables budget variables; a positive value sets an exact budget.
- Run children on Unix receive a niceness increment of 10 by default, configurable from 0 through 19. Failure to lower priority never fails a run. Windows ignores niceness.
- Server config/environment/flags, live Settings API/page and local daemon YAML/flags expose these controls. Operations adds available machine facts. Remote daemon policy stays local; no protocol change or migration.

## Impact
- Spec: `machine-capacity`, extending `add-machine-capacity-cap-2026-10-02`.
- Code: config, executors, CLI adapters, native host, review, services launch boundaries, Settings and operations types/API/web, server and daemon entrypoints.
- Documentation: API, architecture, getting started and CLI.
- Changelog text is supplied in the final report; `CHANGELOG.md` stays unchanged per the brief.
