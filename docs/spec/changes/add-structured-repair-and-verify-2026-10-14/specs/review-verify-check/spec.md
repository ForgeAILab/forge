## ADDED Requirements

### Requirement: Verify configuration in the review config
`ReviewConfig` SHALL accept an optional `verify` object. It can be set
wherever `ci_steps` can be set: the workflow state config, the Project's
`default_review_config`, or the Task's `task_state_config`. The same merge
order applies.

Fields:

| Field | Required | Default | Limits |
|---|---|---|---|
| `start` | no | — | command that starts the app |
| `ready_path` | no | — | URL path, polled until it answers |
| `ready_command` | no | — | command, run until it exits 0 |
| `ready_timeout_seconds` | no | 120 | 1–900 |
| `checks` | yes | — | 1–16 items |
| `timeout_seconds` | no | 1200 | 1–3600 |

- `ready_path` and `ready_command` are mutually exclusive.
- Each item in `checks` is `{ name, command, timeout_seconds }`:
  - `name` matches `[a-z0-9-]{1,32}` and is unique within `checks`.
  - `command` is required.
  - `timeout_seconds` is 1–3600 and defaults to 600.

Invalid values SHALL be rejected by the Project and Task update routes with a
field-level error.

#### Scenario: Valid web verify
- **WHEN** the owner sets `verify: {"start":"pnpm dev --port \"$FORGE_VERIFY_PORT\"","ready_path":"/","checks":[{"name":"smoke","command":"./scripts/verify.sh"}]}` on a Project
- **THEN** the Project's `default_review_config.verify` stores it, and new Tasks inherit it

#### Scenario: Both readiness forms
- **WHEN** a `verify` sets both `ready_path` and `ready_command`
- **THEN** the update is rejected naming `verify.ready_command`

### Requirement: Verify runs after review CI and before the reviewer
Forge SHALL run verify as a separate check request with purpose
`review_verify` when the effective review config has `verify` and every
review CI step passed. Verify runs in the same review attempt, before Forge
dispatches any reviewer execution.

- A failing verify SHALL settle the Review as failed without a reviewer
  execution, and route through the repair rules.
- A passing verify SHALL continue to the reviewer as today.
- Without `verify`, review behaves exactly as before this change.

Verify commands are `Uncacheable`. Verify SHALL NOT change whether the review
CI result can be reused.

A review re-entered after an earlier pass keeps its current handling. A
verify failure there SHALL block the Task the same way a CI failure does on
that path (`merge-fix follow-up failed: ci`), with no new routing.

#### Scenario: Verify fails before the reviewer
- **WHEN** CI passes and the `smoke` check exits 1
- **THEN** the Review fails with an open finding `V-smoke`, no reviewer execution is created, and the coder gets a repair round

#### Scenario: CI fails first
- **WHEN** a review CI step fails
- **THEN** verify does not run in that attempt

### Requirement: Forge starts the app and provides its inputs
For a verify run, Forge SHALL:
- Pick a free loopback port when the run starts.
- Create `<task root>/.forge-task/verify/<review attempt>/`.
- Write `request.json` there, containing:
  - `task_id`, `title`, `description`;
  - the Task's requirement texts;
  - `base_sha`, `commit_sha`, `changed_files`;
  - the open findings from the ledger.
- Export these variables to every verify step:
  - `FORGE_VERIFY_PORT`;
  - `FORGE_VERIFY_URL` (`http://127.0.0.1:<port>`);
  - `FORGE_VERIFY_DIR`;
  - `FORGE_VERIFY_REQUEST`;
  - `FORGE_VERIFY_REPORT` (`<dir>/<check name>.report.json`, set per check).

Steps run in this order:
1. When `start` is set, Forge starts it in the background, with output going
   to `<dir>/app.log`.
2. Forge waits for readiness: `FORGE_VERIFY_URL` + `ready_path` answers with
   any HTTP status below 500, or `ready_command` exits 0. It waits at most
   `ready_timeout_seconds`.
3. Forge runs each check in order.

#### Scenario: Wrapper reads the request
- **WHEN** a verify check runs `jq .changed_files "$FORGE_VERIFY_REQUEST"`
- **THEN** it prints the files changed between the Task's base and candidate commits

### Requirement: Verify processes do not outlive the run
Verify SHALL run under an execution policy that stops every process group the
run started, and verifies that stop, when the run ends, whatever the outcome.
Version 1 SHALL use the unchanged `canonical-ci/1` policy. All verify
commands are `Uncacheable`, so no verify result is ever reused.

On a daemon-owned placement, Forge SHALL NOT run verify. It SHALL record
`verify.status = not_run` with reason `placement_unsupported` in the Review
details, and continue to the reviewer.

#### Scenario: Dev server is stopped
- **WHEN** a verify run with `start: "pnpm dev"` finishes, passed or failed
- **THEN** no process from that run is left on the machine and the receipt says the process tree was stopped

#### Scenario: Daemon placement
- **WHEN** a Task placed on a daemon reaches review with `verify` configured
- **THEN** the Review details record `verify: {"status":"not_run","reason":"placement_unsupported"}`, the reviewer runs, and the review comment says verify did not run

### Requirement: Verify verdicts
Each verify check SHALL have exactly one result:
- **fail** when the command exits non-zero, times out, or its report has
  `verdict` `fail` or `failed`.
- **inconclusive** when it exits 0 and its report has `verdict`
  `inconclusive`.
- **pass** otherwise.

The report at `FORGE_VERIFY_REPORT` is optional JSON with these fields:
- `verdict`;
- `summary`;
- `findings`: a list of `{summary, where, expected, observed}`;
- `evidence`: a list of `{path, kind, caption}`.

A failed check becomes the finding `V-<name>`. Its text comes from the report
when one is present, otherwise from the output tail. If the app's start
command exits or readiness times out, the result is a fail with `V-start` or
`V-ready`, carrying the tail of `app.log`.

On the first inconclusive result in a review attempt, Forge SHALL re-run
verify once, using the volatile budget `review_verify_rerun` with limit 1. On
a second inconclusive result, the Task SHALL park with `review_blocked`
(source `verify`), offering `retry` (re-run verify), `mark_reviewed` (reason
required) and `cancel_task`. Neither park charges the review budget.

A runner infrastructure failure SHALL keep the existing review CI
infrastructure handling.

#### Scenario: Report overrides a zero exit
- **WHEN** a check exits 0 but its report says `{"verdict":"failed","summary":"Save button does nothing"}`
- **THEN** the check fails and `V-<name>` carries "Save button does nothing"

#### Scenario: App never becomes ready
- **WHEN** `ready_path` keeps refusing connections for `ready_timeout_seconds`
- **THEN** the Review fails with `V-ready`, whose observed text is the tail of `app.log`

#### Scenario: Inconclusive twice
- **WHEN** a check reports `inconclusive` on its run and on the automatic re-run
- **THEN** the Task parks with `review_blocked` (source verify) offering retry, mark_reviewed and cancel, and its review budget is unchanged

### Requirement: Verify evidence is kept and shown to the reviewer
Forge SHALL capture each `evidence[].path` in a check's report as Task
evidence through the existing `task.evidence` storage, with the same kinds and
the 25 MiB per-file limit. Paths are relative to `FORGE_VERIFY_DIR` and must
stay inside it.

The Review's details SHALL gain a `verify` object with:
- the overall status;
- per check: name, result, summary and evidence media ids;
- the `app.log` tail.

`verify` SHALL be an allowed Review detail key and SHALL be exposed in the
review responses and generated TypeScript.

When verify passes, the reviewer's prompt SHALL include a "Verify results"
section with each check's result, summary and evidence file paths.

#### Scenario: Screenshots reach the reviewer
- **WHEN** a passing `smoke` report lists two screenshots
- **THEN** both are stored as Task evidence, their media ids appear in `details.verify.checks[0].evidence`, and the reviewer prompt names both files

#### Scenario: Path escape refused
- **WHEN** a report lists `"path": "../../etc/passwd"`
- **THEN** that entry is skipped with a note in the Review details, and the check's result is unchanged
