---
created_at: 2026-09-30T21:00:00Z
updated_at: 2026-09-30T21:00:00Z
---

## Why

Two gaps kept NovelKit Tasks stuck with nobody told.

1. **No way for an agent to ask for configuration.** NK-5 needed a live
   OpenAI-compatible endpoint to prove "summary under 60s", and NK-7 needed a
   real Ollama host. The agents could only fail the review or write "not
   verified" in prose. Project `environment.env` is plain text that only the
   owner edits, and it is explicitly not a secret store.
2. **The Project Agent could not unblock anything.** On 9/30 it was woken for
   each `environment_not_ready` block and ran `reexecute` 7 times in 30
   minutes while the disk was still at 7G. Each retry re-blocked, wake
   suppression (`duplicate_incident`, `cooldown`, `retry_exhausted_same_chat`)
   silenced it, and nothing woke it when the disk recovered. Every reply said
   "no user action needed". Earlier that morning, 85 wakes were dropped as
   `budget_exhausted` after repeated readiness attempts that failed the same
   way.

Depends on `add-project-flow-control` (environment pause, `review_needs_owner`).

## What Changes

- **Project configuration items.** A Project has named configuration items,
  each with `kind: value | secret`, a description, and a status (`requested`
  or `provided`). Provided items are injected as environment variables into
  every execution, review setup/CI step, conformance check, environment check,
  and lifecycle hook, exactly like `environment.env`. Secret values are sealed
  in the existing protected store, never returned by the API, and redacted
  from logs and transcripts.
- **Agents request, owners provide.** Three sources can create a
  `requested` item with a reason and the Tasks it blocks: the Project Agent
  (tool `project.config.request`), the coder (outbox entry), and the reviewer
  (`needs_config` in the result block). Agents cannot write values. The Task
  parks as `needs_config`, with no retry budget spent.
- **Owner pastes the value.** Pending requests appear in Attention, on the
  Project header ("2 settings needed"), and on a Project **Configuration**
  page with a paste field. Once every item a parked Task needs is provided,
  Forge re-runs that Task's blocked step automatically.
- **Project Agent unblocking duties.**
  - While a Project is environment-paused, recovery attempts on its Tasks are
    refused with the pause detail, so there are no blind retries. The agent
    gets one project-level wake per pause instead of one per Task.
  - The agent must end each blocker wake with an unblocking action or an owner
    escalation. The escalation is a Notification plus an Attention item that
    names the exact need; "no action needed" is not allowed while the blocker
    persists.
  - The agent is woken again when a pause clears or a config item is provided.
  - Blocker wakes draw from a reserved share of the wake budget, so delivery
    follow-ups cannot starve them.
- New API: `GET/POST /api/v1/projects/{id}/config`, `PUT
  /api/v1/projects/{id}/config/{name}` (owner only), and `DELETE`; forge-ctl
  `project config`; a new MCP/native tool `project.config.request`; a new
  `FailureKind::NeedsConfig`.

## Impact

- Affected specs: `project-config-requests`, `project-agent-unblocking`
- Affected code:
  - `crates/db`: migration V150 (`project_config_item`, sealed values via the protected store)
  - `crates/api-types`
  - `crates/api/src/routes/projects.rs`
  - `crates/services`: env injection next to `mark_task_environment`, review result parsing, the cascade park, outbox intake, wake routing and budget, and Project Agent tools and doctrine
  - `crates/mcp-server`
  - `web`: Configuration page, Attention card, header badge
  - `docs/api.md`, `docs/architecture.md`, `docs/cli.md`, `CHANGELOG.md`
