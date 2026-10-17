---
created_at: 2026-09-30T21:00:00Z
updated_at: 2026-10-04T23:59:28Z
completed_at:
---

## 1. Configuration items
- [ ] 1.1 V150 `project_config_item` (name, kind, description, status, requester, blocked task ids, plain value or sealed ciphertext + nonce + key revision)
- [ ] 1.2 Seal and open through the protected store cipher; name validation (env name syntax, reserved names, collision with `environment.env`)
- [ ] 1.3 REST: `GET`/`POST /projects/{id}/config`, owner-only `PUT`/`DELETE /config/{name}`; api-types, generated TS, `docs/api.md`
- [ ] 1.4 Injection alongside `mark_task_environment`, review steps, conformance checks, env checks, and hooks; secret values added to the redaction set
- [ ] 1.5 forge-ctl `project config list|set|unset`
- [ ] 1.6 Focused tests: secret never serialized, injection reaches the executor and review check, redaction, owner-only writes

## 2. Requests and parks
- [ ] 2.1 `FailureKind::NeedsConfig` park (no coder dispatch, no budget)
- [ ] 2.2 Reviewer `needs_config` in the result block (lenient parse) + prompt text
- [ ] 2.3 Coder outbox `config_request` intake
- [ ] 2.4 Project Agent tool `project.config.request` (native + MCP), refused when the item is already provided
- [ ] 2.5 On provide: clear parks whose items are all provided, re-run the blocked step, emit `project.config.provided`
- [ ] 2.6 Focused tests: reviewer request parks, provide resumes the review, partial fill waits, duplicate request refused

## 3. Project Agent unblocking
- [ ] 3.1 `task.recover` refused with `environment_paused` while the Project is env-paused; one project-level wake per pause
- [x] 3.2 `project.escalate` tool → Notification + Attention; automatic escalation when a blocker turn ends with no recorded outcome
- [x] 3.3 Wake when the owner answers an escalation (folded into Refactor 3.5)
- [ ] 3.3a Wake on environment pause cleared: follow-up; next has only the in-process `project.resumed` event, no durable event to consume
- [ ] 3.3b Wake on config provided: follow-up to sections 1–2, which are not built
- [x] 3.4 Reserved budget share for blocker wakes; `repeated_failure` suppression for delivery follow-ups
- [x] 3.5 Doctrine: verify recovery took effect, never retry an unchanged blocker, escalate the exact need
- [x] 3.6 Focused tests for the folded 3.2–3.5 scope (3.1, 3.3a and 3.3b remain follow-ups)

## 4. UI and release
- [ ] 4.1 Configuration page (list, paste, mask), header badge, Attention card, `needs_config` task card text
- [ ] 4.2 Docs (`api.md`, `architecture.md`, `cli.md`), CHANGELOG
- [ ] 4.3 Live check on NovelKit: NK-5-style live-endpoint request → paste → review resumes; disk pause → agent escalates once → resume wakes the agent
