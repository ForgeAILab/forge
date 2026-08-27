<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/forge-wordmark-transparent.png">
  <img src="assets/forge-wordmark.png" alt="Forge" width="420">
</picture>

**Make multiple coding agents collaborate on one repo — without stepping on each other.**

[![CI](https://github.com/ForgeAILab/forge/actions/workflows/ci.yml/badge.svg)](https://github.com/ForgeAILab/forge/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/ForgeAILab/forge?include_prereleases&sort=semver)](https://github.com/ForgeAILab/forge/releases)
[![npm](https://img.shields.io/npm/v/@forgeailab/forge)](https://www.npmjs.com/package/@forgeailab/forge)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status: public beta](https://img.shields.io/badge/status-public%20beta-orange.svg)](#status)

Forge hosts durable embedded assistants and task-scoped coding agents in one
local control plane. Talk to one global Main Agent, hand approved context to
one Project Agent per Project, and keep each chat's continuity without granting
repository access outside an admitted Task. Every coding Task still gets an
isolated git worktree, CI gate, and review before changes touch `main`. REST,
MCP, CLI, and web UI ship in one self-hosted binary.

[Quickstart](#5-minute-quickstart) · [Why Forge](#why-forge) · [Docs](docs/) · [Changelog](CHANGELOG.md) · [Contributing](CONTRIBUTING.md)

</div>

---

## Why Forge

Running two coding agents against the same repo is how you lose diffs. Forge fixes
that: every task runs in its own git worktree, hits your CI gate, and waits for
review before it merges. **Agents collaborate; they don't collide.**

- **One isolated git worktree per task** — Claude Code, Codex, Cursor, Gemini, and Smith can each work in parallel without overwriting each other or polluting your main checkout.
- **Main Chat and Project Agent Chats** — one global discovery timeline and one durable conversation per Project; the agent creates tasks and shapes Project records directly from chat, and Main-to-Project handoffs stay explicit and provenance-linked.
- **Persistent embedded identities** — use Agent Settings for API keys, guided provider login, and exact CLI runtimes. Any enabled configured Agent can serve as Main, Project, Worker, or reviewer—including the same Agent in multiple roles—while each turn or Task still receives only its scoped permissions.
- **Mission Control** — inspect attention, commitments, current scope, health, and recent outcomes without opening a wall of runtime logs.
- **Review that follows the Task** — define `ci_steps` per Task and choose Agent review, no review, or human-required review. Human-required review can be accepted or rejected by the user or the bound Project Agent.
- **Structured task lifecycle** — `todo → in_progress → review → merging → done`, with an audit log and explicit cancellation paths so handoffs between agents (and humans) are legible.
- **BYO agent** — first-class adapters for Claude Code, Codex, Cursor, Gemini, opencode, Smith, and a generic shell executor. Add your own with a small adapter.
- **One binary, every surface** — REST API, MCP JSON-RPC, `forge-ctl` CLI, and a React web UI ship together. Drive Forge from a script, an editor, or a browser.
- **Local-first by default** — single binary, SQLite, loopback-only server with a persisted local port. No telemetry, no SaaS, your data stays on disk.

## Who it's for

- **Developers already running more than one coding agent** (Claude Code + Codex, Cursor + Gemini, …) who keep losing diffs to branch collisions or shared-tree edits.
- **Small engineering teams** piloting agent workflows who need worktree isolation, audit trails, and a review gate before code lands on `main`.
- **Builders** who want a local, hackable control plane for AI coding work — not another hosted dashboard.

If you only want an unscoped chat UI bolted onto your editor, Forge is not for
you. Forge is built around durable identity, explicit authority, delivery
evidence, and gated repository work.

## 5-minute quickstart

```bash
# Run instantly through npm (macOS / Linux)
npx @forgeailab/forge --demo

# Install via Homebrew (macOS / Linux)
brew install forgeailab/tap/forge

# Or grab the latest release directly
curl -fsSL https://raw.githubusercontent.com/ForgeAILab/forge/main/install.sh | bash

# Start the server with seeded demo data if you installed it locally
forge --demo
```

Open the `management_url` printed in the server logs. That's it — you should
see a demo project with a labelled task and a fake daemon report. From here:

- Drive a real task end-to-end → [docs/getting-started.md](docs/getting-started.md)
- Wire up Claude Code / Codex / Cursor / Gemini → [docs/getting-started.md#agents](docs/getting-started.md#configuring-agents)
- Hit the API directly → [docs/api.md](docs/api.md)

Prefer to build from source? `cargo run -p forge-cli -- --demo`.

## Core concepts

<picture>
  <img src="assets/screenshots/board.png" alt="Forge kanban board showing tasks across backlog, todo, in_progress, review, and done columns">
</picture>

| Concept | What it is |
|---|---|
| **Agent identity** | A durable account-owned agent with immutable selectable provider/CLI profiles. |
| **Main Agent** | The account's single global assistant for discovery, Project lifecycle, bounded summaries, and explicit handoff. |
| **Project** | A workspace grouping repos, Tasks, one Project Agent Chat, and a workflow definition. |
| **Project Agent** | The single persistent manager for a Project; it manages Tasks only through the Project workflow. |
| **Project Charter** | The user-approved, revisioned source of Project identity, scope, constraints, success, and unresolved knowledge. |
| **Agent Chat** | The immutable, scope-isolated timeline for the Main Agent or one Project Agent. |
| **Repo** | A pointer to a local git checkout that tasks operate on. |
| **Task** | A unit of agent work with a state, optional CI steps, and an audit log. |
| **Milestone / release** | An outcome contract with evidence-backed readiness and an immutable `Mxxx-rN` Forge snapshot. |
| **Evidence** | Project-authorized image, video, or report metadata that can reuse Task media and survive release pinning. |
| **Daemon** | The local process that reports installed CLIs and runs executions. |
| **Worktree** | An isolated git checkout created per task, cleaned up on `done`/`cancelled`. |
| **Review mode** | Agent review, no review, or human-required review layered on the Task workflow and CI checks. |

<table>
  <tr>
    <td width="50%"><img src="assets/screenshots/task-detail.png" alt="Task detail page with status, assignees by role, observability metrics, and lifecycle actions"></td>
    <td width="50%"><img src="assets/screenshots/daemons.png" alt="Daemon detail showing auto-detected CLIs: codex, claude_code, cursor, gemini, opencode, shell"></td>
  </tr>
  <tr>
    <td><sub><em>Task detail — lifecycle, role assignments, CI/review gate, audit log</em></sub></td>
    <td><sub><em>Daemon — auto-detects installed CLIs on the host (Claude Code, Codex, Cursor, Gemini, opencode, shell)</em></sub></td>
  </tr>
</table>

Deeper dive → [docs/architecture.md](docs/architecture.md).

## Documentation

| Doc | What's in it |
|---|---|
| [Getting started](docs/getting-started.md) | Install, first project, agents, end-to-end task walkthrough. |
| [Architecture](docs/architecture.md) | Authority model, Project truth, milestones/evidence, crate graph, task state machine, database, event bus. |
| [API reference](docs/api.md) | Charter/Project/Document/Milestone/Release REST endpoints, media retention, pagination, MCP tools, SSE. |
| [forge-ctl CLI](docs/cli.md) | Subcommands, daemon link, scripted runs. |
| [Execution logs](docs/execution-logs.md) | JSONL log schema and chat-history reconstruction. |
| [Changelog](CHANGELOG.md) | Per-release changes and breaking notes. |

## Status

Forge is in **public beta** (`0.1.x`). The local-first single-user product is usable
end-to-end, but APIs, schemas, and CLI flags can change without deprecation cycles.
The Main/Project Agent Chat model described above is the approved replacement
surface currently being implemented; its data-preserving migration starts at
`V071+`. Check [CHANGELOG.md](CHANGELOG.md) for the visible breaking transition
before building against collaboration APIs.
Track breaking changes in [CHANGELOG.md](CHANGELOG.md). A stable `1.0` will land
once the workflow engine, multi-user story, and release artifacts (signing, SBOMs,
Homebrew, Windows builds) are finalized.

## Contributing

Issues, PRs, and design discussion are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md)
and check `good first issue` and `help wanted` labels. By participating you agree to
the [Code of Conduct](CODE_OF_CONDUCT.md).

## Security

Please report vulnerabilities privately per [.github/SECURITY.md](.github/SECURITY.md).

## License

[MIT](LICENSE) © Forge contributors.
