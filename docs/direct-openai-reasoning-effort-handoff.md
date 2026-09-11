# Direct OpenAI reasoning effort: resume handoff

Status: paused on 2026-09-10. No implementation changes were made in the
session that produced this handoff.

## Goal

Allow a direct embedded agent backed by an OpenAI API-key provider entry to
set `reasoning_effort` and have the value reach the OpenAI request. Preserve
the existing ChatGPT OAuth behavior, including its Codex-specific effort
ladder.

This is an application/API behavior change, not a database migration. The
request and profile fields already exist in the current working tree.

## Important worktree context

The worktree was already substantially dirty before this task. Do not reset,
checkout, or broadly reformat it. In particular, the current uncommitted
changes already include the ChatGPT OAuth effort work in:

- `crates/services/src/embedded_agent_service.rs`
- `crates/agent-host/src/native.rs`
- `crates/forge-client/src/embedded.rs`
- `crates/api-types/` and `web/src/types/generated/`
- `web/src/features/federation/`
- `docs/api.md`, `docs/architecture.md`, `docs/cli.md`, and
  `docs/getting-started.md`

Build on those changes and keep unrelated edits intact.

## What is already wired

The following path is present:

1. `CreateEmbeddedAgentRequest` and `ConnectEmbeddedProfileRequest` accept
   `reasoning_effort`.
2. The embedded-agent routes pass it to `EmbeddedAgentService`.
3. The service stores it on the immutable agent profile.
4. Native turn builders copy the profile value into `NativeProviderConfig`.
5. `crates/agent-host/src/native.rs` applies it to Agent Runtime through
   `ReasoningConfig`.
6. The shared Agent Runtime OpenAI-compatible adapter serializes the value as
   the Chat Completions field `reasoning_effort`.

## Current blockers

### Server validation rejects API-key entries

`crates/services/src/embedded_agent_service.rs` currently has
`validate_native_reasoning_effort`. It only accepts:

- provider `openai`;
- credential method `oauth_bundle`; and
- the ChatGPT Codex backend URL.

An OpenAI API-key entry therefore fails agent creation/profile connection when
`reasoning_effort` is supplied.

Keep the existing OAuth/Codex branch and add a separate OpenAI API-key branch.
The official OpenAI API currently documents these general effort values:
`none`, `minimal`, `low`, `medium`, `high`, and `xhigh`. Model support still
varies, so the provider remains authoritative for the selected model.
See the [OpenAI Chat Completions reference](https://platform.openai.com/docs/api-reference/chat/message-list?lang=ruby).

Suggested server policy:

```text
openai + oauth_bundle + ChatGPT Codex URL
  -> validate against codex_reasoning_efforts_for_model(model)

openai + api_key + non-Codex endpoint
  -> validate against the OpenAI API effort list

everything else
  -> reject a supplied reasoning_effort
```

Treat blank input as omitted, as the existing helper does. Do not permit
ChatGPT-only values such as `ultra` or `max` on the API-key branch unless the
provider contract is deliberately expanded later.

### Native OpenAI capabilities advertise reasoning as unsupported

The direct OpenAI branch in `crates/agent-host/src/native.rs` constructs
`OpenAiConfig::new(...)`. The shared adapter starts from
`Capabilities::basic_streaming()`, whose reasoning capability is
`ReasoningSupport::Unsupported`.

Agent Runtime validates capabilities before network I/O. Therefore, merely
setting `ReasoningConfig` on the builder is insufficient: a real direct
OpenAI turn can be rejected before the adapter serializes the request.

When the direct OpenAI request has an effort, configure the OpenAI adapter's
capabilities as `ReasoningSupport::Controllable` before constructing the
provider. Keep this scoped to the OpenAI direct path; do not silently claim
reasoning support for unrelated OpenAI-compatible providers.

Do not edit the external Cargo checkout under `~/.cargo/git/checkouts`.

### The UI only exposes the selector for OAuth entries

`web/src/features/federation/format.ts` currently makes
`supportsDirectReasoningEntry` true only for an OpenAI OAuth entry. Extend it
to include an OpenAI API-key entry.

The existing UI obtains OAuth/Codex options through the Codex discovery
endpoint. Do not reuse that ladder for OpenAI API keys because it exposes
ChatGPT-only values. For an API-key direct entry, use a small static list of
the OpenAI API values above; retain model-aware Codex discovery for OAuth.
Possible locations are a shared helper in `format.ts` or a dedicated direct
OpenAI options constant consumed by both `AgentsTab.tsx` and
`AgentDetailPanel.tsx`.

Ensure the API-key path does not show a misleading Codex loading state and
does not render two reasoning selectors.

## Recommended implementation sequence

1. Update `validate_native_reasoning_effort` and its tests in
   `crates/services/src/embedded_agent_service.rs`.
2. Update the OpenAI provider capability setup in
   `crates/agent-host/src/native.rs`.
3. Add/adjust a host-level test proving an OpenAI direct provider reports
   controllable reasoning when effort is requested. If practical, assert the
   serialized request contains `"reasoning_effort": "high"` using the
   shared adapter's test transport; do not make a real network call.
4. Update the direct-agent UI in `web/src/features/federation/format.ts`,
   `AgentsTab.tsx`, and `AgentDetailPanel.tsx`.
5. Extend the existing Federation page test to create or edit an OpenAI
   API-key direct agent with `high` (or `medium`) and assert the request body.
6. Update the existing behavior documentation in `docs/cli.md`,
   `docs/getting-started.md`, `docs/api.md`, and `docs/architecture.md`.
   Add an Unreleased `CHANGELOG.md` entry because this changes a public
   profile/API behavior.

## Verification

Run focused checks first:

```bash
cargo fmt --all -- --check
cargo test -p services embedded_agent_service
cargo test -p forge-agent-host

cd web
pnpm test -- --run src/pages/__tests__/FederatedAgentsPage.test.tsx
pnpm typecheck
pnpm lint
```

Then run the broader checks appropriate for the final diff:

```bash
cargo test
cargo clippy --workspace --all-targets -- -D warnings
cd web && pnpm build
```

The key acceptance criterion is that a direct OpenAI API-key profile with
`reasoning_effort: "high"` can be created, selected, and executed, and that
the outbound Chat Completions JSON contains the effort field. A ChatGPT OAuth
profile must continue to accept only its model-specific Codex values.
