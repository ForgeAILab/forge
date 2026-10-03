# Workflow Hook Actions

This directory contains the flexible workflow engine and the curated library of Rust hook actions that workflow definitions reference by name. Workflow JSON never executes user code; `registry.rs::resolve_action` maps a string such as `"run_review"` to a compiled Rust type implementing `HookAction`.

## Action Contract

Implement `HookAction` from `mod.rs`:

```rust
#[async_trait::async_trait]
impl HookAction for MyAction {
    async fn execute(&self, ctx: &HookContext) -> HookResult {
        // validate context, do work, return a HookResult
    }
}
```

`execute(&self, ctx: &HookContext) -> HookResult` is async and must be deterministic for the current database state. Keep actions small; load only the rows needed for the action.

Return one of:

- `HookResult::Ok`: action succeeded.
- `HookResult::Skipped { reason }`: action does not apply to this transition/context.
- `HookResult::Failed { reason }`: action tried to run and failed.
- `HookResult::Cascade { to, reason }`: request a durable queued follow-up transition; use `TaskService::drain(task_id)` in tests before asserting its settled state.

Always check preconditions first. Return `Skipped` when required context is missing, such as no workspace, no executor/execution row, no role assignment, or no applicable state config. This keeps human-driven board flows working: a user can move cards without having an agent workspace, and hooks record that they did not apply instead of breaking the transition.

## Hook Context

`HookContext` currently provides:

- `task_id`
- `from_state`
- `to_state`
- `db`
- `event_bus`
- `workspace_id`
- `agent_id`
- `execution_id`
- `state_config`
- `gate_config`
- `workflow`

`state_config` is the merged state config for the hook's phase. `workflow` is an `Arc<WorkflowDefinition>` so actions can inspect roles, states, and transitions without reloading project JSON.

## Failure Policy

Workflow definitions attach actions through `HookSpec`:

- `FailurePolicy::Block`: for `before_exit` guards. A failed guard aborts the transition; the API boundary maps this to HTTP 412.
- `FailurePolicy::Log`: for effects. Failures are logged/emitted and the transition remains committed.
- `FailurePolicy::Cascade(target)`: declared in the API type for auto-advance policy. In the current engine, auto-advance is implemented by an action returning `HookResult::Cascade` from `after_enter`; verify engine support before relying on policy-driven cascade from `on_exit` or `on_enter`.

The engine writes a `transition_log` row after the status update, then backfills hook results after hooks complete.

## Audience

Hooks carry `applies_to: HookAudience`:

- `HookAudience::All`: runs for any trigger.
- `HookAudience::AgentOnly`: runs when `triggered_by` starts with `"agent:"` or equals `"system"`.
- `HookAudience::UserOnly`: runs only when `triggered_by` starts with `"user:"`.

The filter is applied before action resolution/execution. Non-matching hooks are not recorded as skipped hook results.

Human-triggered task movement uses `triggered_by = "user:*"` and has higher
priority than automation gates. Actions that protect AI work, such as
`dependency_gate`, must skip those management transitions and enforce the same
precondition again at execution or dispatch time.

## Registration

After adding an action type in `actions/`, register it in `registry.rs::resolve_action`:

```rust
"my_action" => Box::new(MyAction),
```

Unknown action names return `ServiceError::InvalidOperation`, so workflow definitions can only reference registered actions.

## Testing

There is no separate action-test crate. Keep action tests close to the implementation using module-local `#[cfg(test)]` blocks, following the pattern already used in `cache.rs`. For behavior that depends on legacy task transitions or API routes, use the existing service/API tests around `TaskService` and task routes.
