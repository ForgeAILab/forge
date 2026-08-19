use api_types::GateConfig;

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryBudgetKind {
    Review,
    MergeFix,
    Execution,
}

impl RetryBudgetKind {
    fn key(self) -> &'static str {
        match self {
            Self::Review => "review",
            Self::MergeFix => "merge_fix",
            Self::Execution => "execution",
        }
    }

    fn default_value(self) -> i32 {
        match self {
            Self::Review => 3,
            Self::MergeFix => 1,
            Self::Execution => 3,
        }
    }
}

pub(crate) fn runtime_retry_budget(
    task: &Task,
    kind: RetryBudgetKind,
    state_config: Option<&Value>,
    gate_config: Option<&GateConfig>,
) -> Result<i32> {
    if let Some(value) = configured_task_retry_budget(task, kind) {
        return Ok(value);
    }
    if let Some(value) = state_config.and_then(|value| retry_budget_from_value(value, kind)) {
        return Ok(value);
    }
    if matches!(kind, RetryBudgetKind::Review | RetryBudgetKind::MergeFix) {
        if let Some(value) = gate_config.and_then(|config| config.max_rejections) {
            return Ok(value);
        }
    }
    Ok(kind.default_value())
}

pub(crate) fn configured_task_retry_budget(task: &Task, kind: RetryBudgetKind) -> Option<i32> {
    task.task_state_config
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|value| retry_budget_from_value(&value, kind))
}

fn retry_budget_from_value(value: &Value, kind: RetryBudgetKind) -> Option<i32> {
    value
        .get("retry_budgets")
        .and_then(|budgets| budgets.get(kind.key()))
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value >= 0)
}

pub(super) fn executor_snapshot_with_resume_thread(
    snapshot_json: &str,
    agent_session_id: &str,
) -> Result<String> {
    let mut snapshot = parse_json_value("executor config snapshot", snapshot_json)?;
    let executor_type = snapshot
        .get("executor_type")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(config) = snapshot.get_mut("config").and_then(Value::as_object_mut) {
        match executor_type.as_deref() {
            Some("codex") => {
                config.insert(
                    RESUME_THREAD_ID_CONFIG_KEY.to_owned(),
                    Value::String(agent_session_id.to_owned()),
                );
                // Task execution follow-ups resume the coder's existing thread.
                // Review/auditor runs use their own config path when they need a
                // separate review context.
                config.insert("resume_thread_in_place".to_owned(), Value::Bool(true));
                config.remove("resume_fallback_prompt");
            }
            Some("claude_code") => {
                config.insert(
                    "resume_session_id".to_owned(),
                    Value::String(agent_session_id.to_owned()),
                );
            }
            Some("cursor") | Some("smith") | Some("gemini") => {
                config.insert(
                    "resume_session_id".to_owned(),
                    Value::String(agent_session_id.to_owned()),
                );
            }
            _ => {}
        }
    }
    // Mark this snapshot as a session-resume dispatch so the UI can show continuity context
    // without inspecting executor-specific config fields. Keep the existing `dispatch`
    // object in sync because older snapshots and debug views already read it.
    if let Some(obj) = snapshot.as_object_mut() {
        let dispatch = obj
            .entry("dispatch".to_owned())
            .or_insert_with(|| json!({}));
        if let Some(dispatch_obj) = dispatch.as_object_mut() {
            dispatch_obj.insert(
                "execution_policy".to_owned(),
                Value::String("resume_latest_target_role_thread".to_owned()),
            );
        }
        obj.insert(
            "dispatch_metadata".to_owned(),
            json!({ "execution_policy": "resume_latest_target_role_thread" }),
        );
    }
    serde_json::to_string(&snapshot).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid executor config snapshot: {error}"))
    })
}

/// Sticky, identity-safe resume for follow-ups that build a fresh snapshot:
/// promote the parent execution's winning candidate to the front of the
/// fresh route and inject the parent session onto it — but only when that
/// exact candidate (not merely the executor family) is still in the route.
/// Otherwise return the fresh snapshot untouched: configured order, fresh
/// session.
pub(super) fn executor_snapshot_with_sticky_resume(
    fresh_snapshot_json: &str,
    parent_snapshot_json: &str,
    agent_session_id: &str,
) -> Result<String> {
    let parent = parse_json_value("parent executor config snapshot", parent_snapshot_json)?;
    let fresh = parse_json_value("executor config snapshot", fresh_snapshot_json)?;

    // Normalize before keying: legacy snapshots may hold un-normalized
    // configs, and `{}` must key identically to its normalized expansion.
    let candidate_key_of = |value: &Value| -> Option<String> {
        let kind = value
            .get("executor_type")
            .and_then(Value::as_str)?
            .parse::<ExecutorKind>()
            .ok()?;
        let config = value.get("config")?;
        let normalized =
            resolve_config_value(kind.clone(), config, &ExecutionOverrides::default()).ok()?;
        Some(executors::candidate_key(&kind, &normalized))
    };

    let Some(parent_key) = candidate_key_of(&parent) else {
        // Unintelligible parent snapshot: start fresh rather than guess.
        return Ok(fresh_snapshot_json.to_owned());
    };

    if candidate_key_of(&fresh).as_deref() == Some(parent_key.as_str()) {
        return executor_snapshot_with_resume_thread(fresh_snapshot_json, agent_session_id);
    }

    let mut fresh = fresh;
    let route_match = fresh
        .get(executors::ROUTING_SNAPSHOT_KEY)
        .and_then(|routing| routing.get("candidates"))
        .and_then(Value::as_array)
        .and_then(|candidates| {
            candidates.iter().find(|candidate| {
                candidate_key_of(candidate).as_deref() == Some(parent_key.as_str())
            })
        })
        .cloned();

    match route_match {
        Some(candidate) => {
            if let Some(object) = fresh.as_object_mut() {
                if let Some(executor_type) = candidate.get("executor_type").cloned() {
                    object.insert("executor_type".to_owned(), executor_type);
                }
                if let Some(config) = candidate.get("config").cloned() {
                    object.insert("config".to_owned(), config);
                }
            }
            let promoted = serde_json::to_string(&fresh).map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "invalid executor config snapshot: {error}"
                ))
            })?;
            executor_snapshot_with_resume_thread(&promoted, agent_session_id)
        }
        // The session-producing candidate is no longer in the route:
        // configured order, fresh session.
        None => Ok(fresh_snapshot_json.to_owned()),
    }
}

#[allow(dead_code)]
pub(super) fn executor_snapshot_without_resume_thread(snapshot_json: &str) -> Result<String> {
    let mut snapshot = parse_json_value("executor config snapshot", snapshot_json)?;
    if let Some(config) = snapshot.get_mut("config").and_then(Value::as_object_mut) {
        config.remove(RESUME_THREAD_ID_CONFIG_KEY);
        config.remove("resume_thread_in_place");
        config.remove("resume_fallback_prompt");
        config.remove("resume_session_id");
    }
    if let Some(obj) = snapshot.as_object_mut() {
        obj.remove("dispatch_metadata");
        if let Some(dispatch_obj) = obj.get_mut("dispatch").and_then(Value::as_object_mut) {
            dispatch_obj.remove("execution_policy");
        }
    }
    serde_json::to_string(&snapshot).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid executor config snapshot: {error}"))
    })
}

pub(super) fn truncate_utf8_bytes(bytes: &[u8], max_bytes: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= max_bytes {
        return text.into_owned();
    }

    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut truncated = text[..end].to_owned();
    truncated.push_str("[truncated]");
    truncated
}

pub(super) async fn build_executor_config_snapshot(
    db: &SqliteDb,
    task: &Task,
    agent: &Agent,
    overrides: Option<ExecutionOverrides>,
) -> Result<Option<String>> {
    // Native profiles are hosted by Forge itself and deliberately have no
    // daemon authority.  CLI profiles retain the existing daemon resolution
    // and snapshot provenance.
    let resolved_daemon_id = if agent.backend_kind == "native" {
        None
    } else {
        Some(
            crate::agent_service::resolve_daemon_for_agent(db, agent)
                .await?
                .id,
        )
    };
    let mut base_config = parse_json_value("agent config_json", &agent.config_json)?;
    // Extract before normalization: the typed config round-trip drops
    // unknown fields, which would silently delete the chain.
    let fallbacks = extract_fallbacks(&mut base_config)?;
    apply_agent_fields_to_config(agent, &mut base_config)?;
    let capabilities = parse_json_value("agent capabilities_json", &agent.capabilities_json)?;
    let kind = agent
        .executor_type
        .parse::<ExecutorKind>()
        .map_err(ServiceError::invalid_operation)?;
    let execution_overrides = execution_overrides_to_config_layer(overrides)?;
    let (merged_config, overrides_applied) =
        merge_config_layers(&base_config, &execution_overrides);
    let normalized_config =
        resolve_config_value(kind.clone(), &merged_config, &ExecutionOverrides::default())?;
    let overrides_applied = overrides_applied.retain_config_keys(&normalized_config);
    let mut snapshot = json!({
        "agent_id": agent.id,
        // Native execution consumes this immutable profile reference from the
        // Task snapshot.  Provider credentials remain behind the protected
        // profile/store boundary and are never copied into public execution
        // snapshot JSON.
        "profile_id": agent.profile_id,
        "provider": agent.provider,
        "executor_type": agent.executor_type,
        "model": agent.model,
        "prompt_template": agent.prompt_template,
        "reasoning_effort": agent.reasoning_effort,
        "permission_policy": agent.permission_policy,
        "config": normalized_config,
        "capabilities": capabilities,
        "resolved_daemon_id": resolved_daemon_id,
        "overrides_applied": overrides_applied.to_json(),
        "snapshotted_at": now_rfc3339(),
    });
    if let Some(routing) = routing_snapshot_value(kind, &snapshot["config"], &fallbacks)? {
        snapshot[executors::ROUTING_SNAPSHOT_KEY] = routing;
    }
    // Charter-backed discovery and planning Tasks may inspect a repository,
    // but they are never allowed to receive a write-capable execution
    // profile.  Persist the capability in the immutable execution snapshot
    // so every executor backend sees the same server-derived restriction.
    if matches!(task.task_type.as_str(), "planning_task" | "discovery") {
        executors::mark_worktree_read_only(&mut snapshot);
    }
    serde_json::to_string(&snapshot)
        .map(Some)
        .map_err(|error| ServiceError::invalid_operation(format!("invalid JSON snapshot: {error}")))
}

/// Remove and return the authored `fallbacks` entries from an agent config.
pub(super) fn extract_fallbacks(config: &mut Value) -> Result<Vec<Value>> {
    let Some(object) = config.as_object_mut() else {
        return Ok(Vec::new());
    };
    match object.remove(executors::FALLBACKS_CONFIG_KEY) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(entries)) => Ok(entries),
        Some(other) => Err(ServiceError::invalid_operation(format!(
            "agent config fallbacks must be a JSON array, got: {other}"
        ))),
    }
}

/// What a routed execution actually did, applied back onto its snapshot for
/// provenance and sticky selection. Built from the local `ExecutionResult`
/// or the remote terminal notification — both structured, never log prose.
#[derive(Debug, Default, Clone)]
pub(crate) struct RouteOutcome {
    /// (candidate_key, executor_type, resolved config) of the winner.
    pub selected: Option<(String, String, Value)>,
    /// (candidate_key, outcome) per attempt, in attempt order.
    pub attempts: Vec<(String, String)>,
    /// RFC3339 retry hint when the whole route was unavailable.
    pub unavailable_retry_at: Option<Option<String>>,
}

/// Fold a route outcome into an execution snapshot. Returns `None` for
/// snapshots without a `routing` block — legacy single-candidate executions
/// stay byte-identical.
pub(crate) fn apply_route_outcome_to_snapshot(
    snapshot_json: &str,
    outcome: &RouteOutcome,
) -> Result<Option<String>> {
    let mut snapshot = parse_json_value("executor config snapshot", snapshot_json)?;
    if snapshot.get(executors::ROUTING_SNAPSHOT_KEY).is_none() {
        return Ok(None);
    }
    let Some(object) = snapshot.as_object_mut() else {
        return Ok(None);
    };
    if let Some((_, executor_type, config)) = &outcome.selected {
        object.insert(
            "executor_type".to_owned(),
            Value::String(executor_type.clone()),
        );
        object.insert("config".to_owned(), config.clone());
    }
    let Some(routing) = object
        .get_mut(executors::ROUTING_SNAPSHOT_KEY)
        .and_then(Value::as_object_mut)
    else {
        return Ok(None);
    };
    if let Some((candidate_key, _, _)) = &outcome.selected {
        routing.insert(
            "selected_candidate_key".to_owned(),
            Value::String(candidate_key.clone()),
        );
    }
    if !outcome.attempts.is_empty() {
        let attempts: Vec<Value> = outcome
            .attempts
            .iter()
            .map(|(candidate_key, outcome)| {
                json!({"candidate_key": candidate_key, "outcome": outcome})
            })
            .collect();
        routing.insert("attempts".to_owned(), Value::Array(attempts));
    }
    if let Some(retry_at) = &outcome.unavailable_retry_at {
        routing.insert(
            "disposition".to_owned(),
            json!({
                "failure_class": "executor_unavailable",
                "retry_at": retry_at,
            }),
        );
    }
    serde_json::to_string(&snapshot).map(Some).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid executor config snapshot: {error}"))
    })
}

/// Build the validated `routing` snapshot block, or `None` when the agent
/// has no fallbacks (legacy snapshots stay byte-identical).
fn routing_snapshot_value(
    kind: ExecutorKind,
    normalized_primary: &Value,
    fallbacks: &[Value],
) -> Result<Option<Value>> {
    if fallbacks.is_empty() {
        return Ok(None);
    }
    let routing =
        executors::build_ordered_fallback_routing(kind, normalized_primary.clone(), fallbacks)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    serde_json::to_value(&routing).map(Some).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid routing snapshot: {error}"))
    })
}

pub(super) async fn create_failed_execution_record(
    db: &SqliteDb,
    task_id: &str,
    agent: &Agent,
    workspace: &Workspace,
    execution_id: &str,
    error: String,
) -> Result<()> {
    let now = now_rfc3339();
    ExecutionRepo::create(
        db,
        CreateExecution {
            id: execution_id.to_owned(),
            task_id: task_id.to_owned(),
            agent_id: Some(agent.id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Failed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: Some(error),
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await?;
    Ok(())
}

fn apply_agent_fields_to_config(agent: &Agent, config: &mut Value) -> Result<()> {
    let Some(config_object) = config.as_object_mut() else {
        return Err(ServiceError::invalid_operation(
            "agent config_json must be a JSON object",
        ));
    };
    if let Some(model) = &agent.model {
        config_object.insert("model".to_owned(), Value::String(model.clone()));
    }
    if let Some(reasoning_effort) = &agent.reasoning_effort {
        config_object.insert(
            "model_reasoning_effort".to_owned(),
            Value::String(reasoning_effort.clone()),
        );
        config_object.insert("effort".to_owned(), Value::String(reasoning_effort.clone()));
    }
    if let Some(permission_policy) = &agent.permission_policy {
        config_object.insert(
            "permission_policy".to_owned(),
            Value::String(permission_policy.clone()),
        );
    }
    if let Some(prompt_template) = &agent.prompt_template {
        config_object.insert(
            "prompt_template".to_owned(),
            Value::String(prompt_template.clone()),
        );
    }
    Ok(())
}

pub(super) fn parse_json_value(field: &str, value: &str) -> Result<Value> {
    serde_json::from_str(value)
        .map_err(|error| ServiceError::invalid_operation(format!("invalid {field}: {error}")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OverridesApplied {
    pub(super) agent: Vec<String>,
    pub(super) execution: Vec<String>,
}

impl OverridesApplied {
    fn to_json(&self) -> Value {
        json!({
            "agent": self.agent,
            "execution": self.execution,
        })
    }

    pub(super) fn retain_config_keys(mut self, config: &Value) -> Self {
        let Some(config_object) = config.as_object() else {
            self.agent.clear();
            self.execution.clear();
            return self;
        };

        self.agent
            .retain(|key| config_object.contains_key(key.as_str()));
        self.execution
            .retain(|key| config_object.contains_key(key.as_str()));
        self
    }
}

pub(super) fn merge_config_layers(agent: &Value, execution: &Value) -> (Value, OverridesApplied) {
    let mut merged = agent.clone();
    let mut overrides_applied = OverridesApplied {
        agent: object_keys(agent),
        execution: Vec::new(),
    };

    merge_override_layer(
        "execution overrides",
        &mut merged,
        execution,
        &mut overrides_applied.execution,
    );

    (merged, overrides_applied)
}

fn object_keys(value: &Value) -> Vec<String> {
    value
        .as_object()
        .map(|object| object.keys().cloned().collect())
        .unwrap_or_default()
}

pub(super) fn execution_overrides_to_config_layer(
    overrides: Option<ExecutionOverrides>,
) -> Result<Value> {
    let mut layer = json!({});
    if let Some(overrides) = overrides {
        merge_overrides(&mut layer, &overrides)?;
    }
    Ok(layer)
}

#[cfg(test)]
pub(super) fn parse_config_override_layer(field: &str, value: &str) -> Value {
    match serde_json::from_str::<Value>(value) {
        Ok(value) => override_value_or_empty(field, Some(value)),
        Err(error) => {
            tracing::warn!(field = %field, %error, "config override ignored because it is invalid JSON");
            Value::Object(serde_json::Map::new())
        }
    }
}

#[cfg(test)]
pub(super) fn override_value_or_empty(field: &str, value: Option<Value>) -> Value {
    match value {
        Some(Value::Object(map)) => Value::Object(map),
        Some(Value::Null) | None => Value::Object(serde_json::Map::new()),
        Some(value) => {
            tracing::warn!(
                field = %field,
                value = %value,
                "config override ignored because it is not a JSON object"
            );
            Value::Object(serde_json::Map::new())
        }
    }
}

fn merge_override_layer(
    field: &str,
    merged: &mut Value,
    layer: &Value,
    applied_keys: &mut Vec<String>,
) {
    let Some(layer_object) = layer.as_object() else {
        tracing::warn!(
            field = %field,
            layer = %layer,
            "config override layer ignored because it is not a JSON object"
        );
        return;
    };
    let Some(merged_object) = merged.as_object_mut() else {
        return;
    };
    for (key, value) in layer_object {
        merged_object.insert(key.clone(), value.clone());
        applied_keys.push(key.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_fallbacks_removes_key_and_returns_entries() {
        let mut config = serde_json::json!({
            "profile": "acct-1",
            "fallbacks": [
                {"executor_type": "smith", "config": {"profile": "acct-2"}}
            ]
        });
        let fallbacks = extract_fallbacks(&mut config).expect("fallbacks extract");
        assert_eq!(fallbacks.len(), 1);
        assert!(config.get("fallbacks").is_none());
        assert_eq!(config["profile"], "acct-1");

        let mut without = serde_json::json!({"profile": "acct-1"});
        assert!(extract_fallbacks(&mut without)
            .expect("no fallbacks is fine")
            .is_empty());

        let mut invalid = serde_json::json!({"fallbacks": "acct-2"});
        assert!(extract_fallbacks(&mut invalid).is_err());
    }

    #[test]
    fn routing_snapshot_value_builds_only_with_fallbacks() {
        let primary = resolve_config_value(
            ExecutorKind::Smith,
            &serde_json::json!({"profile": "acct-1"}),
            &ExecutionOverrides::default(),
        )
        .expect("primary normalizes");

        assert!(routing_snapshot_value(ExecutorKind::Smith, &primary, &[])
            .expect("legacy path succeeds")
            .is_none());

        let routing = routing_snapshot_value(
            ExecutorKind::Smith,
            &primary,
            &[serde_json::json!({"executor_type": "claude_code", "config": {}})],
        )
        .expect("routing builds")
        .expect("routing present");
        assert_eq!(routing["policy"], "ordered_fallback_v1");
        assert_eq!(routing["candidates"].as_array().unwrap().len(), 2);
        assert_eq!(routing["candidates"][1]["executor_type"], "claude_code");
    }

    fn smith_snapshot(profile: &str, with_routing: bool) -> String {
        let config = resolve_config_value(
            ExecutorKind::Smith,
            &serde_json::json!({"profile": profile}),
            &ExecutionOverrides::default(),
        )
        .expect("config resolves");
        let mut snapshot = serde_json::json!({
            "executor_type": "smith",
            "config": config,
        });
        if with_routing {
            let routing = routing_snapshot_value(
                ExecutorKind::Smith,
                &snapshot["config"],
                &[serde_json::json!({"executor_type": "smith", "config": {"profile": "acct-2"}})],
            )
            .expect("routing builds")
            .expect("routing present");
            snapshot["routing"] = routing;
        }
        snapshot.to_string()
    }

    #[test]
    fn sticky_resume_matches_parent_winner_at_top_level() {
        let fresh = smith_snapshot("acct-1", true);
        let parent = smith_snapshot("acct-1", true);

        let resumed = executor_snapshot_with_sticky_resume(&fresh, &parent, "session-1")
            .expect("sticky resume succeeds");
        let snapshot: Value = serde_json::from_str(&resumed).unwrap();
        assert_eq!(snapshot["config"]["resume_session_id"], "session-1");
        assert_eq!(snapshot["config"]["profile"], "acct-1");
    }

    #[test]
    fn sticky_resume_promotes_parent_winner_from_route() {
        let fresh = smith_snapshot("acct-1", true);
        // Parent ran on the fallback candidate acct-2 (its winner was
        // persisted at top level).
        let parent = smith_snapshot("acct-2", false);

        let resumed = executor_snapshot_with_sticky_resume(&fresh, &parent, "session-2")
            .expect("sticky resume succeeds");
        let snapshot: Value = serde_json::from_str(&resumed).unwrap();
        assert_eq!(snapshot["config"]["profile"], "acct-2");
        assert_eq!(snapshot["config"]["resume_session_id"], "session-2");
        // The full route stays available for fallback.
        assert_eq!(
            snapshot["routing"]["candidates"].as_array().unwrap().len(),
            2
        );
    }

    #[test]
    fn sticky_resume_clears_session_when_candidate_left_route() {
        // Same executor family, but the parent's profile is not in the
        // fresh route: configured order, fresh session.
        let fresh = smith_snapshot("acct-1", true);
        let parent = smith_snapshot("acct-9", false);

        let resumed = executor_snapshot_with_sticky_resume(&fresh, &parent, "session-3")
            .expect("sticky resume succeeds");
        let snapshot: Value = serde_json::from_str(&resumed).unwrap();
        // Normalized configs carry `"resume_session_id": null`; injected
        // sessions are strings — assert no string session survived.
        assert!(snapshot["config"]["resume_session_id"].as_str().is_none());
        assert_eq!(snapshot["config"]["profile"], "acct-1");
    }

    #[test]
    fn sticky_resume_without_routing_requires_exact_candidate() {
        let fresh = smith_snapshot("acct-1", false);
        let matching_parent = smith_snapshot("acct-1", false);
        let other_parent = smith_snapshot("acct-2", false);

        let resumed = executor_snapshot_with_sticky_resume(&fresh, &matching_parent, "session-4")
            .expect("sticky resume succeeds");
        let snapshot: Value = serde_json::from_str(&resumed).unwrap();
        assert_eq!(snapshot["config"]["resume_session_id"], "session-4");

        let fresh_session =
            executor_snapshot_with_sticky_resume(&fresh, &other_parent, "session-5")
                .expect("sticky resume succeeds");
        let snapshot: Value = serde_json::from_str(&fresh_session).unwrap();
        assert!(snapshot["config"]["resume_session_id"].as_str().is_none());
    }

    #[test]
    fn apply_route_outcome_records_winner_attempts_and_disposition() {
        let snapshot = smith_snapshot("acct-1", true);
        let outcome = RouteOutcome {
            selected: Some((
                "smith:profile=acct-2#test".to_owned(),
                "smith".to_owned(),
                serde_json::json!({"profile": "acct-2"}),
            )),
            attempts: vec![
                (
                    "smith:profile=acct-1#test".to_owned(),
                    "usage_exhausted".to_owned(),
                ),
                (
                    "smith:profile=acct-2#test".to_owned(),
                    "completed".to_owned(),
                ),
            ],
            unavailable_retry_at: None,
        };

        let updated = apply_route_outcome_to_snapshot(&snapshot, &outcome)
            .expect("outcome applies")
            .expect("routed snapshot updates");
        let value: Value = serde_json::from_str(&updated).unwrap();
        assert_eq!(value["config"]["profile"], "acct-2");
        assert_eq!(
            value["routing"]["selected_candidate_key"],
            "smith:profile=acct-2#test"
        );
        assert_eq!(
            value["routing"]["attempts"][0]["outcome"],
            "usage_exhausted"
        );
        // Provenance: the configured route is retained.
        assert_eq!(value["routing"]["candidates"].as_array().unwrap().len(), 2);

        // Legacy snapshots without routing are untouched.
        let legacy = smith_snapshot("acct-1", false);
        assert!(apply_route_outcome_to_snapshot(&legacy, &outcome)
            .expect("legacy path succeeds")
            .is_none());
    }

    #[test]
    fn executor_snapshot_with_resume_thread_sets_codex_resume_thread_id() {
        let snapshot_json = r#"{"executor_type":"codex","dispatch":{"execution_policy":"new_execution","target_role":"coder"},"config":{"model":"gpt-5-codex","resume_fallback_prompt":"full prompt should not be reused"}}"#;

        let updated = executor_snapshot_with_resume_thread(snapshot_json, "thread-123")
            .expect("snapshot updates");
        let snapshot: Value = serde_json::from_str(&updated).expect("snapshot is valid json");

        assert_eq!(
            snapshot["config"][RESUME_THREAD_ID_CONFIG_KEY],
            "thread-123"
        );
        assert_eq!(snapshot["config"]["resume_thread_in_place"], true);
        assert!(snapshot["config"].get("resume_session_id").is_none());
        assert!(snapshot["config"].get("resume_fallback_prompt").is_none());
        assert_eq!(
            snapshot["dispatch"]["execution_policy"],
            "resume_latest_target_role_thread"
        );
        assert_eq!(snapshot["dispatch"]["target_role"], "coder");
        assert_eq!(
            snapshot["dispatch_metadata"]["execution_policy"],
            "resume_latest_target_role_thread"
        );
    }
}
