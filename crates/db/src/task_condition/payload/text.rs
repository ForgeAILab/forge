use super::*;
use serde_json::Value;

fn value(value: &mut Value, limit: usize) {
    match value {
        Value::String(text) => cut(text, limit),
        Value::Array(items) => {
            items.truncate(4);
            for item in items {
                value_in_field(item, limit);
            }
        }
        Value::Object(fields) => {
            for (key, item) in fields.iter_mut() {
                if !matches!(
                    key.as_str(),
                    "state" | "status" | "kind" | "type" | "id" | "execution_id" | "task_step_id"
                ) {
                    value_in_field(item, limit);
                }
            }
        }
        _ => {}
    }
}
fn value_in_field(item: &mut Value, limit: usize) {
    value(item, limit);
    if serde_json::to_vec(item).is_ok_and(|raw| raw.len() > limit * 4) {
        *item = Value::String(diagnostics::truncate(&item.to_string(), limit));
    }
}

pub(super) fn tighten(condition: &mut TaskCondition, limit: usize) {
    let Some(read) = &mut condition.evidence_mut().presentation else {
        return;
    };
    if let Some(raw) = &mut read.operator_reason {
        if raw.len() > limit * 4 {
            // Keep a JSON document even when its visible text needs a smaller budget.
            let mut parsed =
                serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone()));
            value_in_field(&mut parsed, limit);
            *raw = parsed.to_string();
        }
    }
    if let Some(d) = &mut read.diagnostic {
        if let Some(hook) = &mut d.hook {
            value_in_field(hook, limit);
        }
        if let Some(artifact) = &mut d.artifact {
            cut_optional(&mut artifact.log_path, limit);
        }
    }
    for item in [&mut read.entry, &mut read.environment, &mut read.placement]
        .into_iter()
        .flatten()
    {
        // Entry state/status are operational; its diagnostic values are reloadable.
        value(item, limit);
        if let Value::Object(fields) = item {
            fields.retain(|key, _| {
                matches!(
                    key.as_str(),
                    "state"
                        | "status"
                        | "kind"
                        | "type"
                        | "id"
                        | "execution_id"
                        | "task_step_id"
                        | "reason"
                        | "message"
                        | "annotation"
                        | "diagnostic"
                )
            });
        }
    }
}

fn fields(condition: &TaskCondition) -> BTreeMap<&'static str, String> {
    let mut fields = BTreeMap::new();
    if let Some(read) = &condition.evidence().presentation {
        for (name, text) in [
            ("failure_message", read.failure_message.as_deref()),
            ("blocked_message", read.blocked_message.as_deref()),
            ("exception_message", read.exception_message.as_deref()),
            (
                "diagnostic_reason",
                read.diagnostic.as_ref().map(|d| d.blocking_reason.as_str()),
            ),
            (
                "diagnostic_message",
                read.diagnostic.as_ref().and_then(|d| d.message.as_deref()),
            ),
            (
                "interruption_reason",
                read.interruption.as_ref().map(|i| i.reason.as_str()),
            ),
            (
                "retry_reason",
                read.retry.as_ref().map(|r| r.reason.as_str()),
            ),
            (
                "retry_display_reason",
                read.retry_display
                    .as_ref()
                    .and_then(|r| r.reason.as_deref()),
            ),
            (
                "refusal_message",
                read.refusal.as_ref().map(|r| r.safe_message.as_str()),
            ),
        ] {
            if let Some(text) = text {
                fields.insert(name, text.to_owned());
            }
        }
        if let Some(raw) = &read.operator_reason {
            fields.insert("operator_reason", raw.clone());
        }
        if let Some(d) = &read.diagnostic {
            if let Some(hook) = &d.hook {
                fields.insert("diagnostic_hook", hook.to_string());
            }
            if let Some(artifact) = &d.artifact {
                fields.insert(
                    "diagnostic_artifact",
                    serde_json::to_string(artifact).expect("artifact serializes"),
                );
            }
        }
        for (name, item) in [
            ("entry", &read.entry),
            ("environment", &read.environment),
            ("placement", &read.placement),
        ] {
            if let Some(item) = item {
                fields.insert(name, item.to_string());
            }
        }
    }
    fields
}

pub(super) async fn record(
    c: &mut SqliteConnection,
    task_id: &str,
    original: &TaskCondition,
    bounded: &mut TaskCondition,
) -> Result<()> {
    let compact = fields(bounded);
    for (name, text) in fields(original) {
        if compact.get(name) == Some(&text) {
            continue;
        }
        let id = diagnostics::record(
            c,
            task_id,
            RecordTaskDiagnosticText {
                kind: name,
                source: "condition_payload",
                owner: &DiagnosticOwner::StoredCondition,
                text: &text,
            },
        )
        .await?;
        if !bounded.evidence().diagnostic_ids.contains(&id) {
            bounded.evidence_mut().diagnostic_ids.push(id);
        }
    }
    Ok(())
}
