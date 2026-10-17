use db::*;
use db::{UsageSurface as DbUsageSurface, UsageTelemetryState as DbUsageTelemetryState};
pub fn invocation(id: &str) -> UsageInvocation {
    UsageInvocation {
        id: id.to_owned(),
        owner_user_id: None,
        project_id: Some("project".to_owned()),
        domain_kind: db::PricingDomainKind::Execution,
        surface: DbUsageSurface::TaskExecution,
        source_id: format!("source-{id}"),
        execution_id: Some(format!("source-{id}")),
        task_id: Some("task".to_owned()),
        domain_idempotency_key: format!("key-{id}"),
        candidate_key: Some(format!("candidate-{id}")),
        attempt_ordinal: 0,
        pricing_selection_id: format!("selection-{id}"),
        admitted_provider_id: Some("provider".to_owned()),
        admitted_model_id: Some("model".to_owned()),
        admitted_runtime_model: None,
        pricing_subject_id: None,
        pricing_subject_revision_id: None,
        subject_revision_digest: None,
        agent_id: Some("agent".to_owned()),
        profile_id: Some("profile".to_owned()),
        agent_name_snapshot: None,
        project_name_snapshot: None,
        executor_type: Some("executor".to_owned()),
        backend_kind: Some("backend".to_owned()),
        provenance_kind: db::PricingAdmissionProvenanceKind::Runtime,
        lifecycle: UsageInvocationLifecycle::Settled,
        telemetry_state: DbUsageTelemetryState::Metered,
        terminal_reason: None,
        version: 1,
        admitted_at: "2026-01-01T00:00:00Z".to_owned(),
        started_at: Some("2026-01-01T00:00:01Z".to_owned()),
        settled_at: Some("2026-01-01T00:00:02Z".to_owned()),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
        updated_at: "2026-01-01T00:00:02Z".to_owned(),
    }
}

pub fn event(id: &str, invocation_id: &str) -> UsageEvent {
    UsageEvent {
        id: id.to_owned(),
        invocation_id: invocation_id.to_owned(),
        owner_user_id: None,
        project_id: Some("project".to_owned()),
        surface: DbUsageSurface::TaskExecution,
        source_id: format!("source-{invocation_id}"),
        execution_id: Some(format!("source-{invocation_id}")),
        task_id: Some("task".to_owned()),
        event_idempotency_key: format!("event-key-{id}"),
        source_report_id: format!("report-{id}"),
        report_sequence: 0,
        report_mode: db::UsageEventReportMode::Delta,
        provenance_kind: UsageEventProvenanceKind::RuntimeReport,
        legacy_source_table: None,
        legacy_source_id: None,
        legacy_provider_raw: None,
        legacy_provider_sqlite_type: None,
        legacy_provider_sql_literal: None,
        legacy_model_raw: None,
        legacy_model_sqlite_type: None,
        legacy_model_sql_literal: None,
        legacy_counter_values_json: "{}".to_owned(),
        legacy_cost_usd_raw: None,
        legacy_created_at_raw: None,
        legacy_project_owner_raw: None,
        legacy_invalid_usage: false,
        provider_id: Some("provider".to_owned()),
        model_id: Some("model".to_owned()),
        runtime_model: None,
        candidate_key: Some("candidate".to_owned()),
        attempt_ordinal: 0,
        agent_id: Some("agent".to_owned()),
        profile_id: Some("profile".to_owned()),
        agent_name_snapshot: None,
        project_name_snapshot: None,
        executor_type: Some("executor".to_owned()),
        pricing_subject_revision_id: None,
        subject_revision_digest: None,
        telemetry_state: DbUsageTelemetryState::Metered,
        input_tokens: None,
        output_tokens: None,
        cache_read_tokens: None,
        cache_write_tokens: None,
        context_tokens: None,
        selected_tier: None,
        provider_reported_nano_usd: Some(0),
        legacy_reported_cost_usd: None,
        estimated_nano_usd: None,
        cost_kind: UsageCostKind::ProviderReported,
        rate_revision_id: None,
        catalog_snapshot_id: None,
        formula_revision: None,
        retrospective: false,
        coverage_reason_code: None,
        occurred_at: "2026-01-01T00:00:02Z".to_owned(),
        created_at: "2026-01-01T00:00:02Z".to_owned(),
    }
}

pub async fn insert_json(db: &SqliteDb, table: &str, value: serde_json::Value) {
    let object = value.as_object().unwrap();
    let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(format!(
        "INSERT INTO {table} ({} ) VALUES (",
        object.keys().cloned().collect::<Vec<_>>().join(",")
    ));
    let mut values = query.separated(",");
    for value in object.values() {
        match value {
            serde_json::Value::Null => {
                values.push_bind(Option::<String>::None);
            }
            serde_json::Value::Number(n) if n.is_i64() => {
                values.push_bind(n.as_i64());
            }
            serde_json::Value::Bool(b) => {
                values.push_bind(*b);
            }
            serde_json::Value::String(s) => {
                values.push_bind(s);
            }
            value => {
                values.push_bind(value.to_string());
            }
        }
    }
    values.push_unseparated(")");
    query.build().execute(db.pool()).await.unwrap();
}

/// Seed real migrated ledger rows using the supported historical provenance.
/// No constraints or triggers are disabled.
pub async fn seed(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
    ordinal: usize,
    event_count: usize,
) {
    let project_id: String = sqlx::query_scalar("SELECT project_id FROM task WHERE id = ?")
        .bind(task_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let owner: Option<String> = sqlx::query_scalar("SELECT owner_id FROM project WHERE id = ?")
        .bind(&project_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let id = format!("fixture-{task_id}-{ordinal}");
    let mut i = invocation(&id);
    i.owner_user_id = owner.clone();
    i.source_id = execution_id.to_owned();
    i.execution_id = Some(execution_id.to_owned());
    i.task_id = Some(task_id.to_owned());
    i.project_id = Some(project_id.clone());
    i.attempt_ordinal = ordinal as i64;
    i.provenance_kind = PricingAdmissionProvenanceKind::LegacyExecutionAggregate;
    sqlx::query("INSERT INTO pricing_selection (id, owner_user_id, project_id, domain_kind, surface, source_id, execution_id, task_id, candidate_key, attempt_ordinal, admitted_provider_id, admitted_model_id, provenance_kind, selection_status, selection_digest, selected_at, created_at)
        VALUES (?, ?, ?, 'execution', 'task_execution', ?, ?, ?, ?, ?, 'provider', 'model', 'legacy_execution_aggregate', 'unpriced', ?, ?, ?)")
        .bind(&i.pricing_selection_id).bind(owner).bind(project_id).bind(execution_id).bind(execution_id).bind(task_id)
        .bind(&i.candidate_key).bind(i.attempt_ordinal).bind(format!("digest-{id}"))
        .bind(&i.admitted_at).bind(&i.created_at).execute(db.pool()).await.unwrap();
    insert_json(
        db,
        "usage_invocation",
        serde_json::Value::Object(serde_json::Map::from_iter([
            ("id".to_owned(), serde_json::to_value(&i.id).unwrap()),
            (
                "owner_user_id".to_owned(),
                serde_json::to_value(&i.owner_user_id).unwrap(),
            ),
            (
                "project_id".to_owned(),
                serde_json::to_value(&i.project_id).unwrap(),
            ),
            (
                "domain_kind".to_owned(),
                serde_json::to_value(i.domain_kind.to_string()).unwrap(),
            ),
            (
                "surface".to_owned(),
                serde_json::to_value(i.surface.to_string()).unwrap(),
            ),
            (
                "source_id".to_owned(),
                serde_json::to_value(&i.source_id).unwrap(),
            ),
            (
                "execution_id".to_owned(),
                serde_json::to_value(&i.execution_id).unwrap(),
            ),
            (
                "task_id".to_owned(),
                serde_json::to_value(&i.task_id).unwrap(),
            ),
            (
                "domain_idempotency_key".to_owned(),
                serde_json::to_value(&i.domain_idempotency_key).unwrap(),
            ),
            (
                "candidate_key".to_owned(),
                serde_json::to_value(&i.candidate_key).unwrap(),
            ),
            (
                "attempt_ordinal".to_owned(),
                serde_json::to_value(i.attempt_ordinal).unwrap(),
            ),
            (
                "pricing_selection_id".to_owned(),
                serde_json::to_value(&i.pricing_selection_id).unwrap(),
            ),
            (
                "admitted_provider_id".to_owned(),
                serde_json::to_value(&i.admitted_provider_id).unwrap(),
            ),
            (
                "admitted_model_id".to_owned(),
                serde_json::to_value(&i.admitted_model_id).unwrap(),
            ),
            (
                "admitted_runtime_model".to_owned(),
                serde_json::to_value(&i.admitted_runtime_model).unwrap(),
            ),
            (
                "pricing_subject_id".to_owned(),
                serde_json::to_value(&i.pricing_subject_id).unwrap(),
            ),
            (
                "pricing_subject_revision_id".to_owned(),
                serde_json::to_value(&i.pricing_subject_revision_id).unwrap(),
            ),
            (
                "subject_revision_digest".to_owned(),
                serde_json::to_value(&i.subject_revision_digest).unwrap(),
            ),
            (
                "agent_id".to_owned(),
                serde_json::to_value(&i.agent_id).unwrap(),
            ),
            (
                "profile_id".to_owned(),
                serde_json::to_value(&i.profile_id).unwrap(),
            ),
            (
                "agent_name_snapshot".to_owned(),
                serde_json::to_value(&i.agent_name_snapshot).unwrap(),
            ),
            (
                "project_name_snapshot".to_owned(),
                serde_json::to_value(&i.project_name_snapshot).unwrap(),
            ),
            (
                "executor_type".to_owned(),
                serde_json::to_value(&i.executor_type).unwrap(),
            ),
            (
                "backend_kind".to_owned(),
                serde_json::to_value(&i.backend_kind).unwrap(),
            ),
            (
                "provenance_kind".to_owned(),
                serde_json::to_value(i.provenance_kind.to_string()).unwrap(),
            ),
            (
                "lifecycle".to_owned(),
                serde_json::to_value(i.lifecycle.to_string()).unwrap(),
            ),
            (
                "telemetry_state".to_owned(),
                serde_json::to_value(i.telemetry_state.to_string()).unwrap(),
            ),
            (
                "terminal_reason".to_owned(),
                serde_json::to_value(&i.terminal_reason).unwrap(),
            ),
            (
                "version".to_owned(),
                serde_json::to_value(i.version).unwrap(),
            ),
            (
                "admitted_at".to_owned(),
                serde_json::to_value(&i.admitted_at).unwrap(),
            ),
            (
                "started_at".to_owned(),
                serde_json::to_value(&i.started_at).unwrap(),
            ),
            (
                "settled_at".to_owned(),
                serde_json::to_value(&i.settled_at).unwrap(),
            ),
            (
                "created_at".to_owned(),
                serde_json::to_value(&i.created_at).unwrap(),
            ),
            (
                "updated_at".to_owned(),
                serde_json::to_value(&i.updated_at).unwrap(),
            ),
        ])),
    )
    .await;
    for n in 0..event_count {
        let mut e = event(&format!("{id}-{n}"), &id);
        e.owner_user_id = i.owner_user_id.clone();
        e.source_id = i.source_id.clone();
        e.execution_id = i.execution_id.clone();
        e.project_id = i.project_id.clone();
        e.task_id = i.task_id.clone();
        e.candidate_key = i.candidate_key.clone();
        e.attempt_ordinal = i.attempt_ordinal;
        e.provenance_kind = UsageEventProvenanceKind::LegacyExecutionAggregate;
        e.report_mode = UsageEventReportMode::LegacyAggregate;
        e.report_sequence = n as i64;
        e.input_tokens = Some(20);
        e.output_tokens = Some(10);
        e.cache_read_tokens = Some(0);
        e.cache_write_tokens = Some(0);
        e.provider_reported_nano_usd = Some(1000);
        e.legacy_source_table = Some("execution_usage".to_owned());
        e.legacy_source_id = Some(e.id.clone());
        e.legacy_provider_sqlite_type = Some("text".to_owned());
        e.legacy_provider_sql_literal = Some("'provider'".to_owned());
        e.legacy_model_sqlite_type = Some("text".to_owned());
        e.legacy_model_sql_literal = Some("'model'".to_owned());
        e.legacy_cost_usd_raw = Some("0.000001".to_owned());
        e.legacy_created_at_raw = Some(e.created_at.clone());
        e.legacy_project_owner_raw = Some("NULL".to_owned());
        insert_json(
            db,
            "usage_event",
            serde_json::Value::Object(serde_json::Map::from_iter([
                ("id".to_owned(), serde_json::to_value(&e.id).unwrap()),
                (
                    "invocation_id".to_owned(),
                    serde_json::to_value(&e.invocation_id).unwrap(),
                ),
                (
                    "owner_user_id".to_owned(),
                    serde_json::to_value(&e.owner_user_id).unwrap(),
                ),
                (
                    "project_id".to_owned(),
                    serde_json::to_value(&e.project_id).unwrap(),
                ),
                (
                    "surface".to_owned(),
                    serde_json::to_value(e.surface.to_string()).unwrap(),
                ),
                (
                    "source_id".to_owned(),
                    serde_json::to_value(&e.source_id).unwrap(),
                ),
                (
                    "execution_id".to_owned(),
                    serde_json::to_value(&e.execution_id).unwrap(),
                ),
                (
                    "task_id".to_owned(),
                    serde_json::to_value(&e.task_id).unwrap(),
                ),
                (
                    "event_idempotency_key".to_owned(),
                    serde_json::to_value(&e.event_idempotency_key).unwrap(),
                ),
                (
                    "source_report_id".to_owned(),
                    serde_json::to_value(&e.source_report_id).unwrap(),
                ),
                (
                    "report_sequence".to_owned(),
                    serde_json::to_value(e.report_sequence).unwrap(),
                ),
                (
                    "report_mode".to_owned(),
                    serde_json::to_value(e.report_mode.to_string()).unwrap(),
                ),
                (
                    "provenance_kind".to_owned(),
                    serde_json::to_value(e.provenance_kind.to_string()).unwrap(),
                ),
                (
                    "legacy_source_table".to_owned(),
                    serde_json::to_value(&e.legacy_source_table).unwrap(),
                ),
                (
                    "legacy_source_id".to_owned(),
                    serde_json::to_value(&e.legacy_source_id).unwrap(),
                ),
                (
                    "legacy_provider_raw".to_owned(),
                    serde_json::to_value(&e.legacy_provider_raw).unwrap(),
                ),
                (
                    "legacy_provider_sqlite_type".to_owned(),
                    serde_json::to_value(&e.legacy_provider_sqlite_type).unwrap(),
                ),
                (
                    "legacy_provider_sql_literal".to_owned(),
                    serde_json::to_value(&e.legacy_provider_sql_literal).unwrap(),
                ),
                (
                    "legacy_model_raw".to_owned(),
                    serde_json::to_value(&e.legacy_model_raw).unwrap(),
                ),
                (
                    "legacy_model_sqlite_type".to_owned(),
                    serde_json::to_value(&e.legacy_model_sqlite_type).unwrap(),
                ),
                (
                    "legacy_model_sql_literal".to_owned(),
                    serde_json::to_value(&e.legacy_model_sql_literal).unwrap(),
                ),
                (
                    "legacy_counter_values_json".to_owned(),
                    serde_json::to_value(&e.legacy_counter_values_json).unwrap(),
                ),
                (
                    "legacy_cost_usd_raw".to_owned(),
                    serde_json::to_value(&e.legacy_cost_usd_raw).unwrap(),
                ),
                (
                    "legacy_created_at_raw".to_owned(),
                    serde_json::to_value(&e.legacy_created_at_raw).unwrap(),
                ),
                (
                    "legacy_project_owner_raw".to_owned(),
                    serde_json::to_value(&e.legacy_project_owner_raw).unwrap(),
                ),
                (
                    "legacy_invalid_usage".to_owned(),
                    serde_json::to_value(e.legacy_invalid_usage).unwrap(),
                ),
                (
                    "provider_id".to_owned(),
                    serde_json::to_value(&e.provider_id).unwrap(),
                ),
                (
                    "model_id".to_owned(),
                    serde_json::to_value(&e.model_id).unwrap(),
                ),
                (
                    "runtime_model".to_owned(),
                    serde_json::to_value(&e.runtime_model).unwrap(),
                ),
                (
                    "candidate_key".to_owned(),
                    serde_json::to_value(&e.candidate_key).unwrap(),
                ),
                (
                    "attempt_ordinal".to_owned(),
                    serde_json::to_value(e.attempt_ordinal).unwrap(),
                ),
                (
                    "agent_id".to_owned(),
                    serde_json::to_value(&e.agent_id).unwrap(),
                ),
                (
                    "profile_id".to_owned(),
                    serde_json::to_value(&e.profile_id).unwrap(),
                ),
                (
                    "agent_name_snapshot".to_owned(),
                    serde_json::to_value(&e.agent_name_snapshot).unwrap(),
                ),
                (
                    "project_name_snapshot".to_owned(),
                    serde_json::to_value(&e.project_name_snapshot).unwrap(),
                ),
                (
                    "executor_type".to_owned(),
                    serde_json::to_value(&e.executor_type).unwrap(),
                ),
                (
                    "pricing_subject_revision_id".to_owned(),
                    serde_json::to_value(&e.pricing_subject_revision_id).unwrap(),
                ),
                (
                    "subject_revision_digest".to_owned(),
                    serde_json::to_value(&e.subject_revision_digest).unwrap(),
                ),
                (
                    "telemetry_state".to_owned(),
                    serde_json::to_value(e.telemetry_state.to_string()).unwrap(),
                ),
                (
                    "input_tokens".to_owned(),
                    serde_json::to_value(e.input_tokens).unwrap(),
                ),
                (
                    "output_tokens".to_owned(),
                    serde_json::to_value(e.output_tokens).unwrap(),
                ),
                (
                    "cache_read_tokens".to_owned(),
                    serde_json::to_value(e.cache_read_tokens).unwrap(),
                ),
                (
                    "cache_write_tokens".to_owned(),
                    serde_json::to_value(e.cache_write_tokens).unwrap(),
                ),
                (
                    "context_tokens".to_owned(),
                    serde_json::to_value(e.context_tokens).unwrap(),
                ),
                (
                    "selected_tier".to_owned(),
                    serde_json::to_value(&e.selected_tier).unwrap(),
                ),
                (
                    "provider_reported_nano_usd".to_owned(),
                    serde_json::to_value(e.provider_reported_nano_usd).unwrap(),
                ),
                (
                    "legacy_reported_cost_usd".to_owned(),
                    serde_json::to_value(e.legacy_reported_cost_usd).unwrap(),
                ),
                (
                    "estimated_nano_usd".to_owned(),
                    serde_json::to_value(e.estimated_nano_usd).unwrap(),
                ),
                (
                    "cost_kind".to_owned(),
                    serde_json::to_value(e.cost_kind.to_string()).unwrap(),
                ),
                (
                    "rate_revision_id".to_owned(),
                    serde_json::to_value(&e.rate_revision_id).unwrap(),
                ),
                (
                    "catalog_snapshot_id".to_owned(),
                    serde_json::to_value(&e.catalog_snapshot_id).unwrap(),
                ),
                (
                    "formula_revision".to_owned(),
                    serde_json::to_value(&e.formula_revision).unwrap(),
                ),
                (
                    "retrospective".to_owned(),
                    serde_json::to_value(e.retrospective).unwrap(),
                ),
                (
                    "coverage_reason_code".to_owned(),
                    serde_json::to_value(e.coverage_reason_code).unwrap(),
                ),
                (
                    "occurred_at".to_owned(),
                    serde_json::to_value(&e.occurred_at).unwrap(),
                ),
                (
                    "created_at".to_owned(),
                    serde_json::to_value(&e.created_at).unwrap(),
                ),
            ])),
        )
        .await;
    }
}
