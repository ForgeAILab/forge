//! Shared usage ledger fixtures, mutations, and reference aggregation helpers.

use super::*;
pub(super) const AT: &str = "1970-01-01T00:01:40Z";
pub(super) const AGENTS: [&str; 3] = ["a", "b", "c"];

pub(super) async fn fixture() -> Arc<db::SqliteDb> {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    pricing::sqlite::test_support::subject_fixture(&db).await;
    for id in AGENTS {
        sqlx::query("INSERT INTO agent_identity (id,name,max_concurrent_tasks,created_at,updated_at) VALUES (?,?,10000,?,?)")
            .bind(id)
            .bind(id)
            .bind(AT)
            .bind(AT)
            .execute(db.pool())
            .await
            .unwrap();
    }
    // Historical cross-Agent attribution is part of the read contract, but
    // current admission forbids it. Keep every other scope/lifecycle check.
    let trigger:String=sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='usage_event_invocation_guard_insert'").fetch_one(db.pool()).await.unwrap();
    sqlx::query("DROP TRIGGER usage_event_invocation_guard_insert")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(&trigger.replace("AND i.agent_id IS NEW.agent_id", ""))
        .execute(db.pool())
        .await
        .unwrap();
    db
}
pub(super) async fn project(db: &db::SqliteDb, id: &str) {
    ProjectRepo::create(
        db,
        db::CreateProject {
            id: id.to_owned(),
            name: id.to_owned(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            primary_repo_id: None,
            owner_id: Some("pricing-owner".into()),
            created_at: AT.into(),
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap();
    TaskRepo::create(
        db,
        db::CreateTask {
            id: format!("task-{id}"),
            project_id: id.into(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: id.into(),
            description: None,
            task_type: "task".into(),
            status: "todo".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            plan: None,
            created_at: AT.into(),
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap();
}
pub(super) async fn execution(
    db: &db::SqliteDb,
    id: &str,
    project: &str,
    agent: &str,
    status: db::ExecutionStatus,
) -> db::Execution {
    ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: id.into(),
            task_id: format!("task-{project}"),
            agent_id: Some(agent.into()),
            role: "coder".into(),
            status,
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
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: None,
            created_at: AT.into(),
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap()
}
pub(super) async fn admit(
    db: &db::SqliteDb,
    id: &str,
    project: &str,
    source: &str,
    agent: &str,
) -> db::UsageInvocation {
    UsageLedgerRepo::create_pricing_selection(
        db,
        db::CreatePricingSelection {
            id: format!("selection-{id}"),
            owner_user_id: Some("pricing-owner".into()),
            project_id: Some(project.into()),
            domain_kind: db::PricingDomainKind::Execution,
            surface: DbUsageSurface::TaskExecution,
            source_id: source.into(),
            execution_id: Some(source.into()),
            task_id: Some(format!("task-{project}")),
            candidate_key: Some(id.into()),
            attempt_ordinal: 0,
            subject_id: None,
            subject_revision_id: None,
            subject_revision_digest: None,
            binding_id: None,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_freshness: None,
            runtime_model: None,
            admitted_provider_id: Some("openai".into()),
            admitted_model_id: Some("gpt-test".into()),
            source_kind: None,
            provenance_kind: db::PricingAdmissionProvenanceKind::LegacyExecutionAggregate,
            selection_status: db::PricingSelectionStatus::Unpriced,
            selection_reason: None,
            selection_digest: id.into(),
            selected_at: AT.into(),
            created_at: AT.into(),
        },
    )
    .await
    .unwrap();
    UsageLedgerRepo::create_usage_invocation(
        db,
        db::CreateUsageInvocation {
            id: id.into(),
            owner_user_id: Some("pricing-owner".into()),
            project_id: Some(project.into()),
            domain_kind: db::PricingDomainKind::Execution,
            surface: DbUsageSurface::TaskExecution,
            source_id: source.into(),
            execution_id: Some(source.into()),
            task_id: Some(format!("task-{project}")),
            domain_idempotency_key: id.into(),
            candidate_key: Some(id.into()),
            attempt_ordinal: 0,
            pricing_selection_id: format!("selection-{id}"),
            admitted_provider_id: Some("openai".into()),
            admitted_model_id: Some("gpt-test".into()),
            admitted_runtime_model: None,
            pricing_subject_id: None,
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            agent_id: Some(agent.into()),
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: None,
            backend_kind: None,
            provenance_kind: db::PricingAdmissionProvenanceKind::LegacyExecutionAggregate,
            admitted_at: AT.into(),
            created_at: AT.into(),
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap()
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn scoped_admit(
    db: &db::SqliteDb,
    id: &str,
    project: Option<&str>,
    source: &str,
    agent: Option<&str>,
    surface: DbUsageSurface,
    ordinal: i64,
    priced: bool,
) -> db::UsageInvocation {
    let domain = match surface {
        DbUsageSurface::TaskExecution => db::PricingDomainKind::Execution,
        DbUsageSurface::MainInquiry => db::PricingDomainKind::Inquiry,
        _ => db::PricingDomainKind::Chat,
    };
    let binding = if priced {
        db::PricingSubjectRepo::get_pricing_subject_binding(db, "admission-binding")
            .await
            .unwrap()
    } else {
        None
    };
    let rate = if let Some(binding) = &binding {
        db::PricingCatalogRepo::get_pricing_rate_revision(db, &binding.rate_revision_id)
            .await
            .unwrap()
    } else {
        None
    };
    let digest = binding.as_ref().map(|b| b.subject_revision_digest.clone());
    UsageLedgerRepo::create_pricing_selection(
        db,
        db::CreatePricingSelection {
            id: format!("selection-{id}"),
            owner_user_id: Some("pricing-owner".into()),
            project_id: project.map(str::to_owned),
            domain_kind: domain,
            surface,
            source_id: source.into(),
            execution_id: (surface == DbUsageSurface::TaskExecution).then(|| source.into()),
            task_id: None,
            candidate_key: Some(id.into()),
            attempt_ordinal: ordinal,
            subject_id: binding.as_ref().map(|b| b.subject_id.clone()),
            subject_revision_id: binding.as_ref().map(|b| b.subject_revision_id.clone()),
            subject_revision_digest: digest.clone(),
            binding_id: binding.as_ref().map(|b| b.id.clone()),
            rate_revision_id: rate.as_ref().map(|r| r.id.clone()),
            catalog_snapshot_id: rate.as_ref().and_then(|r| r.catalog_snapshot_id.clone()),
            catalog_freshness: priced.then(|| "fresh".into()),
            runtime_model: priced.then(|| "gpt-test".into()),
            admitted_provider_id: Some("openai".into()),
            admitted_model_id: Some("gpt-test".into()),
            source_kind: priced.then_some(db::PricingRateSourceKind::ModelsDevCatalog),
            provenance_kind: db::PricingAdmissionProvenanceKind::Runtime,
            selection_status: if priced {
                db::PricingSelectionStatus::Priced
            } else {
                db::PricingSelectionStatus::Unpriced
            },
            selection_reason: None,
            selection_digest: id.into(),
            selected_at: AT.into(),
            created_at: AT.into(),
        },
    )
    .await
    .unwrap();
    UsageLedgerRepo::create_usage_invocation(
        db,
        db::CreateUsageInvocation {
            id: id.into(),
            owner_user_id: Some("pricing-owner".into()),
            project_id: project.map(str::to_owned),
            domain_kind: domain,
            surface,
            source_id: source.into(),
            execution_id: (surface == DbUsageSurface::TaskExecution).then(|| source.into()),
            task_id: None,
            domain_idempotency_key: id.into(),
            candidate_key: Some(id.into()),
            attempt_ordinal: ordinal,
            pricing_selection_id: format!("selection-{id}"),
            admitted_provider_id: Some("openai".into()),
            admitted_model_id: Some("gpt-test".into()),
            admitted_runtime_model: priced.then(|| "gpt-test".into()),
            pricing_subject_id: binding.as_ref().map(|b| b.subject_id.clone()),
            pricing_subject_revision_id: binding.as_ref().map(|b| b.subject_revision_id.clone()),
            subject_revision_digest: digest.clone(),
            agent_id: agent.map(str::to_owned),
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: None,
            backend_kind: None,
            provenance_kind: db::PricingAdmissionProvenanceKind::Runtime,
            admitted_at: AT.into(),
            created_at: AT.into(),
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap()
}
pub(super) async fn start(db: &db::SqliteDb, i: &db::UsageInvocation) -> db::UsageInvocation {
    UsageLedgerRepo::start_usage_invocation(
        db,
        db::StartUsageInvocation {
            id: i.id.clone(),
            expected_version: i.version,
            started_at: AT.into(),
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap()
}
pub(super) async fn settle(db: &db::SqliteDb, i: &db::UsageInvocation) -> db::UsageInvocation {
    UsageLedgerRepo::settle_usage_invocation(
        db,
        db::SettleUsageInvocation {
            id: i.id.clone(),
            expected_version: i.version,
            telemetry_state: DbUsageTelemetryState::Metered,
            terminal_reason: None,
            settled_at: AT.into(),
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap()
}
pub(super) async fn event(
    db: &db::SqliteDb,
    i: &db::UsageInvocation,
    id: &str,
    agent: &str,
    reported: bool,
    ordinal: i64,
) -> UsageEvent {
    let mut e = crate::task_usage_fixture::event(id, &i.id);
    e.surface = i.surface;
    e.telemetry_state = i.telemetry_state;
    e.runtime_model = i.admitted_runtime_model.clone();
    e.pricing_subject_revision_id = i.pricing_subject_revision_id.clone();
    e.subject_revision_digest = i.subject_revision_digest.clone();
    e.owner_user_id = i.owner_user_id.clone();
    e.project_id = i.project_id.clone();
    e.source_id = i.source_id.clone();
    e.execution_id = i.execution_id.clone();
    e.task_id = i.task_id.clone();
    e.candidate_key = i.candidate_key.clone();
    e.attempt_ordinal = i.attempt_ordinal;
    e.agent_id = Some(agent.into());
    e.profile_id = None;
    e.executor_type = None;
    e.provenance_kind = UsageEventProvenanceKind::LegacyExecutionAggregate;
    e.report_mode = db::UsageEventReportMode::LegacyAggregate;
    e.report_sequence = ordinal;
    e.legacy_source_table = Some("execution_usage".into());
    e.legacy_source_id = Some(id.into());
    e.legacy_provider_sqlite_type = Some("text".into());
    e.legacy_provider_sql_literal = Some("'openai'".into());
    e.legacy_model_sqlite_type = Some("text".into());
    e.legacy_model_sql_literal = Some("'gpt-test'".into());
    e.legacy_cost_usd_raw = Some("0.000001".into());
    e.legacy_created_at_raw = Some(AT.into());
    e.legacy_project_owner_raw = Some("pricing-owner".into());
    e.provider_id = Some("openai".into());
    e.model_id = Some("gpt-test".into());
    e.input_tokens = Some(1 + ordinal);
    e.output_tokens = Some(2);
    e.cache_read_tokens = Some(0);
    e.cache_write_tokens = Some(0);
    e.occurred_at = timestamp(ordinal as u64);
    e.created_at = AT.into();
    e.provider_reported_nano_usd = reported.then_some(1000);
    e.cost_kind = if reported {
        UsageCostKind::ProviderReported
    } else {
        UsageCostKind::None
    };
    e.coverage_reason_code = (!reported).then_some(DbCostCoverageReasonCode::MissingBinding);
    if i.provenance_kind == db::PricingAdmissionProvenanceKind::Runtime {
        e.provenance_kind = UsageEventProvenanceKind::RuntimeReport;
        e.report_mode = db::UsageEventReportMode::Delta;
        e.legacy_source_table = None;
        e.legacy_source_id = None;
        e.legacy_provider_raw = None;
        e.legacy_provider_sqlite_type = None;
        e.legacy_provider_sql_literal = None;
        e.legacy_model_raw = None;
        e.legacy_model_sqlite_type = None;
        e.legacy_model_sql_literal = None;
        e.legacy_cost_usd_raw = None;
        e.legacy_created_at_raw = None;
        e.legacy_project_owner_raw = None;
        let selection = UsageLedgerRepo::get_pricing_selection(db, &i.pricing_selection_id)
            .await
            .unwrap()
            .unwrap();
        e.rate_revision_id = selection.rate_revision_id;
        e.catalog_snapshot_id = selection.catalog_snapshot_id;
        if !reported && e.rate_revision_id.is_some() {
            e.cost_kind = UsageCostKind::Estimated;
            e.estimated_nano_usd = Some(10 + ordinal);
            e.formula_revision = Some("admission-formula".into());
            e.coverage_reason_code = None;
        } else if !reported {
            e.coverage_reason_code = Some(
                [
                    DbCostCoverageReasonCode::MissingProvider,
                    DbCostCoverageReasonCode::MissingModel,
                    DbCostCoverageReasonCode::MissingRate,
                    DbCostCoverageReasonCode::UnresolvedTier,
                    DbCostCoverageReasonCode::IdentityMismatch,
                ][ordinal as usize % 5],
            );
        }
    }
    if i.telemetry_state == DbUsageTelemetryState::Unmetered {
        e.input_tokens = None;
        e.output_tokens = None;
        e.cache_read_tokens = None;
        e.cache_write_tokens = None;
        e.cost_kind = UsageCostKind::ProviderReported;
        e.provider_reported_nano_usd = Some(1000);
        e.estimated_nano_usd = None;
        e.coverage_reason_code = None;
        if e.provenance_kind == UsageEventProvenanceKind::RuntimeReport {
            e.report_mode = db::UsageEventReportMode::ReportedMoney;
        }
    }
    UsageLedgerRepo::append_usage_event(db, into_create_event(e))
        .await
        .unwrap()
}

pub(super) fn into_create_event(e: UsageEvent) -> db::CreateUsageEvent {
    db::CreateUsageEvent {
        id: e.id,
        invocation_id: e.invocation_id,
        owner_user_id: e.owner_user_id,
        project_id: e.project_id,
        surface: e.surface,
        source_id: e.source_id,
        execution_id: e.execution_id,
        task_id: e.task_id,
        event_idempotency_key: e.event_idempotency_key,
        source_report_id: e.source_report_id,
        report_sequence: e.report_sequence,
        report_mode: e.report_mode,
        provenance_kind: e.provenance_kind,
        legacy_source_table: e.legacy_source_table,
        legacy_source_id: e.legacy_source_id,
        legacy_provider_raw: e.legacy_provider_raw,
        legacy_provider_sqlite_type: e.legacy_provider_sqlite_type,
        legacy_provider_sql_literal: e.legacy_provider_sql_literal,
        legacy_model_raw: e.legacy_model_raw,
        legacy_model_sqlite_type: e.legacy_model_sqlite_type,
        legacy_model_sql_literal: e.legacy_model_sql_literal,
        legacy_counter_values_json: e.legacy_counter_values_json,
        legacy_cost_usd_raw: e.legacy_cost_usd_raw,
        legacy_created_at_raw: e.legacy_created_at_raw,
        legacy_project_owner_raw: e.legacy_project_owner_raw,
        legacy_invalid_usage: e.legacy_invalid_usage,
        provider_id: e.provider_id,
        model_id: e.model_id,
        runtime_model: e.runtime_model,
        candidate_key: e.candidate_key,
        attempt_ordinal: e.attempt_ordinal,
        agent_id: e.agent_id,
        profile_id: e.profile_id,
        agent_name_snapshot: e.agent_name_snapshot,
        project_name_snapshot: e.project_name_snapshot,
        executor_type: e.executor_type,
        pricing_subject_revision_id: e.pricing_subject_revision_id,
        subject_revision_digest: e.subject_revision_digest,
        telemetry_state: e.telemetry_state,
        input_tokens: e.input_tokens,
        output_tokens: e.output_tokens,
        cache_read_tokens: e.cache_read_tokens,
        cache_write_tokens: e.cache_write_tokens,
        context_tokens: e.context_tokens,
        selected_tier: e.selected_tier,
        provider_reported_nano_usd: e.provider_reported_nano_usd,
        legacy_reported_cost_usd: e.legacy_reported_cost_usd,
        estimated_nano_usd: e.estimated_nano_usd,
        cost_kind: e.cost_kind,
        rate_revision_id: e.rate_revision_id,
        catalog_snapshot_id: e.catalog_snapshot_id,
        formula_revision: e.formula_revision,
        retrospective: e.retrospective,
        coverage_reason_code: e.coverage_reason_code,
        occurred_at: e.occurred_at,
        created_at: e.created_at,
    }
}

pub(super) async fn finish(db: &db::SqliteDb, e: &db::Execution, failed: bool) {
    ExecutionRepo::terminalize(
        db,
        db::TerminalizeExecution {
            execution_id: e.id.clone(),
            expected_version: e.execution_version,
            lease_owner: e.lease_owner.clone(),
            status: if failed {
                db::ExecutionStatus::Failed
            } else {
                db::ExecutionStatus::Completed
            },
            stop_reason: None,
            stopped_by: None,
            stopped_at: Some(Some(AT.into())),
            resume_policy: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: AT.into(),
            actor_type: "system".into(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: db::ExecutionLeaseDisposition::Revoke,
        },
    )
    .await
    .unwrap();
}
pub(super) async fn assert_reference(
    db: &Arc<db::SqliteDb>,
    index: &UsageLedgerIndex,
    seed: u64,
    step: usize,
) {
    let actual = index.operations().await.unwrap();
    let expected = usage_aggregate_for_operations(db).await.unwrap();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap(),
        "Operations seed={seed} step={step}"
    );
    for id in AGENTS {
        let actual = index.agent(id).await.unwrap();
        let expected = usage_aggregate_for_agent(db, id).await.unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap(),
            "Agent {id} seed={seed} step={step}"
        );
        let stats = index.agent_execution_stats(&[id.to_owned()]).await.unwrap();
        let fresh = db::ExecutionRepo::stats_by_agent(&**db, id).await.unwrap();
        assert_eq!(
            stats[id].0, fresh,
            "execution stats {id} seed={seed} step={step}"
        );
    }
}
pub(super) fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
pub(super) async fn reprice(db: &Arc<db::SqliteDb>, e: &UsageEvent, step: usize) {
    use pricing::{PricingCatalogRepository, RetrospectiveEstimateRepository};
    let body = format!(
        r#"{{"openai":{{"id":"openai","name":"OpenAI","models":{{"gpt-test":{{"id":"gpt-test","last_updated":"2026-09-01","cost":{{"input":{},"output":2,"cache_read":0}}}}}}}}}}"#,
        1 + step
    );
    let at = |n| SystemTime::UNIX_EPOCH + Duration::from_secs(n);
    let snapshot = pricing::parse_models_dev_catalog(body.as_bytes())
        .unwrap()
        .into_snapshot(format!("catalog-{step}"), None, at(10), at(10))
        .unwrap();
    let repository = pricing::sqlite::SqlitePricingRepository::new(db.clone());
    repository
        .activate_catalog_snapshot(snapshot.clone(), &format!("activate-{step}"))
        .await
        .unwrap();
    let preview = pricing::preview_retrospective_estimates(
        e.project_id.clone().unwrap(),
        &snapshot,
        &[pricing::RetrospectiveUsageEvent {
            event_id: e.id.clone(),
            provider_id: e.provider_id.clone(),
            model_id: e.model_id.clone(),
            counters: Some(pricing::EventTokenCounts::new(
                e.input_tokens.unwrap() as u64,
                e.output_tokens.unwrap() as u64,
                0,
                0,
            )),
            provider_reported_amount: None,
            context_tokens: None,
            occurred_at: chrono::DateTime::parse_from_rfc3339(&e.occurred_at)
                .unwrap()
                .into(),
        }],
        at(110 + step as u64),
        Duration::from_secs(1_000_000),
    )
    .unwrap();
    let request = pricing::RetrospectiveCommitRequest {
        preview_id: preview.id.clone(),
        usage_set_digest: preview.usage_set_digest.clone(),
        idempotency_key: format!("commit-{step}"),
    };
    repository
        .commit_retrospective_preview(preview, request, at(111 + step as u64))
        .await
        .unwrap();
}

pub(super) fn timestamp(n: u64) -> String {
    format!(
        "1970-01-01T00:{:02}:{:02}.{:03}Z",
        2 + n % 50,
        (n / 50) % 60,
        (n / 3000) % 1000
    )
}
pub(super) async fn finish_at(db: &db::SqliteDb, e: &db::Execution, failed: bool, at: &str) {
    ExecutionRepo::terminalize(
        db,
        db::TerminalizeExecution {
            execution_id: e.id.clone(),
            expected_version: e.execution_version,
            lease_owner: e.lease_owner.clone(),
            status: if failed {
                db::ExecutionStatus::Failed
            } else {
                db::ExecutionStatus::Completed
            },
            stop_reason: None,
            stopped_by: None,
            stopped_at: Some(Some(at.into())),
            resume_policy: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            last_progress_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            updated_at: at.into(),
            actor_type: "system".into(),
            actor_id: None,
            correlation_id: None,
            causation_id: None,
            causation_depth: 0,
            lease_disposition: db::ExecutionLeaseDisposition::Revoke,
        },
    )
    .await
    .unwrap();
}
pub(super) async fn assert_reference_full(
    db: &Arc<db::SqliteDb>,
    index: &UsageLedgerIndex,
    seed: u64,
    step: usize,
) {
    assert_reference(db, index, seed, step).await;
    for id in AGENTS {
        let stats = index.agent_execution_stats(&[id.to_owned()]).await.unwrap();
        let running = db::AgentRepo::count_running_executions(&**db, id)
            .await
            .unwrap();
        assert_eq!(
            stats[id].1, running,
            "running count {id} seed={seed} step={step}"
        );
    }
    let all = AGENTS.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let batch = index.agents(&all).await.unwrap();
    for id in AGENTS {
        let expected = usage_aggregate_for_agent(db, id).await.unwrap();
        assert_eq!(
            serde_json::to_value(&batch[id]).unwrap(),
            serde_json::to_value(expected).unwrap(),
            "batch agent {id} seed={seed} step={step}"
        );
        let pre_commit = old_agent_reference(db, id).await.unwrap();
        assert_eq!(
            serde_json::to_value(&batch[id]).unwrap(),
            serde_json::to_value(pre_commit).unwrap(),
            "pre-commit agent impl {id} seed={seed} step={step}"
        );
    }
    assert!(
        index.state.lock().await.overflow.is_none(),
        "valid small ledger must stay incremental: seed={seed} step={step}"
    );
}

// The agent aggregate as it was before this commit (per-source, per-invocation walk).
pub(super) async fn old_agent_reference(
    db: &db::SqliteDb,
    identity_id: &str,
) -> Result<UsageAggregate> {
    let source_rows = sqlx::query(
        "SELECT DISTINCT source_id FROM usage_invocation WHERE agent_id = ?
         UNION SELECT DISTINCT source_id FROM usage_event WHERE agent_id = ?
         ORDER BY source_id ASC",
    )
    .bind(identity_id)
    .bind(identity_id)
    .fetch_all(db.pool())
    .await?;
    let mut invocations = Vec::new();
    let mut events_by_invocation = HashMap::new();
    for row in source_rows {
        let source_id: String = sqlx::Row::try_get(&row, "source_id")?;
        for invocation in UsageLedgerRepo::list_usage_invocations_for_source(db, &source_id).await?
        {
            let events = list_effective_usage_events(db, &invocation).await?;
            let invocation_matches = invocation.agent_id.as_deref() == Some(identity_id);
            let matching_events = events
                .iter()
                .any(|effective| effective.event.agent_id.as_deref() == Some(identity_id));
            if !invocation_matches && !matching_events {
                continue;
            }
            let events = if invocation_matches {
                events
            } else {
                events
                    .into_iter()
                    .filter(|effective| effective.event.agent_id.as_deref() == Some(identity_id))
                    .collect()
            };
            events_by_invocation.insert(invocation.id.clone(), events);
            invocations.push(invocation);
        }
    }
    let mut domain_runs = invocations
        .iter()
        .map(|invocation| UsageDomainRun {
            surface: invocation.surface,
            source_id: invocation.source_id.clone(),
            pending: false,
        })
        .collect::<Vec<_>>();
    let execution_rows = sqlx::query("SELECT id, status FROM execution WHERE agent_id = ?")
        .bind(identity_id)
        .fetch_all(db.pool())
        .await?;
    for row in execution_rows {
        let source_id: String = sqlx::Row::try_get(&row, "id")?;
        let status: String = sqlx::Row::try_get(&row, "status")?;
        domain_runs.push(UsageDomainRun {
            surface: DbUsageSurface::TaskExecution,
            source_id,
            pending: status == "running",
        });
    }
    aggregate_usage_with_sources(&invocations, &events_by_invocation, &domain_runs)
}

pub(super) async fn file_db(path: &std::path::Path) -> Arc<db::SqliteDb> {
    let pool = db::create_sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    pricing::sqlite::test_support::subject_fixture(&db).await;
    for id in AGENTS {
        sqlx::query("INSERT INTO agent_identity (id,name,max_concurrent_tasks,created_at,updated_at) VALUES (?,?,100000,?,?)")
            .bind(id).bind(id).bind(AT).bind(AT).execute(db.pool()).await.unwrap();
    }
    db
}
pub(super) fn repair_citations(state: &mut State, events: &[EffectiveUsageEvent]) {
    for (scope, rollup) in &mut state.scopes {
        for (key, book) in &mut rollup.sources {
            let mut repairs = Vec::new();
            for citation in book.variants.values() {
                if !citation.needs_repair {
                    continue;
                }
                let winner = events
                    .iter()
                    .filter_map(|e| {
                        let inv = state.invocations.get(e.event.invocation_id.as_str())?;
                        if inv.lifecycle == UsageInvocationLifecycle::Unsettled
                            || (scope.is_some()
                                && scope != &inv.owner
                                && scope.as_deref() != e.event.agent_id.as_deref())
                            || e.source.as_ref() != Some(&(key.clone(), citation.reference.clone()))
                        {
                            return None;
                        }
                        Some(SourceOrder {
                            source: Arc::from(inv.run.source_id.as_str()),
                            ordinal: inv.ordinal,
                            invocation: Arc::from(e.event.invocation_id.as_str()),
                            occurred: Arc::from(e.event.occurred_at.as_str()),
                            event: Arc::from(e.event.id.as_str()),
                        })
                    })
                    .max();
                repairs.push((citation.reference.clone(), winner));
            }
            for (reference, winner) in repairs {
                book.repair(&reference, winner).unwrap();
            }
        }
    }
}

pub(super) async fn admission_binding(db: &Arc<db::SqliteDb>) {
    use pricing::PricingCatalogRepository;
    let snapshot=pricing::parse_models_dev_catalog(br#"{"openai":{"id":"openai","name":"OpenAI","models":{"gpt-test":{"id":"gpt-test","last_updated":"2026-09-01","cost":{"input":1,"output":2,"cache_read":0}}}}}"#).unwrap().into_snapshot("admission-catalog",None,SystemTime::UNIX_EPOCH+Duration::from_secs(10),SystemTime::UNIX_EPOCH+Duration::from_secs(10)).unwrap();
    pricing::sqlite::SqlitePricingRepository::new(db.clone())
        .activate_catalog_snapshot(snapshot.clone(), "admission-activate")
        .await
        .unwrap();
    let rate = db::PricingCatalogRepo::get_catalog_rate_revision(
        &**db,
        &snapshot.id,
        "openai",
        "gpt-test",
    )
    .await
    .unwrap()
    .unwrap();
    let digest = sqlx::query_scalar::<_, String>(
        "SELECT revision_digest FROM pricing_subject_revision WHERE id='pricing-subject-revision'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    db::PricingSubjectRepo::create_pricing_subject_binding(
        &**db,
        db::CreatePricingSubjectBinding {
            id: "admission-binding".into(),
            owner_user_id: "pricing-owner".into(),
            subject_id: "pricing-subject".into(),
            subject_revision_id: "pricing-subject-revision".into(),
            subject_revision_digest: digest,
            scope_key: "account".into(),
            runtime_model: "gpt-test".into(),
            source_kind: db::PricingRateSourceKind::ModelsDevCatalog,
            catalog_provider_id: Some("openai".into()),
            catalog_model_id: Some("gpt-test".into()),
            rate_revision_id: rate.id,
            binding_digest: "admission-binding-digest".into(),
            effective_at: AT.into(),
            created_at: AT.into(),
            updated_at: AT.into(),
        },
    )
    .await
    .unwrap();
}
