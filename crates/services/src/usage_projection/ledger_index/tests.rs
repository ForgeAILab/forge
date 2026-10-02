use super::*;
use db::{ExecutionRepo, ProjectRepo, TaskRepo, UsageLedgerRepo};
use std::time::{Duration, SystemTime};
const AT: &str = "1970-01-01T00:01:40Z";
const AGENTS: [&str; 3] = ["a", "b", "c"];

async fn fixture() -> Arc<db::SqliteDb> {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    crate::pricing_db::tests::subject_fixture(&db).await;
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
async fn project(db: &db::SqliteDb, id: &str) {
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
async fn execution(
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
async fn admit(
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
async fn scoped_admit(
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
async fn start(db: &db::SqliteDb, i: &db::UsageInvocation) -> db::UsageInvocation {
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
async fn settle(db: &db::SqliteDb, i: &db::UsageInvocation) -> db::UsageInvocation {
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
async fn event(
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

fn into_create_event(e: UsageEvent) -> db::CreateUsageEvent {
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

async fn finish(db: &db::SqliteDb, e: &db::Execution, failed: bool) {
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
async fn assert_reference(
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
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
async fn reprice(db: &Arc<db::SqliteDb>, e: &UsageEvent, step: usize) {
    use crate::pricing::{PricingCatalogRepository, RetrospectiveEstimateRepository};
    let body = format!(
        r#"{{"openai":{{"id":"openai","name":"OpenAI","models":{{"gpt-test":{{"id":"gpt-test","last_updated":"2026-09-01","cost":{{"input":{},"output":2,"cache_read":0}}}}}}}}}}"#,
        1 + step
    );
    let at = |n| SystemTime::UNIX_EPOCH + Duration::from_secs(n);
    let snapshot = crate::pricing::parse_models_dev_catalog(body.as_bytes())
        .unwrap()
        .into_snapshot(format!("catalog-{step}"), None, at(10), at(10))
        .unwrap();
    let repository = crate::pricing_db::SqlitePricingRepository::new(db.clone());
    repository
        .activate_catalog_snapshot(snapshot.clone(), &format!("activate-{step}"))
        .await
        .unwrap();
    let preview = crate::pricing::preview_retrospective_estimates(
        e.project_id.clone().unwrap(),
        &snapshot,
        &[crate::pricing::RetrospectiveUsageEvent {
            event_id: e.id.clone(),
            provider_id: e.provider_id.clone(),
            model_id: e.model_id.clone(),
            counters: Some(crate::pricing::EventTokenCounts::new(
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
    let request = crate::pricing::RetrospectiveCommitRequest {
        preview_id: preview.id.clone(),
        usage_set_digest: preview.usage_set_digest.clone(),
        idempotency_key: format!("commit-{step}"),
    };
    repository
        .commit_retrospective_preview(preview, request, at(111 + step as u64))
        .await
        .unwrap();
}

#[tokio::test]
async fn randomized_differential_ledger_mutations() {
    // Each seed executes a few hundred real repository mutations. Attribution
    // tolerance is tested through append_usage_event with only its attribution
    // guard relaxed; every remaining V135 identity/lifecycle guard stays live.
    for seed in [0x1234_5678, 0xabc0_0191, 0xfade_7013] {
        let db = fixture().await;
        let index = UsageLedgerIndex::new(db.clone());
        let mut rng = seed;
        let mut projects = Vec::<String>::new();
        let mut executions = Vec::<String>::new();
        let mut invocations = Vec::<String>::new();
        let mut events = Vec::<String>::new();
        project(&db, "prelude").await;
        projects.push("prelude".to_owned());
        let exec = execution(
            &db,
            "prelude-run",
            "prelude",
            "a",
            db::ExecutionStatus::Running,
        )
        .await;
        executions.push(exec.id.clone());
        let inv = admit(&db, "prelude-inv", "prelude", &exec.id, "b").await;
        invocations.push(inv.id.clone());
        assert_reference(&db, &index, seed, 1000).await;
        let inv = start(&db, &inv).await;
        assert_reference(&db, &index, seed, 1001).await;
        let inv = UsageLedgerRepo::mark_usage_invocation_pending_settlement(
            &*db,
            db::MarkUsageInvocationPendingSettlement {
                id: inv.id,
                expected_version: inv.version,
                updated_at: AT.into(),
            },
        )
        .await
        .unwrap();
        assert_reference(&db, &index, seed, 1002).await;
        let inv = settle(&db, &inv).await;
        assert_reference(&db, &index, seed, 1003).await;
        let e = event(&db, &inv, "prelude-e", "a", false, 0).await;
        events.push(e.id.clone());
        assert_reference(&db, &index, seed, 1004).await;
        reprice(&db, &e, 5000).await;
        assert_reference(&db, &index, seed, 1005).await;
        reprice(&db, &e, 5001).await;
        assert_reference(&db, &index, seed, 1006).await;
        finish(&db, &exec, true).await;
        assert_reference(&db, &index, seed, 1007).await;
        let mut serial = 0;
        for step in 0..360 {
            let choice = (next(&mut rng) % 13) as usize;
            serial += 1;
            if projects.is_empty() || choice == 0 {
                let id = format!("p-{serial}");
                project(&db, &id).await;
                projects.push(id);
            } else if executions.is_empty() || choice == 1 || choice == 2 {
                let p = &projects[next(&mut rng) as usize % projects.len()];
                let id = format!("exec-{serial}");
                execution(
                    &db,
                    &id,
                    p,
                    AGENTS[next(&mut rng) as usize % 3],
                    if choice == 2 {
                        db::ExecutionStatus::Completed
                    } else {
                        db::ExecutionStatus::Running
                    },
                )
                .await;
                executions.push(id);
            } else if invocations.is_empty() || choice == 3 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                if let Some(e) = ExecutionRepo::get_by_id(&*db, id).await.unwrap() {
                    let task = TaskRepo::get_by_id(&*db, &e.task_id, false)
                        .await
                        .unwrap()
                        .unwrap();
                    let id = format!("inv-{serial}");
                    admit(
                        &db,
                        &id,
                        &task.project_id,
                        &e.id,
                        AGENTS[next(&mut rng) as usize % 3],
                    )
                    .await;
                    invocations.push(id);
                }
            } else if choice <= 6 {
                let id = &invocations[next(&mut rng) as usize % invocations.len()];
                if let Some(i) = UsageLedgerRepo::get_usage_invocation(&*db, id)
                    .await
                    .unwrap()
                {
                    match i.lifecycle {
                        UsageInvocationLifecycle::Admitted => {
                            start(&db, &i).await;
                        }
                        UsageInvocationLifecycle::Started
                        | UsageInvocationLifecycle::PendingSettlement => {
                            if choice == 4 && i.lifecycle == UsageInvocationLifecycle::Started {
                                UsageLedgerRepo::mark_usage_invocation_pending_settlement(
                                    &*db,
                                    db::MarkUsageInvocationPendingSettlement {
                                        id: i.id.clone(),
                                        expected_version: i.version,
                                        updated_at: AT.into(),
                                    },
                                )
                                .await
                                .unwrap();
                            } else if choice == 5 {
                                UsageLedgerRepo::mark_usage_invocation_unsettled(
                                    &*db,
                                    db::MarkUsageInvocationUnsettled {
                                        id: i.id.clone(),
                                        expected_version: i.version,
                                        terminal_reason: "fixture failure".into(),
                                        settled_at: AT.into(),
                                        updated_at: AT.into(),
                                    },
                                )
                                .await
                                .unwrap();
                            } else {
                                settle(&db, &i).await;
                            }
                        }
                        _ => {}
                    }
                }
            } else if choice == 7 || choice == 8 {
                let id = &invocations[next(&mut rng) as usize % invocations.len()];
                if let Some(i) = UsageLedgerRepo::get_usage_invocation(&*db, id)
                    .await
                    .unwrap()
                {
                    if i.lifecycle == UsageInvocationLifecycle::Settled {
                        let id = format!("event-{serial}");
                        event(
                            &db,
                            &i,
                            &id,
                            AGENTS[next(&mut rng) as usize % 3],
                            choice == 7,
                            serial,
                        )
                        .await;
                        events.push(id);
                    }
                }
            } else if choice == 9 && !events.is_empty() {
                let id = &events[next(&mut rng) as usize % events.len()];
                if let Some(e) = UsageLedgerRepo::get_usage_event(&*db, id).await.unwrap() {
                    if e.cost_kind != UsageCostKind::ProviderReported {
                        reprice(&db, &e, step).await;
                    }
                }
            } else if choice == 10 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                if let Some(e) = ExecutionRepo::get_by_id(&*db, id).await.unwrap() {
                    if e.status == db::ExecutionStatus::Running {
                        finish(&db, &e, next(&mut rng).is_multiple_of(2)).await;
                    }
                }
            } else if choice == 11 {
                // There is no execution-delete repository API; this is the
                // exact SQL boundary used by task_service/workspace.rs.
                let n = next(&mut rng) as usize % executions.len();
                let id = executions.remove(n);
                sqlx::query("DELETE FROM execution WHERE id=?")
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else if choice == 12 && step % 7 == 0 {
                let n = next(&mut rng) as usize % projects.len();
                let id = projects.remove(n);
                for e in ExecutionRepo::list_running_for_project(&*db, &id)
                    .await
                    .unwrap()
                {
                    finish(&db, &e, true).await;
                }
                ProjectRepo::delete(&*db, &id).await.unwrap();
                executions = sqlx::query_scalar("SELECT id FROM execution ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                invocations = sqlx::query_scalar("SELECT id FROM usage_invocation ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                events = sqlx::query_scalar("SELECT id FROM usage_event ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
            }
            assert_reference(&db, &index, seed, step).await;
        }
    }
}

#[tokio::test]
async fn appended_events_read_only_deltas_at_two_history_sizes() {
    for history in [12, 420] {
        let db = fixture().await;
        project(&db, "p").await;
        for n in 0..history {
            let id = format!("run-{n}");
            execution(&db, &id, "p", "a", db::ExecutionStatus::Completed).await;
            let i = admit(&db, &format!("attempt-{n}"), "p", &id, "a").await;
            let i = start(&db, &i).await;
            let i = settle(&db, &i).await;
            event(&db, &i, &format!("old-{n}"), "a", true, 0).await;
        }
        let index = UsageLedgerIndex::new(db.clone());
        index.operations().await.unwrap();
        let before_b = index.agent("b").await.unwrap();
        let i = UsageLedgerRepo::get_usage_invocation(&*db, "attempt-0")
            .await
            .unwrap()
            .unwrap();
        for n in 1..=7 {
            event(&db, &i, &format!("delta-{n}"), "a", true, n).await;
        }
        index.operations().await.unwrap();
        let audit = index.state.lock().await.audit;
        assert_eq!(
            audit.payload_rows, 14,
            "only seven events and seven provenance joins, history={history}"
        );
        assert_eq!(audit.attempt_updates, 0);
        assert_reference(&db, &index, 0, history).await;
        assert_eq!(
            serde_json::to_value(index.agent("b").await.unwrap()).unwrap(),
            serde_json::to_value(before_b).unwrap()
        );
        let state = index.state.lock().await;
        assert!(state.bytes < MAX_INDEX_BYTES);
        assert!(!state.scopes.contains_key(&Some(Arc::from("b"))));
    }
}

#[tokio::test]
async fn old_execution_ownership_and_source_provenance_updates_are_exact() {
    let db = fixture().await;
    project(&db, "p").await;
    execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
    let i = admit(&db, "i", "p", "run", "a").await;
    let i = start(&db, &i).await;
    let i = settle(&db, &i).await;
    let e = event(&db, &i, "e", "b", false, 1).await;
    let index = UsageLedgerIndex::new(db.clone());
    assert_reference(&db, &index, 1, 0).await;
    // Metadata edits below rowid watermarks cannot be inferred from running
    // work. The revision trigger captures them regardless of terminal status.
    sqlx::query("UPDATE execution SET agent_id='c' WHERE id='run'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_reference(&db, &index, 1, 1).await;
    reprice(&db, &e, 1).await;
    assert_reference(&db, &index, 1, 2).await;
    reprice(&db, &e, 2).await;
    assert_reference(&db, &index, 1, 3).await;
    sqlx::query("DELETE FROM execution WHERE id='run'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_reference(&db, &index, 1, 4).await;
    ProjectRepo::delete(&*db, "p").await.unwrap();
    assert_reference(&db, &index, 1, 5).await;
    // Rowid reuse after teardown must remain visible after a rebuild.
    project(&db, "next").await;
    execution(&db, "new", "next", "b", db::ExecutionStatus::Running).await;
    assert_reference(&db, &index, 1, 6).await;
}

#[tokio::test]
async fn a_long_single_run_and_capacity_fallback_remain_correct() {
    let db = fixture().await;
    project(&db, "p").await;
    execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
    let i = admit(&db, "i", "p", "run", "a").await;
    let i = start(&db, &i).await;
    let i = settle(&db, &i).await;
    for n in 0..420 {
        event(&db, &i, &format!("history-{n}"), "a", true, n).await;
    }
    let index = UsageLedgerIndex::new(db.clone());
    index.operations().await.unwrap();
    event(&db, &i, "delta", "a", true, 421).await;
    index.operations().await.unwrap();
    assert_eq!(
        index.state.lock().await.audit.payload_rows,
        2,
        "one event plus its frozen provenance, even on a long run"
    );
    assert_reference(&db, &index, 0, 0).await;
    index.state.lock().await.bytes = MAX_INDEX_BYTES;
    event(&db, &i, "overflow", "a", true, 422).await;
    assert_reference(&db, &index, 0, 1).await;
    assert!(index.state.lock().await.overflow.is_some());
    ProjectRepo::delete(&*db, "p").await.unwrap();
    assert_reference(&db, &index, 0, 2).await;
    assert!(index.state.lock().await.overflow.is_none());
    println!(
        "summary bytes: Invocation={}, Events={}, Run={}, Execution={}, budget={}",
        std::mem::size_of::<Invocation>(),
        std::mem::size_of::<Events>(),
        std::mem::size_of::<Run>(),
        std::mem::size_of::<Execution>(),
        MAX_INDEX_BYTES
    );
}

#[test]
fn provenance_last_writer_is_ordered_not_additive() {
    let mut first = crate::task_usage_fixture::invocation("a");
    first.source_id = "run".into();
    first.agent_id = Some("a".into());
    let mut second = first.clone();
    second.id = "b".into();
    second.agent_id = Some("b".into());
    second.attempt_ordinal = 1;
    let make = |id: &str, inv: &str, freshness, retrospective, amount| {
        let mut event = crate::task_usage_fixture::event(id, inv);
        event.source_id = "run".into();
        event.agent_id = Some("a".into());
        event.input_tokens = Some(1);
        event.output_tokens = Some(0);
        event.provider_reported_nano_usd = None;
        event.estimated_nano_usd = Some(amount);
        event.cost_kind = UsageCostKind::Estimated;
        EffectiveUsageEvent {
            event,
            source: Some((
                "estimated:models_dev_catalog:rate:catalog".into(),
                CostSourceRef {
                    source_kind: CostSourceKind::ModelsDevCatalog,
                    rate_revision_id: Some("rate".into()),
                    catalog_snapshot_id: Some("catalog".into()),
                    catalog_digest: None,
                    effective_at: None,
                    fetched_at: None,
                    freshness,
                    retrospective,
                    formula_revision: Some("formula".into()),
                },
            )),
        }
    };
    let e1 = make("1", "a", CostSourceFreshness::Fresh, false, 1000);
    let e2 = make("2", "b", CostSourceFreshness::Stale, true, 2000);
    let mut state = State::default();
    state.invocation(first.clone()).unwrap();
    state.invocation(second.clone()).unwrap();
    state.event(e2.clone(), 1).unwrap();
    state.event(e1.clone(), 1).unwrap();
    let compare = |state: &mut State, second_event: EffectiveUsageEvent| {
        let expected = aggregate_usage_with_sources(
            &[first.clone(), second.clone()],
            &HashMap::from([
                ("a".into(), vec![e1.clone()]),
                ("b".into(), vec![second_event]),
            ]),
            &[],
        )
        .unwrap();
        let actual = state.scopes.get_mut(&None).unwrap().public().unwrap();
        assert_eq!(actual, expected);
    };
    compare(&mut state, e2.clone());
    state.event(e2, -1).unwrap();
    let changed = make("2", "b", CostSourceFreshness::RefreshFailed, true, 3000);
    state.event(changed.clone(), 1).unwrap();
    compare(&mut state, changed);
}

#[test]
fn retained_citation_winner_yields_to_newer_events_in_same_delta() {
    let reference = CostSourceRef {
        source_kind: CostSourceKind::ModelsDevCatalog,
        rate_revision_id: Some("rate".into()),
        catalog_snapshot_id: Some("catalog".into()),
        catalog_digest: None,
        effective_at: None,
        fetched_at: None,
        freshness: CostSourceFreshness::Fresh,
        retrospective: true,
        formula_revision: Some("formula".into()),
    };
    let order = |event: &str| SourceOrder {
        source: Arc::from("run"),
        ordinal: 0,
        invocation: Arc::from("inv"),
        occurred: Arc::from(AT),
        event: Arc::from(event),
    };
    let mut book = Citations::default();
    book.change(&reference, order("a"), 1, false).unwrap();
    book.change(&reference, order("b"), 1, false).unwrap();
    book.change(&reference, order("b"), -1, false).unwrap();
    book.change(&reference, order("b"), 1, false).unwrap();
    book.change(&reference, order("c"), 1, false).unwrap();
    book.repair(&reference, Some(order("b"))).unwrap();
    assert_eq!(book.winners.last_key_value().unwrap().0.event.as_ref(), "c");
    assert_eq!(book.winner().unwrap(), reference);
}

fn timestamp(n: u64) -> String {
    format!(
        "1970-01-01T00:{:02}:{:02}.{:03}Z",
        2 + n % 50,
        (n / 50) % 60,
        (n / 3000) % 1000
    )
}
async fn finish_at(db: &db::SqliteDb, e: &db::Execution, failed: bool, at: &str) {
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
async fn assert_reference_full(
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

#[tokio::test]
async fn randomized_batched_deltas() {
    // Each seed executes a few hundred real repository mutations. Attribution
    // tolerance is tested through append_usage_event with only its attribution
    // guard relaxed; every remaining V135 identity/lifecycle guard stays live.
    for seed in [
        0x1234_5678u64,
        0xabc0_0191,
        0xfade_7013,
        0x51ed_270b,
        0x0bad_cafe,
        0x7777_1234,
    ] {
        let mut check_rng = seed ^ 0x9e37_79b9;
        let mut until_read = 1 + next(&mut check_rng) % 8;
        let db = fixture().await;
        let index = UsageLedgerIndex::new(db.clone());
        let mut rng = seed;
        let mut projects = Vec::<String>::new();
        let mut executions = Vec::<String>::new();
        let mut invocations = Vec::<String>::new();
        let mut events = Vec::<String>::new();
        project(&db, "prelude").await;
        projects.push("prelude".to_owned());
        let exec = execution(
            &db,
            "prelude-run",
            "prelude",
            "a",
            db::ExecutionStatus::Running,
        )
        .await;
        executions.push(exec.id.clone());
        let inv = admit(&db, "prelude-inv", "prelude", &exec.id, "b").await;
        invocations.push(inv.id.clone());
        assert_reference(&db, &index, seed, 1000).await;
        let inv = start(&db, &inv).await;
        assert_reference(&db, &index, seed, 1001).await;
        let inv = UsageLedgerRepo::mark_usage_invocation_pending_settlement(
            &*db,
            db::MarkUsageInvocationPendingSettlement {
                id: inv.id,
                expected_version: inv.version,
                updated_at: AT.into(),
            },
        )
        .await
        .unwrap();
        assert_reference(&db, &index, seed, 1002).await;
        let inv = settle(&db, &inv).await;
        assert_reference(&db, &index, seed, 1003).await;
        let e = event(&db, &inv, "prelude-e", "a", false, 0).await;
        events.push(e.id.clone());
        assert_reference(&db, &index, seed, 1004).await;
        reprice(&db, &e, 5000).await;
        assert_reference(&db, &index, seed, 1005).await;
        reprice(&db, &e, 5001).await;
        assert_reference(&db, &index, seed, 1006).await;
        finish(&db, &exec, true).await;
        assert_reference(&db, &index, seed, 1007).await;
        admission_binding(&db).await;
        let mut serial = 0;
        for step in 0..360 {
            let choice = (next(&mut rng) % 19) as usize;
            serial += 1;
            if projects.is_empty() || choice == 0 {
                let id = format!("p-{serial}");
                project(&db, &id).await;
                projects.push(id);
            } else if executions.is_empty() || choice == 1 || choice == 2 {
                let p = &projects[next(&mut rng) as usize % projects.len()];
                let id = format!("exec-{serial}");
                execution(
                    &db,
                    &id,
                    p,
                    AGENTS[next(&mut rng) as usize % 3],
                    if choice == 2 {
                        db::ExecutionStatus::Completed
                    } else {
                        db::ExecutionStatus::Running
                    },
                )
                .await;
                executions.push(id);
            } else if invocations.is_empty() || choice == 3 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                if let Some(e) = ExecutionRepo::get_by_id(&*db, id).await.unwrap() {
                    let task = TaskRepo::get_by_id(&*db, &e.task_id, false)
                        .await
                        .unwrap()
                        .unwrap();
                    let id = format!("inv-{serial}");
                    let surface = [
                        DbUsageSurface::TaskExecution,
                        DbUsageSurface::ProjectChat,
                        DbUsageSurface::MainChat,
                        DbUsageSurface::GenesisChat,
                        DbUsageSurface::MainInquiry,
                    ][next(&mut rng) as usize % 5];
                    let project = matches!(
                        surface,
                        DbUsageSurface::TaskExecution | DbUsageSurface::ProjectChat
                    )
                    .then_some(task.project_id.as_str());
                    let owner = (!next(&mut rng).is_multiple_of(4))
                        .then_some(AGENTS[next(&mut rng) as usize % 3]);
                    let ordinal = (next(&mut rng) % 4) as i64;
                    let priced = next(&mut rng).is_multiple_of(2);
                    scoped_admit(
                        &db,
                        &id,
                        project,
                        &format!("{}-{surface}", e.id),
                        owner,
                        surface,
                        ordinal,
                        priced,
                    )
                    .await;
                    invocations.push(id);
                }
            } else if choice <= 6 {
                let id = &invocations[next(&mut rng) as usize % invocations.len()];
                if let Some(i) = UsageLedgerRepo::get_usage_invocation(&*db, id)
                    .await
                    .unwrap()
                {
                    match i.lifecycle {
                        UsageInvocationLifecycle::Admitted => {
                            start(&db, &i).await;
                        }
                        UsageInvocationLifecycle::Started
                        | UsageInvocationLifecycle::PendingSettlement => {
                            if choice == 4 && i.lifecycle == UsageInvocationLifecycle::Started {
                                UsageLedgerRepo::mark_usage_invocation_pending_settlement(
                                    &*db,
                                    db::MarkUsageInvocationPendingSettlement {
                                        id: i.id.clone(),
                                        expected_version: i.version,
                                        updated_at: AT.into(),
                                    },
                                )
                                .await
                                .unwrap();
                            } else if choice == 5 {
                                UsageLedgerRepo::mark_usage_invocation_unsettled(
                                    &*db,
                                    db::MarkUsageInvocationUnsettled {
                                        id: i.id.clone(),
                                        expected_version: i.version,
                                        terminal_reason: "fixture failure".into(),
                                        settled_at: AT.into(),
                                        updated_at: AT.into(),
                                    },
                                )
                                .await
                                .unwrap();
                            } else {
                                if next(&mut rng).is_multiple_of(4) {
                                    UsageLedgerRepo::settle_usage_invocation(
                                        &*db,
                                        db::SettleUsageInvocation {
                                            id: i.id.clone(),
                                            expected_version: i.version,
                                            telemetry_state: DbUsageTelemetryState::Unmetered,
                                            terminal_reason: Some("provider did not meter".into()),
                                            settled_at: timestamp(next(&mut rng)),
                                            updated_at: timestamp(next(&mut rng)),
                                        },
                                    )
                                    .await
                                    .unwrap();
                                } else {
                                    settle(&db, &i).await;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else if choice == 7 || choice == 8 {
                let id = &invocations[next(&mut rng) as usize % invocations.len()];
                if let Some(i) = UsageLedgerRepo::get_usage_invocation(&*db, id)
                    .await
                    .unwrap()
                {
                    if i.lifecycle == UsageInvocationLifecycle::Settled {
                        let id = format!("event-{serial}");
                        event(
                            &db,
                            &i,
                            &id,
                            AGENTS[next(&mut rng) as usize % 3],
                            choice == 7,
                            serial,
                        )
                        .await;
                        events.push(id);
                    }
                }
            } else if choice == 9 && !events.is_empty() {
                let id = &events[next(&mut rng) as usize % events.len()];
                if let Some(e) = UsageLedgerRepo::get_usage_event(&*db, id).await.unwrap() {
                    if e.cost_kind != UsageCostKind::ProviderReported
                        && e.project_id.is_some()
                        && e.input_tokens.is_some()
                    {
                        reprice(&db, &e, step).await;
                    }
                }
            } else if choice == 10 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                if let Some(e) = ExecutionRepo::get_by_id(&*db, id).await.unwrap() {
                    if e.status == db::ExecutionStatus::Running {
                        {
                            let failed = next(&mut rng).is_multiple_of(2);
                            let at = timestamp(next(&mut rng));
                            finish_at(&db, &e, failed, &at).await;
                        }
                    }
                }
            } else if choice == 11 {
                // There is no execution-delete repository API; this is the
                // exact SQL boundary used by task_service/workspace.rs.
                let n = next(&mut rng) as usize % executions.len();
                let id = executions.remove(n);
                sqlx::query("DELETE FROM execution WHERE id=?")
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else if choice == 12 && step % 7 == 0 {
                let n = next(&mut rng) as usize % projects.len();
                let id = projects.remove(n);
                for e in ExecutionRepo::list_running_for_project(&*db, &id)
                    .await
                    .unwrap()
                {
                    finish(&db, &e, true).await;
                }
                ProjectRepo::delete(&*db, &id).await.unwrap();
                executions = sqlx::query_scalar("SELECT id FROM execution ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                invocations = sqlx::query_scalar("SELECT id FROM usage_invocation ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                events = sqlx::query_scalar("SELECT id FROM usage_event ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
            } else if choice == 13 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                let at = timestamp(next(&mut rng));
                sqlx::query("UPDATE execution SET updated_at=?, last_activity_at=? WHERE id=? AND status='running'")
                    .bind(&at).bind(&at).bind(id).execute(db.pool()).await.unwrap();
            } else if choice == 14 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                let at = timestamp(next(&mut rng));
                sqlx::query("UPDATE execution SET updated_at=? WHERE id=? AND status!='running'")
                    .bind(&at)
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else if choice == 15 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                let agent = AGENTS[next(&mut rng) as usize % 3];
                sqlx::query("UPDATE execution SET agent_id=? WHERE id=?")
                    .bind(agent)
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else if choice == 16 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                let at = format!("1970-01-01T00:00:{:02}Z", next(&mut rng) % 60);
                sqlx::query("UPDATE execution SET created_at=? WHERE id=?")
                    .bind(&at)
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            until_read -= 1;
            if step == 359 || until_read == 0 {
                until_read = 1 + next(&mut check_rng) % 8;
                assert_reference_full(&db, &index, seed, step).await;
            }
        }
        let nonempty_cold = UsageLedgerIndex::new(db.clone());
        assert_reference_full(&db, &nonempty_cold, seed, 9997).await;
        // Remove Project scopes through their guarded repository boundary;
        // NULL-Project usage then exercises the final account-row cascade.
        for project in &projects {
            for execution in ExecutionRepo::list_running_for_project(&*db, project)
                .await
                .unwrap()
            {
                finish(&db, &execution, true).await;
            }
            ProjectRepo::delete(&*db, project).await.unwrap();
        }
        db::UserRepo::delete_user(&*db, "pricing-owner")
            .await
            .unwrap();
        assert_reference_full(&db, &index, seed, 9998).await;
        let cold = UsageLedgerIndex::new(db.clone());
        assert_reference_full(&db, &cold, seed, 9999).await;
    }
}

// The agent aggregate as it was before this commit (per-source, per-invocation walk).
async fn old_agent_reference(db: &db::SqliteDb, identity_id: &str) -> Result<UsageAggregate> {
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

#[tokio::test]
async fn batched_agent_reference_equals_per_invocation_reference() {
    // Reuse the batched random generator indirectly: build a mixed ledger, then compare.
    for seed in [0x1234_5678u64, 0xabc0_0191, 0xfade_7013] {
        let db = fixture().await;
        let mut rng = seed;
        project(&db, "p").await;
        let mut invs = Vec::new();
        let mut evs = Vec::new();
        for n in 0..60 {
            let id = format!("run-{n}");
            let status = if next(&mut rng).is_multiple_of(3) {
                db::ExecutionStatus::Running
            } else {
                db::ExecutionStatus::Completed
            };
            execution(&db, &id, "p", AGENTS[next(&mut rng) as usize % 3], status).await;
            for k in 0..(next(&mut rng) % 3) {
                let i = admit(
                    &db,
                    &format!("inv-{n}-{k}"),
                    "p",
                    &id,
                    AGENTS[next(&mut rng) as usize % 3],
                )
                .await;
                let i = start(&db, &i).await;
                let i = match next(&mut rng) % 4 {
                    0 => i,
                    1 => UsageLedgerRepo::mark_usage_invocation_unsettled(
                        &*db,
                        db::MarkUsageInvocationUnsettled {
                            id: i.id.clone(),
                            expected_version: i.version,
                            terminal_reason: "x".into(),
                            settled_at: AT.into(),
                            updated_at: AT.into(),
                        },
                    )
                    .await
                    .unwrap(),
                    _ => settle(&db, &i).await,
                };
                if i.lifecycle == UsageInvocationLifecycle::Settled {
                    for j in 0..(next(&mut rng) % 3) {
                        let e = event(
                            &db,
                            &i,
                            &format!("ev-{n}-{k}-{j}"),
                            AGENTS[next(&mut rng) as usize % 3],
                            next(&mut rng).is_multiple_of(2),
                            j as i64,
                        )
                        .await;
                        evs.push(e);
                    }
                }
                invs.push(i);
            }
        }
        let mut step = 0;
        for e in &evs {
            if e.cost_kind != UsageCostKind::ProviderReported && next(&mut rng).is_multiple_of(3) {
                step += 1;
                reprice(&db, e, 7000 + step).await;
            }
        }
        for id in AGENTS {
            let new = usage_aggregate_for_agent(&db, id).await.unwrap();
            let old = old_agent_reference(&db, id).await.unwrap();
            assert_eq!(
                serde_json::to_value(new).unwrap(),
                serde_json::to_value(old).unwrap(),
                "agent {id} seed {seed}"
            );
        }
    }
}

async fn file_db(path: &std::path::Path) -> Arc<db::SqliteDb> {
    let pool = db::create_sqlite_pool(&format!("sqlite:{}", path.display()))
        .await
        .unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(db::SqliteDb::new(pool));
    crate::pricing_db::tests::subject_fixture(&db).await;
    for id in AGENTS {
        sqlx::query("INSERT INTO agent_identity (id,name,max_concurrent_tasks,created_at,updated_at) VALUES (?,?,100000,?,?)")
            .bind(id).bind(id).bind(AT).bind(AT).execute(db.pool()).await.unwrap();
    }
    db
}
fn repair_citations(state: &mut State, events: &[EffectiveUsageEvent]) {
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_event_and_reprice_deltas_are_atomic() {
    use std::future::Future;
    use std::task::Poll;
    for reprice_delta in [false, true] {
        let mut completed = false;
        for cancel_after in 1..=200usize {
            let directory = tempfile::tempdir().unwrap();
            let db = file_db(&directory.path().join("cancel.db")).await;
            project(&db, "p").await;
            execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
            let i = admit(&db, "i", "p", "run", "a").await;
            let i = start(&db, &i).await;
            let i = settle(&db, &i).await;
            let e = event(&db, &i, "e0", "a", !reprice_delta, 0).await;
            if reprice_delta {
                reprice(&db, &e, 2000).await;
            }
            let index = UsageLedgerIndex::new(db.clone());
            assert_reference_full(&db, &index, 0, 0).await;
            if reprice_delta {
                reprice(&db, &e, 2001).await;
            } else {
                event(&db, &i, "e1", "a", true, 1).await;
            }
            completed = {
                let future = index.operations();
                tokio::pin!(future);
                let mut polls = 0;
                std::future::poll_fn(|cx| {
                    polls += 1;
                    if polls > cancel_after {
                        return Poll::Ready(false);
                    }
                    match future.as_mut().poll(cx) {
                        Poll::Ready(value) => {
                            value.unwrap();
                            Poll::Ready(true)
                        }
                        Poll::Pending => Poll::Pending,
                    }
                })
                .await
            };
            assert_reference_full(&db, &index, u64::from(reprice_delta), cancel_after).await;
            assert_reference_full(&db, &index, u64::from(reprice_delta), cancel_after).await;
            if completed {
                break;
            }
        }
        assert!(
            completed,
            "never reached completion reprice={reprice_delta}"
        );
    }
}
#[path = "concurrent_tests.rs"]
mod concurrent_tests;
#[path = "fold_tests.rs"]
mod fold_tests;

async fn admission_binding(db: &Arc<db::SqliteDb>) {
    use crate::pricing::PricingCatalogRepository;
    let snapshot=crate::pricing::parse_models_dev_catalog(br#"{"openai":{"id":"openai","name":"OpenAI","models":{"gpt-test":{"id":"gpt-test","last_updated":"2026-09-01","cost":{"input":1,"output":2,"cache_read":0}}}}}"#).unwrap().into_snapshot("admission-catalog",None,SystemTime::UNIX_EPOCH+Duration::from_secs(10),SystemTime::UNIX_EPOCH+Duration::from_secs(10)).unwrap();
    crate::pricing_db::SqlitePricingRepository::new(db.clone())
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

#[tokio::test]
async fn compact_citations_recover_removed_winners_without_retaining_events() {
    let db = fixture().await;
    project(&db, "p").await;
    admission_binding(&db).await;
    let inv = scoped_admit(
        &db,
        "priced",
        Some("p"),
        "z-run",
        Some("a"),
        DbUsageSurface::ProjectChat,
        2,
        true,
    )
    .await;
    let inv = start(&db, &inv).await;
    let inv = settle(&db, &inv).await;
    event(&db, &inv, "first", "a", false, 0).await;
    let last = event(&db, &inv, "last", "a", false, 1).await;
    let index = UsageLedgerIndex::new(db.clone());
    assert_reference_full(&db, &index, 0, 0).await;
    let before = index.state.lock().await.charged_bytes();
    for n in 2..502 {
        event(&db, &inv, &format!("more-{n}"), "a", false, n).await;
    }
    assert_reference_full(&db, &index, 0, 1).await;
    let state = index.state.lock().await;
    assert!(
        state.charged_bytes() < before + 4096,
        "same provenance must not retain event citations"
    );
    assert_eq!(
        state.scopes[&None]
            .sources
            .values()
            .map(|book| book.variants.len())
            .sum::<usize>(),
        1
    );
    let winning_id = state.scopes[&None]
        .sources
        .values()
        .next()
        .unwrap()
        .variants
        .values()
        .next()
        .unwrap()
        .winner
        .as_ref()
        .unwrap()
        .event
        .to_string();
    drop(state);
    // Reprice the current winning event. Its old key still has 501 events,
    // so the index must recover the next winner from the ledger snapshot.
    let winning = UsageLedgerRepo::get_usage_event(&*db, &winning_id)
        .await
        .unwrap()
        .unwrap();
    {
        let state = index.state.lock().await;
        let through = state.watermarks.unwrap();
        let mut connection = db.pool().acquire().await.unwrap();
        let old = effective_events(
            &mut connection,
            vec![winning.clone()],
            through.estimate_rowid,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let staged = Staged::Delta {
            changes: vec![Change::Event(Box::new(old.clone()), -1)],
            bytes: MAX_INDEX_BYTES - 1,
            audit: Audit::default(),
        };
        assert!(
            matches!(
                citation_repairs(&state, &staged, &mut connection, through)
                    .await
                    .unwrap(),
                CitationRepairs::Overflow(_)
            ),
            "repair buffers must share the delta's memory bound"
        );
        let mut repriced = old.clone();
        repriced.event.estimated_nano_usd = Some(old.event.estimated_nano_usd.unwrap() + 1);
        let changes = vec![
            Change::Event(Box::new(old), -1),
            Change::Event(Box::new(repriced), 1),
        ];
        let staged = Staged::Delta {
            bytes: state.charged_bytes() + changes.iter().map(Change::staging_bytes).sum::<usize>(),
            changes,
            audit: Audit::default(),
        };
        // A connection with no schema is a read seam: any historical lookup
        // fails. Amount-only repricing must retain the known citation winner.
        let mut empty = <sqlx::SqliteConnection as sqlx::Connection>::connect("sqlite::memory:")
            .await
            .unwrap();
        let CitationRepairs::Ready(repairs, _) =
            citation_repairs(&state, &staged, &mut empty, through)
                .await
                .unwrap()
        else {
            panic!("small reprice must fit")
        };
        assert_eq!(
            repairs.len(),
            2,
            "global and owning Agent keep the same winner"
        );
    }
    reprice(&db, &winning, 9200).await;
    assert_reference_full(&db, &index, 0, 2).await;
    reprice(&db, &last, 9201).await;
    assert_reference_full(&db, &index, 0, 3).await;
}

#[tokio::test]
async fn invariant_failure_and_small_deletion_use_memoized_unlocked_fallback() {
    let db = fixture().await;
    project(&db, "p").await;
    for n in 0..4 {
        execution(
            &db,
            &format!("run-{n}"),
            "p",
            "a",
            db::ExecutionStatus::Completed,
        )
        .await;
    }
    let index = UsageLedgerIndex::new(db.clone());
    index.operations().await.unwrap();
    // A corrupt internal counter never becomes an API InvalidTransition.
    index
        .state
        .lock()
        .await
        .scopes
        .get_mut(&None)
        .unwrap()
        .sources
        .insert("broken".into(), Citations::default());
    index
        .state
        .lock()
        .await
        .scopes
        .get_mut(&None)
        .unwrap()
        .cached = None;
    assert_reference(&db, &index, 0, 0).await;
    assert!(index.state.lock().await.overflow.is_some());
    assert!(index.fallback.lock().await.operations.is_some());
    sqlx::query("DELETE FROM execution WHERE id='run-0'")
        .execute(db.pool())
        .await
        .unwrap();
    // Exactly 25% shrink is enough to retry, and this small ledger fits again.
    assert_reference_full(&db, &index, 0, 1).await;
    assert!(index.state.lock().await.overflow.is_none());
}

#[tokio::test]
async fn cold_build_skips_change_markers_and_repairs_stale_rowid_header() {
    let db = fixture().await;
    project(&db, "p").await;
    execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
    let inv = admit(&db, "i", "p", "run", "a").await;
    let inv = start(&db, &inv).await;
    let inv = settle(&db, &inv).await;
    event(&db, &inv, "e", "a", true, 0).await;
    sqlx::query("UPDATE execution SET updated_at='1970-01-01T00:04:00Z' WHERE id='run'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE usage_ledger_revision SET invocation_rowid=0,event_rowid=0,execution_rowid=0 WHERE id=1").execute(db.pool()).await.unwrap();
    let index = UsageLedgerIndex::new(db.clone());
    index.operations().await.unwrap();
    let audit = index.state.lock().await.audit;
    assert_eq!(
        audit.payload_rows, 4,
        "one invocation, event, provenance join and execution; no change-marker replay"
    );
    assert_reference_full(&db, &index, 0, 0).await;
    let inv = UsageLedgerRepo::get_usage_invocation(&*db, "i")
        .await
        .unwrap()
        .unwrap();
    event(&db, &inv, "new", "a", true, 1).await;
    assert_reference_full(&db, &index, 0, 1).await;
    // A future table rebuild can also leave an oversized maximum. An insert
    // below that stale maximum still has to invalidate the warm projection.
    sqlx::query("UPDATE usage_ledger_revision SET event_rowid=1000 WHERE id=1")
        .execute(db.pool())
        .await
        .unwrap();
    let high = UsageLedgerIndex::new(db.clone());
    assert_reference_full(&db, &high, 0, 2).await;
    event(&db, &inv, "after-rebuild", "a", true, 2).await;
    assert_reference_full(&db, &high, 0, 3).await;
}

#[test]
fn cold_fit_probe_matches_hash_table_capacity_without_allocating_history() {
    for rows in [
        1usize, 3, 4, 7, 8, 14, 15, 28, 29, 100, 448, 449, 1792, 1793, 7168, 7169,
    ] {
        let map = HashMap::<u64, u64>::with_capacity(rows);
        let buckets = rows
            .saturating_mul(8)
            .div_ceil(7)
            .max(4)
            .next_power_of_two();
        assert_eq!(map.capacity(), buckets * 7 / 8, "rows={rows}");
    }
    let header = Watermarks {
        rows: [99_000, 99_000, 0, 33_000],
        owned_executions: 33_000,
        ..Watermarks::default()
    };
    assert!(UsageLedgerIndex::minimum_charge(header) > MAX_INDEX_BYTES);
}

#[tokio::test]
async fn new_event_and_applied_revision_in_one_delta_are_folded_once() {
    let db = fixture().await;
    project(&db, "p").await;
    execution(&db, "run", "p", "a", db::ExecutionStatus::Completed).await;
    let index = UsageLedgerIndex::new(db.clone());
    index.operations().await.unwrap();
    let inv = admit(&db, "i", "p", "run", "a").await;
    let inv = start(&db, &inv).await;
    let inv = settle(&db, &inv).await;
    let event = event(&db, &inv, "new", "b", false, 0).await;
    reprice(&db, &event, 9400).await;
    assert_reference_full(&db, &index, 0, 0).await;
}

#[tokio::test]
async fn overflow_source_batches_equal_reference_for_all_scopes() {
    // Reuse the batched random generator indirectly: build a mixed ledger, then compare.
    for seed in [0x1234_5678u64, 0xabc0_0191, 0xfade_7013] {
        let db = fixture().await;
        let mut rng = seed;
        project(&db, "p").await;
        let mut invs = Vec::new();
        let mut evs = Vec::new();
        for n in 0..300 {
            let id = format!("run-{n}");
            let status = if next(&mut rng).is_multiple_of(3) {
                db::ExecutionStatus::Running
            } else {
                db::ExecutionStatus::Completed
            };
            execution(&db, &id, "p", AGENTS[next(&mut rng) as usize % 3], status).await;
            for k in 0..(next(&mut rng) % 3) {
                let i = admit(
                    &db,
                    &format!("inv-{n}-{k}"),
                    "p",
                    &id,
                    AGENTS[next(&mut rng) as usize % 3],
                )
                .await;
                let i = start(&db, &i).await;
                let i = match next(&mut rng) % 4 {
                    0 => i,
                    1 => UsageLedgerRepo::mark_usage_invocation_unsettled(
                        &*db,
                        db::MarkUsageInvocationUnsettled {
                            id: i.id.clone(),
                            expected_version: i.version,
                            terminal_reason: "x".into(),
                            settled_at: AT.into(),
                            updated_at: AT.into(),
                        },
                    )
                    .await
                    .unwrap(),
                    _ => settle(&db, &i).await,
                };
                if i.lifecycle == UsageInvocationLifecycle::Settled {
                    for j in 0..(next(&mut rng) % 3) {
                        let e = event(
                            &db,
                            &i,
                            &format!("ev-{n}-{k}-{j}"),
                            AGENTS[next(&mut rng) as usize % 3],
                            next(&mut rng).is_multiple_of(2),
                            j as i64,
                        )
                        .await;
                        evs.push(e);
                    }
                }
                invs.push(i);
            }
        }
        let mut step = 0;
        for e in &evs {
            if e.cost_kind != UsageCostKind::ProviderReported && next(&mut rng).is_multiple_of(3) {
                step += 1;
                reprice(&db, e, 7000 + step).await;
            }
        }
        let mut tx = db.pool().begin().await.unwrap();
        let operations = overflow_usage_snapshot(&mut tx, None).await.unwrap();
        let ids = AGENTS.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
        let agents = overflow_usage_snapshot(&mut tx, Some(&ids)).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            operations[&None],
            usage_aggregate_for_operations(&db).await.unwrap()
        );
        for id in AGENTS {
            assert_eq!(
                agents[&Some(Arc::from(id))],
                usage_aggregate_for_agent(&db, id).await.unwrap()
            );
            let new = usage_aggregate_for_agent(&db, id).await.unwrap();
            let old = old_agent_reference(&db, id).await.unwrap();
            assert_eq!(
                serde_json::to_value(new).unwrap(),
                serde_json::to_value(old).unwrap(),
                "agent {id} seed {seed}"
            );
        }
    }
}
