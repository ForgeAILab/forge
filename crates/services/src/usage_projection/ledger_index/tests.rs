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
    e.occurred_at = AT.into();
    e.created_at = AT.into();
    e.provider_reported_nano_usd = reported.then_some(1000);
    e.cost_kind = if reported {
        UsageCostKind::ProviderReported
    } else {
        UsageCostKind::None
    };
    e.coverage_reason_code = (!reported).then_some(DbCostCoverageReasonCode::MissingBinding);
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
            occurred_at: at(100),
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
