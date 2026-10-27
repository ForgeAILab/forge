//! Real writer calls compared with snapshots captured from git archive 94609411.
use super::*;
use crate::recovery::test_support::writer_fixture as fixture;
use fixture::{after, Fixture, SnapshotOptions};

const WALL_CLOCK_FIELDS: &[(&str, &str)] = &[
    (
        "/owner_wait/started_at",
        "fixture-time/owner_wait/started_at",
    ),
    (
        "/deferred_dispatch/not_before",
        "fixture-time/deferred_dispatch/not_before",
    ),
];

macro_rules! case {
    ($name:expr, $state:expr, $f:ident, $body:block) => {
        case!($name, $state, $f, SnapshotOptions::default(), $body)
    };
    ($name:expr, $state:expr, $f:ident, $options:expr, $body:block) => {{
        let $f = Fixture::new($state).await;
        db::task_writer::in_task_step($f.step.clone(), async {
            $body;
            after(&$f, $name, $options).await;
        })
        .await;
    }};
}

#[tokio::test]
async fn dependency_and_budget_writers_match_main() {
    case!("dependency-hook", "in_progress", f, {
        f.ctx
            .task_service
            .write_dependency_block(&f.task, &["dependency".into()], None)
            .await
            .unwrap();
    });
    for (name, saved) in [
        (
            "dependency-superseded",
            json!({"status":"in_progress","error_annotation":"{historical}","blocked_json":null,"failed_json":null}),
        ),
        (
            "dependency-carried",
            json!({"caller_extension":{"history":[1,true,null]}}),
        ),
    ] {
        case!(name, "in_progress", f, {
            f.ctx
                .task_service
                .write_dependency_block(
                    &f.task,
                    &["dependency".into()],
                    Some(saved.as_object().unwrap().clone()),
                )
                .await
                .unwrap();
        });
    }
    case!("dependency-hold", "in_progress", f, {
        let row = f
            .ctx
            .task_service
            .write_dependency_block(&f.task, &["dependency".into()], None)
            .await
            .unwrap();
        f.ctx
            .task_service
            .hold_under_dependency_block(
                &row,
                "historical hold".into(),
                "historical interruption".into(),
            )
            .await
            .unwrap()
            .unwrap();
    });
    case!("dependency-snapshot", "in_progress", f, {
        sqlx::query("UPDATE task SET error_annotation='{historical}' WHERE id=?")
            .bind(&f.task.id)
            .execute(f.db.pool())
            .await
            .unwrap();
        let row = f.read().await;
        f.ctx
            .task_service
            .block_cancelled_dependencies(&row, &["dependency".into()])
            .await
            .unwrap();
    });
    case!("replan-budget", "in_progress", f, {
        f.ctx
            .task_service
            .block_replan_exhausted(&f.task, "trigger")
            .await
            .unwrap();
    });
}

#[tokio::test]
async fn review_writers_match_main() {
    case!("review-candidate", "review", f, {
        f.ctx
            .task_service
            .missing_review_candidate(&f.task)
            .await
            .unwrap();
    });
    for (name, retry, reset, barrier) in [
        ("review-ci", false, false, None),
        ("review-reset", false, true, None),
        ("review-retry", true, false, None),
        ("review-empty-barrier", false, false, Some("{}")),
        (
            "review-known-barrier",
            false,
            false,
            Some(r#"{"state":"review","status":"blocked","started_at":"2026-10-25T00:00:00Z"}"#),
        ),
        (
            "review-legacy-barrier",
            false,
            false,
            Some(r#"{"state":"review","status":"running","caller_extension":[1,null]}"#),
        ),
        (
            "review-null-barrier",
            false,
            false,
            Some(r#"{"state":"review","status":"blocked","started_at":null}"#),
        ),
    ] {
        case!(
            name,
            "review",
            f,
            SnapshotOptions {
                metadata_times: if retry { WALL_CLOCK_FIELDS } else { &[] },
                ..Default::default()
            },
            {
                if let Some(raw) = barrier {
                    sqlx::query("UPDATE task SET entry_barrier_json=? WHERE id=?")
                        .bind(raw)
                        .bind(&f.task.id)
                        .execute(f.db.pool())
                        .await
                        .unwrap();
                }
                let row = f.read().await;
                f.ctx
                    .task_service
                    .annotate_review_ci_interruption(&row, &f.ctx, "ci unavailable", retry, reset)
                    .await
                    .unwrap();
            }
        );
    }
}

#[tokio::test]
async fn metadata_writers_match_main() {
    case!("deferred", "in_progress", f, {
        crate::deferred_dispatch::set(
            &f.db,
            &f.task,
            "in_progress",
            "2099-01-01T00:00:00+00:00",
            "retry",
        )
        .await
        .unwrap();
    });
    case!("deferred-latest-execution", "in_progress", f, {
        let execution = f.execution(db::ExecutionStatus::Failed).await;
        let authority = db::LatestExecutionAuthority {
            execution_id: execution.id.clone(),
            role: execution.role.clone(),
            agent_id: execution.agent_id.clone(),
            execution_updated_at: execution.updated_at.clone(),
            expected_project_version: db::ProjectRepo::get_by_id(&*f.db, &f.task.project_id)
                .await
                .unwrap()
                .unwrap()
                .version,
        };
        assert!(
            crate::deferred_dispatch::set_with_mutations_for_latest_execution(
                &f.db,
                &f.task,
                authority,
                "in_progress",
                "2099-01-01T00:00:00+00:00",
                "retry",
                vec![]
            )
            .await
            .unwrap()
        );
    });
    case!("paused-integration", "merging", f, {
        crate::deferred_dispatch::defer_integration_for_pause(&f.db, &f.task)
            .await
            .unwrap();
    });
    case!("paused-refresh", "merging", f, {
        crate::deferred_dispatch::defer_integration_for_pause(&f.db, &f.task)
            .await
            .unwrap();
        crate::deferred_dispatch::refresh_paused_integration_for_pause(&f.db, &f.read().await)
            .await
            .unwrap();
    });
    case!("disposition", "in_progress", f, {
        crate::deferred_dispatch::record_project_capacity_wait(
            &f.db,
            &f.task,
            db::ProjectWait::WaitingOnOwner,
            "wait",
        )
        .await
        .unwrap();
    });
    case!("disposition-dependency", "in_progress", f, {
        crate::deferred_dispatch::record_dispatch_disposition_naming(
            &f.db,
            &f.task,
            "dependency_wait",
            "wait",
            &["dependency".into()],
        )
        .await
        .unwrap();
    });
    case!("disposition-disk", "in_progress", f, {
        crate::deferred_dispatch::record_capacity_wait(
            &f.db,
            &f.task,
            crate::placement::CapacityWait::Disk,
        )
        .await
        .unwrap();
    });
    case!("disposition-no-dependency", "in_progress", f, {
        crate::deferred_dispatch::record_dispatch_disposition_naming(
            &f.db,
            &f.task,
            "dependency_wait",
            "wait",
            &[],
        )
        .await
        .unwrap();
    });
}

#[tokio::test]
async fn placement_writers_match_main() {
    for (name, codes, checks) in [
        (
            "placement",
            vec![api_types::PlacementFilterCode::EnvironmentUnverified],
            vec!["check".into()],
        ),
        (
            "placement-empty-checks",
            vec![api_types::PlacementFilterCode::EnvironmentUnverified],
            vec![],
        ),
        (
            "placement-provision-failed",
            vec![api_types::PlacementFilterCode::ProvisionFailed],
            vec![],
        ),
    ] {
        case!(name, "in_progress", f, {
            let repo = db::ProjectRepo::get_by_id(&*f.db, &f.task.project_id)
                .await
                .unwrap()
                .unwrap()
                .primary_repo_id
                .unwrap();
            let error =
                ServiceError::PlacementUnavailable(crate::placement::PlacementUnavailable {
                    task_id: f.task.id.clone(),
                    repo_id: repo,
                    rejected_candidates: vec![crate::placement::CandidateRejection {
                        failing_checks: checks,
                        repo_location_id: "location".into(),
                        owner_kind: "daemon".into(),
                        daemon_id: Some("daemon".into()),
                        runtime_id: Some("runtime".into()),
                        filter_codes: codes,
                    }],
                });
            f.ctx
                .task_service
                .record_placement_dispatch_refusal(&f.task, &error)
                .await
                .unwrap();
            // All placement_refusal marker writes share this producer; cover
            // its unverified, empty-check and provisioning-failure branches.
            let stored = f.read().await;
            let metadata: Value =
                serde_json::from_str(stored.metadata_json.as_deref().unwrap()).unwrap();
            let annotation = metadata["placement_refusal"]["annotation"]
                .as_object()
                .expect("the production writer stored a placement_refusal marker");
            assert!(
                !annotation.contains_key("blocking_reason"),
                "{name}: production markers must omit the restore-only reason"
            );
        });
    }
    case!(
        "admission-owner-wait",
        "in_progress",
        f,
        SnapshotOptions {
            metadata_times: WALL_CLOCK_FIELDS,
            ..Default::default()
        },
        {
            f.ctx
                .task_service
                .defer_placement_refusal(
                    &f.task,
                    &ServiceError::DaemonUnavailable {
                        daemon_id: "daemon".into(),
                    },
                )
                .await
                .unwrap();
        }
    );
    case!(
        "admission-expired",
        "in_progress",
        f,
        SnapshotOptions {
            metadata_times: WALL_CLOCK_FIELDS,
            condition_times: &[
                ("/blocked_at", "fixture-admission-time"),
                ("/created_at", "fixture-admission-time")
            ],
            ..Default::default()
        },
        {
            TaskRepo::mutate_metadata(
                &*f.db,
                &f.task.id,
                None,
                vec![db::TaskMetadataMutation::Set {
                    key: "owner_wait".into(),
                    value: json!({"daemon_id":"daemon","started_at":"2000-01-01T00:00:00+00:00"}),
                }],
                &db::now_rfc3339(),
            )
            .await
            .unwrap();
            f.ctx
                .task_service
                .defer_placement_refusal(
                    &f.read().await,
                    &ServiceError::DaemonUnavailable {
                        daemon_id: "daemon".into(),
                    },
                )
                .await
                .unwrap();
            let row = f.read().await;
            let metadata: Value =
                serde_json::from_str(row.metadata_json.as_deref().unwrap()).unwrap();
            assert_eq!(
                metadata["owner_wait"]["started_at"],
                "2000-01-01T00:00:00+00:00"
            );
        }
    );
}

#[tokio::test]
async fn remote_cancel_writer_matches_main() {
    case!("remote-cancel", "in_progress", f, {
        let execution = f.execution(db::ExecutionStatus::Failed).await;
        f.ctx
            .task_service
            .block_task_after_executor_failure(&f.task, &execution)
            .await
            .unwrap();
        let repo_id = db::ProjectRepo::get_by_id(&*f.db, &f.task.project_id)
            .await
            .unwrap()
            .unwrap()
            .primary_repo_id
            .unwrap();
        db::WorkspaceRepo::create(
            &*f.db,
            db::CreateWorkspace {
                id: "historical-workspace".into(),
                task_id: f.task.id.clone(),
                repo_id,
                worktree_path: "workspace".into(),
                branch: "branch".into(),
                status: db::WorkspaceStatus::Cleaned,
                before_sha: None,
                created_at: db::now_rfc3339(),
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('daemon','daemon','machine','linux','x86_64','offline',?,?)").bind(db::now_rfc3339()).bind(db::now_rfc3339()).execute(f.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO pending_remote_cancel VALUES ('operation','step','historical-workspace','placement','daemon','runtime',1,0,?)").bind(db::now_rfc3339()).execute(f.db.pool()).await.unwrap();
        let current = f.read().await;
        f.ctx
            .task_service
            .perform_task_action(
                &f.task.id,
                api_types::TaskAction::Restart { reason: None },
                current.version,
            )
            .await
            .unwrap();
    });
}

#[tokio::test]
async fn carried_superseded_objects_keep_deep_existing_values() {
    let f = Fixture::new("in_progress").await;
    let mut value = json!([1, true, null]);
    for _ in 0..40 {
        value = json!({"caller_extension": value});
    }
    db::task_writer::in_task_step(f.step.clone(), async {
        let row = f
            .ctx
            .task_service
            .write_dependency_block(
                &f.task,
                &["dependency".into()],
                Some(value.as_object().unwrap().clone()),
            )
            .await
            .unwrap();
        let blocked: Value = serde_json::from_str(row.blocked_json.as_deref().unwrap()).unwrap();
        assert_eq!(blocked["details"]["superseded"], value);
        after(&f, "dependency-deep-carried", SnapshotOptions::default()).await;
    })
    .await;
}

#[tokio::test]
async fn refresh_pause_without_generation_matches_main() {
    case!("paused-refresh-without-generation", "merging", f, {
        TaskRepo::mutate_metadata(
            &*f.db,
            &f.task.id,
            None,
            vec![db::TaskMetadataMutation::Set {
                key: "paused_integration".into(),
                value: json!({"state":"merging","deferred_at":"2026-10-25T00:00:00Z"}),
            }],
            &db::now_rfc3339(),
        )
        .await
        .unwrap();
        crate::deferred_dispatch::refresh_paused_integration_for_pause(&f.db, &f.read().await)
            .await
            .unwrap();
    });
}
