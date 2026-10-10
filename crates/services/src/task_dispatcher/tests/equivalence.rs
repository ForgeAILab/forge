//! Real-database scheduler equivalence against `9d9b228f`.
//!
//! This file compiles unchanged on the base commit. There it records what the
//! scanning dispatcher did with each shape (`SCHEDULER_EQUIVALENCE_RECORD=<dir>`);
//! here it replays the same rows through the reconciling dispatcher and compares
//! the outcome with that checked-in recording: dispatched or held, the target,
//! and every legacy field and event written. The only differences accepted are
//! the `@<Exception>` lines of the fixture, each naming one allowed change.
use super::*;
use std::collections::HashMap;

struct Ctx {
    db: Arc<db::SqliteDb>,
    pid: String,
    a1: String,
    a2: String,
    dispatcher: TaskDispatcher,
    rx: mpsc::UnboundedReceiver<ExecutionContext>,
    events: tokio::sync::broadcast::Receiver<events::ForgeEvent>,
    _repo: TempDir,
    _ws: TempDir,
}

async fn seed_second_agent(db: &db::SqliteDb, max: i64) -> String {
    // Same embedded machine as seed_agent, second Agent identity.
    seed_agent_with_executor(db, max, DaemonStatus::Online, AgentStatus::Idle, "shell").await
}

async fn ctx(prime: bool) -> Ctx {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let ws = TempDir::new().unwrap();
    let (pid, _) = seed_project_repo(&db, repo.path()).await;
    let a1 = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let a2 = seed_second_agent(&db, 3).await;
    let (dispatcher, rx) = build_dispatcher(Arc::clone(&db), ws.path()).await;
    let events = dispatcher.event_bus.subscribe();
    if prime {
        let _ = dispatcher.check_once_and_drain().await;
        let _ = dispatcher.check_once_and_drain().await;
    }
    Ctx {
        db,
        pid,
        a1,
        a2,
        dispatcher,
        rx,
        events,
        _repo: repo,
        _ws: ws,
    }
}

async fn raw(c: &Ctx, id: &str, set: &str) {
    let sql = format!("UPDATE task SET {set} WHERE id = ?");
    sqlx::query(&sql)
        .bind(id)
        .execute(c.db.pool())
        .await
        .unwrap_or_else(|e| panic!("raw {set}: {e}"));
    let mut tx = db::begin_immediate(c.db.pool()).await.unwrap();
    c.db.sync_condition_in_tx(&mut tx, id).await.unwrap();
    tx.commit().await.unwrap();
}
async fn meta(c: &Ctx, id: &str, v: serde_json::Value) {
    sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
        .bind(v.to_string())
        .bind(id)
        .execute(c.db.pool())
        .await
        .unwrap();
    let mut tx = db::begin_immediate(c.db.pool()).await.unwrap();
    c.db.sync_condition_in_tx(&mut tx, id).await.unwrap();
    tx.commit().await.unwrap();
}
async fn ann(c: &Ctx, id: &str, text: &str) {
    sqlx::query("UPDATE task SET error_annotation = ?, version = version + 1 WHERE id = ?")
        .bind(text)
        .bind(id)
        .execute(c.db.pool())
        .await
        .unwrap();
    let mut tx = db::begin_immediate(c.db.pool()).await.unwrap();
    c.db.sync_condition_in_tx(&mut tx, id).await.unwrap();
    tx.commit().await.unwrap();
}
async fn task(c: &Ctx, title: &str, status: &str) -> Task {
    let t = seed_task(&c.db, &c.pid, title, status, 0).await;
    assign_role(&c.db, &t.id, "coder", &c.a1).await;
    t
}
async fn dep(c: &Ctx, t: &str, on: &str) {
    sqlx::query("INSERT INTO task_dependency VALUES (?,?,?)")
        .bind(t)
        .bind(on)
        .bind(now_rfc3339())
        .execute(c.db.pool())
        .await
        .unwrap();
}
fn ago(minutes: i64) -> String {
    (chrono::Utc::now() - chrono::Duration::minutes(minutes)).to_rfc3339()
}

const SHAPES: &[&str] = &[
    "todo_coder",
    "todo_unassigned",
    "todo_defaults",
    "todo_user_assignee",
    "dep_unfinished",
    "dep_done",
    "dep_cancelled",
    "children_seq",
    "children_first_done",
    "root_children_complete",
    "root_flag_children_incomplete",
    "root_flag_no_children",
    "active_noexec",
    "active_running",
    "active_completed_unsettled",
    "active_failed",
    "active_cancelled_manual",
    "active_cancelled_auto",
    "active_cancelled_nopolicy",
    "review_reviewer",
    "review_ci_no_review",
    "review_no_reviewer",
    "blocked_json_active",
    "failed_json_active",
    "barrier_running_active",
    "barrier_blocked_active",
    "blocked_json_todo",
    "barrier_todo",
    "ann_manual_stop",
    "ann_workflow_loop",
    "ann_cascade_failed",
    "ann_workspace_error",
    "ann_agent_timeout",
    "ann_recovery_required",
    "ann_workspace_reset_required",
    "ann_max_turns_exceeded",
    "ann_before_work_hook_failed",
    "ann_before_work_hook_timeout",
    "ann_review_needs_owner",
    "ann_dispatch_failed",
    "ann_dispatch_failed_todo",
    "ann_merge_conflict",
    "ann_ci_failed",
    "ann_empty_object",
    "ann_malformed",
    "held",
    "deferred_future",
    "deferred_past",
    "deferred_future_todo",
    "disposition_current",
    "disposition_stale",
    "disposition_current_todo",
    "disposition_acked",
    "disposition_acked_todo",
    "queued_recovery_malformed",
    "paused_integration",
    "owner_wait_fresh",
    "owner_wait_expired",
    "env_wait_unverified",
    "env_wait_provision_failed",
    "env_wait_probe",
    "placement_refusal",
    "publication_claim_orphan",
    "publication_claim_malformed",
    "publication_cleanup_orphan",
    "malformed_metadata_active",
    "malformed_metadata_todo",
    "project_paused",
    "project_limit",
    "agent_paused",
    "agent_full",
    "agent_offline",
    "machine_cap",
    "merging_no_hooks",
    "merging_no_hooks_witness",
    "custom_state_unknown",
    "awaiting_human_active",
    "awaiting_human_planning",
    "plan_review_wait",
    "backlog",
    "done_task",
    "todo_archived",
    "merge_failed_state",
    "two_todo_priority",
    "queued_recovery_valid_waiting",
    "queued_recovery_valid_free",
    "remote_cancel_fence_active",
    "remote_cancel_fence_todo",
    "merging_custom_hooks",
    "merging_custom_hooks_witness",
    "owner_offline_admitted_limit",
    "review_needs_owner_limit",
    "active_no_role",
    "review_needs_owner_review",
    "root_unknown_state_children",
    "active_user_assignee",
    "review_user_reviewer",
];

async fn apply(name: &str, c: &Ctx) {
    match name {
        "todo_coder" => {
            task(c, "T", "todo").await;
        }
        "todo_unassigned" => {
            seed_task(&c.db, &c.pid, "T", "todo", 0).await;
        }
        "todo_defaults" => {
            let p = ProjectRepo::get_by_id(&*c.db, &c.pid)
                .await
                .unwrap()
                .unwrap();
            ProjectRepo::update_at_version(
                &*c.db,
                UpdateProject {
                    id: c.pid.clone(),
                    name: None,
                    settings: Some(
                        serde_json::json!({"default_role_assignments":[{"role_name":"coder","assignee_type":"agent","assignee_id":c.a1}]})
                            .to_string(),
                    ),
                    primary_repo_id: None,
                    paused_at: None,
                    updated_at: now_rfc3339(),
                },
                p.version,
                None,
            )
            .await
            .unwrap();
            seed_task(&c.db, &c.pid, "T", "todo", 0).await;
        }
        "todo_user_assignee" => {
            let t = seed_task(&c.db, &c.pid, "T", "todo", 0).await;
            sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES (?,?,'coder','user','user-1',?,?)")
                .bind(new_uuid_v4()).bind(&t.id).bind(now_rfc3339()).bind(now_rfc3339())
                .execute(c.db.pool()).await.unwrap();
        }
        "dep_unfinished" | "dep_done" | "dep_cancelled" => {
            let d = seed_task(
                &c.db,
                &c.pid,
                "D",
                match name {
                    "dep_done" => "done",
                    "dep_cancelled" => "cancelled",
                    _ => "todo",
                },
                0,
            )
            .await;
            let t = task(c, "T", "todo").await;
            dep(c, &t.id, &d.id).await;
        }
        "children_seq"
        | "children_first_done"
        | "root_children_complete"
        | "root_flag_children_incomplete" => {
            let root_status = if name == "children_seq" {
                "todo"
            } else {
                "in_progress"
            };
            let r = task(c, "R", root_status).await;
            let (s1, s2) = match name {
                "children_first_done" => ("done", "todo"),
                "root_children_complete" => ("done", "done"),
                _ => ("todo", "todo"),
            };
            let c1 = seed_subtask(&c.db, &r, "C1", s1, 0).await;
            let c2 = seed_subtask(&c.db, &r, "C2", s2, 1).await;
            assign_role(&c.db, &c1.id, "coder", &c.a1).await;
            assign_role(&c.db, &c2.id, "coder", &c.a1).await;
            if name == "root_flag_children_incomplete" {
                meta(
                    c,
                    &r.id,
                    serde_json::json!({"coordination_review_pending":true}),
                )
                .await;
            }
        }
        "root_flag_no_children" => {
            let t = task(c, "T", "in_progress").await;
            meta(
                c,
                &t.id,
                serde_json::json!({"coordination_review_pending":true}),
            )
            .await;
        }
        "active_noexec" => {
            task(c, "T", "in_progress").await;
        }
        "active_running" => {
            let t = task(c, "T", "in_progress").await;
            seed_running_execution(&c.db, &t.id, &c.a1, "coder").await;
        }
        "active_completed_unsettled" => {
            let t = task(c, "T", "in_progress").await;
            seed_completed_coder_execution(&c.db, &t.id).await;
        }
        "active_failed" => {
            let t = task(c, "T", "in_progress").await;
            seed_failed_coder_execution(&c.db, &t.id, &c.pid, &c.a1).await;
        }
        "active_cancelled_manual" | "active_cancelled_auto" | "active_cancelled_nopolicy" => {
            let t = task(c, "T", "in_progress").await;
            seed_cancelled_execution(
                &c.db,
                &t.id,
                &c.a1,
                "coder",
                None,
                match name {
                    "active_cancelled_manual" => Some(ResumePolicy::Manual),
                    "active_cancelled_auto" => Some(ResumePolicy::Auto),
                    _ => None,
                },
            )
            .await;
        }
        "review_reviewer" => {
            let t = task(c, "T", "review").await;
            assign_role(&c.db, &t.id, "reviewer", &c.a2).await;
        }
        "review_ci_no_review" => {
            let t = task(c, "T", "review").await;
            assign_role(&c.db, &t.id, "reviewer", &c.a2).await;
            set_review_ci_config(&c.db, &t).await;
        }
        "review_no_reviewer" => {
            task(c, "T", "review").await;
        }
        "blocked_json_active" => {
            let t = task(c, "T", "in_progress").await;
            raw(
                c,
                &t.id,
                r#"blocked_json = '{"kind":"other","reason":"x"}', version = version + 1"#,
            )
            .await;
        }
        "failed_json_active" => {
            let t = task(c, "T", "in_progress").await;
            raw(
                c,
                &t.id,
                r#"failed_json = '{"kind":"other","reason":"x"}', version = version + 1"#,
            )
            .await;
        }
        "barrier_running_active" => {
            let t = task(c, "T", "in_progress").await;
            raw(
                c,
                &t.id,
                r#"entry_barrier_json = '{"status":"running"}', version = version + 1"#,
            )
            .await;
        }
        "barrier_blocked_active" => {
            let t = task(c, "T", "in_progress").await;
            raw(
                c,
                &t.id,
                r#"entry_barrier_json = '{"status":"blocked"}', version = version + 1"#,
            )
            .await;
        }
        "blocked_json_todo" => {
            let t = task(c, "T", "todo").await;
            raw(
                c,
                &t.id,
                r#"blocked_json = '{"kind":"other","reason":"x"}', version = version + 1"#,
            )
            .await;
        }
        "barrier_todo" => {
            let t = task(c, "T", "todo").await;
            raw(
                c,
                &t.id,
                r#"entry_barrier_json = '{"status":"running"}', version = version + 1"#,
            )
            .await;
        }
        n if n.starts_with("ann_") => {
            let status = if n.ends_with("_todo") {
                "todo"
            } else {
                "in_progress"
            };
            let t = task(c, "T", status).await;
            let kind = n.trim_start_matches("ann_").trim_end_matches("_todo");
            let text = match kind {
                "empty_object" => "{}".to_owned(),
                "malformed" => "{not json".to_owned(),
                k => serde_json::json!({"type":k,"message":"m"}).to_string(),
            };
            ann(c, &t.id, &text).await;
        }
        "held" => {
            let t = task(c, "T", "in_progress").await;
            sqlx::query("UPDATE task SET error_annotation = ?, blocked_json = ?, version = version + 1 WHERE id = ?")
                .bind(serde_json::json!({"type":"manual_stop","blocking_reason":"hold","blocked_by":"user"}).to_string())
                .bind(serde_json::json!({"kind":"manual_stop","reason":"hold","created_at":now_rfc3339()}).to_string())
                .bind(&t.id).execute(c.db.pool()).await.unwrap();
            raw(c, &t.id, "title = title").await;
        }
        "deferred_future" | "deferred_past" | "deferred_future_todo" => {
            let status = if name.ends_with("_todo") {
                "todo"
            } else {
                "in_progress"
            };
            let t = task(c, "T", status).await;
            let at = if name == "deferred_past" {
                ago(5)
            } else {
                ago(-30)
            };
            meta(c, &t.id, serde_json::json!({"deferred_dispatch":{"not_before":at,"reason":"backoff","target_state":"in_progress"}})).await;
        }
        "disposition_current"
        | "disposition_stale"
        | "disposition_current_todo"
        | "disposition_acked"
        | "disposition_acked_todo" => {
            let status = if name.ends_with("_todo") {
                "todo"
            } else {
                "in_progress"
            };
            let t = task(c, "T", status).await;
            let capability = if status == "todo" {
                "coder"
            } else {
                "in_progress"
            };
            deferred_dispatch::record_dispatch_disposition(&c.db, &t, capability, "refused: probe")
                .await
                .unwrap();
            if name.starts_with("disposition_acked") {
                let _ = sqlx::query("UPDATE task_schedule_dirty SET external=0 WHERE task_id=?")
                    .bind(&t.id)
                    .execute(c.db.pool())
                    .await;
            }
            if name == "disposition_stale" {
                raw(c, &t.id, "version = version + 1").await;
            }
        }
        "queued_recovery_malformed" => {
            let t = task(c, "T", "in_progress").await;
            meta(c, &t.id, serde_json::json!({"queued_recovery":{"id":"x"}})).await;
        }
        "paused_integration" => {
            let t = task(c, "T", "merging").await;
            meta(
                c,
                &t.id,
                serde_json::json!({"paused_integration":{"state":"merging","deferred_at":ago(1)}}),
            )
            .await;
        }
        "owner_wait_fresh" | "owner_wait_expired" => {
            let t = task(c, "T", "in_progress").await;
            let daemon: String = sqlx::query_scalar("SELECT id FROM daemon LIMIT 1")
                .fetch_one(c.db.pool())
                .await
                .unwrap();
            let started = if name == "owner_wait_expired" {
                ago(60 * 48)
            } else {
                ago(0)
            };
            meta(
                c,
                &t.id,
                serde_json::json!({"owner_wait":{"daemon_id":daemon,"started_at":started}}),
            )
            .await;
        }
        "env_wait_unverified" | "env_wait_provision_failed" | "env_wait_probe" => {
            let t = task(c, "T", "in_progress").await;
            let kind = match name {
                "env_wait_unverified" => "environment_unverified",
                "env_wait_provision_failed" => "provision_failed",
                _ => "probe_pending",
            };
            meta(c, &t.id, serde_json::json!({"environment_wait":{"kind":kind,"machine":{"owner_kind":"server","daemon_id":"","runtime_id":""}}})).await;
        }
        "placement_refusal" => {
            let t = task(c, "T", "in_progress").await;
            meta(
                c,
                &t.id,
                serde_json::json!({"placement_refusal":{"code":"no_machine","message":"m"}}),
            )
            .await;
        }
        "publication_claim_orphan" => {
            let t = task(c, "T", "planning").await;
            meta(c, &t.id, serde_json::json!({"plan_publication_claim":{"execution_id":"missing-exec","state":"planning","project_version":1,"state_entry_token":null}})).await;
        }
        "publication_claim_malformed" => {
            let t = task(c, "T", "planning").await;
            meta(
                c,
                &t.id,
                serde_json::json!({"plan_publication_claim":"zzz"}),
            )
            .await;
        }
        "publication_cleanup_orphan" => {
            let t = task(c, "T", "in_progress").await;
            meta(c, &t.id, serde_json::json!({"plan_publication_cleanup":{"execution_id":"missing-exec","state":"planning","project_version":1,"state_entry_token":null}})).await;
        }
        "malformed_metadata_active" | "malformed_metadata_todo" => {
            let t = task(
                c,
                "T",
                if name.ends_with("todo") {
                    "todo"
                } else {
                    "in_progress"
                },
            )
            .await;
            raw(c, &t.id, "metadata_json = '{not json'").await;
        }
        "project_paused" => {
            task(c, "T", "todo").await;
            task(c, "U", "in_progress").await;
            sqlx::query("UPDATE project SET paused_at = ?, version = version + 1 WHERE id = ?")
                .bind(now_rfc3339())
                .bind(&c.pid)
                .execute(c.db.pool())
                .await
                .unwrap();
        }
        "project_limit" => {
            let r = task(c, "RUN", "in_progress").await;
            seed_running_execution(&c.db, &r.id, &c.a1, "coder").await;
            task(c, "T", "todo").await;
            task(c, "U", "in_progress").await;
            set_project_active_task_limit(&c.db, &c.pid, 1).await;
        }
        "agent_paused" => {
            task(c, "T", "todo").await;
            task(c, "U", "in_progress").await;
            sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
                .bind(&c.a1)
                .execute(c.db.pool())
                .await
                .unwrap();
        }
        "agent_full" => {
            let full = seed_agent(&c.db, 0, DaemonStatus::Online, AgentStatus::Idle).await;
            let r = seed_task(&c.db, &c.pid, "RUN", "in_progress", 0).await;
            assign_role(&c.db, &r.id, "coder", &full).await;
            seed_running_execution(&c.db, &r.id, &full, "coder").await;
            let t = seed_task(&c.db, &c.pid, "T", "todo", 0).await;
            assign_role(&c.db, &t.id, "coder", &full).await;
            let u = seed_task(&c.db, &c.pid, "U", "in_progress", 0).await;
            assign_role(&c.db, &u.id, "coder", &full).await;
        }
        "agent_offline" => {
            let off = seed_agent(&c.db, 2, DaemonStatus::Offline, AgentStatus::Offline).await;
            let t = seed_task(&c.db, &c.pid, "T", "todo", 0).await;
            assign_role(&c.db, &t.id, "coder", &off).await;
            let u = seed_task(&c.db, &c.pid, "U", "in_progress", 0).await;
            assign_role(&c.db, &u.id, "coder", &off).await;
        }
        "machine_cap" => {
            let r = task(c, "RUN", "in_progress").await;
            seed_running_execution(&c.db, &r.id, &c.a1, "coder").await;
            task(c, "T", "todo").await;
            task(c, "U", "in_progress").await;
            c.db.server_run_cap.set(
                Some(1),
                config::resolved_run_cap(Some(1)),
                &config::embedded_machine_id(),
            );
        }
        "merging_no_hooks" => {
            task(c, "T", "merging").await;
        }
        "merging_no_hooks_witness" => {
            let t = task(c, "T", "merging").await;
            TransitionLogRepo::insert(
                &*c.db,
                db::CreateTransitionLog {
                    id: new_uuid_v4(),
                    task_id: t.id.clone(),
                    from_state: "review".to_owned(),
                    to_state: "merging".to_owned(),
                    trigger_name: None,
                    triggered_by: Actor::system(SystemComponent::Workflow).display(),
                    bridge: Default::default(),
                    trigger_reason: "review passed".to_owned(),
                    hook_results_json: None,
                    rejection: false,
                    created_at: ago(10),
                },
            )
            .await
            .unwrap();
        }
        "custom_state_unknown" => {
            task(c, "T", "weird_state").await;
        }
        "awaiting_human_active" => {
            let t = task(c, "T", "in_progress").await;
            meta(
                c,
                &t.id,
                serde_json::json!({"awaiting_human":true,"awaiting_human_reason":"needs input"}),
            )
            .await;
        }
        "awaiting_human_planning" => {
            let t = task(c, "T", "planning").await;
            meta(
                c,
                &t.id,
                serde_json::json!({"awaiting_human":true,"awaiting_human_reason":"needs input"}),
            )
            .await;
        }
        "plan_review_wait" => {
            let t = task(c, "T", "planning").await;
            assign_role(&c.db, &t.id, "planner", &c.a2).await;
            meta(c, &t.id, serde_json::json!({"awaiting_human":true,"awaiting_human_reason":"plan_review","planning_execution_id":"missing"})).await;
        }
        "backlog" => {
            task(c, "T", "backlog").await;
        }
        "done_task" => {
            task(c, "T", "done").await;
        }
        "todo_archived" => {
            let t = task(c, "T", "todo").await;
            raw(c, &t.id, "archived_at = updated_at, version = version + 1").await;
        }
        "merge_failed_state" => {
            task(c, "T", "merge_failed").await;
        }
        "two_todo_priority" => {
            let low = seed_task(&c.db, &c.pid, "LOW", "todo", 1).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            let high = seed_task(&c.db, &c.pid, "HIGH", "todo", 10).await;
            assign_role(&c.db, &low.id, "coder", &c.a1).await;
            assign_role(&c.db, &high.id, "coder", &c.a1).await;
        }
        "queued_recovery_valid_waiting" | "queued_recovery_valid_free" => {
            let busy = task(c, "RUN", "in_progress").await;
            seed_running_execution(&c.db, &busy.id, &c.a1, "coder").await;
            c.db.server_run_cap
                .set(Some(1), 1, &config::embedded_machine_id());
            let t = task(c, "T", "in_progress").await;
            seed_cancelled_execution(
                &c.db,
                &t.id,
                &c.a1,
                "coder",
                Some(StopReason::UserCancelled),
                Some(ResumePolicy::Manual),
            )
            .await;
            let t = TaskRepo::update(
                &*c.db,
                UpdateTask {
                    id: t.id.clone(),
                    expected_version: t.version,
                    title: None,
                    description: None,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    error_annotation: Some(Some(
                        serde_json::json!({"type":"recovery_required","blocking_reason":"crash_recovery","blocked_execution_id":null,"recovery_actions":["reexecute"]})
                            .to_string(),
                    )),
                    blocked_json: Some(Some(
                        serde_json::json!({"kind":"recovery_required","reason":"crash_recovery"}).to_string(),
                    )),
                    failed_json: None,
                    task_state_config: None,
                    parent_task_id: None,
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
            set_prompt_execution_snapshots(&c.db, &c.a1).await;
            let queued = Box::pin(c.dispatcher.task_service.perform_task_action(
                &t.id,
                api_types::TaskAction::Retry {
                    reason: Some("operator retry".to_owned()),
                    fresh_session: Some(true),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                t.version,
            ))
            .await
            .unwrap()
            .task;
            assert!(deferred_dispatch::queued_recovery(&queued).is_some());
            if name == "queued_recovery_valid_free" {
                c.db.server_run_cap
                    .set(Some(4), 4, &config::embedded_machine_id());
            }
        }
        "remote_cancel_fence_active" | "remote_cancel_fence_todo" => {
            let t = task(
                c,
                "T",
                if name.ends_with("todo") {
                    "todo"
                } else {
                    "in_progress"
                },
            )
            .await;
            let repo: String = sqlx::query_scalar("SELECT id FROM repo LIMIT 1")
                .fetch_one(c.db.pool())
                .await
                .unwrap();
            let workspace = WorkspaceRepo::create(
                &*c.db,
                CreateWorkspace {
                    id: new_uuid_v4(),
                    task_id: t.id.clone(),
                    repo_id: repo,
                    worktree_path: c._ws.path().join("fenced").to_string_lossy().into_owned(),
                    branch: "task/fenced".to_owned(),
                    status: WorkspaceStatus::Ready,
                    before_sha: None,
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
            sqlx::query("INSERT INTO pending_remote_cancel(operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,created_at) VALUES ('op','step',?,'placement','remote-machine','runtime',1,0,?)")
                .bind(&workspace.id)
                .bind(now_rfc3339())
                .execute(c.db.pool())
                .await
                .unwrap();
            raw(c, &t.id, "title = title").await;
        }
        "merging_custom_hooks" | "merging_custom_hooks_witness" => {
            let mut workflow = WorkflowEngine::resolve_workflow("{}");
            let merging = workflow
                .states
                .iter_mut()
                .find(|s| s.name == "merging")
                .unwrap();
            let mut custom = merging.hooks.on_enter[0].clone();
            custom.action = "notify_release_channel".to_owned();
            merging.hooks.on_enter.insert(0, custom);
            sqlx::query(
                "UPDATE project SET workflow_definition = ?, version = version + 1 WHERE id = ?",
            )
            .bind(serde_json::to_string(&workflow).unwrap())
            .bind(&c.pid)
            .execute(c.db.pool())
            .await
            .unwrap();
            let t = task(c, "T", "merging").await;
            if name.ends_with("witness") {
                TransitionLogRepo::insert(
                    &*c.db,
                    db::CreateTransitionLog {
                        id: new_uuid_v4(),
                        task_id: t.id.clone(),
                        from_state: "review".to_owned(),
                        to_state: "merging".to_owned(),
                        trigger_name: None,
                        triggered_by: Actor::system(SystemComponent::Workflow).display(),
                        bridge: Default::default(),
                        trigger_reason: "review passed".to_owned(),
                        hook_results_json: None,
                        rejection: false,
                        created_at: ago(10),
                    },
                )
                .await
                .unwrap();
            }
        }
        "owner_offline_admitted_limit" => {
            // An admitted owner-offline wait keeps its active slot: with a
            // limit of one, nothing else is admitted behind it.
            let t = task(c, "OWNER", "in_progress").await;
            let daemon: String = sqlx::query_scalar("SELECT id FROM daemon LIMIT 1")
                .fetch_one(c.db.pool())
                .await
                .unwrap();
            meta(
                c,
                &t.id,
                serde_json::json!({"owner_wait":{"daemon_id":daemon,"started_at":ago(0)}}),
            )
            .await;
            task(c, "T", "todo").await;
            set_project_active_task_limit(&c.db, &c.pid, 1).await;
        }
        "review_needs_owner_limit" => {
            // A Review waiting on its owner holds a parked slot, not an
            // active one: the next Task is admitted, the one after is not.
            let t = task(c, "OWNER", "review").await;
            assign_role(&c.db, &t.id, "reviewer", &c.a2).await;
            let t = TaskRepo::get_by_id(&*c.db, &t.id, false)
                .await
                .unwrap()
                .unwrap();
            set_task_error_annotation(
                &c.db,
                &t,
                r#"{"type":"review_needs_owner","message":"needs the owner"}"#,
            )
            .await;
            task(c, "T", "todo").await;
            tokio::time::sleep(Duration::from_millis(5)).await;
            task(c, "U", "todo").await;
            set_project_active_task_limit(&c.db, &c.pid, 1).await;
        }
        "review_needs_owner_review" => {
            let t = task(c, "T", "review").await;
            assign_role(&c.db, &t.id, "reviewer", &c.a2).await;
            let t = TaskRepo::get_by_id(&*c.db, &t.id, false)
                .await
                .unwrap()
                .unwrap();
            set_task_error_annotation(
                &c.db,
                &t,
                r#"{"type":"review_needs_owner","message":"needs the owner"}"#,
            )
            .await;
        }
        "active_no_role" => {
            seed_task(&c.db, &c.pid, "T", "in_progress", 0).await;
        }
        "root_unknown_state_children" => {
            // A coordination root in a state the workflow does not define.
            let r = task(c, "R", "weird_state").await;
            let c1 = seed_subtask(&c.db, &r, "C1", "todo", 0).await;
            let c2 = seed_subtask(&c.db, &r, "C2", "todo", 1).await;
            assign_role(&c.db, &c1.id, "coder", &c.a1).await;
            assign_role(&c.db, &c2.id, "coder", &c.a1).await;
        }
        "active_user_assignee" | "review_user_reviewer" => {
            // A person holds the state's role.
            let (status, role) = if name == "active_user_assignee" {
                ("in_progress", "coder")
            } else {
                ("review", "reviewer")
            };
            let t = if role == "coder" {
                seed_task(&c.db, &c.pid, "T", status, 0).await
            } else {
                task(c, "T", status).await
            };
            sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES (?,?,?,'user','user-1',?,?)")
                .bind(new_uuid_v4()).bind(&t.id).bind(role).bind(now_rfc3339()).bind(now_rfc3339())
                .execute(c.db.pool()).await.unwrap();
        }
        other => panic!("unknown shape {other}"),
    }
}

struct Snap {
    status: String,
    version: i64,
    line: String,
}

/// Row ids, clocks and temporary paths differ per run; everything else in a
/// stored message is compared verbatim.
fn scrub(text: &str) -> String {
    let mut out = Vec::new();
    for word in text.split(' ') {
        let core = word.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '/' && c != '-');
        let uuid = core.len() == 36
            && core.bytes().enumerate().all(|(i, b)| match i {
                8 | 13 | 18 | 23 => b == b'-',
                _ => b.is_ascii_hexdigit(),
            });
        let stamp = core.len() >= 20
            && core.as_bytes()[4] == b'-'
            && core.as_bytes()[10] == b'T'
            && core[..4].bytes().all(|b| b.is_ascii_digit());
        out.push(if uuid {
            word.replace(core, "<id>")
        } else if stamp {
            word.replace(core, "<time>")
        } else if core.starts_with('/') {
            word.replace(core, "<path>")
        } else {
            word.to_owned()
        });
    }
    out.join(" ")
}

fn clip(text: &str) -> String {
    scrub(text).chars().take(200).collect()
}

async fn snapshot(
    db: &db::SqliteDb,
    pid: &str,
    agents: &HashMap<String, String>,
) -> Vec<(String, Snap)> {
    use sqlx::Row;
    let rows = sqlx::query("SELECT id,title,status,version,error_annotation,blocked_json,failed_json,entry_barrier_json,metadata_json FROM task WHERE project_id=? ORDER BY title,id")
        .bind(pid).fetch_all(db.pool()).await.unwrap();
    let mut out = Vec::new();
    for row in rows {
        let id: String = row.get("id");
        let title: String = row.get("title");
        let status: String = row.get("status");
        let version: i64 = row.get("version");
        let annotation: Option<String> = row.get("error_annotation");
        let ann = annotation.as_deref().map(|a| {
            serde_json::from_str::<serde_json::Value>(a)
                .ok()
                .map(|v| {
                    format!(
                        "{}/{}/{}",
                        v["type"].as_str().unwrap_or("?"),
                        v["blocking_reason"].as_str().unwrap_or("-"),
                        clip(v["message"].as_str().unwrap_or("-")),
                    )
                })
                .unwrap_or_else(|| "unparsable".into())
        });
        let kind = |raw: Option<String>| {
            raw.map(|raw| {
                serde_json::from_str::<serde_json::Value>(&raw)
                    .ok()
                    .map(|v| {
                        format!(
                            "{}/{}",
                            v["kind"].as_str().or(v["status"].as_str()).unwrap_or("?"),
                            clip(v["reason"].as_str().unwrap_or("-"))
                        )
                    })
                    .unwrap_or_else(|| "unparsable".into())
            })
            .unwrap_or_else(|| "-".into())
        };
        let b = kind(row.get("blocked_json"));
        let f = kind(row.get("failed_json"));
        let e = kind(row.get("entry_barrier_json"));
        let m: Option<String> = row.get("metadata_json");
        let (keys, disp) = match m.as_deref().map(serde_json::from_str::<serde_json::Value>) {
            None => ("-".to_owned(), "-".to_owned()),
            Some(Err(_)) => ("unparsable".to_owned(), "-".to_owned()),
            Some(Ok(v)) => {
                let mut k: Vec<String> = v
                    .as_object()
                    .map(|o| o.keys().cloned().collect())
                    .unwrap_or_default();
                k.sort();
                let d = v["dispatch_disposition"]["capability"]
                    .as_str()
                    .map(|c| {
                        format!(
                            "{c}:{}",
                            clip(
                                v["dispatch_disposition"]["safe_message"]
                                    .as_str()
                                    .unwrap_or("")
                            )
                        )
                    })
                    .unwrap_or_else(|| "-".into());
                (k.join(","), d)
            }
        };
        let execs = sqlx::query(
            "SELECT role,agent_id,status FROM execution WHERE task_id=? ORDER BY created_at,rowid",
        )
        .bind(&id)
        .fetch_all(db.pool())
        .await
        .unwrap();
        let execs: Vec<String> = execs
            .iter()
            .map(|x| {
                let a: Option<String> = x.get("agent_id");
                format!(
                    "{}@{}:{}",
                    x.get::<String, _>("role"),
                    a.as_deref()
                        .map(|a| agents.get(a).cloned().unwrap_or_else(|| "A?".into()))
                        .unwrap_or_else(|| "none".into()),
                    x.get::<String, _>("status")
                )
            })
            .collect();
        let hooks: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_step WHERE task_id=? AND kind='hooks'")
                .bind(&id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let line = format!(
            "status={status} ann={} B={b} F={f} E={e} meta=[{keys}] disp=[{disp}] execs=[{}] hooks={hooks}",
            ann.unwrap_or_else(|| "-".into()),
            execs.join(" "),
        );
        out.push((
            title,
            Snap {
                status,
                version,
                line,
            },
        ));
    }
    out
}

async fn agent_labels(c: &Ctx) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert(c.a1.clone(), "A1".to_owned());
    m.insert(c.a2.clone(), "A2".to_owned());
    m
}

async fn slots_line(db: &db::SqliteDb, pid: &str) -> String {
    let project = ProjectRepo::get_by_id(db, pid).await.unwrap().unwrap();
    let slots = slots::load_project_slots(db, &project).await.unwrap();
    format!(
        "active={} parked={} queued={} limit={}",
        slots.active, slots.parked, slots.queued, slots.limit
    )
}

async fn tick(d: &TaskDispatcher, seconds: u64) -> String {
    match tokio::time::timeout(
        Duration::from_secs(seconds),
        Box::pin(d.check_once_and_drain()),
    )
    .await
    {
        Ok(Ok(_)) => "ok".into(),
        Ok(Err(e)) => format!("Err({})", clip(&e.to_string())),
        Err(_) => "TIMEOUT".into(),
    }
}

async fn run_shape(mode: &str, name: &'static str, prime: bool) -> Vec<String> {
    let mut c = ctx(prime).await;
    Box::pin(apply(name, &c)).await;
    let agents = agent_labels(&c).await;
    let before = snapshot(&c.db, &c.pid, &agents).await;
    while c.events.try_recv().is_ok() {}
    let mut ticks = Vec::new();
    for _ in 0..3 {
        ticks.push(tick(&c.dispatcher, 20).await);
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    let mut spawned = 0;
    while c.rx.try_recv().is_ok() {
        spawned += 1;
    }
    let mut published: std::collections::BTreeMap<String, u32> = Default::default();
    loop {
        match c.events.try_recv() {
            Ok(event) => *published.entry(event.event_type).or_default() += 1,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                *published.entry("lagged".into()).or_default() += 1
            }
            Err(_) => break,
        }
    }
    let after = snapshot(&c.db, &c.pid, &agents).await;
    let mut out = Vec::new();
    for (title, a) in &after {
        let b = before.iter().find(|(t, _)| t == title).map(|(_, s)| s);
        out.push(format!(
            "{mode} {name} {title}: from={} dv={} {}",
            b.map(|b| b.status.as_str()).unwrap_or("?"),
            b.map(|b| a.version - b.version).unwrap_or(0),
            a.line
        ));
    }
    let failed: Vec<&String> = ticks.iter().filter(|t| *t != "ok").collect();
    out.push(format!(
        "{mode} {name} (pass): spawned={spawned} failed_ticks={failed:?} slots=[{}]",
        slots_line(&c.db, &c.pid).await
    ));
    out.push(format!("{mode} {name} (events): {published:?}"));
    out
}

/// The dispatcher's admission futures are large in a debug build, on the
/// polling thread as well as on the workers.
fn on_runtime<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send,
    Fut: std::future::Future<Output = ()>,
{
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(64 << 20)
            .spawn_scoped(scope, || {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_stack_size(64 << 20)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(test())
            })
            .unwrap()
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

const FIXTURE: &str = include_str!("fixtures/scheduler_equivalence_9d9b228f.txt");

/// The only differences from `9d9b228f` this stage may make. A fixture line
/// `@<name> <key>: <outcome>` replaces the recorded outcome for that key.
const ALLOWED: &[&str] = &[
    "ExplicitOwnerPark",
    "MissedWakeRepair",
    "IdleTickQuiescence",
    // The base counted a Task's own running execution against that Task: on
    // a full machine every running Task was given a `machine_capacity` wait
    // (or the `project_capacity` wait it was converted to). A Task that
    // holds the slot waits for none.
    "OwnSlotIsNoWait",
];

/// `OwnSlotIsNoWait` may only remove a capacity wait from a Task whose own
/// execution is running, and change nothing else.
fn own_slot_exception_is_sound(recorded: &str, expected: &str) -> bool {
    let Some((head, rest)) = recorded.split_once("meta=[dispatch_disposition] disp=[") else {
        return false;
    };
    let Some((disposition, tail)) = rest.split_once("] ") else {
        return false;
    };
    (disposition.starts_with("machine_capacity:") || disposition.starts_with("project_capacity:"))
        && tail.contains(":running]")
        && expected == format!("{head}meta=[-] disp=[-] {tail}")
}

fn key(line: &str) -> &str {
    line.split_once(": ").map_or(line, |(key, _)| key)
}

/// On the base commit this writes the recording; here it compares with it.
fn settle(section: &str, lines: Vec<String>) {
    if let Ok(dir) = std::env::var("SCHEDULER_EQUIVALENCE_RECORD") {
        std::fs::write(
            std::path::Path::new(&dir).join(format!("{section}.txt")),
            lines.join("\n") + "\n",
        )
        .unwrap();
        return;
    }
    let mut base: HashMap<&str, &str> = HashMap::new();
    let mut allowed: HashMap<&str, (&str, &str)> = HashMap::new();
    for line in FIXTURE
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        if let Some(rest) = line.strip_prefix('@') {
            let (name, line) = rest.split_once(' ').expect("exception names a line");
            assert!(ALLOWED.contains(&name), "unknown exception {name}");
            allowed.insert(key(line), (name, line));
        } else if line.starts_with(&format!("{section} "))
            || line.starts_with(&format!("{section}/"))
        {
            base.insert(key(line), line);
        }
    }
    assert!(!base.is_empty(), "no recording for section {section}");
    let mut differences = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for line in &lines {
        seen.insert(key(line));
        let Some(recorded) = base.get(key(line)) else {
            differences.push(format!("not recorded: {line}"));
            continue;
        };
        match allowed.get(key(line)) {
            Some((name, expected)) => {
                assert_ne!(
                    expected, recorded,
                    "exception {name} changes nothing: {line}"
                );
                if name == &"OwnSlotIsNoWait" {
                    assert!(
                        own_slot_exception_is_sound(recorded, expected),
                        "{name} may only clear a running Task's own capacity wait: {recorded}"
                    );
                }
                if name == &"ExplicitOwnerPark" {
                    // The park may only add a visible blocker to a Task the
                    // base left untouched: no step, owner or annotation.
                    for field in ["status=", "execs=[", "hooks=", "from=", "meta=["] {
                        let of = |l: &str| {
                            l.split(' ')
                                .find(|w| w.starts_with(field))
                                .map(str::to_owned)
                        };
                        assert_eq!(
                            of(recorded),
                            of(expected),
                            "{name} may not change {field} of {line}"
                        );
                    }
                    assert!(
                        recorded.contains(" dv=0 ")
                            && recorded.contains(" ann=- ")
                            && recorded.contains(" B=- ")
                            && recorded.contains("execs=[] hooks=0"),
                        "{name} covers a Task the base already owned: {recorded}"
                    );
                }
                let expected = if name == &"ExplicitOwnerPark" {
                    recorded
                } else {
                    expected
                };
                // Stage four moves visibility to the condition; legacy fields now match the recorded base.
                if line != expected {
                    differences.push(format!(
                        "allowed {name}\n  expected {expected}\n  now      {line}"
                    ));
                }
            }
            None if line != recorded => differences.push(format!("base {recorded}\n  now  {line}")),
            None => {}
        }
    }
    for (key, line) in &base {
        if !seen.contains(key) {
            differences.push(format!("missing: {line}"));
        }
    }
    differences.sort();
    assert!(
        differences.is_empty(),
        "{} scheduler decisions differ from 9d9b228f:\n{}",
        differences.len(),
        differences.join("\n")
    );
}

async fn shapes(mode: &'static str, prime: bool) {
    let only = std::env::var("SCHEDULER_EQUIVALENCE_ONLY").ok();
    let mut lines = Vec::new();
    for name in SHAPES {
        if only.as_deref().is_some_and(|o| !name.contains(o)) {
            continue;
        }
        match tokio::time::timeout(Duration::from_secs(90), run_shape(mode, name, prime)).await {
            Ok(shape) => lines.extend(shape),
            Err(_) => lines.push(format!("{mode} {name} (pass): SHAPE_TIMEOUT")),
        }
    }
    if only.is_some() {
        println!("{}", lines.join("\n"));
        return;
    }
    settle(mode, lines);
}

#[test]
fn scheduler_equivalence_shapes_steady() {
    on_runtime(|| shapes("steady", true));
}

#[test]
fn scheduler_equivalence_shapes_startup() {
    on_runtime(|| shapes("startup", false));
}

#[test]
fn scheduler_equivalence_failed_review() {
    on_runtime(|| async {
        let mut lines = Vec::new();
        for (label, minutes) in [("failed_review_old", 5), ("failed_review_in_grace", 0)] {
            let f = failed_review_fixture(chrono::Duration::minutes(minutes), 3).await;
            let pid = f.task.project_id.clone();
            let agents = HashMap::new();
            let before = snapshot(&f.db, &pid, &agents).await;
            for _ in 0..3 {
                tick(&f.dispatcher, 20).await;
            }
            let after = snapshot(&f.db, &pid, &agents).await;
            for ((title, a), (_, b)) in after.iter().zip(before.iter()) {
                lines.push(format!(
                    "review {label} {title}: from={} dv={} {}",
                    b.status,
                    a.version - b.version,
                    a.line
                ));
            }
        }
        settle("review", lines);
    });
}

/// 30 Tasks, two Agents, a machine run cap of four and a Project limit of six:
/// admission order, target Agents and slot counts per round.
#[test]
fn scheduler_equivalence_under_load() {
    on_runtime(|| async {
        let mut lines = Vec::new();
        for (mode, prime) in [("load/steady", true), ("load/startup", false)] {
            let mut c = ctx(false).await;
            c.db.server_run_cap.set(
                Some(4),
                config::resolved_run_cap(Some(4)),
                &config::embedded_machine_id(),
            );
            set_project_active_task_limit(&c.db, &c.pid, 6).await;
            if prime {
                let _ = c.dispatcher.check_once_and_drain().await;
            }
            let mut titles: HashMap<String, String> = HashMap::new();
            for i in 0..18 {
                let t = seed_task(
                    &c.db,
                    &c.pid,
                    &format!("S{i:02}"),
                    "todo",
                    [0, 5, 10, 5, 0, 10][i % 6],
                )
                .await;
                assign_role(
                    &c.db,
                    &t.id,
                    "coder",
                    if i % 2 == 0 { &c.a1 } else { &c.a2 },
                )
                .await;
                titles.insert(t.id.clone(), format!("S{i:02}"));
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            for r in 0..3 {
                let root = seed_task(&c.db, &c.pid, &format!("R{r}"), "todo", [10, 0, 5][r]).await;
                assign_role(&c.db, &root.id, "coder", &c.a1).await;
                titles.insert(root.id.clone(), format!("R{r}"));
                for k in 0..3 {
                    let child = seed_subtask(&c.db, &root, &format!("R{r}C{k}"), "todo", k).await;
                    assign_role(
                        &c.db,
                        &child.id,
                        "coder",
                        if k % 2 == 0 { &c.a2 } else { &c.a1 },
                    )
                    .await;
                    titles.insert(child.id.clone(), format!("R{r}C{k}"));
                }
            }
            let agents = agent_labels(&c).await;
            for round in 0..5 {
                for _ in 0..2 {
                    tick(&c.dispatcher, 60).await;
                    tokio::time::sleep(Duration::from_millis(60)).await;
                }
                let mut spawned = Vec::new();
                while let Ok(x) = c.rx.try_recv() {
                    spawned.push(
                        titles
                            .get(&x.task_id)
                            .cloned()
                            .unwrap_or_else(|| "?".into()),
                    );
                }
                spawned.sort();
                let created: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT t.title, COALESCE(e.agent_id,''), e.status FROM execution e JOIN task t ON t.id=e.task_id ORDER BY e.created_at,e.rowid",
            )
            .fetch_all(c.db.pool())
            .await
            .unwrap();
                let mut running: Vec<String> = created
                    .iter()
                    .filter(|x| x.2 == "running")
                    .map(|x| format!("{}@{}", x.0, agents.get(&x.1).cloned().unwrap_or_default()))
                    .collect();
                running.sort();
                let admitted: Vec<String> = sqlx::query_scalar(
                "SELECT t.title FROM transition_log l JOIN task t ON t.id=l.task_id WHERE l.from_state='todo' AND l.to_state!='todo' ORDER BY l.created_at,l.rowid",
            )
            .fetch_all(c.db.pool())
            .await
            .unwrap();
                lines.push(format!(
                    "{mode} round{round} (admission order): {admitted:?}"
                ));
                lines.push(format!("{mode} round{round} (spawned): {spawned:?}"));
                lines.push(format!(
                    "{mode} round{round} (running): {running:?} slots=[{}]",
                    slots_line(&c.db, &c.pid).await
                ));
                for (title, s) in snapshot(&c.db, &c.pid, &agents).await {
                    lines.push(format!("{mode} round{round} {title}: {}", s.line));
                }
                // Two finish per round, chosen by title: creation order between
                // the two Agents is not stable within a millisecond.
                sqlx::query("UPDATE execution SET status='completed', updated_at=? WHERE id IN (SELECT e.id FROM execution e JOIN task t ON t.id=e.task_id WHERE e.status='running' ORDER BY t.title LIMIT 2)")
                .bind(now_rfc3339()).execute(c.db.pool()).await.unwrap();
            }
        }
        settle("load", lines);
    });
}

/// A wake against a stored deferral and a stored refusal: what a Project-wide
/// wake and a single-Task wake leave in the legacy metadata, and whether the
/// Task is then dispatched.
#[test]
fn scheduler_equivalence_wake_clears_what_the_base_cleared() {
    on_runtime(|| async {
        let mut lines = Vec::new();
        for stored in ["deferral", "refusal"] {
            for wake in ["project", "task", "none"] {
                for status in ["in_progress", "todo"] {
                    let c = ctx(true).await;
                    let t = task(&c, "T", status).await;
                    if stored == "deferral" {
                        deferred_dispatch::set(
                            &c.db,
                            &t,
                            "in_progress",
                            &(chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339(),
                            "backoff",
                        )
                        .await
                        .unwrap();
                    } else {
                        let capability = if status == "todo" {
                            "coder"
                        } else {
                            "in_progress"
                        };
                        deferred_dispatch::record_dispatch_disposition(
                            &c.db,
                            &t,
                            capability,
                            "refused: probe",
                        )
                        .await
                        .unwrap();
                    }
                    let agents = agent_labels(&c).await;
                    for _ in 0..2 {
                        tick(&c.dispatcher, 20).await;
                    }
                    let before = snapshot(&c.db, &c.pid, &agents).await;
                    match wake {
                        "project" => {
                            TaskRepo::wake_dispatch_for_project(&*c.db, &c.pid, &now_rfc3339())
                                .await
                                .unwrap();
                        }
                        "task" => crate::wake_task_dispatch(&c.db, &t.id, "probe")
                            .await
                            .unwrap(),
                        _ => {}
                    }
                    c.dispatcher.drain_steps().await.unwrap();
                    let woken = snapshot(&c.db, &c.pid, &agents).await;
                    for _ in 0..3 {
                        tick(&c.dispatcher, 20).await;
                    }
                    let after = snapshot(&c.db, &c.pid, &agents).await;
                    let name = format!("wake {stored}_{wake}_{status}");
                    lines.push(format!("{name} (parked): {}", before[0].1.line));
                    lines.push(format!(
                        "{name} (woken): dv={} {}",
                        woken[0].1.version - before[0].1.version,
                        woken[0].1.line
                    ));
                    lines.push(format!(
                        "{name} (after): dv={} {}",
                        after[0].1.version - before[0].1.version,
                        after[0].1.line
                    ));
                }
            }
        }
        settle("wake", lines);
    });
}
