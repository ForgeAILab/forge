//! Producers state a condition from what their write changed. Every test here
//! compares the stored result with the full recompute
//! (`task_condition_violations`), which shares no carried state with them.
use super::tests::{claim, db, task};
use super::*;
use crate::{EnqueueTaskStep, ProjectRepo, TaskRepo, TaskStepRepo, WorkspaceRepo};
use serde_json::json;

pub(super) async fn enqueue(
    db: &SqliteDb,
    task_id: &str,
    kind: &str,
    status: &str,
    payload: &str,
) -> String {
    let id = crate::new_uuid_v4();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.enqueue_step_in_tx(
        &mut tx,
        &EnqueueTaskStep {
            id: id.clone(),
            task_id: task_id.into(),
            kind: kind.into(),
            payload_json: payload.into(),
            causation_step_id: None,
            causation_key: id.clone(),
            chain_id: id.clone(),
            chain_position: 1,
            expected_status: status.into(),
            expected_version: 1,
            expected_epoch: None,
            lane: "fast".into(),
            available_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    id
}
async fn no_violations(db: &SqliteDb, context: &str) {
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        Vec::<String>::new(),
        "{context}"
    );
}
pub(super) async fn set_parent(db: &SqliteDb, parent: &str, children: &[&str]) {
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    for (order, child) in children.iter().enumerate() {
        sqlx::query("UPDATE task SET parent_task_id=?,subtask_order=? WHERE id=?")
            .bind(parent)
            .bind(order as i64)
            .bind(child)
            .execute(&mut *tx)
            .await
            .unwrap();
        db.sync_condition_in_tx(&mut tx, child).await.unwrap();
    }
    tx.commit().await.unwrap();
}
async fn flag_children(db: &SqliteDb, root: &str) {
    let step = claim(db, root).await;
    crate::task_writer::in_task_step(step.clone(), async {
        TaskRepo::mutate_metadata(
            db,
            root,
            None,
            vec![crate::TaskMetadataMutation::Set {
                key: "coordination_review_pending".into(),
                value: json!(true),
            }],
            &crate::now_rfc3339(),
        )
        .await
        .unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &step, "done", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    })
    .await;
}
/// A workspace owned by `owner`, placed on the server.
pub(super) async fn workspace(db: &SqliteDb, id: &str, owner: &str) -> tempfile::TempDir {
    let now = crate::now_rfc3339();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_string_lossy().into_owned();
    sqlx::query("INSERT OR IGNORE INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES('r','p','r','main',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES(?,?,'r',?,?,'ready',?,?)").bind(id).bind(owner).bind(&path).bind(format!("task/{id}")).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,status,created_at,updated_at) VALUES(?,'r','server',?,'primary_checkout','ready',?,?)").bind(format!("l-{id}")).bind(&path).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,state,selected_by,selection_reason,created_at,updated_at) VALUES(?,?,?,'server',?,'h','ready','backfill','{}',?,?)").bind(format!("pl-{id}")).bind(id).bind(owner).bind(format!("l-{id}")).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    directory
}
/// A running remote operation of `step` in `workspace`, marked for cancel.
pub(super) async fn pending_cancel(
    db: &SqliteDb,
    operation: &str,
    step_id: &str,
    workspace: &str,
) -> crate::RemoteTaskOperation {
    sqlx::query("INSERT INTO task_remote_operation(operation_id,step_id,workspace_id,placement_id,daemon_id,runtime_id,generation,expected_epoch,state,created_at) VALUES(?,?,?,?,'owner','runtime',1,0,'running',?)").bind(operation).bind(step_id).bind(workspace).bind(format!("pl-{workspace}")).bind(crate::now_rfc3339()).execute(db.pool()).await.unwrap();
    let operation = db
        .running_remote_task_operations(step_id)
        .await
        .unwrap()
        .remove(0);
    db.mark_pending_remote_cancel(&operation).await.unwrap();
    operation
}

/// An owner command that preempts supersedes the pending entry hooks row. The
/// condition must stop naming the dead step.
#[tokio::test]
async fn preempt_superseding_entry_hooks_restates_the_condition() {
    let db = db().await;
    let t = task(&db, "preempt").await;
    let hook = enqueue(&db, &t.id, "hooks", "todo", "{}").await;
    assert!(
        matches!(db.task_condition(&t.id).await.unwrap(), TaskCondition::Entering{step_id,..} if step_id==hook)
    );
    enqueue(
        &db,
        &t.id,
        "command",
        "todo",
        &json!({"preempt":true}).to_string(),
    )
    .await;
    let status: String = sqlx::query_scalar("SELECT status FROM task_step WHERE id=?")
        .bind(&hook)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(status, "superseded");
    assert!(matches!(
        db.task_condition(&t.id).await.unwrap(),
        TaskCondition::Clear { .. }
    ));
    no_violations(&db, "preempt").await;
}

/// Terminal classification reads the Project workflow, so a workflow edit
/// restates the Tasks it reclassifies, and the roots waiting on them.
#[tokio::test]
async fn project_workflow_edit_restates_reclassified_tasks() {
    let db = db().await;
    let t = task(&db, "wf").await;
    let untouched = task(&db, "wf-todo").await;
    let root = task(&db, "wf-root").await;
    let child = task(&db, "wf-child").await;
    set_parent(&db, &root.id, &[&child.id]).await;
    for id in [&t.id, &child.id] {
        let current = TaskRepo::get_by_id(&db, id, false).await.unwrap().unwrap();
        let step = claim(&db, id).await;
        crate::task_writer::in_task_step(step, async {
            TaskRepo::update_status(
                &db,
                crate::UpdateTaskStatus {
                    id: id.clone(),
                    expected_version: current.version,
                    status: "released".into(),
                    assignee_id: None,
                    error_annotation: None,
                    blocked_json: None,
                    failed_json: None,
                    updated_at: crate::now_rfc3339(),
                },
            )
            .await
            .unwrap();
        })
        .await;
    }
    flag_children(&db, &root.id).await;
    no_violations(&db, "before the edit").await;
    assert!(matches!(
        db.task_condition(&root.id).await.unwrap(),
        TaskCondition::Parked {
            primary: ParkReason::Children { .. },
            ..
        }
    ));
    let before: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&untouched.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let project = ProjectRepo::get_by_id(&db, "p").await.unwrap().unwrap();
    ProjectRepo::update_workflow(
        &db,
        "p",
        &json!({"states":[{"name":"todo","kind":"initial"},{"name":"released","kind":"terminal"}]})
            .to_string(),
        None,
        project.version,
        &crate::now_rfc3339(),
    )
    .await
    .unwrap();
    assert!(matches!(
        db.task_condition(&t.id).await.unwrap(),
        TaskCondition::Settled {
            outcome: TerminalOutcome::Completed,
            ..
        }
    ));
    // Legacy `subtask_is_terminal` now calls the child finished.
    assert!(matches!(
        db.task_condition(&root.id).await.unwrap(),
        TaskCondition::Deferred {
            reason: RetryCause::ChildrenReady,
            ..
        }
    ));
    assert_eq!(
        before,
        sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
            .bind(&untouched.id)
            .fetch_one(db.pool())
            .await
            .unwrap()
    );
    no_violations(&db, "after the edit").await;
}

/// Children run in the root's shared workspace. Legacy fences every Task that
/// owns the workspace or has an execution in it, and no other; the condition
/// parks exactly those.
#[tokio::test]
async fn remote_cancel_parks_exactly_the_tasks_legacy_fences() {
    let db = db().await;
    let root = task(&db, "rc-root").await;
    let a = task(&db, "rc-a").await;
    let b = task(&db, "rc-b").await;
    set_parent(&db, &root.id, &[&a.id, &b.id]).await;
    let _directory = workspace(&db, "w", &root.id).await;
    // Sibling B ran in the shared workspace earlier.
    let now = crate::now_rfc3339();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,workspace_id,created_at,updated_at) VALUES('b-run',?,'coder','completed','w',?,?)").bind(&b.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.sync_condition_in_tx(&mut tx, &b.id).await.unwrap();
    tx.commit().await.unwrap();
    // The marker comes from child A's step, which has no execution there.
    let step = claim(&db, &a.id).await;
    let operation = pending_cancel(&db, "op", &step.id, "w").await;
    for (id, fenced) in [(&root.id, true), (&a.id, false), (&b.id, true)] {
        assert_eq!(
            db.task_has_pending_remote_cancel(id).await.unwrap(),
            fenced,
            "{id}: legacy"
        );
        let condition = db.task_condition(id).await.unwrap();
        assert_eq!(condition.is_blocked(), fenced, "{id}: {condition:?}");
        assert_eq!(
            matches!(
                &condition,
                TaskCondition::Parked {
                    primary: ParkReason::RemoteCancelPending { .. },
                    ..
                }
            ),
            fenced,
            "{id}"
        );
    }
    no_violations(&db, "marked").await;
    // An execution admitted into the fenced workspace joins the fence.
    crate::ExecutionRepo::create(
        &db,
        crate::CreateExecution {
            id: "a-run".into(),
            task_id: a.id.clone(),
            agent_id: None,
            role: "coder".into(),
            status: crate::ExecutionStatus::Completed,
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
            workspace_id: Some("w".into()),
            created_at: crate::now_rfc3339(),
            updated_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    assert!(db.task_has_pending_remote_cancel(&a.id).await.unwrap());
    assert!(db.task_condition(&a.id).await.unwrap().is_blocked());
    no_violations(&db, "joined").await;
    db.acknowledge_remote_cancel(&operation).await.unwrap();
    for id in [&root.id, &a.id, &b.id] {
        assert!(!db.task_has_pending_remote_cancel(id).await.unwrap());
        assert!(!db.task_condition(id).await.unwrap().is_blocked(), "{id}");
    }
    no_violations(&db, "acknowledged").await;
}

/// Legacy treats a flagged root with no visible child as an ordinary Task and
/// counts a child in a Project-workflow terminal state as finished.
#[tokio::test]
async fn coordination_flag_matches_what_legacy_dispatches() {
    let db = db().await;
    let root = task(&db, "co-root").await;
    flag_children(&db, &root.id).await;
    let none = db.task_condition(&root.id).await.unwrap();
    assert!(
        matches!(&none, TaskCondition::Clear { evidence } if evidence.observations.iter().any(|o| matches!(o, ParkReason::UnknownCondition { source, .. } if source.key.as_deref() == Some("coordination_review_pending")))),
        "{none:?}"
    );
    no_violations(&db, "no children").await;
    let project = ProjectRepo::get_by_id(&db, "p").await.unwrap().unwrap();
    ProjectRepo::update_workflow(
        &db,
        "p",
        &json!({"states":[{"name":"todo","kind":"initial"},{"name":"wont_do","kind":"terminal"},{"name":"done","kind":"terminal"}]}).to_string(),
        None,
        project.version,
        &crate::now_rfc3339(),
    )
    .await
    .unwrap();
    let open = task(&db, "co-open").await;
    let closed = task(&db, "co-closed").await;
    set_parent(&db, &root.id, &[&open.id, &closed.id]).await;
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.sync_condition_in_tx(&mut tx, &root.id).await.unwrap();
    tx.commit().await.unwrap();
    let mut waiting = vec![open.id.clone(), closed.id.clone()];
    for id in [&closed.id, &open.id] {
        assert!(
            matches!(db.task_condition(&root.id).await.unwrap(), TaskCondition::Parked { primary: ParkReason::Children { remaining, .. }, .. } if remaining == waiting)
        );
        let current = TaskRepo::get_by_id(&db, id, false).await.unwrap().unwrap();
        let step = claim(&db, id).await;
        crate::task_writer::in_task_step(step, async {
            TaskRepo::update_status(
                &db,
                crate::UpdateTaskStatus {
                    id: id.clone(),
                    expected_version: current.version,
                    status: "wont_do".into(),
                    assignee_id: None,
                    error_annotation: None,
                    blocked_json: None,
                    failed_json: None,
                    updated_at: crate::now_rfc3339(),
                },
            )
            .await
            .unwrap();
        })
        .await;
        // A subtask outside the inherited states uses the Project workflow.
        assert!(matches!(
            db.task_condition(id).await.unwrap(),
            TaskCondition::Settled { .. }
        ));
        waiting.retain(|child| child != id);
        no_violations(&db, id).await;
    }
    assert!(matches!(
        db.task_condition(&root.id).await.unwrap(),
        TaskCondition::Deferred {
            reason: RetryCause::ChildrenReady,
            ..
        }
    ));
}

/// Child witnesses are in sequence order, so both reorder writers restate the
/// parent; so do a child's soft deletion and a reparent.
#[tokio::test]
async fn child_order_and_membership_writers_restate_the_parent() {
    let db = db().await;
    let root = task(&db, "ro-root").await;
    let other = task(&db, "ro-other").await;
    let a = task(&db, "ro-a").await;
    let b = task(&db, "ro-b").await;
    set_parent(&db, &root.id, &[&a.id, &b.id]).await;
    flag_children(&db, &root.id).await;
    flag_children(&db, &other.id).await;
    let remaining = |condition: TaskCondition| match condition {
        TaskCondition::Parked {
            primary: ParkReason::Children { remaining, .. },
            ..
        } => remaining,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        remaining(db.task_condition(&root.id).await.unwrap()),
        vec![a.id.clone(), b.id.clone()]
    );
    TaskRepo::reorder_subtasks(
        &db,
        &root.id,
        &[b.id.clone(), a.id.clone()],
        &crate::now_rfc3339(),
    )
    .await
    .unwrap();
    assert_eq!(
        remaining(db.task_condition(&root.id).await.unwrap()),
        vec![b.id.clone(), a.id.clone()]
    );
    no_violations(&db, "reorder_subtasks").await;
    // Reparent A: both parents' witnesses change.
    let current = TaskRepo::get_by_id(&db, &a.id, false)
        .await
        .unwrap()
        .unwrap();
    let step = claim(&db, &a.id).await;
    crate::task_writer::in_task_step(step, async {
        TaskRepo::update(
            &db,
            crate::UpdateTask {
                id: a.id.clone(),
                expected_version: current.version,
                title: None,
                description: None,
                priority: None,
                merge_config: None,
                plan: None,
                error_annotation: None,
                blocked_json: None,
                failed_json: None,
                task_state_config: None,
                parent_task_id: Some(Some(other.id.clone())),
                updated_at: crate::now_rfc3339(),
            },
        )
        .await
        .unwrap();
    })
    .await;
    assert_eq!(
        remaining(db.task_condition(&root.id).await.unwrap()),
        vec![b.id.clone()]
    );
    assert_eq!(
        remaining(db.task_condition(&other.id).await.unwrap()),
        vec![a.id.clone()]
    );
    no_violations(&db, "reparent").await;
    // Soft-delete B: the root has no visible child left and is ordinary again.
    let current = TaskRepo::get_by_id(&db, &b.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::soft_delete(
        &db,
        crate::SoftDeleteTask {
            id: b.id.clone(),
            expected_version: current.version,
            deleted_at: crate::now_rfc3339(),
            updated_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        db.task_condition(&root.id).await.unwrap(),
        TaskCondition::Clear { .. }
    ));
    no_violations(&db, "soft delete").await;
}

/// A cancellation fences its Tasks through the workspace row. Deleting the
/// row lifts the fence and restates them.
#[tokio::test]
async fn workspace_delete_restates_the_tasks_it_fenced() {
    let db = db().await;
    let t = task(&db, "ws-owner").await;
    let _directory = workspace(&db, "w", &t.id).await;
    let step = claim(&db, &t.id).await;
    pending_cancel(&db, "op", &step.id, "w").await;
    assert!(db.task_condition(&t.id).await.unwrap().is_blocked());
    // While the operation's step is live, deletion is a typed conflict.
    sqlx::query("UPDATE task_step SET status='superseded',completed_at=? WHERE id=?")
        .bind(crate::now_rfc3339())
        .bind(&step.id)
        .execute(db.pool())
        .await
        .unwrap();
    WorkspaceRepo::delete(&db, "w").await.unwrap();
    assert!(!db.task_has_pending_remote_cancel(&t.id).await.unwrap());
    assert!(!db.task_condition(&t.id).await.unwrap().is_blocked());
    no_violations(&db, "workspace delete").await;
}

/// m3: no receipt outside the engine CAS and board moves carries a status
/// epoch, so `enqueue_recovered_entry_hooks` (`durable.rs`) reads what it
/// read before the shadow existed. A same-state receipt (a recovery marker)
/// never becomes the Task's entry: a pre-epoch Task keeps its running
/// execution bound across one.
#[tokio::test]
async fn recovery_marker_is_unstamped_and_never_an_entry() {
    let db = db().await;
    let t = task(&db, "marker").await;
    let now = crate::now_rfc3339();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('run',?,'coder','running',?,?)").bind(&t.id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.sync_condition_in_tx(&mut tx, &t.id).await.unwrap();
    tx.commit().await.unwrap();
    let before = db.task_condition(&t.id).await.unwrap();
    assert!(
        matches!(&before, TaskCondition::Running { execution_id, .. } if execution_id == "run")
    );
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    for id in ["marker-1", "marker-2"] {
        crate::TransitionLogRepo::insert(
            &db,
            crate::CreateTransitionLog {
                id: id.into(),
                task_id: t.id.clone(),
                from_state: "todo".into(),
                to_state: "todo".into(),
                trigger_name: None,
                triggered_by: "system".into(),
                bridge: Default::default(),
                trigger_reason: "recovery".into(),
                hook_results_json: None,
                rejection: false,
                created_at: crate::now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
    // A pre-epoch Task: the legacy read takes the newest unstamped receipt,
    // as it did before the shadow. A stamp would hand it `marker-1` instead.
    assert_eq!(
        recovered_entry(&db, &t.id, "todo", 0).await.as_deref(),
        Some("marker-2")
    );
    assert_eq!(before, db.task_condition(&t.id).await.unwrap());
    no_violations(&db, "markers").await;
}
/// The entry `enqueue_recovered_entry_hooks` would re-drive: its two reads.
async fn recovered_entry(db: &SqliteDb, task_id: &str, status: &str, epoch: i64) -> Option<String> {
    let stamped: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transition_log WHERE task_id=? AND status_epoch IS NOT NULL",
    )
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        stamped, 0,
        "only the engine CAS and board moves stamp an epoch"
    );
    let mut entry: Option<String> = sqlx::query_scalar("SELECT id FROM transition_log WHERE task_id = ? AND to_state = ? AND status_epoch = ? ORDER BY created_at, rowid LIMIT 1")
        .bind(task_id).bind(status).bind(epoch).fetch_optional(db.pool()).await.unwrap();
    if entry.is_none() && epoch == 0 {
        entry = sqlx::query_scalar("SELECT id FROM transition_log WHERE task_id = ? AND to_state = ? AND status_epoch IS NULL ORDER BY created_at DESC, rowid DESC LIMIT 1")
            .bind(task_id).bind(status).fetch_optional(db.pool()).await.unwrap();
    }
    entry
}

/// The producer that reads receipts by token is a claim: it writes the status
/// and its entry receipt through the recovery-marker insert, unstamped, and
/// admits an execution under that receipt. The condition binds that execution,
/// and the one a later retry marker admits, without any stamp, so what
/// `durable.rs` would re-drive for the Task is unchanged: nothing.
#[tokio::test]
async fn claim_and_retry_receipts_bind_their_executions_without_an_epoch_stamp() {
    let db = db().await;
    let t = task(&db, "claimed").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step, claim_and_retry(&db, &t)).await;
}
async fn claim_and_retry(db: &SqliteDb, t: &Task) {
    let db = db.clone();
    TaskRepo::update_status(
        &db,
        crate::UpdateTaskStatus {
            id: t.id.clone(),
            expected_version: t.version,
            status: "in_progress".into(),
            assignee_id: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            updated_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let admit = |execution: &'static str, token: &'static str| {
        let db = &db;
        let task_id = t.id.clone();
        async move {
            crate::ExecutionRepo::create(
                db,
                crate::CreateExecution {
                    id: execution.into(),
                    task_id,
                    agent_id: None,
                    role: "coder".into(),
                    status: crate::ExecutionStatus::Running,
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
                    executor_config_snapshot_json: Some(
                        json!({"state_entry_token":token,"task_state":"in_progress"}).to_string(),
                    ),
                    workspace_id: None,
                    created_at: crate::now_rfc3339(),
                    updated_at: crate::now_rfc3339(),
                },
            )
            .await
            .unwrap();
        }
    };
    let receipt = |id: &'static str, from: &'static str| crate::CreateTransitionLog {
        id: id.into(),
        task_id: t.id.clone(),
        from_state: from.into(),
        to_state: "in_progress".into(),
        trigger_name: None,
        triggered_by: "system".into(),
        bridge: Default::default(),
        trigger_reason: "receipt".into(),
        hook_results_json: None,
        rejection: false,
        created_at: crate::now_rfc3339(),
    };
    // The claim's entry receipt, then its execution.
    crate::TransitionLogRepo::insert(&db, receipt("claim-receipt", "todo"))
        .await
        .unwrap();
    admit("claimed-run", "claim-receipt").await;
    let running = db.task_condition(&t.id).await.unwrap();
    assert!(
        matches!(&running, TaskCondition::Running { execution_id, .. } if execution_id == "claimed-run"),
        "{running:?}"
    );
    assert!(running
        .evidence()
        .witnesses
        .iter()
        .any(|w| matches!(w, ConditionWitness::Entry { transition_id: Some(id), .. } if id == "claim-receipt")));
    no_violations(&db, "claim").await;
    // A retry marker refreshes the token; the entry stays the claim's.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    crate::TransitionLogRepo::insert(&db, receipt("retry-marker", "in_progress"))
        .await
        .unwrap();
    admit("retried-run", "retry-marker").await;
    let retried = db.task_condition(&t.id).await.unwrap();
    assert!(
        matches!(&retried, TaskCondition::Running { execution_id, .. } if execution_id == "retried-run"),
        "{retried:?}"
    );
    assert!(retried
        .evidence()
        .witnesses
        .iter()
        .any(|w| matches!(w, ConditionWitness::Entry { transition_id: Some(id), .. } if id == "claim-receipt")));
    no_violations(&db, "retry").await;
    // An execution recorded already settled is history: it is no witness,
    // costs no condition write and does not hide the running owner.
    let stored_before: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&t.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET condition_json='{\"kind\":\"untouched\"}' WHERE id=?")
        .bind(&t.id)
        .execute(db.pool())
        .await
        .unwrap();
    crate::ExecutionRepo::create(
        &db,
        crate::CreateExecution {
            id: "settled-record".into(),
            task_id: t.id.clone(),
            agent_id: None,
            role: "coder".into(),
            status: crate::ExecutionStatus::Failed,
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
            created_at: crate::now_rfc3339(),
            updated_at: crate::now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let untouched: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&t.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(untouched, "{\"kind\":\"untouched\"}");
    sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
        .bind(&stored_before)
        .bind(&t.id)
        .execute(db.pool())
        .await
        .unwrap();
    no_violations(&db, "settled record").await;
    // An execution admitted under a receipt of another state is not this entry's.
    admit("stray-run", "no-such-receipt").await;
    assert!(matches!(
        db.task_condition(&t.id).await.unwrap(),
        TaskCondition::Clear { .. }
    ));
    no_violations(&db, "stray").await;
    let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
        .bind(&t.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(epoch, 1);
    assert_eq!(
        recovered_entry(&db, &t.id, "in_progress", epoch).await,
        None
    );
}

/// The fact statements a Task write runs stay index lookups: no table scan,
/// and nothing that grows with the Task's transition or execution history.
#[tokio::test]
async fn producer_fact_queries_use_indexes() {
    let db = db().await;
    let snapshot = |families: facts::Families| families.sql().replace("t.id=?", "t.id='t'");
    for (name, sql) in [
        ("row", snapshot(facts::Families::ROW)),
        ("entry", snapshot(facts::Families::ENTRY)),
        ("hooks", snapshot(facts::Families::HOOKS)),
        ("execution", snapshot(facts::Families::EXECUTION)),
        ("budget", "SELECT kind,window_id,spent FROM task_budget WHERE task_id='t' AND spent>0 ORDER BY kind".to_owned()),
        ("children", "SELECT id,status FROM task WHERE parent_task_id='t' AND deleted_at IS NULL ORDER BY subtask_order,id".to_owned()),
        ("check page", "SELECT id FROM task WHERE id>'a' ORDER BY id LIMIT 50".to_owned()),
        ("joined workspace", "SELECT 1 FROM pending_remote_cancel WHERE workspace_id='w'".to_owned()),
    ] {
        let plan: Vec<(i64, i64, i64, String)> =
            sqlx::query_as(&format!("EXPLAIN QUERY PLAN {sql}"))
                .fetch_all(db.pool())
                .await
                .unwrap();
        let mut previous = "";
        for (_, _, _, step) in &plan {
            // The one scan and sort allowed are over the executions running
            // right now, read from the running-only partial index.
            let running = "idx_execution_running_task";
            assert!(
                !step.starts_with("SCAN") || step.contains(running),
                "{name}: {step}"
            );
            assert!(
                !step.contains("TEMP B-TREE") || previous.contains(running),
                "{name}: {step} after {previous}"
            );
            previous = step;
        }
        assert!(!plan.is_empty(), "{name}");
    }
}

/// A producer reads only the family its write can change. A budget charge on
/// a Task that is not exhausted, a non-hooks step and an interactive session
/// leave the stored condition untouched even when it is stale, which only a
/// producer that did not recompute it can do.
#[tokio::test]
async fn producers_do_not_rederive_unrelated_families() {
    let db = db().await;
    let t = task(&db, "narrow").await;
    let stale = encode(&TaskCondition::Clear {
        evidence: ConditionEvidence {
            witnesses: vec![ConditionWitness::Entry {
                task_id: t.id.clone(),
                epoch: 0,
                transition_id: Some("carried".into()),
                since: "carried".into(),
                initial: false,
                human_wait: false,
                review_wait: false,
                review_failure: false,
            }],
            ..Default::default()
        },
    });
    sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
        .bind(&stale)
        .bind(&t.id)
        .execute(db.pool())
        .await
        .unwrap();
    let stored = || async {
        sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id='narrow'")
            .fetch_one(db.pool())
            .await
            .unwrap()
    };
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step.clone(), async {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        crate::budget::charge(&mut tx, &t.id, "execution", 3, &step.id)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(stored().await, stale, "a charge reads no entry");
        // A legacy write carries the entry it did not change.
        TaskRepo::mutate_metadata(
            &db,
            &t.id,
            None,
            vec![crate::TaskMetadataMutation::Set {
                key: "owner_wait".into(),
                value: json!({"daemon_id":"d"}),
            }],
            &crate::now_rfc3339(),
        )
        .await
        .unwrap();
        let carried = db.task_condition(&t.id).await.unwrap();
        assert!(carried.is_blocked());
        assert!(carried
            .evidence()
            .witnesses
            .contains(&ConditionWitness::Entry {
                task_id: t.id.clone(),
                epoch: 0,
                transition_id: Some("carried".into()),
                since: "carried".into(),
                initial: false,
                human_wait: false,
                review_wait: false,
                review_failure: false,
            }));
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        db.finish_step_in_tx(&mut tx, &step, "done", None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            db.task_condition(&t.id).await.unwrap(),
            carried,
            "a command step is not witnessed"
        );
    })
    .await;
    // The invariant check is what re-derives everything.
    assert_eq!(
        db.task_condition_violations().await.unwrap(),
        vec![t.id.clone()]
    );
    db.check_task_conditions(CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    no_violations(&db, "repaired").await;
}

/// The strict seam takes the producer's witnesses as stated: it refuses a
/// condition that is not the legacy mapping under them, without reading any
/// fact table.
#[tokio::test]
async fn set_condition_checks_the_stated_condition_against_legacy_only() {
    let db = db().await;
    let t = task(&db, "stated").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step, async {
        let input = LegacyConditionInput::from(&t);
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        let facts = ConditionFacts::load(&mut tx, &t.id).await.unwrap();
        let stated = facts.apply(map_legacy_condition(&input));
        db.set_condition(&mut tx, &t.id, t.version, &input, &stated)
            .await
            .unwrap();
        // Another Task's witnesses, a stale epoch, or a variant the legacy
        // fields do not map to are all refused.
        let mut other = facts.clone();
        other.task_id = "someone-else".into();
        let mut stale = facts.clone();
        stale.epoch += 1;
        for refused in [
            other.apply(map_legacy_condition(&input)),
            stale.apply(map_legacy_condition(&input)),
            facts.apply(TaskCondition::Parked {
                primary: ParkReason::Held {
                    actor: "user".into(),
                },
                additional: Vec::new(),
                resume: ConditionContinuation::Reconcile,
                since: None,
                evidence: Default::default(),
            }),
        ] {
            assert!(matches!(
                db.set_condition(&mut tx, &t.id, t.version, &input, &refused)
                    .await,
                Err(DbError::Check(_))
            ));
        }
        tx.rollback().await.unwrap();
    })
    .await;
}

/// The engine CAS and the worker's step settlement state their condition
/// through the strict seam while their step is live. Once the step is no
/// longer theirs, the legacy write they already made still has to be
/// described: the seam falls back to the version fence and never refuses it.
#[tokio::test]
async fn stated_condition_follows_the_legacy_write_with_or_without_a_live_lease() {
    let db = db().await;
    let t = task(&db, "stated-seam").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step.clone(), async {
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET error_annotation=? WHERE id=?")
            .bind(json!({"type":"manual_stop"}).to_string())
            .bind(&t.id)
            .execute(&mut *tx)
            .await
            .unwrap();
        db.state_condition_in_tx(&mut tx, &t.id, ConditionChange::Legacy)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(matches!(
            db.task_condition(&t.id).await.unwrap(),
            TaskCondition::Parked {
                primary: ParkReason::Held { .. },
                ..
            }
        ));
        // The lease now belongs to a successor: `set_condition` itself refuses.
        sqlx::query("UPDATE task_step SET claimed_by='successor' WHERE id=?")
            .bind(&step.id)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET error_annotation=NULL WHERE id=?")
            .bind(&t.id)
            .execute(&mut *tx)
            .await
            .unwrap();
        let input = LegacyConditionInput::default();
        let facts = ConditionFacts::load(&mut tx, &t.id).await.unwrap();
        assert!(matches!(
            db.set_condition(
                &mut tx,
                &t.id,
                t.version,
                &input,
                &facts.apply(map_legacy_condition(&input))
            )
            .await,
            Err(DbError::VersionConflict)
        ));
        db.state_condition_in_tx(&mut tx, &t.id, ConditionChange::Legacy)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(matches!(
            db.task_condition(&t.id).await.unwrap(),
            TaskCondition::Clear { .. }
        ));
    })
    .await;
    no_violations(&db, "stated").await;
}
