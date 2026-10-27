//! Tests for condition guards in scheduler equivalence.

use super::*;
/// A subtask in a merge state that no entry hook finished (its row is put
/// there, as a manual move or an entry that skipped leaves it): subtasks do
/// not merge, so no attempt, step or park would ever own it. The first
/// dispatcher pass finishes it by the state's landed-merge target, and the
/// passes after it write nothing more. The shape is not in the recording of
/// 9d9b228f, which left such a subtask resting with a clear condition.
#[test]
fn a_subtask_resting_in_a_merge_state_is_finished_by_a_pass() {
    on_runtime(|| async {
        let c = ctx(false).await;
        Box::pin(apply("merging_subtask", &c)).await;
        async fn state(c: &Ctx, title: &str) -> (String, String, i64) {
            sqlx::query_as(
                "SELECT id, status, (SELECT COUNT(*) FROM task_step WHERE task_id=task.id AND status IN ('pending','claimed')) FROM task WHERE project_id=? AND title=?",
            )
            .bind(&c.pid)
            .bind(title)
            .fetch_one(c.db.pool())
            .await
            .unwrap()
        }
        let (id, status, live_steps) = state(&c, "C1").await;
        assert_eq!((status.as_str(), live_steps), ("merging", 0));
        c.dispatcher.reconcile_all().await.unwrap();
        c.dispatcher.drain_steps().await.unwrap();
        let (_, status, _) = state(&c, "C1").await;
        assert_eq!(status, "done", "one pass finishes the subtask");
        let last: (String, String, String) = sqlx::query_as(
            "SELECT from_state, to_state, trigger_reason FROM transition_log WHERE task_id=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
        )
        .bind(&id)
        .fetch_one(c.db.pool())
        .await
        .unwrap();
        assert_eq!(
            (last.0.as_str(), last.1.as_str(), last.2.as_str()),
            (
                "merging",
                "done",
                crate::integration_steps::reconcile::SUBTASK_HAS_NO_MERGE
            )
        );
        // The same fixture again, under the twenty-pass guard.
        let c = ctx(false).await;
        Box::pin(apply("merging_subtask", &c)).await;
        let findings = unchanged_passes("merging_subtask", &c).await;
        assert!(findings.is_empty(), "{}", findings.join("\n"));
        let (_, status, _) = state(&c, "C1").await;
        assert_eq!(status, "done");
    });
}

#[test]
fn unchanged_failure_condition_matrix_guard() {
    on_runtime(|| async {
        use api_types::FailureKind::*;
        let c = ctx(false).await;
        // The action matrix's shared fixture factory supplies each annotation.
        let kinds = [
            MergeConflict,
            TargetRepoDirty,
            DirtyWorktree,
            CiFailed,
            ReviewGateFailed,
            ReviewBudgetExhausted,
            ReviewBlocked,
            ReviewNeedsOwner,
            EnvironmentNotReady,
            RetryExhausted,
            MergeFixBudgetExhausted,
            WorkflowGuardRejected,
            InternalCommandFailed,
            ExecutorFailed,
            WorkspaceFailed,
            WorkspaceResetRequired,
            WorkspaceError,
            BeforeWorkHookTimeout,
            BeforeWorkHookFailed,
            MaxTurnsExceeded,
            ManualStop,
            RecoveryRequired,
            ExecutorUnavailable,
            Unknown,
            WorkflowLoop,
            CascadeFailed,
            DispatchFailed,
            DependencyCancelled,
        ];
        for state in [
            "backlog",
            "todo",
            "planning",
            "in_progress",
            "review",
            "merging",
            "merge_failed",
            "done",
            "cancelled",
        ] {
            for kind in kinds {
                let t = task(&c, &format!("{state}/{kind}"), state).await;
                let sample = crate::task_actions::test_support::snapshot(state, Some(kind));
                ann(&c, &t.id, sample.task.error_annotation.as_deref().unwrap()).await;
            }
            for kind in [
                RetryExhausted,
                ReviewGateFailed,
                MergeFixBudgetExhausted,
                ExecutorFailed,
                ManualStop,
            ] {
                let t = task(&c, &format!("{state}/blocked:{kind}"), state).await;
                sqlx::query("UPDATE task SET blocked_json=? WHERE id=?")
                    .bind(serde_json::json!({"kind":kind,"reason":"fixture","created_at":now_rfc3339()}).to_string())
                    .bind(&t.id).execute(c.db.pool()).await.unwrap();
                raw(&c, &t.id, "title=title").await;
            }
            for extra in [
                "failed_json",
                "legacy_annotation",
                "barrier_blocked",
                "queued",
                "awaiting_human_meta",
                "pr_merge_wait",
            ] {
                let t = task(&c, &format!("{state}/{extra}"), state).await;
                match extra {
                    "failed_json" => raw(&c,&t.id,r#"failed_json='{"kind":"executor_failed","reason":"fixture"}'"#).await,
                    "legacy_annotation" => ann(&c,&t.id,r#"{"message":"old free-form annotation"}"#).await,
                    "barrier_blocked" => {
                        sqlx::query("UPDATE task SET entry_barrier_json=? WHERE id=?")
                            .bind(serde_json::json!({"status":"blocked","state":state}).to_string())
                            .bind(&t.id).execute(c.db.pool()).await.unwrap();
                        raw(&c,&t.id,"title=title").await;
                    }
                    "queued" => meta(&c,&t.id,serde_json::json!({"queued_recovery":{"id":"q"}})).await,
                    "awaiting_human_meta" => meta(&c,&t.id,serde_json::json!({"awaiting_human":true})).await,
                    "pr_merge_wait" => meta(&c,&t.id,serde_json::json!({"awaiting_human":true,"awaiting_human_reason":"pull_request_merge"})).await,
                    _ => unreachable!(),
                }
            }
        }
        let mut baseline = Vec::new();
        let mut findings = Vec::new();
        for pass in 1..=20 {
            tokio::time::timeout(Duration::from_secs(60), c.dispatcher.reconcile_all())
                .await
                .unwrap()
                .unwrap();
            c.dispatcher.drain_steps().await.unwrap();
            let after = idle_bookkeeping(&c).await;
            if pass == 2 {
                baseline = after;
            } else if pass > 2 && after != baseline {
                for (before, after) in baseline.iter().zip(&after) {
                    if before != after {
                        findings.push(format!("pass {pass}: {before:?} -> {after:?}"));
                    }
                }
                baseline = after;
            }
        }
        assert!(
            findings.is_empty(),
            "unchanged failure conditions wrote after pass 2:\n{}",
            findings.join("\n")
        );
    });
}
