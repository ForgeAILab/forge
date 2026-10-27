use crate::worker_runtime::queue::writer_parity::case;

#[tokio::test]
async fn removed_machine() {
    for lost in [true, false] {
        case!(&format!("removed-machine-{lost}"), "in_progress", f, {
            let facts = super::RemovedMachineFacts {
                actor_id: "owner".into(),
                hostname: "machine".into(),
                executions: vec![],
                workspace_lost: lost,
                placed: true,
            };
            super::annotate_machine_removed(
                &f.db,
                &f.task,
                &facts,
                &api_types::Actor::user(api_types::UserActionSource::Api),
            )
            .await
            .unwrap();
        });
    }
}

#[tokio::test]
async fn refreshed_cancel_keeps_raw_fence() {
    for historical in [false, true] {
        case!(&format!("cancel-refresh-{historical}"), "in_progress", f, {
            let repo = db::ProjectRepo::get_by_id(&*f.db, &f.task.project_id)
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
                    repo_id: repo,
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
            sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,removed_at,created_at,updated_at) VALUES ('removed','removed','removed','linux','x86_64','offline','removed',?,?)").bind(db::now_rfc3339()).bind(db::now_rfc3339()).execute(f.db.pool()).await.unwrap();
            sqlx::query("INSERT INTO pending_remote_cancel VALUES ('operation','step','historical-workspace','placement','remaining','runtime',1,0,?)").bind(db::now_rfc3339()).execute(f.db.pool()).await.unwrap();
            let raw = if historical {
                "{\"type\":\"manual_stop\",\"blocking_reason\":\"pending_remote_cancel\",\"extension\":true}".into()
            } else {
                crate::worker_runtime::queue::writer_parity::channel_sample("remote-cancel")
            };
            sqlx::query("UPDATE task SET error_annotation=? WHERE id=?")
                .bind(format!("  {raw}  "))
                .bind(&f.task.id)
                .execute(f.db.pool())
                .await
                .unwrap();
            let facts = super::RemovedMachineFacts {
                actor_id: "owner".into(),
                hostname: "removed".into(),
                executions: vec![],
                workspace_lost: false,
                placed: false,
            };
            super::settle_removed_machine_task(
                &f.db,
                &f.ctx.event_bus,
                &f.task.id,
                "removed",
                &facts,
            )
            .await
            .unwrap();
            let row = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
                .await
                .unwrap()
                .unwrap();
            assert!(row.error_annotation.unwrap().contains("machine:remaining"));
        });
    }
}
