use super::*;
use crate::task_service::workspace::{
    prepare_workspace_for_test,
    tests::{seed_project_with_real_repo, seed_task, sqlite_db},
};
use tempfile::TempDir;

/// A data directory, a system temp directory and a user's repository, each
/// in its own temp directory: nothing here is a real root.
pub(super) struct Dirs {
    pub data: TempDir,
    pub temp: TempDir,
    pub repo: TempDir,
}

impl Dirs {
    pub(super) fn new() -> Self {
        Self {
            data: TempDir::new().unwrap(),
            temp: TempDir::new().unwrap(),
            repo: TempDir::new().unwrap(),
        }
    }

    pub(super) fn default_root(&self) -> PathBuf {
        self.data.path().join("worktrees")
    }

    pub(super) fn legacy_root(&self) -> PathBuf {
        legacy_temp_root(self.temp.path())
    }

    fn default_choice(&self) -> RootChoice {
        RootChoice {
            configured: self.default_root(),
            explicit: false,
            data_dir: self.data.path().to_path_buf(),
            system_temp: self.temp.path().to_path_buf(),
        }
    }

    fn explicit_choice(&self, root: &Path) -> RootChoice {
        RootChoice {
            configured: root.to_path_buf(),
            explicit: true,
            ..self.default_choice()
        }
    }
}

/// One ready Task workspace of a user's repository under `root`.
pub(super) async fn seed_workspace_under(db: &SqliteDb, dirs: &Dirs, root: &Path) -> db::Workspace {
    std::fs::create_dir_all(root).unwrap();
    let (project_id, _) = seed_project_with_real_repo(db, dirs.repo.path()).await;
    let task = seed_task(db, &project_id, None).await;
    prepare_workspace_for_test(db, root, &task, &task.id, None)
        .await
        .expect("workspace creates")
}

async fn status(db: &SqliteDb) -> api_types::OperatorStatusResponse {
    crate::operator_status::OperatorStatusService::new_for_test(std::sync::Arc::new(db.clone()))
        .compute_status()
        .await
        .unwrap()
}

#[tokio::test]
async fn a_fresh_data_dir_runs_on_its_own_worktrees_directory_and_records_it() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(
        settled,
        SettledRoot {
            root: dirs.default_root(),
            in_system_temp: false,
            recorded_now: true,
        }
    );
    assert_eq!(recorded_root(&db).await.unwrap(), Some(dirs.default_root()));
    assert!(status(&db)
        .await
        .recent_errors
        .iter()
        .all(|issue| issue.entity_type != "workspace_root"));
    // The next start reads the record instead of deciding again.
    let again = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(
        (again.root, again.recorded_now),
        (dirs.default_root(), false)
    );
}

#[tokio::test]
async fn a_recorded_root_survives_a_changed_default() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let first = dirs.data.path().join("somewhere-else");
    settle(&db, &dirs.explicit_choice(&first)).await.unwrap();
    // The setting is gone and today's default is another directory.
    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!((settled.root, settled.recorded_now), (first.clone(), false));
    assert_eq!(recorded_root(&db).await.unwrap(), Some(first));
}

#[tokio::test]
async fn an_install_on_the_temp_default_keeps_it_records_it_and_reports_it() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let legacy = dirs.legacy_root();
    let workspace = seed_workspace_under(&db, &dirs, &legacy).await;
    assert!(Path::new(workspace.embedded_worktree_path_for_backend()).starts_with(&legacy));

    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(
        settled,
        SettledRoot {
            root: legacy.clone(),
            in_system_temp: true,
            recorded_now: true,
        }
    );
    assert_eq!(recorded_root(&db).await.unwrap(), Some(legacy.clone()));
    assert!(!dirs.default_root().exists());

    let reported = status(&db).await;
    let issue = reported
        .recent_errors
        .iter()
        .find(|issue| issue.entity_type == "workspace_root")
        .expect("the temp root is reported");
    assert_eq!(issue.entity_id, legacy.display().to_string());
    assert!(issue.error.contains("system temp directory"));
    assert!(issue.error.contains("forge --migrate-workspace-root"));
    assert_ne!(
        reported.overall_severity,
        api_types::OperatorSeverity::Healthy
    );
    assert_eq!(
        system_temp_warning(&legacy),
        issue.error,
        "the log line and the operator entry say the same thing"
    );
}

#[tokio::test]
async fn directories_alone_mark_an_install_on_the_temp_default() {
    for marker in [".repos", ".forge"] {
        let db = sqlite_db().await;
        let dirs = Dirs::new();
        std::fs::create_dir_all(dirs.legacy_root().join(marker)).unwrap();
        let settled = settle(&db, &dirs.default_choice()).await.unwrap();
        assert_eq!(settled.root, dirs.legacy_root(), "{marker}");
        assert!(settled.in_system_temp);
    }
    // An unrelated directory in the temp directory is not an install.
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    std::fs::create_dir_all(dirs.legacy_root().join("stray")).unwrap();
    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(settled.root, dirs.default_root());
}

#[tokio::test]
async fn a_configured_root_that_would_strand_live_data_refuses_the_start() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let recorded = dirs.default_root();
    settle(&db, &dirs.default_choice()).await.unwrap();
    let workspace = seed_workspace_under(&db, &dirs, &recorded).await;
    std::fs::create_dir_all(recorded.join(".repos").join("clone")).unwrap();
    let other = dirs.data.path().join("other-root");

    let refused = settle(&db, &dirs.explicit_choice(&other))
        .await
        .unwrap_err();
    let WorkspaceRootError::Refused(message) = &refused else {
        panic!("expected a refusal, got {refused:?}");
    };
    assert!(message.contains(&other.display().to_string()), "{message}");
    assert!(
        message.contains(&recorded.display().to_string()),
        "{message}"
    );
    assert!(
        message.contains(&format!(
            "forge --migrate-workspace-root {}",
            other.display()
        )),
        "{message}"
    );
    assert!(message.contains("1 workspace(s)"), "{message}");
    assert!(message.contains("1 repository clone(s)"), "{message}");
    // Nothing was recorded and nothing was created.
    assert_eq!(recorded_root(&db).await.unwrap(), Some(recorded.clone()));
    assert!(!other.exists());

    // The clone alone is still live data.
    sqlx::query("UPDATE workspace SET status = 'cleaned' WHERE id = ?")
        .bind(&workspace.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(settle(&db, &dirs.explicit_choice(&other)).await.is_err());

    // Only cleaned rows left: the operator's change is taken and recorded.
    std::fs::remove_dir_all(recorded.join(".repos")).unwrap();
    let settled = settle(&db, &dirs.explicit_choice(&other)).await.unwrap();
    assert_eq!((settled.root, settled.recorded_now), (other.clone(), true));
    assert_eq!(recorded_root(&db).await.unwrap(), Some(other));
}

#[tokio::test]
async fn a_running_run_under_the_recorded_root_refuses_the_start() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let recorded = dirs.default_root();
    settle(&db, &dirs.default_choice()).await.unwrap();
    let workspace = seed_workspace_under(&db, &dirs, &recorded).await;
    sqlx::query("UPDATE workspace SET status = 'cleaned' WHERE id = ?")
        .bind(&workspace.id)
        .execute(db.pool())
        .await
        .unwrap();
    assert!(LiveData::under(&db, &recorded).await.unwrap().is_empty());
    seed_running_execution(&db, &workspace).await;
    let live = LiveData::under(&db, &recorded).await.unwrap();
    assert_eq!((live.workspaces, live.running), (0, 1));
    let other = dirs.data.path().join("other-root");
    let refused = settle(&db, &dirs.explicit_choice(&other))
        .await
        .unwrap_err();
    assert!(
        refused.to_string().contains("1 running run(s)"),
        "{refused}"
    );
}

#[tokio::test]
async fn an_unfinished_move_refuses_the_start() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    std::fs::write(dirs.data.path().join(JOURNAL_FILE), "{}").unwrap();
    let refused = settle(&db, &dirs.default_choice()).await.unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("forge --migrate-workspace-root"),
        "{refused}"
    );
    assert_eq!(recorded_root(&db).await.unwrap(), None);
}

#[cfg(unix)]
#[tokio::test]
async fn a_root_reached_through_a_link_is_the_same_root() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let real = dirs.data.path().join("real");
    std::fs::create_dir_all(&real).unwrap();
    let link = dirs.data.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    settle(&db, &dirs.explicit_choice(&link)).await.unwrap();
    seed_workspace_under(&db, &dirs, &link).await;
    // Naming the resolved directory is not a change of root.
    let resolved = real.canonicalize().unwrap();
    let settled = settle(&db, &dirs.explicit_choice(&resolved)).await.unwrap();
    assert_eq!((settled.root, settled.recorded_now), (link, false));
}

/// A `running` execution in `workspace`, logging under its root.
pub(super) async fn seed_running_execution(db: &SqliteDb, workspace: &db::Workspace) -> String {
    let task = db::TaskRepo::get_by_id(db, &workspace.task_id, false)
        .await
        .unwrap()
        .unwrap();
    let id = db::new_uuid_v4();
    let root = Path::new(workspace.embedded_worktree_path_for_backend())
        .parent()
        .and_then(Path::parent)
        .unwrap();
    let logs =
        crate::task_service::logs::execution_logs_path(root, &task.project_id, &task.id, &id);
    let now = now_rfc3339();
    db::ExecutionRepo::create(
        db,
        db::CreateExecution {
            id: id.clone(),
            task_id: task.id.clone(),
            agent_id: None,
            role: "coder".to_owned(),
            status: db::ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: Some(logs),
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("execution creates");
    id
}
