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
            migrate_command: MIGRATE_COMMAND.to_owned(),
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
            warnings: Vec::new(),
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
            warnings: Vec::new(),
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
        system_temp_warning(&legacy, MIGRATE_COMMAND),
        issue.error,
        "the log line and the operator entry say the same thing"
    );
}

/// Every data directory on a machine used to share the temp default, so
/// directories there say nothing about this database: a fresh data
/// directory beside a real install gets its own root.
#[tokio::test]
async fn directories_in_the_temp_default_alone_are_not_this_install() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    for name in [".repos", ".forge", "3f0c2b0e-6d53-4b7e-9d0c-0d5a4f3f8a11"] {
        std::fs::create_dir_all(dirs.legacy_root().join(name)).unwrap();
    }
    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(settled.root, dirs.default_root());
    assert!(!settled.in_system_temp);
}

/// What this database recorded there is: a clone, or an execution log, even
/// when every workspace is already cleaned.
#[tokio::test]
async fn a_recorded_clone_or_log_marks_an_install_on_the_temp_default() {
    for evidence in ["log", "clone"] {
        let db = sqlite_db().await;
        let dirs = Dirs::new();
        let legacy = dirs.legacy_root();
        let workspace = seed_workspace_under(&db, &dirs, &legacy).await;
        let execution = seed_running_execution(&db, &workspace).await;
        sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
            .bind(&execution)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE workspace SET status = 'cleaned'")
            .execute(db.pool())
            .await
            .unwrap();
        if evidence == "clone" {
            sqlx::query("UPDATE execution SET logs_path = NULL")
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("UPDATE repo_location SET path = ?")
                .bind(legacy.join(".repos/clone").to_string_lossy().as_ref())
                .execute(db.pool())
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE repo_location SET path = '/elsewhere/repo'")
                .execute(db.pool())
                .await
                .unwrap();
        }
        let settled = settle(&db, &dirs.default_choice()).await.unwrap();
        assert_eq!(settled.root, legacy, "{evidence}");
        assert!(settled.in_system_temp, "{evidence}");
    }
    // Neither: only cleaned history, so this is a new install.
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    seed_workspace_under(&db, &dirs, &dirs.legacy_root()).await;
    sqlx::query("UPDATE workspace SET status = 'cleaned'")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE repo_location SET path = '/elsewhere/repo'")
        .execute(db.pool())
        .await
        .unwrap();
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
    assert!(
        refused.to_string().starts_with("migration in progress"),
        "{refused}"
    );
    assert_eq!(recorded_root(&db).await.unwrap(), None);
}

fn warnings_reported(status: &api_types::OperatorStatusResponse) -> Vec<String> {
    status
        .recent_errors
        .iter()
        .filter(|issue| issue.entity_type == "workspace_root")
        .map(|issue| issue.error.clone())
        .collect()
}

/// Upgrade shape (iii): the rows were written under another temp directory
/// than this launch has (service manager and shell, `/tmp` and macOS's
/// `/var/folders`). The root comes from the rows, never from today's temp.
#[tokio::test]
async fn an_install_written_under_another_temp_directory_keeps_its_root() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let then = TempDir::new().unwrap();
    let written_under = legacy_temp_root(then.path());
    let workspace = seed_workspace_under(&db, &dirs, &written_under).await;
    assert!(!is_under(&written_under, dirs.temp.path()));

    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(settled.root, written_under);
    assert!(settled.recorded_now && settled.in_system_temp);
    assert!(settled.warnings.is_empty(), "{:?}", settled.warnings);
    assert_eq!(
        recorded_root(&db).await.unwrap(),
        Some(written_under.clone())
    );
    assert!(Path::new(workspace.embedded_worktree_path_for_backend()).is_dir());
    assert!(!dirs.default_root().exists());
    // Still reported on the next start, whatever that launch's temp is.
    let again = settle(&db, &dirs.default_choice()).await.unwrap();
    assert!(again.in_system_temp && !again.recorded_now);
    assert!(warnings_reported(&status(&db).await)[0].contains("system temp directory"));
}

/// Upgrade shape (iii), two roots in one database: no guess. The start
/// runs on the root holding the most live workspaces, and says so.
#[tokio::test]
async fn rows_under_two_roots_start_on_the_one_holding_the_live_rows_and_warn() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let (first, second) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let busy = legacy_temp_root(first.path());
    let quiet = legacy_temp_root(second.path());
    let repos = [
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    ];
    for (root, repo) in [(&busy, &repos[0]), (&quiet, &repos[1]), (&busy, &repos[2])] {
        std::fs::create_dir_all(root).unwrap();
        let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
        let task = seed_task(&db, &project_id, None).await;
        prepare_workspace_for_test(&db, root, &task, &task.id, None)
            .await
            .expect("workspace creates");
    }
    let stored = stored_roots(&db).await.unwrap();
    assert_eq!(
        stored
            .iter()
            .map(|root| (root.root.clone(), root.workspaces))
            .collect::<Vec<_>>(),
        [
            (busy.display().to_string(), 2),
            (quiet.display().to_string(), 1)
        ]
    );
    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(settled.root, busy);
    assert_eq!(settled.warnings.len(), 1, "{:?}", settled.warnings);
    let warning = &settled.warnings[0];
    assert!(warning.contains(&quiet.display().to_string()), "{warning}");
    assert!(warning.contains("1 workspace(s)"), "{warning}");
    assert!(warning.contains(MIGRATE_COMMAND), "{warning}");
    assert!(warnings_reported(&status(&db).await).contains(warning));
}

/// Upgrade shape (ii): a root set before the upgrade is recorded as it is,
/// and rows the old release left elsewhere never refuse the start.
#[tokio::test]
async fn a_root_chosen_before_the_upgrade_is_recorded_and_never_refused() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let chosen = dirs.data.path().join("chosen");
    seed_workspace_under(&db, &dirs, &chosen).await;
    // A workspace the temp default still holds from before the root was set.
    let repo = TempDir::new().unwrap();
    let (project_id, _) = seed_project_with_real_repo(&db, repo.path()).await;
    let task = seed_task(&db, &project_id, None).await;
    std::fs::create_dir_all(dirs.legacy_root()).unwrap();
    prepare_workspace_for_test(&db, &dirs.legacy_root(), &task, &task.id, None)
        .await
        .unwrap();
    let settled = settle(&db, &dirs.explicit_choice(&chosen)).await.unwrap();
    assert_eq!((settled.root, settled.recorded_now), (chosen, true));
    assert!(!settled.in_system_temp);
    assert_eq!(settled.warnings.len(), 1);
    assert!(settled.warnings[0].contains(&dirs.legacy_root().display().to_string()));
}

/// Upgrade shape (v): the recorded root is gone (the system emptied its temp
/// directory). The start goes on: the root is made again and the loss said.
#[tokio::test]
async fn a_recorded_root_the_system_emptied_is_made_again_and_reported() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let legacy = dirs.legacy_root();
    seed_workspace_under(&db, &dirs, &legacy).await;
    settle(&db, &dirs.default_choice()).await.unwrap();
    std::fs::remove_dir_all(dirs.temp.path().join("forge")).unwrap();

    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(
        (settled.root.clone(), settled.recorded_now),
        (legacy.clone(), false)
    );
    assert!(legacy.is_dir());
    assert_eq!(settled.warnings.len(), 1, "{:?}", settled.warnings);
    assert!(
        settled.warnings[0].contains("was missing"),
        "{:?}",
        settled.warnings
    );
    assert!(settled.warnings[0].contains("1 stored workspace(s)"));
    assert!(warnings_reported(&status(&db).await).contains(&settled.warnings[0]));
    // A fresh install's root that was never made is no loss and no warning.
    let fresh = sqlite_db().await;
    let other = Dirs::new();
    settle(&fresh, &other.default_choice()).await.unwrap();
    std::fs::remove_dir_all(other.default_root()).unwrap();
    assert!(settle(&fresh, &other.default_choice())
        .await
        .unwrap()
        .warnings
        .is_empty());
}

/// Upgrade shape (vi): the data directory came from another machine or
/// path, and its recorded root cannot even be created. The refusal names
/// the exact command, with this data directory, and that command works.
#[tokio::test]
async fn a_recorded_root_that_cannot_exist_here_refuses_with_the_command_that_fixes_it() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let blocker = dirs.data.path().join("not-a-directory");
    std::fs::write(&blocker, "file").unwrap();
    let elsewhere = blocker.join("forge/worktrees");
    record_root(db.pool(), &elsewhere).await.unwrap();
    let command = format!(
        "forge --data-dir {} --migrate-workspace-root",
        dirs.data.path().display()
    );
    assert_eq!(
        RootChoice::migrate_command_for(dirs.data.path(), Path::new("/home/x/.forge")),
        command
    );
    let choice = RootChoice {
        migrate_command: command.clone(),
        ..dirs.default_choice()
    };
    let refused = settle(&db, &choice).await.unwrap_err();
    let WorkspaceRootError::Refused(message) = &refused else {
        panic!("{refused:?}");
    };
    assert!(message.contains(&format!("`{command}`")), "{message}");
    assert!(message.contains("cannot be created"), "{message}");

    let report = migrate::migrate(
        &db,
        &migrate::MigrateRequest::new(
            dirs.data.path().to_path_buf(),
            None,
            dirs.temp.path().to_path_buf(),
            0,
        ),
    )
    .await
    .unwrap();
    assert_eq!(report.nothing_to_move, None);
    let settled = settle(&db, &choice).await.unwrap();
    assert_eq!(
        settled.root,
        dirs.data.path().canonicalize().unwrap().join("worktrees")
    );
}

/// Upgrade shape (vi): a root another database adopted (two data
/// directories shared the old temp default) is used as before, and said.
#[tokio::test]
async fn a_root_another_database_adopted_starts_and_is_reported() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let legacy = dirs.legacy_root();
    seed_workspace_under(&db, &dirs, &legacy).await;
    let gc_dir = legacy.join(executors::gc::GC_DIR);
    std::fs::create_dir_all(&gc_dir).unwrap();
    std::fs::write(gc_dir.join(executors::gc::OWNER_FILE), "another-database").unwrap();
    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(settled.root, legacy);
    assert_eq!(settled.warnings.len(), 1, "{:?}", settled.warnings);
    assert!(settled.warnings[0].contains("another-database"));
}

/// A configured root that differs from a recorded one that is gone: still
/// the operator's move to make, and the refusal says nothing is left to
/// move.
#[tokio::test]
async fn a_changed_root_whose_recorded_root_is_gone_says_so() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let recorded = dirs.default_root();
    settle(&db, &dirs.default_choice()).await.unwrap();
    seed_workspace_under(&db, &dirs, &recorded).await;
    std::fs::remove_dir_all(&recorded).unwrap();
    let other = dirs.data.path().join("other-root");
    let refused = settle(&db, &dirs.explicit_choice(&other))
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("no longer exists"), "{refused}");
    assert!(
        refused.contains(&format!("{MIGRATE_COMMAND} {}", other.display())),
        "{refused}"
    );
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

/// Code that holds only a database reads the root the start recorded; a
/// database no server started on gets the fixture root, which is no
/// install's root and not an environment variable.
#[tokio::test]
async fn the_recorded_root_is_the_one_source_for_code_without_a_root() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let fixture = fixture_root();
    assert_eq!(root_of(&db).await.unwrap(), fixture);
    assert_eq!(fixture, fixture_root(), "one value per process");
    assert!(!fixture.ends_with("forge/worktrees"));
    assert!(fixture
        .to_string_lossy()
        .contains(&format!("forge-fixture-{}", std::process::id())));
    let settled = settle(&db, &dirs.default_choice()).await.unwrap();
    assert_eq!(root_of(&db).await.unwrap(), settled.root);
    // Every fixture constructor uses that same value.
    let service = crate::task_service::TaskService::new_for_test(
        std::sync::Arc::new(sqlite_db().await),
        std::sync::Arc::new(events::EventBus::new(4)),
    );
    assert_eq!(service.workspace_root(), fixture);
}
