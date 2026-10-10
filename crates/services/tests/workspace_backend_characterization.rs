use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use api_types::{ExecutionOutboxEntry, MAX_EXECUTION_OUTBOX_FILE_BYTES};
use db::{
    create_sqlite_pool, run_migrations, CreateExecution, CreateProject, CreateRepo,
    CreateRepoLocation, CreateReview, CreateTask, CreateWorkspace, CreateWorkspacePlacement,
    ExecutionRepo, ExecutionStatus, PlacementOwnerKind, PlacementSelectedBy, PlacementState,
    ProjectRepo, RepoLocationKind, RepoLocationOwnerKind, RepoLocationRepo, RepoLocationStatus,
    RepoRepo, ReviewRepo, ReviewStatus, SqliteDb, TaskRepo, WorkspacePlacementRepo, WorkspaceRepo,
    WorkspaceStatus,
};
use events::EventBus;
use serde_json::json;
use services::{
    workspace_backend::{
        DiffSpec, EmbeddedWorkspaceBackend, MergeSpec, PrepareSpec, ResetSpec, RunSpec,
        WorkspaceBackend, WorkspaceBackendError, WorkspaceBackendRouter, WorkspaceRunPurpose,
    },
    MergeOutcome, MergeService, ServiceError,
};
use tempfile::TempDir;

const TASK_ID: &str = "11111111-1111-4111-8111-111111111111";
const PROJECT_ID: &str = "22222222-2222-4222-8222-222222222222";
const REPO_ID: &str = "33333333-3333-4333-8333-333333333333";
const WORKSPACE_ID: &str = "44444444-4444-4444-8444-444444444444";
const EXECUTION_ID: &str = "55555555-5555-4555-8555-555555555555";
const REVIEWER_ID: &str = "66666666-6666-4666-8666-666666666666";
const TITLE: &str = "Add greeting";
const DESCRIPTION: &str = "Add a greeting file without changing the existing file.";
const NOW: &str = "2026-09-29T00:00:00Z";

// Snapshot every table, including rows the effect must leave alone. SQLite's
// quote() preserves NULL/blob/text distinctions; sorting removes row order.
async fn table_digests(db: &SqliteDb) -> BTreeMap<String, String> {
    use sha2::{Digest, Sha256};
    let tables: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .fetch_all(db.pool())
            .await
            .unwrap();
    let mut digests = BTreeMap::new();
    for table in tables {
        let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info(?)")
            .bind(&table)
            .fetch_all(db.pool())
            .await
            .unwrap();
        let expression = columns
            .iter()
            .map(|column| format!("quote(\"{}\")", column.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(" || '|' || ");
        async fn digest(db: &SqliteDb, table: &str, expression: &str) -> String {
            let mut rows: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT {expression} FROM \"{}\"",
                table.replace('"', "\"\""),
            ))
            .fetch_all(db.pool())
            .await
            .unwrap();
            rows.sort();
            hex::encode(Sha256::digest(serde_json::to_vec(&rows).unwrap()))
        }
        digests.insert(table.clone(), digest(db, &table, &expression).await);
        let volatile: &[&str] = match table.as_str() {
            "execution" => &["before_sha", "after_sha", "updated_at"],
            "project" => &["list_revision"],
            "usage_ledger_revision" => &["execution_revision"],
            _ => &[],
        };
        if !volatile.is_empty() {
            let stable = columns
                .iter()
                .filter(|column| !volatile.contains(&column.as_str()))
                .map(|column| format!("quote(\"{}\")", column.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(" || '|' || ");
            digests.insert(
                format!("{table}::stable"),
                digest(db, &table, &stable).await,
            );
        }
        if let Some(column) = match table.as_str() {
            "project" => Some("list_revision"),
            "usage_ledger_revision" => Some("execution_revision"),
            _ => None,
        } {
            let revision: i64 = sqlx::query_scalar(&format!("SELECT {column} FROM {table}"))
                .fetch_one(db.pool())
                .await
                .unwrap();
            digests.insert(format!("{table}::{column}"), revision.to_string());
        }
    }
    digests
}

async fn assert_execution_evidence_writes(
    db: &SqliteDb,
    mut before: BTreeMap<String, String>,
    mut after: BTreeMap<String, String>,
    updates: i64,
) {
    // Pin the existing trigger effects instead of excluding their entire data.
    for table in [
        "execution",
        "project",
        "usage_ledger_revision",
        "usage_changed_execution",
    ] {
        assert_ne!(before.remove(table), after.remove(table), "{table}");
    }
    for key in [
        "project::list_revision",
        "usage_ledger_revision::execution_revision",
    ] {
        let old: i64 = before.remove(key).unwrap().parse().unwrap();
        let new: i64 = after.remove(key).unwrap().parse().unwrap();
        assert_eq!(new, old + updates, "{key}");
    }
    let revision: i64 = sqlx::query_scalar("SELECT execution_revision FROM usage_ledger_revision")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let changed: Vec<(String, i64)> =
        sqlx::query_as("SELECT id,revision FROM usage_changed_execution")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(changed, vec![(EXECUTION_ID.into(), revision)]);
    let changed_tables: Vec<_> = before
        .keys()
        .filter(|key| before.get(*key) != after.get(*key))
        .collect();
    assert!(
        changed_tables.is_empty(),
        "unexpected changes: {changed_tables:?}"
    );
    assert_eq!(before, after);
}

struct Fixture {
    temp: TempDir,
    repo: PathBuf,
    worktree: PathBuf,
    base_sha: String,
    candidate_sha: String,
}

async fn run_git(path: &Path, args: &[&str]) -> String {
    let output = tokio::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env("GIT_AUTHOR_NAME", "Forge Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@forge.dev")
        .env("GIT_COMMITTER_NAME", "Forge Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@forge.dev")
        .env("GIT_AUTHOR_DATE", NOW)
        .env("GIT_COMMITTER_DATE", NOW)
        .env("LC_ALL", "C")
        .output()
        .await
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git output is UTF-8")
        .trim()
        .to_owned()
}

async fn commit(path: &Path, message: &str) -> String {
    run_git(path, &["add", "."]).await;
    run_git(path, &["commit", "-m", message]).await;
    run_git(path, &["rev-parse", "HEAD"]).await
}

async fn fixture() -> Fixture {
    let temp = TempDir::new().expect("fixture directory creates");
    let repo = temp.path().join("checkout");
    std::fs::create_dir_all(&repo).unwrap();
    run_git(&repo, &["init", "--object-format=sha1"]).await;
    run_git(&repo, &["symbolic-ref", "HEAD", "refs/heads/main"]).await;
    for (key, value) in [
        ("user.name", "Forge Fixture"),
        ("user.email", "fixture@forge.dev"),
        ("core.autocrlf", "false"),
        ("core.hooksPath", "/dev/null"),
        ("commit.gpgsign", "false"),
        ("diff.algorithm", "myers"),
        ("diff.mnemonicPrefix", "false"),
        ("diff.noprefix", "false"),
        ("merge.conflictStyle", "merge"),
        ("rerere.enabled", "false"),
    ] {
        run_git(&repo, &["config", key, value]).await;
    }
    std::fs::write(repo.join("file.txt"), "base\n").unwrap();
    let base_sha = commit(&repo, "initial fixture").await;
    let worktree = temp.path().join("worktrees").join(TASK_ID).join("repo");
    git::create_worktree(&repo, &workspace::task_branch_name(TASK_ID), &worktree)
        .await
        .unwrap();
    std::fs::write(worktree.join("feature.txt"), "hello\n").unwrap();
    let candidate_sha = commit(&worktree, "greeting fixture").await;
    Fixture {
        temp,
        repo,
        worktree,
        base_sha,
        candidate_sha,
    }
}

async fn seed(fixture: &Fixture, review_config: Option<String>) -> Arc<SqliteDb> {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: PROJECT_ID.to_owned(),
            name: "Workspace fixture".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_owned(),
            primary_repo_id: None,
            owner_id: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    RepoRepo::create(
        &*db,
        CreateRepo {
            id: REPO_ID.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            name: "repo".to_owned(),
            remote_url: Some(fixture.repo.display().to_string()),
            local_path: Some(fixture.repo.display().to_string()),
            default_branch: "main".to_owned(),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    ProjectRepo::update_at_version(
        &*db,
        db::UpdateProject {
            id: PROJECT_ID.to_owned(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(REPO_ID.to_owned())),
            paused_at: None,
            updated_at: NOW.to_owned(),
        },
        1,
        None,
    )
    .await
    .unwrap();
    TaskRepo::create(
        &*db,
        CreateTask {
            id: TASK_ID.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: TITLE.to_owned(),
            description: Some(DESCRIPTION.to_owned()),
            task_type: "task".to_owned(),
            status: "review".to_owned(),
            is_automation: false,
            priority: 0,
            task_state_config: review_config,
            merge_config: None,
            plan: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: WORKSPACE_ID.to_owned(),
            task_id: TASK_ID.to_owned(),
            repo_id: REPO_ID.to_owned(),
            worktree_path: fixture.worktree.display().to_string(),
            branch: workspace::task_branch_name(TASK_ID),
            status: WorkspaceStatus::Ready,
            before_sha: Some(fixture.base_sha.clone()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    ExecutionRepo::create(
        &*db,
        execution(EXECUTION_ID, "executor", ExecutionStatus::Completed),
    )
    .await
    .unwrap();
    db
}

fn execution(id: &str, role: &str, status: ExecutionStatus) -> CreateExecution {
    CreateExecution {
        id: id.to_owned(),
        task_id: TASK_ID.to_owned(),
        agent_id: None,
        role: role.to_owned(),
        status,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: (role == "reviewer").then(|| EXECUTION_ID.to_owned()),
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: Some(WORKSPACE_ID.to_owned()),
        created_at: NOW.to_owned(),
        updated_at: NOW.to_owned(),
    }
}

fn merge_service(db: &Arc<SqliteDb>, fixture: &Fixture) -> Arc<MergeService> {
    Arc::new(MergeService::new_for_test(
        Arc::clone(db),
        Arc::new(EventBus::new(16)),
        fixture.temp.path().join("worktrees"),
    ))
}

async fn embedded(
    db: &Arc<SqliteDb>,
    fixture: &Fixture,
) -> (Arc<EmbeddedWorkspaceBackend>, db::WorkspacePlacement) {
    RepoLocationRepo::create(
        &**db,
        CreateRepoLocation {
            id: "location".to_owned(),
            repo_id: REPO_ID.to_owned(),
            owner_kind: RepoLocationOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            path: fixture.repo.display().to_string(),
            kind: RepoLocationKind::PrimaryCheckout,
            is_default: true,
            status: RepoLocationStatus::Ready,
            last_verified_at: Some(NOW.to_owned()),
            last_error: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    let placement = WorkspacePlacementRepo::create(
        &**db,
        CreateWorkspacePlacement {
            id: "placement".to_owned(),
            workspace_id: WORKSPACE_ID.to_owned(),
            task_id: TASK_ID.to_owned(),
            agent_id: None,
            owner_kind: PlacementOwnerKind::Server,
            daemon_id: None,
            runtime_id: None,
            repo_location_id: "location".to_owned(),
            execution_daemon_id: None,
            workspace_handle: Some(fixture.worktree.display().to_string()),
            generation: 1,
            state: PlacementState::Ready,
            selected_by: PlacementSelectedBy::Backfill,
            selection_reason: "{}".to_owned(),
            reserved_until: None,
            disconnected_at: None,
            failure_cause: None,
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
    )
    .await
    .unwrap();
    (
        Arc::new(EmbeddedWorkspaceBackend::new(
            Arc::clone(db),
            merge_service(db, fixture),
            fixture.temp.path().join("worktrees"),
        )),
        placement,
    )
}

#[tokio::test]
async fn reviewer_prompt_bytes_for_server_workspace() {
    let fixture = fixture().await;
    let db = seed(
        &fixture,
        Some(
            json!({"review":{"setup_steps":["printf ready"],"ci_steps":["test -f feature.txt"]}})
                .to_string(),
        ),
    )
    .await;
    ExecutionRepo::create(
        &*db,
        execution(REVIEWER_ID, "reviewer", ExecutionStatus::Running),
    )
    .await
    .unwrap();
    ReviewRepo::create(&*db, CreateReview {
        id: "review".to_owned(), task_id: TASK_ID.to_owned(), execution_id: REVIEWER_ID.to_owned(), attempt_number: 1,
        status: ReviewStatus::Running, step_results_json: json!({"ci_steps":[{
            "index":0,"command":"test -f feature.txt","exit_code":0,"stderr_tail":"","output_tail":"",
            "started_at":NOW,"finished_at":NOW
        }]}).to_string(), started_at: NOW.to_owned(), created_at: NOW.to_owned(), updated_at: NOW.to_owned(),
    }).await.unwrap();
    let diff = review::read_git_diff(&fixture.worktree, "main")
        .await
        .unwrap();
    let prompt = review::auditor::render_auditor_prompt(TITLE, Some(DESCRIPTION), &diff, None);
    let prompt = review::contract::prepare_prompt(
        &db,
        REVIEWER_ID,
        TASK_ID,
        &fixture.worktree,
        true,
        false,
        prompt,
    )
    .await
    .unwrap();

    assert_eq!(
        prompt.as_bytes(),
        include_bytes!("fixtures/server_workspace_reviewer.prompt.snap")
    );
}

#[tokio::test]
async fn clean_merge_outcome() {
    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    embedded(&db, &fixture).await;
    let before = table_digests(&db).await;
    let outcome = merge_service(&db, &fixture).merge(TASK_ID).await.unwrap();
    assert_eq!(
        outcome,
        MergeOutcome::Done {
            before_sha: fixture.base_sha.clone(),
            after_sha: fixture.candidate_sha.clone(),
            branch: "main".to_owned(),
        }
    );
    assert_eq!(
        git::get_current_sha(&fixture.repo).await.unwrap(),
        fixture.candidate_sha
    );
    let execution = ExecutionRepo::get_by_id(&*db, EXECUTION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.before_sha, Some(fixture.candidate_sha.clone()));
    assert_eq!(execution.after_sha, Some(fixture.candidate_sha));
    assert_execution_evidence_writes(&db, before, table_digests(&db).await, 2).await;
}

#[tokio::test]
async fn resolved_review_io_preserves_ci_limits_and_git_evidence() {
    use review::{CommandLimits, ReviewWorkspace};

    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let (backend, _) = embedded(&db, &fixture).await;
    let router = WorkspaceBackendRouter::new(backend);
    let workspace = WorkspaceRepo::get_by_id(&*db, WORKSPACE_ID)
        .await
        .unwrap()
        .unwrap();
    let resolved = router.resolve(&db, &workspace).await.unwrap();
    let command = "printf '%8192s' x";
    let output = resolved.run(command, &BTreeMap::new(), None).await.unwrap();
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.stdout.len(), 8192);
    let bounded = resolved
        .run(
            command,
            &BTreeMap::new(),
            Some(CommandLimits {
                timeout_secs: 5,
                max_output_bytes: 4096,
            }),
        )
        .await;
    assert!(
        matches!(bounded, Err(review::ReviewError::Workspace(reason))
        if reason == "review command output exceeds size budget")
    );
    assert_eq!(
        resolved.diff("main").await.unwrap(),
        review::read_git_diff(&fixture.worktree, "main")
            .await
            .unwrap()
    );
    assert_eq!(
        resolved
            .git_read(&["rev-parse", "HEAD"], false)
            .await
            .unwrap()
            .unwrap()
            .trim(),
        fixture.candidate_sha
    );
}

#[tokio::test]
async fn workspace_hook_preserves_git_environment_and_redacts_only_project_values_once() {
    use services::lifecycle::{LifecycleHookContext, LifecycleHookRunner};

    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let (backend, _) = embedded(&db, &fixture).await;
    let router = WorkspaceBackendRouter::new(backend);
    let workspace = WorkspaceRepo::get_by_id(&*db, WORKSPACE_ID)
        .await
        .unwrap()
        .unwrap();
    let resolved = router.resolve(&db, &workspace).await.unwrap();
    let ctx = LifecycleHookContext {
        event: api_types::LifecycleEvent::BeforeWork,
        task_id: TASK_ID.to_owned(),
        task_title: TITLE.to_owned(),
        task_status: "in_progress".to_owned(),
        previous_status: "todo".to_owned(),
        project_id: PROJECT_ID.to_owned(),
        project_name: "Workspace fixture".to_owned(),
        repo_path: fixture.repo.display().to_string(),
        worktree_path: Some(fixture.worktree.display().to_string()),
        agent_id: None,
        execution_id: Some(EXECUTION_ID.to_owned()),
        log_dir: None,
        env: BTreeMap::from([
            ("GIT_DIR".to_owned(), "inherited".to_owned()),
            ("FIXTURE_TOKEN".to_owned(), "REDACTED".to_owned()),
        ]),
    };
    let run = LifecycleHookRunner::test_workspace_script_hook(
        &ctx,
        0,
        "test \"$GIT_DIR\" = inherited && printf '%s|%s' \"$FORGE_TASK_TITLE\" \"$FIXTURE_TOKEN\"",
        5,
        &resolved,
    )
    .await
    .unwrap();
    assert_eq!(run.exit_code, Some(0));
    assert_eq!(run.stdout, format!("{TITLE}|[REDACTED]"));
}

#[tokio::test]
async fn unplaced_server_workspace_records_one_authoritative_scheduler_placement() {
    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let path = fixture.worktree.display().to_string();
    assert!(WorkspaceRepo::task_owns_embedded_path(&*db, TASK_ID, &path)
        .await
        .unwrap());
    assert!(
        !WorkspaceRepo::task_owns_embedded_path(&*db, TASK_ID, "/unowned")
            .await
            .unwrap()
    );
    let backend = Arc::new(EmbeddedWorkspaceBackend::new(
        Arc::clone(&db),
        merge_service(&db, &fixture),
        fixture.temp.path().join("worktrees"),
    ));
    let router = WorkspaceBackendRouter::new(backend);
    let workspace = WorkspaceRepo::get_by_id(&*db, WORKSPACE_ID)
        .await
        .unwrap()
        .unwrap();
    let first = router.resolve(&db, &workspace).await.unwrap();
    let second = router.resolve(&db, &workspace).await.unwrap();
    assert_eq!(first.placement, second.placement);
    assert_eq!(first.placement.owner_kind, PlacementOwnerKind::Server);
    assert_eq!(first.placement.selected_by, PlacementSelectedBy::Scheduler);
    assert_eq!(
        first.placement.workspace_handle.as_deref(),
        Some(path.as_str())
    );
    let location = RepoLocationRepo::get_by_id(&*db, &first.placement.repo_location_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(location.kind, RepoLocationKind::PrimaryCheckout);
    assert_eq!(location.path, fixture.repo.display().to_string());
    let updated = sqlx::query(
        "UPDATE workspace_placement SET workspace_handle = ?, version = version + 1
         WHERE id = ? AND version = ?",
    )
    .bind("/recorded-server-handle")
    .bind(&first.placement.id)
    .bind(first.placement.version)
    .execute(db.pool())
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    assert!(
        !WorkspaceRepo::task_owns_embedded_path(&*db, TASK_ID, &path)
            .await
            .unwrap()
    );
    assert!(
        WorkspaceRepo::task_owns_embedded_path(&*db, TASK_ID, "/recorded-server-handle")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn owner_harvested_outbox_ingests_bytes_without_owner_paths_and_replays_once() {
    use api_types::{
        ExecutionOutboxArtifact, ExecutionOutboxEvidenceKind, ExecutionOutboxWorklogKind,
    };
    use services::{native_tools::ExecutionOutboxInput, CoordinationToolProvider};

    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    let media_root = fixture.temp.path().join("media");
    provider.set_media_root(media_root.clone());
    let input = ExecutionOutboxInput {
        task_id: TASK_ID,
        execution_id: EXECUTION_ID,
        agent_id: "outbox-agent",
        role: Some("executor"),
        worktree_path: "/owner-only/nonexistent/worktree",
    };
    let entries = vec![
        ExecutionOutboxEntry::Worklog {
            position: "2".to_owned(),
            kind: ExecutionOutboxWorklogKind::Validation,
            summary: "Owner validation passed".to_owned(),
        },
        ExecutionOutboxEntry::Evidence {
            position: "3".to_owned(),
            kind: ExecutionOutboxEvidenceKind::Report,
            caption: "Owner proof".to_owned(),
            path: Some("/owner-only/nonexistent/report.md".to_owned()),
            content: None,
            artifact: Some(ExecutionOutboxArtifact {
                filename: "report.md".to_owned(),
                content_type: "text/markdown".to_owned(),
                bytes: b"owner evidence\n".to_vec(),
            }),
        },
    ];
    for _ in 0..2 {
        let report = provider
            .ingest_execution_outbox_entries(&input, entries.clone())
            .await;
        assert!(report.rejected.is_empty(), "{report:?}");
        assert_eq!(report.worklog_entries, 1);
        assert_eq!(report.evidence_items, 1);
    }
    let comments = db::TaskCommentRepo::list_comments(
        &*db,
        TASK_ID,
        db::PageRequest {
            cursor: None,
            limit: 50,
            include_total: false,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
        },
    )
    .await
    .unwrap()
    .items;
    assert_eq!(comments.len(), 2);
    assert!(comments.iter().all(|comment| {
        comment.execution_id.as_deref() == Some(EXECUTION_ID)
            && comment.role.as_deref() == Some("executor")
            && comment.author_id.as_deref() == Some("outbox-agent")
    }));
    let media = db::TaskMediaRepo::list_active_media_for_task(&*db, TASK_ID)
        .await
        .unwrap();
    assert_eq!(media.len(), 1);
    assert_eq!(
        std::fs::read(media_root.join(&media[0].storage_key)).unwrap(),
        b"owner evidence\n"
    );
}

#[tokio::test]
async fn conflict_merge_outcome_and_abort() {
    let fixture = fixture().await;
    std::fs::write(fixture.worktree.join("file.txt"), "task change\n").unwrap();
    let candidate = commit(&fixture.worktree, "task change").await;
    std::fs::write(fixture.repo.join("file.txt"), "target change\n").unwrap();
    let target = commit(&fixture.repo, "target change").await;
    let db = seed(&fixture, None).await;
    embedded(&db, &fixture).await;
    let before = table_digests(&db).await;
    let outcome = merge_service(&db, &fixture).merge(TASK_ID).await.unwrap();
    assert_eq!(outcome, MergeOutcome::Conflict {
        target_branch: "main".to_owned(),
        details: "Auto-merging file.txt\nCONFLICT (content): Merge conflict in file.txt\nAutomatic merge failed; fix conflicts and then commit the result.\n".to_owned(),
        conflict_paths: vec![PathBuf::from("file.txt")],
    });
    assert_eq!(git::get_current_sha(&fixture.repo).await.unwrap(), target);
    assert!(git::is_worktree_clean(&fixture.repo).await.unwrap());
    assert!(!git::detect_interrupted_merge(&fixture.repo).await.unwrap());
    let execution = ExecutionRepo::get_by_id(&*db, EXECUTION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.before_sha, Some(candidate));
    assert_eq!(execution.after_sha, None);
    assert_execution_evidence_writes(&db, before, table_digests(&db).await, 1).await;
}

#[tokio::test]
async fn dirty_target_merge_outcome_preserves_target() {
    let fixture = fixture().await;
    std::fs::write(fixture.repo.join("file.txt"), "uncommitted target\n").unwrap();
    let db = seed(&fixture, None).await;
    embedded(&db, &fixture).await;
    let before = table_digests(&db).await;
    assert_eq!(
        merge_service(&db, &fixture).merge(TASK_ID).await.unwrap(),
        MergeOutcome::TargetDirty {
            files: vec!["file.txt".to_owned()]
        }
    );
    assert_eq!(
        git::get_current_sha(&fixture.repo).await.unwrap(),
        fixture.base_sha
    );
    assert_eq!(
        std::fs::read_to_string(fixture.repo.join("file.txt")).unwrap(),
        "uncommitted target\n"
    );
    let execution = ExecutionRepo::get_by_id(&*db, EXECUTION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.before_sha, None);
    assert_eq!(execution.after_sha, None);
    assert_eq!(before, table_digests(&db).await);
}

#[tokio::test]
async fn dirty_task_merge_writes_nothing() {
    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    embedded(&db, &fixture).await;
    std::fs::write(fixture.worktree.join("file.txt"), "uncommitted task\n").unwrap();
    let before = table_digests(&db).await;
    let bus = Arc::new(EventBus::new(16));
    let mut events = bus.subscribe();
    let service =
        MergeService::new_for_test(db.clone(), bus, fixture.temp.path().join("worktrees"));
    assert_eq!(
        service.merge(TASK_ID).await.unwrap(),
        MergeOutcome::Dirty {
            files: vec!["file.txt".into()]
        }
    );
    assert_eq!(before, table_digests(&db).await);
    assert!(events.try_recv().is_err());
    assert_eq!(
        git::get_current_sha(&fixture.worktree).await.unwrap(),
        fixture.candidate_sha
    );
    assert_eq!(
        git::get_current_sha(&fixture.repo).await.unwrap(),
        fixture.base_sha
    );
}

#[tokio::test]
async fn server_rebase_clean_conflict_and_restart_write_nothing() {
    for case in ["clean", "conflict", "restart", "abort"] {
        let fixture = fixture().await;
        let db = seed(&fixture, None).await;
        let (backend, placement) = embedded(&db, &fixture).await;
        if case != "clean" {
            std::fs::write(fixture.worktree.join("file.txt"), "task\n").unwrap();
            commit(&fixture.worktree, "task change").await;
        }
        std::fs::write(fixture.repo.join("file.txt"), "target\n").unwrap();
        let target = commit(&fixture.repo, "target change").await;
        if matches!(case, "restart" | "abort") {
            assert!(matches!(
                git::rebase(&fixture.worktree, "main").await,
                Err(git::GitError::MergeConflict { .. })
            ));
        }
        let before = table_digests(&db).await;
        let resolved = services::workspace_backend::ResolvedWorkspace { placement, backend };
        let result = resolved
            .rebase_target("main", case != "abort")
            .await
            .unwrap();
        match case {
            "clean" => assert!(matches!(
                result,
                api_types::WorkspaceOwnerOperationOutcome::Rebased
            )),
            "abort" => assert!(
                matches!(result, api_types::WorkspaceOwnerOperationOutcome::Conflict { details, conflict_paths } if details == "aborted interrupted rebase" && conflict_paths.is_empty())
            ),
            _ => assert!(
                matches!(result, api_types::WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths == vec!["file.txt"])
            ),
        }
        assert!(!git::detect_rebase_in_progress(&fixture.worktree)
            .await
            .unwrap());
        assert!(git::is_worktree_clean(&fixture.worktree).await.unwrap());
        assert_eq!(git::get_current_sha(&fixture.repo).await.unwrap(), target);
        assert_eq!(before, table_digests(&db).await);
        if case == "clean" {
            let head = git::get_current_sha(&fixture.worktree).await.unwrap();
            assert_eq!(
                merge_service(&db, &fixture).merge(TASK_ID).await.unwrap(),
                MergeOutcome::Done {
                    before_sha: target,
                    after_sha: head,
                    branch: "main".into(),
                }
            );
        }
    }
}

#[tokio::test]
async fn embedded_workspace_operations_delegate_to_existing_paths() {
    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let (backend, placement) = embedded(&db, &fixture).await;
    let prepared = backend
        .prepare(
            &placement,
            &PrepareSpec {
                base_ref: "main".to_owned(),
            },
        )
        .await
        .unwrap();
    assert_eq!(prepared.handle, fixture.worktree.display().to_string());
    assert_eq!(prepared.base_sha, fixture.base_sha);
    let state = backend.describe(&placement).await.unwrap();
    assert_eq!(state.head_sha, Some(fixture.candidate_sha.clone()));
    assert!(state.exists && !state.dirty && !state.locked);
    assert_eq!(state.branch, Some(workspace::task_branch_name(TASK_ID)));
    assert!(state.active_execution_ids.is_empty() && state.journaled_execution_ids.is_empty());
    let diff = backend
        .diff(
            &placement,
            &DiffSpec {
                base_ref: "main".to_owned(),
                head_ref: None,
                max_bytes: usize::MAX,
            },
        )
        .await
        .unwrap();
    let original = services::DiffService::new_for_test(Arc::clone(&db))
        .workspace_diff(WORKSPACE_ID)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(diff.response).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    assert!(!diff.truncated);
    std::fs::write(
        fixture.worktree.parent().unwrap().join("plan.md"),
        "- [ ] greeting\n",
    )
    .unwrap();
    assert_eq!(
        backend
            .read(&placement, "../plan.md", 64)
            .await
            .unwrap()
            .as_slice(),
        b"- [ ] greeting\n"
    );
    assert!(backend
        .read(&placement, "../../outside.txt", 64)
        .await
        .is_err());
    assert!(backend.read(&placement, "feature.txt", 2).await.is_err());
    let result = backend
        .run(
            &placement,
            &RunSpec {
                purpose: WorkspaceRunPurpose::CiStep,
                command: "printf '%s' \"$FIXTURE_VALUE\"; printf err >&2; exit 7".to_owned(),
                env: BTreeMap::from([("FIXTURE_VALUE".to_owned(), "fixture-secret".to_owned())]),
                timeout_secs: 5,
                max_output_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(result.exit_code, 7);
    assert!(!result.stdout_tail.contains("fixture-secret"));
    assert_eq!(result.stderr_tail, "err");
    std::fs::write(fixture.worktree.join("file.txt"), "dirty\n").unwrap();
    backend
        .reset(
            &placement,
            &ResetSpec {
                expected_head_sha: fixture.candidate_sha.clone(),
                base_ref: fixture.candidate_sha.clone(),
            },
        )
        .await
        .unwrap();
    assert!(git::is_worktree_clean(&fixture.worktree).await.unwrap());
    assert!(backend.cleanup(&placement).await.unwrap().removed);
    assert!(!backend.cleanup(&placement).await.unwrap().removed);
    assert!(!fixture.worktree.exists());
    assert_eq!(
        WorkspacePlacementRepo::get_by_id(&*db, &placement.id)
            .await
            .unwrap()
            .unwrap(),
        placement
    );
}

#[tokio::test]
async fn embedded_merge_reuses_the_merge_service_outcome() {
    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let (backend, placement) = embedded(&db, &fixture).await;
    assert_eq!(
        backend
            .merge(
                &placement,
                &MergeSpec {
                    target_branch: "main".to_owned(),
                    expected_target_sha: fixture.base_sha.clone(),
                    handed_off_paths: Vec::new(),
                }
            )
            .await
            .unwrap(),
        MergeOutcome::Done {
            before_sha: fixture.base_sha.clone(),
            after_sha: fixture.candidate_sha.clone(),
            branch: "main".to_owned(),
        }
    );
}

#[tokio::test]
async fn configured_router_selects_daemon_backend_from_placement() {
    use services::workspace_backend as ws;

    struct FakeDaemonBackend;

    #[async_trait::async_trait]
    impl WorkspaceBackend for FakeDaemonBackend {
        async fn prepare(
            &self,
            _: &db::WorkspacePlacement,
            _: &PrepareSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn describe(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::WorkspaceState> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn run(&self, _: &db::WorkspacePlacement, _: &RunSpec) -> ws::Result<ws::RunResult> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn diff(&self, _: &db::WorkspacePlacement, _: &DiffSpec) -> ws::Result<ws::Diff> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn read(&self, _: &db::WorkspacePlacement, _: &str, _: u64) -> ws::Result<Vec<u8>> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn merge(
            &self,
            _: &db::WorkspacePlacement,
            _: &MergeSpec,
        ) -> ws::Result<MergeOutcome> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn reset(
            &self,
            _: &db::WorkspacePlacement,
            _: &ResetSpec,
        ) -> ws::Result<ws::PreparedWorkspace> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn cleanup(&self, _: &db::WorkspacePlacement) -> ws::Result<ws::CleanupAck> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn harvest_outbox(
            &self,
            _: &db::WorkspacePlacement,
            _: &str,
        ) -> ws::Result<ws::OutboxHarvest> {
            unreachable!("routing does not perform workspace I/O")
        }

        async fn consume_outbox(&self, _: &db::WorkspacePlacement, _: &str) -> ws::Result<()> {
            unreachable!("routing does not perform workspace I/O")
        }
    }

    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let (embedded, mut placement) = embedded(&db, &fixture).await;
    let embedded: Arc<dyn WorkspaceBackend> = embedded;
    let daemon: Arc<dyn WorkspaceBackend> = Arc::new(FakeDaemonBackend);
    let router =
        WorkspaceBackendRouter::new(Arc::clone(&embedded)).with_daemon(Arc::clone(&daemon));
    assert!(Arc::ptr_eq(
        &router.for_placement(&placement).unwrap(),
        &embedded,
    ));
    placement.owner_kind = PlacementOwnerKind::Daemon;
    assert!(Arc::ptr_eq(
        &router.for_placement(&placement).unwrap(),
        &daemon,
    ));
}

#[tokio::test]
async fn stale_placement_and_daemon_owner_are_rejected() {
    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let (backend, placement) = embedded(&db, &fixture).await;
    let router = WorkspaceBackendRouter::new(backend.clone());
    assert!(router.for_placement(&placement).is_ok());
    let mut daemon = placement.clone();
    daemon.owner_kind = PlacementOwnerKind::Daemon;
    assert!(matches!(
        router.for_placement(&daemon),
        Err(WorkspaceBackendError::OwnerUnsupported { .. })
    ));
    assert!(matches!(
        backend.describe(&daemon).await,
        Err(WorkspaceBackendError::WrongOwner { .. })
    ));
    sqlx::query("UPDATE workspace_placement SET generation = generation + 1, version = version + 1 WHERE id = ? AND version = ?")
        .bind(&placement.id).bind(placement.version).execute(db.pool()).await.unwrap();
    let error = backend.describe(&placement).await.unwrap_err();
    assert!(matches!(
        error,
        WorkspaceBackendError::StaleGeneration {
            expected: 1,
            actual: 2,
            ..
        }
    ));
    assert!(matches!(
        ServiceError::from(error),
        ServiceError::Conflict(_)
    ));
}

#[tokio::test]
async fn outbox_harvest_is_bounded_preserves_line_ids_and_captures_bytes() {
    let fixture = fixture().await;
    let db = seed(&fixture, None).await;
    let (backend, placement) = embedded(&db, &fixture).await;
    let outbox = executors::execution_outbox_path(&fixture.worktree, EXECUTION_ID).unwrap();
    std::fs::create_dir_all(&outbox).unwrap();
    std::fs::write(
        outbox.join(executors::OUTBOX_WORKLOG_FILE),
        "\n{\"kind\":\"progress\",\"summary\":\" completed greeting \"}\nnot-json\n",
    )
    .unwrap();
    std::fs::write(outbox.join("report.md"), "evidence\n").unwrap();
    std::fs::write(
        outbox.join(executors::OUTBOX_EVIDENCE_FILE),
        format!(
            "{}\n",
            json!({"kind":"report","caption":"greeting proof","path":outbox.join("report.md")})
        ),
    )
    .unwrap();
    let harvested = backend
        .harvest_outbox(&placement, EXECUTION_ID)
        .await
        .unwrap();
    assert_eq!(harvested.entries.len(), 2);
    assert!(
        matches!(&harvested.entries[0], ExecutionOutboxEntry::Worklog { position, summary, .. } if position == "2" && summary == "completed greeting")
    );
    assert!(
        matches!(&harvested.entries[1], ExecutionOutboxEntry::Evidence { position, artifact: Some(artifact), .. } if position == "1" && artifact.bytes.as_slice() == b"evidence\n" && artifact.filename == "report.md")
    );
    assert_eq!(harvested.rejected.len(), 1);
    assert!(outbox.exists());
    assert_eq!(
        backend
            .harvest_outbox(&placement, EXECUTION_ID)
            .await
            .unwrap(),
        harvested
    );
    std::fs::write(
        outbox.join(executors::OUTBOX_WORKLOG_FILE),
        (0..201)
            .map(|_| "{\"kind\":\"progress\",\"summary\":\"ok\"}\n")
            .collect::<String>(),
    )
    .unwrap();
    assert_eq!(
        backend
            .harvest_outbox(&placement, EXECUTION_ID)
            .await
            .unwrap()
            .entries
            .len(),
        201
    );
    std::fs::write(
        outbox.join(executors::OUTBOX_WORKLOG_FILE),
        vec![b' '; MAX_EXECUTION_OUTBOX_FILE_BYTES as usize + 1],
    )
    .unwrap();
    let oversized = backend
        .harvest_outbox(&placement, EXECUTION_ID)
        .await
        .unwrap();
    assert_eq!(oversized.entries.len(), 1);
    assert_eq!(oversized.rejected.len(), 1);
    assert!(backend
        .harvest_outbox(&placement, "../escape")
        .await
        .unwrap()
        .entries
        .is_empty());
}

fn effect_binding(
    placement: &db::WorkspacePlacement,
) -> services::integration_effects::EffectWorkspace {
    services::integration_effects::EffectWorkspace {
        workspace_id: placement.workspace_id.clone(),
        placement_id: placement.id.clone(),
        generation: placement.generation,
        owner: services::integration_effects::EffectOwner::Server,
        handle: placement.workspace_handle.clone().unwrap(),
    }
}

#[tokio::test]
async fn merge_primitives_return_facts_without_any_table_or_event_write() {
    use services::integration_effects::merge::*;
    for case in [
        "clean",
        "candidate",
        "target",
        "conflict",
        "dirty_task",
        "dirty_target",
    ] {
        let fixture = fixture().await;
        match case {
            "candidate" => {
                run_git(
                    &fixture.worktree,
                    &["commit", "--allow-empty", "-m", "unreviewed"],
                )
                .await;
            }
            "target" => {
                run_git(&fixture.repo, &["commit", "--allow-empty", "-m", "target"]).await;
            }
            "conflict" => {
                std::fs::write(fixture.worktree.join("file.txt"), "task\n").unwrap();
                commit(&fixture.worktree, "task").await;
                std::fs::write(fixture.repo.join("file.txt"), "target\n").unwrap();
                commit(&fixture.repo, "target").await;
            }
            "dirty_task" => {
                std::fs::write(fixture.worktree.join("file.txt"), "dirty\n").unwrap();
            }
            "dirty_target" => {
                std::fs::write(fixture.repo.join("file.txt"), "dirty\n").unwrap();
            }
            _ => {}
        }
        let db = seed(&fixture, None).await;
        let (_, placement) = embedded(&db, &fixture).await;
        let binding = effect_binding(&placement);
        let bus = EventBus::new(16);
        let mut events = bus.subscribe();
        let before = table_digests(&db).await;
        let outcome = if let Some(outcome) = merge_cleanliness(&fixture.worktree, &fixture.repo)
            .await
            .unwrap()
        {
            outcome
        } else {
            let (before_sha, head_sha) =
                merge_heads(&fixture.repo, &fixture.worktree).await.unwrap();
            let tip = target_tip(&fixture.repo, "main").await.unwrap();
            let branch = workspace::task_branch_name(TASK_ID);
            let input = MergeEffectInput {
                workspace: &binding,
                worktree_path: &fixture.worktree,
                repo_path: &fixture.repo,
                target_branch: "main",
                task_branch: &branch,
                diagnostic_entity_id: TASK_ID,
                before_sha: &before_sha,
                expected_head_sha: &head_sha,
                observed_target_sha: &tip,
                reviewed: (case != "conflict").then(|| ReviewedMergeObject {
                    commit_sha: fixture.candidate_sha.clone(),
                    base_sha: fixture.base_sha.clone(),
                }),
            };
            match validate_merge_candidate(&input).await.unwrap() {
                MergeCandidateOutcome::Refused(outcome) => outcome,
                MergeCandidateOutcome::Ready { already_merged } => {
                    let applied = apply_merge(&input, already_merged).await.unwrap();
                    merge_result(&input, already_merged, applied).await.unwrap()
                }
            }
        };
        match case {
            "clean" => assert!(
                matches!(outcome, MergeOutcome::Done { after_sha, .. } if after_sha == fixture.candidate_sha)
            ),
            "candidate" => assert!(matches!(outcome, MergeOutcome::ReviewRequired { .. })),
            "target" => assert!(matches!(outcome, MergeOutcome::TargetMoved { .. })),
            "conflict" => {
                assert!(
                    matches!(outcome, MergeOutcome::Conflict { conflict_paths, .. } if conflict_paths == vec![PathBuf::from("file.txt")])
                );
                assert!(!git::detect_interrupted_merge(&fixture.repo).await.unwrap());
                assert!(git::is_worktree_clean(&fixture.repo).await.unwrap());
            }
            "dirty_task" => assert!(matches!(outcome, MergeOutcome::Dirty { .. })),
            "dirty_target" => assert!(matches!(outcome, MergeOutcome::TargetDirty { .. })),
            _ => unreachable!(),
        }
        assert_eq!(before, table_digests(&db).await, "{case}");
        assert!(events.try_recv().is_err());
    }
}

#[tokio::test]
async fn rebase_primitive_returns_conflict_paths_without_any_table_or_event_write() {
    use services::integration_effects::rebase::*;
    let fixture = fixture().await;
    std::fs::write(fixture.worktree.join("file.txt"), "task\n").unwrap();
    commit(&fixture.worktree, "task").await;
    std::fs::write(fixture.repo.join("file.txt"), "target\n").unwrap();
    commit(&fixture.repo, "target").await;
    let db = seed(&fixture, None).await;
    let (_, placement) = embedded(&db, &fixture).await;
    let binding = effect_binding(&placement);
    let before = table_digests(&db).await;
    let bus = EventBus::new(16);
    let mut events = bus.subscribe();
    let outcome = rebase(&RebaseEffectInput {
        workspace: &binding,
        worktree_path: &fixture.worktree,
        target_branch: "main",
        handoff_conflicts: true,
        expected_head_sha: None,
        expected_target_sha: None,
        deadline: None,
    })
    .await
    .unwrap();
    assert!(
        matches!(outcome, api_types::WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. } if conflict_paths == vec!["file.txt"])
    );
    assert!(!git::detect_rebase_in_progress(&fixture.worktree)
        .await
        .unwrap());
    assert!(git::is_worktree_clean(&fixture.worktree).await.unwrap());
    assert_eq!(
        git::paths_adding_conflict_markers(&fixture.worktree, "main", "HEAD")
            .await
            .unwrap(),
        vec!["file.txt"]
    );
    assert_eq!(before, table_digests(&db).await);
    assert!(events.try_recv().is_err());
}
