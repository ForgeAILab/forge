use std::{path::PathBuf, sync::Arc};

use db::{SqliteDb, Task, Workspace, WorkspaceRepo, WorkspaceStatus};
use tempfile::TempDir;

use super::*;
use crate::task_service::workspace::{
    prepare_workspace_for_test,
    tests::{seed_project_with_real_repo, seed_task, sqlite_db},
};

const REPAIRING: [Purpose; 6] = [
    Purpose::Execute,
    Purpose::Review,
    Purpose::Check,
    Purpose::Integrate,
    Purpose::Hook,
    Purpose::Reset,
];

/// Purposes that act on the candidate without being allowed to move it.
const CANDIDATE: [Purpose; 4] = [
    Purpose::Review,
    Purpose::Check,
    Purpose::Integrate,
    Purpose::Hook,
];

struct Fixture {
    db: SqliteDb,
    repo_dir: TempDir,
    root: TempDir,
    /// The workspace root as Forge is configured with it: `root`, or a
    /// symbolic link to it.
    root_path: PathBuf,
    task: Task,
    workspace: Workspace,
    path: PathBuf,
    router: Arc<WorkspaceBackendRouter>,
}

impl Fixture {
    async fn new() -> Self {
        Self::behind(None).await
    }

    /// `link`: reach the workspace root through this symbolic link, the way
    /// `/tmp` and `/var` are reached on macOS.
    async fn behind(link: Option<PathBuf>) -> Self {
        let db = sqlite_db().await;
        let repo_dir = TempDir::new().expect("repo dir creates");
        let (project_id, _repo_id) = seed_project_with_real_repo(&db, repo_dir.path()).await;
        let root = TempDir::new().expect("workspace root creates");
        let root_path = match link {
            #[cfg(unix)]
            Some(link) => {
                std::os::unix::fs::symlink(root.path(), &link).expect("root link creates");
                link
            }
            _ => root.path().to_path_buf(),
        };
        let task = seed_task(&db, &project_id, None).await;
        let workspace = prepare_workspace_for_test(&db, &root_path, &task, &task.id, None)
            .await
            .expect("workspace creates");
        assert_eq!(workspace.status, WorkspaceStatus::Ready);
        let path = PathBuf::from(workspace.embedded_worktree_path_for_backend());
        let router = crate::lifecycle::context::embedded_workspace_router_for_test(
            Arc::new(db.clone()),
            root_path.clone(),
            None,
        );
        Self {
            db,
            repo_dir,
            root,
            root_path,
            task,
            workspace,
            path,
            router,
        }
    }

    async fn row(&self) -> Option<Workspace> {
        WorkspaceRepo::get_by_id(&self.db, &self.workspace.id)
            .await
            .expect("workspace lookup succeeds")
    }

    async fn check(&self, purpose: Purpose) -> Result<ValidWorkspace, WorkspaceUnavailable> {
        let workspace = self.row().await.expect("workspace row exists");
        WorkspaceManager::new(&self.db, &self.root_path, None, &self.router)
            .ensure_valid(&self.task, workspace, purpose)
            .await
    }

    fn head_ref(&self) -> String {
        git(&self.path, &["rev-parse", "--symbolic-full-name", "HEAD"])
    }

    fn task_ref(&self) -> String {
        format!("refs/heads/{}", self.workspace.branch)
    }

    /// Whatever sits beside the worktree in the Task root.
    fn siblings(&self) -> Vec<String> {
        let mut names = std::fs::read_dir(self.path.parent().expect("Task root"))
            .expect("Task root is readable")
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}

fn git(cwd: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env("GIT_AUTHOR_NAME", "Forge Test")
        .env("GIT_AUTHOR_EMAIL", "forge@example.com")
        .env("GIT_COMMITTER_NAME", "Forge Test")
        .env("GIT_COMMITTER_EMAIL", "forge@example.com")
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?} in {} failed: {}",
        cwd.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn try_git(cwd: &std::path::Path, args: &[&str]) -> bool {
    std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env("GIT_AUTHOR_NAME", "Forge Test")
        .env("GIT_AUTHOR_EMAIL", "forge@example.com")
        .env("GIT_COMMITTER_NAME", "Forge Test")
        .env("GIT_COMMITTER_EMAIL", "forge@example.com")
        .output()
        .expect("git runs")
        .status
        .success()
}

fn assert_reset_required(result: Result<ValidWorkspace, WorkspaceUnavailable>, needle: &str) {
    match result {
        Err(WorkspaceUnavailable::ResetRequired { reason, .. }) => {
            assert!(reason.contains(needle), "unexpected reason: {reason}")
        }
        Err(other) => panic!("expected ResetRequired, got {other:?}"),
        Ok(_) => panic!("expected ResetRequired, got a valid workspace"),
    }
}

fn assert_absent(result: Result<ValidWorkspace, WorkspaceUnavailable>, needle: &str) {
    match result {
        Err(WorkspaceUnavailable::Absent { reason }) => {
            assert!(reason.contains(needle), "unexpected reason: {reason}")
        }
        Err(other) => panic!("expected Absent, got {other:?}"),
        Ok(_) => panic!("expected Absent, got a valid workspace"),
    }
}

/// A recovered worktree is one a command can run in, on the Task branch.
fn assert_recovered(fixture: &Fixture, valid: &ValidWorkspace) {
    assert_eq!(valid.repair(), Repair::Recovered);
    assert_eq!(valid.path(), Some(fixture.path.as_path()));
    assert_eq!(valid.workspace().id, fixture.workspace.id);
    assert_eq!(fixture.head_ref(), fixture.task_ref());
    git(&fixture.path, &["status", "--porcelain"]);
}

#[tokio::test]
async fn healthy_workspace_is_returned_unchanged_for_every_purpose() {
    let fixture = Fixture::new().await;
    let head = git(&fixture.path, &["rev-parse", "HEAD"]);
    for purpose in REPAIRING.into_iter().chain([Purpose::Inspect]) {
        let valid = fixture.check(purpose).await.expect("healthy is valid");
        assert_eq!(valid.repair(), Repair::None, "{purpose:?}");
        assert!(valid.on_task_branch(), "{purpose:?}");
        assert_eq!(valid.path(), Some(fixture.path.as_path()));
        assert_eq!(valid.workspace(), &fixture.workspace);
        assert_eq!(
            valid.resolved().placement.owner_kind,
            db::PlacementOwnerKind::Server
        );
    }
    assert_eq!(git(&fixture.path, &["rev-parse", "HEAD"]), head);
    assert_eq!(fixture.siblings(), ["repo"]);
    assert_eq!(fixture.row().await, Some(fixture.workspace.clone()));
}

/// The measured cost of the healthy path, printed with `--nocapture`.
#[tokio::test]
async fn healthy_validation_is_cheap() {
    let fixture = Fixture::new().await;
    fixture.check(Purpose::Execute).await.expect("warm up");
    let runs = 20;
    let started = std::time::Instant::now();
    for _ in 0..runs {
        fixture.check(Purpose::Execute).await.expect("healthy");
    }
    let each = started.elapsed() / runs;
    println!("ensure_valid healthy path: {each:?} per call over {runs} calls");
    assert!(
        each < std::time::Duration::from_millis(500),
        "validation took {each:?} per call"
    );
}

#[tokio::test]
async fn deleted_directory_is_recreated_for_every_repairing_purpose() {
    for purpose in REPAIRING {
        let fixture = Fixture::new().await;
        std::fs::remove_dir_all(&fixture.path).unwrap();
        let valid = fixture.check(purpose).await.expect("worktree recreated");
        assert_recovered(&fixture, &valid);
    }
}

#[tokio::test]
async fn inspect_reports_a_deleted_directory_and_changes_nothing() {
    let fixture = Fixture::new().await;
    std::fs::remove_dir_all(&fixture.path).unwrap();
    assert_absent(fixture.check(Purpose::Inspect).await, "missing");
    assert!(!fixture.path.exists());
    assert_eq!(fixture.row().await, Some(fixture.workspace.clone()));
}

#[tokio::test]
async fn directory_that_is_not_a_worktree_is_moved_aside_and_recreated() {
    for purpose in REPAIRING {
        let fixture = Fixture::new().await;
        std::fs::remove_dir_all(&fixture.path).unwrap();
        std::fs::create_dir_all(&fixture.path).unwrap();
        std::fs::write(fixture.path.join("stray.txt"), "not a worktree").unwrap();
        let valid = fixture.check(purpose).await.expect("worktree recreated");
        assert_recovered(&fixture, &valid);
        assert!(!fixture.path.join("stray.txt").exists());
        assert!(
            fixture
                .siblings()
                .iter()
                .any(|name| name.starts_with("repo.broken-")),
            "the unusable directory is kept aside: {:?}",
            fixture.siblings()
        );
    }

    let fixture = Fixture::new().await;
    std::fs::remove_dir_all(&fixture.path).unwrap();
    std::fs::create_dir_all(&fixture.path).unwrap();
    assert_absent(fixture.check(Purpose::Inspect).await, "not a Git worktree");
    assert_eq!(fixture.siblings(), ["repo"]);
}

/// Put a linked worktree of an unrelated repository at the recorded path.
fn replace_with_worktree_of_another_repository(fixture: &Fixture) -> TempDir {
    let other = TempDir::new().expect("other repo dir creates");
    git(other.path(), &["init", "-q", "-b", "main"]);
    git(
        other.path(),
        &["commit", "-q", "--allow-empty", "-m", "other"],
    );
    std::fs::remove_dir_all(&fixture.path).unwrap();
    git(
        other.path(),
        &[
            "worktree",
            "add",
            "-q",
            "-b",
            &fixture.workspace.branch,
            &fixture.path.to_string_lossy(),
        ],
    );
    other
}

#[tokio::test]
async fn worktree_of_another_repository_is_moved_aside_and_recreated() {
    for purpose in REPAIRING {
        let fixture = Fixture::new().await;
        let other = replace_with_worktree_of_another_repository(&fixture);
        let foreign_head = git(&fixture.path, &["rev-parse", "HEAD"]);

        let valid = fixture.check(purpose).await.expect("worktree recreated");
        assert_recovered(&fixture, &valid);
        assert_ne!(git(&fixture.path, &["rev-parse", "HEAD"]), foreign_head);
        let common = git(
            &fixture.path,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        );
        assert_eq!(
            std::fs::canonicalize(common).unwrap(),
            std::fs::canonicalize(fixture.repo_dir.path().join(".git")).unwrap()
        );
        assert!(fixture
            .siblings()
            .iter()
            .any(|name| name.starts_with("repo.broken-")));
        drop(other);
    }

    let fixture = Fixture::new().await;
    let _other = replace_with_worktree_of_another_repository(&fixture);
    assert_absent(
        fixture.check(Purpose::Inspect).await,
        "not of the recorded repository",
    );
    assert_eq!(fixture.siblings(), ["repo"]);
}

/// A worktree Git can still use is never left without its row. When it
/// belongs to another repository and the recorded one no longer has the Task
/// branch, the create-or-reuse path asks for the reset and keeps the row and
/// the directory, with whatever work is in it.
#[tokio::test]
async fn usable_worktree_of_another_repository_is_never_forgotten() {
    let fixture = Fixture::new().await;
    let _other = replace_with_worktree_of_another_repository(&fixture);
    std::fs::write(fixture.path.join("uncommitted.txt"), "work in progress").unwrap();
    git(
        fixture.repo_dir.path(),
        &["update-ref", "-d", &fixture.task_ref()],
    );

    let workspace = fixture.row().await.expect("workspace row exists");
    let result = WorkspaceManager::new(&fixture.db, &fixture.root_path, None, &fixture.router)
        .ensure_valid_or_forget(&fixture.task, workspace, Purpose::Execute)
        .await;

    assert_reset_required(result, "are both gone");
    assert_eq!(fixture.row().await, Some(fixture.workspace.clone()));
    assert_eq!(fixture.siblings(), ["repo"]);
    assert_eq!(
        std::fs::read_to_string(fixture.path.join("uncommitted.txt")).unwrap(),
        "work in progress"
    );
}

/// Known gap, pinned so a change is deliberate: a directory that is a
/// repository of its own (not a linked worktree) is accepted, as before. Many
/// existing fixtures model a Task worktree that way.
#[tokio::test]
async fn standalone_repository_at_the_recorded_path_is_still_accepted() {
    let fixture = Fixture::new().await;
    std::fs::remove_dir_all(&fixture.path).unwrap();
    std::fs::create_dir_all(&fixture.path).unwrap();
    git(
        &fixture.path,
        &["init", "-q", "-b", &fixture.workspace.branch],
    );
    git(
        &fixture.path,
        &["commit", "-q", "--allow-empty", "-m", "own"],
    );
    for purpose in REPAIRING.into_iter().chain([Purpose::Inspect]) {
        let valid = fixture.check(purpose).await.expect("accepted as before");
        assert_eq!(valid.repair(), Repair::None);
    }
}

/// HEAD off the Task branch at the Task branch's own commit: checking the
/// branch out changes no file, so every purpose that repairs does it, with
/// local changes kept.
#[tokio::test]
async fn worktree_off_the_task_branch_at_its_commit_is_put_back_without_loss() {
    for detach in [false, true] {
        for purpose in [Purpose::Execute].into_iter().chain(CANDIDATE) {
            let fixture = Fixture::new().await;
            let switch: &[&str] = if detach {
                &["checkout", "-q", "--detach"]
            } else {
                &["checkout", "-q", "-b", "elsewhere"]
            };
            git(&fixture.path, switch);
            std::fs::write(fixture.path.join("uncommitted.txt"), "work in progress").unwrap();
            let off = fixture.head_ref();
            assert_ne!(off, fixture.task_ref());

            let inspected = fixture
                .check(Purpose::Inspect)
                .await
                .expect("inspect reads it");
            assert!(!inspected.on_task_branch());
            assert_eq!(fixture.head_ref(), off, "inspect never moves HEAD");

            let valid = fixture.check(purpose).await.expect("put back");
            assert_eq!(valid.repair(), Repair::CheckedOutTaskBranch, "{purpose:?}");
            assert!(valid.on_task_branch());
            assert_eq!(fixture.head_ref(), fixture.task_ref());
            assert_eq!(
                std::fs::read_to_string(fixture.path.join("uncommitted.txt")).unwrap(),
                "work in progress",
                "{purpose:?} keeps uncommitted work"
            );
        }
    }
}

/// HEAD behind the Task branch: the checkout moves the tree. Only a launch
/// does that, and only when the worktree is clean.
#[tokio::test]
async fn worktree_behind_the_task_branch_returns_to_it_only_for_a_clean_launch() {
    let fixture = Fixture::new().await;
    git(
        &fixture.path,
        &["commit", "-q", "--allow-empty", "-m", "task work"],
    );
    git(&fixture.path, &["checkout", "-q", "--detach", "HEAD~1"]);
    let off = fixture.head_ref();

    for purpose in CANDIDATE {
        assert_reset_required(fixture.check(purpose).await, "candidate is not moved");
        assert_eq!(fixture.head_ref(), off, "{purpose:?} never moves HEAD");
    }

    std::fs::write(fixture.path.join("uncommitted.txt"), "work in progress").unwrap();
    assert_reset_required(fixture.check(Purpose::Execute).await, "uncommitted changes");
    assert_eq!(fixture.head_ref(), off);
    assert!(fixture.path.join("uncommitted.txt").exists());
    assert_eq!(fixture.row().await, Some(fixture.workspace.clone()));
    let error = ServiceError::from(fixture.check(Purpose::Execute).await.err().unwrap());
    assert!(matches!(
        error,
        ServiceError::WorkspaceResetRequired { ref task_id, .. } if *task_id == fixture.task.id
    ));

    std::fs::remove_file(fixture.path.join("uncommitted.txt")).unwrap();
    let valid = fixture
        .check(Purpose::Execute)
        .await
        .expect("a clean launch checks out");
    assert_eq!(valid.repair(), Repair::CheckedOutTaskBranch);
    assert_eq!(fixture.head_ref(), fixture.task_ref());
}

/// Every purpose a Task step uses: launch, review, check, hook, delivery.
const STEPS: [Purpose; 5] = [
    Purpose::Execute,
    Purpose::Review,
    Purpose::Check,
    Purpose::Hook,
    Purpose::Integrate,
];

async fn task_row(fixture: &Fixture) -> Task {
    db::TaskRepo::get_by_id(&fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .expect("task row exists")
}

async fn forge_comments(fixture: &Fixture) -> Vec<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT content FROM task_comment WHERE task_id = ? AND author_type = 'system' \
         ORDER BY created_at",
    )
    .bind(&fixture.task.id)
    .fetch_all(fixture.db.pool())
    .await
    .unwrap()
}

fn rescued_refs(fixture: &Fixture) -> Vec<String> {
    git(
        &fixture.path,
        &[
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            RESCUED_REF_NAMESPACE,
        ],
    )
    .lines()
    .map(str::to_owned)
    .collect()
}

/// Commits made on a detached HEAD or another branch, on top of the Task
/// branch: the branch is advanced to them and checked out. Nothing is lost,
/// nothing is reset, uncommitted work stays, and the Task row (review and
/// retry budgets included) is untouched.
#[tokio::test]
async fn commits_ahead_of_the_task_branch_fast_forward_it_for_every_step() {
    for detach in [false, true] {
        for purpose in STEPS {
            let fixture = Fixture::new().await;
            let before = task_row(&fixture).await;
            let switch: &[&str] = if detach {
                &["checkout", "-q", "--detach"]
            } else {
                &["checkout", "-q", "-b", "elsewhere"]
            };
            git(&fixture.path, switch);
            std::fs::write(fixture.path.join("work.txt"), "the Task's work").unwrap();
            git(&fixture.path, &["add", "work.txt"]);
            git(
                &fixture.path,
                &["commit", "-q", "-m", "work off the branch"],
            );
            std::fs::write(fixture.path.join("uncommitted.txt"), "in progress").unwrap();
            let commit = git(&fixture.path, &["rev-parse", "HEAD"]);

            let valid = fixture.check(purpose).await.expect("fast-forwarded");
            assert_eq!(
                valid.repair(),
                Repair::FastForwardedTaskBranch,
                "{purpose:?}"
            );
            assert!(valid.on_task_branch());
            assert_eq!(valid.rescued_ref(), None);
            assert_eq!(fixture.head_ref(), fixture.task_ref());
            assert_eq!(
                git(&fixture.path, &["rev-parse", &fixture.task_ref()]),
                commit,
                "{purpose:?}: the Task branch holds the commit"
            );
            assert!(fixture.path.join("work.txt").exists());
            assert_eq!(
                std::fs::read_to_string(fixture.path.join("uncommitted.txt")).unwrap(),
                "in progress"
            );
            assert!(rescued_refs(&fixture).is_empty());
            assert!(forge_comments(&fixture).await.is_empty());
            assert_eq!(task_row(&fixture).await, before, "{purpose:?}: no budget");

            // The step's next check is the healthy path again.
            let again = fixture.check(purpose).await.expect("healthy");
            assert_eq!(again.repair(), Repair::None);
        }
    }
}

/// HEAD and the Task branch each have commits the other lacks: HEAD's are
/// kept under a Forge ref, the Task is told, and the worktree returns to the
/// Task branch so the step continues.
#[tokio::test]
async fn diverged_head_is_rescued_under_a_forge_ref_for_every_step() {
    for purpose in STEPS {
        let fixture = Fixture::new().await;
        let before = task_row(&fixture).await;
        let base = git(&fixture.path, &["rev-parse", "HEAD"]);
        git(
            &fixture.path,
            &["commit", "-q", "--allow-empty", "-m", "task work"],
        );
        let branch_tip = git(&fixture.path, &["rev-parse", "HEAD"]);
        git(&fixture.path, &["checkout", "-q", "--detach", &base]);
        std::fs::write(fixture.path.join("stray.txt"), "stray work").unwrap();
        git(&fixture.path, &["add", "stray.txt"]);
        git(&fixture.path, &["commit", "-q", "-m", "stray"]);
        let stray = git(&fixture.path, &["rev-parse", "HEAD"]);

        let valid = fixture.check(purpose).await.expect("rescued");
        assert_eq!(
            valid.repair(),
            Repair::RescuedOffBranchCommits,
            "{purpose:?}"
        );
        let kept = valid.rescued_ref().expect("the ref is reported").to_owned();
        assert!(
            kept.starts_with(&format!("refs/forge/rescued/{}/", fixture.task.id)),
            "{kept}"
        );
        assert_eq!(git(&fixture.path, &["rev-parse", &kept]), stray);
        assert_eq!(fixture.head_ref(), fixture.task_ref());
        assert_eq!(git(&fixture.path, &["rev-parse", "HEAD"]), branch_tip);
        assert!(!fixture.path.join("stray.txt").exists());
        let comments = forge_comments(&fixture).await;
        assert_eq!(comments.len(), 1, "{comments:?}");
        assert!(comments[0].contains(&kept) && comments[0].contains(&stray));
        assert_eq!(task_row(&fixture).await, before, "{purpose:?}: no budget");

        let again = fixture.check(purpose).await.expect("healthy");
        assert_eq!(again.repair(), Repair::None);
        assert_eq!(rescued_refs(&fixture).len(), 1);
        assert_eq!(forge_comments(&fixture).await.len(), 1);
    }
}

/// The checkout after a rescue is never forced. Uncommitted changes it would
/// overwrite stay where they are, the reason names the ref, and checking
/// again writes no second ref and no second comment: there is no reset loop.
#[tokio::test]
async fn diverged_head_with_conflicting_local_changes_parks_once_with_the_ref_named() {
    let fixture = Fixture::new().await;
    let base = git(&fixture.path, &["rev-parse", "HEAD"]);
    std::fs::write(fixture.path.join("shared.txt"), "task version").unwrap();
    git(&fixture.path, &["add", "shared.txt"]);
    git(&fixture.path, &["commit", "-q", "-m", "task work"]);
    git(&fixture.path, &["checkout", "-q", "--detach", &base]);
    std::fs::write(fixture.path.join("shared.txt"), "stray version").unwrap();
    git(&fixture.path, &["add", "shared.txt"]);
    git(&fixture.path, &["commit", "-q", "-m", "stray"]);
    let stray = git(&fixture.path, &["rev-parse", "HEAD"]);
    std::fs::write(fixture.path.join("shared.txt"), "uncommitted version").unwrap();

    for purpose in STEPS.into_iter().chain(STEPS) {
        assert_reset_required(
            fixture.check(purpose).await,
            "commit(s) are kept under refs/forge/rescued/",
        );
        assert_eq!(git(&fixture.path, &["rev-parse", "HEAD"]), stray);
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("shared.txt")).unwrap(),
            "uncommitted version"
        );
    }
    assert_eq!(rescued_refs(&fixture).len(), 1);
    assert_eq!(forge_comments(&fixture).await.len(), 1);
    assert_eq!(fixture.row().await, Some(fixture.workspace.clone()));
}

/// A step that finds its directory gone, or not a worktree, gets it back
/// from the Task branch in the same call and spends no budget. One call
/// makes one attempt: when the Task branch is gone too the answer is the
/// typed reset, again and again, with nothing recreated in between.
#[tokio::test]
async fn unusable_directory_is_recreated_once_per_step_without_budget() {
    for purpose in STEPS {
        for not_a_worktree in [false, true] {
            let fixture = Fixture::new().await;
            let before = task_row(&fixture).await;
            std::fs::remove_dir_all(&fixture.path).unwrap();
            if not_a_worktree {
                std::fs::create_dir_all(&fixture.path).unwrap();
                std::fs::write(fixture.path.join("left-over.txt"), "x").unwrap();
            }
            let valid = fixture.check(purpose).await.expect("recreated");
            assert_recovered(&fixture, &valid);
            assert_eq!(task_row(&fixture).await, before, "{purpose:?}: no budget");
            assert_eq!(
                fixture.check(purpose).await.expect("healthy").repair(),
                Repair::None
            );
        }

        let fixture = Fixture::new().await;
        std::fs::remove_dir_all(&fixture.path).unwrap();
        git(fixture.repo_dir.path(), &["worktree", "prune"]);
        git(
            fixture.repo_dir.path(),
            &["branch", "-D", &fixture.workspace.branch],
        );
        for _ in 0..2 {
            let error = ServiceError::from(fixture.check(purpose).await.err().expect("parked"));
            assert!(
                matches!(error, ServiceError::WorkspaceResetRequired { .. }),
                "{purpose:?}: {error}"
            );
            assert!(!fixture.path.exists(), "{purpose:?}: nothing is recreated");
        }
        assert_eq!(fixture.row().await, Some(fixture.workspace.clone()));
    }
}

/// The reassignment reset discards uncommitted work at the current HEAD. It
/// is handed a worktree off the Task branch as it is, dirty or not.
#[tokio::test]
async fn reset_purpose_accepts_a_dirty_worktree_off_the_task_branch() {
    let fixture = Fixture::new().await;
    git(&fixture.path, &["checkout", "-q", "-b", "elsewhere"]);
    git(
        &fixture.path,
        &["commit", "-q", "--allow-empty", "-m", "work off the branch"],
    );
    std::fs::write(fixture.path.join("uncommitted.txt"), "work in progress").unwrap();
    let commit = git(&fixture.path, &["rev-parse", "HEAD"]);

    let valid = fixture
        .check(Purpose::Reset)
        .await
        .expect("the reset path is not refused");
    assert_eq!(valid.repair(), Repair::None);
    assert!(!valid.on_task_branch());
    assert_eq!(valid.path(), Some(fixture.path.as_path()));
    assert_eq!(fixture.head_ref(), "refs/heads/elsewhere");
    assert_eq!(git(&fixture.path, &["rev-parse", "HEAD"]), commit);
    assert!(fixture.path.join("uncommitted.txt").exists());
}

/// A workspace root behind a symbolic link (`/tmp` and `/var` on macOS, a
/// relocated data directory) is an ordinary root: only the Task root and the
/// worktree themselves must be real directories.
#[cfg(unix)]
#[tokio::test]
async fn workspace_root_behind_a_symbolic_link_is_healthy_and_repairable() {
    let links = TempDir::new().expect("link dir creates");
    let fixture = Fixture::behind(Some(links.path().join("root-link"))).await;
    assert!(
        fixture.path.starts_with(links.path()),
        "the recorded path runs through the link: {}",
        fixture.path.display()
    );
    assert!(!fixture.path.starts_with(fixture.root.path()));

    for purpose in REPAIRING.into_iter().chain([Purpose::Inspect]) {
        let valid = fixture.check(purpose).await.expect("healthy behind a link");
        assert_eq!(valid.repair(), Repair::None, "{purpose:?}");
        assert!(valid.on_task_branch(), "{purpose:?}");
        assert_eq!(valid.path(), Some(fixture.path.as_path()));
    }

    std::fs::remove_dir_all(&fixture.path).unwrap();
    let valid = fixture
        .check(Purpose::Execute)
        .await
        .expect("recreated behind a link");
    assert_eq!(valid.repair(), Repair::Recovered);
    assert!(fixture.path.join(".git").exists());
    fixture
        .check(Purpose::Review)
        .await
        .expect("healthy after the repair");
}

/// Git detaches HEAD for the length of a rebase. The rebase owner aborts or
/// continues it; validation must not turn that state into a reset.
#[tokio::test]
async fn interrupted_rebase_is_left_to_its_owner() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path.join("file.txt"), "task\n").unwrap();
    git(&fixture.path, &["add", "file.txt"]);
    git(&fixture.path, &["commit", "-q", "-m", "task change"]);
    std::fs::write(fixture.repo_dir.path().join("file.txt"), "main\n").unwrap();
    git(fixture.repo_dir.path(), &["add", "file.txt"]);
    git(
        fixture.repo_dir.path(),
        &["commit", "-q", "-m", "main change"],
    );
    assert!(
        !try_git(&fixture.path, &["rebase", "main"]),
        "the rebase stops on the conflict"
    );
    assert_eq!(fixture.head_ref(), "HEAD");

    for purpose in REPAIRING.into_iter().chain([Purpose::Inspect]) {
        let valid = fixture.check(purpose).await.expect("mid-rebase is valid");
        assert_eq!(valid.repair(), Repair::None, "{purpose:?}");
    }
    assert_eq!(fixture.head_ref(), "HEAD");
    git(&fixture.path, &["rebase", "--abort"]);
    assert_eq!(fixture.head_ref(), fixture.task_ref());
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_worktree_path_is_never_used_or_repaired() {
    for purpose in REPAIRING.into_iter().chain([Purpose::Inspect]) {
        let fixture = Fixture::new().await;
        let elsewhere = TempDir::new().expect("target dir creates");
        let target = elsewhere.path().join("moved");
        git(
            fixture.repo_dir.path(),
            &[
                "worktree",
                "move",
                &fixture.path.to_string_lossy(),
                &target.to_string_lossy(),
            ],
        );
        std::os::unix::fs::symlink(&target, &fixture.path).unwrap();
        // Through the link this is a usable worktree on the Task branch.
        assert_eq!(fixture.head_ref(), fixture.task_ref());

        let result = fixture.check(purpose).await;
        if purpose == Purpose::Inspect {
            assert_absent(result, "symbolic link");
        } else {
            assert_reset_required(result, "symbolic link");
        }
        assert!(std::fs::symlink_metadata(&fixture.path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert!(target.join(".git").exists(), "the link target is untouched");
        assert_eq!(fixture.siblings(), ["repo"]);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_task_root_is_never_used_or_repaired() {
    let fixture = Fixture::new().await;
    let task_root = fixture.path.parent().unwrap().to_path_buf();
    let elsewhere = TempDir::new().expect("target dir creates");
    let moved = elsewhere.path().join("task-root");
    std::fs::remove_dir_all(&fixture.path).unwrap();
    std::fs::remove_dir_all(&task_root).unwrap();
    std::fs::create_dir_all(&moved).unwrap();
    std::os::unix::fs::symlink(&moved, &task_root).unwrap();

    assert_reset_required(fixture.check(Purpose::Execute).await, "symbolic link");
    assert_absent(fixture.check(Purpose::Inspect).await, "symbolic link");
    assert_eq!(std::fs::read_dir(&moved).unwrap().count(), 0);
}

#[tokio::test]
async fn branch_and_directory_both_gone_need_a_reset_and_keep_the_row() {
    let fixture = Fixture::new().await;
    std::fs::remove_dir_all(&fixture.path).unwrap();
    git(fixture.repo_dir.path(), &["worktree", "prune"]);
    git(
        fixture.repo_dir.path(),
        &["branch", "-D", &fixture.workspace.branch],
    );

    for purpose in REPAIRING {
        assert_reset_required(fixture.check(purpose).await, "are both gone");
        assert!(fixture.row().await.is_some(), "{purpose:?} keeps the row");
    }
    assert_absent(fixture.check(Purpose::Inspect).await, "missing");

    // The owning Task's launch forgets the row, so its next launch starts
    // again from the default branch (unchanged behaviour).
    let workspace = fixture.row().await.unwrap();
    let forgotten = WorkspaceManager::new(&fixture.db, fixture.root.path(), None, &fixture.router)
        .ensure_valid_or_forget(&fixture.task, workspace, Purpose::Execute)
        .await;
    assert_reset_required(forgotten, "are both gone");
    assert!(fixture.row().await.is_none());
    let fresh = prepare_workspace_for_test(
        &fixture.db,
        fixture.root.path(),
        &fixture.task,
        &fixture.task.id,
        None,
    )
    .await
    .expect("the next launch creates a fresh workspace");
    assert_ne!(fresh.id, fixture.workspace.id);
    git(&fixture.path, &["status", "--porcelain"]);
}

/// The "deleted worktree reuse" wedge: a launch on a `ready` row whose
/// directory is gone gets a real worktree back, never a Git error.
#[tokio::test]
async fn launch_on_ready_row_with_deleted_directory_recreates_and_runs() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.path.join("work.txt"), "committed work\n").unwrap();
    git(&fixture.path, &["add", "work.txt"]);
    git(&fixture.path, &["commit", "-q", "-m", "task work"]);
    let tip = git(&fixture.path, &["rev-parse", "HEAD"]);
    std::fs::remove_dir_all(&fixture.path).unwrap();
    assert_eq!(
        fixture.row().await.unwrap().status,
        WorkspaceStatus::Ready,
        "the row still says ready"
    );

    let launched = prepare_workspace_for_test(
        &fixture.db,
        fixture.root.path(),
        &fixture.task,
        &fixture.task.id,
        None,
    )
    .await
    .expect("the launch recreates the worktree");
    assert_eq!(launched.id, fixture.workspace.id);
    let path = PathBuf::from(launched.embedded_worktree_path_for_backend());

    let run = crate::workspace_backend::run_environment_checkout(
        &path,
        &crate::workspace_backend::RunSpec {
            purpose: api_types::WorkspaceRunPurpose::Hook,
            command: "git rev-parse HEAD && cat work.txt".to_owned(),
            env: Default::default(),
            timeout_secs: 30,
            max_output_bytes: 4096,
        },
    )
    .await
    .expect("a command runs in the recreated worktree");
    assert_eq!(run.exit_code, 0, "stderr: {}", run.stderr_tail);
    assert!(run.stdout_tail.contains(&tip));
    assert!(run.stdout_tail.contains("committed work"));
    assert!(!run.stderr_tail.contains("not a git repository"));
}

#[tokio::test]
async fn row_that_is_not_ready_is_refused_as_before() {
    let fixture = Fixture::new().await;
    sqlx::query("UPDATE workspace SET status = 'cleaning' WHERE id = ?")
        .bind(&fixture.workspace.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    match fixture.check(Purpose::Execute).await {
        Err(WorkspaceUnavailable::Infrastructure(error)) => assert_eq!(
            error.to_string(),
            ServiceError::invalid_operation(format!(
                "workspace for task {} is not ready",
                fixture.task.id
            ))
            .to_string()
        ),
        other => panic!("unexpected outcome: {:?}", other.err()),
    }
    // Inspect reports the disk, whatever the row says.
    let inspected = fixture
        .check(Purpose::Inspect)
        .await
        .expect("the worktree is still there");
    assert_eq!(inspected.path(), Some(fixture.path.as_path()));
    std::fs::remove_dir_all(&fixture.path).unwrap();
    assert_absent(fixture.check(Purpose::Inspect).await, "missing");
}

#[test]
fn unavailable_outcomes_keep_the_service_errors_callers_match_on() {
    assert!(matches!(
        ServiceError::from(WorkspaceUnavailable::ResetRequired {
            task_id: "t".to_owned(),
            reason: "r".to_owned(),
        }),
        ServiceError::WorkspaceResetRequired { task_id, reason } if task_id == "t" && reason == "r"
    ));
    assert!(matches!(
        ServiceError::from(WorkspaceUnavailable::OwnerUnreachable {
            daemon_id: "d".to_owned(),
        }),
        ServiceError::DaemonUnavailable { daemon_id } if daemon_id == "d"
    ));
    assert!(matches!(
        ServiceError::from(WorkspaceUnavailable::Busy {
            reason: "workspace still has a running execution".to_owned(),
        }),
        ServiceError::Conflict(message) if message == "workspace still has a running execution"
    ));
    // A round trip through the typed outcome loses nothing.
    for error in [
        ServiceError::WorkspaceResetRequired {
            task_id: "t".to_owned(),
            reason: "r".to_owned(),
        },
        ServiceError::DaemonUnavailable {
            daemon_id: "d".to_owned(),
        },
        ServiceError::invalid_operation("workspace for task t is not ready"),
    ] {
        let text = error.to_string();
        assert_eq!(
            ServiceError::from(WorkspaceUnavailable::from(error)).to_string(),
            text
        );
    }
}

/// Raw path getters: each hands out a recorded path that nothing has checked.
const RAW_PATH_GETTERS: [&str; 3] = [
    ".embedded_path(",
    "embedded_worktree_path_for_backend(",
    "recorded_server_path(",
];

/// Non-test uses of a raw path getter, per file, relative to `crates/`: an
/// upper bound for each file that still has any.
///
/// The first group is the manager and the owner-local backend it is built
/// on. The second group is callers that still take an unchecked path and are
/// converted by the stage named beside them. A raw getter in a file that is
/// not listed, or more uses in a listed file than recorded, fails this test:
/// go through `WorkspaceManager::ensure_valid` instead. Fewer uses pass, so
/// converting a caller or refactoring one of these files needs no edit here;
/// lower the entry when convenient.
const RAW_PATH_CALLERS: &[(&str, usize)] = &[
    // Manager and backend internals.
    ("services/src/workspace_manager.rs", 2),
    ("services/src/task_service/workspace.rs", 2),
    ("services/src/workspace_backend/embedded.rs", 5),
    ("services/src/workspace_backend/review.rs", 10),
    ("review/src/workspace.rs", 2),
    // Pending conversion: owned by another job while this one landed.
    ("services/src/merge_service.rs", 2),          // 3.2 D
    ("services/src/task_actions.rs", 1),           // 3.2 D
    ("services/src/task_service/execution.rs", 1), // 3.4 C
    ("services/src/task_service/execution/runner.rs", 7), // 3.4 C
    // Pending conversion: 3.4 B part 2.
    ("services/src/lifecycle/plugin.rs", 1),
    ("services/src/native_tools.rs", 1),
    ("services/src/plan_artifact.rs", 5),
    ("services/src/recovery.rs", 1),
    ("services/src/task_service.rs", 2),
    ("services/src/terminal_service.rs", 1),
];

fn raw_path_uses(source: &str) -> usize {
    let lines = source.lines().collect::<Vec<_>>();
    let mut uses = 0;
    let mut index = 0;
    while index < lines.len() {
        // A `#[cfg(test)]` module builds its own fixtures: skip to its
        // closing brace. Items after it are production code again.
        let opens_test_module = lines[index] == "#[cfg(test)]"
            && lines[index + 1..]
                .iter()
                .take(2)
                .any(|line| line.starts_with("mod ") || line.starts_with("pub(crate) mod "));
        if opens_test_module {
            while index < lines.len() && lines[index] != "}" {
                index += 1;
            }
            index += 1;
            continue;
        }
        let line = lines[index];
        if !line.trim_start().starts_with("//") {
            uses += RAW_PATH_GETTERS
                .iter()
                .map(|getter| line.matches(getter).count())
                .sum::<usize>();
        }
        index += 1;
    }
    uses
}

fn scan_raw_path_uses(
    crates: &std::path::Path,
    dir: &std::path::Path,
    found: &mut Vec<(String, usize)>,
) {
    for entry in std::fs::read_dir(dir)
        .expect("directory readable")
        .flatten()
    {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if name != "tests" {
                scan_raw_path_uses(crates, &path, found);
            }
            continue;
        }
        if !name.ends_with(".rs")
            || name == "tests.rs"
            || name.ends_with("_test.rs")
            || name.ends_with("_tests.rs")
        {
            continue;
        }
        let uses = raw_path_uses(&std::fs::read_to_string(&path).expect("source file readable"));
        if uses > 0 {
            let relative = path.strip_prefix(crates).unwrap_or(&path);
            found.push((relative.to_string_lossy().replace('\\', "/"), uses));
        }
    }
}

#[test]
fn raw_workspace_path_getters_have_no_new_callers() {
    let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory");
    let mut found = Vec::new();
    for source in [
        "services/src",
        "api/src",
        "mcp-server/src",
        "review/src",
        "executors/src",
    ] {
        scan_raw_path_uses(crates, &crates.join(source), &mut found);
    }
    found.sort();
    let over = raw_path_callers_over_the_bound(&found);
    assert!(
        over.is_empty(),
        "raw workspace path getters ({RAW_PATH_GETTERS:?}) have new callers: {over:?} \
         (file, uses found, uses recorded). Take the path from \
         WorkspaceManager::ensure_valid."
    );
}

/// Files whose raw getter uses exceed their recorded bound (0 when unlisted).
fn raw_path_callers_over_the_bound(found: &[(String, usize)]) -> Vec<(String, usize, usize)> {
    found
        .iter()
        .filter_map(|(file, uses)| {
            let recorded = RAW_PATH_CALLERS
                .iter()
                .find(|(listed, _)| listed == file)
                .map_or(0, |(_, recorded)| *recorded);
            (*uses > recorded).then(|| (file.clone(), *uses, recorded))
        })
        .collect()
}

#[test]
fn raw_path_gate_allows_fewer_uses_and_refuses_more_or_new_files() {
    let (listed, recorded) = RAW_PATH_CALLERS[0];
    assert!(raw_path_callers_over_the_bound(&[(listed.to_owned(), recorded)]).is_empty());
    assert!(raw_path_callers_over_the_bound(&[(listed.to_owned(), recorded - 1)]).is_empty());
    assert_eq!(
        raw_path_callers_over_the_bound(&[(listed.to_owned(), recorded + 1)]),
        [(listed.to_owned(), recorded + 1, recorded)]
    );
    assert_eq!(
        raw_path_callers_over_the_bound(&[("services/src/new_caller.rs".to_owned(), 1)]),
        [("services/src/new_caller.rs".to_owned(), 1, 0)]
    );
}

#[test]
fn raw_path_scan_counts_production_code_only() {
    let source = "fn a() { x.embedded_path(); }\n// y.embedded_path()\n\
                  fn b() { w.embedded_worktree_path_for_backend(); }\n\
                  #[cfg(test)]\n#[allow(clippy::items_after_test_module)]\nmod tests {\n\
                  \x20   fn c() { z.embedded_path(); }\n}\n\
                  fn d() { v.recorded_server_path(); }\n\
                  #[cfg(test)]\nmod more;\n";
    assert_eq!(raw_path_uses(source), 3);
}
