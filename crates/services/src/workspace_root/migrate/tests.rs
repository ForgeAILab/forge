use super::*;
use crate::{
    task_service::workspace::{
        prepare_workspace_for_test,
        tests::{seed_project_with_real_repo, seed_task, sqlite_db},
    },
    workspace_cleanup::WorkspaceCleanupScheduler,
    workspace_manager::{Purpose, WorkspaceManager},
    workspace_root::{
        recorded_root, settle,
        tests::{seed_running_execution, Dirs},
        RootChoice,
    },
};
use db::{Task, Workspace, WorkspaceRepo};
use std::collections::{BTreeMap, HashMap};
use tempfile::TempDir;

fn git_ok(cwd: &Path, args: &[&str]) -> String {
    let output = git(cwd, args).expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?} in {}: {}",
        cwd.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// An install an older release left in the system temp directory: a real
/// clone under `.repos` with two Task worktrees, a worktree of a user's own
/// repository, an execution log, and an adopted garbage-collection marker.
struct Install {
    db: SqliteDb,
    dirs: Dirs,
    _user_repo: TempDir,
    user_repo: PathBuf,
    root: PathBuf,
    clone: PathBuf,
    repo_id: String,
    /// Two Tasks of the cloned repository, then one of the user's.
    tasks: Vec<Task>,
    log_path: PathBuf,
}

const LOG_LINE: &str = "{\"type\":\"log\",\"line\":\"kept\"}\n";

impl Install {
    async fn new() -> Self {
        let db = sqlite_db().await;
        let dirs = Dirs::new();
        let root = dirs.legacy_root();
        std::fs::create_dir_all(&root).unwrap();

        // A remote repository: Forge clones it into `<root>/.repos`.
        let (project_id, repo_id) = seed_project_with_real_repo(&db, dirs.repo.path()).await;
        sqlx::query("UPDATE repo SET local_path = NULL WHERE id = ?")
            .bind(&repo_id)
            .execute(db.pool())
            .await
            .unwrap();
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let task = seed_task(&db, &project_id, None).await;
            prepare_workspace_for_test(&db, &root, &task, &task.id, None)
                .await
                .expect("clone workspace creates");
            tasks.push(task);
        }
        let clone = root.join(".repos").join(&repo_id);
        assert!(clone.join(".git").is_dir(), "a real clone");
        git_ok(&clone, &["config", "user.email", "test@forge.dev"]);
        git_ok(&clone, &["config", "user.name", "Forge Test"]);

        // A user's own checkout: its worktree lives under the root too.
        let user_repo_dir = TempDir::new().unwrap();
        let user_repo = user_repo_dir.path().to_path_buf();
        let (user_project, _) = seed_project_with_real_repo(&db, &user_repo).await;
        let user_task = seed_task(&db, &user_project, None).await;
        prepare_workspace_for_test(&db, &root, &user_task, &user_task.id, None)
            .await
            .expect("user workspace creates");
        tasks.push(user_task);

        let install = Self {
            db,
            dirs,
            _user_repo: user_repo_dir,
            user_repo,
            root,
            clone,
            repo_id,
            tasks,
            log_path: PathBuf::new(),
        };
        // Committed work in the first worktree.
        let first = install.worktree(0).await;
        std::fs::write(first.join("committed.txt"), "committed work\n").unwrap();
        git_ok(&first, &["add", "-A"]);
        git_ok(&first, &["commit", "-m", "task work"]);
        // Uncommitted work in the second: a changed file, an untracked
        // directory, an executable and a link.
        let second = install.worktree(1).await;
        std::fs::write(second.join("README.md"), "# Test\nuncommitted edit\n").unwrap();
        std::fs::create_dir_all(second.join("notes")).unwrap();
        std::fs::write(second.join("notes/untracked.txt"), "never committed\n").unwrap();
        std::fs::write(second.join("run.sh"), "#!/bin/sh\necho ok\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                second.join("run.sh"),
                std::fs::Permissions::from_mode(0o755),
            )
            .unwrap();
            std::os::unix::fs::symlink("notes/untracked.txt", second.join("link")).unwrap();
            // What a toolchain leaves behind: a directory nobody may write.
            let frozen = second.join("notes/frozen");
            std::fs::create_dir_all(&frozen).unwrap();
            std::fs::write(frozen.join("module.txt"), "read-only\n").unwrap();
            std::fs::set_permissions(&frozen, std::fs::Permissions::from_mode(0o555)).unwrap();
        }

        // A finished execution with a log.
        let workspace = install.workspace(0).await;
        let execution_id = seed_running_execution(&install.db, &workspace).await;
        sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
            .bind(&execution_id)
            .execute(install.db.pool())
            .await
            .unwrap();
        let log_path: String = sqlx::query_scalar("SELECT logs_path FROM execution WHERE id = ?")
            .bind(&execution_id)
            .fetch_one(install.db.pool())
            .await
            .unwrap();
        let log_path = PathBuf::from(log_path);
        assert!(log_path.starts_with(install.root.join(".forge/logs")));
        std::fs::create_dir_all(log_path.parent().unwrap()).unwrap();
        std::fs::write(&log_path, LOG_LINE).unwrap();

        install.seed_every_stored_path().await;

        // The server that ran here: it recorded and adopted the root.
        let settled = settle(&install.db, &install.choice()).await.unwrap();
        assert_eq!(settled.root, install.root);
        assert!(settled.in_system_temp);
        assert_eq!(
            install
                .scheduler(&install.root)
                .adopt_workspace_root()
                .await
                .unwrap(),
            gc::Ownership::Mine
        );
        // A daemon sharing the root: its Task roots and its own marker.
        let daemon_root = install.root.join(".forge/workspaces/workspace-7b0c9f6e");
        std::fs::create_dir_all(daemon_root.join("repo")).unwrap();
        std::fs::write(daemon_root.join("repo/daemon.txt"), "daemon work\n").unwrap();
        std::fs::create_dir_all(install.root.join(gc::GC_DIR)).unwrap();
        std::fs::write(
            install.root.join(gc::GC_DIR).join(gc::DAEMON_OWNER_FILE),
            "daemon-state-id",
        )
        .unwrap();
        Self {
            log_path,
            ..install
        }
    }

    /// A row in every other column that stores a path under the root, a
    /// root that is only a prefix of another path (`<root>2`), a runtime
    /// row of a daemon that is not the server's, and a directory in the
    /// root that is nobody Forge knows.
    async fn seed_every_stored_path(&self) {
        let root = self.root.display().to_string();
        let now = "2026-01-01T00:00:00Z";
        std::fs::create_dir_all(self.root.join("repos/provisioned")).unwrap();
        std::fs::write(
            self.root.join("repos/provisioned/file.txt"),
            "provisioned\n",
        )
        .unwrap();
        std::fs::create_dir_all(self.root.join("main-agents/account/inquiries/one")).unwrap();
        std::fs::write(
            self.root
                .join("main-agents/account/inquiries/one/findings.md"),
            "findings\n",
        )
        .unwrap();
        // Named by a row only (not a name Forge always uses): it moves.
        std::fs::create_dir_all(self.root.join("project-agent-space")).unwrap();
        std::fs::write(self.root.join("project-agent-space/notes.md"), "notes\n").unwrap();
        // Not Forge's: it stays.
        std::fs::create_dir_all(self.root.join("someone-elses")).unwrap();
        std::fs::write(self.root.join("someone-elses/file.txt"), "not Forge's\n").unwrap();

        let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('repo')")
            .fetch_all(self.db.pool())
            .await
            .unwrap();
        let values: Vec<String> = columns
            .iter()
            .map(|column| match column.as_str() {
                "id" => "'provisioned-repo'".to_owned(),
                "name" => "'provisioned'".to_owned(),
                "local_path" => sql_text(&format!("{root}/repos/provisioned")),
                "remote_url" => sql_text(&format!("{root}/repos/origin.git")),
                other => other.to_owned(),
            })
            .collect();
        let payload = serde_json::json!({
            "worktree": format!("{root}/x/repo"),
            "root": root,
            "neighbour": format!("{root}2/y"),
            "count": 3,
        })
        .to_string();
        let statements = [
            format!(
                "INSERT INTO repo ({}) SELECT {} FROM repo WHERE id = '{}'",
                columns.join(", "),
                values.join(", "),
                self.repo_id
            ),
            format!("INSERT INTO daemon (id, machine_id, hostname, os, arch, status, created_at, updated_at) VALUES ('embedded-daemon', 'embedded:host:os:arch', 'host', 'os', 'arch', 'offline', '{now}', '{now}'), ('other-daemon', 'other-machine', 'other', 'os', 'arch', 'offline', '{now}', '{now}')"),
            format!("INSERT INTO runtime (id, daemon_id, kind, workspace_root, status, created_at, updated_at) VALUES ('embedded-runtime', 'embedded-daemon', 'codex', {root}, 'offline', '{now}', '{now}'), ('other-runtime', 'other-daemon', 'codex', {root}, 'offline', '{now}', '{now}')", root = sql_text(&root)),
            format!("INSERT INTO agent_context_scope (id, identity_id, scope_type, scope_id, workspace_access, workspace_path, created_at, updated_at) VALUES ('scope-1', 'identity', 'account', 'account', 'account_scratch', {}, '{now}', '{now}'), ('scope-2', 'identity', 'project', 'project', 'project_verify', {}, '{now}', '{now}'), ('scope-3', 'identity', 'project', 'daemon-project', 'project_verify', {}, '{now}', '{now}')", sql_text(&format!("{root}/main-agents/account")), sql_text(&format!("{root}/project-agent-space")), sql_text(&format!("{root}/.forge/workspaces/workspace-7b0c9f6e/repo"))),
            format!("INSERT INTO agent_inquiry (id, chat_id, identity_id, owner_user_id, title, question, status, findings_path, workspace_path, created_at, updated_at, started_at) VALUES ('inquiry-1', 'chat', 'identity', 'user', 'title', 'question', 'completed', {}, {}, '{now}', '{now}', '{now}')", sql_text(&format!("{root}/main-agents/account/inquiries/one/findings.md")), sql_text(&format!("{root}/main-agents/account/inquiries/one"))),
            attempt_sql("done-attempt", "completed", &format!("{root}/.repos/{}", self.repo_id)),
            step_sql(&self.tasks[0].id, 900, "hooks", "done", &payload),
        ];
        let statements: Vec<&str> = statements.iter().map(String::as_str).collect();
        raw(&self.db, &statements).await;
    }

    /// Every column the fixture gives one row, read back: `table.column`
    /// and the value.
    async fn stored(&self) -> BTreeMap<&'static str, String> {
        let mut all = BTreeMap::new();
        for (name, sql) in [
            (
                "repo.local_path",
                "SELECT local_path FROM repo WHERE id = 'provisioned-repo'",
            ),
            (
                "repo.remote_url",
                "SELECT remote_url FROM repo WHERE id = 'provisioned-repo'",
            ),
            (
                "runtime.embedded",
                "SELECT workspace_root FROM runtime WHERE id = 'embedded-runtime'",
            ),
            (
                "runtime.other",
                "SELECT workspace_root FROM runtime WHERE id = 'other-runtime'",
            ),
            (
                "agent_context_scope.account",
                "SELECT workspace_path FROM agent_context_scope WHERE id = 'scope-1'",
            ),
            (
                "agent_context_scope.project",
                "SELECT workspace_path FROM agent_context_scope WHERE id = 'scope-2'",
            ),
            (
                "agent_context_scope.daemon",
                "SELECT workspace_path FROM agent_context_scope WHERE id = 'scope-3'",
            ),
            (
                "agent_inquiry.findings_path",
                "SELECT findings_path FROM agent_inquiry WHERE id = 'inquiry-1'",
            ),
            (
                "agent_inquiry.workspace_path",
                "SELECT workspace_path FROM agent_inquiry WHERE id = 'inquiry-1'",
            ),
            (
                "integration_attempt.repo_location_ref",
                "SELECT repo_location_ref FROM integration_attempt WHERE id = 'done-attempt'",
            ),
            (
                "task_step.payload_json",
                "SELECT payload_json FROM task_step WHERE id = 'step-900'",
            ),
            (
                "task_step.result_json",
                "SELECT result_json FROM task_step WHERE id = 'step-900'",
            ),
        ] {
            let value: String = sqlx::query_scalar(sql)
                .fetch_one(self.db.pool())
                .await
                .unwrap();
            all.insert(name, value);
        }
        all
    }

    fn choice(&self) -> RootChoice {
        RootChoice {
            configured: self.target(),
            explicit: false,
            data_dir: self.dirs.data.path().to_path_buf(),
            system_temp: self.dirs.temp.path().to_path_buf(),
            migrate_command: MIGRATE_COMMAND.to_owned(),
        }
    }

    /// The default target, as the move records it.
    fn target(&self) -> PathBuf {
        self.dirs
            .data
            .path()
            .canonicalize()
            .unwrap()
            .join("worktrees")
    }

    fn request(&self) -> MigrateRequest {
        MigrateRequest::new(
            self.dirs.data.path().to_path_buf(),
            None,
            self.dirs.temp.path().to_path_buf(),
            0,
        )
    }

    fn scheduler(&self, root: &Path) -> WorkspaceCleanupScheduler {
        WorkspaceCleanupScheduler::new(
            Arc::new(self.db.clone()),
            Arc::new(events::EventBus::new(16)),
            root.to_path_buf(),
        )
    }

    async fn workspace(&self, index: usize) -> Workspace {
        WorkspaceRepo::get_by_task_id(&self.db, &self.tasks[index].id)
            .await
            .unwrap()
            .expect("workspace row")
    }

    async fn worktree(&self, index: usize) -> PathBuf {
        PathBuf::from(
            self.workspace(index)
                .await
                .embedded_worktree_path_for_backend(),
        )
    }

    async fn statuses(&self) -> Vec<String> {
        let mut all = Vec::new();
        for index in 0..self.tasks.len() {
            let worktree = self.worktree(index).await;
            all.push(format!(
                "{}{}",
                git_ok(&worktree, &["status", "--porcelain=v1", "--branch"]),
                git_ok(&worktree, &["rev-parse", "HEAD"])
            ));
        }
        all
    }

    fn journal(&self) -> PathBuf {
        self.dirs.data.path().join(JOURNAL_FILE)
    }

    async fn snapshot(&self) -> Before {
        Before {
            files: tree(&self.root),
            statuses: self.statuses().await,
            location_version: self.location_version().await,
        }
    }

    async fn location_version(&self) -> i64 {
        sqlx::query_scalar("SELECT version FROM repo_location WHERE repo_id = ?")
            .bind(&self.repo_id)
            .fetch_one(self.db.pool())
            .await
            .unwrap()
    }

    /// Everything the move promises, checked against the state before it.
    async fn assert_moved(&self, before: &Before) {
        let target = self.target();
        // Every file is where it was, relative to the root, byte for byte.
        let after = tree(&target);
        let missing: Vec<_> = before
            .files
            .iter()
            .filter(|(path, entry)| after.get(*path) != Some(entry))
            .map(|(path, _)| path)
            .collect();
        assert!(missing.is_empty(), "changed or missing: {missing:?}");
        let extra: Vec<_> = after
            .keys()
            .filter(|path| !before.files.contains_key(*path))
            .collect();
        assert!(extra.is_empty(), "appeared: {extra:?}");
        // The old root keeps the marker and what a daemon owns there, and
        // nothing else.
        let left: Vec<String> = tree_with(&self.root, true).into_keys().collect();
        assert_eq!(
            left,
            [
                ".forge",
                ".forge/gc",
                ".forge/gc/daemon-owner",
                ".forge/workspaces",
                ".forge/workspaces/workspace-7b0c9f6e",
                ".forge/workspaces/workspace-7b0c9f6e/repo",
                ".forge/workspaces/workspace-7b0c9f6e/repo/daemon.txt",
                "someone-elses",
                "someone-elses/file.txt",
            ]
        );
        assert!(!target.join("someone-elses").exists());
        assert_eq!(
            std::fs::read_to_string(
                self.root
                    .join(".forge/workspaces/workspace-7b0c9f6e/repo/daemon.txt")
            )
            .unwrap(),
            "daemon work\n"
        );
        assert!(!target.join(".forge/workspaces").exists());
        assert!(!target.join(gc::GC_DIR).join(gc::DAEMON_OWNER_FILE).exists());
        assert!(std::fs::read_to_string(self.root.join(MOVED_MARKER))
            .unwrap()
            .contains(&target.display().to_string()));
        assert!(!self.journal().exists());

        // The database names the new root and only the new root.
        assert_eq!(recorded_root(&self.db).await.unwrap(), Some(target.clone()));
        for index in 0..self.tasks.len() {
            let worktree = self.worktree(index).await;
            assert!(worktree.starts_with(&target), "{}", worktree.display());
            assert!(worktree.is_dir(), "{}", worktree.display());
        }
        let location: String =
            sqlx::query_scalar("SELECT path FROM repo_location WHERE repo_id = ?")
                .bind(&self.repo_id)
                .fetch_one(self.db.pool())
                .await
                .unwrap();
        assert_eq!(
            PathBuf::from(&location),
            target.join(".repos").join(&self.repo_id)
        );
        assert!(Path::new(&location).join(".git").is_dir());
        assert_eq!(self.location_version().await, before.location_version + 1);
        for index in 0..self.tasks.len() {
            let handle: String = sqlx::query_scalar(
                "SELECT workspace_handle FROM workspace_placement WHERE task_id = ?",
            )
            .bind(&self.tasks[index].id)
            .fetch_one(self.db.pool())
            .await
            .unwrap();
            assert_eq!(PathBuf::from(handle), self.worktree(index).await);
        }
        let logs: String = sqlx::query_scalar("SELECT logs_path FROM execution")
            .fetch_one(self.db.pool())
            .await
            .unwrap();
        assert!(Path::new(&logs).starts_with(target.join(".forge/logs")));
        assert_eq!(std::fs::read_to_string(&logs).unwrap(), LOG_LINE);
        // Every other column that stores a path, each with a real row.
        let old = self.root.display().to_string();
        let new = target.display().to_string();
        let stored = self.stored().await;
        for (column, rest) in [
            ("repo.local_path", "/repos/provisioned"),
            ("repo.remote_url", "/repos/origin.git"),
            ("runtime.embedded", ""),
            ("agent_context_scope.account", "/main-agents/account"),
            ("agent_context_scope.project", "/project-agent-space"),
            (
                "agent_inquiry.findings_path",
                "/main-agents/account/inquiries/one/findings.md",
            ),
            (
                "agent_inquiry.workspace_path",
                "/main-agents/account/inquiries/one",
            ),
        ] {
            assert_eq!(stored[column], format!("{new}{rest}"), "{column}");
        }
        assert_eq!(
            stored["integration_attempt.repo_location_ref"],
            format!("{new}/.repos/{}", self.repo_id)
        );
        // Another daemon's runtime keeps the root it reported, and a path
        // into what a daemon keeps in the old root still names it there.
        assert_eq!(stored["runtime.other"], old);
        assert_eq!(
            stored["agent_context_scope.daemon"],
            format!("{old}/.forge/workspaces/workspace-7b0c9f6e/repo")
        );
        assert!(Path::new(&stored["agent_context_scope.daemon"]).is_dir());
        // JSON: parsed back, the root and paths under it are rewritten, a
        // path that only starts with the same text is not, nothing else
        // changed.
        for column in ["task_step.payload_json", "task_step.result_json"] {
            let document: serde_json::Value = serde_json::from_str(&stored[column]).unwrap();
            assert_eq!(
                document,
                serde_json::json!({
                    "worktree": format!("{new}/x/repo"),
                    "root": new,
                    "neighbour": format!("{old}2/y"),
                    "count": 3,
                }),
                "{column}"
            );
        }
        assert_eq!(
            std::fs::read_to_string(target.join("project-agent-space/notes.md")).unwrap(),
            "notes\n"
        );
        // What still mentions the old root is exactly what must: the other
        // daemon's runtime and the neighbouring path.
        for spelling in spellings(&self.root) {
            let left = mentions(&self.db, &spelling).await.unwrap();
            let left: Vec<&str> = left.iter().map(|(column, _)| column.as_str()).collect();
            assert!(
                left.iter().all(|column| [
                    "runtime.workspace_root",
                    "agent_context_scope.workspace_path",
                    "task_step.payload_json",
                    "task_step.result_json"
                ]
                .contains(column)),
                "{left:?}"
            );
        }
        // A user's repository did not move, and is named as before.
        let user_path: String = sqlx::query_scalar(
            "SELECT local_path FROM repo WHERE local_path IS NOT NULL AND id != 'provisioned-repo'",
        )
        .fetch_one(self.db.pool())
        .await
        .unwrap();
        assert_eq!(PathBuf::from(user_path), self.user_repo);

        // Git sees the same worktrees in the same state.
        assert_eq!(self.statuses().await, before.statuses);
        let old = self.root.display().to_string();
        for (repository, tasks) in [
            (&target.join(".repos").join(&self.repo_id), 0..2),
            (&self.user_repo, 2..3),
        ] {
            let listed = git_ok(repository, &["worktree", "list", "--porcelain"]);
            assert!(!listed.contains(&old), "{listed}");
            for index in tasks {
                let worktree = self.worktree(index).await;
                assert!(
                    listed.contains(&format!("worktree {}\n", worktree.display())),
                    "{listed}"
                );
                let dot_git = std::fs::read_to_string(worktree.join(".git")).unwrap();
                assert!(
                    dot_git.starts_with("gitdir: ") && !dot_git.contains(&old),
                    "{dot_git}"
                );
                let admin = PathBuf::from(dot_git.trim().trim_start_matches("gitdir: "));
                assert_eq!(
                    std::fs::read_to_string(admin.join("gitdir"))
                        .unwrap()
                        .trim(),
                    worktree.join(".git").display().to_string()
                );
            }
        }
        // Work goes on: a commit in a moved worktree.
        let first = self.worktree(0).await;
        std::fs::write(first.join("after-move.txt"), "new work\n").unwrap();
        git_ok(&first, &["add", "-A"]);
        git_ok(&first, &["commit", "-m", "after the move"]);
        assert!(git_ok(&first, &["log", "--oneline"]).contains("after the move"));

        // The next start runs on the new root, with nothing to report.
        let settled = settle(&self.db, &self.choice()).await.unwrap();
        assert_eq!(
            (settled.root.clone(), settled.in_system_temp),
            (target.clone(), false)
        );

        // Garbage collection owns the new root and takes nothing from it.
        let scheduler = self.scheduler(&target);
        assert_eq!(
            scheduler.adopt_workspace_root().await.unwrap(),
            gc::Ownership::Mine
        );
        scheduler.sweep().await.unwrap();
        let quarantined: Vec<_> = std::fs::read_dir(target.join(gc::GC_DIR))
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| uuid::Uuid::parse_str(name.get(..36).unwrap_or_default()).is_ok())
            .collect();
        assert!(quarantined.is_empty(), "{quarantined:?}");
        for index in 0..self.tasks.len() {
            assert!(self.worktree(index).await.is_dir());
        }
        assert!(target.join(".repos").join(&self.repo_id).is_dir());

        // A run can start in a moved worktree.
        let router = crate::lifecycle::context::embedded_workspace_router_for_test(
            Arc::new(self.db.clone()),
            target.clone(),
            None,
        );
        for index in 0..self.tasks.len() {
            let valid = WorkspaceManager::new(&self.db, &target, None, &router)
                .ensure_valid(
                    &self.tasks[index],
                    self.workspace(index).await,
                    Purpose::Execute,
                )
                .await
                .unwrap_or_else(|error| panic!("workspace {index} is not valid: {error:?}"));
            assert_eq!(valid.repair(), crate::workspace_manager::Repair::None);
            assert!(valid.path().is_some_and(|path| path.starts_with(&target)));
            assert!(valid.on_task_branch());
        }
    }

    /// A refused move changed nothing.
    async fn assert_untouched(&self, before: &Before) {
        assert_eq!(tree(&self.root), before.files);
        assert_eq!(self.statuses().await, before.statuses);
        assert!(!self.journal().exists());
        assert_eq!(
            recorded_root(&self.db).await.unwrap(),
            Some(self.root.clone())
        );
        assert!(!self.root.join(MOVED_MARKER).exists());
    }
}

struct Before {
    files: BTreeMap<String, Entry>,
    statuses: Vec<String>,
    location_version: i64,
}

#[derive(Debug, PartialEq, Eq)]
enum Entry {
    Dir(u32),
    File(u32, Vec<u8>),
    Link(PathBuf),
}

/// Every entry under `root` by relative path. The files Git itself rewrites
/// when a worktree moves are left out: the two link files (`.git` in a
/// worktree, `gitdir` in the repository) and the index, whose cached file
/// times a copy changes.
fn tree(root: &Path) -> BTreeMap<String, Entry> {
    tree_with(root, false)
}

/// `daemon`: include what a daemon sharing the root owns (which a move
/// leaves where it is) instead of leaving it out.
fn tree_with(root: &Path, daemon: bool) -> BTreeMap<String, Entry> {
    fn walk(root: &Path, path: &Path, daemon: bool, all: &mut BTreeMap<String, Entry>) {
        for entry in std::fs::read_dir(path).unwrap().flatten() {
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).unwrap();
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let name = entry.file_name().to_string_lossy().into_owned();
            if !daemon
                && (relative == ".forge/workspaces"
                    || relative == ".forge/gc/daemon-owner"
                    || relative == "someone-elses")
            {
                continue;
            }
            #[cfg(unix)]
            let mode = std::os::unix::fs::PermissionsExt::mode(&metadata.permissions()) & 0o7777;
            #[cfg(not(unix))]
            let mode = 0;
            if metadata.file_type().is_symlink() {
                all.insert(relative, Entry::Link(std::fs::read_link(&path).unwrap()));
            } else if metadata.is_dir() {
                all.insert(relative, Entry::Dir(mode));
                walk(root, &path, daemon, all);
            } else if !(name == "gitdir"
                || name == "index"
                || name == MOVED_MARKER
                || (name == ".git" && metadata.is_file()))
            {
                all.insert(relative, Entry::File(mode, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut all = BTreeMap::new();
    walk(root, root, daemon, &mut all);
    all
}

#[tokio::test]
async fn a_move_on_one_filesystem_keeps_every_file_git_state_and_database_path() {
    let install = Install::new().await;
    let before = install.snapshot().await;
    assert!(
        before.statuses[1].contains(" M README.md"),
        "{:?}",
        before.statuses
    );
    assert!(
        before.statuses[1].contains("?? notes/"),
        "{:?}",
        before.statuses
    );

    let user_repo_before = tree(&install.user_repo);
    let report = migrate(&install.db, &install.request()).await.unwrap();
    assert_eq!(report.source, install.root);
    assert_eq!(report.target, install.target());
    assert!(!report.copied && !report.resumed);
    assert_eq!(report.worktrees.len(), 3);
    assert_eq!(report.gc_state, "owned");
    assert!(report
        .moved
        .contains(&format!(".repos/{}", install.repo_id)));
    assert!(report
        .moved
        .iter()
        .any(|unit| unit.starts_with(".forge/logs/")));
    assert!(report.moved.contains(&"repos/provisioned".to_owned()));
    assert!(report.moved.contains(&"main-agents/account".to_owned()));
    assert!(report.moved.contains(&".forge/gc/owner".to_owned()));
    assert!(!report
        .moved
        .iter()
        .any(|unit| unit.contains("workspaces") || unit.contains("daemon-owner")));
    for task in &install.tasks {
        assert!(report.moved.contains(&task.id), "{:?}", report.moved);
    }
    let rewritten = |column: &str| {
        report
            .rows
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, rows)| *rows)
    };
    assert_eq!(rewritten("workspace.worktree_path"), Some(3));
    assert_eq!(rewritten("repo_location.path"), Some(1));
    assert_eq!(rewritten("workspace_placement.workspace_handle"), Some(3));
    assert_eq!(rewritten("execution.logs_path"), Some(1));
    for column in [
        "repo.local_path",
        "repo.remote_url",
        "agent_inquiry.workspace_path",
        "agent_inquiry.findings_path",
        "runtime.workspace_root",
        "integration_attempt.repo_location_ref",
    ] {
        assert_eq!(rewritten(column), Some(1), "{column}");
    }
    // The fixture's step, and any step the workspace preparation recorded.
    for column in ["task_step.payload_json", "task_step.result_json"] {
        assert!(rewritten(column) >= Some(1), "{column}");
    }
    assert_eq!(rewritten("agent_context_scope.workspace_path"), Some(2));
    assert_eq!(report.left_behind, ["someone-elses"]);
    assert!(report.moved.contains(&"project-agent-space".to_owned()));
    assert!(report.not_copied.is_empty() && report.warnings.is_empty());
    // A user's own repository: Git re-registered the worktree there, and
    // nothing else in it changed.
    assert_eq!(
        report.user_repositories,
        std::slice::from_ref(&install.user_repo)
    );
    assert_eq!(tree(&install.user_repo), user_repo_before);
    let summary = report.to_string();
    assert!(summary.contains("left in the old root (not this database's): someone-elses"));
    assert!(summary.contains("`git worktree repair` was run in your repository"));
    assert!(
        summary.contains("Daemon-owned workspaces were not touched"),
        "{summary}"
    );
    install.assert_moved(&before).await;

    // Asking again is not an error and moves nothing.
    let again = migrate(&install.db, &install.request()).await.unwrap();
    assert!(again.nothing_to_move.is_some());
}

#[tokio::test]
async fn a_move_across_filesystems_copies_compares_and_only_then_removes() {
    let install = Install::new().await;
    let before = install.snapshot().await;
    let report = migrate(&install.db, &install.request().copying())
        .await
        .unwrap();
    assert!(report.copied);
    assert_eq!(report.worktrees.len(), 3);
    install.assert_moved(&before).await;
}

/// A crash after any step: no server starts on the half-moved install, and
/// running the command again finishes the move with nothing lost.
async fn crash_at_every_step(copying: bool) {
    let request = |install: &Install| {
        if copying {
            install.request().copying()
        } else {
            install.request()
        }
    };
    let steps = {
        let install = Install::new().await;
        migrate(&install.db, &request(&install))
            .await
            .unwrap()
            .steps
    };
    assert!(steps.len() > 10, "{steps:?}");
    for prefix in [
        "plan",
        "moved:",
        "repaired",
        "database:uncommitted",
        "database",
        "gc",
        "marker",
    ] {
        assert!(
            steps.iter().any(|step| step.starts_with(prefix)),
            "{prefix}: {steps:?}"
        );
    }
    for (index, step) in steps.iter().enumerate() {
        let install = Install::new().await;
        let before = install.snapshot().await;
        let crashed = migrate(&install.db, &request(&install).crashing_at(index))
            .await
            .unwrap_err();
        assert!(
            matches!(crashed, WorkspaceRootError::Incomplete(_)),
            "{step}: {crashed:?}"
        );
        // The half-moved install is not started on.
        assert!(install.journal().exists(), "{step}");
        let refused = settle(&install.db, &install.choice()).await.unwrap_err();
        assert!(
            refused.to_string().contains(MIGRATE_COMMAND),
            "{step}: {refused}"
        );
        // The database names one root, never a mix of the two.
        let recorded = recorded_root(&install.db).await.unwrap().unwrap();
        for index in 0..install.tasks.len() {
            assert!(
                install.worktree(index).await.starts_with(&recorded),
                "{step}"
            );
        }
        // Another target is refused while this move is unfinished.
        let elsewhere = MigrateRequest::new(
            install.dirs.data.path().to_path_buf(),
            Some(install.dirs.data.path().join("elsewhere")),
            install.dirs.temp.path().to_path_buf(),
            0,
        );
        assert!(matches!(
            migrate(&install.db, &elsewhere).await.unwrap_err(),
            WorkspaceRootError::Refused(_)
        ));

        let report = migrate(&install.db, &install.request())
            .await
            .unwrap_or_else(|error| panic!("rerun after a crash at {step}: {error}"));
        assert!(report.resumed, "{step}");
        assert_eq!(report.copied, copying, "{step}");
        install.assert_moved(&before).await;
    }
}

#[tokio::test]
async fn a_crash_at_any_step_of_a_rename_move_is_finished_by_a_rerun() {
    crash_at_every_step(false).await;
}

#[tokio::test]
async fn a_crash_at_any_step_of_a_copy_move_is_finished_by_a_rerun() {
    crash_at_every_step(true).await;
}

#[tokio::test]
async fn a_copy_that_differs_keeps_the_original() {
    let dir = TempDir::new().unwrap();
    let (from, to) = (dir.path().join("from"), dir.path().join("to"));
    std::fs::create_dir_all(from.join("deep")).unwrap();
    std::fs::write(from.join("deep/file"), "original").unwrap();
    copy_tree(&from, &to, &mut HashMap::new()).unwrap();
    assert_eq!(compare_trees(&from, &to), Ok(()));
    std::fs::write(to.join("deep/file"), "0riginal").unwrap();
    assert!(compare_trees(&from, &to).unwrap_err().contains("content"));
    std::fs::write(to.join("deep/file"), "original!").unwrap();
    assert!(compare_trees(&from, &to).unwrap_err().contains("size"));
    std::fs::remove_file(to.join("deep/file")).unwrap();
    assert!(compare_trees(&from, &to).unwrap_err().contains("entries"));
}

#[tokio::test]
async fn a_move_is_refused_while_a_run_is_recorded_running() {
    let install = Install::new().await;
    let before = install.snapshot().await;
    let workspace = install.workspace(1).await;
    let execution = seed_running_execution(&install.db, &workspace).await;
    let refused = migrate(&install.db, &install.request()).await.unwrap_err();
    let WorkspaceRootError::Refused(message) = &refused else {
        panic!("{refused:?}");
    };
    assert!(message.contains("1 running execution(s)"), "{message}");
    install.assert_untouched(&before).await;
    assert!(!install.target().exists());

    // A claimed hook step and a running check refuse it too.
    sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
        .bind(&execution)
        .execute(install.db.pool())
        .await
        .unwrap();
    assert!(Running::read(&install.db).await.unwrap().is_empty());
    // Each kind of recorded work in flight refuses it, with a real row.
    let task = &install.tasks[0];
    let project_id = task.project_id.clone();
    let rows: [(&str, String, &str); 5] = [
        (
            "1 claimed or suspended task step(s)",
            step_sql(&task.id, 901, "hooks", "claimed", "{}"),
            "DELETE FROM task_step WHERE seq = 901",
        ),
        (
            "1 claimed or suspended task step(s)",
            step_sql(&task.id, 902, "hooks", "suspended", "{}"),
            "DELETE FROM task_step WHERE seq = 902",
        ),
        (
            "1 integration attempt(s)",
            attempt_sql("live-attempt", "queued", "/elsewhere"),
            "DELETE FROM integration_attempt WHERE id = 'live-attempt'",
        ),
        (
            "1 integration attempt(s)",
            attempt_sql("review-attempt", "needs_review", "/elsewhere"),
            "DELETE FROM integration_attempt WHERE id = 'review-attempt'",
        ),
        (
            "1 running check run(s)",
            format!(
                "INSERT INTO check_run (id, project_id, repo_id, commit_sha, spec_digest, identity_key, input_json, cacheable, state, operation_id, created_at, updated_at)
                 VALUES ('check-1', '{project_id}', '{}', 'sha', 'digest', 'identity', '{{}}', 0, 'running', 'operation-1', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
                install.repo_id
            ),
            "DELETE FROM check_run WHERE id = 'check-1'",
        ),
    ];
    for (expected, insert, delete) in rows {
        raw(&install.db, &[insert.as_str()]).await;
        let refused = migrate(&install.db, &install.request()).await.unwrap_err();
        let WorkspaceRootError::Refused(message) = &refused else {
            panic!("{expected}: {refused:?}");
        };
        assert!(message.contains(expected), "{expected}: {message}");
        assert!(message.contains(MIGRATE_COMMAND), "{message}");
        install.assert_untouched(&before).await;
        assert!(!install.target().exists(), "{expected}");
        raw(&install.db, &[delete]).await;
        assert!(
            Running::read(&install.db).await.unwrap().is_empty(),
            "{expected}"
        );
    }
    // An active lease refuses it as well. (The schema lets only the
    // scheduler's governed path write one, so no fixture row: the count is
    // the query `status = 'active' AND expires_at > now`.)
    let leased = Running {
        leases: 1,
        ..Running::default()
    };
    assert!(!leased.is_empty());
    assert!(leased.to_string().contains("1 active workspace lease(s)"));
    let _ = execution;
    // A parked attempt holds nothing.
    raw(
        &install.db,
        &[attempt_sql("parked-attempt", "parked", "/elsewhere").as_str()],
    )
    .await;
    assert!(Running::read(&install.db).await.unwrap().is_empty());
}

/// Fixture rows written straight into tables whose own writers need a
/// whole running system: constraints are off for these statements only.
async fn raw(db: &SqliteDb, statements: &[&str]) {
    let mut connection = db.pool().acquire().await.unwrap();
    for pragma in [
        "PRAGMA foreign_keys = OFF",
        "PRAGMA ignore_check_constraints = ON",
    ] {
        sqlx::query(pragma).execute(&mut *connection).await.unwrap();
    }
    for statement in statements {
        sqlx::query(statement)
            .execute(&mut *connection)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    for pragma in [
        "PRAGMA ignore_check_constraints = OFF",
        "PRAGMA foreign_keys = ON",
    ] {
        sqlx::query(pragma).execute(&mut *connection).await.unwrap();
    }
}

fn sql_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn step_sql(task_id: &str, seq: i64, kind: &str, status: &str, payload: &str) -> String {
    format!(
        "INSERT INTO task_step (id, task_id, seq, kind, payload_json, result_json, causation_key, chain_id, chain_position, expected_status, expected_version, status, available_at, created_at, updated_at)
         VALUES ('step-{seq}', '{task_id}', {seq}, '{kind}', {payload}, {payload}, 'cause-{seq}', 'chain-{seq}', 1, 'in_progress', 1, '{status}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        payload = sql_text(payload)
    )
}

fn attempt_sql(id: &str, state: &str, location: &str) -> String {
    format!(
        "INSERT INTO integration_attempt (id, task_ref, project_ref, queue_seq, current, admission_key, expected_status, expected_epoch, observed_task_version, enqueued_at, repo_location_ref, state, created_at, updated_at)
         VALUES ('{id}', 'task', 'project', 1, 0, 'admission-{id}', 'merging', 0, 1, '2026-01-01T00:00:00Z', {}, '{state}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        sql_text(location)
    )
}

#[tokio::test]
async fn a_move_is_refused_into_a_directory_that_is_not_empty() {
    let install = Install::new().await;
    let before = install.snapshot().await;
    std::fs::create_dir_all(install.target()).unwrap();
    std::fs::write(install.target().join("somebody-elses-file"), "keep").unwrap();
    let refused = migrate(&install.db, &install.request()).await.unwrap_err();
    assert!(refused.to_string().contains("not empty"), "{refused}");
    install.assert_untouched(&before).await;
    assert_eq!(
        std::fs::read_to_string(install.target().join("somebody-elses-file")).unwrap(),
        "keep"
    );
}

#[tokio::test]
async fn a_move_is_refused_into_the_old_root_or_around_it() {
    let install = Install::new().await;
    let before = install.snapshot().await;
    let to = |target: PathBuf| {
        MigrateRequest::new(
            install.dirs.data.path().to_path_buf(),
            Some(target),
            install.dirs.temp.path().to_path_buf(),
            0,
        )
    };
    let inside = install.root.join("moved-here");
    let refused = migrate(&install.db, &to(inside.clone())).await.unwrap_err();
    assert!(
        refused.to_string().contains("inside the old one"),
        "{refused}"
    );
    assert!(!inside.exists());
    let around = install.root.parent().unwrap().to_path_buf();
    let refused = migrate(&install.db, &to(around)).await.unwrap_err();
    assert!(
        matches!(refused, WorkspaceRootError::Refused(_)),
        "{refused:?}"
    );
    // A file where the new root should be.
    let file = install.dirs.data.path().join("a-file");
    std::fs::write(&file, "x").unwrap();
    let refused = migrate(&install.db, &to(file)).await.unwrap_err();
    assert!(refused.to_string().contains("not a directory"), "{refused}");
    install.assert_untouched(&before).await;
}

#[tokio::test]
async fn a_move_is_refused_into_a_directory_that_cannot_be_a_workspace_root() {
    // The rule itself: what the garbage collector would never sweep is
    // never made a root. (The command-line test drives the whole refusal
    // with the home directory as the target.)
    let dir = TempDir::new().unwrap();
    let root = dir.path().canonicalize().unwrap().join("root");
    std::fs::create_dir_all(root.join(".git")).unwrap();
    assert!(gc::refuse_root(&root).is_some());
}

#[tokio::test]
async fn a_copy_is_refused_without_room_for_it() {
    let install = Install::new().await;
    let before = install.snapshot().await;
    let request = MigrateRequest::new(
        install.dirs.data.path().to_path_buf(),
        None,
        install.dirs.temp.path().to_path_buf(),
        u64::MAX / 2,
    )
    .copying();
    let refused = migrate(&install.db, &request).await.unwrap_err();
    assert!(refused.to_string().contains("bytes free"), "{refused}");
    install.assert_untouched(&before).await;
    // The directory the refused run made is gone again.
    assert!(!install.target().exists());
}

#[tokio::test]
async fn nothing_is_moved_for_a_database_without_a_root() {
    let db = sqlite_db().await;
    let dirs = Dirs::new();
    let request = MigrateRequest::new(
        dirs.data.path().to_path_buf(),
        None,
        dirs.temp.path().to_path_buf(),
        0,
    );
    let report = migrate(&db, &request).await.unwrap();
    assert!(report.nothing_to_move.is_some());
    assert!(!dirs.default_root().exists());
    assert_eq!(recorded_root(&db).await.unwrap(), None);
}

#[tokio::test]
async fn a_root_the_system_already_emptied_is_moved_in_the_database() {
    let install = Install::new().await;
    // The temp cleaner took everything.
    gc::remove_exact(&install.root, &mut gc::GcReport::default());
    assert!(!install.root.exists());
    let report = migrate(&install.db, &install.request()).await.unwrap();
    assert!(report.moved.is_empty());
    assert_eq!(
        recorded_root(&install.db).await.unwrap(),
        Some(install.target())
    );
    assert!(install.worktree(0).await.starts_with(install.target()));
    let _ = (&install.clone, &install.log_path);
}

#[test]
fn every_stored_path_is_named() {
    let columns = stored_path_columns();
    for column in [
        "workspace.worktree_path",
        "repo_location.path",
        "workspace_placement.workspace_handle",
        "task_step.payload_json",
        "repo.local_path",
        "execution.logs_path",
        "agent_context_scope.workspace_path",
        "agent_inquiry.workspace_path",
        "agent_inquiry.findings_path",
        "runtime.workspace_root",
        "system_setting[workspace_root]",
    ] {
        assert!(columns.iter().any(|known| known == column), "{column}");
    }
}

/// The post-condition of the database rewrite: a column that stores a path
/// and still names the old root fails the command, commits nothing, and
/// leaves the journal saying the database is pending.
#[tokio::test]
async fn a_column_still_naming_the_old_root_fails_the_command_with_the_database_pending() {
    let install = Install::new().await;
    let before = install.snapshot().await;
    let failed = migrate(
        &install.db,
        &install.request().skipping_column("worktree_path"),
    )
    .await
    .unwrap_err();
    let WorkspaceRootError::Incomplete(message) = &failed else {
        panic!("{failed:?}");
    };
    assert!(message.starts_with("db pending"), "{message}");
    assert!(
        message.contains("workspace.worktree_path (3 row(s))"),
        "{message}"
    );
    assert!(message.contains(MIGRATE_COMMAND), "{message}");
    // Nothing of the rewrite was committed: one root in the database.
    assert_eq!(
        recorded_root(&install.db).await.unwrap(),
        Some(install.root.clone())
    );
    assert_eq!(
        install.stored().await["runtime.embedded"],
        install.root.display().to_string()
    );
    let journal: Journal =
        serde_json::from_str(&std::fs::read_to_string(install.journal()).unwrap()).unwrap();
    assert_eq!(journal.phase, Phase::Repaired);
    let refused = settle(&install.db, &install.choice()).await.unwrap_err();
    assert!(
        refused.to_string().starts_with("migration in progress"),
        "{refused}"
    );
    // The same command finishes it.
    let report = migrate(&install.db, &install.request()).await.unwrap();
    assert!(report.resumed);
    install.assert_moved(&before).await;
}

/// A copy never removes what it did not copy: a pipe a dead run left stays
/// in the old root, and the report names it. Hard links stay hard links.
#[cfg(unix)]
#[tokio::test]
async fn a_copy_leaves_pipes_where_they_are_and_keeps_hard_links() {
    use std::os::unix::fs::MetadataExt;
    let install = Install::new().await;
    let run = install.root.join(".forge-tmp/7b0c9f6e12");
    std::fs::create_dir_all(&run).unwrap();
    std::fs::write(run.join("scratch.txt"), "scratch\n").unwrap();
    let made = Command::new("mkfifo")
        .arg(run.join("pipe"))
        .status()
        .unwrap();
    assert!(made.success());
    let first = install.worktree(0).await;
    std::fs::write(first.join("linked-a"), "one file, two names\n").unwrap();
    std::fs::hard_link(first.join("linked-a"), first.join("linked-b")).unwrap();

    let report = migrate(&install.db, &install.request().copying())
        .await
        .unwrap();
    let target = install.target();
    assert_eq!(report.not_copied, [run.join("pipe")]);
    assert!(report
        .to_string()
        .contains("a socket, pipe or device cannot be copied"));
    // The pipe and the directories above it are still there, nothing else.
    assert!(std::fs::symlink_metadata(run.join("pipe")).is_ok());
    assert!(!run.join("scratch.txt").exists());
    assert_eq!(
        std::fs::read_to_string(target.join(".forge-tmp/7b0c9f6e12/scratch.txt")).unwrap(),
        "scratch\n"
    );
    assert!(!target.join(".forge-tmp/7b0c9f6e12/pipe").exists());
    let moved = install.worktree(0).await;
    let (a, b) = (
        std::fs::metadata(moved.join("linked-a")).unwrap(),
        std::fs::metadata(moved.join("linked-b")).unwrap(),
    );
    assert_eq!((a.ino(), a.nlink()), (b.ino(), 2));
    assert_eq!(
        std::fs::read_to_string(moved.join("linked-b")).unwrap(),
        "one file, two names\n"
    );
}

/// The data directory was moved by hand with its worktrees inside: the old
/// root is gone and the files already are at the new one. The command moves
/// nothing, repairs Git's links and points the database at them.
#[tokio::test]
async fn a_root_moved_by_hand_is_adopted_where_it_is() {
    let install = Install::new().await;
    let statuses = install.statuses().await;
    let target = install.target();
    std::fs::rename(&install.root, &target).unwrap();
    assert!(!install.root.exists());

    let report = migrate(&install.db, &install.request()).await.unwrap();
    assert!(report.moved.is_empty());
    assert_eq!(report.worktrees.len(), 3);
    assert_eq!(
        recorded_root(&install.db).await.unwrap(),
        Some(target.clone())
    );
    for index in 0..install.tasks.len() {
        assert!(install.worktree(index).await.starts_with(&target));
    }
    assert_eq!(install.statuses().await, statuses);
    assert!(settle(&install.db, &install.choice()).await.is_ok());
}

/// A submodule or nested worktree whose link names the old root by absolute
/// path is named and the move refused, before anything changes.
#[tokio::test]
async fn a_nested_repository_linked_by_absolute_path_refuses_the_move() {
    let install = Install::new().await;
    let nested = install.worktree(1).await.join("vendor/library");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(
        nested.join(".git"),
        format!(
            "gitdir: {}/.repos/{}/.git/modules/library\n",
            install.root.display(),
            install.repo_id
        ),
    )
    .unwrap();
    let before = install.snapshot().await;
    let refused = migrate(&install.db, &install.request()).await.unwrap_err();
    let WorkspaceRootError::Refused(message) = &refused else {
        panic!("{refused:?}");
    };
    assert!(message.contains("vendor/library/.git"), "{message}");
    assert!(message.contains("submodule"), "{message}");
    install.assert_untouched(&before).await;
    assert!(!install.target().exists());
    // Without it the move goes through.
    std::fs::remove_dir_all(nested.parent().unwrap()).unwrap();
    assert!(migrate(&install.db, &install.request()).await.is_ok());
}

/// Every data directory on a machine used to share the temp-directory
/// default. What another database keeps in the shared root stays there:
/// its Task roots, its clones, its logs, and its garbage-collection claim.
#[tokio::test]
async fn what_another_database_keeps_in_a_shared_root_stays() {
    let install = Install::new().await;
    let root = &install.root;
    let theirs = [
        "9a1b2c3d-0000-4000-8000-00000000abcd/repo/work.txt",
        ".repos/their-repository/HEAD",
        ".forge/logs/their-project/their-task/run.jsonl",
        ".forge/build/their-cache/object",
        ".forge-tmp/their-run/scratch",
        "repos/their-provisioned/file",
    ];
    for path in theirs {
        std::fs::create_dir_all(root.join(path).parent().unwrap()).unwrap();
        std::fs::write(root.join(path), "theirs\n").unwrap();
    }
    // The other database adopted the root for garbage collection.
    let marker = root.join(gc::GC_DIR).join(gc::OWNER_FILE);
    std::fs::write(&marker, "another-database").unwrap();
    let statuses = install.statuses().await;

    let report = migrate(&install.db, &install.request()).await.unwrap();
    let target = install.target();
    for path in theirs {
        assert_eq!(
            std::fs::read_to_string(root.join(path)).unwrap(),
            "theirs\n",
            "{path}"
        );
        assert!(!target.join(path).exists(), "{path}");
    }
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        "another-database"
    );
    for entry in [
        "9a1b2c3d-0000-4000-8000-00000000abcd",
        ".repos/their-repository",
        ".forge/logs/their-project",
        ".forge/build",
        ".forge-tmp",
        ".forge/gc/owner",
        "repos/their-provisioned",
        "someone-elses",
    ] {
        assert!(
            report.left_behind.contains(&entry.to_owned()),
            "{entry}: {:?}",
            report.left_behind
        );
    }
    // This database's own things moved and work; the new root is its own.
    assert_eq!(report.gc_state, "owned");
    assert_eq!(install.statuses().await, statuses);
    for index in 0..install.tasks.len() {
        assert!(install.worktree(index).await.starts_with(&target));
    }
    assert!(target.join(".repos").join(&install.repo_id).is_dir());
    let log = install.log_path.strip_prefix(root).unwrap();
    assert!(target.join(log).is_file());
}

/// Without a named root the move goes to the configured root; and a move
/// that did not finish is finished by the bare command whatever root is
/// configured by then (only a root the operator names can conflict).
#[tokio::test]
async fn the_configured_root_is_the_default_target_and_never_blocks_a_rerun() {
    let install = Install::new().await;
    let configured = install
        .dirs
        .data
        .path()
        .canonicalize()
        .unwrap()
        .join("configured-root");
    let crashed = migrate(
        &install.db,
        &install
            .request()
            .with_default_target(Some(configured.clone()))
            .crashing_at(3),
    )
    .await
    .unwrap_err();
    assert!(matches!(crashed, WorkspaceRootError::Incomplete(_)));
    let other = install.dirs.data.path().join("configured-later");
    let report = migrate(
        &install.db,
        &install.request().with_default_target(Some(other.clone())),
    )
    .await
    .unwrap();
    assert!(report.resumed);
    assert_eq!(report.target, configured);
    assert_eq!(recorded_root(&install.db).await.unwrap(), Some(configured));
    assert!(!other.exists());
}
