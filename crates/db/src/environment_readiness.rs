//! Internal per-Project, per-workspace-owner environment facts.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{DbError, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "owner_kind", rename_all = "snake_case")]
pub enum EnvironmentMachine {
    Server,
    Daemon {
        daemon_id: String,
        runtime_id: String,
    },
}

impl EnvironmentMachine {
    pub fn from_location(location: &crate::RepoLocation) -> Self {
        if location.owner_kind == crate::RepoLocationOwnerKind::Server {
            Self::Server
        } else {
            Self::Daemon {
                daemon_id: location.daemon_id.clone().unwrap_or_default(),
                runtime_id: location.runtime_id.clone().unwrap_or_default(),
            }
        }
    }

    pub fn from_placement(placement: &crate::WorkspacePlacement) -> Self {
        if placement.owner_kind == crate::PlacementOwnerKind::Server {
            Self::Server
        } else {
            Self::Daemon {
                daemon_id: placement.daemon_id.clone().unwrap_or_default(),
                runtime_id: placement.runtime_id.clone().unwrap_or_default(),
            }
        }
    }

    pub fn columns(&self) -> (&str, &str, &str) {
        match self {
            Self::Server => ("server", "", ""),
            Self::Daemon {
                daemon_id,
                runtime_id,
            } => ("daemon", daemon_id, runtime_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentReadinessStatus {
    Ready,
    NotReady,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessCheckFailure {
    pub name: String,
    pub output_tail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessCheckResult {
    pub name: String,
    pub passed: bool,
    pub exit_code: Option<i32>,
    pub output_tail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMachineReadiness {
    pub project_id: String,
    pub machine: EnvironmentMachine,
    pub status: EnvironmentReadinessStatus,
    pub checks_digest: String,
    pub failing_checks: Vec<ReadinessCheckFailure>,
    pub check_results: Vec<ReadinessCheckResult>,
    pub output_tail: String,
    pub scope_covered: String,
    pub role: Option<String>,
    pub workspace_id: Option<String>,
    pub checked_at: Option<String>,
    pub next_check_at: Option<String>,
    pub version: i64,
}

pub fn environment_checks_digest(environment: &api_types::ProjectEnvironment) -> String {
    let bytes = serde_json::to_vec(&(&environment.env, &environment.checks))
        .expect("environment contains only serializable values");
    hex::encode(Sha256::digest(bytes))
}

pub(crate) fn settings_environment(settings: &str) -> Result<api_types::ProjectEnvironment> {
    serde_json::from_str::<api_types::ProjectSettings>(settings)
        .map(|settings| settings.environment)
        .map_err(|error| DbError::Check(format!("invalid Project environment: {error}")))
}

#[async_trait]
pub trait ProjectMachineReadinessRepo: Send + Sync {
    async fn due_readiness(&self, now: &str) -> Result<Vec<ProjectMachineReadiness>>;
    async fn reschedule_readiness(
        &self,
        row: &ProjectMachineReadiness,
        next_check_at: &str,
    ) -> Result<bool>;

    async fn list_readiness_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        project_id: &str,
    ) -> Result<Vec<ProjectMachineReadiness>>;
    async fn get_readiness(
        &self,
        project_id: &str,
        machine: &EnvironmentMachine,
    ) -> Result<Option<ProjectMachineReadiness>>;
    async fn list_readiness(&self, project_id: &str) -> Result<Vec<ProjectMachineReadiness>>;
    /// Insert with None, or replace the exact observed version. The Project's
    /// current digest is checked in the same transaction as the result write.
    async fn put_readiness(
        &self,
        row: ProjectMachineReadiness,
        expected_version: Option<i64>,
    ) -> Result<ProjectMachineReadiness>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProjectRepo, SqliteDb};
    async fn fixture() -> (SqliteDb, String, api_types::ProjectEnvironment) {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool);
        let id = crate::new_uuid_v4();
        let now = crate::now_rfc3339();
        let settings = r#"{"environment":{"env":{"TOOL":"rust"},"checks":[{"name":"cargo","command":"cargo --version"}]}}"#;
        ProjectRepo::create(
            &db,
            crate::CreateProject {
                id: id.clone(),
                name: "Readiness".into(),
                settings: settings.into(),
                workflow_definition: "{}".into(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        (db, id, settings_environment(settings).unwrap())
    }
    fn row(id: &str, environment: &api_types::ProjectEnvironment) -> ProjectMachineReadiness {
        ProjectMachineReadiness {
            project_id: id.into(),
            machine: EnvironmentMachine::Server,
            status: EnvironmentReadinessStatus::Ready,
            checks_digest: environment_checks_digest(environment),
            failing_checks: vec![],
            check_results: vec![],
            output_tail: String::new(),
            scope_covered: "full".into(),
            role: None,
            workspace_id: None,
            checked_at: None,
            next_check_at: None,
            version: 1,
        }
    }

    #[tokio::test]
    async fn scope_upgrade_recomputes_digest_without_losing_failure_or_due_time() {
        let (db, id, environment) = fixture().await;
        let mut failure = row(&id, &environment);
        failure.status = EnvironmentReadinessStatus::NotReady;
        failure.failing_checks = vec![ReadinessCheckFailure {
            name: "cargo".into(),
            output_tail: "missing".into(),
        }];
        failure.next_check_at = Some("2000-01-01T00:00:00Z".into());
        db.put_readiness(failure, None).await.unwrap();
        sqlx::raw_sql("DROP TABLE repo_provision_retry; UPDATE project_machine_readiness SET checks_digest = 'before-scope';").execute(db.pool()).await.unwrap();
        sqlx::raw_sql(include_str!(
            "../migrations/V202610021500__repo_provision_retry.sql"
        ))
        .execute(db.pool())
        .await
        .unwrap();
        crate::sqlite::environment_readiness::fill_migrated_digests(db.pool())
            .await
            .unwrap();
        let migrated = db
            .get_readiness(&id, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            migrated.checks_digest,
            environment_checks_digest(&environment)
        );
        assert_eq!(migrated.status, EnvironmentReadinessStatus::NotReady);
        assert_eq!(migrated.failing_checks[0].name, "cargo");
        assert_eq!(
            migrated.next_check_at.as_deref(),
            Some("2000-01-01T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn machine_only_readiness_keeps_provisioning_wait_until_full_checks() {
        let (db, id, environment) = fixture().await;
        let now = crate::now_rfc3339();
        let machine = EnvironmentMachine::Daemon {
            daemon_id: "probe-owner".into(),
            runtime_id: "probe-runtime".into(),
        };
        sqlx::query("INSERT INTO task (id,project_id,title,task_type,status,metadata_json,created_at,updated_at) VALUES ('provision-wait',?,'Task','task','in_progress',?,?,?)")
            .bind(&id).bind(serde_json::json!({"deferred_dispatch":{"kind":"environment_probe_pending"},"environment_wait":{"machine":machine,"kind":"environment_probe_pending"}}).to_string()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let mut ready = row(&id, &environment);
        ready.machine = machine;
        ready.scope_covered = "machine".into();
        ready.status = EnvironmentReadinessStatus::Unknown;
        let unknown = db.put_readiness(ready, None).await.unwrap();
        let mut ready = unknown.clone();
        ready.status = EnvironmentReadinessStatus::Ready;
        let saved = db
            .put_readiness(ready, Some(unknown.version))
            .await
            .unwrap();
        let wait: Option<String> = sqlx::query_scalar("SELECT json_extract(metadata_json,'$.environment_wait.kind') FROM task WHERE id='provision-wait'").fetch_one(db.pool()).await.unwrap();
        assert_eq!(wait.as_deref(), Some("environment_probe_pending"));
        let mut full = saved.clone();
        full.scope_covered = "full".into();
        db.put_readiness(full, Some(saved.version)).await.unwrap();
        let wait: Option<String> = sqlx::query_scalar("SELECT json_extract(metadata_json,'$.environment_wait.kind') FROM task WHERE id='provision-wait'").fetch_one(db.pool()).await.unwrap();
        assert!(wait.is_none());
    }

    #[test]
    fn digest_covers_only_env_and_checks() {
        let mut environment = api_types::ProjectEnvironment::default();
        let original = environment_checks_digest(&environment);
        environment.recheck_interval_seconds = 60;
        assert_eq!(environment_checks_digest(&environment), original);
        environment.env.insert("TOOL".into(), "rust".into());
        assert_ne!(environment_checks_digest(&environment), original);
        let with_env = environment_checks_digest(&environment);
        environment.checks =
            serde_json::from_str(r#"[{"name":"cargo","command":"cargo --version"}]"#).unwrap();
        assert_ne!(environment_checks_digest(&environment), with_env);
    }

    #[tokio::test]
    async fn readiness_versions_fence_newer_results_and_digest_edits() {
        let (db, id, environment) = fixture().await;
        let initial = db
            .put_readiness(row(&id, &environment), None)
            .await
            .unwrap();
        assert!(matches!(
            db.put_readiness(initial.clone(), None).await,
            Err(DbError::VersionConflict)
        ));
        let saved = db
            .put_readiness(initial.clone(), Some(initial.version))
            .await
            .unwrap();
        assert_eq!(saved.version, initial.version + 1);
        assert!(matches!(
            db.put_readiness(initial.clone(), Some(initial.version))
                .await,
            Err(DbError::VersionConflict)
        ));
        let project = ProjectRepo::get_by_id(&db, &id).await.unwrap().unwrap();
        ProjectRepo::update_at_version(
            &db,
            crate::UpdateProject {
                id: id.clone(),
                name: None,
                settings: Some(
                    r#"{"environment":{"checks":[{"name":"node","command":"node --version"}]}}"#
                        .into(),
                ),
                primary_repo_id: None,
                paused_at: None,
                updated_at: crate::now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .unwrap();
        let unknown = db
            .get_readiness(&id, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unknown.status, EnvironmentReadinessStatus::Unknown);
        assert!(unknown.version > saved.version);
        assert!(matches!(
            db.put_readiness(saved.clone(), Some(saved.version)).await,
            Err(DbError::VersionConflict)
        ));
        let project = ProjectRepo::get_by_id(&db, &id).await.unwrap().unwrap();
        ProjectRepo::update_at_version(
            &db,
            crate::UpdateProject {
                id: id.clone(),
                name: None,
                settings: Some("{}".into()),
                primary_repo_id: None,
                paused_at: None,
                updated_at: crate::now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .unwrap();
        assert!(db.list_readiness(&id).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn readiness_migration_carries_pauses_and_preserves_projects_and_placements() {
        let (db, id, environment) = fixture().await;
        let now = crate::now_rfc3339();
        let repo = crate::new_uuid_v4();
        let task = crate::new_uuid_v4();
        let workspace = crate::new_uuid_v4();
        let location = crate::new_uuid_v4();
        let placement = crate::new_uuid_v4();
        sqlx::query("INSERT INTO repo (id, project_id, name, local_path, default_branch, created_at, updated_at) VALUES (?, ?, 'repo', '/checkout', 'main', ?, ?)").bind(&repo).bind(&id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task (id, project_id, title, task_type, status, created_at, updated_at) VALUES (?, ?, 'Task', 'task', 'in_progress', ?, ?)").bind(&task).bind(&id).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        crate::WorkspaceRepo::create(
            &db,
            crate::CreateWorkspace {
                id: workspace.clone(),
                task_id: task.clone(),
                repo_id: repo.clone(),
                worktree_path: "/worktree".into(),
                branch: "task/one".into(),
                status: crate::WorkspaceStatus::Ready,
                before_sha: Some("base".into()),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO repo_location (id, repo_id, owner_kind, path, kind, status, created_at, updated_at) VALUES (?, ?, 'server', '/checkout', 'primary_checkout', 'ready', ?, ?)").bind(&location).bind(&repo).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement (id, workspace_id, task_id, owner_kind, repo_location_id, workspace_handle, state, selected_by, selection_reason, created_at, updated_at) VALUES (?, ?, ?, 'server', ?, '/worktree', 'ready', 'scheduler', '{}', ?, ?)").bind(&placement).bind(&workspace).bind(&task).bind(&location).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        db.put_readiness(row(&id, &environment), None)
            .await
            .unwrap();
        let project = ProjectRepo::get_by_id(&db, &id).await.unwrap().unwrap();
        let ready = db
            .get_readiness(&id, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        assert!(!ProjectRepo::set_environment_pause_if_unchanged(&db,&id,project.version,&now,
            &serde_json::json!({"machine":{"owner_kind":"server"},"checks":["cargo"],"readiness_versions":[{"owner_kind":"server","daemon_id":"","runtime_id":"","version":ready.version-1}]}).to_string()).await.unwrap(),
            "a newer readiness version fences the task-specific pause decision");
        let detail = serde_json::json!({"checks":["cargo"],"output":"missing Rust","role":"coder","workspace_id":workspace,"last_checked_at":now,"next_check_at":"2099-01-01T00:00:00Z"});
        sqlx::query("UPDATE project SET paused_at = ?, system_pause_reason = 'environment_not_ready', environment_pause_json = ? WHERE id = ?").bind(&now).bind(detail.to_string()).bind(&id).execute(db.pool()).await.unwrap();
        let before = ProjectRepo::get_by_id(&db, &id).await.unwrap().unwrap();
        sqlx::query("DROP TABLE project_machine_readiness")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM _migration WHERE version = 202610020600")
            .execute(db.pool())
            .await
            .unwrap();
        crate::run_migrations(db.pool()).await.unwrap();
        let after = ProjectRepo::get_by_id(&db, &id).await.unwrap().unwrap();
        assert_eq!(before, after);
        let migrated = db
            .get_readiness(&id, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(migrated.status, EnvironmentReadinessStatus::NotReady);
        assert_eq!(migrated.failing_checks[0].name, "cargo");
        assert_eq!(migrated.failing_checks[0].output_tail, "missing Rust");
        assert_eq!(
            migrated.next_check_at.as_deref(),
            Some("2099-01-01T00:00:00Z")
        );
        assert_eq!(
            migrated.checks_digest,
            environment_checks_digest(&environment)
        );
        let binding = crate::WorkspacePlacementRepo::get_by_workspace_id(&db, &workspace)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(binding.id, placement);
        assert_eq!(binding.workspace_handle.as_deref(), Some("/worktree"));
        assert_eq!(binding.state, crate::PlacementState::Ready);
        assert!(crate::TaskRepo::get_by_id(&db, &task, false)
            .await
            .unwrap()
            .is_some());

        // Old details name only a workspace: migration must infer its daemon.
        sqlx::query("INSERT INTO daemon (id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('migration-daemon','migration-machine','owner','linux','aarch64','online',?,?)")
            .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO runtime (id,daemon_id,kind,workspace_root,status,created_at,updated_at) VALUES ('migration-runtime','migration-daemon','native','/owner','ready',?,?)")
            .bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE workspace_placement SET owner_kind='daemon',daemon_id='migration-daemon',runtime_id='migration-runtime' WHERE id=?")
            .bind(&placement).execute(db.pool()).await.unwrap();
        let asset_only = crate::new_uuid_v4();
        sqlx::query("INSERT INTO project (id,name,settings,workflow_definition,paused_at,system_pause_reason,environment_pause_json,created_at,updated_at) VALUES (?,'asset-only','{}','{}',?,'environment_not_ready','{\"checks\":[],\"output\":\"asset missing\"}',?,?)")
            .bind(&asset_only).bind(&now).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let malformed = crate::new_uuid_v4();
        sqlx::query("INSERT INTO project (id,name,settings,workflow_definition,paused_at,system_pause_reason,environment_pause_json,created_at,updated_at) VALUES (?,'malformed','{bad','{}',?,'environment_not_ready','{\"checks\":[\"cargo\"]}',?,?)")
            .bind(&malformed).bind(&now).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("DROP TABLE project_machine_readiness")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM _migration WHERE version=202610020600")
            .execute(db.pool())
            .await
            .unwrap();
        crate::run_migrations(db.pool()).await.unwrap();
        let machine = EnvironmentMachine::Daemon {
            daemon_id: "migration-daemon".into(),
            runtime_id: "migration-runtime".into(),
        };
        assert_eq!(
            db.get_readiness(&id, &machine)
                .await
                .unwrap()
                .unwrap()
                .status,
            EnvironmentReadinessStatus::NotReady
        );
        assert!(db
            .get_readiness(&id, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .is_none());
        assert!(db.list_readiness(&asset_only).await.unwrap().is_empty());
        assert!(
            ProjectRepo::get_by_id(&db, &asset_only)
                .await
                .unwrap()
                .unwrap()
                .paused_at
                .is_some(),
            "asset-only legacy pause is preserved without a check cache"
        );
        let invalid = db
            .get_readiness(&malformed, &EnvironmentMachine::Server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(invalid.status, EnvironmentReadinessStatus::Unknown);
        assert_eq!(invalid.checks_digest, "invalid_settings");
        crate::run_migrations(db.pool()).await.unwrap();
        assert_eq!(
            db.get_readiness(&malformed, &EnvironmentMachine::Server)
                .await
                .unwrap()
                .unwrap(),
            invalid,
            "malformed row is handled only once at startup"
        );
        let project = ProjectRepo::get_by_id(&db, &malformed)
            .await
            .unwrap()
            .unwrap();
        ProjectRepo::update_at_version(
            &db,
            crate::UpdateProject {
                id: malformed.clone(),
                name: Some("editable".into()),
                settings: Some("still malformed".into()),
                primary_repo_id: None,
                paused_at: None,
                updated_at: now,
            },
            project.version,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            db.get_readiness(&malformed, &EnvironmentMachine::Server)
                .await
                .unwrap()
                .unwrap()
                .status,
            EnvironmentReadinessStatus::Unknown
        );
    }
    #[tokio::test]
    async fn readiness_wakes_exact_environment_markers_only() {
        let (db, id, environment) = fixture().await;
        let now = crate::now_rfc3339();
        for (task, kind) in [
            ("probe", "environment_probe_pending"),
            ("wait", "environment_not_ready"),
            ("unverified", "environment_unverified"),
            ("lookalike", "environmentXprobe_pending"),
            ("other", "environment_unrelated"),
        ] {
            sqlx::query("INSERT INTO task (id,project_id,title,task_type,status,metadata_json,created_at,updated_at) VALUES (?,?,'Task','task','todo',?, ?,?)")
                .bind(task).bind(&id).bind(serde_json::json!({"deferred_dispatch":{"kind":kind},"environment_wait":{"machine":{"owner_kind":"server"}}}).to_string()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        }
        let machine = EnvironmentMachine::Daemon {
            daemon_id: "owner".into(),
            runtime_id: "native".into(),
        };
        sqlx::query("INSERT INTO task (id,project_id,title,task_type,status,metadata_json,created_at,updated_at) VALUES ('daemon-wait',?,'Task','task','in_progress',?, ?,?)")
            .bind(&id).bind(serde_json::json!({"deferred_dispatch":{"kind":"environment_not_ready"},"environment_wait":{"machine":machine}}).to_string()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        db.put_readiness(row(&id, &environment), None)
            .await
            .unwrap();
        for (task, cleared) in [
            ("probe", true),
            ("wait", true),
            ("unverified", true),
            ("lookalike", false),
            ("other", false),
        ] {
            let marker:Option<String>=sqlx::query_scalar("SELECT json_extract(metadata_json,'$.deferred_dispatch.kind') FROM task WHERE id=?")
                .bind(task).fetch_one(db.pool()).await.unwrap();
            assert_eq!(marker.is_none(), cleared);
        }
        let pending:Option<String>=sqlx::query_scalar("SELECT json_extract(metadata_json,'$.deferred_dispatch.kind') FROM task WHERE id='daemon-wait'").fetch_one(db.pool()).await.unwrap();
        assert_eq!(
            pending.as_deref(),
            Some("environment_not_ready"),
            "server completion cannot clear a daemon wait"
        );
        let mut ready = row(&id, &environment);
        ready.machine = machine;
        db.put_readiness(ready, None).await.unwrap();
        let pending:Option<String>=sqlx::query_scalar("SELECT json_extract(metadata_json,'$.deferred_dispatch.kind') FROM task WHERE id='daemon-wait'").fetch_one(db.pool()).await.unwrap();
        assert!(
            pending.is_none(),
            "canonical daemon identity clears its exact marker"
        );
    }
    #[tokio::test]
    async fn readiness_digest_edit_retires_only_an_obsolete_environment_pause() {
        let (db, id, environment) = fixture().await;
        let machine = EnvironmentMachine::Daemon {
            daemon_id: "daemon".into(),
            runtime_id: "runtime".into(),
        };
        let mut failure = row(&id, &environment);
        failure.machine = machine.clone();
        failure.status = EnvironmentReadinessStatus::NotReady;
        failure.failing_checks = vec![ReadinessCheckFailure {
            name: "cargo".into(),
            output_tail: "missing".into(),
        }];
        db.put_readiness(failure, None).await.unwrap();
        let original = ProjectRepo::get_by_id(&db, &id).await.unwrap().unwrap();
        let now = crate::now_rfc3339();
        assert!(ProjectRepo::set_environment_pause_if_unchanged(&db,&id,original.version,&now,
            &serde_json::json!({"machine":machine,"checks":["cargo"],"role":"coder","output":"missing","paused_at":now,"last_checked_at":now,"next_check_at":"2099-01-01T00:00:00Z"}).to_string()).await.unwrap());
        let paused = ProjectRepo::get_by_id(&db, &id).await.unwrap().unwrap();
        let edited = ProjectRepo::update_at_version(
            &db,
            crate::UpdateProject {
                id: id.clone(),
                name: None,
                settings: Some(
                    r#"{"environment":{"checks":[{"name":"cargo","command":"true"}]}}"#.into(),
                ),
                primary_repo_id: None,
                paused_at: None,
                updated_at: crate::now_rfc3339(),
            },
            paused.version,
            None,
        )
        .await
        .unwrap();
        assert!(
            edited.paused_at.is_none(),
            "unknown daemon facts must reach a new launch without a stale Project veto"
        );
        assert_eq!(
            db.get_readiness(&id, &machine)
                .await
                .unwrap()
                .unwrap()
                .status,
            EnvironmentReadinessStatus::Unknown
        );
        ProjectRepo::set_paused_at(&db, &id, Some(now.clone()))
            .await
            .unwrap();
        let user_paused = ProjectRepo::get_by_id(&db, &id).await.unwrap().unwrap();
        let edited = ProjectRepo::update_at_version(
            &db,
            crate::UpdateProject {
                id: id.clone(),
                name: None,
                settings: Some(
                    r#"{"environment":{"checks":[{"name":"cargo","command":"false"}]}}"#.into(),
                ),
                primary_repo_id: None,
                paused_at: None,
                updated_at: crate::now_rfc3339(),
            },
            user_paused.version,
            None,
        )
        .await
        .unwrap();
        assert_eq!(edited.paused_at, Some(now));
        assert!(edited.system_pause_reason.is_none());
    }
    #[tokio::test]
    async fn readiness_due_scan_isolates_and_reschedules_an_undecodable_row() {
        let (db, id, environment) = fixture().await;
        let mut bad = row(&id, &environment);
        bad.status = EnvironmentReadinessStatus::NotReady;
        bad.next_check_at = Some("2000-01-01T00:00:00Z".into());
        let bad = db.put_readiness(bad, None).await.unwrap();
        let mut good = bad.clone();
        good.machine = EnvironmentMachine::Daemon {
            daemon_id: "healthy".into(),
            runtime_id: "runtime".into(),
        };
        let mut bad_version = good.clone();
        bad_version.machine = EnvironmentMachine::Daemon {
            daemon_id: "bad-version".into(),
            runtime_id: "runtime".into(),
        };
        db.put_readiness(bad_version, None).await.unwrap();
        db.put_readiness(good, None).await.unwrap();
        sqlx::query("UPDATE project_machine_readiness SET version='invalid' WHERE project_id=? AND daemon_id='bad-version'")
            .bind(&id).execute(db.pool()).await.unwrap();
        sqlx::query("UPDATE project_machine_readiness SET failing_checks_json='[{\"name\":null}]' WHERE project_id=? AND owner_kind='server'").bind(&id).execute(db.pool()).await.unwrap();
        let due = db.due_readiness(&crate::now_rfc3339()).await.unwrap();
        assert_eq!(due.len(), 1);
        assert!(matches!(due[0].machine, EnvironmentMachine::Daemon { .. }));
        let (version,next):(i64,String)=sqlx::query_as("SELECT version,next_check_at FROM project_machine_readiness WHERE project_id=? AND owner_kind='server'").bind(&id).fetch_one(db.pool()).await.unwrap();
        let next_bad_version: String = sqlx::query_scalar("SELECT next_check_at FROM project_machine_readiness WHERE project_id=? AND daemon_id='bad-version'")
            .bind(&id).fetch_one(db.pool()).await.unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(&next_bad_version).unwrap() > chrono::Utc::now()
        );
        assert!(version > bad.version);
        assert!(chrono::DateTime::parse_from_rfc3339(&next).unwrap() > chrono::Utc::now());
        assert_eq!(
            db.due_readiness(&crate::now_rfc3339()).await.unwrap().len(),
            1
        );
    }
}
