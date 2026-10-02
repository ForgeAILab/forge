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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMachineReadiness {
    pub project_id: String,
    pub machine: EnvironmentMachine,
    pub status: EnvironmentReadinessStatus,
    pub checks_digest: String,
    pub failing_checks: Vec<ReadinessCheckFailure>,
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
            scope_covered: "full".into(),
            role: None,
            workspace_id: None,
            checked_at: None,
            next_check_at: None,
            version: 1,
        }
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
        assert!(
            !ProjectRepo::set_environment_pause_if_unchanged(
                &db,
                &id,
                project.version,
                &now,
                &serde_json::json!({"machine":{"owner_kind":"server"},"checks":["cargo"]})
                    .to_string()
            )
            .await
            .unwrap(),
            "a readiness result arriving before the pause write prevents a last-resort pause"
        );
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
    }
}
