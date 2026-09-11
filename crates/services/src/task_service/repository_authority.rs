use super::*;

/// Canonical pre-execution repository authority for a Task.
///
/// Tasks belong to Projects and never select repositories themselves. The
/// Project's current primary Repo is resolved at the admission boundary; a
/// Workspace and WorkspaceLease then pin that Repo for the execution attempt.
#[derive(Debug, Clone)]
pub(crate) struct TaskRepositoryAuthority {
    pub project: db::Project,
    pub repo: db::Repo,
}

/// Transaction-scoped subset used while a claim is establishing its running
/// execution and WorkspaceLease atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TransactionalTaskRepositoryAuthority {
    pub project_id: String,
    pub repo_id: String,
    pub default_branch: String,
    pub task_version: i64,
}

pub(crate) async fn resolve_task_repository_authority(
    db: &SqliteDb,
    task: &Task,
) -> Result<TaskRepositoryAuthority> {
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("project", task.project_id.clone()))?;
    let repo_id =
        project
            .primary_repo_id
            .as_deref()
            .ok_or_else(|| ServiceError::MissingPrimaryRepo {
                project_id: project.id.clone(),
            })?;
    let repo = RepoRepo::get_by_id(db, repo_id).await?.ok_or_else(|| {
        ServiceError::PrimaryRepoNotFound {
            project_id: project.id.clone(),
            repo_id: repo_id.to_owned(),
        }
    })?;
    if repo.project_id != project.id {
        return Err(ServiceError::RepoMismatch {
            project_id: project.id,
        });
    }
    Ok(TaskRepositoryAuthority { project, repo })
}

impl TaskService {
    pub(crate) async fn resolve_task_repository(
        &self,
        task: &Task,
    ) -> Result<TaskRepositoryAuthority> {
        resolve_task_repository_authority(&self.db, task).await
    }

    pub(super) async fn resolve_task_repository_in_tx(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        task: &Task,
    ) -> Result<TransactionalTaskRepositoryAuthority> {
        let row = sqlx::query(
            "SELECT t.project_id, t.version AS task_version,
                    p.primary_repo_id,
                    r.id AS resolved_repo_id,
                    r.project_id AS repo_project_id,
                    r.default_branch
             FROM task t
             JOIN project p ON p.id = t.project_id
             LEFT JOIN repo r ON r.id = p.primary_repo_id
             WHERE t.id = ? AND t.deleted_at IS NULL",
        )
        .bind(&task.id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;

        let project_id: String = row.get("project_id");
        let task_version: i64 = row.get("task_version");
        if project_id != task.project_id {
            return Err(ServiceError::RepoMismatch {
                project_id: task.project_id.clone(),
            });
        }
        if task_version != task.version {
            return Err(ServiceError::Db(DbError::VersionConflict));
        }

        let primary_repo_id = row
            .get::<Option<String>, _>("primary_repo_id")
            .ok_or_else(|| ServiceError::MissingPrimaryRepo {
                project_id: project_id.clone(),
            })?;
        let resolved_repo_id = row
            .get::<Option<String>, _>("resolved_repo_id")
            .ok_or_else(|| ServiceError::PrimaryRepoNotFound {
                project_id: project_id.clone(),
                repo_id: primary_repo_id.clone(),
            })?;
        let repo_project_id: String = row.get("repo_project_id");
        if resolved_repo_id != primary_repo_id || repo_project_id != project_id {
            return Err(ServiceError::RepoMismatch { project_id });
        }

        Ok(TransactionalTaskRepositoryAuthority {
            project_id,
            repo_id: resolved_repo_id,
            default_branch: row.get("default_branch"),
            task_version,
        })
    }
}
