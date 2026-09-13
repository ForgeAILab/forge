use crate::{
    external_api::{
        gitea::GiteaClient, github::GitHubClient, resolve_token, IssueFetcher, SyncFilter,
    },
    Result, ServiceError, TaskService,
};
use db::{
    new_uuid_v4, now_rfc3339, AssigneeKind, CreateProjectIntegration, CreateTaskExternalLink,
    CreateTaskRoleAssignment, ExternalLinkRepo, IntegrationPlatform, IntegrationRepo,
    ProjectIntegration, ProjectRepo, SqliteDb, TaskRepo, TaskRoleAssignmentRepo,
    UpdateProjectIntegration,
};
use events::EventBus;
use std::{str::FromStr, sync::Arc};

pub struct IntegrationService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_service: Arc<TaskService>,
}

pub struct SyncResult {
    pub imported: u32,
    pub skipped: u32,
    pub errors: u32,
}

impl IntegrationService {
    pub fn new(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        task_service: Arc<TaskService>,
    ) -> Self {
        Self {
            db,
            event_bus,
            task_service,
        }
    }

    pub async fn create_integration(
        &self,
        input: CreateProjectIntegration,
    ) -> Result<ProjectIntegration> {
        self.validate_create(&input).await?;
        Ok(IntegrationRepo::create_integration(&*self.db, input).await?)
    }

    pub async fn get_by_id(&self, id: &str) -> Result<Option<ProjectIntegration>> {
        validate_required("id", id)?;
        Ok(IntegrationRepo::get_by_id(&*self.db, id).await?)
    }

    pub async fn get_by_project_id(&self, project_id: &str) -> Result<Option<ProjectIntegration>> {
        validate_required("project_id", project_id)?;
        Ok(IntegrationRepo::get_by_project_id(&*self.db, project_id).await?)
    }

    pub async fn update_integration(
        &self,
        input: UpdateProjectIntegration,
    ) -> Result<ProjectIntegration> {
        self.validate_update(&input).await?;
        Ok(IntegrationRepo::update_integration(&*self.db, input).await?)
    }

    pub async fn delete_integration(&self, id: &str) -> Result<()> {
        validate_required("id", id)?;
        Ok(IntegrationRepo::delete_integration(&*self.db, id).await?)
    }

    pub async fn sync_integration(&self, integration: &ProjectIntegration) -> Result<SyncResult> {
        let _ = &self.event_bus;
        let token = resolve_token(&integration.token_secret_ref)
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let sync_filter = parse_sync_filter(&integration.sync_filter);
        let fetcher = issue_fetcher(integration);
        let issues = fetcher
            .fetch_issues(
                &integration.owner,
                &integration.repo,
                &token,
                integration.last_polled_at.as_deref(),
                &sync_filter,
            )
            .await
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;

        let mut imported = 0;
        let mut skipped = 0;
        for issue in issues {
            if integration.platform == IntegrationPlatform::Github
                && issue.html_url.contains("/pull/")
            {
                skipped += 1;
                continue;
            }

            let platform = integration.platform.to_string();
            let global_id = compute_global_id(
                &platform,
                &integration.base_url,
                &integration.owner,
                &integration.repo,
                issue.number,
            );
            if ExternalLinkRepo::get_by_global_id(&*self.db, &global_id)
                .await?
                .is_some()
            {
                skipped += 1;
                continue;
            }

            let task = self
                .task_service
                .create_task(
                    integration.project_id.clone(),
                    issue.title.clone(),
                    issue.body.clone(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await?;

            let now = now_rfc3339();
            ExternalLinkRepo::create_link(
                &*self.db,
                CreateTaskExternalLink {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    integration_id: integration.id.clone(),
                    platform,
                    remote_owner: integration.owner.clone(),
                    remote_repo: integration.repo.clone(),
                    remote_issue_number: issue.number,
                    remote_url: compute_remote_url(
                        &integration.platform,
                        &integration.base_url,
                        &integration.owner,
                        &integration.repo,
                        issue.number,
                    ),
                    global_id,
                    synced_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await?;

            assign_default_coder(&self.db, &task.id, integration).await?;
            imported += 1;
        }

        Ok(SyncResult {
            imported,
            skipped,
            errors: 0,
        })
    }

    async fn validate_create(&self, input: &CreateProjectIntegration) -> Result<()> {
        validate_required("id", &input.id)?;
        validate_required("project_id", &input.project_id)?;
        validate_required("base_url", &input.base_url)?;
        validate_required("owner", &input.owner)?;
        validate_required("repo", &input.repo)?;
        validate_required("token_secret_ref", &input.token_secret_ref)?;
        validate_poll_interval(input.poll_interval_secs)?;
        validate_sync_filter(&input.sync_filter)?;
        validate_assignee(
            input.default_assignee_type.as_deref(),
            input.default_assignee_id.as_deref(),
        )?;
        ProjectRepo::get_by_id(&*self.db, &input.project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", input.project_id.clone()))?;
        Ok(())
    }

    async fn validate_update(&self, input: &UpdateProjectIntegration) -> Result<()> {
        validate_required("id", &input.id)?;
        let existing = IntegrationRepo::get_by_id(&*self.db, &input.id)
            .await?
            .ok_or_else(|| ServiceError::not_found("integration", input.id.clone()))?;
        if let Some(project_id) = &input.project_id {
            validate_required("project_id", project_id)?;
            ProjectRepo::get_by_id(&*self.db, project_id)
                .await?
                .ok_or_else(|| ServiceError::not_found("project", project_id.clone()))?;
        }
        if let Some(base_url) = &input.base_url {
            validate_required("base_url", base_url)?;
        }
        if let Some(owner) = &input.owner {
            validate_required("owner", owner)?;
        }
        if let Some(repo) = &input.repo {
            validate_required("repo", repo)?;
        }
        if let Some(token_secret_ref) = &input.token_secret_ref {
            validate_required("token_secret_ref", token_secret_ref)?;
        }
        if let Some(poll_interval_secs) = input.poll_interval_secs {
            validate_poll_interval(poll_interval_secs)?;
        }
        if let Some(sync_filter) = &input.sync_filter {
            validate_sync_filter(sync_filter)?;
        }
        let assignee_type = match &input.default_assignee_type {
            Some(value) => value.as_deref(),
            None => existing.default_assignee_type.as_deref(),
        };
        let assignee_id = match &input.default_assignee_id {
            Some(value) => value.as_deref(),
            None => existing.default_assignee_id.as_deref(),
        };
        validate_assignee(assignee_type, assignee_id)?;
        Ok(())
    }
}

fn issue_fetcher(integration: &ProjectIntegration) -> Box<dyn IssueFetcher> {
    match integration.platform {
        IntegrationPlatform::Github => Box::new(GitHubClient),
        IntegrationPlatform::Gitea => Box::new(GiteaClient {
            base_url: integration.base_url.clone(),
        }),
    }
}

fn parse_sync_filter(sync_filter: &str) -> SyncFilter {
    if sync_filter.trim().is_empty() {
        return SyncFilter::default();
    }
    serde_json::from_str(sync_filter).unwrap_or_default()
}

fn validate_required(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{field} is required"
        )));
    }
    Ok(())
}

fn validate_poll_interval(poll_interval_secs: i64) -> Result<()> {
    if poll_interval_secs <= 0 {
        return Err(ServiceError::invalid_operation(
            "poll_interval_secs must be greater than 0",
        ));
    }
    Ok(())
}

fn validate_sync_filter(sync_filter: &str) -> Result<()> {
    if sync_filter.trim().is_empty() {
        return Ok(());
    }
    serde_json::from_str::<serde_json::Value>(sync_filter)
        .map(|_| ())
        .map_err(|error| ServiceError::invalid_operation(format!("invalid sync_filter: {error}")))
}

fn validate_assignee(assignee_type: Option<&str>, assignee_id: Option<&str>) -> Result<()> {
    match (assignee_type, assignee_id) {
        (Some(kind), Some(id)) => {
            validate_required("default_assignee_type", kind)?;
            validate_required("default_assignee_id", id)?;
            kind.parse::<AssigneeKind>()
                .map_err(ServiceError::invalid_operation)?;
            Ok(())
        }
        (None, None) => Ok(()),
        _ => Err(ServiceError::invalid_operation(
            "default_assignee_type and default_assignee_id must be provided together",
        )),
    }
}

async fn assign_default_coder(
    db: &SqliteDb,
    task_id: &str,
    integration: &ProjectIntegration,
) -> Result<()> {
    let (Some(assignee_type), Some(assignee_id)) = (
        integration.default_assignee_type.as_deref(),
        integration.default_assignee_id.as_deref(),
    ) else {
        return Ok(());
    };
    let assignee_type =
        AssigneeKind::from_str(assignee_type).map_err(ServiceError::invalid_operation)?;
    let now = now_rfc3339();
    let task = TaskRepo::get_by_id(db, task_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", task_id.to_owned()))?;
    let previous = TaskRoleAssignmentRepo::get_by_task_and_role(db, task_id, "coder").await?;
    let input = CreateTaskRoleAssignment {
        id: new_uuid_v4(),
        task_id: task_id.to_owned(),
        role_name: "coder".to_owned(),
        assignee_type: Some(assignee_type),
        assignee_id: Some(assignee_id.to_owned()),
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    if previous.as_ref().is_some_and(|previous| {
        previous.assignee_type == input.assignee_type && previous.assignee_id == input.assignee_id
    }) {
        let previous = previous.as_ref().expect("same-assignment branch has a row");
        TaskRoleAssignmentRepo::assign_if_unchanged(db, input, Some(previous)).await?;
    } else {
        TaskRoleAssignmentRepo::assign_and_clear_review_authority(
            db,
            input,
            previous.as_ref(),
            task.version,
            &now,
        )
        .await?;
    }
    crate::wake_task_dispatch(db, task_id, "integration default coder assignment changed").await?;
    Ok(())
}

fn compute_global_id(
    platform: &str,
    base_url: &str,
    owner: &str,
    repo: &str,
    number: i64,
) -> String {
    let host = url::Url::parse(base_url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_default();
    let is_well_known = matches!(host.as_str(), "api.github.com" | "github.com");
    if is_well_known {
        format!("{platform}:{owner}/{repo}#{number}")
    } else {
        format!("{platform}:{host}:{owner}/{repo}#{number}")
    }
}

fn compute_remote_url(
    platform: &IntegrationPlatform,
    base_url: &str,
    owner: &str,
    repo: &str,
    number: i64,
) -> String {
    match platform {
        IntegrationPlatform::Github => {
            format!("https://github.com/{owner}/{repo}/issues/{number}")
        }
        IntegrationPlatform::Gitea => {
            let base = base_url.trim_end_matches('/');
            format!("{base}/{owner}/{repo}/issues/{number}")
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn compute_global_id_omits_well_known_github_host() {
        let global_id =
            super::compute_global_id("github", "https://api.github.com", "owner", "repo", 7);

        assert_eq!(global_id, "github:owner/repo#7");
    }

    #[test]
    fn compute_global_id_includes_self_hosted_gitea_host() {
        let global_id =
            super::compute_global_id("gitea", "https://gitea.example.com", "owner", "repo", 42);

        assert_eq!(global_id, "gitea:gitea.example.com:owner/repo#42");
    }

    #[test]
    fn compute_global_id_distinguishes_different_hosts() {
        let first = super::compute_global_id("gitea", "https://gitea.a.com", "owner", "repo", 1);
        let second = super::compute_global_id("gitea", "https://gitea.b.com", "owner", "repo", 1);

        assert_ne!(first, second);
    }
}
