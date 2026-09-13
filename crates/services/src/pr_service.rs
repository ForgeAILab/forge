use crate::{DomainEventService, Result, ServiceError};
use async_trait::async_trait;
use db::{
    new_uuid_v4, now_rfc3339, CreatePrMetadata, PrMetadata, PrMetadataRepo, PrProviderConfig,
    PrProviderConfigRepo, Repo, RepoRepo, SqliteDb, Task, TaskMetadata, TaskMetadataMutation,
    TaskRepo, UpdatePrMetadata, UpdateTaskStatus, WorkspaceRepo,
};
use events::EventBus;
use serde_json::json;
use sqlx::Row;
use std::{sync::Arc, time::Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrCreateRequest {
    pub repo_remote_url: String,
    pub source_branch: String,
    pub target_branch: String,
    pub title: String,
    pub body: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrUpdateRequest {
    pub provider_pr_id: String,
    pub source_branch: String,
    pub target_branch: String,
    pub title: String,
    pub body: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrRecord {
    pub provider_pr_id: String,
    pub pr_url: Option<String>,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemotePrStatus {
    Open,
    Merged,
    Closed,
}

#[async_trait]
pub trait PrProvider: Send + Sync {
    async fn create_pr(&self, request: PrCreateRequest) -> Result<PrRecord>;
    async fn update_pr(&self, request: PrUpdateRequest) -> Result<PrRecord>;
    async fn get_pr_status(&self, metadata: &PrMetadata) -> Result<RemotePrStatus>;
    async fn close_pr(&self, metadata: &PrMetadata) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct GitHubPrProvider {
    config: PrProviderConfig,
    token: String,
}

impl GitHubPrProvider {
    pub fn new(config: PrProviderConfig, token: String) -> Self {
        Self { config, token }
    }
}

#[async_trait]
impl PrProvider for GitHubPrProvider {
    async fn create_pr(&self, request: PrCreateRequest) -> Result<PrRecord> {
        tracing::info!(
            repo = %request.repo_remote_url,
            source_branch = %request.source_branch,
            target_branch = %request.target_branch,
            provider = %self.config.provider_type,
            "placeholder GitHub PR create"
        );
        let _ = self.token.len();
        Ok(PrRecord {
            provider_pr_id: format!("placeholder-{}", request.source_branch),
            pr_url: Some(format!(
                "{}/pull/{}",
                self.config
                    .base_url
                    .as_deref()
                    .unwrap_or("https://github.com/forge-placeholder"),
                request.source_branch
            )),
            state: "open".to_owned(),
        })
    }

    async fn update_pr(&self, request: PrUpdateRequest) -> Result<PrRecord> {
        tracing::info!(
            provider_pr_id = %request.provider_pr_id,
            source_branch = %request.source_branch,
            target_branch = %request.target_branch,
            provider = %self.config.provider_type,
            "placeholder GitHub PR update"
        );
        let _ = self.token.len();
        Ok(PrRecord {
            provider_pr_id: request.provider_pr_id,
            pr_url: Some(format!(
                "{}/pull/{}",
                self.config
                    .base_url
                    .as_deref()
                    .unwrap_or("https://github.com/forge-placeholder"),
                request.source_branch
            )),
            state: "open".to_owned(),
        })
    }

    async fn get_pr_status(&self, metadata: &PrMetadata) -> Result<RemotePrStatus> {
        tracing::info!(
            task_id = %metadata.task_id,
            provider_pr_id = ?metadata.provider_pr_id,
            provider = %self.config.provider_type,
            "placeholder GitHub PR status read"
        );
        let _ = self.token.len();
        Ok(match metadata.pr_state.as_str() {
            "merged" => RemotePrStatus::Merged,
            "closed" => RemotePrStatus::Closed,
            _ => RemotePrStatus::Open,
        })
    }

    async fn close_pr(&self, metadata: &PrMetadata) -> Result<()> {
        tracing::info!(
            task_id = %metadata.task_id,
            provider_pr_id = ?metadata.provider_pr_id,
            provider = %self.config.provider_type,
            "placeholder GitHub PR close"
        );
        let _ = self.token.len();
        Ok(())
    }
}

pub struct PublishedPr {
    pub metadata: PrMetadata,
    pub pr_url: Option<String>,
}

#[derive(Clone)]
pub struct PrService {
    db: Arc<SqliteDb>,
}

impl PrService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    pub async fn publish_pr(
        &self,
        task: &Task,
        repo: &Repo,
        source_branch: &str,
        target_branch: &str,
    ) -> Result<PublishedPr> {
        let config = PrProviderConfigRepo::get_by_repo_id(&*self.db, &repo.id)
            .await?
            .ok_or_else(|| ServiceError::PrProviderMissing {
                repo_id: repo.id.clone(),
            })?;
        let provider = self.provider_for(&config)?;
        let existing = PrMetadataRepo::get_by_task_id(&*self.db, &task.id).await?;
        let body = Some(format!("Forge task: {}", task.id));
        let record = if let Some(metadata) = existing.as_ref() {
            if let Some(provider_pr_id) = metadata.provider_pr_id.clone() {
                provider
                    .update_pr(PrUpdateRequest {
                        provider_pr_id,
                        source_branch: source_branch.to_owned(),
                        target_branch: target_branch.to_owned(),
                        title: task.title.clone(),
                        body: body.clone(),
                    })
                    .await?
            } else {
                provider
                    .create_pr(PrCreateRequest {
                        repo_remote_url: repo.remote_url.clone(),
                        source_branch: source_branch.to_owned(),
                        target_branch: target_branch.to_owned(),
                        title: task.title.clone(),
                        body: body.clone(),
                    })
                    .await?
            }
        } else {
            provider
                .create_pr(PrCreateRequest {
                    repo_remote_url: repo.remote_url.clone(),
                    source_branch: source_branch.to_owned(),
                    target_branch: target_branch.to_owned(),
                    title: task.title.clone(),
                    body: body.clone(),
                })
                .await?
        };

        let now = now_rfc3339();
        let metadata = if let Some(existing) = existing {
            PrMetadataRepo::update(
                &*self.db,
                UpdatePrMetadata {
                    id: existing.id,
                    provider_type: Some(config.provider_type.clone()),
                    provider_pr_id: Some(Some(record.provider_pr_id)),
                    pr_url: Some(record.pr_url.clone()),
                    source_branch: Some(source_branch.to_owned()),
                    target_branch: Some(target_branch.to_owned()),
                    pr_state: Some(record.state),
                    merge_status: Some("pending".to_owned()),
                    last_synced_at: Some(Some(now.clone())),
                    updated_at: now.clone(),
                },
            )
            .await?
        } else {
            PrMetadataRepo::create(
                &*self.db,
                CreatePrMetadata {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    provider_type: config.provider_type.clone(),
                    provider_pr_id: Some(record.provider_pr_id),
                    pr_url: record.pr_url.clone(),
                    source_branch: source_branch.to_owned(),
                    target_branch: target_branch.to_owned(),
                    pr_state: record.state,
                    merge_status: "pending".to_owned(),
                    last_synced_at: Some(now.clone()),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await?
        };
        set_task_awaiting_human(&self.db, task, true).await?;
        Ok(PublishedPr {
            pr_url: metadata.pr_url.clone(),
            metadata,
        })
    }

    fn provider_for(&self, config: &PrProviderConfig) -> Result<Box<dyn PrProvider>> {
        let token =
            resolve_token_secret(config)?.ok_or_else(|| ServiceError::PrProviderTokenMissing {
                repo_id: config.repo_id.clone(),
            })?;
        match config.provider_type.as_str() {
            "github" => Ok(Box::new(GitHubPrProvider::new(config.clone(), token))),
            provider_type => Err(ServiceError::invalid_operation(format!(
                "unsupported PR provider type: {provider_type}"
            ))),
        }
    }
}

pub struct PrReconciler {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    interval: Duration,
}

impl PrReconciler {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>, interval: Option<Duration>) -> Self {
        Self {
            db,
            event_bus,
            interval: interval.unwrap_or(Duration::from_secs(60)),
        }
    }

    pub fn run(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.interval);
            loop {
                ticker.tick().await;
                if let Err(error) = self.reconcile_once().await {
                    tracing::warn!(%error, "PR reconciliation pass failed");
                }
            }
        })
    }

    pub async fn reconcile_once(&self) -> Result<()> {
        for metadata in pending_pr_metadata(&self.db).await? {
            if let Err(error) = self.reconcile_metadata(metadata).await {
                tracing::warn!(%error, "PR metadata reconciliation failed");
            }
        }
        Ok(())
    }

    async fn reconcile_metadata(&self, metadata: PrMetadata) -> Result<()> {
        let task = TaskRepo::get_by_id(&*self.db, &metadata.task_id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", metadata.task_id.clone()))?;
        let execution = crate::task_service::latest_executor_execution_for_task(&self.db, &task)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "task {} has no executor execution for PR reconciliation",
                    task.id
                ))
            })?;
        let workspace_id = execution.workspace_id.as_deref().ok_or_else(|| {
            ServiceError::invalid_operation(format!(
                "executor execution {} is missing workspace_id",
                execution.id
            ))
        })?;
        let workspace = WorkspaceRepo::get_by_id(&*self.db, workspace_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("workspace", workspace_id.to_owned()))?;
        let repo = RepoRepo::get_by_id(&*self.db, &workspace.repo_id)
            .await?
            .filter(|repo| repo.project_id == task.project_id)
            .ok_or_else(|| ServiceError::not_found("repo", workspace.repo_id.clone()))?;
        let config = PrProviderConfigRepo::get_by_repo_id(&*self.db, &repo.id)
            .await?
            .ok_or_else(|| ServiceError::PrProviderMissing {
                repo_id: repo.id.clone(),
            })?;
        let provider = PrService::new(Arc::clone(&self.db)).provider_for(&config)?;
        let now = now_rfc3339();
        match provider.get_pr_status(&metadata).await? {
            RemotePrStatus::Open => {
                PrMetadataRepo::update(
                    &*self.db,
                    UpdatePrMetadata {
                        id: metadata.id,
                        provider_type: None,
                        provider_pr_id: None,
                        pr_url: None,
                        source_branch: None,
                        target_branch: None,
                        pr_state: Some("open".to_owned()),
                        merge_status: Some("pending".to_owned()),
                        last_synced_at: Some(Some(now)),
                        updated_at: now_rfc3339(),
                    },
                )
                .await?;
            }
            RemotePrStatus::Merged => {
                PrMetadataRepo::update(
                    &*self.db,
                    UpdatePrMetadata {
                        id: metadata.id,
                        provider_type: None,
                        provider_pr_id: None,
                        pr_url: None,
                        source_branch: None,
                        target_branch: None,
                        pr_state: Some("merged".to_owned()),
                        merge_status: Some("merged".to_owned()),
                        last_synced_at: Some(Some(now.clone())),
                        updated_at: now.clone(),
                    },
                )
                .await?;
                set_task_awaiting_human(&self.db, &task, false).await?;
                let updated = TaskRepo::update_status(
                    &*self.db,
                    UpdateTaskStatus {
                        id: task.id,
                        expected_version: task.version,
                        status: "done".to_owned(),
                        assignee_id: Some(None),
                        error_annotation: Some(None),
                        blocked_json: Some(None),
                        failed_json: Some(None),
                        updated_at: now,
                    },
                )
                .await?;
                self.publish_task_status_event(&updated).await;
            }
            RemotePrStatus::Closed => {
                PrMetadataRepo::update(
                    &*self.db,
                    UpdatePrMetadata {
                        id: metadata.id,
                        provider_type: None,
                        provider_pr_id: None,
                        pr_url: None,
                        source_branch: None,
                        target_branch: None,
                        pr_state: Some("closed".to_owned()),
                        merge_status: Some("closed_without_merge".to_owned()),
                        last_synced_at: Some(Some(now.clone())),
                        updated_at: now.clone(),
                    },
                )
                .await?;
                set_task_awaiting_human(&self.db, &task, false).await?;
                let blocked = json!({
                    "kind": api_types::FailureKind::PrClosedWithoutMerge,
                    "reason": "pull request was closed without merge",
                    "provider_pr_id": metadata.provider_pr_id,
                    "pr_url": metadata.pr_url,
                    "recovery_actions": ["return_to_implementation", "retry_pr_publication", "cancel_task"],
                    "blocked_at": now,
                });
                let updated = TaskRepo::update_status(
                    &*self.db,
                    UpdateTaskStatus {
                        id: task.id,
                        expected_version: task.version,
                        status: "blocked".to_owned(),
                        assignee_id: Some(None),
                        error_annotation: None,
                        blocked_json: Some(Some(blocked.to_string())),
                        failed_json: Some(None),
                        updated_at: now_rfc3339(),
                    },
                )
                .await?;
                self.publish_task_status_event(&updated).await;
            }
        }
        Ok(())
    }

    async fn publish_task_status_event(&self, task: &Task) {
        let service = DomainEventService::new(Arc::clone(&self.db), Arc::clone(&self.event_bus));
        let dedupe_key = format!("task-status-update:{}:{}", task.id, task.version);
        if let Err(error) = service.publish_by_dedupe(&dedupe_key).await {
            tracing::warn!(task_id = %task.id, %error, "failed to mirror PR task status domain event");
        }
    }
}

async fn pending_pr_metadata(db: &SqliteDb) -> Result<Vec<PrMetadata>> {
    let rows = sqlx::query("SELECT task_id FROM pr_metadata WHERE merge_status = 'pending'")
        .fetch_all(db.pool())
        .await?;
    let mut metadata = Vec::with_capacity(rows.len());
    for row in rows {
        let task_id: String = row.try_get("task_id")?;
        if let Some(item) = PrMetadataRepo::get_by_task_id(db, &task_id).await? {
            metadata.push(item);
        }
    }
    Ok(metadata)
}

fn resolve_token_secret(config: &PrProviderConfig) -> Result<Option<String>> {
    let Some(secret_ref) = config
        .token_secret_ref
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    Ok(std::env::var(secret_ref).ok())
}

async fn set_task_awaiting_human(db: &SqliteDb, task: &Task, awaiting_human: bool) -> Result<()> {
    if awaiting_human {
        return TaskRepo::mutate_metadata(
            db,
            &task.id,
            Some(task.version),
            vec![
                TaskMetadataMutation::Set {
                    key: "awaiting_human".to_owned(),
                    value: json!(true),
                },
                TaskMetadataMutation::Set {
                    key: "awaiting_human_reason".to_owned(),
                    value: json!("pull_request_merge"),
                },
                TaskMetadataMutation::Set {
                    key: "awaiting_human_marker_id".to_owned(),
                    value: json!(new_uuid_v4()),
                },
            ],
            &now_rfc3339(),
        )
        .await
        .map(|_| ())
        .map_err(|error| match error {
            db::DbError::Check(reason) => ServiceError::invalid_operation(reason),
            error => error.into(),
        });
    }

    let metadata = TaskMetadata::parse(task.metadata_json.as_deref()).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid task metadata for {}: {error}", task.id))
    })?;
    let Some(expected) = metadata.extra.get("awaiting_human_marker_id").cloned() else {
        // Legacy markers have no identity that can distinguish this clear
        // from a newer pull-request marker with the same reason.
        return Ok(());
    };
    TaskRepo::mutate_metadata(
        db,
        &task.id,
        None,
        vec![TaskMetadataMutation::CompareAndMutate {
            key: "awaiting_human_marker_id".to_owned(),
            expected,
            mutations: vec![
                TaskMetadataMutation::Remove {
                    key: "awaiting_human".to_owned(),
                },
                TaskMetadataMutation::Remove {
                    key: "awaiting_human_reason".to_owned(),
                },
                TaskMetadataMutation::Remove {
                    key: "awaiting_human_marker_id".to_owned(),
                },
            ],
        }],
        &now_rfc3339(),
    )
    .await
    .map(|_| ())
    .map_err(|error| match error {
        db::DbError::Check(reason) => ServiceError::invalid_operation(reason),
        error => error.into(),
    })?;
    Ok(())
}
