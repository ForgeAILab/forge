//! Authenticated Project execution-setup mutations.
//!
//! Read responses are deliberately delegated to the canonical setup
//! projection. This service only performs authorization, optimistic
//! concurrency checks, eligibility validation, durable command replay, and
//! the underlying mutation.

use std::sync::Arc;

use api_types::{
    canonical_digest_with_schema, AttachPrimaryRepositoryRequest, ProjectExecutionSetupResponse,
    RetryProvisioningRequest, SelectExecutionPrincipalRequest,
};
use db::{
    now_rfc3339, ApplyProjectExecutionSetupCommand, CommandReceiptRepo, CreateCommandReceipt,
    Project, ProjectExecutionSetupCommandRepo, ProjectProvisioningRepo, ProjectRepo,
    ReconcileProjectProvisioningMetadata, RepoRepo, ScheduleProjectProvisioningRetry, SqliteDb,
};
use serde_json::{json, Value};

use crate::{
    execution_setup::{eligible_project_execution_agents, resolve_project_execution_roles},
    load_project_execution_setup,
    project_provisioning::reconcile_project_setup_metadata,
    Result, ServiceError,
};

const RECEIPT_SCHEMA: &str = "forge.project-execution-setup/v1";
const SELECT_WORKER_OPERATION: &str = "project.execution_setup.select_worker";
const SELECT_REVIEWER_OPERATION: &str = "project.execution_setup.select_independent_reviewer";
const ATTACH_REPOSITORY_OPERATION: &str = "project.execution_setup.attach_repository";
const RETRY_PROVISIONING_OPERATION: &str = "project.execution_setup.retry_provisioning";

struct ApplySetupCommand<'a> {
    actor_user_id: &'a str,
    project_id: &'a str,
    operation: &'a str,
    idempotency_key: &'a str,
    input: &'a Value,
    expected_project_version: Option<i64>,
    settings: Option<String>,
    primary_repo_id: Option<Option<String>>,
    bump_project_version: bool,
    provisioning_retry: Option<ScheduleProjectProvisioningRetry>,
    provisioning_metadata: Option<ReconcileProjectProvisioningMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPrincipalRole {
    Worker,
    IndependentReviewer,
}

impl ExecutionPrincipalRole {
    fn role_name(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::IndependentReviewer => "independent_reviewer",
        }
    }

    fn operation(self) -> &'static str {
        match self {
            Self::Worker => SELECT_WORKER_OPERATION,
            Self::IndependentReviewer => SELECT_REVIEWER_OPERATION,
        }
    }
}

#[derive(Clone)]
pub struct ProjectExecutionSetupService {
    db: Arc<SqliteDb>,
}

impl ProjectExecutionSetupService {
    #[must_use]
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    /// Return the same scope-neutral response used by the canonical GET
    /// projection. Keeping this method on the mutation service makes route
    /// handlers independent of projection internals.
    pub async fn get(&self, project_id: &str) -> Result<ProjectExecutionSetupResponse> {
        load_project_execution_setup(&self.db, project_id).await
    }

    pub async fn select_execution_principal(
        &self,
        project_id: &str,
        role: ExecutionPrincipalRole,
        request: &SelectExecutionPrincipalRequest,
        actor_user_id: &str,
    ) -> Result<ProjectExecutionSetupResponse> {
        require_idempotency_key(&request.idempotency_key)?;
        let project = self.get_project(project_id).await?;
        self.require_admin(&project, actor_user_id).await?;

        let input = json!({
            "identity_id": request.identity_id,
            "expected_project_version": request.expected_project_version,
            "role": role.role_name(),
        });
        if let Some(replay) = self
            .replay(
                actor_user_id,
                project_id,
                role.operation(),
                &request.idempotency_key,
                &input,
            )
            .await?
        {
            return Ok(replay);
        }

        let roles = resolve_project_execution_roles(&self.db, &project).await?;
        let required_role = match role {
            ExecutionPrincipalRole::Worker => roles.worker_role.as_deref(),
            ExecutionPrincipalRole::IndependentReviewer => roles.reviewer_role.as_deref(),
        }
        .ok_or_else(|| {
            ServiceError::invalid_operation(format!(
                "Project workflow does not require a {} role",
                role.role_name()
            ))
        })?;

        // A response-loss replay is safe even after the Project version has
        // advanced. It is intentionally checked against the stored
        // assignment, rather than the current eligibility resolver: an
        // identity may have become paused after the successful mutation.
        if configured_role_identity(&project, required_role)?.as_deref()
            == Some(request.identity_id.as_str())
        {
            let provisioning_metadata = self.ready_metadata(&project).await?;
            return self
                .apply_project_command(ApplySetupCommand {
                    actor_user_id,
                    project_id,
                    operation: role.operation(),
                    idempotency_key: &request.idempotency_key,
                    input: &input,
                    expected_project_version: None,
                    settings: None,
                    primary_repo_id: None,
                    bump_project_version: false,
                    provisioning_retry: None,
                    provisioning_metadata,
                })
                .await;
        }
        if request.expected_project_version < 1
            || project.version != request.expected_project_version
        {
            return Err(db::DbError::VersionConflict.into());
        }

        let eligible = eligible_project_execution_agents(&self.db, &project).await?;
        if !eligible.iter().any(|agent| agent.id == request.identity_id) {
            return Err(ServiceError::conflict(format!(
                "identity {} is not eligible for the Project {} role",
                request.identity_id,
                role.role_name()
            )));
        }
        if role == ExecutionPrincipalRole::IndependentReviewer
            && configured_role_identity(&project, roles.worker_role.as_deref().unwrap_or("worker"))?
                .as_deref()
                == Some(request.identity_id.as_str())
        {
            return Err(ServiceError::conflict(
                "independent reviewer must be distinct from the Worker identity",
            ));
        }
        if role == ExecutionPrincipalRole::Worker
            && roles.reviewer_identity_id.as_deref() == Some(request.identity_id.as_str())
        {
            return Err(ServiceError::conflict(
                "Worker and independent reviewer identities must be distinct",
            ));
        }

        let mut settings: Value = serde_json::from_str(&project.settings).map_err(|error| {
            ServiceError::invalid_operation(format!("invalid Project settings: {error}"))
        })?;
        let assignments = settings
            .as_object_mut()
            .ok_or_else(|| ServiceError::invalid_operation("Project settings must be an object"))?
            .entry("default_role_assignments")
            .or_insert_with(|| Value::Array(Vec::new()));
        let assignments = assignments.as_array_mut().ok_or_else(|| {
            ServiceError::invalid_operation("default_role_assignments must be an array")
        })?;
        assignments.retain(|assignment| {
            assignment.get("role_name").and_then(Value::as_str) != Some(required_role)
        });
        assignments.push(json!({
            "role_name": required_role,
            "assignee_type": "agent",
            "assignee_id": request.identity_id,
        }));
        let mut projected_project = project.clone();
        projected_project.settings = settings.to_string();
        let provisioning_metadata = self.ready_metadata(&projected_project).await?;

        self.apply_project_command(ApplySetupCommand {
            actor_user_id,
            project_id,
            operation: role.operation(),
            idempotency_key: &request.idempotency_key,
            input: &input,
            expected_project_version: Some(request.expected_project_version),
            settings: Some(settings.to_string()),
            primary_repo_id: None,
            bump_project_version: true,
            provisioning_retry: None,
            provisioning_metadata,
        })
        .await
    }

    pub async fn attach_primary_repository(
        &self,
        project_id: &str,
        request: &AttachPrimaryRepositoryRequest,
        actor_user_id: &str,
    ) -> Result<ProjectExecutionSetupResponse> {
        require_idempotency_key(&request.idempotency_key)?;
        let project = self.get_project(project_id).await?;
        self.require_admin(&project, actor_user_id).await?;
        let input = json!({
            "repo_id": request.repo_id,
            "expected_project_version": request.expected_project_version,
        });
        if let Some(replay) = self
            .replay(
                actor_user_id,
                project_id,
                ATTACH_REPOSITORY_OPERATION,
                &request.idempotency_key,
                &input,
            )
            .await?
        {
            return Ok(replay);
        }
        if project.primary_repo_id.as_deref() == Some(request.repo_id.as_str()) {
            let provisioning_metadata = self.ready_metadata(&project).await?;
            return self
                .apply_project_command(ApplySetupCommand {
                    actor_user_id,
                    project_id,
                    operation: ATTACH_REPOSITORY_OPERATION,
                    idempotency_key: &request.idempotency_key,
                    input: &input,
                    expected_project_version: None,
                    settings: None,
                    primary_repo_id: None,
                    bump_project_version: false,
                    provisioning_retry: None,
                    provisioning_metadata,
                })
                .await;
        }
        if request.expected_project_version < 1
            || project.version != request.expected_project_version
        {
            return Err(db::DbError::VersionConflict.into());
        }
        let repo = RepoRepo::get_by_id(&*self.db, &request.repo_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("repo", request.repo_id.clone()))?;
        if repo.project_id != project_id {
            return Err(ServiceError::not_found("repo", request.repo_id.clone()));
        }

        let mut projected_project = project.clone();
        projected_project.primary_repo_id = Some(request.repo_id.clone());
        let provisioning_metadata = self.ready_metadata(&projected_project).await?;

        self.apply_project_command(ApplySetupCommand {
            actor_user_id,
            project_id,
            operation: ATTACH_REPOSITORY_OPERATION,
            idempotency_key: &request.idempotency_key,
            input: &input,
            expected_project_version: Some(request.expected_project_version),
            settings: None,
            primary_repo_id: Some(Some(request.repo_id.clone())),
            bump_project_version: true,
            provisioning_retry: None,
            provisioning_metadata,
        })
        .await
    }

    pub async fn retry_provisioning(
        &self,
        project_id: &str,
        request: &RetryProvisioningRequest,
        actor_user_id: &str,
    ) -> Result<ProjectExecutionSetupResponse> {
        require_idempotency_key(&request.idempotency_key)?;
        let project = self.get_project(project_id).await?;
        self.require_admin(&project, actor_user_id).await?;
        let input = json!({
            "expected_operation_version": request.expected_operation_version,
        });
        if self
            .replay(
                actor_user_id,
                project_id,
                RETRY_PROVISIONING_OPERATION,
                &request.idempotency_key,
                &input,
            )
            .await?
            .is_some()
        {
            if let Some(operation) =
                ProjectProvisioningRepo::get_provisioning_operation(&*self.db, project_id).await?
            {
                if operation.status == "provisioning"
                    && !lease_is_active(operation.lease_expires_at.as_deref())
                {
                    if let Some(lease_owner) = operation.lease_owner.as_deref() {
                        crate::project_provisioning::provision_genesis_project_with_lease(
                            &self.db,
                            project_id,
                            lease_owner,
                        )
                        .await?;
                    }
                }
            }
            return self.get(project_id).await;
        }
        let operation = ProjectProvisioningRepo::get_provisioning_operation(&*self.db, project_id)
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation(
                    "Project has no durable provisioning operation to retry",
                )
            })?;
        // An in-flight or completed operation is already the result of a
        // prior retry. Returning the projection makes response-loss retries
        // safe without stealing another worker's lease.
        if operation.version != request.expected_operation_version {
            if operation.status == "ready"
                || (operation.status == "provisioning"
                    && lease_is_active(operation.lease_expires_at.as_deref()))
            {
                return self.get(project_id).await;
            }
            return Err(db::DbError::VersionConflict.into());
        }
        if operation.status == "ready" {
            return self.get(project_id).await;
        }
        if operation.attempt_count >= operation.max_attempts
            || (!operation.retryable && operation.status == "failed")
        {
            return Err(ServiceError::conflict(
                "Project provisioning retry budget is exhausted",
            ));
        }

        let now = now_rfc3339();
        let lease_expires_at = chrono::DateTime::parse_from_rfc3339(&now)
            .map(|time| (time + chrono::Duration::seconds(60)).to_rfc3339())
            .map_err(|error| {
                ServiceError::invalid_operation(format!(
                    "could not calculate provisioning lease expiry: {error}"
                ))
            })?;
        let lease_owner = format!("execution-setup-retry:{}", db::new_uuid_v4());
        // Commit the user command receipt and durable provisioning lease in
        // one SQLite transaction before invoking the cross-filesystem
        // reconciler. If the process stops after this transaction, recovery
        // sees the owned lease and can resume it without an unrecorded retry.
        let _accepted = self
            .apply_project_command(ApplySetupCommand {
                actor_user_id,
                project_id,
                operation: RETRY_PROVISIONING_OPERATION,
                idempotency_key: &request.idempotency_key,
                input: &input,
                expected_project_version: None,
                settings: None,
                primary_repo_id: None,
                bump_project_version: false,
                provisioning_retry: Some(ScheduleProjectProvisioningRetry {
                    operation_id: operation.id.clone(),
                    expected_version: operation.version,
                    lease_owner: lease_owner.clone(),
                    lease_expires_at,
                    updated_at: now,
                }),
                provisioning_metadata: None,
            })
            .await?;
        crate::project_provisioning::provision_genesis_project_with_lease(
            &self.db,
            project_id,
            &lease_owner,
        )
        .await?;
        self.get(project_id).await
    }

    async fn get_project(&self, project_id: &str) -> Result<Project> {
        ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))
    }

    async fn require_admin(&self, project: &Project, actor_user_id: &str) -> Result<()> {
        if project.owner_id.as_deref() == Some(actor_user_id) {
            return Ok(());
        }
        let member: i64 = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM project_member
                 WHERE project_id = ? AND user_id = ? AND role IN ('owner', 'admin')
             )",
        )
        .bind(&project.id)
        .bind(actor_user_id)
        .fetch_one(self.db.pool())
        .await?;
        if member == 0 {
            return Err(ServiceError::AuthorizationDenied {
                message: "Project owner or admin role is required".to_owned(),
            });
        }
        Ok(())
    }

    async fn ready_metadata(
        &self,
        project: &Project,
    ) -> Result<Option<ReconcileProjectProvisioningMetadata>> {
        reconcile_project_setup_metadata(&self.db, project).await
    }

    async fn replay(
        &self,
        actor_user_id: &str,
        project_id: &str,
        operation: &str,
        idempotency_key: &str,
        input: &Value,
    ) -> Result<Option<ProjectExecutionSetupResponse>> {
        let input_digest = action_input_digest(input)?;
        let receipt = CommandReceiptRepo::get_command_receipt(
            &*self.db,
            "user",
            actor_user_id,
            "project",
            project_id,
            operation,
            idempotency_key,
            &input_digest,
        )
        .await?;
        if receipt.is_some() {
            return self.get(project_id).await.map(Some);
        }
        Ok(None)
    }

    async fn apply_project_command(
        &self,
        command: ApplySetupCommand<'_>,
    ) -> Result<ProjectExecutionSetupResponse> {
        let ApplySetupCommand {
            actor_user_id,
            project_id,
            operation,
            idempotency_key,
            input,
            expected_project_version,
            settings,
            primary_repo_id,
            bump_project_version,
            provisioning_retry,
            provisioning_metadata,
        } = command;
        let input_digest = action_input_digest(input)?;
        let now = now_rfc3339();
        let receipt = CreateCommandReceipt {
            id: db::new_uuid_v4(),
            principal_type: "user".to_owned(),
            principal_id: actor_user_id.to_owned(),
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            operation: operation.to_owned(),
            idempotency_key: idempotency_key.to_owned(),
            input_digest,
            policy_result: "allowed".to_owned(),
            correlation_id: db::new_uuid_v4(),
            causation_id: None,
            causation_depth: 0,
            event_id: db::new_uuid_v4(),
            agent_action_execution_id: None,
            outcome_json: json!({
                "accepted": true,
                "operation": operation,
                "project_id": project_id,
                "expected_project_version": expected_project_version,
                "accepted_input": input,
            })
            .to_string(),
            committed_at: now,
        };
        ProjectExecutionSetupCommandRepo::apply_project_execution_setup_command(
            &*self.db,
            ApplyProjectExecutionSetupCommand {
                project_id: project_id.to_owned(),
                expected_project_version,
                settings,
                primary_repo_id,
                bump_project_version,
                provisioning_retry,
                provisioning_metadata,
                receipt,
            },
        )
        .await?;
        self.get(project_id).await
    }
}

fn configured_role_identity(project: &Project, role_name: &str) -> Result<Option<String>> {
    let settings: Value = serde_json::from_str(&project.settings).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid Project settings: {error}"))
    })?;
    Ok(settings
        .get("default_role_assignments")
        .and_then(Value::as_array)
        .and_then(|assignments| {
            assignments.iter().rev().find_map(|assignment| {
                (assignment.get("role_name").and_then(Value::as_str) == Some(role_name))
                    .then(|| assignment.get("assignee_id").and_then(Value::as_str))
                    .flatten()
                    .map(str::to_owned)
            })
        }))
}

fn action_input_digest(input: &Value) -> Result<String> {
    canonical_digest_with_schema(RECEIPT_SCHEMA, input).map_err(|error| {
        ServiceError::invalid_operation(format!("execution-setup input digest failed: {error}"))
    })
}

fn lease_is_active(expires_at: Option<&str>) -> bool {
    let Some(expires_at) = expires_at else {
        return false;
    };
    chrono::DateTime::parse_from_rfc3339(expires_at)
        .map(|expiry| expiry > chrono::Utc::now())
        .unwrap_or(false)
}

fn require_idempotency_key(key: &str) -> Result<()> {
    if key.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "idempotency_key must not be empty",
        ));
    }
    Ok(())
}
