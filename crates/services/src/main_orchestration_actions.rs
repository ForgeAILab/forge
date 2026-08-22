//! Typed materialization for Main Agent orchestration proposals.
//!
//! Main orchestration tools intentionally create an `AgentAction` proposal
//! first.  This module is the only service boundary which may turn the
//! proposal into a Charter revision, a Charter projection, or the atomic
//! Charter-backed Project handoff.  The generic action executor must not be
//! used for these operations because accepting an arbitrary result there
//! would make a successful ledger row without doing the domain work.

use std::{collections::BTreeMap, sync::Arc};

use api_types::{ProductMaturity, ProjectMode};
use db::{
    new_uuid_v4, now_rfc3339, AgentAction, AgentActionExecution, AgentActionExecutionStatus,
    AgentActionPolicyResult, AgentActionStatus, CreateAgentActionExecution, CreateCommandReceipt,
    SqliteDb,
};
use forge_agent_host::MAIN_PROJECT_CREATE_OPERATION;
use serde_json::{json, Value};
use sqlx::Row;

use crate::{
    create_project_from_charter_approval, AgentActionProvenance, AgentActionService,
    AuthorizationProvenance, CommandContext, CommandPrincipal, CommandScope, CommandScopeType,
    CreateProjectAuthorization, CreateProjectFromCharterApprovalInput, ExpectedCommandState,
    NewCommandContext, Result, ServiceError,
};

/// Input to the typed Main orchestration execution boundary.  `executed_by`
/// is deliberately explicit: Project creation may only be executed by the
/// authenticated user who owns the Main Agent account, while draft/projection
/// operations may be executed by the bound Main identity or that user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteMainOrchestrationActionInput {
    pub action_id: String,
    pub expected_version: i64,
    pub executed_by_type: String,
    pub executed_by_id: String,
    pub idempotency_key: String,
}

#[derive(Clone)]
pub struct MainOrchestrationActionService {
    db: Arc<SqliteDb>,
    actions: AgentActionService,
}

impl MainOrchestrationActionService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self {
            actions: AgentActionService::new(Arc::clone(&db)),
            db,
        }
    }

    /// Execute a Main orchestration proposal through its typed domain path.
    /// Replays are resolved from the action execution ledger before checking
    /// mutable Charter/approval state, so a response lost after commit is
    /// safe to retry with the same idempotency key.
    pub async fn execute(
        &self,
        input: ExecuteMainOrchestrationActionInput,
    ) -> Result<AgentActionExecution> {
        let action = self.actions.get(&input.action_id).await?;
        if action.operation != MAIN_PROJECT_CREATE_OPERATION {
            return Err(ServiceError::invalid_operation(
                "action is not a Main orchestration proposal",
            ));
        }
        let payload: Value = serde_json::from_str(&action.payload_json).map_err(|_| {
            ServiceError::invalid_operation("Main orchestration action payload is invalid")
        })?;
        let command_context = main_command_context(&self.db, &action, &input, &payload).await?;

        // A committed command receipt is the replay boundary. Resolve it
        // before mutable actor, Genesis, Charter, or approval checks so a
        // response lost after commit cannot rerun against changed state.
        if action.operation == MAIN_PROJECT_CREATE_OPERATION {
            if let Some(receipt) = db::CommandReceiptRepo::get_command_receipt(
                &*self.db,
                command_context.principal().principal_type(),
                command_context.principal().principal_id(),
                command_context.canonical_scope().scope_type().as_str(),
                command_context.canonical_scope().scope_id(),
                command_context.operation(),
                command_context.idempotency_key(),
                command_context.input_digest(),
            )
            .await?
            {
                let execution = db::AgentActionRepo::get_successful_action_execution(
                    &*self.db,
                    &input.action_id,
                )
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict(
                        "Main command receipt has no successful AgentAction execution".to_owned(),
                    )
                })?;
                if receipt.agent_action_execution_id.as_deref() != Some(execution.id.as_str())
                    || receipt.outcome_json != execution.result_json.clone().unwrap_or_default()
                {
                    return Err(ServiceError::Conflict(
                        "Main command receipt provenance does not match its AgentAction execution"
                            .to_owned(),
                    ));
                }
                return refresh_project_create_execution(&self.db, execution).await;
            }
        }
        authorize_action_actor(
            &self.db,
            &action,
            &input.executed_by_type,
            &input.executed_by_id,
        )
        .await?;

        if let Some(existing) =
            db::AgentActionRepo::get_successful_action_execution(&*self.db, &input.action_id)
                .await?
        {
            if existing.idempotency_key != input.idempotency_key {
                return Err(ServiceError::conflict(
                    "Main orchestration action already has a successful execution with a different idempotency key",
                ));
            }
            // A replay is an exact authorization replay, not merely a lookup
            // by dedupe key.  AgentActionExecution does not persist the
            // expected action version, so bind the replay to the durable
            // executor envelope that is actually recorded rather than
            // pretending an unavailable version field exists.
            if existing.executed_by_type != input.executed_by_type
                || existing.executed_by_id != input.executed_by_id
                || input
                    .expected_version
                    .checked_add(1)
                    .is_none_or(|version| action.version != version)
            {
                return Err(ServiceError::conflict(
                    "Main orchestration replay authorization differs from the committed execution",
                ));
            }
            return Ok(existing);
        }

        if action.policy_result == AgentActionPolicyResult::Denied
            || matches!(
                action.status,
                AgentActionStatus::Denied | AgentActionStatus::Cancelled
            )
        {
            return Err(ServiceError::invalid_operation(
                "denied or cancelled Main orchestration action cannot execute",
            ));
        }
        let admitted = matches!(
            (&action.policy_result, &action.status),
            (
                AgentActionPolicyResult::Allowed,
                AgentActionStatus::Proposed
            ) | (
                AgentActionPolicyResult::ApprovalRequired,
                AgentActionStatus::Approved,
            )
        );
        if !admitted {
            return Err(ServiceError::invalid_operation(
                "Main orchestration action requires an admitted policy result and status",
            ));
        }
        if action.version != input.expected_version {
            return Err(ServiceError::Db(db::DbError::VersionConflict));
        }

        match action.operation.as_str() {
            MAIN_PROJECT_CREATE_OPERATION => {
                if input.executed_by_type != "user" {
                    return Err(ServiceError::invalid_operation(
                        "Project creation from a Charter approval is user-only",
                    ));
                }
                self.execute_project_create(
                    &action,
                    &payload,
                    &input.executed_by_id,
                    &input.idempotency_key,
                    &input,
                    &command_context,
                )
                .await?;
            }
            _ => unreachable!("operation was validated above"),
        }
        #[cfg(test)]
        if crate::test_support::take_after_domain_commit(&action.id) {
            return Err(ServiceError::conflict(
                "characterization failpoint: stopped after Main domain commit before AgentAction receipt",
            ));
        }
        let execution =
            db::AgentActionRepo::get_successful_action_execution(&*self.db, &input.action_id)
                .await?
                .ok_or_else(|| {
                    ServiceError::Conflict(
                        "Main command committed without its AgentAction execution receipt"
                            .to_owned(),
                    )
                })?;
        refresh_project_create_execution(&self.db, execution).await
    }

    async fn execute_project_create(
        &self,
        action: &AgentAction,
        payload: &Value,
        user_id: &str,
        create_idempotency_key: &str,
        execution_input: &ExecuteMainOrchestrationActionInput,
        command_context: &CommandContext,
    ) -> Result<Value> {
        let approval_id = payload
            .get("approval_id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ServiceError::invalid_operation("approval_id is required"))?;
        let placeholder_result = json!({
            "operation": MAIN_PROJECT_CREATE_OPERATION,
            "approval_id": approval_id,
        })
        .to_string();
        let (command_receipt, action_execution) = main_command_finalization(
            action,
            execution_input,
            command_context,
            &placeholder_result,
        )?;
        let created = create_project_from_charter_approval(
            Arc::clone(&self.db),
            CreateProjectFromCharterApprovalInput {
                approval_id: approval_id.to_owned(),
                idempotency_key: create_idempotency_key.to_owned(),
                account_id: user_id.to_owned(),
                authorization: CreateProjectAuthorization {
                    principal_type: "user".to_owned(),
                    principal_id: user_id.to_owned(),
                    action: "product_genesis.create_project_from_approval".to_owned(),
                    authorization_basis: "authenticated user executed typed project.create action"
                        .to_owned(),
                    event_id: action.id.clone(),
                    occurred_at: action.created_at.clone(),
                },
                correlation_id: action.correlation_id.clone(),
                causation_depth: action.causation_depth + 1,
                command_receipt: Some(command_receipt),
                action_execution: Some(action_execution),
            },
        )
        .await?;
        let execution_setup =
            crate::load_project_execution_setup(&self.db, &created.project.id).await?;
        Ok(json!({
            "operation": MAIN_PROJECT_CREATE_OPERATION,
            "project_id": created.project.id,
            "project_agent_binding_id": created.project_agent_binding_id,
            "project_chat_id": created.project_chat_id,
            "charter_id": created.charter_id,
            "charter_revision_id": created.charter_revision_id,
            "handoff_id": created.handoff_id,
            "target_message_id": created.target_message_id,
            "target_turn_id": created.target_turn_id,
            "execution_setup": execution_setup,
        }))
    }
}

/// The command receipt/action execution keeps the original Project-create
/// identifiers immutable. The setup projection is deliberately refreshed on
/// each returned response so a replay after a provisioning retry reports the
/// current setup state without mutating the frozen receipt.
async fn refresh_project_create_execution(
    db: &Arc<SqliteDb>,
    mut execution: AgentActionExecution,
) -> Result<AgentActionExecution> {
    let Some(result_json) = execution.result_json.as_deref() else {
        return Ok(execution);
    };
    let mut outcome = serde_json::from_str::<Value>(result_json).map_err(|_| {
        ServiceError::invalid_operation("stored Main Project-create outcome is invalid")
    })?;
    let Some(project_id) = outcome.get("project_id").and_then(Value::as_str) else {
        return Ok(execution);
    };
    let setup = crate::load_project_execution_setup(db, project_id).await?;
    outcome["execution_setup"] = serde_json::to_value(setup).map_err(|_| {
        ServiceError::invalid_operation("Project execution setup projection is not serializable")
    })?;
    execution.result_json = Some(outcome.to_string());
    Ok(execution)
}

async fn main_command_context(
    db: &SqliteDb,
    action: &AgentAction,
    execution_input: &ExecuteMainOrchestrationActionInput,
    payload: &Value,
) -> Result<CommandContext> {
    let account_id = action_account_id(db, action).await?;
    let principal_type = required_value(
        "typed orchestration executor type",
        &execution_input.executed_by_type,
    )?;
    let principal_id = required_value(
        "typed orchestration executor id",
        &execution_input.executed_by_id,
    )?;
    let idempotency_key = required_value(
        "execution idempotency key",
        &execution_input.idempotency_key,
    )?;
    CommandContext::from_authorized_input(
        NewCommandContext {
            principal: CommandPrincipal {
                principal_type,
                principal_id,
            },
            canonical_scope: CommandScope {
                scope_type: CommandScopeType::Account,
                scope_id: account_id,
            },
            operation: action.operation.clone(),
            idempotency_key,
            expected_state: ExpectedCommandState {
                versions: BTreeMap::from([(action.id.clone(), execution_input.expected_version)]),
                digests: BTreeMap::new(),
            },
            authorization_provenance: Some(AuthorizationProvenance {
                policy_result: action.policy_result.to_string(),
                policy_revision: None,
                policy_digest: None,
                requested_permission: Some(action.requested_permission.clone()),
            }),
            action_provenance: Some(AgentActionProvenance {
                action_id: action.id.clone(),
                expected_action_version: execution_input.expected_version,
                attempt: 1,
                execution_idempotency_key: execution_input.idempotency_key.clone(),
                executed_by_type: execution_input.executed_by_type.clone(),
                executed_by_id: execution_input.executed_by_id.clone(),
            }),
            correlation_id: action.correlation_id.clone(),
            causation_id: action.causation_id.clone(),
            causation_depth: action.causation_depth,
        },
        payload,
    )
    .map_err(|error| {
        ServiceError::invalid_operation(format!("serialize Main command input digest: {error}"))
    })
}

async fn action_account_id(db: &SqliteDb, action: &AgentAction) -> Result<String> {
    let account_id =
        sqlx::query_scalar::<_, Option<String>>("SELECT owner_id FROM agent_identity WHERE id = ?")
            .bind(&action.actor_identity_id)
            .fetch_optional(db.pool())
            .await?
            .flatten()
            .ok_or_else(|| {
                ServiceError::not_found("agent_identity", action.actor_identity_id.clone())
            })?;
    match action.scope_type.as_str() {
        "account" if action.scope_id == account_id => Ok(account_id),
        "agent_chat" => {
            let row = sqlx::query(
                "SELECT kind, account_id FROM agent_chat WHERE id = ? AND kind = 'account_main'",
            )
            .bind(&action.scope_id)
            .fetch_optional(db.pool())
            .await?
            .ok_or_else(|| {
                ServiceError::invalid_operation("Main action scope is not a Main Chat")
            })?;
            let chat_account =
                row.try_get::<Option<String>, _>("account_id")?
                    .ok_or_else(|| {
                        ServiceError::invalid_operation("Main Chat has no owning account")
                    })?;
            if chat_account != account_id {
                return Err(ServiceError::invalid_operation(
                    "Main action identity does not own the Main Chat account",
                ));
            }
            Ok(account_id)
        }
        _ => Err(ServiceError::invalid_operation(
            "Main orchestration action must be account- or Main-Chat-scoped",
        )),
    }
}

async fn authorize_action_actor(
    db: &SqliteDb,
    action: &AgentAction,
    executed_by_type: &str,
    executed_by_id: &str,
) -> Result<()> {
    let account_id = action_account_id(db, action).await?;
    if executed_by_id.trim().is_empty() {
        return Err(ServiceError::invalid_operation(
            "typed orchestration executor id is required",
        ));
    }
    if executed_by_type == "agent" && executed_by_id == action.actor_identity_id {
        return Ok(());
    }
    if executed_by_type == "user" && executed_by_id == account_id {
        return Ok(());
    }
    Err(ServiceError::invalid_operation(
        "typed orchestration executor is not the bound Main identity or account owner",
    ))
}

pub(crate) fn parse_project_mode(value: &str) -> Result<ProjectMode> {
    match value {
        "compact" => Ok(ProjectMode::Compact),
        "standard" => Ok(ProjectMode::Standard),
        _ => Err(ServiceError::invalid_operation(
            "persisted Charter mode is invalid",
        )),
    }
}

pub(crate) fn parse_maturity(value: &str) -> Result<ProductMaturity> {
    match value {
        "prototype" => Ok(ProductMaturity::Prototype),
        "mvp" => Ok(ProductMaturity::Mvp),
        "production" => Ok(ProductMaturity::Production),
        "critical" => Ok(ProductMaturity::Critical),
        _ => Err(ServiceError::invalid_operation(
            "persisted Charter maturity is invalid",
        )),
    }
}

fn required_value(field: &'static str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ServiceError::invalid_operation(format!(
            "{field} must not be empty"
        )));
    }
    Ok(value.to_owned())
}

fn main_command_finalization(
    action: &AgentAction,
    execution_input: &ExecuteMainOrchestrationActionInput,
    command_context: &CommandContext,
    result_json: &str,
) -> Result<(CreateCommandReceipt, CreateAgentActionExecution)> {
    let idempotency_key = required_value(
        "execution idempotency key",
        &execution_input.idempotency_key,
    )?;
    let executed_by_id = required_value(
        "typed orchestration executor id",
        &execution_input.executed_by_id,
    )?;
    let execution_id = new_uuid_v4();
    let committed_at = now_rfc3339();
    let execution = CreateAgentActionExecution {
        id: execution_id.clone(),
        action_id: action.id.clone(),
        expected_action_version: execution_input.expected_version,
        attempt: 1,
        status: AgentActionExecutionStatus::Succeeded,
        result_json: Some(result_json.to_owned()),
        error: None,
        executed_by_type: execution_input.executed_by_type.clone(),
        executed_by_id: executed_by_id.clone(),
        idempotency_key: idempotency_key.clone(),
        action_status: AgentActionStatus::Executed,
        action_outcome_json: Some(result_json.to_owned()),
        created_at: committed_at.clone(),
        completed_at: Some(committed_at.clone()),
        updated_at: committed_at.clone(),
    };
    let receipt = CreateCommandReceipt {
        id: new_uuid_v4(),
        principal_type: command_context.principal().principal_type().to_owned(),
        principal_id: command_context.principal().principal_id().to_owned(),
        scope_type: command_context
            .canonical_scope()
            .scope_type()
            .as_str()
            .to_owned(),
        scope_id: command_context.canonical_scope().scope_id().to_owned(),
        operation: command_context.operation().to_owned(),
        idempotency_key,
        input_digest: command_context.input_digest().to_owned(),
        policy_result: action.policy_result.to_string(),
        correlation_id: command_context.correlation_id().to_owned(),
        causation_id: command_context.causation_id.clone(),
        causation_depth: command_context.causation_depth,
        // The domain repository replaces this template value with the event
        // generated in the same transaction before inserting the receipt.
        event_id: String::new(),
        agent_action_execution_id: Some(execution_id),
        outcome_json: result_json.to_owned(),
        committed_at,
    };
    Ok((receipt, execution))
}
