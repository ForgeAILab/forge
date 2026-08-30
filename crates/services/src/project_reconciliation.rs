//! Shared, scoped Project reconciliation query/command service (design D15).
//!
//! `project_reconciliation_record`/`project_canonical_conflict` rows have
//! existed since V076, but until this module the only caller of
//! `ProjectOrchestrationRepo::resolve_project_reconciliation` was a database
//! test -- there was no service, REST route, or UI control that could ever
//! resolve one (finding F10).  This service is the single place that:
//!
//! - authorizes the Project and principal before any lookup;
//! - checks record/conflict/governing-reference consistency;
//! - enforces the expected-version and idempotency-key contract;
//! - validates the closed resolution-action vocabulary and requires an exact
//!   `replacement_ref` for `revised`/`superseded`;
//! - commits the resolution, canonical-conflict disposition, command
//!   receipt, and durable domain event atomically (in the `db` transaction);
//! - and, only after that commit, publishes the event and wakes the exactly
//!   one affected Task's dispatcher.
//!
//! Execution-baseline, Charter, waiver, and release-governing resolutions
//! remain interactive-user decisions per the design addendum, and no chat
//! agent is ever given a generic self-resolve tool -- so this service
//! requires an interactive `PrincipalKind::User` authorization for every
//! resolution, not only the especially sensitive record types.  Read access
//! (`list`/`get`) has no such restriction: the Project Agent may read the
//! bounded reconciliation state through the same methods.

use std::sync::Arc;

use api_types::{
    canonical_digest_with_schema, AuthorizationProvenance, PrincipalKind, PrincipalRef,
    ProjectReconciliation, ReconciliationConflictSummary, ReconciliationRecordRef,
    ReconciliationReplacementRef, ReconciliationResolutionAction, ReconciliationResolutionSummary,
    ReconciliationState, ResolveProjectReconciliationRequest, ResolveProjectReconciliationResponse,
};
use chrono::{DateTime, Utc};
use db::{
    CommandReceiptRepo, CreateCommandReceipt, CreateDomainEvent, DomainEventRepo,
    ProjectMemberRepo, ProjectOrchestrationRepo, ProjectReconciliationRecord, ProjectRepo,
    ResolveProjectReconciliation, SqliteDb,
};
use events::EventBus;
use serde_json::json;

use crate::{DomainEventService, Result, ServiceError};

pub const RESOLVE_PROJECT_RECONCILIATION_OPERATION: &str = "project.reconciliation.resolve";

const RECONCILIATION_INPUT_DIGEST_SCHEMA: &str = "forge.reconciliation-resolve-input/v1";
const MAX_AUTHORIZATION_TIMESTAMP_LEN: usize = 64;
const MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS: i64 = 48 * 60 * 60;
const RESOLUTION_ACTIONS: [ReconciliationResolutionAction; 5] = [
    ReconciliationResolutionAction::Retained,
    ReconciliationResolutionAction::Revised,
    ReconciliationResolutionAction::Cancelled,
    ReconciliationResolutionAction::Superseded,
    ReconciliationResolutionAction::Invalidated,
];

/// One page of the keyset-paginated reconciliation list.  Ordered by
/// `updated_at DESC, id DESC`, matching `list_project_reconciliations`.
pub struct ProjectReconciliationPage {
    pub items: Vec<ProjectReconciliation>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Clone)]
pub struct ProjectReconciliationService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

impl ProjectReconciliationService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    /// List reconciliations for a Project.  Bounded, read-only, and safe for
    /// the Project Agent's own operating context -- unlike `resolve`, no
    /// interactive-user restriction applies here.
    pub async fn list(
        &self,
        project_id: &str,
        user_id: &str,
        cursor: Option<&str>,
        limit: i64,
    ) -> Result<ProjectReconciliationPage> {
        self.authorize_project(project_id, user_id).await?;
        let limit = limit.clamp(1, 100);
        let cursor = decode_cursor(cursor)?;
        let records =
            ProjectOrchestrationRepo::list_project_reconciliations(&*self.db, project_id).await?;
        let mut page: Vec<&ProjectReconciliationRecord> = records
            .iter()
            .filter(|record| match &cursor {
                None => true,
                Some((updated_at, id)) => {
                    (record.updated_at.as_str(), record.id.as_str())
                        < (updated_at.as_str(), id.as_str())
                }
            })
            .collect();
        let has_more = page.len() > limit as usize;
        page.truncate(limit as usize);
        let mut items = Vec::with_capacity(page.len());
        for record in page {
            items.push(self.project_reconciliation(record).await?);
        }
        let next_cursor = items
            .last()
            .map(|item| encode_cursor(&item.updated_at, &item.id))
            .filter(|_| has_more);
        Ok(ProjectReconciliationPage {
            items,
            next_cursor,
            has_more,
        })
    }

    pub async fn get(
        &self,
        project_id: &str,
        user_id: &str,
        reconciliation_id: &str,
    ) -> Result<ProjectReconciliation> {
        self.authorize_project(project_id, user_id).await?;
        let record =
            ProjectOrchestrationRepo::get_project_reconciliation(&*self.db, reconciliation_id)
                .await?
                .filter(|record| record.project_id == project_id)
                .ok_or_else(|| {
                    ServiceError::not_found("project_reconciliation", reconciliation_id.to_owned())
                })?;
        self.project_reconciliation(&record).await
    }

    /// Resolve a reconciliation.  User-only: `validate_resolve_authorization`
    /// rejects any non-`user` principal before any mutation is attempted, so
    /// this method can never be wired as a generic agent self-resolve tool.
    pub async fn resolve(
        &self,
        project_id: &str,
        user_id: &str,
        reconciliation_id: &str,
        request: ResolveProjectReconciliationRequest,
    ) -> Result<ResolveProjectReconciliationResponse> {
        self.authorize_project(project_id, user_id).await?;
        validate_resolve_authorization(
            &request.mutation.authorization,
            user_id,
            RESOLVE_PROJECT_RECONCILIATION_OPERATION,
        )?;
        if request.reason.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "reconciliation resolution requires a non-empty reason",
            ));
        }
        if request.mutation.idempotency_key.trim().is_empty() {
            return Err(ServiceError::invalid_operation(
                "reconciliation resolution requires an idempotency key",
            ));
        }

        let record =
            ProjectOrchestrationRepo::get_project_reconciliation(&*self.db, reconciliation_id)
                .await?
                .filter(|record| record.project_id == project_id)
                .ok_or_else(|| {
                    ServiceError::not_found("project_reconciliation", reconciliation_id.to_owned())
                })?;
        let conflict = ProjectOrchestrationRepo::get_project_canonical_conflict(
            &*self.db,
            &record.conflict_id,
        )
        .await?
        .filter(|conflict| conflict.project_id == project_id)
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "reconciliation references a canonical conflict outside this Project",
            )
        })?;
        if conflict.conflicting_record_type != record.record_type
            || conflict.conflicting_record_id != record.record_id
            || conflict.governing_record_type != record.governing_record_type
            || conflict.governing_record_id != record.governing_record_id
        {
            return Err(ServiceError::invalid_operation(
                "reconciliation record is inconsistent with its governing conflict",
            ));
        }

        let replacement_required = matches!(
            request.action,
            ReconciliationResolutionAction::Revised | ReconciliationResolutionAction::Superseded
        );
        match (&request.replacement_ref, replacement_required) {
            (None, true) => {
                return Err(ServiceError::invalid_operation(format!(
                    "{} requires an exact replacement_ref",
                    reconciliation_action_wire(request.action)
                )));
            }
            (Some(_), false) => {
                return Err(ServiceError::invalid_operation(
                    "replacement_ref is only valid for revised/superseded resolutions",
                ));
            }
            _ => {}
        }
        if let Some(replacement_ref) = &request.replacement_ref {
            if replacement_ref.record_type.trim().is_empty()
                || replacement_ref.record_id.trim().is_empty()
            {
                return Err(ServiceError::invalid_operation(
                    "replacement_ref requires a non-empty record_type and record_id",
                ));
            }
        }

        let now = db::now_rfc3339();
        let resolution_id = db::new_uuid_v4();
        let event_id = db::new_uuid_v4();
        let action_wire = reconciliation_action_wire(request.action);
        let principal = request.mutation.authorization.principal.clone();
        let principal_type = principal_kind_wire(principal.kind).to_owned();
        let correlation_id = request.mutation.authorization.event_id.clone();

        let input_digest = canonical_digest_with_schema(
            RECONCILIATION_INPUT_DIGEST_SCHEMA,
            &json!({
                "project_id": project_id,
                "reconciliation_id": reconciliation_id,
                "expected_version": request.mutation.expected_version,
                "action": action_wire,
                "replacement_ref": request.replacement_ref,
                "reason": request.reason,
                "principal": principal,
            }),
        )
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "reconciliation resolve input is invalid: {error}"
            ))
        })?;
        let payload_json = serde_json::to_string(&json!({
            "project_id": project_id,
            "reconciliation_id": reconciliation_id,
            "conflict_id": record.conflict_id,
            "action": action_wire,
            "record_type": record.record_type,
            "record_id": record.record_id,
            "replacement_ref": request.replacement_ref,
        }))
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "reconciliation resolve event payload is invalid: {error}"
            ))
        })?;
        let outcome_json = json!({
            "state": action_wire,
            "reconciliation_id": reconciliation_id,
        })
        .to_string();

        let domain_event = CreateDomainEvent {
            id: event_id.clone(),
            event_type: "project.reconciliation.resolved".to_owned(),
            entity_type: "project_reconciliation".to_owned(),
            entity_id: reconciliation_id.to_owned(),
            actor_type: principal_type.clone(),
            actor_id: Some(principal.id.clone()),
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            correlation_id: correlation_id.clone(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(format!(
                "project.reconciliation.resolved:{project_id}:{reconciliation_id}:{}",
                request.mutation.idempotency_key
            )),
            payload_json,
            created_at: now.clone(),
        };
        let command_receipt = CreateCommandReceipt {
            id: db::new_uuid_v4(),
            principal_type: principal_type.clone(),
            principal_id: principal.id.clone(),
            scope_type: "project".to_owned(),
            scope_id: project_id.to_owned(),
            operation: RESOLVE_PROJECT_RECONCILIATION_OPERATION.to_owned(),
            idempotency_key: request.mutation.idempotency_key.clone(),
            input_digest,
            policy_result: "allowed".to_owned(),
            correlation_id,
            causation_id: None,
            causation_depth: 0,
            event_id: event_id.clone(),
            agent_action_execution_id: None,
            outcome_json,
            committed_at: now.clone(),
        };

        let resolved = ProjectOrchestrationRepo::resolve_project_reconciliation(
            &*self.db,
            ResolveProjectReconciliation {
                id: reconciliation_id.to_owned(),
                expected_version: request.mutation.expected_version,
                resolution_id,
                action: action_wire.to_owned(),
                principal_type: principal_type.clone(),
                principal_id: principal.id.clone(),
                authorization_basis: request.mutation.authorization.authorization_basis.clone(),
                authorization_action: request.mutation.authorization.action.clone(),
                explicit_event: request.mutation.authorization.event_id.clone(),
                authorization_occurred_at: request.mutation.authorization.occurred_at.clone(),
                reason: request.reason.clone(),
                replacement_ref_type: request
                    .replacement_ref
                    .as_ref()
                    .map(|replacement_ref| replacement_ref.record_type.clone()),
                replacement_ref_id: request
                    .replacement_ref
                    .as_ref()
                    .map(|replacement_ref| replacement_ref.record_id.clone()),
                replacement_ref_revision: request
                    .replacement_ref
                    .as_ref()
                    .and_then(|replacement_ref| replacement_ref.record_revision.clone()),
                // The client-authorized timestamp, not a fresh server clock
                // read: an identical retry must compare equal to the first
                // attempt's stored resolution row for the idempotency replay
                // check below to recognize it as the same command rather
                // than a stale-version conflict.
                occurred_at: request.mutation.authorization.occurred_at.clone(),
                idempotency_key: request.mutation.idempotency_key.clone(),
                updated_at: now,
                domain_event,
                command_receipt,
            },
        )
        .await?;

        // The transaction above may have short-circuited on a replay, in
        // which case the ids generated above were discarded in favor of the
        // first commit's.  Look the receipt up by its stable identity rather
        // than trusting the locally generated ids, so the response is exact
        // on both a fresh commit and a replay.
        let receipt = CommandReceiptRepo::get_command_receipt_by_identity(
            &*self.db,
            &principal_type,
            &principal.id,
            "project",
            project_id,
            RESOLVE_PROJECT_RECONCILIATION_OPERATION,
            &request.mutation.idempotency_key,
        )
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "reconciliation resolution committed without a durable command receipt",
            )
        })?;

        // Post-commit publication: the transaction already committed, so a
        // failure to mirror it to the in-process bus does not roll anything
        // back and must not be reported as a command failure.
        if let Some(event) = DomainEventRepo::get_event(&*self.db, &receipt.event_id).await? {
            DomainEventService::new(Arc::clone(&self.db), Arc::clone(&self.event_bus))
                .publish_committed(&event);
        }

        let dispatch_woken = if resolved.record_type == "task" {
            match crate::wake_task_dispatch(
                &self.db,
                &resolved.record_id,
                "reconciliation_resolved",
            )
            .await
            {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!(
                        reconciliation_id = %resolved.id,
                        task_id = %resolved.record_id,
                        %error,
                        "reconciliation resolved but waking the affected Task dispatch failed"
                    );
                    false
                }
            }
        } else {
            false
        };

        let reconciliation = self.project_reconciliation(&resolved).await?;
        Ok(ResolveProjectReconciliationResponse {
            reconciliation,
            receipt_id: receipt.id,
            event_id: receipt.event_id,
            dispatch_woken,
        })
    }

    async fn authorize_project(&self, project_id: &str, user_id: &str) -> Result<()> {
        let project = ProjectRepo::get_by_id(&*self.db, project_id)
            .await?
            .ok_or_else(|| ServiceError::not_found("project", project_id.to_owned()))?;
        if project.owner_id.as_deref() != Some(user_id)
            && ProjectMemberRepo::get_member(&*self.db, project_id, user_id)
                .await?
                .is_none()
        {
            return Err(ServiceError::not_found("project", project_id.to_owned()));
        }
        Ok(())
    }

    async fn resolution_summary(
        &self,
        resolution_id: &str,
    ) -> Result<ReconciliationResolutionSummary> {
        let resolution = ProjectOrchestrationRepo::get_project_reconciliation_resolution(
            &*self.db,
            resolution_id,
        )
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation("reconciliation references a missing resolution record")
        })?;
        let replacement_ref = match (
            resolution.replacement_ref_type,
            resolution.replacement_ref_id,
        ) {
            (Some(record_type), Some(record_id)) => Some(ReconciliationReplacementRef {
                record_type,
                record_id,
                record_revision: resolution.replacement_ref_revision,
            }),
            _ => None,
        };
        Ok(ReconciliationResolutionSummary {
            id: resolution.id,
            action: reconciliation_action_from_wire(&resolution.action)?,
            principal: PrincipalRef {
                kind: principal_kind_from_wire(&resolution.principal_type),
                id: resolution.principal_id,
                display_name: None,
            },
            reason: resolution.reason,
            replacement_ref,
            occurred_at: resolution.occurred_at,
        })
    }

    async fn project_reconciliation(
        &self,
        record: &ProjectReconciliationRecord,
    ) -> Result<ProjectReconciliation> {
        let conflict = ProjectOrchestrationRepo::get_project_canonical_conflict(
            &*self.db,
            &record.conflict_id,
        )
        .await?
        .ok_or_else(|| {
            ServiceError::invalid_operation(
                "reconciliation references a missing canonical conflict",
            )
        })?;
        let affected_paths: Vec<String> =
            serde_json::from_str(&conflict.affected_paths_json).unwrap_or_default();
        let state = reconciliation_state_from_wire(&record.state)?;
        let adaptive_boundary = conflict.conflict_code == "adaptive_task_boundary_crossed";
        let _ = adaptive_boundary;
        let suggested_replacement_ref = None;
        let allowed_actions = if state == ReconciliationState::Required {
            RESOLUTION_ACTIONS.to_vec()
        } else {
            Vec::new()
        };
        let resolution = match &record.current_resolution_id {
            Some(resolution_id) => Some(self.resolution_summary(resolution_id).await?),
            None => None,
        };
        Ok(ProjectReconciliation {
            id: record.id.clone(),
            project_id: record.project_id.clone(),
            conflict: ReconciliationConflictSummary {
                id: conflict.id,
                domain: conflict.domain,
                governing: ReconciliationRecordRef {
                    record_type: conflict.governing_record_type,
                    record_id: conflict.governing_record_id,
                    record_revision: conflict.governing_record_revision,
                    record_digest: conflict.governing_record_digest,
                },
                conflicting: ReconciliationRecordRef {
                    record_type: conflict.conflicting_record_type,
                    record_id: conflict.conflicting_record_id,
                    record_revision: conflict.conflicting_record_revision,
                    record_digest: conflict.conflicting_record_digest,
                },
                affected_paths,
                conflict_code: conflict.conflict_code,
                description: conflict.description,
                detected_by_type: conflict.detected_by_type,
                detected_by_id: conflict.detected_by_id,
                created_at: conflict.created_at,
            },
            affected: ReconciliationRecordRef {
                record_type: record.record_type.clone(),
                record_id: record.record_id.clone(),
                record_revision: record.record_revision.clone(),
                record_digest: record.record_digest.clone(),
            },
            governing: ReconciliationRecordRef {
                record_type: record.governing_record_type.clone(),
                record_id: record.governing_record_id.clone(),
                record_revision: record.governing_record_revision.clone(),
                record_digest: record.governing_record_digest.clone(),
            },
            state,
            required_principal: PrincipalKind::User,
            allowed_actions,
            suggested_replacement_ref,
            resolution,
            version: record.version,
            created_at: record.created_at.clone(),
            updated_at: record.updated_at.clone(),
        })
    }
}

/// Reject anything but a fresh, interactive-user authorization event.  No
/// chat agent is ever given a generic self-resolve tool, so this check is
/// unconditional -- not narrowed to baseline/Charter/waiver/release record
/// types -- and is enforced here rather than left to callers.
fn validate_resolve_authorization(
    authorization: &AuthorizationProvenance,
    user_id: &str,
    expected_action: &str,
) -> Result<()> {
    if authorization.principal.kind != PrincipalKind::User
        || authorization.principal.id != user_id
        || authorization.action != expected_action
        || authorization.authorization_basis.trim().is_empty()
        || authorization.event_id.trim().is_empty()
        || authorization.occurred_at.trim().is_empty()
        || !well_formed_authorization_timestamp(&authorization.occurred_at)
    {
        return Err(ServiceError::AuthorizationDenied {
            message: "reconciliation resolution requires an explicit authenticated user authorization event"
                .to_owned(),
        });
    }
    if !valid_authorization_timestamp(&authorization.occurred_at) {
        return Err(ServiceError::AuthorizationDenied {
            message:
                "reconciliation resolution requires a recent authenticated user authorization event"
                    .to_owned(),
        });
    }
    Ok(())
}

fn well_formed_authorization_timestamp(value: &str) -> bool {
    value.len() <= MAX_AUTHORIZATION_TIMESTAMP_LEN
        && value.trim() == value
        && DateTime::parse_from_rfc3339(value).is_ok()
}

fn valid_authorization_timestamp(value: &str) -> bool {
    if !well_formed_authorization_timestamp(value) {
        return false;
    }
    let Ok(timestamp) = DateTime::parse_from_rfc3339(value) else {
        return false;
    };
    let elapsed = Utc::now().signed_duration_since(timestamp.with_timezone(&Utc));
    elapsed.num_seconds().abs() <= MAX_AUTHORIZATION_CLOCK_SKEW_SECONDS
}

fn reconciliation_action_wire(action: ReconciliationResolutionAction) -> &'static str {
    match action {
        ReconciliationResolutionAction::Retained => "retained",
        ReconciliationResolutionAction::Revised => "revised",
        ReconciliationResolutionAction::Cancelled => "cancelled",
        ReconciliationResolutionAction::Superseded => "superseded",
        ReconciliationResolutionAction::Invalidated => "invalidated",
    }
}

fn reconciliation_action_from_wire(value: &str) -> Result<ReconciliationResolutionAction> {
    Ok(match value {
        "retained" => ReconciliationResolutionAction::Retained,
        "revised" => ReconciliationResolutionAction::Revised,
        "cancelled" => ReconciliationResolutionAction::Cancelled,
        "superseded" => ReconciliationResolutionAction::Superseded,
        "invalidated" => ReconciliationResolutionAction::Invalidated,
        other => {
            return Err(ServiceError::invalid_operation(format!(
                "reconciliation resolution has an unknown action '{other}'"
            )))
        }
    })
}

fn reconciliation_state_from_wire(value: &str) -> Result<ReconciliationState> {
    Ok(match value {
        "required" => ReconciliationState::Required,
        "retained" => ReconciliationState::Retained,
        "revised" => ReconciliationState::Revised,
        "cancelled" => ReconciliationState::Cancelled,
        "superseded" => ReconciliationState::Superseded,
        "invalidated" => ReconciliationState::Invalidated,
        other => {
            return Err(ServiceError::invalid_operation(format!(
                "reconciliation record has an unknown state '{other}'"
            )))
        }
    })
}

fn principal_kind_wire(kind: PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::User => "user",
        PrincipalKind::Agent => "agent",
        PrincipalKind::Worker => "worker",
        PrincipalKind::Reviewer => "reviewer",
        PrincipalKind::Service => "service",
        PrincipalKind::System => "system",
    }
}

fn principal_kind_from_wire(value: &str) -> PrincipalKind {
    match value {
        "user" => PrincipalKind::User,
        "agent" => PrincipalKind::Agent,
        "worker" => PrincipalKind::Worker,
        "reviewer" => PrincipalKind::Reviewer,
        "service" => PrincipalKind::Service,
        _ => PrincipalKind::System,
    }
}

fn encode_cursor(updated_at: &str, id: &str) -> String {
    hex::encode(format!("{updated_at}\0{id}"))
}

fn decode_cursor(value: Option<&str>) -> Result<Option<(String, String)>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let bytes = hex::decode(value)
        .map_err(|_| ServiceError::invalid_operation("invalid reconciliation list cursor"))?;
    let decoded = String::from_utf8(bytes)
        .map_err(|_| ServiceError::invalid_operation("invalid reconciliation list cursor"))?;
    let (updated_at, id) = decoded
        .split_once('\0')
        .ok_or_else(|| ServiceError::invalid_operation("invalid reconciliation list cursor"))?;
    if updated_at.is_empty() || id.is_empty() {
        return Err(ServiceError::invalid_operation(
            "invalid reconciliation list cursor",
        ));
    }
    Ok(Some((updated_at.to_owned(), id.to_owned())))
}
