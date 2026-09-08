//! Typed pricing admission and settlement helpers for chat and inquiry runs.
//!
//! A chat turn/inquiry is a domain operation, but provider calls are still
//! attempt-shaped.  This module creates exactly one immutable selection and
//! invocation immediately before the adapter call and reuses the task ledger's
//! report-to-event estimator for settlement.

use std::{collections::HashMap, time::SystemTime};

use db::{
    now_rfc3339, Agent, AgentChat, AgentChatMessageRepo, AgentChatRepo, AgentChatTurnJob,
    AgentInquiry, AgentProfile, AgentProfileRepo, AgentRepo, CostCoverageReasonCode,
    CreatePricingSelection, CreateUsageInvocation, CredentialHandleRepo,
    PricingAdmissionProvenanceKind, PricingDomainKind, PricingRateSourceKind, PricingSelection,
    PricingSelectionStatus, ProjectRepo, SqliteDb, UsageInvocation, UsageInvocationLifecycle,
    UsageLedgerSettlement, UsageSurface, UsageTelemetryState,
};
use executors::ExecutorKind;
use executors::{UsageCounters, UsageReport, UsageTelemetryState as ExecutorTelemetryState};
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::{Result, ServiceError};

const CHAT_CANDIDATE: &str = "chat";
#[derive(Debug, Clone)]
struct ScopeSnapshot {
    owner_user_id: String,
    project_id: Option<String>,
    project_name: Option<String>,
    surface: UsageSurface,
}

enum AdmissionGuard<'a> {
    ChatTurn {
        id: &'a str,
        expected_version: i64,
        lease_owner: &'a str,
    },
    Inquiry {
        id: &'a str,
        expected_version: i64,
    },
}

fn stable_id(prefix: &str, source_id: &str, candidate: &str, ordinal: i64) -> Result<String> {
    let mut digest = Sha256::new();
    for value in [prefix, source_id, candidate] {
        let length = u64::try_from(value.len())
            .map_err(|_| ServiceError::invalid_operation("usage identity component is too long"))?;
        digest.update(length.to_be_bytes());
        digest.update(value.as_bytes());
    }
    digest.update(ordinal.to_be_bytes());
    Ok(format!("{prefix}:{}", hex::encode(digest.finalize())))
}

fn chat_attempt_ordinal(attempt_count: i64) -> Result<i64> {
    attempt_count
        .checked_sub(1)
        .filter(|value| *value >= 0)
        .ok_or_else(|| ServiceError::invalid_operation("chat attempt count is invalid"))
}

async fn scope_snapshot(
    db: &SqliteDb,
    chat: &AgentChat,
    job: Option<&AgentChatTurnJob>,
) -> Result<ScopeSnapshot> {
    match chat.kind.as_str() {
        "project" => {
            if chat.account_id.is_some() {
                return Err(ServiceError::invalid_operation(
                    "Project Chat unexpectedly has an account scope",
                ));
            }
            let project_id = chat
                .project_id
                .clone()
                .ok_or_else(|| ServiceError::invalid_operation("Project Chat has no project"))?;
            let row = sqlx::query("SELECT owner_id, name FROM project WHERE id = ?")
                .bind(&project_id)
                .fetch_optional(db.pool())
                .await?
                .ok_or_else(|| ServiceError::not_found("project", project_id.clone()))?;
            let owner_user_id: Option<String> = row.try_get("owner_id")?;
            let owner_user_id = owner_user_id
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    ServiceError::invalid_operation("Project Chat project has no account owner")
                })?;
            Ok(ScopeSnapshot {
                owner_user_id,
                project_id: Some(project_id),
                project_name: row.try_get("name")?,
                surface: UsageSurface::ProjectChat,
            })
        }
        "account_main" => {
            if chat.project_id.is_some() {
                return Err(ServiceError::invalid_operation(
                    "Main Chat unexpectedly has a Project scope",
                ));
            }
            let owner_user_id = chat
                .account_id
                .clone()
                .ok_or_else(|| ServiceError::invalid_operation("Main Chat has no account"))?;
            let Some(job) = job else {
                return Ok(ScopeSnapshot {
                    owner_user_id,
                    project_id: None,
                    project_name: None,
                    surface: UsageSurface::MainChat,
                });
            };
            let trigger_id = &job.triggering_message_id;
            // An active Genesis session identifies the Main turn as Genesis
            // work.  A project is attached only after the immutable handoff
            // boundary proves the exact source turn/message belongs to it.
            let active = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                   SELECT 1 FROM product_genesis_session
                   WHERE main_chat_id = ?
                     AND account_id = ?
                     AND lifecycle IN ('discovering', 'ready_for_project')
                )",
            )
            .bind(&chat.id)
            .bind(&owner_user_id)
            .fetch_one(db.pool())
            .await?
                != 0;
            let handed_off_project: Option<String> = sqlx::query_scalar(
                "SELECT g.project_id
                 FROM product_genesis_session g
                 JOIN agent_handoff h ON h.id = g.handoff_id
                 JOIN project p
                   ON p.id = g.project_id
                  AND p.owner_id = g.account_id
                 JOIN project_admission_receipt receipt
                   ON receipt.project_id = g.project_id
                  AND receipt.source_kind = 'genesis_handoff'
                  AND receipt.handoff_id = g.handoff_id
                 JOIN agent_chat target ON target.id = h.target_chat_id
                 JOIN agent_handoff_delivery delivery
                   ON delivery.handoff_id = h.id
                  AND delivery.delivery_sequence = 1
                  AND delivery.status = 'delivered'
                  AND delivery.target_message_id IS h.target_message_id
                  AND delivery.target_turn_job_id IS h.target_turn_job_id
                 WHERE g.main_chat_id = ? AND g.lifecycle = 'handed_off'
                   AND g.project_id IS NOT NULL AND h.target_chat_id = target.id
                   AND g.account_id = ? AND h.source_chat_id = g.main_chat_id
                   AND h.status = 'delivered'
                   AND target.kind = 'project' AND target.project_id = g.project_id
                   AND json_valid(h.source_revisions_json)
                   AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
                   AND (h.source_turn_job_id = ? OR h.source_message_id = ?
                        OR EXISTS (
                            SELECT 1
                            FROM json_each(
                                json_extract(h.source_revisions_json, '$.source.message_ids')
                            )
                            WHERE json_each.value = ?
                        ))
                 ORDER BY h.created_at DESC, h.id DESC LIMIT 1",
            )
            .bind(&chat.id)
            .bind(&owner_user_id)
            .bind(&job.id)
            .bind(trigger_id)
            .bind(trigger_id)
            .fetch_optional(db.pool())
            .await?;
            if !active && handed_off_project.is_none() {
                return Ok(ScopeSnapshot {
                    owner_user_id,
                    project_id: None,
                    project_name: None,
                    surface: UsageSurface::MainChat,
                });
            }
            let (project_name, project_id) = if let Some(project_id) = handed_off_project {
                let project = ProjectRepo::get_by_id(db, &project_id)
                    .await?
                    .ok_or_else(|| ServiceError::not_found("project", project_id.clone()))?;
                if project.owner_id.as_deref() != Some(owner_user_id.as_str()) {
                    return Err(ServiceError::invalid_operation(
                        "Genesis handoff Project is not owned by the Main account",
                    ));
                }
                (Some(project.name), Some(project_id))
            } else {
                (None, None)
            };
            Ok(ScopeSnapshot {
                owner_user_id,
                project_id,
                project_name,
                surface: UsageSurface::GenesisChat,
            })
        }
        _ => Err(ServiceError::invalid_operation(
            "usage admission requires a canonical Agent Chat",
        )),
    }
}

async fn load_agent_profile(
    db: &SqliteDb,
    identity_id: Option<&str>,
    profile_id: Option<&str>,
) -> Result<(Agent, AgentProfile)> {
    let identity_id = identity_id
        .ok_or_else(|| ServiceError::invalid_operation("chat turn has no responder identity"))?;
    let agent = AgentRepo::get_by_id(db, identity_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("agent_identity", identity_id.to_owned()))?;
    let profile_id = profile_id.unwrap_or(&agent.profile_id);
    let profile = AgentProfileRepo::get_profile(db, profile_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("agent_profile", profile_id.to_owned()))?;
    Ok((agent, profile))
}

fn provider_model(profile: &AgentProfile) -> (Option<String>, Option<String>) {
    (profile.provider.clone(), profile.model.clone())
}

/// Check the immutable Profile snapshot before the ledger is admitted.  The
/// runtime adapters repeat these checks at their own boundary, but a queued
/// job must not create an accounting invocation when it is already clear that
/// no provider attempt can be made (for example, a disconnected credential or
/// malformed native configuration).
pub(crate) async fn validate_provider_availability(
    db: &SqliteDb,
    profile: &AgentProfile,
    owner_user_id: &str,
) -> Result<()> {
    match profile.backend_kind.as_str() {
        "native" => {
            let provider = profile
                .provider
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| ServiceError::invalid_operation("Agent profile has no provider"))?;
            if !matches!(
                provider,
                "xai" | "gemini" | "openai" | "openai_compatible" | "openrouter"
            ) {
                return Err(ServiceError::invalid_operation(
                    "native Agent profile provider is unsupported",
                ));
            }
            if profile
                .model
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(ServiceError::invalid_operation(
                    "Agent profile has no model",
                ));
            }
            let config = serde_json::from_str::<serde_json::Value>(&profile.config_json)
                .map_err(|_| ServiceError::invalid_operation("Agent profile config is invalid"))?;
            let base_url = config
                .get("base_url")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    ServiceError::invalid_operation("native Agent profile has no base URL")
                })?;
            url::Url::parse(base_url).map_err(|_| {
                ServiceError::invalid_operation("native Agent profile base URL is invalid")
            })?;
            let credential_ref = profile
                .credential_ref
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    ServiceError::invalid_operation("Agent profile has no credential")
                })?;
            let handle = CredentialHandleRepo::get_credential_handle(db, credential_ref)
                .await?
                .ok_or_else(|| {
                    ServiceError::invalid_operation("referenced provider entry is unavailable")
                })?;
            if handle.owner_user_id != owner_user_id {
                return Err(ServiceError::invalid_operation(
                    "referenced provider entry is not owned by the Agent account",
                ));
            }
            if handle.status != "configured" {
                return Err(ServiceError::invalid_operation(
                    "referenced provider entry is disconnected",
                ));
            }
            if !handle.enabled {
                return Err(ServiceError::invalid_operation(
                    "referenced provider entry is disabled",
                ));
            }
        }
        "cli" => {
            let executor = profile
                .executor_type
                .parse::<ExecutorKind>()
                .map_err(ServiceError::invalid_operation)?;
            if matches!(executor, ExecutorKind::Embedded | ExecutorKind::Shell) {
                return Err(ServiceError::invalid_operation(
                    "selected executor cannot run a legacy CLI Agent Chat turn",
                ));
            }
            let config = serde_json::from_str::<serde_json::Value>(&profile.config_json)
                .map_err(|_| ServiceError::invalid_operation("Agent profile config is invalid"))?;
            if !config.is_object() {
                return Err(ServiceError::invalid_operation(
                    "Agent profile CLI config must be an object",
                ));
            }
        }
        _ => {
            return Err(ServiceError::invalid_operation(
                "selected Agent Chat backend is unsupported",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn create_chat_invocation(
    db: &SqliteDb,
    source_id: &str,
    ordinal: i64,
    domain_kind: PricingDomainKind,
    scope: &ScopeSnapshot,
    agent: &Agent,
    profile: &AgentProfile,
    guard: AdmissionGuard<'_>,
) -> Result<UsageInvocation> {
    let (provider_id, runtime_model) = provider_model(profile);
    let admitted_at = now_rfc3339();
    let provider_entry = profile.credential_ref.clone();
    let daemon_id = profile.daemon_id.clone();
    let selection_id = stable_id("pricing-selection", source_id, CHAT_CANDIDATE, ordinal)?;
    let invocation_id = stable_id("usage-invocation", source_id, CHAT_CANDIDATE, ordinal)?;
    let mut transaction = db::begin_immediate(db.pool()).await?;
    match guard {
        AdmissionGuard::ChatTurn {
            id,
            expected_version,
            lease_owner,
        } => {
            let valid = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM agent_chat_turn_job
                 WHERE id = ? AND version = ? AND status = 'leased'
                   AND lease_owner = ?",
            )
            .bind(id)
            .bind(expected_version)
            .bind(lease_owner)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !valid {
                return Err(db::DbError::VersionConflict.into());
            }
        }
        AdmissionGuard::Inquiry {
            id,
            expected_version,
        } => {
            let valid = sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM agent_inquiry
                 WHERE id = ? AND version = ? AND status = 'running'",
            )
            .bind(id)
            .bind(expected_version)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
            if !valid {
                return Err(db::DbError::VersionConflict.into());
            }
        }
    }
    let resolved = if let Some(runtime_model) = runtime_model.as_deref() {
        db::PricingSubjectRepo::resolve_active_pricing_subject_binding_in_tx(
            db,
            &mut transaction,
            &scope.owner_user_id,
            provider_entry.as_deref(),
            daemon_id.as_deref(),
            Some(profile.executor_type.as_str()),
            runtime_model,
        )
        .await?
    } else {
        None
    };
    let mut selection_reason = runtime_model
        .as_deref()
        .is_none()
        .then_some(CostCoverageReasonCode::MissingModel);
    if selection_reason.is_none() && resolved.is_none() {
        selection_reason = Some(if provider_id.is_none() {
            CostCoverageReasonCode::MissingProvider
        } else {
            CostCoverageReasonCode::MissingBinding
        });
    }
    let (
        subject_id,
        subject_revision_id,
        subject_revision_digest,
        binding_id,
        rate_revision_id,
        catalog_snapshot_id,
        source_kind,
        admitted_provider_id,
        catalog_freshness,
        rate,
    ) = if let Some(resolved) = resolved {
        let subject_id = Some(resolved.subject.id.clone());
        let subject_revision_id = Some(resolved.revision.id.clone());
        let subject_revision_digest = Some(resolved.revision.revision_digest.clone());
        if let Some(binding) = resolved.binding {
            let rate = resolved.rate.ok_or_else(|| {
                ServiceError::invalid_operation("pricing binding resolved without a rate")
            })?;
            let source_kind = binding.source_kind;
            let catalog_freshness = match source_kind {
                PricingRateSourceKind::ManualOverride => Some("not_applicable".to_owned()),
                PricingRateSourceKind::ModelsDevCatalog => Some(
                    crate::pricing_db::catalog_freshness_for_snapshot_in_tx(
                        &mut transaction,
                        rate.catalog_snapshot_id.as_deref().ok_or_else(|| {
                            ServiceError::invalid_operation(
                                "catalog rate has no immutable snapshot",
                            )
                        })?,
                        SystemTime::now(),
                    )
                    .await
                    .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
                    .as_str()
                    .to_owned(),
                ),
            };
            let admitted_provider_id = provider_id
                .clone()
                .or_else(|| binding.catalog_provider_id.clone());
            selection_reason = None;
            (
                subject_id,
                subject_revision_id,
                subject_revision_digest,
                Some(binding.id),
                Some(rate.id.clone()),
                rate.catalog_snapshot_id.clone(),
                Some(source_kind),
                admitted_provider_id,
                catalog_freshness,
                Some(rate),
            )
        } else {
            (
                subject_id,
                subject_revision_id,
                subject_revision_digest,
                None,
                None,
                None,
                None,
                provider_id.clone(),
                None,
                None,
            )
        }
    } else {
        (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            provider_id.clone(),
            None,
            None,
        )
    };
    let selection_status = if rate.is_some() {
        PricingSelectionStatus::Priced
    } else {
        PricingSelectionStatus::Unpriced
    };
    let temporary = PricingSelection {
        id: selection_id.clone(),
        owner_user_id: Some(scope.owner_user_id.clone()),
        project_id: scope.project_id.clone(),
        domain_kind,
        surface: scope.surface,
        source_id: source_id.to_owned(),
        execution_id: None,
        task_id: None,
        candidate_key: Some(CHAT_CANDIDATE.to_owned()),
        attempt_ordinal: ordinal,
        invocation_id: None,
        subject_id: subject_id.clone(),
        subject_revision_id: subject_revision_id.clone(),
        subject_revision_digest: subject_revision_digest.clone(),
        binding_id: binding_id.clone(),
        rate_revision_id: rate_revision_id.clone(),
        catalog_snapshot_id: catalog_snapshot_id.clone(),
        catalog_freshness: catalog_freshness.clone(),
        runtime_model: runtime_model.clone(),
        admitted_provider_id: admitted_provider_id.clone(),
        admitted_model_id: runtime_model.clone(),
        source_kind,
        provenance_kind: PricingAdmissionProvenanceKind::Runtime,
        selection_status,
        selection_reason,
        selection_digest: String::new(),
        selected_at: admitted_at.clone(),
        created_at: admitted_at.clone(),
    };
    let frozen = crate::task_service::execution::ledger::frozen_selection(
        &temporary,
        rate.as_ref(),
        selection_reason,
    )?;
    let selection_digest = frozen
        .validated_selection_digest()
        .map(str::to_owned)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    let selection = db::UsageLedgerRepo::create_pricing_selection_in_tx(
        db,
        &mut transaction,
        CreatePricingSelection {
            id: selection_id,
            owner_user_id: Some(scope.owner_user_id.clone()),
            project_id: scope.project_id.clone(),
            domain_kind,
            surface: scope.surface,
            source_id: source_id.to_owned(),
            execution_id: None,
            task_id: None,
            candidate_key: Some(CHAT_CANDIDATE.to_owned()),
            attempt_ordinal: ordinal,
            subject_id,
            subject_revision_id,
            subject_revision_digest,
            binding_id,
            rate_revision_id,
            catalog_snapshot_id,
            catalog_freshness,
            runtime_model: runtime_model.clone(),
            admitted_provider_id: admitted_provider_id.clone(),
            admitted_model_id: runtime_model.clone(),
            source_kind,
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            selection_status,
            selection_reason,
            selection_digest,
            selected_at: admitted_at.clone(),
            created_at: admitted_at.clone(),
        },
    )
    .await?;
    let invocation = db::UsageLedgerRepo::create_usage_invocation_in_tx(
        db,
        &mut transaction,
        CreateUsageInvocation {
            id: invocation_id.clone(),
            owner_user_id: selection.owner_user_id.clone(),
            project_id: selection.project_id.clone(),
            domain_kind,
            surface: selection.surface,
            source_id: source_id.to_owned(),
            execution_id: None,
            task_id: None,
            domain_idempotency_key: format!("chat-provider-call:{invocation_id}"),
            candidate_key: Some(CHAT_CANDIDATE.to_owned()),
            attempt_ordinal: ordinal,
            pricing_selection_id: selection.id.clone(),
            admitted_provider_id: selection.admitted_provider_id.clone(),
            admitted_model_id: selection.admitted_model_id.clone(),
            admitted_runtime_model: selection.runtime_model.clone(),
            pricing_subject_id: selection.subject_id.clone(),
            pricing_subject_revision_id: selection.subject_revision_id.clone(),
            subject_revision_digest: selection.subject_revision_digest.clone(),
            agent_id: Some(agent.id.clone()),
            profile_id: Some(profile.id.clone()),
            agent_name_snapshot: Some(agent.name.clone()),
            project_name_snapshot: scope.project_name.clone(),
            executor_type: Some(profile.executor_type.clone()),
            backend_kind: Some(profile.backend_kind.clone()),
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            admitted_at: admitted_at.clone(),
            created_at: admitted_at.clone(),
            updated_at: admitted_at.clone(),
        },
    )
    .await?;
    let invocation = match invocation.lifecycle {
        UsageInvocationLifecycle::Admitted => {
            db::UsageLedgerRepo::start_usage_invocation_in_tx(
                db,
                &mut transaction,
                db::StartUsageInvocation {
                    id: invocation.id.clone(),
                    expected_version: invocation.version,
                    started_at: admitted_at.clone(),
                    updated_at: admitted_at,
                },
            )
            .await?
        }
        UsageInvocationLifecycle::Started => invocation,
        UsageInvocationLifecycle::PendingSettlement
        | UsageInvocationLifecycle::Settled
        | UsageInvocationLifecycle::Unsettled => invocation,
    };
    transaction.commit().await?;
    Ok(invocation)
}

pub(crate) async fn admit_chat_usage(
    db: &SqliteDb,
    job: &AgentChatTurnJob,
) -> Result<UsageInvocation> {
    if job.canonical_scope_type != "agent_chat" || job.canonical_scope_id != job.chat_id {
        return Err(ServiceError::invalid_operation(
            "Agent Chat usage admission has a mismatched canonical scope",
        ));
    }
    let chat = AgentChatRepo::get_agent_chat(db, &job.chat_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("agent_chat", job.chat_id.clone()))?;
    let input = AgentChatMessageRepo::get_agent_chat_message(db, &job.triggering_message_id)
        .await?
        .filter(|message| message.chat_id == chat.id)
        .ok_or_else(|| {
            ServiceError::not_found("agent_chat_message", job.triggering_message_id.clone())
        })?;
    if input.chat_id != job.chat_id {
        return Err(ServiceError::invalid_operation(
            "Agent Chat usage admission trigger belongs to another chat",
        ));
    }
    let scope = scope_snapshot(db, &chat, Some(job)).await?;
    let (agent, profile) = load_agent_profile(
        db,
        job.responder_identity_id.as_deref(),
        job.profile_id.as_deref(),
    )
    .await?;
    if agent.owner_id.as_deref() != Some(scope.owner_user_id.as_str())
        || profile.identity_id != agent.id
        || job.responder_identity_id.as_deref() != Some(agent.id.as_str())
        || job.profile_id.as_deref() != Some(profile.id.as_str())
    {
        return Err(ServiceError::invalid_operation(
            "Agent Chat responder is not owned by the immutable chat scope",
        ));
    }
    create_chat_invocation(
        db,
        &job.id,
        chat_attempt_ordinal(job.attempt_count)?,
        PricingDomainKind::Chat,
        &scope,
        &agent,
        &profile,
        AdmissionGuard::ChatTurn {
            id: &job.id,
            expected_version: job.version,
            lease_owner: job.lease_owner.as_deref().ok_or_else(|| {
                ServiceError::invalid_operation("Agent Chat job has no active lease")
            })?,
        },
    )
    .await
}

pub(crate) async fn admit_inquiry_usage(
    db: &SqliteDb,
    inquiry: &AgentInquiry,
    profile: &AgentProfile,
) -> Result<UsageInvocation> {
    let chat = AgentChatRepo::get_agent_chat(db, &inquiry.chat_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("agent_chat", inquiry.chat_id.clone()))?;
    let scope = ScopeSnapshot {
        owner_user_id: inquiry.owner_user_id.clone(),
        project_id: None,
        project_name: None,
        surface: UsageSurface::MainInquiry,
    };
    if chat.kind != "account_main"
        || chat.project_id.is_some()
        || chat.account_id.as_deref() != Some(inquiry.owner_user_id.as_str())
    {
        return Err(ServiceError::invalid_operation(
            "Main Inquiry must be owned by the account Main Chat",
        ));
    }
    let agent = AgentRepo::get_by_id(db, &inquiry.identity_id)
        .await?
        .ok_or_else(|| ServiceError::not_found("agent_identity", inquiry.identity_id.clone()))?;
    if profile.identity_id != agent.id {
        return Err(ServiceError::invalid_operation(
            "Main Inquiry profile does not belong to its identity",
        ));
    }
    if agent.owner_id.as_deref() != Some(inquiry.owner_user_id.as_str())
        || profile.identity_id != agent.id
    {
        return Err(ServiceError::invalid_operation(
            "Main Inquiry responder is not owned by the account",
        ));
    }
    validate_provider_availability(db, profile, &inquiry.owner_user_id).await?;
    create_chat_invocation(
        db,
        &inquiry.id,
        0,
        PricingDomainKind::Inquiry,
        &scope,
        &agent,
        profile,
        AdmissionGuard::Inquiry {
            id: &inquiry.id,
            expected_version: inquiry.version,
        },
    )
    .await
}

pub(crate) fn usage_report_from_host(
    report: &forge_agent_host::AgentTurnUsageReport,
    candidate_key: &str,
    attempt_ordinal: i64,
    sequence: u32,
) -> Result<UsageReport> {
    let telemetry_state = match report.telemetry_state {
        forge_agent_host::AgentTurnTelemetryState::Metered => ExecutorTelemetryState::Metered,
        forge_agent_host::AgentTurnTelemetryState::Unmetered => ExecutorTelemetryState::Unmetered,
    };
    Ok(UsageReport {
        report_id: report.report_id.clone(),
        request_id: report.request_id.clone(),
        report_sequence: sequence,
        candidate_key: Some(candidate_key.to_owned()),
        attempt_ordinal: u32::try_from(attempt_ordinal)
            .map_err(|_| ServiceError::invalid_operation("usage attempt ordinal overflows"))?,
        provider_id: report.provider_id.clone(),
        model_id: report.model_id.clone(),
        counters: UsageCounters {
            input_tokens: report.input_tokens,
            output_tokens: report.output_tokens,
            cache_read_tokens: report.cache_read_tokens,
            cache_write_tokens: report.cache_write_tokens,
        },
        telemetry_state,
        context_tokens: None,
        selected_tier: None,
        reported_cost_usd: None,
        outcome: None,
        partial: report.failed,
    })
}

pub(crate) async fn build_chat_usage_settlements(
    db: &SqliteDb,
    source_id: &str,
    reports: &[UsageReport],
    now: &str,
) -> Result<Vec<UsageLedgerSettlement>> {
    let invocations = db::UsageLedgerRepo::list_usage_invocations_for_source(db, source_id).await?;
    let selections = db::UsageLedgerRepo::list_pricing_selections_for_source(db, source_id).await?;
    let selections_by_id: HashMap<String, PricingSelection> = selections
        .into_iter()
        .map(|selection| (selection.id.clone(), selection))
        .collect();
    let mut settlements = Vec::new();
    for invocation in invocations {
        if !matches!(
            invocation.lifecycle,
            UsageInvocationLifecycle::Started | UsageInvocationLifecycle::PendingSettlement
        ) {
            continue;
        }
        let selection = selections_by_id
            .get(&invocation.pricing_selection_id)
            .ok_or_else(|| ServiceError::invalid_operation("usage selection is missing"))?;
        // Validate the immutable admission provenance even when no report was
        // observed.  Unmetered settlement must not turn a corrupted pricing
        // selection into an apparently clean terminal invocation.
        crate::task_service::execution::ledger::validate_persisted_selection_digest(db, selection)
            .await?;
        let matching = reports.iter().filter(|report| {
            i64::from(report.attempt_ordinal) == invocation.attempt_ordinal
                && report.candidate_key.as_deref() == invocation.candidate_key.as_deref()
        });
        let mut events = Vec::new();
        let mut metered = false;
        for report in matching {
            if report.telemetry_state == ExecutorTelemetryState::Metered
                && report.counters.has_any()
            {
                metered = true;
            }
            // The shared task converter stores only an opaque digest of this
            // identity. Include the transport sequence in the local report
            // identity so a provider that reuses a report id for successive
            // deltas cannot collide with the ledger's per-invocation unique
            // source-report key.
            let mut event_report = report.clone();
            event_report.report_id =
                format!("{}:sequence:{}", report.report_id, report.report_sequence);
            if let Some(event) = crate::task_service::execution::ledger::usage_event_for_report(
                db,
                &invocation,
                selection,
                &event_report,
                now,
            )
            .await?
            {
                events.push(event);
            }
        }
        settlements.push(UsageLedgerSettlement {
            invocation_id: invocation.id,
            expected_version: invocation.version,
            telemetry_state: if metered {
                UsageTelemetryState::Metered
            } else {
                UsageTelemetryState::Unmetered
            },
            terminal_reason: if metered {
                None
            } else {
                Some("unmetered".to_owned())
            },
            settled_at: now.to_owned(),
            updated_at: now.to_owned(),
            events,
        });
    }
    Ok(settlements)
}

pub(crate) async fn settle_late_chat_usage(
    db: &SqliteDb,
    source_id: &str,
    reports: &[UsageReport],
    now: &str,
) -> Result<()> {
    let settlements = build_chat_usage_settlements(db, source_id, reports, now).await?;
    if !settlements.is_empty() {
        db::UsageLedgerRepo::settle_usage_invocations_with_events(db, settlements).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_usage_ids_length_prefix_data_components() {
        let left = stable_id("usage", "a|b", "c", 0).expect("stable id");
        let right = stable_id("usage", "a", "b|c", 0).expect("stable id");
        assert_ne!(left, right);
    }

    #[test]
    fn invalid_attempt_ordinals_are_rejected_without_numeric_clamping() {
        assert!(chat_attempt_ordinal(i64::MIN).is_err());
        let report = forge_agent_host::AgentTurnUsageReport {
            report_id: "provider-report".to_owned(),
            request_id: None,
            attempt_id: None,
            provider_id: None,
            model_id: None,
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
            telemetry_state: forge_agent_host::AgentTurnTelemetryState::Unmetered,
            failed: false,
        };
        assert!(usage_report_from_host(&report, CHAT_CANDIDATE, -1, 0).is_err());
    }
}
