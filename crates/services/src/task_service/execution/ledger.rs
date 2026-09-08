//! Task-execution pricing admission and usage-ledger settlement.
//!
//! This module is deliberately kept beside the runner.  It translates the
//! executor's neutral per-candidate reports into immutable DB payloads and
//! leaves all multi-row durability to the DB transaction boundary.

use super::super::*;
use async_trait::async_trait;
use db::{
    CostCoverageReasonCode, CreatePricingSelection, CreateUsageEvent, CreateUsageInvocation,
    Execution, PricingAdmissionProvenanceKind, PricingDomainKind, PricingRateSourceKind,
    PricingSelection, PricingSelectionStatus, SqliteDb, Task, TerminalizeExecutionWithLedger,
    UsageEventProvenanceKind, UsageEventReportMode, UsageInvocation, UsageInvocationLifecycle,
    UsageLedgerSettlement, UsageSurface, UsageTelemetryState,
};
use executors::{
    candidate_key, resolve_config_value, ExecutionContext, ExecutionOverrides, ExecutorKind,
    ProviderCallAdmission, UsageReport,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Sqlite, Transaction};
use std::{collections::HashMap, sync::Arc, time::SystemTime};

#[derive(Debug, Clone)]
struct Candidate {
    candidate_key: String,
    attempt_ordinal: i64,
    executor_type: ExecutorKind,
    config: Value,
    provider_id: Option<String>,
    model_id: Option<String>,
    runtime_model: Option<String>,
}

#[derive(Debug, Clone)]
struct PriceIdentity {
    subject_id: Option<String>,
    subject_revision_id: Option<String>,
    subject_revision_digest: Option<String>,
    binding_id: Option<String>,
    rate_revision_id: Option<String>,
    catalog_snapshot_id: Option<String>,
    catalog_provider_id: Option<String>,
    catalog_freshness: Option<crate::pricing::CatalogFreshness>,
    /// Immutable rate materialized during admission so the canonical frozen
    /// selection digest can be persisted, rather than a separate envelope
    /// digest that settlement could not authenticate.
    rate: Option<db::PricingRateRevision>,
    source_kind: Option<PricingRateSourceKind>,
    status: PricingSelectionStatus,
    reason: Option<CostCoverageReasonCode>,
}

impl Default for PriceIdentity {
    fn default() -> Self {
        Self {
            subject_id: None,
            subject_revision_id: None,
            subject_revision_digest: None,
            binding_id: None,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_provider_id: None,
            catalog_freshness: None,
            rate: None,
            source_kind: None,
            status: PricingSelectionStatus::Unpriced,
            reason: None,
        }
    }
}

fn stable_ledger_id(prefix: &str, execution_id: &str, candidate_key: &str, ordinal: i64) -> String {
    let mut digest = Sha256::new();
    for value in [prefix, execution_id, candidate_key] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    digest.update(ordinal.to_be_bytes());
    format!("{prefix}:{}", hex::encode(digest.finalize()))
}

/// Provider report identifiers are evidence, not safe persistence keys. A
/// provider may accidentally echo prompt text, credentials, or another
/// unbounded secret in that field, so only this fixed-size opaque digest is
/// written to the ledger. Invocation identity and report sequence keep empty
/// or reused provider IDs deterministic within the exact attempt.
fn opaque_source_report_id(invocation_id: &str, report: &UsageReport) -> String {
    let mut digest = Sha256::new();
    let domain = "forge:usage-report:v1";
    digest.update((domain.len() as u64).to_be_bytes());
    digest.update(domain.as_bytes());
    let report_identity = if report.report_id.trim().is_empty() {
        format!("sequence:{}", report.report_sequence)
    } else {
        report.report_id.clone()
    };
    for value in [invocation_id, report_identity.as_str()] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    format!("report:{}", hex::encode(digest.finalize()))
}

fn nonempty(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Provider/model identities arrive from an untrusted daemon/provider report.
/// Keep only bounded, single-line identifiers for comparison. Persisted event
/// identity still prefers the immutable admitted/configured values below; a
/// reported value is used only as bounded evidence for the identity check.
const MAX_REPORTED_IDENTITY_CHARS: usize = 128;

fn bounded_report_identity(value: Option<&str>) -> (Option<String>, bool) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return (None, false);
    };
    if value.chars().count() > MAX_REPORTED_IDENTITY_CHARS
        || !value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(character, '-' | '_' | '.' | '/' | ':' | '@' | '+')
        })
    {
        return (None, true);
    }
    (Some(value.to_owned()), false)
}

fn candidates_from_snapshot(snapshot: &Value) -> Result<Vec<Candidate>> {
    let executor_type = snapshot
        .get("executor_type")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ServiceError::invalid_operation("executor config snapshot missing executor_type")
        })?
        .parse::<ExecutorKind>()
        .map_err(ServiceError::invalid_operation)?;
    // Identity must be derived exactly as the runtime derives it, or the
    // frozen selection can never be matched at the provider-call boundary.
    let primary_config = executors::candidate_config_from_snapshot(&executor_type, snapshot);
    let primary_provider =
        nonempty(primary_config.get("provider")).or_else(|| nonempty(snapshot.get("provider")));
    let primary_model =
        nonempty(primary_config.get("model")).or_else(|| nonempty(snapshot.get("model")));
    let primary_key = candidate_key(&executor_type, &primary_config);
    let mut candidates = vec![Candidate {
        candidate_key: primary_key,
        attempt_ordinal: 0,
        executor_type,
        config: primary_config,
        provider_id: primary_provider,
        model_id: primary_model.clone(),
        runtime_model: primary_model,
    }];

    if let Some(routing) = snapshot.get(executors::ROUTING_SNAPSHOT_KEY) {
        let routing: executors::ExecutorRouting =
            serde_json::from_value(routing.clone()).map_err(|error| {
                ServiceError::invalid_operation(format!("invalid routing block: {error}"))
            })?;
        if routing.policy != executors::ROUTING_POLICY_ORDERED_FALLBACK_V1 {
            return Err(ServiceError::invalid_operation(format!(
                "unknown routing policy: {}",
                routing.policy
            )));
        }
        for routed in routing.candidates {
            let config = resolve_config_value(
                routed.executor_type.clone(),
                &routed.config,
                &ExecutionOverrides::default(),
            )?;
            let key = candidate_key(&routed.executor_type, &config);
            if candidates
                .iter()
                .any(|candidate| candidate.candidate_key == key)
            {
                continue;
            }
            let provider_id = nonempty(config.get("provider"));
            let model_id = nonempty(config.get("model"));
            let ordinal = i64::try_from(candidates.len()).map_err(|_| {
                ServiceError::invalid_operation("routing candidate ordinal overflows")
            })?;
            candidates.push(Candidate {
                candidate_key: key,
                attempt_ordinal: ordinal,
                executor_type: routed.executor_type,
                config,
                provider_id,
                model_id: model_id.clone(),
                runtime_model: model_id,
            });
        }
    }
    Ok(candidates)
}

fn subject_ref(
    candidate: &Candidate,
    snapshot: &Value,
    agent: &db::Agent,
) -> (Option<String>, Option<String>) {
    let provider_entry = nonempty(candidate.config.get("credential_ref"))
        .or_else(|| nonempty(candidate.config.get("provider_entry_id")))
        .or_else(|| nonempty(snapshot.get("credential_ref")))
        .or_else(|| agent.credential_ref.clone());
    let daemon_id = nonempty(candidate.config.get("daemon_id"))
        .or_else(|| nonempty(snapshot.get("resolved_daemon_id")))
        .or_else(|| agent.daemon_id.clone());
    (provider_entry, daemon_id)
}

async fn resolve_price_identity(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    owner_user_id: &str,
    candidate: &Candidate,
    snapshot: &Value,
    agent: &db::Agent,
    admitted_at: &str,
) -> Result<PriceIdentity> {
    let Some(runtime_model) = candidate.runtime_model.as_deref() else {
        return Ok(PriceIdentity {
            status: PricingSelectionStatus::Unpriced,
            reason: Some(CostCoverageReasonCode::MissingModel),
            ..Default::default()
        });
    };
    let (provider_entry, daemon_id) = subject_ref(candidate, snapshot, agent);
    let executor_type = candidate.executor_type.to_string();
    let mut resolved = db::PricingSubjectRepo::resolve_active_pricing_subject_binding_in_tx(
        db,
        tx,
        owner_user_id,
        provider_entry.as_deref(),
        daemon_id.as_deref(),
        Some(executor_type.as_str()),
        runtime_model,
    )
    .await?;
    // Reviewed provider identities receive their initial exact catalog alias
    // at admission time, after the subject/revision has been resolved.  The
    // helper is deliberately bounded to the four built-in provider endpoint
    // identities; custom, subscription, and CLI subjects remain explicit.
    let current_subject_id = resolved.as_ref().map(|current| current.subject.id.clone());
    if let Some(current_subject_id) = current_subject_id {
        let admitted_at_time = chrono::DateTime::parse_from_rfc3339(admitted_at)
            .map(|value| value.with_timezone(&chrono::Utc).into())
            .unwrap_or_else(|_| SystemTime::now());
        if crate::pricing_db::ensure_reviewed_provider_binding_in_tx(
            db,
            tx,
            &current_subject_id,
            runtime_model,
            admitted_at_time,
        )
        .await
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?
        {
            resolved = db::PricingSubjectRepo::resolve_active_pricing_subject_binding_in_tx(
                db,
                tx,
                owner_user_id,
                provider_entry.as_deref(),
                daemon_id.as_deref(),
                Some(executor_type.as_str()),
                runtime_model,
            )
            .await?;
        }
    }
    let Some(resolved) = resolved else {
        return Ok(PriceIdentity {
            status: PricingSelectionStatus::Unpriced,
            reason: Some(if candidate.provider_id.is_none() {
                CostCoverageReasonCode::MissingProvider
            } else {
                CostCoverageReasonCode::MissingBinding
            }),
            ..Default::default()
        });
    };

    let subject_id = Some(resolved.subject.id.clone());
    let subject_revision_id = Some(resolved.revision.id.clone());
    let subject_revision_digest = Some(resolved.revision.revision_digest.clone());
    let Some(binding) = resolved.binding else {
        return Ok(PriceIdentity {
            subject_id,
            subject_revision_id,
            subject_revision_digest,
            status: PricingSelectionStatus::Unpriced,
            reason: Some(CostCoverageReasonCode::MissingBinding),
            ..Default::default()
        });
    };
    let Some(rate) = resolved.rate else {
        return Err(ServiceError::invalid_operation(
            "pricing binding resolved without a rate revision",
        ));
    };
    let catalog_provider_id = match binding.source_kind {
        PricingRateSourceKind::ModelsDevCatalog => binding.catalog_provider_id.clone(),
        PricingRateSourceKind::ManualOverride => None,
    };
    // The candidate provider is runtime evidence, while the catalog provider
    // is canonical source provenance. A relay/openai-compatible subject may
    // intentionally use an exact `openai` catalog row, so admission does not
    // compare those labels. Settlement compares trustworthy actual provider
    // evidence with the admitted runtime identity and only then rejects a
    // contradiction as `identity_mismatch`.
    let catalog_freshness = match binding.source_kind {
        PricingRateSourceKind::ManualOverride => {
            Some(crate::pricing::CatalogFreshness::NotApplicable)
        }
        PricingRateSourceKind::ModelsDevCatalog => {
            let snapshot_id = rate.catalog_snapshot_id.as_deref().ok_or_else(|| {
                ServiceError::invalid_operation("catalog pricing binding has no immutable snapshot")
            })?;
            Some(
                crate::pricing_db::catalog_freshness_for_snapshot_in_tx(
                    tx,
                    snapshot_id,
                    chrono::DateTime::parse_from_rfc3339(admitted_at)
                        .map(|value| value.with_timezone(&chrono::Utc).into())
                        .unwrap_or_else(|_| SystemTime::now()),
                )
                .await
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
            )
        }
    };
    Ok(PriceIdentity {
        subject_id,
        subject_revision_id,
        subject_revision_digest,
        binding_id: Some(binding.id),
        rate_revision_id: Some(rate.id.clone()),
        catalog_snapshot_id: rate.catalog_snapshot_id.clone(),
        catalog_provider_id,
        catalog_freshness,
        source_kind: Some(binding.source_kind),
        status: PricingSelectionStatus::Priced,
        reason: None,
        rate: Some(rate),
    })
}

fn selection_digest(
    execution_id: &str,
    candidate: &Candidate,
    owner_user_id: Option<&str>,
    project_id: &str,
    identity: &PriceIdentity,
) -> Result<String> {
    let persisted_admitted_provider_id = if identity.status == PricingSelectionStatus::Priced
        && identity.source_kind == Some(PricingRateSourceKind::ModelsDevCatalog)
    {
        candidate
            .provider_id
            .clone()
            .or_else(|| identity.catalog_provider_id.clone())
    } else {
        candidate.provider_id.clone()
    };
    let selection = PricingSelection {
        id: stable_ledger_id(
            "pricing-selection",
            execution_id,
            &candidate.candidate_key,
            candidate.attempt_ordinal,
        ),
        owner_user_id: owner_user_id.map(str::to_owned),
        project_id: Some(project_id.to_owned()),
        domain_kind: PricingDomainKind::Execution,
        surface: UsageSurface::TaskExecution,
        source_id: execution_id.to_owned(),
        execution_id: Some(execution_id.to_owned()),
        task_id: None,
        candidate_key: Some(candidate.candidate_key.clone()),
        attempt_ordinal: candidate.attempt_ordinal,
        invocation_id: None,
        subject_id: identity.subject_id.clone(),
        subject_revision_id: identity.subject_revision_id.clone(),
        subject_revision_digest: identity.subject_revision_digest.clone(),
        binding_id: identity.binding_id.clone(),
        rate_revision_id: identity.rate_revision_id.clone(),
        catalog_snapshot_id: identity.catalog_snapshot_id.clone(),
        catalog_freshness: identity
            .catalog_freshness
            .map(|value| value.as_str().to_owned()),
        runtime_model: candidate.runtime_model.clone(),
        admitted_provider_id: persisted_admitted_provider_id,
        admitted_model_id: candidate.model_id.clone(),
        source_kind: identity.source_kind,
        provenance_kind: PricingAdmissionProvenanceKind::Runtime,
        selection_status: identity.status,
        selection_reason: identity.reason,
        selection_digest: String::new(),
        selected_at: String::new(),
        created_at: String::new(),
    };
    let frozen = frozen_selection(
        &selection,
        (identity.status == PricingSelectionStatus::Priced)
            .then_some(identity.rate.as_ref())
            .flatten(),
        identity.reason,
    )?;
    frozen
        .validated_selection_digest()
        .map(str::to_owned)
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

/// Same admission operation with the concrete DB handle required by the
/// repository trait.
pub(crate) async fn admit_task_execution_in_tx_with_db(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    task: &Task,
    execution: &Execution,
    agent: &db::Agent,
    snapshot: &Value,
) -> Result<()> {
    // Keep the implementation single-sourced while avoiding a DB handle in
    // every pure candidate helper.
    let owner_candidate: Option<String> =
        sqlx::query_scalar("SELECT COALESCE(owner_id, ?) FROM project WHERE id = ?")
            .bind(agent.owner_id.as_deref())
            .bind(&task.project_id)
            .fetch_optional(&mut **tx)
            .await?;
    let owner_candidate = owner_candidate.filter(|value| !value.trim().is_empty());
    let owner_user_id = match owner_candidate {
        Some(owner_user_id) => {
            sqlx::query_scalar::<_, String>("SELECT id FROM user WHERE id = ?")
                .bind(owner_user_id)
                .fetch_optional(&mut **tx)
                .await?
        }
        None => None,
    };
    // A legacy Project can have no surviving account principal. Without an
    // owner there is no valid pricing authority or account scope, so preserve
    // execution behavior and leave the run visible through the domain-run
    // denominator instead of minting an ownerless runtime ledger row.
    let Some(owner_user_id) = owner_user_id else {
        return Ok(());
    };
    if sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pricing_selection WHERE source_id = ? AND domain_kind = 'execution'",
    )
    .bind(&execution.id)
    .fetch_one(&mut **tx)
    .await?
        > 0
    {
        return Ok(());
    }
    let admitted_at = db::now_rfc3339();
    for candidate in candidates_from_snapshot(snapshot)? {
        let identity = resolve_price_identity(
            db,
            tx,
            &owner_user_id,
            &candidate,
            snapshot,
            agent,
            &admitted_at,
        )
        .await?;
        let digest = selection_digest(
            &execution.id,
            &candidate,
            Some(&owner_user_id),
            &task.project_id,
            &identity,
        )?;
        db::UsageLedgerRepo::create_pricing_selection_in_tx(
            db,
            tx,
            CreatePricingSelection {
                id: stable_ledger_id(
                    "pricing-selection",
                    &execution.id,
                    &candidate.candidate_key,
                    candidate.attempt_ordinal,
                ),
                owner_user_id: Some(owner_user_id.clone()),
                project_id: Some(task.project_id.clone()),
                domain_kind: PricingDomainKind::Execution,
                surface: UsageSurface::TaskExecution,
                source_id: execution.id.clone(),
                execution_id: Some(execution.id.clone()),
                task_id: Some(task.id.clone()),
                candidate_key: Some(candidate.candidate_key.clone()),
                attempt_ordinal: candidate.attempt_ordinal,
                subject_id: identity.subject_id,
                subject_revision_id: identity.subject_revision_id,
                subject_revision_digest: identity.subject_revision_digest,
                binding_id: identity.binding_id,
                rate_revision_id: if identity.status == PricingSelectionStatus::Priced {
                    identity.rate_revision_id
                } else {
                    None
                },
                catalog_snapshot_id: if identity.status == PricingSelectionStatus::Priced {
                    identity.catalog_snapshot_id
                } else {
                    None
                },
                catalog_freshness: identity
                    .catalog_freshness
                    .map(|value| value.as_str().to_owned()),
                runtime_model: candidate.runtime_model,
                admitted_provider_id: if identity.status == PricingSelectionStatus::Priced
                    && identity.source_kind == Some(PricingRateSourceKind::ModelsDevCatalog)
                {
                    candidate
                        .provider_id
                        .or_else(|| identity.catalog_provider_id.clone())
                } else {
                    candidate.provider_id
                },
                admitted_model_id: candidate.model_id,
                source_kind: if identity.status == PricingSelectionStatus::Priced {
                    identity.source_kind
                } else {
                    None
                },
                provenance_kind: PricingAdmissionProvenanceKind::Runtime,
                selection_status: identity.status,
                selection_reason: identity.reason,
                selection_digest: digest,
                selected_at: admitted_at.clone(),
                created_at: admitted_at.clone(),
            },
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn ensure_task_execution_admission(
    db: &SqliteDb,
    task: &Task,
    execution: &Execution,
    snapshot: &Value,
) -> Result<bool> {
    let Some(agent_id) = execution.agent_id.as_deref() else {
        return Ok(false);
    };
    let Some(agent) = db::AgentRepo::get_by_id(db, agent_id).await? else {
        return Ok(false);
    };
    let mut tx = db::begin_immediate(db.pool()).await?;
    admit_task_execution_in_tx_with_db(db, &mut tx, task, execution, &agent, snapshot).await?;
    let admitted = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pricing_selection WHERE source_id = ? AND domain_kind = 'execution'",
    )
    .bind(&execution.id)
    .fetch_one(&mut *tx)
    .await?
        > 0;
    tx.commit().await?;
    Ok(admitted)
}

pub(crate) struct TaskProviderCallAdmission {
    db: Arc<SqliteDb>,
    agent_id: Option<String>,
    profile_id: Option<String>,
    executor_type: Option<String>,
    backend_kind: Option<String>,
}

/// Prepare the server-side view of remote provider calls from the daemon's
/// per-attempt terminal reports. Remote calls do not execute inside this
/// process, so the neutral executor hook cannot run on the server before the
/// call. A report is nevertheless authoritative evidence that the daemon
/// crossed that boundary. The returned rows are handed to the composite DB
/// terminalizer and inserted there, never in a separate transaction.
pub(crate) async fn ensure_remote_task_usage_invocations_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    execution: &Execution,
    reports: &[UsageReport],
    snapshot: Option<&Value>,
    now: &str,
) -> Result<Vec<CreateUsageInvocation>> {
    if reports.is_empty() {
        return Ok(Vec::new());
    }
    let selections =
        db::UsageLedgerRepo::list_pricing_selections_for_source_in_tx(db, tx, &execution.id)
            .await?;
    let existing =
        db::UsageLedgerRepo::list_usage_invocations_for_source_in_tx(db, tx, &execution.id).await?;
    ensure_remote_task_usage_invocations_from_rows(
        execution, reports, snapshot, now, selections, existing,
    )
}

fn ensure_remote_task_usage_invocations_from_rows(
    execution: &Execution,
    reports: &[UsageReport],
    snapshot: Option<&Value>,
    now: &str,
    selections: Vec<PricingSelection>,
    existing: Vec<UsageInvocation>,
) -> Result<Vec<CreateUsageInvocation>> {
    let existing_ids: std::collections::HashSet<String> = existing
        .into_iter()
        .map(|invocation| invocation.id)
        .collect();
    let mut materialized = std::collections::HashSet::new();
    let mut prepared = Vec::new();
    for report in reports {
        let selection = selections
            .iter()
            .find(|selection| {
                selection.candidate_key == report.candidate_key
                    && selection.attempt_ordinal == i64::from(report.attempt_ordinal)
            })
            .ok_or_else(|| {
                ServiceError::invalid_operation(format!(
                    "remote usage report has no admitted candidate selection: {}:{}",
                    report.candidate_key.as_deref().unwrap_or("<unknown>"),
                    report.attempt_ordinal
                ))
            })?;
        if !materialized.insert(selection.id.clone()) {
            continue;
        }

        let invocation_id = stable_ledger_id(
            "usage-invocation",
            &execution.id,
            selection.candidate_key.as_deref().unwrap_or("<candidate>"),
            selection.attempt_ordinal,
        );
        if existing_ids.contains(&invocation_id) {
            continue;
        }
        prepared.push(CreateUsageInvocation {
            id: invocation_id.clone(),
            owner_user_id: selection.owner_user_id.clone(),
            project_id: selection.project_id.clone(),
            domain_kind: PricingDomainKind::Execution,
            surface: UsageSurface::TaskExecution,
            source_id: execution.id.clone(),
            execution_id: Some(execution.id.clone()),
            task_id: selection
                .task_id
                .clone()
                .or_else(|| Some(execution.task_id.clone())),
            domain_idempotency_key: format!("task-provider-call:{invocation_id}"),
            candidate_key: selection.candidate_key.clone(),
            attempt_ordinal: selection.attempt_ordinal,
            pricing_selection_id: selection.id.clone(),
            admitted_provider_id: selection.admitted_provider_id.clone(),
            admitted_model_id: selection.admitted_model_id.clone(),
            admitted_runtime_model: selection.runtime_model.clone(),
            pricing_subject_id: selection.subject_id.clone(),
            pricing_subject_revision_id: selection.subject_revision_id.clone(),
            subject_revision_digest: selection.subject_revision_digest.clone(),
            agent_id: execution.agent_id.clone(),
            profile_id: snapshot
                .and_then(|value| value.get("profile_id"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: snapshot
                .and_then(|value| value.get("executor_type"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            backend_kind: snapshot
                .and_then(|value| value.get("backend_kind"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            admitted_at: selection.selected_at.clone(),
            // Invocation provenance is frozen at admission.  The remote
            // terminal path can be retried with a different receipt arrival
            // time, so do not make the create payload depend on that time.
            created_at: selection.created_at.clone(),
            updated_at: now.to_owned(),
        });
    }
    Ok(prepared)
}

impl TaskProviderCallAdmission {
    pub(crate) fn new(db: Arc<SqliteDb>, snapshot: &Value, execution: &Execution) -> Self {
        Self {
            db,
            agent_id: execution.agent_id.clone(),
            profile_id: snapshot
                .get("profile_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            executor_type: snapshot
                .get("executor_type")
                .and_then(Value::as_str)
                .map(str::to_owned),
            backend_kind: snapshot
                .get("backend_kind")
                .and_then(Value::as_str)
                .map(str::to_owned),
        }
    }
}

#[async_trait]
impl ProviderCallAdmission for TaskProviderCallAdmission {
    async fn before_provider_call(
        &self,
        ctx: &ExecutionContext,
        candidate_key: &str,
        attempt_ordinal: u32,
    ) -> std::result::Result<(), executors::ExecutorError> {
        let selections =
            db::UsageLedgerRepo::list_pricing_selections_for_source(&*self.db, &ctx.execution_id)
                .await
                .map_err(|error| executors::ExecutorError::Other(error.to_string()))?;
        let selection = selections
            .iter()
            .find(|selection| {
                selection.candidate_key.as_deref() == Some(candidate_key)
                    && selection.attempt_ordinal == i64::from(attempt_ordinal)
            })
            .cloned()
            .ok_or_else(|| {
                executors::ExecutorError::Other(format!(
                    "no immutable pricing selection for candidate {candidate_key}"
                ))
            })?;
        validate_persisted_selection_digest(&self.db, &selection)
            .await
            .map_err(|error| executors::ExecutorError::Other(error.to_string()))?;
        let admitted_candidate_key = selection
            .candidate_key
            .clone()
            .unwrap_or_else(|| candidate_key.to_owned());
        let invocation_id = stable_ledger_id(
            "usage-invocation",
            &ctx.execution_id,
            &admitted_candidate_key,
            i64::from(attempt_ordinal),
        );
        let now = db::now_rfc3339();
        if let Some(existing) = db::UsageLedgerRepo::get_usage_invocation(&*self.db, &invocation_id)
            .await
            .map_err(|error| executors::ExecutorError::Other(error.to_string()))?
        {
            match existing.lifecycle {
                UsageInvocationLifecycle::Started => return Ok(()),
                // Finish the admission under the same BEGIN IMMEDIATE below;
                // this closes the race where another dispatcher inserted the
                // durable row between our lookup and start boundary.
                UsageInvocationLifecycle::Admitted => {}
                _ => {
                    return Err(executors::ExecutorError::Other(
                        "provider invocation identity was already terminalized".to_owned(),
                    ));
                }
            }
        }
        let mut tx = db::begin_immediate(self.db.pool())
            .await
            .map_err(|error| executors::ExecutorError::Other(error.to_string()))?;
        let invocation = db::UsageLedgerRepo::create_usage_invocation_in_tx(
            &*self.db,
            &mut tx,
            CreateUsageInvocation {
                id: invocation_id.clone(),
                owner_user_id: selection.owner_user_id.clone(),
                project_id: selection.project_id.clone(),
                domain_kind: PricingDomainKind::Execution,
                surface: UsageSurface::TaskExecution,
                source_id: ctx.execution_id.clone(),
                execution_id: Some(ctx.execution_id.clone()),
                task_id: selection.task_id.clone(),
                domain_idempotency_key: format!("task-provider-call:{invocation_id}"),
                candidate_key: selection.candidate_key.clone(),
                attempt_ordinal: selection.attempt_ordinal,
                pricing_selection_id: selection.id.clone(),
                admitted_provider_id: selection.admitted_provider_id.clone(),
                admitted_model_id: selection.admitted_model_id.clone(),
                admitted_runtime_model: selection.runtime_model.clone(),
                pricing_subject_id: selection.subject_id.clone(),
                pricing_subject_revision_id: selection.subject_revision_id.clone(),
                subject_revision_digest: selection.subject_revision_digest.clone(),
                agent_id: self.agent_id.clone(),
                profile_id: self.profile_id.clone(),
                agent_name_snapshot: None,
                project_name_snapshot: None,
                executor_type: self.executor_type.clone(),
                backend_kind: self.backend_kind.clone(),
                provenance_kind: PricingAdmissionProvenanceKind::Runtime,
                admitted_at: selection.selected_at.clone(),
                // The selection row is the immutable admission record.  Reusing
                // its creation timestamp makes a retry after a partially
                // successful persistence attempt byte-for-byte identical; a
                // fresh wall-clock value here would turn the same invocation key
                // into an IdempotencyConflict and (incorrectly) block the
                // provider call on retry.
                created_at: selection.created_at.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .map_err(|error| executors::ExecutorError::Other(error.to_string()))?;
        match invocation.lifecycle {
            UsageInvocationLifecycle::Admitted => {
                db::UsageLedgerRepo::start_usage_invocation_in_tx(
                    &*self.db,
                    &mut tx,
                    db::StartUsageInvocation {
                        id: invocation.id,
                        expected_version: invocation.version,
                        started_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .map_err(|error| executors::ExecutorError::Other(error.to_string()))?;
            }
            UsageInvocationLifecycle::Started => {}
            _ => {
                return Err(executors::ExecutorError::Other(
                    "provider invocation identity was already terminalized".to_owned(),
                ));
            }
        }
        tx.commit()
            .await
            .map_err(|error| executors::ExecutorError::Other(error.to_string()))?;
        Ok(())
    }
}

fn parse_reported_nano_usd(value: &str) -> Result<i64> {
    if value.is_empty() || value.trim() != value {
        return Err(ServiceError::invalid_operation(
            "reported USD amount must be a strict decimal",
        ));
    }
    if value.starts_with(['+', '-']) || value.contains(['e', 'E']) {
        return Err(ServiceError::invalid_operation(
            "reported USD amount must be non-negative decimal text",
        ));
    }
    let mut split = value.split('.');
    let whole = split.next().unwrap_or_default();
    let fraction = split.next().unwrap_or_default();
    if split.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || (value.contains('.') && fraction.is_empty())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > 9
    {
        return Err(ServiceError::invalid_operation(
            "reported USD amount must have at most nine fractional digits",
        ));
    }
    let whole = whole
        .parse::<i128>()
        .map_err(|_| ServiceError::invalid_operation("reported USD amount overflows"))?;
    let fraction_value =
        if fraction.is_empty() {
            0_i128
        } else {
            fraction
                .parse::<i128>()
                .map_err(|_| ServiceError::invalid_operation("reported USD amount overflows"))?
                * 10_i128.pow(u32::try_from(9 - fraction.len()).map_err(|_| {
                    ServiceError::invalid_operation("reported USD fraction is invalid")
                })?)
        };
    let nanos = whole
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(fraction_value))
        .filter(|value| *value <= i128::from(i64::MAX))
        .ok_or_else(|| ServiceError::invalid_operation("reported USD amount overflows"))?;
    i64::try_from(nanos)
        .map_err(|_| ServiceError::invalid_operation("reported USD amount overflows"))
}

fn db_reason_to_service(
    reason: Option<CostCoverageReasonCode>,
) -> crate::pricing::PriceSelectionReasonCode {
    match reason.unwrap_or(CostCoverageReasonCode::MissingBinding) {
        CostCoverageReasonCode::MissingProvider => {
            crate::pricing::PriceSelectionReasonCode::MissingProvider
        }
        CostCoverageReasonCode::MissingModel => {
            crate::pricing::PriceSelectionReasonCode::MissingModel
        }
        CostCoverageReasonCode::IdentityMismatch => {
            crate::pricing::PriceSelectionReasonCode::IdentityMismatch
        }
        CostCoverageReasonCode::MissingRate => {
            crate::pricing::PriceSelectionReasonCode::MissingRate
        }
        CostCoverageReasonCode::UnresolvedTier => {
            crate::pricing::PriceSelectionReasonCode::UnresolvedTier
        }
        CostCoverageReasonCode::Unmetered => crate::pricing::PriceSelectionReasonCode::Unmetered,
        _ => crate::pricing::PriceSelectionReasonCode::MissingBinding,
    }
}

fn persisted_catalog_freshness(
    selection: &PricingSelection,
) -> Result<crate::pricing::CatalogFreshness> {
    match selection.catalog_freshness.as_deref() {
        Some("fresh") => Ok(crate::pricing::CatalogFreshness::Fresh),
        Some("stale") => Ok(crate::pricing::CatalogFreshness::Stale),
        Some("refresh_failed") => Ok(crate::pricing::CatalogFreshness::RefreshFailed),
        Some("not_applicable") | None => Ok(crate::pricing::CatalogFreshness::NotApplicable),
        Some(value) => Err(ServiceError::invalid_operation(format!(
            "unknown persisted catalog freshness: {value}"
        ))),
    }
}

pub(crate) fn frozen_selection(
    selection: &PricingSelection,
    rate: Option<&db::PricingRateRevision>,
    reason: Option<CostCoverageReasonCode>,
) -> Result<crate::pricing::FrozenPriceSelection> {
    let rate = (selection.selection_status == PricingSelectionStatus::Priced)
        .then_some(rate)
        .flatten();
    let reason = selection.selection_reason.or(reason);
    let source_kind = selection.source_kind.map(|kind| match kind {
        PricingRateSourceKind::ModelsDevCatalog => {
            crate::pricing::PricingSourceKind::ModelsDevCatalog
        }
        PricingRateSourceKind::ManualOverride => crate::pricing::PricingSourceKind::ManualOverride,
    });
    let rates = rate.map(|rate| {
        crate::pricing::EventBucketRates::new(
            rate.rates
                .input
                .and_then(crate::pricing::NanoUsdPerMillion::from_nano_usd),
            rate.rates
                .output
                .and_then(crate::pricing::NanoUsdPerMillion::from_nano_usd),
            rate.rates
                .cache_read
                .and_then(crate::pricing::NanoUsdPerMillion::from_nano_usd),
            rate.rates
                .cache_write
                .and_then(crate::pricing::NanoUsdPerMillion::from_nano_usd),
        )
    });
    let tiers = rate
        .filter(|rate| !rate.tiers_json.trim().is_empty() && rate.tiers_json.trim() != "[]")
        .map(|rate| crate::pricing::parse_persisted_context_tiers(&rate.tiers_json))
        .transpose()
        .map_err(|error| {
            ServiceError::invalid_operation(format!("invalid persisted pricing tiers: {error}"))
        })?
        .unwrap_or_default();
    let legacy_context_over_200k = rate
        .and_then(|rate| rate.legacy_context_over_200k_json.as_deref())
        .map(crate::pricing::parse_persisted_legacy_context_rate)
        .transpose()
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "invalid persisted legacy pricing tier: {error}"
            ))
        })?;
    let status = if selection.selection_status == PricingSelectionStatus::Priced && rate.is_some() {
        crate::pricing::PriceSelectionStatus::Priced
    } else {
        crate::pricing::PriceSelectionStatus::Unpriced
    };
    let attempt_ordinal = u32::try_from(selection.attempt_ordinal).map_err(|_| {
        ServiceError::invalid_operation("persisted candidate attempt ordinal does not fit in u32")
    })?;
    let catalog_freshness = persisted_catalog_freshness(selection)?;
    let selection = crate::pricing::freeze_persisted_price_selection(
        selection.subject_id.clone(),
        selection.subject_revision_digest.clone(),
        selection.runtime_model.clone(),
        selection.candidate_key.clone(),
        attempt_ordinal,
        selection.admitted_provider_id.clone(),
        rate.and_then(|rate| rate.catalog_provider_id.clone()),
        rate.and_then(|rate| rate.catalog_model_id.clone()),
        source_kind,
        selection.rate_revision_id.clone(),
        selection.catalog_snapshot_id.clone(),
        rates,
        tiers,
        legacy_context_over_200k,
        catalog_freshness,
        status,
        (status == crate::pricing::PriceSelectionStatus::Unpriced)
            .then_some(db_reason_to_service(reason)),
    );
    selection
        .validate()
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    Ok(selection)
}

pub(crate) async fn usage_event_for_report(
    db: &SqliteDb,
    invocation: &UsageInvocation,
    selection: &PricingSelection,
    report: &UsageReport,
    now: &str,
) -> Result<Option<CreateUsageEvent>> {
    validate_persisted_selection_digest(db, selection).await?;
    let rate = match selection.rate_revision_id.as_deref() {
        Some(id) => db::PricingCatalogRepo::get_pricing_rate_revision(db, id).await?,
        None => None,
    };
    usage_event_for_report_with_rate(invocation, selection, report, now, rate.as_ref()).await
}

pub(crate) async fn usage_event_for_report_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    invocation: &UsageInvocation,
    selection: &PricingSelection,
    report: &UsageReport,
    now: &str,
) -> Result<Option<CreateUsageEvent>> {
    validate_persisted_selection_digest_in_tx(db, tx, selection).await?;
    let rate = match selection.rate_revision_id.as_deref() {
        Some(id) => db::PricingCatalogRepo::get_pricing_rate_revision_in_tx(db, tx, id).await?,
        None => None,
    };
    usage_event_for_report_with_rate(invocation, selection, report, now, rate.as_ref()).await
}

async fn usage_event_for_report_with_rate(
    invocation: &UsageInvocation,
    selection: &PricingSelection,
    report: &UsageReport,
    now: &str,
    rate: Option<&db::PricingRateRevision>,
) -> Result<Option<CreateUsageEvent>> {
    let has_counters = report.telemetry_state == executors::UsageTelemetryState::Metered
        && report.counters.has_any();
    let reported_nano_usd = report
        .reported_cost_usd
        .as_deref()
        .map(parse_reported_nano_usd)
        .transpose()?;
    if !has_counters && reported_nano_usd.is_none() {
        return Ok(None);
    }
    let (reported_provider, invalid_reported_provider) =
        bounded_report_identity(report.provider_id.as_deref());
    let (reported_model, invalid_reported_model) =
        bounded_report_identity(report.model_id.as_deref());
    let actual_provider = reported_provider
        .clone()
        .or_else(|| invocation.admitted_provider_id.clone());
    let actual_model = reported_model
        .clone()
        .or_else(|| invocation.admitted_model_id.clone());
    let reported_tier = report
        .selected_tier
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let admitted_reason = if selection.selection_status == PricingSelectionStatus::Unpriced {
        selection.selection_reason.or_else(|| {
            Some(if selection.admitted_provider_id.is_none() {
                CostCoverageReasonCode::MissingProvider
            } else if selection.runtime_model.is_none() {
                CostCoverageReasonCode::MissingModel
            } else {
                CostCoverageReasonCode::MissingBinding
            })
        })
    } else {
        None
    };
    let reported = reported_nano_usd
        .map(|value| {
            crate::pricing::NanoUsd::from_nano_usd(value)
                .ok_or_else(|| ServiceError::invalid_operation("reported USD amount overflows"))
        })
        .transpose()?;
    let frozen_selection = frozen_selection(selection, rate, admitted_reason)?;
    let canonical_digest = frozen_selection
        .validated_selection_digest()
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    if canonical_digest != selection.selection_digest {
        return Err(ServiceError::invalid_operation(
            "persisted pricing selection digest does not match immutable provenance",
        ));
    }

    // A sparse report is still retained as metered evidence. Turning an
    // omitted bucket into zero would manufacture a complete (and potentially
    // undercharged) estimate — but only for a bucket this selection actually
    // charges for. Providers report the buckets they bill: OpenAI never
    // reports a cache write, so requiring all four would leave every OpenAI
    // event unpriced even under an exact rate. An omitted bucket the frozen
    // rate does not price hides no spend and reads as zero.
    let priced_buckets = frozen_selection.priced_buckets();
    let reported_buckets = [
        report.counters.input_tokens.is_some(),
        report.counters.output_tokens.is_some(),
        report.counters.cache_read_tokens.is_some(),
        report.counters.cache_write_tokens.is_some(),
    ];
    let omits_priced_bucket = priced_buckets
        .iter()
        .zip(reported_buckets)
        .any(|(priced, reported)| *priced && !reported);
    // An unpriced selection carries no rates, so nothing proves an omitted
    // bucket free. It keeps the strict rule and reports the sparse reason.
    let complete_counters = has_counters
        && if frozen_selection.status == crate::pricing::PriceSelectionStatus::Priced {
            !omits_priced_bucket
        } else {
            reported_buckets.iter().all(|reported| *reported)
        };
    let counters = complete_counters.then(|| {
        crate::pricing::EventTokenCounts::new(
            report.counters.input_tokens.unwrap_or_default(),
            report.counters.output_tokens.unwrap_or_default(),
            report.counters.cache_read_tokens.unwrap_or_default(),
            report.counters.cache_write_tokens.unwrap_or_default(),
        )
    });
    let mut estimate = if has_counters && !complete_counters && reported.is_none() {
        crate::pricing::EventEstimate::Unpriced {
            reason: crate::pricing::PriceSelectionReasonCode::Unmetered,
        }
    } else {
        crate::pricing::estimate_usage_event(crate::pricing::UsageEventEstimateInput {
            counters,
            provider_reported_amount: reported,
            selection: frozen_selection,
            actual_provider_id: actual_provider,
            actual_model_id: actual_model.clone(),
            context_tokens: report.context_tokens,
        })
    };
    // Provider tier labels are evidence only. Forge's frozen selection and
    // request context decide the tier; a contradictory producer label must
    // never replace that decision or leave a misleading priced event.
    if reported.is_none() && (invalid_reported_provider || invalid_reported_model) {
        // Malformed/unbounded producer identity is itself a contradiction:
        // do not silently fall back to the admitted rate or persist the raw
        // value. Retain token evidence as explicitly unpriced.
        estimate = crate::pricing::EventEstimate::Unpriced {
            reason: crate::pricing::PriceSelectionReasonCode::IdentityMismatch,
        };
    } else if reported.is_none() {
        let selected_tier = match &estimate {
            crate::pricing::EventEstimate::Estimated { provenance, .. } => {
                provenance.selected_tier.as_deref()
            }
            crate::pricing::EventEstimate::Partial {
                provenance: Some(provenance),
                ..
            } => provenance.selected_tier.as_deref(),
            _ => None,
        };
        if reported_tier
            .as_deref()
            .is_some_and(|reported_tier| selected_tier != Some(reported_tier))
        {
            estimate = crate::pricing::EventEstimate::Unpriced {
                reason: crate::pricing::PriceSelectionReasonCode::UnresolvedTier,
            };
        }
    }
    let (
        provider_reported_nano_usd,
        estimated_nano_usd,
        cost_kind,
        coverage_reason_code,
        formula_revision,
        selected_tier,
    ) = match estimate {
        crate::pricing::EventEstimate::ProviderReported { amount } => (
            Some(amount.as_nano_usd()),
            None,
            db::UsageCostKind::ProviderReported,
            None,
            None,
            None,
        ),
        crate::pricing::EventEstimate::Estimated { amount, provenance } => (
            None,
            Some(amount.as_nano_usd()),
            db::UsageCostKind::Estimated,
            None,
            Some(provenance.formula_revision),
            provenance.selected_tier,
        ),
        crate::pricing::EventEstimate::Partial {
            reason, provenance, ..
        } => (
            None,
            None,
            db::UsageCostKind::None,
            Some(match reason {
                crate::pricing::PriceSelectionReasonCode::MissingProvider => {
                    CostCoverageReasonCode::MissingProvider
                }
                crate::pricing::PriceSelectionReasonCode::MissingModel => {
                    CostCoverageReasonCode::MissingModel
                }
                crate::pricing::PriceSelectionReasonCode::IdentityMismatch => {
                    CostCoverageReasonCode::IdentityMismatch
                }
                crate::pricing::PriceSelectionReasonCode::MissingRate => {
                    CostCoverageReasonCode::MissingRate
                }
                crate::pricing::PriceSelectionReasonCode::UnresolvedTier => {
                    CostCoverageReasonCode::UnresolvedTier
                }
                crate::pricing::PriceSelectionReasonCode::Unmetered => {
                    CostCoverageReasonCode::Unmetered
                }
                _ => CostCoverageReasonCode::MissingBinding,
            }),
            provenance
                .as_ref()
                .map(|value| value.formula_revision.clone()),
            provenance.and_then(|value| value.selected_tier),
        ),
        crate::pricing::EventEstimate::Unpriced { reason } => (
            None,
            None,
            db::UsageCostKind::None,
            Some(match reason {
                crate::pricing::PriceSelectionReasonCode::MissingProvider => {
                    CostCoverageReasonCode::MissingProvider
                }
                crate::pricing::PriceSelectionReasonCode::MissingModel => {
                    CostCoverageReasonCode::MissingModel
                }
                crate::pricing::PriceSelectionReasonCode::IdentityMismatch => {
                    CostCoverageReasonCode::IdentityMismatch
                }
                crate::pricing::PriceSelectionReasonCode::MissingRate => {
                    CostCoverageReasonCode::MissingRate
                }
                crate::pricing::PriceSelectionReasonCode::UnresolvedTier => {
                    CostCoverageReasonCode::UnresolvedTier
                }
                crate::pricing::PriceSelectionReasonCode::Unmetered => {
                    CostCoverageReasonCode::Unmetered
                }
                _ => CostCoverageReasonCode::MissingBinding,
            }),
            None,
            None,
        ),
    };
    let source_report_id = opaque_source_report_id(&invocation.id, report);
    let event_id = stable_ledger_id(
        "usage-event",
        &invocation.id,
        &source_report_id,
        i64::from(report.report_sequence),
    );
    let report_mode = if report.reported_cost_usd.is_some() && !has_counters {
        UsageEventReportMode::ReportedMoney
    } else {
        UsageEventReportMode::FinalSnapshot
    };
    Ok(Some(CreateUsageEvent {
        id: event_id.clone(),
        invocation_id: invocation.id.clone(),
        owner_user_id: invocation.owner_user_id.clone(),
        project_id: invocation.project_id.clone(),
        surface: invocation.surface,
        source_id: invocation.source_id.clone(),
        execution_id: invocation.execution_id.clone(),
        task_id: invocation.task_id.clone(),
        event_idempotency_key: event_id,
        source_report_id,
        report_sequence: i64::from(report.report_sequence),
        report_mode,
        provenance_kind: UsageEventProvenanceKind::RuntimeReport,
        legacy_source_table: None,
        legacy_source_id: None,
        legacy_provider_raw: None,
        legacy_provider_sqlite_type: None,
        legacy_provider_sql_literal: None,
        legacy_model_raw: None,
        legacy_model_sqlite_type: None,
        legacy_model_sql_literal: None,
        legacy_counter_values_json: "{}".to_owned(),
        legacy_cost_usd_raw: None,
        legacy_created_at_raw: None,
        legacy_project_owner_raw: None,
        legacy_invalid_usage: false,
        // A syntactically bounded report identity is the actual runtime
        // identity and remains distinct from the immutable admitted identity
        // on the invocation/selection. Invalid producer text is discarded and
        // falls back to admitted provenance rather than entering the ledger.
        provider_id: reported_provider.or_else(|| invocation.admitted_provider_id.clone()),
        model_id: reported_model.or_else(|| invocation.admitted_model_id.clone()),
        runtime_model: invocation.admitted_runtime_model.clone(),
        candidate_key: invocation.candidate_key.clone(),
        attempt_ordinal: invocation.attempt_ordinal,
        agent_id: invocation.agent_id.clone(),
        profile_id: invocation.profile_id.clone(),
        agent_name_snapshot: invocation.agent_name_snapshot.clone(),
        project_name_snapshot: invocation.project_name_snapshot.clone(),
        executor_type: invocation.executor_type.clone(),
        pricing_subject_revision_id: invocation.pricing_subject_revision_id.clone(),
        subject_revision_digest: invocation.subject_revision_digest.clone(),
        telemetry_state: if has_counters {
            UsageTelemetryState::Metered
        } else {
            UsageTelemetryState::Unmetered
        },
        input_tokens: has_counters
            .then_some(report.counters.input_tokens)
            .flatten()
            .map(i64::try_from)
            .transpose()
            .map_err(|_| ServiceError::invalid_operation("input token count overflows"))?,
        output_tokens: has_counters
            .then_some(report.counters.output_tokens)
            .flatten()
            .map(i64::try_from)
            .transpose()
            .map_err(|_| ServiceError::invalid_operation("output token count overflows"))?,
        cache_read_tokens: has_counters
            .then_some(report.counters.cache_read_tokens)
            .flatten()
            .map(i64::try_from)
            .transpose()
            .map_err(|_| ServiceError::invalid_operation("cache-read token count overflows"))?,
        cache_write_tokens: has_counters
            .then_some(report.counters.cache_write_tokens)
            .flatten()
            .map(i64::try_from)
            .transpose()
            .map_err(|_| ServiceError::invalid_operation("cache-write token count overflows"))?,
        context_tokens: report
            .context_tokens
            .map(i64::try_from)
            .transpose()
            .map_err(|_| ServiceError::invalid_operation("context token count overflows"))?,
        selected_tier,
        provider_reported_nano_usd,
        legacy_reported_cost_usd: None,
        estimated_nano_usd,
        cost_kind,
        rate_revision_id: selection.rate_revision_id.clone(),
        catalog_snapshot_id: selection.catalog_snapshot_id.clone(),
        formula_revision,
        retrospective: false,
        coverage_reason_code,
        occurred_at: now.to_owned(),
        created_at: now.to_owned(),
    }))
}

/// Reconstruct the frozen selection from the immutable rate revision and
/// compare its canonical digest with the value persisted at admission. This
/// check deliberately runs even for an unmetered/no-event report: a mutated
/// provenance row must not be silently settled as if its accounting identity
/// were still trustworthy.
pub(crate) async fn validate_persisted_selection_digest(
    db: &SqliteDb,
    selection: &PricingSelection,
) -> Result<()> {
    let rate = match selection.rate_revision_id.as_deref() {
        Some(id) => db::PricingCatalogRepo::get_pricing_rate_revision(db, id).await?,
        None => None,
    };
    let reason = if selection.selection_status == PricingSelectionStatus::Unpriced {
        selection.selection_reason.or_else(|| {
            Some(if selection.admitted_provider_id.is_none() {
                CostCoverageReasonCode::MissingProvider
            } else if selection.runtime_model.is_none() {
                CostCoverageReasonCode::MissingModel
            } else {
                CostCoverageReasonCode::MissingBinding
            })
        })
    } else {
        None
    };
    let frozen = frozen_selection(selection, rate.as_ref(), reason)?;
    let canonical_digest = frozen
        .validated_selection_digest()
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    if canonical_digest != selection.selection_digest {
        return Err(ServiceError::invalid_operation(
            "persisted pricing selection digest does not match immutable provenance",
        ));
    }
    Ok(())
}

pub(crate) async fn validate_persisted_selection_digest_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    selection: &PricingSelection,
) -> Result<()> {
    let rate = match selection.rate_revision_id.as_deref() {
        Some(id) => db::PricingCatalogRepo::get_pricing_rate_revision_in_tx(db, tx, id).await?,
        None => None,
    };
    let reason = if selection.selection_status == PricingSelectionStatus::Unpriced {
        selection.selection_reason.or_else(|| {
            Some(if selection.admitted_provider_id.is_none() {
                CostCoverageReasonCode::MissingProvider
            } else if selection.runtime_model.is_none() {
                CostCoverageReasonCode::MissingModel
            } else {
                CostCoverageReasonCode::MissingBinding
            })
        })
    } else {
        None
    };
    let frozen = frozen_selection(selection, rate.as_ref(), reason)?;
    let canonical_digest = frozen
        .validated_selection_digest()
        .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
    if canonical_digest != selection.selection_digest {
        return Err(ServiceError::invalid_operation(
            "persisted pricing selection digest does not match immutable provenance",
        ));
    }
    Ok(())
}

pub(crate) async fn build_task_usage_settlements(
    db: &SqliteDb,
    execution_id: &str,
    reports: &[UsageReport],
    now: &str,
) -> Result<Vec<UsageLedgerSettlement>> {
    build_task_usage_settlements_with_prepared_invocations(db, execution_id, reports, now, &[])
        .await
}

pub(crate) async fn build_task_usage_settlements_with_prepared_invocations(
    db: &SqliteDb,
    execution_id: &str,
    reports: &[UsageReport],
    now: &str,
    prepared: &[CreateUsageInvocation],
) -> Result<Vec<UsageLedgerSettlement>> {
    let mut invocations =
        db::UsageLedgerRepo::list_usage_invocations_for_source(db, execution_id).await?;
    let existing_ids: std::collections::HashSet<String> = invocations
        .iter()
        .map(|invocation| invocation.id.clone())
        .collect();
    for input in prepared {
        if existing_ids.contains(&input.id) {
            continue;
        }
        invocations.push(UsageInvocation {
            id: input.id.clone(),
            owner_user_id: input.owner_user_id.clone(),
            project_id: input.project_id.clone(),
            domain_kind: input.domain_kind,
            surface: input.surface,
            source_id: input.source_id.clone(),
            execution_id: input.execution_id.clone(),
            task_id: input.task_id.clone(),
            domain_idempotency_key: input.domain_idempotency_key.clone(),
            candidate_key: input.candidate_key.clone(),
            attempt_ordinal: input.attempt_ordinal,
            pricing_selection_id: input.pricing_selection_id.clone(),
            admitted_provider_id: input.admitted_provider_id.clone(),
            admitted_model_id: input.admitted_model_id.clone(),
            admitted_runtime_model: input.admitted_runtime_model.clone(),
            pricing_subject_id: input.pricing_subject_id.clone(),
            pricing_subject_revision_id: input.pricing_subject_revision_id.clone(),
            subject_revision_digest: input.subject_revision_digest.clone(),
            agent_id: input.agent_id.clone(),
            profile_id: input.profile_id.clone(),
            agent_name_snapshot: input.agent_name_snapshot.clone(),
            project_name_snapshot: input.project_name_snapshot.clone(),
            executor_type: input.executor_type.clone(),
            backend_kind: input.backend_kind.clone(),
            provenance_kind: input.provenance_kind,
            lifecycle: UsageInvocationLifecycle::Started,
            telemetry_state: UsageTelemetryState::Pending,
            terminal_reason: None,
            version: 2,
            admitted_at: input.admitted_at.clone(),
            started_at: Some(now.to_owned()),
            settled_at: None,
            created_at: input.created_at.clone(),
            updated_at: input.updated_at.clone(),
        });
    }
    let selections =
        db::UsageLedgerRepo::list_pricing_selections_for_source(db, execution_id).await?;
    let selections_by_id: HashMap<String, PricingSelection> = selections
        .into_iter()
        .map(|selection| (selection.id.clone(), selection))
        .collect();
    let mut settlements = Vec::with_capacity(invocations.len());
    for invocation in invocations {
        let is_settled = invocation.lifecycle == UsageInvocationLifecycle::Settled;
        if !is_settled
            && !matches!(
                invocation.lifecycle,
                UsageInvocationLifecycle::Admitted
                    | UsageInvocationLifecycle::Started
                    | UsageInvocationLifecycle::PendingSettlement
            )
        {
            continue;
        }
        let selection = selections_by_id
            .get(&invocation.pricing_selection_id)
            .ok_or_else(|| {
                ServiceError::invalid_operation("usage invocation selection is missing")
            })?;
        validate_persisted_selection_digest(db, selection).await?;
        let matching = reports
            .iter()
            .filter(|report| {
                i64::from(report.attempt_ordinal) == invocation.attempt_ordinal
                    && report.candidate_key.as_deref() == invocation.candidate_key.as_deref()
            })
            .collect::<Vec<_>>();
        // A settled invocation is only included when this delivery contains
        // a report for it. This lets the DB boundary compare an exact replay
        // while keeping unrelated terminal reports a no-op.
        if is_settled && matching.is_empty() {
            continue;
        }
        let existing_events = if is_settled {
            db::UsageLedgerRepo::list_usage_events_for_invocation(db, &invocation.id).await?
        } else {
            Vec::new()
        };
        let settled_at = invocation
            .settled_at
            .clone()
            .unwrap_or_else(|| now.to_owned());
        let updated_at = if is_settled {
            invocation.updated_at.clone()
        } else {
            now.to_owned()
        };
        let mut events = Vec::new();
        let mut metered = false;
        for report in matching {
            if report.telemetry_state == executors::UsageTelemetryState::Metered
                && report.counters.has_any()
            {
                metered = true;
            }
            let source_report_id = opaque_source_report_id(&invocation.id, report);
            let event_now = existing_events
                .iter()
                .find(|event| event.source_report_id == source_report_id)
                .map(|event| event.created_at.as_str())
                .unwrap_or(settled_at.as_str());
            if let Some(mut event) =
                usage_event_for_report(db, &invocation, selection, report, event_now).await?
            {
                // Preserve both immutable timestamps when rebuilding a
                // settled report. Task reports normally use one timestamp,
                // but a replay must compare every event field exactly.
                if let Some(existing) = existing_events
                    .iter()
                    .find(|existing| existing.source_report_id == source_report_id)
                {
                    event.occurred_at = existing.occurred_at.clone();
                    event.created_at = existing.created_at.clone();
                }
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
            settled_at,
            updated_at,
            events,
        });
    }
    Ok(settlements)
}

/// Transaction-scoped counterpart used by remote terminal preparation. Every
/// pricing-selection/rate/invocation/event read stays behind the same
/// `BEGIN IMMEDIATE` owner guard; the caller commits the guard only after this
/// complete immutable preparation has finished.
pub(crate) async fn build_task_usage_settlements_with_prepared_invocations_in_tx(
    db: &SqliteDb,
    tx: &mut Transaction<'_, Sqlite>,
    execution_id: &str,
    reports: &[UsageReport],
    now: &str,
    prepared: &[CreateUsageInvocation],
) -> Result<Vec<UsageLedgerSettlement>> {
    let mut invocations =
        db::UsageLedgerRepo::list_usage_invocations_for_source_in_tx(db, tx, execution_id).await?;
    let existing_ids: std::collections::HashSet<String> = invocations
        .iter()
        .map(|invocation| invocation.id.clone())
        .collect();
    for input in prepared {
        if existing_ids.contains(&input.id) {
            continue;
        }
        invocations.push(UsageInvocation {
            id: input.id.clone(),
            owner_user_id: input.owner_user_id.clone(),
            project_id: input.project_id.clone(),
            domain_kind: input.domain_kind,
            surface: input.surface,
            source_id: input.source_id.clone(),
            execution_id: input.execution_id.clone(),
            task_id: input.task_id.clone(),
            domain_idempotency_key: input.domain_idempotency_key.clone(),
            candidate_key: input.candidate_key.clone(),
            attempt_ordinal: input.attempt_ordinal,
            pricing_selection_id: input.pricing_selection_id.clone(),
            admitted_provider_id: input.admitted_provider_id.clone(),
            admitted_model_id: input.admitted_model_id.clone(),
            admitted_runtime_model: input.admitted_runtime_model.clone(),
            pricing_subject_id: input.pricing_subject_id.clone(),
            pricing_subject_revision_id: input.pricing_subject_revision_id.clone(),
            subject_revision_digest: input.subject_revision_digest.clone(),
            agent_id: input.agent_id.clone(),
            profile_id: input.profile_id.clone(),
            agent_name_snapshot: input.agent_name_snapshot.clone(),
            project_name_snapshot: input.project_name_snapshot.clone(),
            executor_type: input.executor_type.clone(),
            backend_kind: input.backend_kind.clone(),
            provenance_kind: input.provenance_kind,
            lifecycle: UsageInvocationLifecycle::Started,
            telemetry_state: UsageTelemetryState::Pending,
            terminal_reason: None,
            version: 2,
            admitted_at: input.admitted_at.clone(),
            started_at: Some(now.to_owned()),
            settled_at: None,
            created_at: input.created_at.clone(),
            updated_at: input.updated_at.clone(),
        });
    }
    let selections =
        db::UsageLedgerRepo::list_pricing_selections_for_source_in_tx(db, tx, execution_id).await?;
    let selections_by_id: HashMap<String, PricingSelection> = selections
        .into_iter()
        .map(|selection| (selection.id.clone(), selection))
        .collect();
    let mut settlements = Vec::with_capacity(invocations.len());
    for invocation in invocations {
        let is_settled = invocation.lifecycle == UsageInvocationLifecycle::Settled;
        if !is_settled
            && !matches!(
                invocation.lifecycle,
                UsageInvocationLifecycle::Admitted
                    | UsageInvocationLifecycle::Started
                    | UsageInvocationLifecycle::PendingSettlement
            )
        {
            continue;
        }
        let selection = selections_by_id
            .get(&invocation.pricing_selection_id)
            .ok_or_else(|| {
                ServiceError::invalid_operation("usage invocation selection is missing")
            })?;
        validate_persisted_selection_digest_in_tx(db, tx, selection).await?;
        let matching = reports
            .iter()
            .filter(|report| {
                i64::from(report.attempt_ordinal) == invocation.attempt_ordinal
                    && report.candidate_key.as_deref() == invocation.candidate_key.as_deref()
            })
            .collect::<Vec<_>>();
        if is_settled && matching.is_empty() {
            continue;
        }
        let existing_events = if is_settled {
            db::UsageLedgerRepo::list_usage_events_for_invocation_in_tx(db, tx, &invocation.id)
                .await?
        } else {
            Vec::new()
        };
        let settled_at = invocation
            .settled_at
            .clone()
            .unwrap_or_else(|| now.to_owned());
        let updated_at = if is_settled {
            invocation.updated_at.clone()
        } else {
            now.to_owned()
        };
        let mut events = Vec::new();
        let mut metered = false;
        for report in matching {
            if report.telemetry_state == executors::UsageTelemetryState::Metered
                && report.counters.has_any()
            {
                metered = true;
            }
            let source_report_id = opaque_source_report_id(&invocation.id, report);
            let event_now = existing_events
                .iter()
                .find(|event| event.source_report_id == source_report_id)
                .map(|event| event.created_at.as_str())
                .unwrap_or(settled_at.as_str());
            if let Some(mut event) =
                usage_event_for_report_in_tx(db, tx, &invocation, selection, report, event_now)
                    .await?
            {
                if let Some(existing) = existing_events
                    .iter()
                    .find(|existing| existing.source_report_id == source_report_id)
                {
                    event.occurred_at = existing.occurred_at.clone();
                    event.created_at = existing.created_at.clone();
                }
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
            settled_at,
            updated_at,
            events,
        });
    }
    Ok(settlements)
}

pub(crate) async fn settle_late_task_usage(
    db: &SqliteDb,
    execution_id: &str,
    reports: &[UsageReport],
    now: &str,
) -> Result<()> {
    let settlements = build_task_usage_settlements(db, execution_id, reports, now).await?;
    if !settlements.is_empty() {
        db::UsageLedgerRepo::settle_usage_invocations_with_events(db, settlements).await?;
    }
    Ok(())
}

pub(crate) fn terminal_with_ledger(
    terminal: db::TerminalizeExecution,
    settlements: Vec<UsageLedgerSettlement>,
    terminal_report_id: Option<String>,
    terminal_report_digest: Option<String>,
) -> TerminalizeExecutionWithLedger {
    TerminalizeExecutionWithLedger {
        terminal,
        settlements,
        terminal_report_id,
        terminal_report_digest,
        mark_unreplayable_pending_unsettled: false,
        allow_late_settlement: false,
        preserve_pending_settlement: false,
    }
}

pub(crate) fn terminal_with_late_ledger(
    terminal: db::TerminalizeExecution,
    settlements: Vec<UsageLedgerSettlement>,
    terminal_report_id: Option<String>,
    terminal_report_digest: Option<String>,
) -> TerminalizeExecutionWithLedger {
    let mut input = terminal_with_ledger(
        terminal,
        settlements,
        terminal_report_id,
        terminal_report_digest,
    );
    input.allow_late_settlement = true;
    input
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, run_migrations, PricingAdmissionProvenanceKind, PricingDomainKind,
        UsageEventProvenanceKind,
    };

    fn selection(reason: Option<CostCoverageReasonCode>) -> PricingSelection {
        let mut selection = PricingSelection {
            id: "selection-1".to_owned(),
            owner_user_id: None,
            project_id: None,
            domain_kind: PricingDomainKind::Execution,
            surface: UsageSurface::TaskExecution,
            source_id: "execution-1".to_owned(),
            execution_id: Some("execution-1".to_owned()),
            task_id: Some("task-1".to_owned()),
            candidate_key: Some("candidate-1".to_owned()),
            attempt_ordinal: 0,
            invocation_id: Some("invocation-1".to_owned()),
            subject_id: None,
            subject_revision_id: None,
            subject_revision_digest: None,
            binding_id: None,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_freshness: None,
            runtime_model: Some("model-1".to_owned()),
            admitted_provider_id: Some("provider-1".to_owned()),
            admitted_model_id: Some("model-1".to_owned()),
            source_kind: None,
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            selection_status: PricingSelectionStatus::Unpriced,
            selection_reason: reason,
            selection_digest: String::new(),
            selected_at: "2026-09-08T00:00:00Z".to_owned(),
            created_at: "2026-09-08T00:00:00Z".to_owned(),
        };
        selection.selection_digest = frozen_selection(&selection, None, reason)
            .expect("test selection freezes")
            .selection_digest;
        selection
    }

    fn invocation() -> UsageInvocation {
        UsageInvocation {
            id: "invocation-1".to_owned(),
            owner_user_id: None,
            project_id: None,
            domain_kind: PricingDomainKind::Execution,
            surface: UsageSurface::TaskExecution,
            source_id: "execution-1".to_owned(),
            execution_id: Some("execution-1".to_owned()),
            task_id: Some("task-1".to_owned()),
            domain_idempotency_key: "task-provider-call:invocation-1".to_owned(),
            candidate_key: Some("candidate-1".to_owned()),
            attempt_ordinal: 0,
            pricing_selection_id: "selection-1".to_owned(),
            admitted_provider_id: Some("provider-1".to_owned()),
            admitted_model_id: Some("model-1".to_owned()),
            admitted_runtime_model: Some("model-1".to_owned()),
            pricing_subject_id: None,
            pricing_subject_revision_id: None,
            subject_revision_digest: None,
            agent_id: None,
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: Some("codex".to_owned()),
            backend_kind: None,
            provenance_kind: PricingAdmissionProvenanceKind::Runtime,
            lifecycle: UsageInvocationLifecycle::Started,
            telemetry_state: UsageTelemetryState::Pending,
            terminal_reason: None,
            version: 2,
            admitted_at: "2026-09-08T00:00:00Z".to_owned(),
            started_at: Some("2026-09-08T00:00:01Z".to_owned()),
            settled_at: None,
            created_at: "2026-09-08T00:00:00Z".to_owned(),
            updated_at: "2026-09-08T00:00:01Z".to_owned(),
        }
    }

    async fn test_db() -> SqliteDb {
        let pool = create_sqlite_pool("sqlite::memory:")
            .await
            .expect("test pool creates");
        run_migrations(&pool).await.expect("test migrations apply");
        SqliteDb::new(pool)
    }

    #[test]
    fn reported_decimal_is_strict_and_converted_to_nano_usd() {
        assert_eq!(
            parse_reported_nano_usd("1").expect("whole USD parses"),
            1_000_000_000
        );
        assert_eq!(
            parse_reported_nano_usd("0.000000001").expect("nano USD parses"),
            1
        );
        assert!(parse_reported_nano_usd("1.").is_err());
        assert!(parse_reported_nano_usd("1e-9").is_err());
        assert!(parse_reported_nano_usd(" 1").is_err());
        assert!(parse_reported_nano_usd("0.0000000001").is_err());
    }

    fn priced_selection(rate_id: &str) -> PricingSelection {
        let mut selection = selection(None);
        selection.selection_status = PricingSelectionStatus::Priced;
        selection.selection_reason = None;
        selection.source_kind = Some(PricingRateSourceKind::ManualOverride);
        selection.rate_revision_id = Some(rate_id.to_owned());
        selection.subject_id = Some("subject-1".to_owned());
        selection.subject_revision_id = Some("subject-revision-1".to_owned());
        selection.subject_revision_digest = Some("subject-digest-1".to_owned());
        selection
    }

    fn rate_revision(id: &str, cache_write: Option<i64>) -> db::PricingRateRevision {
        db::PricingRateRevision {
            id: id.to_owned(),
            source_kind: PricingRateSourceKind::ManualOverride,
            owner_user_id: None,
            catalog_snapshot_id: None,
            catalog_provider_id: None,
            catalog_model_id: None,
            pricing_subject_revision_id: None,
            pricing_subject_revision_digest: None,
            runtime_model: Some("model-1".to_owned()),
            source_model_key: None,
            source_last_updated: None,
            currency: "USD".to_owned(),
            // 1.25 / 10.00 / 0.125 USD per million, in nano-USD per million.
            rates: db::RateBuckets::new(
                Some(1_250_000_000),
                Some(10_000_000_000),
                Some(125_000_000),
                cache_write,
            ),
            tiers_json: "[]".to_owned(),
            legacy_context_over_200k_json: None,
            context_tier_state: "exact".to_owned(),
            received_rates_json: "{}".to_owned(),
            rate_digest: format!("digest-{id}"),
            effective_at: "2026-09-08T00:00:00Z".to_owned(),
            created_at: "2026-09-08T00:00:00Z".to_owned(),
        }
    }

    /// A provider reports only the buckets it bills — OpenAI never emits a
    /// cache-write counter — so a bucket the frozen rate leaves unpriced must
    /// not veto the estimate. A bucket the rate *does* price still must.
    #[tokio::test]
    async fn omitted_bucket_blocks_the_estimate_only_when_the_rate_prices_it() {
        let invocation = invocation();
        let report = UsageReport::metered(
            "openai-shaped",
            executors::UsageCounters {
                input_tokens: Some(1_000_000),
                output_tokens: Some(1_000_000),
                cache_read_tokens: Some(1_000_000),
                cache_write_tokens: None,
            },
        );

        let unpriced_write = rate_revision("rate-free-write", None);
        let mut selection = priced_selection(&unpriced_write.id);
        selection.selection_digest = frozen_selection(&selection, Some(&unpriced_write), None)
            .expect("priced selection freezes")
            .selection_digest;
        let event = usage_event_for_report_with_rate(
            &invocation,
            &selection,
            &report,
            "2026-09-08T00:00:02Z",
            Some(&unpriced_write),
        )
        .await
        .expect("report converts")
        .expect("evidence is retained");
        assert_eq!(event.cost_kind, db::UsageCostKind::Estimated);
        // 1.25 + 10.00 + 0.125 USD over one million tokens per bucket.
        assert_eq!(event.estimated_nano_usd, Some(11_375_000_000));
        assert_eq!(event.cache_write_tokens, None);

        let priced_write = rate_revision("rate-priced-write", Some(1_250_000_000));
        let mut selection = priced_selection(&priced_write.id);
        selection.selection_digest = frozen_selection(&selection, Some(&priced_write), None)
            .expect("priced selection freezes")
            .selection_digest;
        let event = usage_event_for_report_with_rate(
            &invocation,
            &selection,
            &report,
            "2026-09-08T00:00:02Z",
            Some(&priced_write),
        )
        .await
        .expect("report converts")
        .expect("evidence is retained");
        assert_eq!(event.cost_kind, db::UsageCostKind::None);
        assert_eq!(
            event.coverage_reason_code,
            Some(CostCoverageReasonCode::Unmetered)
        );
    }

    #[tokio::test]
    async fn sparse_and_explicit_zero_usage_reports_remain_distinguishable() {
        let db = test_db().await;
        let invocation = invocation();
        let selection = selection(Some(CostCoverageReasonCode::MissingBinding));
        let sparse = UsageReport::metered(
            "sparse",
            executors::UsageCounters {
                input_tokens: Some(7),
                output_tokens: None,
                cache_read_tokens: None,
                cache_write_tokens: None,
            },
        );
        let sparse_event = usage_event_for_report(
            &db,
            &invocation,
            &selection,
            &sparse,
            "2026-09-08T00:00:02Z",
        )
        .await
        .expect("sparse report converts")
        .expect("sparse evidence is retained");
        assert_eq!(sparse_event.input_tokens, Some(7));
        assert_eq!(sparse_event.output_tokens, None);
        assert_eq!(sparse_event.cost_kind, db::UsageCostKind::None);
        assert_eq!(
            sparse_event.coverage_reason_code,
            Some(CostCoverageReasonCode::Unmetered)
        );

        let zero = UsageReport::metered("zero", executors::UsageCounters::explicit_zero());
        let zero_event =
            usage_event_for_report(&db, &invocation, &selection, &zero, "2026-09-08T00:00:02Z")
                .await
                .expect("zero report converts")
                .expect("explicit zero evidence is retained");
        assert_eq!(zero_event.input_tokens, Some(0));
        assert_eq!(zero_event.output_tokens, Some(0));
        assert_eq!(zero_event.cache_read_tokens, Some(0));
        assert_eq!(zero_event.cache_write_tokens, Some(0));
        assert_eq!(zero_event.telemetry_state, UsageTelemetryState::Metered);

        let reported_only = UsageReport {
            report_id: "money".to_owned(),
            reported_cost_usd: Some("0.000000001".to_owned()),
            ..UsageReport::default()
        };
        let money_event = usage_event_for_report(
            &db,
            &invocation,
            &selection,
            &reported_only,
            "2026-09-08T00:00:02Z",
        )
        .await
        .expect("reported money converts")
        .expect("reported money evidence is retained");
        assert_eq!(money_event.provider_reported_nano_usd, Some(1));
        assert_eq!(money_event.estimated_nano_usd, None);
        assert_eq!(money_event.cost_kind, db::UsageCostKind::ProviderReported);
        assert!(!money_event.source_report_id.contains("money"));

        let sentinel = UsageReport {
            report_id: "SENTINEL-provider-secret-prompt".to_owned(),
            reported_cost_usd: Some("0".to_owned()),
            ..UsageReport::default()
        };
        let sentinel_event = usage_event_for_report(
            &db,
            &invocation,
            &selection,
            &sentinel,
            "2026-09-08T00:00:02Z",
        )
        .await
        .expect("sentinel report converts")
        .expect("sentinel money evidence is retained");
        assert!(!sentinel_event
            .source_report_id
            .contains("SENTINEL-provider-secret-prompt"));

        let identity_sentinel = UsageReport {
            provider_id: Some("Bearer sk-provider-secret".to_owned()),
            model_id: Some("model=secret prompt".to_owned()),
            counters: executors::UsageCounters {
                input_tokens: Some(1),
                output_tokens: Some(0),
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
            },
            telemetry_state: executors::UsageTelemetryState::Metered,
            ..UsageReport::default()
        };
        let identity_event = usage_event_for_report(
            &db,
            &invocation,
            &selection,
            &identity_sentinel,
            "2026-09-08T00:00:02Z",
        )
        .await
        .expect("identity sentinel converts")
        .expect("identity sentinel evidence is retained");
        assert_eq!(
            identity_event.coverage_reason_code,
            Some(CostCoverageReasonCode::IdentityMismatch)
        );
        assert_eq!(identity_event.cost_kind, db::UsageCostKind::None);
        assert_eq!(identity_event.provider_id.as_deref(), Some("provider-1"));
        assert_eq!(identity_event.model_id.as_deref(), Some("model-1"));

        let reported_mismatch = UsageReport {
            provider_id: Some("actual-provider".to_owned()),
            model_id: Some("actual/model-v2".to_owned()),
            reported_cost_usd: Some("0.25".to_owned()),
            ..UsageReport::default()
        };
        let reported_mismatch_event = usage_event_for_report(
            &db,
            &invocation,
            &selection,
            &reported_mismatch,
            "2026-09-08T00:00:02Z",
        )
        .await
        .expect("reported mismatch converts")
        .expect("reported mismatch evidence is retained");
        assert_eq!(
            reported_mismatch_event.provider_id.as_deref(),
            Some("actual-provider")
        );
        assert_eq!(
            reported_mismatch_event.model_id.as_deref(),
            Some("actual/model-v2")
        );
        assert_eq!(
            reported_mismatch_event.cost_kind,
            db::UsageCostKind::ProviderReported
        );
        assert_eq!(
            reported_mismatch_event.provider_reported_nano_usd,
            Some(250_000_000)
        );
        assert_eq!(reported_mismatch_event.estimated_nano_usd, None);

        let empty = UsageReport::unmetered("empty");
        assert!(usage_event_for_report(
            &db,
            &invocation,
            &selection,
            &empty,
            "2026-09-08T00:00:02Z",
        )
        .await
        .expect("empty report converts")
        .is_none());
    }

    #[test]
    fn admission_reason_survives_frozen_selection_reconstruction() {
        let selection = selection(Some(CostCoverageReasonCode::IdentityMismatch));
        let frozen = frozen_selection(&selection, None, None).expect("selection freezes");
        assert_eq!(
            frozen.reason,
            Some(crate::pricing::PriceSelectionReasonCode::IdentityMismatch)
        );
        assert_eq!(
            frozen.catalog_freshness,
            crate::pricing::CatalogFreshness::NotApplicable
        );
    }

    #[test]
    fn usage_event_provenance_keeps_runtime_report_identity() {
        let selection = selection(Some(CostCoverageReasonCode::IdentityMismatch));
        assert_eq!(
            selection.provenance_kind,
            PricingAdmissionProvenanceKind::Runtime
        );
        assert_eq!(
            UsageEventProvenanceKind::RuntimeReport.to_string(),
            "runtime_report"
        );
    }
}
