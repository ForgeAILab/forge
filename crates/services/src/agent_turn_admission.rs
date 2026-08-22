//! Shared responder resolution and turn-admission preparation.
//!
//! A chat binding is an ownership pointer, not a Profile snapshot.  The
//! resolver therefore follows the active Main/Project binding to its identity
//! and then follows the identity's current selected Profile.  The resulting
//! revision set is copied into the turn job by the caller and is never
//! reconstructed from a later binding/Profile edit while the job runs.

use std::sync::Arc;

use async_trait::async_trait;
use db::{
    AccountMainAgentBindingRepo, AdmitAgentChatTurn, AdmittedAgentChatTurn, AgentChat,
    AgentChatTransactionRepo, AgentProfileRepo, AgentRepo, CreateAgentChatMessage,
    CreateAgentChatTurnJob, ProjectAgentBindingRepo, SqliteDb,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

use crate::{
    operating_skills::{MAIN_BASELINE_OPERATING_SKILL_REVISION, MAIN_OPERATING_SKILL_KEY},
    Result, ServiceError,
};

const MAIN_CHAT_KIND: &str = "account_main";
const PROJECT_CHAT_KIND: &str = "project";
const READY_CHAT_STATUS: &str = "ready";
const ACTIVE_BINDING_STATE: &str = "active";
const TURN_ADMISSION_SCHEMA: &str = "forge.agent-turn-admission/v1";
const DIGEST_SCHEMA: &str = "forge.agent-turn-policy/v1";
const CONTENT_DIGEST_SCHEMA: &str = "forge.agent-chat-content/v1";

/// The trigger is part of the admission digest.  A retry of one trigger must
/// replay its original admission, while a different trigger using the same
/// external dedupe key must be rejected by the persistence boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTurnTrigger {
    UserMessage,
    /// A typed Main-Agent control transfer from the account baseline into an
    /// atomically-created Product Genesis session. It reuses the original
    /// visible user message while freezing a new discovery-skill admission.
    GenesisContinuation,
    MainProjectHandoff,
    AutonomousWake,
    /// User-approved execution-baseline activation delivered into the
    /// Project Agent Chat. This is distinct from an autonomous wake so the
    /// admission digest preserves the approval-triggered continuation.
    BaselineActivation,
}

/// Readiness of the canonical chat/binding boundary.  Setup and unavailable
/// outcomes are deliberately distinct so a wake consumer can defer a
/// transient read without pretending that user configuration is complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentTurnReadiness {
    Ready,
    SetupRequired,
    Unavailable,
}

/// Exact responder/policy provenance prepared for one admitted turn.
///
/// The optional fields are only used by the legacy-read path for jobs created
/// before the provenance migration.  New admissions always populate every
/// field, and [`Self::require_frozen`] rejects an incomplete set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedAgentResponder {
    pub chat_id: String,
    pub canonical_scope_type: String,
    pub canonical_scope_id: String,
    pub readiness: AgentTurnReadiness,
    pub binding_id: Option<String>,
    pub binding_version: Option<i64>,
    pub identity_id: Option<String>,
    pub identity_version: Option<i64>,
    pub profile_id: Option<String>,
    pub profile_version: Option<i64>,
    pub operating_skill_revision: Option<String>,
    pub policy_revision: Option<String>,
    pub policy_digest: Option<String>,
    pub permission_policy_digest: Option<String>,
    pub tool_policy_digest: Option<String>,
}

impl ResolvedAgentResponder {
    fn setup(chat: &AgentChat) -> Self {
        Self {
            chat_id: chat.id.clone(),
            canonical_scope_type: "agent_chat".to_owned(),
            canonical_scope_id: chat.id.clone(),
            readiness: AgentTurnReadiness::SetupRequired,
            binding_id: None,
            binding_version: None,
            identity_id: None,
            identity_version: None,
            profile_id: None,
            profile_version: None,
            operating_skill_revision: None,
            policy_revision: None,
            policy_digest: None,
            permission_policy_digest: None,
            tool_policy_digest: None,
        }
    }

    fn unavailable(chat: &AgentChat) -> Self {
        Self {
            readiness: AgentTurnReadiness::Unavailable,
            ..Self::setup(chat)
        }
    }

    /// Ensure this resolution is suitable for a newly admitted turn.
    pub fn require_frozen(&self) -> Result<()> {
        if self.readiness != AgentTurnReadiness::Ready {
            return Err(match self.readiness {
                AgentTurnReadiness::SetupRequired => ServiceError::Conflict(
                    "Agent Chat responder setup is required before admitting a turn".to_owned(),
                ),
                AgentTurnReadiness::Unavailable => ServiceError::Conflict(
                    "Agent Chat responder is temporarily unavailable".to_owned(),
                ),
                AgentTurnReadiness::Ready => unreachable!(),
            });
        }
        if [
            self.binding_id.as_deref(),
            self.identity_id.as_deref(),
            self.profile_id.as_deref(),
            self.operating_skill_revision.as_deref(),
            self.policy_revision.as_deref(),
            self.policy_digest.as_deref(),
            self.permission_policy_digest.as_deref(),
            self.tool_policy_digest.as_deref(),
        ]
        .into_iter()
        .any(|value| value.is_none())
            || self.binding_version.is_none()
            || self.identity_version.is_none()
            || self.profile_version.is_none()
        {
            return Err(ServiceError::invalid_operation(
                "ready Agent Chat responder has incomplete frozen provenance",
            ));
        }
        Ok(())
    }

    pub fn identity_id(&self) -> Result<&str> {
        self.identity_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat responder has no identity"))
    }

    pub fn profile_id(&self) -> Result<&str> {
        self.profile_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("Agent Chat responder has no Profile"))
    }

    /// Serialize only the redaction-safe resolver output kept alongside a
    /// queued job.  Policy documents and credentials never enter this JSON;
    /// their canonical digests are persisted in the dedicated columns.
    pub fn provenance_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|error| {
            ServiceError::invalid_operation(format!("turn provenance serialization: {error}"))
        })
    }
}

/// Input material frozen by a turn admission.  `content_digest` is included
/// so reusing a dedupe key with changed content cannot replay the old turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentTurnAdmissionInput<'a> {
    pub trigger: AgentTurnTrigger,
    pub dedupe_key: &'a str,
    pub content_digest: &'a str,
    pub causation_id: Option<&'a str>,
    pub causation_depth: i64,
    pub responder: &'a ResolvedAgentResponder,
    pub source_responder: Option<&'a ResolvedAgentResponder>,
}

/// Request parameters for resolving and freezing one turn admission.
///
/// Keeping the trigger material in one value makes it difficult for a
/// producer to omit one of the inputs that participates in the admission
/// digest.  The request borrows caller-owned values because preparation does
/// not need to allocate until it has resolved and validated the responder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnPrepareInput<'a> {
    pub chat: &'a AgentChat,
    pub trigger: AgentTurnTrigger,
    pub dedupe_key: &'a str,
    pub content_digest: &'a str,
    pub causation_id: Option<&'a str>,
    pub causation_depth: i64,
    pub source_responder: Option<&'a ResolvedAgentResponder>,
}

/// Request parameters for a complete message + turn admission.  The
/// preparation fields are nested so `admit` and `prepare` share precisely the
/// same trigger/provenance contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnAdmitInput<'a> {
    pub preparation: AgentTurnPrepareInput<'a>,
    pub message: CreateAgentChatMessage,
    pub turn: CreateAgentChatTurnJob,
}

/// A validated, immutable admission prepared for persistence. Producers may
/// add trigger-specific message/handoff rows, but they must use this object
/// to populate the turn job's responder and policy fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedAgentTurnAdmission {
    pub responder: ResolvedAgentResponder,
    pub trigger: AgentTurnTrigger,
    pub dedupe_key: String,
    pub content_digest: String,
    pub causation_id: Option<String>,
    pub causation_depth: i64,
    pub admission_digest: String,
    pub canonical_scope_provenance_json: String,
}

impl PreparedAgentTurnAdmission {
    pub fn apply_to_turn(
        &self,
        mut turn: CreateAgentChatTurnJob,
    ) -> Result<CreateAgentChatTurnJob> {
        if turn.chat_id != self.responder.chat_id {
            return Err(ServiceError::invalid_operation(
                "prepared Agent Chat admission scope does not match turn",
            ));
        }
        turn.responder_identity_id = self.responder.identity_id()?.to_owned();
        turn.profile_id = self.responder.profile_id()?.to_owned();
        turn.responder_binding_id = self.responder.binding_id.clone();
        turn.responder_binding_version = self.responder.binding_version;
        turn.responder_identity_version = self.responder.identity_version;
        turn.profile_version = self.responder.profile_version;
        turn.operating_skill_revision_id = self.responder.operating_skill_revision.clone();
        turn.policy_revision = self.responder.policy_revision.clone();
        turn.policy_digest = self.responder.policy_digest.clone();
        turn.permission_policy_digest = self.responder.permission_policy_digest.clone();
        turn.tool_policy_digest = self.responder.tool_policy_digest.clone();
        turn.admission_digest = Some(self.admission_digest.clone());
        turn.canonical_scope_type = self.responder.canonical_scope_type.clone();
        turn.canonical_scope_id = self.responder.canonical_scope_id.clone();
        turn.canonical_scope_provenance_json = Some(self.canonical_scope_provenance_json.clone());
        turn.dedupe_key = self.dedupe_key.clone();
        turn.causation_id = self.causation_id.clone();
        turn.causation_depth = self.causation_depth;
        Ok(turn)
    }
}

/// Compute the immutable digest persisted with a turn job.
pub fn admission_digest(input: &AgentTurnAdmissionInput<'_>) -> Result<String> {
    api_types::canonical_digest_with_schema(TURN_ADMISSION_SCHEMA, input)
        .map_err(|error| ServiceError::invalid_operation(format!("turn admission digest: {error}")))
}

/// Compute a stable digest for a policy/permission JSON document.  Parsing is
/// intentionally part of the digest contract: semantically equivalent JSON
/// receives the same digest, while malformed policy is rejected by callers.
pub fn policy_digest(value: &str) -> Result<String> {
    let parsed: Value = serde_json::from_str(value)
        .map_err(|_| ServiceError::invalid_operation("Agent Chat policy JSON is invalid"))?;
    api_types::canonical_digest_with_schema(DIGEST_SCHEMA, &parsed)
        .map_err(|error| ServiceError::invalid_operation(format!("policy digest: {error}")))
}

/// Compute the digest of the guarded trigger content that participates in
/// admission replay identity.
pub fn content_digest(value: &str) -> Result<String> {
    api_types::canonical_digest_with_schema(CONTENT_DIGEST_SCHEMA, &value)
        .map_err(|error| ServiceError::invalid_operation(format!("turn content digest: {error}")))
}

/// Handoffs include the bounded source-revision packet in their admission
/// identity in addition to the visible content.
pub fn handoff_content_digest(content: &str, source_revisions_json: &str) -> Result<String> {
    handoff_content_digest_with_sources(content, source_revisions_json, None, None)
}

/// Compute the stable handoff content/trigger digest including source
/// references.  The references are caller-owned trigger material; generated
/// handoff/message/turn identifiers are deliberately excluded so a
/// response-loss retry can reproduce the same admission digest.
pub fn handoff_content_digest_with_sources(
    content: &str,
    source_revisions_json: &str,
    source_message_id: Option<&str>,
    source_turn_job_id: Option<&str>,
) -> Result<String> {
    #[derive(Serialize)]
    struct HandoffContent<'a> {
        content: &'a str,
        source_revisions_json: &'a str,
        source_message_id: Option<&'a str>,
        source_turn_job_id: Option<&'a str>,
    }

    api_types::canonical_digest_with_schema(
        CONTENT_DIGEST_SCHEMA,
        &HandoffContent {
            content,
            source_revisions_json,
            source_message_id,
            source_turn_job_id,
        },
    )
    .map_err(|error| ServiceError::invalid_operation(format!("handoff content digest: {error}")))
}

/// Repository capability used by the transport-neutral resolver.  Keeping
/// this below REST/MCP/native adapters ensures they cannot grow independent
/// binding or Profile lookup logic.
#[async_trait]
pub trait AgentResponderStore: Send + Sync {
    async fn resolve_agent_responder(&self, chat: &AgentChat) -> Result<ResolvedAgentResponder>;
}

/// Shared admission preparation service.  It does not execute a model turn;
/// it prepares the exact immutable fields that the existing AgentChat turn
/// transaction persists.  User, handoff, retry, and wake producers can all
/// call the same resolver and digest function.
#[derive(Clone)]
pub struct AgentTurnAdmissionService<D> {
    db: Arc<D>,
}

impl<D> std::fmt::Debug for AgentTurnAdmissionService<D> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentTurnAdmissionService")
            .finish_non_exhaustive()
    }
}

impl<D> AgentTurnAdmissionService<D> {
    pub fn new(db: Arc<D>) -> Self {
        Self { db }
    }
}

impl<D> AgentTurnAdmissionService<D>
where
    D: AgentResponderStore,
{
    pub async fn resolve(&self, chat: &AgentChat) -> Result<ResolvedAgentResponder> {
        self.db.resolve_agent_responder(chat).await
    }

    pub fn digest(&self, input: &AgentTurnAdmissionInput<'_>) -> Result<String> {
        admission_digest(input)
    }

    pub async fn prepare(
        &self,
        request: AgentTurnPrepareInput<'_>,
    ) -> Result<PreparedAgentTurnAdmission> {
        let responder = self.resolve(request.chat).await?;
        responder.require_frozen()?;
        let admission_digest = {
            let digest_input = AgentTurnAdmissionInput {
                trigger: request.trigger,
                dedupe_key: request.dedupe_key,
                content_digest: request.content_digest,
                causation_id: request.causation_id,
                causation_depth: request.causation_depth,
                responder: &responder,
                source_responder: request.source_responder,
            };
            self.digest(&digest_input)?
        };
        Ok(PreparedAgentTurnAdmission {
            canonical_scope_provenance_json: responder.provenance_json()?,
            responder,
            trigger: request.trigger,
            dedupe_key: request.dedupe_key.to_owned(),
            content_digest: request.content_digest.to_owned(),
            causation_id: request.causation_id.map(str::to_owned),
            causation_depth: request.causation_depth,
            admission_digest,
        })
    }
}

impl<D> AgentTurnAdmissionService<D>
where
    D: AgentResponderStore + AgentChatTransactionRepo,
{
    /// Resolve, prepare, map, and atomically admit one user/wake/retry turn.
    /// Callers provide only trigger linkage and non-authority turn metadata;
    /// the prepared admission overwrites every responder, policy, scope,
    /// dedupe, and causation field before persistence.
    pub async fn admit(&self, input: AgentTurnAdmitInput<'_>) -> Result<AdmittedAgentChatTurn> {
        if input.message.chat_id != input.preparation.chat.id
            || input.turn.chat_id != input.preparation.chat.id
        {
            return Err(ServiceError::invalid_operation(
                "Agent Chat admission scope does not match canonical chat",
            ));
        }
        if input.turn.triggering_message_id != input.message.id {
            return Err(ServiceError::invalid_operation(
                "Agent Chat turn trigger does not match admitted message",
            ));
        }
        let prepared = self.prepare(input.preparation).await?;
        let turn = prepared.apply_to_turn(input.turn)?;
        Ok(AgentChatTransactionRepo::admit_agent_chat_turn(
            &*self.db,
            AdmitAgentChatTurn {
                message: input.message,
                turn,
            },
        )
        .await?)
    }
}

#[async_trait]
impl AgentResponderStore for SqliteDb {
    async fn resolve_agent_responder(&self, chat: &AgentChat) -> Result<ResolvedAgentResponder> {
        if chat.kind != MAIN_CHAT_KIND && chat.kind != PROJECT_CHAT_KIND {
            return Ok(ResolvedAgentResponder::unavailable(chat));
        }
        if chat.status != READY_CHAT_STATUS {
            return Ok(ResolvedAgentResponder::setup(chat));
        }

        let (
            binding_id,
            binding_version,
            identity_id,
            policy_revision,
            resolved_policy_digest,
            permission_json,
            operating_skill_revision,
        ) = if chat.kind == MAIN_CHAT_KIND {
            let Some(account_id) = chat.account_id.as_deref() else {
                return Ok(ResolvedAgentResponder::unavailable(chat));
            };
            let Some(binding) =
                AccountMainAgentBindingRepo::get_active_main_binding(self, account_id).await?
            else {
                return Ok(ResolvedAgentResponder::setup(chat));
            };
            let genesis_active = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                     SELECT 1 FROM product_genesis_session
                     WHERE main_chat_id = ? AND lifecycle IN ('discovering', 'ready_for_project')
                 )",
            )
            .bind(&chat.id)
            .fetch_one(self.pool())
            .await?
                != 0;
            let skill_revision = if genesis_active {
                current_skill_revision(self, MAIN_OPERATING_SKILL_KEY).await?
            } else {
                Some(MAIN_BASELINE_OPERATING_SKILL_REVISION.to_owned())
            };
            let Some(skill_revision) = skill_revision else {
                return Ok(ResolvedAgentResponder::unavailable(chat));
            };
            (
                binding.id,
                binding.version,
                binding.identity_id.clone(),
                binding.tool_policy_revision,
                policy_digest(&binding.autonomy_policy_json)?,
                account_permission_ceiling(self, &binding.identity_id).await?,
                skill_revision,
            )
        } else {
            let Some(project_id) = chat.project_id.as_deref() else {
                return Ok(ResolvedAgentResponder::unavailable(chat));
            };
            let Some(binding) =
                ProjectAgentBindingRepo::get_active_project_binding(self, project_id).await?
            else {
                return Ok(ResolvedAgentResponder::setup(chat));
            };
            if binding.state != ACTIVE_BINDING_STATE {
                return Ok(ResolvedAgentResponder::setup(chat));
            }
            let Some(identity_id) = binding.identity_id else {
                return Ok(ResolvedAgentResponder::setup(chat));
            };
            // These fields are part of the binding's server-owned provenance,
            // but intentionally are not exposed by the legacy DB model.
            let row = sqlx::query(
                "SELECT operating_skill_revision_id, policy_revision, policy_digest
                 FROM project_agent_binding WHERE id = ? AND project_id = ?",
            )
            .bind(&binding.id)
            .bind(project_id)
            .fetch_optional(self.pool())
            .await?;
            let Some(row) = row else {
                return Ok(ResolvedAgentResponder::unavailable(chat));
            };
            let skill_revision: Option<String> = row.try_get("operating_skill_revision_id")?;
            let policy_revision: String = row.try_get("policy_revision")?;
            let stored_policy_digest: String = row.try_get("policy_digest")?;
            let Some(skill_revision) = skill_revision.filter(|value| !value.trim().is_empty())
            else {
                return Ok(ResolvedAgentResponder::setup(chat));
            };
            (
                binding.id,
                binding.version,
                identity_id,
                policy_revision,
                if stored_policy_digest.trim().is_empty() {
                    policy_digest(&binding.permission_ceiling_json)?
                } else {
                    stored_policy_digest
                },
                binding.permission_ceiling_json,
                skill_revision,
            )
        };

        let Some(identity) = AgentRepo::get_by_id(self, &identity_id).await? else {
            return Ok(ResolvedAgentResponder::unavailable(chat));
        };
        if identity.paused {
            return Ok(ResolvedAgentResponder::unavailable(chat));
        }
        let Some(profile) = AgentProfileRepo::get_profile(self, &identity.profile_id)
            .await?
            .filter(|profile| profile.identity_id == identity.id)
        else {
            return Ok(ResolvedAgentResponder::setup(chat));
        };

        let permission_policy_digest = policy_digest(&permission_json)?;
        let tool_policy_digest = policy_digest(&profile.tool_policy_json)?;
        Ok(ResolvedAgentResponder {
            chat_id: chat.id.clone(),
            canonical_scope_type: "agent_chat".to_owned(),
            canonical_scope_id: chat.id.clone(),
            readiness: AgentTurnReadiness::Ready,
            binding_id: Some(binding_id),
            binding_version: Some(binding_version),
            identity_id: Some(identity.id),
            identity_version: Some(identity.version),
            profile_id: Some(profile.id),
            profile_version: Some(profile.version),
            operating_skill_revision: Some(operating_skill_revision),
            policy_revision: Some(policy_revision),
            policy_digest: Some(resolved_policy_digest),
            permission_policy_digest: Some(permission_policy_digest),
            tool_policy_digest: Some(tool_policy_digest),
        })
    }
}

async fn current_skill_revision(db: &SqliteDb, skill_key: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT revision.id
         FROM operating_skill AS skill
         JOIN operating_skill_revision AS revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
         WHERE skill.skill_key = ? AND skill.lifecycle = 'active'
         LIMIT 1",
    )
    .bind(skill_key)
    .fetch_optional(db.pool())
    .await?)
}

async fn account_permission_ceiling(db: &SqliteDb, identity_id: &str) -> Result<String> {
    Ok(sqlx::query_scalar::<_, Option<String>>(
        "SELECT account_permission_ceiling FROM agent_identity WHERE id = ?",
    )
    .bind(identity_id)
    .fetch_optional(db.pool())
    .await?
    .flatten()
    .unwrap_or_else(|| "{}".to_owned()))
}

/// Main/Project handoff preparation carries both sides' frozen provenance so
/// a replay cannot silently substitute a newly rebound source or target.
pub fn handoff_admission_digest(
    target: &ResolvedAgentResponder,
    source: &ResolvedAgentResponder,
    dedupe_key: &str,
    content_digest: &str,
    causation_id: Option<&str>,
    causation_depth: i64,
) -> Result<String> {
    admission_digest(&AgentTurnAdmissionInput {
        trigger: AgentTurnTrigger::MainProjectHandoff,
        dedupe_key,
        content_digest,
        causation_id,
        causation_depth,
        responder: target,
        source_responder: Some(source),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_responder() -> ResolvedAgentResponder {
        ResolvedAgentResponder {
            chat_id: "chat-1".to_owned(),
            canonical_scope_type: "agent_chat".to_owned(),
            canonical_scope_id: "chat-1".to_owned(),
            readiness: AgentTurnReadiness::Ready,
            binding_id: Some("binding-1".to_owned()),
            binding_version: Some(2),
            identity_id: Some("identity-1".to_owned()),
            identity_version: Some(4),
            profile_id: Some("profile-2".to_owned()),
            profile_version: Some(1),
            operating_skill_revision: Some("forge.main.baseline/v1@1".to_owned()),
            policy_revision: Some("policy-2".to_owned()),
            policy_digest: Some("policy-digest-2".to_owned()),
            permission_policy_digest: Some("permission-digest-2".to_owned()),
            tool_policy_digest: Some("tool-digest-2".to_owned()),
        }
    }

    #[test]
    fn direct_and_handoff_admission_share_target_provenance() {
        let target = ready_responder();
        let source = ready_responder();
        let user = admission_digest(&AgentTurnAdmissionInput {
            trigger: AgentTurnTrigger::UserMessage,
            dedupe_key: "same-trigger",
            content_digest: "content",
            causation_id: None,
            causation_depth: 0,
            responder: &target,
            source_responder: None,
        })
        .expect("user digest");
        let handoff =
            handoff_admission_digest(&target, &source, "same-trigger", "content", None, 0)
                .expect("handoff digest");
        assert_ne!(user, handoff, "trigger provenance remains distinguishable");

        let mut changed = target.clone();
        changed.profile_version = Some(2);
        let changed_digest = admission_digest(&AgentTurnAdmissionInput {
            trigger: AgentTurnTrigger::UserMessage,
            dedupe_key: "same-trigger",
            content_digest: "content",
            causation_id: None,
            causation_depth: 0,
            responder: &changed,
            source_responder: None,
        })
        .expect("changed digest");
        assert_ne!(
            user, changed_digest,
            "a later Profile revision changes admission"
        );
    }

    #[test]
    fn setup_and_unavailable_resolutions_are_not_admissible() {
        let mut setup = ready_responder();
        setup.readiness = AgentTurnReadiness::SetupRequired;
        assert!(setup.require_frozen().is_err());
        let mut unavailable = ready_responder();
        unavailable.readiness = AgentTurnReadiness::Unavailable;
        assert!(unavailable.require_frozen().is_err());
    }

    #[test]
    fn baseline_activation_trigger_is_serialized_and_digest_distinct() {
        let target = ready_responder();
        let baseline = admission_digest(&AgentTurnAdmissionInput {
            trigger: AgentTurnTrigger::BaselineActivation,
            dedupe_key: "baseline-trigger",
            content_digest: "content",
            causation_id: Some("approval-event"),
            causation_depth: 1,
            responder: &target,
            source_responder: None,
        })
        .expect("baseline activation digest");
        let wake = admission_digest(&AgentTurnAdmissionInput {
            trigger: AgentTurnTrigger::AutonomousWake,
            dedupe_key: "baseline-trigger",
            content_digest: "content",
            causation_id: Some("approval-event"),
            causation_depth: 1,
            responder: &target,
            source_responder: None,
        })
        .expect("wake digest");
        assert_ne!(baseline, wake);
        assert_eq!(
            serde_json::to_value(AgentTurnTrigger::BaselineActivation)
                .expect("baseline trigger serialization"),
            serde_json::json!("baseline_activation")
        );
    }
}
