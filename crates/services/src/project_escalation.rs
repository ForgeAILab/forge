//! Owner escalation for admitted blocker digests. This module never mutates Tasks.
use crate::{
    attention_service::wake_attention_incident_digest,
    wake_blocker::{blocker_turn_state, is_deterministic_failure, BlockerTurnState},
    Result, ServiceError,
};
use api_types::{
    AnswerProjectEscalationRequest, ProjectEscalateRequest, ProjectEscalationListResponse,
    ProjectEscalationResponse,
};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, AttentionProjection, CreateAttentionProjection, CreateDomainEvent,
    CreateNotification, DomainEventRepo, SqliteDb,
};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, SqliteConnection, Transaction};
use std::sync::Arc;

pub const ESCALATE_DESCRIPTION: &str = "Ask the Project owner for the exact need blocking named Tasks; creates one Notification and Attention item.";
/// The answer recorded when the owner resolves an escalation without text.
pub const RESOLVED_BY_OWNER_ANSWER: &str = "Resolved by the owner.";
const MAX_NEED_CHARS: usize = 4096;
const MAX_NEED_LINE_CHARS: usize = 600;
const MAX_LIST_LIMIT: i64 = 100;

/// Task actions that act on a blocker; recording one means the turn did not
/// end silently.
pub fn is_unblocking_verb(verb: &str) -> bool {
    matches!(
        verb,
        "retry" | "release" | "restart" | "cancel" | "send_back" | "approve"
    )
}

/// Who is asking the owner. The escalation event records this authority.
#[derive(Debug, Clone, Copy)]
pub enum EscalationAuthority<'a> {
    /// The bound Project Agent identity (native tool).
    Agent(&'a str),
    /// The authenticated Project owner (MCP).
    Owner(&'a str),
}

#[derive(Clone)]
pub struct ProjectEscalationService {
    db: Arc<SqliteDb>,
}
impl ProjectEscalationService {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }
    pub async fn escalate(
        &self,
        project_id: &str,
        authority: EscalationAuthority<'_>,
        mut request: ProjectEscalateRequest,
        key: &str,
    ) -> Result<ProjectEscalationResponse> {
        if request.need.trim().is_empty()
            || request.need.chars().count() > MAX_NEED_CHARS
            || request.task_ids.len() > 100
            || key.is_empty()
            || key.len() > 256
        {
            return Err(ServiceError::invalid_operation(
                "escalation requires a bounded need, Task IDs and idempotency key",
            ));
        }
        request.task_ids.sort();
        request.task_ids.dedup();
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let (identity_id, actor) = match authority {
            EscalationAuthority::Agent(identity_id) => {
                let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project_agent_binding WHERE project_id=? AND identity_id=? AND state='active')")
                    .bind(project_id).bind(identity_id).fetch_one(&mut *tx).await?;
                if !active {
                    return Err(ServiceError::invalid_operation(
                        "only the bound Project Agent may escalate",
                    ));
                }
                (Some(identity_id), ("agent", identity_id))
            }
            EscalationAuthority::Owner(user_id) => {
                require_owner(&mut tx, project_id, user_id).await?;
                (None, ("user", user_id))
            }
        };
        for task_id in &request.task_ids {
            let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND project_id=? AND deleted_at IS NULL)")
                .bind(task_id).bind(project_id).fetch_one(&mut *tx).await?;
            if !valid {
                return Err(ServiceError::invalid_operation(
                    "escalation Task is outside this Project",
                ));
            }
        }
        let dedupe = format!("project-escalate:{project_id}:{key}");
        // Only the Agent's own live turn links its blockers to this handoff.
        let turn_id: Option<String> = match identity_id {
            Some(identity_id) => sqlx::query_scalar("SELECT j.id FROM agent_chat_turn_job j JOIN agent_chat c ON c.id=j.chat_id WHERE c.project_id=? AND j.responder_identity_id=? AND j.status='leased' ORDER BY j.created_at DESC LIMIT 1")
                .bind(project_id).bind(identity_id).fetch_optional(&mut *tx).await?,
            None => None,
        };
        if let Some(turn_id) = turn_id.as_deref() {
            // A second tool call during the same blocker turn reuses its owner handoff.
            let reused = sqlx::query("SELECT e.* FROM agent_wake_escalation e JOIN agent_wake_blocker b ON b.escalation_id=e.id WHERE b.turn_job_id=? AND e.dedupe_key<>? LIMIT 1")
                .bind(turn_id).bind(&dedupe).fetch_optional(&mut *tx).await?;
            if let Some(row) = reused {
                return escalation_response(&row);
            }
        }
        let result = self
            .create_in_tx(
                &mut tx,
                project_id,
                actor,
                identity_id,
                &request.need,
                &request.task_ids,
                &dedupe,
                turn_id.as_deref(),
            )
            .await?;
        if let Some(turn_id) = turn_id.as_deref() {
            sqlx::query("UPDATE agent_wake_blocker SET escalation_id=? WHERE turn_job_id=? AND escalation_id IS NULL")
                .bind(&result.id).bind(turn_id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(result)
    }
    /// Successful recovery tools record an outcome only for their own live blocker turn.
    pub async fn record_unblocking_action(
        &self,
        project_id: &str,
        identity_id: &str,
        task_id: &str,
    ) -> Result<()> {
        sqlx::query("UPDATE agent_wake_blocker SET recorded_outcome='recovery_action' WHERE escalation_id IS NULL AND attention_id IN (SELECT id FROM attention_projection WHERE scope_type='project' AND scope_id=? AND json_extract(details_json,'$.entity_id')=?) AND turn_job_id IN (SELECT j.id FROM agent_chat_turn_job j JOIN agent_chat c ON c.id=j.chat_id WHERE c.project_id=? AND j.responder_identity_id=? AND j.status='leased')")
            .bind(project_id).bind(task_id).bind(project_id).bind(identity_id).execute(self.db.pool()).await?;
        Ok(())
    }
    pub async fn list_for_owner(
        &self,
        project_id: &str,
        owner_id: &str,
        status: Option<&str>,
        cursor: Option<&str>,
        limit: Option<i64>,
    ) -> Result<ProjectEscalationListResponse> {
        let mut conn = self.db.pool().acquire().await?;
        require_owner(&mut conn, project_id, owner_id).await?;
        if status.is_some_and(|status| !matches!(status, "open" | "answered")) {
            return Err(ServiceError::invalid_operation(
                "status must be open or answered",
            ));
        }
        let after = cursor.map(decode_cursor).transpose()?;
        let limit = limit.unwrap_or(50).clamp(1, MAX_LIST_LIMIT);
        let rows = sqlx::query(
            "SELECT * FROM agent_wake_escalation
             WHERE project_id = ? AND (? IS NULL OR status = ?)
               AND (? IS NULL OR created_at > ? OR (created_at = ? AND id > ?))
             ORDER BY created_at, id LIMIT ?",
        )
        .bind(project_id)
        .bind(status)
        .bind(status)
        .bind(after.as_ref().map(|a| a.0.as_str()))
        .bind(after.as_ref().map(|a| a.0.as_str()))
        .bind(after.as_ref().map(|a| a.0.as_str()))
        .bind(after.as_ref().map(|a| a.1.as_str()))
        .bind(limit + 1)
        .fetch_all(&mut *conn)
        .await?;
        let has_more = rows.len() as i64 > limit;
        let mut items = Vec::with_capacity(rows.len());
        let mut last = None;
        for row in rows.iter().take(limit as usize) {
            last = Some((
                row.try_get::<String, _>("created_at")?,
                row.try_get::<String, _>("id")?,
            ));
            items.push(escalation_response(row)?);
        }
        Ok(ProjectEscalationListResponse {
            items,
            next_cursor: if has_more {
                last.map(|(at, id)| hex::encode(format!("{at}\0{id}")))
            } else {
                None
            },
            has_more,
        })
    }
    pub async fn get_for_owner(
        &self,
        project_id: &str,
        id: &str,
        owner_id: &str,
    ) -> Result<ProjectEscalationResponse> {
        let mut conn = self.db.pool().acquire().await?;
        require_owner(&mut conn, project_id, owner_id).await?;
        let row = sqlx::query("SELECT * FROM agent_wake_escalation WHERE id=? AND project_id=?")
            .bind(id)
            .bind(project_id)
            .fetch_optional(&mut *conn)
            .await?
            .ok_or_else(|| ServiceError::not_found("project_escalation", id.to_owned()))?;
        escalation_response(&row)
    }
    pub async fn answer(
        &self,
        project_id: &str,
        id: &str,
        owner_id: &str,
        request: AnswerProjectEscalationRequest,
    ) -> Result<ProjectEscalationResponse> {
        if request.answer.trim().is_empty() || request.answer.chars().count() > MAX_NEED_CHARS {
            return Err(ServiceError::invalid_operation(
                "escalation answer must be bounded and nonblank",
            ));
        }
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        require_owner(&mut tx, project_id, owner_id).await?;
        let current =
            sqlx::query("SELECT * FROM agent_wake_escalation WHERE id=? AND project_id=?")
                .bind(id)
                .bind(project_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| ServiceError::not_found("project_escalation", id.to_owned()))?;
        let response = escalation_response(&current)?;
        let now = now_rfc3339();
        let changed=sqlx::query("UPDATE agent_wake_escalation SET status='answered',answer=?,answered_at=?,version=version+1 WHERE id=? AND project_id=? AND version=? AND status='open'")
            .bind(&request.answer).bind(&now).bind(id).bind(project_id).bind(request.expected_version).execute(&mut *tx).await?;
        if changed.rows_affected() == 0 {
            return Err(db::DbError::VersionConflict.into());
        }
        let answer_event=self.db.append_event_in_tx(&mut tx, &CreateDomainEvent {
            id:new_uuid_v4(),event_type:"project.escalation.answered".to_owned(),entity_type:"project_escalation".to_owned(),entity_id:id.to_owned(),actor_type:"user".to_owned(),actor_id:Some(owner_id.to_owned()),scope_type:"project".to_owned(),scope_id:project_id.to_owned(),correlation_id:id.to_owned(),causation_id:None,causation_depth:0,dedupe_key:Some(format!("escalation-answered:{id}")),
            payload_json:json!({"project_id":project_id,"escalation_id":id,"answer":request.answer,"task_ids":response.task_ids}).to_string(),created_at:now.clone(),
        }).await?;
        self.db
            .resolve_attention_by_dedupe_in_tx(
                &mut tx,
                &format!("owner-escalation:{id}"),
                &answer_event.id,
                &now,
            )
            .await?;
        let row = sqlx::query("SELECT * FROM agent_wake_escalation WHERE id=?")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        let result = escalation_response(&row)?;
        tx.commit().await?;
        Ok(result)
    }
    /// A generic Resolve of an escalation's Attention item is the owner's
    /// answer: it closes the escalation and wakes the Agent like any answer.
    pub async fn resolve_by_owner(
        &self,
        attention: &AttentionProjection,
        owner_id: &str,
    ) -> Result<ProjectEscalationResponse> {
        let id = serde_json::from_str::<Value>(&attention.details_json)
            .ok()
            .and_then(|details| {
                details
                    .get("escalation_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .ok_or_else(|| ServiceError::invalid_operation("Attention item has no escalation"))?;
        let current = self
            .get_for_owner(&attention.scope_id, &id, owner_id)
            .await?;
        if current.status != "open" {
            return Ok(current);
        }
        self.answer(
            &attention.scope_id,
            &id,
            owner_id,
            AnswerProjectEscalationRequest {
                expected_version: current.version,
                answer: RESOLVED_BY_OWNER_ANSWER.to_owned(),
            },
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    async fn create_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        actor: (&str, &str),
        identity_id: Option<&str>,
        need: &str,
        task_ids: &[String],
        key: &str,
        turn_id: Option<&str>,
    ) -> Result<ProjectEscalationResponse> {
        if let Some(row) = sqlx::query("SELECT * FROM agent_wake_escalation WHERE dedupe_key=?")
            .bind(key)
            .fetch_optional(&mut **tx)
            .await?
        {
            let existing = escalation_response(&row)?;
            if existing.need != need || existing.task_ids != task_ids {
                return Err(db::DbError::IdempotencyConflict.into());
            }
            return Ok(existing);
        }
        let id = new_uuid_v4();
        let attention_id = new_uuid_v4();
        let notification_id = new_uuid_v4();
        let now = now_rfc3339();
        let event = self
            .db
            .append_event_in_tx(
                tx,
                &CreateDomainEvent {
                    id: new_uuid_v4(),
                    event_type: "project.escalated".to_owned(),
                    entity_type: "project_escalation".to_owned(),
                    entity_id: id.clone(),
                    actor_type: actor.0.to_owned(),
                    actor_id: Some(actor.1.to_owned()),
                    scope_type: "project".to_owned(),
                    scope_id: project_id.to_owned(),
                    correlation_id: id.clone(),
                    causation_id: turn_id.map(str::to_owned),
                    causation_depth: 0,
                    dedupe_key: Some(key.to_owned()),
                    payload_json: json!({"escalation_id":id,"task_ids":task_ids}).to_string(),
                    created_at: now.clone(),
                },
            )
            .await?;
        self.db
            .insert_attention_in_tx(
                tx,
                CreateAttentionProjection {
                    id: attention_id.clone(),
                    attention_type: "human_input_required".to_owned(),
                    scope_type: "project".to_owned(),
                    scope_id: project_id.to_owned(),
                    identity_id: identity_id.map(str::to_owned),
                    source_event_id: event.id,
                    priority: 95,
                    status: "open".to_owned(),
                    summary: need
                        .lines()
                        .next()
                        .unwrap_or(need)
                        .trim_start_matches("- ")
                        .chars()
                        .take(160)
                        .collect(),
                    details_json: json!({"escalation_id":id,"need":need,"task_ids":task_ids})
                        .to_string(),
                    dedupe_key: format!("owner-escalation:{id}"),
                    occurred_at: now.clone(),
                    updated_at: now.clone(),
                    acknowledged_at: None,
                    snoozed_until: None,
                    resolved_at: None,
                    updated_by_user_id: None,
                    recommended_action: "answer_escalation".to_owned(),
                    source_sequence: Some(event.sequence),
                },
            )
            .await?;
        self.db
            .create_notification_in_tx(
                tx,
                &CreateNotification {
                    id: notification_id.clone(),
                    project_id: project_id.to_owned(),
                    task_id: None,
                    event_type: "project.escalated".to_owned(),
                    title: "Project Agent needs owner input".to_owned(),
                    body: Some(need.to_owned()),
                    read: false,
                    created_at: now.clone(),
                },
            )
            .await?;
        sqlx::query("INSERT INTO agent_wake_escalation(id,project_id,identity_id,dedupe_key,need,task_ids_json,attention_id,notification_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)")
            .bind(&id).bind(project_id).bind(identity_id).bind(key).bind(need).bind(json!(task_ids).to_string()).bind(&attention_id).bind(&notification_id).bind(&now).execute(&mut **tx).await?;
        Ok(ProjectEscalationResponse {
            id,
            project_id: project_id.to_owned(),
            need: need.to_owned(),
            task_ids: task_ids.to_vec(),
            attention_id,
            notification_id,
            status: "open".to_owned(),
            answer: None,
            version: 1,
        })
    }
    /// Escalate the blockers a completed turn left unchanged. A blocker whose
    /// turn recorded a recovery outcome is not escalated, unless it recurred
    /// after that turn ended (the recovery did not take effect).
    pub(crate) async fn escalate_blocker(&self, attention: &AttentionProjection) -> Result<bool> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let Some(current) = self.db.get_attention_in_tx(&mut tx, &attention.id).await? else {
            return Ok(false);
        };
        let details = serde_json::from_str::<Value>(&current.details_json).unwrap_or(Value::Null);
        // A winning Task repair can precede Attention projection. Check its canonical
        // intervention state under the escalation transaction's writer lock.
        if details.get("source_event_type").and_then(Value::as_str)
            == Some("task.interruption_changed")
            && current.attention_type == "execution_failed"
        {
            let task_id = details
                .get("entity_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let task = sqlx::query("SELECT error_annotation,blocked_json,failed_json FROM task WHERE id=? AND project_id=? AND deleted_at IS NULL AND status NOT IN ('done','cancelled')")
                .bind(task_id).bind(&current.scope_id).fetch_optional(&mut *tx).await?;
            let persists = task.is_some_and(|row| {
                db::task_interruption_requires_intervention(
                    row.try_get::<Option<String>, _>("error_annotation")
                        .ok()
                        .flatten()
                        .as_deref(),
                    row.try_get::<Option<String>, _>("blocked_json")
                        .ok()
                        .flatten()
                        .as_deref(),
                    row.try_get::<Option<String>, _>("failed_json")
                        .ok()
                        .flatten()
                        .as_deref(),
                )
            });
            if !persists {
                self.db
                    .resolve_attention_by_dedupe_in_tx(
                        &mut tx,
                        &current.dedupe_key,
                        &current.source_event_id,
                        &now_rfc3339(),
                    )
                    .await?;
                tx.commit().await?;
                return Ok(false);
            }
        }
        let digest = wake_attention_incident_digest(&current);
        if current.status != "open" || digest != wake_attention_incident_digest(attention) {
            return Ok(false);
        }
        if blocker_turn_state(&mut tx, &current.id, &digest, None, Utc::now()).await?
            != (BlockerTurnState::Completed { escalated: false })
        {
            return Ok(false);
        }
        let row = sqlx::query("SELECT b.turn_job_id,j.responder_identity_id,j.created_at,j.updated_at FROM agent_wake_blocker b JOIN agent_chat_turn_job j ON j.id=b.turn_job_id WHERE b.attention_id=? AND b.incident_digest=? AND b.legacy_source_event_id IS NULL")
            .bind(&current.id).bind(&digest).fetch_one(&mut *tx).await?;
        let turn_id: String = row.try_get("turn_job_id")?;
        let identity: Option<String> = row.try_get("responder_identity_id")?;
        let turn_started: String = row.try_get("created_at")?;
        let turn_ended: String = row.try_get("updated_at")?;
        let linked: Vec<(String, String, Option<String>)> = sqlx::query_as("SELECT attention_id,incident_digest,recorded_outcome FROM agent_wake_blocker WHERE turn_job_id=? AND escalation_id IS NULL AND legacy_source_event_id IS NULL")
            .bind(&turn_id).fetch_all(&mut *tx).await?;
        let mut needs = Vec::new();
        let mut task_ids = Vec::new();
        let mut escalated = Vec::new();
        for (id, admitted_digest, outcome) in linked {
            let Some(a) = self.db.get_attention_in_tx(&mut tx, &id).await? else {
                continue;
            };
            if a.status != "open" || wake_attention_incident_digest(&a) != admitted_digest {
                continue;
            }
            let recovered = outcome.is_some()
                || recovery_transition_recorded(
                    &mut tx,
                    identity.as_deref(),
                    &turn_started,
                    &blocker_task_ids(&a),
                )
                .await?;
            if recovered && !recurred_after(&a, &turn_ended) {
                continue;
            }
            needs.push(blocker_need(&a));
            task_ids.extend(blocker_task_ids(&a));
            escalated.push(id);
        }
        if escalated.is_empty() {
            return Ok(false);
        }
        task_ids.sort();
        task_ids.dedup();
        let need = bounded_chars(&needs.join("\n"), MAX_NEED_CHARS);
        let (actor_type, actor_id) = match identity.as_deref() {
            Some(identity) => ("agent", identity),
            None => ("system", "attention_projection"),
        };
        let result = self
            .create_in_tx(
                &mut tx,
                &current.scope_id,
                (actor_type, actor_id),
                identity.as_deref(),
                &need,
                &task_ids,
                &format!("blocker-owner:{turn_id}:{}", current.id),
                Some(&turn_id),
            )
            .await?;
        for id in escalated {
            sqlx::query("UPDATE agent_wake_blocker SET escalation_id=? WHERE turn_job_id=? AND attention_id=? AND escalation_id IS NULL")
                .bind(&result.id).bind(&turn_id).bind(&id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(true)
    }
    /// Escalate a completed blocker turn that left its blockers unchanged.
    pub async fn escalate_silent_turn(&self, turn_id: &str) -> Result<()> {
        let ids:Vec<String>=sqlx::query_scalar("SELECT attention_id FROM agent_wake_blocker WHERE turn_job_id=? AND escalation_id IS NULL").bind(turn_id).fetch_all(self.db.pool()).await?;
        for id in ids {
            if let Some(attention) = db::AttentionRepo::get_attention(&*self.db, &id).await? {
                self.escalate_blocker(&attention).await?;
            }
        }
        Ok(())
    }
    /// After a wake turn ends: a completed turn may escalate its blockers; a
    /// deterministic provider failure raises "Project Agent can't run" once and
    /// never escalates. Infrastructure failures leave the sweep to re-admit.
    pub async fn after_wake_turn(&self, turn_id: &str) -> Result<()> {
        let Some(job) =
            db::AgentChatTurnJobRepo::get_agent_chat_turn_job(&*self.db, turn_id).await?
        else {
            return Ok(());
        };
        match job.status {
            db::AgentChatTurnState::Succeeded => self.escalate_silent_turn(turn_id).await,
            db::AgentChatTurnState::Failed
                if crate::agent_chat_turn_policy::is_autonomous_wake_turn(&job)
                    && is_deterministic_failure(job.error_code.as_deref()) =>
            {
                self.raise_cannot_run_notice(&job).await
            }
            _ => Ok(()),
        }
    }
    async fn raise_cannot_run_notice(&self, job: &db::AgentChatTurnJob) -> Result<()> {
        let project_id: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM agent_chat WHERE id=?")
                .bind(&job.chat_id)
                .fetch_optional(self.db.pool())
                .await?
                .flatten();
        let Some(project_id) = project_id else {
            return Ok(());
        };
        let code = job.error_code.as_deref().unwrap_or("provider_failure");
        // Once per responder state and failure: a new Profile or version re-arms it.
        let dedupe = format!(
            "autonomy-stalled:provider:{project_id}:{}:{}:{}:{code}",
            job.responder_identity_id.as_deref().unwrap_or_default(),
            job.profile_id.as_deref().unwrap_or_default(),
            job.profile_version.unwrap_or_default(),
        );
        if DomainEventRepo::get_event_by_dedupe(&*self.db, &dedupe)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let now = now_rfc3339();
        DomainEventRepo::append_event(
            &*self.db,
            CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "notification.requested".to_owned(),
                entity_type: "project".to_owned(),
                entity_id: project_id.clone(),
                actor_type: "system".to_owned(),
                actor_id: None,
                scope_type: "project".to_owned(),
                scope_id: project_id.clone(),
                correlation_id: job.correlation_id.clone(),
                causation_id: Some(job.id.clone()),
                causation_depth: 0,
                dedupe_key: Some(dedupe),
                payload_json: json!({
                    "project_id": project_id, "task_id": null,
                    "event_type": "project.autonomy_stalled",
                    "title": format!("Project Agent can't run: {code}"),
                    "body": "The provider rejected an autonomous wake. Open blockers wait until the Agent's Profile or provider can run them; they are not escalated."
                })
                .to_string(),
                created_at: now,
            },
        )
        .await?;
        Ok(())
    }
}

/// 404 for anyone who cannot see the Project; 403 for a member who is not its owner.
async fn require_owner(conn: &mut SqliteConnection, project_id: &str, user_id: &str) -> Result<()> {
    let owner: Option<Option<String>> =
        sqlx::query_scalar("SELECT owner_id FROM project WHERE id=?")
            .bind(project_id)
            .fetch_optional(&mut *conn)
            .await?;
    match owner {
        Some(Some(owner)) if owner == user_id => Ok(()),
        Some(_) => {
            let member: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM project_member WHERE project_id=? AND user_id=?)",
            )
            .bind(project_id)
            .bind(user_id)
            .fetch_one(&mut *conn)
            .await?;
            if member {
                Err(ServiceError::AuthorizationDenied {
                    message: "only the Project owner may read or answer an escalation".to_owned(),
                })
            } else {
                Err(ServiceError::not_found("project", project_id.to_owned()))
            }
        }
        None => Err(ServiceError::not_found("project", project_id.to_owned())),
    }
}

fn decode_cursor(value: &str) -> Result<(String, String)> {
    let invalid = || ServiceError::invalid_operation("invalid cursor");
    let bytes = hex::decode(value).map_err(|_| invalid())?;
    let decoded = String::from_utf8(bytes).map_err(|_| invalid())?;
    let (at, id) = decoded.split_once('\0').ok_or_else(invalid)?;
    if at.is_empty() || id.is_empty() {
        return Err(invalid());
    }
    Ok((at.to_owned(), id.to_owned()))
}

/// Persisted evidence that the turn's responder acted on a blocker Task:
/// `task.transitioned` events it authored after the turn started.
async fn recovery_transition_recorded(
    conn: &mut SqliteConnection,
    identity: Option<&str>,
    turn_started: &str,
    task_ids: &[String],
) -> Result<bool> {
    let Some(identity) = identity else {
        return Ok(false);
    };
    for task_id in task_ids {
        let recorded: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM domain_event
             WHERE entity_type='task' AND entity_id=? AND event_type='task.transitioned'
               AND actor_type='agent' AND actor_id=? AND created_at>=?)",
        )
        .bind(task_id)
        .bind(identity)
        .bind(turn_started)
        .fetch_one(&mut *conn)
        .await?;
        if recorded {
            return Ok(true);
        }
    }
    Ok(false)
}

fn recurred_after(attention: &AttentionProjection, turn_ended: &str) -> bool {
    let parse = |value: &str| {
        DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|at| at.with_timezone(&Utc))
    };
    matches!((parse(&attention.occurred_at), parse(turn_ended)), (Some(at), Some(ended)) if at > ended)
}

fn bounded_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    let mut bounded = value.chars().take(limit - 1).collect::<String>();
    bounded.push('…');
    bounded
}

/// One readable line per blocker: its summary and the details an owner needs.
fn blocker_need(attention: &AttentionProjection) -> String {
    let details = serde_json::from_str::<Value>(&attention.details_json).unwrap_or(Value::Null);
    let text = |pointer: &str| {
        details
            .pointer(pointer)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let mut parts = Vec::new();
    if let Some(title) = text("/task/task_title") {
        parts.push(format!("Task \"{title}\""));
    }
    if let Some(status) = text("/task/task_status") {
        parts.push(format!("status {status}"));
    }
    if let Some(role) = text("/role") {
        parts.push(format!("role {role}"));
    }
    if let Some(kind) = text("/failure_class/kind") {
        parts.push(format!("failure {kind}"));
    }
    if let Some(reason) = text("/interruption/reason")
        .or_else(|| text("/error"))
        .or_else(|| text("/stop_reason"))
        .or_else(|| text("/need"))
    {
        parts.push(format!("reason: {reason}"));
    }
    let line = if parts.is_empty() {
        format!("- {}", attention.summary)
    } else {
        format!("- {} ({})", attention.summary, parts.join("; "))
    };
    bounded_chars(&line, MAX_NEED_LINE_CHARS)
}

fn escalation_response(row: &sqlx::sqlite::SqliteRow) -> Result<ProjectEscalationResponse> {
    Ok(ProjectEscalationResponse {
        id: row.try_get("id")?,
        project_id: row.try_get("project_id")?,
        need: row.try_get("need")?,
        task_ids: serde_json::from_str(&row.try_get::<String, _>("task_ids_json")?)
            .map_err(|e| ServiceError::Domain(e.to_string()))?,
        attention_id: row.try_get("attention_id")?,
        notification_id: row.try_get("notification_id")?,
        status: row.try_get("status")?,
        answer: row.try_get("answer")?,
        version: row.try_get("version")?,
    })
}
pub(crate) fn blocker_task_ids(attention: &AttentionProjection) -> Vec<String> {
    let details = serde_json::from_str::<Value>(&attention.details_json).unwrap_or(Value::Null);
    let mut ids = details
        .get("task_ids")
        .and_then(Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if details.get("entity_type").and_then(Value::as_str) == Some("task") {
        if let Some(id) = details.get("entity_id").and_then(Value::as_str) {
            ids.push(id.to_owned());
        }
    }
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attention(details: Value) -> AttentionProjection {
        AttentionProjection {
            id: "a".into(),
            attention_type: "execution_failed".into(),
            scope_type: "project".into(),
            scope_id: "p".into(),
            identity_id: None,
            source_event_id: "e".into(),
            priority: 0,
            status: "open".into(),
            summary: "Execution failed".into(),
            details_json: details.to_string(),
            dedupe_key: "k".into(),
            occurred_at: "2026-10-04T00:00:00Z".into(),
            updated_at: "2026-10-04T00:00:00Z".into(),
            version: 1,
            acknowledged_at: None,
            snoozed_until: None,
            resolved_at: None,
            updated_by_user_id: None,
            recommended_action: "inspect".into(),
            source_sequence: None,
        }
    }

    #[test]
    fn escalation_need_is_a_bounded_readable_summary_not_raw_json() {
        let a = attention(json!({
            "task": {"task_title": "Build login", "task_status": "failed"},
            "interruption": {"reason": "cargo test failed: 3 errors"},
            "recovery": {"actions": [{"verb": "retry", "parameters": {"fresh_session": true}}]},
        }));
        let need = blocker_need(&a);
        assert_eq!(
            need,
            "- Execution failed (Task \"Build login\"; status failed; reason: cargo test failed: 3 errors)"
        );
        assert!(!need.contains('{') && !need.contains("fresh_session"));
        let long = attention(json!({"error": "x".repeat(5000)}));
        assert_eq!(blocker_need(&long).chars().count(), MAX_NEED_LINE_CHARS);
    }

    #[test]
    fn cursor_round_trips_and_rejects_garbage() {
        let cursor = hex::encode("2026-10-04T00:00:00Z\0id-1");
        assert_eq!(
            decode_cursor(&cursor).unwrap(),
            ("2026-10-04T00:00:00Z".to_owned(), "id-1".to_owned())
        );
        assert!(decode_cursor("zz").is_err());
        assert!(decode_cursor(&hex::encode("no-separator")).is_err());
    }
}
