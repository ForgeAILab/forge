//! Owner escalation for admitted blocker digests. This module never mutates Tasks.
use crate::{attention_service::wake_attention_incident_digest, Result, ServiceError};
use api_types::{
    AnswerProjectEscalationRequest, ProjectEscalateRequest, ProjectEscalationResponse,
};
use db::{
    new_uuid_v4, now_rfc3339, AttentionProjection, CreateAttentionProjection, CreateDomainEvent,
    CreateNotification, DomainEventRepo, SqliteDb,
};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};
use std::sync::Arc;

pub const ESCALATE_DESCRIPTION: &str = "Ask the Project owner for the exact need blocking named Tasks; creates one Notification and Attention item.";
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
        identity_id: &str,
        mut request: ProjectEscalateRequest,
        key: &str,
    ) -> Result<ProjectEscalationResponse> {
        if request.need.trim().is_empty()
            || request.need.chars().count() > 4096
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
        let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project_agent_binding WHERE project_id=? AND identity_id=? AND state='active')")
            .bind(project_id).bind(identity_id).fetch_one(&mut *tx).await?;
        if !active {
            return Err(ServiceError::invalid_operation(
                "only the bound Project Agent may escalate",
            ));
        }
        for task_id in &request.task_ids {
            let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task WHERE id=? AND project_id=? AND deleted_at IS NULL)")
                .bind(task_id).bind(project_id).fetch_one(&mut *tx).await?;
            if !valid {
                return Err(ServiceError::invalid_operation(
                    "escalation Task is outside this Project",
                ));
            }
        }
        let turn_id: Option<String> = sqlx::query_scalar("SELECT j.id FROM agent_chat_turn_job j JOIN agent_chat c ON c.id=j.chat_id WHERE c.project_id=? AND j.responder_identity_id=? AND j.status='leased' ORDER BY j.created_at DESC LIMIT 1")
            .bind(project_id).bind(identity_id).fetch_optional(&mut *tx).await?;
        let result = self
            .create_in_tx(
                &mut tx,
                project_id,
                Some(identity_id),
                &request.need,
                &request.task_ids,
                &format!("project-escalate:{project_id}:{key}"),
                turn_id.as_deref(),
            )
            .await?;
        tx.commit().await?;
        Ok(result)
    }
    /// Successful recovery tools record an outcome only for their own live blocker turn.
    pub(crate) async fn record_unblocking_action(
        &self,
        project_id: &str,
        identity_id: &str,
        task_id: &str,
    ) -> Result<()> {
        sqlx::query("UPDATE agent_wake_blocker SET recorded_outcome='recovery_action' WHERE escalation_id IS NULL AND attention_id IN (SELECT id FROM attention_projection WHERE scope_type='project' AND scope_id=? AND json_extract(details_json,'$.entity_id')=?) AND turn_job_id IN (SELECT j.id FROM agent_chat_turn_job j JOIN agent_chat c ON c.id=j.chat_id WHERE c.project_id=? AND j.responder_identity_id=? AND j.status='leased')")
            .bind(project_id).bind(task_id).bind(project_id).bind(identity_id).execute(self.db.pool()).await?;
        Ok(())
    }
    pub async fn get_for_owner(
        &self,
        project_id: &str,
        id: &str,
        owner_id: &str,
    ) -> Result<ProjectEscalationResponse> {
        let owner: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project WHERE id=? AND owner_id=?)")
                .bind(project_id)
                .bind(owner_id)
                .fetch_one(self.db.pool())
                .await?;
        if !owner {
            return Err(ServiceError::invalid_operation(
                "only the Project owner may read or answer an escalation",
            ));
        }
        let row = sqlx::query("SELECT * FROM agent_wake_escalation WHERE id=? AND project_id=?")
            .bind(id)
            .bind(project_id)
            .fetch_optional(self.db.pool())
            .await?
            .ok_or(db::DbError::NotFound)?;
        escalation_response(&row)
    }
    pub async fn answer(
        &self,
        project_id: &str,
        id: &str,
        owner_id: &str,
        request: AnswerProjectEscalationRequest,
    ) -> Result<ProjectEscalationResponse> {
        if request.answer.trim().is_empty() || request.answer.chars().count() > 4096 {
            return Err(ServiceError::invalid_operation(
                "escalation answer must be bounded and nonblank",
            ));
        }
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let owner: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project WHERE id=? AND owner_id=?)")
                .bind(project_id)
                .bind(owner_id)
                .fetch_one(&mut *tx)
                .await?;
        if !owner {
            return Err(ServiceError::invalid_operation(
                "only the Project owner may answer an escalation",
            ));
        }
        let current =
            sqlx::query("SELECT * FROM agent_wake_escalation WHERE id=? AND project_id=?")
                .bind(id)
                .bind(project_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(db::DbError::NotFound)?;
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
    #[allow(clippy::too_many_arguments)]
    async fn create_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
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
        // A second tool call during the same blocker turn reuses its owner handoff.
        if let Some(turn_id) = turn_id {
            if let Some(row)=sqlx::query("SELECT e.* FROM agent_wake_escalation e JOIN agent_wake_blocker b ON b.escalation_id=e.id WHERE b.turn_job_id=? LIMIT 1").bind(turn_id).fetch_optional(&mut **tx).await? {
                return escalation_response(&row);
            }
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
                    actor_type: if identity_id.is_some() {
                        "agent"
                    } else {
                        "system"
                    }
                    .to_owned(),
                    actor_id: identity_id.map(str::to_owned),
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
                    summary: need.chars().take(160).collect(),
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
        if let Some(turn_id) = turn_id {
            sqlx::query("UPDATE agent_wake_blocker SET escalation_id=? WHERE turn_job_id=? AND escalation_id IS NULL").bind(&id).bind(turn_id).execute(&mut **tx).await?;
        }
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
    /// Sweep path after one admitted turn. It cannot run while that turn is pending.
    pub(crate) async fn escalate_blocker(
        &self,
        attention: &AttentionProjection,
        require_silent: bool,
    ) -> Result<bool> {
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
        let Some(row)=sqlx::query("SELECT b.turn_job_id,b.escalation_id,b.recorded_outcome,j.status,j.responder_identity_id,j.created_at FROM agent_wake_blocker b JOIN agent_chat_turn_job j ON j.id=b.turn_job_id WHERE b.attention_id=? AND b.incident_digest=?")
            .bind(&current.id).bind(&digest).fetch_optional(&mut *tx).await? else {return Ok(false)};
        let turn_id: String = row.try_get("turn_job_id")?;
        if row.try_get::<Option<String>, _>("escalation_id")?.is_some()
            || !matches!(
                row.try_get::<String, _>("status")?.as_str(),
                "succeeded" | "failed" | "cancelled"
            )
        {
            return Ok(false);
        }
        let identity: Option<String> = row.try_get("responder_identity_id")?;
        if require_silent
            && row
                .try_get::<Option<String>, _>("recorded_outcome")?
                .is_some()
        {
            return Ok(false);
        }
        if require_silent {
            let recorded:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM domain_event WHERE actor_type='agent' AND actor_id=? AND created_at>=? AND scope_type='project' AND scope_id=? AND event_type IN ('task.recovery_action','task.action','project.environment.rechecked','project.config.requested'))")
                .bind(identity.as_deref()).bind(row.try_get::<String,_>("created_at")?).bind(&current.scope_id).fetch_one(&mut *tx).await?;
            if recorded {
                return Ok(false);
            }
        }
        let linked: Vec<(String,String)> = sqlx::query_as("SELECT attention_id,incident_digest FROM agent_wake_blocker WHERE turn_job_id=? AND escalation_id IS NULL")
            .bind(&turn_id).fetch_all(&mut *tx).await?;
        let mut needs = Vec::new();
        let mut task_ids = Vec::new();
        for (id, admitted_digest) in linked {
            if let Some(a) = self.db.get_attention_in_tx(&mut tx, &id).await? {
                if a.status == "open" && wake_attention_incident_digest(&a) == admitted_digest {
                    let details =
                        serde_json::from_str::<Value>(&a.details_json).unwrap_or(Value::Null);
                    needs.push(format!("{}: {}", a.summary, details));
                    task_ids.extend(blocker_task_ids(&a));
                }
            }
        }
        task_ids.sort();
        task_ids.dedup();
        let need = needs.join("\n").chars().take(4096).collect::<String>();
        let result = self
            .create_in_tx(
                &mut tx,
                &current.scope_id,
                identity.as_deref(),
                &need,
                &task_ids,
                &format!("blocker-owner:{}:{digest}", current.id),
                Some(&turn_id),
            )
            .await?;
        sqlx::query("UPDATE agent_wake_blocker SET escalation_id=? WHERE attention_id=? AND incident_digest=? AND escalation_id IS NULL").bind(result.id).bind(&current.id).bind(digest).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(true)
    }
    pub async fn escalate_silent_turn(&self, turn_id: &str) -> Result<()> {
        let ids:Vec<String>=sqlx::query_scalar("SELECT attention_id FROM agent_wake_blocker WHERE turn_job_id=? AND escalation_id IS NULL").bind(turn_id).fetch_all(self.db.pool()).await?;
        for id in ids {
            if let Some(attention) = db::AttentionRepo::get_attention(&*self.db, &id).await? {
                self.escalate_blocker(&attention, true).await?;
            }
        }
        Ok(())
    }
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
