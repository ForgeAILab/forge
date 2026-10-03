//! Project layout feedback from durable conflict handoffs; never creates Tasks.
use std::{collections::HashSet, sync::Arc};

use api_types::{Actor, SystemComponent, TaskTransitionEventPayload};
use async_trait::async_trait;
use db::{new_uuid_v4, now_rfc3339, CreateDomainEvent, DomainEvent, DomainEventRepo, SqliteDb};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};

use super::{consumer_error, Outcome, Subscription, Worker, WorkerError};
use crate::workflow::{CONFLICT_HANDOFF_MARKER, CONFLICT_HANDOFF_PATHS_PREFIX};

pub(crate) const CONSUMER_NAME: &str = "conflict-hotspots";
pub(crate) const DETECTED_EVENT: &str = "project.conflict_hotspot.detected";
/// Three independent Tasks distinguish a shared layout bottleneck from one repair loop.
const MIN_TASKS: usize = 3;
/// A week captures current parallel work without letting old layout incidents accumulate.
const WINDOW_DAYS: i64 = 7;
/// Bound wake context while preserving the total distinct-Task count separately.
const MAX_TASK_IDS: usize = 10;
/// Dependency lockfiles are generated shared outputs and cannot be split into modules.
const UNSPLITTABLE_LOCKFILES: &[&str] = &[
    "Cargo.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "go.sum",
    "poetry.lock",
    "uv.lock",
    "Gemfile.lock",
    "composer.lock",
];

// The existing type/sequence index seeks only transitions after installation
// through this delivery, avoiding a full-table scan. Project/time and path
// filtering retain the workflow's JSON semantics, including RFC3339 offsets.
const HANDOFF_QUERY: &str = "SELECT entity_id, actor_type, actor_id, payload_json
    FROM domain_event INDEXED BY idx_domain_event_type_sequence
    WHERE event_type = 'task.transitioned' AND json_valid(payload_json)
      AND json_extract(payload_json, '$.project_id') = ?
      AND julianday(created_at) >= julianday(?) - ?
      AND julianday(created_at) <= julianday(?)
      AND julianday(created_at) > julianday(COALESCE(?, '0001-01-01T00:00:00Z'))
      AND sequence > (SELECT cutover_sequence FROM event_consumer_cutover
                      WHERE consumer_name = 'conflict-hotspots')
      AND sequence <= ? AND entity_type = 'task'
    ORDER BY julianday(created_at) DESC, sequence DESC";

pub(crate) struct ConflictHotspotConsumer {
    db: Arc<SqliteDb>,
}

pub(crate) struct Handoff {
    project_id: String,
    paths: Vec<String>,
}

/// Shared with Attention so punctuation and long paths have one stable incident key.
pub(crate) fn incident_key(project_id: &str, path: &str) -> String {
    format!(
        "attention:conflict_hotspot:project:{project_id}:path:{}",
        hex::encode(Sha256::digest(path.as_bytes()))
    )
}

fn parse_handoff(actor_type: &str, actor_id: Option<&str>, payload_json: &str) -> Option<Handoff> {
    let actor = actor_id.map_or_else(|| actor_type.to_owned(), |id| format!("{actor_type}:{id}"));
    if actor != Actor::system(SystemComponent::Workflow).display() {
        return None;
    }
    let payload: TaskTransitionEventPayload = serde_json::from_str(payload_json).ok()?;
    if payload.from_state != "merging"
        || payload.to_state != "merge_failed"
        || !payload.trigger_reason.contains(CONFLICT_HANDOFF_MARKER)
    {
        return None;
    }
    let (_, encoded) = payload
        .trigger_reason
        .rsplit_once(CONFLICT_HANDOFF_PATHS_PREFIX)?;
    let mut paths: Vec<String> = serde_json::from_str(encoded).ok()?;
    paths.retain(|path| !UNSPLITTABLE_LOCKFILES.contains(&path.rsplit('/').next().unwrap_or(path)));
    paths.sort();
    paths.dedup();
    Some(Handoff {
        project_id: payload.project_id,
        paths,
    })
}

impl ConflictHotspotConsumer {
    pub(crate) fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    async fn detect_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        handoff: &Handoff,
    ) -> crate::Result<()> {
        let now = now_rfc3339();
        for path in &handoff.paths {
            let key = incident_key(&handoff.project_id, path);
            let attention: Option<(String, Option<String>)> = sqlx::query_as(
                "SELECT status, resolved_at FROM attention_projection WHERE dedupe_key = ?",
            )
            .bind(&key)
            .fetch_optional(&mut **tx)
            .await?;
            if let Some((_, Some(resolved_at))) = attention {
                sqlx::query("INSERT INTO conflict_hotspot_boundary (project_id, path, resolved_after)
                    VALUES (?, ?, ?) ON CONFLICT(project_id, path) DO UPDATE SET
                    resolved_after = excluded.resolved_after
                    WHERE julianday(excluded.resolved_after) > julianday(conflict_hotspot_boundary.resolved_after)")
                    .bind(&handoff.project_id).bind(path).bind(resolved_at)
                    .execute(&mut **tx).await?;
            }
            let resolved_at: Option<String> = sqlx::query_scalar(
                "SELECT resolved_after FROM conflict_hotspot_boundary WHERE project_id = ? AND path = ?",
            ).bind(&handoff.project_id).bind(path).fetch_optional(&mut **tx).await?;
            let detection_key = format!("{key}:detected:{}", event.id);
            // Source/path identity also makes an explicit dead-letter replay idempotent.
            let detected: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM domain_event WHERE dedupe_key = ?)",
            )
            .bind(&detection_key)
            .fetch_one(&mut **tx)
            .await?;
            if detected {
                continue;
            }
            let rows = sqlx::query(HANDOFF_QUERY)
                .bind(&handoff.project_id)
                .bind(&now)
                .bind(WINDOW_DAYS)
                .bind(&now)
                .bind(&resolved_at)
                .bind(event.sequence)
                .fetch_all(&mut **tx)
                .await?;
            let mut seen = HashSet::new();
            let mut task_ids = Vec::new();
            for row in rows {
                let candidate = parse_handoff(
                    &row.try_get::<String, _>("actor_type")?,
                    row.try_get::<Option<String>, _>("actor_id")?.as_deref(),
                    &row.try_get::<String, _>("payload_json")?,
                );
                if candidate.is_some_and(|handoff| handoff.paths.contains(path)) {
                    let task_id: String = row.try_get("entity_id")?;
                    if seen.insert(task_id.clone()) {
                        task_ids.push(task_id);
                    }
                }
            }
            let handoff_count = task_ids.len();
            if handoff_count < MIN_TASKS {
                continue;
            }
            task_ids.truncate(MAX_TASK_IDS);
            self.db.append_event_in_tx(tx, &CreateDomainEvent {
                id: new_uuid_v4(), event_type: DETECTED_EVENT.into(),
                entity_type: "project".into(), entity_id: handoff.project_id.clone(),
                actor_type: "system".into(), actor_id: Some(CONSUMER_NAME.into()),
                scope_type: "project".into(), scope_id: handoff.project_id.clone(),
                correlation_id: event.correlation_id.clone(), causation_id: Some(event.id.clone()),
                causation_depth: event.causation_depth.saturating_add(1), dedupe_key: Some(detection_key),
                payload_json: json!({"project_id": handoff.project_id, "path": path,
                    "task_ids": task_ids, "handoff_count": handoff_count, "window_days": WINDOW_DAYS}).to_string(),
                created_at: now.clone(),
            }).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Worker for ConflictHotspotConsumer {
    type Prepared = Handoff;
    fn name(&self) -> &str {
        CONSUMER_NAME
    }
    fn subscription(&self) -> Subscription {
        Subscription::Exact(vec!["task.transitioned".into()])
    }
    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<Outcome<Handoff>, WorkerError> {
        if event.event_type != "task.transitioned" || event.entity_type != "task" {
            return Ok(Outcome::Skip);
        }
        Ok(
            match parse_handoff(
                &event.actor_type,
                event.actor_id.as_deref(),
                &event.payload_json,
            ) {
                Some(handoff) if !handoff.paths.is_empty() => Outcome::Done(handoff),
                _ => Outcome::Skip,
            },
        )
    }
    async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        handoff: &Handoff,
    ) -> std::result::Result<(), WorkerError> {
        self.detect_in_tx(tx, event, handoff)
            .await
            .map_err(consumer_error)
    }
}

#[cfg(test)]
mod tests;
