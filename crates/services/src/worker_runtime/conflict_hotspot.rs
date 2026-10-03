//! Project layout feedback from durable conflict handoffs; never creates Tasks.
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use api_types::{Actor, SystemComponent, TaskTransitionEventPayload};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
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

// Existing Project and Task/state/time indexes bound the seek before parsing.
// One query supplies every eligible path; UTC RFC3339 bounds match DB writers.
const HANDOFF_QUERY: &str = "SELECT tl.task_id, tl.trigger_reason, tl.created_at
    FROM task t JOIN transition_log tl ON tl.task_id = t.id
    WHERE t.project_id = ? AND tl.from_state = 'merging' AND tl.to_state = 'merge_failed'
      AND tl.triggered_by = ? AND tl.created_at > ? AND tl.created_at <= ?
    ORDER BY tl.created_at DESC, tl.rowid DESC";
const EPISODE_QUERY: &str = "SELECT resolved_after, open_since
    FROM conflict_hotspot_boundary WHERE project_id = ? AND path = ?";
const CLOSE_EPISODE: &str = "UPDATE conflict_hotspot_boundary
    SET resolved_after = ?, open_since = NULL WHERE project_id = ? AND path = ?";
const OPEN_EPISODE: &str = "INSERT INTO conflict_hotspot_boundary
    (project_id, path, resolved_after, open_since) VALUES (?, ?, ?, ?)
    ON CONFLICT(project_id, path) DO UPDATE SET open_since = excluded.open_since";

/// Durable conflict observation shared by the server and Solo worker runtime.
pub struct ConflictHotspotConsumer {
    db: Arc<SqliteDb>,
}

/// Immutable source data prepared before the writer transaction.
pub struct Handoff {
    project_id: String,
    paths: Vec<String>,
    created_at: String,
}

struct PathCount {
    boundary: Option<DateTime<Utc>>,
    seen: HashSet<String>,
    task_ids: Vec<String>,
}

/// Shared with Attention so punctuation and long paths have one stable incident key.
pub(crate) fn incident_key(project_id: &str, path: &str) -> String {
    format!(
        "attention:conflict_hotspot:project:{project_id}:path:{}",
        hex::encode(Sha256::digest(path.as_bytes()))
    )
}

fn handoff_paths(reason: &str) -> Option<Vec<String>> {
    if !reason.contains(CONFLICT_HANDOFF_MARKER) {
        return None;
    }
    let (_, encoded) = reason.rsplit_once(CONFLICT_HANDOFF_PATHS_PREFIX)?;
    let mut paths: Vec<String> = serde_json::from_str(encoded).ok()?;
    paths.retain(|path| !UNSPLITTABLE_LOCKFILES.contains(&path.rsplit('/').next().unwrap_or(path)));
    paths.sort();
    paths.dedup();
    Some(paths)
}

fn timestamp(value: &str) -> crate::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|_| crate::ServiceError::invalid_operation("invalid conflict handoff timestamp"))
}

impl ConflictHotspotConsumer {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }

    async fn prepare(&self, event: &DomainEvent) -> crate::Result<Option<Handoff>> {
        if event.event_type != "task.transitioned"
            || event.entity_type != "task"
            || event.actor_type != "system"
            || event.actor_id.as_deref() != Some("workflow")
        {
            return Ok(None);
        }
        let Ok(payload) = serde_json::from_str::<TaskTransitionEventPayload>(&event.payload_json)
        else {
            return Ok(None);
        };
        if payload.from_state != "merging" || payload.to_state != "merge_failed" {
            return Ok(None);
        }
        // The ledger reason is bounded. Read the authoritative full reason and
        // timestamp, and refuse a removed or mismatched source Task/log.
        let source = sqlx::query(
            "SELECT tl.from_state, tl.to_state, tl.triggered_by, tl.trigger_reason, tl.created_at
            FROM transition_log tl JOIN task t ON t.id = tl.task_id
            WHERE tl.id = ? AND t.id = ? AND t.project_id = ?",
        )
        .bind(&payload.transition_log_id)
        .bind(&event.entity_id)
        .bind(&payload.project_id)
        .fetch_optional(self.db.pool())
        .await?;
        let Some(source) = source else {
            return Ok(None);
        };
        if source.try_get::<String, _>("from_state")? != "merging"
            || source.try_get::<String, _>("to_state")? != "merge_failed"
            || source.try_get::<String, _>("triggered_by")?
                != Actor::system(SystemComponent::Workflow).display()
        {
            return Ok(None);
        }
        let Some(paths) = handoff_paths(&source.try_get::<String, _>("trigger_reason")?) else {
            return Ok(None);
        };
        Ok((!paths.is_empty()).then(|| Handoff {
            project_id: payload.project_id,
            paths,
            created_at: source.get("created_at"),
        }))
    }

    async fn detect_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        handoff: &Handoff,
    ) -> crate::Result<()> {
        let now = now_rfc3339();
        let mut counts = BTreeMap::new();
        for path in &handoff.paths {
            let state: Option<(Option<String>, Option<String>)> = sqlx::query_as(EPISODE_QUERY)
                .bind(&handoff.project_id)
                .bind(path)
                .fetch_optional(&mut **tx)
                .await?;
            let (mut boundary, open_since) = state.unwrap_or_default();
            if let Some(open_since) = open_since {
                let attention: Option<(String, Option<String>)> = sqlx::query_as(
                    "SELECT status, resolved_at FROM attention_projection WHERE dedupe_key = ?",
                )
                .bind(incident_key(&handoff.project_id, path))
                .fetch_optional(&mut **tx)
                .await?;
                let resolved = match attention {
                    Some((status, Some(at)))
                        if status == "resolved" && timestamp(&at)? >= timestamp(&open_since)? =>
                    {
                        Some(at)
                    }
                    _ => None,
                };
                let Some(resolved) = resolved else {
                    continue;
                };
                boundary = Some(timestamp(&resolved)?.to_rfc3339());
                sqlx::query(CLOSE_EPISODE)
                    .bind(&boundary)
                    .bind(&handoff.project_id)
                    .bind(path)
                    .execute(&mut **tx)
                    .await?;
            }
            counts.insert(
                path.clone(),
                PathCount {
                    boundary: boundary.as_deref().map(timestamp).transpose()?,
                    seen: HashSet::new(),
                    task_ids: Vec::new(),
                },
            );
        }
        // Projection lag, acknowledgement and snoozing keep the episode open:
        // no history query, upsert or new wake for any of those states.
        if counts.is_empty() {
            return Ok(());
        }
        let cutover: String = sqlx::query_scalar(
            "SELECT created_at FROM event_consumer_cutover WHERE consumer_name = ?",
        )
        .bind(CONSUMER_NAME)
        .fetch_one(&mut **tx)
        .await?;
        let cutover = timestamp(&cutover)?;
        let end = timestamp(&handoff.created_at)?;
        let start = end - Duration::days(WINDOW_DAYS);
        // A strict lower bound includes the exact window start while excluding
        // the installation/resolution instants. More recent path boundaries
        // are applied per path below, after this single indexed query.
        let lower = counts
            .values()
            .map(|count| count.boundary.unwrap_or(cutover))
            .min()
            .unwrap_or(cutover);
        let lower = lower.max(cutover).max(start - Duration::nanoseconds(1));
        let rows = sqlx::query(HANDOFF_QUERY)
            .bind(&handoff.project_id)
            .bind(Actor::system(SystemComponent::Workflow).display())
            .bind(lower.to_rfc3339())
            .bind(end.to_rfc3339())
            .fetch_all(&mut **tx)
            .await?;
        for row in rows {
            let at = timestamp(&row.try_get::<String, _>("created_at")?)?;
            let Some(paths) = handoff_paths(&row.try_get::<String, _>("trigger_reason")?) else {
                continue;
            };
            let task_id: String = row.try_get("task_id")?;
            for path in paths {
                if let Some(count) = counts.get_mut(&path) {
                    if at >= start
                        && count.boundary.is_none_or(|boundary| at > boundary)
                        && count.seen.insert(task_id.clone())
                    {
                        count.task_ids.push(task_id.clone());
                    }
                }
            }
        }
        for (path, mut count) in counts {
            let handoff_count = count.task_ids.len();
            if handoff_count < MIN_TASKS {
                continue;
            }
            count.task_ids.truncate(MAX_TASK_IDS);
            let key = incident_key(&handoff.project_id, &path);
            let detection_key = format!("{key}:detected:{}", event.id);
            // Explicit dead-letter replay also retains source/path identity.
            let detected: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM domain_event WHERE dedupe_key = ?)",
            )
            .bind(&detection_key)
            .fetch_one(&mut **tx)
            .await?;
            if detected {
                continue;
            }
            let payload = json!({
                "project_id": handoff.project_id,
                "path": path,
                "task_ids": count.task_ids,
                "handoff_count": handoff_count,
                "window_days": WINDOW_DAYS,
            });
            let detection = CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: DETECTED_EVENT.into(),
                entity_type: "project".into(),
                entity_id: handoff.project_id.clone(),
                actor_type: "system".into(),
                actor_id: Some(CONSUMER_NAME.into()),
                scope_type: "project".into(),
                scope_id: handoff.project_id.clone(),
                correlation_id: event.correlation_id.clone(),
                causation_id: Some(event.id.clone()),
                causation_depth: event.causation_depth.saturating_add(1),
                dedupe_key: Some(detection_key),
                payload_json: payload.to_string(),
                created_at: now.clone(),
            };
            self.db.append_event_in_tx(tx, &detection).await?;
            sqlx::query(OPEN_EPISODE)
                .bind(&handoff.project_id)
                .bind(&path)
                .bind(count.boundary.map(|at| at.to_rfc3339()))
                .bind(&now)
                .execute(&mut **tx)
                .await?;
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
        self.prepare(event)
            .await
            .map(|handoff| handoff.map_or(Outcome::Skip, Outcome::Done))
            .map_err(consumer_error)
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
