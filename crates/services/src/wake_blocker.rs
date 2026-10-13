//! The level a Project blocker's wake decisions read: which turn, if any, owns
//! the blocker's current digest. Only a completed turn consumes a digest.
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sqlx::SqliteConnection;

use crate::Result;

/// Latest-decision reasons that keep a blocker suppressed for the same digest
/// and responder. Every other reason (budget, cooldown, duplicate, setup) is
/// transient and the sweep reconsiders it.
pub(crate) const POLICY_SUPPRESSION_REASONS: &[&str] = &[
    "ineligible_scope",
    "self_event",
    "reaction_depth_exceeded",
    "resolved_incident",
    "repeated_failure",
    "retry_exhausted_same_chat",
];
/// Turn failure codes the provider will repeat until the responder changes.
const DETERMINISTIC_FAILURES: &[&str] = &[
    "provider_auth",
    "provider_schema",
    "usage_limit",
    "configuration_invalid",
];
/// A usage-limited wake with no reset hint is held this long.
const UNKNOWN_USAGE_RESET_MINUTES: i64 = 60;
const LIVE_TURN_STATUSES: &[&str] = &["queued", "leased", "retry_wait", "awaiting_input"];

/// The Project responder a blocker turn would run as now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResponderKey {
    pub identity_id: String,
    pub profile_id: Option<String>,
    pub profile_version: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BlockerTurnState {
    /// No completed turn for the current digest: a wake may be admitted.
    Eligible,
    /// A linked turn is queued or running, whatever digest it carried.
    InFlight,
    /// A completed turn owns the current digest; owner escalation follows.
    Completed { escalated: bool },
    /// The digest's turn failed deterministically and the responder has not
    /// changed (nor, for a usage limit, has the window reset).
    Held,
}

pub(crate) fn is_deterministic_failure(code: Option<&str>) -> bool {
    code.is_some_and(|code| DETERMINISTIC_FAILURES.contains(&code))
}

pub(crate) fn is_policy_suppression(reason: &str) -> bool {
    POLICY_SUPPRESSION_REASONS.contains(&reason)
}

pub(crate) async fn current_project_responder(
    conn: &mut SqliteConnection,
    project_id: &str,
) -> Result<Option<ResponderKey>> {
    let row = db::SqliteDb::wake_blocker_project_responder_source(&mut *conn, project_id).await?;
    Ok(row
        .map(|row| -> std::result::Result<ResponderKey, db::DbError> {
            Ok(ResponderKey {
                identity_id: row.identity_id()?,
                profile_id: row.selected_profile_id()?,
                profile_version: row.profile_version()?,
            })
        })
        .transpose()?)
}

/// `responder` enables the deterministic-failure hold; without it a failed
/// turn never holds the digest (the admission transaction's race guard).
pub(crate) async fn blocker_turn_state(
    conn: &mut SqliteConnection,
    attention_id: &str,
    digest: &str,
    responder: Option<&ResponderKey>,
    now: DateTime<Utc>,
) -> Result<BlockerTurnState> {
    let rows = db::SqliteDb::wake_blocker_turn_sources(&mut *conn, attention_id).await?;
    let mut current = None;
    for row in rows {
        let status: String = row.status()?;
        if LIVE_TURN_STATUSES.contains(&status.as_str()) {
            return Ok(BlockerTurnState::InFlight);
        }
        let legacy: Option<String> = row.legacy_source_event_id()?;
        let row_digest: String = row.incident_digest()?;
        // An imported pre-upgrade admission never consumes a current digest.
        if legacy.is_none() && row_digest == digest {
            current = Some((status, row));
        }
    }
    let Some((status, row)) = current else {
        return Ok(BlockerTurnState::Eligible);
    };
    if status == "succeeded" {
        let escalation: Option<String> = row.escalation_id()?;
        return Ok(BlockerTurnState::Completed {
            escalated: escalation.is_some(),
        });
    }
    let code: Option<String> = row.error_code()?;
    let Some(responder) = responder else {
        return Ok(BlockerTurnState::Eligible);
    };
    if status != "failed" || !is_deterministic_failure(code.as_deref()) {
        return Ok(BlockerTurnState::Eligible);
    }
    let same_responder = row.responder_identity_id()?.as_deref()
        == Some(responder.identity_id.as_str())
        && row.profile_id()? == responder.profile_id
        && row.profile_version()? == responder.profile_version;
    if !same_responder {
        return Ok(BlockerTurnState::Eligible);
    }
    if code.as_deref() == Some("usage_limit") {
        let failed_at = DateTime::parse_from_rfc3339(&row.updated_at()?)
            .map(|at| at.with_timezone(&Utc))
            .unwrap_or(now);
        let reset = row
            .failure_class_json()?
            .and_then(|json| serde_json::from_str::<Value>(&json).ok())
            .and_then(|failure| failure.get("resets_at").and_then(Value::as_i64))
            .and_then(DateTime::from_timestamp_millis)
            .filter(|reset| *reset > failed_at)
            .unwrap_or(failed_at + Duration::minutes(UNKNOWN_USAGE_RESET_MINUTES));
        if now >= reset {
            return Ok(BlockerTurnState::Eligible);
        }
    }
    Ok(BlockerTurnState::Held)
}
