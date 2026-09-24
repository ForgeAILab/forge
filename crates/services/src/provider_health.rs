//! Provider entry health learned from real provider calls, with backoff.
//!
//! A failing call is classified from the typed provider error text that the
//! agent-host transport produces (`provider returned HTTP 429`, `provider HTTP
//! request failed`, `provider usage limit reached; resets in 2h 5m`, ...).
//! Only provider-side failures count; tool, policy, and configuration errors
//! say nothing about the endpoint.
//!
//! While a timed backoff is active, or an auth failure awaits a successful
//! manual connection test, the entry's agents report `ConnectionDegraded` and
//! queued chat turns wait without spending retries. The first call after a
//! timed backoff lapses is the trial: success clears it, failure waits longer.

use chrono::{DateTime, Duration, Utc};
use db::{AgentRepo, CredentialHandleRepo, ProviderEntryHealth, SqliteDb};

use crate::Result;

const FIRST_BACKOFF_SECONDS: i64 = 30;
const MAX_BACKOFF_SECONDS: i64 = 15 * 60;
const USAGE_EXHAUSTED_DEFAULT_SECONDS: i64 = 30 * 60;
const USAGE_EXHAUSTED_MAX_SECONDS: i64 = 24 * 60 * 60;

/// Provider-side failure classes that affect an entry's health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderFailureKind {
    RateLimited,
    UsageExhausted,
    ServerError,
    Network,
    Auth,
}

impl ProviderFailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimited => "rate_limited",
            Self::UsageExhausted => "usage_exhausted",
            Self::ServerError => "server_error",
            Self::Network => "network",
            Self::Auth => "auth",
        }
    }

    /// Auth and exhausted usage need the owner or the provider's reset; a
    /// retry alone will not clear them.
    fn is_error(self) -> bool {
        matches!(self, Self::Auth | Self::UsageExhausted)
    }
}

/// Classifies a failed provider call, or `None` when the error is not the
/// provider's (tool, policy, configuration, cancellation).
pub fn classify(message: &str) -> Option<ProviderFailureKind> {
    let text = message.to_ascii_lowercase();
    if text.contains("usage limit reached") || text.contains("limit exhausted") {
        return Some(ProviderFailureKind::UsageExhausted);
    }
    if let Some(status) = http_status(&text) {
        return match status {
            401 | 403 => Some(ProviderFailureKind::Auth),
            429 => Some(ProviderFailureKind::RateLimited),
            500..=599 => Some(ProviderFailureKind::ServerError),
            _ => None,
        };
    }
    if text.contains("provider http request failed")
        || text.contains("provider response stream failed")
        || text.contains("provider request timed out")
        || text.contains("provider did not respond")
        || text.contains("provider could not be reached")
        || text.contains("provider request failed before a response")
    {
        return Some(ProviderFailureKind::Network);
    }
    None
}

fn http_status(text: &str) -> Option<u16> {
    if !text.contains("provider ") {
        return None;
    }
    let rest = &text[text.find("http ")? + "http ".len()..];
    rest.get(..3)?.parse().ok()
}

fn public_error_message(kind: ProviderFailureKind, message: &str) -> String {
    match kind {
        ProviderFailureKind::RateLimited => "Provider returned HTTP 429".to_owned(),
        ProviderFailureKind::ServerError => {
            format!(
                "Provider returned HTTP {}",
                http_status(&message.to_ascii_lowercase()).unwrap_or(500)
            )
        }
        ProviderFailureKind::Auth => "Provider rejected the credential".to_owned(),
        ProviderFailureKind::UsageExhausted => "Provider usage limit reached".to_owned(),
        ProviderFailureKind::Network => "Provider request failed before a response".to_owned(),
    }
}

/// `resets in 2h 5m` / `resets in 45m` / `resets in 1d 3h` → seconds.
fn usage_reset_seconds(message: &str) -> Option<i64> {
    let text = message.to_ascii_lowercase();
    let rest = &text[text.find("resets in ")? + "resets in ".len()..];
    let mut total = 0_i64;
    let mut matched = false;
    for token in rest.split_whitespace() {
        let (number, unit) = token.split_at(token.find(|c: char| !c.is_ascii_digit())?);
        let Ok(value) = number.parse::<i64>() else {
            break;
        };
        let seconds = match unit.trim_end_matches(|c: char| !c.is_ascii_alphabetic()) {
            "d" => 86_400,
            "h" => 3_600,
            "m" => 60,
            _ => break,
        };
        total = total.saturating_add(value.saturating_mul(seconds));
        matched = true;
    }
    matched.then_some(total)
}

/// How long to back off after the `failures`-th consecutive failure.
pub fn backoff_seconds(kind: ProviderFailureKind, failures: i64, message: &str) -> Option<i64> {
    match kind {
        ProviderFailureKind::Auth => None,
        ProviderFailureKind::UsageExhausted => usage_reset_seconds(message)
            .unwrap_or(USAGE_EXHAUSTED_DEFAULT_SECONDS)
            .clamp(60, USAGE_EXHAUSTED_MAX_SECONDS)
            .into(),
        _ => {
            let exponent = failures.saturating_sub(1).clamp(0, 10) as u32;
            FIRST_BACKOFF_SECONDS
                .saturating_mul(1_i64 << exponent)
                .min(MAX_BACKOFF_SECONDS)
                .into()
        }
    }
}

/// The next health row after one more failure.
pub fn after_failure(
    current: Option<&ProviderEntryHealth>,
    credential_id: &str,
    owner_user_id: &str,
    kind: ProviderFailureKind,
    message: &str,
    now: DateTime<Utc>,
) -> ProviderEntryHealth {
    let failures = current
        .map(|row| row.consecutive_failures)
        .unwrap_or(0)
        .saturating_add(1);
    let until = backoff_seconds(kind, failures, message)
        .map(|seconds| (now + Duration::seconds(seconds)).to_rfc3339());
    ProviderEntryHealth {
        credential_id: credential_id.to_owned(),
        owner_user_id: owner_user_id.to_owned(),
        status: if kind.is_error() { "error" } else { "backoff" }.to_owned(),
        consecutive_failures: failures,
        last_error_kind: Some(kind.as_str().to_owned()),
        last_error_message: Some(public_error_message(kind, message)),
        last_failure_at: Some(now.to_rfc3339()),
        last_success_at: current.and_then(|row| row.last_success_at.clone()),
        backoff_until: until,
        version: current.map_or(1, |row| row.version.saturating_add(1)),
        updated_at: now.to_rfc3339(),
    }
}

/// Auth failures remain unavailable until a successful manual connection test.
/// Other failures are unavailable only within their backoff window.
pub fn is_unavailable(health: &ProviderEntryHealth, now: DateTime<Utc>) -> bool {
    if health.status == "error" && health.last_error_kind.as_deref() == Some("auth") {
        return true;
    }
    health
        .backoff_until
        .as_deref()
        .and_then(|until| DateTime::parse_from_rfc3339(until).ok())
        .is_some_and(|until| until.with_timezone(&Utc) > now)
}

/// Records one provider call's outcome for a provider entry.
pub async fn record_entry_outcome(
    db: &SqliteDb,
    credential_id: &str,
    outcome: std::result::Result<(), &str>,
) -> Result<()> {
    record_entry_outcome_inner(db, credential_id, outcome, false).await
}

/// A deliberate live connection test can recover an auth failure.
pub async fn record_test_outcome(
    db: &SqliteDb,
    credential_id: &str,
    outcome: std::result::Result<(), &str>,
) -> Result<()> {
    record_entry_outcome_inner(db, credential_id, outcome, true).await
}

async fn record_entry_outcome_inner(
    db: &SqliteDb,
    credential_id: &str,
    outcome: std::result::Result<(), &str>,
    manual_test: bool,
) -> Result<()> {
    let now = Utc::now();
    match outcome {
        Ok(()) => {
            if CredentialHandleRepo::mark_provider_entry_healthy(
                db,
                credential_id,
                &now.to_rfc3339(),
                manual_test,
            )
            .await?
            {
                tracing::info!(credential_id, "provider entry recovered");
            }
        }
        Err(message) => {
            let Some(kind) = classify(message) else {
                return Ok(());
            };
            let Some(handle) =
                CredentialHandleRepo::get_credential_handle(db, credential_id).await?
            else {
                return Ok(());
            };
            loop {
                let current =
                    CredentialHandleRepo::get_provider_entry_health(db, credential_id).await?;
                let next = after_failure(
                    current.as_ref(),
                    credential_id,
                    &handle.owner_user_id,
                    kind,
                    message,
                    now,
                );
                let expected_version = current.as_ref().map(|row| row.version);
                if CredentialHandleRepo::upsert_provider_entry_health(
                    db,
                    next.clone(),
                    expected_version,
                )
                .await?
                {
                    tracing::warn!(
                        credential_id,
                        kind = kind.as_str(),
                        failures = next.consecutive_failures,
                        backoff_until = ?next.backoff_until,
                        "provider entry backing off"
                    );
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Records one provider call's outcome for the entry an agent runs through.
/// Agents without a provider entry (CLI runtimes) are ignored.
pub async fn record_agent_outcome(
    db: &SqliteDb,
    agent_id: &str,
    outcome: std::result::Result<(), &str>,
) -> Result<()> {
    let Some(agent) = AgentRepo::get_by_id(db, agent_id).await? else {
        return Ok(());
    };
    let Some(credential_id) = agent.credential_ref.as_deref() else {
        return Ok(());
    };
    record_entry_outcome(db, credential_id, outcome).await
}

/// Whether an agent's provider entry currently blocks new work.
pub async fn entry_unavailable(db: &SqliteDb, credential_id: &str) -> Result<bool> {
    Ok(
        CredentialHandleRepo::get_provider_entry_health(db, credential_id)
            .await?
            .is_some_and(|health| is_unavailable(&health, Utc::now())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_transport_errors() {
        assert_eq!(
            classify("provider returned HTTP 429"),
            Some(ProviderFailureKind::RateLimited)
        );
        assert_eq!(
            classify("turn failed: provider returned HTTP 503"),
            Some(ProviderFailureKind::ServerError)
        );
        assert_eq!(
            classify("provider returned HTTP 401"),
            Some(ProviderFailureKind::Auth)
        );
        assert_eq!(
            classify("provider rejected the credential (HTTP 401)"),
            Some(ProviderFailureKind::Auth)
        );
        assert_eq!(
            classify("provider could not be reached"),
            Some(ProviderFailureKind::Network)
        );
        assert_eq!(classify("provider returned HTTP 400"), None);
        assert_eq!(
            classify("provider HTTP request failed"),
            Some(ProviderFailureKind::Network)
        );
        assert_eq!(
            classify("provider usage limit reached; resets in 2h 5m"),
            Some(ProviderFailureKind::UsageExhausted)
        );
        assert_eq!(classify("tool permission denied"), None);
    }

    #[test]
    fn backoff_grows_and_caps() {
        let rate = ProviderFailureKind::RateLimited;
        assert_eq!(backoff_seconds(rate, 1, ""), Some(30));
        assert_eq!(backoff_seconds(rate, 2, ""), Some(60));
        assert_eq!(backoff_seconds(rate, 5, ""), Some(480));
        assert_eq!(backoff_seconds(rate, 50, ""), Some(MAX_BACKOFF_SECONDS));
        assert_eq!(backoff_seconds(ProviderFailureKind::Auth, 1, ""), None);
        assert_eq!(
            backoff_seconds(
                ProviderFailureKind::UsageExhausted,
                1,
                "provider usage limit reached; resets in 2h 5m"
            ),
            Some(2 * 3_600 + 5 * 60)
        );
        assert_eq!(
            backoff_seconds(
                ProviderFailureKind::UsageExhausted,
                1,
                "provider usage limit reached"
            ),
            Some(USAGE_EXHAUSTED_DEFAULT_SECONDS)
        );
    }

    #[test]
    fn failure_rows_count_up_and_expire() {
        let now = Utc::now();
        let first = after_failure(
            None,
            "entry",
            "owner",
            ProviderFailureKind::ServerError,
            "provider returned HTTP 502",
            now,
        );
        assert_eq!(first.status, "backoff");
        assert_eq!(first.consecutive_failures, 1);
        assert!(is_unavailable(&first, now));
        assert!(!is_unavailable(&first, now + Duration::seconds(31)));
        let second = after_failure(
            Some(&first),
            "entry",
            "owner",
            ProviderFailureKind::Auth,
            "provider returned HTTP 401",
            now,
        );
        assert_eq!(second.status, "error");
        assert_eq!(second.consecutive_failures, 2);
        assert_eq!(second.backoff_until, None);
        assert!(is_unavailable(&second, now + Duration::days(1)));
    }

    #[tokio::test]
    async fn recorded_failures_gate_work_and_auth_requires_a_successful_test() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool");
        db::run_migrations(&pool).await.expect("migrations");
        let db = SqliteDb::new(pool);
        let now = db::now_rfc3339();
        sqlx::query(
            "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
             VALUES ('health-owner', 'health@example.test', 'test', 'Health owner', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("owner");
        sqlx::query(
            "INSERT INTO credential_handle (id, owner_user_id, provider, label, created_at, updated_at)
             VALUES ('health-entry', 'health-owner', 'openai', 'Test', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("entry");

        record_entry_outcome(
            &db,
            "health-entry",
            Err("provider returned HTTP 429; secret=hidden"),
        )
        .await
        .expect("rate limit recorded");
        let first = CredentialHandleRepo::get_provider_entry_health(&db, "health-entry")
            .await
            .expect("health query")
            .expect("health row");
        assert_eq!(first.status, "backoff");
        assert_eq!(first.consecutive_failures, 1);
        assert_eq!(
            first.last_error_message.as_deref(),
            Some("Provider returned HTTP 429")
        );
        assert!(entry_unavailable(&db, "health-entry")
            .await
            .expect("availability"));

        record_entry_outcome(&db, "health-entry", Ok(()))
            .await
            .expect("transient recovery");
        assert!(!entry_unavailable(&db, "health-entry")
            .await
            .expect("availability"));

        record_entry_outcome(&db, "health-entry", Err("provider returned HTTP 401"))
            .await
            .expect("auth failure recorded");
        let auth = CredentialHandleRepo::get_provider_entry_health(&db, "health-entry")
            .await
            .expect("health query")
            .expect("health row");
        assert_eq!(auth.status, "error");
        assert_eq!(auth.backoff_until, None);
        record_entry_outcome(&db, "health-entry", Ok(()))
            .await
            .expect("ordinary call completed");
        assert!(entry_unavailable(&db, "health-entry")
            .await
            .expect("availability"));
        record_test_outcome(&db, "health-entry", Ok(()))
            .await
            .expect("manual test recovered");
        assert!(!entry_unavailable(&db, "health-entry")
            .await
            .expect("availability"));
        let (a, b, c, d) = tokio::join!(
            record_entry_outcome(&db, "health-entry", Err("provider returned HTTP 503")),
            record_entry_outcome(&db, "health-entry", Err("provider returned HTTP 503")),
            record_entry_outcome(&db, "health-entry", Err("provider returned HTTP 503")),
            record_entry_outcome(&db, "health-entry", Err("provider returned HTTP 503")),
        );
        for result in [a, b, c, d] {
            result.expect("concurrent failure recorded");
        }
        let concurrent = CredentialHandleRepo::get_provider_entry_health(&db, "health-entry")
            .await
            .expect("health query")
            .expect("health row");
        assert_eq!(concurrent.consecutive_failures, 4);
    }
}
