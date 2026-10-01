//! Pure finite-state rules shared by Agent Chat admission and the worker.
//!
//! Persistence is deliberately left to the DB repositories. Keeping retry and
//! lease decisions deterministic here prevents a worker restart or a failed
//! failure-write from inventing an unbounded retry loop.

use api_types::{AgentChatTurnStatus, TurnFailure, TurnRetryDecision};
use chrono::{DateTime, Duration, Utc};

const MAX_BACKOFF_SECONDS: i64 = 300;
const ERROR_LIMIT: usize = 512;
pub const MAX_PRE_PROVIDER_FAILURES: i64 = 3;
pub const MAX_USAGE_LIMIT_DEFERRALS: i64 = 24;

#[derive(Debug, Clone, Copy, Default)]
pub struct UsageLimitDeferrals {
    pub count: i64,
    pub first_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseDecision {
    pub status: AgentChatTurnStatus,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureDecision {
    pub status: AgentChatTurnStatus,
    pub attempt_count: i64,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub error: String,
    pub retry_decision: TurnRetryDecision,
    pub pre_provider_failure_count: i64,
    pub usage_limit_deferral_count: i64,
    pub usage_limit_first_deferred_at: Option<DateTime<Utc>>,
}

/// Claim is valid only for a queued or retry-wait job whose cooldown elapsed.
pub fn claim(
    status: AgentChatTurnStatus,
    attempt_count: i64,
    max_attempts: i64,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    owner: &str,
) -> Option<LeaseDecision> {
    if owner.trim().is_empty() || max_attempts <= 0 || attempt_count >= max_attempts {
        return None;
    }
    if !matches!(
        status,
        AgentChatTurnStatus::Queued | AgentChatTurnStatus::RetryWait
    ) {
        return None;
    }
    if lease_until <= now {
        return None;
    }
    Some(LeaseDecision {
        status: AgentChatTurnStatus::Leased,
        lease_owner: Some(owner.to_owned()),
        lease_expires_at: Some(lease_until),
    })
}

/// The only retry policy table. Claim already charged the invocation; capacity
/// and pre-provider failures refund that charge in the same terminal CAS.
/// Forced session compaction is not exposed by the host, so overflow fails.
pub fn failure_after_claim(
    failure: &TurnFailure,
    attempt_count: i64,
    max_attempts: i64,
    pre_provider_failure_count: i64,
    usage_deferrals: UsageLimitDeferrals,
    now: DateTime<Utc>,
    error: &str,
) -> FailureDecision {
    let charged_attempts = attempt_count.max(1);
    let backoff = |count: i64| {
        Duration::seconds(
            5_i64
                .saturating_mul(1_i64 << count.saturating_sub(1).clamp(0, 8) as u32)
                .min(MAX_BACKOFF_SECONDS),
        )
    };
    let mut attempts = charged_attempts;
    let mut admission_failures = pre_provider_failure_count;
    let mut deferrals = usage_deferrals;
    let (retry_decision, next_attempt_at) = match failure {
        TurnFailure::ProviderRejected {
            retryable: false, ..
        }
        | TurnFailure::Configuration
        | TurnFailure::Authority
        | TurnFailure::ContextOverflow => (TurnRetryDecision::Fail, None),
        TurnFailure::UsageLimit { resets_at } => {
            attempts = attempt_count.saturating_sub(1).max(0);
            deferrals.count = deferrals.count.saturating_add(1);
            let first = *deferrals.first_at.get_or_insert(now);
            let deadline = first + Duration::hours(24);
            if deferrals.count >= MAX_USAGE_LIMIT_DEFERRALS || now >= deadline {
                (TurnRetryDecision::Fail, None)
            } else {
                let floor_minutes = match deferrals.count {
                    1 => 1,
                    2 => 5,
                    3 => 15,
                    4 => 30,
                    _ => 60,
                };
                // Mirror Task capacity deferral: refund the attempt, use a
                // fifteen-minute unknown hint and a six-hour per-wait ceiling.
                // Stale reset timestamps are unknown, never immediate retries.
                let reset = resets_at
                    .and_then(|ms| i64::try_from(ms).ok())
                    .and_then(DateTime::from_timestamp_millis)
                    .filter(|reset| *reset > now)
                    .unwrap_or(now + Duration::minutes(15));
                let next = reset
                    .max(now + Duration::minutes(floor_minutes))
                    .min(now + Duration::hours(6))
                    .min(deadline);
                (TurnRetryDecision::Defer, Some(next))
            }
        }
        TurnFailure::PreProviderAdmission => {
            attempts = attempt_count.saturating_sub(1).max(0);
            admission_failures = admission_failures.saturating_add(1);
            if admission_failures >= MAX_PRE_PROVIDER_FAILURES {
                (TurnRetryDecision::Fail, None)
            } else {
                (
                    TurnRetryDecision::Defer,
                    Some(now + backoff(admission_failures)),
                )
            }
        }
        _ => {
            let budget = max_attempts;
            if budget <= 0 || attempts >= budget {
                (TurnRetryDecision::Fail, None)
            } else {
                let hint = match failure {
                    TurnFailure::Transient { retry_after }
                    | TurnFailure::ProviderRejected {
                        retryable: true,
                        retry_after,
                    } => *retry_after,
                    _ => None,
                };
                let delay = hint
                    .map(|ms| Duration::milliseconds(ms.min(21_600_000) as i64))
                    .unwrap_or_default()
                    .max(backoff(attempts));
                (TurnRetryDecision::Retry, Some(now + delay))
            }
        }
    };
    FailureDecision {
        status: if retry_decision == TurnRetryDecision::Fail {
            AgentChatTurnStatus::Failed
        } else {
            AgentChatTurnStatus::RetryWait
        },
        attempt_count: attempts,
        next_attempt_at,
        error: bounded_error(error),
        retry_decision,
        pre_provider_failure_count: admission_failures,
        usage_limit_deferral_count: deferrals.count,
        usage_limit_first_deferred_at: deferrals.first_at,
    }
}

/// Expired leases become retryable/terminal using the same rule as an
/// invocation failure; no model call is needed to recover a stale lease.
pub fn recover_expired(
    status: AgentChatTurnStatus,
    lease_expires_at: Option<DateTime<Utc>>,
    attempt_count: i64,
    max_attempts: i64,
    now: DateTime<Utc>,
) -> Option<FailureDecision> {
    if status != AgentChatTurnStatus::Leased || lease_expires_at.is_none_or(|until| until > now) {
        return None;
    }
    Some(failure_after_claim(
        &TurnFailure::Unclassified,
        attempt_count,
        max_attempts,
        0,
        UsageLimitDeferrals::default(),
        now,
        "turn execution lease expired",
    ))
}

pub fn bounded_error(error: &str) -> String {
    error
        .chars()
        .filter(|character| !matches!(character, '\r' | '\n' | '\0'))
        .take(ERROR_LIMIT)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).expect("valid timestamp")
    }

    #[test]
    fn every_failure_variant_has_a_finite_typed_policy() {
        use api_types::TurnLimitCause;
        let cases = [
            (
                TurnFailure::ProviderRejected {
                    retryable: false,
                    retry_after: None,
                },
                0,
            ),
            (TurnFailure::Configuration, 0),
            (TurnFailure::Authority, 0),
            (TurnFailure::ContextOverflow, 0),
            (
                TurnFailure::PostconditionUnmet {
                    event: "milestone.readiness.evaluated".into(),
                },
                3,
            ),
            (
                TurnFailure::ProviderRejected {
                    retryable: true,
                    retry_after: None,
                },
                3,
            ),
            (TurnFailure::Transient { retry_after: None }, 3),
            (
                TurnFailure::UsageLimit {
                    resets_at: Some(120_000),
                },
                -1,
            ),
            (TurnFailure::UsageLimit { resets_at: None }, -1),
            (TurnFailure::PreProviderAdmission, -2),
            (TurnFailure::EmptyResponse, 3),
            (TurnFailure::Unclassified, 3),
            (
                TurnFailure::TurnLimit {
                    cause: TurnLimitCause::ProviderAttempts,
                },
                3,
            ),
            (
                TurnFailure::TurnLimit {
                    cause: TurnLimitCause::ToolSteps,
                },
                3,
            ),
            (
                TurnFailure::TurnLimit {
                    cause: TurnLimitCause::Time,
                },
                3,
            ),
            (
                TurnFailure::TurnLimit {
                    cause: TurnLimitCause::Output,
                },
                3,
            ),
        ];
        for (failure, budget) in cases {
            for attempt in 1..=4 {
                for admission_failures in 0..=3 {
                    let decision = failure_after_claim(
                        &failure,
                        attempt,
                        3,
                        admission_failures,
                        UsageLimitDeferrals::default(),
                        at(100),
                        "detail",
                    );
                    let expected = match budget {
                        -1 => TurnRetryDecision::Defer,
                        -2 if admission_failures + 1 < MAX_PRE_PROVIDER_FAILURES => {
                            TurnRetryDecision::Defer
                        }
                        -2 => TurnRetryDecision::Fail,
                        limit if attempt < limit => TurnRetryDecision::Retry,
                        _ => TurnRetryDecision::Fail,
                    };
                    assert_eq!(
                        decision.retry_decision, expected,
                        "{failure:?}, attempt {attempt}, pre-provider {admission_failures}"
                    );
                    assert_eq!(
                        decision.attempt_count,
                        if budget < 0 { attempt - 1 } else { attempt }
                    );
                    assert_eq!(
                        decision.next_attempt_at.is_some(),
                        expected != TurnRetryDecision::Fail
                    );
                    if let Some(next) = decision.next_attempt_at {
                        assert!(next > at(100));
                    }
                }
            }
        }
    }

    #[test]
    fn provider_hints_have_backoff_floor_and_six_hour_ceiling() {
        let decide = |failure| {
            failure_after_claim(
                &failure,
                1,
                3,
                0,
                UsageLimitDeferrals::default(),
                at(100),
                "detail",
            )
        };
        assert_eq!(
            decide(TurnFailure::Transient {
                retry_after: Some(20_000)
            })
            .next_attempt_at,
            Some(at(120))
        );
        assert_eq!(
            decide(TurnFailure::ProviderRejected {
                retryable: true,
                retry_after: Some(20_000)
            })
            .next_attempt_at,
            Some(at(120))
        );
        assert_eq!(
            decide(TurnFailure::UsageLimit {
                resets_at: Some(120_000)
            })
            .next_attempt_at,
            Some(at(160))
        );
        assert_eq!(
            decide(TurnFailure::UsageLimit {
                resets_at: Some(99_000)
            })
            .next_attempt_at,
            Some(at(1000))
        );
        assert_eq!(
            decide(TurnFailure::UsageLimit { resets_at: None }).next_attempt_at,
            Some(at(1000))
        );
        assert_eq!(
            decide(TurnFailure::UsageLimit {
                resets_at: Some(u64::MAX - 1)
            })
            .next_attempt_at,
            Some(at(1000))
        );
        assert_eq!(
            decide(TurnFailure::UsageLimit {
                resets_at: Some(99_000_000)
            })
            .next_attempt_at,
            Some(at(21700))
        );
    }

    #[test]
    fn capacity_deferrals_escalate_and_stop_by_count_or_elapsed_time() {
        for (count, minutes) in [(0, 1), (1, 5), (2, 15), (3, 30), (4, 60), (10, 60)] {
            let decision = failure_after_claim(
                &TurnFailure::UsageLimit {
                    resets_at: Some(101_000),
                },
                1,
                3,
                0,
                UsageLimitDeferrals {
                    count,
                    first_at: Some(at(100)),
                },
                at(100),
                "capacity",
            );
            assert_eq!(
                decision.next_attempt_at,
                Some(at(100) + Duration::minutes(minutes))
            );
            assert_eq!(decision.attempt_count, 0);
            assert_eq!(decision.usage_limit_deferral_count, count + 1);
        }
        for history in [
            UsageLimitDeferrals {
                count: MAX_USAGE_LIMIT_DEFERRALS - 1,
                first_at: Some(at(100)),
            },
            UsageLimitDeferrals {
                count: 1,
                first_at: Some(at(100) - Duration::hours(24)),
            },
        ] {
            let decision = failure_after_claim(
                &TurnFailure::UsageLimit { resets_at: None },
                1,
                3,
                0,
                history,
                at(100),
                "capacity",
            );
            assert_eq!(decision.retry_decision, TurnRetryDecision::Fail);
            assert_eq!(decision.attempt_count, 0);
            assert!(decision.next_attempt_at.is_none());
        }
    }

    #[test]
    fn claim_accepts_only_live_queued_or_retry_jobs() {
        let now = at(100);
        let lease_until = at(220);
        assert_eq!(
            claim(
                AgentChatTurnStatus::Queued,
                0,
                3,
                now,
                lease_until,
                "worker-1"
            )
            .expect("queued claim")
            .status,
            AgentChatTurnStatus::Leased
        );
        assert!(claim(
            AgentChatTurnStatus::Succeeded,
            0,
            3,
            now,
            lease_until,
            "worker-1"
        )
        .is_none());
        assert!(claim(
            AgentChatTurnStatus::Queued,
            3,
            3,
            now,
            lease_until,
            "worker-1"
        )
        .is_none());
    }

    #[test]
    fn failures_use_finite_exponential_backoff_then_terminal_state() {
        let first = failure_after_claim(
            &TurnFailure::Unclassified,
            1,
            3,
            0,
            UsageLimitDeferrals::default(),
            at(100),
            "temporary\nprovider failure",
        );
        assert_eq!(first.status, AgentChatTurnStatus::RetryWait);
        assert_eq!(first.attempt_count, 1);
        assert_eq!(first.next_attempt_at, Some(at(105)));
        assert_eq!(first.error, "temporaryprovider failure");

        let second = failure_after_claim(
            &TurnFailure::Unclassified,
            first.attempt_count + 1,
            3,
            0,
            UsageLimitDeferrals::default(),
            at(105),
            "again",
        );
        assert_eq!(second.next_attempt_at, Some(at(115)));
        let terminal = failure_after_claim(
            &TurnFailure::Unclassified,
            second.attempt_count + 1,
            3,
            0,
            UsageLimitDeferrals::default(),
            at(115),
            "last",
        );
        assert_eq!(terminal.status, AgentChatTurnStatus::Failed);
        assert_eq!(terminal.attempt_count, 3);
        assert!(terminal.next_attempt_at.is_none());
    }

    #[test]
    fn failure_after_claim_does_not_charge_attempt_twice() {
        let decision = failure_after_claim(
            &TurnFailure::Unclassified,
            1,
            3,
            0,
            UsageLimitDeferrals::default(),
            at(100),
            "backend",
        );
        assert_eq!(decision.status, AgentChatTurnStatus::RetryWait);
        assert_eq!(decision.attempt_count, 1);
        assert_eq!(decision.next_attempt_at, Some(at(105)));

        let terminal = failure_after_claim(
            &TurnFailure::Unclassified,
            3,
            3,
            0,
            UsageLimitDeferrals::default(),
            at(100),
            "backend",
        );
        assert_eq!(terminal.status, AgentChatTurnStatus::Failed);
        assert_eq!(terminal.attempt_count, 3);
        assert!(terminal.next_attempt_at.is_none());
    }

    #[test]
    fn expired_lease_recovery_is_deterministic_and_does_not_reinvoke_model() {
        let recovered = recover_expired(AgentChatTurnStatus::Leased, Some(at(99)), 2, 3, at(100))
            .expect("expired lease is recoverable");
        assert_eq!(recovered.status, AgentChatTurnStatus::RetryWait);
        assert_eq!(recovered.attempt_count, 2);
        assert_eq!(recovered.next_attempt_at, Some(at(110)));
        assert!(
            recover_expired(AgentChatTurnStatus::Leased, Some(at(101)), 0, 3, at(100),).is_none()
        );
    }

    #[test]
    fn bounded_error_is_safe_for_visible_turn_state() {
        let error = bounded_error(&format!("{}\nsecret", "x".repeat(600)));
        assert_eq!(error.len(), 512);
        assert!(!error.contains('\n'));
    }
}
