//! Provider, lifecycle, runtime, and cleanup failure classification tests.

use super::*;
use agent_runtime::core::provider::{ProviderError, ProviderErrorKind};
use api_types::TurnFailure;

#[test]
fn terminal_cleanup_preserves_primary_and_successful_turn_reports_cleanup_failure() {
    for error in [
        RuntimeError::conflict("fenced"),
        RuntimeError::internal("secondary"),
    ] {
        assert!(native_turn_cleanup(true, vec![("shutdown", Err(error))]).is_ok());
    }
    let error = native_turn_cleanup(
        false,
        vec![
            (
                "persist",
                Err(RuntimeError::internal("primary persistence failure")),
            ),
            (
                "shutdown",
                Err(RuntimeError::conflict("secondary shutdown failure")),
            ),
        ],
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "runtime failed: Internal: primary persistence failure"
    );
}

#[test]
fn provider_rejections_keep_forge_classification_after_coarse_kind_change() {
    use agent_runtime::core::{
        error::{ErrorKind, FailureClass, FailureStage},
        metadata::MetaValue,
    };
    for (provider_kind, runtime_kind) in [
        (ProviderErrorKind::BadRequest, ErrorKind::Provider),
        (ProviderErrorKind::Unsupported, ErrorKind::Config),
    ] {
        for retryable in [false, true] {
            let mut original = ProviderError::new(provider_kind, "checked rejection");
            original
                .metadata
                .insert("provider.detail", "invalid_request: bad argument");
            original.metadata.insert("http.status", 400_i64);
            if retryable {
                original = original.retry_after(1200);
            }
            let expected = if retryable {
                TurnFailure::ProviderRejected {
                    retryable: true,
                    retry_after: Some(1200),
                }
            } else {
                TurnFailure::ProviderSchema
            };
            assert_eq!(provider_turn_failure(&original), expected);
            let converted =
                RuntimeError::from(original).with_class(FailureClass::RequestRejected {
                    stage: FailureStage::Provider,
                });
            assert_eq!(converted.kind, runtime_kind);
            assert_eq!(
                converted.metadata.get("http.status"),
                Some(&MetaValue::Int(400))
            );
            assert_eq!(runtime_turn_failure(&converted), expected);
            assert!(
                !serde_json::to_string(&expected)
                    .unwrap()
                    .contains("bad argument")
            );
        }
    }
}

#[test]
fn lifecycle_and_runtime_failures_keep_only_typed_evidence() {
    use agent_runtime::core::error::ErrorKind;
    for error in [
        AgentHostError::AgentPaused {
            agent_id: "agent".into(),
        },
        AgentHostError::ProjectPaused {
            project_id: "project".into(),
        },
    ] {
        assert_eq!(error.turn_failure(), TurnFailure::Authority);
    }
    // Session-open failures must remain Runtime for Task dispatch handling.
    assert!(matches!(
        host_runtime_error(RuntimeError::new(
            ErrorKind::Internal,
            "session open failed"
        )),
        AgentHostError::Runtime(_)
    ));
    for message in ["compaction planning failed", "configuration invalid"] {
        assert_eq!(
            runtime_turn_failure(&RuntimeError::new(ErrorKind::Config, message)),
            TurnFailure::Unclassified
        );
    }
    assert_eq!(
        runtime_turn_failure(&RuntimeError::new(ErrorKind::Cancelled, "shutdown")),
        TurnFailure::Unclassified
    );
    assert_eq!(
        runtime_turn_failure(&RuntimeError::new(ErrorKind::Approval, "paused")),
        TurnFailure::Authority
    );
}

#[test]
fn runtime_failure_classes_use_existing_projections_without_granting_retries() {
    use agent_runtime::core::{
        error::{ErrorKind, FailureClass, FailureComponent, FailureStage},
        event::LimitKind,
    };
    let stage = FailureStage::PreProvider;
    let component = FailureComponent::Lcm;
    let cases = [
        (FailureClass::PolicyDenied { stage }, TurnFailure::Authority),
        (
            FailureClass::ContextOverflow {
                stage,
                required_tokens: Some(100),
                available_tokens: Some(50),
            },
            TurnFailure::ContextOverflow,
        ),
        (FailureClass::Auth { stage }, TurnFailure::ProviderAuth),
        (
            FailureClass::QuotaExhausted { stage },
            TurnFailure::UsageLimit { resets_at: Some(0) },
        ),
        (
            FailureClass::RequestRejected {
                stage: FailureStage::Provider,
            },
            TurnFailure::ProviderSchema,
        ),
        (
            FailureClass::TurnLimit {
                stage,
                limit: LimitKind::ToolSteps,
            },
            TurnFailure::TurnLimit {
                cause: api_types::TurnLimitCause::ToolSteps,
            },
        ),
        (FailureClass::Transient { stage }, TurnFailure::Unclassified),
        (
            FailureClass::RateLimited { stage },
            TurnFailure::Unclassified,
        ),
        (
            FailureClass::RequestRejected { stage },
            TurnFailure::Unclassified,
        ),
        (
            FailureClass::StateConflict { stage, component },
            TurnFailure::Unclassified,
        ),
        (
            FailureClass::HostComponent { stage, component },
            TurnFailure::Unclassified,
        ),
        (FailureClass::Cancelled { stage }, TurnFailure::Unclassified),
        (FailureClass::Internal { stage }, TurnFailure::Unclassified),
        (FailureClass::Unclassified, TurnFailure::Unclassified),
    ];
    for (class, expected) in cases {
        let mut error =
            RuntimeError::new(ErrorKind::Config, "SECRET_TOKEN=opaque").with_class(class);
        error.retry_after_ms = Some(0);
        error.limit_resets_at_ms = Some(0);
        let failure = runtime_turn_failure(&error);
        assert_eq!(failure, expected, "{:?}", error.class);
        assert!(
            !serde_json::to_string(&failure)
                .unwrap()
                .contains("SECRET_TOKEN")
        );
    }
    // Typed transience does not override the runtime's local admission
    // outcome, even if retryability is present in diagnostic evidence.
    let mut local = RuntimeError::new(ErrorKind::Config, "local failure")
        .with_class(FailureClass::Transient { stage });
    local.retryable = true;
    assert_eq!(runtime_turn_failure(&local), TurnFailure::Unclassified);
    for class in [
        FailureClass::Transient { stage },
        FailureClass::RateLimited { stage },
    ] {
        let mut error =
            RuntimeError::new(ErrorKind::Provider, "provider failure").with_class(class);
        assert_eq!(runtime_turn_failure(&error), TurnFailure::Unclassified);
        error.retryable = true;
        error.retry_after_ms = Some(1200);
        assert_eq!(
            runtime_turn_failure(&error),
            TurnFailure::Transient {
                retry_after: Some(1200)
            }
        );
    }
    // Future wire reasons become unclassified and keep the coarse policy.
    let future: RuntimeError = serde_json::from_value(serde_json::json!({
        "kind": "conflict", "message": "opaque", "retryable": false,
        "class": {"reason": "future_reason", "stage": "future_stage"},
    }))
    .unwrap();
    assert_eq!(runtime_turn_failure(&future), TurnFailure::Unclassified);
}

#[test]
fn provider_failure_fields_survive_without_message_inference() {
    let cases = [
        (
            ProviderError::new(
                ProviderErrorKind::BadRequest,
                "usage limit config credential",
            ),
            TurnFailure::ProviderSchema,
        ),
        (
            ProviderError::new(ProviderErrorKind::BadRequest, "rejected").retry_after(1200),
            TurnFailure::ProviderRejected {
                retryable: true,
                retry_after: Some(1200),
            },
        ),
        (
            ProviderError::new(ProviderErrorKind::LimitExhausted, "x").limit_resets_at(12345),
            TurnFailure::UsageLimit {
                resets_at: Some(12345),
            },
        ),
        (
            ProviderError::new(ProviderErrorKind::Network, "x").retry_after(900),
            TurnFailure::Transient {
                retry_after: Some(900),
            },
        ),
        (
            ProviderError::new(ProviderErrorKind::Auth, "x"),
            TurnFailure::ProviderAuth,
        ),
        (
            ProviderError::new(ProviderErrorKind::MalformedStream, "config usage limit"),
            TurnFailure::Unclassified,
        ),
    ];
    for (error, expected) in cases {
        assert_eq!(provider_turn_failure(&error), expected);
    }
}
