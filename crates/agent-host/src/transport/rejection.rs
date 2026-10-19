//! Checked diagnostics for Forge's custom HTTP transport. The runtime repeats
//! this check at the provider boundary; the transport must also be safe before
//! an adapter sees the error (including in logs and direct callers).
use agent_runtime::{
    core::{provider::ProviderError, store::Secret},
    provider::transport::HttpRequest,
};

const MAX_REASON_CHARS: usize = 300;
pub(super) const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;

#[derive(Debug)]
pub(super) struct RequestDiagnostics(Vec<Secret>);

impl RequestDiagnostics {
    pub(super) fn new(request: &HttpRequest) -> Self {
        let mut values = Vec::new();
        for (name, value) in &request.headers {
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "accept"
                    | "accept-encoding"
                    | "content-type"
                    | "content-length"
                    | "user-agent"
                    | "anthropic-version"
                    | "anthropic-beta"
            ) {
                continue;
            }
            values.push(value.clone());
            values.extend(value.split_whitespace().map(str::to_owned));
        }
        if let Ok(url) = url::Url::parse(&request.url) {
            values.extend(url.query_pairs().map(|(_, value)| value.into_owned()));
        }
        Self(
            values
                .into_iter()
                .filter(|value| !value.is_empty())
                .map(Secret::new)
                .collect(),
        )
    }

    fn checked_reason(&self, body: &[u8]) -> Option<String> {
        let from = |value: serde_json::Value| value.get("error").cloned();
        let error = serde_json::from_slice(body)
            .ok()
            .and_then(from)
            .or_else(|| {
                std::str::from_utf8(body).ok()?.lines().find_map(|line| {
                    from(serde_json::from_str(line.strip_prefix("data:")?.trim()).ok()?)
                })
            })?;
        let label = error
            .get("status")
            .or_else(|| error.get("code"))
            .and_then(|v| v.as_str());
        let message = error.get("message").and_then(|v| v.as_str());
        let raw = match (label, message) {
            (Some(label), Some(message)) => format!("{label}: {message}"),
            (Some(label), None) => label.to_owned(),
            (None, Some(message)) => message.to_owned(),
            (None, None) => return None,
        };
        let reflects = |text: &str| self.0.iter().any(|secret| text.contains(secret.expose()));
        if reflects(&raw) {
            return None;
        }
        let normalized: String = raw
            .chars()
            .filter_map(|ch| match ch {
                '\r' | '\n' | '\t' => Some(' '),
                ch if ch.is_control() => None,
                ch => Some(ch),
            })
            .collect();
        let normalized = normalized.trim();
        if normalized.is_empty() || reflects(normalized) {
            return None;
        }
        Some(if normalized.chars().count() > MAX_REASON_CHARS {
            normalized
                .chars()
                .take(MAX_REASON_CHARS - 1)
                .chain(std::iter::once('…'))
                .collect()
        } else {
            normalized.to_owned()
        })
    }

    pub(super) fn attach(
        &self,
        mut error: ProviderError,
        status: u16,
        body: &[u8],
    ) -> ProviderError {
        error.metadata.insert("http.status", i64::from(status));
        if let Some(reason) = self.checked_reason(body) {
            // The provider adapter checks this again before adding it to its
            // fixed message. Never put an unchecked body into error prose.
            error.metadata.insert("provider.detail", reason);
        }
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_runtime::core::{metadata::MetaValue, provider::ProviderErrorKind};

    fn diagnostics() -> RequestDiagnostics {
        RequestDiagnostics::new(&HttpRequest {
            url: "https://example.test?key=query-secret".into(),
            headers: vec![
                ("Authorization".into(), "Bearer sent-credential".into()),
                ("X-Custom-Auth".into(), "custom-secret".into()),
            ],
            body: vec![],
        })
    }

    #[test]
    fn provider_reasons_are_checked_before_truncation_and_after_normalization() {
        for reason in [
            "sent-credential".into(),
            "custom-secret".into(),
            "query-secret".into(),
            format!("{}sent-credential", "x".repeat(400)),
            "sent-cred\u{0001}ential".into(),
        ] {
            let body =
                serde_json::to_vec(&serde_json::json!({"error":{"message":reason}})).unwrap();
            let error = diagnostics().attach(
                ProviderError::new(ProviderErrorKind::BadRequest, "rejected"),
                400,
                &body,
            );
            assert_eq!(error.metadata.get("provider.detail"), None);
            assert_eq!(
                error.metadata.get("http.status"),
                Some(&MetaValue::Int(400))
            );
            for secret in ["sent-credential", "custom-secret", "query-secret"] {
                assert!(!format!("{error:?}").contains(secret));
            }
        }
        assert!(!format!("{:?}", diagnostics()).contains("sent-credential"));
    }

    #[test]
    fn json_and_gemini_sse_reasons_are_bounded_and_keep_error_classification() {
        for body in [
            r#"{"error":{"code":"invalid_request","message":"bad argument"}}"#,
            "event: error\ndata: {\"error\":{\"status\":\"invalid_request\",\"message\":\"bad argument\"}}\n",
        ] {
            let original = super::super::http_rejection(400, body, None);
            let error = diagnostics().attach(original.clone(), 400, body.as_bytes());
            assert_eq!(error.kind, original.kind);
            assert_eq!(error.retryable, original.retryable);
            assert_eq!(error.message, original.message);
            assert_eq!(
                error.metadata.get("provider.detail"),
                Some(&MetaValue::Text("invalid_request: bad argument".into()))
            );
        }
        let body = serde_json::to_vec(&serde_json::json!({"error":{"message":"字\n".repeat(400)}}))
            .unwrap();
        let reason = diagnostics().checked_reason(&body).unwrap();
        assert_eq!(reason.chars().count(), MAX_REASON_CHARS);
        assert!(!reason.contains('\n'));
        assert!(reason.ends_with('…'));
        for body in ["not JSON", r#"{"error":{"debug":"sent-credential"}}"#] {
            assert!(diagnostics().checked_reason(body.as_bytes()).is_none());
        }
    }
}
