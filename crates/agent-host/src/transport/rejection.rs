//! Checked diagnostics for Forge's custom HTTP transport. The runtime repeats
//! this check at the provider boundary; the transport must also be safe before
//! an adapter sees the error (including in logs and direct callers).
use agent_runtime::{
    core::{provider::ProviderError, store::Secret},
    provider::transport::HttpRequest,
};

const MAX_REASON_CHARS: usize = 300;
pub(super) const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;
/// Shorter sent values (`Bearer`, a short selector) only match verbatim.
const MIN_FOLDED_SECRET_CHARS: usize = 8;
/// A reason holding this many consecutive characters of a sent value reflects
/// it: a gateway that echoes a truncated key still exposes most of it, while a
/// masked key (`sk-proj-****…abcd`) shows far fewer.
const PARTIAL_ECHO_CHARS: usize = 16;

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
            // The value as sent, still percent-encoded.
            values.extend(url.query().into_iter().flat_map(|query| {
                query
                    .split('&')
                    .filter_map(|pair| pair.split_once('=').map(|(_, value)| value.to_owned()))
            }));
        }
        // A provider may echo a value URL-encoded.
        let encoded: Vec<String> = values
            .iter()
            .map(|value| url::form_urlencoded::byte_serialize(value.as_bytes()).collect())
            .filter(|encoded: &String| !values.contains(encoded))
            .collect();
        values.extend(encoded);
        Self(
            values
                .into_iter()
                .filter(|value| !value.is_empty())
                .map(Secret::new)
                .collect(),
        )
    }

    /// Whether `text` echoes a sent value verbatim, or a long one with
    /// whitespace, invisible characters or ASCII case changed, or truncated.
    fn reflects(&self, text: &str) -> bool {
        let folded = fold(text);
        self.0.iter().any(|secret| {
            let secret = secret.expose();
            if text.contains(secret) {
                return true;
            }
            let secret: Vec<char> = fold(secret).chars().collect();
            if secret.len() < MIN_FOLDED_SECRET_CHARS {
                return false;
            }
            secret
                .windows(PARTIAL_ECHO_CHARS.min(secret.len()))
                .any(|run| folded.contains(&run.iter().collect::<String>()))
        })
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
        if self.reflects(&raw) {
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
        if normalized.is_empty() || self.reflects(normalized) {
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

/// Drops whitespace, control and default-ignorable characters and folds ASCII
/// case, so a split, invisibly joined or case-changed echo compares equal.
fn fold(text: &str) -> String {
    text.chars()
        .filter(|ch| !ch.is_whitespace() && !ch.is_control() && !is_default_ignorable(*ch))
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
}

fn is_default_ignorable(ch: char) -> bool {
    matches!(
        ch,
        '\u{00AD}'
            | '\u{034F}'
            | '\u{061C}'
            | '\u{115F}'
            | '\u{1160}'
            | '\u{17B4}'
            | '\u{17B5}'
            | '\u{180B}'..='\u{180F}'
            | '\u{200B}'..='\u{200F}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2060}'..='\u{206F}'
            | '\u{3164}'
            | '\u{FE00}'..='\u{FE0F}'
            | '\u{FEFF}'
            | '\u{FFA0}'
            | '\u{1BCA0}'..='\u{1BCA3}'
            | '\u{1D173}'..='\u{1D17A}'
            | '\u{E0000}'..='\u{E0FFF}'
    )
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

    const KEY: &str = "sk-proj-A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8";
    const QUERY: &str = "q/9+Zt3=Vx7Lm2Np4Rs6";

    fn echo_diagnostics() -> RequestDiagnostics {
        RequestDiagnostics::new(&HttpRequest {
            url: "https://example.test/v1?key=q%2F9%2BZt3%3DVx7Lm2Np4Rs6".into(),
            headers: vec![
                ("Authorization".into(), format!("Bearer {KEY}")),
                ("chatgpt-account-id".into(), "acct-7f3a9c2e1b4d".into()),
                ("Content-Type".into(), "application/json".into()),
            ],
            body: vec![],
        })
    }

    fn detail(diagnostics: &RequestDiagnostics, message: &str) -> Option<String> {
        let body = serde_json::to_vec(&serde_json::json!({"error":{"message":message}})).unwrap();
        diagnostics.checked_reason(&body)
    }

    #[test]
    fn credential_echoes_in_other_shapes_are_withheld() {
        let diagnostics = echo_diagnostics();
        let (head, tail) = KEY.split_at(20);
        let leaked: Vec<String> = [
            // Split across a line, a space or a tab.
            format!("Invalid key {head}\n{tail}"),
            format!("Invalid key {head} {tail}"),
            format!("Invalid key {head}\t{tail}"),
            // Invisible characters render the key intact.
            format!("Invalid key {head}\u{200B}{tail}"),
            format!("Invalid key {head}\u{00AD}{tail}"),
            format!("Invalid key {head}\u{FEFF}{tail}"),
            // Case-folded echo.
            format!("invalid key {}", KEY.to_ascii_uppercase()),
            // A gateway that truncates the key it echoes.
            format!("Invalid key {}...", &KEY[..24]),
            format!("Invalid key ...{}", &KEY[KEY.len() - 20..]),
            // The query credential as sent (percent-encoded) and decoded.
            "Bad request to /v1?key=q%2F9%2BZt3%3DVx7Lm2Np4Rs6".into(),
            format!("Bad key {QUERY}"),
            // The account selector header.
            "Account acct-7f3a9c2e1b4d is not entitled to this model".into(),
        ]
        .into_iter()
        .filter_map(|message| detail(&diagnostics, &message))
        .collect();
        assert!(leaked.is_empty(), "{leaked:#?}");
    }

    #[test]
    fn masked_keys_and_ordinary_reasons_still_pass() {
        let diagnostics = echo_diagnostics();
        for message in [
            format!(
                "Incorrect API key provided: sk-proj-{}{}",
                "*".repeat(30),
                &KEY[KEY.len() - 4..]
            ),
            "Incorrect API key provided: sk-...Q7r8".to_owned(),
            "Missing bearer authentication in header".to_owned(),
            "Unsupported content type application/json; charset=utf-8".to_owned(),
            "INVALID_ARGUMENT: Request contains an invalid argument.".to_owned(),
        ] {
            assert_eq!(
                detail(&diagnostics, &message).as_deref(),
                Some(message.as_str())
            );
        }
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
