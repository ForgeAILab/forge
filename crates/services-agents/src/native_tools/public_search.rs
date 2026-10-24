//! Native public search requests, result validation, and HTTP clients.

use super::*;

impl CoordinationToolProvider {
    pub(super) async fn run_public_search(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        search_scope: PublicSearchScope,
        query: &str,
        limit: u64,
    ) -> Result<Value, AgentHostError> {
        if query.trim().is_empty() || query.chars().count() > 512 {
            return Err(invalid_arguments(
                "search query must contain 1 to 512 characters".to_owned(),
            ));
        }
        if !(1..=10).contains(&limit) {
            return Err(invalid_arguments(
                "search result limit must be between 1 and 10".to_owned(),
            ));
        }

        // Re-authorize the role and derive the account/Project from the
        // authenticated scope before any network request.  Model-provided
        // identifiers are intentionally not accepted here.
        match search_scope {
            PublicSearchScope::Main => {
                self.authorization
                    .main_account_id(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
            }
            PublicSearchScope::Project => {
                self.authorization
                    .project_orchestration_target(actor_identity_id, scope)
                    .await
                    .map_err(native_scope_error)?;
            }
        }

        let config = self.public_search_config().ok_or_else(|| {
            AgentHostError::Configuration("public web search is not configured".to_owned())
        })?;
        config.validate().map_err(|_| {
            AgentHostError::Configuration("configured public search limits are invalid".to_owned())
        })?;
        let endpoint = config.endpoint.ok_or_else(|| {
            AgentHostError::Configuration("public web search is not configured".to_owned())
        })?;
        let mut endpoint = url::Url::parse(&endpoint).map_err(|_| {
            AgentHostError::Configuration("configured public search endpoint is invalid".to_owned())
        })?;
        if endpoint.scheme() != "https"
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint
                .host_str()
                .is_some_and(is_private_or_local_search_host)
        {
            return Err(AgentHostError::Configuration(
                "configured public search endpoint must be a public HTTPS URL without credentials"
                    .to_owned(),
            ));
        }
        endpoint
            .query_pairs_mut()
            .append_pair("q", query.trim())
            .append_pair("limit", &limit.to_string());

        let client = public_search_http_client(
            &endpoint,
            Duration::from_millis(config.timeout_ms),
            &ProxyEnvironment::from_env(),
        )
        .await?;
        let response = client
            .get(endpoint)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| AgentHostError::Runtime("public search request failed".to_owned()))?;
        if !response.status().is_success() {
            return Err(AgentHostError::Runtime(
                "public search endpoint returned an error".to_owned(),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > config.max_response_bytes)
        {
            return Err(AgentHostError::Runtime(
                "public search response is too large".to_owned(),
            ));
        }
        let mut body = Vec::new();
        let mut response = response;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| AgentHostError::Runtime("public search response failed".to_owned()))?
        {
            if body.len().saturating_add(chunk.len()) > config.max_response_bytes as usize {
                return Err(AgentHostError::Runtime(
                    "public search response is too large".to_owned(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let parsed: PublicSearchResponse = serde_json::from_slice(&body).map_err(|_| {
            AgentHostError::Runtime("public search response is not valid bounded JSON".to_owned())
        })?;
        let truncated = parsed.results.len() > limit as usize;
        let retrieved_at = Utc::now().to_rfc3339();
        let results = parsed
            .results
            .into_iter()
            .take(limit as usize)
            .map(|result| {
                let url = normalize_public_result_url(&result.url)?;
                Ok(json!({
                    "url": url,
                    "title": bounded_untrusted_text(&result.title, 512),
                    "snippet": bounded_untrusted_text(&result.snippet, 2048),
                    "retrieved_at": retrieved_at,
                    "untrusted": true,
                }))
            })
            .collect::<Result<Vec<_>, AgentHostError>>()?;
        let result_count = results.len();
        Ok(json!({
            "scope": match search_scope {
                PublicSearchScope::Main => "main",
                PublicSearchScope::Project => "project",
            },
            "query": query.trim(),
            "results": results,
            "result_count": result_count,
            "truncated": truncated,
            "content_trust": "untrusted_external_data",
            "instructions_are_data": true,
            "materialized": false,
            "persisted": false,
        }))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicSearchResponse {
    results: Vec<PublicSearchResult>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicSearchResult {
    url: String,
    title: String,
    snippet: String,
}

fn normalize_public_result_url(value: &str) -> Result<String, AgentHostError> {
    if value.chars().count() > 2048 {
        return Err(AgentHostError::Runtime(
            "public search result URL is too long".to_owned(),
        ));
    }
    // URL values are untrusted endpoint data.  Reject control characters
    // before parsing/serializing so logs, rendered links, and downstream
    // clients cannot receive a delimiter or terminal injection payload.
    if value.chars().any(char::is_control) {
        return Err(AgentHostError::Runtime(
            "public search result URL contains control characters".to_owned(),
        ));
    }
    let parsed = url::Url::parse(value)
        .map_err(|_| AgentHostError::Runtime("public search result URL is invalid".to_owned()))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
        || parsed
            .host_str()
            .is_some_and(is_private_or_local_search_host)
    {
        return Err(AgentHostError::Runtime(
            "public search result URL is not a public HTTP(S) URL".to_owned(),
        ));
    }
    Ok(parsed.to_string())
}

fn is_private_or_local_search_host(host: &str) -> bool {
    let normalized = host
        .trim_end_matches('.')
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    // Zone identifiers are local-interface selectors (for example
    // `fe80::1%25en0`), not public DNS/HTTP hosts.  Reject them before the
    // `IpAddr` parser can treat the value as an opaque hostname.
    if normalized.contains('%') {
        return true;
    }
    if is_restricted_public_hostname(&normalized) {
        return true;
    }
    let Ok(address) = normalized.parse::<IpAddr>() else {
        // Hostnames are checked again by the request-time resolver.  Result
        // URLs are metadata only, so reject known local names immediately.
        return false;
    };
    is_blocked_public_search_address(address)
}

async fn public_search_http_client(
    endpoint: &url::Url,
    timeout: Duration,
    proxies: &ProxyEnvironment,
) -> Result<reqwest::Client, AgentHostError> {
    public_search_http_client_with_builder(
        timeout,
        public_http_client_builder(endpoint, PublicAddressPolicy::Search, proxies),
    )
    .await
}

/// Keep resolution and the subsequent request within one budget. The future
/// seam lets tests spend resolution time without performing external DNS.
async fn public_search_http_client_with_builder(
    timeout: Duration,
    builder: impl std::future::Future<Output = Result<reqwest::ClientBuilder, PublicHttpError>>,
) -> Result<reqwest::Client, AgentHostError> {
    let started = tokio::time::Instant::now();
    let builder = tokio::time::timeout(timeout, builder)
        .await
        .map_err(|_| AgentHostError::Runtime("public search resolution timed out".to_owned()))?
        .map_err(public_search_egress_refusal)?;
    let remaining = timeout.saturating_sub(started.elapsed());
    if remaining.is_zero() {
        return Err(AgentHostError::Runtime(
            "public search resolution timed out".to_owned(),
        ));
    }
    builder
        .timeout(remaining)
        .build()
        .map_err(|_| AgentHostError::Configuration("public search client unavailable".to_owned()))
}

/// A lookup failure is a runtime failure of this request, as it was when
/// resolution ran inside the client; only a refused answer is configuration.
fn public_search_egress_refusal(error: PublicHttpError) -> AgentHostError {
    match error {
        PublicHttpError::ResolutionFailed => {
            AgentHostError::Runtime("public search endpoint could not be resolved".to_owned())
        }
        PublicHttpError::UnusableProxy => AgentHostError::Configuration(error.to_string()),
        _ => AgentHostError::Configuration(
            "public search endpoint must resolve exclusively to public addresses".to_owned(),
        ),
    }
}

#[cfg(test)]
mod proxy_tests;
#[cfg(test)]
mod tests;
