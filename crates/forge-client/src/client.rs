use anyhow::{anyhow, Result};
use api_types::PromptPreviewResponse;
use reqwest::{multipart::Form, StatusCode};
use serde::{de::DeserializeOwned, Serialize};

use crate::auth::{normalize_server_url, stored_token_for_server};

#[derive(Clone)]
pub struct ForgeClient {
    base_url: String,
    http: reqwest::Client,
    bearer_token: Option<String>,
}

impl ForgeClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url = normalize_server_url(&base_url.into());
        let bearer_token = stored_token_for_server(&base_url).ok().flatten();
        Self {
            base_url,
            http: reqwest::Client::new(),
            bearer_token,
        }
    }

    pub fn new_without_credentials(base_url: impl Into<String>) -> Self {
        Self {
            base_url: normalize_server_url(&base_url.into()),
            http: reqwest::Client::new(),
            bearer_token: None,
        }
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .apply_auth(self.http.get(self.url(path)))
            .send()
            .await?;
        decode_json(response).await
    }

    pub async fn prompt_preview(
        &self,
        task_id: &str,
        role: &str,
        trigger: Option<&str>,
    ) -> Result<PromptPreviewResponse> {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("role", role);
        if let Some(trigger) = trigger {
            query.append_pair("trigger", trigger);
        }
        self.get(&format!(
            "/api/v1/tasks/{task_id}/prompt-preview?{}",
            query.finish()
        ))
        .await
    }

    pub async fn post<B: Serialize, T: DeserializeOwned>(&self, path: &str, body: &B) -> Result<T> {
        let response = self
            .apply_auth(self.http.post(self.url(path)))
            .json(body)
            .send()
            .await?;
        decode_json(response).await
    }

    pub async fn post_empty<B: Serialize>(&self, path: &str, body: &B) -> Result<()> {
        let response = self
            .apply_auth(self.http.post(self.url(path)))
            .json(body)
            .send()
            .await?;
        decode_empty(response).await
    }

    pub async fn patch<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let response = self
            .apply_auth(self.http.patch(self.url(path)))
            .json(body)
            .send()
            .await?;
        decode_json(response).await
    }

    pub async fn put<B: Serialize, T: DeserializeOwned>(&self, path: &str, body: &B) -> Result<T> {
        let response = self
            .apply_auth(self.http.put(self.url(path)))
            .json(body)
            .send()
            .await?;
        decode_json(response).await
    }

    pub async fn post_multipart<T: DeserializeOwned>(&self, path: &str, form: Form) -> Result<T> {
        let response = self
            .apply_auth(self.http.post(self.url(path)))
            .multipart(form)
            .send()
            .await?;
        decode_json(response).await
    }

    pub async fn post_bearer<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        token: &str,
        body: &B,
    ) -> Result<T> {
        let response = self
            .http
            .post(self.url(path))
            .bearer_auth(token)
            .json(body)
            .send()
            .await?;
        decode_json(response).await
    }

    /// Relays a provider's browser OAuth callback to the server. The response
    /// is a redirect meant for a browser, so redirects are not followed and
    /// only the status is inspected.
    pub async fn relay_provider_callback(&self, path: &str) -> Result<()> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let response = http.get(self.url(path)).send().await?;
        let status = response.status();
        if status.is_success() || status.is_redirection() {
            return Ok(());
        }
        Err(request_error(
            status,
            response.text().await.unwrap_or_default(),
        ))
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let response = self
            .apply_auth(self.http.delete(self.url(path)))
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        Err(request_error(
            status,
            response.text().await.unwrap_or_default(),
        ))
    }

    pub async fn delete_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .apply_auth(self.http.delete(self.url(path)))
            .send()
            .await?;
        decode_json(response).await
    }

    pub fn url(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        }
    }

    pub fn bearer_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    fn apply_auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = self.bearer_token() {
            builder.bearer_auth(token)
        } else {
            builder
        }
    }
}

async fn decode_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    let body = response.bytes().await?;
    if !status.is_success() {
        return Err(request_error(status, String::from_utf8_lossy(&body).into()));
    }

    serde_json::from_slice(&body).map_err(Into::into)
}

async fn decode_empty(response: reqwest::Response) -> Result<()> {
    let status = response.status();
    let body = response.bytes().await?;
    if status.is_success() {
        return Ok(());
    }
    Err(request_error(status, String::from_utf8_lossy(&body).into()))
}

#[derive(Debug)]
pub struct ActionUnavailable {
    pub response: serde_json::Value,
}
impl std::fmt::Display for ActionUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "action_unavailable")
    }
}
impl std::error::Error for ActionUnavailable {}

fn request_error(status: StatusCode, body: String) -> anyhow::Error {
    if status == StatusCode::CONFLICT {
        if let Ok(response) = serde_json::from_str::<serde_json::Value>(&body) {
            if response.get("code").and_then(serde_json::Value::as_str)
                == Some("action_unavailable")
            {
                return ActionUnavailable { response }.into();
            }
        }
    }
    if body.trim().is_empty() {
        anyhow!("request failed with status {status}")
    } else {
        anyhow!("request failed with status {status}: {body}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unavailable_actions_are_typed_but_stale_versions_remain_generic() {
        let unavailable = request_error(
            StatusCode::CONFLICT,
            r#"{"code":"action_unavailable","details":{"available_actions":[]}}"#.into(),
        );
        assert!(unavailable.is::<ActionUnavailable>());
        assert!(!request_error(
            StatusCode::CONFLICT,
            r#"{"code":"version_conflict"}"#.into()
        )
        .is::<ActionUnavailable>());
    }
}
