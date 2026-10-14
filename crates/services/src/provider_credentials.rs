//! Provider-entry endpoints and their shared URL-shape validation.

use crate::{Result, ServiceError};
use db::CredentialHandle;
use serde_json::Value;
use std::net::IpAddr;

/// Default API endpoint for API-key entries. `openai_compatible` has no
/// default and requires an explicit base URL.
pub fn default_api_key_base_url(provider: &str) -> Option<&'static str> {
    match provider {
        "openai" => Some("https://api.openai.com/v1"),
        "xai" => Some("https://api.x.ai/v1"),
        "gemini" => Some("https://generativelanguage.googleapis.com/v1beta"),
        "openrouter" => Some("https://openrouter.ai/api/v1"),
        _ => None,
    }
}

/// [`entry_base_url`] as the pricing crate asks for it: the endpoint, or `None`
/// when the entry has no usable one.
pub fn provider_entry_endpoint(entry: &CredentialHandle) -> Option<String> {
    entry_base_url(entry).ok()
}

fn default_oauth_base_url(provider: &str) -> Option<&'static str> {
    match provider {
        "openai" => Some("https://chatgpt.com/backend-api/codex"),
        "xai" => Some("https://api.x.ai/v1"),
        "gemini" => Some("https://generativelanguage.googleapis.com/v1beta"),
        _ => None,
    }
}

/// The API endpoint an agent built on this entry talks to: stored entry
/// metadata first, then the provider/method default.
pub fn entry_base_url(entry: &CredentialHandle) -> Result<String> {
    let stored = serde_json::from_str::<Value>(&entry.metadata_json)
        .ok()
        .and_then(|metadata| {
            metadata
                .get("base_url")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    if let Some(base_url) = stored {
        let base_url = base_url.trim();
        validate_provider_url_shape(base_url).map_err(ServiceError::invalid_operation)?;
        return Ok(base_url.to_owned());
    }
    let fallback = if entry.credential_method == "oauth_bundle" {
        default_oauth_base_url(&entry.provider)
    } else {
        default_api_key_base_url(&entry.provider)
    };
    let fallback = fallback.ok_or_else(|| {
        ServiceError::invalid_operation("provider entry has no usable API endpoint")
    })?;
    validate_provider_url_shape(fallback).map_err(ServiceError::invalid_operation)?;
    Ok(fallback.to_owned())
}

/// Validate the URL properties that do not require DNS resolution. This is
/// shared by metadata/profile persistence and the full async provider probe,
/// so a legacy or tampered metadata value cannot bypass the endpoint policy
/// simply because it was read from storage instead of submitted by a caller.
pub(crate) fn validate_provider_url_shape(
    base_url: &str,
) -> std::result::Result<url::Url, &'static str> {
    let parsed = url::Url::parse(base_url).map_err(|_| "base_url must be an absolute URL")?;
    if parsed.username() != ""
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err("base_url must not contain userinfo, a query, or a fragment");
    }
    if parsed.scheme() != "https" {
        return Err("base_url must use https");
    }
    let host = parsed.host().ok_or("base_url must include a host")?;
    let _port = parsed
        .port_or_known_default()
        .ok_or("base_url has no known port")?;
    match host {
        url::Host::Ipv4(address) if restricted_provider_ip(IpAddr::V4(address)) => {
            return Err("base_url must not target a private or local address");
        }
        url::Host::Ipv6(address) if restricted_provider_ip(IpAddr::V6(address)) => {
            return Err("base_url must not target a private or local address");
        }
        url::Host::Domain(domain) if restricted_provider_hostname(domain) => {
            return Err("base_url must not target a local or private hostname");
        }
        _ => {}
    }
    Ok(parsed)
}

fn restricted_provider_hostname(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host.ends_with(".home.arpa")
}

pub(crate) fn restricted_provider_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(address) => {
            let octets = address.octets();
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_broadcast()
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
                || (octets[0] == 198 && (18..=19).contains(&octets[1]))
                || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
                || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
                || octets[0] >= 224
        }
        IpAddr::V6(address) => {
            let segments = address.segments();
            address.is_loopback()
                || address.is_unspecified()
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] & 0xffc0) == 0xfec0
                || (segments[0] & 0xfe00) == 0xfc00
                || segments[0] >= 0xff00
                || address
                    .to_ipv4()
                    .is_some_and(|mapped| restricted_provider_ip(IpAddr::V4(mapped)))
        }
    }
}
