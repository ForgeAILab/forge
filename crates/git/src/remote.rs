use std::path::Path;

use url::Url;

/// Compare repository identities across local, SSH and HTTP transport forms.
/// Host names are case-insensitive; repository paths and non-default ports are not.
pub fn normalize_remote_url(remote: &str) -> String {
    let remote = remote.trim();
    if Path::new(remote).is_absolute() {
        return format!("file:{}", repository_path(remote));
    }
    if let Ok(url) = Url::parse(remote) {
        if url.scheme() == "file" {
            if let Ok(path) = url.to_file_path() {
                return format!("file:{}", repository_path(&path.to_string_lossy()));
            }
        } else if let Some(identity) = network_identity(&url) {
            return identity;
        }
    }
    // Git's scp-like form has no scheme: [user@]host:path.
    if let Some((host, path)) = remote.split_once(':').filter(|_| !remote.contains("://")) {
        if !host.is_empty() && !host.contains(['/', '\\']) && !path.is_empty() {
            if let Ok(url) = Url::parse(&format!("ssh://{host}/{}", path.trim_start_matches('/'))) {
                if let Some(identity) = network_identity(&url) {
                    return identity;
                }
            }
        }
    }
    repository_path(remote).to_owned()
}

fn network_identity(url: &Url) -> Option<String> {
    let default_port = match url.scheme() {
        "http" => 80,
        "https" => 443,
        "ssh" => 22,
        "git" => 9418,
        _ => return None,
    };
    let host = url.host_str()?.to_ascii_lowercase();
    let port = url
        .port()
        .filter(|port| *port != default_port)
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    Some(format!(
        "remote:{host}{port}/{}",
        repository_path(url.path()).trim_start_matches('/')
    ))
}

fn repository_path(path: &str) -> &str {
    let path = path.trim_end_matches('/');
    path.strip_suffix(".git").unwrap_or(path)
}

/// Remove URL credentials from diagnostic text without changing the URL used
/// for Git. Git may echo an encoded URL or only a decoded query value.
pub fn redact_remote_credentials(text: &str, remote: &str) -> String {
    if remote.is_empty() {
        return text.to_owned();
    }
    let Ok(mut url) = Url::parse(remote) else {
        return text.replace(remote, "[REDACTED remote]");
    };
    let original = url.to_string();
    let mut secrets = Vec::new();
    if !url.username().is_empty() && url.username().len() >= 4 {
        secrets.push(url.username().to_owned());
    }
    if let Some(password) = url.password() {
        secrets.push(password.to_owned());
    }
    let pairs: Vec<_> = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    let _ = url.set_username("");
    let _ = url.set_password(None);
    if url.query().is_some() {
        let mut query = url.query_pairs_mut();
        query.clear();
        for (key, value) in pairs {
            let name = key.to_ascii_lowercase();
            let sensitive = [
                "token",
                "secret",
                "password",
                "credential",
                "signature",
                "auth",
            ]
            .iter()
            .any(|word| name.contains(word))
                || matches!(
                    name.as_str(),
                    "key" | "apikey" | "api-key" | "sig" | "jwt" | "pat"
                )
                || name.ends_with("_key");
            if sensitive {
                secrets.push(value.clone());
                query.append_pair(&key, "[REDACTED]");
            } else {
                query.append_pair(&key, &value);
            }
        }
    }
    let mut redacted = text
        .replace(remote, url.as_str())
        .replace(&original, url.as_str());
    secrets.sort_by_key(|value| std::cmp::Reverse(value.len()));
    for secret in secrets.into_iter().filter(|value| !value.is_empty()) {
        redacted = redact_secret(&redacted, &secret);
        // Decode percent-encoded userinfo using the same URL decoding rules.
        let decoded = url::form_urlencoded::parse(format!("v={secret}").as_bytes())
            .next()
            .map(|(_, value)| value.into_owned())
            .unwrap_or_default();
        if !decoded.is_empty() {
            redacted = redact_secret(&redacted, &decoded);
        }
    }
    redacted
}

fn redact_secret(text: &str, secret: &str) -> String {
    let mut result = text.replace(secret, "[REDACTED]");
    // A bounded tail can start partway through a credential, so the complete
    // value no longer appears. Mask an overlapping secret suffix at its start.
    if !result.starts_with("[REDACTED]") {
        let overlap = (1..=secret.len().min(result.len())).rev().find(|length| {
            result.is_char_boundary(*length)
                && secret.is_char_boundary(secret.len() - length)
                && result[..*length] == secret[secret.len() - length..]
        });
        if let Some(length) = overlap {
            result.replace_range(..length, "[REDACTED]");
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_normalization_equates_transport_forms_and_defaults() {
        let expected = normalize_remote_url("https://github.com/o/r");
        for remote in [
            "git@github.com:o/r.git",
            "github.com:o/r/",
            "ssh://git@github.com/o/r.git/",
            "ssh://git@GITHUB.COM:22/o/r",
            "https://GITHUB.com:443/o/r.git/",
            "http://github.com:80/o/r.git",
            "git://github.com:9418/o/r",
            " https://github.com/o/r.git/// ",
        ] {
            assert_eq!(normalize_remote_url(remote), expected, "{remote}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn remote_normalization_equates_file_urls_and_absolute_paths() {
        let root = tempfile::tempdir().unwrap();
        let origin = root.path().join("origin.git");
        let expected = normalize_remote_url(origin.to_str().unwrap());
        let file = url::Url::from_file_path(&origin).unwrap();
        let mut local = file.clone();
        local.set_host(Some("localhost")).unwrap();
        for remote in [
            file.to_string(),
            local.to_string(),
            format!("{}/", origin.display()),
        ] {
            assert_eq!(normalize_remote_url(&remote), expected, "{remote}");
        }
        let spaced = root.path().join("a b/origin.git");
        assert_eq!(
            normalize_remote_url(url::Url::from_file_path(&spaced).unwrap().as_str()),
            normalize_remote_url(root.path().join("a b/origin").to_str().unwrap())
        );
    }

    #[test]
    fn remote_diagnostics_redact_userinfo_and_token_queries() {
        let url = "https://username:pa%24s@example.invalid/repo?token=supersecret&access_token=encoded%24token&branch=main";
        let message = format!("clone {url} failed: supersecret, encoded$token and pa$s");
        let output = super::redact_remote_credentials(&message, url);
        for secret in [
            "username",
            "pa%24s",
            "pa$s",
            "supersecret",
            "encoded$token",
            "encoded%24token",
        ] {
            assert!(!output.contains(secret), "{output}");
        }
        assert!(output.contains("branch=main"));
    }

    #[test]
    fn remote_diagnostics_redact_credentials_cut_by_bounded_tail() {
        let secret = "abcdefSECRETTAIL";
        let remote = format!("https://example.invalid/repo?token={secret}");
        let tail = "SECRETTAIL failed to authenticate";
        let output = super::redact_remote_credentials(tail, &remote);
        assert!(!output.contains("SECRETTAIL"));
        assert!(output.contains("failed to authenticate"));
    }

    #[test]
    fn remote_normalization_preserves_different_repositories_and_ports() {
        let expected = normalize_remote_url("https://github.com/o/r");
        for remote in [
            "git@elsewhere.test:o/r.git",
            "git@github.com:other/r.git",
            "git@github.com:o/other.git",
            "https://github.com/O/r",
            "ssh://git@github.com:2222/o/r",
            "https://github.com:8443/o/r",
            "/github.com/o/r",
            "file:///github.com/o/r",
        ] {
            assert_ne!(normalize_remote_url(remote), expected, "{remote}");
        }
        assert_ne!(
            normalize_remote_url("file:///tmp/origin.git"),
            normalize_remote_url("file:///tmp/other.git")
        );
    }
}
