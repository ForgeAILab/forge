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
        let expected = normalize_remote_url("/Volumes/Data/tmp/origin.git");
        for remote in [
            "file:///Volumes/Data/tmp/origin.git",
            "file://localhost/Volumes/Data/tmp/origin/",
            "/Volumes/Data/tmp/origin.git/",
        ] {
            assert_eq!(normalize_remote_url(remote), expected, "{remote}");
        }
        assert_eq!(
            normalize_remote_url("file:///tmp/a%20b/origin.git"),
            normalize_remote_url("/tmp/a b/origin")
        );
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
