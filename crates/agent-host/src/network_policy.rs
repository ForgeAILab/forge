//! Hostname policy for provider probes, native requests and web tools.

/// Hostnames that cannot name a public outbound endpoint.
pub fn restricted_provider_hostname(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host.ends_with(".home.arpa")
}

#[cfg(test)]
mod tests {
    use config::is_restricted_public_address;

    #[test]
    fn former_6to4_relay_anycast_is_restricted() {
        for address in [
            "192.88.99.0",
            "192.88.99.1",
            "192.88.99.255",
            "64:ff9b::192.88.99.1",
            "2002:c058:6301::",
            "::ffff:192.88.99.1",
            "::192.88.99.1",
        ] {
            assert!(
                is_restricted_public_address(address.parse().unwrap()),
                "{address}"
            );
        }
        for address in ["192.88.98.255", "192.88.100.0"] {
            assert!(
                !is_restricted_public_address(address.parse().unwrap()),
                "{address}"
            );
        }
    }

    #[test]
    fn nat64_applies_embedded_ipv4_policy() {
        for address in ["64:ff9b::8.8.8.8", "64:ff9b::1.1.1.1"] {
            assert!(
                !is_restricted_public_address(address.parse().unwrap()),
                "{address}"
            );
        }
        for address in [
            "64:ff9b::10.0.0.1",
            "64:ff9b::127.0.0.1",
            "64:ff9b::169.254.169.254",
            "64:ff9b:1::8.8.8.8",
            "64:ff9b:1:1234:5678:9abc:808:808",
        ] {
            assert!(
                is_restricted_public_address(address.parse().unwrap()),
                "{address}"
            );
        }
    }
}
