//! Shared destination policy for provider probes, native requests and web tools.

use std::net::IpAddr;

/// Hostnames that cannot name a public outbound endpoint.
pub fn restricted_provider_hostname(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host.ends_with(".home.arpa")
}

/// Reject local, private, special-use and restricted transition destinations.
pub fn restricted_provider_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(address) => {
            let octets = address.octets();
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_unspecified()
                || address.is_broadcast()
                || octets[0] == 0
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
                // The well-known NAT64 /96 carries its IPv4 destination in
                // the last 32 bits. Apply the same policy to that destination.
                || (segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0]
                    && restricted_provider_ip(IpAddr::V4(std::net::Ipv4Addr::new(
                        (segments[6] >> 8) as u8,
                        segments[6] as u8,
                        (segments[7] >> 8) as u8,
                        segments[7] as u8,
                    ))))
                // Local-use NAT64 /48 and Teredo have no admitted public mapping.
                || (segments[..3] == [0x64, 0xff9b, 1])
                || (segments[0] == 0x2001 && segments[1] == 0)
                // A 6to4 address carries its IPv4 destination in bits 16..48.
                || (segments[0] == 0x2002
                    && restricted_provider_ip(IpAddr::V4(std::net::Ipv4Addr::new(
                        (segments[1] >> 8) as u8,
                        segments[1] as u8,
                        (segments[2] >> 8) as u8,
                        segments[2] as u8,
                    ))))
                || address
                    .to_ipv4()
                    .is_some_and(|mapped| restricted_provider_ip(IpAddr::V4(mapped)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nat64_applies_embedded_ipv4_policy() {
        for address in ["64:ff9b::8.8.8.8", "64:ff9b::1.1.1.1"] {
            assert!(
                !restricted_provider_ip(address.parse().unwrap()),
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
                restricted_provider_ip(address.parse().unwrap()),
                "{address}"
            );
        }
    }
}
