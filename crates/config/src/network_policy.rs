//! Destination address policy shared by providers, fetch and public search.

use std::net::{IpAddr, Ipv4Addr};

/// Reject local, private, special-use and restricted transition destinations.
/// IPv4-carrying IPv6 forms are judged by their embedded IPv4 destination.
pub fn is_restricted_public_address(ip: IpAddr) -> bool {
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
                || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99)
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
                    && is_restricted_public_address(IpAddr::V4(Ipv4Addr::new(
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
                    && is_restricted_public_address(IpAddr::V4(Ipv4Addr::new(
                        (segments[1] >> 8) as u8,
                        segments[1] as u8,
                        (segments[2] >> 8) as u8,
                        segments[2] as u8,
                    ))))
                || address
                    .to_ipv4()
                    .is_some_and(|mapped| is_restricted_public_address(IpAddr::V4(mapped)))
        }
    }
}

/// Public search uses the shared destination policy, plus its existing
/// conservative IPv6 exclusions. See docs/architecture.md for the deliberate
/// differences from provider/fetch policy.
pub fn is_blocked_public_search_address(address: IpAddr) -> bool {
    if is_restricted_public_address(address) {
        return true;
    }
    let IpAddr::V6(address) = address else {
        return false;
    };
    let segments = address.segments();
    let first = segments[0];
    // Public search refuses mapped/compatible IPv4 and 6to4 even when their
    // embedded IPv4 is public. Well-known NAT64 uses only the shared policy.
    address.to_ipv4().is_some()
        || first == 0x2002
        || (first == 0x2001 && segments[1] == 0x0db8)
        || (first == 0x2001 && (1..=5).contains(&segments[1]))
        || (first == 0x2001 && (0x0010..=0x001f).contains(&segments[1]))
        || (first == 0x2001 && (0x0020..=0x002f).contains(&segments[1]))
        // Preserve search's conservative documentation-range guard.
        || (0x3ff0..=0x3fff).contains(&first)
        // Discard-only 100::/64.
        || segments[..4] == [0x0100, 0, 0, 0]
}
