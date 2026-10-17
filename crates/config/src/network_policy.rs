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
        // The rest of 64:ff9b::/48 outside the well-known /96 is unassigned.
        || (segments[..3] == [0x64, 0xff9b, 0] && segments[3..6] != [0, 0, 0])
        || (first == 0x2001 && segments[1] == 0x0db8)
        || (first == 0x2001 && (1..=5).contains(&segments[1]))
        || (first == 0x2001 && (0x0010..=0x001f).contains(&segments[1]))
        || (first == 0x2001 && (0x0020..=0x002f).contains(&segments[1]))
        // Preserve search's conservative documentation-range guard.
        || (0x3ff0..=0x3fff).contains(&first)
        // Discard-only 100::/64.
        || segments[..4] == [0x0100, 0, 0, 0]
}

#[cfg(test)]
mod tests {
    //! Old-vs-new verdicts. The two predicates as they stood at `fb957882` are
    //! copied verbatim below and compared with the shared policy over a broad,
    //! reproducible address set. The only permitted differences are the two
    //! intended changes: provider/fetch now refuses `192.88.99.0/24` (also
    //! when carried by an IPv6 form), and public search judges the well-known
    //! NAT64 `64:ff9b::/96` by its embedded IPv4 destination.

    use super::*;
    use std::net::Ipv6Addr;

    /// `forge_agent_host::network_policy::restricted_provider_ip` at `fb957882`.
    fn old_restricted_provider_ip(ip: IpAddr) -> bool {
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
                    || (segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0]
                        && old_restricted_provider_ip(IpAddr::V4(Ipv4Addr::new(
                            (segments[6] >> 8) as u8,
                            segments[6] as u8,
                            (segments[7] >> 8) as u8,
                            segments[7] as u8,
                        ))))
                    || (segments[..3] == [0x64, 0xff9b, 1])
                    || (segments[0] == 0x2001 && segments[1] == 0)
                    || (segments[0] == 0x2002
                        && old_restricted_provider_ip(IpAddr::V4(Ipv4Addr::new(
                            (segments[1] >> 8) as u8,
                            segments[1] as u8,
                            (segments[2] >> 8) as u8,
                            segments[2] as u8,
                        ))))
                    || address
                        .to_ipv4()
                        .is_some_and(|mapped| old_restricted_provider_ip(IpAddr::V4(mapped)))
            }
        }
    }

    /// `config::is_blocked_public_search_address` at `fb957882`.
    fn old_is_blocked_public_search_address(address: IpAddr) -> bool {
        match address {
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
            IpAddr::V6(address) => old_is_blocked_public_ipv6(address),
        }
    }

    fn old_is_blocked_public_ipv6(address: Ipv6Addr) -> bool {
        let segments = address.segments();
        let first = segments[0];
        address.is_loopback()
            || address.is_unspecified()
            || address.is_unique_local()
            || address.is_unicast_link_local()
            || address.to_ipv4().is_some()
            || (first & 0xffc0 == 0xfec0)
            || (first & 0xff00 == 0xff00)
            || (first == 0x2001 && segments[1] == 0x0db8)
            || (first == 0x2001 && segments[1] == 0x0002 && segments[2] == 0)
            || (first == 0x2001 && (0..=5).contains(&segments[1]))
            || (0x3ff0..=0x3fff).contains(&first)
            || (first == 0x2001 && segments[1] == 0)
            || (first == 0x2001 && (0x0010..=0x001f).contains(&segments[1]))
            || (first == 0x2001 && (0x0020..=0x002f).contains(&segments[1]))
            || first == 0x2002
            || (first == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] == 0)
            || (first == 0x0064 && segments[1] == 0xff9b && segments[2] == 0)
            || (first == 0x0064 && segments[1] == 0xff9b && segments[2] == 1)
    }

    /// SplitMix64, so the random part of the address set is reproducible.
    struct SplitMix(u64);

    impl SplitMix {
        fn next_u64(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn next_u128(&mut self) -> u128 {
            (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64())
        }
    }

    /// Edges (one below, first, second, second-to-last, last, one above) and
    /// `samples` random members of each IPv4 block.
    fn ipv4_set(rng: &mut SplitMix) -> Vec<Ipv4Addr> {
        let blocks: [([u8; 4], u32, usize); 20] = [
            ([0, 0, 0, 0], 0, 4096),
            ([0, 0, 0, 0], 8, 32),
            ([1, 0, 0, 0], 8, 32),
            ([8, 8, 8, 8], 32, 0),
            ([10, 0, 0, 0], 8, 32),
            ([100, 64, 0, 0], 10, 32),
            ([127, 0, 0, 0], 8, 32),
            ([169, 254, 0, 0], 16, 32),
            ([172, 16, 0, 0], 12, 32),
            ([192, 0, 0, 0], 24, 32),
            ([192, 0, 2, 0], 24, 32),
            ([192, 88, 99, 0], 24, 32),
            ([192, 168, 0, 0], 16, 32),
            ([198, 18, 0, 0], 15, 32),
            ([198, 51, 100, 0], 24, 32),
            ([203, 0, 113, 0], 24, 32),
            ([224, 0, 0, 0], 4, 32),
            ([240, 0, 0, 0], 4, 32),
            ([255, 255, 255, 255], 32, 0),
            ([93, 184, 216, 0], 24, 32),
        ];
        let mut out = Vec::new();
        for (base, len, samples) in blocks {
            let host = u32::MAX.checked_shr(len).unwrap_or(0);
            let start = u32::from_be_bytes(base) & !host;
            let end = start | host;
            for value in [
                start.wrapping_sub(1),
                start,
                start.wrapping_add(1),
                end.wrapping_sub(1),
                end,
                end.wrapping_add(1),
            ] {
                out.push(Ipv4Addr::from(value));
            }
            for _ in 0..samples {
                out.push(Ipv4Addr::from(start | (rng.next_u64() as u32 & host)));
            }
        }
        out
    }

    /// Edges and random members of each IPv6 block.
    fn ipv6_set(rng: &mut SplitMix) -> Vec<Ipv6Addr> {
        let blocks: [(&str, u32, usize); 37] = [
            ("::", 0, 4096),
            ("::", 128, 0),
            ("::1", 128, 0),
            ("::", 96, 64),
            ("::", 8, 64),
            ("::ffff:0:0", 96, 64),
            ("::ffff:0:0:0", 96, 64),
            ("64:ff9b::", 96, 256),
            ("64:ff9b::", 48, 256),
            ("64:ff9b:1::", 48, 256),
            ("64:ff9b:2::", 48, 64),
            ("100::", 64, 64),
            ("100::", 8, 64),
            ("2000::", 3, 1024),
            ("2001::", 23, 256),
            ("2001::", 32, 256),
            ("2001:1::", 32, 64),
            ("2001:2::", 48, 64),
            ("2001:2::", 32, 64),
            ("2001:3::", 32, 64),
            ("2001:4::", 32, 64),
            ("2001:5::", 32, 64),
            ("2001:6::", 32, 64),
            ("2001:10::", 28, 64),
            ("2001:20::", 28, 64),
            ("2001:30::", 28, 64),
            ("2001:db8::", 32, 64),
            ("2002::", 16, 1024),
            ("3ff0::", 12, 64),
            ("3fff::", 20, 64),
            ("4000::", 2, 64),
            ("fc00::", 7, 64),
            ("fe80::", 10, 64),
            ("fec0::", 10, 64),
            ("ff00::", 8, 64),
            ("2606:4700:4700::", 48, 64),
            ("2001:4860:4860::", 48, 64),
        ];
        let mut out = Vec::new();
        for (base, len, samples) in blocks {
            let host = u128::MAX.checked_shr(len).unwrap_or(0);
            let start = u128::from(base.parse::<Ipv6Addr>().unwrap()) & !host;
            let end = start | host;
            for value in [
                start.wrapping_sub(1),
                start,
                start.wrapping_add(1),
                end.wrapping_sub(1),
                end,
                end.wrapping_add(1),
            ] {
                out.push(Ipv6Addr::from(value));
            }
            for _ in 0..samples {
                out.push(Ipv6Addr::from(start | (rng.next_u128() & host)));
            }
        }
        out
    }

    /// Every IPv4 value in each IPv6 form that can carry it.
    fn carriers(address: Ipv4Addr) -> [Ipv6Addr; 10] {
        let value = u128::from(u32::from(address));
        let [a, b, c, d] = address.octets();
        let high = u16::from_be_bytes([a, b]);
        let low = u16::from_be_bytes([c, d]);
        let in_prefix =
            |prefix: &str| Ipv6Addr::from(u128::from(prefix.parse::<Ipv6Addr>().unwrap()) | value);
        [
            address.to_ipv6_mapped(),
            address.to_ipv6_compatible(),
            in_prefix("::ffff:0:0:0"),
            in_prefix("64:ff9b::"),
            in_prefix("64:ff9b:1::"),
            in_prefix("64:ff9b:0:1::"),
            Ipv6Addr::new(0x2002, high, low, 0, 0, 0, 0, 0),
            Ipv6Addr::new(0x2002, high, low, 1, 0, 0, 0, 1),
            Ipv6Addr::new(0x2001, 0, high, low, 0, 0, 0, 0),
            Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, high, low),
        ]
    }

    fn address_set() -> Vec<IpAddr> {
        let mut rng = SplitMix(0x0f0f_5eed_2026_1017);
        let ipv4 = ipv4_set(&mut rng);
        let mut out: Vec<IpAddr> = ipv4.iter().copied().map(IpAddr::V4).collect();
        for address in &ipv4 {
            out.extend(carriers(*address).map(IpAddr::V6));
        }
        out.extend(ipv6_set(&mut rng).into_iter().map(IpAddr::V6));
        out
    }

    /// IPv4 destinations the provider policy reads out of an IPv6 address.
    fn embedded_ipv4(address: Ipv6Addr) -> Vec<Ipv4Addr> {
        let segments = address.segments();
        let pair = |high: u16, low: u16| Ipv4Addr::from((u32::from(high) << 16) | u32::from(low));
        let mut out = Vec::new();
        if segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
            out.push(pair(segments[6], segments[7]));
        }
        if segments[0] == 0x2002 {
            out.push(pair(segments[1], segments[2]));
        }
        out.extend(address.to_ipv4());
        out
    }

    fn is_former_6to4_relay(address: Ipv4Addr) -> bool {
        address.octets()[..3] == [192, 88, 99]
    }

    #[test]
    fn shared_policy_changes_only_the_two_intended_verdicts() {
        let addresses = address_set();
        assert!(addresses.len() > 40_000, "{}", addresses.len());
        let mut violations = Vec::new();
        let mut provider_relay_refusals = 0usize;
        let mut search_nat64_admissions = 0usize;
        for address in addresses {
            let (old, new) = (
                old_restricted_provider_ip(address),
                is_restricted_public_address(address),
            );
            if old != new {
                let carries_relay = match address {
                    IpAddr::V4(address) => is_former_6to4_relay(address),
                    IpAddr::V6(address) => {
                        embedded_ipv4(address).into_iter().any(is_former_6to4_relay)
                    }
                };
                if !old && carries_relay {
                    provider_relay_refusals += 1;
                } else {
                    violations.push(format!("provider/fetch {address}: old {old}, new {new}"));
                }
            }

            let (old, new) = (
                old_is_blocked_public_search_address(address),
                is_blocked_public_search_address(address),
            );
            if old != new {
                let public_nat64 = match address {
                    IpAddr::V6(address) => {
                        let segments = address.segments();
                        segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0]
                            && !old_is_blocked_public_search_address(IpAddr::V4(Ipv4Addr::from(
                                (u32::from(segments[6]) << 16) | u32::from(segments[7]),
                            )))
                    }
                    IpAddr::V4(_) => false,
                };
                if old && public_nat64 {
                    search_nat64_admissions += 1;
                } else {
                    violations.push(format!("public search {address}: old {old}, new {new}"));
                }
            }
        }
        assert!(
            violations.is_empty(),
            "{} unexpected verdict changes, first: {:#?}",
            violations.len(),
            &violations[..violations.len().min(20)]
        );
        // Both intended changes are exercised, so the comparison is not vacuous.
        assert!(provider_relay_refusals > 0);
        assert!(search_nat64_admissions > 0);
        eprintln!(
            "provider/fetch newly refuses {provider_relay_refusals}; \
             public search newly admits {search_nat64_admissions}"
        );
    }
}
