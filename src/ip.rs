use std::net::Ipv4Addr;

// to avoid using nightly for just this feature
// https://doc.rust-lang.org/nightly/src/core/net/ip_addr.rs.html#820

pub const fn is_benchmarking(ip: &Ipv4Addr) -> bool {
    ip.octets()[0] == 198 && (ip.octets()[1] & 0xfe) == 18
}

pub fn is_shared(ip: &Ipv4Addr) -> bool {
    ip.octets()[0] == 100 && (ip.octets()[1] & 0b1100_0000 == 0b0100_0000)
}

pub fn is_global(ip: &Ipv4Addr) -> bool {
    !(ip.octets()[0] == 0 // "This network"
            || ip.is_private()
            || is_shared(&ip)
            || ip.is_loopback()
            || ip.is_link_local()
            // addresses reserved for future protocols (`192.0.0.0/24`)
            // .9 and .10 are documented as globally reachable so they're excluded
            || (
                ip.octets()[0] == 192 && ip.octets()[1] == 0 && ip.octets()[2] == 0
                && ip.octets()[3] != 9 && ip.octets()[3] != 10
            )
            || ip.is_documentation()
            || is_benchmarking(&ip)
            || is_reserved(&ip)
            || ip.is_broadcast())
}

pub const fn is_reserved(ip: &Ipv4Addr) -> bool {
    ip.octets()[0] & 240 == 240 && !ip.is_broadcast()
}
