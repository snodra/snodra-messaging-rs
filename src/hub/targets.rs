//! The hubs an instance connects to (`KRAKEN_HUB_ADDRESS`, ADR 0016), and the pure rules that
//! pick one of them for a request.
//!
//! - **Targets**: a comma-separated list of `host:port` (`static://` or `http(s)://` prefix
//!   optional), or one `dns:///name:port` whose addresses are re-resolved every
//!   [`DNS_REFRESH`]. One target is the single-hub setup of before.
//! - **Unkeyed requests** go round-robin over the hubs whose stream is acknowledged.
//! - **Keyed requests** go to the live hub with the highest [`rendezvous_score`] of
//!   `(hub_id, routing key)`: a key keeps its hub while the set of hubs is stable, and only the
//!   keys of a hub that leaves move.
//! - **Permits** are split: a hub gets [`permit_share`] of `max_concurrent_handlers`.

use std::time::Duration;

use crate::error::MessagingError;

use super::connection::grpc_target;

/// How often a `dns:///` target is resolved again.
pub const DNS_REFRESH: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HubTargets {
    /// Fixed addresses in configured order, exact duplicates removed.
    Static(Vec<String>),
    /// Every address `host` resolves to (on `port`) is a hub.
    Dns { host: String, port: u16 },
}

impl HubTargets {
    /// Parses `KRAKEN_HUB_ADDRESS` / `Config.hub_address`. `InvalidMessage` when it names no hub,
    /// when a `dns:///` target is not alone, or when it lacks a host or a port (1-65535).
    pub fn parse(address: &str) -> Result<HubTargets, MessagingError> {
        let bad = |why: &str| {
            MessagingError::InvalidMessage(format!(
                "hub_address (KRAKEN_HUB_ADDRESS) '{address}': {why}"
            ))
        };
        let entries: Vec<&str> = address
            .split(',')
            .map(str::trim)
            .filter(|e| !e.is_empty())
            .collect();
        if entries.is_empty() {
            return Err(bad("names no hub"));
        }
        if entries.iter().any(|e| e.starts_with("dns:")) {
            let [entry] = entries.as_slice() else {
                return Err(bad("a dns:/// target must be the only one"));
            };
            let name = entry.strip_prefix("dns:///").ok_or_else(|| {
                bad("only dns:///name:port is supported (no DNS server authority)")
            })?;
            let (host, port) = name
                .rsplit_once(':')
                .ok_or_else(|| bad("dns:///name:port needs a port"))?;
            let port = port
                .parse::<u16>()
                .ok()
                .filter(|&p| p > 0)
                .ok_or_else(|| bad("dns:///name:port needs a port from 1 to 65535"))?;
            if host.is_empty() {
                return Err(bad("dns:///name:port needs a name"));
            }
            return Ok(HubTargets::Dns {
                host: host.to_string(),
                port,
            });
        }
        let mut seen = Vec::new();
        let mut targets = Vec::new();
        for entry in entries {
            let normalized = grpc_target(entry);
            if !seen.contains(&normalized) {
                seen.push(normalized);
                targets.push(entry.to_string());
            }
        }
        Ok(HubTargets::Static(targets))
    }
}

/// The rendezvous score of `(hub_id, key)`: 64-bit FNV-1a (offset basis `0xcbf29ce484222325`,
/// prime `0x100000001b3`) over the UTF-8 bytes of `hub_id`, one zero byte, then the UTF-8 bytes of
/// `key`, followed by the MurmurHash3 `fmix64` finalizer (`k ^= k >> 33; k *= 0xff51afd7ed558ccd;
/// k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53; k ^= k >> 33`, wrapping). The zero byte keeps `("ab",
/// "c")` and `("a", "bc")` apart (hub ids never contain it). The finalizer is needed: plain FNV-1a
/// mixes its high bits poorly, so with ids that differ in one character (`hub-1`, `hub-2`) one hub
/// got 58 % of 30 000 sequential keys and 80 % of the first 1 000; with it the split is even.
pub fn rendezvous_score(hub_id: &str, key: &str) -> u64 {
    const BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let fnv = hub_id
        .as_bytes()
        .iter()
        .chain(std::iter::once(&0u8))
        .chain(key.as_bytes())
        .fold(BASIS, |h, b| (h ^ u64::from(*b)).wrapping_mul(PRIME));
    fmix64(fnv)
}

/// MurmurHash3's 64-bit finalizer: every input bit affects every output bit.
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^ (k >> 33)
}

/// `ceil(max_concurrent_handlers / hubs)` (`hubs` 0 counts as 1), at most `u32::MAX`.
pub fn permit_share(max_concurrent_handlers: usize, hubs: usize) -> u32 {
    max_concurrent_handlers
        .div_ceil(hubs.max(1))
        .min(u32::MAX as usize) as u32
}

/// Indices of `hub_ids` from the preferred to the least preferred hub for `key`: highest
/// [`rendezvous_score`] first, ties (never seen in practice) by hub id.
pub(crate) fn rank_keyed(hub_ids: &[&str], key: &str) -> Vec<usize> {
    let mut ranked: Vec<(u64, &str, usize)> = hub_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (rendezvous_score(id, key), *id, i))
        .collect();
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    ranked.into_iter().map(|(_, _, i)| i).collect()
}

/// Indices of `hubs` live hubs in round-robin order, starting at `turn`.
pub(crate) fn rank_round_robin(hubs: usize, turn: usize) -> Vec<usize> {
    if hubs == 0 {
        return vec![];
    }
    (0..hubs).map(|i| (turn + i) % hubs).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statics(list: &[&str]) -> HubTargets {
        HubTargets::Static(list.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn a_single_address_is_one_static_target_as_before() {
        for a in [
            "static://hub:7000",
            "hub:7000",
            "http://localhost:7002",
            " hub:7000 ",
        ] {
            assert_eq!(HubTargets::parse(a).unwrap(), statics(&[a.trim()]), "{a}");
        }
    }

    #[test]
    fn a_list_keeps_its_order_and_drops_blanks_and_duplicates() {
        assert_eq!(
            HubTargets::parse("static://hub-1:7000, hub-2:7000,,static://hub-1:7000,").unwrap(),
            statics(&["static://hub-1:7000", "hub-2:7000"])
        );
        // The same hub written two ways is still one target.
        assert_eq!(
            HubTargets::parse("hub-1:7000,static://hub-1:7000,http://hub-1:7000").unwrap(),
            statics(&["hub-1:7000"])
        );
    }

    #[test]
    fn a_dns_target_names_a_host_and_a_port() {
        assert_eq!(
            HubTargets::parse("dns:///kraken-hub:7000").unwrap(),
            HubTargets::Dns {
                host: "kraken-hub".into(),
                port: 7000
            }
        );
        assert_eq!(
            HubTargets::parse(" dns:///hubs.internal.example:7100 ").unwrap(),
            HubTargets::Dns {
                host: "hubs.internal.example".into(),
                port: 7100
            }
        );
    }

    #[test]
    fn malformed_targets_are_rejected() {
        for bad in [
            "",
            " , ",
            "dns:///kraken-hub",             // no port
            "dns:///kraken-hub:0",           // port 0
            "dns:///kraken-hub:x",           // not a port
            "dns:///:7000",                  // no host
            "dns://8.8.8.8/kraken-hub:7000", // an authority (DNS server) is not supported
            "dns:///a:7000,dns:///b:7000",   // dns only alone
            "hub-1:7000,dns:///b:7000",
        ] {
            assert!(
                matches!(
                    HubTargets::parse(bad),
                    Err(MessagingError::InvalidMessage(_))
                ),
                "{bad:?} accepted: {:?}",
                HubTargets::parse(bad)
            );
        }
    }

    /// Test vectors for kraken-messaging (Kotlin): FNV-1a 64 over `hub_id + "\0" + key`, then fmix64.
    #[test]
    fn rendezvous_score_is_fnv1a_64_over_hub_id_zero_key_then_fmix64() {
        assert_eq!(fnv(b"\0"), 0xaf63_bd4c_8601_b7df, "the reference FNV-1a 64");
        assert_eq!(rendezvous_score("", ""), 0xb903_4ad3_7056_f5fb);
        assert_eq!(rendezvous_score("hub-1", "order-42"), 0x5c85_5509_f5ad_7c8d);
        assert_eq!(rendezvous_score("hub-2", "order-42"), 0xd109_ca17_7e04_ae14);
        for (id, key) in [
            ("hub-1", "order-42"),
            ("h", ""),
            ("", "k"),
            ("hub-ü", "kéy"),
        ] {
            let bytes = [id.as_bytes(), b"\0", key.as_bytes()].concat();
            assert_eq!(
                rendezvous_score(id, key),
                fmix64(fnv(&bytes)),
                "{id:?} {key:?}"
            );
        }
        // The separator keeps (id, key) pairs apart.
        assert_ne!(rendezvous_score("a", ""), rendezvous_score("", "a"));
        assert_ne!(rendezvous_score("ab", "c"), rendezvous_score("a", "bc"));
    }

    /// Reference FNV-1a 64 (offset basis 0xcbf29ce484222325, prime 0x100000001b3).
    fn fnv(bytes: &[u8]) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in bytes {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }

    #[test]
    fn rendezvous_choice_is_stable_and_only_the_leaving_hubs_keys_move() {
        let three = ["hub-a", "hub-b", "hub-c"];
        let keys: Vec<String> = (0..1_000).map(|i| format!("order-{i}")).collect();
        let pick = |ids: &[&str], key: &str| ids[rank_keyed(ids, key)[0]].to_string();
        let before: Vec<String> = keys.iter().map(|k| pick(&three, k)).collect();
        // Stable: the same choice every time, whatever order the hubs are listed in.
        let shuffled = ["hub-c", "hub-a", "hub-b"];
        for (k, hub) in keys.iter().zip(&before) {
            assert_eq!(&pick(&three, k), hub);
            assert_eq!(&pick(&shuffled, k), hub);
        }
        // Spread: every hub owns a fair part of the keys.
        for id in three {
            let owned = before.iter().filter(|h| *h == id).count();
            assert!(
                (200..=470).contains(&owned),
                "{id} owns {owned} of 1000 keys"
            );
        }
        // hub-b leaves: its keys move, nobody else's.
        let two = ["hub-a", "hub-c"];
        for (k, hub) in keys.iter().zip(&before) {
            let now = pick(&two, k);
            if hub == "hub-b" {
                assert_ne!(now, "hub-b");
            } else {
                assert_eq!(&now, hub, "{k} moved from {hub} though {hub} stayed");
            }
        }
        // The full ranking is the fallback order: the runner-up takes over the leaving hub's keys.
        for (k, hub) in keys.iter().zip(&before) {
            if hub == "hub-b" {
                let ranked = rank_keyed(&three, k);
                assert_eq!(three[ranked[1]], pick(&two, k));
            }
        }
    }

    #[test]
    fn round_robin_rotates_over_every_hub_starting_at_the_turn() {
        assert_eq!(rank_round_robin(3, 0), [0, 1, 2]);
        assert_eq!(rank_round_robin(3, 1), [1, 2, 0]);
        assert_eq!(rank_round_robin(3, 5), [2, 0, 1]);
        assert_eq!(rank_round_robin(1, 7), [0]);
        assert!(rank_round_robin(0, 3).is_empty());
    }

    #[test]
    fn permit_share_is_the_ceiling_of_max_over_hubs() {
        assert_eq!(permit_share(256, 1), 256);
        assert_eq!(permit_share(256, 2), 128);
        assert_eq!(permit_share(256, 3), 86);
        assert_eq!(
            permit_share(1, 3),
            1,
            "never 0: every hub may forward something"
        );
        assert_eq!(permit_share(5, 2), 3);
        assert_eq!(permit_share(10, 0), 10, "no hub counts as one");
        assert_eq!(permit_share(usize::MAX, 1), u32::MAX);
    }
}
