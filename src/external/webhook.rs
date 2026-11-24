//! Verifies a webhook request against a shared secret, by
//! the rules of an identity provider webhook:
//!
//! - **shared secret**: the header `X-Snodra-Webhook-Secret` holds the secret itself;
//! - **HMAC**: the header `X-Snodra-Signature` holds `sha256=<hex>`, the HMAC-SHA256 of the raw
//!   body keyed by the secret (the `sha256=` prefix is optional, the case does not matter).
//!
//! Either header, matching the current secret or, during a rotation, the previous one, verifies
//! the request. Header names are case-insensitive. Every comparison is constant time
//! ([`constant_time_eq`]; [`verify_with`] is the seam a test uses to see it is the only way a
//! secret or signature is compared). Empty secrets are ignored: a service with none configured
//! verifies nothing. Verify the **raw** body bytes, before parsing them.
//!
//! The HMAC is RFC 2104 over the crate's `sha2` (no extra dependency).

use sha2::{Digest, Sha256};

pub const SECRET_HEADER: &str = "X-Snodra-Webhook-Secret";
pub const SIGNATURE_HEADER: &str = "X-Snodra-Signature";

const BLOCK: usize = 64;

/// The HMAC-SHA256 of `body` keyed by `secret`, lowercase hex: what a partner computes for
/// `X-Snodra-Signature`.
pub fn hmac_hex(secret: &[u8], body: &[u8]) -> String {
    let mut key = [0u8; BLOCK];
    if secret.len() > BLOCK {
        key[..32].copy_from_slice(&Sha256::digest(secret));
    } else {
        key[..secret.len()].copy_from_slice(secret);
    }
    let pad = |byte: u8| key.map(|k| k ^ byte);
    let inner = Sha256::new()
        .chain_update(pad(0x36))
        .chain_update(body)
        .finalize();
    let outer = Sha256::new()
        .chain_update(pad(0x5c))
        .chain_update(inner)
        .finalize();
    outer.iter().map(|b| format!("{b:02x}")).collect()
}

/// Whether `a` and `b` are equal, in a time that depends only on their lengths (every byte of
/// the longer one is looked at, whatever differs first).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let len = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u64;
    for i in 0..len {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= u64::from(x ^ y);
    }
    std::hint::black_box(diff) == 0
}

/// Verifies the request whose `headers` (name/value pairs: a `&HashMap<String, String>`, an
/// `http::HeaderMap`, a slice of tuples) and raw `body` arrived, against `secret` and, during a
/// rotation, `previous`. See the module docs.
pub fn verify<I, K, V>(secret: &str, previous: Option<&str>, headers: I, body: &[u8]) -> bool
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<[u8]>,
{
    verify_with(&constant_time_eq, secret, previous, headers, body)
}

/// [`verify`] with the byte comparison given (the seam of the constant-time test).
pub fn verify_with<F, I, K, V>(
    compare: &F,
    secret: &str,
    previous: Option<&str>,
    headers: I,
    body: &[u8],
) -> bool
where
    F: Fn(&[u8], &[u8]) -> bool,
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<[u8]>,
{
    let secrets: Vec<&str> = std::iter::once(secret)
        .chain(previous)
        .filter(|s| !s.is_empty())
        .collect();
    let mut shared = None;
    let mut signature = None;
    for (name, value) in headers {
        let name = name.as_ref();
        if shared.is_none() && name.eq_ignore_ascii_case(SECRET_HEADER) {
            shared = Some(value.as_ref().to_vec());
        } else if signature.is_none() && name.eq_ignore_ascii_case(SIGNATURE_HEADER) {
            signature = Some(value.as_ref().to_vec());
        }
    }
    if let Some(shared) = shared.filter(|s| !s.is_empty())
        && secrets.iter().any(|s| compare(s.as_bytes(), &shared))
    {
        return true;
    }
    if let Some(signature) = signature.filter(|s| !s.is_empty()) {
        let given = match signature.get(..7) {
            Some(prefix) if prefix.eq_ignore_ascii_case(b"sha256=") => &signature[7..],
            _ => &signature[..],
        }
        .to_ascii_lowercase();
        return secrets
            .iter()
            .any(|s| compare(hmac_hex(s.as_bytes(), body).as_bytes(), &given));
    }
    false
}
