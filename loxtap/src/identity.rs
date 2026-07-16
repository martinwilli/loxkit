//! Stable client identity for token requests, derived from the host.
//!
//! The server tracks every token it issues by a client uuid. That id must stay
//! stable across restarts (a changing id would orphan the old token slot and
//! slowly pile up dead entries), yet it carries no real information, so asking
//! an operator to invent and pin one is pure friction. Instead we derive it
//! from the host's machine-id, and a cosmetic label from the hostname; nothing
//! about identity lives in the config.

use std::fs;

use anyhow::{Context as _, bail};
use hmac::{Hmac, KeyInit as _, Mac as _};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Path to the host's machine-id, the stable per-host identifier.
const MACHINE_ID_PATH: &str = "/etc/machine-id";

/// Path exposing the live kernel hostname.
const HOSTNAME_PATH: &str = "/proc/sys/kernel/hostname";

/// Fixed 128-bit application id folded into the client-uuid derivation.
///
/// The uuid is `HMAC-SHA256(key = machine-id, msg = this)`, the same
/// construction as systemd's `sd_id128_get_machine_app_specific`. Mixing in a
/// per-app constant keeps the derived id stable per host while revealing
/// nothing about the machine-id itself (which is meant to stay confidential).
/// Generated once and frozen: changing it would remint every client id.
const APP_ID: [u8; 16] = [
    0x9d, 0x3c, 0x7a, 0x11, 0x4e, 0x82, 0x4b, 0x6f, 0xa1, 0x0d, 0x5c, 0x2e, 0x88, 0xf3, 0x61, 0xb4,
];

/// The host-derived client identity sent with a token request.
#[derive(Clone, Debug)]
pub struct ClientIdentity {
    /// Stable uuid the server keys issued tokens on.
    pub uuid: String,
    /// Human-readable label shown in the server's authorized-devices list.
    pub info: String,
}

impl ClientIdentity {
    /// Resolve the identity from the host.
    ///
    /// Errors only if the machine-id is absent or malformed, so a host without
    /// one fails fast rather than silently minting a fresh (orphaning) id. The
    /// label is cosmetic and never fails.
    pub fn resolve() -> anyhow::Result<Self> {
        Ok(Self { uuid: machine_uuid()?, info: host_label() })
    }
}

/// Derive the stable client uuid from the host's machine-id.
fn machine_uuid() -> anyhow::Result<String> {
    let raw = fs::read_to_string(MACHINE_ID_PATH)
        .with_context(|| format!("reading {MACHINE_ID_PATH}"))?;
    uuid_from_machine_id(raw.trim())
        .with_context(|| format!("deriving a client uuid from {MACHINE_ID_PATH}"))
}

/// The pure core of [`machine_uuid`], split out to test without the
/// filesystem: `HMAC-SHA256(key = machine-id bytes, msg = APP_ID)`, first 16
/// bytes, with the uuid version (4) and variant nibbles stamped in.
fn uuid_from_machine_id(machine_id: &str) -> anyhow::Result<String> {
    let key = decode_machine_id(machine_id)?;
    let mut mac = HmacSha256::new_from_slice(&key).expect("HMAC accepts any key length");
    mac.update(&APP_ID);
    let full = mac.finalize().into_bytes();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&full[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant DCE
    Ok(uuid::Uuid::from_bytes(bytes).to_string())
}

/// Decode a machine-id's 32 hex chars into its 16 raw bytes.
fn decode_machine_id(machine_id: &str) -> anyhow::Result<[u8; 16]> {
    let hex = machine_id.as_bytes();
    if hex.len() != 32 {
        bail!("expected 32 hex chars, got {}", machine_id.len());
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = (hex_val(hex[2 * i])? << 4) | hex_val(hex[2 * i + 1])?;
    }
    Ok(out)
}

/// Value of a single hex digit, case-insensitive.
fn hex_val(c: u8) -> anyhow::Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => bail!("non-hex character in machine-id"),
    }
}

/// Build the `loxtap@<hostname>` label, so several hosts logging to one server
/// stay distinguishable there. Falls back to a bare `loxtap` if the hostname is
/// unreadable, since the label is only cosmetic.
fn host_label() -> String {
    match fs::read_to_string(HOSTNAME_PATH) {
        Ok(h) if !h.trim().is_empty() => format!("loxtap@{}", h.trim()),
        _ => "loxtap".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A well-formed machine-id: 32 hex chars, as `/etc/machine-id` carries.
    const SAMPLE_ID: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn derives_a_valid_v4_uuid() {
        let u = uuid_from_machine_id(SAMPLE_ID).unwrap();
        // Canonical 8-4-4-4-12 form, version nibble 4, variant in {8,9,a,b}.
        assert_eq!(u.len(), 36);
        assert_eq!(u.chars().nth(14), Some('4'));
        assert!(matches!(u.chars().nth(19), Some('8' | '9' | 'a' | 'b')));
    }

    #[test]
    fn is_stable_and_host_specific() {
        // Same id -> same uuid across calls; a different id -> a different one.
        assert_eq!(uuid_from_machine_id(SAMPLE_ID).unwrap(), uuid_from_machine_id(SAMPLE_ID).unwrap());
        let other = uuid_from_machine_id("fedcba9876543210fedcba9876543210").unwrap();
        assert_ne!(uuid_from_machine_id(SAMPLE_ID).unwrap(), other);
    }

    #[test]
    fn is_case_insensitive() {
        let upper = SAMPLE_ID.to_uppercase();
        assert_eq!(uuid_from_machine_id(SAMPLE_ID).unwrap(), uuid_from_machine_id(&upper).unwrap());
    }

    #[test]
    fn rejects_malformed_machine_ids() {
        // Empty, too short, and non-hex all fail rather than derive junk.
        assert!(uuid_from_machine_id("").is_err());
        assert!(uuid_from_machine_id("abcd").is_err());
        assert!(uuid_from_machine_id("0123456789abcdef0123456789abcdeg").is_err());
    }

    #[test]
    fn label_is_prefixed_with_loxtap() {
        assert!(host_label().starts_with("loxtap"));
    }
}
