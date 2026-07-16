//! Plain data types shared across the daemon.
//!
//! These are deliberately storage-agnostic: the ingestion path produces
//! [`Reading`]s keyed by a [`LoxUuid`], and a sink decides what to do with
//! them. [`SeriesMeta`] carries the mutable human labels that a later
//! structure fetch resolves for each uuid.

use std::fmt;
use std::str::FromStr;
use std::time::SystemTime;

/// A Loxone control/state identifier.
///
/// On the wire these are 16 raw bytes, little-endian in their first three
/// groups. Loxone's textual form is *not* RFC-4122: it is `8-4-4-16`, where
/// the final group is the last eight bytes rendered verbatim with no internal
/// dashes. The same identifier keys both the structure map and the value
/// events, so the byte <-> string conversion lives here to be shared.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LoxUuid([u8; 16]);

impl LoxUuid {
    /// Wrap the 16 raw bytes as they arrive on the wire.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Borrow the raw 16-byte representation.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Render the `8-4-4-16` textual form.
    ///
    /// Groups one to three are little-endian integers; the trailing group is
    /// the last eight bytes as hex, in order.
    pub fn to_lox_string(&self) -> String {
        let b = &self.0;
        let d1 = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        let d2 = u16::from_le_bytes([b[4], b[5]]);
        let d3 = u16::from_le_bytes([b[6], b[7]]);
        let mut tail = String::with_capacity(16);
        for byte in &b[8..16] {
            use fmt::Write as _;
            let _ = write!(tail, "{byte:02x}");
        }
        format!("{d1:08x}-{d2:04x}-{d3:04x}-{tail}")
    }
}

/// Error returned when a `8-4-4-16` uuid string is malformed.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum UuidParseError {
    /// Wrong number of groups or a group of the wrong length.
    #[error("malformed uuid shape, expected 8-4-4-16")]
    Shape,
    /// A group was not valid hexadecimal.
    #[error("uuid contains non-hex characters")]
    Hex,
}

impl FromStr for LoxUuid {
    type Err = UuidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split('-');
        let p0 = parts.next().ok_or(UuidParseError::Shape)?;
        let p1 = parts.next().ok_or(UuidParseError::Shape)?;
        let p2 = parts.next().ok_or(UuidParseError::Shape)?;
        let p3 = parts.next().ok_or(UuidParseError::Shape)?;
        if parts.next().is_some() {
            return Err(UuidParseError::Shape);
        }
        if p0.len() != 8 || p1.len() != 4 || p2.len() != 4 || p3.len() != 16 {
            return Err(UuidParseError::Shape);
        }

        let d1 = u32::from_str_radix(p0, 16).map_err(|_| UuidParseError::Hex)?;
        let d2 = u16::from_str_radix(p1, 16).map_err(|_| UuidParseError::Hex)?;
        let d3 = u16::from_str_radix(p2, 16).map_err(|_| UuidParseError::Hex)?;

        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(&d1.to_le_bytes());
        bytes[4..6].copy_from_slice(&d2.to_le_bytes());
        bytes[6..8].copy_from_slice(&d3.to_le_bytes());
        for (i, out) in bytes[8..16].iter_mut().enumerate() {
            let hi = i * 2;
            *out = u8::from_str_radix(&p3[hi..hi + 2], 16).map_err(|_| UuidParseError::Hex)?;
        }
        Ok(Self(bytes))
    }
}

impl fmt::Display for LoxUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_lox_string())
    }
}

impl fmt::Debug for LoxUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LoxUuid({})", self.to_lox_string())
    }
}

/// A single value observation for a series.
///
/// The wire carries only `{uuid, f64}`; the timestamp is stamped on receipt
/// (UTC) since value events are on-change pushes with no per-point time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Reading {
    pub uuid: LoxUuid,
    pub ts: SystemTime,
    pub value: f64,
}

/// How a series' values aggregate over time.
///
/// A gauge is an instantaneous reading (e.g. kW); a counter is a monotonic
/// total (e.g. kWh). Gauge is the safe default: time-weighting an already
/// instantaneous value is always correct, while only a true monotonic counter
/// needs the delta treatment across buckets and resets. So a series with no
/// explicit counter signal stays a gauge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum AggMode {
    #[default]
    Gauge,
    Counter,
}

impl AggMode {
    /// The text form persisted in the `series_meta.agg_mode` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            AggMode::Gauge => "gauge",
            AggMode::Counter => "counter",
        }
    }
}

/// The mutable human labels resolved for a series uuid.
///
/// A rename in Loxone Config changes these fields, never the uuid, which is
/// the whole point: the series stays stable across renames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesMeta {
    pub uuid: LoxUuid,
    pub control: String,
    pub name: String,
    pub room: Option<String>,
    pub category: Option<String>,
    /// The state name this uuid was published under (e.g. `prodCurr`). Unlike
    /// the other fields it is a stable protocol name, not a mutable label; it is
    /// carried so a sink can tell apart co-located states of one control, which
    /// otherwise share name, room and category.
    pub sub: String,
    pub unit: Option<String>,
    /// The series' aggregation kind, classified once at structure-parse from
    /// Loxone's `visuType`; a sink persists it so downstream readers bucket by
    /// kind rather than guessing from the unit.
    pub agg_mode: AggMode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_string_round_trips() {
        // Groups 1-3 are little-endian; the tail is byte-for-byte.
        let bytes: [u8; 16] = [
            0x78, 0x56, 0x34, 0x12, // d1 -> 0x12345678
            0xbc, 0x9a, // d2 -> 0x9abc
            0xf0, 0xde, // d3 -> 0xdef0
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // tail verbatim
        ];
        let uuid = LoxUuid::from_bytes(bytes);
        let s = uuid.to_lox_string();
        assert_eq!(s, "12345678-9abc-def0-1122334455667788");

        let parsed: LoxUuid = s.parse().unwrap();
        assert_eq!(parsed, uuid);
        assert_eq!(parsed.as_bytes(), &bytes);
    }

    #[test]
    fn uuid_parses_synthetic_value() {
        let uuid: LoxUuid = "deadbeef-cafe-0042-0011223344556677".parse().unwrap();
        // First group little-endian: 0xdeadbeef -> ef be ad de.
        assert_eq!(&uuid.as_bytes()[0..4], &[0xef, 0xbe, 0xad, 0xde]);
        assert_eq!(uuid.to_lox_string(), "deadbeef-cafe-0042-0011223344556677");
    }

    #[test]
    fn agg_mode_defaults_to_gauge_with_stable_strings() {
        // Gauge is the safe default, and the strings feed the series_meta column.
        assert_eq!(AggMode::default(), AggMode::Gauge);
        assert_eq!(AggMode::Gauge.as_str(), "gauge");
        assert_eq!(AggMode::Counter.as_str(), "counter");
    }

    #[test]
    fn uuid_rejects_bad_shapes() {
        assert_eq!("too-few-groups".parse::<LoxUuid>(), Err(UuidParseError::Shape));
        assert_eq!(
            "12345678-9abc-def0-11223344556677".parse::<LoxUuid>(),
            Err(UuidParseError::Shape),
        );
        assert_eq!(
            "gggggggg-9abc-def0-1122334455667788".parse::<LoxUuid>(),
            Err(UuidParseError::Hex),
        );
    }
}
