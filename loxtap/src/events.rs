//! Binary value/text event-table decoding and the event loop.
//!
//! Once the structure map is loaded, the daemon asks the Miniserver to start
//! pushing state changes with `enablebinstatusupdate`. The server first sends
//! the *full* state tables, then a stream of deltas as values change. Each table
//! arrives as a binary message whose 8-byte header (resolved by the transport)
//! classifies it; this module decodes the two table kinds we care about and
//! feeds resolved readings into a [`Sink`].
//!
//! - **Value tables (type 2)** are packed `uuid[16] + f64[8]` entries, all
//!   little-endian. `entries = len / 24`. Each becomes a [`Reading`].
//! - **Text tables (type 3)** are `uuid[16] + iconUuid[16] + u32 len + text`,
//!   each entry padded up to the next 4-byte boundary. Decoded and logged; there
//!   is no numeric series to write for them yet.
//! - **Daytimer (4) and weather (7)** tables are deferred: skipped cleanly.
//!
//! Value events carry no per-point timestamp (only `uuid + f64`), so the receive
//! time is stamped as the reading's `ts` in UTC.
//!
//! The event loop must own the socket: [`Transport::command`] skips binary
//! frames while awaiting an `LL` reply, which would swallow the very tables we
//! want, so [`run_events`] drives [`Transport::next_inbound`] directly. It is
//! shaped as a single flat loop so a later supervisor can wrap it with reconnect,
//! keepalive, and token refresh.

use std::time::SystemTime;

use tracing::{debug, info, warn};

use crate::model::{LoxUuid, Reading, SeriesMeta};
use crate::sink::Sink;
use crate::structure::Structure;
use crate::transport::{Inbound, MessageType, Transport, TransportError};

/// Command that asks the server to start pushing binary state tables.
const CMD_ENABLE_UPDATES: &str = "jdev/sps/enablebinstatusupdate";

/// Bytes of one type-2 value entry: `uuid[16] + f64[8]`, little-endian.
const VALUE_ENTRY_SIZE: usize = 24;

/// Fixed prefix of a type-3 text entry: `uuid[16] + iconUuid[16] + u32 len[4]`.
/// The variable-length text (and its padding) follow.
const TEXT_ENTRY_PREFIX: usize = 36;

/// Errors from enabling or running the event stream.
///
/// Only the transport error is terminal for the loop: a dropped or closed
/// socket surfaces as [`TransportError::Closed`], which the supervisor turns
/// into a reconnect. Sink write failures are logged and the loop continues, so
/// they never appear here.
#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("enablebinstatusupdate failed with code {0}")]
    Enable(i64),
}

/// One decoded type-3 text-state entry.
///
/// The icon uuid the wire carries is dropped: it identifies a display glyph, not
/// a series. The text is decoded lossily so a stray non-UTF-8 byte never aborts
/// the whole table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextEntry {
    pub uuid: LoxUuid,
    pub text: String,
}

/// Decode a type-2 value table into readings, stamping each with `ts`.
///
/// Every entry is exactly `uuid[16] + f64[8]` little-endian, so a table is a
/// flat run of 24-byte records. A trailing partial record (should never happen)
/// is ignored rather than misread.
pub fn decode_value_table(payload: &[u8], ts: SystemTime) -> Vec<Reading> {
    payload
        .chunks_exact(VALUE_ENTRY_SIZE)
        .map(|entry| {
            let mut uuid = [0u8; 16];
            uuid.copy_from_slice(&entry[..16]);
            let value = f64::from_le_bytes(entry[16..24].try_into().expect("8 bytes"));
            Reading {
                uuid: LoxUuid::from_bytes(uuid),
                ts,
                value,
            }
        })
        .collect()
}

/// Decode a type-3 text table into its entries.
///
/// Entries are variable-length: a fixed 36-byte prefix (`uuid`, `iconUuid`, and
/// the u32 text length) followed by the text, with the whole entry padded up to
/// the next 4-byte boundary before the next one starts. A truncated final entry
/// (declared length runs past the payload) ends the decode rather than panicking.
pub fn decode_text_table(payload: &[u8]) -> Vec<TextEntry> {
    let mut out = Vec::new();
    let mut off = 0;
    while off + TEXT_ENTRY_PREFIX <= payload.len() {
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&payload[off..off + 16]);
        // payload[off + 16 .. off + 32] is the icon uuid, unused.
        let len = u32::from_le_bytes(
            payload[off + 32..off + TEXT_ENTRY_PREFIX]
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        let text_start = off + TEXT_ENTRY_PREFIX;
        let Some(text_end) = text_start.checked_add(len) else {
            break;
        };
        if text_end > payload.len() {
            break;
        }
        let text = String::from_utf8_lossy(&payload[text_start..text_end]).into_owned();
        out.push(TextEntry {
            uuid: LoxUuid::from_bytes(uuid),
            text,
        });
        // Each entry is padded up to the next 4-byte boundary. `off` and the
        // 36-byte prefix are both 4-aligned, so aligning `text_end` advances to
        // the start of the next entry.
        off = text_end.next_multiple_of(4);
    }
    out
}

/// Enable the binary status stream, then decode tables until the socket drops.
///
/// The loop owns the socket by driving [`Transport::next_inbound`] directly.
/// Value tables are resolved against `structure` and handed to `sink`; text
/// tables are decoded and logged; keepalive and the deferred table kinds are
/// handled benignly. It returns only on a transport error (a disconnect), which
/// a supervisor treats as its reconnect trigger.
///
/// `enablebinstatusupdate` is sent as a plain command: the pushed event stream
/// is not application-encrypted, so the subscription does not need the session
/// cipher either.
pub async fn run_events(
    transport: &mut Transport,
    structure: &Structure,
    sink: &dyn Sink,
) -> Result<(), EventError> {
    enable_updates(transport).await?;
    loop {
        match transport.next_inbound().await? {
            Inbound::Binary { header, payload } => {
                handle_table(header.msg_type, &payload, structure, sink).await;
            }
            // Post-enable text frames are unexpected (replies were already
            // consumed); surface them at debug rather than treating as an error.
            Inbound::Text(text) => debug!(%text, "unexpected text frame in event stream"),
        }
    }
}

/// Send `enablebinstatusupdate` and require a 200 before streaming.
///
/// `pub(crate)` so the supervisor can enable the stream before entering its own
/// keepalive/refresh-aware read loop instead of [`run_events`].
pub(crate) async fn enable_updates(transport: &mut Transport) -> Result<(), EventError> {
    let resp = transport.command(CMD_ENABLE_UPDATES).await?;
    if resp.code() != 200 {
        return Err(EventError::Enable(resp.code()));
    }
    info!("binary status updates enabled");
    Ok(())
}

/// Decode and dispatch one table by its message type.
///
/// A sink failure is logged and swallowed: it must not tear down the event loop
/// (reconnecting would not fix a storage error), so it never becomes an
/// [`EventError`]. `pub(crate)` so the supervisor's read loop reuses the exact
/// same per-frame dispatch as [`run_events`].
pub(crate) async fn handle_table(
    msg_type: MessageType,
    payload: &[u8],
    structure: &Structure,
    sink: &dyn Sink,
) {
    match msg_type {
        MessageType::ValueStates => {
            if !payload.len().is_multiple_of(VALUE_ENTRY_SIZE) {
                warn!(len = payload.len(), "value table is not a whole number of entries");
            }
            let readings = decode_value_table(payload, SystemTime::now());
            if readings.is_empty() {
                return;
            }
            // Teach the sink the labels for the uuids in this batch so names
            // render, then write the readings (unknown uuids are written too,
            // logged under their uuid). The upsert is idempotent, so repeating
            // it for a uuid seen in an earlier delta is harmless.
            let metas = resolve_metas(&readings, structure);
            if let Err(e) = sink.upsert_meta(&metas).await {
                warn!("sink upsert_meta failed: {e:#}");
            }
            if let Err(e) = sink.write(&readings).await {
                warn!("sink write failed: {e:#}");
            }
        }
        MessageType::TextStates => {
            for entry in decode_text_table(payload) {
                let name = structure.resolve(&entry.uuid).map(|e| e.name.as_str());
                debug!(uuid = %entry.uuid, name, text = %entry.text, "text state");
            }
        }
        MessageType::Keepalive => debug!("keepalive"),
        // Deferred table kinds: recognized, decoded later if ever needed.
        MessageType::Daytimer | MessageType::Weather => {
            debug!(?msg_type, "skipping deferred table type");
        }
        MessageType::OutOfService => warn!("server announced it is going out of service"),
        // Not part of the event stream; ignore if one ever slips through.
        MessageType::Text | MessageType::BinaryFile => {
            debug!(?msg_type, "ignoring non-event binary message");
        }
    }
}

/// Collect the [`SeriesMeta`] for every reading whose uuid the structure knows.
fn resolve_metas(readings: &[Reading], structure: &Structure) -> Vec<SeriesMeta> {
    readings
        .iter()
        .filter_map(|r| structure.series_meta(&r.uuid))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    // A synthetic structure fixture: invented rooms, categories and uuids, never
    // a capture of a real Miniserver.
    const FIXTURE: &str = r#"{
      "lastModified": "2026-01-02 03:04:05",
      "rooms": { "deadbeef-0000-0000-0000000000000001": { "name": "Room A" } },
      "cats": { "deadbeef-0000-0000-0000000000000002": { "name": "Category X" } },
      "controls": {
        "deadbeef-1111-0000-0000000000000001": {
          "name": "Temperature",
          "type": "InfoOnlyAnalog",
          "room": "deadbeef-0000-0000-0000000000000001",
          "cat": "deadbeef-0000-0000-0000000000000002",
          "details": { "format": "%.1f°C" },
          "states": { "value": "deadbeef-2222-0000-0000000000000001" }
        },
        "deadbeef-1111-0000-0000000000000002": {
          "name": "Status Text",
          "type": "InfoOnlyText",
          "states": { "text": "deadbeef-5555-0000-0000000000000001" }
        }
      }
    }"#;

    fn uuid(s: &str) -> LoxUuid {
        s.parse().unwrap()
    }

    /// Build a synthetic value table from `(uuid, value)` pairs.
    fn value_blob(entries: &[(&str, f64)]) -> Vec<u8> {
        let mut blob = Vec::new();
        for (id, value) in entries {
            blob.extend_from_slice(uuid(id).as_bytes());
            blob.extend_from_slice(&value.to_le_bytes());
        }
        blob
    }

    /// Build a synthetic text table from `(uuid, text)` pairs, padding each entry
    /// to the next 4-byte boundary exactly as the wire does.
    fn text_blob(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut blob = Vec::new();
        for (id, text) in entries {
            blob.extend_from_slice(uuid(id).as_bytes());
            blob.extend_from_slice(&[0u8; 16]); // icon uuid
            blob.extend_from_slice(&(text.len() as u32).to_le_bytes());
            blob.extend_from_slice(text.as_bytes());
            while !blob.len().is_multiple_of(4) {
                blob.push(0);
            }
        }
        blob
    }

    /// A sink that records everything it is handed, for asserting the wiring.
    #[derive(Default)]
    struct RecordingSink {
        readings: Mutex<Vec<Reading>>,
        metas: Mutex<Vec<SeriesMeta>>,
    }

    #[async_trait::async_trait]
    impl Sink for RecordingSink {
        async fn write(&self, batch: &[Reading]) -> anyhow::Result<()> {
            self.readings.lock().unwrap().extend_from_slice(batch);
            Ok(())
        }
        async fn upsert_meta(&self, meta: &[SeriesMeta]) -> anyhow::Result<()> {
            self.metas.lock().unwrap().extend_from_slice(meta);
            Ok(())
        }
    }

    #[test]
    fn decodes_multi_entry_value_table() {
        let blob = value_blob(&[
            ("deadbeef-2222-0000-0000000000000001", 21.5),
            ("deadbeef-2222-0000-0000000000000010", -3.25),
            ("deadbeef-2222-0000-0000000000000011", 1000.0),
        ]);
        let ts = SystemTime::UNIX_EPOCH;
        let readings = decode_value_table(&blob, ts);
        assert_eq!(readings.len(), 3);
        assert_eq!(readings[0].uuid, uuid("deadbeef-2222-0000-0000000000000001"));
        assert_eq!(readings[0].value, 21.5);
        assert_eq!(readings[0].ts, ts);
        assert_eq!(readings[1].value, -3.25);
        assert_eq!(readings[2].value, 1000.0);
    }

    #[test]
    fn value_table_ignores_trailing_partial_entry() {
        // One whole entry plus five stray bytes: only the whole one decodes.
        let mut blob = value_blob(&[("deadbeef-2222-0000-0000000000000001", 9.0)]);
        blob.extend_from_slice(&[0xaa; 5]);
        let readings = decode_value_table(&blob, SystemTime::now());
        assert_eq!(readings.len(), 1);
        assert_eq!(readings[0].value, 9.0);
    }

    #[test]
    fn empty_value_table_decodes_to_nothing() {
        assert!(decode_value_table(&[], SystemTime::now()).is_empty());
    }

    #[test]
    fn decodes_text_table_with_four_byte_padding() {
        // "on" (2 bytes) and "hello" (5 bytes) both need padding to a 4-boundary,
        // so the second entry only decodes if the padding is handled.
        let blob = text_blob(&[
            ("deadbeef-5555-0000-0000000000000001", "on"),
            ("deadbeef-5555-0000-0000000000000002", "hello"),
        ]);
        let entries = decode_text_table(&blob);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].uuid, uuid("deadbeef-5555-0000-0000000000000001"));
        assert_eq!(entries[0].text, "on");
        assert_eq!(entries[1].uuid, uuid("deadbeef-5555-0000-0000000000000002"));
        assert_eq!(entries[1].text, "hello");
    }

    #[test]
    fn text_table_stops_on_truncated_entry() {
        // A prefix declaring a 99-byte text but with no text bytes decodes to
        // nothing rather than reading past the buffer.
        let id = uuid("deadbeef-5555-0000-0000000000000001");
        let mut blob = Vec::new();
        blob.extend_from_slice(id.as_bytes());
        blob.extend_from_slice(&[0u8; 16]);
        blob.extend_from_slice(&99u32.to_le_bytes());
        assert!(decode_text_table(&blob).is_empty());
    }

    #[tokio::test]
    async fn value_table_resolves_metas_and_writes_all_readings() {
        let structure = Structure::parse(FIXTURE).unwrap();
        let sink = RecordingSink::default();
        // One known uuid (Temperature) and one the structure does not know.
        let blob = value_blob(&[
            ("deadbeef-2222-0000-0000000000000001", 21.5),
            ("00000000-0000-0000-0000000000000099", 7.0),
        ]);
        handle_table(MessageType::ValueStates, &blob, &structure, &sink).await;

        // Both readings are written; the unknown uuid is not dropped.
        let readings = sink.readings.lock().unwrap();
        assert_eq!(readings.len(), 2);

        // Only the known uuid resolves to a label.
        let metas = sink.metas.lock().unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].name, "Temperature");
        assert_eq!(metas[0].room.as_deref(), Some("Room A"));
        assert_eq!(metas[0].unit.as_deref(), Some("°C"));
    }

    #[tokio::test]
    async fn keepalive_and_deferred_tables_write_nothing() {
        let structure = Structure::parse(FIXTURE).unwrap();
        let sink = RecordingSink::default();
        for kind in [
            MessageType::Keepalive,
            MessageType::Daytimer,
            MessageType::Weather,
            MessageType::OutOfService,
        ] {
            handle_table(kind, &[], &structure, &sink).await;
        }
        assert!(sink.readings.lock().unwrap().is_empty());
        assert!(sink.metas.lock().unwrap().is_empty());
    }
}
