//! A debug sink that logs each resolved reading via `tracing`.
//!
//! It writes nothing durable; it exists to validate the ingestion path by eye
//! before a real store lands. Since [`Sink::write`](super::Sink::write) receives
//! only uuid-keyed readings, the sink caches the labels handed to
//! [`upsert_meta`](super::Sink::upsert_meta) and joins them back in at log time,
//! exactly as a storage backend would join its labels table. A reading whose
//! uuid has no cached label yet is logged under its uuid.

use std::collections::HashMap;
use std::sync::RwLock;

use tracing::info;

use super::Sink;
use crate::model::{LoxUuid, Reading, SeriesMeta};

/// A sink that logs each reading as `[category/room] name = value unit`.
#[derive(Default)]
pub struct DebugSink {
    /// Labels learned via `upsert_meta`, joined into the log line by uuid.
    labels: RwLock<HashMap<LoxUuid, SeriesMeta>>,
}

impl DebugSink {
    /// Create an empty debug sink.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl Sink for DebugSink {
    async fn write(&self, batch: &[Reading]) -> anyhow::Result<()> {
        let labels = self.labels.read().expect("labels lock poisoned");
        for reading in batch {
            info!("{}", format_reading(labels.get(&reading.uuid), reading));
        }
        Ok(())
    }

    async fn upsert_meta(&self, meta: &[SeriesMeta]) -> anyhow::Result<()> {
        let mut labels = self.labels.write().expect("labels lock poisoned");
        for m in meta {
            labels.insert(m.uuid, m.clone());
        }
        Ok(())
    }
}

/// Format one reading for the log, joining in its labels when known.
///
/// A pure function so the wording is testable without capturing log output:
/// `[category/room] name = value unit`, degrading gracefully as labels are
/// missing (no brackets without a room or category, no unit suffix without a
/// unit, the bare uuid as the name when nothing is cached).
///
/// Co-located states of one control share a name, room and category, so the
/// state name (`sub`) is appended as `name/sub` to tell them apart. It is
/// dropped when it adds nothing: the trivial `value` sub, or a sub equal to the
/// name (the flat globalStates entries).
fn format_reading(meta: Option<&SeriesMeta>, reading: &Reading) -> String {
    let name = match meta {
        Some(m) if m.sub != "value" && m.sub != m.name => format!("{}/{}", m.name, m.sub),
        Some(m) => m.name.clone(),
        None => reading.uuid.to_lox_string(),
    };
    let location = match meta.map(|m| (m.room.as_deref(), m.category.as_deref())) {
        Some((Some(room), Some(cat))) => format!("[{cat}/{room}] "),
        Some((Some(room), None)) => format!("[{room}] "),
        Some((None, Some(cat))) => format!("[{cat}] "),
        _ => String::new(),
    };
    let unit = match meta.and_then(|m| m.unit.as_deref()) {
        Some(unit) => format!(" {unit}"),
        None => String::new(),
    };
    format!("{location}{name} = {}{unit}", reading.value)
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use super::*;

    fn reading(uuid: &str, value: f64) -> Reading {
        Reading {
            uuid: uuid.parse().unwrap(),
            ts: UNIX_EPOCH,
            value,
        }
    }

    fn meta(uuid: &str, name: &str, sub: &str, room: Option<&str>, cat: Option<&str>, unit: Option<&str>) -> SeriesMeta {
        SeriesMeta {
            uuid: uuid.parse().unwrap(),
            control: "InfoOnlyAnalog".to_owned(),
            name: name.to_owned(),
            room: room.map(str::to_owned),
            category: cat.map(str::to_owned),
            sub: sub.to_owned(),
            unit: unit.map(str::to_owned),
        }
    }

    #[test]
    fn formats_fully_labeled_reading() {
        // The trivial "value" sub is dropped, so a single-state control renders
        // under its bare name.
        let m = meta(
            "deadbeef-2222-0000-0000000000000001",
            "Temperature",
            "value",
            Some("Room A"),
            Some("Category X"),
            Some("°C"),
        );
        let r = reading("deadbeef-2222-0000-0000000000000001", 21.5);
        assert_eq!(
            format_reading(Some(&m), &r),
            "[Category X/Room A] Temperature = 21.5 °C"
        );
    }

    #[test]
    fn appends_sub_to_tell_co_located_states_apart() {
        // Two states of one control share name/room/category; the non-"value"
        // sub distinguishes them as `name/sub`.
        let m = meta(
            "deadbeef-2222-0000-0000000000000020",
            "Energiemonitor",
            "prodCurr",
            Some("Technik"),
            Some("Strom"),
            None,
        );
        let r = reading("deadbeef-2222-0000-0000000000000020", 14.37);
        assert_eq!(
            format_reading(Some(&m), &r),
            "[Strom/Technik] Energiemonitor/prodCurr = 14.37"
        );

        // A sub equal to the name (a flat globalState) adds nothing, so drop it.
        let g = meta(
            "deadbeef-9999-0000-0000000000000001",
            "hasInternet",
            "hasInternet",
            None,
            None,
            None,
        );
        let r = reading("deadbeef-9999-0000-0000000000000001", 1.0);
        assert_eq!(format_reading(Some(&g), &r), "hasInternet = 1");
    }

    #[test]
    fn formats_partial_and_missing_labels() {
        // Room only, no category, no unit.
        let m = meta(
            "deadbeef-2222-0000-0000000000000010",
            "Meter",
            "value",
            Some("Room A"),
            None,
            None,
        );
        let r = reading("deadbeef-2222-0000-0000000000000010", 42.0);
        assert_eq!(format_reading(Some(&m), &r), "[Room A] Meter = 42");

        // No labels at all: fall back to the uuid string, no brackets, no unit.
        let r = reading("deadbeef-2222-0000-0000000000000099", 7.0);
        assert_eq!(
            format_reading(None, &r),
            "deadbeef-2222-0000-0000000000000099 = 7"
        );
    }

    #[tokio::test]
    async fn upsert_then_write_resolves_labels() {
        let sink = DebugSink::new();
        let m = meta(
            "deadbeef-2222-0000-0000000000000001",
            "Temperature",
            "value",
            Some("Room A"),
            Some("Category X"),
            Some("°C"),
        );
        sink.upsert_meta(std::slice::from_ref(&m)).await.unwrap();
        let r = reading("deadbeef-2222-0000-0000000000000001", 21.5);
        // The upserted labels are cached and join the reading by uuid. Scope the
        // read guard so it never lives across the write's await below.
        {
            let labels = sink.labels.read().unwrap();
            assert_eq!(
                format_reading(labels.get(&r.uuid), &r),
                "[Category X/Room A] Temperature = 21.5 °C"
            );
        }
        sink.write(std::slice::from_ref(&r)).await.unwrap();
    }
}
