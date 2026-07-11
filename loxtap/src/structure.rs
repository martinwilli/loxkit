//! Structure-file fetch and the uuid -> metadata map.
//!
//! The Miniserver serves a JSON structure file describing every control, its
//! room and category, and the state uuids it publishes. Value events, however,
//! arrive keyed only by a state uuid, so this module inverts the file into a
//! flat `uuid -> labels` map at load time: resolving an event to its human
//! labels is then a single hash lookup instead of a tree walk per event.
//!
//! The crux of the file is the `states` object on each control. It is keyed by
//! the state name (the "sub"), and each value is either a single uuid string or
//! an array of uuid strings. Every one of those uuids is a series we might see
//! on the wire, so all of them are inverted; the walk also recurses into a
//! control's `subControls`, which inherit their parent's room and category.
//!
//! Not every wire uuid lives under a control: the top-level `globalStates`
//! block (`hasInternet`, `sunrise`, ...) publishes uuids too. It carries no
//! owning control, room or category, so it is folded in flat, keyed on the
//! state name as the label.
//!
//! Freshness is cheap to check: `jdev/sps/LoxAPPversion3` returns the same
//! `lastModified` stamp the structure file carries, so the (large) file is only
//! re-fetched when that stamp changes.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use crate::model::{LoxUuid, SeriesMeta};
use crate::transport::{Inbound, Transport, TransportError};

/// Command that returns the structure file as a raw-JSON text frame.
const CMD_STRUCTURE: &str = "data/LoxAPP3.json";
/// Command that returns just the structure file's `lastModified` stamp.
const CMD_VERSION: &str = "jdev/sps/LoxAPPversion3";

/// Errors from fetching or parsing the structure file.
#[derive(Debug, thiserror::Error)]
pub enum StructureError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("structure file is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("LoxAPPversion3 failed with code {0}")]
    Version(i64),
    #[error("LoxAPPversion3 reply carried no lastModified stamp")]
    MissingVersion,
}

/// The resolved labels for a single state uuid.
///
/// Produced by inverting a control's `states`; `sub` is the state name the uuid
/// was published under. The state uuid is globally unique, so it alone keys a
/// series; `sub` is carried so a later phase can decide whether it ever means
/// more than a within-control index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateEntry {
    /// The owning control's type (e.g. `InfoOnlyAnalog`).
    pub control: String,
    /// The owning control's human name (a mutable label).
    pub name: String,
    /// The resolved room name, if the control (or its parent) has one.
    pub room: Option<String>,
    /// The resolved category name, if the control (or its parent) has one.
    pub category: Option<String>,
    /// The state name under which this uuid was published.
    pub sub: String,
    /// A display unit derived from the control's format, if any.
    pub unit: Option<String>,
}

/// A parsed structure file: the flat uuid map plus its freshness stamp.
#[derive(Clone, Debug, Default)]
pub struct Structure {
    last_modified: String,
    index: HashMap<LoxUuid, StateEntry>,
}

impl Structure {
    /// Parse a structure-file JSON string into the inverted uuid map.
    ///
    /// Split out from the fetch so it is unit-testable against a fixture. The
    /// input is the raw structure JSON, not an `LL` envelope.
    pub fn parse(json: &str) -> Result<Self, StructureError> {
        let raw: RawStructure = serde_json::from_str(json)?;
        let mut index = HashMap::new();
        for control in raw.controls.values() {
            index_control(&mut index, control, &raw.rooms, &raw.cats, None, None);
        }
        // The value stream also carries uuids that live outside `controls{}`:
        // the top-level `globalStates` block. It has no owning control, room
        // or category, so fold it in flat with the state name as the label
        // instead of leaving it bare on the wire.
        index_flat_states(&mut index, &raw.global_states, "GlobalState");
        Ok(Self {
            last_modified: raw.last_modified,
            index,
        })
    }

    /// Resolve a state uuid to its labels in O(1); the event-time hot path.
    pub fn resolve(&self, uuid: &LoxUuid) -> Option<&StateEntry> {
        self.index.get(uuid)
    }

    /// Build the [`SeriesMeta`] a sink upserts for a resolved uuid.
    pub fn series_meta(&self, uuid: &LoxUuid) -> Option<SeriesMeta> {
        self.index.get(uuid).map(|e| SeriesMeta {
            uuid: *uuid,
            control: e.control.clone(),
            name: e.name.clone(),
            room: e.room.clone(),
            category: e.category.clone(),
            sub: e.sub.clone(),
            unit: e.unit.clone(),
        })
    }

    /// The `lastModified` stamp this structure was fetched at.
    pub fn last_modified(&self) -> &str {
        &self.last_modified
    }

    /// Number of state uuids in the map.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

/// Fetch the current `lastModified` stamp of the structure file.
///
/// A plain status command with a normal `LL` reply, so it goes through
/// [`Transport::command`].
pub async fn fetch_version(transport: &mut Transport) -> Result<String, StructureError> {
    let resp = transport.command(CMD_VERSION).await?;
    if resp.code() != 200 {
        return Err(StructureError::Version(resp.code()));
    }
    resp.value()
        .as_str()
        .map(str::to_owned)
        .ok_or(StructureError::MissingVersion)
}

/// Fetch and parse the whole structure file.
pub async fn fetch_structure(transport: &mut Transport) -> Result<Structure, StructureError> {
    let json = fetch_structure_json(transport).await?;
    Structure::parse(&json)
}

/// Re-fetch the structure only if its stamp changed.
///
/// Returns `Some(new)` when the file changed and `None` when the in-memory
/// `current` is still fresh, so a caller keeps the existing map on the common
/// no-change path. This is the freshness gate the supervisor drives on reconnect
/// or on a periodic poll.
pub async fn refresh_if_stale(
    transport: &mut Transport,
    current: &Structure,
) -> Result<Option<Structure>, StructureError> {
    let version = fetch_version(transport).await?;
    if version == current.last_modified {
        return Ok(None);
    }
    Ok(Some(fetch_structure(transport).await?))
}

/// Send `data/LoxAPP3.json` and collect its raw-JSON text payload.
///
/// Unlike a normal command, the structure file is not an `LL` envelope: it comes
/// back as the raw JSON, typically preceded by an 8-byte binary header that
/// announces the payload. [`Transport::command`] would wait for an `LL` reply
/// and skip the binary frame, so drive [`Transport::next_inbound`] directly
/// instead. That already resolves both shapes the box might use (a direct text
/// frame, or a header frame followed by a text frame) into [`Inbound::Text`];
/// any stray binary table seen meanwhile is skipped.
///
/// NOTE (Phase E, confirm live): sent as a plain command here. If a live box
/// declines the plain `data/LoxAPP3.json` once encryption is active and wants it
/// through the `enc` channel, route it via the session instead.
async fn fetch_structure_json(transport: &mut Transport) -> Result<String, StructureError> {
    transport.send_text(CMD_STRUCTURE).await?;
    loop {
        match transport.next_inbound().await? {
            Inbound::Text(text) => return Ok(text),
            Inbound::Binary { .. } => continue,
        }
    }
}

/// The subset of the structure file we care about.
#[derive(Deserialize)]
struct RawStructure {
    #[serde(rename = "lastModified", default)]
    last_modified: String,
    #[serde(default)]
    rooms: HashMap<String, RawNamed>,
    #[serde(default)]
    cats: HashMap<String, RawNamed>,
    #[serde(default)]
    controls: HashMap<String, RawControl>,
    /// Predefined system states keyed by a fixed name (e.g. `hasInternet`),
    /// each a uuid string or an array of them. Not under any control.
    #[serde(rename = "globalStates", default)]
    global_states: HashMap<String, Value>,
}

/// A room or category: we only need its name.
#[derive(Deserialize)]
struct RawNamed {
    #[serde(default)]
    name: String,
}

/// A control (or subcontrol) and the state uuids it publishes.
#[derive(Deserialize)]
struct RawControl {
    #[serde(default)]
    name: String,
    #[serde(rename = "type", default)]
    control_type: String,
    #[serde(default)]
    room: Option<String>,
    #[serde(default)]
    cat: Option<String>,
    /// State name -> uuid string, or an array of uuid strings. Kept as raw
    /// `Value` so an unexpected shape skips that state rather than failing the
    /// whole parse.
    #[serde(default)]
    states: HashMap<String, Value>,
    #[serde(rename = "subControls", default)]
    sub_controls: HashMap<String, RawControl>,
    #[serde(default)]
    details: Option<RawDetails>,
}

/// A control's display details; we mine the format string for a unit.
#[derive(Deserialize)]
struct RawDetails {
    #[serde(default)]
    format: Option<String>,
}

/// Invert one control's states into the flat map, then recurse into its
/// subcontrols. `inherited_room`/`inherited_cat` are the parent's already
/// resolved names, used when a subcontrol carries none of its own.
fn index_control(
    out: &mut HashMap<LoxUuid, StateEntry>,
    control: &RawControl,
    rooms: &HashMap<String, RawNamed>,
    cats: &HashMap<String, RawNamed>,
    inherited_room: Option<&str>,
    inherited_cat: Option<&str>,
) {
    let room = control
        .room
        .as_deref()
        .and_then(|id| rooms.get(id))
        .map(|r| r.name.clone())
        .or_else(|| inherited_room.map(str::to_owned));
    let category = control
        .cat
        .as_deref()
        .and_then(|id| cats.get(id))
        .map(|c| c.name.clone())
        .or_else(|| inherited_cat.map(str::to_owned));
    let unit = control
        .details
        .as_ref()
        .and_then(|d| d.format.as_deref())
        .and_then(unit_from_format);

    for (sub, value) in &control.states {
        for uuid_str in state_uuids(value) {
            if let Ok(uuid) = uuid_str.parse::<LoxUuid>() {
                out.insert(
                    uuid,
                    StateEntry {
                        control: control.control_type.clone(),
                        name: control.name.clone(),
                        room: room.clone(),
                        category: category.clone(),
                        sub: sub.clone(),
                        unit: unit.clone(),
                    },
                );
            }
        }
    }

    for sub in control.sub_controls.values() {
        index_control(out, sub, rooms, cats, room.as_deref(), category.as_deref());
    }
}

/// Invert a flat `name -> uuid | [uuid]` state map into the index.
///
/// `globalStates` has no owning control, room or category, so the state name
/// doubles as the human label. `control` is a synthetic type tag
/// ("GlobalState") that keeps these apart from real controls downstream.
/// Unexpected value shapes are skipped, matching [`index_control`].
fn index_flat_states(
    out: &mut HashMap<LoxUuid, StateEntry>,
    states: &HashMap<String, Value>,
    control: &str,
) {
    for (name, value) in states {
        for uuid_str in state_uuids(value) {
            if let Ok(uuid) = uuid_str.parse::<LoxUuid>() {
                out.insert(
                    uuid,
                    StateEntry {
                        control: control.to_owned(),
                        name: name.clone(),
                        room: None,
                        category: None,
                        sub: name.clone(),
                        unit: None,
                    },
                );
            }
        }
    }
}

/// The uuid strings a single state value carries: one string, or an array of
/// them. Any other shape yields nothing.
fn state_uuids(value: &Value) -> Vec<&str> {
    match value {
        Value::String(s) => vec![s.as_str()],
        Value::Array(items) => items.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

/// Extract a display unit from a Loxone printf-style format string.
///
/// The Miniserver embeds units in a control's `details.format`, e.g. `"%.1f°C"`
/// or `"%.0f%%"`. The unit is whatever trails the numeric conversion, with an
/// escaped `%%` collapsed to a literal `%`. Returns `None` when no unit trails
/// the conversion (e.g. a bare `"%i"`).
fn unit_from_format(format: &str) -> Option<String> {
    let bytes = format.as_bytes();
    let pct = bytes.iter().position(|&b| b == b'%')?;
    // Skip the conversion flags/width/precision to reach the conversion letter.
    let mut i = pct + 1;
    while i < bytes.len() && matches!(bytes[i], b'-' | b'+' | b' ' | b'#' | b'0'..=b'9' | b'.') {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    // The unit is the text past the single conversion letter.
    let unit = format[i + 1..].replace("%%", "%");
    let unit = unit.trim();
    if unit.is_empty() {
        None
    } else {
        Some(unit.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A synthetic structure file with invented rooms, categories and uuids —
    // never a capture of a real Miniserver. It exercises the tricky shapes: a
    // single-uuid state, an array-valued state, a nested subcontrol that
    // inherits its parent's room and category, plus the flat `globalStates`
    // block that lives outside `controls{}`.
    const FIXTURE: &str = r#"{
      "lastModified": "2026-01-02 03:04:05",
      "rooms": {
        "deadbeef-0000-0000-0000000000000001": { "name": "Room A" }
      },
      "cats": {
        "deadbeef-0000-0000-0000000000000002": { "name": "Category X", "type": "undefined" }
      },
      "globalStates": {
        "hasInternet": "deadbeef-9999-0000-0000000000000001",
        "sunrise": "deadbeef-9999-0000-0000000000000002"
      },
      "controls": {
        "deadbeef-1111-0000-0000000000000001": {
          "name": "Temperature",
          "type": "InfoOnlyAnalog",
          "uuidAction": "deadbeef-1111-0000-0000000000000001",
          "room": "deadbeef-0000-0000-0000000000000001",
          "cat": "deadbeef-0000-0000-0000000000000002",
          "details": { "format": "%.1f°C" },
          "states": { "value": "deadbeef-2222-0000-0000000000000001" }
        },
        "deadbeef-1111-0000-0000000000000002": {
          "name": "Meter",
          "type": "InfoOnlyAnalog",
          "uuidAction": "deadbeef-1111-0000-0000000000000002",
          "room": "deadbeef-0000-0000-0000000000000001",
          "cat": "deadbeef-0000-0000-0000000000000002",
          "states": {
            "outputs": [
              "deadbeef-2222-0000-0000000000000010",
              "deadbeef-2222-0000-0000000000000011"
            ]
          },
          "subControls": {
            "deadbeef-3333-0000-0000000000000001": {
              "name": "Sub Meter",
              "type": "InfoOnlyAnalog",
              "uuidAction": "deadbeef-3333-0000-0000000000000001",
              "states": { "value": "deadbeef-4444-0000-0000000000000001" }
            }
          }
        }
      }
    }"#;

    fn uuid(s: &str) -> LoxUuid {
        s.parse().unwrap()
    }

    #[test]
    fn parses_stamp_and_all_state_uuids() {
        let structure = Structure::parse(FIXTURE).unwrap();
        assert_eq!(structure.last_modified(), "2026-01-02 03:04:05");
        // Four control states (one single-value, two array outputs, one
        // subcontrol) plus two globalStates.
        assert_eq!(structure.len(), 6);
    }

    #[test]
    fn folds_global_states_flat() {
        let structure = Structure::parse(FIXTURE).unwrap();
        let entry = structure
            .resolve(&uuid("deadbeef-9999-0000-0000000000000001"))
            .unwrap();
        // The state name doubles as the label; no owning control, room or cat.
        assert_eq!(entry.name, "hasInternet");
        assert_eq!(entry.control, "GlobalState");
        assert_eq!(entry.sub, "hasInternet");
        assert_eq!(entry.room, None);
        assert_eq!(entry.category, None);
        assert_eq!(entry.unit, None);
    }

    #[test]
    fn resolves_single_state_with_room_cat_and_unit() {
        let structure = Structure::parse(FIXTURE).unwrap();
        let entry = structure
            .resolve(&uuid("deadbeef-2222-0000-0000000000000001"))
            .unwrap();
        assert_eq!(entry.name, "Temperature");
        assert_eq!(entry.control, "InfoOnlyAnalog");
        assert_eq!(entry.sub, "value");
        assert_eq!(entry.room.as_deref(), Some("Room A"));
        assert_eq!(entry.category.as_deref(), Some("Category X"));
        assert_eq!(entry.unit.as_deref(), Some("°C"));
    }

    #[test]
    fn inverts_array_valued_state() {
        let structure = Structure::parse(FIXTURE).unwrap();
        for id in [
            "deadbeef-2222-0000-0000000000000010",
            "deadbeef-2222-0000-0000000000000011",
        ] {
            let entry = structure.resolve(&uuid(id)).unwrap();
            assert_eq!(entry.name, "Meter");
            assert_eq!(entry.sub, "outputs");
            assert_eq!(entry.room.as_deref(), Some("Room A"));
            // No format on this control, so no unit.
            assert_eq!(entry.unit, None);
        }
    }

    #[test]
    fn recurses_into_subcontrol_inheriting_room_and_cat() {
        let structure = Structure::parse(FIXTURE).unwrap();
        let entry = structure
            .resolve(&uuid("deadbeef-4444-0000-0000000000000001"))
            .unwrap();
        assert_eq!(entry.name, "Sub Meter");
        assert_eq!(entry.sub, "value");
        // The subcontrol carries no room/cat of its own; it inherits Meter's.
        assert_eq!(entry.room.as_deref(), Some("Room A"));
        assert_eq!(entry.category.as_deref(), Some("Category X"));
    }

    #[test]
    fn series_meta_carries_the_sub_and_labels() {
        let structure = Structure::parse(FIXTURE).unwrap();
        let id = uuid("deadbeef-2222-0000-0000000000000001");
        let meta = structure.series_meta(&id).unwrap();
        assert_eq!(meta.uuid, id);
        assert_eq!(meta.name, "Temperature");
        assert_eq!(meta.sub, "value");
        assert_eq!(meta.room.as_deref(), Some("Room A"));
        assert_eq!(meta.unit.as_deref(), Some("°C"));
        assert!(structure.series_meta(&uuid("00000000-0000-0000-0000000000000000")).is_none());
    }

    #[test]
    fn unit_extraction_handles_common_formats() {
        assert_eq!(unit_from_format("%.1f°C").as_deref(), Some("°C"));
        assert_eq!(unit_from_format("%.0f%%").as_deref(), Some("%"));
        assert_eq!(unit_from_format("%.1f kWh").as_deref(), Some("kWh"));
        assert_eq!(unit_from_format("%i"), None);
        assert_eq!(unit_from_format("no-conversion"), None);
    }

    #[test]
    fn skips_non_uuid_state_shapes_without_failing() {
        // An object-valued state (unexpected shape) is ignored, not fatal.
        let json = r#"{
          "controls": {
            "deadbeef-1111-0000-0000000000000009": {
              "name": "Odd",
              "type": "Weird",
              "states": {
                "junk": { "nested": true },
                "value": "deadbeef-2222-0000-0000000000000099"
              }
            }
          }
        }"#;
        let structure = Structure::parse(json).unwrap();
        assert_eq!(structure.len(), 1);
        assert!(structure.resolve(&uuid("deadbeef-2222-0000-0000000000000099")).is_some());
    }
}
