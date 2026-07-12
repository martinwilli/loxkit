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
//! A control's own `uuidAction` (the handle you command it by) can also stream a
//! value, and it is not listed under `states`. It is indexed as an `action`
//! fallback so it resolves to the control's labels rather than logging bare,
//! but never over a real state that happens to share it.
//!
//! A control can also define `statistic.outputs`: named, printf-formatted series
//! the operator chose to log. Each output carries its own name and unit, so it
//! wins over a plain state label for the same uuid, and some outputs have a
//! dedicated uuid that is not a state at all.
//!
//! Freshness is cheap to check: `jdev/sps/LoxAPPversion3` returns the same
//! `lastModified` stamp the structure file carries, so the (large) file is only
//! re-fetched when that stamp changes.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;
use tracing::debug;

use crate::model::{AggMode, LoxUuid, SeriesMeta};
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
    /// A display unit derived from the control's format. Set on the primary
    /// `value` state only; other states (an `error` flag, status/enum states)
    /// carry none, since the format describes the value.
    pub unit: Option<String>,
    /// How this series aggregates over time. Classified from a statistic
    /// output's `visuType`; a series with no statistic output stays a gauge,
    /// the safe default (time-weighting an instantaneous value is correct,
    /// only a true monotonic counter needs delta treatment).
    pub agg_mode: AggMode,
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
            agg_mode: e.agg_mode,
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
    /// The uuid the control is commanded by. Not under `states`, but the box may
    /// still stream a value for it, so it is indexed as an `action` fallback.
    #[serde(rename = "uuidAction", default)]
    uuid_action: Option<String>,
    /// Operator-configured logged series, each with its own name, format and
    /// uuid. Self-describing, so they win over a plain state label.
    #[serde(default)]
    statistic: Option<RawStatistic>,
}

/// A control's display details; we mine the format string for a unit.
#[derive(Deserialize)]
struct RawDetails {
    #[serde(default)]
    format: Option<String>,
}

/// The `statistic` block on a control: the outputs it logs.
#[derive(Deserialize)]
struct RawStatistic {
    #[serde(default)]
    outputs: Vec<RawStatOutput>,
}

/// One statistic output: a named, printf-formatted series with its own uuid.
#[derive(Deserialize)]
struct RawStatOutput {
    #[serde(default)]
    name: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    uuid: Option<String>,
    /// Loxone's own gauge/counter tag: `0` = instantaneous gauge (e.g. kW),
    /// `2` = monotonic counter (e.g. kWh). Absent or unknown maps to the gauge
    /// default.
    #[serde(rename = "visuType", default)]
    visu_type: Option<i64>,
}

/// Invert one control's states into the flat map, then recurse into its
/// subcontrols. `inherited_room`/`inherited_cat` are the parent's already
/// resolved names, used when a subcontrol carries none of its own. The control's
/// own `uuidAction` is indexed as an `action` fallback (see below) so it resolves
/// even though it is not one of the `states`.
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
        // The control's `details.format` describes its primary `value` state;
        // every other state (an analog sensor's `error` flag, a status/enum
        // state) carries no unit of its own, so a wrong unit never beats none.
        let entry_unit = if sub == "value" { unit.clone() } else { None };
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
                        unit: entry_unit.clone(),
                        // A plain state carries no statistic output, so it is a
                        // gauge; a visuType-2 output for the same uuid overwrites
                        // this below and reclassifies it as a counter.
                        agg_mode: AggMode::Gauge,
                    },
                );
            }
        }
    }

    // Statistic outputs are the series the operator configured this control to
    // log, each with its own name and printf format. They win over a plain state
    // label for the same uuid (a chosen name and unit beat the terse state sub),
    // and some outputs have a dedicated uuid that is no state at all. Insert (not
    // or_insert) so they overwrite any state entry above; uuids are globally
    // unique, so this only ever replaces this control's own state.
    if let Some(statistic) = &control.statistic {
        for output in &statistic.outputs {
            let Some(uuid) = output
                .uuid
                .as_deref()
                .and_then(|s| s.parse::<LoxUuid>().ok())
            else {
                continue;
            };
            out.insert(
                uuid,
                StateEntry {
                    control: control.control_type.clone(),
                    name: output.name.clone(),
                    room: room.clone(),
                    category: category.clone(),
                    // The name is authoritative; carry it as the sub too so the
                    // sink renders one clean name rather than name/sub.
                    sub: output.name.clone(),
                    unit: output.format.as_deref().and_then(unit_from_format),
                    // The output's visuType is Loxone's own gauge/counter signal.
                    // Insert (not or_insert) means a visuType-2 output overwrites
                    // any backing state entry, so a state-backed total classifies
                    // as a counter.
                    agg_mode: agg_mode_from_visu_type(output.visu_type),
                },
            );
        }
    }

    // The value stream can also carry the control's own uuidAction value, which
    // is not listed under `states`. Fall back to indexing it under a synthetic
    // "action" sub so it resolves to at least the control's labels instead of
    // logging bare. Never over a real state, though: for some control types the
    // action uuid *is* a state uuid, and that state's descriptive sub must win.
    // States above were inserted first and uuids are globally unique, so
    // or_insert leaves any real state entry untouched.
    if let Some(uuid) = control
        .uuid_action
        .as_deref()
        .and_then(|s| s.parse::<LoxUuid>().ok())
    {
        out.entry(uuid).or_insert_with(|| StateEntry {
            control: control.control_type.clone(),
            name: control.name.clone(),
            room: room.clone(),
            category: category.clone(),
            sub: "action".to_owned(),
            // The action value carries no declared unit of its own.
            unit: None,
            agg_mode: AggMode::Gauge,
        });
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
                        agg_mode: AggMode::Gauge,
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

/// Classify a statistic output's aggregation mode from its `visuType`.
///
/// Loxone tags each output `0` for an instantaneous gauge (e.g. kW) or `2` for a
/// monotonic counter (e.g. kWh). An absent tag is the expected no-statistic case
/// and stays a gauge silently; an unseen non-`{0,2}` value (none on the box
/// today, but a future firmware might add one) also falls back to gauge, the safe
/// default, and logs it so it surfaces.
fn agg_mode_from_visu_type(visu_type: Option<i64>) -> AggMode {
    match visu_type {
        Some(0) | None => AggMode::Gauge,
        Some(2) => AggMode::Counter,
        Some(other) => {
            debug!("unknown statistic visuType {other}, defaulting to gauge");
            AggMode::Gauge
        }
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
    // inherits its parent's room and category, the flat `globalStates`
    // block that lives outside `controls{}`, a Switch whose uuidAction is
    // one of its own state uuids (the action-fallback collision), and a
    // Solar control with statistic outputs covering every aggregation-mode
    // shape: a visuType-0 gauge output overlapping the prodCurr state, a
    // dedicated visuType-2 counter (no backing state), and a state-backed
    // visuType-2 counter overwriting the `total` state.
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
          "states": {
            "value": "deadbeef-2222-0000-0000000000000001",
            "error": "deadbeef-2222-0000-0000000000000002"
          }
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
        },
        "deadbeef-1111-0000-0000000000000003": {
          "name": "Switch",
          "type": "Switch",
          "uuidAction": "deadbeef-2222-0000-0000000000000030",
          "room": "deadbeef-0000-0000-0000000000000001",
          "states": { "active": "deadbeef-2222-0000-0000000000000030" }
        },
        "deadbeef-1111-0000-0000000000000004": {
          "name": "Solar",
          "type": "Fronius",
          "uuidAction": "deadbeef-1111-0000-0000000000000004",
          "room": "deadbeef-0000-0000-0000000000000001",
          "cat": "deadbeef-0000-0000-0000000000000002",
          "states": {
            "prodCurr": "deadbeef-6666-0000-0000000000000001",
            "total": "deadbeef-6666-0000-0000000000000003"
          },
          "statistic": {
            "outputs": [
              { "id": 0, "name": "Leistung Produktion", "format": "%.3f kW", "visuType": 0, "uuid": "deadbeef-6666-0000-0000000000000001" },
              { "id": 1, "name": "Zähler Produktion", "format": "%.3f kWh", "visuType": 2, "uuid": "deadbeef-6666-0000-0000000000000002" },
              { "id": 2, "name": "Zähler Verbrauch", "format": "%.3f kWh", "visuType": 2, "uuid": "deadbeef-6666-0000-0000000000000003" }
            ]
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
        // Eight state uuids (Temperature value + error, two Meter outputs, one
        // subcontrol, one Switch, one Solar prodCurr, one Solar total), two
        // globalStates, one statistic-only output (Solar's Zähler Produktion; the
        // other two outputs overwrite the prodCurr and total states, no new key),
        // and four action fallbacks (Temperature, Meter, Sub Meter, Solar — the
        // Switch action collides with its own state).
        assert_eq!(structure.len(), 15);
    }

    #[test]
    fn statistic_output_resolves_with_its_own_name_and_unit() {
        let structure = Structure::parse(FIXTURE).unwrap();
        // A statistic-only uuid (no matching state) resolves to the output's own
        // name and format-derived unit, under the owning control's room/category.
        let entry = structure
            .resolve(&uuid("deadbeef-6666-0000-0000000000000002"))
            .unwrap();
        assert_eq!(entry.name, "Zähler Produktion");
        assert_eq!(entry.unit.as_deref(), Some("kWh"));
        assert_eq!(entry.room.as_deref(), Some("Room A"));
        assert_eq!(entry.category.as_deref(), Some("Category X"));
        assert_eq!(entry.control, "Fronius");
    }

    #[test]
    fn statistic_output_wins_over_a_state_for_the_same_uuid() {
        let structure = Structure::parse(FIXTURE).unwrap();
        // Solar's prodCurr state and statistic output id 0 share a uuid; the
        // statistic's chosen name and unit win over the terse state.
        let entry = structure
            .resolve(&uuid("deadbeef-6666-0000-0000000000000001"))
            .unwrap();
        assert_eq!(entry.name, "Leistung Produktion");
        assert_eq!(entry.unit.as_deref(), Some("kW"));
        // sub mirrors the name so the sink renders one clean label, not name/sub.
        assert_eq!(entry.sub, "Leistung Produktion");
    }

    #[test]
    fn classifies_aggregation_mode_from_visu_type() {
        let structure = Structure::parse(FIXTURE).unwrap();

        // A visuType-0 output (Leistung Produktion, kW) is a gauge, even though
        // it overwrites the prodCurr state for the same uuid.
        let gauge_out = uuid("deadbeef-6666-0000-0000000000000001");
        assert_eq!(structure.resolve(&gauge_out).unwrap().agg_mode, AggMode::Gauge);
        assert_eq!(structure.series_meta(&gauge_out).unwrap().agg_mode, AggMode::Gauge);

        // A dedicated visuType-2 output (Zähler Produktion, no backing state) is
        // a counter.
        let dedicated = uuid("deadbeef-6666-0000-0000000000000002");
        assert_eq!(structure.resolve(&dedicated).unwrap().agg_mode, AggMode::Counter);
        assert_eq!(structure.series_meta(&dedicated).unwrap().agg_mode, AggMode::Counter);

        // A state-backed visuType-2 output (Zähler Verbrauch shares the `total`
        // state uuid): the state alone would default to gauge, but the output's
        // insert overwrites it as a counter — the load-bearing overwrite.
        let state_backed = uuid("deadbeef-6666-0000-0000000000000003");
        assert_eq!(structure.resolve(&state_backed).unwrap().agg_mode, AggMode::Counter);
        assert_eq!(
            structure.series_meta(&state_backed).unwrap().agg_mode,
            AggMode::Counter
        );

        // A plain state with no statistic output defaults to gauge.
        let plain = uuid("deadbeef-2222-0000-0000000000000001");
        assert_eq!(structure.resolve(&plain).unwrap().agg_mode, AggMode::Gauge);
        assert_eq!(structure.series_meta(&plain).unwrap().agg_mode, AggMode::Gauge);
    }

    #[test]
    fn format_unit_lands_only_on_the_value_state() {
        let structure = Structure::parse(FIXTURE).unwrap();
        // Temperature's format ("%.1f°C") units its value state...
        let value = structure
            .resolve(&uuid("deadbeef-2222-0000-0000000000000001"))
            .unwrap();
        assert_eq!(value.sub, "value");
        assert_eq!(value.unit.as_deref(), Some("°C"));
        // ...but not its error state, which carries no unit of its own.
        let error = structure
            .resolve(&uuid("deadbeef-2222-0000-0000000000000002"))
            .unwrap();
        assert_eq!(error.sub, "error");
        assert_eq!(error.name, "Temperature");
        assert_eq!(error.unit, None);
    }

    #[test]
    fn indexes_uuid_action_as_action_fallback() {
        let structure = Structure::parse(FIXTURE).unwrap();
        // Temperature's uuidAction is not among its states, so it resolves to
        // the control's labels under a synthetic "action" sub.
        let entry = structure
            .resolve(&uuid("deadbeef-1111-0000-0000000000000001"))
            .unwrap();
        assert_eq!(entry.name, "Temperature");
        assert_eq!(entry.control, "InfoOnlyAnalog");
        assert_eq!(entry.sub, "action");
        assert_eq!(entry.room.as_deref(), Some("Room A"));
        assert_eq!(entry.category.as_deref(), Some("Category X"));
        // The action value has no declared unit, even though the control's state
        // does (°C).
        assert_eq!(entry.unit, None);
    }

    #[test]
    fn uuid_action_does_not_override_a_real_state() {
        let structure = Structure::parse(FIXTURE).unwrap();
        // The Switch's uuidAction equals its "active" state uuid; the real state
        // label must win over the generic action fallback.
        let entry = structure
            .resolve(&uuid("deadbeef-2222-0000-0000000000000030"))
            .unwrap();
        assert_eq!(entry.name, "Switch");
        assert_eq!(entry.sub, "active");
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
