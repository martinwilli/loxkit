//! The dashboard view model, its validation, and the reload-aware holder.
//!
//! A dashboard is a list of *views*; a view is a list of *panels*; a panel is
//! `axes[] + series[]`. Every series is either **bound** (a `uuid` plus an
//! optional `sub`/`agg`, read straight from the store) or **derived** (an
//! `expr` computed from its sibling series). Presentation keys are all optional
//! and appear only when they carry meaning, so the common series is one line.
//!
//! The model is deliberately dumb data; the interesting parts are:
//!
//! * [`Dashboards::validate`] — the semantic checks that a bare parse cannot do:
//!   binding exclusivity, axis references, and `expr` reference resolution plus
//!   cycle detection over the sibling graph.
//! * [`DashboardStore`] — the live set behind an [`ArcSwap`], hot-swapped when
//!   the file changes on disk. A bad reload is logged and dropped; the prior set
//!   stays live, so an operator's typo never takes the dashboards down.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context as _, bail};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

/// loxtap's default sub-channel: a control's primary state carries no sub-name
/// of its own, so a bound series that omits `sub` resolves to this. Mirrors the
/// daemon's structure resolution (a plain `value` state).
pub const DEFAULT_SUB: &str = "value";

/// The whole dashboard set — the parsed contents of `dashboards.yaml`.
///
/// Also `Serialize`: the HTTP layer hands the current snapshot straight to the
/// frontend as JSON. The `skip_serializing_if` on the optional fields keeps that
/// JSON as terse as the YAML — an absent key stays absent, never `null`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dashboards {
    /// The views, each a tab of panels.
    pub views: Vec<View>,
}

/// A view: one named tab of panels sharing a default time window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct View {
    /// Stable id (used in URLs and API paths).
    pub id: String,
    /// Human title.
    pub title: String,
    /// Default time window, e.g. `now-6M`. Absent means the app default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    /// Auto-refresh cadence, e.g. `10s`, or `off`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh: Option<String>,
    /// Calendar-month bucketing for this view (the one calendar exception; else
    /// the engine tier-routes by span). Inherited by panels that omit their own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<Bucket>,
    /// The panels in this view.
    pub panels: Vec<Panel>,
}

/// A panel: one chart, `axes[] + series[]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Panel {
    /// Panel title.
    pub title: String,
    /// Relative height hint (grid rows); layout is otherwise automatic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    /// Calendar-month bucketing for this panel, overriding the view's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<Bucket>,
    /// Declared y-axes; a series may target one by `id`. Empty means a single
    /// implicit axis.
    #[serde(default)]
    pub axes: Vec<Axis>,
    /// The series drawn in this panel.
    pub series: Vec<Series>,
}

/// A declared y-axis a series can be pinned to.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Axis {
    /// Axis id, referenced by `series.axis`.
    pub id: String,
    /// Which side to draw the axis on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<Side>,
    /// Unit label; absent falls back to `series_meta.unit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// Fixed axis minimum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    /// Fixed axis maximum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

/// A single series: either bound to a store uuid or derived from siblings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Series {
    /// Optional id, so a derived sibling can reference this series in an `expr`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Legend label; absent falls back to `series_meta.name`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Target axis id (must name an [`Axis`] in the same panel).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub axis: Option<String>,
    /// Draw as a line or bars.
    #[serde(rename = "type", default)]
    pub kind: SeriesType,
    /// Explicit color; absent uses the default palette.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// Line interpolation.
    #[serde(default)]
    pub interp: Interp,
    /// Fill the area under a line.
    #[serde(default)]
    pub fill: bool,
    /// Stack group; series sharing a group stack together.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
    /// Multiplier applied to values, e.g. `-1` to mirror below zero or `0.001`
    /// to rescale units.
    #[serde(default = "default_scale")]
    pub scale: f64,

    // --- binding: exactly one of {uuid (+sub,+agg)} or {expr} ---
    /// Bound series: the store uuid to read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    /// Bound series: the sub-channel; omitted resolves to [`DEFAULT_SUB`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    /// Bound series: the aggregation. Absent is resolved later from
    /// `series_meta.agg_mode` (commit 3), so no default is imposed here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agg: Option<Agg>,
    /// Bound series: how an empty bucket is filled on the shared query grid —
    /// carry the last value (`locf`), linearly `interpolate`, or leave it empty
    /// (`none`, read as 0). Absent defaults to `none`; a signal that should carry
    /// or ramp across gaps (a temperature, a level) opts into `locf`/`interpolate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gapfill: Option<Gapfill>,
    /// Derived series: an arithmetic expression over sibling series ids.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expr: Option<String>,
}

impl Series {
    /// The sub-channel this bound series reads, applying loxtap's `value`
    /// default when a control names no sub explicitly. Meaningless for a
    /// derived series (which has no uuid); callers gate on the binding.
    pub fn resolved_sub(&self) -> &str {
        self.sub.as_deref().unwrap_or(DEFAULT_SUB)
    }
}

/// Calendar-month bucketing. `None` on a view/panel means the engine
/// tier-routes by span; `month` is the single calendar exception.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Bucket {
    /// Real calendar months in local time.
    Month,
}

/// Which side of the panel an axis is drawn on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Left,
    Right,
}

/// A series' chart type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeriesType {
    /// A line (the default).
    #[default]
    Line,
    /// Bars.
    Bar,
}

/// Line interpolation between points.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Interp {
    /// Straight segments (the default).
    #[default]
    Linear,
    /// Smoothed curve.
    Smooth,
    /// Step (hold last value), e.g. for a valve position.
    Step,
}

/// The aggregation applied when bucketing a bound series. No default: an absent
/// `agg` is resolved from `series_meta.agg_mode` at query time (commit 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Agg {
    /// Time-weighted (or plain) average.
    Avg,
    Max,
    Min,
    Median,
    /// Last value in the bucket.
    Last,
    /// Counter delta over the bucket.
    Delta,
    /// Time-weighted integral over the bucket.
    Integral,
}

/// How an empty bucket gets a value on the shared query grid. Every bound series
/// is bucketed onto the same dense grid so a panel's stack lines up index-for-
/// index; this picks what fills the empties. Absent on a series defaults to
/// `None` (no fill); `locf`/`interpolate` are explicit opt-ins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Gapfill {
    /// Carry the last observed value forward (a zero-order hold); for a level
    /// that genuinely holds between samples, like a temperature or a setpoint.
    /// (An on-change power meter that goes quiet at zero wants `None`, not this.)
    Locf,
    /// Linearly interpolate between the surrounding points (a first-order hold);
    /// for a smoothly-varying, coarsely-sampled signal like a forecast.
    Interpolate,
    /// No fill (the default): an empty bucket stays empty and reads as 0. Right
    /// for an accumulation, or an on-change meter that goes quiet at zero.
    None,
}

fn default_scale() -> f64 {
    1.0
}

impl Dashboards {
    /// Validate the semantic invariants a bare parse cannot enforce: per-series
    /// binding exclusivity, axis references, and `expr` reference resolution
    /// plus cycle detection. Errors name the view/panel/series so an operator
    /// can find the offending line.
    pub fn validate(&self) -> anyhow::Result<()> {
        for view in &self.views {
            for panel in &view.panels {
                validate_panel(view, panel)?;
            }
        }
        Ok(())
    }
}

/// A `view / panel` prefix for error messages.
fn loc(view: &View, panel: &Panel) -> String {
    format!("view '{}' / panel '{}'", view.id, panel.title)
}

/// A best-effort human handle for a series in error messages.
fn series_label(series: &Series, idx: usize) -> String {
    if let Some(id) = &series.id {
        format!("series '{id}'")
    } else if let Some(label) = &series.label {
        format!("series '{label}'")
    } else {
        format!("series #{}", idx + 1)
    }
}

fn validate_panel(view: &View, panel: &Panel) -> anyhow::Result<()> {
    let axis_ids: HashSet<&str> = panel.axes.iter().map(|a| a.id.as_str()).collect();

    // First pass: binding exclusivity, id uniqueness, and axis references.
    let mut series_ids: HashSet<&str> = HashSet::new();
    for (idx, series) in panel.series.iter().enumerate() {
        let sref = series_label(series, idx);

        match (series.uuid.is_some(), series.expr.is_some()) {
            (true, true) => bail!(
                "{}: {sref} has both uuid and expr; a series is either bound (uuid) or derived (expr)",
                loc(view, panel)
            ),
            (false, false) => bail!(
                "{}: {sref} has neither uuid nor expr; a series must be bound (uuid) or derived (expr)",
                loc(view, panel)
            ),
            (false, true) => {
                if series.sub.is_some() || series.agg.is_some() || series.gapfill.is_some() {
                    bail!(
                        "{}: derived {sref} must not carry sub/agg/gapfill (those apply only to a bound uuid series)",
                        loc(view, panel)
                    );
                }
            }
            (true, false) => {}
        }

        if let Some(id) = &series.id
            && !series_ids.insert(id.as_str())
        {
            bail!("{}: duplicate series id '{id}'", loc(view, panel));
        }

        if let Some(axis) = &series.axis
            && !axis_ids.contains(axis.as_str())
        {
            bail!(
                "{}: {sref} references axis '{axis}' which is not declared in this panel",
                loc(view, panel)
            );
        }
    }

    // Second pass (needs the full id set): resolve expr references and build the
    // sibling dependency graph for cycle detection.
    let mut deps: HashMap<String, Vec<String>> = HashMap::new();
    for (idx, series) in panel.series.iter().enumerate() {
        let sref = series_label(series, idx);
        if let Some(expr) = &series.expr {
            let refs = parse_expr_refs(expr).with_context(|| {
                format!("{}: {sref} has an invalid expr '{expr}'", loc(view, panel))
            })?;
            for r in &refs {
                if !series_ids.contains(r.as_str()) {
                    bail!(
                        "{}: {sref} expr references unknown series '{r}' (must be a sibling series id in this panel)",
                        loc(view, panel)
                    );
                }
            }
            if let Some(id) = &series.id {
                deps.insert(id.clone(), refs.into_iter().collect());
            }
        } else if let Some(id) = &series.id {
            // A bound series is a leaf in the reference graph.
            deps.entry(id.clone()).or_default();
        }
    }

    detect_cycle(view, panel, &deps)
}

// --- expr grammar: `+ - * /`, parens, numeric constants, sibling ids ---

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Num(f64),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
}

/// Tokenize an `expr`, rejecting any character outside the grammar.
fn tokenize(input: &str) -> anyhow::Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some(&(i, c)) = chars.peek() {
        match c {
            c if c.is_whitespace() => {
                chars.next();
            }
            '+' => {
                tokens.push(Token::Plus);
                chars.next();
            }
            '-' => {
                tokens.push(Token::Minus);
                chars.next();
            }
            '*' => {
                tokens.push(Token::Star);
                chars.next();
            }
            '/' => {
                tokens.push(Token::Slash);
                chars.next();
            }
            '(' => {
                tokens.push(Token::LParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RParen);
                chars.next();
            }
            c if c.is_ascii_digit() || c == '.' => {
                let mut s = String::new();
                while let Some(&(_, d)) = chars.peek() {
                    if d.is_ascii_digit() || d == '.' {
                        s.push(d);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let n: f64 = s
                    .parse()
                    .with_context(|| format!("invalid numeric constant '{s}'"))?;
                tokens.push(Token::Num(n));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let mut s = String::new();
                while let Some(&(_, d)) = chars.peek() {
                    if d.is_ascii_alphanumeric() || d == '_' {
                        s.push(d);
                        chars.next();
                    } else {
                        break;
                    }
                }
                tokens.push(Token::Ident(s));
            }
            other => bail!("unexpected character '{other}' at byte {i}"),
        }
    }
    Ok(tokens)
}

/// A recursive-descent parser that validates the grammar and collects the set
/// of identifiers (sibling series ids) the expression references.
struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
    refs: HashSet<String>,
}

impl Parser<'_> {
    fn peek(&self) -> Option<Token> {
        self.tokens.get(self.pos).cloned()
    }

    fn parse_expr(&mut self) -> anyhow::Result<()> {
        self.parse_term()?;
        while matches!(self.peek(), Some(Token::Plus | Token::Minus)) {
            self.pos += 1;
            self.parse_term()?;
        }
        Ok(())
    }

    fn parse_term(&mut self) -> anyhow::Result<()> {
        self.parse_factor()?;
        while matches!(self.peek(), Some(Token::Star | Token::Slash)) {
            self.pos += 1;
            self.parse_factor()?;
        }
        Ok(())
    }

    fn parse_factor(&mut self) -> anyhow::Result<()> {
        match self.peek() {
            Some(Token::Plus | Token::Minus) => {
                self.pos += 1;
                self.parse_factor()
            }
            Some(Token::LParen) => {
                self.pos += 1;
                self.parse_expr()?;
                match self.peek() {
                    Some(Token::RParen) => {
                        self.pos += 1;
                        Ok(())
                    }
                    _ => bail!("expected ')'"),
                }
            }
            Some(Token::Num(_)) => {
                self.pos += 1;
                Ok(())
            }
            Some(Token::Ident(name)) => {
                self.refs.insert(name);
                self.pos += 1;
                Ok(())
            }
            Some(_) => bail!("unexpected operator"),
            None => bail!("unexpected end of expression"),
        }
    }
}

/// Parse an `expr`, validate its grammar, and return the sibling ids it names.
fn parse_expr_refs(expr: &str) -> anyhow::Result<HashSet<String>> {
    let tokens = tokenize(expr)?;
    if tokens.is_empty() {
        bail!("empty expression");
    }
    let mut parser = Parser {
        tokens: &tokens,
        pos: 0,
        refs: HashSet::new(),
    };
    parser.parse_expr()?;
    if parser.pos != tokens.len() {
        bail!("trailing tokens after a complete expression");
    }
    Ok(parser.refs)
}

// --- cycle detection over the sibling reference graph ---

#[derive(Clone, Copy, PartialEq, Eq)]
enum Color {
    White,
    Gray,
    Black,
}

fn detect_cycle(
    view: &View,
    panel: &Panel,
    deps: &HashMap<String, Vec<String>>,
) -> anyhow::Result<()> {
    let mut color: HashMap<&str, Color> =
        deps.keys().map(|k| (k.as_str(), Color::White)).collect();
    let mut stack: Vec<&str> = Vec::new();
    let roots: Vec<&str> = deps.keys().map(String::as_str).collect();
    for node in roots {
        if color.get(node) == Some(&Color::White)
            && let Some(cycle) = find_cycle(node, deps, &mut color, &mut stack)
        {
            bail!(
                "{}: expr cycle among series [{}]",
                loc(view, panel),
                cycle.join(" -> ")
            );
        }
    }
    Ok(())
}

/// DFS with white/gray/black coloring; returns the offending path on a
/// back-edge into a node still on the stack.
fn find_cycle<'a>(
    node: &'a str,
    deps: &'a HashMap<String, Vec<String>>,
    color: &mut HashMap<&'a str, Color>,
    stack: &mut Vec<&'a str>,
) -> Option<Vec<String>> {
    color.insert(node, Color::Gray);
    stack.push(node);
    if let Some(children) = deps.get(node) {
        for child in children {
            match color.get(child.as_str()).copied().unwrap_or(Color::White) {
                Color::Gray => {
                    let mut cycle: Vec<String> = stack
                        .iter()
                        .copied()
                        .skip_while(|n| *n != child.as_str())
                        .map(str::to_owned)
                        .collect();
                    cycle.push(child.clone());
                    return Some(cycle);
                }
                Color::White => {
                    if let Some(cycle) = find_cycle(child.as_str(), deps, color, stack) {
                        return Some(cycle);
                    }
                }
                Color::Black => {}
            }
        }
    }
    stack.pop();
    color.insert(node, Color::Black);
    None
}

// --- reload-aware holder ---

/// The live dashboard set behind an [`ArcSwap`], with the file it reloads from.
///
/// Request handlers take a snapshot with [`current`](Self::current) and finish
/// on it; a file-change reload reparses and, only on success, swaps in the new
/// `Arc`.
pub struct DashboardStore {
    inner: ArcSwap<Dashboards>,
    path: PathBuf,
}

impl DashboardStore {
    /// Read, parse, and validate the file; fail-fast (startup must reject a bad
    /// file, unlike a reload).
    pub fn load(path: &Path) -> anyhow::Result<Arc<Self>> {
        let yaml = std::fs::read_to_string(path)
            .with_context(|| format!("reading dashboards file {}", path.display()))?;
        let dashboards = Self::parse_and_validate(&yaml)
            .with_context(|| format!("loading dashboards file {}", path.display()))?;
        Ok(Arc::new(Self {
            inner: ArcSwap::from_pointee(dashboards),
            path: path.to_owned(),
        }))
    }

    /// Snapshot the current set; in-flight requests finish on their snapshot.
    pub fn current(&self) -> Arc<Dashboards> {
        self.inner.load_full()
    }

    /// The pure seam: parse then validate, no filesystem, no swap.
    fn parse_and_validate(yaml: &str) -> anyhow::Result<Dashboards> {
        let dashboards: Dashboards =
            yaml_serde::from_str(yaml).context("parsing dashboards YAML")?;
        dashboards.validate()?;
        Ok(dashboards)
    }

    /// Parse+validate `yaml` and, **on success only**, swap in the new set. On
    /// failure the current set is untouched (the error is returned, not logged).
    pub fn apply_reload(&self, yaml: &str) -> anyhow::Result<()> {
        let dashboards = Self::parse_and_validate(yaml)?;
        self.inner.store(Arc::new(dashboards));
        Ok(())
    }

    /// Re-read the file and apply it. Validate-before-swap: on any error the old
    /// set stays live and we only log, so a bad edit never drops the service.
    pub fn reload(&self) {
        let yaml = match std::fs::read_to_string(&self.path) {
            Ok(yaml) => yaml,
            Err(e) => {
                error!(
                    error = %e,
                    path = %self.path.display(),
                    "reading dashboards for reload failed; keeping the previous set"
                );
                return;
            }
        };
        match self.apply_reload(&yaml) {
            Ok(()) => info!(views = self.current().views.len(), "dashboards reloaded"),
            Err(e) => error!(
                error = %e,
                path = %self.path.display(),
                "reloading dashboards failed; keeping the previous set"
            ),
        }
    }
}

/// Spawn a detached task that reloads the dashboards file in place whenever its mtime
/// changes. Replaces the old SIGHUP trigger: a plain editor save is picked up within the
/// poll interval, with reload()'s log-and-keep-previous semantics. Kept off the shutdown
/// path, like the reload task it replaces.
pub fn spawn_reload_watch(store: Arc<DashboardStore>) {
    tokio::spawn(async move {
        let mut last = mtime(&store.path);
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        loop {
            ticker.tick().await; // first tick is immediate; cur == last so no spurious reload
            let cur = mtime(&store.path);
            if cur != last {
                last = cur;
                store.reload();
            }
        }
    });
}

/// Last-modified time of `path`, or None if it can't be stat'd (missing/unreadable). A
/// None -> Some transition (file recreated) counts as a change and drives a reload.
fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = r##"
views:
  - id: overview
    title: Overview
    range: now-6M
    refresh: 10s
    panels:
      - title: Heating
        height: 10
        axes:
          - {id: energy, side: left, unit: kWh}
          - {id: ratio, side: right, min: 0, max: 6}
        series:
          - {id: heat,  label: Heat,  uuid: "deadbeef-0000-0000-0000-000000000001", sub: total, agg: delta, type: bar, axis: energy}
          - {id: power, label: Power, uuid: "deadbeef-0000-0000-0000-000000000002", sub: total, agg: delta, type: bar, axis: energy}
          - {label: Temperature, uuid: "deadbeef-0000-0000-0000-000000000003", agg: avg, type: line, axis: ratio, color: "#4a7fb5"}
          - {label: COP, expr: "heat / power", type: line, axis: ratio, interp: smooth}
      - title: Live
        series:
          - {id: prod, label: Production,  uuid: "deadbeef-0000-0000-0000-000000000004", sub: prodCurrDay, agg: max, type: bar, stack: gen}
          - {id: cons, label: Consumption, uuid: "deadbeef-0000-0000-0000-000000000005", sub: consCurrDay, agg: max, type: bar, stack: use, scale: -1}
          - {label: Net, expr: "prod + cons", type: line, interp: smooth}
  - id: yearly
    title: Yearly
    range: now-3y
    bucket: month
    panels:
      - title: Monthly draw
        series:
          - {label: Draw, uuid: "deadbeef-0000-0000-0000-000000000006", agg: delta, type: bar}
"##;

    #[test]
    fn parses_and_validates_a_multi_view_config() {
        let dbs = DashboardStore::parse_and_validate(GOOD).unwrap();
        assert_eq!(dbs.views.len(), 2);

        let overview = &dbs.views[0];
        assert_eq!(overview.id, "overview");
        assert_eq!(overview.range.as_deref(), Some("now-6M"));
        assert_eq!(overview.refresh.as_deref(), Some("10s"));
        assert_eq!(overview.bucket, None);

        let heating = &overview.panels[0];
        assert_eq!(heating.height, Some(10));
        assert_eq!(heating.axes.len(), 2);
        let ratio = &heating.axes[1];
        assert_eq!(ratio.id, "ratio");
        assert_eq!(ratio.side, Some(Side::Right));
        assert_eq!(ratio.min, Some(0.0));
        assert_eq!(ratio.max, Some(6.0));

        // A bound series.
        let heat = &heating.series[0];
        assert_eq!(heat.kind, SeriesType::Bar);
        assert_eq!(heat.agg, Some(Agg::Delta));
        assert_eq!(heat.axis.as_deref(), Some("energy"));
        assert_eq!(heat.resolved_sub(), "total");
        assert_eq!(heat.scale, 1.0);
        assert_eq!(heat.interp, Interp::Linear);

        // sub omitted -> DEFAULT_SUB; agg deferred as None.
        let temp = &heating.series[2];
        assert_eq!(temp.resolved_sub(), "value");
        assert_eq!(temp.color.as_deref(), Some("#4a7fb5"));

        // A derived series referencing two siblings.
        let cop = &heating.series[3];
        assert_eq!(cop.expr.as_deref(), Some("heat / power"));
        assert_eq!(cop.kind, SeriesType::Line);
        assert_eq!(cop.interp, Interp::Smooth);
        assert_eq!(cop.uuid, None);

        // A mirrored (scale -1) member of a stack group.
        let cons = &dbs.views[0].panels[1].series[1];
        assert_eq!(cons.scale, -1.0);
        assert_eq!(cons.stack.as_deref(), Some("use"));

        // The derived Net references two siblings by id.
        let net = &dbs.views[0].panels[1].series[2];
        assert_eq!(net.expr.as_deref(), Some("prod + cons"));

        // Calendar-month bucketing on the yearly view.
        assert_eq!(dbs.views[1].bucket, Some(Bucket::Month));
    }

    #[test]
    fn rejects_a_dangling_axis_reference() {
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        axes:
          - {id: left}
        series:
          - {label: A, uuid: "deadbeef-0000-0000-0000-000000000001", axis: right}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("axis 'right'"), "message was: {msg}");
        assert!(msg.contains("not declared"), "message was: {msg}");
    }

    #[test]
    fn rejects_an_unknown_expr_symbol() {
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {id: a, uuid: "deadbeef-0000-0000-0000-000000000001"}
          - {label: D, expr: "a + ghost"}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("unknown series 'ghost'"), "message was: {msg}");
    }

    #[test]
    fn rejects_an_expr_cycle() {
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {id: a, expr: "b + 1"}
          - {id: b, expr: "a + 1"}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("cycle"), "message was: {msg}");
    }

    #[test]
    fn rejects_a_derived_series_carrying_gapfill() {
        // gapfill applies to a bound uuid query; a derived series is computed
        // client-side and must not carry it (same rule as sub/agg).
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {id: a, uuid: "deadbeef-0000-0000-0000-000000000001"}
          - {label: D, expr: "a + 1", gapfill: locf}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("must not carry sub/agg/gapfill"), "message was: {msg}");
    }

    #[test]
    fn rejects_a_series_with_both_uuid_and_expr() {
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {id: a, uuid: "deadbeef-0000-0000-0000-000000000001", expr: "1 + 1"}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("both uuid and expr"), "message was: {msg}");
    }

    #[test]
    fn rejects_a_series_with_neither_uuid_nor_expr() {
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {label: nothing}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("neither uuid nor expr"), "message was: {msg}");
    }

    #[test]
    fn rejects_a_malformed_expr_grammar() {
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {id: a, uuid: "deadbeef-0000-0000-0000-000000000001"}
          - {label: D, expr: "a + + "}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("invalid expr"), "message was: {msg}");
    }

    #[test]
    fn bad_reload_retains_the_prior_arc() {
        let good = DashboardStore::parse_and_validate(GOOD).unwrap();
        let store = DashboardStore {
            inner: ArcSwap::from_pointee(good),
            path: PathBuf::from("/nonexistent"),
        };
        let before = store.current();

        // A parseable but semantically invalid set (dangling axis) must be
        // rejected by validation before any swap happens.
        let bad = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        axes:
          - {id: left}
        series:
          - {label: A, uuid: "deadbeef-0000-0000-0000-000000000001", axis: right}
"#;
        assert!(store.apply_reload(bad).is_err());
        let after = store.current();
        assert!(Arc::ptr_eq(&before, &after), "the old Arc must stay live");
        assert_eq!(*before, *after);
    }

    #[test]
    fn good_reload_swaps_in_the_new_set() {
        let good = DashboardStore::parse_and_validate(GOOD).unwrap();
        let store = DashboardStore {
            inner: ArcSwap::from_pointee(good),
            path: PathBuf::from("/nonexistent"),
        };
        let before = store.current();
        assert_eq!(before.views.len(), 2);

        let smaller = r#"
views:
  - id: only
    title: Only
    panels:
      - title: P
        series:
          - {label: A, uuid: "deadbeef-0000-0000-0000-000000000001"}
"#;
        store.apply_reload(smaller).unwrap();
        let after = store.current();
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(after.views.len(), 1);
        assert_eq!(after.views[0].id, "only");
    }

    /// The shipped example must round-trip through the very loader an operator
    /// hits, so the sample stays runnable and documents the schema by example.
    #[test]
    fn example_dashboards_file_round_trips() {
        let yaml = include_str!("../dashboards.example.yaml");
        let dbs = DashboardStore::parse_and_validate(yaml)
            .expect("dashboards.example.yaml must parse and validate");
        assert_eq!(dbs.views.len(), 2);

        // A three-axis panel carrying a derived series.
        let climate = &dbs.views[0].panels[0];
        assert_eq!(climate.axes.len(), 3);
        assert!(climate.series.iter().any(|s| s.expr.is_some()));

        // Outdoor temp opts into interpolate gapfill; the others take the default.
        assert_eq!(climate.series[1].gapfill, Some(Gapfill::Interpolate));
        assert_eq!(climate.series[0].gapfill, None);

        // The second view buckets real calendar months.
        assert_eq!(dbs.views[1].bucket, Some(Bucket::Month));
    }
}
