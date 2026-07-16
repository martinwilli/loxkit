//! The dashboard view model, its validation, and the reload-aware holder.
//!
//! A dashboard is a list of *views*; a view is a list of *panels*; a panel is
//! `axes[] + series[]`. Every series is either **bound** (a `uuid` plus an
//! optional `agg`, read straight from the store) or **derived** (an
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
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context as _, bail};
use arc_swap::ArcSwap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing::{error, info};

/// The whole dashboard set — the parsed contents of `dashboards.yaml`.
///
/// Also `Serialize`: the HTTP layer hands the current snapshot straight to the
/// frontend as JSON. The `skip_serializing_if` on the optional fields keeps that
/// JSON as terse as the YAML — an absent key stays absent, never `null`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dashboards {
    /// The views, each a tab of panels.
    pub views: Vec<View>,
}

/// A view: one named tab of panels sharing a default time window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct View {
    /// Stable id (used in URLs and API paths).
    pub id: String,
    /// Human title.
    pub title: String,
    /// Default time window, e.g. `now-6M`. Absent means the app default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
    /// Bucketing policy for this view: the scalar shorthand `day`/`month` (a
    /// fixed calendar unit), or a zoom-driven [`BucketSpec::Schedule`] that picks
    /// the bucket width from the current window span. Absent means the engine
    /// tier-routes by span. Inherited by panels that omit their own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<BucketSpec>,
    /// The panels in this view.
    pub panels: Vec<Panel>,
}

/// A panel: one chart, `axes[] + series[]`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Panel {
    /// Panel title.
    pub title: String,
    /// Relative height hint (grid rows); layout is otherwise automatic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    /// Bucketing policy for this panel (scalar `day`/`month` or a
    /// [`BucketSpec::Schedule`]), overriding the view's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<BucketSpec>,
    /// Declared y-axes; a series may target one by `id`. Empty means a single
    /// implicit axis.
    #[serde(default)]
    pub axes: Vec<Axis>,
    /// The series drawn in this panel.
    pub series: Vec<Series>,
}

/// A declared y-axis a series can be pinned to.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Let the axis hug the data range instead of forcing the zero baseline in.
    /// A value axis otherwise always includes zero (the ECharts default), so a
    /// band of readings far from it (room temperatures, say) is squashed into a
    /// sliver at the top. An explicit `min`/`max` still pins that bound.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub fit: bool,
}

/// A single series: either bound to a store uuid or derived from siblings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Computed for `expr` references but never drawn — no legend entry, no
    /// plotted line/bars. For a bound input a derived sibling needs that has no
    /// place on the panel itself (a raw counter feeding a ratio, say). Requires
    /// an `id`, since a hidden series is pointless unless something references it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hidden: bool,
    /// Stack group; series sharing a group stack together.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
    /// How this series accumulates within its stack group. Absent is `samesign`
    /// (a mirrored `scale: -1` member stacks below zero); set `all` to stack
    /// across signs, e.g. for a min-max band whose base goes negative, or
    /// `negative` to pin the member below zero regardless of its per-bucket sign.
    #[serde(default)]
    pub stack_strategy: StackStrategy,
    /// Multiplier applied to values, e.g. `-1` to mirror below zero or `0.001`
    /// to rescale units.
    #[serde(default = "default_scale")]
    pub scale: f64,

    // --- binding: exactly one of {uuid (+agg)} or {expr} ---
    /// Bound series: the store uuid to read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    /// Bound series: the aggregation. Absent is resolved later from
    /// `series_meta.agg_mode` (commit 3), so no default is imposed here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agg: Option<Agg>,
    /// Bound series: how an empty bucket is filled on the shared query grid —
    /// carry the last value (`locf`), linearly `interpolate`, or leave it empty
    /// (`none`, read as 0). Absent resolves a default from the chart type and
    /// aggregation (a gauge line carries, a bar or a delta/integral accumulation
    /// reads 0); set this to override, e.g. `interpolate` for a smooth signal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gapfill: Option<Gapfill>,
    /// Derived series: an arithmetic expression over sibling series ids.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expr: Option<String>,
}

/// A fixed calendar bucket, the scalar shorthand of a [`BucketSpec`]: `day` and
/// `month` are the two calendar exceptions the engine's span router skips, so a
/// daily-reset counter charted with `max` reads as one whole-day (or -month)
/// total instead of fragmenting across span-routed sub-day buckets. Each is
/// equivalent to a one-rule always-on schedule (`{1, Day}` / `{1, Month}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Bucket {
    /// Real calendar days in local time.
    Day,
    /// Real calendar months in local time.
    Month,
}

/// A time unit for a [`BucketInterval`]. The letters match the `range` grammar:
/// `s`=second, `m`=minute, `h`=hour, `d`=day, `w`=week, `M`=month, `y`=year —
/// case-sensitive, so lowercase `m` is minutes and uppercase `M` is months.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl TimeUnit {
    /// The compact grammar letter for this unit.
    fn letter(self) -> char {
        match self {
            TimeUnit::Second => 's',
            TimeUnit::Minute => 'm',
            TimeUnit::Hour => 'h',
            TimeUnit::Day => 'd',
            TimeUnit::Week => 'w',
            TimeUnit::Month => 'M',
            TimeUnit::Year => 'y',
        }
    }

    /// The singular Postgres `interval` unit name (pluralized with a trailing
    /// `s` for a count above one).
    fn pg_name(self) -> &'static str {
        match self {
            TimeUnit::Second => "second",
            TimeUnit::Minute => "minute",
            TimeUnit::Hour => "hour",
            TimeUnit::Day => "day",
            TimeUnit::Week => "week",
            TimeUnit::Month => "month",
            TimeUnit::Year => "year",
        }
    }

    /// An approximate second count, for span-threshold comparison only (a month
    /// is 30 days, a year 365 — fuzzy is fine; it only orders schedule rules).
    pub fn approx_secs(self) -> u64 {
        match self {
            TimeUnit::Second => 1,
            TimeUnit::Minute => 60,
            TimeUnit::Hour => 3_600,
            TimeUnit::Day => 86_400,
            TimeUnit::Week => 604_800,
            TimeUnit::Month => 2_592_000,
            TimeUnit::Year => 31_536_000,
        }
    }

    /// Whether this unit gets tz-aware calendar bucketing (`day`/`week`/`month`/
    /// `year` land on local boundaries); the sub-day units are clock buckets
    /// where the zone is irrelevant.
    pub fn is_calendar(self) -> bool {
        matches!(
            self,
            TimeUnit::Day | TimeUnit::Week | TimeUnit::Month | TimeUnit::Year
        )
    }
}

/// A time granularity: a count and a unit, parsed from a compact string like
/// `10h`, `1m`, `1M`, `5y`. Serializes back to that compact form; [`to_pg_interval`]
/// renders the Postgres `interval` literal for SQL.
///
/// [`to_pg_interval`]: BucketInterval::to_pg_interval
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BucketInterval {
    pub count: u32,
    pub unit: TimeUnit,
}

impl BucketInterval {
    /// The Postgres `interval` literal, e.g. `1 day`, `1 month`, `5 minutes`. A
    /// count of one is singular, above one appends an `s`.
    pub fn to_pg_interval(&self) -> String {
        let name = self.unit.pg_name();
        if self.count == 1 {
            format!("1 {name}")
        } else {
            format!("{} {name}s", self.count)
        }
    }

    /// The approximate span in seconds (`count * unit.approx_secs()`), for
    /// ordering and matching schedule rules.
    pub fn span_secs(&self) -> u64 {
        u64::from(self.count) * self.unit.approx_secs()
    }
}

/// The compact `<count><letter>` form (the serialized/YAML form); the inverse of
/// [`FromStr`]. Distinct from [`to_pg_interval`](BucketInterval::to_pg_interval).
impl fmt::Display for BucketInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.count, self.unit.letter())
    }
}

impl FromStr for BucketInterval {
    type Err = anyhow::Error;

    /// Parse `<digits><unit-letter>`: reject an empty count, a zero count, an
    /// unknown or missing unit, and any trailing junk.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
        let (digits, unit_str) = s.split_at(split);
        if digits.is_empty() {
            bail!("bucket interval '{s}' has no leading count");
        }
        let count: u32 = digits
            .parse()
            .with_context(|| format!("bucket interval '{s}' has an out-of-range count"))?;
        if count == 0 {
            bail!("bucket interval '{s}' must have a positive count");
        }
        let unit = match unit_str {
            "s" => TimeUnit::Second,
            "m" => TimeUnit::Minute,
            "h" => TimeUnit::Hour,
            "d" => TimeUnit::Day,
            "w" => TimeUnit::Week,
            "M" => TimeUnit::Month,
            "y" => TimeUnit::Year,
            "" => bail!("bucket interval '{s}' has no unit letter (want s/m/h/d/w/M/y)"),
            other => {
                bail!("bucket interval '{s}' has an unknown unit '{other}' (want s/m/h/d/w/M/y)")
            }
        };
        Ok(BucketInterval { count, unit })
    }
}

/// Serialize as the compact `<count><letter>` string, so the JSON/YAML form
/// round-trips through [`FromStr`].
impl Serialize for BucketInterval {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for BucketInterval {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// One rule in a bucket schedule: for windows whose span is strictly less than
/// `upto`, bucket at `every`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketRule {
    pub upto: BucketInterval,
    pub every: BucketInterval,
}

/// A view/panel bucketing policy: a fixed calendar unit (the scalar `day`/`month`
/// shorthand) or a zoom-driven schedule. Untagged, so both `bucket: day` and
/// `bucket: [ {upto, every}, … ]` parse.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BucketSpec {
    /// A fixed calendar unit; equivalent to a one-rule always-on schedule.
    Fixed(Bucket),
    /// A schedule of rules, matched by window span (first `span < upto` wins,
    /// else the last rule's `every` clamps the coarsest end).
    Schedule(Vec<BucketRule>),
}

impl BucketSpec {
    /// The bucket width to use for a window of `span_secs`. A fixed unit ignores
    /// the span; a schedule returns the first rule whose `upto` the span is below,
    /// or the coarsest (last) rule's `every` when the span outgrows them all.
    pub fn resolve(&self, span_secs: u64) -> BucketInterval {
        match self {
            BucketSpec::Fixed(Bucket::Day) => BucketInterval {
                count: 1,
                unit: TimeUnit::Day,
            },
            BucketSpec::Fixed(Bucket::Month) => BucketInterval {
                count: 1,
                unit: TimeUnit::Month,
            },
            BucketSpec::Schedule(rules) => rules
                .iter()
                .find(|r| span_secs < r.upto.span_secs())
                .or_else(|| rules.last())
                .map_or(
                    // Unreachable in practice: validate() rejects an empty
                    // schedule before it can be resolved.
                    BucketInterval {
                        count: 1,
                        unit: TimeUnit::Day,
                    },
                    |r| r.every,
                ),
        }
    }
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

/// How a series accumulates within its [`Series::stack`] group. Maps straight to
/// the charting library's stack strategy of the same names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StackStrategy {
    /// Positive and negative values accumulate on their own side of zero (the
    /// default), so a `scale: -1` member lands below zero within the group.
    #[default]
    Samesign,
    /// Accumulate across signs. Needed for a min-max band, whose base (the daily
    /// `min`) can dip below zero: the `max - min` height must stack onto that
    /// negative base to reach `max`, which `samesign` refuses.
    All,
    /// Only ever accumulate onto the negative side of zero, whatever the value's
    /// own sign in a given bucket. `samesign` picks the side per bucket from that
    /// sign, so a derived series flips to the wrong side whenever measurement
    /// noise nudges it across zero; pinning it `negative` keeps a series that must
    /// live below zero (e.g. a mirrored import derived from counter deltas) there.
    Negative,
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
/// index; this picks what fills the empties. Absent on a series resolves a
/// default from the chart type and aggregation (see [`Series::gapfill`]);
/// `locf`/`interpolate`/`none` set here are explicit overrides.
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
    /// No fill: an empty bucket stays empty and reads as 0. Right for an
    /// accumulation, or an on-change meter that goes quiet at zero — the resolved
    /// default for a bar and for delta/integral.
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
            if let Some(spec) = &view.bucket {
                validate_bucket(spec)
                    .with_context(|| format!("view '{}' bucket", view.id))?;
            }
            for panel in &view.panels {
                if let Some(spec) = &panel.bucket {
                    validate_bucket(spec).with_context(|| {
                        format!("view '{}' / panel '{}' bucket", view.id, panel.title)
                    })?;
                }
                validate_panel(view, panel)?;
            }
        }
        Ok(())
    }
}

/// Validate a bucket schedule: at least one rule, `upto` thresholds strictly
/// increasing (by approximate span), and positive counts throughout. A fixed
/// scalar bucket carries nothing to check.
fn validate_bucket(spec: &BucketSpec) -> anyhow::Result<()> {
    let BucketSpec::Schedule(rules) = spec else {
        return Ok(());
    };
    if rules.is_empty() {
        bail!("schedule is empty; give at least one {{upto, every}} rule");
    }
    let mut prev: Option<u64> = None;
    for rule in rules {
        // FromStr already rejects a zero count, but a programmatically built spec
        // (or a future non-string deserializer) might not, so assert it here.
        if rule.upto.count == 0 || rule.every.count == 0 {
            bail!("schedule rule has a zero count");
        }
        let upto = rule.upto.span_secs();
        if let Some(p) = prev
            && upto <= p
        {
            bail!(
                "schedule 'upto' thresholds must strictly increase; '{}' does not exceed the prior rule",
                rule.upto
            );
        }
        prev = Some(upto);
    }
    Ok(())
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
                if series.agg.is_some() || series.gapfill.is_some() {
                    bail!(
                        "{}: derived {sref} must not carry agg/gapfill (those apply only to a bound uuid series)",
                        loc(view, panel)
                    );
                }
            }
            (true, false) => {}
        }

        if series.hidden && series.id.is_none() {
            bail!(
                "{}: {sref} is hidden but has no id; a hidden series is only useful if a sibling expr references it",
                loc(view, panel)
            );
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
    panels:
      - title: Heating
        height: 10
        axes:
          - {id: energy, side: left, unit: kWh}
          - {id: ratio, side: right, min: 0, max: 6}
        series:
          - {id: heat,  label: Heat,  uuid: "deadbeef-0000-0000-0000-000000000001", agg: delta, type: bar, axis: energy}
          - {id: power, label: Power, uuid: "deadbeef-0000-0000-0000-000000000002", agg: delta, type: bar, axis: energy}
          - {label: Temperature, uuid: "deadbeef-0000-0000-0000-000000000003", agg: avg, type: line, axis: ratio, color: "#4a7fb5"}
          - {label: COP, expr: "heat / power", type: line, axis: ratio, interp: smooth}
      - title: Live
        series:
          - {id: prod, label: Production,  uuid: "deadbeef-0000-0000-0000-000000000004", agg: max, type: bar, stack: gen, stack_strategy: all}
          - {id: cons, label: Consumption, uuid: "deadbeef-0000-0000-0000-000000000005", agg: max, type: bar, stack: use, scale: -1, stack_strategy: negative}
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
        assert_eq!(heat.scale, 1.0);
        assert_eq!(heat.interp, Interp::Linear);
        assert_eq!(heat.stack_strategy, StackStrategy::Samesign);

        // agg deferred as None.
        let temp = &heating.series[2];
        assert_eq!(temp.color.as_deref(), Some("#4a7fb5"));

        // A derived series referencing two siblings.
        let cop = &heating.series[3];
        assert_eq!(cop.expr.as_deref(), Some("heat / power"));
        assert_eq!(cop.kind, SeriesType::Line);
        assert_eq!(cop.interp, Interp::Smooth);
        assert_eq!(cop.uuid, None);

        // A mirrored (scale -1) member of a stack group, pinned below zero.
        let cons = &dbs.views[0].panels[1].series[1];
        assert_eq!(cons.scale, -1.0);
        assert_eq!(cons.stack.as_deref(), Some("use"));
        assert_eq!(cons.stack_strategy, StackStrategy::Negative);

        // A stack member that opts into across-sign accumulation.
        let prod = &dbs.views[0].panels[1].series[0];
        assert_eq!(prod.stack_strategy, StackStrategy::All);

        // The derived Net references two siblings by id.
        let net = &dbs.views[0].panels[1].series[2];
        assert_eq!(net.expr.as_deref(), Some("prod + cons"));

        // Calendar-month bucketing on the yearly view.
        assert_eq!(dbs.views[1].bucket, Some(BucketSpec::Fixed(Bucket::Month)));
    }

    #[test]
    fn parses_a_calendar_day_bucket() {
        // The second calendar exception: a view (or panel) can pin whole days,
        // so a daily-reset counter charted with max reads as one bar per day.
        let yaml = r#"
views:
  - id: daily
    title: Daily
    bucket: day
    panels:
      - title: P
        series:
          - {label: Draw, uuid: "deadbeef-0000-0000-0000-000000000001", agg: max, type: bar}
"#;
        let dbs = DashboardStore::parse_and_validate(yaml).unwrap();
        assert_eq!(dbs.views[0].bucket, Some(BucketSpec::Fixed(Bucket::Day)));
    }

    #[test]
    fn bucket_interval_parses_and_renders() {
        let bi = |s: &str| s.parse::<BucketInterval>().unwrap();
        assert_eq!(
            bi("10h"),
            BucketInterval {
                count: 10,
                unit: TimeUnit::Hour
            }
        );
        assert_eq!(
            bi("1m"),
            BucketInterval {
                count: 1,
                unit: TimeUnit::Minute
            }
        );
        assert_eq!(
            bi("1M"),
            BucketInterval {
                count: 1,
                unit: TimeUnit::Month
            }
        );
        assert_eq!(
            bi("5y"),
            BucketInterval {
                count: 5,
                unit: TimeUnit::Year
            }
        );
        assert_eq!(
            bi("30m"),
            BucketInterval {
                count: 30,
                unit: TimeUnit::Minute
            }
        );

        // to_pg_interval: singular at 1, pluralized above.
        assert_eq!(bi("1d").to_pg_interval(), "1 day");
        assert_eq!(bi("1M").to_pg_interval(), "1 month");
        assert_eq!(bi("5m").to_pg_interval(), "5 minutes");
        assert_eq!(bi("30m").to_pg_interval(), "30 minutes");
        assert_eq!(bi("1h").to_pg_interval(), "1 hour");

        // Display is the compact inverse of FromStr (m vs M preserved).
        assert_eq!(bi("30m").to_string(), "30m");
        assert_eq!(bi("1M").to_string(), "1M");

        // Rejects: empty, no count, zero, unknown unit, a float.
        assert!("".parse::<BucketInterval>().is_err());
        assert!("h".parse::<BucketInterval>().is_err());
        assert!("0d".parse::<BucketInterval>().is_err());
        assert!("10x".parse::<BucketInterval>().is_err());
        assert!("1.5h".parse::<BucketInterval>().is_err());
    }

    /// The 4-rule target schedule, reused by the parse and resolve tests.
    const SCHED: &str = r#"
views:
  - id: zoom
    title: Zoom
    bucket:
      - {upto: 10h, every: 1m}
      - {upto: 10d, every: 1h}
      - {upto: 10M, every: 1d}
      - {upto: 5y,  every: 1M}
    panels:
      - title: P
        series:
          - {label: Draw, uuid: "deadbeef-0000-0000-0000-000000000001", agg: avg}
"#;

    #[test]
    fn parses_a_bucket_schedule() {
        let dbs = DashboardStore::parse_and_validate(SCHED).unwrap();
        let Some(BucketSpec::Schedule(rules)) = &dbs.views[0].bucket else {
            panic!("expected a schedule, got {:?}", dbs.views[0].bucket);
        };
        assert_eq!(rules.len(), 4);
        assert_eq!(rules[0].upto.unit, TimeUnit::Hour);
        assert_eq!(rules[0].every.unit, TimeUnit::Minute);
        assert_eq!(rules[2].upto.unit, TimeUnit::Month);
        assert_eq!(rules[2].every.unit, TimeUnit::Day);
        assert_eq!(rules[3].every.unit, TimeUnit::Month);
    }

    #[test]
    fn resolves_a_schedule_by_span() {
        let dbs = DashboardStore::parse_and_validate(SCHED).unwrap();
        let spec = dbs.views[0].bucket.as_ref().unwrap();
        let d = 86_400;
        let minute = BucketInterval {
            count: 1,
            unit: TimeUnit::Minute,
        };
        let hour = BucketInterval {
            count: 1,
            unit: TimeUnit::Hour,
        };
        let day = BucketInterval {
            count: 1,
            unit: TimeUnit::Day,
        };
        let month = BucketInterval {
            count: 1,
            unit: TimeUnit::Month,
        };
        assert_eq!(spec.resolve(5 * 3_600), minute); // 5h  -> 1m
        assert_eq!(spec.resolve(5 * d), hour); // 5d  -> 1h
        assert_eq!(spec.resolve(150 * d), day); // ~5M -> 1d
        assert_eq!(spec.resolve(3 * 365 * d), month); // 3y  -> 1M
        assert_eq!(spec.resolve(10 * 365 * d), month); // 10y -> clamp to 1M
    }

    #[test]
    fn resolves_the_scalar_shorthand() {
        assert_eq!(
            BucketSpec::Fixed(Bucket::Day).resolve(999),
            BucketInterval {
                count: 1,
                unit: TimeUnit::Day
            }
        );
        assert_eq!(
            BucketSpec::Fixed(Bucket::Month).resolve(999),
            BucketInterval {
                count: 1,
                unit: TimeUnit::Month
            }
        );
    }

    #[test]
    fn rejects_a_non_increasing_schedule() {
        let yaml = r#"
views:
  - id: v
    title: V
    bucket:
      - {upto: 10d, every: 1h}
      - {upto: 10h, every: 1m}
    panels:
      - title: P
        series:
          - {label: A, uuid: "deadbeef-0000-0000-0000-000000000001"}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("strictly increase"), "message was: {msg}");
    }

    #[test]
    fn rejects_an_empty_schedule() {
        let yaml = r#"
views:
  - id: v
    title: V
    bucket: []
    panels:
      - title: P
        series:
          - {label: A, uuid: "deadbeef-0000-0000-0000-000000000001"}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("empty"), "message was: {msg}");
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
        // client-side and must not carry it (same rule as agg).
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
        assert!(msg.contains("must not carry agg/gapfill"), "message was: {msg}");
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
    fn rejects_an_unknown_series_key() {
        // A misspelled or obsolete key (here `colour` for `color`) must be a hard
        // parse error, not a silently-dropped no-op that misleads the author.
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {label: A, uuid: "deadbeef-0000-0000-0000-000000000001", colour: red}
"#;
        assert!(DashboardStore::parse_and_validate(yaml).is_err());
    }

    #[test]
    fn parses_a_hidden_input_series() {
        // A hidden bound counter feeds a derived sibling but is not drawn; the
        // derived series that references it is visible as usual.
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {id: a, uuid: "deadbeef-0000-0000-0000-000000000001", agg: delta, hidden: true}
          - {label: R, expr: "a / 2"}
"#;
        let dbs = DashboardStore::parse_and_validate(yaml).unwrap();
        assert!(dbs.views[0].panels[0].series[0].hidden);
        assert!(!dbs.views[0].panels[0].series[1].hidden);
    }

    #[test]
    fn parses_an_axis_fit_flag() {
        // fit:true opts an axis out of the zero baseline; absent it defaults
        // to false (the ECharts zero-anchored behavior).
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        axes:
          - {id: temp, side: left, unit: "°C", fit: true}
          - {id: pct, side: right, unit: "%"}
        series:
          - {uuid: "deadbeef-0000-0000-0000-000000000001", axis: temp}
"#;
        let dbs = DashboardStore::parse_and_validate(yaml).unwrap();
        assert!(dbs.views[0].panels[0].axes[0].fit);
        assert!(!dbs.views[0].panels[0].axes[1].fit);
    }

    #[test]
    fn rejects_a_hidden_series_without_id() {
        // Hidden and unreferenceable is a dead series; reject it so the mistake
        // surfaces at load rather than silently drawing nothing.
        let yaml = r#"
views:
  - id: v
    title: V
    panels:
      - title: P
        series:
          - {uuid: "deadbeef-0000-0000-0000-000000000001", hidden: true}
"#;
        let err = DashboardStore::parse_and_validate(yaml).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("hidden but has no id"), "message was: {msg}");
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
        assert_eq!(dbs.views[1].bucket, Some(BucketSpec::Fixed(Bucket::Month)));
    }
}
