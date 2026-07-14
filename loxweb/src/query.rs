//! The tiered SQL query layer: a resolved series request in, downsampled
//! points out.
//!
//! A dashboard series is always `{one uuid, a time range, an aggregation}`. The
//! store keeps that series at three resolutions — full-resolution raw
//! `readings`, and the `readings_1h` / `readings_1d` continuous aggregates — so
//! a multi-year query need not scan every raw point. This module routes a
//! request to the coarsest tier that still yields enough points, snaps the
//! bucket width to a nice ladder, and emits one parametrized `time_bucket`
//! query.
//!
//! Two seams matter:
//!
//! * [`build_sql`] is **pure and DB-free**: a concrete `(agg, span, bucket)`
//!   maps to one deterministic SQL string, so the whole router is unit-testable
//!   with no database. The uuid and the time bounds are the only bind
//!   parameters (`$1`/`$2`/`$3`); the tier, interval, value expression and time
//!   column are chosen from fixed enums/ladders and inlined, so they are never
//!   user input.
//! * [`query_series`] is the DB-touching path: it resolves an absent `agg` from
//!   `series_meta.agg_mode`, builds the SQL, runs it on loxweb's own read pool,
//!   and maps rows to [`Point`]s. Every series is gapfilled onto the same grid
//!   ([`build_sql`]), so a panel's stacked series align index-for-index.
//!
//! ## Tier routing
//!
//! Aiming for a roughly constant point count across any span, we compute
//! `ideal = span / TARGET_BUCKETS` and pick by band:
//!
//! * `ideal < 1 hour` → raw `readings`, bucket snapped up the sub-hour ladder.
//! * `1 hour ≤ ideal < 1 day` → `readings_1h`, snapped up the whole-hour ladder.
//! * `ideal ≥ 1 day` → `readings_1d`, snapped up the whole-day ladder (capped).
//!
//! Two aggregations override the tier: `delta` and `median` need per-point data
//! the caggs don't carry, so they force the raw tier regardless of span while
//! keeping the span-appropriate bucket width. `delta` is last-minus-first over
//! the bucket and the caggs store `last_val` but no `first_val` to compose it
//! from; a percentile can't be composed from bucket rollups at all.
//!
//! `bucket: month` is the one calendar exception: real calendar months in local
//! time via the 3-arg `time_bucket('1 month', <tcol>, current_setting('TimeZone'))`,
//! sourced from `readings_1h` (hour boundaries nest cleanly into local months;
//! `readings_1d`'s UTC-midnight days straddle them) — or from raw for the
//! raw-only aggregations. loxweb only *reads* the pinned zone; the daemon's sink
//! is what `ALTER DATABASE … SET TimeZone` once.

use std::time::SystemTime;

use anyhow::{Context as _, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;
use uuid::Uuid;

use loxtap::config::TimescaleConfig;

use crate::dashboard::{Agg, Bucket, Gapfill, SeriesType};

/// Target number of buckets across any span; the router snaps to whatever nice
/// ladder step comes closest to `span / TARGET_BUCKETS`, so a 1-hour and a
/// 3-year window both come back at roughly this many points.
const TARGET_BUCKETS: u64 = 1_000;

/// Seconds in an hour and a day: the two `ideal`-band thresholds.
const HOUR_SECS: u64 = 3_600;
const DAY_SECS: u64 = 86_400;

/// Sub-hour bucket ladder for the raw tier: `(width in seconds, SQL literal)`.
/// A 1-2-5-10-15-30 rung per decade, so a zoom crosses a rung every ~2x rather
/// than the old 5x jumps (`1s→5s`, `1m→5m`) that made the resolution feel static.
const SUBHOUR_LADDER: &[(u64, &str)] = &[
    (1, "1 second"),
    (2, "2 seconds"),
    (5, "5 seconds"),
    (10, "10 seconds"),
    (15, "15 seconds"),
    (30, "30 seconds"),
    (60, "1 minute"),
    (120, "2 minutes"),
    (300, "5 minutes"),
    (600, "10 minutes"),
    (900, "15 minutes"),
    (1_800, "30 minutes"),
];

/// Whole-hour bucket ladder for the `readings_1h` tier.
const HOUR_LADDER: &[(u64, &str)] = &[
    (3_600, "1 hour"),
    (2 * 3_600, "2 hours"),
    (3 * 3_600, "3 hours"),
    (4 * 3_600, "4 hours"),
    (6 * 3_600, "6 hours"),
    (8 * 3_600, "8 hours"),
    (12 * 3_600, "12 hours"),
];

/// Whole-day bucket ladder for the `readings_1d` tier (capped at the top step).
/// 3-day and 5-day rungs break up the old 2d→7d gap (a 3.5x jump).
const DAY_LADDER: &[(u64, &str)] = &[
    (86_400, "1 day"),
    (2 * 86_400, "2 days"),
    (3 * 86_400, "3 days"),
    (5 * 86_400, "5 days"),
    (7 * 86_400, "7 days"),
    (14 * 86_400, "14 days"),
    (30 * 86_400, "30 days"),
];

/// A resolved request for one series' downsampled history.
#[derive(Clone, Debug)]
pub struct SeriesRequest {
    /// The stable state uuid — the one thing that keys a series in the store.
    pub uuid: Uuid,
    /// The sub-channel, carried for provenance/logging only. `readings` keys on
    /// uuid alone (there is no `sub` column), so this never enters a WHERE
    /// clause; the uuid already pins the exact series.
    pub sub: String,
    /// The aggregation. `None` means "resolve from `series_meta.agg_mode`" at
    /// query time (gauge → avg, counter → delta).
    pub agg: Option<Agg>,
    /// How empty buckets fill on the shared grid. `None` defers to a default
    /// resolved from `kind` and the (resolved) `agg` (see [`default_gapfill`]):
    /// a bar or a `delta`/`integral` accumulation reads an empty bucket as 0,
    /// a gauge line carries the last level. An explicit `locf`/`interpolate`/
    /// `none` overrides that default.
    pub gapfill: Option<Gapfill>,
    /// The chart type, used only to resolve the gapfill default (a bar's empty
    /// buckets zero-fill, a line's carry forward). Never enters the SQL.
    pub kind: SeriesType,
    /// Inclusive start of the window.
    pub from: SystemTime,
    /// Exclusive end of the window.
    pub to: SystemTime,
    /// Calendar-month bucketing, or `None` to tier-route by span.
    pub bucket: Option<Bucket>,
}

/// One downsampled bucket: its start instant and aggregated value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Point {
    /// Bucket start (the `time_bucket` boundary).
    pub t: SystemTime,
    /// The aggregated value.
    pub v: f64,
}

/// The result of a series query: the dense points plus the resolved unit label
/// from `series_meta` (`None` when the uuid has no meta row or a NULL unit).
#[derive(Clone, Debug)]
pub struct SeriesResult {
    /// The store's unit label for this series, the display default.
    pub unit: Option<String>,
    /// The tier-routed, gapfilled points.
    pub points: Vec<Point>,
}

/// The source tier a query reads from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tier {
    /// Full-resolution `readings`, time column `ts`.
    Raw,
    /// The hourly continuous aggregate `readings_1h`, time column `bucket`.
    Hour,
    /// The daily continuous aggregate `readings_1d`, time column `bucket`.
    Day,
}

impl Tier {
    /// The relation this tier reads from.
    fn table(self) -> &'static str {
        match self {
            Tier::Raw => "readings",
            Tier::Hour => "readings_1h",
            Tier::Day => "readings_1d",
        }
    }

    /// The time column to bucket and range-filter on.
    fn tcol(self) -> &'static str {
        match self {
            Tier::Raw => "ts",
            Tier::Hour | Tier::Day => "bucket",
        }
    }
}

/// Whether an aggregation can only be computed from raw per-point data. The
/// caggs carry `(n, s, vmin, vmax, last_val, tw)`: `delta` is last-minus-first
/// but there is no `first_val` to pair with the stored `last_val`, and a
/// percentile can't be composed from bucket rollups at all — so `delta` and
/// `median` always read raw. (Adding `first_val` to the caggs would let `delta`
/// roll up as `last(last_val) - first(first_val)`; deferred.)
fn is_raw_only(agg: Agg) -> bool {
    matches!(agg, Agg::Delta | Agg::Median)
}

/// The first ladder step at least as wide as `ideal`, or the widest step when
/// `ideal` outgrows the ladder (the top is a cap).
fn snap(ladder: &[(u64, &'static str)], ideal: u64) -> &'static str {
    for &(secs, lit) in ladder {
        if secs >= ideal {
            return lit;
        }
    }
    ladder.last().expect("bucket ladders are non-empty").1
}

/// Route a request by span to a `(tier, bucket-width literal)`. `delta`/`median`
/// force the raw tier but keep the span-appropriate width.
fn route(agg: Agg, span_secs: u64) -> (Tier, &'static str) {
    let ideal = span_secs / TARGET_BUCKETS;
    let (natural_tier, interval) = if ideal < HOUR_SECS {
        (Tier::Raw, snap(SUBHOUR_LADDER, ideal))
    } else if ideal < DAY_SECS {
        (Tier::Hour, snap(HOUR_LADDER, ideal))
    } else {
        (Tier::Day, snap(DAY_LADDER, ideal))
    };
    let tier = if is_raw_only(agg) {
        Tier::Raw
    } else {
        natural_tier
    };
    (tier, interval)
}

/// The per-tier SQL value expression for an aggregation. The cagg branch never
/// sees `delta`/`median` because [`route`] forces those onto the raw tier.
fn value_expr(agg: Agg, tier: Tier) -> &'static str {
    match tier {
        Tier::Raw => match agg {
            Agg::Avg => "avg(value)",
            Agg::Max => "max(value)",
            Agg::Min => "min(value)",
            Agg::Last => "last(value, ts)",
            Agg::Median => "percentile_cont(0.5) WITHIN GROUP (ORDER BY value)",
            // Net change over the bucket: last value minus first. Deliberately
            // NOT delta(counter_agg(...)): a counter that dithers in its last
            // float ULP (or takes the odd sub-unit backward correction) trips
            // counter_agg's reset detection, which reads each dip as a wrap and
            // adds a full-counter jump -- turning a few-unit delta into millions.
            // last-first ignores intra-bucket wobble; it only misreads a genuine
            // mid-bucket reset, which a lifetime meter total effectively never
            // does. first()/last() per timescaledb_toolkit 1.23.0, verify live.
            Agg::Delta => "last(value, ts) - first(value, ts)",
            // toolkit: integral() finalizes a time_weight('LOCF') partial; its
            // default unit is value·seconds (leave unit scaling to the config's
            // scale). One-arg form on purpose. Signature per toolkit 1.23.0,
            // verify live.
            Agg::Integral => "integral(time_weight('LOCF', ts, value))",
        },
        Tier::Hour | Tier::Day => match agg {
            // Exact hierarchical mean: sum(s)/sum(n), never an average-of-averages.
            Agg::Avg => "sum(s) / NULLIF(sum(n), 0)",
            Agg::Max => "max(vmax)",
            Agg::Min => "min(vmin)",
            Agg::Last => "last(last_val, bucket)",
            // toolkit: rollup() composes the stored tw partials, integral()
            // finalizes; identical result to the raw path. Signature per
            // toolkit 1.23.0, verify live.
            Agg::Integral => "integral(rollup(tw))",
            Agg::Median | Agg::Delta => {
                unreachable!("delta/median force the raw tier in route()")
            }
        },
    }
}

/// Build the parametrized SQL for a request. **Pure and DB-free**: the returned
/// string is a deterministic function of `(agg, gapfill, span, bucket)`. The uuid
/// and the two time bounds bind as `$1`/`$2`/`$3` in that order; everything else
/// is chosen from the enums/ladders and safely inlined.
fn build_sql(
    agg: Agg,
    gapfill: Gapfill,
    from: SystemTime,
    to: SystemTime,
    bucket: Option<Bucket>,
) -> String {
    // A reversed range degenerates to a zero span (finest bucket, empty result).
    let span_secs = to.duration_since(from).map(|d| d.as_secs()).unwrap_or(0);

    // Every query gapfills onto a fixed grid spanning [$2, $3): a dashboard
    // panel stacks series index-for-index, so they must share the same bucket
    // set. Emitting only non-empty buckets (different per series) is what breaks
    // a stack, so gapfill densifies each series to the identical grid.
    let (tier, bucket_expr) = if bucket == Some(Bucket::Month) {
        // Calendar months: tz-aware time_bucket_gapfill, from readings_1h for the
        // plain aggs + integral, from raw for the raw-only aggs.
        let tier = if is_raw_only(agg) {
            Tier::Raw
        } else {
            Tier::Hour
        };
        let tcol = tier.tcol();
        // The `::timestamptz` casts pin the bind types: time_bucket_gapfill is
        // overloaded, so an un-cast $2/$3 leaves the param type unresolved (it
        // defaults to text and breaks the `tcol >= $2` comparison).
        let expr = format!(
            "time_bucket_gapfill('1 month', {tcol}, current_setting('TimeZone'), \
             $2::timestamptz, $3::timestamptz)"
        );
        (tier, expr)
    } else {
        let (tier, interval) = route(agg, span_secs);
        let tcol = tier.tcol();
        let expr =
            format!("time_bucket_gapfill('{interval}', {tcol}, $2::timestamptz, $3::timestamptz)");
        (tier, expr)
    };

    // Fill an empty bucket per the resolved mode: locf carries the last level,
    // interpolate ramps between points, none leaves it NULL (coalesced to 0 when
    // rows are mapped). locf/interpolate must be the top-level call, so any
    // residual NULL→0 coalesce happens in Rust, not SQL.
    let base = value_expr(agg, tier);
    let value = match gapfill {
        Gapfill::Locf => format!("locf({base})"),
        Gapfill::Interpolate => format!("interpolate({base})"),
        Gapfill::None => base.to_owned(),
    };
    let table = tier.table();
    let tcol = tier.tcol();
    format!(
        "SELECT {bucket_expr} AS bucket, {value} AS value \
         FROM {table} \
         WHERE uuid = $1 AND {tcol} >= $2 AND {tcol} < $3 \
         GROUP BY 1 ORDER BY 1"
    )
}

/// Assemble loxweb's read pool from its [`TimescaleConfig`]. Mirrors the sink's
/// programmatic `tokio_postgres::Config` build (NoTls on a trusted LAN); the pool
/// is lazy, so this does not connect. Distinct from the daemon's single writer.
pub fn build_pool(config: &TimescaleConfig) -> Result<Pool> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .host(&config.host)
        .port(config.port)
        .user(&config.user)
        .password(&config.password)
        .dbname(&config.dbname);
    pool_from_pg_config(pg_config)
}

/// Build a pool from an already-assembled `tokio_postgres::Config`.
fn pool_from_pg_config(pg_config: tokio_postgres::Config) -> Result<Pool> {
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
    Pool::builder(mgr).build().context("building the read pool")
}

/// One series' `series_meta`: the ingest-time aggregation class and the unit
/// label. Both are absent when the uuid has no meta row yet (an unresolved wire
/// uuid still gets its readings, just no labels).
struct Meta {
    agg_mode: Option<String>,
    unit: Option<String>,
}

/// Read a series' `series_meta` row (agg_mode and unit) in one lookup. A missing
/// row yields an all-`None` [`Meta`].
async fn fetch_meta(client: &deadpool_postgres::Client, uuid: Uuid) -> Result<Meta> {
    let row = client
        .query_opt("SELECT agg_mode, unit FROM series_meta WHERE uuid = $1", &[&uuid])
        .await
        .context("reading series_meta")?;
    Ok(match row {
        Some(r) => Meta {
            agg_mode: r.get(0),
            unit: r.get(1),
        },
        None => Meta {
            agg_mode: None,
            unit: None,
        },
    })
}

/// Resolve an absent `agg` from `series_meta.agg_mode`: counter → delta, gauge
/// (or an unresolved uuid) → avg. Matches the ingest classification the sink
/// persists.
fn agg_from_mode(mode: Option<&str>) -> Agg {
    match mode {
        Some("counter") => Agg::Delta,
        _ => Agg::Avg,
    }
}

/// The gapfill to use when a series names none explicitly. `delta`/`integral`
/// are accumulations: an empty bucket is a genuine zero (no flow, no counter
/// change), so they never carry. The gauge aggregations straddle two cases the
/// chart type tells apart — a bar is a quantity that reads 0 when absent, a line
/// is a level that holds between samples. So a line carries the last level
/// (`locf`); zoomed in past the on-change log cadence it stays a curve instead
/// of picket-fencing down to 0 on every empty bucket.
fn default_gapfill(agg: Agg, kind: SeriesType) -> Gapfill {
    match agg {
        Agg::Delta | Agg::Integral => Gapfill::None,
        _ => match kind {
            SeriesType::Bar => Gapfill::None,
            SeriesType::Line => Gapfill::Locf,
        },
    }
}

/// Resolve the aggregation (if absent) and the unit from `series_meta`, build the
/// tier-routed SQL, run it, and map rows to points. Gapfill emits one row per
/// bucket; an empty bucket's NULL coalesces to 0, so every series comes back on
/// the identical dense grid.
pub async fn query_series(pool: &Pool, req: &SeriesRequest) -> Result<SeriesResult> {
    let client = pool.get().await.context("getting a read connection")?;
    let meta = fetch_meta(&client, req.uuid).await?;
    let agg = req
        .agg
        .unwrap_or_else(|| agg_from_mode(meta.agg_mode.as_deref()));
    let gapfill = req
        .gapfill
        .unwrap_or_else(|| default_gapfill(agg, req.kind));
    let sql = build_sql(agg, gapfill, req.from, req.to, req.bucket);
    let rows = client
        .query(&sql, &[&req.uuid, &req.from, &req.to])
        .await
        .context("running the series query")?;
    let mut points = Vec::with_capacity(rows.len());
    for row in rows {
        let t: SystemTime = row.get(0);
        // Gapfill emits a row for every bucket in the window; an empty one is
        // NULL (a leading gap before the first sample, or an accumulation gap).
        // Coalesce to 0 so every series returns the identical dense grid and a
        // panel's stack aligns index-for-index.
        let v = row.get::<_, Option<f64>>(1).unwrap_or(0.0);
        points.push(Point { t, v });
    }
    Ok(SeriesResult {
        unit: meta.unit,
        points,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    /// A stable instant, so expected SQL is deterministic.
    fn t(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// SQL for a span of `span_secs`, span-routed (no calendar bucket). Passes
    /// `Gapfill::None` (the default) so these assert the bare per-tier aggregate;
    /// the `locf`/`interpolate` wrappers have their own tests below.
    fn sql_for_span(agg: Agg, span_secs: u64) -> String {
        build_sql(
            agg,
            Gapfill::None,
            t(1_600_000_000),
            t(1_600_000_000 + span_secs),
            None,
        )
    }

    // --- one query per agg class on the raw tier (a 1-hour span → 5s) ---

    #[test]
    fn avg_on_raw_is_a_plain_mean() {
        assert_eq!(
            sql_for_span(Agg::Avg, 3_600),
            "SELECT time_bucket_gapfill('5 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, avg(value) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn max_min_last_on_raw() {
        assert_eq!(
            sql_for_span(Agg::Max, 3_600),
            "SELECT time_bucket_gapfill('5 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, max(value) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
        assert_eq!(
            sql_for_span(Agg::Min, 3_600),
            "SELECT time_bucket_gapfill('5 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, min(value) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
        assert_eq!(
            sql_for_span(Agg::Last, 3_600),
            "SELECT time_bucket_gapfill('5 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, last(value, ts) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn integral_on_raw_finalizes_a_time_weight_partial() {
        assert_eq!(
            sql_for_span(Agg::Integral, 3_600),
            "SELECT time_bucket_gapfill('5 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, \
             integral(time_weight('LOCF', ts, value)) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    // --- tier boundaries: spans straddling the 1h and 1d ideal thresholds ---

    #[test]
    fn just_under_the_hour_threshold_stays_raw() {
        // ideal = 3599 < 3600 → raw; the sub-hour ladder tops out, so 30 minutes.
        let span = HOUR_SECS * TARGET_BUCKETS - TARGET_BUCKETS; // ideal = 3599
        assert_eq!(
            sql_for_span(Agg::Avg, span),
            "SELECT time_bucket_gapfill('30 minutes', ts, $2::timestamptz, $3::timestamptz) AS bucket, avg(value) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn at_the_hour_threshold_routes_to_readings_1h() {
        // ideal = 3600 → hour tier, snapped to 1 hour.
        let span = HOUR_SECS * TARGET_BUCKETS; // ideal = 3600
        assert_eq!(
            sql_for_span(Agg::Avg, span),
            "SELECT time_bucket_gapfill('1 hour', bucket, $2::timestamptz, $3::timestamptz) AS bucket, sum(s) / NULLIF(sum(n), 0) AS value \
             FROM readings_1h WHERE uuid = $1 AND bucket >= $2 AND bucket < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn mid_hour_band_snaps_up_the_hour_ladder() {
        // 60 days: ideal = 5184 → 2 hours on readings_1h.
        assert_eq!(
            sql_for_span(Agg::Max, 60 * DAY_SECS),
            "SELECT time_bucket_gapfill('2 hours', bucket, $2::timestamptz, $3::timestamptz) AS bucket, max(vmax) AS value \
             FROM readings_1h WHERE uuid = $1 AND bucket >= $2 AND bucket < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn just_under_the_day_threshold_stays_hourly() {
        // ideal = 86399 < 86400 → hour tier, capped at 12 hours.
        let span = DAY_SECS * TARGET_BUCKETS - TARGET_BUCKETS; // ideal = 86399
        assert_eq!(
            sql_for_span(Agg::Last, span),
            "SELECT time_bucket_gapfill('12 hours', bucket, $2::timestamptz, $3::timestamptz) AS bucket, last(last_val, bucket) AS value \
             FROM readings_1h WHERE uuid = $1 AND bucket >= $2 AND bucket < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn at_the_day_threshold_routes_to_readings_1d() {
        // ideal = 86400 → day tier, snapped to 1 day.
        let span = DAY_SECS * TARGET_BUCKETS; // ideal = 86400
        assert_eq!(
            sql_for_span(Agg::Avg, span),
            "SELECT time_bucket_gapfill('1 day', bucket, $2::timestamptz, $3::timestamptz) AS bucket, sum(s) / NULLIF(sum(n), 0) AS value \
             FROM readings_1d WHERE uuid = $1 AND bucket >= $2 AND bucket < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn integral_on_a_cagg_rolls_up_the_partial() {
        // 60 days → hour tier; integral composes the stored tw partials.
        assert_eq!(
            sql_for_span(Agg::Integral, 60 * DAY_SECS),
            "SELECT time_bucket_gapfill('2 hours', bucket, $2::timestamptz, $3::timestamptz) AS bucket, integral(rollup(tw)) AS value \
             FROM readings_1h WHERE uuid = $1 AND bucket >= $2 AND bucket < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    // --- the delta/median raw-force override ---

    #[test]
    fn delta_forces_raw_even_on_a_huge_span() {
        // 1000 days would route to readings_1d (1-day buckets), but a counter
        // delta must read raw; the 1-day width is kept.
        assert_eq!(
            sql_for_span(Agg::Delta, 1_000 * DAY_SECS),
            "SELECT time_bucket_gapfill('1 day', ts, $2::timestamptz, $3::timestamptz) AS bucket, last(value, ts) - first(value, ts) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn median_forces_raw_even_on_a_huge_span() {
        assert_eq!(
            sql_for_span(Agg::Median, 1_000 * DAY_SECS),
            "SELECT time_bucket_gapfill('1 day', ts, $2::timestamptz, $3::timestamptz) AS bucket, \
             percentile_cont(0.5) WITHIN GROUP (ORDER BY value) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    // --- calendar-month bucketing ---

    #[test]
    fn month_rolls_up_from_readings_1h_in_local_time() {
        let sql = build_sql(
            Agg::Integral,
            Gapfill::None,
            t(1_600_000_000),
            t(1_700_000_000),
            Some(Bucket::Month),
        );
        assert_eq!(
            sql,
            "SELECT time_bucket_gapfill('1 month', bucket, current_setting('TimeZone'), $2::timestamptz, $3::timestamptz) AS bucket, \
             integral(rollup(tw)) AS value \
             FROM readings_1h WHERE uuid = $1 AND bucket >= $2 AND bucket < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn month_for_delta_sources_from_raw() {
        // delta is raw-only, so a monthly counter bucket reads raw with the same
        // tz-aware time_bucket_gapfill over ts.
        let sql = build_sql(
            Agg::Delta,
            Gapfill::None,
            t(1_600_000_000),
            t(1_700_000_000),
            Some(Bucket::Month),
        );
        assert_eq!(
            sql,
            "SELECT time_bucket_gapfill('1 month', ts, current_setting('TimeZone'), $2::timestamptz, $3::timestamptz) AS bucket, \
             last(value, ts) - first(value, ts) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn finest_bucket_for_a_tiny_span() {
        // span < TARGET_BUCKETS → ideal 0 → the 1-second floor.
        assert_eq!(
            sql_for_span(Agg::Avg, 100),
            "SELECT time_bucket_gapfill('1 second', ts, $2::timestamptz, $3::timestamptz) AS bucket, avg(value) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn densified_sub_hour_rung_snaps_to_two_seconds() {
        // ideal = 2 lands on the 2-second rung added to break the old 1s→5s gap.
        assert_eq!(
            sql_for_span(Agg::Avg, 2 * TARGET_BUCKETS),
            "SELECT time_bucket_gapfill('2 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, avg(value) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn densified_day_rung_snaps_to_three_days() {
        // ideal = 3 days lands on the 3-day rung added to break the old 2d→7d gap.
        assert_eq!(
            sql_for_span(Agg::Avg, 3 * DAY_SECS * TARGET_BUCKETS),
            "SELECT time_bucket_gapfill('3 days', bucket, $2::timestamptz, $3::timestamptz) AS bucket, sum(s) / NULLIF(sum(n), 0) AS value \
             FROM readings_1d WHERE uuid = $1 AND bucket >= $2 AND bucket < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    // --- gapfill mode: the agg default and explicit overrides ---

    #[test]
    fn locf_override_carries_the_last_level() {
        // A held level (a temperature, a valve) opts into locf so an empty bucket
        // carries the previous value instead of reading 0. It is no longer a
        // default: an absent gapfill is None (see the bare-aggregate tests above).
        let sql = build_sql(
            Agg::Avg,
            Gapfill::Locf,
            t(1_600_000_000),
            t(1_600_003_600),
            None,
        );
        assert_eq!(
            sql,
            "SELECT time_bucket_gapfill('5 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, \
             locf(avg(value)) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn interpolate_override_ramps_between_points() {
        // An explicit interpolate wraps the value in interpolate(), for a
        // smoothly-varying, coarsely-sampled signal (an absent gapfill is none).
        let sql = build_sql(
            Agg::Avg,
            Gapfill::Interpolate,
            t(1_600_000_000),
            t(1_600_003_600),
            None,
        );
        assert_eq!(
            sql,
            "SELECT time_bucket_gapfill('5 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, \
             interpolate(avg(value)) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    #[test]
    fn none_override_leaves_a_gauge_bare() {
        // none is the default; on a gauge it leaves the bare aggregate, and an
        // empty bucket reads 0 when rows are mapped.
        let sql = build_sql(
            Agg::Avg,
            Gapfill::None,
            t(1_600_000_000),
            t(1_600_003_600),
            None,
        );
        assert_eq!(
            sql,
            "SELECT time_bucket_gapfill('5 seconds', ts, $2::timestamptz, $3::timestamptz) AS bucket, \
             avg(value) AS value \
             FROM readings WHERE uuid = $1 AND ts >= $2 AND ts < $3 GROUP BY 1 ORDER BY 1"
        );
    }

    // --- the absent-gapfill default: keyed on chart type, agg pins the flows ---

    #[test]
    fn gauge_line_defaults_to_locf() {
        // A held level drawn as a line carries across empty buckets, so zooming
        // in past the log cadence stays a curve instead of picket-fencing to 0.
        for agg in [Agg::Avg, Agg::Median, Agg::Min, Agg::Max, Agg::Last] {
            assert_eq!(default_gapfill(agg, SeriesType::Line), Gapfill::Locf);
        }
    }

    #[test]
    fn gauge_bar_defaults_to_none() {
        // A quantity drawn as bars reads 0 when absent (an on-change meter quiet
        // at zero), so an empty bucket must stay 0, not carry the last level.
        for agg in [Agg::Avg, Agg::Median, Agg::Min, Agg::Max, Agg::Last] {
            assert_eq!(default_gapfill(agg, SeriesType::Bar), Gapfill::None);
        }
    }

    #[test]
    fn accumulations_default_to_none_regardless_of_type() {
        // delta/integral have a genuine zero in an empty bucket; the type never
        // overrides that, so a counter charted as a line still reads 0, not locf.
        for agg in [Agg::Delta, Agg::Integral] {
            assert_eq!(default_gapfill(agg, SeriesType::Line), Gapfill::None);
            assert_eq!(default_gapfill(agg, SeriesType::Bar), Gapfill::None);
        }
    }

    #[test]
    fn build_pool_is_lazy_and_needs_no_database() {
        // deadpool creates connections on demand, so a pool over an unreachable
        // host still builds; this keeps the offline suite green.
        let config = TimescaleConfig {
            host: "db.invalid".to_owned(),
            port: 5432,
            user: "loxweb_ro".to_owned(),
            password: "x".to_owned(),
            dbname: "loxtap".to_owned(),
            sslmode: None,
            timezone: None,
        };
        assert!(build_pool(&config).is_ok());
    }

    /// Round-trip a couple of synthetic raw rows through the pool and prove an
    /// avg and an integral query come back. Skips cleanly when TEST_DATABASE_URL
    /// is unset, so the offline suite stays green (there is no live store here).
    #[tokio::test]
    async fn queries_synthetic_rows_against_a_live_store() {
        let Ok(dsn) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset, skipping live query test");
            return;
        };
        let pg_config: tokio_postgres::Config = dsn.parse().expect("parse TEST_DATABASE_URL");
        let pool = pool_from_pg_config(pg_config).expect("build pool");

        let uuid: Uuid = "deadbeef-0003-0000-0000-000000000001".parse().unwrap();
        let base = UNIX_EPOCH + Duration::from_secs(1_600_000_000);

        let client = pool.get().await.expect("get client");
        // Clear leftovers from an aborted run before inserting.
        client
            .execute("DELETE FROM readings WHERE uuid = $1", &[&uuid])
            .await
            .expect("cleanup");
        for i in 0u32..3 {
            let ts = base + Duration::from_secs(u64::from(i) * 10);
            let value = f64::from(i);
            client
                .execute(
                    "INSERT INTO readings (uuid, ts, value) VALUES ($1, $2, $3)",
                    &[&uuid, &ts, &value],
                )
                .await
                .expect("insert synthetic reading");
        }

        let from = base;
        let to = base + Duration::from_secs(3_600);

        for agg in [Agg::Avg, Agg::Integral] {
            let req = SeriesRequest {
                uuid,
                sub: "value".to_owned(),
                agg: Some(agg),
                gapfill: None,
                kind: SeriesType::Line,
                from,
                to,
                bucket: None,
            };
            let result = query_series(&pool, &req).await.expect("query series");
            assert!(!result.points.is_empty(), "{agg:?} must return buckets");
        }

        client
            .execute("DELETE FROM readings WHERE uuid = $1", &[&uuid])
            .await
            .expect("cleanup");
    }
}
