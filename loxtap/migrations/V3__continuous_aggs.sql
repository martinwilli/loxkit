-- Hierarchical continuous aggregates, so a multi-year range query reads a handful
-- of pre-aggregated rows instead of scanning raw points.
--
-- The toolkit supplies the time-weighted and counter partials the correct
-- integrals need. It is created here, IF NOT EXISTS-guarded, and fails loud with an
-- instructive error if it is ever absent.
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;

-- readings_1m: the fine 1-minute tier, the base every coarser tier rolls up from.
-- Created WITH NO DATA so the policy materializes forward; the one-time full-history
-- backfill runs outside any transaction in a later milestone.
--
-- The average is carried as the pair (n, s) so a hierarchical average is the exact
-- sum(s)/sum(n), never a lossy average-of-averages. time_weight and counter_agg are
-- stored as partials and finalized at query time: integral(rollup(tw)) gives a
-- correct time-weighted integral that a plain sample-count average gets wrong, and
-- delta(rollup(ctr)) handles counter resets. A gauge and a monotonic counter are not
-- interchangeable, so a query buckets by the stored agg_mode and reads the matching
-- partial.
CREATE MATERIALIZED VIEW readings_1m WITH (timescaledb.continuous) AS
SELECT uuid,
       time_bucket('1 minute', ts) AS bucket,
       count(value) AS n,
       sum(value)   AS s,
       min(value)   AS vmin,
       max(value)   AS vmax,
       last(value, ts) AS last_val,
       time_weight('LOCF', ts, value) AS tw,
       counter_agg(ts, value)         AS ctr
FROM readings
GROUP BY uuid, bucket
WITH NO DATA;

-- readings_1h rolls up readings_1m; readings_1d rolls up readings_1h. Rolling the
-- coarse tiers up from the fine one keeps refresh cheap and the partials composable:
-- sum(s)/sum(n) for the average, min(vmin)/max(vmax) for the extremes,
-- last(last_val, bucket) for the latest value, and rollup() over the time-weight and
-- counter partials so integral()/delta() finalize identically at any tier.
CREATE MATERIALIZED VIEW readings_1h WITH (timescaledb.continuous) AS
SELECT uuid,
       time_bucket('1 hour', bucket) AS bucket,
       sum(n)    AS n,
       sum(s)    AS s,
       min(vmin) AS vmin,
       max(vmax) AS vmax,
       last(last_val, bucket) AS last_val,
       rollup(tw)  AS tw,
       rollup(ctr) AS ctr
FROM readings_1m
GROUP BY uuid, time_bucket('1 hour', bucket)
WITH NO DATA;

CREATE MATERIALIZED VIEW readings_1d WITH (timescaledb.continuous) AS
SELECT uuid,
       time_bucket('1 day', bucket) AS bucket,
       sum(n)    AS n,
       sum(s)    AS s,
       min(vmin) AS vmin,
       max(vmax) AS vmax,
       last(last_val, bucket) AS last_val,
       rollup(tw)  AS tw,
       rollup(ctr) AS ctr
FROM readings_1h
GROUP BY uuid, time_bucket('1 day', bucket)
WITH NO DATA;

-- Materialize each tier forward. Every tier's end_offset is at least its parent's
-- bucket width, so a refresh never reads a not-yet-materialized parent row.
SELECT add_continuous_aggregate_policy('readings_1m',
    start_offset      => INTERVAL '3 hours',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

SELECT add_continuous_aggregate_policy('readings_1h',
    start_offset      => INTERVAL '1 day',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('readings_1d',
    start_offset      => INTERVAL '1 month',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');
