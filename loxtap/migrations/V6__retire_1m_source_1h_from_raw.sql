-- Retire the 1-minute continuous aggregate; source the hourly tier from raw.
--
-- The 1m tier stored more than the raw it summarized: a point lands only on
-- change, so most series occupy one 1m bucket per reading, and a bucket row
-- carries the variable-length time_weight/counter_agg partials that the float
-- codec can't touch (V5's warning, borne out: it compressed ~1x, not the raw's
-- ~12x). It also lost to a plain raw scan for the single-series queries it was
-- meant to accelerate, since raw is narrow and Gorilla-compressed. So drop it and
-- roll the hourly tier up from readings directly; raw stays the full-resolution
-- source for anything finer than an hour.
--
-- counter_agg goes too. Only a handful of series are counters, but a cagg reads
-- readings (uuid/ts/value) and can't tell a counter from a gauge (agg_mode lives
-- in series_meta, and a cagg can't join), so the 184-byte partial rode every
-- gauge bucket for nothing. A counter's delta()/rate() now comes from raw on
-- demand (cheap for one uuid over a bounded range), which is where the coarse
-- gauge partials came from anyway.

-- Drop in dependency order: 1d rolls up 1h rolls up 1m, so the parents go first.
-- Dropping a cagg takes its refresh and compression policies with it.
DROP MATERIALIZED VIEW readings_1d;
DROP MATERIALIZED VIEW readings_1h;
DROP MATERIALIZED VIEW readings_1m;

-- readings_1h: now the fine tier, aggregating raw readings directly. The average
-- is carried as (n, s) so a hierarchical average stays the exact sum(s)/sum(n);
-- time_weight is stored as a partial and finalized with integral()/average() at
-- query time. No counter_agg (see above). Created WITH NO DATA: the policy
-- materializes forward and the one-time full-history refresh runs outside a
-- transaction (the backfill driver).
CREATE MATERIALIZED VIEW readings_1h WITH (timescaledb.continuous) AS
SELECT uuid,
       time_bucket('1 hour', ts) AS bucket,
       count(value) AS n,
       sum(value)   AS s,
       min(value)   AS vmin,
       max(value)   AS vmax,
       last(value, ts) AS last_val,
       time_weight('LOCF', ts, value) AS tw
FROM readings
GROUP BY uuid, bucket
WITH NO DATA;

-- readings_1d rolls up readings_1h: sum(s)/sum(n) for the average, min/max for the
-- extremes, last(last_val, bucket) for the latest, and rollup() over the time-
-- weight partial so integral()/average() finalize identically at either tier.
CREATE MATERIALIZED VIEW readings_1d WITH (timescaledb.continuous) AS
SELECT uuid,
       time_bucket('1 day', bucket) AS bucket,
       sum(n)    AS n,
       sum(s)    AS s,
       min(vmin) AS vmin,
       max(vmax) AS vmax,
       last(last_val, bucket) AS last_val,
       rollup(tw) AS tw
FROM readings_1h
GROUP BY uuid, time_bucket('1 day', bucket)
WITH NO DATA;

-- Materialize each tier forward. readings_1h reads raw (always present), so it has
-- no not-yet-materialized-parent constraint; readings_1d's end_offset (1 day) stays
-- at least its parent's bucket width (1 hour).
SELECT add_continuous_aggregate_policy('readings_1h',
    start_offset      => INTERVAL '1 day',
    end_offset        => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

SELECT add_continuous_aggregate_policy('readings_1d',
    start_offset      => INTERVAL '1 month',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');

-- Chunk intervals: the materialization hypertable defaults to 10x the bucket, too
-- wide to age chunks out at a useful cadence, so shrink each tier as V5 did. New
-- chunks only; nothing exists yet under WITH NO DATA.
SELECT set_chunk_time_interval('readings_1h', INTERVAL '30 days');
SELECT set_chunk_time_interval('readings_1d', INTERVAL '365 days');

-- Columnstore each tier, segmenting by uuid and ordering by bucket so a series'
-- run of buckets sits contiguously and a single-uuid query skips the rest.
ALTER MATERIALIZED VIEW readings_1h SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'uuid',
    timescaledb.compress_orderby   = 'bucket DESC');
ALTER MATERIALIZED VIEW readings_1d SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'uuid',
    timescaledb.compress_orderby   = 'bucket DESC');

-- Compress closed chunks, with compress_after above each tier's refresh start_offset
-- so a bucket is only compressed once it has fallen out of the refresh window.
SELECT add_compression_policy('readings_1h', compress_after => INTERVAL '7 days');
SELECT add_compression_policy('readings_1d', compress_after => INTERVAL '3 months');
