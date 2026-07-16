-- Add first_val to both continuous aggregates so delta rolls up off the tiers.
--
-- delta is net change over a bucket, last value minus first (the counter-safe
-- form that ignores intra-bucket float-ULP wobble). Until now the caggs stored
-- last_val but no first_val, so delta could not be composed on the coarse tiers
-- and every counter chart fell back to a per-point raw scan, even over a
-- multi-year span. With first_val alongside last_val, delta composes as
-- last(last_val) - first(first_val) straight off readings_1h/readings_1d.
--
-- A cagg's column list is fixed at creation (no ALTER MATERIALIZED VIEW ... ADD
-- COLUMN for a continuous aggregate), so both tiers are dropped and recreated,
-- parents first, exactly as the previous retire migration did. Dropping a cagg
-- takes its refresh and compression policies with it, so every policy, chunk
-- interval, and columnstore setting is reattached below. Recreated WITH NO DATA:
-- the forward policy materializes the recent window, and the one-time full-
-- history refresh runs out of band (outside a transaction, the backfill driver).

-- Drop in dependency order: 1d rolls up 1h, so the parent goes first. Dropping a
-- cagg takes its refresh and compression policies with it.
DROP MATERIALIZED VIEW readings_1d;
DROP MATERIALIZED VIEW readings_1h;

-- readings_1h: the fine tier, aggregating raw readings directly. The average is
-- carried as (n, s) so a hierarchical average stays the exact sum(s)/sum(n);
-- time_weight is stored as a partial and finalized with integral()/average() at
-- query time. first_val/last_val bracket the bucket for delta. Created WITH NO
-- DATA: the policy materializes forward and the one-time full-history refresh
-- runs outside a transaction (the backfill driver).
CREATE MATERIALIZED VIEW readings_1h WITH (timescaledb.continuous) AS
SELECT uuid,
       time_bucket('1 hour', ts) AS bucket,
       count(value) AS n,
       sum(value)   AS s,
       min(value)   AS vmin,
       max(value)   AS vmax,
       first(value, ts) AS first_val,
       last(value, ts)  AS last_val,
       time_weight('LOCF', ts, value) AS tw
FROM readings
GROUP BY uuid, bucket
WITH NO DATA;

-- readings_1d rolls up readings_1h: sum(s)/sum(n) for the average, min/max for the
-- extremes, first(first_val, bucket)/last(last_val, bucket) to bracket the day for
-- delta, and rollup() over the time-weight partial so integral()/average()
-- finalize identically at either tier.
CREATE MATERIALIZED VIEW readings_1d WITH (timescaledb.continuous) AS
SELECT uuid,
       time_bucket('1 day', bucket) AS bucket,
       sum(n)    AS n,
       sum(s)    AS s,
       min(vmin) AS vmin,
       max(vmax) AS vmax,
       first(first_val, bucket) AS first_val,
       last(last_val, bucket)   AS last_val,
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
-- wide to age chunks out at a useful cadence, so shrink each tier as before. New
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
