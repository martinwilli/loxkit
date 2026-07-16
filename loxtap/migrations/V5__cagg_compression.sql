-- Columnar compression on the continuous aggregates. V2 compresses the raw
-- readings hypertable about 7x; once that lands, the pre-aggregated tiers (the
-- 1-minute one above all) become the store's dominant consumer, so they need
-- compressing too or they undo the multi-year sizing.
--
-- Segment each tier by uuid and order by bucket, mirroring readings: a series' run
-- of buckets sits contiguously, so a query filtering one uuid skips the other
-- segments and the codec sees each series in order. The catch specific to the
-- caggs: the average and extended-stat partials (time_weight, counter_agg) are
-- variable-length toolkit types that only take generic compression, not the float
-- codec the raw f64s get. So the tiers compress a few-x, not ~7x, and only once a
-- chunk holds many buckets per series. compress_after (below) is what guarantees a
-- chunk is closed and full before it compresses, sidestepping the case where a
-- barely-populated chunk compresses to larger than it started.

-- The materialization hypertable's chunk interval defaults to 10x the parent's, so
-- 70 days here. At that width the actively-refreshed tail keeps the whole chunk hot
-- for its entire life: nothing ages out to compress and the uncompressed hot set is
-- unbounded. Shrink each tier so closed chunks age out at a useful cadence and the
-- hot set stays bounded. New chunks only; the current chunk keeps its width. The
-- cagg name is the portable handle here (the mat-hypertable's internal name is not
-- stable across installs).
SELECT set_chunk_time_interval('readings_1m', INTERVAL '7 days');
SELECT set_chunk_time_interval('readings_1h', INTERVAL '30 days');
SELECT set_chunk_time_interval('readings_1d', INTERVAL '365 days');

-- Enable columnstore on each tier.
ALTER MATERIALIZED VIEW readings_1m SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'uuid',
    timescaledb.compress_orderby   = 'bucket DESC');
ALTER MATERIALIZED VIEW readings_1h SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'uuid',
    timescaledb.compress_orderby   = 'bucket DESC');
ALTER MATERIALIZED VIEW readings_1d SET (
    timescaledb.compress = true,
    timescaledb.compress_segmentby = 'uuid',
    timescaledb.compress_orderby   = 'bucket DESC');

-- Compress closed chunks, with compress_after set above each tier's refresh
-- start_offset (3 hours / 1 day / 1 month in V3) so a bucket is only compressed
-- once it has fallen out of the refresh window and will never be re-materialized.
SELECT add_compression_policy('readings_1m', compress_after => INTERVAL '1 day');
SELECT add_compression_policy('readings_1h', compress_after => INTERVAL '7 days');
SELECT add_compression_policy('readings_1d', compress_after => INTERVAL '3 months');
