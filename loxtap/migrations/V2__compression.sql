-- Columnar compression on the readings hypertable.
--
-- readings is append-mostly and, by design, holds long runs of bit-identical f64
-- values for a series that sits still (a point lands only on change). Segmenting a
-- compressed chunk by uuid and ordering by ts groups each series' run contiguously,
-- where the columnar float codec XORs consecutive equal doubles down to almost
-- nothing. This is what makes the store viable at multi-year scale. The policy is
-- mandatory and fails silently if forgotten, so it lives in the schema rather than
-- in an operator runbook.
ALTER TABLE readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'uuid',
    timescaledb.compress_orderby   = 'ts DESC'
);

-- Compress chunks once they age past 7 days: recent data stays row-oriented for
-- cheap writes and point lookups, history goes columnar.
SELECT add_compression_policy('readings', INTERVAL '7 days');
