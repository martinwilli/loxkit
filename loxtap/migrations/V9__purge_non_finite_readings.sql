-- Purge the non-finite readings stored before ingest started dropping them.
--
-- A Miniserver forwards NaN for a value it failed to compute, and the daemon
-- used to store it verbatim. A single one poisons every aggregate over its
-- bucket: avg and the time-weighted integral turn NaN, and so does max, since
-- Postgres sorts NaN above every number. The continuous aggregates carry it up,
-- so one bad sample blanks a whole day on readings_1d. Infinities break the same
-- aggregates, so they go too.
--
-- A plain DELETE ... WHERE value IN (...) is a trap on the compressed
-- hypertable: value is neither segmentby nor orderby, so nothing prunes the
-- batches, and the delete decompresses the entire history back into the chunk
-- heaps within one transaction. So find the rows with a read-only scan first
-- (which decompresses in memory only), then delete each one by its uuid and ts.
-- Spelled out as literals, those let chunk exclusion pick the one chunk, and the
-- segmentby plus the ts min/max metadata pick the one batch to decompress. The
-- per-transaction decompression limit is lifted, as those batches add up past
-- its default across a long history.
--
-- The delete invalidates the affected buckets in both continuous aggregates, but
-- the refresh policies only reach back their start_offset (1 day on readings_1h,
-- 1 month on readings_1d), so older buckets keep their NaN until a one-time
-- refresh. That has to run out of band, as refresh_continuous_aggregate()
-- refuses a transaction block. A NULL window refreshes just the invalidated
-- ranges, and readings_1d rolls up readings_1h, so the order matters:
--
--   CALL refresh_continuous_aggregate('readings_1h', NULL, NULL);
--   CALL refresh_continuous_aggregate('readings_1d', NULL, NULL);
SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0;

DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT uuid, ts FROM readings WHERE value IN ('NaN', 'Infinity', '-Infinity')
    LOOP
        EXECUTE format(
            'DELETE FROM readings WHERE uuid = %L AND ts = %L '
            'AND value IN (''NaN'', ''Infinity'', ''-Infinity'')',
            r.uuid, r.ts);
    END LOOP;
END
$$;
