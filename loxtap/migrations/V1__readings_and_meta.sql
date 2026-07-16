CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Narrow hot table: the state uuid alone keys a series, so no sub column and no
-- constraints. An unresolved wire uuid must still land, so readings constrains
-- nothing; this also keeps the column set minimal for columnar compression.
CREATE TABLE readings (
    uuid  uuid             NOT NULL,
    ts    timestamptz      NOT NULL,
    value double precision NOT NULL
);
SELECT create_hypertable('readings', by_range('ts'));
CREATE INDEX readings_uuid_ts ON readings (uuid, ts DESC);

-- Mutable labels, keyed on the stable uuid: a rename is one UPDATE here, never a
-- new series. agg_mode is populated by the ingest path; the column exists here.
CREATE TABLE series_meta (
    uuid     uuid PRIMARY KEY,
    control  text NOT NULL,
    name     text NOT NULL,
    room     text,
    category text,
    sub      text NOT NULL,
    unit     text,
    agg_mode text NOT NULL DEFAULT 'gauge'    -- 'gauge' | 'counter'
);

-- Last-observed liveness plus latest-value cache, one row per series, updated on
-- every event. Derivable from readings, so a rebuildable cache, not a source of
-- truth. Created here so the schema is coherent in a single migration.
CREATE TABLE series_state (
    uuid       uuid PRIMARY KEY,
    last_seen  timestamptz      NOT NULL,
    last_value double precision NOT NULL
);
