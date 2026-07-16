-- Two labeled views over the uuid-keyed join, at their two natural cardinalities.
-- Both LEFT JOIN series_meta so an unresolved wire uuid (one written to readings
-- and series_state with no series_meta row yet) still appears under its uuid
-- instead of being silently dropped.

-- One row per reading: the time-series graphing view. Splices the mutable labels
-- onto every stored point so a query can select and group by name/room/category.
CREATE VIEW readings_labeled AS
SELECT r.ts, r.uuid, r.value,
       m.name, m.room, m.category, m.sub, m.unit, m.agg_mode
FROM readings r
LEFT JOIN series_meta m USING (uuid);

-- One row per series: the current-state view, splicing the liveness/latest-value
-- cache onto the labels. Drives status and liveness panels and gives an O(1)
-- latest-per-series lookup without a DISTINCT ON ... ORDER BY ts DESC scan over
-- the compressed hypertable. A sibling of readings_labeled rather than extra
-- columns on it because the cardinalities differ: joining the per-series state
-- into the per-reading view would stamp the same last_seen and last_value onto
-- every historical row and add a join to the hot graphing path.
CREATE VIEW series_current AS
SELECT s.uuid, s.last_seen, s.last_value,
       m.name, m.room, m.category, m.sub, m.unit, m.agg_mode
FROM series_state s
LEFT JOIN series_meta m USING (uuid);
