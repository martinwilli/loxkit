# loxweb

A config-driven dashboard engine for a Loxone → TimescaleDB store. It reads the
uuid-keyed series that the `loxtap` bridge writes and charts them with
[Apache ECharts](https://echarts.apache.org/) — the views are *data*
(`dashboards.yaml`), not code, so you describe panels once and the engine renders
them. loxweb is a pure reader: it needs the shared database, not the loxtap
process.

## Running

```sh
loxweb --config config.yaml
```

Start from the samples:

- `config.example.yaml` — the server bind, an optional `base_path` for a
  reverse-proxy sub-path mount, the read-only database connection, and the path
  to the dashboards file.
- `dashboards.example.yaml` — a synthetic set of views that exercises every
  schema feature; it runs out of the box.

Editing `dashboards.yaml` reloads the views automatically within a couple of
seconds, without dropping the service and with no signal or command needed; an
invalid edit is logged and the previous set stays live.

## The dashboards schema

A file is `views[]`; a view is a tab of `panels[]`; a panel is `axes[] +
series[]`. A **series** is either *bound* to a store uuid or *derived* from its
siblings.

```yaml
views:
  - id: overview          # stable id, used in the URL
    title: Overview
    range: now-2d          # default window (relative: now-<n>{s,m,h,d,w,M,y})
    panels:
      - title: Climate
        height: 10                       # relative height hint
        axes:
          - {id: temp, side: left,  unit: "°C", min: 0, max: 40}
          - {id: hum,  side: right, unit: "%",  min: 0, max: 100}
        series:
          - {id: t_in, label: Indoor, uuid: <u>, agg: avg, type: line, axis: temp, interp: smooth, fill: true}
          - {label: Humidity, uuid: <u>, agg: avg, type: bar, axis: hum, color: "#8bc34a"}
          - {label: Ratio, expr: "t_in / 100", type: line, axis: temp}
  - id: energy
    title: Energy
    range: now-3y
    bucket: month          # real calendar months, local time
    panels: [ ... ]
```

### Series keys

| Key | Meaning |
|-----|---------|
| `uuid` | Bound series: the store series to read (a `series_meta.uuid`). |
| `sub` | The sub-channel; defaults to `value`. Documentary — the uuid identifies the series. |
| `agg` | `avg` \| `max` \| `min` \| `median` \| `last` (plain), or `delta` \| `integral` (counter / time-weighted). Absent resolves from the series' stored kind. |
| `expr` | Derived series: `+ - * /`, parens, constants, and sibling series `id`s. Evaluated in declaration order, so a derived series may reference an earlier one. |
| `type` | `line` (default) or `bar`; may be mixed in one panel. |
| `axis` | Which declared axis `id` the series is drawn against. |
| `stack` | Series sharing a group stack together; a `scale: -1` series stacks below zero within its group. |
| `stack_strategy` | `samesign` (default) keeps mirrored (`scale: -1`) members below zero; `all` stacks across signs, for a min-max band whose base goes negative. |
| `scale` | Multiplier applied to every value, e.g. `-1` to mirror or `0.001` to rescale units. |
| `color` | A CSS color; absent uses the default palette. |
| `interp` | `linear` (default), `smooth`, or `step`. |
| `fill` | `true` to fill the area under a line. |
| `label` | Legend label. |

### Axes

Up to three y-axes per panel. Each has an `id` (referenced by `series.axis`), a
`side` (`left`/`right`), an optional `unit` label, and optional `min`/`max`. By
default a value axis includes the zero baseline; set `fit: true` to let it hug
the data range instead (an explicit `min`/`max` still pins that bound). A panel
with no `axes` gets a single implicit axis.

### Bucketing

The engine tier-routes by span: a wide window reads a coarse aggregate, a zoom
re-queries a finer one automatically. A `bucket` on a view or panel overrides
that router. It takes two forms.

**Scalar shorthand** — `bucket: day` or `bucket: month` pins real calendar days
or months in the store's local time, so a daily-reset counter charted with `max`
reads as one whole-day (or -month) total instead of fragmenting across sub-day
buckets. Each is just a one-rule always-on schedule (below).

**Schedule** — an array of `{upto, every}` rules that pick the bucket width from
the current window span, so the resolution tracks the zoom you choose:

```yaml
bucket:
  - {upto: 10h, every: 1m}    # span < 10 hours  -> 1-minute buckets
  - {upto: 10d, every: 1h}    # span < 10 days   -> 1-hour buckets
  - {upto: 10M, every: 1d}    # span < 10 months -> calendar days (local tz)
  - {upto: 5y,  every: 1M}    # span < 5 years   -> calendar months (local tz)
```

Rules are matched in order: the first whose `upto` the span is **strictly below**
wins; past the last rule the coarsest `every` clamps. Both `upto` and `every` use
the compact `<count><unit>` grammar of `range` — `s`/`m`/`h`/`d`/`w`/`M`/`y`, with
lowercase `m` minutes and uppercase `M` months. The `every` unit decides the rest:
a **calendar** unit (`d`/`w`/`M`/`y`) buckets on local boundaries, a **clock** unit
(`s`/`m`/`h`) in plain wall time. `upto` thresholds must strictly increase.

## Seeding synthetic data for the example

`dashboards.example.yaml` references made-up uuids, so the panels render empty
against a fresh store. To see lines, insert a few points for one of them:

```sql
INSERT INTO series_meta (uuid, control, name, room, sub, unit, agg_mode)
VALUES ('deadbeef-0000-0000-0000-000000000001', 'InfoOnlyAnalog',
        'Indoor Temp', 'Room A', 'value', '°C', 'gauge');

INSERT INTO readings (uuid, ts, value)
SELECT 'deadbeef-0000-0000-0000-000000000001',
       now() - (g || ' hours')::interval,
       20 + 5 * sin(g / 3.0)
FROM generate_series(0, 47) AS g;
```

Repeat for the other uuids, or point the config at your own series.

## License

The engine is MIT-licensed. The vendored `web/echarts.min.js` is Apache ECharts,
distributed under Apache-2.0 (see `web/echarts.LICENSE`).
